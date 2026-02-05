from __future__ import annotations

import os
import json
import re
import hashlib
import datetime as dt
from typing import Dict, List, Tuple, Any, Optional

import gspread
from google.oauth2.service_account import Credentials


# ----------------------------
# Logging
# ----------------------------
def log(msg: str) -> None:
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"{ts} | {msg}", flush=True)


# ----------------------------
# Env helpers
# ----------------------------
def _env(k: str, default: str = "") -> str:
    return str(os.getenv(k, default) or "").strip()


def _env_int(k: str, default: int) -> int:
    try:
        return int(_env(k, str(default)))
    except Exception:
        return default


def _env_bool(k: str, default: bool = False) -> bool:
    v = _env(k, "")
    if not v:
        return default
    return v.lower() in ("1", "true", "yes", "y", "on")


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (s or "").strip().lower()).strip("_")


# ----------------------------
# Google client
# ----------------------------
def gspread_client() -> gspread.Client:
    raw = _env("GCP_SA_JSON_ONE_LINE") or _env("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON or GCP_SA_JSON_ONE_LINE")

    # Robust to either raw JSON or escaped-newline variants
    try:
        info = json.loads(raw)
    except Exception:
        info = json.loads(raw.replace("\\\\n", "\\n").replace("\\n", "\n"))

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def open_ws(sh: gspread.Spreadsheet, name: str) -> gspread.Worksheet:
    name = (name or "").strip()
    if not name:
        raise RuntimeError("WorksheetNotFound: '' (blank tab name)")
    try:
        return sh.worksheet(name)
    except Exception as e:
        raise RuntimeError(f"WorksheetNotFound: '{name}'") from e


# ----------------------------
# Deterministic choice
# ----------------------------
def stable_pick(items: List[Dict[str, Any]], key_seed: str) -> Optional[Dict[str, Any]]:
    if not items:
        return None
    h = hashlib.sha256(key_seed.encode("utf-8")).hexdigest()
    idx = int(h[:8], 16) % len(items)
    return items[idx]


# ----------------------------
# Headers
# ----------------------------
def build_header_index(headers: List[str]) -> Tuple[Dict[str, int], Dict[str, List[int]]]:
    """
    Returns:
      primary_idx: norm_header -> first index
      dups: norm_header -> all indices (len>1)
    """
    normed = [_norm(h) for h in headers]
    all_idx: Dict[str, List[int]] = {}
    for j, h in enumerate(normed):
        if not h:
            continue
        all_idx.setdefault(h, []).append(j)

    primary_idx = {h: idxs[0] for h, idxs in all_idx.items()}
    dups = {h: idxs for h, idxs in all_idx.items() if len(idxs) > 1}
    return primary_idx, dups


# ----------------------------
# Main
# ----------------------------
def main() -> int:
    log("============================================================")
    log("🧩 TravelTxter Enrichment Router — V5 (IATA backfill + Phrase selection)")
    log("============================================================")

    SPREADSHEET_ID = _env("SPREADSHEET_ID") or _env("SHEET_ID")
    if not SPREADSHEET_ID:
        log("❌ Missing SPREADSHEET_ID / SHEET_ID")
        return 1

    RAW_TAB = _env("RAW_DEALS_TAB", "RAW_DEALS")
    PHRASE_BANK_TAB = _env("PHRASE_BANK_TAB", "PHRASE_BANK") or _env("PHRASES_TAB", "PHRASE_BANK")
    IATA_MASTER_TAB = _env("IATA_MASTER_TAB", "IATA_MASTER")

    PHRASE_CHANNEL = _env("PHRASE_CHANNEL", "vip").lower()  # vip/free/ig
    MAX_ROWS_PER_RUN = _env_int("ENRICH_MAX_ROWS_PER_RUN", 60)
    ENFORCE_MAX_PER_MONTH = _env_bool("ENFORCE_MAX_PER_MONTH", True)
    REQUIRE_PHRASE = _env_bool("REQUIRE_PHRASE", False)

    # ✅ V5 contract: enrich happens AFTER scorer marks publishability.
    # Include legacy + new minimal states.
    ELIGIBLE_STATUSES = {
        "PUBLISH_READY",      # V5 minimal publishable state
        "SCORED",             # if you still use this
        "READY_TO_POST",      # legacy
        "READY_TO_PUBLISH",   # legacy
        "PUBLISH_AM",         # legacy
        "PUBLISH_PM",         # legacy
        "PUBLISH_BOTH",       # legacy
        "VIP_DONE",           # optional: allow phrase fills even after VIP post
    }

    log(f"{RAW_TAB= } | {PHRASE_BANK_TAB= } | {IATA_MASTER_TAB= }")
    log(f"{PHRASE_CHANNEL= } | {MAX_ROWS_PER_RUN= } | {ENFORCE_MAX_PER_MONTH= } | {REQUIRE_PHRASE= }")
    log(f"ELIGIBLE_STATUSES={sorted(ELIGIBLE_STATUSES)}")

    gc = gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_raw = open_ws(sh, RAW_TAB)
    ws_iata = open_ws(sh, IATA_MASTER_TAB)
    ws_phr = open_ws(sh, PHRASE_BANK_TAB)

    # ----------------------------
    # Load IATA_MASTER
    # Expected headers: iata_code, city, country
    # ----------------------------
    t0 = dt.datetime.now(dt.timezone.utc)
    iata_values = ws_iata.get_all_values()
    iata_map: Dict[str, Tuple[str, str]] = {}

    if iata_values and len(iata_values) >= 2:
        i_idx, i_dups = build_header_index(iata_values[0])
        if i_dups:
            log(f"⚠️ IATA_MASTER duplicate headers (normed): {i_dups}")

        c_iata = i_idx.get("iata_code")
        c_city = i_idx.get("city")
        c_country = i_idx.get("country")

        if c_iata is None:
            log("❌ IATA_MASTER missing required header: iata_code")
            return 1

        for r in iata_values[1:]:
            code = (r[c_iata] if c_iata < len(r) else "").strip().upper()
            if not code:
                continue
            city = (r[c_city] if c_city is not None and c_city < len(r) else "").strip()
            country = (r[c_country] if c_country is not None and c_country < len(r) else "").strip()
            if code not in iata_map:
                iata_map[code] = (city, country)

    log(f"✅ IATA_MASTER indexed: {len(iata_map)} entries ({(dt.datetime.now(dt.timezone.utc)-t0).total_seconds():.1f}s)")

    # ----------------------------
    # Load PHRASE_BANK (approved only)
    # Expected headers:
    # destination_iata theme category phrase approved channel_hint max_per_month notes context_hint
    # ----------------------------
    t1 = dt.datetime.now(dt.timezone.utc)
    phr_values = ws_phr.get_all_values()
    phrase_rows: List[Dict[str, Any]] = []

    if phr_values and len(phr_values) >= 2:
        p_idx, p_dups = build_header_index(phr_values[0])
        if p_dups:
            log(f"⚠️ PHRASE_BANK duplicate headers (normed): {p_dups}")

        def pv(row: List[str], h: str) -> str:
            j = p_idx.get(_norm(h))
            return (row[j] if j is not None and j < len(row) else "").strip()

        for r in phr_values[1:]:
            phrase = pv(r, "phrase")
            if not phrase:
                continue

            approved = pv(r, "approved").lower()
            if approved not in ("true", "1", "yes", "y"):
                continue

            dest_iata = pv(r, "destination_iata").upper()
            theme = pv(r, "theme").lower()
            channel_hint = pv(r, "channel_hint").strip()
            max_per_month = pv(r, "max_per_month")

            try:
                mpm = int(max_per_month) if max_per_month else 0
            except Exception:
                mpm = 0

            phrase_rows.append(
                {
                    "destination_iata": dest_iata,
                    "theme": theme,
                    "category": pv(r, "category"),
                    "phrase": phrase,
                    "channel_hint": channel_hint,
                    "max_per_month": mpm,  # 0 = unlimited
                }
            )

    log(f"✅ PHRASE_BANK loaded (approved): {len(phrase_rows)} phrases ({(dt.datetime.now(dt.timezone.utc)-t1).total_seconds():.1f}s)")

    phrases_by_key: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for pr in phrase_rows:
        phrases_by_key.setdefault((pr["destination_iata"], pr["theme"]), []).append(pr)

    # ----------------------------
    # Load RAW_DEALS
    # ----------------------------
    raw_values = ws_raw.get_all_values()
    if not raw_values or len(raw_values) < 2:
        log("❌ RAW_DEALS empty.")
        return 1

    hdr = raw_values[0]
    hidx, hdups = build_header_index(hdr)
    if hdups:
        log(f"⚠️ RAW_DEALS duplicate headers detected (normed): {hdups}")

    def col(name: str) -> Optional[int]:
        return hidx.get(_norm(name))

    # required
    c_status = col("status")
    c_deal_id = col("deal_id")
    c_origin_iata = col("origin_iata")
    c_dest_iata = col("destination_iata")

    missing = [n for n, c in [("status", c_status), ("deal_id", c_deal_id), ("origin_iata", c_origin_iata), ("destination_iata", c_dest_iata)] if c is None]
    if missing:
        log(f"❌ RAW_DEALS missing required headers: {missing}")
        return 1

    # optional enrich targets
    c_origin_city = col("origin_city")
    c_dest_city = col("destination_city")
    c_dest_country = col("destination_country")

    c_theme = col("theme")

    # phrase outputs
    c_phrase_used = col("phrase_used")
    c_phrase_bank = col("phrase_bank")
    c_phrase_category = col("phrase_category")

    # choose write targets deterministically
    phrase_target_col = c_phrase_used if c_phrase_used is not None else c_phrase_bank
    phrase_target_name = "phrase_used" if c_phrase_used is not None else ("phrase_bank" if c_phrase_bank is not None else "")

    if not phrase_target_name:
        log("⚠️ RAW_DEALS has no phrase_used or phrase_bank column. Phrase fill disabled.")
    else:
        log(f"🧩 Phrase write target: {phrase_target_name}")

    # monthly usage index (simple: count phrases already present in the sheet)
    usage: Dict[str, int] = {}
    if c_phrase_used is not None:
        for r in raw_values[1:]:
            pu = (r[c_phrase_used] if c_phrase_used < len(r) else "").strip()
            if pu:
                usage[pu] = usage.get(pu, 0) + 1
    log(f"✅ Phrase usage index built: {len(usage)} unique phrases")

    # ----------------------------
    # Enrichment loop (batch updates)
    # ----------------------------
    scanned = len(raw_values) - 1
    eligible = 0
    enriched_rows = 0
    cityfills = 0
    phrasefills = 0
    phrasecatfills = 0
    phrase_misses = 0

    updates: List[gspread.Cell] = []

    def queue_cell(rownum: int, colnum0: int, value: str) -> None:
        updates.append(gspread.Cell(rownum, colnum0 + 1, value))

    def getv(r: List[str], c: Optional[int]) -> str:
        if c is None:
            return ""
        return (r[c] if c < len(r) else "").strip()

    def phrase_candidates(dest_iata: str, theme_resolved: str) -> List[Dict[str, Any]]:
        dest_iata = (dest_iata or "").strip().upper()
        theme_resolved = (theme_resolved or "").strip().lower()
        if not dest_iata or not theme_resolved:
            return []
        items = phrases_by_key.get((dest_iata, theme_resolved), [])
        if not items:
            return []

        # Only treat channel_hint as a strict filter if it's a real channel keyword.
        real_channels = {"vip", "free", "ig", "all"}
        out: List[Dict[str, Any]] = []
        for it in items:
            ch = (it.get("channel_hint") or "").strip().lower()
            if ch in real_channels:
                if ch == "all" or ch == PHRASE_CHANNEL:
                    out.append(it)
            else:
                out.append(it)  # descriptive label, not a filter
        return out or items

    def governance_ok(pr: Dict[str, Any]) -> bool:
        if not ENFORCE_MAX_PER_MONTH:
            return True
        mpm = int(pr.get("max_per_month") or 0)
        if mpm <= 0:
            return True
        phrase = pr.get("phrase") or ""
        return usage.get(phrase, 0) < mpm

    for i, r in enumerate(raw_values[1:], start=2):  # sheet row numbers
        status = getv(r, c_status)
        if status not in ELIGIBLE_STATUSES:
            continue

        deal_id = getv(r, c_deal_id)
        if not deal_id:
            continue

        eligible += 1
        if enriched_rows >= MAX_ROWS_PER_RUN:
            break

        origin_iata = getv(r, c_origin_iata).upper()
        dest_iata = getv(r, c_dest_iata).upper()
        theme_resolved = getv(r, c_theme).strip().lower()

        row_had_update = False

        # 1) City/country fills
        if iata_map:
            if dest_iata and dest_iata in iata_map:
                city_name, country_name = iata_map[dest_iata]
                if c_dest_city is not None and not getv(r, c_dest_city) and city_name:
                    queue_cell(i, c_dest_city, city_name)
                    cityfills += 1
                    row_had_update = True
                if c_dest_country is not None and not getv(r, c_dest_country) and country_name:
                    queue_cell(i, c_dest_country, country_name)
                    cityfills += 1
                    row_had_update = True

            if origin_iata and origin_iata in iata_map:
                city_name, _country = iata_map[origin_iata]
                if c_origin_city is not None and not getv(r, c_origin_city) and city_name:
                    queue_cell(i, c_origin_city, city_name)
                    cityfills += 1
                    row_had_update = True

        # 2) Phrase fill (+ phrase_category)
        if phrase_target_col is not None:
            existing_phrase = getv(r, phrase_target_col)
            if not existing_phrase:
                cands = phrase_candidates(dest_iata, theme_resolved)
                cands_ok = [p for p in cands if governance_ok(p)]
                chosen = stable_pick(cands_ok, key_seed=f"{deal_id}:{dest_iata}:{theme_resolved}") if cands_ok else None

                if chosen:
                    phrase = chosen["phrase"]
                    queue_cell(i, phrase_target_col, phrase)
                    phrasefills += 1
                    usage[phrase] = usage.get(phrase, 0) + 1
                    row_had_update = True

                    # also write phrase_category if the column exists and it's empty
                    if c_phrase_category is not None and not getv(r, c_phrase_category):
                        cat = (chosen.get("category") or "").strip()
                        if cat:
                            queue_cell(i, c_phrase_category, cat)
                            phrasecatfills += 1
                            row_had_update = True
                else:
                    if dest_iata and theme_resolved:
                        phrase_misses += 1
                        # if REQUIRE_PHRASE, downstream can gate; we do not change status here

        if row_had_update:
            enriched_rows += 1

    log("------------------------------------------------------------")
    log(f"Scanned rows: {scanned}")
    log(f"Eligible rows: {eligible}")
    log(f"Rows enriched: {enriched_rows} (cap {MAX_ROWS_PER_RUN})")
    log(f"City/Country fills: {cityfills}")
    log(f"Phrase fills: {phrasefills} (channel={PHRASE_CHANNEL})")
    log(f"Phrase category fills: {phrasecatfills}")
    log(f"Phrase misses: {phrase_misses}")
    log(f"Cells written (batch): {len(updates)}")

    if updates:
        ws_raw.update_cells(updates, value_input_option="USER_ENTERED")
        log("✅ Batch write complete.")
    else:
        log("✅ No changes needed (idempotent; safe to re-run).")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
