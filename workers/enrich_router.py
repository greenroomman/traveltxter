# workers/enrich_router.py
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

    try:
        info = json.loads(raw)
    except Exception:
        info = json.loads(raw.replace("\\\\n", "\\n"))

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


def month_key_utc(now: dt.datetime) -> str:
    return now.strftime("%Y-%m")


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

    PHRASE_CHANNEL = _env("PHRASE_CHANNEL", "vip").lower()  # vip/free/ig (bank can hint)
    MAX_ROWS_PER_RUN = _env_int("ENRICH_MAX_ROWS_PER_RUN", 60)
    ENFORCE_MAX_PER_MONTH = _env_bool("ENFORCE_MAX_PER_MONTH", True)
    REQUIRE_PHRASE = _env_bool("REQUIRE_PHRASE", False)

    ELIGIBLE_STATUSES = {
        "SCORED",
        "READY_TO_POST",
        "READY_TO_PUBLISH",
        "PUBLISH_AM",
        "PUBLISH_PM",
        "PUBLISH_BOTH",
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
    # ----------------------------
    t0 = dt.datetime.now(dt.timezone.utc)
    iata_values = ws_iata.get_all_values()
    if not iata_values or len(iata_values) < 2:
        log("⚠️ IATA_MASTER empty; city/country fills disabled.")
        iata_map: Dict[str, Tuple[str, str]] = {}
    else:
        i_hdr = [_norm(h) for h in iata_values[0]]
        i_idx = {h: j for j, h in enumerate(i_hdr) if h}
        # expected: iata_code, city, country
        c_iata = i_idx.get("iata_code")
        c_city = i_idx.get("city")
        c_country = i_idx.get("country")

        iata_map = {}
        if c_iata is not None:
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
    # Load PHRASE_BANK (avoid get_all_records due to duplicate headers risk)
    # Headers you gave earlier:
    # destination_iata theme category phrase approved channel_hint max_per_month notes context_hint
    # ----------------------------
    t1 = dt.datetime.now(dt.timezone.utc)
    phr_values = ws_phr.get_all_values()
    phrase_rows: List[Dict[str, Any]] = []
    if phr_values and len(phr_values) >= 2:
        p_hdr_raw = phr_values[0]
        p_hdr = [_norm(h) for h in p_hdr_raw]
        p_idx = {h: j for j, h in enumerate(p_hdr) if h}

        def pv(row: List[str], h: str) -> str:
            j = p_idx.get(h)
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
            channel_hint = pv(r, "channel_hint").lower()
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
                    "max_per_month": mpm,  # 0 means unlimited
                }
            )

    log(f"✅ PHRASE_BANK loaded (approved): {len(phrase_rows)} phrases ({(dt.datetime.now(dt.timezone.utc)-t1).total_seconds():.1f}s)")

    # Index phrases by (dest_iata, theme)
    phrases_by_key: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for pr in phrase_rows:
        key = (pr["destination_iata"], pr["theme"])
        phrases_by_key.setdefault(key, []).append(pr)

    # ----------------------------
    # Load RAW_DEALS
    # ----------------------------
    t2 = dt.datetime.now(dt.timezone.utc)
    raw_values = ws_raw.get_all_values()
    if not raw_values or len(raw_values) < 2:
        log("❌ RAW_DEALS empty.")
        return 1

    hdr = raw_values[0]
    hnorm = [_norm(h) for h in hdr]
    hidx = {h: j for j, h in enumerate(hnorm) if h}

    def col(name: str) -> Optional[int]:
        return hidx.get(_norm(name))

    # Required columns (by your header list)
    c_status = col("status")
    c_deal_id = col("deal_id")
    c_origin_iata = col("origin_iata")
    c_dest_iata = col("destination_iata")
    c_origin_city = col("origin_city")
    c_origin_country = col("origin_country")
    c_dest_city = col("destination_city")
    c_dest_country = col("destination_country")
    c_theme = col("theme")
    c_deal_theme = col("deal_theme")
    c_phrase_bank = col("phrase_bank")
    c_phrase_used = col("phrase_used")  # read only (for governance counts)

    # Optional publish timestamps for "unpublished" gating in phrase governance (read-only)
    c_post_vip = col("posted_telegram_vip_at")
    c_post_free = col("posted_telegram_free_at")

    # Validate minimums
    needed = {
        "status": c_status,
        "deal_id": c_deal_id,
        "origin_iata": c_origin_iata,
        "destination_iata": c_dest_iata,
        "phrase_bank": c_phrase_bank,
    }
    missing = [k for k, v in needed.items() if v is None]
    if missing:
        log(f"❌ RAW_DEALS missing required headers: {missing}")
        return 1

    # Build phrase usage index (month-aware) from RAW_DEALS phrase_used + phrase_bank
    now = dt.datetime.now(dt.timezone.utc)
    mkey = month_key_utc(now)
    usage: Dict[str, int] = {}  # phrase -> count this month

    # We treat phrase usage as any phrase that appears in phrase_used or phrase_bank on any row this month
    # If you later add a dedicated "phrase_month" column, you can tighten this.
    # For now, we only count by appearance (idempotent enough for max_per_month).
    if c_phrase_used is not None or c_phrase_bank is not None:
        for r in raw_values[1:]:
            # If a row has created_at/posted timestamp month you want to key on later, wire it here.
            # For now: global monthly bucket (simple & safe).
            pu = (r[c_phrase_used] if c_phrase_used is not None and c_phrase_used < len(r) else "").strip()
            pb = (r[c_phrase_bank] if c_phrase_bank is not None and c_phrase_bank < len(r) else "").strip()
            for p in (pu, pb):
                if p:
                    usage[p] = usage.get(p, 0) + 1

    log(f"✅ Phrase usage index built (month bucket {mkey}): {len(usage)} unique phrases")

    # ----------------------------
    # Enrichment loop
    # ----------------------------
    scanned = len(raw_values) - 1
    eligible = 0
    enriched = 0
    cityfills = 0
    phrasefills = 0
    phrase_misses = 0

    # Build list of (row_number, col_number, new_value) updates
    updates: List[gspread.Cell] = []

    def cell(rownum: int, colnum0: int, value: str) -> None:
        # gspread.Cell takes 1-based col, row
        updates.append(gspread.Cell(rownum, colnum0 + 1, value))

    def getv(r: List[str], c: Optional[int]) -> str:
        if c is None:
            return ""
        return (r[c] if c < len(r) else "").strip()

    # Deterministic phrase candidate filter
    def phrase_candidates(dest_iata: str, theme_resolved: str) -> List[Dict[str, Any]]:
        dest_iata = (dest_iata or "").strip().upper()
        theme_resolved = (theme_resolved or "").strip().lower()
        if not dest_iata or not theme_resolved:
            return []
        items = phrases_by_key.get((dest_iata, theme_resolved), [])
        if not items:
            return []
        # channel_hint filter (soft)
        out = []
        for it in items:
            ch = (it.get("channel_hint") or "").strip().lower()
            if not ch or ch == "all" or ch == PHRASE_CHANNEL:
                out.append(it)
        return out or items

    def governance_ok(pr: Dict[str, Any]) -> bool:
        if not ENFORCE_MAX_PER_MONTH:
            return True
        mpm = int(pr.get("max_per_month") or 0)
        if mpm <= 0:
            return True
        phrase = pr.get("phrase") or ""
        return usage.get(phrase, 0) < mpm

    for i, r in enumerate(raw_values[1:], start=2):  # sheet row number
        status = getv(r, c_status)
        if status not in ELIGIBLE_STATUSES:
            continue

        deal_id = getv(r, c_deal_id)
        if not deal_id:
            continue

        eligible += 1
        if enriched >= MAX_ROWS_PER_RUN:
            break

        origin_iata = getv(r, c_origin_iata).upper()
        dest_iata = getv(r, c_dest_iata).upper()

        # Resolve theme: theme first, else deal_theme
        theme_resolved = (getv(r, c_theme) or getv(r, c_deal_theme)).strip().lower()

        # 1) City/country fills (only if blank and IATA exists)
        if iata_map:
            # destination city/country
            dest_city = getv(r, c_dest_city)
            dest_country = getv(r, c_dest_country)
            if (not dest_city or not dest_country) and dest_iata and dest_iata in iata_map:
                city_name, country_name = iata_map[dest_iata]
                if c_dest_city is not None and not dest_city and city_name:
                    cell(i, c_dest_city, city_name)
                    cityfills += 1
                if c_dest_country is not None and not dest_country and country_name:
                    cell(i, c_dest_country, country_name)
                    cityfills += 1

            # origin city/country
            org_city = getv(r, c_origin_city)
            org_country = getv(r, c_origin_country)
            if (not org_city or not org_country) and origin_iata and origin_iata in iata_map:
                city_name, country_name = iata_map[origin_iata]
                if c_origin_city is not None and not org_city and city_name:
                    cell(i, c_origin_city, city_name)
                    cityfills += 1
                if c_origin_country is not None and not org_country and country_name:
                    cell(i, c_origin_country, country_name)
                    cityfills += 1

        # 2) Phrase fill into phrase_bank only (never overwrite; phrase_used is downstream/publish audit)
        existing_phrase_bank = getv(r, c_phrase_bank)
        if not existing_phrase_bank:
            # Select phrase only if we have theme + dest_iata
            cands = phrase_candidates(dest_iata, theme_resolved)

            # Apply max-per-month governance
            cands_ok = [p for p in cands if governance_ok(p)]

            chosen = stable_pick(cands_ok, key_seed=f"{deal_id}:{dest_iata}:{theme_resolved}") if cands_ok else None
            if chosen:
                phrase = chosen["phrase"]
                cell(i, c_phrase_bank, phrase)
                phrasefills += 1
                # update usage index so subsequent rows in this run respect max_per_month
                usage[phrase] = usage.get(phrase, 0) + 1
            else:
                # Only count as miss if we would have tried (theme+dest exist) but governance eliminated all
                if dest_iata and theme_resolved:
                    phrase_misses += 1
                    if REQUIRE_PHRASE:
                        # Do nothing else; publisher can gate on missing phrase_bank if you want
                        pass

        if updates:
            enriched += 1

    log("------------------------------------------------------------")
    log(f"Scanned rows: {scanned}")
    log(f"Eligible rows ({'/'.join(sorted(ELIGIBLE_STATUSES))}): {eligible}")
    log(f"Rows enriched: {enriched} (cap {MAX_ROWS_PER_RUN})")
    log(f"City/Country fills: {cityfills}")
    log(f"Phrase fills: {phrasefills} (channel={PHRASE_CHANNEL})")
    log(f"Phrase misses (no candidate after governance): {phrase_misses}")
    log(f"Cells written (batch): {len(updates)}")

    if updates:
        ws_raw.update_cells(updates, value_input_option="USER_ENTERED")
        log("✅ Batch write complete.")
    else:
        log("✅ No changes needed (idempotent; safe to re-run).")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
