#!/usr/bin/env python3
# workers/enrich_router.py
#
# TRAVELTXTTER V5 — ENRICH ROUTER (MINIMAL CONTRACT)
#
# PURPOSE
# - Fill missing city/country fields via IATA_MASTER lookup
# - Fill phrase_used + phrase_category via PHRASE_BANK (approved only)
#
# READS
# - RAW_DEALS
# - IATA_MASTER (iata_code, city, country)
# - PHRASE_BANK (destination_iata, theme, category, phrase, approved, channel_hint, max_per_month, ...)
#
# WRITES (RAW_DEALS ONLY; IF COLUMNS EXIST)
# - origin_city
# - destination_city
# - destination_country
# - phrase_used
# - phrase_category
#
# NEVER WRITES
# - status / publish_window / score / posted timestamps / RDV
#
# IDEMPOTENT
# - does not overwrite existing non-empty cells

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import json
import os
import re
from typing import Any, Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials

GOOGLE_SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]


# ----------------------------- logging -----------------------------


def log(msg: str) -> None:
    ts = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    print(f"{ts} | {msg}", flush=True)


# ----------------------------- env helpers -----------------------------


def env(k: str, d: str = "") -> str:
    return (os.getenv(k, d) or "").strip()


def env_int(k: str, d: int) -> int:
    try:
        return int(env(k, str(d)))
    except Exception:
        return d


def env_bool(k: str, d: bool = False) -> bool:
    v = env(k, "")
    if not v:
        return d
    return v.lower() in ("1", "true", "yes", "y", "on")


# ----------------------------- sheet helpers -----------------------------


def _norm_header(h: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (h or "").strip().lower()).strip("_")


def header_map_first(headers: List[str]) -> Dict[str, int]:
    """Duplicate-header-safe: first occurrence wins."""
    m: Dict[str, int] = {}
    for i, h in enumerate(headers):
        nh = _norm_header(h)
        if nh and nh not in m:
            m[nh] = i
    return m


def get_all(ws: gspread.Worksheet) -> List[List[str]]:
    return ws.get_all_values() or []


def getv(row: List[str], i: Optional[int]) -> str:
    if i is None or i >= len(row):
        return ""
    return (row[i] or "").strip()


def truthy(v: str) -> bool:
    return (v or "").strip().lower() in ("true", "1", "yes", "y", "approved")


# ----------------------------- service account parsing (robust) -----------------------------


def _strip_control_chars(s: str) -> str:
    # remove JSON-breaking control chars; keep \t\n\r
    return re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", s)


def _maybe_b64_decode(s: str) -> Optional[str]:
    """
    If secret is base64, decode it.
    If it isn't, return None.
    """
    t = (s or "").strip()
    if not t:
        return None
    # fast reject: if it contains obvious JSON markers, it's not base64
    if t.startswith("{") and '"private_key"' in t:
        return None
    if not re.fullmatch(r"[A-Za-z0-9+/=\s]+", t):
        return None
    try:
        raw = base64.b64decode(t.encode("utf-8"), validate=False).decode("utf-8", "ignore").strip()
    except Exception:
        return None
    if raw.startswith("{") and '"private_key"' in raw:
        return raw
    return None


def _repair_private_key_field(raw: str) -> str:
    """
    Make private_key safe for json.loads:
    - If the JSON has literal line breaks inside the private_key string,
      convert them to \\n escapes.
    """
    pat = re.compile(r'("private_key"\s*:\s*")(.+?)(")', re.DOTALL)
    m = pat.search(raw)
    if not m:
        return raw
    prefix, pk, suffix = m.group(1), m.group(2), m.group(3)

    pk_fixed = pk.replace("\r\n", "\n").replace("\r", "\n")
    # escape backslashes then escape newlines
    pk_fixed = pk_fixed.replace("\\", "\\\\").replace("\n", "\\n")
    return raw[: m.start()] + prefix + pk_fixed + suffix + raw[m.end() :]


def load_sa_info() -> Dict[str, Any]:
    raw = env("GCP_SA_JSON_ONE_LINE") or env("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON / GCP_SA_JSON_ONE_LINE")

    raw = _strip_control_chars(raw.strip())

    b64 = _maybe_b64_decode(raw)
    if b64:
        raw = b64

    raw = _repair_private_key_field(raw)

    # Attempt 1: strict JSON
    try:
        info = json.loads(raw)
        return info
    except json.JSONDecodeError:
        pass

    # Attempt 2: common pattern where key contains \\n and code needs real \n
    try:
        info = json.loads(raw.replace("\\n", "\n"))
        return info
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Service account JSON could not be parsed: {e}") from e


def gspread_client() -> gspread.Client:
    info = load_sa_info()
    pk = (info.get("private_key") or "").strip()

    # Hard guard: this is the exact failure you were seeing.
    if "BEGIN PRIVATE KEY" not in pk:
        raise RuntimeError(
            "Service account JSON loaded but private_key looks malformed. "
            "Ensure GitHub secret contains the full JSON with a valid private_key."
        )

    creds = Credentials.from_service_account_info(info, scopes=GOOGLE_SCOPE)
    return gspread.authorize(creds)


# ----------------------------- phrase selection -----------------------------


def stable_pick(items: List[Dict[str, Any]], seed: str) -> Optional[Dict[str, Any]]:
    if not items:
        return None
    h = hashlib.sha1(seed.encode("utf-8")).hexdigest()
    n = int(h[:8], 16)
    return items[n % len(items)]


# ----------------------------- main -----------------------------


def main() -> int:
    RAW_TAB = env("RAW_DEALS_TAB", "RAW_DEALS")
    IATA_MASTER_TAB = env("IATA_MASTER_TAB", "IATA_MASTER")
    PHRASE_BANK_TAB = env("PHRASE_BANK_TAB") or env("PHRASES_TAB") or "PHRASE_BANK"

    MAX_ROWS = env_int("ENRICH_MAX_ROWS_PER_RUN", 60)
    PHRASE_CHANNEL = env("PHRASE_CHANNEL", "vip").lower()
    ENFORCE_MAX_PER_MONTH = env_bool("ENFORCE_MAX_PER_MONTH", True)
    REQUIRE_PHRASE = env_bool("REQUIRE_PHRASE", False)

    # Contract: enrichment can run at multiple points; never changes status.
    ELIGIBLE_STATUSES = {
        "NEW",
        "SCORED",
        "READY_TO_POST",
        "READY_TO_PUBLISH",
        "READY_FREE",
        "VIP_DONE",
        "PUBLISH_AM",
        "PUBLISH_PM",
        "PUBLISH_BOTH",
    }

    log("============================================================")
    log("🧩 TravelTxter Enrichment Router — V5 (IATA backfill + Phrase Bank selection)")
    log("============================================================")
    log(f"RAW_TAB='{RAW_TAB}' | PHRASE_BANK_TAB='{PHRASE_BANK_TAB}' | IATA_MASTER_TAB='{IATA_MASTER_TAB}'")
    log(f"PHRASE_CHANNEL='{PHRASE_CHANNEL}' | MAX_ROWS_PER_RUN={MAX_ROWS}")
    log(f"ENFORCE_MAX_PER_MONTH={ENFORCE_MAX_PER_MONTH} | REQUIRE_PHRASE={REQUIRE_PHRASE}")
    log(f"ELIGIBLE_STATUSES={sorted(ELIGIBLE_STATUSES)}")

    sid = env("SPREADSHEET_ID") or env("SHEET_ID")
    if not sid:
        raise RuntimeError("Missing SPREADSHEET_ID (or SHEET_ID)")

    gc = gspread_client()
    sh = gc.open_by_key(sid)

    ws_raw = sh.worksheet(RAW_TAB)
    data = get_all(ws_raw)
    if len(data) < 2:
        log("RAW_DEALS empty. Nothing to enrich.")
        return 0

    headers = data[0]
    h = header_map_first(headers)

    # Required reads
    required_reads = ["deal_id", "status", "origin_iata", "destination_iata", "theme"]
    for req in required_reads:
        if _norm_header(req) not in h:
            raise RuntimeError(f"RAW_DEALS missing required header for enrich: {req}")

    # Optional writes (only if columns exist)
    c_origin_city = h.get("origin_city")
    c_dest_city = h.get("destination_city")
    c_dest_country = h.get("destination_country")
    c_phrase_used = h.get("phrase_used")
    c_phrase_cat = h.get("phrase_category")

    c_status = h["status"]
    c_deal_id = h["deal_id"]
    c_origin_iata = h["origin_iata"]
    c_dest_iata = h["destination_iata"]
    c_theme = h["theme"]

    # ---- Load IATA_MASTER into map ----
    iata: Dict[str, Tuple[str, str]] = {}
    try:
        ws_iata = sh.worksheet(IATA_MASTER_TAB)
        vals = get_all(ws_iata)
        if len(vals) >= 2:
            hi = header_map_first(vals[0])
            i_code = hi.get("iata_code")
            i_city = hi.get("city")
            i_country = hi.get("country")
            if i_code is None or i_city is None or i_country is None:
                raise RuntimeError("IATA_MASTER must have headers: iata_code, city, country")

            for r in vals[1:]:
                code = getv(r, i_code).upper()
                if not code:
                    continue
                city = getv(r, i_city)
                country = getv(r, i_country)
                iata[code] = (city, country)

        log(f"✅ IATA_MASTER loaded: {len(iata)} entries")
    except Exception as e:
        log(f"⚠️ IATA_MASTER load failed (non-fatal): {e}")

    # ---- Load PHRASE_BANK into map: (dest, theme) -> list ----
    phrases: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    try:
        ws_pb = sh.worksheet(PHRASE_BANK_TAB)
        vals = get_all(ws_pb)
        if len(vals) >= 2:
            hp = header_map_first(vals[0])
            i_dest = hp.get("destination_iata")
            i_th = hp.get("theme")
            i_cat = hp.get("category")
            i_phrase = hp.get("phrase")
            i_appr = hp.get("approved")
            i_ch = hp.get("channel_hint")
            i_mpm = hp.get("max_per_month")

            needed = [i_dest, i_th, i_cat, i_phrase, i_appr]
            if any(x is None for x in needed):
                raise RuntimeError(
                    "PHRASE_BANK must have headers: destination_iata, theme, category, phrase, approved"
                )

            for r in vals[1:]:
                if not truthy(getv(r, i_appr)):
                    continue
                dest = getv(r, i_dest).upper()
                th = getv(r, i_th).lower()
                phr = getv(r, i_phrase)
                cat = getv(r, i_cat)
                ch = getv(r, i_ch).lower() if i_ch is not None else ""
                mpm = getv(r, i_mpm) if i_mpm is not None else ""

                if not dest or not th or not phr:
                    continue

                phrases.setdefault((dest, th), []).append(
                    {"phrase": phr, "category": cat, "channel_hint": ch, "max_per_month": mpm}
                )

        log(f"✅ PHRASE_BANK loaded: {len(phrases)} keys")
    except Exception as e:
        log(f"⚠️ PHRASE_BANK load failed (non-fatal): {e}")

    # ---- Build month-aware usage counts (only if phrase_used exists) ----
    usage: Dict[str, int] = {}
    now = dt.datetime.now(dt.timezone.utc)
    month_prefix = now.strftime("%Y-%m")  # matches ISO timestamps

    posted_cols = [
        h.get("posted_vip_at"),
        h.get("posted_free_at"),
        h.get("posted_instagram_at"),
    ]

    if c_phrase_used is not None:
        for r in data[1:]:
            phrase_text = getv(r, c_phrase_used)
            if not phrase_text:
                continue

            ts = ""
            for ci in posted_cols:
                if ci is None:
                    continue
                ts = getv(r, ci)
                if ts:
                    break

            if ts.startswith(month_prefix):
                usage[phrase_text] = usage.get(phrase_text, 0) + 1

    def channel_ok(p: Dict[str, Any]) -> bool:
        ch = (p.get("channel_hint") or "").strip().lower()
        # Only enforce when it is explicitly a channel gate
        if ch in ("vip", "free", "ig", "all"):
            return ch == "all" or ch == PHRASE_CHANNEL
        return True

    def governance_ok(p: Dict[str, Any]) -> bool:
        if not ENFORCE_MAX_PER_MONTH:
            return True
        mpm_s = (p.get("max_per_month") or "").strip()
        if not mpm_s:
            return True
        try:
            mpm = int(float(mpm_s))
        except Exception:
            return True
        if mpm <= 0:
            return True
        phrase_text = (p.get("phrase") or "").strip()
        return usage.get(phrase_text, 0) < mpm

    # ---- Queue updates (batch) ----
    updates: List[gspread.cell.Cell] = []
    scanned = 0
    eligible = 0
    changed_rows = 0

    def queue(row_1: int, col_1: int, value: Any) -> None:
        updates.append(gspread.cell.Cell(row=row_1, col=col_1, value=value))

    for row_num, r in enumerate(data[1:], start=2):
        scanned += 1

        status = getv(r, c_status).upper()
        if status not in ELIGIBLE_STATUSES:
            continue

        deal_id = getv(r, c_deal_id)
        if not deal_id:
            continue

        eligible += 1
        if changed_rows >= MAX_ROWS:
            break

        origin = getv(r, c_origin_iata).upper()
        dest = getv(r, c_dest_iata).upper()
        theme = getv(r, c_theme).lower()

        row_changed = False

        # IATA backfill (idempotent)
        if iata:
            if c_origin_city is not None and not getv(r, c_origin_city) and origin in iata:
                ocity, _ = iata[origin]
                if ocity:
                    queue(row_num, c_origin_city + 1, ocity)
                    row_changed = True

            if dest in iata:
                dcity, dcountry = iata[dest]
                if c_dest_city is not None and not getv(r, c_dest_city) and dcity:
                    queue(row_num, c_dest_city + 1, dcity)
                    row_changed = True
                if c_dest_country is not None and not getv(r, c_dest_country) and dcountry:
                    queue(row_num, c_dest_country + 1, dcountry)
                    row_changed = True

        # Phrase fill (idempotent)
        if c_phrase_used is not None and not getv(r, c_phrase_used):
            cands = phrases.get((dest, theme), [])
            cands = [p for p in cands if channel_ok(p)]
            cands = [p for p in cands if governance_ok(p)]

            chosen = stable_pick(cands, seed=f"{deal_id}:{dest}:{theme}")
            if chosen:
                phr = (chosen.get("phrase") or "").strip()
                cat = (chosen.get("category") or "").strip()
                if phr:
                    queue(row_num, c_phrase_used + 1, phr)
                    usage[phr] = usage.get(phr, 0) + 1
                    row_changed = True
                    if c_phrase_cat is not None and not getv(r, c_phrase_cat) and cat:
                        queue(row_num, c_phrase_cat + 1, cat)
                        row_changed = True
            else:
                # If phrase is mandatory, we still don't write status here.
                # Upstream can decide what to do with missing phrase.
                pass

        if row_changed:
            changed_rows += 1

    log("------------------------------------------------------------")
    log(f"Scanned: {scanned} | Eligible: {eligible} | Rows changed: {changed_rows} | Cells queued: {len(updates)}")

    if not updates:
        log("✅ No changes needed (idempotent).")
        return 0

    ws_raw.update_cells(updates, value_input_option="USER_ENTERED")
    log("✅ Enrichment batch write complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
