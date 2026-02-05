#!/usr/bin/env python3
# workers/enrich_router.py
#
# TRAVELTXTTER — ENRICH ROUTER (V5, MINIMAL CONTRACT)
#
# PURPOSE
# - Fill missing city/country using IATA_MASTER (iata_code -> city/country)
# - Select an approved phrase from PHRASE_BANK and write:
#     phrase_used (the phrase text)
#     phrase_category (category)
#
# READS:
# - RAW_DEALS (status-filtered)
# - IATA_MASTER (iata_code/city/country)
# - PHRASE_BANK (destination_iata/theme -> approved phrases)
#
# WRITES (RAW_DEALS only, if columns exist):
# - origin_city
# - origin_country (ONLY if column exists in RAW_DEALS)
# - destination_city
# - destination_country
# - phrase_used
# - phrase_category
#
# RULES
# - No status changes. Ever.
# - Idempotent: never overwrites existing filled values.
# - Duplicate header safe: first occurrence wins.
# - Robust SA JSON handling: fixes "Invalid control character" failures.

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
from typing import Any, Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials


GOOGLE_SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]


def log(msg: str) -> None:
    ts = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    print(f"{ts} | {msg}", flush=True)


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


def _norm_header(h: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (h or "").strip().lower()).strip("_")


def _first_norm_index(headers: List[str]) -> Dict[str, int]:
    idx: Dict[str, int] = {}
    for i, h in enumerate(headers):
        nh = _norm_header(h)
        if nh and nh not in idx:
            idx[nh] = i
    return idx


def _strip_control_chars(s: str) -> str:
    # Remove JSON-breaking control chars (except \t \n \r which are handled separately)
    return re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", s)


def _repair_private_key_field(raw: str) -> str:
    """
    If a secret accidentally contains literal newlines inside the JSON string for "private_key",
    json.loads() can fail. Convert literal newlines in that value to escaped \\n.
    """
    pat = re.compile(r'("private_key"\s*:\s*")(.+?)(")', re.DOTALL)
    m = pat.search(raw)
    if not m:
        return raw
    prefix, pk, suffix = m.group(1), m.group(2), m.group(3)
    pk_fixed = pk.replace("\r\n", "\n").replace("\r", "\n")
    # Ensure it's a valid JSON string: escape backslashes and newlines
    pk_fixed = pk_fixed.replace("\\", "\\\\").replace("\n", "\\n")
    return raw[: m.start()] + prefix + pk_fixed + suffix + raw[m.end() :]


def _load_service_account_info() -> Dict[str, Any]:
    """
    Robustly load SA JSON from either:
      - GCP_SA_JSON_ONE_LINE (preferred)
      - GCP_SA_JSON
    Handles:
      - stray control characters
      - private_key newline issues
      - common GitHub Secrets escaping
    """
    raw = env("GCP_SA_JSON_ONE_LINE") or env("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON / GCP_SA_JSON_ONE_LINE")

    raw = raw.strip()
    raw = _strip_control_chars(raw)
    raw = _repair_private_key_field(raw)

    # Attempt 1: direct json
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    # Attempt 2: common escaping pattern (\\n)
    try:
        return json.loads(raw.replace("\\n", "\n"))
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Service account JSON could not be parsed: {e}") from e


def gspread_client() -> gspread.Client:
    info = _load_service_account_info()
    creds = Credentials.from_service_account_info(info, scopes=GOOGLE_SCOPE)
    return gspread.authorize(creds)


def get_all(ws: gspread.Worksheet) -> List[List[str]]:
    return ws.get_all_values() or []


def col_i(headers: List[str], name: str) -> Optional[int]:
    want = _norm_header(name)
    nidx = _first_norm_index(headers)
    return nidx.get(want, None)


def truthy(v: str) -> bool:
    return (v or "").strip().lower() in ("true", "1", "yes", "y", "approved")


def stable_pick(items: List[Dict[str, Any]], seed: str) -> Optional[Dict[str, Any]]:
    if not items:
        return None
    h = hashlib.sha1(seed.encode("utf-8")).hexdigest()
    n = int(h[:8], 16)
    return items[n % len(items)]


def main() -> int:
    RAW_TAB = env("RAW_DEALS_TAB", "RAW_DEALS")
    PHRASE_BANK_TAB = env("PHRASE_BANK_TAB") or env("PHRASES_TAB") or "PHRASE_BANK"
    IATA_MASTER_TAB = env("IATA_MASTER_TAB", "IATA_MASTER")

    MAX_ROWS = env_int("ENRICH_MAX_ROWS_PER_RUN", 60)
    PHRASE_CHANNEL = env("PHRASE_CHANNEL", "vip").lower()
    ENFORCE_MAX_PER_MONTH = env_bool("ENFORCE_MAX_PER_MONTH", True)
    REQUIRE_PHRASE = env_bool("REQUIRE_PHRASE", False)

    # Eligible statuses (V5). Enrich is allowed to run early; it still won't touch status.
    ELIGIBLE = {
        "NEW",
        "SCORED",
        "PUBLISH_AM",
        "PUBLISH_PM",
        "PUBLISH_BOTH",
        "READY_TO_POST",
        "READY_TO_PUBLISH",
        "READY_FREE",
        "VIP_DONE",
    }

    log("============================================================")
    log("🧩 TravelTxter Enrichment Router — V5 (IATA backfill + Phrase Bank selection)")
    log("============================================================")
    log(f"RAW_TAB='{RAW_TAB}' | PHRASE_BANK_TAB='{PHRASE_BANK_TAB}' | IATA_MASTER_TAB='{IATA_MASTER_TAB}'")
    log(f"PHRASE_CHANNEL='{PHRASE_CHANNEL}' | MAX_ROWS_PER_RUN={MAX_ROWS}")
    log(f"ENFORCE_MAX_PER_MONTH={ENFORCE_MAX_PER_MONTH} | REQUIRE_PHRASE={REQUIRE_PHRASE}")
    log(f"ELIGIBLE_STATUSES={sorted(list(ELIGIBLE))}")

    gc = gspread_client()
    sid = env("SPREADSHEET_ID") or env("SHEET_ID")
    if not sid:
        raise RuntimeError("Missing SPREADSHEET_ID (or SHEET_ID)")
    sh = gc.open_by_key(sid)

    ws_raw = sh.worksheet(RAW_TAB)
    raw = get_all(ws_raw)
    if not raw or len(raw) < 2:
        log("RAW_DEALS empty. Nothing to enrich.")
        return 0

    headers = raw[0]
    idx = _first_norm_index(headers)

    # Required minimal fields to function
    need = ["status", "deal_id", "origin_iata", "destination_iata", "theme"]
    missing = [n for n in need if _norm_header(n) not in idx]
    if missing:
        raise RuntimeError(f"RAW_DEALS missing required headers for enrich: {missing}")

    # Targets (write only if present)
    c_origin_city = idx.get("origin_city")
    c_origin_country = idx.get("origin_country")  # optional (may not exist in RD)
    c_dest_city = idx.get("destination_city")
    c_dest_country = idx.get("destination_country")
    c_phrase_used = idx.get("phrase_used")
    c_phrase_cat = idx.get("phrase_category")

    # Load IATA_MASTER (required for city/country fills; non-fatal if missing tab)
    iata_map: Dict[str, Tuple[str, str]] = {}
    try:
        ws_iata = sh.worksheet(IATA_MASTER_TAB)
        vals = get_all(ws_iata)
        if vals and len(vals) >= 2:
            h = vals[0]
            i_code = col_i(h, "iata_code")
            i_city = col_i(h, "city")
            i_country = col_i(h, "country")
            if i_code is None or i_city is None or i_country is None:
                raise RuntimeError("IATA_MASTER missing required headers: iata_code, city, country")

            for r in vals[1:]:
                code = (r[i_code] if i_code < len(r) else "").strip().upper()
                if not code:
                    continue
                city = (r[i_city] if i_city < len(r) else "").strip()
                country = (r[i_country] if i_country < len(r) else "").strip()
                iata_map[code] = (city, country)

        log(f"✅ IATA_MASTER loaded: {len(iata_map)} entries")
    except Exception as e:
        log(f"⚠️ Could not load IATA_MASTER (non-fatal): {e}")

    # Load PHRASE_BANK (approved, keyed by (dest, theme))
    phrases_by_key: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    try:
        ws_pb = sh.worksheet(PHRASE_BANK_TAB)
        vals = get_all(ws_pb)
        if vals and len(vals) >= 2:
            h = vals[0]
            i_dest = col_i(h, "destination_iata")
            i_theme = col_i(h, "theme")
            i_cat = col_i(h, "category")
            i_phrase = col_i(h, "phrase")
            i_appr = col_i(h, "approved")
            i_ch = col_i(h, "channel_hint")
            i_mpm = col_i(h, "max_per_month")

            req = [i_dest, i_theme, i_cat, i_phrase, i_appr]
            if any(x is None for x in req):
                raise RuntimeError("PHRASE_BANK missing required headers: destination_iata, theme, category, phrase, approved")

            for r in vals[1:]:
                if not truthy(r[i_appr] if i_appr < len(r) else ""):
                    continue
                dest = (r[i_dest] if i_dest < len(r) else "").strip().upper()
                theme = (r[i_theme] if i_theme < len(r) else "").strip().lower()
                phrase = (r[i_phrase] if i_phrase < len(r) else "").strip()
                cat = (r[i_cat] if i_cat < len(r) else "").strip()
                ch = (r[i_ch] if i_ch is not None and i_ch < len(r) else "").strip().lower()
                mpm = (r[i_mpm] if i_mpm is not None and i_mpm < len(r) else "").strip()

                if not dest or not theme or not phrase:
                    continue

                phrases_by_key.setdefault((dest, theme), []).append(
                    {"phrase": phrase, "category": cat, "channel_hint": ch, "max_per_month": mpm}
                )

        log(f"✅ PHRASE_BANK loaded (approved keys): {len(phrases_by_key)}")
    except Exception as e:
        log(f"⚠️ Could not load PHRASE_BANK (non-fatal): {e}")

    # Phrase usage index (month-aware) based on phrase_used + posted timestamps, to enforce max_per_month.
    usage: Dict[str, int] = {}
    now = dt.datetime.now(dt.timezone.utc)
    month_key = now.strftime("%Y-%m")

    if c_phrase_used is not None:
        for r in raw[1:]:
            phrase = (r[c_phrase_used] if c_phrase_used < len(r) else "").strip()
            if not phrase:
                continue
            ts = ""
            for col in ("posted_vip_at", "posted_free_at", "posted_instagram_at"):
                ci = idx.get(col)
                if ci is not None and ci < len(r):
                    ts = (r[ci] or "").strip()
                    if ts:
                        break
            if ts and ts.startswith(month_key):
                usage[phrase] = usage.get(phrase, 0) + 1

    def governance_ok(p: Dict[str, Any]) -> bool:
        if not ENFORCE_MAX_PER_MONTH:
            return True
        mpm_s = (p.get("max_per_month") or "").strip()
        try:
            mpm = int(float(mpm_s)) if mpm_s else 0
        except Exception:
            mpm = 0
        if mpm <= 0:
            return True
        phrase = (p.get("phrase") or "").strip()
        return usage.get(phrase, 0) < mpm

    def channel_ok(p: Dict[str, Any]) -> bool:
        ch = (p.get("channel_hint") or "").strip().lower()
        # If channel_hint is descriptive ("Destination-specific"), treat as not a filter.
        if ch in ("vip", "free", "ig", "all"):
            return ch == "all" or ch == PHRASE_CHANNEL
        return True

    # Helpers
    c_status = idx["status"]
    c_deal_id = idx["deal_id"]
    c_origin_iata = idx["origin_iata"]
    c_dest_iata = idx["destination_iata"]
    c_theme = idx["theme"]

    def getv(r: List[str], i: Optional[int]) -> str:
        if i is None or i >= len(r):
            return ""
        return (r[i] or "").strip()

    updates: List[gspread.cell.Cell] = []

    def queue(row_1: int, col_1: int, value: Any) -> None:
        updates.append(gspread.cell.Cell(row=row_1, col=col_1, value=value))

    scanned = 0
    eligible = 0
    enriched_rows = 0
    cityfills = 0
    phrasefills = 0
    phrase_misses = 0

    for row_num, r in enumerate(raw[1:], start=2):
        scanned += 1
        st = getv(r, c_status).upper()
        if st not in ELIGIBLE:
            continue

        did = getv(r, c_deal_id)
        if not did:
            continue

        eligible += 1
        if enriched_rows >= MAX_ROWS:
            break

        origin = getv(r, c_origin_iata).upper()
        dest = getv(r, c_dest_iata).upper()
        theme = getv(r, c_theme).lower()

        row_changed = False

        # City/country fills (origin + destination)
        if iata_map:
            if c_origin_city is not None and not getv(r, c_origin_city) and origin in iata_map:
                ocity, ocountry = iata_map[origin]
                if ocity:
                    queue(row_num, c_origin_city + 1, ocity)
                    cityfills += 1
                    row_changed = True
                # origin_country only if column exists
                if c_origin_country is not None and not getv(r, c_origin_country) and ocountry:
                    queue(row_num, c_origin_country + 1, ocountry)
                    cityfills += 1
                    row_changed = True

            if c_dest_city is not None and not getv(r, c_dest_city) and dest in iata_map:
                dcity, dcountry = iata_map[dest]
                if dcity:
                    queue(row_num, c_dest_city + 1, dcity)
                    cityfills += 1
                    row_changed = True
                if c_dest_country is not None and not getv(r, c_dest_country) and dcountry:
                    queue(row_num, c_dest_country + 1, dcountry)
                    cityfills += 1
                    row_changed = True

        # Phrase fill (phrase_used + phrase_category)
        if c_phrase_used is not None and not getv(r, c_phrase_used):
            cands = phrases_by_key.get((dest, theme), [])
            cands = [p for p in cands if channel_ok(p)]
            cands = [p for p in cands if governance_ok(p)]

            chosen = stable_pick(cands, seed=f"{did}:{dest}:{theme}")
            if chosen:
                phrase = (chosen.get("phrase") or "").strip()
                cat = (chosen.get("category") or "").strip()
                if phrase:
                    queue(row_num, c_phrase_used + 1, phrase)
                    phrasefills += 1
                    usage[phrase] = usage.get(phrase, 0) + 1
                    row_changed = True
                    if c_phrase_cat is not None and not getv(r, c_phrase_cat) and cat:
                        queue(row_num, c_phrase_cat + 1, cat)
                        row_changed = True
            else:
                # Only count a miss if we actually had a key (dest/theme) to try.
                if dest and theme:
                    phrase_misses += 1
                    if REQUIRE_PHRASE:
                        # Leave blank; downstream can gate if you choose.
                        pass

        if row_changed:
            enriched_rows += 1

    log("------------------------------------------------------------")
    log(f"Scanned rows: {scanned}")
    log(f"Eligible rows: {eligible}")
    log(f"Rows enriched: {enriched_rows} (cap {MAX_ROWS})")
    log(f"City/Country fills: {cityfills}")
    log(f"Phrase fills: {phrasefills} (channel={PHRASE_CHANNEL})")
    log(f"Phrase misses: {phrase_misses}")
    log(f"Cells queued: {len(updates)}")

    if not updates:
        log("✅ No changes needed (idempotent).")
        return 0

    ws_raw.update_cells(updates, value_input_option="USER_ENTERED")
    log("✅ Batch write complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
