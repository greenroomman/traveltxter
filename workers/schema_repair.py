#!/usr/bin/env python3
"""
TravelTxter V4.5x — schema_repair.py (one-off legacy data repair)

Repairs legacy rows where city fields contain IATA codes (e.g., "LGW", "TFS").

What it repairs:
- origin_city: if it looks like IATA or equals origin_iata -> replace with best known city for that IATA
- destination_city: if it looks like IATA or equals destination_iata -> replace with best known city for that IATA
- destination_country: if blank -> replace with best known country for destination_iata (when available)

Repair sources (in this order):
1) Internal map built from RAW_DEALS good rows (most reliable: your own data)
2) CONFIG_SIGNALS lookup (if tab exists)

Hard rules:
- Header-mapped only (no column numbers)
- Does not touch statuses
- Does not overwrite a non-IATA city name
- Safe caps per run

Env required:
- SPREADSHEET_ID
- GCP_SA_JSON_ONE_LINE (or GCP_SA_JSON)
- RAW_DEALS_TAB (default RAW_DEALS)

Env optional:
- MAX_REPAIRS_PER_RUN (default 200)
"""

from __future__ import annotations

import os
import json
import re
import datetime as dt
from typing import Dict, Any, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# Logging
# ============================================================

def utcnow() -> dt.datetime:
    return dt.datetime.utcnow()

def ts() -> str:
    return utcnow().replace(microsecond=0).isoformat() + "Z"

def log(msg: str) -> None:
    print(f"{ts()} | {msg}", flush=True)


# ============================================================
# Env
# ============================================================

def env_str(k: str, default: str = "") -> str:
    return os.environ.get(k, default).strip()

def env_int(k: str, default: int) -> int:
    try:
        return int(env_str(k, str(default)))
    except Exception:
        return default


# ============================================================
# Sheets
# ============================================================

def get_client() -> gspread.Client:
    sa = env_str("GCP_SA_JSON_ONE_LINE") or env_str("GCP_SA_JSON")
    if not sa:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE (recommended) or GCP_SA_JSON")
    try:
        info = json.loads(sa)
    except json.JSONDecodeError:
        info = json.loads(sa.replace("\\n", "\n"))

    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds)

def col_letter(n1: int) -> str:
    s = ""
    n = n1
    while n:
        n, rr = divmod(n - 1, 26)
        s = chr(65 + rr) + s
    return s

def a1(rownum: int, col0: int) -> str:
    return f"{col_letter(col0 + 1)}{rownum}"


# ============================================================
# Helpers
# ============================================================

IATA_RE = re.compile(r"^[A-Z]{3}$")

def looks_like_iata(s: str) -> bool:
    s = (s or "").strip().upper()
    return bool(IATA_RE.match(s))

def is_good_city(s: str) -> bool:
    """
    Heuristic: if it's not IATA-like and has letters, treat as a city name.
    """
    s = (s or "").strip()
    if not s:
        return False
    if looks_like_iata(s):
        return False
    # "London", "Bristol", "Santa Cruz de Tenerife" etc.
    return any(ch.isalpha() for ch in s)

def safe_upper(s: str) -> str:
    return (s or "").strip().upper()

def load_config_signals(spread: gspread.Spreadsheet) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Best-effort read CONFIG_SIGNALS:
    returns (iata_to_city, iata_to_country)
    Accepts a few header variants.
    """
    try:
        ws = spread.worksheet("CONFIG_SIGNALS")
    except Exception:
        return {}, {}

    values = ws.get_all_values()
    if not values or len(values) < 2:
        return {}, {}

    headers = [h.strip() for h in values[0]]
    h = {name: i for i, name in enumerate(headers)}

    def pick(*names: str) -> Optional[int]:
        for n in names:
            if n in h:
                return h[n]
        return None

    i_iata = pick("destination_iata", "iata", "airport_iata", "dest_iata")
    i_city = pick("destination_city", "city", "dest_city")
    i_country = pick("destination_country", "country", "dest_country")

    if i_iata is None:
        return {}, {}

    iata_to_city: Dict[str, str] = {}
    iata_to_country: Dict[str, str] = {}

    for r in values[1:]:
        code = (r[i_iata] if i_iata < len(r) else "").strip().upper()
        if not looks_like_iata(code):
            continue
        city = (r[i_city] if (i_city is not None and i_city < len(r)) else "").strip()
        country = (r[i_country] if (i_country is not None and i_country < len(r)) else "").strip()
        if city and is_good_city(city):
            iata_to_city[code] = city
        if country:
            iata_to_country[code] = country

    return iata_to_city, iata_to_country


def build_internal_maps(rows: List[List[str]], h: Dict[str, int]) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Builds IATA->City and IATA->Country maps from *existing good rows* in RAW_DEALS.
    """
    iata_to_city: Dict[str, str] = {}
    iata_to_country: Dict[str, str] = {}

    def get(r: List[str], col: str) -> str:
        idx = h.get(col)
        return (r[idx] if (idx is not None and idx < len(r)) else "").strip()

    for r in rows:
        oi = safe_upper(get(r, "origin_iata"))
        oc = get(r, "origin_city")
        di = safe_upper(get(r, "destination_iata"))
        dc = get(r, "destination_city")
        country = get(r, "destination_country")

        if looks_like_iata(oi) and is_good_city(oc):
            iata_to_city.setdefault(oi, oc)

        if looks_like_iata(di) and is_good_city(dc):
            iata_to_city.setdefault(di, dc)

        # Country map only for destination (that’s what you publish)
        if looks_like_iata(di) and country:
            iata_to_country.setdefault(di, country)

    return iata_to_city, iata_to_country


# ============================================================
# Main
# ============================================================

def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    max_repairs = env_int("MAX_REPAIRS_PER_RUN", 200)

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")

    gc = get_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(tab)

    values = ws.get_all_values()
    if not values or len(values) < 2:
        log("Sheet empty. Nothing to repair.")
        return 0

    headers = [h.strip() for h in values[0]]
    h = {name: i for i, name in enumerate(headers)}
    rows = values[1:]

    required = ["origin_iata", "destination_iata", "origin_city", "destination_city", "destination_country"]
    for c in required:
        if c not in h:
            raise RuntimeError(f"Missing required column in RAW_DEALS: {c}")

    # Build repair maps
    internal_city, internal_country = build_internal_maps(rows, h)
    signals_city, signals_country = load_config_signals(sh)

    def lookup_city(iata: str) -> str:
        code = safe_upper(iata)
        return (
            internal_city.get(code)
            or signals_city.get(code)
            or ""
        )

    def lookup_country(iata: str) -> str:
        code = safe_upper(iata)
        return (
            internal_country.get(code)
            or signals_country.get(code)
            or ""
        )

    updates: List[Dict[str, Any]] = []
    repaired = 0
    scanned = 0

    for rownum, r in enumerate(rows, start=2):
        if repaired >= max_repairs:
            break
        scanned += 1

        oi = safe_upper(r[h["origin_iata"]] if h["origin_iata"] < len(r) else "")
        di = safe_upper(r[h["destination_iata"]] if h["destination_iata"] < len(r) else "")

        oc = (r[h["origin_city"]] if h["origin_city"] < len(r) else "").strip()
        dc = (r[h["destination_city"]] if h["destination_city"] < len(r) else "").strip()
        country = (r[h["destination_country"]] if h["destination_country"] < len(r) else "").strip()

        # Decide if fields are bad (IATA or equals IATA)
        origin_bad = (not is_good_city(oc)) and (looks_like_iata(oc) or (oi and safe_upper(oc) == oi))
        dest_bad = (not is_good_city(dc)) and (looks_like_iata(dc) or (di and safe_upper(dc) == di))
        country_bad = (not country)

        did_any = False

        if origin_bad and looks_like_iata(oi):
            fix = lookup_city(oi)
            if fix and is_good_city(fix):
                updates.append({"range": a1(rownum, h["origin_city"]), "values": [[fix]]})
                did_any = True

        if dest_bad and looks_like_iata(di):
            fix = lookup_city(di)
            if fix and is_good_city(fix):
                updates.append({"range": a1(rownum, h["destination_city"]), "values": [[fix]]})
                did_any = True

        if country_bad and looks_like_iata(di):
            fix = lookup_country(di)
            if fix:
                updates.append({"range": a1(rownum, h["destination_country"]), "values": [[fix]]})
                did_any = True

        if did_any:
            repaired += 1

        # Batch flush to avoid giant payloads
        if len(updates) >= 400:
            ws.batch_update(updates)
            log(f"🧹 Applied batch updates: {len(updates)} cells")
            updates = []

    if updates:
        ws.batch_update(updates)
        log(f"🧹 Applied final batch updates: {len(updates)} cells")

    log(f"Done. scanned_rows={scanned} repaired_rows={repaired}")
    log("Note: Repairs only happen when we can confidently map IATA -> City/Country from your own data or CONFIG_SIGNALS.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
