#!/usr/bin/env python3
"""
TravelTxter V4.5x — instagram_publisher.py

Hardening:
- If city/country are missing or city is IATA, resolve from CONFIG_SIGNALS.
- If still missing destination_country after resolution, dead-letter the row (ERROR_HARD)
  so it cannot loop.

Caption format unchanged.
"""

from __future__ import annotations

import os
import json
import time
import datetime as dt
import hashlib
import re
from typing import Dict, Any, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError


# ============================================================
# Logging
# ============================================================

def ts() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def log(msg: str) -> None:
    print(f"{ts()} | {msg}", flush=True)


# ============================================================
# Env helpers
# ============================================================

def env_str(k: str, default: str = "") -> str:
    return (os.environ.get(k, default) or "").strip()

def env_int(k: str, default: int) -> int:
    try:
        return int(env_str(k, str(default)))
    except Exception:
        return default


# ============================================================
# Robust SA JSON parsing
# ============================================================

def _extract_json_object(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()

    for attempt in (raw, raw.replace("\\n", "\n")):
        try:
            return json.loads(attempt)
        except Exception:
            pass

    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise RuntimeError("Invalid GCP_SA_JSON_ONE_LINE: no JSON object found")

    candidate = raw[start:end + 1]
    for attempt in (candidate, candidate.replace("\\n", "\n")):
        try:
            return json.loads(attempt)
        except Exception:
            pass

    raise RuntimeError("Invalid GCP_SA_JSON_ONE_LINE: JSON parse failed")


def get_client() -> gspread.Client:
    sa = env_str("GCP_SA_JSON_ONE_LINE") or env_str("GCP_SA_JSON")
    if not sa:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")

    info = _extract_json_object(sa)
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds)


def open_sheet_with_backoff(gc: gspread.Client, spreadsheet_id: str, attempts: int = 8) -> gspread.Spreadsheet:
    delay = 4.0
    for i in range(1, attempts + 1):
        try:
            return gc.open_by_key(spreadsheet_id)
        except APIError as e:
            msg = str(e)
            if "429" in msg or "Quota exceeded" in msg:
                log(f"⏳ Sheets quota (429). Retry {i}/{attempts} in {int(delay)}s...")
                time.sleep(delay)
                delay = min(delay * 1.6, 45.0)
                continue
            raise
    raise RuntimeError("Sheets quota still exceeded after retries (429). Try again shortly.")


# ============================================================
# A1 helpers
# ============================================================

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
# Sheet helpers
# ============================================================

def ensure_columns(ws: gspread.Worksheet, headers: List[str], required: List[str]) -> List[str]:
    missing = [c for c in required if c not in headers]
    if not missing:
        return headers
    ws.update([headers + missing], "A1")
    log(f"🛠️  Added missing columns: {missing}")
    return headers + missing

def safe_get(row: List[str], idx: int) -> str:
    return row[idx].strip() if 0 <= idx < len(row) else ""


# ============================================================
# IATA detection + city/country lookup from CONFIG_SIGNALS
# ============================================================

IATA_RE = re.compile(r"^[A-Z]{3}$")

def is_iata3(s: str) -> bool:
    return bool(IATA_RE.match((s or "").strip().upper()))

UK_AIRPORT_CITY_FALLBACK = {
    "LHR": "London", "LGW": "London", "STN": "London", "LTN": "London", "LCY": "London", "SEN": "London",
    "MAN": "Manchester", "BRS": "Bristol", "BHX": "Birmingham", "EDI": "Edinburgh", "GLA": "Glasgow",
    "NCL": "Newcastle", "LPL": "Liverpool", "NQY": "Newquay", "SOU": "Southampton", "CWL": "Cardiff", "EXT": "Exeter",
}

def load_config_signals_maps(sh: gspread.Spreadsheet) -> Tuple[Dict[str, str], Dict[str, str]]:
    try:
        ws = sh.worksheet("CONFIG_SIGNALS")
    except Exception:
        return {}, {}

    values = ws.get_all_values()
    if not values or len(values) < 2:
        return {}, {}

    headers = [h.strip() for h in values[0]]
    idx = {h: i for i, h in enumerate(headers)}

    def pick(*names: str) -> Optional[int]:
        for n in names:
            if n in idx:
                return idx[n]
        return None

    i_iata = pick("iata_hint", "destination_iata", "iata", "airport_iata")
    i_city = pick("destination_city", "city", "dest_city", "airport_city")
    i_country = pick("destination_country", "country", "dest_country")

    if i_iata is None:
        return {}, {}

    iata_to_city: Dict[str, str] = {}
    iata_to_country: Dict[str, str] = {}

    for r in values[1:]:
        code = (r[i_iata] if i_iata < len(r) else "").strip().upper()
        if not is_iata3(code):
            continue
        city = (r[i_city] if (i_city is not None and i_city < len(r)) else "").strip()
        country = (r[i_country] if (i_country is not None and i_country < len(r)) else "").strip()
        if city:
            iata_to_city[code] = city
        if country:
            iata_to_country[code] = country

    return iata_to_city, iata_to_country

def resolve_city(maybe_city: str, maybe_iata: str, iata_to_city: Dict[str, str]) -> str:
    c = (maybe_city or "").strip()
    if c and not is_iata3(c):
        return c
    code = (maybe_iata or c or "").strip().upper()
    if is_iata3(code):
        return iata_to_city.get(code) or UK_AIRPORT_CITY_FALLBACK.get(code) or code
    return c

def resolve_country(maybe_country: str, dest_iata: str, iata_to_country: Dict[str, str]) -> str:
    c = (maybe_country or "").strip()
    if c:
        return c
    code = (dest_iata or "").strip().upper()
    if is_iata3(code):
        return iata_to_country.get(code, "")
    return ""


# ============================================================
# Flags + formatting
# ============================================================

FLAG_MAP = {
    "ICELAND": "🇮🇸"
