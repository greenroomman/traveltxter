from __future__ import annotations

import os
import re
import json
import math
from datetime import datetime, timezone
from typing import Dict, Any, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# CONFIG
# ============================================================

SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS")
RAW_DEALS_VIEW_TAB = os.getenv("RAW_DEALS_VIEW_TAB", "RAW_DEALS_VIEW")

RENDER_URL = os.environ["RENDER_URL"]

GOOGLE_SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SERVICE_ACCOUNT_JSON = os.environ["GCP_SA_JSON_ONE_LINE"]


# ============================================================
# THEME GOVERNANCE (LOCKED)
# ============================================================

AUTHORITATIVE_THEMES = {
    "winter_sun",
    "summer_sun",
    "beach_break",
    "snow",
    "northern_lights",
    "surf",
    "adventure",
    "city_breaks",
    "culture_history",
    "long_haul",
    "luxury_value",
    "unexpected_value",
}

THEME_PRIORITY = [
    "luxury_value",
    "unexpected_value",
    "long_haul",
    "snow",
    "northern_lights",
    "winter_sun",
    "summer_sun",
    "beach_break",
    "surf",
    "culture_history",
    "city_breaks",
    "adventure",
]

THEME_ALIASES = {
    "winter sun": "winter_sun",
    "summer sun": "summer_sun",
    "beach break": "beach_break",
    "northern lights": "northern_lights",
    "city breaks": "city_breaks",
    "culture / history": "culture_history",
    "culture & history": "culture_history",
    "culture": "culture_history",
    "history": "culture_history",
    "long haul": "long_haul",
    "luxury": "luxury_value",
    "unexpected": "unexpected_value",
}


def normalize_theme(raw_theme: str | None) -> str:
    if not raw_theme:
        return "adventure"

    parts = re.split(r"[,\|;]+", raw_theme.lower())
    tokens: list[str] = []

    for p in parts:
        t = p.strip()
        if not t:
            continue
        if t in THEME_ALIASES:
            t = THEME_ALIASES[t]
        t = re.sub(r"[^a-z0-9_]+", "_", t).strip("_")
        if t in THEME_ALIASES:
            t = THEME_ALIASES[t]
        if t in AUTHORITATIVE_THEMES:
            tokens.append(t)

    if not tokens:
        return "adventure"

    for priority in THEME_PRIORITY:
        if priority in tokens:
            return priority

    return tokens[0]


# ============================================================
# GOOGLE SHEETS
# ============================================================

def get_gspread_client():
    creds_dict = json.loads(SERVICE_ACCOUNT_JSON)
    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=GOOGLE_SCOPE,
    )
    return gspread.authorize(creds)


# ============================================================
# NORMALISERS (LOCKED RENDER CONTRACT)
#   OUT/IN: ddmmyy (6 digits)
#   PRICE: £<integer> (rounded up)
# ============================================================

_MONTHS = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}


def _two_digit_year(year: int) -> int:
    return year % 100


def _safe_int(s: str) -> Optional[int]:
    try:
        return int(s)
    except Exception:
        return None


def normalize_price_gbp(raw_price: str | None) -> str:
    """
    Convert to '£123' (integer, rounded up).
    Accepts: '51', '£51', '£51.20', '51.2', 'GBP 51.2', etc.
    """
    if not raw_price:
        return ""

    s = str(raw_price).strip()
    if not s:
        return ""

    # Extract first number (supports decimals)
    m = re.search(r"(\d+(?:\.\d+)?)", s.replace(",", ""))
    if not m:
        return ""

    val = float(m.group(1))
    val_int = int(math.ceil(val))
    return f"£{val_int}"


def normalize_date_ddmmyy(raw_date: str | None) -> str:
    """
    Convert many possible inputs to ddmmyy (6 digits).
    Accepts (examples):
      - '260426' (already ddmmyy)
      - '26/04/26', '26-04-26'
      - '26/04/2026', '26-04-2026'
      - '2026-04-26'
      - 'APR 26 2026', '26 APR 2026'
      - 'APR 26' or '26 APR' (assumes current year, with year-rollover guard)
    """
    if not raw_date:
        return ""

    s = str(raw_date).strip()
    if not s:
        return ""

    # Already ddmmyy
    if re.fullmatch(r"\d{6}", s):
        return s

    # ddmmyyyy -> ddmmyy
    if re.fullmatch(r"\d{8}", s):
        dd = s[0:2]
        mm = s[2:4]
        yy = s[6:8]
        return f"{dd}{mm}{yy}"

    # dd/mm/yy or dd-mm-yy
    m = re.fullmatch(r"(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2})", s)
    if m:
        d = int(m.group(1))
        mo = int(m.group(2))
        y = int(m.group(3))
        return f"{d:02d}{mo:02d}{y:02d}"

    # dd/mm/yyyy or dd-mm-yyyy
    m = re.fullmatch(r"(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})", s)
    if m:
        d = int(m.group(1))
        mo = int(m.group(2))
        y = int(m.group(3))
        return f"{d:02d}{mo:02d}{_two_digit_year(y):02d}"

    # yyyy-mm-dd
    m = re.fullmatch(r"(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})", s)
    if m:
        y = int(m.group(1))
        mo = int(m.group(2))
        d = int(m.group(3))
        return f"{d:02d}{mo:02d}{_two_digit_year(y):02d}"

    # Month name formats: "APR 26 2026" / "26 APR 2026" / "APR 26" / "26 APR"
    tokens = re.split(r"\s+", re.sub(r"[,\.\-]+", " ", s).strip())
    tokens_l = [t.lower() for t in tokens if t.strip()]

    # Helper: infer year if missing (rollover guard)
    now = datetime.now(timezone.utc)
    current_year = now.year

    def infer_year(d: int, mo: int) -> int:
        """
        If no year is given, assume current year, but if date looks far in the past
        relative to 'now' (e.g., we're in Dec and date is Jan), allow next-year rollover.
        """
        try:
            candidate = datetime(current_year, mo, d, tzinfo=timezone.utc)
        except Exception:
            return current_year
        delta_days = (candidate - now).days
        # If candidate is more than ~180 days behind, assume next year.
        if delta_days < -180:
            return current_year + 1
        return current_year

    # Pattern A: MON DD [YYYY]
    if len(tokens_l) in (2, 3) and tokens_l[0] in _MONTHS:
        mo = _MONTHS[tokens_l[0]]
        d = _safe_int(tokens_l[1])
        if d is None:
            raise ValueError(f"Cannot parse date (day): {raw_date}")
        if len(tokens_l) == 3:
            y = _safe_int(tokens_l[2])
            if y is None:
                raise ValueError(f"Cannot parse date (year): {raw_date}")
            if y < 100:
                yy = y
            else:
                yy = _two_digit_year(y)
            return f"{d:02d}{mo:02d}{yy:02d}"
        y_full = infer_year(d, mo)
        return f"{d:02d}{mo:02d}{_two_digit_year(y_full):02d}"

    # Pattern B: DD MON [YYYY]
    if len(tokens_l) in (2, 3) and tokens_l[1] in _MONTHS:
        d = _safe_int(tokens_l[0])
        if d is None:
            raise ValueError(f"Cannot parse date (day): {raw_date}")
        mo = _MONTHS[tokens_l[1]]
        if len(tokens_l) == 3:
            y = _safe_int(tokens_l[2])
            if y is None:
                raise ValueError(f"Cannot parse date (year): {raw_date}")
            if y < 100:
                yy = y
            else:
                yy = _two_digit_year(y)
            return f"{d:02d}{mo:02d}{yy:02d}"
        y_full = infer_year(d, mo)
        return f"{d:02d}{mo:02d}{_two_digit_year(y_full):02d}"

    # If we get here, we don't know how to parse it
    raise ValueError(f"Cannot normalize date to ddmmyy: {raw_date!r}")


# ============================================================
# RENDER PAYLOAD
# ============================================================

def build_render_payload(
    row: Dict[str, Any],
    dynamic_theme: str | None,
) -> Dict[str, Any]:
    out_raw = row.get("out_date", "")
    in_raw = row.get("in_date", "")
    price_raw = row.get("price", "")

    out_ddmmyy = normalize_date_ddmmyy(out_raw) if out_raw else ""
    in_ddmmyy = normalize_date_ddmmyy(in_raw) if in_raw else ""
    price_gbp = normalize_price_gbp(price_raw) if price_raw else ""

    payload = {
        "FROM": row.get("from_city", ""),
        "TO": row.get("to_city", ""),
        "OUT": out_ddmmyy,
        "IN": in_ddmmyy,
        "PRICE": price_gbp,
        "theme": normalize_theme(dynamic_theme),
    }
    return payload


# ============================================================
# MAIN RENDER FUNCTION
# ============================================================

def render_row(row_index: int) -> bool:
    """
    row_index is 1-based Google Sheets row index (excluding header).
    RAW_DEALS_VIEW is assumed to be 1:1 aligned with RAW_DEALS.
    """
    gc = get_gspread_client()

    sheet = gc.open_by_key(SPREADSHEET_ID)
    raw_ws = sheet.worksheet(RAW_DEALS_TAB)
    rdv_ws = sheet.worksheet(RAW_DEALS_VIEW_TAB)

    raw_headers = raw_ws.row_values(1)
    raw_row = raw_ws.row_values(row_index + 1)

    row_data = dict(zip(raw_headers, raw_row))

    # Column AT = dynamic_theme (1-based index 46)
    dynamic_theme = rdv_ws.cell(row_index + 1, 46).value

    # Build payload (enforces locked ddmmyy contract)
    payload = build_render_payload(row_data, dynamic_theme)

    # Hard guard: renderer will 500 if OUT/IN are not ddmmyy
    if payload["OUT"] and not re.fullmatch(r"\d{6}", payload["OUT"]):
        raise ValueError(f"OUT not ddmmyy after normalization: {payload['OUT']!r}")
    if payload["IN"] and not re.fullmatch(r"\d{6}", payload["IN"]):
        raise ValueError(f"IN not ddmmyy after normalization: {payload['IN']!r}")

    response = requests.post(RENDER_URL, json=payload, timeout=30)

    if not response.ok:
        raise RuntimeError(
            f"Render failed ({response.status_code}): {response.text} | payload={payload}"
        )

    return True


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    ok = render_row(1)
    print("Render OK:", ok)
