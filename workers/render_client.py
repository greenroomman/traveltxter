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

# Status transition (strict)
STATUS_READY_TO_POST = os.getenv("STATUS_READY_TO_POST", "READY_TO_POST")
STATUS_READY_TO_PUBLISH = os.getenv("STATUS_READY_TO_PUBLISH", "READY_TO_PUBLISH")


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


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def find_col_index(headers: list[str], col_name: str) -> Optional[int]:
    target = col_name.strip().lower()
    for i, h in enumerate(headers, start=1):
        if str(h).strip().lower() == target:
            return i
    return None


def a1(col_index: int, row_index_1_based: int) -> str:
    n = col_index
    letters = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        letters = chr(65 + r) + letters
    return f"{letters}{row_index_1_based}"


def update_cells_by_header(
    ws: gspread.Worksheet,
    headers: list[str],
    sheet_row_1_based: int,
    updates: Dict[str, str],
) -> None:
    data = []
    for col_name, value in updates.items():
        idx = find_col_index(headers, col_name)
        if not idx:
            continue
        data.append(
            {
                "range": a1(idx, sheet_row_1_based),
                "values": [[value]],
            }
        )

    if not data:
        return

    ws.batch_update(data)


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
    if not raw_price:
        return ""
    s = str(raw_price).strip()
    if not s:
        return ""

    m = re.search(r"(\d+(?:\.\d+)?)", s.replace(",", ""))
    if not m:
        return ""

    val = float(m.group(1))
    val_int = int(math.ceil(val))
    return f"£{val_int}"


def normalize_date_ddmmyy(raw_date: str | None) -> str:
    if not raw_date:
        return ""
    s = str(raw_date).strip()
    if not s:
        return ""

    if re.fullmatch(r"\d{6}", s):
        return s

    if re.fullmatch(r"\d{8}", s):
        dd = s[0:2]
        mm = s[2:4]
        yy = s[6:8]
        return f"{dd}{mm}{yy}"

    m = re.fullmatch(r"(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2})", s)
    if m:
        d = int(m.group(1))
        mo = int(m.group(2))
        y = int(m.group(3))
        return f"{d:02d}{mo:02d}{y:02d}"

    m = re.fullmatch(r"(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})", s)
    if m:
        d = int(m.group(1))
        mo = int(m.group(2))
        y = int(m.group(3))
        return f"{d:02d}{mo:02d}{_two_digit_year(y):02d}"

    m = re.fullmatch(r"(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})", s)
    if m:
        y = int(m.group(1))
        mo = int(m.group(2))
        d = int(m.group(3))
        return f"{d:02d}{mo:02d}{_two_digit_year(y):02d}"

    tokens = re.split(r"\s+", re.sub(r"[,\.\-]+", " ", s).strip())
    tokens_l = [t.lower() for t in tokens if t.strip()]

    now = datetime.now(timezone.utc)
    current_year = now.year

    def infer_year(d: int, mo: int) -> int:
        try:
            candidate = datetime(current_year, mo, d, tzinfo=timezone.utc)
        except Exception:
            return current_year
        delta_days = (candidate - now).days
        if delta_days < -180:
            return current_year + 1
        return current_year

    if len(tokens_l) in (2, 3) and tokens_l[0] in _MONTHS:
        mo = _MONTHS[tokens_l[0]]
        d = _safe_int(tokens_l[1])
        if d is None:
            raise ValueError(f"Cannot parse date (day): {raw_date}")
        if len(tokens_l) == 3:
            y = _safe_int(tokens_l[2])
            if y is None:
                raise ValueError(f"Cannot parse date (year): {raw_date}")
            yy = y if y < 100 else _two_digit_year(y)
            return f"{d:02d}{mo:02d}{yy:02d}"
        y_full = infer_year(d, mo)
        return f"{d:02d}{mo:02d}{_two_digit_year(y_full):02d}"

    if len(tokens_l) in (2, 3) and tokens_l[1] in _MONTHS:
        d = _safe_int(tokens_l[0])
        if d is None:
            raise ValueError(f"Cannot parse date (day): {raw_date}")
        mo = _MONTHS[tokens_l[1]]
        if len(tokens_l) == 3:
            y = _safe_int(tokens_l[2])
            if y is None:
                raise ValueError(f"Cannot parse date (year): {raw_date}")
            yy = y if y < 100 else _two_digit_year(y)
            return f"{d:02d}{mo:02d}{yy:02d}"
        y_full = infer_year(d, mo)
        return f"{d:02d}{mo:02d}{_two_digit_year(y_full):02d}"

    raise ValueError(f"Cannot normalize date to ddmmyy: {raw_date!r}")


# ============================================================
# RENDER PAYLOAD
# ============================================================

def build_render_payload(
    row: Dict[str, Any],
    dynamic_theme: str | None,
) -> Dict[str, Any]:
    out_raw = row.get("outbound_date", "") or row.get("out_date", "")
    in_raw = row.get("return_date", "") or row.get("in_date", "")
    price_raw = row.get("price_gbp", "") or row.get("price", "")
    from_city = row.get("origin_city", "") or row.get("from_city", "")
    to_city = row.get("destination_city", "") or row.get("to_city", "")

    out_ddmmyy = normalize_date_ddmmyy(out_raw) if out_raw else ""
    in_ddmmyy = normalize_date_ddmmyy(in_raw) if in_raw else ""
    price_gbp = normalize_price_gbp(price_raw) if price_raw else ""

    return {
        "FROM": from_city,
        "TO": to_city,
        "OUT": out_ddmmyy,
        "IN": in_ddmmyy,
        "PRICE": price_gbp,
        "theme": normalize_theme(dynamic_theme),
    }


def extract_graphic_url(resp_json: Dict[str, Any]) -> str:
    for k in ("graphic_url", "png_url", "image_url", "url"):
        v = resp_json.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    if isinstance(resp_json.get("data"), dict):
        return extract_graphic_url(resp_json["data"])
    return ""


# ============================================================
# MAIN RENDER FUNCTION
# ============================================================

def render_row(row_index: int) -> bool:
    gc = get_gspread_client()
    sheet = gc.open_by_key(SPREADSHEET_ID)
    raw_ws = sheet.worksheet(RAW_DEALS_TAB)
    rdv_ws = sheet.worksheet(RAW_DEALS_VIEW_TAB)

    raw_headers = raw_ws.row_values(1)
    raw_row = raw_ws.row_values(row_index + 1)

    print(f"Row {row_index + 1} has {len(raw_row)} values")
    print(f"Headers: {len(raw_headers)} columns")

    if not raw_row or all(not str(v).strip() for v in raw_row):
        print(f"⚠️ Row {row_index + 1} is empty or all blank - skipping")
        return False

    row_data = dict(zip(raw_headers, raw_row))

    # Column AT = dynamic_theme (1-based index 46)
    dynamic_theme = rdv_ws.cell(row_index + 1, 46).value
    print(f"Dynamic theme from RDV col 46: {dynamic_theme}")

    payload = build_render_payload(row_data, dynamic_theme)
    print(f"Built payload: {payload}")

    if payload["OUT"] and not re.fullmatch(r"\d{6}", payload["OUT"]):
        raise ValueError(f"OUT not ddmmyy after normalization: {payload['OUT']!r}")
    if payload["IN"] and not re.fullmatch(r"\d{6}", payload["IN"]):
        raise ValueError(f"IN not ddmmyy after normalization: {payload['IN']!r}")

    if not payload["TO"] or not payload["FROM"] or not payload["OUT"] or not payload["IN"]:
        raise ValueError(
            f"Payload has empty required fields! "
            f"TO={payload['TO']!r} FROM={payload['FROM']!r} "
            f"OUT={payload['OUT']!r} IN={payload['IN']!r}"
        )

    sheet_row_1_based = row_index + 1  # includes header row

    try:
        response = requests.post(RENDER_URL, json=payload, timeout=60)
        if not response.ok:
            raise RuntimeError(f"{response.status_code}: {response.text}")

        try:
            resp_json = response.json()
        except Exception:
            raise RuntimeError(f"Render returned non-JSON: {response.text[:200]}")

        graphic_url = extract_graphic_url(resp_json)
        if not graphic_url:
            raise RuntimeError(f"Render JSON missing graphic_url (keys={list(resp_json.keys())})")

        ts = utc_now_iso()

        updates: Dict[str, str] = {
            "graphic_url": graphic_url,
            "rendered_timestamp": ts,
            "rendered_at": ts,
            "render_error": "",
        }

        # Strict status promotion: READY_TO_POST -> READY_TO_PUBLISH only
        status_idx = find_col_index(raw_headers, "status")
        if status_idx:
            current_status = str(row_data.get("status") or "").strip()
            if current_status == STATUS_READY_TO_POST:
                updates["status"] = STATUS_READY_TO_PUBLISH

        update_cells_by_header(raw_ws, raw_headers, sheet_row_1_based, updates)

        print(
            f"✅ Render successful for row {sheet_row_1_based} -> graphic_url set"
            f"{' + status promoted' if updates.get('status') else ''}"
        )
        return True

    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        print(f"❌ Render failed for row {sheet_row_1_based}: {err}")

        try:
            update_cells_by_header(
                raw_ws,
                raw_headers,
                sheet_row_1_based,
                {
                    "render_error": err,
                    "rendered_timestamp": utc_now_iso(),
                    "rendered_at": utc_now_iso(),
                },
            )
        except Exception as e2:
            print(f"⚠️ Could not write render_error back to sheet: {type(e2).__name__}: {e2}")

        raise


if __name__ == "__main__":
    ok = render_row(1)
    print("Render OK:", ok)
