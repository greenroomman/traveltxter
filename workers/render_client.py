from __future__ import annotations

import os
import re
import json
import math
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Tuple

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

STATUS_READY_TO_POST = os.getenv("STATUS_READY_TO_POST", "READY_TO_POST")
STATUS_READY_TO_PUBLISH = os.getenv("STATUS_READY_TO_PUBLISH", "READY_TO_PUBLISH")

# How many rows to render per run (default 1 to keep it safe)
RENDER_MAX_PER_RUN = int(os.getenv("RENDER_MAX_PER_RUN", "1"))

# RDV locked column index for dynamic_theme (AT = 46)
RDV_DYNAMIC_THEME_COL = int(os.getenv("RDV_DYNAMIC_THEME_COL", "46"))


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
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat() + "Z"


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
    if data:
        ws.batch_update(data)


# ============================================================
# NORMALISERS (LOCKED RENDER CONTRACT)
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

    m = re.fullmatch(r"(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})", s)
    if m:
        y = int(m.group(1))
        mo = int(m.group(2))
        d = int(m.group(3))
        return f"{d:02d}{mo:02d}{_two_digit_year(y):02d}"

    m = re.fullmatch(r"(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})", s)
    if m:
        d = int(m.group(1))
        mo = int(m.group(2))
        y = int(m.group(3))
        return f"{d:02d}{mo:02d}{_two_digit_year(y):02d}"

    # If it's already ISO YYYY-MM-DD but failed above, try split
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        y, mo, d = s.split("-")
        return f"{int(d):02d}{int(mo):02d}{_two_digit_year(int(y)):02d}"

    raise ValueError(f"Cannot normalize date to ddmmyy: {raw_date!r}")


# ============================================================
# RENDER PAYLOAD
# ============================================================

def build_render_payload(row: Dict[str, Any], dynamic_theme: str | None) -> Dict[str, Any]:
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
# CORE: RENDER A SPECIFIC SHEET ROW
# ============================================================

def render_sheet_row(sheet_row_1_based: int) -> bool:
    gc = get_gspread_client()
    sheet = gc.open_by_key(SPREADSHEET_ID)
    raw_ws = sheet.worksheet(RAW_DEALS_TAB)
    rdv_ws = sheet.worksheet(RAW_DEALS_VIEW_TAB)

    raw_headers = raw_ws.row_values(1)
    raw_row = raw_ws.row_values(sheet_row_1_based)

    print(f"Target sheet row: {sheet_row_1_based}")
    print(f"Row has {len(raw_row)} values | Headers: {len(raw_headers)} columns")

    if not raw_row or all(not str(v).strip() for v in raw_row):
        print("⚠️ Row is empty - skipping")
        return False

    row_data = dict(zip(raw_headers, raw_row))

    status = str(row_data.get("status") or "").strip()
    if status != STATUS_READY_TO_POST:
        print(f"⚠️ Skip row {sheet_row_1_based}: status={status!r} (needs {STATUS_READY_TO_POST!r})")
        return False

    # RDV dynamic_theme (AT=46) aligned by row number
    dynamic_theme = rdv_ws.cell(sheet_row_1_based, RDV_DYNAMIC_THEME_COL).value
    print(f"Dynamic theme from RDV col {RDV_DYNAMIC_THEME_COL}: {dynamic_theme}")

    payload = build_render_payload(row_data, dynamic_theme)
    print(f"Built payload: {payload}")

    if not payload["TO"] or not payload["FROM"] or not payload["OUT"] or not payload["IN"]:
        raise ValueError("Payload missing required fields (FROM/TO/OUT/IN)")

    if not re.fullmatch(r"\d{6}", payload["OUT"]):
        raise ValueError(f"OUT must be ddmmyy (6 digits). Got: {payload['OUT']!r}")
    if not re.fullmatch(r"\d{6}", payload["IN"]):
        raise ValueError(f"IN must be ddmmyy (6 digits). Got: {payload['IN']!r}")

    # Call renderer
    response = requests.post(RENDER_URL, json=payload, timeout=60)
    if not response.ok:
        raise RuntimeError(f"Render failed ({response.status_code}): {response.text}")

    try:
        resp_json = response.json()
    except Exception:
        raise RuntimeError(f"Render returned non-JSON: {response.text[:200]}")

    graphic_url = extract_graphic_url(resp_json)
    if not graphic_url:
        raise RuntimeError("Render JSON missing graphic_url")

    ts = utc_now_iso()

    updates: Dict[str, str] = {
        "graphic_url": graphic_url,
        "rendered_timestamp": ts,
        "rendered_at": ts,
        "render_error": "",
        "status": STATUS_READY_TO_PUBLISH,  # promote after successful asset creation
    }

    update_cells_by_header(raw_ws, raw_headers, sheet_row_1_based, updates)

    print(f"✅ Render OK row {sheet_row_1_based} | status {STATUS_READY_TO_POST}→{STATUS_READY_TO_PUBLISH}")
    return True


# ============================================================
# SELECT: FIND ELIGIBLE ROWS (READY_TO_POST, no graphic_url)
# ============================================================

def find_render_candidates(
    raw_ws: gspread.Worksheet,
    raw_headers: List[str],
    max_n: int,
) -> List[int]:
    status_col = find_col_index(raw_headers, "status")
    graphic_col = find_col_index(raw_headers, "graphic_url")

    if not status_col:
        raise RuntimeError("RAW_DEALS missing required header: status")

    # Pull only needed columns (cheaper than full sheet)
    status_vals = raw_ws.col_values(status_col)  # includes header
    graphic_vals = raw_ws.col_values(graphic_col) if graphic_col else []

    candidates: List[int] = []
    for r in range(2, len(status_vals) + 1):  # sheet rows start at 2 for data
        st = str(status_vals[r - 1] or "").strip()
        if st != STATUS_READY_TO_POST:
            continue

        if graphic_col and r - 1 < len(graphic_vals):
            gu = str(graphic_vals[r - 1] or "").strip()
            if gu:
                continue

        candidates.append(r)
        if len(candidates) >= max_n:
            break

    return candidates


def main() -> int:
    gc = get_gspread_client()
    sheet = gc.open_by_key(SPREADSHEET_ID)
    raw_ws = sheet.worksheet(RAW_DEALS_TAB)

    raw_headers = raw_ws.row_values(1)

    candidates = find_render_candidates(raw_ws, raw_headers, max_n=RENDER_MAX_PER_RUN)

    if not candidates:
        print(f"ℹ️ No candidates found with status={STATUS_READY_TO_POST} and blank graphic_url")
        return 0

    print(f"🎯 Render candidates (sheet rows): {candidates}")

    ok_count = 0
    for sheet_row in candidates:
        try:
            if render_sheet_row(sheet_row):
                ok_count += 1
        except Exception as e:
            # Write render_error on the row (best effort) and continue
            try:
                ts = utc_now_iso()
                update_cells_by_header(
                    raw_ws,
                    raw_headers,
                    sheet_row,
                    {
                        "render_error": f"{type(e).__name__}: {e}",
                        "rendered_timestamp": ts,
                        "rendered_at": ts,
                    },
                )
            except Exception:
                pass
            raise

    print(f"✅ Render complete. Success count: {ok_count}/{len(candidates)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
