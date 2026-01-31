from __future__ import annotations

import os
import re
import json
import requests
from typing import Dict, Any

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
# RENDER PAYLOAD
# ============================================================

def build_render_payload(
    row: Dict[str, Any],
    dynamic_theme: str | None,
) -> Dict[str, Any]:
    return {
        "FROM": row.get("from_city", ""),
        "TO": row.get("to_city", ""),
        "OUT": row.get("out_date", ""),
        "IN": row.get("in_date", ""),
        "PRICE": row.get("price", ""),
        "theme": normalize_theme(dynamic_theme),
    }


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

    payload = build_render_payload(row_data, dynamic_theme)

    response = requests.post(RENDER_URL, json=payload, timeout=30)

    if not response.ok:
        raise RuntimeError(
            f"Render failed ({response.status_code}): {response.text}"
        )

    return True


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    ok = render_row(1)
    print("Render OK:", ok)
