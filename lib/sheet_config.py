# lib/sheet_config.py
"""
Centralised Google Sheets config reader for TravelTxter V4.5x

This module MUST:
- never raise on missing optional tabs
- return safe defaults
- avoid business logic (read-only)
"""

from __future__ import annotations

import json
from typing import Dict, List, Any

import gspread
from google.oauth2.service_account import Credentials


# -------------------------
# Core auth / client
# -------------------------

def _open_sheet(spreadsheet_id: str, sa_json_one_line: str):
    creds_dict = json.loads(sa_json_one_line)
    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    gc = gspread.authorize(creds)
    return gc.open_by_key(spreadsheet_id)


def _read_tab(ws) -> List[Dict[str, Any]]:
    rows = ws.get_all_records()
    return rows if rows else []


# -------------------------
# Public loaders
# -------------------------

def load_config(spreadsheet_id: str, sa_json_one_line: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Loads all control-plane tabs used by V4.5x.

    Missing tabs return empty lists.
    """
    sh = _open_sheet(spreadsheet_id, sa_json_one_line)

    tabs = {
        "CONFIG": [],
        "CONFIG_ORIGIN_POOLS": [],
        "CONFIG_CARRIER_BIAS": [],
        "THEMES": [],
        "CONFIG_SIGNALS": [],
        "PHRASE_BANK": [],
        "MVP_RULES": [],
        "DUFFEL_SEARCH_LOG": [],
    }

    for tab in list(tabs.keys()):
        try:
            ws = sh.worksheet(tab)
            tabs[tab] = _read_tab(ws)
        except gspread.WorksheetNotFound:
            tabs[tab] = []

    return tabs

