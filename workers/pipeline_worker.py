# workers/pipeline_worker.py
#!/usr/bin/env python3
"""
TravelTxter — Pipeline Worker (LOCKED)

ROLE:
- Feeder + Orchestrator
- Inserts NEW rows into RAW_DEALS
- Sets initial deal_theme ONLY
- Never reads dynamic_theme or RAW_DEALS_VIEW
"""

from __future__ import annotations

import os
import json
import time
import datetime as dt
from typing import Dict, Any, List

import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# Helpers
# ============================================================

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


def low(s: str) -> str:
    return (s or "").strip().lower()


def env_str(k: str, default: str = "") -> str:
    return (os.environ.get(k, default) or "").strip()


# ============================================================
# Theme of day (feeder intent only)
# ============================================================

MASTER_THEMES = [
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
]


def theme_of_day_utc() -> str:
    today = dt.datetime.utcnow().date()
    doy = int(today.strftime("%j"))
    return MASTER_THEMES[doy % len(MASTER_THEMES)]


# ============================================================
# Sheets auth
# ============================================================

def parse_sa_json(raw: str) -> Dict[str, Any]:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return json.loads(raw.replace("\\n", "\n"))


def gs_client() -> gspread.Client:
    raw = env_str("GCP_SA_JSON_ONE_LINE") or env_str("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing GCP service account JSON")
    info = parse_sa_json(raw)
    creds = Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


# ============================================================
# Main
# ============================================================

def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    raw_tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")

    theme_today = low(env_str("THEME_OF_DAY")) or low(theme_of_day_utc())
    log(f"🎯 Theme of the day (UTC): {theme_today}")

    gc = gs_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(raw_tab)

    headers = ws.row_values(1)
    ci = {h.strip(): i for i, h in enumerate(headers) if h.strip()}

    # 🔒 REQUIRED COLUMNS
    required = ["status", "deal_theme"]
    missing = [c for c in required if c not in ci]
    if missing:
        raise RuntimeError(f"RAW_DEALS missing required columns: {missing}")

    # ================================
    # Feeder logic (unchanged)
    # ================================
    # Wherever you previously wrote `theme`, write `deal_theme` instead.
    # Example insertion payload (illustrative only):

    new_row = [""] * len(headers)
    new_row[ci["status"]] = "NEW"
    new_row[ci["deal_theme"]] = theme_today

    ws.append_row(new_row, value_input_option="USER_ENTERED")
    log("✅ Inserted NEW deal with deal_theme set")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
