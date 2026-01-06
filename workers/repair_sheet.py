#!/usr/bin/env python3
"""
TravelTxter V4.5x — repair_sheet.py (LOCKED)

ROLE:
- One-time (or occasional) schema repair for Google Sheet
- Ensures RAW_DEALS has all required columns (adds missing to the right)
- Does NOT move data, does NOT delete anything, does NOT change statuses
- Safe for non-technical use

HOW TO RUN (GitHub Actions or locally):
- GitHub Actions: add a temporary step: `python workers/repair_sheet.py`
- Locally:
    export SPREADSHEET_ID="..."
    export RAW_DEALS_TAB="RAW_DEALS"
    export GCP_SA_JSON_ONE_LINE='{"type":"service_account",...}'
    python workers/repair_sheet.py
"""

from __future__ import annotations

import os
import json
import time
import datetime as dt
from typing import Dict, Any, List

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
    return os.environ.get(k, default).strip()


# ============================================================
# Robust SA JSON parsing (fixes messy GitHub secrets)
# ============================================================

def _extract_json_object(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()

    try:
        return json.loads(raw)
    except Exception:
        pass

    try:
        return json.loads(raw.replace("\\n", "\n"))
    except Exception:
        pass

    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise RuntimeError("Invalid GCP_SA_JSON_ONE_LINE: no JSON object found")

    candidate = raw[start:end + 1]

    try:
        return json.loads(candidate)
    except Exception:
        pass

    try:
        return json.loads(candidate.replace("\\n", "\n"))
    except Exception as e:
        raise RuntimeError("Invalid GCP_SA_JSON_ONE_LINE: JSON parse failed") from e


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
# Sheet helpers
# ============================================================

def ensure_columns(ws: gspread.Worksheet, headers: List[str], required: List[str]) -> List[str]:
    missing = [c for c in required if c not in headers]
    if not missing:
        return headers
    ws.update([headers + missing], "A1")
    log(f"🛠️  Added missing columns: {missing}")
    return headers + missing


# ============================================================
# Canonical RAW_DEALS schema (V4.5x)
# ============================================================

RAW_DEALS_REQUIRED = [
    # Core
    "status",
    "deal_id",
    "price_gbp",
    "origin_iata",
    "destination_iata",
    "origin_city",
    "destination_city",
    "destination_country",
    "outbound_date",
    "return_date",
    "stops",
    "deal_theme",

    # Scoring
    "deal_score",
    "dest_variety_score",
    "theme_variety_score",
    "scored_timestamp",

    # Rendering
    "graphic_url",
    "rendered_timestamp",
    "render_error",
    "render_response_snippet",

    # Publishing
    "posted_instagram_at",
    "posted_telegram_vip_at",
    "posted_telegram_free_at",

    # Links
    "affiliate_url",
    "booking_link_vip",
    "affiliate_source",
    "link_routed_at",
]


# ============================================================
# Main
# ============================================================

def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")

    gc = get_client()
    sh = open_sheet_with_backoff(gc, spreadsheet_id)

    ws = sh.worksheet(tab)

    vals = ws.get_all_values()
    if not vals:
        # Create headers if the sheet is empty
        ws.update([RAW_DEALS_REQUIRED], "A1")
        log("✅ RAW_DEALS was empty — created canonical header row.")
        return 0

    headers = [h.strip() for h in vals[0]]
    ensure_columns(ws, headers, RAW_DEALS_REQUIRED)
    log("✅ RAW_DEALS schema repair complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
