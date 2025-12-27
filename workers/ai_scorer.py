#!/usr/bin/env python3
"""
Traveltxter - AI Scorer (V3)
Stage 1: Validate environment + connect to Google Sheet
Stage 2 (later): Read NEW rows, score, write back

This file is intentionally simple and CI-safe.
"""

import os
import sys
import json
from typing import Dict, Any, List, Optional

import gspread
from google.oauth2.service_account import Credentials


def log(msg: str) -> None:
    print(msg, flush=True)


def get_env(name: str, required: bool = True, default: str = "") -> str:
    val = os.getenv(name)
    if (val is None or val.strip() == "") and required:
        log(f"ERROR: Missing environment variable: {name}")
        sys.exit(1)
    return (val or default).strip()


# ---------- Google Sheets ----------

def get_worksheet():
    """
    Uses:
      - SHEET_ID (required)
      - WORKSHEET_NAME (optional, defaults to RAW_DEALS)
      - GCP_SA_JSON (required) : the *JSON string* of your service account
    """
    sheet_id = get_env("SHEET_ID", required=True)
    worksheet_name = get_env("WORKSHEET_NAME", required=False, 
default="RAW_DEALS") or "RAW_DEALS"
    sa_json = get_env("GCP_SA_JSON", required=True)

    try:
        sa_info = json.loads(sa_json)
    except json.JSONDecodeError:
        log("ERROR: GCP_SA_JSON is not valid JSON. It must be the full 
JSON content as a single string.")
        sys.exit(1)

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(sa_info, scopes=scopes)

    client = gspread.authorize(creds)
    sheet = client.open_by_key(sheet_id)
    return sheet.worksheet(worksheet_name)


def read_one_deal(ws) -> Optional[Dict[str, Any]]:
    """
    Simple helper: returns the first row (as a dict) if present, else 
None.
    """
    rows = ws.get_all_records()  # list[dict]
    if not rows:
        return None
    return rows[0]


# ---------- Main ----------

def main() -> None:
    log("AI SCORER STARTING")

    # Stage 1: Environment + Sheets connection
    _ = get_env("SHEET_ID", required=True)
    _ = get_env("GCP_SA_JSON", required=True)
    # WORKSHEET_NAME optional; defaults internally

    log("Environment OK")

    try:
        ws = get_worksheet()
    except Exception as e:
        log(f"ERROR: Could not open Google Sheet / worksheet: {e}")
        sys.exit(1)

    log(f"Connected to worksheet: {ws.title}")

    # Stage 1 complete
    log("AI SCORER STAGE 1 COMPLETE")

    # (Optional sanity read - safe)
    deal = read_one_deal(ws)
    if deal:
        log("Sample row read OK (first row exists).")
    else:
        log("Sheet is reachable, but no rows found yet.")


if __name__ == "__main__":
    main()

