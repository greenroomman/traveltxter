#!/usr/bin/env python3
"""
TravelTxter V4.5.3 — Feeder Engine (Locked)
Step 1: Ingestion
"""

import os
import json
import random
import datetime as dt
import requests
import gspread
from google.oauth2.service_account import Credentials

def now_utc():
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")

def main():
    # 1. Setup Connections
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds_json = os.getenv("GCP_SA_JSON_ONE_LINE")
    if not creds_json:
        raise ValueError("Missing GCP_SA_JSON_ONE_LINE secret.")
    
    creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scope)
    gc = gspread.authorize(creds)
    
    # 2. Open Your Spreadsheet
    sh = gc.open_by_key(os.getenv("SPREADSHEET_ID"))
    raw_ws = sh.worksheet("RAW_DEALS")
    
    # 3. Search Logic (Simplified for Setup)
    # This matches the 'Canonical' headers in your RAW_DEALS tab
    deal_id = f"deal_{int(dt.datetime.now().timestamp())}"
    
    # This list must match your RAW_DEALS columns exactly
    new_row = [
        deal_id,             # deal_id
        "London",            # origin_city
        "LGW",               # origin_iata
        "Barcelona",         # destination_city
        "BCN",               # destination_iata
        "Spain",             # destination_country
        45.00,               # price_gbp
        "2026-03-01",        # outbound_date
        "2026-03-05",        # return_date
        4,                   # trip_length_days
        0,                   # stops
        "Vueling",           # airline
        "CITY_BREAK",        # theme
        "CITY_BREAK",        # resolved_theme
        "CITY_BREAK",        # theme_final
        "", "", "", "", "",  # Scores (Filled by Step 2)
        "", "", "",          # AI Fields
        "duffel",            # deal_source
        now_utc(),           # date_added
        "NEW"                # STATUS: This triggers the next step
    ]
    
    # 4. Insert into Sheet
    raw_ws.append_row(new_row, value_input_option="USER_ENTERED")
    print(f"✅ Success: Added {deal_id} to RAW_DEALS with status 'NEW'")

if __name__ == "__main__":
    main()
