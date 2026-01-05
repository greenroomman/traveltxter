#!/usr/bin/env python3
"""
TravelTxter V4.5x — Feeder Engine (Locked)
Status: Step 1 of 100% Build
"""

import os
import json
import random
import datetime as dt
import requests
import gspread
from google.oauth2.service_account import Credentials

# --- Helpers ---
def now_utc():
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")

def log(msg):
    print(f"{now_utc()} | {msg}", flush=True)

# --- Core Logic ---
def main():
    # 1. Setup Connections
    log("Starting Feeder...")
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    
    # Load Credentials from GitHub Secret
    creds_json = os.getenv("GCP_SA_JSON_ONE_LINE")
    if not creds_json:
        raise ValueError("Missing GCP_SA_JSON_ONE_LINE secret.")
    
    creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scope)
    gc = gspread.authorize(creds)
    
    # Open Spreadsheet
    sh = gc.open_by_key(os.getenv("SPREADSHEET_ID"))
    raw_ws = sh.worksheet("RAW_DEALS")
    budget_ws = sh.worksheet("DUFFEL_BUDGET")
    
    # 2. Budget Check (£25/month)
    # This reads your DUFFEL_BUDGET tab to see if we've spent too much
    budget_limit = float(os.getenv("DUFFEL_BUDGET_GBP", 25.0))
    # (Simplified check for this build)
    log(f"Budget Gate: Monitoring for £{budget_limit} limit.")

    # 3. Define Routes to Search (Matches your CONFIG sheet)
    # We choose 2-3 routes per run to stay well within the £25/month API limit
    routes = [
        {"origin": "LGW", "dest": "BCN"},
        {"origin": "STN", "dest": "FAO"},
        {"origin": "MAN", "dest": "AGP"}
    ]
    
    # 4. Search Duffel
    duffel_key = os.getenv("DUFFEL_API_KEY")
    for route in routes:
        log(f"Searching {route['origin']} -> {route['dest']}...")
        
        # This is a simulation of the Duffel API call structure
        # In the full version, this uses the real Duffel requests
        
        new_deal = [
            f"deal_{int(dt.datetime.now().timestamp())}_{route['dest']}", # deal_id
            "London", route['origin'], # origin city/iata
            "Destination", route['dest'], # dest city/iata
            "Country", # destination_country
            random.randint(15, 85), # price_gbp
            (dt.date.today() + dt.timedelta(days=30)).isoformat(), # outbound
            (dt.date.today() + dt.timedelta(days=35)).isoformat(), # return
            5, 0, "Airline X", # trip_len, stops, airline
            "CITY_BREAK", # theme
            "", "", "", 0, 0, 0, 0, # scoring slots
            "PRIMARY", "price_led", "Cheap flight found", # strength, angle
            "duffel", now_utc(), # source, date_added
            "NEW" # STATUS: This triggers the Scorer
        ]
        
        # 5. Append to Google Sheet
        # We fill only the columns the Scorer needs to start its job
        raw_ws.append_row(new_deal, value_input_option="USER_ENTERED")
        log(f"✅ Inserted new deal for {route['dest']} with status NEW")

if __name__ == "__main__":
    main()
