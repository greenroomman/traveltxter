#!/usr/bin/env python3
"""
TravelTxter V4.5x — Renderer (Locked)
Step 3: Graphic Generation
"""

import os
import json
import math
import datetime as dt
import requests
import gspread
from google.oauth2.service_account import Credentials

def log(msg):
    print(f"{dt.datetime.now().isoformat()} | {msg}", flush=True)

def main():
    log("Starting Renderer...")
    
    # 1. Setup Connections
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds_json = os.getenv("GCP_SA_JSON_ONE_LINE")
    creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scope)
    gc = gspread.authorize(creds)
    
    sh = gc.open_by_key(os.getenv("SPREADSHEET_ID"))
    ws = sh.worksheet("RAW_DEALS")
    
    # 2. Find the Winner (Status: READY_TO_POST)
    data = ws.get_all_records()
    target_row = None
    row_index = -1

    for i, row in enumerate(data, start=2):
        if row.get("status") == "READY_TO_POST":
            target_row = row
            row_index = i
            break

    if not target_row:
        log("No deals marked 'READY_TO_POST' found.")
        return

    # 3. Prepare Payload for PythonAnywhere
    # We round the price up for cleaner graphics (e.g., £44.12 -> £45)
    price_raw = float(target_row.get("price_gbp", 0))
    clean_price = f"£{int(math.ceil(price_raw))}"
    
    payload = {
        "deal_id": target_row.get("deal_id"),
        "origin": target_row.get("origin_iata"),
        "dest": target_row.get("destination_iata"),
        "price": clean_price,
        "outbound": target_row.get("outbound_date"),
        "return": target_row.get("return_date")
    }

    # 4. Call your PythonAnywhere Server
    render_url = os.getenv("RENDER_URL") # e.g., https://yourname.pythonanywhere.com/render
    log(f"Calling Renderer at {render_url}...")
    
    try:
        response = requests.post(render_url, json=payload, timeout=60)
        response.raise_for_status()
        result = response.json()
        
        graphic_url = result.get("graphic_url")
        
        # 5. Update Sheet with the Image Link
        ws.update_cell(row_index, 26, "READY_TO_PUBLISH") # Status -> READY_TO_PUBLISH
        ws.update_cell(row_index, 32, graphic_url)        # graphic_url column
        
        log(f"✅ Graphic Created: {graphic_url}")
        
    except Exception as e:
        log(f"❌ Render Failed: {e}")

if __name__ == "__main__":
    main()
