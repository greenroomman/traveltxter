#!/usr/bin/env python3
"""
TravelTxter V4.5x — AI Scorer (Locked)
Step 2: Scoring & Winner Promotion
"""

import os
import json
import datetime as dt
import gspread
from google.oauth2.service_account import Credentials

def log(msg):
    print(f"{dt.datetime.now().isoformat()} | {msg}", flush=True)

def main():
    log("Starting Scorer...")
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds_json = os.getenv("GCP_SA_JSON_ONE_LINE")
    creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scope)
    gc = gspread.authorize(creds)
    
    sh = gc.open_by_key(os.getenv("SPREADSHEET_ID"))
    ws = sh.worksheet("RAW_DEALS")
    
    # 1. Get all deals
    data = ws.get_all_records()
    scored_candidates = []

    # 2. Process "NEW" deals
    # We use a loop to find every deal the feeder just added
    for i, row in enumerate(data, start=2):
        if row.get("status") == "NEW":
            deal_id = row.get("deal_id")
            log(f"Scoring deal: {deal_id}")
            
            # --- Scoring Logic ---
            # We calculate a score from 0-100 based on your business rules
            price = float(row.get("price_gbp", 100))
            score = 100 - (price / 2) # Simple example: cheaper is better
            score = max(0, min(100, score))
            
            # --- Write Scores back to Sheet ---
            # These column numbers match your Master Spreadsheet
            ws.update_cell(i, 26, "SCORED")  # Status -> SCORED
            ws.update_cell(i, 27, score)     # ai_score
            ws.update_cell(i, 30, f"Check out this deal to {row['destination_city']} for £{price}!") # ai_caption
            
            scored_candidates.append((i, score))

    # 3. Promote a Winner (SCORED -> READY_TO_POST)
    # This picks the highest scoring deal from the whole sheet to post today
    if not scored_candidates:
        # If no NEW deals were processed, look for existing SCORED deals
        for i, row in enumerate(data, start=2):
            if row.get("status") == "SCORED":
                scored_candidates.append((i, float(row.get("ai_score", 0))))

    if scored_candidates:
        # Sort by score (highest first)
        scored_candidates.sort(key=lambda x: x[1], reverse=True)
        winner_row, winner_score = scored_candidates[0]
        
        ws.update_cell(winner_row, 26, "READY_TO_POST")
        log(f"🏆 Winner Selected: Row {winner_row} (Score: {winner_score})")
    else:
        log("⚠️ No deals found to promote.")

if __name__ == "__main__":
    main()
