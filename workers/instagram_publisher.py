#!/usr/bin/env python3
"""
TravelTxter V4.5x — Instagram Publisher (Locked)
Step 4: Social Media Distribution
"""

import os
import time
import datetime as dt
import requests
import gspread
from google.oauth2.service_account import Credentials

def log(msg):
    print(f"{dt.datetime.now().isoformat()} | {msg}", flush=True)

def main():
    log("Starting Instagram Publisher...")
    
    # 1. Setup Connections
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds_json = os.getenv("GCP_SA_JSON_ONE_LINE")
    creds = Credentials.from_service_account_info(eval(creds_json), scopes=scope)
    gc = gspread.authorize(creds)
    
    sh = gc.open_by_key(os.getenv("SPREADSHEET_ID"))
    ws = sh.worksheet("RAW_DEALS")
    
    # 2. Find the Ready Deal (Status: READY_TO_PUBLISH)
    data = ws.get_all_records()
    target_row = None
    row_index = -1

    for i, row in enumerate(data, start=2):
        if row.get("status") == "READY_TO_PUBLISH":
            target_row = row
            row_index = i
            break

    if not target_row:
        log("No deals marked 'READY_TO_PUBLISH' found.")
        return

    # 3. Instagram Graph API Credentials
    ig_token = os.getenv("IG_ACCESS_TOKEN")
    ig_user_id = os.getenv("IG_USER_ID")
    image_url = target_row.get("graphic_url")
    caption = target_row.get("ai_caption")

    log(f"Publishing Deal {target_row['deal_id']} to Instagram...")

    try:
        # A. Create Media Container
        post_url = f"https://graph.facebook.com/v20.0/{ig_user_id}/media"
        payload = {
            "image_url": image_url,
            "caption": caption,
            "access_token": ig_token
        }
        r1 = requests.post(post_url, data=payload)
        creation_id = r1.json().get("id")

        if not creation_id:
            raise Exception(f"Failed to create container: {r1.text}")

        # B. Publish Container
        publish_url = f"https://graph.facebook.com/v20.0/{ig_user_id}/media_publish"
        r2 = requests.post(publish_url, data={
            "creation_id": creation_id,
            "access_token": ig_token
        })
        media_id = r2.json().get("id")

        if media_id:
            # 4. Update Sheet Status
            ws.update_cell(row_index, 26, "POSTED_INSTAGRAM")
            ws.update_cell(row_index, 33, dt.datetime.now().isoformat()) # published_timestamp
            log(f"✅ SUCCESSFULLY POSTED! Media ID: {media_id}")
        else:
            log(f"❌ Publish failed: {r2.text}")

    except Exception as e:
        log(f"❌ Instagram Error: {e}")

if __name__ == "__main__":
    main()
