#!/usr/bin/env python3
"""
TravelTxter V4.5x — Telegram Publisher (Locked)
Step 5: Tiered VIP & Free Distribution
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
    log("Starting Telegram Pipeline...")
    
    # 1. Setup Connections
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds_json = os.getenv("GCP_SA_JSON_ONE_LINE")
    creds = Credentials.from_service_account_info(eval(creds_json), scopes=scope)
    gc = gspread.authorize(creds)
    
    sh = gc.open_by_key(os.getenv("SPREADSHEET_ID"))
    ws = sh.worksheet("RAW_DEALS")
    
    # 2. Logic: Process VIP and Free Tiers
    data = ws.get_all_records()
    now = dt.datetime.now(dt.timezone.utc)

    for i, row in enumerate(data, start=2):
        status = row.get("status")
        
        # --- TIER A: VIP (Instant Access) ---
        if status == "POSTED_INSTAGRAM":
            log(f"Processing VIP alert for {row['deal_id']}...")
            send_telegram_msg(row, is_vip=True)
            ws.update_cell(i, 26, "POSTED_VIP") # Status -> POSTED_VIP
            ws.update_cell(i, 40, now.isoformat()) # tg_monthly_timestamp (VIP)

        # --- TIER B: FREE (24-Hour Delay) ---
        elif status == "POSTED_VIP":
            # Check if 24 hours have passed since VIP post
            vip_time_str = row.get("tg_monthly_timestamp")
            if vip_time_str:
                vip_time = dt.datetime.fromisoformat(vip_time_str.replace("Z", "+00:00"))
                if now - vip_time > dt.timedelta(hours=24):
                    log(f"24h passed. Processing FREE alert for {row['deal_id']}...")
                    send_telegram_msg(row, is_vip=False)
                    ws.update_cell(i, 26, "ARCHIVED") # Final Status
                    ws.update_cell(i, 38, now.isoformat()) # tg_free_timestamp

def send_telegram_msg(row, is_vip):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TG_VIP_CHAT_ID") if is_vip else os.getenv("TG_FREE_CHAT_ID")
    
    # VIPs get direct booking links; Free users get the "Join VIP" message
    if is_vip:
        link_text = f"🔗 Book Now: {row.get('booking_link_vip')}"
        header = "🔥 VIP EARLY ACCESS 🔥"
    else:
        link_text = "🔒 Members saw this 24h ago. Upgrade for links!"
        header = "✈️ DAILY DEAL (FREE TIER) ✈️"

    message = f"{header}\n\n{row.get('ai_caption')}\n\n{link_text}"
    
    url = f"https://api.telegram.org/bot{token}/sendPhoto"
    payload = {
        "chat_id": chat_id,
        "photo": row.get("graphic_url"),
        "caption": message,
        "parse_mode": "HTML"
    }
    requests.post(url, data=payload)

if __name__ == "__main__":
    main()
