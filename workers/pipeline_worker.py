#!/usr/bin/env python3
"""
Traveltxter V4.5.3_Waterwheel
Pipeline Publisher Worker

Responsibilities:
- Publish 1 deal per run
- AM → Telegram VIP
- PM → Telegram FREE
- Enforce no emojis except national flags
"""

import os
import sys
import datetime as dt
import requests
import gspread
from google.oauth2.service_account import Credentials

# ============================================================
# ENV
# ============================================================

RUN_SLOT = os.getenv("RUN_SLOT", "AM")  # AM or PM

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL = os.getenv("TELEGRAM_CHANNEL")

TELEGRAM_BOT_TOKEN_VIP = os.getenv("TELEGRAM_BOT_TOKEN_VIP")
TELEGRAM_CHANNEL_VIP = os.getenv("TELEGRAM_CHANNEL_VIP")

STRIPE_MONTHLY = os.getenv("STRIPE_LINK_MONTHLY")
STRIPE_YEARLY = os.getenv("STRIPE_LINK_YEARLY")

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS")

# ============================================================
# GOOGLE SHEETS
# ============================================================

def get_sheet():
    creds_info = os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON")
    creds = Credentials.from_service_account_info(
        eval(creds_info),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID).worksheet(RAW_DEALS_TAB)


# ============================================================
# TELEGRAM
# ============================================================

def send_telegram(token, channel, text):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": channel,
        "text": text,
        "disable_web_page_preview": True
    }
    r = requests.post(url, json=payload, timeout=20)
    r.raise_for_status()


# ============================================================
# MESSAGE BUILDERS
# ============================================================

def build_vip_message(d):
    return f"""£{d['price']} to {d['country']} {d['flag']}

TO: {d['city']}
FROM: {d['origin']}
OUT: {d['out_date']}
BACK: {d['back_date']}

{d['description']}

[BOOK NOW]({d['booking_link']})
"""


def build_free_message(d):
    return f"""£{d['price']} to {d['country']} {d['flag']}

TO: {d['city']}
FROM: {d['origin']}
OUT: {d['out_date']}
BACK: {d['back_date']}

Heads up:
• VIP members saw this 24 hours ago
• Availability is running low
• Best deals go to VIPs first

Want instant access?
Join TravelTxter Nomad
for £7.99 / month:

- Live deals
- Direct booking links
- Exclusive mistake fares

[Upgrade Monthly]({STRIPE_MONTHLY})
[Upgrade Yearly]({STRIPE_YEARLY})
"""


# ============================================================
# MAIN
# ============================================================

def main():
    sheet = get_sheet()
    rows = sheet.get_all_records()

    if not rows:
        print("No deals found.")
        return

    # Pick first READY deal
    deal = rows[0]

    deal_data = {
        "price": deal["price"],
        "country": deal["country"],
        "flag": deal.get("flag", ""),
        "city": deal["city"],
        "origin": deal["origin_city"],
        "out_date": deal["out_date"],
        "back_date": deal["return_date"],
        "booking_link": deal["booking_link"],
        "description": deal["description_phrase"],
    }

    if RUN_SLOT == "AM":
        print("Publishing VIP message")
        msg = build_vip_message(deal_data)
        send_telegram(
            TELEGRAM_BOT_TOKEN_VIP,
            TELEGRAM_CHANNEL_VIP,
            msg
        )
    else:
        print("Publishing FREE message")
        msg = build_free_message(deal_data)
        send_telegram(
            TELEGRAM_BOT_TOKEN,
            TELEGRAM_CHANNEL,
            msg
        )


if __name__ == "__main__":
    main()
