#!/usr/bin/env python3
"""
TravelTxter V4.5x — telegram_publisher.py (FINAL LOCKED OUTPUT)

AM (RUN_SLOT=AM):
  POSTED_INSTAGRAM -> POSTED_TELEGRAM_VIP

PM (RUN_SLOT=PM):
  POSTED_TELEGRAM_VIP -> POSTED_ALL
"""

from __future__ import annotations

import os
import json
import datetime as dt
import hashlib
from typing import Dict, Any, List

import requests
import gspread
from google.oauth2.service_account import Credentials


# ------------------------------------------------
# Logging
# ------------------------------------------------

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


# ------------------------------------------------
# Env helpers
# ------------------------------------------------

def env(k: str, default: str = "") -> str:
    return (os.environ.get(k, default) or "").strip()

def clean_url(u: str) -> str:
    return (u or "").strip().replace(" ", "")


# ------------------------------------------------
# Telegram helpers
# ------------------------------------------------

def html_escape(s: str) -> str:
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )

def html_link(url: str, label: str) -> str:
    if not url:
        return ""
    return f'<a href="{html_escape(url)}">{html_escape(label)}</a>'


def tg_send(token: str, chat_id: str, text: str) -> None:
    r = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": False,
        },
        timeout=30,
    )
    if r.status_code >= 400:
        raise RuntimeError(r.text)


# ------------------------------------------------
# Google Sheets
# ------------------------------------------------

def gs_client():
    raw = env("GCP_SA_JSON_ONE_LINE")
    info = json.loads(raw.replace("\\n", "\n"))
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)


# ------------------------------------------------
# Formatting helpers
# ------------------------------------------------

FLAG_MAP = {
    "ICELAND": "🇮🇸",
    "SPAIN": "🇪🇸",
    "THAILAND": "🇹🇭",
    "HUNGARY": "🇭🇺",
}

def flag(country: str) -> str:
    return FLAG_MAP.get((country or "").upper(), "")

def price_fmt(x: str) -> str:
    s = (x or "").replace("£", "").strip()
    try:
        v = float(s)
        return f"£{v:.2f}" if not v.is_integer() else f"£{int(v)}"
    except:
        return f"£{s}"

def pick_phrase(rows, theme, deal_id):
    approved = [r for r in rows if r.get("approved", "").lower() == "true"]
    themed = [r for r in approved if r.get("theme", "").upper() == theme.upper()]
    pool = themed if themed else approved
    if not pool:
        return ""
    h = int(hashlib.md5(deal_id.encode()).hexdigest(), 16)
    return pool[h % len(pool)]["phrase"]


# ------------------------------------------------
# Templates
# ------------------------------------------------

def build_vip(row, phrase):
    return "\n".join([
        f"{price_fmt(row['price_gbp'])} to {row['destination_country']} {flag(row['destination_country'])}".strip(),
        f"To: {row['destination_city']}",
        f"From: {row['origin_city']}",
        f"Out: {row['outbound_date']}",
        f"Back: {row['return_date']}",
        "",
        html_escape(phrase) if phrase else "",
        "",
        html_link(clean_url(row.get("booking_link_vip") or row.get("affiliate_url")), "BOOKING LINK"),
    ]).strip()


def build_free(row, phrase, monthly, yearly):
    return "\n".join([
        f"{price_fmt(row['price_gbp'])} to {row['destination_country']} {flag(row['destination_country'])}".strip(),
        f"To: {row['destination_city']}",
        f"From: {row['origin_city']}",
        f"Out: {row['outbound_date']}",
        f"Back: {row['return_date']}",
        "",
        html_escape(phrase) if phrase else "",
        "",
        "Want instant access?",
        "Join TravelTxter for early access",
        "",
        "* VIP members saw this 24 hours ago",
        "* Deals 24 hours early",
        "* Direct booking links",
        "* Exclusive mistake fares",
        "* £3 p/m or £30 p/a",
        "* Cancel anytime",
        "",
        f"{html_link(monthly, 'Upgrade now (Monthly)')} | {html_link(yearly, 'Upgrade now (Yearly)')}",
    ]).strip()


# ------------------------------------------------
# Main
# ------------------------------------------------

def main():
    run_slot = env("RUN_SLOT", "AM")
    gc = gs_client()
    sh = gc.open_by_key(env("SPREADSHEET_ID"))
    ws = sh.worksheet(env("RAW_DEALS_TAB", "RAW_DEALS"))

    rows = ws.get_all_records()
    phrases = sh.worksheet("PHRASE_BANK").get_all_records()

    for i, row in enumerate(rows, start=2):
        if run_slot == "AM" and row["status"] == "POSTED_INSTAGRAM":
            phrase = pick_phrase(phrases, row["deal_theme"], row["deal_id"])
            msg = build_vip(row, phrase)
            tg_send(env("TELEGRAM_BOT_TOKEN_VIP"), env("TELEGRAM_CHANNEL_VIP"), msg)
            ws.update_cell(i, ws.find("status").col, "POSTED_TELEGRAM_VIP")
            return

        if run_slot == "PM" and row["status"] == "POSTED_TELEGRAM_VIP":
            phrase = pick_phrase(phrases, row["deal_theme"], row["deal_id"])
            msg = build_free(row, phrase, env("STRIPE_MONTHLY_LINK"), env("STRIPE_YEARLY_LINK"))
            tg_send(env("TELEGRAM_BOT_TOKEN"), env("TELEGRAM_CHANNEL"), msg)
            ws.update_cell(i, ws.find("status").col, "POSTED_ALL")
            return


if __name__ == "__main__":
    main()
