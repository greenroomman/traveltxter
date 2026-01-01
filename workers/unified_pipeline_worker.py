#!/usr/bin/env python3
"""
TravelTxter V4.2 — Unified Pipeline (A+B LOCKED)

LOCKED BEHAVIOUR:
- Instagram posts every run (AM + PM)
- Human UK English captions (short, varied, non-AI)
- Render stage REQUIRED before Instagram
- VIP Telegram first, FREE after delay
- City names everywhere (IATA fallback only)
"""

import os
import json
import uuid
import datetime as dt
import requests
import gspread
from google.oauth2.service_account import Credentials

# ============================================================
# ENV
# ============================================================

def env(name, default="", required=False):
    v = (os.getenv(name) or "").strip()
    if not v:
        v = default
    if required and not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v

SPREADSHEET_ID = env("SPREADSHEET_ID", required=True)
RAW_DEALS_TAB = env("RAW_DEALS_TAB", "RAW_DEALS")
GCP_SA_JSON = env("GCP_SA_JSON", required=True)

RENDER_URL = env("RENDER_URL", required=True)

IG_ACCESS_TOKEN = env("IG_ACCESS_TOKEN", required=True)
IG_USER_ID = env("IG_USER_ID", required=True)

TELEGRAM_BOT_TOKEN_VIP = env("TELEGRAM_BOT_TOKEN_VIP", required=True)
TELEGRAM_VIP_CHANNEL = env("TELEGRAM_VIP_CHANNEL", required=True)

TELEGRAM_BOT_TOKEN = env("TELEGRAM_BOT_TOKEN", required=True)
TELEGRAM_FREE_CHANNEL = env("TELEGRAM_FREE_CHANNEL", required=True)

VIP_DELAY_HOURS = int(env("VIP_DELAY_HOURS", "24"))

RUN_SLOT = env("RUN_SLOT", "AM").upper()  # AM / PM

# ============================================================
# CONSTANTS
# ============================================================

STATUS_NEW = "NEW"
STATUS_READY_TO_POST = "READY_TO_POST"
STATUS_READY_TO_PUBLISH = "READY_TO_PUBLISH"
STATUS_POSTED_INSTAGRAM = "POSTED_INSTAGRAM"
STATUS_POSTED_TELEGRAM_VIP = "POSTED_TELEGRAM_VIP"
STATUS_POSTED_ALL = "POSTED_ALL"

# ============================================================
# HELPERS
# ============================================================

def now():
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def log(msg):
    print(f"{now()} | {msg}", flush=True)

def strip(v):
    return str(v).replace("\u00A0", " ").strip() if v else ""

def hours_since(ts):
    if not ts:
        return 9999
    t = dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return (dt.datetime.now(dt.timezone.utc) - t).total_seconds() / 3600

def gbp(p):
    p = strip(p).replace("£", "")
    try:
        return f"£{int(float(p))}"
    except:
        return f"£{p}"

# ============================================================
# INSTAGRAM COPY (A LOCK)
# ============================================================

def instagram_caption(rec):
    origin = strip(rec.get("origin_city"))
    dest = strip(rec.get("destination_city"))
    price = gbp(rec.get("price_gbp"))

    variants = [
        f"{origin} to {dest} for {price}. Solid value if you’re flexible.",
        f"{price} flights from {origin} to {dest}. This won’t hang around.",
        f"{origin} → {dest} at {price}. Decent timing, decent price.",
        f"Cheap flights like this don’t last. {price} to {dest}.",
    ]

    base = variants[hash(dest) % len(variants)]

    return (
        f"{base}\n"
        f"VIPs saw this yesterday.\n"
        f"Link in bio if you want early access."
    )

# ============================================================
# GOOGLE SHEETS
# ============================================================

def get_ws():
    creds = Credentials.from_service_account_info(
        json.loads(GCP_SA_JSON),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    client = gspread.authorize(creds)
    return client.open_by_key(SPREADSHEET_ID).worksheet(RAW_DEALS_TAB)

def headers_map(headers):
    return {h: i for i, h in enumerate(headers)}

# ============================================================
# RENDER STAGE (B LOCK)
# ============================================================

def render_image(rec):
    payload = {
        "deal_id": rec["deal_id"],
        "origin_city": rec["origin_city"],
        "destination_city": rec["destination_city"],
        "price_gbp": gbp(rec["price_gbp"]),
        "outbound_date": rec["outbound_date"],
        "return_date": rec["return_date"],
    }

    r = requests.post(RENDER_URL, json=payload, timeout=30)
    r.raise_for_status()
    return r.json().get("graphic_url")

# ============================================================
# INSTAGRAM
# ============================================================

def ig_post(image_url, caption):
    create = requests.post(
        f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media",
        data={
            "image_url": image_url,
            "caption": caption,
            "access_token": IG_ACCESS_TOKEN,
        },
        timeout=30,
    ).json()

    cid = create.get("id")
    if not cid:
        raise RuntimeError(create)

    publish = requests.post(
        f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media_publish",
        data={
            "creation_id": cid,
            "access_token": IG_ACCESS_TOKEN,
        },
        timeout=30,
    ).json()

    return publish.get("id")

# ============================================================
# TELEGRAM
# ============================================================

def tg_send(token, chat, text):
    r = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat, "text": text},
        timeout=30,
    )
    r.raise_for_status()

# ============================================================
# MAIN PIPELINE
# ============================================================

def main():
    ws = get_ws()
    rows = ws.get_all_records()
    headers = ws.row_values(1)
    hmap = headers_map(headers)

    for i, rec in enumerate(rows, start=2):
        status = strip(rec.get("status"))

        # ---------- RENDER ----------
        if status == STATUS_READY_TO_POST:
            log(f"Rendering row {i}")
            img = render_image(rec)
            ws.update_cell(i, hmap["graphic_url"] + 1, img)
            ws.update_cell(i, hmap["status"] + 1, STATUS_READY_TO_PUBLISH)
            continue

        # ---------- INSTAGRAM ----------
        if status == STATUS_READY_TO_PUBLISH:
            log(f"Posting IG row {i}")
            caption = instagram_caption(rec)
            ig_id = ig_post(rec["graphic_url"], caption)
            ws.update_cell(i, hmap["ig_media_id"] + 1, ig_id)
            ws.update_cell(i, hmap["status"] + 1, STATUS_POSTED_INSTAGRAM)
            continue

        # ---------- TELEGRAM VIP ----------
        if status == STATUS_POSTED_INSTAGRAM and RUN_SLOT == "AM":
            tg_send(
                TELEGRAM_BOT_TOKEN_VIP,
                TELEGRAM_VIP_CHANNEL,
                f"{gbp(rec['price_gbp'])} to {rec['destination_city']}\nVIP early access",
            )
            ws.update_cell(i, hmap["tg_vip_timestamp"] + 1, now())
            ws.update_cell(i, hmap["status"] + 1, STATUS_POSTED_TELEGRAM_VIP)
            continue

        # ---------- TELEGRAM FREE ----------
        if status == STATUS_POSTED_TELEGRAM_VIP and RUN_SLOT == "PM":
            if hours_since(rec.get("tg_vip_timestamp")) >= VIP_DELAY_HOURS:
                tg_send(
                    TELEGRAM_BOT_TOKEN,
                    TELEGRAM_FREE_CHANNEL,
                    f"{gbp(rec['price_gbp'])} to {rec['destination_city']}\nVIPs saw this first",
                )
                ws.update_cell(i, hmap["status"] + 1, STATUS_POSTED_ALL)

if __name__ == "__main__":
    main()
