# workers/pipeline_worker.py
from __future__ import annotations

import os
import sys
import json
import time
import uuid
import random
from datetime import datetime, timedelta, timezone

import gspread
import requests
from google.oauth2.service_account import Credentials

# ============================================================
# CONFIG
# ============================================================

RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS")
FEEDER_CONFIG_TAB = os.getenv("FEEDER_CONFIG_TAB", "CONFIG")
ROUTE_MAP_TAB = "ROUTE_CAPABILITY_MAP"
IATA_MASTER_TAB = "IATA_MASTER"

DUFFEL_API_KEY = os.getenv("DUFFEL_API_KEY")
DUFFEL_URL = "https://api.duffel.com/air/offer_requests"

MAX_INSERTS = int(os.getenv("DUFFEL_MAX_INSERTS", "50"))
MAX_SEARCHES = int(os.getenv("DUFFEL_MAX_SEARCHES_PER_RUN", "12"))

UTC = timezone.utc
NOW = datetime.now(UTC)

# ============================================================
# GOOGLE SHEETS
# ============================================================

def get_gspread():
    if os.getenv("GCP_SA_JSON_ONE_LINE"):
        creds_info = json.loads(os.getenv("GCP_SA_JSON_ONE_LINE"))
    else:
        creds_info = json.loads(os.getenv("GCP_SA_JSON"))

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)

gc = get_gspread()
sh = gc.open_by_key(os.getenv("SPREADSHEET_ID"))

raw_ws = sh.worksheet(RAW_DEALS_TAB)
config_ws = sh.worksheet(FEEDER_CONFIG_TAB)
route_ws = sh.worksheet(ROUTE_MAP_TAB)
iata_ws = sh.worksheet(IATA_MASTER_TAB)

# ============================================================
# LOAD LOOKUPS
# ============================================================

def load_iata_geo():
    rows = iata_ws.get_all_records()
    return {
        r["iata"]: {
            "city": r.get("city"),
            "country": r.get("country")
        }
        for r in rows
        if r.get("iata")
    }

IATA_GEO = load_iata_geo()

ROUTE_MAP = {
    (r["origin_iata"], r["destination_iata"]): r
    for r in route_ws.get_all_records()
    if r.get("enabled") is True
}

CONFIG_ROWS = [
    r for r in config_ws.get_all_records()
    if r.get("enabled") is True and r.get("active_in_feeder") is True
]

# ============================================================
# DUFFEL SEARCH
# ============================================================

def duffel_search(origin, dest, out_date, ret_date, max_connections):
    headers = {
        "Authorization": f"Bearer {DUFFEL_API_KEY}",
        "Duffel-Version": "v2",
        "Content-Type": "application/json"
    }

    payload = {
        "data": {
            "slices": [
                {"origin": origin, "destination": dest, "departure_date": out_date},
                {"origin": dest, "destination": origin, "departure_date": ret_date},
            ],
            "passengers": [{"type": "adult"}],
            "max_connections": max_connections,
            "cabin_class": "economy"
        }
    }

    r = requests.post(DUFFEL_URL, headers=headers, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()["data"]["offers"]

# ============================================================
# DEAL NORMALISATION (CRITICAL FIX)
# ============================================================

def build_raw_deal(offer, origin, dest, theme):
    price_gbp = round(float(offer["total_amount"]), 0)

    out_seg = offer["slices"][0]["segments"]
    in_seg = offer["slices"][1]["segments"]

    out_date = out_seg[0]["departing_at"][:10]
    ret_date = in_seg[-1]["arriving_at"][:10]

    origin_geo = IATA_GEO.get(origin, {})
    dest_geo = IATA_GEO.get(dest, {})

    return {
        "status": "NEW",
        "deal_id": str(uuid.uuid4()),
        "price_gbp": price_gbp,
        "origin_city": origin_geo.get("city"),
        "origin_country": origin_geo.get("country"),
        "origin_iata": origin,
        "destination_city": dest_geo.get("city"),
        "destination_country": dest_geo.get("country"),
        "destination_iata": dest,
        "outbound_date": out_date,
        "return_date": ret_date,
        "stops": len(out_seg) - 1,
        "deal_theme": theme,
        "ingested_at_utc": NOW.isoformat(),
        "cabin_class": "ECONOMY",
        "connection_type": "direct" if len(out_seg) == 1 else "connecting",
        "carriers": ",".join(
            {seg["marketing_carrier"]["iata_code"] for seg in out_seg}
        ),
        "trip_length_days": (
            datetime.fromisoformat(ret_date) -
            datetime.fromisoformat(out_date)
        ).days,
        "currency": "GBP",
        "created_utc": NOW.isoformat(),
        "timestamp": NOW.isoformat()
    }

# ============================================================
# MAIN
# ============================================================

print("============================================================")
print("TRAVELTXTTER PIPELINE WORKER (FEEDER) START")
print("============================================================")

inserted = 0
searches = 0
rows_to_insert = []

for cfg in CONFIG_ROWS:
    if searches >= MAX_SEARCHES or inserted >= MAX_INSERTS:
        break

    origin = cfg["origin_iata"]
    dest = cfg["destination_iata"]

    if (origin, dest) not in ROUTE_MAP:
        continue

    days_ahead = random.randint(
        int(cfg["days_ahead_min"]),
        int(cfg["days_ahead_max"])
    )
    trip_len = int(cfg["trip_length_days"])

    out_date = (NOW + timedelta(days=days_ahead)).date().isoformat()
    ret_date = (NOW + timedelta(days=days_ahead + trip_len)).date().isoformat()

    offers = duffel_search(
        origin,
        dest,
        out_date,
        ret_date,
        int(cfg["max_connections"])
    )

    searches += 1

    for offer in offers:
        if inserted >= MAX_INSERTS:
            break

        row = build_raw_deal(
            offer,
            origin,
            dest,
            cfg["primary_theme"]
        )
        rows_to_insert.append(row)
        inserted += 1

# ============================================================
# WRITE TO SHEET
# ============================================================

if rows_to_insert:
    headers = raw_ws.row_values(1)
    values = [
        [row.get(h, "") for h in headers]
        for row in rows_to_insert
    ]
    raw_ws.append_rows(values, value_input_option="USER_ENTERED")

print(f"✓ Searches completed: {searches}")
print(f"✓ Deals inserted: {len(rows_to_insert)}")
print("FEEDER COMPLETE")
