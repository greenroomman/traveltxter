#!/usr/bin/env python3
"""
TravelTxter V4.5x — Pipeline Worker (FEEDER + DISCOVERY BANK)

ROLE:
- Query Duffel
- Insert publish-eligible deals into RAW_DEALS as NEW
- Insert non-eligible but interesting deals into DISCOVERY_BANK
- NEVER publishes
- NEVER scores
- NEVER mutates CONFIG / THEMES

Discovery is observation-only.
"""

from __future__ import annotations

import os
import json
import time
import uuid
import random
import datetime as dt
from typing import Dict, List

import requests
import gspread
from google.oauth2.service_account import Credentials

# ============================================================
# Logging
# ============================================================

def log(msg: str):
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)

# ============================================================
# Auth / Sheets
# ============================================================

def get_gspread_client():
    raw = os.environ["GCP_SA_JSON_ONE_LINE"]
    info = json.loads(raw.replace("\\n", "\n"))
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return gspread.authorize(creds)

def get_sheet(tab: str):
    gc = get_gspread_client()
    sh = gc.open_by_key(os.environ["SPREADSHEET_ID"])
    return sh.worksheet(tab)

def ensure_headers(ws, headers: List[str]):
    existing = ws.row_values(1)
    if existing == headers:
        return
    ws.clear()
    ws.update([headers], "A1")
    log(f"🛠️ Initialised {ws.title} headers")

# ============================================================
# Duffel
# ============================================================

DUFFEL_API = "https://api.duffel.com/air/offer_requests"

def duffel_headers():
    return {
        "Authorization": f"Bearer {os.environ['DUFFEL_API_KEY']}",
        "Duffel-Version": "v2",
        "Content-Type": "application/json",
    }

def duffel_search(origin, dest, out_date, ret_date):
    payload = {
        "data": {
            "slices": [
                {"origin": origin, "destination": dest, "departure_date": out_date},
                {"origin": dest, "destination": origin, "departure_date": ret_date},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": "economy",
        }
    }
    r = requests.post(DUFFEL_API, headers=duffel_headers(), json=payload, timeout=40)
    r.raise_for_status()
    return r.json()["data"]["offers"]

# ============================================================
# Helpers
# ============================================================

def today():
    return dt.datetime.utcnow().date()

def pick_dates():
    out = today() + dt.timedelta(days=random.randint(21, 50))
    ret = out + dt.timedelta(days=random.randint(3, 7))
    return out.isoformat(), ret.isoformat()

def norm(s: str) -> str:
    return (s or "").strip().upper().replace(" ", "_")

# ============================================================
# Main
# ============================================================

def main():
    raw_ws = get_sheet(os.environ.get("RAW_DEALS_TAB", "RAW_DEALS"))
    disc_ws = get_sheet("DISCOVERY_BANK")

    RAW_HEADERS = [
        "status","deal_id","price_gbp","origin_iata","destination_iata",
        "origin_city","destination_city","destination_country",
        "outbound_date","return_date","stops","deal_theme","created_utc"
    ]

    DISC_HEADERS = [
        "found_at_utc","origin_iata","destination_iata",
        "destination_city","destination_country",
        "outbound_date","return_date",
        "price","currency","stops","carrier_codes",
        "raw_theme_guess","reason_flag","search_context"
    ]

    ensure_headers(raw_ws, RAW_HEADERS)
    ensure_headers(disc_ws, DISC_HEADERS)

    # --- CONFIG snapshots (read-only) ---
    config_routes = {(r["origin_iata"], r["destination_iata"]) for r in raw_ws.get_all_records()}

    # --- Run context ---
    run_id = uuid.uuid4().hex[:8]
    theme = "CITY_BREAK"
    origins = ["LON","MAN","BRS"]

    inserted = 0
    banked = 0

    for origin in origins:
        dest = random.choice(["BCN","LIS","FCO","AMS","PRG","BUD"])
        out_date, ret_date = pick_dates()

        log(f"✈️ Searching {origin}->{dest}")

        try:
            offers = duffel_search(origin, dest, out_date, ret_date)
        except Exception as e:
            log(f"❌ Duffel error: {e}")
            continue

        for off in offers[:3]:
            price = float(off["total_amount"])
            currency = off["total_currency"]
            carriers = ",".join({s["operating_carrier"]["iata_code"]
                                for sl in off["slices"]
                                for s in sl["segments"]})

            route_key = (origin, dest)

            # ---- PUBLISH CHECKS ----
            if route_key not in config_routes:
                reason = "outside_config"
            elif currency != "GBP":
                reason = "non_gbp"
            elif price > 300:
                reason = "too_expensive"
            else:
                # publish
                raw_ws.append_row([
                    "NEW", uuid.uuid4().hex[:12], price,
                    origin, dest,
                    origin, dest, "",
                    out_date, ret_date,
                    len(off["slices"][0]["segments"]) - 1,
                    theme,
                    dt.datetime.utcnow().isoformat()
                ])
                inserted += 1
                continue

            # ---- DISCOVERY BANK ----
            disc_ws.append_row([
                dt.datetime.utcnow().isoformat(),
                origin, dest,
                dest, "",
                out_date, ret_date,
                price, currency,
                len(off["slices"][0]["segments"]) - 1,
                carriers,
                theme,
                reason,
                f"run:{run_id}"
            ])
            banked += 1

    log(f"Done. published={inserted} banked={banked}")

if __name__ == "__main__":
    main()
