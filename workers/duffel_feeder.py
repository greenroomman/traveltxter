#!/usr/bin/env python3
"""
TravelTxter — Duffel Feeder V4 (Affiliate-SAFE)

Goal:
- Insert deals from Duffel into RAW_DEALS
- Generate FREE + VIP booking links
- IMPORTANT: No Skyscanner affiliate approval required yet.
  If SKYSCANNER_AFFILIATE_ID is missing, VIP link == FREE link (non-affiliate).
  No errors.

Writes only columns that exist (header-map safe):
- booking_link_free
- booking_link_vip
- affiliate_source (only when affiliate ID exists)
- affiliate_url (back-compat, mirrors booking_link_vip if column exists)

Auth via env:
- GCP_SA_JSON
- SPREADSHEET_ID
"""

import os
import json
import time
import uuid
import logging
from datetime import date, timedelta
from typing import Dict, Any, List, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("duffel_feeder_v4_affiliate_safe")


# ========= ENV =========

DUFFEL_API_KEY = os.environ["DUFFEL_API_KEY"]

# Safe: optional
SKYSCANNER_AFFILIATE_ID = os.getenv("SKYSCANNER_AFFILIATE_ID", "").strip()

GCP_SA_JSON = os.environ["GCP_SA_JSON"]
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS").strip()

# Minimal route config via env (so it runs even if CONFIG changes)
ORIGIN_IATA = os.getenv("ORIGIN_IATA", "LON").strip().upper()
DEST_IATA = os.getenv("DEST_IATA", "BCN").strip().upper()
DAYS_AHEAD = int(os.getenv("DAYS_AHEAD", "60"))
TRIP_LENGTH_DAYS = int(os.getenv("TRIP_LENGTH_DAYS", "5"))
CABIN_CLASS = os.getenv("CABIN_CLASS", "economy").strip().lower()
MAX_CONNECTIONS = int(os.getenv("MAX_CONNECTIONS", "1"))

MAX_INSERTS = int(os.getenv("MAX_INSERTS", "3"))
SLEEP_SECONDS = float(os.getenv("SLEEP_SECONDS", "0.6"))

RAW_STATUS_NEW = os.getenv("RAW_STATUS_NEW", "NEW").strip().upper()

DUFFEL_BASE_URL = "https://api.duffel.com/air"
DUFFEL_VERSION = os.getenv("DUFFEL_VERSION", "v2").strip()


# ========= SHEETS =========

def env_client() -> gspread.Client:
    info = json.loads(GCP_SA_JSON)
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def ws():
    gc = env_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    return sh.worksheet(RAW_DEALS_TAB)

def build_row(headers: List[str], idx: Dict[str, int], item: Dict[str, Any]) -> List[str]:
    row = [""] * len(headers)
    for k, v in item.items():
        if k in idx:
            row[idx[k]] = "" if v is None else str(v)
    return row


# ========= LINKS =========

def _fmt_yymmdd(iso_yyyy_mm_dd: str) -> str:
    y, m, d = iso_yyyy_mm_dd.split("-")
    return f"{y[2:]}{m}{d}"

def skyscanner_link(origin_iata: str, dest_iata: str, out_date: str, ret_date: str, include_ref: bool) -> str:
    """
    Skyscanner deep link.

    SAFE behaviour:
    - If include_ref True but SKYSCANNER_AFFILIATE_ID missing, returns non-affiliate link.
    - No errors.
    """
    origin = origin_iata.lower()
    dest = dest_iata.lower()
    out = _fmt_yymmdd(out_date)
    ret = _fmt_yymmdd(ret_date)

    cabin_map = {
        "economy": "economy",
        "premium_economy": "premiumeconomy",
        "business": "business",
        "first": "first",
    }
    cabin_param = cabin_map.get(CABIN_CLASS, "economy")

    base = f"https://www.skyscanner.com/transport/flights/{origin}/{dest}/{out}/{ret}/?adultsv2=1&cabinclass={cabin_param}"

    if include_ref and SKYSCANNER_AFFILIATE_ID:
        return base + f"&ref={SKYSCANNER_AFFILIATE_ID}"
    return base


# ========= DUFFEL =========

def duffel_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {DUFFEL_API_KEY}",
        "Duffel-Version": DUFFEL_VERSION,
        "Content-Type": "application/json",
    }

def search_roundtrip(origin: str, destination: str, outbound_date: date, return_date: date) -> List[Dict[str, Any]]:
    payload = {
        "data": {
            "slices": [
                {"origin": origin, "destination": destination, "departure_date": outbound_date.isoformat()},
                {"origin": destination, "destination": origin, "departure_date": return_date.isoformat()},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": CABIN_CLASS,
            "max_connections": MAX_CONNECTIONS,
        }
    }

    r = requests.post(f"{DUFFEL_BASE_URL}/offer_requests", headers=duffel_headers(), json=payload, timeout=30)
    r.raise_for_status()
    offer_request_id = r.json()["data"]["id"]

    offers = requests.get(
        f"{DUFFEL_BASE_URL}/offers?offer_request_id={offer_request_id}&limit=20",
        headers=duffel_headers(),
        timeout=30,
    )
    offers.raise_for_status()
    return offers.json()["data"]


# ========= PARSE =========

def parse_offer(offer: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        slices = offer.get("slices", [])
        if len(slices) < 2:
            return None

        s0, s1 = slices[0], slices[1]
        seg0, seg1 = s0.get("segments", []), s1.get("segments", [])
        if not seg0 or not seg1:
            return None

        out_date = seg0[0]["departing_at"][:10]
        ret_date = seg1[0]["departing_at"][:10]

        outbound_stops = max(0, len(seg0) - 1)
        return_stops = max(0, len(seg1) - 1)
        stops = str(outbound_stops + return_stops)

        airline = (offer.get("owner") or {}).get("name", "") or ""
        price = offer.get("total_amount", "")

        booking_free = skyscanner_link(ORIGIN_IATA, DEST_IATA, out_date, ret_date, include_ref=False)
        booking_vip = skyscanner_link(ORIGIN_IATA, DEST_IATA, out_date, ret_date, include_ref=True)

        item = {
            "deal_id": str(uuid.uuid4()),
            "origin_city": ORIGIN_IATA,
            "destination_city": DEST_IATA,
            "destination_country": "",
            "price_gbp": price,
            "outbound_date": out_date,
            "return_date": ret_date,
            "trip_length_days": str(TRIP_LENGTH_DAYS),
            "stops": stops,
            "baggage_included": "",
            "airline": airline,
            "deal_source": "DUFFEL_V4",
            "notes": f"{ORIGIN_IATA}→{DEST_IATA}",
            "date_added": date.today().isoformat(),
            "status": RAW_STATUS_NEW,

            # V4 monetisation fields (written only if columns exist)
            "booking_link_free": booking_free,
            "booking_link_vip": booking_vip,
            "affiliate_source": "skyscanner" if SKYSCANNER_AFFILIATE_ID else "",
        }

        # Back-compat (if old column exists)
        item["affiliate_url"] = booking_vip

        return item
    except Exception as e:
        log.warning(f"parse_offer failed: {e}")
        return None


def main():
    w = ws()
    headers = w.row_values(1)
    if not headers:
        raise RuntimeError("RAW_DEALS missing headers")

    idx = {h: i for i, h in enumerate(headers)}

    out = date.today() + timedelta(days=DAYS_AHEAD)
    ret = out + timedelta(days=TRIP_LENGTH_DAYS)

    log.info(f"Searching {ORIGIN_IATA}->{DEST_IATA} {out.isoformat()} to {ret.isoformat()}")
    offers = search_roundtrip(ORIGIN_IATA, DEST_IATA, out, ret)

    batch = []
    inserts = 0

    for offer in offers:
        if inserts >= MAX_INSERTS:
            break
        item = parse_offer(offer)
        if not item:
            continue
        batch.append(build_row(headers, idx, item))
        inserts += 1

    if batch:
        w.append_rows(batch, value_input_option="USER_ENTERED")
        log.info(f"✅ Inserted {len(batch)} rows")
    else:
        log.info("ℹ️ No rows inserted")

    time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    main()
