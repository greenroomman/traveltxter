#!/usr/bin/env python3
"""
TravelTxter V4.1 — Duffel Feeder (Affiliate-SAFE, Free-Tier Safe)

Key rules:
- Hard-capped inserts (MAX_INSERTS default 3) for Duffel free tier
- Writes ONLY columns that exist (header-map safe)
- Uses ONE canonical status column: 'status'
- Does not require Skyscanner approval: VIP link falls back safely

Secrets compatibility:
- SPREADSHEET_ID or SHEET_ID
- GCP_SA_JSON or GCP_SA_JSON_ONE_LINE
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
log = logging.getLogger("duffel_feeder_v4_1")


def env(key: str, default: str = "") -> str:
    return (os.getenv(key) or default).strip()


def env_any(keys: List[str], default: str = "") -> str:
    for k in keys:
        v = env(k)
        if v:
            return v
    return default


# =========================
# REQUIRED
# =========================
DUFFEL_API_KEY = env("DUFFEL_API_KEY")
if not DUFFEL_API_KEY:
    raise RuntimeError("Missing DUFFEL_API_KEY")

GCP_SA_JSON = env_any(["GCP_SA_JSON", "GCP_SA_JSON_ONE_LINE"])
if not GCP_SA_JSON:
    raise RuntimeError("Missing GCP_SA_JSON (or GCP_SA_JSON_ONE_LINE)")

SPREADSHEET_ID = env_any(["SPREADSHEET_ID", "SHEET_ID"])
if not SPREADSHEET_ID:
    raise RuntimeError("Missing SPREADSHEET_ID (or SHEET_ID)")


# =========================
# OPTIONAL
# =========================
RAW_DEALS_TAB = env("RAW_DEALS_TAB", "RAW_DEALS")

SKYSCANNER_AFFILIATE_ID = env("SKYSCANNER_AFFILIATE_ID")  # optional

ORIGIN_IATA = env("ORIGIN_IATA", "LON").upper()
DEST_IATA = env("DEST_IATA", "BCN").upper()

DAYS_AHEAD = int(env("DAYS_AHEAD", "60"))
TRIP_LENGTH_DAYS = int(env("TRIP_LENGTH_DAYS", "5"))

CABIN_CLASS = env("CABIN_CLASS", "economy").lower()
MAX_CONNECTIONS = int(env("MAX_CONNECTIONS", "1"))

MAX_INSERTS = int(env("MAX_INSERTS", "3"))           # Duffel free tier safety
SLEEP_SECONDS = float(env("SLEEP_SECONDS", "0.8"))   # polite spacing

RAW_STATUS_NEW = env("RAW_STATUS_NEW", "NEW").upper()
DUFFEL_VERSION = env("DUFFEL_VERSION", "v2")
DUFFEL_BASE_URL = "https://api.duffel.com/air"


# =========================
# SHEETS
# =========================
def gs_client() -> gspread.Client:
    info = json.loads(GCP_SA_JSON)
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def get_ws():
    gc = gs_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    return sh.worksheet(RAW_DEALS_TAB)


def build_row(headers: List[str], idx: Dict[str, int], item: Dict[str, Any]) -> List[str]:
    row = [""] * len(headers)
    for k, v in item.items():
        if k in idx:
            row[idx[k]] = "" if v is None else str(v)
    return row


# =========================
# LINKS (Skyscanner deep-link)
# =========================
def _fmt_yymmdd(iso_yyyy_mm_dd: str) -> str:
    y, m, d = iso_yyyy_mm_dd.split("-")
    return f"{y[2:]}{m}{d}"


def skyscanner_link(origin_iata: str, dest_iata: str, out_date: str, ret_date: str, include_ref: bool) -> str:
    """
    SAFE:
    - If include_ref=True but SKYSCANNER_AFFILIATE_ID missing, returns non-affiliate link.
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

    base = (
        f"https://www.skyscanner.com/transport/flights/"
        f"{origin}/{dest}/{out}/{ret}/?adultsv2=1&cabinclass={cabin_param}"
    )

    if include_ref and SKYSCANNER_AFFILIATE_ID:
        return base + f"&ref={SKYSCANNER_AFFILIATE_ID}"
    return base


# =========================
# DUFFEL
# =========================
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
            "deal_source": "DUFFEL_V4_1",
            "notes": f"{ORIGIN_IATA}→{DEST_IATA}",
            "date_added": date.today().isoformat(),
            "status": RAW_STATUS_NEW,

            # Monetisation-safe fields (only written if columns exist)
            "booking_link_free": booking_free,
            "booking_link_vip": booking_vip,
            "affiliate_source": "skyscanner" if SKYSCANNER_AFFILIATE_ID else "",
            "affiliate_url": booking_vip,  # back-compat
        }
        return item

    except Exception as e:
        log.warning(f"parse_offer failed: {e}")
        return None


def main() -> int:
    ws = get_ws()
    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("RAW_DEALS missing headers")

    idx = {h: i for i, h in enumerate(headers)}

    out = date.today() + timedelta(days=DAYS_AHEAD)
    ret = out + timedelta(days=TRIP_LENGTH_DAYS)

    log.info(f"V4.1 Feeder searching {ORIGIN_IATA}->{DEST_IATA} {out.isoformat()} to {ret.isoformat()}")
    offers = search_roundtrip(ORIGIN_IATA, DEST_IATA, out, ret)

    batch: List[List[str]] = []
    inserted = 0

    for offer in offers:
        if inserted >= MAX_INSERTS:
            break
        item = parse_offer(offer)
        if not item:
            continue
        batch.append(build_row(headers, idx, item))
        inserted += 1

    if batch:
        ws.append_rows(batch, value_input_option="USER_ENTERED")
        log.info(f"✅ Inserted {len(batch)} row(s)")
    else:
        log.info("ℹ️ No rows inserted")

    time.sleep(SLEEP_SECONDS)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
