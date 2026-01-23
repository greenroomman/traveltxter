# ======================================================================
# TRAVELTXTTER — PIPELINE WORKER (FEEDER)
# FULL FILE REPLACEMENT — V4.7.x
#
# LOCKED RULES:
# - Google Sheets is the single source of truth
# - CONFIG is the canonical brain (no CONFIG_SIGNALS)
# - Full file replacements only
# - No stub inserts (deal_id, price, outbound_date, return_date required)
# - RCM enabled gating uses TEXT "TRUE"
# - Duffel API pinned to v2
# ======================================================================

import os
import sys
import json
import time
import math
import random
import datetime as dt
from typing import Dict, List, Tuple, Optional

import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials


# ----------------------------------------------------------------------
# ENV / CONSTANTS
# ----------------------------------------------------------------------

DUFFEL_API_KEY = os.getenv("DUFFEL_API_KEY")
DUFFEL_API_URL = "https://api.duffel.com/air/offer_requests"
DUFFEL_VERSION = "v2"

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID")
RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS")
CONFIG_TAB = os.getenv("CONFIG_TAB", "CONFIG")
RCM_TAB = os.getenv("RCM_TAB", "ROUTE_CAPABILITY_MAP")

MIN_SLEEP_SECONDS = 0.2
MAX_SEARCHES_PER_RUN = int(os.getenv("DUFFEL_MAX_SEARCHES_PER_RUN", "4"))

PRICE_GATE_MODE = os.getenv("PRICE_GATE_FALLBACK_BEHAVIOR", "BLOCK")  # BLOCK or SCORE

UTC_NOW = dt.datetime.utcnow()

REQUIRED_FIELDS = [
    "deal_id",
    "price_gbp",
    "outbound_date",
    "return_date",
]

# ----------------------------------------------------------------------
# LOGGING
# ----------------------------------------------------------------------

def log(msg: str):
    ts = dt.datetime.utcnow().isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)

def log_block(title: str):
    log("=" * 77)
    log(title)
    log("=" * 77)

# ----------------------------------------------------------------------
# GOOGLE SHEETS
# ----------------------------------------------------------------------

def get_gspread_client():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    if os.getenv("GCP_SA_JSON_ONE_LINE"):
        creds_dict = json.loads(os.getenv("GCP_SA_JSON_ONE_LINE"))
    else:
        creds_dict = json.loads(os.getenv("GCP_SA_JSON"))
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

# ----------------------------------------------------------------------
# HELPERS
# ----------------------------------------------------------------------

def is_true(val) -> bool:
    return str(val).strip().upper() == "TRUE"

def safe_float(val) -> Optional[float]:
    try:
        return float(val)
    except Exception:
        return None

def parse_iso_duration(val: str) -> Optional[int]:
    if not val or not isinstance(val, str):
        return None
    try:
        hours = minutes = 0
        v = val.replace("PT", "")
        if "H" in v:
            hours, v = v.split("H", 1)
            hours = int(hours)
        if "M" in v:
            minutes = int(v.replace("M", ""))
        return hours * 60 + minutes
    except Exception:
        return None

# ----------------------------------------------------------------------
# DUFFEL DATE EXTRACTION (CRITICAL FIX)
# ----------------------------------------------------------------------

def extract_date_from_segment(seg: Dict) -> Optional[str]:
    for k in ("departing_at", "arriving_at"):
        if seg.get(k):
            return seg[k][:10]
    return None

def offer_dates(offer: Dict) -> Tuple[str, str]:
    """
    Robust date extraction:
    1. slice.departure_date
    2. segment.departing_at fallback
    """
    slices = offer.get("slices") or []
    if len(slices) < 2:
        return "", ""

    # outbound
    out = slices[0].get("departure_date")
    if not out:
        segs = slices[0].get("segments") or []
        if segs:
            out = extract_date_from_segment(segs[0])

    # return
    ret = slices[1].get("departure_date")
    if not ret:
        segs = slices[1].get("segments") or []
        if segs:
            ret = extract_date_from_segment(segs[0])

    return (out or "", ret or "")

# ----------------------------------------------------------------------
# LOAD CONFIG
# ----------------------------------------------------------------------

def load_config(ws) -> List[Dict]:
    rows = ws.get_all_records()
    active = [r for r in rows if is_true(r.get("active_in_feeder"))]
    log(f"✅ CONFIG loaded: {len(active)} active rows (of {len(rows)} total)")
    return active

def build_theme_pool(config_rows: List[Dict]) -> Dict[str, int]:
    pool = {}
    for r in config_rows:
        theme = r.get("theme_of_day") or r.get("theme")
        if theme:
            pool[theme] = pool.get(theme, 0) + 1
    return pool

def select_theme(theme_pool: Dict[str, int]) -> str:
    if not theme_pool:
        raise RuntimeError("No themes available in CONFIG")
    themes = sorted(theme_pool.keys())
    idx = UTC_NOW.timetuple().tm_yday % len(themes)
    return themes[idx]

# ----------------------------------------------------------------------
# LOAD RCM
# ----------------------------------------------------------------------

def load_rcm(ws) -> Dict[Tuple[str, str], Dict]:
    rows = ws.get_all_records()
    enabled = {}
    for r in rows:
        if is_true(r.get("enabled")):
            key = (r.get("origin_iata"), r.get("destination_iata"))
            enabled[key] = r
    log(f"✅ ROUTE_CAPABILITY_MAP loaded: {len(enabled)} enabled routes")
    return enabled

# ----------------------------------------------------------------------
# DUFFEL SEARCH
# ----------------------------------------------------------------------

def duffel_offer_request(origin, dest, cfg) -> Optional[List[Dict]]:
    headers = {
        "Authorization": f"Bearer {DUFFEL_API_KEY}",
        "Duffel-Version": DUFFEL_VERSION,
        "Content-Type": "application/json",
    }

    payload = {
        "data": {
            "slices": [
                {"origin": origin, "destination": dest},
                {"origin": dest, "destination": origin},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": cfg.get("cabin_class") or "economy",
        }
    }

    try:
        r = requests.post(DUFFEL_API_URL, headers=headers, json=payload, timeout=30)
    except Exception as e:
        log(f"❌ Duffel request exception: {e}")
        return None

    if r.status_code >= 300:
        log(f"❌ Duffel offer_request failed {r.status_code}: {r.text[:300]}")
        return None

    data = r.json().get("data") or {}
    offers = data.get("offers") or []
    return offers

# ----------------------------------------------------------------------
# MAIN FEEDER
# ----------------------------------------------------------------------

def main():
    log_block("TRAVELTXTTER PIPELINE WORKER (FEEDER) START")

    if not DUFFEL_API_KEY:
        log("❌ DUFFEL_API_KEY missing")
        return

    gc = get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_config = sh.worksheet(CONFIG_TAB)
    ws_rcm = sh.worksheet(RCM_TAB)
    ws_raw = sh.worksheet(RAW_DEALS_TAB)

    config_rows = load_config(ws_config)
    rcm = load_rcm(ws_rcm)

    theme_pool = build_theme_pool(config_rows)
    log(f"🧠 Theme pool mode: theme_of_day | pool_size={len(theme_pool)} | " +
        ", ".join(f"{k}:{v}" for k, v in sorted(theme_pool.items())))

    theme = select_theme(theme_pool)
    log(f"🎯 Theme of the day (UTC): {theme}")

    searches = 0
    inserts = []

    for cfg in config_rows:
        if searches >= MAX_SEARCHES_PER_RUN:
            break

        cfg_theme = cfg.get("theme_of_day") or cfg.get("theme")
        if cfg_theme != theme:
            continue

        origin = cfg.get("origin_iata")
        dest = cfg.get("destination_iata")
        if not origin or not dest:
            continue

        if (origin, dest) not in rcm:
            continue

        searches += 1
        offers = duffel_offer_request(origin, dest, cfg)
        time.sleep(MIN_SLEEP_SECONDS)

        if not offers:
            continue

        for offer in offers:
            deal_id = offer.get("id")
            price = safe_float((offer.get("total_amount") or "0").replace(",", ""))
            out_s, ret_s = offer_dates(offer)

            if not deal_id or not price or not out_s or not ret_s:
                continue

            inserts.append({
                "status": "NEW",
                "deal_id": deal_id,
                "price_gbp": price,
                "origin_iata": origin,
                "destination_iata": dest,
                "outbound_date": out_s,
                "return_date": ret_s,
                "ingested_at_utc": UTC_NOW.isoformat() + "Z",
                "theme": theme,
            })
            break

    if not inserts:
        log("⚠️ No winners to insert")
        return

    headers = ws_raw.row_values(1)
    rows = []
    for ins in inserts:
        row = [""] * len(headers)
        for k, v in ins.items():
            if k in headers:
                row[headers.index(k)] = v
        rows.append(row)

    ws_raw.append_rows(rows, value_input_option="USER_ENTERED")
    log(f"✅ Inserted {len(rows)} new deal(s)")

# ----------------------------------------------------------------------

if __name__ == "__main__":
    main()
