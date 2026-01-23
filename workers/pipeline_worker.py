# ======================================================================
# TRAVELTXTTER — PIPELINE WORKER (FEEDER)
# FINAL NORMALISER — V4.7.x
#
# LOCKED BEHAVIOUR:
# - CONFIG is the brain
# - RCM provides geography (must exist)
# - Duffel dates required, <= 84 days
# - Core fields must exist or row is not inserted
# - Offer-derived fields ALWAYS written with safe placeholders
# ======================================================================

import os
import json
import time
import datetime as dt
from typing import Dict, Tuple, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials


# ----------------------------------------------------------------------
# ENV
# ----------------------------------------------------------------------

DUFFEL_API_KEY = os.getenv("DUFFEL_API_KEY")
DUFFEL_API_URL = "https://api.duffel.com/air/offer_requests"
DUFFEL_VERSION = "v2"

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID")
RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS")
CONFIG_TAB = os.getenv("CONFIG_TAB", "CONFIG")
RCM_TAB = os.getenv("RCM_TAB", "ROUTE_CAPABILITY_MAP")

MAX_SEARCHES_PER_RUN = int(os.getenv("DUFFEL_MAX_SEARCHES_PER_RUN", "12"))
FEEDER_SLEEP_SECONDS = float(os.getenv("FEEDER_SLEEP_SECONDS", "0.1"))

UTC_NOW = dt.datetime.utcnow()
UTC_TODAY = UTC_NOW.date()


# ----------------------------------------------------------------------
# LOGGING
# ----------------------------------------------------------------------

def log(msg: str):
    print(f"{dt.datetime.utcnow().isoformat()}Z | {msg}", flush=True)


# ----------------------------------------------------------------------
# GOOGLE SHEETS AUTH
# ----------------------------------------------------------------------

def get_gspread_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    info = json.loads(os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON"))
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


# ----------------------------------------------------------------------
# HELPERS
# ----------------------------------------------------------------------

def is_true(v) -> bool:
    return str(v).strip().upper() == "TRUE"

def safe_float(v) -> float:
    try:
        return float(str(v).replace(",", ""))
    except Exception:
        return 0.0


# ----------------------------------------------------------------------
# DATE GENERATION (DUFFEL VALID)
# ----------------------------------------------------------------------

def build_dates(cfg: Dict) -> Tuple[str, str]:
    min_days = int(cfg.get("days_ahead_min") or 14)
    max_days = min(int(cfg.get("days_ahead_max") or 84), 84)
    trip_len = int(cfg.get("trip_length_days") or 5)

    outbound = UTC_TODAY + dt.timedelta(days=min_days)
    inbound = outbound + dt.timedelta(days=trip_len)

    return outbound.isoformat(), inbound.isoformat()


# ----------------------------------------------------------------------
# DUFFEL SEARCH
# ----------------------------------------------------------------------

def duffel_search(origin, dest, cabin, out_date, ret_date):
    headers = {
        "Authorization": f"Bearer {DUFFEL_API_KEY}",
        "Duffel-Version": DUFFEL_VERSION,
        "Content-Type": "application/json",
    }

    payload = {
        "data": {
            "slices": [
                {"origin": origin, "destination": dest, "departure_date": out_date},
                {"origin": dest, "destination": origin, "departure_date": ret_date},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": cabin or "economy",
        }
    }

    r = requests.post(DUFFEL_API_URL, headers=headers, json=payload, timeout=30)
    if r.status_code >= 300:
        log(f"❌ Duffel error {r.status_code}: {r.text[:200]}")
        return []

    return (r.json().get("data") or {}).get("offers") or []


# ----------------------------------------------------------------------
# OFFER NORMALISATION
# ----------------------------------------------------------------------

def extract_date(seg: Dict) -> Optional[str]:
    for k in ("departing_at", "arriving_at"):
        if seg.get(k):
            return seg[k][:10]
    return None

def offer_dates(offer: Dict) -> Tuple[str, str]:
    slices = offer.get("slices") or []
    if len(slices) < 2:
        return "", ""

    out = slices[0].get("departure_date")
    if not out:
        segs = slices[0].get("segments") or []
        if segs:
            out = extract_date(segs[0])

    ret = slices[1].get("departure_date")
    if not ret:
        segs = slices[1].get("segments") or []
        if segs:
            ret = extract_date(segs[0])

    return out or "", ret or ""

def normalise_offer(offer: Dict) -> Dict:
    slices = offer.get("slices") or []
    segs_out = slices[0].get("segments") if slices else []
    segs_in = slices[1].get("segments") if len(slices) > 1 else []

    stops = max(len(segs_out) - 1, 0)

    carriers = sorted(
        {s.get("marketing_carrier", {}).get("iata_code") for s in (segs_out + segs_in) if s.get("marketing_carrier")}
    )

    return {
        "stops": stops,
        "bags_incl": int(offer.get("included_checked_bags", 0) or 0),
        "cabin_class": offer.get("cabin_class") or "na",
        "connection_type": "direct" if stops == 0 else "indirect",
        "outbound_duration_minutes": int(offer.get("total_duration_minutes") or 0),
        "inbound_duration_minutes": 0,
        "total_duration_hours": round((offer.get("total_duration_minutes") or 0) / 60, 2),
        "via_hub": segs_out[0].get("destination", {}).get("iata_code") if stops else "na",
        "carriers": ",".join(carriers) if carriers else "na",
        "currency": offer.get("total_currency") or "GBP",
    }


# ----------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------

def main():
    log("TRAVELTXTTER PIPELINE WORKER (FEEDER) START")

    gc = get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_cfg = sh.worksheet(CONFIG_TAB)
    ws_rcm = sh.worksheet(RCM_TAB)
    ws_raw = sh.worksheet(RAW_DEALS_TAB)

    config = [r for r in ws_cfg.get_all_records() if is_true(r.get("active_in_feeder"))]
    log(f"✅ CONFIG loaded: {len(config)} rows")

    rcm_rows = ws_rcm.get_all_records()
    rcm = {
        (r.get("origin_iata"), r.get("destination_iata")): r
        for r in rcm_rows
        if is_true(r.get("enabled"))
    }
    log(f"✅ RCM loaded: {len(rcm)} enabled routes")

    themes = sorted({
        (r.get("theme_of_day") or r.get("theme"))
        for r in config
        if (r.get("theme_of_day") or r.get("theme"))
    })
    theme = themes[UTC_TODAY.timetuple().tm_yday % len(themes)]
    log(f"🎯 Theme of the day (UTC): {theme}")

    headers = ws_raw.row_values(1)
    inserts = []
    searches = 0

    for cfg in config:
        if searches >= MAX_SEARCHES_PER_RUN:
            break

        if (cfg.get("theme_of_day") or cfg.get("theme")) != theme:
            continue

        origin = cfg.get("origin_iata")
        dest = cfg.get("destination_iata")
        key = (origin, dest)

        if key not in rcm:
            continue

        geo = rcm[key]
        if not all([
            geo.get("origin_city"),
            geo.get("origin_country"),
            geo.get("destination_city"),
            geo.get("destination_country"),
        ]):
            continue  # core geography must exist

        out_date, ret_date = build_dates(cfg)
        searches += 1

        offers = duffel_search(origin, dest, cfg.get("cabin_class"), out_date, ret_date)
        time.sleep(FEEDER_SLEEP_SECONDS)

        for offer in offers:
            deal_id = offer.get("id")
            price = safe_float(offer.get("total_amount"))
            out_s, ret_s = offer_dates(offer)

            if not all([deal_id, price, out_s, ret_s]):
                continue

            norm = normalise_offer(offer)

            row = [""] * len(headers)
            def put(k, v):
                if k in headers:
                    row[headers.index(k)] = v

            # core
            put("status", "NEW")
            put("deal_id", deal_id)
            put("price_gbp", price)
            put("origin_iata", origin)
            put("origin_city", geo.get("origin_city"))
            put("origin_country", geo.get("origin_country"))
            put("destination_iata", dest)
            put("destination_city", geo.get("destination_city"))
            put("destination_country", geo.get("destination_country"))
            put("outbound_date", out_s)
            put("return_date", ret_s)
            put("deal_theme", theme)
            put("theme", theme)
            put("ingested_at_utc", UTC_NOW.isoformat() + "Z")

            # offer-derived (always written)
            for k, v in norm.items():
                put(k, v)

            inserts.append(row)
            break

    if not inserts:
        log("⚠️ No winners to insert")
        return

    ws_raw.append_rows(inserts, value_input_option="USER_ENTERED")
    log(f"✅ Inserted {len(inserts)} deal(s)")


if __name__ == "__main__":
    main()
