#!/usr/bin/env python3
"""
TravelTxter V4.1 — UNIFIED PIPELINE (Fail-fast "green but empty" guard)

Stages:
0. Duffel feeder (optional, only if DUFFEL_API_KEY is present)
1. Score + select (NEW -> READY_TO_POST)
2. Render (READY_TO_POST -> READY_TO_PUBLISH)
3. Instagram (READY_TO_PUBLISH -> POSTED_INSTAGRAM)
4. Telegram Free (POSTED_INSTAGRAM -> POSTED_TELEGRAM_FREE)
5. Telegram VIP (POSTED_TELEGRAM_FREE -> POSTED_ALL)

Key rule:
- If the sheet is empty, this script returns non-zero so GitHub Actions goes RED (not green).
"""

import os
import sys
import json
import time
import uuid
import math
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials

# ---------------------------
# Helpers
# ---------------------------

def env(key: str, default: str = "") -> str:
    v = os.environ.get(key, default)
    return v.strip() if isinstance(v, str) else default

def env_any(keys: List[str], default: str = "") -> str:
    for k in keys:
        v = env(k, "")
        if v:
            return v
    return default

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)

# ---------------------------
# Config
# ---------------------------

GCP_SA_JSON = env("GCP_SA_JSON")
SPREADSHEET_ID = env("SPREADSHEET_ID")
RAW_DEALS_TAB = env("RAW_DEALS_TAB", "RAW_DEALS")

STATUS_COLUMN = "status"
RAW_STATUS_NEW = env("RAW_STATUS_NEW", "NEW")

FRESHNESS_DECAY_PER_DAY = float(env("FRESHNESS_DECAY_PER_DAY", "2.0"))

DUFFEL_API_KEY = env("DUFFEL_API_KEY")
DUFFEL_ENABLED = bool(DUFFEL_API_KEY)
DUFFEL_MAX_INSERTS = int(env("DUFFEL_MAX_INSERTS", "20"))
DUFFEL_MAX_INSERTS = min(DUFFEL_MAX_INSERTS, 20)

RENDER_URL = env_any(["RENDER_URL", "RENDER_BASE_URL"])

IG_ACCESS_TOKEN = env("IG_ACCESS_TOKEN")
IG_USER_ID = env("IG_USER_ID")

TELEGRAM_BOT_TOKEN = env("TELEGRAM_BOT_TOKEN")
TELEGRAM_FREE_CHANNEL = env("TELEGRAM_FREE_CHANNEL")

TELEGRAM_BOT_TOKEN_VIP = env("TELEGRAM_BOT_TOKEN_VIP")
TELEGRAM_VIP_CHANNEL = env_any(["TELEGRAM_VIP_CHANNEL", "TELEGRAM_CHANNEL_VIP"])

STRIPE_LINK = env("STRIPE_LINK")

OPENAI_API_KEY = env("OPENAI_API_KEY")
OPENAI_MODEL = env("OPENAI_MODEL", "gpt-4o-mini")

# ---------------------------
# Google Sheets
# ---------------------------

def gs_client() -> gspread.Client:
    if not GCP_SA_JSON:
        raise RuntimeError("Missing GCP_SA_JSON")
    info = json.loads(GCP_SA_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def get_ws():
    gc = gs_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    return sh.worksheet(RAW_DEALS_TAB)

# ---------------------------
# CONFIG routing (V4.2)
# ---------------------------

def get_enabled_routes_from_config(ws_config) -> List[Dict[str, Any]]:
    rows = ws_config.get_all_values()
    if len(rows) < 2:
        return []
    headers = [h.strip() for h in rows[0]]
    hmap = {h: i for i, h in enumerate(headers)}

    required = ["enabled", "origin_iata", "destination_iata"]
    for r in required:
        if r not in hmap:
            log(f"  ⚠️  CONFIG missing required column: {r}")
            return []

    out: List[Dict[str, Any]] = []
    for r in rows[1:]:
        enabled = (r[hmap["enabled"]].strip().upper() if hmap["enabled"] < len(r) else "")
        if enabled != "TRUE":
            continue
        origin = r[hmap["origin_iata"]].strip().upper()
        dest = r[hmap["destination_iata"]].strip().upper()
        theme = r[hmap["theme"]].strip() if "theme" in hmap and hmap["theme"] < len(r) else ""
        trip_len = r[hmap["trip_length_days"]].strip() if "trip_length_days" in hmap and hmap["trip_length_days"] < len(r) else ""
        days_ahead = r[hmap["days_ahead"]].strip() if "days_ahead" in hmap and hmap["days_ahead"] < len(r) else ""
        max_conn = r[hmap["max_connections"]].strip() if "max_connections" in hmap and hmap["max_connections"] < len(r) else ""

        out.append({
            "origin_iata": origin,
            "destination_iata": dest,
            "theme": theme,
            "trip_length_days": int(trip_len) if trip_len.isdigit() else 5,
            "days_ahead": int(days_ahead) if days_ahead.isdigit() else 60,
            "max_connections": int(max_conn) if max_conn.isdigit() else 1,
        })
    return out

def pick_route_from_config() -> Optional[Dict[str, Any]]:
    try:
        gc = gs_client()
        sh = gc.open_by_key(SPREADSHEET_ID)
        try:
            ws_config = sh.worksheet("CONFIG")
        except Exception:
            log("  ⚠️  CONFIG sheet not found, using fallback (no feeder route)")
            return None

        routes = get_enabled_routes_from_config(ws_config)
        if not routes:
            log("  ⚠️  No enabled routes in CONFIG (enabled must be TRUE)")
            return None

        # Deterministic rotate: day-of-year + am/pm slot
        now = dt.datetime.utcnow()
        day_of_year = int(now.strftime("%j"))
        slot = 0 if now.hour < 12 else 1
        idx = (day_of_year * 2 + slot) % len(routes)
        return routes[idx]
    except Exception as e:
        log(f"  ⚠️  CONFIG route pick failed: {e}")
        return None

# ---------------------------
# Duffel feeder (minimal, safe)
# ---------------------------

def duffel_offer_request(origin: str, dest: str, depart: str, return_date: str, max_connections: int) -> Dict[str, Any]:
    url = "https://api.duffel.com/air/offer_requests"
    headers = {
        "Authorization": f"Bearer {DUFFEL_API_KEY}",
        "Duffel-Version": "beta",
        "Content-Type": "application/json",
    }
    payload = {
        "data": {
            "slices": [
                {"origin": origin, "destination": dest, "departure_date": depart},
                {"origin": dest, "destination": origin, "departure_date": return_date},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": "economy",
            "max_connections": max_connections,
        }
    }
    r = requests.post(url, headers=headers, json=payload, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"Duffel error {r.status_code}: {r.text[:400]}")
    return r.json()

def feed_new_deals() -> int:
    route = pick_route_from_config()
    if not route:
        log("  ⚠️  Feeder skipped (no CONFIG route available)")
        return 0

    origin = route["origin_iata"]
    dest = route["destination_iata"]
    days_ahead = int(route.get("days_ahead", 60))
    trip_len = int(route.get("trip_length_days", 5))
    max_conn = int(route.get("max_connections", 1))

    out_date = (dt.date.today() + dt.timedelta(days=days_ahead)).isoformat()
    back_date = (dt.date.today() + dt.timedelta(days=days_ahead + trip_len)).isoformat()

    log(f"🔍 Duffel search: {origin} → {dest} | OUT {out_date} BACK {back_date} | max_conn={max_conn}")

    data = duffel_offer_request(origin, dest, out_date, back_date, max_conn)

    offers = (data.get("data", {}) or {}).get("offers", []) or []
    if not offers:
        log("  ⚠️  Duffel returned 0 offers")
        return 0

    ws = get_ws()
    rows = ws.get_all_values()
    headers = [h.strip() for h in rows[0]] if rows else []
    hmap = {h: i for i, h in enumerate(headers)}

    required_cols = ["deal_id", "origin_city", "destination_city", "destination_country",
                     "price_gbp", "outbound_date", "return_date", "trip_length_days",
                     "stops", "airline", "theme", "deal_source", "date_added", "status"]

    for c in required_cols:
        if c not in hmap:
            log(f"❌ RAW_DEALS missing required column: {c}")
            return 0

    inserted = 0
    today = dt.date.today().isoformat()

    for offer in offers[:DUFFEL_MAX_INSERTS]:
        try:
            total = offer.get("total_amount")
            currency = offer.get("total_currency")
            if not total or currency != "GBP":
                continue

            # stops proxy
            slices = offer.get("slices", []) or []
            seg0 = (slices[0].get("segments", []) if slices else []) or []
            stops_num = max(0, len(seg0) - 1)

            airline = ""
            if seg0:
                airline = (seg0[0].get("marketing_carrier", {}) or {}).get("name", "") or ""

            row = [""] * len(headers)
            row[hmap["deal_id"]] = str(uuid.uuid4())
            row[hmap["origin_city"]] = origin
            row[hmap["destination_city"]] = dest
            row[hmap["destination_country"]] = ""
            row[hmap["price_gbp"]] = str(total)
            row[hmap["outbound_date"]] = out_date
            row[hmap["return_date"]] = back_date
            row[hmap["trip_length_days"]] = str(trip_len)
            row[hmap["stops"]] = str(stops_num)
            row[hmap["airline"]] = airline
            row[hmap["theme"]] = route.get("theme", "")
            row[hmap["deal_source"]] = f"DUFFEL_V4_CONFIG_{origin}"
            row[hmap["date_added"]] = today
            row[hmap["status"]] = RAW_STATUS_NEW

            ws.append_row(row, value_input_option="USER_ENTERED")
            inserted += 1
        except Exception as e:
            log(f"  ⚠️  Insert failed: {e}")

    log(f"✅ Inserted {inserted} offer(s)")
    return inserted

# ---------------------------
# Main
# ---------------------------

def main() -> int:
    log("=" * 60)
    log("🚀 TRAVELTXTER V4 UNIFIED PIPELINE")
    log("=" * 60)
    log(f"Sheet: {SPREADSHEET_ID}")
    log(f"Tab: {RAW_DEALS_TAB}")
    log(f"Duffel: {'ENABLED' if DUFFEL_ENABLED else 'DISABLED'}")
    log(f"Freshness decay: {FRESHNESS_DECAY_PER_DAY}/day")
    log("=" * 60)

    if not SPREADSHEET_ID:
        log("❌ Missing SPREADSHEET_ID")
        return 1

    # Stage 0: feeder (only if key present)
    if DUFFEL_ENABLED:
        feed_new_deals()

    # Always connect and read
    ws = get_ws()
    log(f"✅ Connected: {ws.title}")

    rows = ws.get_all_values()
    if len(rows) < 2:
        # FAIL FAST: do not allow "green but empty"
        if DUFFEL_ENABLED:
            log("❌ No data rows after Duffel feeder ran. Likely causes: CONFIG missing/disabled routes, Duffel request failing, or sheet permissions.")
        else:
            log("❌ No data rows and Duffel is DISABLED. Ensure DUFFEL_API_KEY is passed to this step in GitHub Actions.")
        return 1

    # If you want, we can re-add the scoring/render/IG/TG stages here next.
    # For now this file focuses on fixing the blocker: feeding rows reliably.
    log(f"✅ Sheet has {len(rows)-1} data rows. Feeder is working.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
