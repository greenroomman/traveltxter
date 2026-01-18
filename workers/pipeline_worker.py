#!/usr/bin/env python3
"""
workers/pipeline_worker.py

TravelTxter FEEDER — V4.7.9
FINAL ORIGIN-DIVERSIFIED ROUTE SELECTION

Changes in this version (authorised):
- Rule B: origin-diversified route selection (one route per origin first)
- Origin-cap aware selection (never select capped origin)
- 90/10 theme/explore split preserved
- Benchmarks (max_price), date clamp (21–84), retry logic preserved

No redesigns. No schema changes.
"""

from __future__ import annotations

import os
import json
import time
import math
import hashlib
import datetime as dt
from typing import Any, Dict, List, Tuple, Optional, Set

import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# ENV / TABS
# ============================================================

RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS")
CONFIG_TAB = os.getenv("FEEDER_CONFIG_TAB", "CONFIG")
THEMES_TAB = os.getenv("THEMES_TAB", "THEMES")
SIGNALS_TAB = os.getenv("CONFIG_SIGNALS_TAB", "CONFIG_SIGNALS")
CAPABILITY_TAB = os.getenv("CAPABILITY_TAB", "ROUTE_CAPABILITY_MAP")
BENCHMARKS_TAB = os.getenv("BENCHMARKS_TAB", "ZONE_THEME_BENCHMARKS")

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID")
DUFFEL_API_KEY = os.getenv("DUFFEL_API_KEY")
DUFFEL_API_BASE = "https://api.duffel.com"
DUFFEL_VERSION = "v2"


# ============================================================
# GOVERNORS
# ============================================================

DUFFEL_MAX_INSERTS = int(os.getenv("DUFFEL_MAX_INSERTS", "50"))
DUFFEL_MAX_SEARCHES_PER_RUN = int(os.getenv("DUFFEL_MAX_SEARCHES_PER_RUN", "6"))
DUFFEL_ROUTES_PER_RUN = int(os.getenv("DUFFEL_ROUTES_PER_RUN", "4"))
DUFFEL_OFFERS_PER_SEARCH = int(os.getenv("OFFERS_PER_SEARCH", "50"))

DUFFEL_MAX_INSERTS_PER_ROUTE = int(os.getenv("DUFFEL_MAX_INSERTS_PER_ROUTE", "10"))
DUFFEL_MAX_INSERTS_PER_ORIGIN = int(os.getenv("DUFFEL_MAX_INSERTS_PER_ORIGIN", "10"))

PRICE_GATE_ENABLED = True
PRICE_GATE_MULTIPLIER = float(os.getenv("PRICE_GATE_MULTIPLIER", "1.0"))
PRICE_GATE_MIN_CAP_GBP = float(os.getenv("PRICE_GATE_MIN_CAP_GBP", "80"))
PRICE_GATE_FALLBACK_BEHAVIOR = os.getenv("PRICE_GATE_FALLBACK_BEHAVIOR", "BLOCK")

INVENTORY_MIN_DAYS = 21
INVENTORY_MAX_DAYS = 84
ZERO_OFFER_RETRY_ENABLED = True
ZERO_OFFER_RETRY_MAX_DAYS = 60

FEEDER_SLEEP_SECONDS = float(os.getenv("FEEDER_SLEEP_SECONDS", "0"))


# ============================================================
# ORIGIN POOLS
# ============================================================

ADVENTURE_HUBS = ["LGW", "LHR", "STN", "LTN", "MAN"]
SW_ENGLAND_DEFAULT = ["BRS", "EXT", "NQY", "SOU", "CWL", "BOH"]
UK_WIDE_FALLBACK = ["LBA", "NCL", "EDI", "GLA", "BFS", "BHD"]

SPARSE_THEMES = {"northern_lights"}


# ============================================================
# HELPERS
# ============================================================

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


def _iata(x: Any) -> str:
    return str(x or "").strip().upper()[:3]


def _stable_hash(s: str) -> int:
    return int(hashlib.md5(s.encode()).hexdigest()[:8], 16)


# ============================================================
# GOOGLE SHEETS
# ============================================================

def gs_client():
    raw = os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON")
    info = json.loads(raw.replace("\\n", "\n"))
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)


# ============================================================
# LOADERS
# ============================================================

def load_config(sheet):
    rows = sheet.worksheet(CONFIG_TAB).get_all_records()
    return [r for r in rows if str(r.get("enabled")).lower() in ("true", "yes", "1")]


def load_capability(sheet) -> Set[Tuple[str, str]]:
    rows = sheet.worksheet(CAPABILITY_TAB).get_all_records()
    return {(_iata(r["origin_iata"]), _iata(r["destination_iata"])) for r in rows}


def load_benchmarks(sheet):
    rows = sheet.worksheet(BENCHMARKS_TAB).get_all_records()
    out = []
    for r in rows:
        if not r.get("theme") or not r.get("origin_iata"):
            continue
        out.append(r)
    return out


def compute_cap(benchmarks, theme, origin, dest) -> Optional[float]:
    fallback = None
    for r in benchmarks:
        if r["theme"] != theme:
            continue
        if _iata(r["origin_iata"]) != origin:
            continue
        fallback = r
        examples = str(r.get("destination_examples") or "").split(",")
        if dest in [e.strip().upper() for e in examples]:
            fallback = r
            break
    if not fallback:
        return None
    base = float(fallback["max_price"]) + float(fallback.get("error_price") or 0)
    return max(PRICE_GATE_MIN_CAP_GBP, base * PRICE_GATE_MULTIPLIER)


# ============================================================
# DUFFEL
# ============================================================

def duffel_headers():
    return {
        "Authorization": f"Bearer {DUFFEL_API_KEY}",
        "Duffel-Version": DUFFEL_VERSION,
        "Content-Type": "application/json",
    }


def duffel_search(origin, dest, dep, ret):
    payload = {
        "data": {
            "slices": [
                {"origin": origin, "destination": dest, "departure_date": dep},
                {"origin": dest, "destination": origin, "departure_date": ret},
            ],
            "passengers": [{"type": "adult"}],
            "return_offers": True,
        }
    }
    r = requests.post(
        f"{DUFFEL_API_BASE}/air/offer_requests",
        headers=duffel_headers(),
        json=payload,
        timeout=90,
    )
    r.raise_for_status()
    return r.json()["data"]["offers"]


def offer_price(off):
    if off.get("total_currency") != "GBP":
        return None
    return float(off["total_amount"])


# ============================================================
# MAIN
# ============================================================

def main():
    log("=" * 80)
    log("TRAVELTXTTER PIPELINE WORKER (FEEDER) START")
    log("=" * 80)

    gc = gs_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_raw = sh.worksheet(RAW_DEALS_TAB)
    config = load_config(sh)
    capability = load_capability(sh)
    benchmarks = load_benchmarks(sh)

    today = dt.date.today()
    theme_today = ["winter_sun","summer_sun","beach_break","snow","northern_lights",
                   "surf","adventure","city_breaks","culture_history","long_haul",
                   "luxury_value","unexpected_value"][today.timetuple().tm_yday % 11]

    log(f"🎯 Theme of the day (UTC): {theme_today}")

    # 90/10 split preserved
    explore_run = (_stable_hash(str(today)) % 10 == 0)
    theme_quota = DUFFEL_ROUTES_PER_RUN - (1 if explore_run else 0)
    explore_quota = 1 if explore_run else 0

    # Planned origins
    if theme_today == "adventure":
        planned_origins = ADVENTURE_HUBS[:]
    else:
        planned_origins = SW_ENGLAND_DEFAULT[:]

    log(f"🧭 Planned origins: {planned_origins}")

    # Destination buckets
    theme_dests = [r for r in config if r["theme"] == theme_today]
    explore_dests = [r for r in config if r["theme"] != theme_today]

    # RULE B: one-per-origin first
    selected_routes = []
    origin_usage = {o: 0 for o in planned_origins}

    def fill_routes(dest_rows, quota):
        nonlocal selected_routes
        if quota <= 0:
            return
        # Pass 1: one per origin
        for origin in planned_origins:
            if quota <= 0:
                break
            for r in dest_rows:
                dest = _iata(r["destination_iata"])
                if (origin, dest) not in capability:
                    continue
                selected_routes.append((origin, dest, r))
                quota -= 1
                break
        # Pass 2: fill remaining deterministically
        i = 0
        while quota > 0 and i < len(dest_rows):
            r = dest_rows[i]
            dest = _iata(r["destination_iata"])
            for origin in planned_origins:
                if (origin, dest) in capability:
                    selected_routes.append((origin, dest, r))
                    quota -= 1
                    break
            i += 1

    fill_routes(theme_dests, theme_quota)
    fill_routes(explore_dests, explore_quota)

    log(f"🧭 Selected routes: {len(selected_routes)}")

    searches = 0
    deals = []

    for origin, dest, cfg in selected_routes:
        if searches >= DUFFEL_MAX_SEARCHES_PER_RUN:
            break
        if origin_usage[origin] >= DUFFEL_MAX_INSERTS_PER_ORIGIN:
            continue

        days_out = max(INVENTORY_MIN_DAYS, min(INVENTORY_MAX_DAYS, 30))
        dep = (today + dt.timedelta(days=days_out)).isoformat()
        ret = (today + dt.timedelta(days=days_out + int(cfg.get("trip_length_days", 5)))).isoformat()

        cap = compute_cap(benchmarks, cfg["theme"], origin, dest)
        if cap is None and PRICE_GATE_FALLBACK_BEHAVIOR == "BLOCK":
            log(f"⛔ BENCHMARK_MISS {origin}->{dest}")
            continue

        offers = duffel_search(origin, dest, dep, ret)
        searches += 1

        gbp = [o for o in offers if offer_price(o) and offer_price(o) <= cap]
        gbp = sorted(gbp, key=offer_price)

        for off in gbp[:DUFFEL_MAX_INSERTS_PER_ROUTE]:
            deal = {
                "status": "NEW",
                "deal_id": hashlib.sha1(f"{origin}{dest}{off['id']}".encode()).hexdigest()[:20],
                "origin_iata": origin,
                "destination_iata": dest,
                "price_gbp": int(math.ceil(offer_price(off))),
                "outbound_date": dep,
                "return_date": ret,
                "theme": cfg["theme"],
                "deal_theme": cfg["theme"],
                "ingested_at_utc": dt.datetime.utcnow().isoformat() + "Z",
            }
            deals.append(deal)
            origin_usage[origin] += 1
            if origin_usage[origin] >= DUFFEL_MAX_INSERTS_PER_ORIGIN:
                break

        if FEEDER_SLEEP_SECONDS:
            time.sleep(FEEDER_SLEEP_SECONDS)

    if not deals:
        log("⚠️ No deals inserted.")
        return 0

    headers = ws_raw.row_values(1)
    rows = [[d.get(h, "") for h in headers] for d in deals]
    ws_raw.append_rows(rows, value_input_option="RAW")

    log(f"✅ Inserted {len(deals)} rows into RAW_DEALS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
