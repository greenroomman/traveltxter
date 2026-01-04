#!/usr/bin/env python3
"""
TravelTxter V4.5.3 — WATERWHEEL ENGINE (PRODUCTION)

KEY PRINCIPLES:
- Feeder delivers a BROAD pool
- Themes are SOFT WEIGHTS, not filters
- Price can override theme
- Destination repetition is aggressively penalised
- Config defines product, not code

SAFE FOR:
- Duffel free tier
- GitHub Actions
- Non-tech operators
"""

import os
import json
import uuid
import time
import math
import hashlib
import datetime as dt
from typing import Dict, List, Any, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials


# ==========================================================
# ENV
# ==========================================================
def env(name, default="", required=False):
    val = (os.getenv(name) or "").strip()
    if not val:
        val = default
    if required and not val:
        raise RuntimeError(f"Missing env var: {name}")
    return val


SPREADSHEET_ID = env("SPREADSHEET_ID", required=True)
RAW_DEALS_TAB = env("RAW_DEALS_TAB", "RAW_DEALS")
CONFIG_TAB = env("CONFIG_TAB", "CONFIG")
CONFIG_SIGNALS_TAB = env("CONFIG_SIGNALS_TAB", "CONFIG_SIGNALS")

GCP_SA_JSON = env("GCP_SA_JSON", required=True)

DUFFEL_API_KEY = env("DUFFEL_API_KEY", "")
DUFFEL_MAX_INSERTS = int(env("DUFFEL_MAX_INSERTS", "3"))
DUFFEL_ROUTES_PER_RUN = int(env("DUFFEL_ROUTES_PER_RUN", "2"))

VARIETY_LOOKBACK_HOURS = int(env("VARIETY_LOOKBACK_HOURS", "72"))
DEST_REPEAT_PENALTY = float(env("DEST_REPEAT_PENALTY", "50.0"))

RUN_SLOT = env("RUN_SLOT", "AM").upper()


# ==========================================================
# CONSTANTS
# ==========================================================
STATUS_NEW = "NEW"
STATUS_READY_TO_POST = "READY_TO_POST"


# ==========================================================
# HELPERS
# ==========================================================
def now_utc():
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def log(msg):
    print(f"{now_utc()} | {msg}", flush=True)


def safe_float(v, default=0.0):
    try:
        return float(v)
    except:
        return default


def stable_hash(text):
    return int(hashlib.md5(text.encode()).hexdigest(), 16)


# ==========================================================
# GOOGLE SHEETS
# ==========================================================
def get_ws():
    creds = Credentials.from_service_account_info(
        json.loads(GCP_SA_JSON),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    client = gspread.authorize(creds)
    sh = client.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(RAW_DEALS_TAB)
    headers = ws.row_values(1)
    return ws, headers


def header_map(headers):
    return {h: i + 1 for i, h in enumerate(headers) if h}


# ==========================================================
# CONFIG_SIGNALS (Theme Intelligence)
# ==========================================================
def load_config_signals(sh):
    ws = sh.worksheet(CONFIG_SIGNALS_TAB)
    records = ws.get_all_records()

    signals = {}
    for r in records:
        iata = str(r.get("iata_hint", "")).strip().upper()
        if len(iata) == 3:
            signals[iata] = r

    log(f"Loaded CONFIG_SIGNALS: {len(signals)} destinations")
    return signals


def derive_theme(signals_row, month):
    if not signals_row:
        return "city"

    sun = safe_float(signals_row.get(f"sun_score_m{month:02d}"))
    surf = safe_float(signals_row.get(f"surf_score_m{month:02d}"))
    snow = safe_float(signals_row.get(f"snow_score_m{month:02d}"))

    scores = {
        "winter_sun": sun,
        "surf": surf,
        "snow": snow,
    }

    best_theme, best_score = max(scores.items(), key=lambda x: x[1])
    if best_score < 1.0:
        return "city"

    return best_theme


# ==========================================================
# DUFFEL FEEDER (POOL BUILDER)
# ==========================================================
def duffel_search(origin, dest, out_date, ret_date):
    url = "https://api.duffel.com/air/offer_requests"
    headers = {
        "Authorization": f"Bearer {DUFFEL_API_KEY}",
        "Duffel-Version": "v2",
        "Content-Type": "application/json"
    }

    payload = {
        "data": {
            "slices": [
                {"origin": origin, "destination": dest, "departure_date": out_date},
                {"origin": dest, "destination": origin, "departure_date": ret_date}
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": "economy"
        }
    }

    r = requests.post(url, headers=headers, json=payload, timeout=30)
    r.raise_for_status()
    return r.json().get("data", {}).get("offers", [])


# ==========================================================
# SCORING (ELASTIC)
# ==========================================================
def score_deal(row, target_theme):
    price = safe_float(row.get("price_gbp"))
    theme = row.get("auto_theme")

    # Theme alignment
    theme_score = 0.6 if theme == target_theme else 0.2

    # Price dominance
    price_score = max(0, min(1, (300 - price) / 300)) if price else 0

    # If price is exceptional, override theme
    if price < 60:
        theme_score = max(theme_score, 0.2)
        price_score = 1.0

    stotal = (theme_score * 0.6 + price_score * 0.4) * 100
    return round(stotal, 1)


# ==========================================================
# EDITORIAL SELECTION (DIVERSITY FORCED)
# ==========================================================
def get_recent_destinations(rows, headers):
    cutoff = dt.datetime.utcnow() - dt.timedelta(hours=VARIETY_LOOKBACK_HOURS)
    dests = []

    for r in rows[1:]:
        row = dict(zip(headers, r))
        ts = row.get("ig_published_timestamp") or ""
        if not ts:
            continue
        try:
            t = dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if t > cutoff:
                dests.append(row.get("destination_iata"))
        except:
            pass

    return dests[-3:]


def stage_select_best(ws, headers):
    rows = ws.get_all_values()
    recent = get_recent_destinations(rows, headers)

    candidates = []

    for i, r in enumerate(rows[1:], start=2):
        row = dict(zip(headers, r))
        if row.get("status") != STATUS_NEW:
            continue

        base = safe_float(row.get("stotal"))
        dest = row.get("destination_iata")

        penalty = DEST_REPEAT_PENALTY if dest in recent else 0
        final = base - penalty

        candidates.append((final, i, row))

    if not candidates:
        log("No candidates")
        return 0

    candidates.sort(reverse=True, key=lambda x: x[0])
    _, idx, _ = candidates[0]

    ws.update_cell(idx, headers.index("status") + 1, STATUS_READY_TO_POST)
    log(f"Selected row {idx} (diversity enforced)")
    return 1


# ==========================================================
# MAIN
# ==========================================================
def main():
    log("=" * 60)
    log("TRAVELTXTER V4.5.3 — WATERWHEEL RUN")
    log("=" * 60)

    ws, headers = get_ws()
    sh = ws.spreadsheet
    signals = load_config_signals(sh)

    rows = ws.get_all_values()

    today = dt.date.today()
    month = today.month

    # Auto-theme + score NEW deals
    for i, r in enumerate(rows[1:], start=2):
        row = dict(zip(headers, r))
        if row.get("status") != STATUS_NEW:
            continue

        dest = row.get("destination_iata")
        theme = derive_theme(signals.get(dest), month)

        stotal = score_deal(
            {"price_gbp": row.get("price_gbp"), "auto_theme": theme},
            target_theme=env("DAILY_THEME", "winter_sun")
        )

        ws.update_cells([
            gspread.Cell(i, headers.index("auto_theme") + 1, theme),
            gspread.Cell(i, headers.index("stotal") + 1, stotal)
        ])

    stage_select_best(ws, headers)

    log("RUN COMPLETE")


if __name__ == "__main__":
    main()
