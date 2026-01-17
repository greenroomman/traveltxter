#!/usr/bin/env python3
"""
workers/pipeline_worker.py

TravelTxter Pipeline Worker (Feeder) — V4.6 (Phase 1 price gate only)

WHY THIS EXISTS (LOCKED INTENT)
- Theme-of-day drives route choice (90%) + deterministic explore (10%).
- Duffel searches are expensive; we must NOT ingest junk then hope scoring rescues it.
- Phase 1 price gate = ingestion-only filtering using ZONE_THEME_BENCHMARKS.
- Always land timestamps so ai_scorer can apply MIN_INGEST_AGE_SECONDS correctly.

WHAT THIS FILE FIXES (SURGICAL)
1) ORIGIN PRECEDENCE BUG:
   - Previously: CONFIG origin_iata could override planned origins even when RESPECT_CONFIG_ORIGIN=False.
   - Now: CONFIG origin_iata only overrides if RESPECT_CONFIG_ORIGIN=true.

2) LOW YIELD PER SEARCH:
   - Previously: only ingested up to 10 offers per search.
   - Now: ingests up to DUFFEL_OFFERS_PER_SEARCH per route, bounded by remaining total inserts.

3) TIMESTAMP LANDING:
   - Writes created_utc ALWAYS if the column exists, and ingested_at_utc if present.
   - Also writes created_at/timestamp if those columns exist.

4) PHASE 1 PRICE GATE:
   - Uses ZONE_THEME_BENCHMARKS headers:
     zone,theme,origin_iata,destination_examples,low_price,deal_price,expensive_price,notes
   - Ingest cap = max(PRICE_GATE_MIN_CAP_GBP, expensive_price * PRICE_GATE_MULTIPLIER)
   - If benchmark missing:
       ALLOW = proceed without cap (logged)
       BLOCK = block route (not recommended during stabilisation)

NO Phase 2 (Duffel-side max_price). No architecture changes.
"""

from __future__ import annotations

import os
import json
import math
import time
import hashlib
import datetime as dt
from typing import Any, Dict, List, Tuple, Optional, Set

import requests
import gspread
from google.oauth2.service_account import Credentials


# ==================== ENV / TABS ====================

RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS").strip() or "RAW_DEALS"
CONFIG_TAB = os.getenv("CONFIG_TAB", "CONFIG").strip() or "CONFIG"
THEMES_TAB = os.getenv("THEMES_TAB", "THEMES").strip() or "THEMES"
SIGNALS_TAB = os.getenv("SIGNALS_TAB", "CONFIG_SIGNALS").strip() or "CONFIG_SIGNALS"
CAPABILITY_TAB = os.getenv("CAPABILITY_TAB", "ROUTE_CAPABILITY_MAP").strip() or "ROUTE_CAPABILITY_MAP"
BENCHMARKS_TAB = os.getenv("BENCHMARKS_TAB", "ZONE_THEME_BENCHMARKS").strip() or "ZONE_THEME_BENCHMARKS"

SPREADSHEET_ID = (os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID") or "").strip()

DUFFEL_API_KEY = os.getenv("DUFFEL_API_KEY", "").strip()
DUFFEL_API_BASE = os.getenv("DUFFEL_API_BASE", "https://api.duffel.com").strip()
DUFFEL_VERSION = os.getenv("DUFFEL_VERSION", "v2").strip() or "v2"

# Hard caps (governor)
MAX_INSERTS_TOTAL = int(os.getenv("DUFFEL_MAX_INSERTS", "3") or "3")
MAX_SEARCHES_PER_RUN = int(os.getenv("DUFFEL_MAX_SEARCHES_PER_RUN", "4") or "4")
ROUTES_PER_RUN = int(os.getenv("DUFFEL_ROUTES_PER_RUN", "3") or "3")

# How many offers to attempt ingest per Duffel search (bounded by remaining inserts)
DUFFEL_OFFERS_PER_SEARCH = int(os.getenv("DUFFEL_OFFERS_PER_SEARCH", "50") or "50")

STRICT_CAPABILITY_MAP = (os.getenv("STRICT_CAPABILITY_MAP", "true").strip().lower() == "true")
FEEDER_OPEN_ORIGINS = (os.getenv("FEEDER_OPEN_ORIGINS", "false").strip().lower() == "true")

# IMPORTANT: CONFIG origin override only if true
RESPECT_CONFIG_ORIGIN = (os.getenv("RESPECT_CONFIG_ORIGIN", "false").strip().lower() == "true")

# Date defaults
DEFAULT_DAYS_AHEAD_MIN = int(os.getenv("DAYS_AHEAD_MIN", "14") or "14")
DEFAULT_DAYS_AHEAD_MAX = int(os.getenv("DAYS_AHEAD_MAX", "90") or "90")
DEFAULT_TRIP_LENGTH_DAYS = int(os.getenv("TRIP_LENGTH_DAYS", "5") or "5")

# Deterministic explore control
FEEDER_EXPLORE_RUN_MOD = int(os.getenv("FEEDER_EXPLORE_RUN_MOD", "10") or "10")
FEEDER_EXPLORE_SALT = (os.getenv("FEEDER_EXPLORE_SALT", "traveltxter") or "traveltxter").strip()

FEEDER_SLEEP_SECONDS = float(os.getenv("FEEDER_SLEEP_SECONDS", "0.0") or "0.0")

# Phase 1 price gate
PRICE_GATE_ENABLED = (os.getenv("PRICE_GATE_ENABLED", "true").strip().lower() == "true")
PRICE_GATE_MULTIPLIER = float(os.getenv("PRICE_GATE_MULTIPLIER", "1.5") or "1.5")
PRICE_GATE_MIN_CAP_GBP = float(os.getenv("PRICE_GATE_MIN_CAP_GBP", "80") or "80")
PRICE_GATE_FALLBACK_BEHAVIOR = (os.getenv("PRICE_GATE_FALLBACK_BEHAVIOR", "ALLOW").strip().upper() or "ALLOW")
# ALLOW -> no cap if benchmark missing (logged)
# BLOCK -> skip route if benchmark missing


# ==================== THEMES ====================

MASTER_THEMES = [
    "winter_sun",
    "summer_sun",
    "beach_break",
    "snow",
    "northern_lights",
    "surf",
    "adventure",
    "city_breaks",
    "culture_history",
    "long_haul",
    "luxury_value",
    "unexpected_value",
]

SPARSE_THEMES = {"northern_lights"}

SNOW_THEMES = {"snow", "northern_lights"}
LONG_HAUL_THEMES = {"long_haul", "luxury_value"}

SHORT_HAUL_PRIMARY = ["BRS", "EXT", "NQY", "CWL", "SOU"]
SHORT_HAUL_FALLBACK = ["STN", "LTN", "LGW"]
SNOW_POOL = ["BRS", "LGW", "STN", "LTN"]
LONG_HAUL_POOL = ["LHR", "LGW"]

ORIGIN_CITY_FALLBACK = {
    "LHR": "London", "LGW": "London", "STN": "London", "LTN": "London",
    "LCY": "London", "SEN": "London", "MAN": "Manchester", "BHX": "Birmingham",
    "BRS": "Bristol", "EXT": "Exeter", "NQY": "Newquay", "SOU": "Southampton",
    "CWL": "Cardiff",
}


# ==================== LOGGING / UTILS ====================

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


def low(x: Any) -> str:
    return str(x or "").strip().lower()


def _clean_iata(x: Any) -> str:
    return str(x or "").strip().upper()[:3]


def _run_slot() -> str:
    return (os.getenv("RUN_SLOT") or "").strip().upper()


def theme_of_day_utc() -> str:
    today = dt.datetime.utcnow().date()
    doy = int(today.strftime("%j"))
    return MASTER_THEMES[doy % len(MASTER_THEMES)]


def _stable_mod(key: str, mod: int) -> int:
    h = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return int(h[:8], 16) % max(1, mod)


def should_do_explore_this_run(theme_today: str) -> bool:
    today = dt.datetime.utcnow().date().isoformat()
    run_slot = (_run_slot() or "UNSET")
    gh_run_id = (os.getenv("GITHUB_RUN_ID") or "").strip()
    gh_run_attempt = (os.getenv("GITHUB_RUN_ATTEMPT") or "").strip()
    key = f"{FEEDER_EXPLORE_SALT}|{today}|{run_slot}|{theme_today}|{gh_run_id}|{gh_run_attempt}"
    return _stable_mod(key, FEEDER_EXPLORE_RUN_MOD) == 0


def _deterministic_pick(seq: List[str], seed: str, k: int) -> List[str]:
    if not seq or k <= 0:
        return []
    h = int(hashlib.md5(seed.encode("utf-8")).hexdigest()[:8], 16)
    out: List[str] = []
    n = len(seq)
    for i in range(k):
        idx = (h + i) % n
        val = seq[idx]
        if val not in out:
            out.append(val)
    while len(out) < k:
        out.append(seq[(h + len(out)) % n])
    return out


def origin_plan_for_theme(theme_today: str, routes_per_run: int) -> List[str]:
    today = dt.datetime.utcnow().date().isoformat()
    slot = _run_slot()
    seed_base = f"{today}|{slot}|{theme_today}"
    t = low(theme_today)

    if t in LONG_HAUL_THEMES:
        picks = _deterministic_pick(LONG_HAUL_POOL, seed_base, max(1, min(2, routes_per_run)))
        return [picks[i % len(picks)] for i in range(routes_per_run)]

    if t in SNOW_THEMES:
        picks = _deterministic_pick(SNOW_POOL, seed_base, min(len(SNOW_POOL), routes_per_run))
        return [picks[i % len(picks)] for i in range(routes_per_run)]

    primary_n = 2 if routes_per_run >= 3 else 1
    fallback_n = max(0, routes_per_run - primary_n)
    prim = _deterministic_pick(SHORT_HAUL_PRIMARY, seed_base + "|P", primary_n)
    fb = _deterministic_pick(SHORT_HAUL_FALLBACK, seed_base + "|F", fallback_n)
    return prim + fb


def enforce_origin_diversity(origins: List[str]) -> List[str]:
    # Stops the same origin appearing 4x in a row if the deterministic pick lands that way.
    counts: Dict[str, int] = {}
    out: List[str] = []
    for o in origins:
        counts.setdefault(o, 0)
        if counts[o] >= 2:
            continue
        out.append(o)
        counts[o] += 1
    while len(out) < len(origins):
        for o in origins:
            if len(out) >= len(origins):
                break
            out.append(o)
    return out[: len(origins)]


def resolve_origin_city(iata: str, origin_city_map: Dict[str, str]) -> str:
    i = _clean_iata(iata)
    return (origin_city_map.get(i) or ORIGIN_CITY_FALLBACK.get(i) or "").strip()


# ==================== GOOGLE SHEETS ====================

def _parse_sa_json(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return json.loads(raw.replace("\\n", "\n"))


def gs_client() -> gspread.Client:
    raw = (os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON") or "").strip()
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
    info = _parse_sa_json(raw)
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds)


def load_config_rows(sheet: gspread.Spreadsheet) -> List[Dict[str, Any]]:
    ws = sheet.worksheet(CONFIG_TAB)
    rows = ws.get_all_records()
    out: List[Dict[str, Any]] = []
    for r in rows:
        enabled = str(r.get("enabled", "")).strip().lower()
        if enabled in ("true", "yes", "1", "y"):
            out.append(r)
    return out


def load_themes_dict(sheet: gspread.Spreadsheet) -> Dict[str, List[Dict[str, Any]]]:
    ws = sheet.worksheet(THE
