#!/usr/bin/env python3
"""
workers/pipeline_worker.py

TravelTxter Pipeline Worker (FEEDER) — V4.7.10 + HYGIENE GATE (V4.6 LOCK-COMPAT)

LOCKED PRINCIPLES:
- Build from the existing file (no reinvention, no new architecture, no tab renames).
- Replace full file only.
- Google Sheets is the single source of truth.
- Do NOT write to RAW_DEALS_VIEW.

This change adds ONE thing:
✅ "HYGIENE GATE" to prevent obviously doomed offers entering RAW_DEALS:
- Hard cap on connections (short-haul vs long-haul)
- Hard cap on duration (short-haul vs long-haul)
- Optional "band" cap relative to a computed cap_gbp for the route

Also includes:
- 90/10 theme/explore strategy
- Anchored origin planning by theme
- Windowing & trip length constraints
- Price gate by theme/zone benchmark caps
- Dedupe within time window
- Optional zero-offer retry window

NOTE: Brand & sustainability: minimize wasted searches by keeping origins realistic and by gating low-quality offers.

"""

from __future__ import annotations

import datetime as dt
import json
import os
import random
import re
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import gspread
import requests


# ==================== CONSTANTS / DEFAULTS ====================

# Stable “run slot” to introduce deterministic variety in daily runs
RUN_SLOT = os.getenv("RUN_SLOT", "AM").strip().upper()  # AM/PM or arbitrary label

# Tabs
RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS").strip()
THEMES_TAB = os.getenv("THEMES_TAB", "THEMES").strip()
CONFIG_TAB = os.getenv("FEEDER_CONFIG_TAB", "CONFIG").strip()
FEEDER_LOG_TAB = os.getenv("FEEDER_LOG_TAB", "FEEDER_LOG").strip()

# Theme selection
THEME_DEFAULT = os.getenv("THEME", "DEFAULT").strip().lower()

# Duffel caps / budgets
DUFFEL_MAX_INSERTS = int(float(os.getenv("DUFFEL_MAX_INSERTS", "3")))
DUFFEL_MAX_INSERTS_PER_ORIGIN = int(float(os.getenv("DUFFEL_MAX_INSERTS_PER_ORIGIN", "10")))
DUFFEL_MAX_INSERTS_PER_ROUTE = int(float(os.getenv("DUFFEL_MAX_INSERTS_PER_ROUTE", "10")))
DUFFEL_MAX_SEARCHES_PER_RUN = int(float(os.getenv("DUFFEL_MAX_SEARCHES_PER_RUN", "4")))
DUFFEL_ROUTES_PER_RUN = int(float(os.getenv("DUFFEL_ROUTES_PER_RUN", "3")))
DUFFEL_SEARCH_DEDUPE_HOURS = int(float(os.getenv("DUFFEL_SEARCH_DEDUPE_HOURS", "4")))

# Feeder caps (legacy aliases; keep contract)
FEEDER_MAX_INSERTS = int(float(os.getenv("FEEDER_MAX_INSERTS", str(DUFFEL_MAX_INSERTS))))
FEEDER_MAX_SEARCHES = int(float(os.getenv("FEEDER_MAX_SEARCHES", str(DUFFEL_MAX_SEARCHES_PER_RUN))))

# Hygiene gate
HYGIENE_ENABLED = (os.getenv("HYGIENE_ENABLED", "true").strip().lower() == "true")
HYGIENE_CONN_SHORT = int(float(os.getenv("HYGIENE_CONN_SHORT", "1")))
HYGIENE_CONN_LONG = int(float(os.getenv("HYGIENE_CONN_LONG", "2")))
HYGIENE_DUR_SHORT = int(float(os.getenv("HYGIENE_DUR_SHORT", "720")))   # minutes
HYGIENE_DUR_LONG = int(float(os.getenv("HYGIENE_DUR_LONG", "1200")))    # minutes
HYGIENE_BAND_SHORT = float(os.getenv("HYGIENE_BAND_SHORT", "0.85"))
HYGIENE_BAND_LONG = float(os.getenv("HYGIENE_BAND_LONG", "0.95"))

# Inventory window defaults (days out)
INVENTORY_MIN_DAYS_OUT = int(float(os.getenv("WINDOW_DEFAULT_MIN_DAYS_OUT", "21")))
INVENTORY_MAX_DAYS_OUT = int(float(os.getenv("WINDOW_DEFAULT_MAX_DAYS_OUT", "84")))

# Zero-offer retry (optional)
ZERO_OFFER_RETRY_ENABLED = (os.getenv("ZERO_OFFER_RETRY_ENABLED", "true").strip().lower() == "true")
ZERO_OFFER_RETRY_WINDOW_MAX = int(float(os.getenv("ZERO_OFFER_RETRY_WINDOW_MAX", "60")))

# Origin policy
FEEDER_OPEN_ORIGINS = (os.getenv("FEEDER_OPEN_ORIGINS", "false").strip().lower() == "true")

# Timing / ingest age
MIN_INGEST_AGE_SECONDS = int(float(os.getenv("MIN_INGEST_AGE_SECONDS", "90")))

# Sleep between calls
FEEDER_SLEEP_SECONDS = float(os.getenv("FEEDER_SLEEP_SECONDS", "0.1"))

# Price gate
PRICE_GATE_FALLBACK_BEHAVIOR = os.getenv("PRICE_GATE_FALLBACK_BEHAVIOR", "BLOCK").strip().upper()
PRICE_GATE_MULT = float(os.getenv("PRICE_GATE_MULT", "1.0"))
PRICE_GATE_MINCAP_GBP = float(os.getenv("PRICE_GATE_MINCAP_GBP", "80.0"))

# Strategy: 90/10
FEEDER_EXPLORE_RUN_MOD = int(float(os.getenv("FEEDER_EXPLORE_RUN_MOD", "10")))
FEEDER_EXPLORE_SALT = os.getenv("FEEDER_EXPLORE_SALT", "V451").strip()
PLANNER_MAX_DESTS_PER_THEME = int(float(os.getenv("PLANNER_MAX_DESTS_PER_THEME", "6")))

# Misc
DEST_REPEAT_PENALTY = int(float(os.getenv("DEST_REPEAT_PENALTY", "80")))
VARIETY_LOOKBACK_HOURS = int(float(os.getenv("VARIETY_LOOKBACK_HOURS", "120")))


# ==================== ORIGIN POOLS (legacy) ====================

SW_ENGLAND_DEFAULT = ["BRS", "EXT", "NQY", "SOU", "CWL", "BOH"]

LONDON_FULL = ["LHR", "LGW", "LCY", "STN", "LTN"]
LONDON_LCC = ["LGW", "STN", "LTN"]
MIDLANDS = ["BHX", "EMA"]
NORTH = ["MAN", "LPL", "LBA"]
SCOTLAND = ["GLA", "EDI"]
NI = ["BFS"]

LONGHAUL_PRIMARY_HUBS = ["LHR", "LGW"]
LONGHAUL_SECONDARY_HUBS = ["MAN"]

ORIGIN_CITY_FALLBACK = {
    "LHR": "London",
    "LGW": "London",
    "STN": "London",
    "LTN": "London",
    "LCY": "London",
    "MAN": "Manchester",
    "BRS": "Bristol",
    "EXT": "Exeter",
    "NQY": "Newquay",
    "SOU": "Southampton",
    "CWL": "Cardiff",
    "BOH": "Bournemouth",
    "BHX": "Birmingham",
    "EMA": "East Midlands",
    "LPL": "Liverpool",
    "LBA": "Leeds",
    "GLA": "Glasgow",
    "EDI": "Edinburgh",
    "BFS": "Belfast",
}


# ==================== UTILS ====================

def log(msg: str) -> None:
    print(f"{dt.datetime.utcnow().isoformat(timespec='seconds')}Z | {msg}", flush=True)


def _clean_iata(x: Any) -> str:
    s = (str(x) if x is not None else "").strip().upper()
    s = re.sub(r"[^A-Z]", "", s)
    return s[:3]


def _dedupe_keep_order(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in items:
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def _stable_mod(key: str, mod: int) -> int:
    h = 0
    for ch in key:
        h = (h * 131 + ord(ch)) % 2_147_483_647
    return h % max(1, mod)


def explore_run_today(theme_today: str) -> bool:
    today = dt.datetime.utcnow().date().isoformat()
    gh_run_id = (os.getenv("GITHUB_RUN_ID") or "").strip()
    gh_run_attempt = (os.getenv("GITHUB_RUN_ATTEMPT") or "").strip()
    key = f"{FEEDER_EXPLORE_SALT}|{today}|{RUN_SLOT}|{theme_today}|{gh_run_id}|{gh_run_attempt}"
    return _stable_mod(key, FEEDER_EXPLORE_RUN_MOD) == 0


def _sw_england_origins_from_env() -> List[str]:
    raw = (os.getenv("SW_ENGLAND_ORIGINS", "") or "").strip()
    if not raw:
        return SW_ENGLAND_DEFAULT[:]
    parts = [p.strip().upper() for p in raw.split(",")]
    parts = [p for p in parts if p]
    return parts or SW_ENGLAND_DEFAULT[:]


def _theme_origins_from_env(theme_today: str) -> List[str]:
    """Return ORIGINS_<THEME> if defined, else [].

    Contract: environment variables already exist (ORIGINS_LUXURY_VALUE, ORIGINS_SURF, etc.).
    We treat these as the primary, explicit origin allowlists to avoid unrealistic origin->destination searches.
    """
    key = f"ORIGINS_{(theme_today or '').strip().upper()}"
    raw = (os.getenv(key, "") or "").strip()
    if not raw:
        return []
    parts = [p.strip().upper() for p in raw.split(",")]
    parts = [p for p in parts if p]
    return _dedupe_keep_order(parts)


def _det_hash_score(seed: str, item: str) -> int:
    s = f"{seed}|{item}"
    h = 0
    for ch in s:
        h = (h * 131 + ord(ch)) % 2_147_483_647
    return h


def _weighted_pick_unique(seed: str, pools: List[Tuple[List[str], int]], n: int) -> List[str]:
    # Build weighted list deterministically by hashing
    candidates: List[Tuple[int, str]] = []
    for items, weight in pools:
        items = _dedupe_keep_order([_clean_iata(x) for x in items if _clean_iata(x)])
        for it in items:
            score = _det_hash_score(seed, it)
            # Apply weight by “replicating” score scale
            candidates.append((score // max(1, int(100 / max(1, weight))), it))
    candidates.sort(key=lambda x: x[0])
    out: List[str] = []
    for _, it in candidates:
        if it not in out:
            out.append(it)
        if len(out) >= n:
            break
    return out


# ==================== ORIGIN PLANNING ====================

def origin_plan_for_theme(theme_today: str, plan_n: int) -> List[str]:
    sw = _dedupe_keep_order(_sw_england_origins_from_env())
    seed = f"{dt.datetime.utcnow().date().isoformat()}|{RUN_SLOT}|{theme_today}|ORIGIN_PLAN"

    explicit = _theme_origins_from_env(theme_today)
    if explicit:
        pools = [(explicit, 100)]
        plan = _weighted_pick_unique(seed + "|EXPLICIT", pools, plan_n)
        if len(plan) < plan_n:
            # Fill remainder using the legacy weighted pools for diversity.
            # This preserves existing behavior but prevents searches on implausible origins.
            legacy_seed = seed + "|LEGACY_FILL"
        else:
            return plan

    if theme_today == "surf":
        pools = [(sw, 80), (LONDON_LCC, 20)]
    elif theme_today == "snow":
        pools = [(LONDON_LCC + MIDLANDS, 65), (NORTH + SCOTLAND, 25), (sw, 10)]
    elif theme_today == "winter_sun":
        pools = [(LONDON_LCC + MIDLANDS, 45), (NORTH, 35), (sw, 20)]
    elif theme_today == "summer_sun" or theme_today == "beach_break":
        pools = [(sw, 50), (LONDON_LCC + MIDLANDS, 30), (NORTH, 20)]
    elif theme_today == "city_breaks" or theme_today == "culture_history":
        pools = [(LONDON_LCC + MIDLANDS, 45), (NORTH + SCOTLAND, 35), (sw, 20)]
    elif theme_today == "northern_lights":
        pools = [(LONDON_FULL + NORTH + SCOTLAND + MIDLANDS, 80), (sw, 20)]
    elif theme_today == "adventure":
        pools = [(LONDON_FULL + MIDLANDS + NORTH, 60), (sw, 40)]
    elif theme_today == "long_haul":
        # Long-haul is hub-led: avoid wasting searches on low-liquidity regional origins for true long-haul markets.
        # Use realistic hubs first (LHR/LGW), then MAN as secondary.
        pools = [(LONGHAUL_PRIMARY_HUBS, 80), (LONGHAUL_SECONDARY_HUBS, 20)]
    elif theme_today == "luxury_value" or theme_today == "unexpected_value":
        pools = [(LONDON_FULL + MIDLANDS, 50), (NORTH + SCOTLAND, 30), (sw, 20)]
    else:
        pools = [(sw, 50), (LONDON_LCC + MIDLANDS, 30), (NORTH + SCOTLAND, 20)]

    legacy_seed_use = locals().get("legacy_seed", seed)
    legacy_plan = _weighted_pick_unique(legacy_seed_use, pools, plan_n)

    # If explicit origins were provided, keep them as the front of the plan and fill the remainder.
    if "plan" in locals():
        for o in legacy_plan:
            if o not in plan:
                plan.append(o)
            if len(plan) >= plan_n:
                break
    else:
        plan = legacy_plan


    if len(plan) < min(5, plan_n):
        broad = _dedupe_keep_order(sw + LONDON_LCC + MIDLANDS + NORTH + SCOTLAND + NI)
        extra_seed = seed + "|FILL"
        extras = _weighted_pick_unique(extra_seed, [(broad, 1)], plan_n)
        for o in extras:
            if o not in plan:
                plan.append(o)
            if len(plan) >= plan_n:
                break

    return plan


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
        raise RuntimeError("Missing GCP_SA_JSON or GCP_SA_JSON_ONE_LINE")
    sa = _parse_sa_json(raw)
    return gspread.service_account_from_dict(sa)


def ws_by_title(gc: gspread.Client, title: str) -> gspread.Worksheet:
    sh_id = (os.getenv("SHEET_ID") or os.getenv("SPREADSHEET_ID") or "").strip()
    if not sh_id:
        raise RuntimeError("Missing SHEET_ID/SPREADSHEET_ID")
    sh = gc.open_by_key(sh_id)
    return sh.worksheet(title)


def ws_rows(ws: gspread.Worksheet) -> List[Dict[str, Any]]:
    vals = ws.get_all_values()
    if not vals or len(vals) < 2:
        return []
    header = [h.strip() for h in vals[0]]
    out: List[Dict[str, Any]] = []
    for row in vals[1:]:
        d: Dict[str, Any] = {}
        for i, h in enumerate(header):
            d[h] = row[i] if i < len(row) else ""
        out.append(d)
    return out


def ws_append_rows(ws: gspread.Worksheet, rows: List[List[Any]]) -> None:
    if not rows:
        return
    ws.append_rows(rows, value_input_option="USER_ENTERED")


def ws_clear(ws: gspread.Wo_
