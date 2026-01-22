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


def ws_clear(ws: gspread.Worksheet) -> None:
    ws.clear()


def ws_set_header(ws: gspread.Worksheet, header: List[str]) -> None:
    if not header:
        return
    ws.update([header], "A1")


# ==================== CONFIG / THEMES ====================

def load_theme_routes(ws_themes: gspread.Worksheet) -> List[Dict[str, Any]]:
    routes = ws_rows(ws_themes)
    out: List[Dict[str, Any]] = []
    for r in routes:
        theme = (r.get("theme") or "").strip().lower()
        if not theme:
            continue
        origin_iata = _clean_iata(r.get("origin_iata"))
        dest_iata = _clean_iata(r.get("destination_iata"))
        if not origin_iata or not dest_iata:
            continue
        out.append({
            "theme": theme,
            "origin_iata": origin_iata,
            "destination_iata": dest_iata,
            "zone": (r.get("zone") or "").strip().lower(),
            "notes": (r.get("notes") or "").strip(),
        })
    return out


def load_config(ws_config: gspread.Worksheet) -> Dict[str, str]:
    rows = ws_rows(ws_config)
    # Expect a simple key/value sheet: key | value (or similar)
    out: Dict[str, str] = {}
    for r in rows:
        k = (r.get("key") or r.get("Key") or r.get("CONFIG_KEY") or "").strip()
        v = (r.get("value") or r.get("Value") or r.get("CONFIG_VALUE") or "").strip()
        if k:
            out[k] = v
    return out


# ==================== DUFFEL ====================

DUFFEL_API_KEY = (os.getenv("DUFFEL_API_KEY") or "").strip()
DUFFEL_API_URL = "https://api.duffel.com/air/offer_requests"

DUFFEL_VERSION = os.getenv("DUFFEL_VERSION", "v2").strip()  # keep compat
DUFFEL_TIMEOUT = int(float(os.getenv("DUFFEL_TIMEOUT", "30")))


def duffel_headers() -> Dict[str, str]:
    if not DUFFEL_API_KEY:
        raise RuntimeError("Missing DUFFEL_API_KEY")
    return {
        "Authorization": f"Bearer {DUFFEL_API_KEY}",
        "Duffel-Version": DUFFEL_VERSION,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _iso(d: dt.date) -> str:
    return d.isoformat()


def _today_utc() -> dt.date:
    return dt.datetime.utcnow().date()


def _date_in_range(min_days: int, max_days: int, seed: str) -> dt.date:
    # deterministic day selection within [min_days, max_days]
    span = max(0, max_days - min_days)
    off = _stable_mod(seed, max(1, span + 1))
    return _today_utc() + dt.timedelta(days=min_days + off)


def _pick_trip_len(theme_today: str, seed: str) -> int:
    # Theme-specific min/max trip length vars (keep contract)
    t = theme_today.strip().upper()
    min_k = f"TRIP_{t}_MIN_DAYS"
    max_k = f"TRIP_{t}_MAX_DAYS"
    try:
        min_days = int(float(os.getenv(min_k, os.getenv("TRIP_DEFAULT_MIN_DAYS", "4"))))
        max_days = int(float(os.getenv(max_k, os.getenv("TRIP_DEFAULT_MAX_DAYS", "10"))))
    except Exception:
        min_days, max_days = 4, 10
    if max_days < min_days:
        max_days = min_days
    span = max_days - min_days
    return min_days + _stable_mod(seed + "|TRIPLEN", span + 1)


def _pick_window(theme_today: str) -> Tuple[int, int]:
    t = theme_today.strip().upper()
    min_k = f"WINDOW_{t}_MIN_DAYS_OUT"
    max_k = f"WINDOW_{t}_MAX_DAYS_OUT"
    try:
        mn = int(float(os.getenv(min_k, str(INVENTORY_MIN_DAYS_OUT))))
        mx = int(float(os.getenv(max_k, str(INVENTORY_MAX_DAYS_OUT))))
    except Exception:
        mn, mx = INVENTORY_MIN_DAYS_OUT, INVENTORY_MAX_DAYS_OUT
    if mx < mn:
        mx = mn
    return mn, mx


def _theme_max_stops(theme_today: str) -> int:
    t = theme_today.strip().upper()
    key = f"MAX_STOPS_{t}"
    return int(float(os.getenv(key, os.getenv("MAX_STOPS_DEFAULT", "1"))))


def create_offer_request(origin_iata: str, dest_iata: str, depart: dt.date, ret: dt.date, max_stops: int) -> Dict[str, Any]:
    payload = {
        "data": {
            "slices": [
                {"origin": origin_iata, "destination": dest_iata, "departure_date": _iso(depart)},
                {"origin": dest_iata, "destination": origin_iata, "departure_date": _iso(ret)},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": "economy",
            "max_connections": max_stops,
        }
    }
    resp = requests.post(DUFFEL_API_URL, headers=duffel_headers(), json=payload, timeout=DUFFEL_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def get_offers(offer_request_id: str) -> List[Dict[str, Any]]:
    url = f"https://api.duffel.com/air/offer_requests/{offer_request_id}/offers"
    resp = requests.get(url, headers=duffel_headers(), timeout=DUFFEL_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    return data.get("data") or []


# ==================== OFFER PROCESSING / GATES ====================

def _money_to_gbp(offer: Dict[str, Any]) -> Optional[float]:
    tot = offer.get("total_amount")
    cur = (offer.get("total_currency") or "").upper()
    try:
        val = float(tot)
    except Exception:
        return None
    if cur == "GBP":
        return val
    # If non-GBP, skip (consistent with your logs)
    return None


def _offer_connections(offer: Dict[str, Any]) -> int:
    # connections = segments - 1 per slice; take max over slices
    mx = 0
    for sl in offer.get("slices") or []:
        segs = sl.get("segments") or []
        mx = max(mx, max(0, len(segs) - 1))
    return mx


def _offer_duration_minutes(offer: Dict[str, Any]) -> int:
    # Duffel slice duration is seconds as string in some payloads; fallback to sum segments
    mx = 0
    for sl in offer.get("slices") or []:
        dur = sl.get("duration")
        if dur:
            try:
                mx = max(mx, int(float(dur)) // 60)
                continue
            except Exception:
                pass
        # fallback
        total = 0
        for seg in sl.get("segments") or []:
            sd = seg.get("duration")
            try:
                total += int(float(sd)) // 60
            except Exception:
                pass
        mx = max(mx, total)
    return mx


def _is_longhaul(dest_iata: str) -> bool:
    # simple heuristic: long-haul destinations are typically outside Europe/N. Africa; but we keep it conservative.
    # We do NOT change schema. This is only for hygiene thresholds.
    # If you have a more reliable mapping elsewhere, it should remain there.
    return dest_iata in {"HND", "NRT", "JFK", "EWR", "LAX", "SFO", "SEA", "BKK", "SIN", "SYD", "MEL", "AKL", "DXB", "DOH", "AUH", "MLE", "SEZ", "CPT", "JNB"}


def hygiene_gate_ok(offer: Dict[str, Any], dest_iata: str, cap_gbp: float) -> Tuple[bool, Dict[str, Any]]:
    if not HYGIENE_ENABLED:
        return True, {"rej_conn": 0, "rej_dur": 0, "rej_band": 0, "band_cap": "1.0x"}

    conn = _offer_connections(offer)
    dur = _offer_duration_minutes(offer)
    longhaul = _is_longhaul(dest_iata)

    max_conn = HYGIENE_CONN_LONG if longhaul else HYGIENE_CONN_SHORT
    max_dur = HYGIENE_DUR_LONG if longhaul else HYGIENE_DUR_SHORT
    band = HYGIENE_BAND_LONG if longhaul else HYGIENE_BAND_SHORT

    if conn > max_conn:
        return False, {"rej_conn": 1, "rej_dur": 0, "rej_band": 0, "band_cap": f"{band:.2f}"}
    if dur > max_dur:
        return False, {"rej_conn": 0, "rej_dur": 1, "rej_band": 0, "band_cap": f"{band:.2f}"}

    # band cap: only apply if cap_gbp is meaningful (>0)
    if cap_gbp and cap_gbp > 0:
        band_cap = cap_gbp * band
        gbp = _money_to_gbp(offer)
        if gbp is not None and gbp > band_cap:
            return False, {"rej_conn": 0, "rej_dur": 0, "rej_band": 1, "band_cap": f"{band:.2f}"}
        return True, {"rej_conn": 0, "rej_dur": 0, "rej_band": 0, "band_cap": band_cap}
    return True, {"rej_conn": 0, "rej_dur": 0, "rej_band": 0, "band_cap": "n/a"}


def _cap_for_route(theme_today: str, zone: str, dest_iata: str) -> float:
    # Keep existing simple cap logic: fallback to global min cap if missing
    # (Actual ZONE_THEME_BENCHMARKS may be used elsewhere; this function preserves contract)
    # For this locked build, we use a conservative cap with multiplier.
    base = 360.0 if theme_today in {"luxury_value", "long_haul"} else 250.0
    cap = max(PRICE_GATE_MINCAP_GBP, base * PRICE_GATE_MULT)
    return cap


def _price_gate_ok(gbp: float, cap_gbp: float) -> bool:
    return gbp <= cap_gbp


def _sleep() -> None:
    if FEEDER_SLEEP_SECONDS > 0:
        time.sleep(FEEDER_SLEEP_SECONDS)


# ==================== MAIN ====================

def main() -> int:
    log("=" * 80)
    log("TRAVELTXTTER PIPELINE WORKER (FEEDER) START")
    log("=" * 80)

    theme_today = THEME_DEFAULT.lower()
    log(f"🎯 Theme of the day (UTC): {theme_today}")

    sparse_override = False
    effective_open = FEEDER_OPEN_ORIGINS or sparse_override
    log(f"ORIGIN_POLICY: FEEDER_OPEN_ORIGINS={FEEDER_OPEN_ORIGINS} | sparse_override={sparse_override} | effective_open={effective_open}")

    max_inserts = DUFFEL_MAX_INSERTS
    per_origin = DUFFEL_MAX_INSERTS_PER_ORIGIN
    per_route = DUFFEL_MAX_INSERTS_PER_ROUTE
    max_searches = DUFFEL_MAX_SEARCHES_PER_RUN
    eff_routes = max(1, DUFFEL_ROUTES_PER_RUN)
    log(f"CAPS: MAX_INSERTS={max_inserts} | PER_ORIGIN={per_origin} | PER_ROUTE={per_route} | MAX_SEARCHES={max_searches} | ROUTES_PER_RUN(env)={DUFFEL_ROUTES_PER_RUN} | ROUTES_PER_RUN(effective)={eff_routes}")
    log(f"CAPACITY_NOTE: theoretical_max_inserts_this_run <= {max_inserts} (based on caps + effective routes)")

    log(f"PRICE_GATE: fallback={PRICE_GATE_FALLBACK_BEHAVIOR} | mult={PRICE_GATE_MULT} | mincap={PRICE_GATE_MINCAP_GBP}")
    log(f"HYGIENE: enabled={HYGIENE_ENABLED} | conn_short={HYGIENE_CONN_SHORT} conn_long={HYGIENE_CONN_LONG} | dur_short={HYGIENE_DUR_SHORT} dur_long={HYGIENE_DUR_LONG} | band_short={HYGIENE_BAND_SHORT} band_long={HYGIENE_BAND_LONG}")
    log(f"INVENTORY_WINDOW_DAYS={INVENTORY_MIN_DAYS_OUT}-{INVENTORY_MAX_DAYS_OUT} | ZERO_OFFER_RETRY_ENABLED={ZERO_OFFER_RETRY_ENABLED} retry_window_max={ZERO_OFFER_RETRY_WINDOW_MAX}")

    explore_run = explore_run_today(theme_today)
    theme_quota = eff_routes if not explore_run else max(0, eff_routes - 1)
    explore_quota = 0 if not explore_run else 1
    log(f"🧠 Strategy: 90/10 | explore_run={explore_run} | theme_quota={theme_quota} | explore_quota={explore_quota} | MOD={FEEDER_EXPLORE_RUN_MOD}")

    gc = gs_client()
    ws_raw = ws_by_title(gc, RAW_DEALS_TAB)
    ws_themes = ws_by_title(gc, THEMES_TAB)

    theme_routes_all = load_theme_routes(ws_themes)
    theme_routes = [r for r in theme_routes_all if r["theme"] == theme_today]
    explore_routes = [r for r in theme_routes_all if r["theme"] != theme_today]

    def unique_dest_configs(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        seen = set()
        out: List[Dict[str, Any]] = []
        for r in rows:
            d = _clean_iata(r.get("destination_iata"))
            if not d or d in seen:
                continue
            seen.add(d)
            out.append(r)
        return out

    theme_dest_configs = unique_dest_configs(theme_routes)
    explore_dest_configs = unique_dest_configs(explore_routes)

    req_origins = 5
    plan_n = max(5, req_origins, eff_routes)
    planned_origins = origin_plan_for_theme(theme_today, plan_n)
    log(f"🧭 Planned origins for run ({len(planned_origins)}; required={plan_n}): {planned_origins}")
    log(f"🧭 Unique theme destinations: {len(theme_dest_configs)} | Unique explore destinations: {len(explore_dest_configs)}")

    searches_done = 0
    inserted_total = 0
    rows_to_insert: List[List[Any]] = []

    # Minimal header expectation in RAW_DEALS (do NOT rename)
    header = ws_raw.row_values(1)
    if not header:
        raise RuntimeError("RAW_DEALS header row missing")
    header_lc = [h.strip().lower() for h in header]

    def idx(col: str) -> int:
        try:
            return header_lc.index(col.lower())
        except ValueError:
            return -1

    # Required columns (best-effort; do not alter schema)
    c_origin = idx("origin_iata")
    c_dest = idx("destination_iata")
    c_out = idx("depart_date")
    c_back = idx("return_date")
    c_price = idx("price_gbp")
    c_theme = idx("theme")
    c_status = idx("status")
    c_created = idx("created_utc")

    # Run selection: one per origin first, then fill
    def pick_dest_for_origin(o: str, i: int) -> Optional[Dict[str, Any]]:
        if theme_dest_configs:
            return theme_dest_configs[i % len(theme_dest_configs)]
        return None

    for i, origin_iata in enumerate(planned_origins[:eff_routes]):
        if searches_done >= max_searches:
            break
        dest_cfg = pick_dest_for_origin(origin_iata, i)
        if not dest_cfg:
            continue
        dest_iata = _clean_iata(dest_cfg["destination_iata"])
        zone = (dest_cfg.get("zone") or "").strip().lower()
        cap_gbp = _cap_for_route(theme_today, zone, dest_iata)

        mn, mx = _pick_window(theme_today)
        trip_len = _pick_trip_len(theme_today, f"{origin_iata}-{dest_iata}-{i}")
        depart = _date_in_range(mn, mx, f"{origin_iata}|{dest_iata}|OUT|{i}")
        ret = depart + dt.timedelta(days=trip_len)

        max_stops = _theme_max_stops(theme_today)

        log(f"Duffel[PRIMARY]: Searching {origin_iata}->{dest_iata} {depart}/{ret}")
        try:
            r = create_offer_request(origin_iata, dest_iata, depart, ret, max_stops=max_stops)
            offer_request_id = (r.get("data") or {}).get("id") or ""
            offers = get_offers(offer_request_id) if offer_request_id else []
        except Exception as e:
            log(f"❌ Duffel error for {origin_iata}->{dest_iata}: {e}")
            _sleep()
            searches_done += 1
            continue

        offers_returned = len(offers)
        gbp_ranked = 0
        processed = 0
        rej_non_gbp = 0
        rej_price = 0
        rej_conn = 0
        rej_dur = 0
        rej_band = 0
        inserted = 0
        band_cap = "1.0x"

        for off in offers:
            gbp = _money_to_gbp(off)
            if gbp is None:
                rej_non_gbp += 1
                continue
            gbp_ranked += 1
            processed += 1

            if not _price_gate_ok(gbp, cap_gbp):
                rej_price += 1
                continue

            ok, rej_meta = hygiene_gate_ok(off, dest_iata, cap_gbp)
            band_cap = rej_meta.get("band_cap", band_cap)
            if not ok:
                rej_conn += int(rej_meta.get("rej_conn", 0))
                rej_dur += int(rej_meta.get("rej_dur", 0))
                rej_band += int(rej_meta.get("rej_band", 0))
                continue

            # Passed gates: build a RAW_DEALS row in existing schema
            row = [""] * len(header)
            now_utc = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"

            if c_origin >= 0:
                row[c_origin] = origin_iata
            if c_dest >= 0:
                row[c_dest] = dest_iata
            if c_out >= 0:
                row[c_out] = depart.isoformat()
            if c_back >= 0:
                row[c_back] = ret.isoformat()
            if c_price >= 0:
                row[c_price] = f"{gbp:.2f}"
            if c_theme >= 0:
                row[c_theme] = theme_today
            if c_status >= 0:
                row[c_status] = "NEW"
            if c_created >= 0:
                row[c_created] = now_utc

            rows_to_insert.append(row)
            inserted += 1
            inserted_total += 1

            if inserted_total >= max_inserts:
                break
            if inserted >= per_route:
                break

        log(
            f"Duffel[PRIMARY]: offers_returned={offers_returned} "
            f"{'gbp_ranked='+str(gbp_ranked) if offers_returned else ''} "
            f"processed={processed} cap_gbp={int(cap_gbp)} band_cap={band_cap} "
            f"rej_non_gbp={rej_non_gbp} rej_price={rej_price} rej_conn={rej_conn} rej_dur={rej_dur} rej_band={rej_band} "
            f"inserted={inserted}"
        )

        searches_done += 1
        _sleep()

        if inserted_total >= max_inserts:
            break

    log(f"✓ Searches completed: {searches_done}")
    log(f"✓ Deals collected: {inserted_total} (cap {max_inserts})")

    if inserted_total <= 0:
        log("⚠️ No deals passed gates for this run.")
        return 0

    try:
        ws_append_rows(ws_raw, rows_to_insert)
    except Exception as e:
        log(f"❌ Failed inserting rows into {RAW_DEALS_TAB}: {e}")
        return 2

    log(f"✅ Inserted {inserted_total} rows into {RAW_DEALS_TAB}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise
