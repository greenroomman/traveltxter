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
- Hard cap on total itinerary duration minutes (short-haul vs long-haul)
- Inner "quality band" under the benchmark cap (short-haul vs long-haul)

Everything else is unchanged.

PATCH (2026-01-19):
✅ Anchor-origins guarantee in route selection so low-liquidity origins (EXT/CWL/LPL) cannot consume the
entire quota before hub airports (LGW/STN/LTN/MAN etc.) are considered.
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


# ==================== ENV / TABS ====================

RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS").strip() or "RAW_DEALS"
CONFIG_TAB = os.getenv("FEEDER_CONFIG_TAB", os.getenv("CONFIG_TAB", "CONFIG")).strip() or "CONFIG"
THEMES_TAB = os.getenv("THEMES_TAB", "THEMES").strip() or "THEMES"
SIGNALS_TAB = os.getenv("SIGNALS_TAB", os.getenv("CONFIG_SIGNALS_TAB", "CONFIG_SIGNALS")).strip() or "CONFIG_SIGNALS"
CAPABILITY_TAB = os.getenv("CAPABILITY_TAB", "ROUTE_CAPABILITY_MAP").strip() or "ROUTE_CAPABILITY_MAP"
BENCHMARKS_TAB = os.getenv("BENCHMARKS_TAB", "ZONE_THEME_BENCHMARKS").strip() or "ZONE_THEME_BENCHMARKS"

SPREADSHEET_ID = (os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID") or "").strip()

DUFFEL_API_KEY = os.getenv("DUFFEL_API_KEY", "").strip()
DUFFEL_API_BASE = os.getenv("DUFFEL_API_BASE", "https://api.duffel.com").strip()
DUFFEL_VERSION = os.getenv("DUFFEL_VERSION", "v2").strip() or "v2"

RUN_SLOT = (os.getenv("RUN_SLOT") or "").strip().upper()


# ==================== GOVERNORS (SAFE CASTING) ====================

def _get_int(primary: str, fallback: str, default: int) -> int:
    v = (os.getenv(primary) or "").strip()
    if v:
        try:
            return int(float(v))
        except Exception:
            pass
    v2 = (os.getenv(fallback) or "").strip()
    if v2:
        try:
            return int(float(v2))
        except Exception:
            pass
    return int(default)


def _get_float(primary: str, fallback: str, default: float) -> float:
    v = (os.getenv(primary) or "").strip()
    if v:
        try:
            return float(v)
        except Exception:
            pass
    v2 = (os.getenv(fallback) or "").strip()
    if v2:
        try:
            return float(v2)
        except Exception:
            pass
    return float(default)


DUFFEL_MAX_INSERTS = _get_int("DUFFEL_MAX_INSERTS", "FEEDER_MAX_INSERTS", 3)
DUFFEL_MAX_SEARCHES_PER_RUN = _get_int("DUFFEL_MAX_SEARCHES_PER_RUN", "FEEDER_MAX_SEARCHES", 4)
DUFFEL_ROUTES_PER_RUN = _get_int("DUFFEL_ROUTES_PER_RUN", "FEEDER_ROUTES_PER_RUN", 3)

DUFFEL_OFFERS_PER_SEARCH = _get_int("DUFFEL_OFFERS_PER_SEARCH", "OFFERS_PER_SEARCH", 50)
DUFFEL_MAX_INSERTS_PER_ROUTE = _get_int("DUFFEL_MAX_INSERTS_PER_ROUTE", "MAX_INSERTS_PER_ROUTE", 10)
DUFFEL_MAX_INSERTS_PER_ORIGIN = _get_int("DUFFEL_MAX_INSERTS_PER_ORIGIN", "MAX_INSERTS_PER_ORIGIN", 10)

FEEDER_SLEEP_SECONDS = _get_float("FEEDER_SLEEP_SECONDS", "FEEDER_SLEEP_SECONDS", 0.0)

FEEDER_OPEN_ORIGINS = (os.getenv("FEEDER_OPEN_ORIGINS", "false").strip().lower() == "true")
STRICT_CAPABILITY_MAP = (os.getenv("STRICT_CAPABILITY_MAP", "true").strip().lower() == "true")
RESPECT_CONFIG_ORIGIN = (os.getenv("RESPECT_CONFIG_ORIGIN", "false").strip().lower() == "true")

DEFAULT_DAYS_AHEAD_MIN = _get_int("DAYS_AHEAD_MIN", "DAYS_AHEAD_MIN", 21)
DEFAULT_DAYS_AHEAD_MAX = _get_int("DAYS_AHEAD_MAX", "DAYS_AHEAD_MAX", 84)
DEFAULT_TRIP_LENGTH_DAYS = _get_int("TRIP_LENGTH_DAYS", "TRIP_LENGTH_DAYS", 5)

INVENTORY_MIN_DAYS = _get_int("INVENTORY_MIN_DAYS", "INVENTORY_MIN_DAYS", 21)
INVENTORY_MAX_DAYS = _get_int("INVENTORY_MAX_DAYS", "INVENTORY_MAX_DAYS", 84)

FEEDER_EXPLORE_RUN_MOD = _get_int("FEEDER_EXPLORE_RUN_MOD", "FEEDER_EXPLORE_RUN_MOD", 10)
FEEDER_EXPLORE_SALT = (os.getenv("FEEDER_EXPLORE_SALT", "traveltxter") or "traveltxter").strip()

PRICE_GATE_ENABLED = (os.getenv("PRICE_GATE_ENABLED", "true").strip().lower() == "true")
PRICE_GATE_MULTIPLIER = _get_float("PRICE_GATE_MULTIPLIER", "PRICE_GATE_MULTIPLIER", 1.0)
PRICE_GATE_MIN_CAP_GBP = _get_float("PRICE_GATE_MIN_CAP_GBP", "PRICE_GATE_MIN_CAP_GBP", 80.0)
PRICE_GATE_FALLBACK_BEHAVIOR = (os.getenv("PRICE_GATE_FALLBACK_BEHAVIOR", "BLOCK").strip().upper() or "BLOCK")

ZERO_OFFER_RETRY_ENABLED = (os.getenv("ZERO_OFFER_RETRY_ENABLED", "true").strip().lower() == "true")
ZERO_OFFER_RETRY_MAX_DAYS = _get_int("ZERO_OFFER_RETRY_MAX_DAYS", "ZERO_OFFER_RETRY_MAX_DAYS", 60)

# ==================== HYGIENE GATE (NEW) ====================
# Goal: prevent obviously doomed offers entering RAW_DEALS (connections/duration/near-cap filler).
HYGIENE_ENABLED = (os.getenv("HYGIENE_ENABLED", "true").strip().lower() == "true")

# Connections: short-haul vs long-haul
OFFER_MAX_CONNECTIONS_SHORTHAUL = _get_int("OFFER_MAX_CONNECTIONS_SHORTHAUL", "OFFER_MAX_CONNECTIONS_SHORTHAUL", 1)
OFFER_MAX_CONNECTIONS_LONGHAUL = _get_int("OFFER_MAX_CONNECTIONS_LONGHAUL", "OFFER_MAX_CONNECTIONS_LONGHAUL", 2)

# Duration minutes (total itinerary across slices)
OFFER_MAX_DURATION_MINUTES_SHORTHAUL = _get_int("OFFER_MAX_DURATION_MINUTES_SHORTHAUL", "OFFER_MAX_DURATION_MINUTES_SHORTHAUL", 720)   # 12h
OFFER_MAX_DURATION_MINUTES_LONGHAUL = _get_int("OFFER_MAX_DURATION_MINUTES_LONGHAUL", "OFFER_MAX_DURATION_MINUTES_LONGHAUL", 1200)     # 20h

# Inner "quality band" under cap (keep filler out). Set to 1.0 to disable band filtering.
QUALITY_PRICE_BAND_SHORTHAUL = _get_float("QUALITY_PRICE_BAND_SHORTHAUL", "QUALITY_PRICE_BAND_SHORTHAUL", 0.85)
QUALITY_PRICE_BAND_LONGHAUL = _get_float("QUALITY_PRICE_BAND_LONGHAUL", "QUALITY_PRICE_BAND_LONGHAUL", 0.95)


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

SPARSE_THEMES = {"northern_lights"}  # kept from baseline

# Pools
SW_ENGLAND_DEFAULT = ["BRS", "EXT", "NQY", "SOU", "CWL", "BOH"]
LONDON_LCC = ["STN", "LTN", "LGW"]
LONDON_FULL = ["LHR", "LGW", "STN", "LTN", "LCY", "SEN"]
MIDLANDS = ["BHX", "EMA"]
NORTH = ["MAN", "LBA", "NCL", "LPL"]
SCOTLAND = ["EDI", "GLA"]
NI = ["BFS", "BHD"]

ADVENTURE_HUBS = ["LGW", "LHR", "STN", "LTN", "MAN", "BHX"]

ORIGIN_CITY_FALLBACK = {
    "LHR": "London", "LGW": "London", "STN": "London", "LTN": "London", "LCY": "London", "SEN": "London",
    "MAN": "Manchester", "BHX": "Birmingham", "EMA": "East Midlands", "LPL": "Liverpool", "LBA": "Leeds",
    "NCL": "Newcastle", "EDI": "Edinburgh", "GLA": "Glasgow", "BFS": "Belfast", "BHD": "Belfast",
    "BRS": "Bristol", "EXT": "Exeter", "NQY": "Newquay", "SOU": "Southampton", "CWL": "Cardiff", "BOH": "Bournemouth",
}


# ==================== LOGGING ====================

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


def _clean_iata(x: Any) -> str:
    return str(x or "").strip().upper()[:3]


def _dedupe_keep_order(seq: List[str]) -> List[str]:
    out: List[str] = []
    for x in seq:
        xx = _clean_iata(x)
        if not xx:
            continue
        if xx not in out:
            out.append(xx)
    return out


def _stable_mod(key: str, mod: int) -> int:
    h = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return int(h[:8], 16) % max(1, mod)


def _sw_england_origins_from_env() -> List[str]:
    raw = (os.getenv("SW_ENGLAND_ORIGINS") or "").strip()
    if not raw:
        return SW_ENGLAND_DEFAULT
    parts = [p.strip().upper() for p in raw.split(",") if p.strip()]
    return _dedupe_keep_order(parts) or SW_ENGLAND_DEFAULT


def _weighted_pick_unique(seed: str, pools: List[Tuple[List[str], int]], k: int) -> List[str]:
    # Deterministic weighted pick without replacement.
    candidates: List[Tuple[int, str]] = []
    for items, weight in pools:
        for o in items:
            key = f"{seed}|{o}"
            score = int(hashlib.sha256(key.encode("utf-8")).hexdigest()[:8], 16)
            candidates.append((score * max(1, int(weight)), o))
    candidates.sort(reverse=True, key=lambda t: t[0])
    out: List[str] = []
    for _, o in candidates:
        if o not in out:
            out.append(o)
        if len(out) >= k:
            break
    return out


def required_unique_origins() -> int:
    if DUFFEL_MAX_INSERTS_PER_ORIGIN <= 0:
        return 1
    return int(math.ceil(DUFFEL_MAX_INSERTS / float(DUFFEL_MAX_INSERTS_PER_ORIGIN)))


def effective_routes_per_run() -> int:
    req = required_unique_origins()
    eff = max(DUFFEL_ROUTES_PER_RUN, req)
    eff = min(eff, max(1, DUFFEL_MAX_SEARCHES_PER_RUN))
    return eff


def origin_plan_for_theme(theme_today: str, plan_n: int) -> List[str]:
    sw = _dedupe_keep_order(_sw_england_origins_from_env())
    seed = f"{dt.datetime.utcnow().date().isoformat()}|{RUN_SLOT}|{theme_today}|ORIGIN_PLAN"

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
        pools = [(LONDON_FULL, 65), (NORTH + MIDLANDS + SCOTLAND, 35)]
    elif theme_today == "luxury_value" or theme_today == "unexpected_value":
        pools = [(LONDON_FULL + MIDLANDS, 50), (NORTH + SCOTLAND, 30), (sw, 20)]
    else:
        pools = [(sw, 50), (LONDON_LCC + MIDLANDS, 30), (NORTH + SCOTLAND, 20)]

    plan = _weighted_pick_unique(seed, pools, plan_n)

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
    ws = sheet.worksheet(THEMES_TAB)
    rows = ws.get_all_records()
    themes: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        t = str(r.get("theme", "")).strip()
        if not t:
            continue
        themes.setdefault(t, []).append(r)
    return themes


def load_signals(sheet: gspread.Spreadsheet) -> Dict[str, Dict[str, Any]]:
    ws = sheet.worksheet(SIGNALS_TAB)
    rows = ws.get_all_records()
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        # V4.6 compat: some Sheets exports name the IATA column `iata_hint`.
        # Accept both without changing tabs/headers.
        key = _clean_iata(r.get("iata_hint") or r.get("destination_iata") or r.get("iata") or "")
        if not key:
            continue
        out[key] = r
    return out


def load_capability_map(sheet: gspread.Spreadsheet) -> Tuple[Set[Tuple[str, str]], Dict[str, str], Dict[str, str]]:
    ws = sheet.worksheet(CAPABILITY_TAB)
    rows = ws.get_all_records()
    allowed_pairs: Set[Tuple[str, str]] = set()
    origin_city_map: Dict[str, str] = {}
    dest_city_map: Dict[str, str] = {}
    for r in rows:
        o = _clean_iata(r.get("origin_iata"))
        d = _clean_iata(r.get("destination_iata"))
        if o and d:
            allowed_pairs.add((o, d))
        oc = str(r.get("origin_city") or "").strip()
        dc = str(r.get("destination_city") or "").strip()
        if o and oc:
            origin_city_map[o] = oc
        if d and dc:
            dest_city_map[d] = dc
    return allowed_pairs, origin_city_map, dest_city_map


def load_benchmarks(sheet: gspread.Spreadsheet) -> List[Dict[str, Any]]:
    ws = sheet.worksheet(BENCHMARKS_TAB)
    return ws.get_all_records()


def _a1(col_idx_1: int, row_idx_1: int) -> str:
    col = ""
    n = int(col_idx_1)
    while n > 0:
        n, r = divmod(n - 1, 26)
        col = chr(65 + r) + col
    return f"{col}{int(row_idx_1)}"


def a1_update(ws: gspread.Worksheet, row_idx_1: int, col_idx_1: int, value: Any) -> None:
    a1 = _a1(col_idx_1, row_idx_1)
    ws.update([[value]], a1)


# ==================== DUFFEL ====================

def duffel_headers() -> Dict[str, str]:
    if not DUFFEL_API_KEY:
        raise RuntimeError("Missing DUFFEL_API_KEY")
    return {
        "Authorization": f"Bearer {DUFFEL_API_KEY}",
        "Duffel-Version": DUFFEL_VERSION,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def duffel_search_offer_request(payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{DUFFEL_API_BASE}/air/offer_requests"
    resp = requests.post(url, headers=duffel_headers(), json=payload, timeout=60)
    try:
        data = resp.json()
    except Exception:
        raise RuntimeError(f"Duffel bad JSON response: status={resp.status_code} body={resp.text[:500]}")
    if resp.status_code >= 400:
        raise RuntimeError(f"Duffel API error: status={resp.status_code} body={json.dumps(data)[:1000]}")
    return data


# ==================== PRICE GATE / BENCHMARKS ====================

def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def compute_ingest_cap_gbp(benchmarks: List[Dict[str, Any]], deal_theme: str, origin: str, dest: str) -> Optional[float]:
    t = str(deal_theme or "").strip()
    o = _clean_iata(origin)
    d = _clean_iata(dest)

    # Match by theme + origin. Destination examples are informational only.
    rows = [r for r in benchmarks if str(r.get("theme") or "").strip() == t and _clean_iata(r.get("origin_iata")) == o]
    if not rows:
        return None

    # Prefer any row that includes destination in destination_examples list; else fall back to first row.
    chosen = None
    for r in rows:
        examples = str(r.get("destination_examples") or "").upper()
        if d and d in examples:
            chosen = r
            break
    if chosen is None:
        chosen = rows[0]

    max_price = _to_float(chosen.get("max_price"))
    if max_price is None:
        return None

    cap = max_price * float(PRICE_GATE_MULTIPLIER)
    cap = max(float(PRICE_GATE_MIN_CAP_GBP), cap)
    return cap


# ==================== OFFER HYGIENE ====================

def _offer_total_connections(offer: Dict[str, Any]) -> int:
    slices = offer.get("slices") or []
    total_segments = 0
    for s in slices:
        segs = s.get("segments") or []
        total_segments += len(segs)
    # connections per slice are segments-1; for two slices this is fine as a hard filter
    return max(0, total_segments - len(slices))


def _offer_total_duration_minutes(offer: Dict[str, Any]) -> int:
    slices = offer.get("slices") or []
    total = 0
    for s in slices:
        dur = s.get("duration") or ""
        # Duffel durations are ISO8601 like PT5H30M
        if isinstance(dur, str) and dur.startswith("PT"):
            h = 0
            m = 0
            s2 = dur[2:]
            if "H" in s2:
                parts = s2.split("H")
                try:
                    h = int(parts[0] or 0)
                except Exception:
                    h = 0
                s2 = parts[1] if len(parts) > 1 else ""
            if "M" in s2:
                parts = s2.split("M")
                try:
                    m = int(parts[0] or 0)
                except Exception:
                    m = 0
            total += (h * 60 + m)
    return int(total)


def _offer_amount_gbp(offer: Dict[str, Any]) -> Optional[float]:
    total_amount = _to_float(offer.get("total_amount"))
    currency = str(offer.get("total_currency") or "").upper().strip()
    if currency != "GBP" or total_amount is None:
        return None
    return float(total_amount)


# ==================== MAIN PIPELINE ====================

def main() -> int:
    log("================================================================================")
    log("TRAVELTXTTER PIPELINE WORKER (FEEDER) START")
    log("================================================================================")

    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")

    theme_today = str(os.getenv("THEME_OF_DAY") or "").strip()
    if not theme_today:
        # Deterministic theme-of-day rotation using UTC date
        today = dt.datetime.utcnow().date().isoformat()
        idx = _stable_mod(today + "|" + RUN_SLOT, len(MASTER_THEMES))
        theme_today = MASTER_THEMES[idx]
    log(f"🎯 Theme of the day (UTC): {theme_today}")

    eff_routes = effective_routes_per_run()
    req_origins = required_unique_origins()
    plan_n = max(5, req_origins, eff_routes)

    # Explore run 90/10 style
    today_key = dt.datetime.utcnow().date().isoformat()
    explore_run = (_stable_mod(f"{FEEDER_EXPLORE_SALT}|{today_key}|{RUN_SLOT}|MOD", FEEDER_EXPLORE_RUN_MOD) == 0)
    theme_quota = max(0, int(eff_routes) - (1 if explore_run else 0))
    explore_quota = (1 if explore_run else 0)

    log(f"ORIGIN_POLICY: FEEDER_OPEN_ORIGINS={FEEDER_OPEN_ORIGINS} | sparse_override=False | effective_open={FEEDER_OPEN_ORIGINS}")
    log(f"CAPS: MAX_INSERTS={DUFFEL_MAX_INSERTS} | PER_ORIGIN={DUFFEL_MAX_INSERTS_PER_ORIGIN} | PER_ROUTE={DUFFEL_MAX_INSERTS_PER_ROUTE} | MAX_SEARCHES={DUFFEL_MAX_SEARCHES_PER_RUN} | ROUTES_PER_RUN(env)={DUFFEL_ROUTES_PER_RUN} | ROUTES_PER_RUN(effective)={eff_routes}")
    log(f"CAPACITY_NOTE: theoretical_max_inserts_this_run <= {min(DUFFEL_MAX_INSERTS, eff_routes * DUFFEL_MAX_INSERTS_PER_ROUTE)} (based on caps + effective routes)")
    log(f"PRICE_GATE: fallback={PRICE_GATE_FALLBACK_BEHAVIOR} | mult={PRICE_GATE_MULTIPLIER} | mincap={PRICE_GATE_MIN_CAP_GBP}")
    log(f"HYGIENE: enabled={HYGIENE_ENABLED} | conn_short={OFFER_MAX_CONNECTIONS_SHORTHAUL} conn_long={OFFER_MAX_CONNECTIONS_LONGHAUL} | dur_short={OFFER_MAX_DURATION_MINUTES_SHORTHAUL} dur_long={OFFER_MAX_DURATION_MINUTES_LONGHAUL} | band_short={QUALITY_PRICE_BAND_SHORTHAUL} band_long={QUALITY_PRICE_BAND_LONGHAUL}")
    log(f"INVENTORY_WINDOW_DAYS={INVENTORY_MIN_DAYS}-{INVENTORY_MAX_DAYS} | ZERO_OFFER_RETRY_ENABLED={ZERO_OFFER_RETRY_ENABLED} retry_window_max={ZERO_OFFER_RETRY_MAX_DAYS}")
    log(f"🧠 Strategy: 90/10 | explore_run={explore_run} | theme_quota={theme_quota} | explore_quota={explore_quota} | MOD={FEEDER_EXPLORE_RUN_MOD}")

    gc = gs_client()
    sheet = gc.open_by_key(SPREADSHEET_ID)

    config_rows = load_config_rows(sheet)
    themes_dict = load_themes_dict(sheet)
    signals = load_signals(sheet)

    allowed_pairs: Set[Tuple[str, str]] = set()
    origin_city_map: Dict[str, str] = {}
    dest_city_map: Dict[str, str] = {}

    if STRICT_CAPABILITY_MAP:
        allowed_pairs, origin_city_map, dest_city_map = load_capability_map(sheet)

    benchmarks: List[Dict[str, Any]] = []
    if PRICE_GATE_ENABLED:
        benchmarks = load_benchmarks(sheet)

    # Build route candidates from CONFIG (theme + explore)
    theme_routes: List[Dict[str, Any]] = []
    explore_routes: List[Dict[str, Any]] = []

    for r in config_rows:
        t = str(r.get("theme") or "").strip()
        if not t:
            continue
        if t == theme_today:
            theme_routes.append(r)
        else:
            explore_routes.append(r)

    def unique_dest_configs(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        seen: Set[str] = set()
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

    planned_origins = origin_plan_for_theme(theme_today, plan_n)
    log(f"🧭 Planned origins for run ({len(planned_origins)}; required={plan_n}): {planned_origins}")
    log(f"🧭 Unique theme destinations: {len(theme_dest_configs)} | Unique explore destinations: {len(explore_dest_configs)}")

    # -------- RULE B SELECTION (one per origin first) --------

    # Anchor-origins guarantee: prevent low-liquidity origins consuming the whole quota
    # (Observed failure mode: quota fills on EXT/CWL/LPL before STN/LGW are reached).
    THEME_ANCHOR_ORIGINS = {
        # Short-haul city deals: ensure at least one London LCC origin is considered first
        "city_breaks": ["LGW", "STN", "LTN"],
        "culture_history": ["LGW", "STN", "LTN"],
        # Winter sun often strongest from major hubs (MAN/London)
        "winter_sun": ["MAN", "LGW", "STN", "LTN"],
        # Long haul: full-service hubs
        "long_haul": ["LHR", "LGW", "MAN"],
        # Snow: hubs + Scotland
        "snow": ["LGW", "MAN", "EDI", "GLA", "STN", "LTN"],
        # Surf: SW-first is intentional (but still allow London fallback)
        "surf": ["BRS", "NQY", "EXT", "LGW", "STN", "LTN"],
    }

    def selection_origins_for_theme(theme_key: str) -> List[str]:
        anchors = THEME_ANCHOR_ORIGINS.get(theme_key, [])
        # Keep order: anchors first (if planned), then the rest
        ordered: List[str] = []
        for a in anchors:
            if a in planned_origins and a not in ordered:
                ordered.append(a)
        for o in planned_origins:
            if o not in ordered:
                ordered.append(o)
        return ordered

    def route_precheck_ok(origin: str, dest: str, deal_theme: str) -> bool:
        if allowed_pairs and (origin, dest) not in allowed_pairs:
            return False
        if PRICE_GATE_ENABLED:
            if not benchmarks:
                return PRICE_GATE_FALLBACK_BEHAVIOR != "BLOCK"
            cap = compute_ingest_cap_gbp(benchmarks, deal_theme, origin, dest)
            if cap is None and PRICE_GATE_FALLBACK_BEHAVIOR == "BLOCK":
                return False
        return True

    def select_routes_rule_b(dest_configs: List[Dict[str, Any]], quota: int) -> List[Tuple[str, str, Dict[str, Any]]]:
        if quota <= 0 or not dest_configs:
            return []

        picked: List[Tuple[str, str, Dict[str, Any]]] = []
        used_dests: Set[str] = set()

        sel_origins = selection_origins_for_theme(theme_today)

        for origin in sel_origins:
            if len(picked) >= quota:
                break
            for cfg in dest_configs:
                dest = _clean_iata(cfg.get("destination_iata"))
                if not dest or dest in used_dests:
                    continue
                deal_theme = str(cfg.get("theme") or theme_today).strip() or theme_today
                if not route_precheck_ok(origin, dest, deal_theme):
                    continue
                picked.append((origin, dest, cfg))
                used_dests.add(dest)
                break

        safety = 0
        while len(picked) < quota and safety < 5000:
            safety += 1
            progressed = False
            for origin in sel_origins:
                if len(picked) >= quota:
                    break
                for cfg in dest_configs:
                    dest = _clean_iata(cfg.get("destination_iata"))
                    if not dest or dest in used_dests:
                        continue
                    deal_theme = str(cfg.get("theme") or theme_today).strip() or theme_today
                    if not route_precheck_ok(origin, dest, deal_theme):
                        continue
                    picked.append((origin, dest, cfg))
                    used_dests.add(dest)
                    progressed = True
                    break
            if not progressed:
                break

        return picked

    selected_routes: List[Tuple[str, str, Dict[str, Any]]] = []
    selected_routes.extend(select_routes_rule_b(theme_dest_configs, theme_quota))

    if explore_quota > 0 and explore_dest_configs:
        today = dt.datetime.utcnow().date().isoformat()
        seed = f"{FEEDER_EXPLORE_SALT}|{today}|{RUN_SLOT}|{theme_today}|EXPLORE"
        offset = _stable_mod(seed, max(1, len(explore_dest_configs)))
        rotated = explore_dest_configs[offset:] + explore_dest_configs[:offset]
        selected_routes.extend(select_routes_rule_b(rotated, 1))

    if len(selected_routes) > DUFFEL_MAX_SEARCHES_PER_RUN:
        selected_routes = selected_routes[:DUFFEL_MAX_SEARCHES_PER_RUN]

    if not selected_routes:
        log("⚠️ No eligible routes after capability/benchmark/planned-origin filtering.")
        return 0

    searches_done = 0
    all_deals: List[Dict[str, Any]] = []
    inserted_by_origin: Dict[str, int] = {}

    def is_longhaul_theme(deal_theme: str) -> bool:
        t = str(deal_theme or "").strip()
        return t in {"long_haul", "adventure"}

    def hygiene_limits_for_theme(deal_theme: str) -> Tuple[int, int, float]:
        if is_longhaul_theme(deal_theme):
            return (
                max(0, int(OFFER_MAX_CONNECTIONS_LONGHAUL)),
                max(0, int(OFFER_MAX_DURATION_MINUTES_LONGHAUL)),
                float(QUALITY_PRICE_BAND_LONGHAUL),
            )
        return (
            max(0, int(OFFER_MAX_CONNECTIONS_SHORTHAUL)),
            max(0, int(OFFER_MAX_DURATION_MINUTES_SHORTHAUL)),
            float(QUALITY_PRICE_BAND_SHORTHAUL),
        )

    def _random_like_date(seed: str, days_min: int, days_max: int) -> dt.date:
        days_min = max(0, int(days_min))
        days_max = max(days_min, int(days_max))
        span = max(1, days_max - days_min + 1)
        off = _stable_mod(seed, span) + days_min
        return dt.datetime.utcnow().date() + dt.timedelta(days=off)

    def _pick_dep_ret_dates(cfg: Dict[str, Any], origin: str, destination: str) -> Tuple[dt.date, dt.date]:
        days_ahead_min = int(cfg.get("days_ahead_min") or DEFAULT_DAYS_AHEAD_MIN)
        days_ahead_max = int(cfg.get("days_ahead_max") or DEFAULT_DAYS_AHEAD_MAX)
        trip_len = int(cfg.get("trip_length_days") or DEFAULT_TRIP_LENGTH_DAYS)

        # Guardrails
        days_ahead_min = max(INVENTORY_MIN_DAYS, days_ahead_min)
        days_ahead_max = min(INVENTORY_MAX_DAYS, days_ahead_max)
        days_ahead_max = max(days_ahead_min, days_ahead_max)
        trip_len = max(1, trip_len)

        seed = f"{dt.datetime.utcnow().date().isoformat()}|{RUN_SLOT}|{theme_today}|{origin}|{destination}|{trip_len}|{days_ahead_min}-{days_ahead_max}"
        dep = _random_like_date(seed, days_ahead_min, days_ahead_max)
        ret = dep + dt.timedelta(days=trip_len)
        return dep, ret

    def _compute_cap_for_route(cfg: Dict[str, Any], origin: str, destination: str) -> Optional[float]:
        if not PRICE_GATE_ENABLED:
            return None
        deal_theme = str(cfg.get("theme") or theme_today).strip() or theme_today
        cap = compute_ingest_cap_gbp(benchmarks, deal_theme, origin, destination)
        if cap is None and PRICE_GATE_FALLBACK_BEHAVIOR == "BLOCK":
            return None
        return cap

    def run_search(origin: str, destination: str, dep_date: dt.date, ret_date: dt.date, cfg: Dict[str, Any], cap_gbp: Optional[float]) -> Tuple[int, List[Dict[str, Any]]]:
        nonlocal searches_done, all_deals, inserted_by_origin

        max_conn = int(cfg.get("max_connections") or 0)
        payload = {
            "data": {
                "slices": [
                    {"origin": origin, "destination": destination, "departure_date": dep_date.isoformat()},
                    {"origin": destination, "destination": origin, "departure_date": ret_date.isoformat()},
                ],
                "passengers": [{"type": "adult"}],
                "cabin_class": (cfg.get("cabin_class") or "economy"),
                "max_connections": max_conn,
                "return_offers": True,
            }
        }

        log(f"Duffel[PRIMARY]: Searching {origin}->{destination} {dep_date.isoformat()}/{ret_date.isoformat()}")
        resp = duffel_search_offer_request(payload)
        searches_done += 1

        offers = (resp.get("data") or {}).get("offers") or []
        offers_returned = len(offers)
        if offers_returned == 0:
            log("Duffel[PRIMARY]: offers_returned=0")
            return 0, []

        deal_theme = str(cfg.get("theme") or theme_today).strip() or theme_today
        max_conn_hard, max_dur_hard, band = hygiene_limits_for_theme(deal_theme)

        gbp_offers: List[Dict[str, Any]] = []
        rejected_non_gbp = 0
        rejected_price = 0
        rejected_conn = 0
        rejected_dur = 0
        rejected_band = 0

        # Determine effective cap for logging and band filtering
        effective_cap = cap_gbp if cap_gbp is not None else None
        band_cap = None
        if effective_cap is not None and band is not None:
            try:
                band_cap = float(effective_cap) * float(band)
            except Exception:
                band_cap = None

        for offer in offers:
            amt = _offer_amount_gbp(offer)
            if amt is None:
                rejected_non_gbp += 1
                continue

            if effective_cap is not None and amt > float(effective_cap):
                rejected_price += 1
                continue

            if HYGIENE_ENABLED:
                total_conn = _offer_total_connections(offer)
                if max_conn_hard >= 0 and total_conn > max_conn_hard:
                    rejected_conn += 1
                    continue

                total_dur = _offer_total_duration_minutes(offer)
                if max_dur_hard > 0 and total_dur > max_dur_hard:
                    rejected_dur += 1
                    continue

                if band_cap is not None and amt > float(band_cap):
                    rejected_band += 1
                    continue

            gbp_offers.append(offer)

        if effective_cap is None:
            cap_str = "None"
        else:
            cap_str = str(int(round(float(effective_cap))))

        if band_cap is None:
            band_str = "1.0x"
        else:
            band_str = f\"{band:.2f}x\"

        log(
            f\"Duffel[PRIMARY]: offers_returned={offers_returned} gbp_offers={len(gbp_offers)} cap_gbp={cap_str} band_cap={band_str} "
            f\"rej_non_gbp={rejected_non_gbp} rej_price={rejected_price} rej_conn={rejected_conn} rej_dur={rejected_dur} rej_band={rejected_band} inserted=0\"
        )

        if not gbp_offers:
            return 0, []

        deals_out: List[Dict[str, Any]] = []
        for offer in gbp_offers[:DUFFEL_OFFERS_PER_SEARCH]:
            deals_out.append({
                "origin_iata": origin,
                "destination_iata": destination,
                "dep_date": dep_date.isoformat(),
                "ret_date": ret_date.isoformat(),
                "theme": deal_theme,
                "offer": offer,
            })
        return len(deals_out), deals_out

    # Load RAW_DEALS worksheet for append
    ws_raw = sheet.worksheet(RAW_DEALS_TAB)
    raw_headers = ws_raw.row_values(1)

    def header_idx(name: str) -> Optional[int]:
        try:
            return raw_headers.index(name) + 1
        except ValueError:
            return None

    # Core columns (best-effort; do not invent headers)
    col_status = header_idx("status")
    col_origin = header_idx("origin_iata")
    col_destination = header_idx("destination_iata")
    col_dep = header_idx("outbound_date") or header_idx("dep_date")
    col_ret = header_idx("inbound_date") or header_idx("ret_date")
    col_price = header_idx("price_gbp") or header_idx("price")
    col_theme = header_idx("theme") or header_idx("dynamic_theme")
    col_origin_city = header_idx("origin_city")
    col_destination_city = header_idx("destination_city")
    col_destination_country = header_idx("destination_country")
    col_source = header_idx("source")
    col_created = header_idx("created_at_utc") or header_idx("created_at")

    # Required minimum columns
    if not (col_status and col_origin and col_destination and col_dep and col_ret and col_price and col_theme):
        missing = []
        if not col_status: missing.append("status")
        if not col_origin: missing.append("origin_iata")
        if not col_destination: missing.append("destination_iata")
        if not col_dep: missing.append("outbound_date/dep_date")
        if not col_ret: missing.append("inbound_date/ret_date")
        if not col_price: missing.append("price_gbp/price")
        if not col_theme: missing.append("theme/dynamic_theme")
        raise RuntimeError(f"RAW_DEALS missing required headers: {missing}")

    inserted_total = 0

    for origin, destination, cfg in selected_routes:
        if searches_done >= DUFFEL_MAX_SEARCHES_PER_RUN:
            break
        if inserted_total >= DUFFEL_MAX_INSERTS:
            break

        cap = _compute_cap_for_route(cfg, origin, destination)
        dep_date, ret_date = _pick_dep_ret_dates(cfg, origin, destination)

        n, deals = run_search(origin, destination, dep_date, ret_date, cfg, cap)
        if n == 0:
            continue

        # Append deals (respect per-route and per-origin caps)
        inserted_this_route = 0
        for d in deals:
            if inserted_total >= DUFFEL_MAX_INSERTS:
                break
            if inserted_this_route >= DUFFEL_MAX_INSERTS_PER_ROUTE:
                break
            if inserted_by_origin.get(origin, 0) >= DUFFEL_MAX_INSERTS_PER_ORIGIN:
                break

            offer = d["offer"]
            amt = _offer_amount_gbp(offer)
            if amt is None:
                continue

            # Enrichment: cities/country via capability map + signals
            origin_city = resolve_origin_city(origin, origin_city_map)
            dest_city = (dest_city_map.get(destination) or "").strip()
            dest_country = ""

            sig = signals.get(destination) or {}
            if not dest_city:
                dest_city = str(sig.get("city") or sig.get("destination_city") or "").strip()
            dest_country = str(sig.get("country") or sig.get("destination_country") or "").strip()

            # Prepare row
            row = [""] * len(raw_headers)

            def setv(ci: Optional[int], val: Any) -> None:
                if ci is None:
                    return
                row[ci - 1] = val

            setv(col_status, "NEW")
            setv(col_origin, origin)
            setv(col_destination, destination)
            setv(col_dep, d["dep_date"])
            setv(col_ret, d["ret_date"])
            setv(col_price, round(float(amt), 2))
            setv(col_theme, d["theme"])
            setv(col_source, "DUFFEL")
            if col_created:
                setv(col_created, dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")

            if col_origin_city:
                setv(col_origin_city, origin_city)
            if col_destination_city:
                setv(col_destination_city, dest_city)
            if col_destination_country:
                setv(col_destination_country, dest_country)

            ws_raw.append_row(row, value_input_option="USER_ENTERED")
            inserted_total += 1
            inserted_this_route += 1
            inserted_by_origin[origin] = inserted_by_origin.get(origin, 0) + 1

            if FEEDER_SLEEP_SECONDS > 0:
                time.sleep(float(FEEDER_SLEEP_SECONDS))

            # Update log line with inserted count (last run_search logged inserted=0 for visibility)
            # (Keep log format stable; do not spam logs per insert)
        # end deals loop
    # end selected_routes loop

    log(f"✓ Searches completed: {searches_done}")
    log(f"✓ Deals collected: {inserted_total} (cap {DUFFEL_MAX_INSERTS})")
    if inserted_total == 0:
        log("⚠️ No deals passed gates for this run.")
    return inserted_total


if __name__ == "__main__":
    raise SystemExit(main())
