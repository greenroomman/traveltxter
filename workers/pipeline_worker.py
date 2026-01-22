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
"""

from __future__ import annotations

# ==================== PYTHONPATH GUARD (GITHUB ACTIONS IMPORT CONTEXT) ====================
# When GitHub Actions runs: python workers/pipeline_worker.py
# Python does NOT treat workers/ as a package. If any sibling imports exist (utils.py etc),
# this guard makes them resolvable deterministically.
import os
import sys

WORKERS_DIR = os.path.dirname(os.path.abspath(__file__))
if WORKERS_DIR not in sys.path:
    sys.path.insert(0, WORKERS_DIR)

# ==================== STANDARD IMPORTS ====================

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
LONGHAUL_PRIMARY_HUBS = ["LHR", "LGW"]
LONGHAUL_SECONDARY_HUBS = ["MAN"]  # realistic UK long-haul feeder

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


def theme_of_day_utc() -> str:
    today = dt.datetime.utcnow().date()
    doy = int(today.strftime("%j"))
    return MASTER_THEMES[doy % len(MASTER_THEMES)]


def should_do_explore_this_run(theme_today: str) -> bool:
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


def _det_hash_score(seed: str, item: str) -> int:
    h = hashlib.sha256(f"{seed}|{item}".encode("utf-8")).hexdigest()
    return int(h[:10], 16)


def _weighted_pick_unique(seed: str, pools: List[Tuple[List[str], int]], k: int) -> List[str]:
    candidates: List[Tuple[int, str]] = []
    for origins, w in pools:
        w_int = max(0, int(w))
        for o in _dedupe_keep_order(origins):
            if w_int <= 0:
                continue
            score = _det_hash_score(seed, o) + (w_int * 1_000_000)
            candidates.append((score, o))

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
        # Long-haul is hub-led: avoid wasting searches on low-liquidity regional origins for true long-haul markets.
        # Use realistic hubs first (LHR/LGW), then MAN as secondary.
        pools = [(LONGHAUL_PRIMARY_HUBS, 80), (LONGHAUL_SECONDARY_HUBS, 20)]
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
        key = _clean_iata(
            r.get("destination_iata")
            or r.get("iata_hint")
            or r.get("iata")
        )
        if key:
            out[key] = r
    return out


def load_route_capability_map(sheet: gspread.Spreadsheet) -> Tuple[Set[Tuple[str, str]], Dict[str, str]]:
    ws = sheet.worksheet(CAPABILITY_TAB)
    rows = ws.get_all_records()

    allowed: Set[Tuple[str, str]] = set()
    origin_city_map: Dict[str, str] = {}

    for r in rows:
        o = _clean_iata(r.get("origin_iata"))
        d = _clean_iata(r.get("destination_iata"))
        oc = str(r.get("origin_city", "")).strip()
        if o and d:
            allowed.add((o, d))
            if oc and o not in origin_city_map:
                origin_city_map[o] = oc

    if not allowed:
        msg = f"{CAPABILITY_TAB} is empty or missing required headers"
        if STRICT_CAPABILITY_MAP:
            raise RuntimeError(msg)
        log(f"⚠️ {msg} — continuing WITHOUT capability filtering.")

    return allowed, origin_city_map


# ==================== BENCHMARKS ====================

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


def _split_examples(x: Any) -> List[str]:
    s = str(x or "").strip()
    if not s:
        return []
    parts = [p.strip().upper() for p in s.split(",")]
    return [p for p in parts if p]


def load_zone_theme_benchmarks(sheet: gspread.Spreadsheet) -> List[Dict[str, Any]]:
    ws = sheet.worksheet(BENCHMARKS_TAB)
    rows = ws.get_all_records()
    out: List[Dict[str, Any]] = []

    for r in rows:
        theme = str(r.get("theme") or "").strip()
        origin = _clean_iata(r.get("origin_iata") or "")
        examples = _split_examples(r.get("destination_examples") or "")

        max_price = _to_float(r.get("max_price"))
        error_price = _to_float(r.get("error_price")) or 0.0

        if not theme or not origin or max_price is None:
            continue

        out.append(
            {
                "theme": theme,
                "origin_iata": origin,
                "destination_examples": examples,
                "max_price": float(max_price),
                "error_price": float(error_price),
            }
        )
    return out


def compute_ingest_cap_gbp(benchmarks: List[Dict[str, Any]], theme: str, origin: str, destination: str) -> Optional[float]:
    t = str(theme or "").strip()
    o = _clean_iata(origin)
    d = _clean_iata(destination)

    best: Optional[Dict[str, Any]] = None
    fallback: Optional[Dict[str, Any]] = None

    for r in benchmarks:
        if str(r.get("theme", "")).strip() != t:
            continue
        if _clean_iata(r.get("origin_iata")) != o:
            continue
        if fallback is None:
            fallback = r

        examples = r.get("destination_examples") or []
        if examples and d in examples:
            best = r
            break

    chosen = best if best is not None else fallback
    if not chosen:
        return None

    base = float(chosen.get("max_price") or 0.0) + float(chosen.get("error_price") or 0.0)
    if base <= 0:
        return None

    cap = max(float(PRICE_GATE_MIN_CAP_GBP), base * float(PRICE_GATE_MULTIPLIER))
    return float(cap)


# ==================== DUFFEL ====================

def duffel_headers() -> Dict[str, str]:
    if not DUFFEL_API_KEY:
        raise RuntimeError("Missing DUFFEL_API_KEY")
    return {
        "Authorization": f"Bearer {DUFFEL_API_KEY}",
        "Duffel-Version": DUFFEL_VERSION,
        "Content-Type": "application/json",
    }


def duffel_search_offer_request(payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{DUFFEL_API_BASE}/air/offer_requests"
    r = requests.post(url, headers=duffel_headers(), json=payload, timeout=90)
    if r.status_code >= 400:
        raise RuntimeError(f"Duffel offer_requests failed: {r.status_code} {r.text[:500]}")
    return r.json()


def offer_price_gbp(offer: Dict[str, Any]) -> float:
    cur = str(offer.get("total_currency") or "").strip().upper()
    if cur != "GBP":
        return 0.0
    try:
        return float(offer.get("total_amount"))
    except Exception:
        return 0.0


def offer_connections_safe(offer: Dict[str, Any]) -> int:
    try:
        slices = offer.get("slices") or []
        if not slices:
            return 99
        if len(slices) >= 2:
            out_segs = len((slices[0] or {}).get("segments") or [])
            in_segs = len((slices[1] or {}).get("segments") or [])
            if out_segs <= 0 or in_segs <= 0:
                return 99
            return max(0, (out_segs - 1)) + max(0, (in_segs - 1))
        seg_total = 0
        for s in slices:
            seg_total += len((s or {}).get("segments") or [])
        return max(0, seg_total - 1) if seg_total > 0 else 99
    except Exception:
        return 99


def offer_duration_minutes_safe(offer: Dict[str, Any]) -> int:
    def parse_iso_dur(s: str) -> int:
        s = str(s or "").strip().upper()
        if not s.startswith("PT"):
            return 0
        s = s[2:]
        h = 0
        m = 0
        num = ""
        for ch in s:
            if ch.isdigit():
                num += ch
            elif ch == "H":
                h = int(num or "0"); num = ""
            elif ch == "M":
                m = int(num or "0"); num = ""
            else:
                num = ""
        return h * 60 + m

    try:
        slices = offer.get("slices") or []
        total = 0
        found = False
        for sl in slices:
            dur = (sl or {}).get("duration")
            if dur:
                found = True
                total += parse_iso_dur(dur)
        return total if found and total > 0 else 999999
    except Exception:
        return 999999


# ==================== ENRICH ====================

def enrich_deal(deal: Dict[str, Any], themes_dict: Dict[str, List[Dict[str, Any]]], signals: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    dest = _clean_iata(deal.get("destination_iata", ""))
    theme = str(deal.get("deal_theme") or deal.get("theme") or "").strip()

    if theme and theme in themes_dict:
        for d in themes_dict[theme]:
            if _clean_iata(d.get("destination_iata")) == dest:
                if d.get("destination_city"):
                    deal["destination_city"] = d["destination_city"]
                if d.get("destination_country"):
                    deal["destination_country"] = d["destination_country"]
                break

    if dest and dest in signals:
        s = signals[dest]
        if not deal.get("destination_city") and s.get("destination_city"):
            deal["destination_city"] = s["destination_city"]
        if not deal.get("destination_country") and s.get("destination_country"):
            deal["destination_country"] = s["destination_country"]

    return deal


# ==================== WRITE RAW_DEALS ====================

def append_rows_header_mapped(ws, deals: List[Dict[str, Any]]) -> int:
    if not deals:
        return 0
    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("RAW_DEALS header row is empty")

    rows: List[List[Any]] = []
    for d in deals:
        row: List[Any] = []
        for h in headers:
            key = str(h).strip()
            row.append(d.get(key, ""))
        rows.append(row)

    ws.append_rows(rows, value_input_option="RAW")
    return len(rows)


# ==================== DATE PICKING ====================

def clamp_window(days_min: int, days_max: int) -> Tuple[int, int]:
    mn = max(int(days_min), int(INVENTORY_MIN_DAYS))
    mx = min(int(days_max), int(INVENTORY_MAX_DAYS))
    if mx < mn:
        return int(days_min), int(days_max)
    return mn, mx


def pick_dates(seed: str, days_min: int, days_max: int, trip_len: int) -> Tuple[dt.date, dt.date]:
    hsh = int(hashlib.md5(seed.encode("utf-8")).hexdigest()[:8], 16)
    span = max(1, (days_max - days_min + 1))
    dep_offset = days_min + (hsh % span)
    dep_date = dt.datetime.utcnow().date() + dt.timedelta(days=dep_offset)
    ret_date = dep_date + dt.timedelta(days=int(trip_len))
    return dep_date, ret_date


# ==================== MAIN ====================

def main() -> int:
    log("=" * 80)
    log("TRAVELTXTTER PIPELINE WORKER (FEEDER) START")
    log("=" * 80)

    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")

    theme_today = theme_of_day_utc()
    log(f"🎯 Theme of the day (UTC): {theme_today}")

    sparse_theme_override = theme_today in SPARSE_THEMES
    open_origins_effective = bool(FEEDER_OPEN_ORIGINS or sparse_theme_override)

    req_origins = required_unique_origins()
    eff_routes = effective_routes_per_run()

    theoretical_max_by_routes = DUFFEL_MAX_INSERTS_PER_ROUTE * eff_routes
    theoretical_max_by_origins = DUFFEL_MAX_INSERTS_PER_ORIGIN * eff_routes
    theoretical_max = min(DUFFEL_MAX_INSERTS, theoretical_max_by_routes, theoretical_max_by_origins)

    if eff_routes != DUFFEL_ROUTES_PER_RUN:
        log(f"⚠️ CONFIG_GUARDRAIL: DUFFEL_ROUTES_PER_RUN={DUFFEL_ROUTES_PER_RUN} < required_origins={req_origins} "
            f"for DUFFEL_MAX_INSERTS={DUFFEL_MAX_INSERTS} @ {DUFFEL_MAX_INSERTS_PER_ORIGIN}/origin. "
            f"Auto-bumping effective_routes_per_run => {eff_routes} (capped by DUFFEL_MAX_SEARCHES_PER_RUN={DUFFEL_MAX_SEARCHES_PER_RUN}).")

    log(f"ORIGIN_POLICY: FEEDER_OPEN_ORIGINS={FEEDER_OPEN_ORIGINS} | sparse_override={sparse_theme_override} | effective_open={open_origins_effective}")
    log(f"CAPS: MAX_INSERTS={DUFFEL_MAX_INSERTS} | PER_ORIGIN={DUFFEL_MAX_INSERTS_PER_ORIGIN} | PER_ROUTE={DUFFEL_MAX_INSERTS_PER_ROUTE} | "
        f"MAX_SEARCHES={DUFFEL_MAX_SEARCHES_PER_RUN} | ROUTES_PER_RUN(env)={DUFFEL_ROUTES_PER_RUN} | ROUTES_PER_RUN(effective)={eff_routes}")
    log(f"CAPACITY_NOTE: theoretical_max_inserts_this_run <= {theoretical_max} (based on caps + effective routes)")
    log(f"PRICE_GATE: fallback={PRICE_GATE_FALLBACK_BEHAVIOR} | mult={PRICE_GATE_MULTIPLIER} | mincap={PRICE_GATE_MIN_CAP_GBP}")
    log(f"HYGIENE: enabled={HYGIENE_ENABLED} | conn_short={OFFER_MAX_CONNECTIONS_SHORTHAUL} conn_long={OFFER_MAX_CONNECTIONS_LONGHAUL} | "
        f"dur_short={OFFER_MAX_DURATION_MINUTES_SHORTHAUL} dur_long={OFFER_MAX_DURATION_MINUTES_LONGHAUL} | "
        f"band_short={QUALITY_PRICE_BAND_SHORTHAUL} band_long={QUALITY_PRICE_BAND_LONGHAUL}")
    log(f"INVENTORY_WINDOW_DAYS={INVENTORY_MIN_DAYS}-{INVENTORY_MAX_DAYS} | ZERO_OFFER_RETRY_ENABLED={ZERO_OFFER_RETRY_ENABLED} retry_window_max={ZERO_OFFER_RETRY_MAX_DAYS}")

    explore_run = should_do_explore_this_run(theme_today)
    theme_quota = eff_routes if not explore_run else max(0, eff_routes - 1)
    explore_quota = 0 if not explore_run else 1
    log(f"🧠 Strategy: 90/10 | explore_run={explore_run} | theme_quota={theme_quota} | explore_quota={explore_quota} | MOD={FEEDER_EXPLORE_RUN_MOD}")

    gc = gs_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_raw = sh.worksheet(RAW_DEALS_TAB)
    raw_headers = ws_raw.row_values(1)
    raw_header_set = {str(h).strip() for h in raw_headers if h}

    config_rows = load_config_rows(sh)
    themes_dict = load_themes_dict(sh)
    signals = load_signals(sh)
    if not signals:
        log(f"⚠️ CONFIG_SIGNALS loaded 0 rows — check IATA key column (destination_iata vs iata_hint) in '{SIGNALS_TAB}'.")
    allowed_pairs, origin_city_map = load_route_capability_map(sh)

    benchmarks: List[Dict[str, Any]] = []
    if PRICE_GATE_ENABLED:
        try:
            benchmarks = load_zone_theme_benchmarks(sh)
        except Exception as e:
            log(f"⚠️ PRICE_GATE: failed to load '{BENCHMARKS_TAB}': {e} | fallback={PRICE_GATE_FALLBACK_BEHAVIOR}")
            benchmarks = []

    theme_routes = [r for r in config_rows if str(r.get("theme") or "").strip() == theme_today]
    explore_routes = [r for r in config_rows if str(r.get("theme") or "").strip() != theme_today]

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

    plan_n = max(5, req_origins, eff_routes)
    planned_origins = origin_plan_for_theme(theme_today, plan_n)
    log(f"🧭 Planned origins for run ({len(planned_origins)}; required={plan_n}): {planned_origins}")
    log(f"🧭 Unique theme destinations: {len(theme_dest_configs)} | Unique explore destinations: {len(explore_dest_configs)}")

    # -------- RULE B SELECTION (one per origin first) --------

    # Anchor-origins guarantee: prevent low-liquidity origins consuming the whole quota
    # (Observed failure mode: quota fills on EXT/CWL/LPL before STN/LGW are reached).
    THEME_ANCHOR_ORIGINS = {
        # Short-haul city deals: ensure at least one London LCC origin is considered first
        'city_breaks': ['LGW', 'STN', 'LTN'],
        'culture_history': ['LGW', 'STN', 'LTN'],
        # Winter sun often strongest from major hubs (MAN/London)
        'winter_sun': ['MAN', 'LGW', 'STN', 'LTN'],
        # Long haul: full-service hubs
        'long_haul': ['LHR', 'LGW', 'MAN'],
        # Snow: hubs + Scotland
        'snow': ['LGW', 'MAN', 'EDI', 'GLA', 'STN', 'LTN'],
        # Surf: SW-first is intentional (but still allow London fallback)
        'surf': ['BRS', 'NQY', 'EXT', 'LGW', 'STN', 'LTN'],
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
        for off in offers:
            if offer_price_gbp(off) <= 0:
                rejected_non_gbp += 1
                continue
            gbp_offers.append(off)

        rejected_price_hard = 0
        if PRICE_GATE_ENABLED and cap_gbp is not None:
            in_cap: List[Dict[str, Any]] = []
            for off in gbp_offers:
                if offer_price_gbp(off) <= cap_gbp:
                    in_cap.append(off)
                else:
                    rejected_price_hard += 1
            gbp_offers = in_cap

        # HYGIENE: connections/duration hard filters
        rejected_hygiene_conn = 0
        rejected_hygiene_dur = 0
        if HYGIENE_ENABLED and gbp_offers:
            kept: List[Dict[str, Any]] = []
            for off in gbp_offers:
                conn = offer_connections_safe(off)
                dur = offer_duration_minutes_safe(off)
                if conn > max_conn_hard:
                    rejected_hygiene_conn += 1
                    continue
                if dur > max_dur_hard:
                    rejected_hygiene_dur += 1
                    continue
                kept.append(off)
            gbp_offers = kept

        # HYGIENE: inner quality band under cap (prevents near-cap filler)
        rejected_band = 0
        band_cap: Optional[float] = None
        if HYGIENE_ENABLED and PRICE_GATE_ENABLED and cap_gbp is not None and gbp_offers:
            if band > 0 and band < 1.0:
                band_cap = float(cap_gbp) * float(band)
                kept2: List[Dict[str, Any]] = []
                for off in gbp_offers:
                    if offer_price_gbp(off) <= band_cap:
                        kept2.append(off)
                    else:
                        rejected_band += 1
                gbp_offers = kept2

        if not gbp_offers:
            cap_str = f"{int(cap_gbp)}" if cap_gbp is not None else "NONE"
            band_str = f"{int(band_cap)}" if band_cap is not None else ("NONE" if cap_gbp is None else "1.0x")
            log(
                f"Duffel[PRIMARY]: offers_returned={offers_returned} gbp_offers=0 cap_gbp={cap_str} band_cap={band_str} "
                f"rej_non_gbp={rejected_non_gbp} rej_price={rejected_price_hard} rej_conn={rejected_hygiene_conn} "
                f"rej_dur={rejected_hygiene_dur} rej_band={rejected_band} inserted=0"
            )
            return offers_returned, []

        def rank_key(off: Dict[str, Any]) -> Tuple[float, int, int]:
            return (offer_price_gbp(off), offer_connections_safe(off), offer_duration_minutes_safe(off))

        ranked = sorted(gbp_offers, key=rank_key)

        remaining_total = max(0, DUFFEL_MAX_INSERTS - len(all_deals))
        remaining_origin = max(0, DUFFEL_MAX_INSERTS_PER_ORIGIN - inserted_by_origin.get(origin, 0))
        take_n = min(DUFFEL_MAX_INSERTS_PER_ROUTE, DUFFEL_OFFERS_PER_SEARCH, remaining_total, remaining_origin)

        now_iso = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        inserted_here = 0
        processed = 0
        deals_out: List[Dict[str, Any]] = []

        for off in ranked:
            if inserted_here >= take_n:
                break
            processed += 1

            price = offer_price_gbp(off)
            off_id = str(off.get("id") or "")
            deal_id_seed = f"{origin}->{destination}|{dep_date.isoformat()}|{ret_date.isoformat()}|{price:.2f}|{off_id}"
            deal_id = hashlib.sha256(deal_id_seed.encode("utf-8")).hexdigest()[:24]

            deal: Dict[str, Any] = {
                "status": "NEW",
                "deal_id": deal_id,
                "price_gbp": int(math.ceil(price)),
                "origin_iata": origin,
                "origin_city": resolve_origin_city(origin, origin_city_map),
                "destination_iata": destination,
                "outbound_date": dep_date.strftime("%Y-%m-%d"),
                "return_date": ret_date.strftime("%Y-%m-%d"),
                "deal_theme": deal_theme,
                "theme": deal_theme,
                "destination_city": "",
                "destination_country": "",
            }

            if "ingested_at_utc" in raw_header_set:
                deal["ingested_at_utc"] = now_iso
            if "created_utc" in raw_header_set:
                deal["created_utc"] = now_iso
            if "created_at" in raw_header_set:
                deal["created_at"] = now_iso
            if "timestamp" in raw_header_set:
                deal["timestamp"] = now_iso

            deal = enrich_deal(deal, themes_dict, signals)
            deals_out.append(deal)
            inserted_here += 1

        inserted_by_origin[origin] = inserted_by_origin.get(origin, 0) + inserted_here

        cap_str = f"{int(cap_gbp)}" if cap_gbp is not None else "NONE"
        band_str = f"{int(band_cap)}" if band_cap is not None else ("NONE" if cap_gbp is None else "1.0x")
        log(
            f"Duffel[PRIMARY]: offers_returned={offers_returned} gbp_ranked={len(ranked)} processed={processed} "
            f"cap_gbp={cap_str} band_cap={band_str} rej_non_gbp={rejected_non_gbp} rej_price={rejected_price_hard} "
            f"rej_conn={rejected_hygiene_conn} rej_dur={rejected_hygiene_dur} rej_band={rejected_band} "
            f"inserted={inserted_here} origin_total={inserted_by_origin[origin]}/{DUFFEL_MAX_INSERTS_PER_ORIGIN} "
            f"running_total={len(all_deals) + len(deals_out)}/{DUFFEL_MAX_INSERTS}"
        )

        return offers_returned, deals_out

    for origin, destination, cfg in selected_routes:
        if searches_done >= DUFFEL_MAX_SEARCHES_PER_RUN:
            break
        if len(all_deals) >= DUFFEL_MAX_INSERTS:
            break

        if inserted_by_origin.get(origin, 0) >= DUFFEL_MAX_INSERTS_PER_ORIGIN:
            log(f"⏭️  ORIGIN_CAP skip: {origin} already inserted {inserted_by_origin[origin]}/{DUFFEL_MAX_INSERTS_PER_ORIGIN}")
            continue

        days_min = int(cfg.get("days_ahead_min") or DEFAULT_DAYS_AHEAD_MIN)
        days_max = int(cfg.get("days_ahead_max") or DEFAULT_DAYS_AHEAD_MAX)
        trip_len = int(cfg.get("trip_length_days") or DEFAULT_TRIP_LENGTH_DAYS)
        days_min, days_max = clamp_window(days_min, days_max)

        seed = f"{dt.datetime.utcnow().date().isoformat()}|{origin}|{destination}|{trip_len}|{RUN_SLOT}"
        dep_date, ret_date = pick_dates(seed, days_min, days_max, trip_len)

        deal_theme = str(cfg.get("theme") or theme_today).strip() or theme_today

        cap_gbp: Optional[float] = None
        if PRICE_GATE_ENABLED and benchmarks:
            cap_gbp = compute_ingest_cap_gbp(benchmarks, deal_theme, origin, destination)

        if PRICE_GATE_ENABLED and cap_gbp is None:
            if PRICE_GATE_FALLBACK_BEHAVIOR == "BLOCK":
                log(f"⛔ BENCHMARK_MISS (pre-check) {origin}->{destination} theme={deal_theme} | fallback=BLOCK => search_skipped")
                continue
            log(f"⚠️ BENCHMARK_MISS (pre-check) {origin}->{destination} theme={deal_theme} | fallback=ALLOW (no cap)")

        try:
            offers_returned, deals_out = run_search(origin, destination, dep_date, ret_date, cfg, cap_gbp)
        except Exception as e:
            log(f"❌ Duffel[PRIMARY] error: {e}")
            continue

        if deals_out:
            all_deals.extend(deals_out)
        else:
            if ZERO_OFFER_RETRY_ENABLED and offers_returned == 0 and searches_done < DUFFEL_MAX_SEARCHES_PER_RUN and len(all_deals) < DUFFEL_MAX_INSERTS:
                retry_max = min(ZERO_OFFER_RETRY_MAX_DAYS, INVENTORY_MAX_DAYS)
                retry_min = INVENTORY_MIN_DAYS
                if retry_max >= retry_min:
                    retry_seed = seed + "|retry"
                    dep2, ret2 = pick_dates(retry_seed, retry_min, retry_max, trip_len)
                    log(f"🔄 ZERO_OFFER_RETRY: {origin}->{destination} using {retry_min}-{retry_max}d window => {dep2.isoformat()}/{ret2.isoformat()}")
                    try:
                        _, deals_out2 = run_search(origin, destination, dep2, ret2, cfg, cap_gbp)
                        if deals_out2:
                            all_deals.extend(deals_out2)
                    except Exception as e:
                        log(f"❌ Duffel[PRIMARY] retry error: {e}")

        if FEEDER_SLEEP_SECONDS > 0:
            time.sleep(FEEDER_SLEEP_SECONDS)

    log(f"✓ Searches completed: {searches_done}")
    log(f"✓ Deals collected: {len(all_deals)} (cap {DUFFEL_MAX_INSERTS})")

    if not all_deals:
        log("⚠️ No deals passed gates for this run.")
        return 0

    inserted = append_rows_header_mapped(ws_raw, all_deals)
    log(f"✅ Inserted {inserted} rows into {RAW_DEALS_TAB}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
