#!/usr/bin/env python3
"""
workers/pipeline_worker.py

TravelTxter Pipeline Worker (FEEDER) — V4.7.7 (UNBLOCK PACK)
- FIX: Planned origins are respected when open-origins is false (no silent fallback to random origins).
- FIX: Benchmark cap reads optimized contract: `max_price` (+ optional `error_price`).
- UNBLOCK: Adventure theme uses major-hub origin priority (better Duffel inventory).
- UNBLOCK: Clamp date window to Duffel-friendly inventory window (default 21–84 days).
- UNBLOCK: If offers_returned=0, do ONE deterministic retry with shifted dates (counts against search budget).
- SAFE ranking: best N out of many via (price, connections, duration) with safe-field extraction.
- Caps: total/per-origin/per-route enforced.

LOCKED PRINCIPLES
- Sheets is source of truth; worker stateless.
- No schema changes; no tab renames.
- RAW_DEALS_VIEW read-only (this worker does not touch it).
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


# ==================== GOVERNORS (prefer DUFFEL_*; fallback to FEEDER_*) ====================

def _get_int(primary: str, fallback: str, default: int) -> int:
    v = (os.getenv(primary) or "").strip()
    if v:
        try:
            return int(v)
        except Exception:
            pass
    v2 = (os.getenv(fallback) or "").strip()
    if v2:
        try:
            return int(v2)
        except Exception:
            pass
    return default


DUFFEL_MAX_INSERTS = _get_int("DUFFEL_MAX_INSERTS", "FEEDER_MAX_INSERTS", 3)
DUFFEL_MAX_SEARCHES_PER_RUN = _get_int("DUFFEL_MAX_SEARCHES_PER_RUN", "FEEDER_MAX_SEARCHES", 4)
DUFFEL_ROUTES_PER_RUN = _get_int("DUFFEL_ROUTES_PER_RUN", "FEEDER_ROUTES_PER_RUN", 3)

DUFFEL_OFFERS_PER_SEARCH = _get_int("DUFFEL_OFFERS_PER_SEARCH", "OFFERS_PER_SEARCH", 50)

DUFFEL_MAX_INSERTS_PER_ROUTE = _get_int("DUFFEL_MAX_INSERTS_PER_ROUTE", "MAX_INSERTS_PER_ROUTE", 10)
DUFFEL_MAX_INSERTS_PER_ORIGIN = _get_int("DUFFEL_MAX_INSERTS_PER_ORIGIN", "MAX_INSERTS_PER_ORIGIN", 10)

FEEDER_SLEEP_SECONDS = float(os.getenv("FEEDER_SLEEP_SECONDS", "0") or "0")

FEEDER_OPEN_ORIGINS = (os.getenv("FEEDER_OPEN_ORIGINS", "false").strip().lower() == "true")
STRICT_CAPABILITY_MAP = (os.getenv("STRICT_CAPABILITY_MAP", "true").strip().lower() == "true")
RESPECT_CONFIG_ORIGIN = (os.getenv("RESPECT_CONFIG_ORIGIN", "false").strip().lower() == "true")

DEFAULT_DAYS_AHEAD_MIN = int(os.getenv("DAYS_AHEAD_MIN", "14") or "14")
DEFAULT_DAYS_AHEAD_MAX = int(os.getenv("DAYS_AHEAD_MAX", "90") or "90")
DEFAULT_TRIP_LENGTH_DAYS = int(os.getenv("TRIP_LENGTH_DAYS", "5") or "5")

# Duffel-friendly date clamp (inventory tends to be better 3–12 weeks out)
INVENTORY_MIN_DAYS = int(os.getenv("INVENTORY_MIN_DAYS", "21") or "21")
INVENTORY_MAX_DAYS = int(os.getenv("INVENTORY_MAX_DAYS", "84") or "84")

FEEDER_EXPLORE_RUN_MOD = int(os.getenv("FEEDER_EXPLORE_RUN_MOD", "10") or "10")
FEEDER_EXPLORE_SALT = (os.getenv("FEEDER_EXPLORE_SALT", "traveltxter") or "traveltxter").strip()

PRICE_GATE_ENABLED = (os.getenv("PRICE_GATE_ENABLED", "true").strip().lower() == "true")
PRICE_GATE_MULTIPLIER = float(os.getenv("PRICE_GATE_MULTIPLIER", "1.0") or "1.0")
PRICE_GATE_MIN_CAP_GBP = float(os.getenv("PRICE_GATE_MIN_CAP_GBP", "80") or "80")
PRICE_GATE_FALLBACK_BEHAVIOR = (os.getenv("PRICE_GATE_FALLBACK_BEHAVIOR", "BLOCK").strip().upper() or "BLOCK")

# 0-offer retry
ZERO_OFFER_RETRY_ENABLED = (os.getenv("ZERO_OFFER_RETRY_ENABLED", "true").strip().lower() == "true")
ZERO_OFFER_RETRY_SHIFT_DAYS = int(os.getenv("ZERO_OFFER_RETRY_SHIFT_DAYS", "7") or "7")
ZERO_OFFER_MAX_RETRIES_PER_ROUTE = int(os.getenv("ZERO_OFFER_MAX_RETRIES_PER_ROUTE", "1") or "1")


# ==================== THEMES (LOCKED LIST) ====================

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

# NOTE: This is not “growth optimization”; it’s an availability reality check to stop repeated 0-offer waste.
ADVENTURE_HUBS = ["LGW", "LHR", "STN", "LTN", "MAN", "BHX"]

SW_ENGLAND_DEFAULT = ["BRS", "EXT", "NQY", "SOU", "CWL", "BOH"]

SHORT_HAUL_PRIMARY = ["BRS", "EXT", "NQY", "CWL", "SOU", "BOH"]
SHORT_HAUL_FALLBACK = ["STN", "LTN", "LGW", "BHX", "MAN"]
UK_WIDE_FALLBACK = ["MAN", "LBA", "NCL", "EDI", "GLA", "BFS", "BHD", "LPL", "EMA"]

SNOW_POOL = ["BRS", "LGW", "STN", "LTN", "BHX", "MAN", "EDI", "GLA"]
LONG_HAUL_POOL = ["LHR", "LGW", "MAN", "BHX", "EDI", "GLA"]

ORIGIN_CITY_FALLBACK = {
    "LHR": "London",
    "LGW": "London",
    "STN": "London",
    "LTN": "London",
    "LCY": "London",
    "SEN": "London",
    "MAN": "Manchester",
    "BHX": "Birmingham",
    "EMA": "East Midlands",
    "LPL": "Liverpool",
    "LBA": "Leeds",
    "NCL": "Newcastle",
    "EDI": "Edinburgh",
    "GLA": "Glasgow",
    "BFS": "Belfast",
    "BHD": "Belfast",
    "BRS": "Bristol",
    "EXT": "Exeter",
    "NQY": "Newquay",
    "SOU": "Southampton",
    "CWL": "Cardiff",
    "BOH": "Bournemouth",
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


def _deterministic_pick(seq: List[str], seed: str, k: int) -> List[str]:
    if not seq or k <= 0:
        return []
    h = int(hashlib.md5(seed.encode("utf-8")).hexdigest()[:8], 16)
    out: List[str] = []
    n = len(seq)
    for i in range(k):
        out.append(seq[(h + i) % n])
    return out


def _sw_england_origins_from_env() -> List[str]:
    raw = (os.getenv("SW_ENGLAND_ORIGINS", "") or "").strip()
    if not raw:
        return SW_ENGLAND_DEFAULT[:]
    parts = [p.strip().upper() for p in raw.split(",")]
    parts = [p for p in parts if p]
    return parts or SW_ENGLAND_DEFAULT[:]


def origin_plan_for_theme(theme_today: str, routes_per_run: int) -> List[str]:
    """
    Ensures >=5 unique origins where possible to reach 50 inserts with per-origin caps.
    Adds theme-aware hub preference for adventure to prevent repeated 0-offer waste.
    """
    today = dt.datetime.utcnow().date().isoformat()
    seed = f"{today}|{RUN_SLOT}|{theme_today}"

    sw = _dedupe_keep_order(_sw_england_origins_from_env())

    if theme_today == "adventure":
        base = _dedupe_keep_order(ADVENTURE_HUBS + SHORT_HAUL_FALLBACK + sw + UK_WIDE_FALLBACK)
    elif theme_today in LONG_HAUL_THEMES:
        base = _dedupe_keep_order(LONG_HAUL_POOL + SHORT_HAUL_FALLBACK + sw + UK_WIDE_FALLBACK)
    elif theme_today in SNOW_THEMES:
        base = _dedupe_keep_order(SNOW_POOL + SHORT_HAUL_FALLBACK + sw + UK_WIDE_FALLBACK)
    else:
        base = _dedupe_keep_order(sw + SHORT_HAUL_FALLBACK + UK_WIDE_FALLBACK)

    plan_n = min(len(base), max(5, routes_per_run))
    return _deterministic_pick(base, seed, plan_n)


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
        key = _clean_iata(r.get("destination_iata"))
        if key:
            out[key] = r
    return out


def load_route_capability_map(
    sheet: gspread.Spreadsheet,
) -> Tuple[Set[Tuple[str, str]], Dict[str, str], Dict[str, List[str]]]:
    ws = sheet.worksheet(CAPABILITY_TAB)
    rows = ws.get_all_records()

    allowed: Set[Tuple[str, str]] = set()
    origin_city_map: Dict[str, str] = {}
    dest_to_origins: Dict[str, List[str]] = {}

    for r in rows:
        o = _clean_iata(r.get("origin_iata"))
        d = _clean_iata(r.get("destination_iata"))
        oc = str(r.get("origin_city", "")).strip()
        if o and d:
            allowed.add((o, d))
            dest_to_origins.setdefault(d, [])
            if o not in dest_to_origins[d]:
                dest_to_origins[d].append(o)
            if oc and o not in origin_city_map:
                origin_city_map[o] = oc

    if not allowed:
        msg = f"{CAPABILITY_TAB} is empty or missing required headers"
        if STRICT_CAPABILITY_MAP:
            raise RuntimeError(msg)
        log(f"⚠️ {msg} — continuing WITHOUT capability filtering.")

    return allowed, origin_city_map, dest_to_origins


# ==================== BENCHMARKS (PRICE GATE) ====================

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
    """
    Optimized benchmark contract:
    - theme
    - origin_iata
    - destination_examples (optional)
    - max_price (required)
    - error_price (optional)
    """
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


def compute_ingest_cap_gbp(
    benchmarks: List[Dict[str, Any]],
    theme: str,
    origin: str,
    destination: str,
) -> Optional[float]:
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
                h = int(num or "0")
                num = ""
            elif ch == "M":
                m = int(num or "0")
                num = ""
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


# ==================== ENRICH DEAL ====================

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


# ==================== ROUTE SELECTION ====================

def build_unique_dest_configs(routes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: Set[str] = set()
    out: List[Dict[str, Any]] = []
    for r in routes:
        dest = _clean_iata(r.get("destination_iata"))
        if not dest or dest in seen:
            continue
        seen.add(dest)
        out.append(r)
    return out


def pick_origin_for_dest(
    dest: str,
    planned_origins: List[str],
    allowed_pairs: Set[Tuple[str, str]],
    preferred_origin: str,
    open_origins_effective: bool,
    dest_to_origins: Dict[str, List[str]],
) -> Optional[str]:
    """
    CRITICAL UNBLOCK FIX:
    - If open_origins_effective is False, we ONLY pick from planned_origins (plus optional config origin if RESPECT_CONFIG_ORIGIN).
      We do NOT fall back to a global origin pool.
    - If open_origins_effective is True, we can additionally try capability-discovered origins for the destination.
    """
    dest = _clean_iata(dest)
    preferred_origin = _clean_iata(preferred_origin)

    candidates: List[str] = []

    if preferred_origin and RESPECT_CONFIG_ORIGIN:
        candidates.append(preferred_origin)

    # Always try planned origins first (deterministic order)
    for o in planned_origins:
        oo = _clean_iata(o)
        if oo and oo not in candidates:
            candidates.append(oo)

    # If open-origins allowed, try “known capable” origins for this destination next
    if open_origins_effective:
        for o in dest_to_origins.get(dest, [])[:]:
            oo = _clean_iata(o)
            if oo and oo not in candidates:
                candidates.append(oo)

    # Now pick the first candidate that is capability-allowed (or no capability map present)
    for o in candidates:
        if not allowed_pairs or (o, dest) in allowed_pairs:
            return o

    return None


def clamp_days_window(days_min: int, days_max: int) -> Tuple[int, int]:
    """
    Clamp to Duffel-friendly inventory window.
    This reduces repeated 0-offer searches caused by far-out dates.
    """
    mn = max(days_min, INVENTORY_MIN_DAYS)
    mx = min(days_max, INVENTORY_MAX_DAYS)
    if mx < mn:
        # If config is narrower than our clamp, fall back to original
        return days_min, days_max
    return mn, mx


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
    log(f"FEEDER_OPEN_ORIGINS={FEEDER_OPEN_ORIGINS} | sparse_theme_override={sparse_theme_override} | effective={open_origins_effective}")
    log(f"RESPECT_CONFIG_ORIGIN={RESPECT_CONFIG_ORIGIN}")

    explore_run = should_do_explore_this_run(theme_today)
    theme_quota = DUFFEL_ROUTES_PER_RUN if not explore_run else max(0, DUFFEL_ROUTES_PER_RUN - 1)
    explore_quota = 0 if not explore_run else 1
    log(f"🧠 Feeder strategy: 90/10 | explore_run={explore_run} | theme_quota={theme_quota} | explore_quota={explore_quota} | MOD={FEEDER_EXPLORE_RUN_MOD}")

    log(f"CAPS: DUFFEL_MAX_INSERTS={DUFFEL_MAX_INSERTS} | PER_ORIGIN={DUFFEL_MAX_INSERTS_PER_ORIGIN} | PER_ROUTE={DUFFEL_MAX_INSERTS_PER_ROUTE} | MAX_SEARCHES={DUFFEL_MAX_SEARCHES_PER_RUN} | OFFERS_PER_SEARCH={DUFFEL_OFFERS_PER_SEARCH}")
    log(f"PRICE_GATE_FALLBACK_BEHAVIOR={PRICE_GATE_FALLBACK_BEHAVIOR} | PRICE_GATE_MULTIPLIER={PRICE_GATE_MULTIPLIER} | PRICE_GATE_MIN_CAP_GBP={PRICE_GATE_MIN_CAP_GBP}")
    log(f"INVENTORY_WINDOW_DAYS={INVENTORY_MIN_DAYS}-{INVENTORY_MAX_DAYS} | ZERO_OFFER_RETRY_ENABLED={ZERO_OFFER_RETRY_ENABLED} shift={ZERO_OFFER_RETRY_SHIFT_DAYS} retries={ZERO_OFFER_MAX_RETRIES_PER_ROUTE}")

    gc = gs_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_raw = sh.worksheet(RAW_DEALS_TAB)
    raw_headers = ws_raw.row_values(1)
    raw_header_set = {str(h).strip() for h in raw_headers if h}

    config_rows = load_config_rows(sh)
    themes_dict = load_themes_dict(sh)
    signals = load_signals(sh)
    allowed_pairs, origin_city_map, dest_to_origins = load_route_capability_map(sh)

    benchmarks: List[Dict[str, Any]] = []
    if PRICE_GATE_ENABLED:
        try:
            benchmarks = load_zone_theme_benchmarks(sh)
        except Exception as e:
            log(f"⚠️ PRICE_GATE: failed to load '{BENCHMARKS_TAB}': {e} | fallback={PRICE_GATE_FALLBACK_BEHAVIOR}")
            benchmarks = []

    theme_routes = [r for r in config_rows if str(r.get("theme") or "").strip() == theme_today]
    explore_routes = [r for r in config_rows if str(r.get("theme") or "").strip() != theme_today]

    theme_dest_configs = build_unique_dest_configs(theme_routes)
    explore_dest_configs = build_unique_dest_configs(explore_routes)

    planned_origins = origin_plan_for_theme(theme_today, DUFFEL_ROUTES_PER_RUN)
    log(f"🧭 Planned origins for run ({len(planned_origins)}): {planned_origins}")
    log(f"🧭 Unique theme destinations: {len(theme_dest_configs)} | Unique explore destinations: {len(explore_dest_configs)}")

    # Build selected routes deterministically: rotate over theme_dest_configs, pick origin via planned origins
    selected_routes: List[Tuple[str, str, Dict[str, Any]]] = []

    def add_routes_from_configs(dest_configs: List[Dict[str, Any]], quota: int) -> None:
        if quota <= 0:
            return
        oi = 0
        di = 0
        while len(selected_routes) < DUFFEL_MAX_SEARCHES_PER_RUN and quota > 0 and di < len(dest_configs):
            cfg = dest_configs[di]
            destination = _clean_iata(cfg.get("destination_iata"))
            if not destination:
                di += 1
                continue

            preferred_origin = _clean_iata(cfg.get("origin_iata"))
            origin = pick_origin_for_dest(
                dest=destination,
                planned_origins=planned_origins,
                allowed_pairs=allowed_pairs,
                preferred_origin=preferred_origin,
                open_origins_effective=open_origins_effective,
                dest_to_origins=dest_to_origins,
            )
            if origin:
                selected_routes.append((origin, destination, cfg))
                quota -= 1

            di += 1
            oi += 1

    add_routes_from_configs(theme_dest_configs, theme_quota)

    if explore_quota > 0 and explore_dest_configs:
        today = dt.datetime.utcnow().date().isoformat()
        seed = f"{FEEDER_EXPLORE_SALT}|{today}|{RUN_SLOT}|{theme_today}|EXPLORE"
        offset = _stable_mod(seed, max(1, len(explore_dest_configs)))
        rotated = explore_dest_configs[offset:] + explore_dest_configs[:offset]
        add_routes_from_configs(rotated, 1)

    if not selected_routes:
        log("⚠️ No eligible routes after capability/planned-origin filtering.")
        return 0

    searches_done = 0
    all_deals: List[Dict[str, Any]] = []
    inserted_by_origin: Dict[str, int] = {}

    def do_one_search(origin: str, destination: str, dep_date: dt.date, ret_date: dt.date, cfg: Dict[str, Any], cap_gbp: Optional[float]) -> Tuple[int, List[Dict[str, Any]], int, int]:
        """
        Returns: (offers_returned, deals_appended, rejected_non_gbp, rejected_price)
        deals_appended are raw dicts ready for append (not yet appended to sheet).
        """
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
            return 0, [], 0, 0

        gbp_offers: List[Dict[str, Any]] = []
        rejected_non_gbp = 0
        for off in offers:
            if offer_price_gbp(off) <= 0:
                rejected_non_gbp += 1
                continue
            gbp_offers.append(off)

        rejected_price = 0
        if PRICE_GATE_ENABLED and cap_gbp is not None:
            in_cap: List[Dict[str, Any]] = []
            for off in gbp_offers:
                if offer_price_gbp(off) <= cap_gbp:
                    in_cap.append(off)
                else:
                    rejected_price += 1
            gbp_offers = in_cap

        if not gbp_offers:
            cap_str = f"{int(cap_gbp)}" if cap_gbp is not None else "NONE"
            log(
                f"Duffel[PRIMARY]: offers_returned={offers_returned} gbp_offers=0 cap_gbp={cap_str} "
                f"rejected_price={rejected_price} rejected_non_gbp={rejected_non_gbp} inserted=0"
            )
            return offers_returned, [], rejected_non_gbp, rejected_price

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

        deal_theme = str(cfg.get("theme") or theme_today).strip() or theme_today

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
        log(
            f"Duffel[PRIMARY]: offers_returned={offers_returned} gbp_ranked={len(ranked)} processed={processed} cap_gbp={cap_str} "
            f"rejected_price={rejected_price} rejected_non_gbp={rejected_non_gbp} "
            f"inserted={inserted_here} origin_total={inserted_by_origin[origin]}/{DUFFEL_MAX_INSERTS_PER_ORIGIN} "
            f"running_total={len(all_deals) + len(deals_out)}/{DUFFEL_MAX_INSERTS}"
        )

        return offers_returned, deals_out, rejected_non_gbp, rejected_price

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

        # Clamp to inventory-friendly window
        days_min2, days_max2 = clamp_days_window(days_min, days_max)

        seed = f"{dt.datetime.utcnow().date().isoformat()}|{origin}|{destination}|{trip_len}|{RUN_SLOT}"
        hsh = int(hashlib.md5(seed.encode("utf-8")).hexdigest()[:8], 16)
        span = max(1, (days_max2 - days_min2 + 1))
        dep_offset = days_min2 + (hsh % span)
        dep_date = (dt.datetime.utcnow().date() + dt.timedelta(days=dep_offset))
        ret_date = dep_date + dt.timedelta(days=trip_len)

        deal_theme = str(cfg.get("theme") or theme_today).strip() or theme_today

        cap_gbp: Optional[float] = None
        if PRICE_GATE_ENABLED and benchmarks:
            cap_gbp = compute_ingest_cap_gbp(benchmarks, deal_theme, origin, destination)

        if PRICE_GATE_ENABLED and cap_gbp is None:
            if PRICE_GATE_FALLBACK_BEHAVIOR == "BLOCK":
                log(f"⛔ BENCHMARK_MISS (pre-check) {origin}->{destination} theme={deal_theme} | fallback=BLOCK => search_skipped")
                continue
            log(f"⚠️ BENCHMARK_MISS (pre-check) {origin}->{destination} theme={deal_theme} | fallback=ALLOW (no cap)")

        # Search (and optional retry if 0 offers)
        try:
            offers_returned, deals_out, _, _ = do_one_search(origin, destination, dep_date, ret_date, cfg, cap_gbp)
        except Exception as e:
            log(f"❌ Duffel[PRIMARY] error: {e}")
            continue

        if deals_out:
            all_deals.extend(deals_out)
        else:
            # If Duffel returned 0 offers OR cap-filtered everything, retry once with shifted dates (budget permitting)
            retries = 0
            while (
                ZERO_OFFER_RETRY_ENABLED
                and offers_returned == 0
                and retries < ZERO_OFFER_MAX_RETRIES_PER_ROUTE
                and searches_done < DUFFEL_MAX_SEARCHES_PER_RUN
                and len(all_deals) < DUFFEL_MAX_INSERTS
            ):
                retries += 1
                dep2 = dep_date + dt.timedelta(days=ZERO_OFFER_RETRY_SHIFT_DAYS)
                ret2 = ret_date + dt.timedelta(days=ZERO_OFFER_RETRY_SHIFT_DAYS)
                log(f"🔄 ZERO_OFFER_RETRY {retries}/{ZERO_OFFER_MAX_RETRIES_PER_ROUTE}: {origin}->{destination} shift_days={ZERO_OFFER_RETRY_SHIFT_DAYS}")

                try:
                    offers_returned2, deals_out2, _, _ = do_one_search(origin, destination, dep2, ret2, cfg, cap_gbp)
                except Exception as e:
                    log(f"❌ Duffel[PRIMARY] retry error: {e}")
                    break

                if deals_out2:
                    all_deals.extend(deals_out2)
                    break
                offers_returned = offers_returned2  # continue loop only if still 0 offers

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
