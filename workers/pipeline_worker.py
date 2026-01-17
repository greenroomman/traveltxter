#!/usr/bin/env python3
"""
workers/pipeline_worker.py

TravelTxter Pipeline Worker (Feeder) — V4.6 (Surgical Fix)

THIS FILE IS THE FEEDER. IT MUST:
- Stay true to Theme of the Day (theme-first route selection + small explore slice)
- Land enough supply (not 3 rows)
- Land only plausibly publishable supply (stop ingesting junk like £500 Bilbao)
- Write required timestamps (created_utc, ingested_at_utc) so scorer can gate reliably
- Remain stateless (Sheets is the only memory)

Surgical Fix (2026-01-17) — PHASE 1 ONLY: PRICE GATE AT INGESTION
- Load ZONE_THEME_BENCHMARKS and compute a per-route ingest cap.
- Sort offers cheapest-first.
- Iterate through offers until we fill insert cap or exhaust offers.
- Reject offers above cap BEFORE writing to RAW_DEALS.
- Log returned / processed / rejected_price / inserted per route.
- DO NOT assume any Duffel request-side "max_price" exists (Phase 2 later).
"""

from __future__ import annotations

import os
import json
import math
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

# Safety caps (governor)
MAX_INSERTS_TOTAL = int(os.getenv("DUFFEL_MAX_INSERTS", "3") or "3")
MAX_SEARCHES_PER_RUN = int(os.getenv("DUFFEL_MAX_SEARCHES_PER_RUN", "4") or "4")
ROUTES_PER_RUN = int(os.getenv("DUFFEL_ROUTES_PER_RUN", "3") or "3")

# Offers to consider per route (we may process more if many are rejected, but we won't ingest more than this per route)
OFFERS_PER_SEARCH = int(os.getenv("DUFFEL_OFFERS_PER_SEARCH", "50") or "50")

# PRICE GATE (Phase 1 only)
PRICE_GATE_ENABLED = (os.getenv("PRICE_GATE_ENABLED", "true").strip().lower() == "true")
PRICE_GATE_MULTIPLIER = float(os.getenv("PRICE_GATE_MULTIPLIER", "1.5") or "1.5")
PRICE_GATE_MIN_CAP_GBP = float(os.getenv("PRICE_GATE_MIN_CAP_GBP", "80") or "80")  # safety floor
PRICE_GATE_FALLBACK_BEHAVIOR = (os.getenv("PRICE_GATE_FALLBACK_BEHAVIOR", "ALLOW").strip().upper() or "ALLOW")
# ALLOW = if no benchmark match, allow (but log)
# BLOCK = if no benchmark match, block (NOT recommended while stabilising)

# Optional: strict capability filtering (default TRUE)
STRICT_CAPABILITY_MAP = (os.getenv("STRICT_CAPABILITY_MAP", "true").strip().lower() == "true")

# Reverse capability origin selection (destination -> origins)
FEEDER_OPEN_ORIGINS = (os.getenv("FEEDER_OPEN_ORIGINS", "false").strip().lower() == "true")

# Allow CONFIG origin override (default FALSE)
RESPECT_CONFIG_ORIGIN = (os.getenv("RESPECT_CONFIG_ORIGIN", "false").strip().lower() == "true")

# Date defaults
DEFAULT_DAYS_AHEAD_MIN = int(os.getenv("DAYS_AHEAD_MIN", "14") or "14")
DEFAULT_DAYS_AHEAD_MAX = int(os.getenv("DAYS_AHEAD_MAX", "90") or "90")

# Trip length defaults
DEFAULT_TRIP_LENGTH_DAYS = int(os.getenv("TRIP_LENGTH_DAYS", "5") or "5")

# 90/10 explore control (deterministic)
FEEDER_EXPLORE_RUN_MOD = int(os.getenv("FEEDER_EXPLORE_RUN_MOD", "10") or "10")
FEEDER_EXPLORE_SALT = (os.getenv("FEEDER_EXPLORE_SALT", "traveltxter") or "traveltxter").strip()

# Optional pause
FEEDER_SLEEP_SECONDS = float(os.getenv("FEEDER_SLEEP_SECONDS", "0") or "0")


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

SHORT_HAUL_THEMES = {
    "winter_sun", "summer_sun", "beach_break", "surf",
    "city_breaks", "culture_history", "unexpected_value", "adventure"
}
SNOW_THEMES = {"snow", "northern_lights"}
LONG_HAUL_THEMES = {"long_haul", "luxury_value"}

SHORT_HAUL_PRIMARY = ["BRS", "EXT", "NQY", "CWL", "SOU"]
SHORT_HAUL_FALLBACK = ["STN", "LTN", "LGW"]
SNOW_POOL = ["BRS", "LGW", "STN", "LTN"]
LONG_HAUL_POOL = ["LHR", "LGW"]

ORIGIN_CITY_FALLBACK = {
    "LHR": "London", "LGW": "London", "STN": "London", "LTN": "London", "LCY": "London", "SEN": "London",
    "MAN": "Manchester", "BHX": "Birmingham",
    "BRS": "Bristol", "EXT": "Exeter", "NQY": "Newquay", "SOU": "Southampton", "CWL": "Cardiff",
    "LPL": "Liverpool", "EDI": "Edinburgh", "GLA": "Glasgow", "NCL": "Newcastle", "LBA": "Leeds",
    "EMA": "East Midlands", "DSA": "Doncaster", "ABZ": "Aberdeen", "BFS": "Belfast", "BHD": "Belfast",
}


# ==================== LOGGING ====================

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


def low(s: str) -> str:
    return (s or "").strip().lower()


def _clean_iata(x: Any) -> str:
    return (str(x or "").strip().upper())[:3]


def resolve_origin_city(iata: str, origin_city_map: Dict[str, str]) -> str:
    iata = _clean_iata(iata)
    return (origin_city_map.get(iata) or ORIGIN_CITY_FALLBACK.get(iata) or "").strip()


# ==================== THEME OF DAY ====================

def theme_of_day_utc() -> str:
    today = dt.datetime.utcnow().date()
    doy = int(today.strftime("%j"))
    return MASTER_THEMES[doy % len(MASTER_THEMES)]


# ==================== DETERMINISTIC 90/10 ====================

def _stable_mod(key: str, mod: int) -> int:
    h = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return int(h[:8], 16) % max(1, mod)


def _run_slot() -> str:
    return (os.getenv("RUN_SLOT") or "").strip().upper()


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


def open_sheet(gc: gspread.Client) -> gspread.Spreadsheet:
    if SPREADSHEET_ID:
        return gc.open_by_key(SPREADSHEET_ID)
    raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")


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
        key = str(r.get("destination_iata", "")).strip().upper()
        if key:
            out[key] = r
    return out


def load_route_capability_map(
    sheet: gspread.Spreadsheet
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
        log(f"⚠️ {msg} — continuing WITHOUT capability filtering (not recommended).")

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
    Reads BENCHMARKS_TAB using get_all_records() and expects headers aligned
    with your uploaded CSV (destination_examples + low/deal/expensive style cols).
    """
    ws = sheet.worksheet(BENCHMARKS_TAB)
    rows = ws.get_all_records()

    out: List[Dict[str, Any]] = []
    for r in rows:
        theme = str(r.get("theme") or r.get("Theme") or "").strip()
        origin = _clean_iata(r.get("origin_iata") or r.get("origin") or r.get("Origin") or r.get("origin_iata "))
        dest_examples = r.get("destination_examples") or r.get("destinations") or r.get("examples") or ""

        # Accept multiple possible column names for the three prices
        low_price = _to_float(r.get("low_price") or r.get("low") or r.get("Low") or r.get("low_gbp") or r.get("low_price_gbp"))
        deal_price = _to_float(r.get("deal_price") or r.get("typical_price") or r.get("deal") or r.get("Deal") or r.get("median"))
        expensive_price = _to_float(r.get("expensive_price") or r.get("high_price") or r.get("high") or r.get("High") or r.get("expensive"))

        if not theme or not origin:
            continue

        out.append({
            "theme": theme,
            "origin_iata": origin,
            "destination_examples": _split_examples(dest_examples),
            "low_price": low_price,
            "deal_price": deal_price,
            "expensive_price": expensive_price,
        })

    return out


def compute_ingest_cap_gbp(
    benchmarks: List[Dict[str, Any]],
    theme: str,
    origin: str,
    destination: str,
) -> Optional[float]:
    """
    Phase 1 cap:
      cap = max(PRICE_GATE_MIN_CAP_GBP, expensive_price * PRICE_GATE_MULTIPLIER)
    Matching rule:
      - best: same theme + origin where destination is in destination_examples
      - fallback: same theme + origin with no destination_examples match but has expensive_price
      - if none: return None (caller decides ALLOW/BLOCK)
    """
    t = str(theme or "").strip()
    o = _clean_iata(origin)
    d = _clean_iata(destination)

    best: Optional[float] = None
    fallback: Optional[float] = None

    for r in benchmarks:
        if str(r.get("theme", "")).strip() != t:
            continue
        if _clean_iata(r.get("origin_iata")) != o:
            continue
        exp = r.get("expensive_price")
        if exp is None:
            continue
        examples = r.get("destination_examples") or []
        if examples and d in examples:
            best = float(exp)
            break
        fallback = float(exp) if fallback is None else fallback

    exp_use = best if best is not None else fallback
    if exp_use is None:
        return None

    cap = max(float(PRICE_GATE_MIN_CAP_GBP), float(exp_use) * float(PRICE_GATE_MULTIPLIER))
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
    """
    Returns GBP amount as float, else 0 (Phase 1 rejects non-GBP offers).
    """
    total = offer.get("total_amount")
    currency = str(offer.get("total_currency") or "").strip().upper()
    if currency != "GBP":
        return 0.0
    try:
        return float(total)
    except Exception:
        return 0.0


def extract_offers(resp_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    return (resp_json or {}).get("data", {}).get("offers", []) or []


# ==================== ENRICH DEAL ====================

def enrich_deal(
    deal: Dict[str, Any],
    themes_dict: Dict[str, List[Dict[str, Any]]],
    signals: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    dest = deal.get("destination_iata", "")
    theme = deal.get("deal_theme") or deal.get("theme") or ""

    if theme and theme in themes_dict:
        for d in themes_dict[theme]:
            if _clean_iata(d.get("destination_iata")) == _clean_iata(dest):
                if d.get("destination_city"):
                    deal["destination_city"] = d["destination_city"]
                if d.get("destination_country"):
                    deal["destination_country"] = d["destination_country"]
                break

    if dest in signals:
        s = signals[dest]
        if not deal.get("destination_city") and s.get("destination_city"):
            deal["destination_city"] = s["destination_city"]
        if not deal.get("destination_country") and s.get("destination_country"):
            deal["destination_country"] = s["destination_country"]

    return deal


# ==================== WRITE RAW_DEALS ====================

def append_rows_header_mapped(ws: gspread.Worksheet, deals: List[Dict[str, Any]]) -> int:
    if not deals:
        return 0

    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("RAW_DEALS header row is empty")

    rows = []
    for d in deals:
        row = []
        for h in headers:
            row.append(d.get(h.strip(), ""))
        rows.append(row)

    ws.append_rows(rows, value_input_option="RAW")
    return len(rows)


# ==================== ROUTE SELECTION ====================

def build_today_dest_configs(today_routes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: Set[str] = set()
    out: List[Dict[str, Any]] = []
    for r in today_routes:
        dest = _clean_iata(r.get("destination_iata"))
        if not dest or dest in seen:
            continue
        seen.add(dest)
        out.append(r)
    return out


def _dedupe_keep_order(seq: List[str]) -> List[str]:
    out: List[str] = []
    for x in seq:
        xx = _clean_iata(x)
        if not xx:
            continue
        if xx not in out:
            out.append(xx)
    return out


def pick_origin_for_dest(
    dest: str,
    candidate_origins: List[str],
    allowed_pairs: Set[Tuple[str, str]],
    preferred_origin: str = "",
) -> Optional[str]:
    if preferred_origin and RESPECT_CONFIG_ORIGIN:
        o = _clean_iata(preferred_origin)
        if not allowed_pairs or (o, dest) in allowed_pairs:
            return o

    for o in candidate_origins:
        oo = _clean_iata(o)
        if not allowed_pairs or (oo, dest) in allowed_pairs:
            return oo
    return None


def select_routes_from_dest_configs(
    dest_configs: List[Dict[str, Any]],
    quota: int,
    planned_origins: List[str],
    allowed_pairs: Set[Tuple[str, str]],
    dest_to_origins: Dict[str, List[str]],
    full_origin_pool: List[str],
    open_origins: bool,
    start_di: int = 0,
    start_oi: int = 0,
) -> Tuple[List[Tuple[str, str, Dict[str, Any]]], int, int]:
    selected: List[Tuple[str, str, Dict[str, Any]]] = []
    di = start_di
    oi = start_oi

    while len(selected) < quota and di < len(dest_configs):
        cfg = dest_configs[di]
        destination = _clean_iata(cfg.get("destination_iata"))
        if not destination:
            di += 1
            continue

        preferred_origin = _clean_iata(cfg.get("origin_iata"))
        planned_origin = planned_origins[oi % len(planned_origins)] if planned_origins else ""
        oi += 1

        candidate_try_order: List[str] = []

        # 1) Planned origin first (theme/haul-aware rotation)
        if planned_origin:
            candidate_try_order.append(planned_origin)

        # 2) Reverse capability injection (destination -> origins)
        if open_origins:
            rev = dest_to_origins.get(destination, [])[:]
            rev = sorted(_dedupe_keep_order(rev))
            candidate_try_order.extend([o for o in rev if o not in candidate_try_order])

        # 3) Fallback pools (inventory safety)
        candidate_try_order.extend([o for o in full_origin_pool if o not in candidate_try_order])

        candidate_try_order = _dedupe_keep_order(candidate_try_order)

        origin = pick_origin_for_dest(
            dest=destination,
            candidate_origins=candidate_try_order,
            allowed_pairs=allowed_pairs,
            preferred_origin=preferred_origin,
        )

        if origin:
            selected.append((origin, destination, cfg))
        else:
            log(f"⏭️  No valid origin found for destination={destination} after capability filtering.")

        di += 1

    return selected, di, oi


# ==================== DATE HELPERS ====================

def parse_int(x: Any, default: int) -> int:
    try:
        v = int(str(x).strip())
        return v if v > 0 else default
    except Exception:
        return default


def pick_dates(cfg: Dict[str, Any]) -> Tuple[dt.date, dt.date]:
    days_min = parse_int(cfg.get("days_ahead_min"), DEFAULT_DAYS_AHEAD_MIN)
    days_max = parse_int(cfg.get("days_ahead_max"), DEFAULT_DAYS_AHEAD_MAX)
    trip_len = parse_int(cfg.get("trip_length_days"), DEFAULT_TRIP_LENGTH_DAYS)

    today = dt.datetime.utcnow().date()
    dep = today + dt.timedelta(days=days_min)
    if days_max > days_min:
        # deterministic jitter using destination seed
        seed = f"{cfg.get('destination_iata','')}|{today.isoformat()}|{_run_slot()}"
        jitter = _stable_mod(seed, (days_max - days_min + 1))
        dep = today + dt.timedelta(days=days_min + jitter)

    ret = dep + dt.timedelta(days=trip_len)
    return dep, ret


# ==================== MAIN ====================

def main() -> int:
    log("=" * 80)
    log("TRAVELTXTTER PIPELINE WORKER (FEEDER) START")
    log("=" * 80)

    theme_today = theme_of_day_utc()
    log(f"🎯 Theme of the day (UTC): {theme_today}")

    sparse_theme_override = (low(theme_today) in SPARSE_THEMES)
    open_origins_effective = bool(FEEDER_OPEN_ORIGINS or sparse_theme_override)
    log(f"FEEDER_OPEN_ORIGINS={FEEDER_OPEN_ORIGINS} | sparse_theme_override={sparse_theme_override} | effective={open_origins_effective}")
    log(f"RESPECT_CONFIG_ORIGIN={RESPECT_CONFIG_ORIGIN}")

    explore_run = should_do_explore_this_run(theme_today)
    theme_quota = ROUTES_PER_RUN if not explore_run else max(0, ROUTES_PER_RUN - 1)
    explore_quota = 0 if not explore_run else 1
    log(f"🧠 Feeder strategy: 90/10 | explore_run={explore_run} | theme_quota={theme_quota} | explore_quota={explore_quota} | MOD={FEEDER_EXPLORE_RUN_MOD}")

    gc = gs_client()
    sh = open_sheet(gc)

    raw_ws = sh.worksheet(RAW_DEALS_TAB)
    raw_headers = raw_ws.row_values(1)
    raw_headers_set = set([h.strip() for h in raw_headers if h])

    config_rows = load_config_rows(sh)
    themes_dict = load_themes_dict(sh)
    signals = load_signals(sh)
    allowed_pairs, origin_city_map, dest_to_origins = load_route_capability_map(sh)

    benchmarks: List[Dict[str, Any]] = []
    if PRICE_GATE_ENABLED:
        try:
            benchmarks = load_zone_theme_benchmarks(sh)
        except Exception as e:
            log(f"⚠️ PRICE_GATE: failed to load benchmarks tab '{BENCHMARKS_TAB}': {e} (fallback={PRICE_GATE_FALLBACK_BEHAVIOR})")
            benchmarks = []

    # Split config rows into today's theme vs explore pool
    today_routes: List[Dict[str, Any]] = []
    explore_routes: List[Dict[str, Any]] = []

    for r in config_rows:
        r_theme = str(r.get("theme") or "").strip()
        if r_theme == theme_today:
            today_routes.append(r)
        else:
            explore_routes.append(r)

    today_dest_configs = build_today_dest_configs(today_routes)
    explore_dest_configs = build_today_dest_configs(explore_routes)

    planned_origins = origin_plan_for_theme(theme_today, ROUTES_PER_RUN)
    log(f"🧭 Planned origins for run: {planned_origins}")
    log(f"🧭 Unique theme destinations: {len(today_dest_configs)} | Unique explore destinations: {len(explore_dest_configs)}")

    # Select routes for run
    selected_routes: List[Tuple[str, str, Dict[str, Any]]] = []
    di = 0
    oi = 0

    full_origin_pool = SHORT_HAUL_PRIMARY + SHORT_HAUL_FALLBACK + SNOW_POOL + LONG_HAUL_POOL
    full_origin_pool = _dedupe_keep_order(full_origin_pool)

    if theme_quota > 0 and today_dest_configs:
        picked, di, oi = select_routes_from_dest_configs(
            dest_configs=today_dest_configs,
            quota=theme_quota,
            planned_origins=planned_origins,
            allowed_pairs=allowed_pairs,
            dest_to_origins=dest_to_origins,
            full_origin_pool=full_origin_pool,
            open_origins=open_origins_effective,
            start_di=di,
            start_oi=oi,
        )
        selected_routes.extend(picked)

    if explore_quota > 0 and explore_dest_configs:
        picked, di, oi = select_routes_from_dest_configs(
            dest_configs=explore_dest_configs,
            quota=explore_quota,
            planned_origins=planned_origins,
            allowed_pairs=allowed_pairs,
            dest_to_origins=dest_to_origins,
            full_origin_pool=full_origin_pool,
            open_origins=open_origins_effective,
            start_di=0,
            start_oi=oi,
        )
        selected_routes.extend(picked)

    # Governor: don't exceed searches cap
    selected_routes = selected_routes[:MAX_SEARCHES_PER_RUN]

    searches_done = 0
    all_deals: List[Dict[str, Any]] = []

    def run_routes(routes: List[Tuple[str, str, Dict[str, Any]]], label: str) -> None:
        nonlocal searches_done, all_deals
        for (origin, destination, cfg) in routes:
            if searches_done >= MAX_SEARCHES_PER_RUN:
                break
            if len(all_deals) >= MAX_INSERTS_TOTAL:
                break

            dep_date, ret_date = pick_dates(cfg)
            deal_theme = str(cfg.get("theme") or theme_today).strip() or theme_today

            log(f"Duffel[{label}]: Searching {origin}->{destination} {dep_date.isoformat()}/{ret_date.isoformat()}")

            payload = {
                "data": {
                    "slices": [
                        {"origin": origin, "destination": destination, "departure_date": dep_date.isoformat()},
                        {"origin": destination, "destination": origin, "departure_date": ret_date.isoformat()},
                    ],
                    "passengers": [{"type": "adult"}],
                    "cabin_class": "economy",
                }
            }

            resp = duffel_search_offer_request(payload)
            searches_done += 1

            offers = extract_offers(resp)
            returned = len(offers)
            if returned == 0:
                log(f"Duffel[{label}]: offers_returned=0")
                continue

            # Sort cheapest-first
            offers_sorted = sorted(offers, key=lambda o: offer_price_gbp(o) if offer_price_gbp(o) > 0 else 1e18)

            cap_gbp: Optional[float] = None
            if PRICE_GATE_ENABLED and benchmarks:
                cap_gbp = compute_ingest_cap_gbp(benchmarks, deal_theme, origin, destination)

            if PRICE_GATE_ENABLED and (cap_gbp is None):
                if PRICE_GATE_FALLBACK_BEHAVIOR == "BLOCK":
                    log(f"Duffel[{label}]: BENCHMARK_MISS {origin}->{destination} theme={deal_theme} | fallback=BLOCK => inserted=0")
                    continue
                else:
                    log(f"Duffel[{label}]: BENCHMARK_MISS {origin}->{destination} theme={deal_theme} | fallback=ALLOW")

            remaining_total = max(0, MAX_INSERTS_TOTAL - len(all_deals))
            if remaining_total <= 0:
                break

            # We may need to evaluate more than OFFERS_PER_SEARCH if many get rejected.
            processed_cap = min(returned, max(OFFERS_PER_SEARCH, remaining_total * 6))

            inserted_here = 0
            rejected_price = 0
            processed = 0

            created_iso = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

            for off in offers_sorted[:processed_cap]:
                if len(all_deals) >= MAX_INSERTS_TOTAL:
                    break

                processed += 1
                price = offer_price_gbp(off)
                if price <= 0:
                    rejected_price += 1
                    continue

                if PRICE_GATE_ENABLED and cap_gbp is not None and price > cap_gbp:
                    rejected_price += 1
                    continue

                off_id = str(off.get("id") or "")
                deal_id_seed = f"{origin}->{destination}|{dep_date.isoformat()}|{ret_date.isoformat()}|{price:.2f}|{off_id}"
                deal_id = hashlib.sha256(deal_id_seed.encode("utf-8")).hexdigest()[:24]

                deal: Dict[str, Any] = {
                    "status": "NEW",
                    "deal_theme": deal_theme,
                    "theme": deal_theme,
                    "deal_id": deal_id,
                    "origin_iata": origin,
                    "origin_city": resolve_origin_city(origin, origin_city_map),
                    "destination_iata": destination,
                    "outbound_date": dep_date.strftime("%Y-%m-%d"),
                    "return_date": ret_date.strftime("%Y-%m-%d"),
                    "price_gbp": math.ceil(price),
                    "destination_city": "",
                    "destination_country": "",
                    "graphic_url": "",
                }

                # Timestamps (only set keys that exist in RAW_DEALS headers)
                if "created_utc" in raw_headers_set:
                    deal["created_utc"] = created_iso
                if "ingested_at_utc" in raw_headers_set:
                    deal["ingested_at_utc"] = created_iso
                if "created_at" in raw_headers_set:
                    deal["created_at"] = created_iso
                if "timestamp" in raw_headers_set:
                    deal["timestamp"] = created_iso

                deal = enrich_deal(deal, themes_dict, signals)
                all_deals.append(deal)
                inserted_here += 1

                if inserted_here >= OFFERS_PER_SEARCH:
                    break

            cap_str = f"{cap_gbp:.0f}" if cap_gbp is not None else "NONE"
            log(
                f"Duffel[{label}]: offers_returned={returned} processed={processed} "
                f"cap_gbp={cap_str} rejected_price={rejected_price} inserted={inserted_here} "
                f"running_total={len(all_deals)}/{MAX_INSERTS_TOTAL}"
            )

            if FEEDER_SLEEP_SECONDS > 0:
                import time
                time.sleep(FEEDER_SLEEP_SECONDS)

    # Primary pass
    run_routes(selected_routes, label="PRIMARY")

    # Zero-yield fallback: if primary yields zero deals, try explore burst within remaining budget
    if not all_deals and explore_dest_configs and searches_done < MAX_SEARCHES_PER_RUN:
        remaining = max(0, MAX_SEARCHES_PER_RUN - searches_done)
        fallback_quota = min(2, remaining)
        if fallback_quota > 0:
            log(f"🛟 Zero-yield fallback engaged: running {fallback_quota} explore searches within remaining budget.")
            today = dt.datetime.utcnow().date().isoformat()
            slot = _run_slot()
            seed = f"{FEEDER_EXPLORE_SALT}|{today}|{slot}|{theme_today}|FALLBACK"
            offset = _stable_mod(seed, max(1, len(explore_dest_configs)))
            rotated = explore_dest_configs[offset:] + explore_dest_configs[:offset]

            picked, _, _ = select_routes_from_dest_configs(
                dest_configs=rotated,
                quota=fallback_quota,
                planned_origins=planned_origins,
                allowed_pairs=allowed_pairs,
                dest_to_origins=dest_to_origins,
                full_origin_pool=full_origin_pool,
                open_origins=open_origins_effective,
                start_di=0,
                start_oi=0,
            )
            run_routes(picked, label="FALLBACK")

    log(f"✓ Searches completed: {searches_done}")
    log(f"✓ Deals collected: {len(all_deals)} (cap {MAX_INSERTS_TOTAL})")

    inserted = append_rows_header_mapped(raw_ws, all_deals)
    log(f"✅ Inserted {inserted} rows into {RAW_DEALS_TAB}")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        log("Interrupted.")
        raise
    except Exception as e:
        log(f"❌ FEEDER ERROR: {e}")
        raise
