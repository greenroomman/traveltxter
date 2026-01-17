#!/usr/bin/env python3
"""
TravelTxter Pipeline Worker (Feeder + Orchestrator) — LOCKED

Surgical Fix (V4.6): Theme/haul-aware origin rotation
Surgical Fix (2026-01-12): ISO date write + RAW append
Surgical Fix (2026-01-12): FEEDER_OPEN_ORIGINS (Reverse capability injection)

Surgical Fix (2026-01-12): 90/10 HYBRID FEEDER
- 90%: theme-constrained route selection (brand coherence)
- 10%: single "wildcard" route from enabled CONFIG outside today's theme
- Deterministic + stateless (Sheets remains the only memory)

Surgical Fix (2026-01-16): ZERO-YIELD FEEDER RECOVERY (no refactor)
- Sparse-theme override: enable reverse-origin selection when theme has thin inventory
- Forced explore burst when primary pass yields 0 deals (within remaining search budget)

Surgical Fix (2026-01-17): INGEST TIMESTAMP + OFFER VOLUME GOVERNOR
- Always write created_utc (ISO Z) so ai_scorer freshness gate can function
- Use DUFFEL_OFFERS_PER_SEARCH (default 50) instead of hard-coded 10
- Stable deal_id generation (sha256) to avoid Python hash instability
- Optional RESPECT_CONFIG_ORIGIN to prevent origin collapse to config origin
"""

from __future__ import annotations

import os
import sys
import json
import time
import math
import hashlib
import datetime as dt
from typing import Any, Dict, List, Tuple, Optional, Set

import requests
import gspread
from google.oauth2.service_account import Credentials


# ==================== CONSTANTS ====================

RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS").strip() or "RAW_DEALS"
CONFIG_TAB = os.getenv("CONFIG_TAB", "CONFIG").strip() or "CONFIG"
THEMES_TAB = os.getenv("THEMES_TAB", "THEMES").strip() or "THEMES"
SIGNALS_TAB = os.getenv("SIGNALS_TAB", "CONFIG_SIGNALS").strip() or "CONFIG_SIGNALS"
CAPABILITY_TAB = os.getenv("CAPABILITY_TAB", "ROUTE_CAPABILITY_MAP").strip() or "ROUTE_CAPABILITY_MAP"

DUFFEL_API_KEY = os.getenv("DUFFEL_API_KEY", "").strip()
DUFFEL_API_BASE = os.getenv("DUFFEL_API_BASE", "https://api.duffel.com").strip()
DUFFEL_VERSION = os.getenv("DUFFEL_VERSION", "v2").strip() or "v2"

SPREADSHEET_ID = (os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID") or "").strip()

# Safety caps (free tier governor)
MAX_INSERTS_TOTAL = int(os.getenv("DUFFEL_MAX_INSERTS", "3") or "3")
MAX_SEARCHES_PER_RUN = int(os.getenv("DUFFEL_MAX_SEARCHES_PER_RUN", "4") or "4")
ROUTES_PER_RUN = int(os.getenv("DUFFEL_ROUTES_PER_RUN", "3") or "3")

# Offer volume per Duffel search (how many rows we ingest from each offer_request)
DUFFEL_OFFERS_PER_SEARCH = int(os.getenv("DUFFEL_OFFERS_PER_SEARCH", "50") or "50")

# Optional pause between Duffel searches (API politeness)
FEEDER_SLEEP_SECONDS = float(os.getenv("FEEDER_SLEEP_SECONDS", "0") or "0")

# Optional: strict mode (default TRUE)
STRICT_CAPABILITY_MAP = (os.getenv("STRICT_CAPABILITY_MAP", "true").strip().lower() == "true")

# NEW: open origins using reverse capability map
FEEDER_OPEN_ORIGINS = (os.getenv("FEEDER_OPEN_ORIGINS", "false").strip().lower() == "true")

# NEW: if true, prefer CONFIG origin_iata over planned origins (default false to avoid collapse)
RESPECT_CONFIG_ORIGIN = (os.getenv("RESPECT_CONFIG_ORIGIN", "false").strip().lower() == "true")

# Sparse themes have thin inventory from many regional airports.
# For these themes we allow reverse-capability origin selection even if
# FEEDER_OPEN_ORIGINS is false, so the feeder doesn't return 0 deals by construction.
SPARSE_THEMES = {
    "northern_lights",
}

# Date window defaults (if CONFIG rows omit)
DEFAULT_DAYS_AHEAD_MIN = int(os.getenv("DAYS_AHEAD_MIN", "14") or "14")
DEFAULT_DAYS_AHEAD_MAX = int(os.getenv("DAYS_AHEAD_MAX", "90") or "90")

# Trip length defaults (if CONFIG rows omit)
DEFAULT_TRIP_LENGTH_DAYS = int(os.getenv("TRIP_LENGTH_DAYS", "5") or "5")

# 90/10 hybrid control (deterministic)
FEEDER_EXPLORE_RUN_MOD = int(os.getenv("FEEDER_EXPLORE_RUN_MOD", "10") or "10")  # 10 => ~10%
FEEDER_EXPLORE_SALT = (os.getenv("FEEDER_EXPLORE_SALT", "traveltxter") or "traveltxter").strip()


# ==================== THEME OF DAY ====================

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

# ============================================================
# Origin city fallback (for captions/render when ROUTE_CAPABILITY_MAP lacks origin_city)
# ============================================================
ORIGIN_CITY_FALLBACK = {
    "LHR": "London",
    "LGW": "London",
    "STN": "London",
    "LTN": "London",
    "LCY": "London",
    "SEN": "London",
    "MAN": "Manchester",
    "BHX": "Birmingham",
    "BRS": "Bristol",
    "EXT": "Exeter",
    "NQY": "Newquay",
    "SOU": "Southampton",
    "CWL": "Cardiff",
    "LPL": "Liverpool",
    "EDI": "Edinburgh",
    "GLA": "Glasgow",
    "NCL": "Newcastle",
    "LBA": "Leeds",
    "EMA": "East Midlands",
    "DSA": "Doncaster",
    "ABZ": "Aberdeen",
    "BFS": "Belfast",
    "BHD": "Belfast",
}


def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


def low(s: str) -> str:
    return (s or "").strip().lower()


def theme_of_day_utc() -> str:
    today = dt.datetime.utcnow().date()
    doy = int(today.strftime("%j"))
    return MASTER_THEMES[doy % len(MASTER_THEMES)]


def _clean_iata(x: Any) -> str:
    return (str(x or "").strip().upper())[:3]


def resolve_origin_city(iata: str, origin_city_map: Dict[str, str]) -> str:
    iata = _clean_iata(iata)
    return (origin_city_map.get(iata) or ORIGIN_CITY_FALLBACK.get(iata) or "").strip()


# ==================== 90/10 HYBRID (stateless + deterministic) ====================

def _stable_mod(key: str, mod: int) -> int:
    h = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return int(h[:8], 16) % max(1, mod)


def should_do_explore_this_run(theme_today: str) -> bool:
    """
    About 1 in FEEDER_EXPLORE_RUN_MOD runs becomes an explore run.
    Deterministic: date + RUN_SLOT + GitHub run identifiers.
    """
    today = dt.datetime.utcnow().date().isoformat()
    run_slot = (os.getenv("RUN_SLOT") or "UNSET").strip().upper()
    gh_run_id = (os.getenv("GITHUB_RUN_ID") or "").strip()
    gh_run_attempt = (os.getenv("GITHUB_RUN_ATTEMPT") or "").strip()

    key = f"{FEEDER_EXPLORE_SALT}|{today}|{run_slot}|{theme_today}|{gh_run_id}|{gh_run_attempt}"
    return _stable_mod(key, FEEDER_EXPLORE_RUN_MOD) == 0


# ==================== ORIGIN POOLS (Theme/Haul aware) ====================

SHORT_HAUL_THEMES = {
    "winter_sun",
    "summer_sun",
    "beach_break",
    "surf",
    "city_breaks",
    "culture_history",
    "unexpected_value",
    "adventure",
}

SNOW_THEMES = {"snow", "northern_lights"}

LONG_HAUL_THEMES = {"long_haul", "luxury_value"}

SHORT_HAUL_PRIMARY = ["BRS", "EXT", "NQY", "CWL", "SOU"]
SHORT_HAUL_FALLBACK = ["STN", "LTN", "LGW"]

SNOW_POOL = ["BRS", "LGW", "STN", "LTN"]
LONG_HAUL_POOL = ["LHR", "LGW"]


def _run_slot() -> str:
    return (os.getenv("RUN_SLOT") or "").strip().upper()


def _deterministic_pick(seq: List[str], seed: str, k: int) -> List[str]:
    if not seq or k <= 0:
        return []
    h = int(hashlib.md5(seed.encode()).hexdigest()[:8], 16)
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
    theme_today = low(theme_today)

    if theme_today in LONG_HAUL_THEMES:
        picks = _deterministic_pick(LONG_HAUL_POOL, seed_base, max(1, min(2, routes_per_run)))
        return [picks[i % len(picks)] for i in range(routes_per_run)]

    if theme_today in SNOW_THEMES:
        picks = _deterministic_pick(SNOW_POOL, seed_base, min(len(SNOW_POOL), routes_per_run))
        return [picks[i % len(picks)] for i in range(routes_per_run)]

    primary_n = 2 if routes_per_run >= 3 else 1
    fallback_n = max(0, routes_per_run - primary_n)

    prim = _deterministic_pick(SHORT_HAUL_PRIMARY, seed_base + "|P", primary_n)
    fb = _deterministic_pick(SHORT_HAUL_FALLBACK, seed_base + "|F", fallback_n)
    return (prim + fb)[:routes_per_run]


def enforce_origin_diversity(origins: List[str]) -> List[str]:
    """
    Prevent accidental same-origin collapse.
    Allows repeats, but avoids making the whole run one-origin by construction.
    """
    if not origins:
        return origins
    out: List[str] = []
    counts: Dict[str, int] = {}
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


# ==================== GOOGLE SHEETS HELPERS ====================

def _parse_sa_json(raw: str) -> dict:
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
    out = []
    for r in rows:
        enabled = low(r.get("enabled", ""))
        if enabled in ("true", "yes", "1", "y"):
            out.append(r)
    return out


def load_themes_dict(sheet: gspread.Spreadsheet) -> Dict[str, List[Dict[str, Any]]]:
    ws = sheet.worksheet(THEMES_TAB)
    rows = ws.get_all_records()
    themes: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        t = low(r.get("theme", ""))
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


def _dedupe_keep_order(seq: List[str]) -> List[str]:
    out: List[str] = []
    for x in seq:
        xx = _clean_iata(x)
        if not xx:
            continue
        if xx not in out:
            out.append(xx)
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
        log(f"⚠️ {msg} — continuing WITHOUT capability filtering (not recommended).")

    return allowed, origin_city_map, dest_to_origins


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


# ==================== ENRICH DEAL ====================

def enrich_deal(deal: Dict[str, Any], themes_dict: Dict[str, List[Dict[str, Any]]], signals: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    dest = deal.get("destination_iata", "")
    theme = deal.get("deal_theme") or deal.get("theme") or ""

    if theme and theme in themes_dict:
        for d in themes_dict[theme]:
            if d.get("destination_iata") == dest:
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

def append_rows_header_mapped(ws, deals: List[Dict[str, Any]]) -> int:
    if not deals:
        return 0

    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("RAW_DEALS header row is empty")

    rows = []
    for d in deals:
        row = []
        for h in headers:
            row.append(d.get(h, ""))
        rows.append(row)

    ws.append_rows(rows, value_input_option="RAW")
    return len(rows)


# ==================== ROUTE SELECTION (surgical) ====================

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


def pick_origin_for_dest(
    dest: str,
    candidate_origins: List[str],
    allowed_pairs: Set[Tuple[str, str]],
    preferred_origin: str = "",
) -> Optional[str]:
    """
    If RESPECT_CONFIG_ORIGIN=true, CONFIG origin_iata is tried first.
    Otherwise, planned origin rotation is tried first and CONFIG origin becomes a fallback.
    """
    pref = _clean_iata(preferred_origin) if preferred_origin else ""

    if RESPECT_CONFIG_ORIGIN and pref:
        if not allowed_pairs or (pref, dest) in allowed_pairs:
            return pref

    for o in candidate_origins:
        oo = _clean_iata(o)
        if not allowed_pairs or (oo, dest) in allowed_pairs:
            return oo

    if (not RESPECT_CONFIG_ORIGIN) and pref:
        if not allowed_pairs or (pref, dest) in allowed_pairs:
            return pref

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

        if planned_origin:
            candidate_try_order.append(planned_origin)

        if open_origins:
            rev = dest_to_origins.get(destination, [])[:]
            rev = sorted(_dedupe_keep_order(rev))
            candidate_try_order.extend([o for o in rev if o not in candidate_try_order])

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


# ==================== MAIN ====================

def main() -> int:
    log("=" * 80)
    log("TRAVELTXTTER PIPELINE WORKER (FEEDER) START")
    log("=" * 80)

    theme_today = low(os.getenv("THEME_OF_DAY") or "") or low(theme_of_day_utc())
    open_origins_effective = FEEDER_OPEN_ORIGINS or (theme_today in SPARSE_THEMES)

    log(f"🎯 Theme of the day (UTC): {theme_today}")
    log(f"FEEDER_OPEN_ORIGINS={FEEDER_OPEN_ORIGINS} | sparse_theme_override={theme_today in SPARSE_THEMES} | effective={open_origins_effective}")
    log(f"RESPECT_CONFIG_ORIGIN={RESPECT_CONFIG_ORIGIN}")
    log(f"DUFFEL_OFFERS_PER_SEARCH={DUFFEL_OFFERS_PER_SEARCH} | DUFFEL_MAX_INSERTS={MAX_INSERTS_TOTAL} | DUFFEL_MAX_SEARCHES_PER_RUN={MAX_SEARCHES_PER_RUN} | DUFFEL_ROUTES_PER_RUN={ROUTES_PER_RUN}")
    log(f"FEEDER_SLEEP_SECONDS={FEEDER_SLEEP_SECONDS}")

    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID/SHEET_ID")

    do_explore = should_do_explore_this_run(theme_today)

    gc = gs_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_raw = sh.worksheet(RAW_DEALS_TAB)

    config_rows = load_config_rows(sh)
    themes_dict = load_themes_dict(sh)
    signals = load_signals(sh)
    allowed_pairs, origin_city_map, dest_to_origins = load_route_capability_map(sh)

    today_routes = [r for r in config_rows if low(r.get("theme", "")) == theme_today]
    if not today_routes:
        log(f"⚠️ No enabled CONFIG routes found for theme={theme_today}")
        return 0
    today_routes.sort(key=lambda r: float(r.get("priority", 9999) or 9999))

    explore_routes = [r for r in config_rows if low(r.get("theme", "")) != theme_today]

    planned_origins = enforce_origin_diversity(origin_plan_for_theme(theme_today, ROUTES_PER_RUN))
    log(f"🧭 Planned origins for run: {planned_origins}")

    theme_dest_configs = build_today_dest_configs(today_routes)
    explore_dest_configs = build_today_dest_configs(explore_routes)

    explore_quota = 1 if (do_explore and ROUTES_PER_RUN >= 2 and len(explore_dest_configs) > 0) else 0
    theme_quota = max(0, ROUTES_PER_RUN - explore_quota)

    log(f"🧠 Feeder strategy: 90/10 | explore_run={do_explore} | theme_quota={theme_quota} | explore_quota={explore_quota} | MOD={FEEDER_EXPLORE_RUN_MOD}")
    log(f"🧭 Unique theme destinations: {len(theme_dest_configs)} | Unique explore destinations: {len(explore_dest_configs)}")

    if theme_today in LONG_HAUL_THEMES:
        full_origin_pool = LONG_HAUL_POOL[:]
    elif theme_today in SNOW_THEMES:
        full_origin_pool = SNOW_POOL[:]
    else:
        full_origin_pool = SHORT_HAUL_PRIMARY[:] + SHORT_HAUL_FALLBACK[:]

    selected_routes: List[Tuple[str, str, Dict[str, Any]]] = []
    searches_done = 0
    all_deals: List[Dict[str, Any]] = []

    theme_selected, _, oi = select_routes_from_dest_configs(
        dest_configs=theme_dest_configs,
        quota=theme_quota,
        planned_origins=planned_origins,
        allowed_pairs=allowed_pairs,
        dest_to_origins=dest_to_origins,
        full_origin_pool=full_origin_pool,
        open_origins=open_origins_effective,
        start_di=0,
        start_oi=0,
    )
    selected_routes.extend(theme_selected)

    if explore_quota == 1:
        today = dt.datetime.utcnow().date().isoformat()
        slot = _run_slot()
        seed = f"{FEEDER_EXPLORE_SALT}|{today}|{slot}|{theme_today}|EXPLORE"
        offset = _stable_mod(seed, len(explore_dest_configs))
        rotated = explore_dest_configs[offset:] + explore_dest_configs[:offset]

        explore_selected, _, _ = select_routes_from_dest_configs(
            dest_configs=rotated,
            quota=1,
            planned_origins=planned_origins,
            allowed_pairs=allowed_pairs,
            dest_to_origins=dest_to_origins,
            full_origin_pool=full_origin_pool,
            open_origins=open_origins_effective,
            start_di=0,
            start_oi=oi,
        )
        if explore_selected:
            selected_routes.extend(explore_selected)
            log(f"🧪 Explore route added: {explore_selected[0][0]}->{explore_selected[0][1]} theme={low(explore_selected[0][2].get('theme',''))}")
        else:
            log("🧪 Explore route requested but none valid after capability filtering (not an error).")

    if not selected_routes:
        log("⚠️ No valid (origin,destination) pairs after capability filtering.")
        return 0

    selected_routes = selected_routes[:ROUTES_PER_RUN]

    def run_routes(routes: List[Tuple[str, str, Dict[str, Any]]], label: str) -> None:
        nonlocal searches_done, all_deals

        for origin, destination, r in routes:
            if searches_done >= MAX_SEARCHES_PER_RUN:
                break
            if len(all_deals) >= MAX_INSERTS_TOTAL:
                break

            days_min = int(r.get("days_ahead_min") or DEFAULT_DAYS_AHEAD_MIN)
            days_max = int(r.get("days_ahead_max") or DEFAULT_DAYS_AHEAD_MAX)
            trip_len = int(r.get("trip_length_days") or DEFAULT_TRIP_LENGTH_DAYS)
            max_conn = int(r.get("max_connections") or 0)

            seed = f"{dt.datetime.utcnow().date().isoformat()}|{origin}|{destination}|{trip_len}|{label}"
            hsh = int(hashlib.md5(seed.encode()).hexdigest()[:8], 16)
            dep_offset = days_min + (hsh % max(1, (days_max - days_min + 1)))
            dep_date = (dt.datetime.utcnow().date() + dt.timedelta(days=dep_offset))
            ret_date = (dep_date + dt.timedelta(days=trip_len))

            payload = {
                "data": {
                    "slices": [
                        {"origin": origin, "destination": destination, "departure_date": dep_date.isoformat()},
                        {"origin": destination, "destination": origin, "departure_date": ret_date.isoformat()},
                    ],
                    "passengers": [{"type": "adult"}],
                    "cabin_class": (r.get("cabin_class") or "economy"),
                    "max_connections": max_conn,
                    "return_offers": True,
                }
            }

            try:
                log(f"Duffel[{label}]: Searching {origin}->{destination} {dep_date.isoformat()}/{ret_date.isoformat()}")
                resp = duffel_search_offer_request(payload)
                searches_done += 1
                if FEEDER_SLEEP_SECONDS > 0:
                    time.sleep(FEEDER_SLEEP_SECONDS)
            except Exception as e:
                log(f"❌ Duffel[{label}] error: {e}")
                continue

            offers = (resp.get("data") or {}).get("offers") or []
            if not offers:
                continue

            deal_theme = low(r.get("theme") or theme_today) or theme_today

            remaining = max(0, MAX_INSERTS_TOTAL - len(all_deals))
            take = min(DUFFEL_OFFERS_PER_SEARCH, remaining)

            # CRITICAL: ingest timestamp (ISO Z)
            created_utc = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

            log(f"Duffel[{label}]: offers_returned={len(offers)} | will_ingest_up_to={take}")

            for off in offers[:take]:
                if len(all_deals) >= MAX_INSERTS_TOTAL:
                    break

                try:
                    total_amount = float(off.get("total_amount") or "0")
                except Exception:
                    total_amount = 0.0

                # Stable id (do NOT use Python hash())
                off_id = str(off.get("id") or "")
                deal_id_seed = f"{origin}->{destination}|{dep_date.isoformat()}|{ret_date.isoformat()}|{total_amount}|{off_id}"
                deal_id = str(int(hashlib.sha256(deal_id_seed.encode("utf-8")).hexdigest()[:12], 16))

                deal = {
                    "status": "NEW",
                    "deal_theme": deal_theme,
                    "theme": deal_theme,
                    "deal_id": deal_id,
                    "origin_iata": origin,
                    "origin_city": resolve_origin_city(origin, origin_city_map),
                    "destination_iata": destination,
                    "outbound_date": dep_date.strftime("%Y-%m-%d"),
                    "return_date": ret_date.strftime("%Y-%m-%d"),
                    "price_gbp": math.ceil(total_amount) if total_amount else "",
                    "destination_city": "",
                    "destination_country": "",
                    "created_utc": created_utc,
                    "created_at": created_utc,
                    "timestamp": created_utc,
                    "graphic_url": "",
                }

                deal = enrich_deal(deal, themes_dict, signals)
                all_deals.append(deal)

    run_routes(selected_routes, label="PRIMARY")

    if not all_deals and explore_dest_configs and searches_done < MAX_SEARCHES_PER_RUN:
        remaining = max(0, MAX_SEARCHES_PER_RUN - searches_done)
        fallback_quota = min(2, remaining)
        if fallback_quota > 0:
            today = dt.datetime.utcnow().date().isoformat()
            slot = _run_slot()
            seed = f"{FEEDER_EXPLORE_SALT}|{today}|{slot}|{theme_today}|FALLBACK"
            offset = _stable_mod(seed, len(explore_dest_configs))
            rotated = explore_dest_configs[offset:] + explore_dest_configs[:offset]

            fallback_selected, _, _ = select_routes_from_dest_configs(
                dest_configs=rotated,
                quota=fallback_quota,
                planned_origins=planned_origins,
                allowed_pairs=allowed_pairs,
                dest_to_origins=dest_to_origins,
                full_origin_pool=full_origin_pool,
                open_origins=open_origins_effective,
                start_di=0,
                start_oi=oi,
            )
            if fallback_selected:
                log(f"🛟 Zero-yield fallback engaged: running {len(fallback_selected)} explore searches within remaining budget.")
                run_routes(fallback_selected, label="FALLBACK")
            else:
                log("🛟 Zero-yield fallback engaged but no valid explore routes after capability filtering.")

    log(f"✓ Searches completed: {searches_done}")
    log(f"✓ Deals collected: {len(all_deals)} (cap {MAX_INSERTS_TOTAL})")

    if not all_deals:
        log("⚠️ No deals found. (Not an error; depends on availability/prices.)")
        return 0

    inserted = append_rows_header_mapped(ws_raw, all_deals)
    log(f"✅ Inserted {inserted} rows into {RAW_DEALS_TAB}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
