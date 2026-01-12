# workers/pipeline_worker.py
#!/usr/bin/env python3
"""
TravelTxter Pipeline Worker (Feeder + Orchestrator) — LOCKED

This is the production feeder/orchestrator. It:
- Reads CONFIG (routes/weights)
- Optionally reads ROUTE_CAPABILITY_MAP (allowed route pairs, origin city mapping)
- Searches Duffel within safe caps
- Writes NEW rows to RAW_DEALS (header-mapped writes only)
- Enriches destination city/country via THEMES then CONFIG_SIGNALS
- IMPORTANT: origin_city is now guaranteed via:
    ROUTE_CAPABILITY_MAP origin_city -> fallback UK airport map -> "".

Do NOT reinvent this file. Only surgical fixes.

Surgical Fix (V4.6): Theme/haul-aware origin rotation
- CONFIG remains the source of DESTINATIONS and route rules.
- Origins are chosen per theme to avoid LGW-only lock-in:
  - short-haul themes prefer SW/LCC airports + London LCC fallback
  - snow themes prefer classic snow airports
  - long-haul themes prefer LHR/LGW
- Capability map remains authoritative for allowed (origin,destination) pairs.

Surgical Fix (2026-01-12): ISO date write + RAW append
- Prevents Google Sheets locale parsing flipping DD/MM into MM/DD.
"""

from __future__ import annotations

import os
import sys
import json
import time
import math
import random
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

# Safety caps (free tier governor)
MAX_INSERTS_TOTAL = int(os.getenv("DUFFEL_MAX_INSERTS", "3") or "3")
MAX_SEARCHES_PER_RUN = int(os.getenv("DUFFEL_MAX_SEARCHES_PER_RUN", "4") or "4")
ROUTES_PER_RUN = int(os.getenv("DUFFEL_ROUTES_PER_RUN", "3") or "3")

# Optional: strict mode (default TRUE)
STRICT_CAPABILITY_MAP = (os.getenv("STRICT_CAPABILITY_MAP", "true").strip().lower() == "true")

# Date window defaults (if CONFIG rows omit)
DEFAULT_DAYS_AHEAD_MIN = int(os.getenv("DAYS_AHEAD_MIN", "14") or "14")
DEFAULT_DAYS_AHEAD_MAX = int(os.getenv("DAYS_AHEAD_MAX", "90") or "90")

# Trip length defaults (if CONFIG rows omit)
DEFAULT_TRIP_LENGTH_DAYS = int(os.getenv("TRIP_LENGTH_DAYS", "5") or "5")


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
# Prefer ROUTE_CAPABILITY_MAP origin_city where available.
# If missing, fall back to a small, stable UK airport map (do not expand dynamically).
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

SNOW_THEMES = {
    "snow",
    "northern_lights",
}

LONG_HAUL_THEMES = {
    "long_haul",
    "luxury_value",  # treated as long-haul biased
}

# SW/LCC first (audience fit), London LCC fallback (inventory safety)
SHORT_HAUL_PRIMARY = ["BRS", "EXT", "NQY", "CWL", "SOU"]
SHORT_HAUL_FALLBACK = ["STN", "LTN", "LGW"]  # LCC-heavy London airports + LGW safety

# Snow "classic" airports
SNOW_POOL = ["BRS", "LGW", "STN", "LTN"]

# Long-haul hub airports
LONG_HAUL_POOL = ["LHR", "LGW"]

def _run_slot() -> str:
    # Existing workflows often set RUN_SLOT=AM/PM; if absent, stay deterministic by date only.
    return (os.getenv("RUN_SLOT") or "").strip().upper()

def _deterministic_pick(seq: List[str], seed: str, k: int) -> List[str]:
    """
    Deterministically pick k items from seq without repetition (if possible).
    """
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
    # If k > unique in seq, allow repeats deterministically
    while len(out) < k:
        out.append(seq[(h + len(out)) % n])
    return out

def origin_plan_for_theme(theme_today: str, routes_per_run: int) -> List[str]:
    """
    Returns a list of origins to try for this run (length == routes_per_run),
    biased per theme/haul, deterministic by date + run slot.
    Enforces a practical diversity constraint later (max 2 per origin).
    """
    today = dt.datetime.utcnow().date().isoformat()
    slot = _run_slot()
    seed_base = f"{today}|{slot}|{theme_today}"

    theme_today = low(theme_today)

    if theme_today in LONG_HAUL_THEMES:
        picks = _deterministic_pick(LONG_HAUL_POOL, seed_base, max(1, min(2, routes_per_run)))
        out: List[str] = []
        for i in range(routes_per_run):
            out.append(picks[i % len(picks)])
        return out

    if theme_today in SNOW_THEMES:
        picks = _deterministic_pick(SNOW_POOL, seed_base, min(len(SNOW_POOL), routes_per_run))
        out: List[str] = []
        for i in range(routes_per_run):
            out.append(picks[i % len(picks)])
        return out

    primary_n = 2 if routes_per_run >= 3 else 1
    fallback_n = max(0, routes_per_run - primary_n)

    prim = _deterministic_pick(SHORT_HAUL_PRIMARY, seed_base + "|P", primary_n)
    fb = _deterministic_pick(SHORT_HAUL_FALLBACK, seed_base + "|F", fallback_n)
    return prim + fb

def enforce_origin_diversity(origins: List[str]) -> List[str]:
    """
    Ensures no single origin dominates the run.
    Rule: max 2 occurrences of the same origin unless the list is too small.
    """
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


# ==================== SHEETS INIT ====================

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


# ==================== LOAD CONFIG / THEMES / SIGNALS ====================

def load_config_rows(sheet: gspread.Spreadsheet) -> List[Dict[str, Any]]:
    ws = sheet.worksheet(CONFIG_TAB)
    rows = ws.get_all_records()
    out = []
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
        log(f"⚠️ {msg} — continuing WITHOUT capability filtering (not recommended).")

    return allowed, origin_city_map


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

    # ✅ surgical: RAW preserves ISO strings; avoids locale date flips
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
    if preferred_origin:
        o = _clean_iata(preferred_origin)
        if not allowed_pairs or (o, dest) in allowed_pairs:
            return o

    for o in candidate_origins:
        oo = _clean_iata(o)
        if not allowed_pairs or (oo, dest) in allowed_pairs:
            return oo
    return None


# ==================== MAIN ====================

def main() -> int:
    log("=" * 80)
    log("TRAVELTXTTER PIPELINE WORKER (FEEDER) START")
    log("=" * 80)

    sheet_id = (os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID") or "").strip()
    if not sheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID/SHEET_ID")

    theme_today = low(os.getenv("THEME_OF_DAY") or "") or low(theme_of_day_utc())
    log(f"🎯 Theme of the day (UTC): {theme_today}")

    gc = gs_client()
    sh = gc.open_by_key(sheet_id)

    ws_raw = sh.worksheet(RAW_DEALS_TAB)

    config_rows = load_config_rows(sh)
    themes_dict = load_themes_dict(sh)
    signals = load_signals(sh)

    allowed_pairs, origin_city_map = load_route_capability_map(sh)

    today_routes = [r for r in config_rows if low(r.get("theme", "")) == theme_today]
    if not today_routes:
        log(f"⚠️ No enabled CONFIG routes found for theme={theme_today}")
        return 0

    today_routes.sort(key=lambda r: float(r.get("priority", 9999) or 9999))

    planned_origins = origin_plan_for_theme(theme_today, ROUTES_PER_RUN)
    planned_origins = enforce_origin_diversity(planned_origins)

    dest_configs = build_today_dest_configs(today_routes)

    log(f"🧭 Planned origins for run: {planned_origins}")
    log(f"🧭 Unique destinations available for theme: {len(dest_configs)}")

    searches_done = 0
    all_deals: List[Dict[str, Any]] = []

    theme_l = low(theme_today)
    if theme_l in LONG_HAUL_THEMES:
        full_origin_pool = LONG_HAUL_POOL[:]
    elif theme_l in SNOW_THEMES:
        full_origin_pool = SNOW_POOL[:]
    else:
        full_origin_pool = SHORT_HAUL_PRIMARY[:] + SHORT_HAUL_FALLBACK[:]

    selected_routes: List[Tuple[str, str, Dict[str, Any]]] = []

    di = 0
    oi = 0
    while len(selected_routes) < ROUTES_PER_RUN and di < len(dest_configs):
        cfg = dest_configs[di]
        destination = _clean_iata(cfg.get("destination_iata"))
        if not destination:
            di += 1
            continue

        preferred_origin = _clean_iata(cfg.get("origin_iata"))
        planned_origin = planned_origins[oi % len(planned_origins)] if planned_origins else ""
        oi += 1

        candidate_try_order = []
        if planned_origin:
            candidate_try_order.append(planned_origin)
        candidate_try_order.extend([o for o in full_origin_pool if o not in candidate_try_order])

        origin = pick_origin_for_dest(
            dest=destination,
            candidate_origins=candidate_try_order,
            allowed_pairs=allowed_pairs,
            preferred_origin=preferred_origin,
        )

        if origin:
            selected_routes.append((origin, destination, cfg))

        di += 1

    if not selected_routes:
        log("⚠️ No valid (origin,destination) pairs after capability filtering.")
        return 0

    for origin, destination, r in selected_routes:
        if searches_done >= MAX_SEARCHES_PER_RUN:
            break
        if len(all_deals) >= MAX_INSERTS_TOTAL:
            break

        days_min = int(r.get("days_ahead_min") or DEFAULT_DAYS_AHEAD_MIN)
        days_max = int(r.get("days_ahead_max") or DEFAULT_DAYS_AHEAD_MAX)
        trip_len = int(r.get("trip_length_days") or DEFAULT_TRIP_LENGTH_DAYS)
        max_conn = int(r.get("max_connections") or 0)

        seed = f"{dt.datetime.utcnow().date().isoformat()}|{origin}|{destination}|{trip_len}"
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
            log(f"Duffel: Searching {origin}->{destination} {dep_date.isoformat()}/{ret_date.isoformat()}")
            resp = duffel_search_offer_request(payload)
            searches_done += 1
        except Exception as e:
            log(f"❌ Duffel error: {e}")
            continue

        offers = (resp.get("data") or {}).get("offers") or []
        if not offers:
            continue

        for off in offers[: min(10, MAX_INSERTS_TOTAL - len(all_deals))]:
            try:
                total_amount = float(off.get("total_amount") or "0")
            except Exception:
                total_amount = 0.0

            deal_id_seed = f"{origin}->{destination}|{dep_date.isoformat()}|{ret_date.isoformat()}|{total_amount}"
            deal = {
                "status": "NEW",
                "deal_theme": theme_today,
                "theme": theme_today,
                "deal_id": str(abs(hash(deal_id_seed))),
                "origin_iata": origin,
                "origin_city": resolve_origin_city(origin, origin_city_map),
                "destination_iata": destination,

                # ✅ surgical: write ISO to stop date flips
                "outbound_date": dep_date.strftime("%Y-%m-%d"),
                "return_date": ret_date.strftime("%Y-%m-%d"),

                "price_gbp": math.ceil(total_amount) if total_amount else "",
                "destination_city": "",
                "destination_country": "",
                "graphic_url": "",
            }

            deal = enrich_deal(deal, themes_dict, signals)
            all_deals.append(deal)

            if len(all_deals) >= MAX_INSERTS_TOTAL:
                break

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
