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
    # enabled must be truthy
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
    """
    Returns:
      - allowed_pairs: set of (origin_iata, destination_iata)
      - origin_city_map: origin_iata -> origin_city (best effort)
    """
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
    """Priority: THEMES > CONFIG_SIGNALS for destination city/country."""
    dest = deal.get("destination_iata", "")
    theme = deal.get("deal_theme") or deal.get("theme") or ""

    # THEMES lookup (theme-specific)
    if theme and theme in themes_dict:
        for d in themes_dict[theme]:
            if d.get("destination_iata") == dest:
                if d.get("destination_city"):
                    deal["destination_city"] = d["destination_city"]
                if d.get("destination_country"):
                    deal["destination_country"] = d["destination_country"]
                break

    # CONFIG_SIGNALS fallback
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

    ws.append_rows(rows, value_input_option="USER_ENTERED")
    return len(rows)


# ==================== MAIN ====================

def main() -> int:
    log("=" * 80)
    log("TRAVELTXTTER PIPELINE WORKER (FEEDER) START")
    log("=" * 80)

    sheet_id = (os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID") or "").strip()
    if not sheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID/SHEET_ID")

    # Theme-of-day (feeder intent)
    theme_today = low(os.getenv("THEME_OF_DAY") or "") or low(theme_of_day_utc())
    log(f"🎯 Theme of the day (UTC): {theme_today}")

    gc = gs_client()
    sh = gc.open_by_key(sheet_id)

    ws_raw = sh.worksheet(RAW_DEALS_TAB)

    config_rows = load_config_rows(sh)
    themes_dict = load_themes_dict(sh)
    signals = load_signals(sh)

    allowed_pairs, origin_city_map = load_route_capability_map(sh)

    # Filter config rows to today theme (feeder intent)
    today_routes = [r for r in config_rows if low(r.get("theme", "")) == theme_today]
    if not today_routes:
        log(f"⚠️ No enabled CONFIG routes found for theme={theme_today}")
        return 0

    # Sort by priority (lower number = higher priority)
    today_routes.sort(key=lambda r: float(r.get("priority", 9999) or 9999))

    searches_done = 0
    all_deals: List[Dict[str, Any]] = []

    # Route sampling (respect cap)
    for r in today_routes[:ROUTES_PER_RUN]:
        if searches_done >= MAX_SEARCHES_PER_RUN:
            break
        if len(all_deals) >= MAX_INSERTS_TOTAL:
            break

        origin = _clean_iata(r.get("origin_iata"))
        destination = _clean_iata(r.get("destination_iata"))

        if allowed_pairs and (origin, destination) not in allowed_pairs:
            continue

        days_min = int(r.get("days_ahead_min") or DEFAULT_DAYS_AHEAD_MIN)
        days_max = int(r.get("days_ahead_max") or DEFAULT_DAYS_AHEAD_MAX)
        trip_len = int(r.get("trip_length_days") or DEFAULT_TRIP_LENGTH_DAYS)
        max_conn = int(r.get("max_connections") or 0)

        # choose travel dates deterministically per route/day
        seed = f"{dt.datetime.utcnow().date().isoformat()}|{origin}|{destination}|{trip_len}"
        hsh = int(hashlib.md5(seed.encode()).hexdigest()[:8], 16)
        dep_offset = days_min + (hsh % max(1, (days_max - days_min + 1)))
        dep_date = (dt.datetime.utcnow().date() + dt.timedelta(days=dep_offset))
        ret_date = (dep_date + dt.timedelta(days=trip_len))

        # Duffel payload
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

        # Take a small sample of offers for free tier yield
        for off in offers[: min(10, MAX_INSERTS_TOTAL - len(all_deals))]:
            try:
                total_amount = float(off.get("total_amount") or "0")
            except Exception:
                total_amount = 0.0

            # Build seed for stable deal_id
            deal_id_seed = f"{origin}->{destination}|{dep_date.isoformat()}|{ret_date.isoformat()}|{total_amount}"
            deal = {
                # status lifecycle
                "status": "NEW",

                # theme (write both for compatibility)
                "deal_theme": theme_today,
                "theme": theme_today,

                # identifiers
                "deal_id": str(abs(hash(deal_id_seed))),

                # route
                "origin_iata": origin,
                "origin_city": resolve_origin_city(origin, origin_city_map),  # map->fallback
                "destination_iata": destination,

                # dates
                "outbound_date": dep_date.strftime("%d/%m/%Y"),
                "return_date": ret_date.strftime("%d/%m/%Y"),

                # price
                "price_gbp": math.ceil(total_amount) if total_amount else "",

                # placeholders downstream may fill
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
