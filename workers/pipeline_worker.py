#!/usr/bin/env python3
"""
workers/pipeline_worker.py

TravelTxter Pipeline Worker (FEEDER) - FUNCTIONAL DEAL DISCOVERY ENGINE
Built to REPLACE the demo stub with actual Duffel API integration.

PRINCIPLES:
- Google Sheets is single source of truth
- CONFIG defines active routes (active_in_feeder=TRUE)
- ROUTE_CAPABILITY_MAP validates route exists
- ZONE_THEME_BENCHMARKS filters by price gates
- Duffel API searches for actual flight offers
- RAW_DEALS receives complete deal data
"""

from __future__ import annotations

import os
import sys
import json
import time
import hashlib
import datetime as dt
from typing import Any, Dict, List, Tuple, Optional, Set
from decimal import Decimal

import requests
import gspread
from google.oauth2.service_account import Credentials

# ==================== ENVIRONMENT VARIABLES ====================

# Spreadsheet
SPREADSHEET_ID = (os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID") or "").strip()

# Service Account
GCP_SA_JSON = (os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON") or "").strip()

# Tab names
RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS").strip() or "RAW_DEALS"
CONFIG_TAB = os.getenv("FEEDER_CONFIG_TAB", os.getenv("CONFIG_TAB", "CONFIG")).strip() or "CONFIG"
THEMES_TAB = os.getenv("THEMES_TAB", "THEMES").strip() or "THEMES"
CAPABILITY_TAB = os.getenv("CAPABILITY_TAB", "ROUTE_CAPABILITY_MAP").strip() or "ROUTE_CAPABILITY_MAP"
BENCHMARKS_TAB = os.getenv("BENCHMARKS_TAB", "ZONE_THEME_BENCHMARKS").strip() or "ZONE_THEME_BENCHMARKS"

# Duffel API
DUFFEL_API_KEY = os.getenv("DUFFEL_API_KEY", "").strip()
DUFFEL_API_BASE = os.getenv("DUFFEL_API_BASE", "https://api.duffel.com").strip()
DUFFEL_VERSION = os.getenv("DUFFEL_VERSION", "v2").strip() or "v2"

# Operational limits
DUFFEL_MAX_SEARCHES_PER_RUN = int(os.getenv("DUFFEL_MAX_SEARCHES_PER_RUN", "50"))
DUFFEL_MAX_INSERTS = int(os.getenv("DUFFEL_MAX_INSERTS", "100"))
DUFFEL_MAX_INSERTS_PER_ROUTE = int(os.getenv("DUFFEL_MAX_INSERTS_PER_ROUTE", "3"))

# Search params
FEEDER_SLEEP_SECONDS = float(os.getenv("FEEDER_SLEEP_SECONDS", "1.0"))


# ==================== LOGGING ====================

def log(msg: str) -> None:
    """Thread-safe logging with UTC timestamp"""
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


# ==================== UTILITIES ====================

def _clean_iata(x: Any) -> str:
    """Clean and uppercase IATA code"""
    return str(x or "").strip().upper()[:3]


def _parse_float(x: Any) -> Optional[float]:
    """Safely parse float"""
    if x is None or x == "":
        return None
    try:
        return float(x)
    except (ValueError, TypeError):
        return None


def _truthy(v: Any) -> bool:
    """Check if value is truthy (TRUE, YES, Y, 1)"""
    return str(v or "").strip().upper() in ("TRUE", "YES", "Y", "1")


def _generate_deal_id(origin: str, dest: str, outbound_date: str, price: float) -> str:
    """Generate deterministic deal_id"""
    key = f"{origin}|{dest}|{outbound_date}|{price:.2f}"
    return hashlib.md5(key.encode()).hexdigest()[:16]


# ==================== GOOGLE SHEETS ====================

def _parse_sa_json(raw: str) -> Dict[str, Any]:
    """Parse service account JSON (handles escaped newlines)"""
    raw = (raw or "").strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return json.loads(raw.replace("\\n", "\n"))


def gs_client() -> gspread.Client:
    """Create authenticated Google Sheets client"""
    if not GCP_SA_JSON:
        raise RuntimeError("Missing GCP_SA_JSON")
    
    info = _parse_sa_json(GCP_SA_JSON)
    creds = Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ],
    )
    return gspread.authorize(creds)


# ==================== DATA LOADERS ====================

def load_config_routes(sheet: gspread.Spreadsheet) -> List[Dict[str, Any]]:
    """
    Load active routes from CONFIG tab.
    Only returns routes where active_in_feeder = TRUE.
    """
    try:
        ws = sheet.worksheet(CONFIG_TAB)
        rows = ws.get_all_records()
    except Exception as e:
        log(f"⚠️ CONFIG tab not readable: {e}")
        return []
    
    active_routes = []
    for r in rows:
        # Check if route is active
        if not _truthy(r.get("active_in_feeder", r.get("enabled", "FALSE"))):
            continue
        
        # Extract route details
        route = {
            "origin_iata": _clean_iata(r.get("origin_iata")),
            "destination_iata": _clean_iata(r.get("destination_iata")),
            "theme": str(r.get("theme", "")).strip().lower(),
            "days_ahead_min": int(r.get("days_ahead_min", 21)),
            "days_ahead_max": min(int(r.get("days_ahead_max", 84)), 84),  # Duffel 84-day limit
            "trip_length_days": int(r.get("trip_length_days", 7)),
            "max_connections": int(r.get("max_connections", 1)),
            "included_airlines": str(r.get("included_airlines", "")).strip(),
            "cabin_class": str(r.get("cabin_class", "economy")).strip().lower(),
            "search_weight": _parse_float(r.get("search_weight", 1.0)) or 1.0,
            "priority": int(r.get("priority", 1)),
        }
        
        # Validate required fields
        if not route["origin_iata"] or not route["destination_iata"]:
            continue
        
        active_routes.append(route)
    
    log(f"✅ CONFIG loaded: {len(active_routes)} active routes")
    return active_routes


def load_route_capability_map(sheet: gspread.Spreadsheet) -> Dict[Tuple[str, str], Dict[str, str]]:
    """
    Load ROUTE_CAPABILITY_MAP for validation and enrichment.
    Returns dict keyed by (origin, destination) with city/country info.
    """
    try:
        ws = sheet.worksheet(CAPABILITY_TAB)
        rows = ws.get_all_records()
    except Exception as e:
        log(f"⚠️ ROUTE_CAPABILITY_MAP not readable: {e}")
        return {}
    
    cap_map = {}
    for r in rows:
        origin = _clean_iata(r.get("origin_iata"))
        dest = _clean_iata(r.get("destination_iata"))
        
        if not origin or not dest:
            continue
        
        cap_map[(origin, dest)] = {
            "origin_city": str(r.get("origin_city", "")).strip(),
            "origin_country": str(r.get("origin_country", "")).strip(),
            "destination_city": str(r.get("destination_city", "")).strip(),
            "destination_country": str(r.get("destination_country", "")).strip(),
            "connection_type": str(r.get("connection_type", "")).strip(),
            "via_hub": str(r.get("via_hub", "")).strip(),
        }
    
    log(f"✅ ROUTE_CAPABILITY_MAP loaded: {len(cap_map)} routes")
    return cap_map


def load_benchmarks(sheet: gspread.Spreadsheet) -> Dict[str, Dict[str, float]]:
    """
    Load ZONE_THEME_BENCHMARKS for price filtering.
    Returns dict keyed by zone (e.g., "LGW_winter_sun") with price gates.
    """
    try:
        ws = sheet.worksheet(BENCHMARKS_TAB)
        rows = ws.get_all_records()
    except Exception as e:
        log(f"⚠️ ZONE_THEME_BENCHMARKS not readable: {e}")
        return {}
    
    benchmarks = {}
    for r in rows:
        zone = str(r.get("zone", "")).strip()
        if not zone:
            continue
        
        benchmarks[zone] = {
            "deal_price": _parse_float(r.get("deal_price")) or 999999.0,
            "max_price": _parse_float(r.get("max_price")) or 999999.0,
            "error_price": _parse_float(r.get("error_price")) or 999999.0,
        }
    
    log(f"✅ ZONE_THEME_BENCHMARKS loaded: {len(benchmarks)} zones")
    return benchmarks


# ==================== DUFFEL API ====================

def search_duffel(
    origin: str,
    destination: str,
    departure_date: str,
    return_date: str,
    cabin_class: str = "economy",
    max_connections: int = 1
) -> List[Dict[str, Any]]:
    """
    Search Duffel API for flight offers.
    
    Returns list of offers with structure:
    {
        "offer_id": str,
        "total_price": float,
        "currency": str,
        "owner_airline": str,
        "departure_date": str,
        "return_date": str,
        "outbound_duration": int (minutes),
        "inbound_duration": int (minutes),
        "stops": int,
        "segments": List[Dict]  # Detailed flight info
    }
    """
    
    if not DUFFEL_API_KEY:
        log("⚠️ DUFFEL_API_KEY not configured")
        return []
    
    url = f"{DUFFEL_API_BASE}/air/offer_requests"
    
    headers = {
        "Duffel-Version": DUFFEL_VERSION,
        "Authorization": f"Bearer {DUFFEL_API_KEY}",
        "Content-Type": "application/json",
    }
    
    payload = {
        "slices": [
            {
                "origin": origin,
                "destination": destination,
                "departure_date": departure_date,
            },
            {
                "origin": destination,
                "destination": origin,
                "departure_date": return_date,
            }
        ],
        "passengers": [{"type": "adult"}],
        "cabin_class": cabin_class,
        "max_connections": max_connections,
    }
    
    try:
        log(f"🔍 Searching Duffel: {origin}→{destination} | {departure_date}→{return_date} | {cabin_class}")
        
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        offers = data.get("offers", [])
        
        log(f"   Found {len(offers)} offers")
        
        # Parse offers into simplified structure
        parsed_offers = []
        for offer in offers:
            # Extract price
            total_amount = float(offer.get("total_amount", 0))
            currency = offer.get("total_currency", "GBP")
            
            # Extract owner airline
            owner = offer.get("owner", {})
            owner_airline = owner.get("iata_code", owner.get("name", ""))
            
            # Extract slice details
            slices = offer.get("slices", [])
            if len(slices) < 2:
                continue  # Need outbound + inbound
            
            outbound_slice = slices[0]
            inbound_slice = slices[1]
            
            # Count stops
            outbound_segments = outbound_slice.get("segments", [])
            inbound_segments = inbound_slice.get("segments", [])
            stops = (len(outbound_segments) - 1) + (len(inbound_segments) - 1)
            
            # Extract durations (ISO 8601 format: PT2H30M)
            outbound_duration = _parse_iso_duration(outbound_slice.get("duration", ""))
            inbound_duration = _parse_iso_duration(inbound_slice.get("duration", ""))
            
            parsed_offer = {
                "offer_id": offer.get("id", ""),
                "total_price": total_amount,
                "currency": currency,
                "owner_airline": owner_airline,
                "departure_date": departure_date,
                "return_date": return_date,
                "outbound_duration": outbound_duration,
                "inbound_duration": inbound_duration,
                "stops": stops,
                "segments": outbound_segments + inbound_segments,
            }
            
            parsed_offers.append(parsed_offer)
        
        return parsed_offers
    
    except requests.exceptions.Timeout:
        log(f"   ⚠️ Timeout searching {origin}→{destination}")
        return []
    
    except requests.exceptions.HTTPError as e:
        log(f"   ⚠️ HTTP error: {e}")
        return []
    
    except Exception as e:
        log(f"   ⚠️ Error searching Duffel: {e}")
        return []


def _parse_iso_duration(iso_duration: str) -> int:
    """
    Parse ISO 8601 duration to minutes.
    Example: PT2H30M → 150 minutes
    """
    if not iso_duration:
        return 0
    
    try:
        import re
        hours = re.search(r'(\d+)H', iso_duration)
        minutes = re.search(r'(\d+)M', iso_duration)
        
        total = 0
        if hours:
            total += int(hours.group(1)) * 60
        if minutes:
            total += int(minutes.group(1))
        
        return total
    except:
        return 0


# ==================== PRICE FILTERING ====================

def passes_price_gate(
    price: float,
    origin: str,
    theme: str,
    benchmarks: Dict[str, Dict[str, float]]
) -> bool:
    """
    Check if price passes benchmark gates.
    
    Returns True if price <= max_price for this zone.
    """
    zone = f"{origin}_{theme}"
    
    if zone not in benchmarks:
        # No benchmark defined - use lenient default
        log(f"   ⚠️ No benchmark for zone {zone}, allowing deal")
        return True
    
    gates = benchmarks[zone]
    max_price = gates["max_price"]
    
    if price <= max_price:
        if price <= gates["deal_price"]:
            log(f"   ✅ GREAT DEAL: £{price:.2f} ≤ £{gates['deal_price']:.2f} (deal threshold)")
        else:
            log(f"   ✅ Good deal: £{price:.2f} ≤ £{max_price:.2f} (max threshold)")
        return True
    else:
        log(f"   ❌ Too expensive: £{price:.2f} > £{max_price:.2f} (rejected)")
        return False


# ==================== DEAL CONSTRUCTION ====================

def build_deal_row(
    route: Dict[str, Any],
    offer: Dict[str, Any],
    enrichment: Dict[str, str],
) -> Dict[str, Any]:
    """
    Build complete deal row for RAW_DEALS insertion.
    
    Combines:
    - Route config (theme, audience, etc.)
    - Duffel offer (price, dates, airline)
    - Capability map enrichment (city/country)
    """
    
    origin = route["origin_iata"]
    destination = route["destination_iata"]
    
    # Generate unique deal_id
    deal_id = _generate_deal_id(
        origin,
        destination,
        offer["departure_date"],
        offer["total_price"]
    )
    
    # Build complete row
    deal_row = {
        # Identity
        "deal_id": deal_id,
        "status": "NEW",
        "ingested_at_utc": dt.datetime.utcnow().isoformat() + "Z",
        
        # Route
        "origin_iata": origin,
        "origin_city": enrichment.get("origin_city", ""),
        "origin_country": enrichment.get("origin_country", ""),
        "destination_iata": destination,
        "destination_city": enrichment.get("destination_city", ""),
        "destination_country": enrichment.get("destination_country", ""),
        
        # Theme
        "theme": route["theme"],
        
        # Dates
        "outbound_date": offer["departure_date"],
        "inbound_date": offer["return_date"],
        "trip_length_days": route["trip_length_days"],
        
        # Flight details
        "airline": offer["owner_airline"],
        "cabin_class": route["cabin_class"],
        "stops": offer["stops"],
        "connection_type": enrichment.get("connection_type", "direct" if offer["stops"] == 0 else "via_hub"),
        "via_hub": enrichment.get("via_hub", ""),
        
        # Duration
        "outbound_duration_minutes": offer["outbound_duration"],
        "inbound_duration_minutes": offer["inbound_duration"],
        "total_duration_hours": round((offer["outbound_duration"] + offer["inbound_duration"]) / 60, 1),
        
        # Price
        "total_price": offer["total_price"],
        "currency": offer["currency"],
        
        # Duffel
        "duffel_offer_id": offer["offer_id"],
        
        # Empty fields (populated by other workers)
        "phrase_used": "",
        "phrase_bank": "",
        "booking_link": "",
    }
    
    return deal_row


# ==================== MAIN PIPELINE ====================

def main() -> int:
    log("=" * 80)
    log("TRAVELTXTER FEEDER - FUNCTIONAL DEAL DISCOVERY ENGINE")
    log("=" * 80)
    
    # Validate environment
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID")
    if not DUFFEL_API_KEY:
        raise RuntimeError("Missing DUFFEL_API_KEY")
    
    # Connect to Google Sheets
    log("📊 Connecting to Google Sheets...")
    gc = gs_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    
    # Load data
    log("📚 Loading configuration data...")
    config_routes = load_config_routes(sh)
    capability_map = load_route_capability_map(sh)
    benchmarks = load_benchmarks(sh)
    
    if not config_routes:
        log("⚠️ No active routes in CONFIG (check active_in_feeder column)")
        return 0
    
    # Get RAW_DEALS worksheet
    ws_raw = sh.worksheet(RAW_DEALS_TAB)
    raw_headers = [h.strip() for h in ws_raw.row_values(1)]
    
    # Prioritize routes by search_weight * priority
    config_routes.sort(key=lambda r: r["search_weight"] * r["priority"], reverse=True)
    
    # Search routes
    log(f"🔍 Starting search: {len(config_routes)} routes, max {DUFFEL_MAX_SEARCHES_PER_RUN} searches")
    
    searches_done = 0
    deals_found = 0
    deals_inserted = 0
    all_winners = []
    
    for route in config_routes:
        if searches_done >= DUFFEL_MAX_SEARCHES_PER_RUN:
            log(f"⏸️ Reached search limit ({DUFFEL_MAX_SEARCHES_PER_RUN}), stopping")
            break
        
        origin = route["origin_iata"]
        destination = route["destination_iata"]
        theme = route["theme"]
        
        # Validate route exists in capability map
        if (origin, destination) not in capability_map:
            log(f"⚠️ {origin}→{destination} not in ROUTE_CAPABILITY_MAP, skipping")
            continue
        
        # Calculate search dates
        today = dt.date.today()
        departure_date = today + dt.timedelta(days=route["days_ahead_min"])
        return_date = departure_date + dt.timedelta(days=route["trip_length_days"])
        
        # Search Duffel
        offers = search_duffel(
            origin=origin,
            destination=destination,
            departure_date=departure_date.isoformat(),
            return_date=return_date.isoformat(),
            cabin_class=route["cabin_class"],
            max_connections=route["max_connections"]
        )
        
        searches_done += 1
        
        if not offers:
            log(f"   No offers found")
            time.sleep(FEEDER_SLEEP_SECONDS)
            continue
        
        # Filter by price gates
        route_winners = []
        for offer in offers:
            if passes_price_gate(offer["total_price"], origin, theme, benchmarks):
                route_winners.append(offer)
        
        deals_found += len(route_winners)
        
        # Limit deals per route
        route_winners = route_winners[:DUFFEL_MAX_INSERTS_PER_ROUTE]
        
        # Build deal rows
        enrichment = capability_map[(origin, destination)]
        
        for offer in route_winners:
            if deals_inserted >= DUFFEL_MAX_INSERTS:
                log(f"⏸️ Reached insert limit ({DUFFEL_MAX_INSERTS}), stopping")
                break
            
            deal_row = build_deal_row(route, offer, enrichment)
            all_winners.append(deal_row)
            deals_inserted += 1
        
        # Rate limiting
        time.sleep(FEEDER_SLEEP_SECONDS)
        
        if deals_inserted >= DUFFEL_MAX_INSERTS:
            break
    
    # Insert deals to RAW_DEALS
    if all_winners:
        log(f"💾 Inserting {len(all_winners)} deals to RAW_DEALS...")
        
        rows_to_insert = []
        for deal in all_winners:
            row = [deal.get(h, "") for h in raw_headers]
            rows_to_insert.append(row)
        
        ws_raw.append_rows(rows_to_insert, value_input_option="RAW")
        log(f"✅ Inserted {len(rows_to_insert)} deals")
    else:
        log("⚠️ No deals passed price gates")
    
    # Summary
    log("=" * 80)
    log("FEEDER RUN COMPLETE")
    log(f"  Routes searched: {searches_done}")
    log(f"  Deals found: {deals_found}")
    log(f"  Deals inserted: {deals_inserted}")
    log("=" * 80)
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
