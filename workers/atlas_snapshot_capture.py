#!/usr/bin/env python3
"""
workers/atlas_snapshot_capture.py
ATLAS SNAPSHOT CAPTURE — v2.0 (Supabase Migration)

Built against Atlas Snapshot Oilpan v1.0.

WHAT CHANGED FROM v1.5:
- **MIGRATED TO SUPABASE** — writes to PostgreSQL instead of Google Sheets
- Replaces gspread with supabase-py client
- Same 43-column schema, same logic, different storage layer
- Requires SUPABASE_URL and SUPABASE_KEY env vars
- Batch inserts use database transactions (faster + atomic)
- Deduplication via snapshot_key unique constraint (database-level)

SCHEMA (43 columns - v1.5):
- Core: snapshot_id, snapshot_date, capture_time_utc, origin_iata, destination_iata
- Flight: outbound_date, return_date, dtd, day_of_week_departure, day_of_week_snapshot
- Pricing: price_gbp, currency, carrier_count, lcc_present, direct, stops, cabin_class, seats_remaining
- Backfill labels: price_t7, price_t14, rose_10pct, fell_10pct
- Data quality: snapshot_key, notes, origin_type, shi_variance_flag
- Crisis flagging (9 cols): crisis_flag, crisis_id, crisis_severity, crisis_route_affected, 
  crisis_global_impact, crisis_contamination_pct_t14, crisis_contamination_pct_t7, 
  crisis_label_contaminated, training_action
- Market signals: jet_fuel_usd_gal
- v1.5 expansion (5 cols): carrier_primary_iata, route_distance_km, route_type, shi_score, model_version

OILPAN CONTRACT:
- Stateless — no memory between runs
- Writes only to snapshots table
- Never touches raw_deals (future)
"""

from __future__ import annotations

import os
import json
import time
import math
import random
import datetime as dt
import statistics
from uuid import uuid4
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests
from supabase import create_client, Client


# ─────────────────────────────────────────────
# ENV HELPERS
# ─────────────────────────────────────────────

def env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, default)).strip())
    except Exception:
        return default


def env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


# ─────────────────────────────────────────────
# SUPABASE CONNECTION
# ─────────────────────────────────────────────

def init_supabase() -> Client:
    """Initialize Supabase client from env vars."""
    url = env_str("SUPABASE_URL")
    key = env_str("SUPABASE_KEY")
    
    if not url or not key:
        raise ValueError(
            "Missing Supabase credentials. Set SUPABASE_URL and SUPABASE_KEY env vars.\n"
            "Get these from: https://supabase.com/dashboard/project/<project_id>/settings/api"
        )
    
    return create_client(url, key)


# ─────────────────────────────────────────────
# AIRPORT COORDINATES DATABASE
# ─────────────────────────────────────────────

AIRPORT_COORDS = {
    # UK Origins
    'MAN': (53.3537, -2.2750),
    'LGW': (51.1537, -0.1821),
    'LHR': (51.4700, -0.4543),
    'EDI': (55.9500, -3.3725),
    'BRS': (51.3827, -2.7190),
    'LPL': (53.3337, -2.8497),
    'BHX': (52.4539, -1.7480),
    'NCL': (55.0375, -1.6917),
    'GLA': (55.8719, -4.4333),
    
    # European Destinations
    'AMS': (52.3105, 4.7683),
    'CDG': (49.0097, 2.5479),
    'ORY': (48.7233, 2.3794),
    'FCO': (41.8003, 12.2389),
    'MAD': (40.4983, -3.5676),
    'BCN': (41.2974, 2.0833),
    'DUB': (53.4213, -6.2701),
    'BRU': (50.9010, 4.4856),
    'CPH': (55.6180, 12.6508),
    'ARN': (59.6519, 17.9186),
    'OSL': (60.1939, 11.1004),
    'VIE': (48.1103, 16.5697),
    'ZRH': (47.4647, 8.5492),
    'GVA': (46.2381, 6.1090),
    'PRG': (50.1008, 14.2632),
    'WAW': (52.1672, 20.9679),
    'BUD': (47.4298, 19.2611),
    'ATH': (37.9364, 23.9445),
    'LIS': (38.7742, -9.1342),
    'AGP': (36.6749, -4.4991),
    'ALC': (38.2822, -0.5582),
    'PMI': (39.5517, 2.7388),
    'IBZ': (38.8729, 1.3731),
    'FAO': (37.0144, -7.9659),
    'OPO': (41.2481, -8.6814),
    'NCE': (43.6584, 7.2159),
    'MXP': (45.6306, 8.7281),
    'VCE': (45.5053, 12.3519),
    'NAP': (40.8860, 14.2908),
    'CAI': (30.1219, 31.4056),
    'DXB': (25.2532, 55.3657),
    'IST': (41.2753, 28.7519),
    'SAW': (40.8986, 29.3092),
    'TLV': (32.0114, 34.8867),
    'BEY': (33.8208, 35.4883),
    'AMM': (31.7226, 35.9932),
    'ATL': (33.6407, -84.4277),
    'JFK': (40.6413, -73.7781),
    'LAX': (33.9416, -118.4085),
    'ORD': (41.9742, -87.9073),
    'MIA': (25.7959, -80.2870),
    'YYZ': (43.6777, -79.6248),
    'MEX': (19.4363, -99.0721),
    'GRU': (23.4356, -46.4731),
    'EZE': (34.8222, -58.5358),
    'SCL': (33.3930, -70.7858),
    'SIN': (1.3644, 103.9915),
    'HKG': (22.3080, 113.9185),
    'NRT': (35.7720, 140.3929),
    'ICN': (37.4602, 126.4407),
    'PEK': (40.0799, 116.6031),
    'PVG': (31.1443, 121.8083),
    'SYD': (33.9399, 151.1753),
    'MEL': (37.6690, 144.8410),
    'AKL': (37.0082, 174.7850),
    'JNB': (26.1367, 28.2411),
    'CPT': (33.9690, 18.6029),
    'MUC': (48.3538, 11.7861),
    'FRA': (50.0379, 8.5622),
    'DUS': (51.2895, 6.7668),
    'HAM': (53.6304, 9.9882),
    'SXF': (52.3800, 13.5225),
    'TXL': (52.5597, 13.2877),
    'CGN': (50.8659, 7.1427),
    'STR': (48.6899, 9.2219),
    'HEL': (60.3172, 24.9633),
    'RIX': (56.9236, 23.9711),
    'TLL': (59.4133, 24.8328),
    'VNO': (54.6341, 25.2858),
    'OTP': (44.5711, 26.0850),
    'SOF': (42.6952, 23.4114),
    'SKG': (40.5197, 22.9709),
    'DBV': (42.5614, 18.2682),
    'SPU': (43.5389, 16.2980),
    'ZAG': (45.7429, 16.0688),
    'LJU': (46.2237, 14.4576),
    'BEG': (44.8184, 20.3091),
    'KEF': (63.9850, -22.6056),
    'TRD': (63.4578, 10.9239),
    'BGO': (60.2934, 5.2181),
    'SVG': (58.8767, 5.6378),
    'TBS': (41.6692, 44.9547),
    'EVN': (40.1473, 44.3959),
    'BAK': (40.4675, 50.0467),
    'ALA': (43.3521, 77.0405),
    'TAS': (41.2579, 69.2811),
    'KIV': (46.9277, 28.9310),
    'SJJ': (43.8246, 18.3315),
    'PRN': (42.5728, 21.0358),
    'TGD': (42.3594, 19.2519),
    'VLC': (39.4893, -0.4817),
    'SVQ': (37.4180, -5.8931),
    'BIO': (43.3011, -2.9106),
    'INN': (47.2602, 11.3439),
    'SZG': (47.7933, 13.0043),
    'GRZ': (46.9911, 15.4397),
    'BLL': (55.7403, 9.1518),
    'AAL': (57.0928, 9.8492),
    'GOT': (57.6628, 12.2798),
    'MMX': (55.5361, 13.3761),
    'LYR': (78.2461, 15.4656),
    'RVN': (66.5647, 25.8304),
    'TRF': (69.6833, 18.9189),
    'KRK': (50.0777, 19.7848),
    'GDN': (54.3776, 18.4661),
    'WRO': (51.1027, 16.8858),
    'KTW': (50.4743, 19.0800),
    'BTS': (48.1702, 17.2127),
    'CLJ': (46.7852, 23.6862),
    'IAS': (47.1785, 27.6206),
    'TSR': (45.8099, 21.3379),
    'VAR': (43.2324, 27.8251),
    'BOJ': (42.5695, 27.5152),
}


def haversine_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> int:
    """Calculate great circle distance between two points. Returns km as integer."""
    R = 6371  # Earth radius in km
    
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    
    a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    return round(R * c)


def classify_route_type(distance_km: int) -> str:
    """Classify route based on distance."""
    if distance_km < 500:
        return "domestic"
    elif distance_km < 1500:
        return "european_short"
    elif distance_km < 3000:
        return "european_long"
    else:
        return "intercontinental"


# ─────────────────────────────────────────────
# CRISIS FLAGGING
# ─────────────────────────────────────────────

@dataclass
class CrisisEvent:
    crisis_id: str
    crisis_name: str
    start_date: dt.date
    end_date: Optional[dt.date]
    severity: str
    global_impact: bool
    affected_regions: List[str]
    affected_destinations: List[str]
    label_contamination_window_days: int
    training_action: str


def load_crisis_config() -> List[CrisisEvent]:
    """Load crisis events from atlas_crisis_config.json."""
    config_path = os.path.join(os.path.dirname(__file__), "atlas_crisis_config.json")
    
    if not os.path.exists(config_path):
        print(f"⚠️  Crisis config not found: {config_path}")
        return []
    
    try:
        with open(config_path, "r") as f:
            data = json.load(f)
        
        events = []
        for e in data.get("crisis_events", []):
            events.append(CrisisEvent(
                crisis_id=e["crisis_id"],
                crisis_name=e["crisis_name"],
                start_date=dt.datetime.strptime(e["start_date"], "%Y-%m-%d").date(),
                end_date=dt.datetime.strptime(e["end_date"], "%Y-%m-%d").date() if e.get("end_date") else None,
                severity=e["severity"],
                global_impact=e.get("global_impact", False),
                affected_regions=e.get("affected_regions", []),
                affected_destinations=e.get("affected_destinations", []),
                label_contamination_window_days=e.get("label_contamination_window_days", 14),
                training_action=e.get("training_action", "flag_only")
            ))
        
        print(f"✅ Loaded {len(events)} crisis event(s)")
        return events
    
    except Exception as ex:
        print(f"⚠️  Failed to load crisis config: {ex}")
        return []


def check_crisis_flags(
    snapshot_date: dt.date,
    destination_iata: str,
    crisis_events: List[CrisisEvent]
) -> Dict[str, Any]:
    """
    Returns crisis flags for a snapshot.
    
    Returns dict with keys:
    - crisis_flag (bool)
    - crisis_id (str or None)
    - crisis_severity (str or None)
    - crisis_route_affected (bool)
    - crisis_global_impact (bool)
    - crisis_contamination_pct_t14 (float)
    - crisis_contamination_pct_t7 (float)
    - crisis_label_contaminated (bool)
    - training_action (str or None)
    """
    flags = {
        "crisis_flag": False,
        "crisis_id": None,
        "crisis_severity": None,
        "crisis_route_affected": False,
        "crisis_global_impact": False,
        "crisis_contamination_pct_t14": 0.0,
        "crisis_contamination_pct_t7": 0.0,
        "crisis_label_contaminated": False,
        "training_action": None,
    }
    
    for event in crisis_events:
        # Check if snapshot falls within crisis period
        if snapshot_date < event.start_date:
            continue
        if event.end_date and snapshot_date > event.end_date:
            continue
        
        # Snapshot is during crisis
        flags["crisis_flag"] = True
        flags["crisis_id"] = event.crisis_id
        flags["crisis_severity"] = event.severity
        flags["crisis_global_impact"] = event.global_impact
        
        # Check route impact
        route_affected = (
            event.global_impact or
            destination_iata in event.affected_destinations or
            any(destination_iata.startswith(region) for region in event.affected_regions)
        )
        flags["crisis_route_affected"] = route_affected
        
        # Calculate label contamination
        days_since_start = (snapshot_date - event.start_date).days
        contamination_window = event.label_contamination_window_days
        
        if days_since_start <= contamination_window:
            # t+14 contamination (next 14 days)
            days_remaining_in_window = contamination_window - days_since_start
            pct_t14 = min(100.0, (days_remaining_in_window / 14.0) * 100.0)
            flags["crisis_contamination_pct_t14"] = round(pct_t14, 2)
            
            # t+7 contamination (next 7 days)
            pct_t7 = min(100.0, (days_remaining_in_window / 7.0) * 100.0)
            flags["crisis_contamination_pct_t7"] = round(pct_t7, 2)
            
            flags["crisis_label_contaminated"] = True
        
        # Training action
        flags["training_action"] = event.training_action
        
        # Only process first matching crisis
        break
    
    return flags


# ─────────────────────────────────────────────
# SAMPLING HEALTH INDEX (SHI)
# ─────────────────────────────────────────────

def shi_variance_calculation(
    supabase: Client,
    origin: str,
    dest: str,
    outbound_date: dt.date,
    return_date: dt.date,
    current_price: float
) -> Tuple[str, Optional[float]]:
    """
    Calculate SHI z-score for this route.
    
    Returns:
    - flag: "OK" | "HIGH_VARIANCE" | "INSUFFICIENT_DATA"
    - z_score: float (absolute z-score) or None
    """
    try:
        # Query historical prices for this route
        result = supabase.table('snapshots').select('price_gbp').eq(
            'origin_iata', origin
        ).eq(
            'destination_iata', dest
        ).eq(
            'outbound_date', str(outbound_date)
        ).eq(
            'return_date', str(return_date)
        ).not_.is_('price_gbp', 'null').execute()
        
        prices = [float(row['price_gbp']) for row in result.data if row.get('price_gbp')]
        
        if len(prices) < 5:
            return ("INSUFFICIENT_DATA", None)
        
        mean_price = statistics.mean(prices)
        stdev_price = statistics.stdev(prices)
        
        if stdev_price == 0:
            return ("OK", 0.0)
        
        z_score = abs((current_price - mean_price) / stdev_price)
        
        if z_score > 2.5:
            return ("HIGH_VARIANCE", z_score)
        else:
            return ("OK", z_score)
    
    except Exception as ex:
        print(f"⚠️  SHI calculation failed: {ex}")
        return ("INSUFFICIENT_DATA", None)


# ─────────────────────────────────────────────
# JET FUEL PRICE SIGNAL
# ─────────────────────────────────────────────

def fetch_jet_fuel_price() -> Optional[float]:
    """Fetch current jet fuel spot price from EIA API."""
    api_key = env_str("EIA_API_KEY")
    
    if not api_key:
        print("⚠️  EIA_API_KEY not set, skipping jet fuel price")
        return None
    
    try:
        url = "https://api.eia.gov/v2/petroleum/pri/spt/data/"
        params = {
            "api_key": api_key,
            "frequency": "weekly",
            "data[0]": "value",
            "facets[product][]": "EPD2F",  # Kerosene-Type Jet Fuel
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "offset": 0,
            "length": 1
        }
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        value = data["response"]["data"][0]["value"]
        
        print(f"✅ Jet fuel price: ${value}/gal")
        return float(value)
    
    except Exception as ex:
        print(f"⚠️  Failed to fetch jet fuel price: {ex}")
        return None


# ─────────────────────────────────────────────
# DUFFEL API
# ─────────────────────────────────────────────

def search_duffel(
    origin: str,
    dest: str,
    outbound: dt.date,
    return_date: dt.date,
    cabin_class: str,
    duffel_token: str
) -> Optional[Dict[str, Any]]:
    """Search Duffel API for cheapest offer."""
    url = "https://api.duffel.com/air/offer_requests"
    headers = {
        "Duffel-Version": "v1",
        "Authorization": f"Bearer {duffel_token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "data": {
            "cabin_class": cabin_class,
            "passengers": [{"type": "adult"}],
            "slices": [
                {
                    "origin": origin,
                    "destination": dest,
                    "departure_date": str(outbound)
                },
                {
                    "origin": dest,
                    "destination": origin,
                    "departure_date": str(return_date)
                }
            ]
        }
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        offers = data.get("data", {}).get("offers", [])
        
        if not offers:
            return None
        
        # Get cheapest offer
        cheapest = min(offers, key=lambda o: float(o["total_amount"]))
        
        return {
            "price_gbp": float(cheapest["total_amount"]),
            "currency": cheapest["total_currency"],
            "carrier_count": len(set(
                seg["marketing_carrier"]["iata_code"]
                for slice_ in cheapest["slices"]
                for seg in slice_["segments"]
            )),
            "lcc_present": any(
                seg["marketing_carrier"].get("name", "").lower() in [
                    "ryanair", "easyjet", "wizz air", "norwegian"
                ]
                for slice_ in cheapest["slices"]
                for seg in slice_["segments"]
            ),
            "direct": all(
                len(slice_["segments"]) == 1
                for slice_ in cheapest["slices"]
            ),
            "stops": max(
                len(slice_["segments"]) - 1
                for slice_ in cheapest["slices"]
            ),
            "cabin_class": cabin_class,
            "carrier_primary_iata": cheapest["slices"][0]["segments"][0]["marketing_carrier"]["iata_code"]
        }
    
    except Exception as ex:
        print(f"⚠️  Duffel search failed ({origin}→{dest}): {ex}")
        return None


# ─────────────────────────────────────────────
# MAIN CAPTURE LOGIC
# ─────────────────────────────────────────────

def main():
    print("=" * 70)
    print("ATLAS SNAPSHOT CAPTURE v2.0 (Supabase)")
    print("=" * 70)
    
    # Load config
    config_path = os.path.join(os.path.dirname(__file__), "atlas_snapshot_config.json")
    with open(config_path, "r") as f:
        config = json.load(f)
    
    # Initialize Supabase
    supabase = init_supabase()
    print(f"✅ Connected to Supabase: {env_str('SUPABASE_URL')}")
    
    # Load crisis events
    crisis_events = load_crisis_config()
    
    # Fetch jet fuel price (once per run)
    jet_fuel_price = fetch_jet_fuel_price()
    
    # Capture parameters
    snapshot_date = dt.date.today()
    capture_time = dt.datetime.utcnow().strftime("%H:%M")
    duffel_token = env_str("DUFFEL_ACCESS_TOKEN")
    max_searches = env_int("ATLAS_MAX_SEARCHES", 160)
    max_per_origin = env_int("ATLAS_MAX_SEARCHES_PER_ORIGIN", max_searches // 9)
    
    # Origins and destinations (simplified - you can load from Supabase if needed)
    origins = ["MAN", "LGW", "LHR", "EDI", "BRS", "LPL", "BHX", "NCL", "GLA"]
    
    # Build search list
    routes = []
    for origin in origins:
        for dest in ["AMS", "CDG", "BCN", "DUB", "FCO", "MAD"]:  # Example subset
            outbound = snapshot_date + dt.timedelta(days=14)
            return_date = outbound + dt.timedelta(days=7)
            routes.append((origin, dest, outbound, return_date, "economy"))
    
    random.shuffle(routes)
    
    # Execute searches
    snapshots = []
    searches_per_origin = {o: 0 for o in origins}
    
    for origin, dest, outbound, return_date, cabin in routes:
        if searches_per_origin[origin] >= max_per_origin:
            continue
        if len(snapshots) >= max_searches:
            break
        
        searches_per_origin[origin] += 1
        
        # Search Duffel
        result = search_duffel(origin, dest, outbound, return_date, cabin, duffel_token)
        
        # Calculate distance and route type
        distance_km = None
        route_type = None
        if origin in AIRPORT_COORDS and dest in AIRPORT_COORDS:
            lat1, lon1 = AIRPORT_COORDS[origin]
            lat2, lon2 = AIRPORT_COORDS[dest]
            distance_km = haversine_distance_km(lat1, lon1, lat2, lon2)
            route_type = classify_route_type(distance_km)
        
        # Build snapshot row
        snapshot_id = str(uuid4())
        snapshot_key = f"{origin}_{dest}_{outbound}_{return_date}_{snapshot_date}_{capture_time.replace(':', '')}"
        
        # Crisis flags
        crisis_flags = check_crisis_flags(snapshot_date, dest, crisis_events)
        
        # SHI calculation (if we have a price)
        shi_flag = "INSUFFICIENT_DATA"
        shi_score = None
        if result and result.get("price_gbp"):
            shi_flag, shi_score = shi_variance_calculation(
                supabase, origin, dest, outbound, return_date, result["price_gbp"]
            )
        
        row = {
            "snapshot_id": snapshot_id,
            "snapshot_date": str(snapshot_date),
            "capture_time_utc": capture_time,
            "origin_iata": origin,
            "destination_iata": dest,
            "outbound_date": str(outbound),
            "return_date": str(return_date),
            "dtd": (outbound - snapshot_date).days,
            "day_of_week_departure": outbound.strftime("%A"),
            "day_of_week_snapshot": snapshot_date.strftime("%A"),
            "is_school_holiday_window": False,  # Simplified
            "is_bank_holiday_adjacent": False,  # Simplified
            "price_gbp": result["price_gbp"] if result else None,
            "currency": result["currency"] if result else "GBP",
            "carrier_count": result["carrier_count"] if result else None,
            "lcc_present": result["lcc_present"] if result else None,
            "direct": result["direct"] if result else None,
            "stops": result["stops"] if result else None,
            "cabin_class": cabin,
            "seats_remaining": None,
            "price_t7": None,
            "price_t14": None,
            "rose_10pct": None,
            "fell_10pct": None,
            "snapshot_key": snapshot_key,
            "notes": None,
            "origin_type": "Tier1",  # Simplified
            "shi_variance_flag": shi_flag,
            "crisis_flag": crisis_flags["crisis_flag"],
            "crisis_id": crisis_flags["crisis_id"],
            "crisis_severity": crisis_flags["crisis_severity"],
            "crisis_route_affected": crisis_flags["crisis_route_affected"],
            "crisis_global_impact": crisis_flags["crisis_global_impact"],
            "crisis_contamination_pct_t14": crisis_flags["crisis_contamination_pct_t14"],
            "crisis_contamination_pct_t7": crisis_flags["crisis_contamination_pct_t7"],
            "crisis_label_contaminated": crisis_flags["crisis_label_contaminated"],
            "training_action": crisis_flags["training_action"],
            "jet_fuel_usd_gal": jet_fuel_price,
            "carrier_primary_iata": result["carrier_primary_iata"] if result else None,
            "route_distance_km": distance_km,
            "route_type": route_type,
            "shi_score": shi_score,
            "model_version": "v1_0_0"
        }
        
        snapshots.append(row)
        
        time.sleep(0.5)  # Rate limiting
    
    # Write to Supabase
    if snapshots:
        print(f"\n📥 Writing {len(snapshots)} snapshots to Supabase...")
        try:
            supabase.table('snapshots').insert(snapshots).execute()
            print(f"✅ Successfully wrote {len(snapshots)} rows")
        except Exception as ex:
            print(f"❌ Insert failed: {ex}")
            raise
    
    print("\n" + "=" * 70)
    print(f"✅ Capture complete: {len(snapshots)} snapshots")
    print("=" * 70)


if __name__ == "__main__":
    main()
