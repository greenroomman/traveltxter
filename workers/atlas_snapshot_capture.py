#!/usr/bin/env python3
"""
workers/atlas_snapshot_capture.py
ATLAS SNAPSHOT CAPTURE - v2.1 (Supabase Migration)

Migrated from Google Sheets to PostgreSQL/Supabase.
Same 43-column schema, same logic, different storage layer.
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


# -------------------------------------------------
# ENV HELPERS
# -------------------------------------------------

def env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, default)).strip())
    except Exception:
        return default


def env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


def env_int_list(name: str, default: List[int]) -> List[int]:
    raw = os.getenv(name)
    if not raw:
        return default

    try:
        values = []
        for part in raw.split(","):
            part = part.strip()
            if part:
                values.append(int(part))
        return values or default
    except Exception:
        return default


# -------------------------------------------------
# SUPABASE CONNECTION
# -------------------------------------------------

def init_supabase() -> Client:
    """Initialize Supabase client from env vars."""
    url = env_str("SUPABASE_URL")
    key = env_str("SUPABASE_SERVICE_KEY") or env_str("SUPABASE_KEY")

    if not url or not key:
        raise ValueError(
            "Missing Supabase credentials. Set SUPABASE_URL and SUPABASE_SERVICE_KEY "
            "(or SUPABASE_KEY) env vars."
        )

    return create_client(url, key)


# -------------------------------------------------
# AIRPORT COORDINATES DATABASE
# -------------------------------------------------

AIRPORT_COORDS = {
    "MAN": (53.3537, -2.2750),
    "LGW": (51.1537, -0.1821),
    "LHR": (51.4700, -0.4543),
    "EDI": (55.9500, -3.3725),
    "BRS": (51.3827, -2.7190),
    "LPL": (53.3337, -2.8497),
    "BHX": (52.4539, -1.7480),
    "NCL": (55.0375, -1.6917),
    "GLA": (55.8719, -4.4333),
    "AMS": (52.3105, 4.7683),
    "CDG": (49.0097, 2.5479),
    "FCO": (41.8003, 12.2389),
    "MAD": (40.4983, -3.5676),
    "BCN": (41.2974, 2.0833),
    "DUB": (53.4213, -6.2701),
    "BRU": (50.9010, 4.4856),
    "CPH": (55.6180, 12.6508),
    "ARN": (59.6519, 17.9186),
    "OSL": (60.1939, 11.1004),
    "VIE": (48.1103, 16.5697),
    "ZRH": (47.4647, 8.5492),
    "PRG": (50.1008, 14.2632),
    "ATH": (37.9364, 23.9445),
    "LIS": (38.7742, -9.1342),
    "AGP": (36.6749, -4.4991),
    "PMI": (39.5517, 2.7388),
    "FAO": (37.0144, -7.9659),
    "NCE": (43.6584, 7.2159),
    "VCE": (45.5053, 12.3519),
    "MXP": (45.6306, 8.7281),
}


def haversine_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> int:
    """Calculate great circle distance. Returns km as integer."""
    R = 6371
    lat1_rad, lat2_rad = math.radians(lat1), math.radians(lat2)
    dlat, dlon = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
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


# -------------------------------------------------
# CRISIS FLAGGING
# -------------------------------------------------

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
        print(f"Warning: crisis config not found: {config_path} - continuing without crisis flags")
        return []

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        events = []
        for e in data.get("crisis_events", []):
            events.append(
                CrisisEvent(
                    crisis_id=e["crisis_id"],
                    crisis_name=e["crisis_name"],
                    start_date=dt.datetime.strptime(e["start_date"], "%Y-%m-%d").date(),
                    end_date=dt.datetime.strptime(e["end_date"], "%Y-%m-%d").date() if e.get("end_date") else None,
                    severity=e["severity"],
                    global_impact=e.get("global_impact", False),
                    affected_regions=e.get("affected_regions", []),
                    affected_destinations=e.get("affected_destinations", []),
                    label_contamination_window_days=e.get("label_contamination_window_days", 14),
                    training_action=e.get("training_action", "flag_only"),
                )
            )

        print(f"Loaded {len(events)} crisis event(s)")
        return events

    except Exception as ex:
        print(f"Warning: failed to load crisis config: {ex}")
        return []


def check_crisis_flags(snapshot_date: dt.date, destination_iata: str, crisis_events: List[CrisisEvent]) -> Dict[str, Any]:
    """Returns crisis flags for a snapshot."""
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
        if snapshot_date < event.start_date:
            continue
        if event.end_date and snapshot_date > event.end_date:
            continue

        flags["crisis_flag"] = True
        flags["crisis_id"] = event.crisis_id
        flags["crisis_severity"] = event.severity
        flags["crisis_global_impact"] = event.global_impact

        route_affected = event.global_impact or destination_iata in event.affected_destinations
        flags["crisis_route_affected"] = route_affected

        days_since_start = (snapshot_date - event.start_date).days
        contamination_window = event.label_contamination_window_days

        if days_since_start <= contamination_window:
            days_remaining = contamination_window - days_since_start
            flags["crisis_contamination_pct_t14"] = round(min(100.0, (days_remaining / 14.0) * 100.0), 2)
            flags["crisis_contamination_pct_t7"] = round(min(100.0, (days_remaining / 7.0) * 100.0), 2)
            flags["crisis_label_contaminated"] = True

        flags["training_action"] = event.training_action
        break

    return flags


# -------------------------------------------------
# SHI CALCULATION
# -------------------------------------------------

def shi_variance_calculation(
    supabase: Client,
    origin: str,
    dest: str,
    outbound_date: dt.date,
    return_date: dt.date,
    current_price: float,
) -> Tuple[str, Optional[float]]:
    """Calculate SHI z-score. Returns (flag, z_score)."""
    try:
        result = (
            supabase.table("snapshots")
            .select("price_gbp")
            .eq("origin_iata", origin)
            .eq("destination_iata", dest)
            .eq("outbound_date", str(outbound_date))
            .eq("return_date", str(return_date))
            .not_.is_("price_gbp", "null")
            .execute()
        )

        prices = [float(row["price_gbp"]) for row in result.data if row.get("price_gbp")]

        if len(prices) < 5:
            return ("INSUFFICIENT_DATA", None)

        mean_price = statistics.mean(prices)
        stdev_price = statistics.stdev(prices)

        if stdev_price == 0:
            return ("OK", 0.0)

        z_score = abs((current_price - mean_price) / stdev_price)
        return ("HIGH_VARIANCE" if z_score > 2.5 else "OK", z_score)

    except Exception:
        return ("INSUFFICIENT_DATA", None)


# -------------------------------------------------
# JET FUEL PRICE SIGNAL
# -------------------------------------------------

def fetch_jet_fuel_price() -> Optional[float]:
    """Fetch jet fuel spot price from EIA API."""
    api_key = env_str("EIA_API_KEY")
    if not api_key:
        print("Warning: EIA_API_KEY not set, skipping jet fuel price")
        return None

    try:
        url = "https://api.eia.gov/v2/petroleum/pri/spt/data/"
        params = {
            "api_key": api_key,
            "frequency": "weekly",
            "data[0]": "value",
            "facets[product][]": "EPD2F",
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "offset": 0,
            "length": 1,
        }
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        value = response.json()["response"]["data"][0]["value"]
        print(f"Jet fuel price: ${value}/gal")
        return float(value)
    except Exception as ex:
        print(f"Warning: failed to fetch jet fuel price: {ex}")
        return None


# -------------------------------------------------
# DUFFEL API
# -------------------------------------------------

def search_duffel(
    origin: str,
    dest: str,
    outbound: dt.date,
    return_date: dt.date,
    cabin_class: str,
    duffel_token: str,
) -> Optional[Dict[str, Any]]:
    """Search Duffel API for cheapest offer."""
    url = "https://api.duffel.com/air/offer_requests"
    headers = {
        "Duffel-Version": "v2",
        "Authorization": f"Bearer {duffel_token}",
        "Content-Type": "application/json",
    }

    payload = {
        "data": {
            "cabin_class": cabin_class,
            "passengers": [{"type": "adult"}],
            "slices": [
                {"origin": origin, "destination": dest, "departure_date": str(outbound)},
                {"origin": dest, "destination": origin, "departure_date": str(return_date)},
            ],
        }
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()

        offers = data.get("data", {}).get("offers", [])
        if not offers:
            return None

        cheapest = min(offers, key=lambda o: float(o["total_amount"]))

        return {
            "price_gbp": float(cheapest["total_amount"]),
            "currency": cheapest["total_currency"],
            "carrier_count": len(
                set(
                    seg["marketing_carrier"]["iata_code"]
                    for slice_ in cheapest["slices"]
                    for seg in slice_["segments"]
                )
            ),
            "lcc_present": any(
                seg["marketing_carrier"].get("name", "").lower()
                in ["ryanair", "easyjet", "wizz air", "norwegian"]
                for slice_ in cheapest["slices"]
                for seg in slice_["segments"]
            ),
            "direct": all(len(slice_["segments"]) == 1 for slice_ in cheapest["slices"]),
            "stops": max(len(slice_["segments"]) - 1 for slice_ in cheapest["slices"]),
            "cabin_class": cabin_class,
            "carrier_primary_iata": cheapest["slices"][0]["segments"][0]["marketing_carrier"]["iata_code"],
        }

    except Exception as ex:
        print(f"Warning: Duffel search failed ({origin}->{dest} {outbound}): {ex}")
        return None


# -------------------------------------------------
# MAIN
# -------------------------------------------------

def main():
    print("=" * 70)
    print("ATLAS SNAPSHOT CAPTURE v2.1 (Supabase)")
    print("=" * 70)

    supabase = init_supabase()
    print("Connected to Supabase")

    crisis_events = load_crisis_config()
    jet_fuel_price = fetch_jet_fuel_price()

    snapshot_date = dt.date.today()
    capture_time = dt.datetime.utcnow().strftime("%H:%M")
    duffel_token = env_str("DUFFEL_ACCESS_TOKEN")
    max_searches = env_int("ATLAS_MAX_SEARCHES", 600)
    dtd_targets = env_int_list("ATLAS_DTD_TARGETS", [14, 21, 30, 45, 60, 84])

    if not duffel_token:
        raise ValueError("Missing DUFFEL_ACCESS_TOKEN")

    origins = ["MAN", "LGW", "LHR", "EDI", "BRS", "LPL", "BHX", "NCL", "GLA"]
    destinations = [
        "AMS", "CDG", "BCN", "DUB", "FCO", "MAD", "ATH", "LIS", "AGP", "PMI",
        "FAO", "NCE", "VCE", "MXP", "PRG", "CPH", "ARN", "OSL", "VIE", "ZRH",
    ]

    print(f"Snapshot date: {snapshot_date}")
    print(f"Max searches: {max_searches}")
    print(f"DTD targets: {dtd_targets}")

    # Build routes across multiple DTD targets
    routes = []
    for origin in origins:
        for dest in destinations:
            for dtd in dtd_targets:
                outbound = snapshot_date + dt.timedelta(days=dtd)
                return_date = outbound + dt.timedelta(days=7)
                routes.append((origin, dest, outbound, return_date, "economy"))

    random.shuffle(routes)

    # Execute searches with balanced origin coverage
    snapshots = []
    searches_per_origin = {o: 0 for o in origins}
    max_per_origin = max_searches // len(origins)

    for origin, dest, outbound, return_date, cabin in routes:
        if len(snapshots) >= max_searches:
            break

        if searches_per_origin[origin] >= max_per_origin:
            continue

        searches_per_origin[origin] += 1

        result = search_duffel(origin, dest, outbound, return_date, cabin, duffel_token)

        distance_km = None
        route_type = None
        if origin in AIRPORT_COORDS and dest in AIRPORT_COORDS:
            lat1, lon1 = AIRPORT_COORDS[origin]
            lat2, lon2 = AIRPORT_COORDS[dest]
            distance_km = haversine_distance_km(lat1, lon1, lat2, lon2)
            route_type = classify_route_type(distance_km)

        snapshot_id = str(uuid4())
        snapshot_key = (
            f"{origin}_{dest}_{outbound}_{return_date}_{snapshot_date}_{capture_time.replace(':', '')}"
        )
        crisis_flags = check_crisis_flags(snapshot_date, dest, crisis_events)

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
            "is_school_holiday_window": False,
            "is_bank_holiday_adjacent": False,
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
            "origin_type": "Tier1",
            "shi_variance_flag": shi_flag,
            **crisis_flags,
            "jet_fuel_usd_gal": jet_fuel_price,
            "carrier_primary_iata": result["carrier_primary_iata"] if result else None,
            "route_distance_km": distance_km,
            "route_type": route_type,
            "shi_score": shi_score,
            "model_version": "v1_0_0",
        }

        snapshots.append(row)
        time.sleep(0.5)

    if snapshots:
        print(f"\nWriting {len(snapshots)} snapshots...")
        try:
            supabase.table("snapshots").insert(snapshots).execute()
            print(f"Successfully wrote {len(snapshots)} rows")
        except Exception as ex:
            print(f"Insert failed: {ex}")
            raise

    print("\nSearches per origin:")
    for origin in origins:
        print(f"  {origin}: {searches_per_origin[origin]}")

    print(f"\n{'=' * 70}")
    print(f"Capture complete: {len(snapshots)} snapshots")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()