#!/usr/bin/env python3
"""
TravelTxter V3.2 - Duffel Feeder
Searches for flight deals via Duffel API and populates Google Sheets.

This feeder:
1. Reads route configurations from CONFIG sheet
2. Searches Duffel API for flight offers
3. Filters and deduplicates deals
4. Appends new deals to RAW_DEALS sheet
5. Respects rate limits and quotas
"""

import os
import sys
import time
import uuid
import hashlib
import logging
from pathlib import Path
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import requests

# ============================================================================
# PATH SETUP
# ============================================================================

# Ensure repo root is on the import path (works in GitHub Actions + locally)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from lib.sheets import get_env, get_gspread_client, now_iso
except ImportError as e:
    print(f"❌ Failed to import lib.sheets: {e}")
    print("   Make sure lib/sheets.py exists and is properly configured")
    sys.exit(1)

# ============================================================================
# LOGGING SETUP
# ============================================================================

os.makedirs('logs', exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/duffel_feeder.log', mode='a')
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONSTANTS
# ============================================================================

DUFFEL_BASE_URL = "https://api.duffel.com/air"
DUFFEL_VERSION = "v2"
RAW_STATUS_NEW = "NEW"
FEEDER_SOURCE = os.getenv("FEEDER_SOURCE", "DUFFEL_GHA_FEEDER").strip()

# Default configuration values
DEFAULT_MAX_SEARCHES = 8
DEFAULT_MAX_INSERTS = 20
DEFAULT_SLEEP_SECONDS = 0.6
DEFAULT_MAX_OFFERS_PER_SEARCH = 20
DEFAULT_MAX_RETRIES = 3
DEFAULT_FLUSH_BATCH_SIZE = 10
DEFAULT_FAIL_ON_ZERO_INSERTS = "FALSE"

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def safe_int(x: Any, default: int) -> int:
    """Safely convert value to int with fallback."""
    try:
        return int(float(str(x).strip()))
    except Exception:
        return default


def safe_float(x: Any, default: float) -> float:
    """Safely convert value to float with fallback."""
    try:
        return float(str(x).strip())
    except Exception:
        return default


def truthy(x: Any) -> bool:
    """Check if value represents true."""
    return str(x).strip().upper() in ("TRUE", "1", "YES", "Y")


def deal_fingerprint(origin_city: str, destination_city: str, outbound_date: str,
                     return_date: str, airline: str, stops: str) -> str:
    """
    Generate unique fingerprint for a deal to detect duplicates.
    
    Args:
        origin_city: Origin city name
        destination_city: Destination city name
        outbound_date: Outbound date (YYYY-MM-DD)
        return_date: Return date (YYYY-MM-DD)
        airline: Airline name
        stops: Number of stops
        
    Returns:
        MD5 hash of the deal parameters
    """
    raw = f"{origin_city}|{destination_city}|{outbound_date}|{return_date}|{airline}|{stops}".lower()
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


# ============================================================================
# ENVIRONMENT VALIDATION
# ============================================================================

def require_env() -> None:
    """Validate required environment variables are set."""
    logger.info("🔍 Validating environment variables...")
    
    required = {
        'DUFFEL_API_KEY': 'Duffel API authentication key',
        'SHEET_ID': 'Google Sheet ID'
    }
    
    missing = []
    for var, desc in required.items():
        try:
            value = get_env(var)
            logger.info(f"✅ {var}: Set ({len(value)} chars)")
        except Exception:
            logger.error(f"❌ {var}: NOT SET - {desc}")
            missing.append(var)
    
    if missing:
        raise ValueError(f"Missing required environment variables: {missing}")
    
    logger.info("✅ Environment validated")


def validate_headers(tab_name: str, headers: List[str]) -> None:
    """
    Validate sheet headers for duplicates.
    
    Args:
        tab_name: Name of the worksheet
        headers: List of header values
        
    Raises:
        ValueError: If duplicate headers found
    """
    seen = set()
    dupes = set()
    
    for h in headers:
        if h in seen:
            dupes.add(h)
        seen.add(h)
    
    if dupes:
        raise ValueError(f"{tab_name} has duplicate headers: {sorted(dupes)}")
    
    logger.debug(f"✅ Headers validated for {tab_name}")


# ============================================================================
# DUFFEL API FUNCTIONS
# ============================================================================

def duffel_headers() -> Dict[str, str]:
    """Get headers for Duffel API requests."""
    return {
        "Authorization": f"Bearer {get_env('DUFFEL_API_KEY')}",
        "Duffel-Version": DUFFEL_VERSION,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _log_duffel_error(prefix: str, r: requests.Response) -> None:
    """Log Duffel API error with details."""
    body = "<no body>"
    try:
        body = str(r.json())[:800]
    except Exception:
        try:
            body = (r.text or "")[:800]
        except Exception:
            body = "<unreadable body>"
    
    logger.error(f"[Duffel] {prefix} status={r.status_code} body={body}")


def _post_offer_request(payload: Dict[str, Any], max_retries: int) -> Optional[Dict[str, Any]]:
    """
    Post an offer request to Duffel API with retry logic.
    
    Args:
        payload: Request payload
        max_retries: Maximum retry attempts
        
    Returns:
        Response data or None if failed
    """
    url = f"{DUFFEL_BASE_URL}/offer_requests"
    backoff = 1.0

    for attempt in range(1, max_retries + 1):
        try:
            logger.debug(f"[Duffel] Posting offer request (attempt {attempt}/{max_retries})")
            r = requests.post(url, headers=duffel_headers(), json=payload, timeout=30)
            
        except requests.exceptions.Timeout:
            logger.warning(f"[Duffel] Request timeout (attempt {attempt}/{max_retries})")
            time.sleep(backoff)
            backoff *= 2
            continue
            
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"[Duffel] Connection error (attempt {attempt}/{max_retries}): {e}")
            time.sleep(backoff)
            backoff *= 2
            continue
            
        except Exception as e:
            logger.error(f"[Duffel] Unexpected error (attempt {attempt}/{max_retries}): {e}")
            time.sleep(backoff)
            backoff *= 2
            continue

        # Success
        if r.ok:
            try:
                return r.json()
            except Exception as e:
                logger.error(f"[Duffel] Response ok but JSON parse failed: {e}")
                return None

        # Retryable errors
        if r.status_code in (429, 500, 502, 503, 504):
            _log_duffel_error(f"Retryable error (attempt {attempt}/{max_retries})", r)
            time.sleep(backoff)
            backoff *= 2
            continue

        # Non-retryable error
        _log_duffel_error("Non-retryable error", r)
        return None

    logger.error("[Duffel] Max retries exceeded for offer_request")
    return None


def _extract_offer_request_id(data: Dict[str, Any]) -> Optional[str]:
    """
    Extract offer_request ID from API response.
    
    Args:
        data: API response data
        
    Returns:
        Offer request ID or None
    """
    # Try multiple possible locations
    offer_req = data.get("offer_request") or {}
    cand = offer_req.get("id") or data.get("id")
    
    if cand:
        return str(cand)
    
    logger.warning(f"[Duffel] Could not find offer_request ID in response keys: {list(data.keys())}")
    return None


def _get_offers_for_offer_request(offer_request_id: str, max_retries: int) -> List[Dict[str, Any]]:
    """
    Get offers for a specific offer request.
    
    Args:
        offer_request_id: Offer request ID
        max_retries: Maximum retry attempts
        
    Returns:
        List of offer dictionaries
    """
    url = f"{DUFFEL_BASE_URL}/offer_requests/{offer_request_id}/offers"
    backoff = 1.0

    for attempt in range(1, max_retries + 1):
        try:
            logger.debug(f"[Duffel] Getting offers (attempt {attempt}/{max_retries})")
            r = requests.get(url, headers=duffel_headers(), timeout=30)
            
        except requests.exceptions.Timeout:
            logger.warning(f"[Duffel] Request timeout (attempt {attempt}/{max_retries})")
            time.sleep(backoff)
            backoff *= 2
            continue
            
        except Exception as e:
            logger.error(f"[Duffel] Exception (attempt {attempt}/{max_retries}): {e}")
            time.sleep(backoff)
            backoff *= 2
            continue

        # Success
        if r.ok:
            try:
                data = r.json().get("data") or []
                return data if isinstance(data, list) else []
            except Exception as e:
                logger.error(f"[Duffel] Response ok but JSON parse failed: {e}")
                return []

        # Retryable errors
        if r.status_code in (429, 500, 502, 503, 504):
            _log_duffel_error(f"Retryable error (attempt {attempt}/{max_retries})", r)
            time.sleep(backoff)
            backoff *= 2
            continue

        # Non-retryable error
        _log_duffel_error("Non-retryable error", r)
        return []

    logger.error("[Duffel] Max retries exceeded for offers")
    return []


def search_roundtrip(
    origin: str,
    destination: str,
    departure_date: str,
    return_date: str,
    cabin_class: str,
    max_connections: int,
    max_retries: int,
) -> List[Dict[str, Any]]:
    """
    Search for roundtrip flight offers.
    
    Args:
        origin: Origin airport code (IATA)
        destination: Destination airport code (IATA)
        departure_date: Departure date (YYYY-MM-DD)
        return_date: Return date (YYYY-MM-DD)
        cabin_class: Cabin class (economy, premium_economy, business, first)
        max_connections: Maximum number of connections
        max_retries: Maximum retry attempts
        
    Returns:
        List of offer dictionaries
    """
    logger.info(f"🔍 Searching: {origin}→{destination} out={departure_date} ret={return_date}")
    
    payload = {
        "data": {
            "slices": [
                {
                    "origin": origin,
                    "destination": destination,
                    "departure_date": departure_date
                },
                {
                    "origin": destination,
                    "destination": origin,
                    "departure_date": return_date
                },
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": cabin_class,
            "max_connections": max_connections,
        }
    }

    resp = _post_offer_request(payload, max_retries=max_retries)
    if not resp:
        logger.warning(f"⚠️ No response for {origin}→{destination}")
        return []

    data = resp.get("data") or {}

    # If offers are included directly in response
    offers = data.get("offers")
    if isinstance(offers, list) and offers:
        logger.info(f"✅ Got {len(offers)} offers directly")
        return offers

    # Otherwise fetch offers via offer_request ID
    offer_request_id = _extract_offer_request_id(data)
    if not offer_request_id:
        logger.warning(f"⚠️ No offer_request ID found for {origin}→{destination}")
        return []

    offers = _get_offers_for_offer_request(offer_request_id, max_retries=max_retries)
    logger.info(f"✅ Got {len(offers)} offers via offer_request")
    return offers


# ============================================================================
# DEAL PARSING
# ============================================================================

def parse_offer(offer: Dict[str, Any], route: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Parse a Duffel offer into a deal record.
    
    Args:
        offer: Duffel offer dictionary
        route: Route configuration
        
    Returns:
        Deal dictionary or None if invalid
    """
    try:
        slices = offer.get("slices") or []
        if len(slices) < 2:
            logger.debug("Skipping offer: less than 2 slices")
            return None

        seg0 = ((slices[0] or {}).get("segments") or [])
        seg1 = ((slices[1] or {}).get("segments") or [])
        
        if not seg0 or not seg1:
            logger.debug("Skipping offer: missing segments")
            return None

        # Get first segment of each slice
        out_seg = seg0[0]
        ret_seg = seg1[0]

        # Extract dates
        out_date = str(out_seg.get("departing_at", ""))[:10]
        ret_date = str(ret_seg.get("departing_at", ""))[:10]
        
        if len(out_date) != 10 or len(ret_date) != 10:
            logger.debug(f"Skipping offer: invalid dates {out_date} / {ret_date}")
            return None

        # Extract price
        price = safe_float(offer.get("total_amount", 0), 0.0)
        currency = str(offer.get("total_currency", "")).upper().strip()
        
        if currency != "GBP":
            logger.debug(f"Skipping offer: currency {currency} != GBP")
            return None

        # Check price limits
        max_price = safe_float(route.get("max_price_gbp", 999999), 999999.0)
        if price <= 0 or price > max_price:
            logger.debug(f"Skipping offer: price £{price} outside range (max: £{max_price})")
            return None

        # Extract airline
        airline = str((offer.get("owner") or {}).get("name", "")).strip()
        if not airline:
            airline = "Unknown"

        # Calculate stops
        outbound_stops = max(0, len(seg0) - 1)
        return_stops = max(0, len(seg1) - 1)
        total_stops = outbound_stops + return_stops
        stops = str(total_stops)

        # Generate fingerprint
        fp = deal_fingerprint(
            origin_city=route["origin_city"],
            destination_city=route["destination_city"],
            outbound_date=out_date,
            return_date=ret_date,
            airline=airline,
            stops=stops,
        )

        deal = {
            "deal_id": uuid.uuid4().hex[:12],
            "origin_city": route["origin_city"],
            "destination_city": route["destination_city"],
            "destination_country": route["destination_country"],
            "price_gbp": f"{price:.2f}",
            "outbound_date": out_date,
            "return_date": ret_date,
            "trip_length_days": safe_int(route.get("trip_length_days", 4), 4),
            "stops": stops,
            "baggage_included": "",
            "airline": airline,
            "deal_source": FEEDER_SOURCE,
            "notes": route.get("notes", f'{route["origin_iata"]}->{route["destination_iata"]}'),
            "date_added": now_iso(),
            "raw_status": RAW_STATUS_NEW,
            "deal_fingerprint": fp,
        }

        logger.debug(f"✅ Parsed deal: {deal['origin_city']}→{deal['destination_city']} £{deal['price_gbp']}")
        return deal

    except Exception as e:
        logger.error(f"❌ Error parsing offer: {e}")
        return None


# ============================================================================
# SHEET OPERATIONS
# ============================================================================

def load_existing_fingerprints(raw_ws, raw_headers: List[str]) -> set:
    """
    Load existing deal fingerprints from sheet to avoid duplicates.
    
    Args:
        raw_ws: Raw deals worksheet
        raw_headers: List of header names
        
    Returns:
        Set of existing fingerprints
    """
    logger.info("📥 Loading existing fingerprints...")
    
    if "deal_fingerprint" not in raw_headers:
        logger.warning("⚠️ No deal_fingerprint column found - will add all deals")
        return set()
    
    try:
        fp_col = raw_headers.index("deal_fingerprint") + 1
        vals = raw_ws.col_values(fp_col)
        fingerprints = set(v.strip() for v in vals[1:] if v and v.strip())
        logger.info(f"✅ Loaded {len(fingerprints)} existing fingerprints")
        return fingerprints
        
    except Exception as e:
        logger.error(f"❌ Error loading fingerprints: {e}")
        return set()


def build_row(raw_headers: List[str], raw_idx: Dict[str, int], item: Dict[str, Any]) -> List[str]:
    """
    Build a row for appending to sheet.
    
    Args:
        raw_headers: List of header names
        raw_idx: Header name to index mapping
        item: Deal dictionary
        
    Returns:
        List of values matching header order
    """
    row = [""] * len(raw_headers)
    for k, v in item.items():
        if k in raw_idx:
            row[raw_idx[k]] = str(v)
    return row


# ============================================================================
# MAIN LOGIC
# ============================================================================

def main() -> None:
    """Main feeder execution."""
    logger.info("\n" + "="*60)
    logger.info("🚀 TravelTxter Duffel Feeder Starting")
    logger.info("="*60)
    
    try:
        # Validate environment
        require_env()
        
        # Load configuration from environment
        max_searches = safe_int(os.getenv("FEEDER_MAX_SEARCHES"), DEFAULT_MAX_SEARCHES)
        max_inserts = safe_int(os.getenv("FEEDER_MAX_INSERTS"), DEFAULT_MAX_INSERTS)
        sleep_seconds = safe_float(os.getenv("FEEDER_SLEEP_SECONDS"), DEFAULT_SLEEP_SECONDS)
        max_offers = safe_int(os.getenv("FEEDER_MAX_OFFERS_PER_SEARCH"), DEFAULT_MAX_OFFERS_PER_SEARCH)
        max_retries = safe_int(os.getenv("FEEDER_MAX_RETRIES"), DEFAULT_MAX_RETRIES)
        flush_batch = safe_int(os.getenv("FEEDER_FLUSH_BATCH_SIZE"), DEFAULT_FLUSH_BATCH_SIZE)
        fail_on_zero_inserts = truthy(os.getenv("FEEDER_FAIL_ON_ZERO_INSERTS", DEFAULT_FAIL_ON_ZERO_INSERTS))
        
        logger.info(f"⚙️ Configuration:")
        logger.info(f"   Max searches: {max_searches}")
        logger.info(f"   Max inserts: {max_inserts}")
        logger.info(f"   Sleep between searches: {sleep_seconds}s")
        logger.info(f"   Max offers per search: {max_offers}")
        logger.info(f"   Max retries: {max_retries}")
        logger.info(f"   Flush batch size: {flush_batch}")
        
        # Connect to Google Sheets
        logger.info("\n📊 Connecting to Google Sheets...")
        sh = get_gspread_client().open_by_key(get_env("SHEET_ID"))
        
        raw_tab_name = os.getenv("RAW_DEALS_TAB", "RAW_DEALS")
        cfg_tab_name = os.getenv("FEEDER_CONFIG_TAB", "CONFIG")
        
        logger.info(f"   Opening worksheet: {raw_tab_name}")
        raw_ws = sh.worksheet(raw_tab_name)
        
        logger.info(f"   Opening worksheet: {cfg_tab_name}")
        cfg_ws = sh.worksheet(cfg_tab_name)
        
        # Validate RAW_DEALS sheet
        raw_headers = raw_ws.row_values(1)
        validate_headers("RAW_DEALS", raw_headers)
        raw_idx = {h: i for i, h in enumerate(raw_headers)}
        logger.info(f"✅ RAW_DEALS has {len(raw_headers)} columns")
        
        # Load and validate CONFIG
        cfg = cfg_ws.get_all_values()
        if len(cfg) < 2:
            logger.warning("⚠️ No routes found in CONFIG sheet")
            return
        
        cfg_headers = cfg[0]
        validate_headers("CONFIG", cfg_headers)
        cfg_idx = {h: i for i, h in enumerate(cfg_headers)}
        
        # Check for required CONFIG columns
        required_cfg = [
            "enabled", "origin_iata", "origin_city",
            "destination_iata", "destination_city", "destination_country",
            "trip_length_days", "max_connections", "cabin_class",
            "max_price_gbp", "step_days", "window_days", "days_ahead",
        ]
        missing = [c for c in required_cfg if c not in cfg_idx]
        if missing:
            raise ValueError(f"CONFIG missing required columns: {missing}")
        
        logger.info(f"✅ CONFIG has {len(cfg_headers)} columns")
        
        # Load existing fingerprints
        existing_fp = load_existing_fingerprints(raw_ws, raw_headers)
        
        # Parse enabled routes from CONFIG
        routes: List[Dict[str, Any]] = []
        logger.info(f"\n📋 Processing {len(cfg) - 1} CONFIG rows...")
        
        for row_num, r in enumerate(cfg[1:], start=2):
            if not r:
                continue
            
            # Check if enabled
            enabled = str(r[cfg_idx["enabled"]]).strip().upper()
            if enabled != "TRUE":
                logger.debug(f"   Row {row_num}: Skipped (not enabled)")
                continue
            
            # Extract route data
            origin_iata = str(r[cfg_idx["origin_iata"]]).strip().upper()
            origin_city = str(r[cfg_idx["origin_city"]]).strip()
            dest_iata = str(r[cfg_idx["destination_iata"]]).strip().upper()
            dest_city = str(r[cfg_idx["destination_city"]]).strip()
            dest_country = str(r[cfg_idx["destination_country"]]).strip()
            
            if not all([origin_iata, origin_city, dest_iata, dest_city, dest_country]):
                logger.warning(f"   Row {row_num}: Skipped (missing required fields)")
                continue
            
            route = {
                "origin_iata": origin_iata,
                "origin_city": origin_city,
                "destination_iata": dest_iata,
                "destination_city": dest_city,
                "destination_country": dest_country,
                "trip_length_days": safe_int(r[cfg_idx["trip_length_days"]], 4),
                "max_connections": safe_int(r[cfg_idx["max_connections"]], 1),
                "cabin_class": (str(r[cfg_idx["cabin_class"]]).strip().lower() or "economy"),
                "max_price_gbp": safe_float(r[cfg_idx["max_price_gbp"]], 999999.0),
                "step_days": safe_int(r[cfg_idx["step_days"]], 7),
                "window_days": safe_int(r[cfg_idx["window_days"]], 28),
                "days_ahead": safe_int(r[cfg_idx["days_ahead"]], 7),
                "notes": f"{origin_iata}->{dest_iata}",
            }
            
            routes.append(route)
            logger.info(f"   ✅ Row {row_num}: {origin_iata}→{dest_iata}")
        
        logger.info(f"✅ Loaded {len(routes)} enabled routes")
        
        if not routes:
            logger.warning("⚠️ No enabled routes to process")
            return
        
        # Process routes
        searches = 0
        failed_searches = 0
        inserts = 0
        skipped_duplicates = 0
        batch: List[List[str]] = []
        
        logger.info(f"\n🔍 Starting flight searches...")
        logger.info(f"   Limits: {max_searches} searches, {max_inserts} inserts")
        
        for route_num, route in enumerate(routes, start=1):
            if searches >= max_searches or inserts >= max_inserts:
                logger.info(f"⚠️ Reached limits (searches={searches}, inserts={inserts})")
                break
            
            logger.info(f"\n📍 Route {route_num}/{len(routes)}: {route['origin_iata']}→{route['destination_iata']}")
            
            # Calculate search window
            start = date.today() + timedelta(days=route["days_ahead"])
            end = start + timedelta(days=route["window_days"])
            d = start
            
            route_inserts = 0
            
            while d <= end:
                if searches >= max_searches or inserts >= max_inserts:
                    break
                
                searches += 1
                return_date = d + timedelta(days=route["trip_length_days"])
                
                # Search for offers
                offers = search_roundtrip(
                    route["origin_iata"],
                    route["destination_iata"],
                    d.isoformat(),
                    return_date.isoformat(),
                    route["cabin_class"],
                    int(route["max_connections"]),
                    max_retries=max_retries,
                )
                
                if not offers:
                    failed_searches += 1
                    logger.warning(f"   ⚠️ No offers for {d.isoformat()}")
                else:
                    logger.info(f"   ✅ Found {len(offers)} offers for {d.isoformat()}")
                
                # Process offers
                for offer in offers[:max_offers]:
                    parsed = parse_offer(offer, route)
                    if not parsed:
                        continue
                    
                    fp = parsed["deal_fingerprint"]
                    if fp in existing_fp:
                        skipped_duplicates += 1
                        logger.debug(f"      Skipped duplicate: {fp[:8]}")
                        continue
                    
                    # New deal!
                    existing_fp.add(fp)
                    batch.append(build_row(raw_headers, raw_idx, parsed))
                    inserts += 1
                    route_inserts += 1
                    
                    logger.info(f"      ✅ New deal: {parsed['origin_city']}→{parsed['destination_city']} £{parsed['price_gbp']}")
                    
                    if inserts >= max_inserts:
                        break
                
                # Flush batch if needed
                if batch and (len(batch) >= flush_batch or inserts >= max_inserts):
                    try:
                        raw_ws.append_rows(batch, value_input_option="USER_ENTERED")
                        logger.info(f"   💾 Appended {len(batch)} rows to sheet (total: {inserts})")
                        batch = []
                    except Exception as e:
                        logger.error(f"   ❌ Failed to append rows: {e}")
                
                # Move to next date
                d += timedelta(days=route["step_days"])
                
                # Rate limiting
                if d <= end:
                    time.sleep(sleep_seconds)
            
            logger.info(f"   Route complete: {route_inserts} new deals inserted")
        
        # Final flush
        if batch:
            try:
                raw_ws.append_rows(batch, value_input_option="USER_ENTERED")
                logger.info(f"💾 Final flush: {len(batch)} rows appended")
            except Exception as e:
                logger.error(f"❌ Failed to append final batch: {e}")
        
        # Summary
        logger.info("\n" + "="*60)
        logger.info("📊 FEEDER SUMMARY")
        logger.info("="*60)
        logger.info(f"Searches performed: {searches}")
        logger.info(f"Failed searches: {failed_searches}")
        logger.info(f"New deals inserted: {inserts}")
        logger.info(f"Duplicates skipped: {skipped_duplicates}")
        logger.info("="*60 + "\n")
        
        # Save stats for GitHub Actions
        try:
            stats = {
                "searches": searches,
                "failed_searches": failed_searches,
                "inserts": inserts,
                "skipped_duplicates": skipped_duplicates,
                "routes_processed": len(routes),
                "timestamp": now_iso()
            }
            
            import json
            with open('logs/feeder_stats.json', 'w') as f:
                json.dump(stats, f, indent=2)
            logger.info("📊 Stats saved to logs/feeder_stats.json")
        except Exception as e:
            logger.warning(f"⚠️ Could not save stats: {e}")
        
        # Fail workflow if configured and no inserts
        if fail_on_zero_inserts and searches > 0 and inserts == 0:
            logger.error("❌ ALERT: Searches performed but zero inserts!")
            logger.error("   This may indicate an issue with Duffel API or pricing filters")
            raise SystemExit(1)
        
        logger.info("✅ Feeder completed successfully")
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️ Feeder interrupted by user")
        sys.exit(130)
        
    except Exception as e:
        logger.error(f"\n❌ Feeder failed: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
