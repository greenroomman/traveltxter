#!/usr/bin/env python3
"""
TravelTxter V4.2 REFACTORED - Best-of-Batch Selection Engine

Key Changes:
1. Multi-route rotation from CONFIG tab (priority-sorted)
2. Batch scoring ALL NEW deals, select BEST for publishing
3. Marketing priority flags (instagram_eligible, telegram_priority)
4. Public-facing: City names, rounded prices, YYMMDD dates
5. Max 20 inserts per Duffel search (maximize search credits)
"""

import os
import json
import uuid
import datetime as dt
import re
import time
import math
from typing import Dict, Any, List, Tuple, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials
from openai import OpenAI


# =========================
# Logging
# =========================
def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


def now_utc() -> dt.datetime:
    return dt.datetime.utcnow()


# =========================
# ENV
# =========================
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "").strip()
RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS").strip()
CONFIG_TAB = os.getenv("CONFIG_TAB", "CONFIG").strip()

GCP_SA_JSON = os.getenv("GCP_SA_JSON", "").strip()

DUFFEL_API_KEY = os.getenv("DUFFEL_API_KEY", "").strip()
DUFFEL_VERSION = os.getenv("DUFFEL_VERSION", "v2").strip()
DUFFEL_MAX_INSERTS = int(os.getenv("DUFFEL_MAX_INSERTS", "20"))  # Maximize value
DUFFEL_ROUTES_PER_RUN = int(os.getenv("DUFFEL_ROUTES_PER_RUN", "2"))  # 1-2 routes
DUFFEL_ENABLED = os.getenv("DUFFEL_ENABLED", "true").strip().lower() in ("1", "true", "yes")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()

# Rendering & Instagram
RENDER_URL = os.getenv("RENDER_URL", "").strip()
IG_ACCESS_TOKEN = os.getenv("IG_ACCESS_TOKEN", "").strip()
IG_USER_ID = os.getenv("IG_USER_ID", "").strip()

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHANNEL = os.getenv("TELEGRAM_CHANNEL", "").strip()
TELEGRAM_BOT_TOKEN_VIP = os.getenv("TELEGRAM_BOT_TOKEN_VIP", "").strip()
TELEGRAM_CHANNEL_VIP = os.getenv("TELEGRAM_CHANNEL_VIP", "").strip()

# Subscription links
STRIPE_LINK_MONTHLY = os.getenv("STRIPE_LINK_MONTHLY", "").strip()
STRIPE_LINK_YEARLY = os.getenv("STRIPE_LINK_YEARLY", "").strip()
SKYSCANNER_AFFILIATE_ID = os.getenv("SKYSCANNER_AFFILIATE_ID", "").strip()

VIP_DELAY_HOURS = int(os.getenv("VIP_DELAY_HOURS", "24"))
RUN_SLOT = os.getenv("RUN_SLOT", "AM").strip().upper()


# =========================
# Status constants
# =========================
STATUS_NEW = "NEW"
STATUS_READY_TO_POST = "READY_TO_POST"
STATUS_READY_TO_PUBLISH = "READY_TO_PUBLISH"
STATUS_POSTED_INSTAGRAM = "POSTED_INSTAGRAM"
STATUS_POSTED_TELEGRAM_VIP = "POSTED_TELEGRAM_VIP"
STATUS_POSTED_ALL = "POSTED_ALL"


# =========================
# Formatting Helpers (Marketing Rules)
# =========================
def round_price_up(price_str: str) -> int:
    """Round price UP to nearest whole pound for public display"""
    try:
        price = float(price_str)
        return math.ceil(price)
    except:
        return 0


def format_date_yymmdd(date_str: str) -> str:
    """Convert YYYY-MM-DD to YYMMDD for public display"""
    try:
        if not date_str:
            return ""
        # Handle various date formats
        date_str = date_str.strip()
        
        # Already YYYYMMDD or YYMMDD?
        if len(date_str) == 8 and date_str.isdigit():
            return date_str[2:]  # Return last 6 digits
        if len(date_str) == 6 and date_str.isdigit():
            return date_str
        
        # Parse ISO format (YYYY-MM-DD)
        if '-' in date_str:
            parts = date_str.split('-')
            if len(parts) == 3:
                yy = parts[0][2:]  # Last 2 digits of year
                mm = parts[1].zfill(2)
                dd = parts[2].zfill(2)
                return f"{yy}{mm}{dd}"
        
        # Parse MM/DD/YY or DD/MM/YY
        if '/' in date_str:
            parts = date_str.split('/')
            if len(parts) == 3:
                # Assume MM/DD/YY format from spreadsheet
                mm, dd, yy = parts
                return f"{yy.zfill(2)}{mm.zfill(2)}{dd.zfill(2)}"
        
        return ""
    except:
        return ""


# =========================
# Google Sheets helpers
# =========================
def load_sa_json() -> Dict[str, Any]:
    if GCP_SA_JSON:
        return json.loads(GCP_SA_JSON)
    raise RuntimeError("Missing GCP_SA_JSON")


def get_ws() -> Tuple[gspread.Worksheet, List[str]]:
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID")

    sa = load_sa_json()
    creds = Credentials.from_service_account_info(
        sa,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(RAW_DEALS_TAB)
    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("RAW_DEALS has no headers")
    return ws, headers


def header_map(headers: List[str]) -> Dict[str, int]:
    return {h.strip(): i + 1 for i, h in enumerate(headers) if h.strip()}


def safe_get(row: Dict[str, str], key: str) -> str:
    return (row.get(key) or "").strip()


# =========================
# CONFIG: Multi-Route Rotation
# =========================
def load_routes_from_config(ws_parent: gspread.Spreadsheet) -> List[Tuple[int, str, str, str, str, int]]:
    """
    Load enabled routes from CONFIG tab, sorted by priority.
    Returns: List[(priority, origin_iata, origin_city, dest_iata, dest_city, days_ahead)]
    """
    try:
        cfg = ws_parent.worksheet(CONFIG_TAB)
    except Exception:
        log("CONFIG tab not found - using defaults")
        return []

    values = cfg.get_all_values()
    if not values or len(values) < 2:
        return []

    headers = [h.strip() for h in values[0]]
    idx = {h: i for i, h in enumerate(headers) if h}

    routes: List[Tuple[int, str, str, str, str, int]] = []
    
    for r in values[1:]:
        # Check if enabled
        enabled = ""
        if "enabled" in idx and idx["enabled"] < len(r):
            enabled = (r[idx["enabled"]] or "").strip().upper()
        if enabled not in ("TRUE", "1", "YES", "Y"):
            continue

        # Extract fields
        priority = 1
        if "priority" in idx and idx["priority"] < len(r):
            try:
                priority = int(r[idx["priority"]] or "1")
            except:
                priority = 1

        origin_iata = ""
        origin_city = ""
        dest_iata = ""
        dest_city = ""
        days_ahead = 60

        if "origin_iata" in idx and idx["origin_iata"] < len(r):
            origin_iata = (r[idx["origin_iata"]] or "").strip().upper()
        if "origin_city" in idx and idx["origin_city"] < len(r):
            origin_city = (r[idx["origin_city"]] or "").strip()
        if "destination_iata" in idx and idx["destination_iata"] < len(r):
            dest_iata = (r[idx["destination_iata"]] or "").strip().upper()
        if "destination_city" in idx and idx["destination_city"] < len(r):
            dest_city = (r[idx["destination_city"]] or "").strip()
        if "days_ahead" in idx and idx["days_ahead"] < len(r):
            try:
                days_ahead = int(r[idx["days_ahead"]] or "60")
            except:
                days_ahead = 60

        if origin_iata and dest_iata:
            routes.append((priority, origin_iata, origin_city, dest_iata, dest_city, days_ahead))

    # Sort by priority (ascending - priority 1 is highest)
    routes.sort(key=lambda x: x[0])
    
    log(f"Loaded {len(routes)} enabled routes from CONFIG, sorted by priority")
    return routes


def select_routes_rotating(routes: List[Tuple[int, str, str, str, str, int]], 
                           max_routes: int) -> List[Tuple[int, str, str, str, str, int]]:
    """
    Select routes using deterministic rotation to ensure all routes get searched.
    Uses day-of-year + slot to rotate through the full library.
    """
    if not routes:
        return []
    
    if len(routes) <= max_routes:
        return routes
    
    # Deterministic selection based on day
    day_of_year = dt.date.today().timetuple().tm_yday
    slot_offset = 0 if RUN_SLOT == "AM" else 1
    
    # Calculate starting index (rotates through all routes over time)
    start_idx = ((day_of_year * 2) + slot_offset) % len(routes)
    
    # Select consecutive routes (wraps around)
    selected = []
    for i in range(max_routes):
        idx = (start_idx + i) % len(routes)
        selected.append(routes[idx])
    
    return selected


# =========================
# Skyscanner Affiliate Links
# =========================
def generate_skyscanner_link(origin_iata: str, dest_iata: str, 
                            out_date: str, ret_date: str) -> str:
    """Generate Skyscanner affiliate link with proper date formatting"""
    # Convert dates to YYYYMMDD format for Skyscanner
    out_formatted = out_date.replace('-', '')
    ret_formatted = ret_date.replace('-', '')
    
    base_url = f"https://www.skyscanner.net/transport/flights/{origin_iata}/{dest_iata}/{out_formatted}/{ret_formatted}/"
    
    if SKYSCANNER_AFFILIATE_ID:
        return f"{base_url}?affiliateid={SKYSCANNER_AFFILIATE_ID}"
    
    return base_url
# =========================
# DUFFEL FEED - Multi-Route with Maximum Inserts
# =========================
def duffel_offer_request(origin: str, dest: str, out_date: str, ret_date: str) -> Dict[str, Any]:
    url = "https://api.duffel.com/air/offer_requests"
    headers = {
        "Authorization": f"Bearer {DUFFEL_API_KEY}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Duffel-Version": DUFFEL_VERSION,
    }
    payload = {
        "data": {
            "slices": [
                {"origin": origin, "destination": dest, "departure_date": out_date},
                {"origin": dest, "destination": origin, "departure_date": ret_date},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": "economy",
        }
    }
    r = requests.post(url, headers=headers, json=payload, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"Duffel error {r.status_code}: {r.text[:500]}")
    return r.json()


def run_duffel_feeder(ws: gspread.Worksheet, headers: List[str]) -> int:
    """
    REFACTORED: Multi-route rotation with priority sorting.
    Searches 1-2 routes per run (CONFIG priority), inserts up to 20 offers per route.
    """
    if not DUFFEL_ENABLED:
        log("Duffel: DISABLED")
        return 0
    if not DUFFEL_API_KEY:
        log("Duffel: ENABLED but missing API key")
        return 0

    sh = ws.spreadsheet
    routes = load_routes_from_config(sh)
    
    if not routes:
        log("No enabled routes in CONFIG - skipping Duffel")
        return 0

    # Select 1-2 routes for this run (rotating through all routes)
    selected_routes = select_routes_rotating(routes, DUFFEL_ROUTES_PER_RUN)
    
    log(f"Duffel: Searching {len(selected_routes)} routes (max {DUFFEL_MAX_INSERTS} inserts/route)")
    for priority, o_iata, o_city, d_iata, d_city, days_ahead in selected_routes:
        log(f"  Priority {priority}: {o_city} ({o_iata}) → {d_city} ({d_iata}), {days_ahead} days ahead")

    hmap = header_map(headers)
    total_inserted = 0

    for priority, origin_iata, origin_city, dest_iata, dest_city, days_ahead in selected_routes:
        
        # Calculate search dates
        today = dt.date.today()
        out_date = today + dt.timedelta(days=days_ahead)
        
        # Get trip_length from first matching config row (default 5)
        trip_length = 5
        for r in routes:
            if r[1] == origin_iata and r[3] == dest_iata:
                # Look up trip_length_days from CONFIG if available
                trip_length = 5  # Default, could enhance to read from CONFIG
                break
        
        ret_date = out_date + dt.timedelta(days=trip_length)

        log(f"Searching: {origin_city}->{dest_city} | Out: {out_date} | Back: {ret_date}")
        
        try:
            data = duffel_offer_request(origin_iata, dest_iata, str(out_date), str(ret_date))
        except Exception as e:
            log(f"Duffel API error for {origin_iata}->{dest_iata}: {e}")
            continue

        offers = []
        try:
            offers = (data.get("data") or {}).get("offers") or []
        except:
            offers = []

        if not offers:
            log(f"  0 offers returned for {origin_city}->{dest_city}")
            continue

        log(f"  {len(offers)} offers found, inserting up to {DUFFEL_MAX_INSERTS}")

        rows_to_append: List[List[Any]] = []
        inserted_for_route = 0

        for off in offers:
            if inserted_for_route >= DUFFEL_MAX_INSERTS:
                break

            price = off.get("total_amount") or ""
            currency = off.get("total_currency") or "GBP"
            if currency != "GBP":
                continue

            airline = (off.get("owner") or {}).get("name") or ""

            slices = off.get("slices") or []
            stops = "0"
            try:
                segs = (slices[0].get("segments") or [])
                stops = str(max(0, len(segs) - 1))
            except:
                pass

            # Extract destination country from Duffel if available
            dest_country = ""
            try:
                if slices:
                    d = (slices[0].get("destination") or {})
                    dest_country = (d.get("country_name") or "").strip()
            except:
                pass

            deal_id = str(uuid.uuid4())

            # Build row
            row_obj = {h: "" for h in headers}
            row_obj["deal_id"] = deal_id
            row_obj["origin_iata"] = origin_iata
            row_obj["origin_city"] = origin_city or origin_iata
            row_obj["destination_iata"] = dest_iata
            row_obj["destination_city"] = dest_city or dest_iata
            row_obj["destination_country"] = dest_country
            row_obj["outbound_date"] = str(out_date)
            row_obj["return_date"] = str(ret_date)
            row_obj["price_gbp"] = price
            row_obj["trip_length_days"] = str(trip_length)
            row_obj["deal_source"] = "DUFFEL"
            row_obj["date_added"] = now_utc().replace(microsecond=0).isoformat() + "Z"
            row_obj["airline"] = airline
            row_obj["stops"] = stops
            row_obj["status"] = STATUS_NEW

            # Generate affiliate links
            booking_link = generate_skyscanner_link(origin_iata, dest_iata, str(out_date), str(ret_date))
            row_obj["booking_link_vip"] = booking_link
            row_obj["booking_link_free"] = booking_link
            row_obj["affiliate_url"] = booking_link

            rows_to_append.append([row_obj.get(h, "") for h in headers])
            inserted_for_route += 1

        if rows_to_append:
            ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
            log(f"  ✓ Inserted {len(rows_to_append)} deals for {origin_city}->{dest_city}")
            total_inserted += len(rows_to_append)
        else:
            log(f"  No valid GBP offers to insert")

    log(f"Duffel complete: {total_inserted} total deals inserted")
    return total_inserted


# =========================
# AI SCORING - Batch Processing with Marketing Flags
# =========================
def openai_client() -> OpenAI:
    if not OPENAI_API_KEY:
        raise RuntimeError("Missing OPENAI_API_KEY")
    return OpenAI(api_key=OPENAI_API_KEY)


def score_deal_with_ai(row: Dict[str, str]) -> Dict[str, Any]:
    """
    Score a deal with AI and return:
    {
        'ai_score': int (0-100),
        'ai_verdict': str (EXCELLENT/GOOD/AVERAGE/POOR),
        'ai_caption': str (short explanation),
        'is_instagram_eligible': bool (score > 90),
        'telegram_priority': str (High/Medium/Low)
    }
    """
    client = openai_client()

    origin_city = safe_get(row, "origin_city") or safe_get(row, "origin_iata")
    dest_city = safe_get(row, "destination_city") or safe_get(row, "destination_iata")
    dest_country = safe_get(row, "destination_country")
    price = safe_get(row, "price_gbp")
    out_date = safe_get(row, "outbound_date")
    ret_date = safe_get(row, "return_date")
    airline = safe_get(row, "airline") or "Unknown"
    stops = safe_get(row, "stops") or "0"
    trip_length = safe_get(row, "trip_length_days") or "5"

    prompt = f"""
You are a UK travel expert evaluating flight deals for TravelTxter subscribers.

Deal Details:
- Route: {origin_city} → {dest_city}, {dest_country}
- Price: £{price}
- Dates: {out_date} to {ret_date} ({trip_length} days)
- Airline: {airline}
- Stops: {stops}

Evaluate this deal and return JSON with:
1. ai_score (0-100): How good is this price? Consider:
   - Typical market prices for this route
   - Seasonality (cheaper in winter, expensive in summer/holidays)
   - Stop count (direct flights worth more)
   - Trip length vs price value
   
2. ai_verdict: EXCELLENT (90-100), GOOD (75-89), AVERAGE (60-74), POOR (<60)

3. ai_caption (1-2 sentences, max 120 chars): Why should someone book this? 
   Be specific, factual, conversational. NO EMOJIS.
   Examples:
   - "Direct flights to Reykjavik for under £110 are rare outside January"
   - "This undercuts typical £180 Barcelona fares by nearly 40 percent"
   - "Weekend in Porto for the price of a train to Edinburgh"

Return ONLY valid JSON, no markdown.
""".strip()

    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": "You are a travel deals expert. Return only valid JSON. Never use emojis."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=250
        )

        text_out = resp.choices[0].message.content.strip()

        # Clean markdown fences
        text_out = text_out.replace("```json", "").replace("```", "").strip()

        data = json.loads(text_out)

        ai_score = int(data.get("ai_score", 60))
        ai_verdict = str(data.get("ai_verdict", "AVERAGE")).upper()
        ai_caption = str(data.get("ai_caption", "")).strip()

        if not ai_caption:
            ai_caption = f"Return flights to {dest_city} for £{price}"

        # Marketing flags
        is_instagram_eligible = ai_score >= 90
        telegram_priority = "High" if ai_score >= 85 else ("Medium" if ai_score >= 70 else "Low")

        return {
            "ai_score": ai_score,
            "ai_verdict": ai_verdict,
            "ai_caption": ai_caption,
            "is_instagram_eligible": is_instagram_eligible,
            "telegram_priority": telegram_priority
        }

    except Exception as e:
        log(f"OpenAI error: {e}")
        # Fallback
        return {
            "ai_score": 60,
            "ai_verdict": "AVERAGE",
            "ai_caption": f"Return flights to {dest_city} for £{price}",
            "is_instagram_eligible": False,
            "telegram_priority": "Low"
        }


def stage_score_all_new_deals(ws: gspread.Worksheet, headers: List[str]) -> int:
    """
    REFACTORED: Batch score ALL deals with status=NEW.
    Sets ai_score, ai_verdict, ai_caption, is_instagram_eligible, telegram_priority.
    Does NOT promote - that happens in selection stage.
    """
    hmap = header_map(headers)

    if "status" not in hmap:
        raise RuntimeError("Missing 'status' column")

    rows = ws.get_all_values()
    if len(rows) < 2:
        return 0

    log("Scoring all NEW deals...")
    scored_count = 0

    for i in range(2, len(rows) + 1):
        row_vals = rows[i - 1]
        row = {headers[c]: (row_vals[c] if c < len(row_vals) else "") for c in range(len(headers))}

        status = safe_get(row, "status").upper()
        if status != STATUS_NEW:
            continue

        # Skip if already scored
        if safe_get(row, "ai_score"):
            log(f"  Row {i} already scored (score: {safe_get(row, 'ai_score')})")
            continue

        log(f"  Scoring row {i}: {safe_get(row, 'origin_city')}->{safe_get(row, 'destination_city')} £{safe_get(row, 'price_gbp')}")

        result = score_deal_with_ai(row)

        # Update sheet
        updates = []
        if "ai_score" in hmap:
            updates.append(gspread.Cell(i, hmap["ai_score"], result["ai_score"]))
        if "ai_verdict" in hmap:
            updates.append(gspread.Cell(i, hmap["ai_verdict"], result["ai_verdict"]))
        if "ai_caption" in hmap:
            updates.append(gspread.Cell(i, hmap["ai_caption"], result["ai_caption"]))
        if "is_instagram_eligible" in hmap:
            updates.append(gspread.Cell(i, hmap["is_instagram_eligible"], result["is_instagram_eligible"]))
        if "telegram_priority" in hmap:
            updates.append(gspread.Cell(i, hmap["telegram_priority"], result["telegram_priority"]))
        if "scored_timestamp" in hmap:
            updates.append(gspread.Cell(i, hmap["scored_timestamp"], now_utc().replace(microsecond=0).isoformat() + "Z"))

        if updates:
            ws.update_cells(updates, value_input_option="USER_ENTERED")

        log(f"    ✓ Score: {result['ai_score']}/100 | Verdict: {result['ai_verdict']} | IG Eligible: {result['is_instagram_eligible']}")
        scored_count += 1

    log(f"Scored {scored_count} NEW deals")
    return scored_count


def stage_select_best_for_publishing(ws: gspread.Worksheet, headers: List[str], max_to_promote: int = 1) -> int:
    """
    REFACTORED: Select the BEST scored deals and promote to READY_TO_PUBLISH.
    Looks at ALL deals with status=NEW and ai_score set.
    Selects top N by ai_score (with freshness decay).
    """
    hmap = header_map(headers)

    rows = ws.get_all_values()
    if len(rows) < 2:
        return 0

    log("Selecting best deals for publishing...")

    # Collect all scored NEW deals
    candidates: List[Tuple[int, float, Dict[str, str]]] = []

    for i in range(2, len(rows) + 1):
        row_vals = rows[i - 1]
        row = {headers[c]: (row_vals[c] if c < len(row_vals) else "") for c in range(len(headers))}

        status = safe_get(row, "status").upper()
        if status != STATUS_NEW:
            continue

        ai_score_str = safe_get(row, "ai_score")
        if not ai_score_str:
            continue

        try:
            ai_score = float(ai_score_str)
        except:
            continue

        # Apply freshness decay (2 points per day)
        date_added_str = safe_get(row, "date_added")
        age_days = 0
        if date_added_str:
            try:
                date_added = dt.datetime.fromisoformat(date_added_str.replace("Z", "+00:00"))
                age_days = (now_utc() - date_added).days
            except:
                pass

        final_score = ai_score - (2.0 * age_days)
        candidates.append((i, final_score, row))

    if not candidates:
        log("  No scored deals to promote")
        return 0

    # Sort by final_score descending
    candidates.sort(key=lambda x: x[1], reverse=True)

    log(f"  Found {len(candidates)} scored candidates, selecting top {max_to_promote}")

    promoted = 0
    for row_idx, final_score, row_data in candidates[:max_to_promote]:
        if "status" in hmap:
            ws.update_cell(row_idx, hmap["status"], STATUS_READY_TO_PUBLISH)
            log(f"  ✓ Promoted row {row_idx} to READY_TO_PUBLISH (score: {final_score:.1f})")
            log(f"     {safe_get(row_data, 'origin_city')}->{safe_get(row_data, 'destination_city')} £{safe_get(row_data, 'price_gbp')}")
            promoted += 1

    return promoted
# =========================
# TELEGRAM - With Marketing Formatting Rules
# =========================
def parse_ts(s: str) -> Optional[dt.datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1]
        return dt.datetime.fromisoformat(s)
    except:
        return None


def tg_send(bot_token: str, channel: str, msg: str, parse_mode: str = "HTML") -> None:
    if not bot_token or not channel:
        raise RuntimeError("Missing Telegram token or channel")

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": channel,
        "text": msg,
        "parse_mode": parse_mode,
        "disable_web_page_preview": False
    }
    r = requests.post(url, json=payload, timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"Telegram error {r.status_code}: {r.text[:300]}")


def format_telegram_vip(row: Dict[str, str]) -> str:
    """
    Format VIP message using MARKETING RULES:
    - City names (not IATA codes)
    - Rounded UP price
    - Dates in YYMMDD format
    """
    # Get raw data
    price_raw = safe_get(row, "price_gbp")
    dest_city = safe_get(row, "destination_city") or safe_get(row, "destination_iata")
    dest_country = safe_get(row, "destination_country")
    origin_city = safe_get(row, "origin_city") or safe_get(row, "origin_iata")
    out_date_raw = safe_get(row, "outbound_date")
    ret_date_raw = safe_get(row, "return_date")
    ai_caption = safe_get(row, "ai_caption") or "Great value for this route"
    booking_link = safe_get(row, "booking_link_vip") or safe_get(row, "affiliate_url")

    # Apply marketing formatting
    price_display = round_price_up(price_raw)
    out_date_display = format_date_yymmdd(out_date_raw)
    ret_date_display = format_date_yymmdd(ret_date_raw)

    dest_display = f"{dest_city}, {dest_country}" if dest_country else dest_city

    message = f"""£{price_display} to {dest_display}

<b>TO:</b> {dest_city.upper()}
<b>FROM:</b> {origin_city}

<b>OUT:</b>  {out_date_display}
<b>BACK:</b> {ret_date_display}

<b>Heads up:</b>
• {ai_caption}
• Availability is running low

"""

    if booking_link:
        message += f'<a href="{booking_link}">BOOK NOW</a>'
    else:
        message += "Search on Skyscanner to book"

    return message


def format_telegram_free(row: Dict[str, str]) -> str:
    """
    Format FREE message using MARKETING RULES:
    - City names (not IATA codes)
    - Rounded UP price
    - Dates in YYMMDD format
    """
    # Get raw data
    price_raw = safe_get(row, "price_gbp")
    dest_city = safe_get(row, "destination_city") or safe_get(row, "destination_iata")
    dest_country = safe_get(row, "destination_country")
    origin_city = safe_get(row, "origin_city") or safe_get(row, "origin_iata")
    out_date_raw = safe_get(row, "outbound_date")
    ret_date_raw = safe_get(row, "return_date")
    booking_link = safe_get(row, "booking_link_free") or safe_get(row, "affiliate_url")

    # Apply marketing formatting
    price_display = round_price_up(price_raw)
    out_date_display = format_date_yymmdd(out_date_raw)
    ret_date_display = format_date_yymmdd(ret_date_raw)

    dest_display = f"{dest_city}, {dest_country}" if dest_country else dest_city

    message = f"""£{price_display} to {dest_display}

<b>TO:</b> {dest_city.upper()}
<b>FROM:</b> {origin_city}

<b>OUT:</b>  {out_date_display}
<b>BACK:</b> {ret_date_display}

<b>Heads up:</b>
• VIP members saw this 24 hours ago
• Availability is running low
• Best deals go to VIPs first

"""

    if booking_link:
        message += f'<a href="{booking_link}">Book now</a>\n\n'
    else:
        message += "\n"

    # Add upsell
    message += "<b>Want instant access?</b>\nJoin TravelTxter Community\n\n"
    message += "• Deals 24 hours early\n"
    message += "• Direct booking links\n"
    message += "• Exclusive mistake fares\n"
    message += "• Cancel anytime\n\n"

    if STRIPE_LINK_MONTHLY and STRIPE_LINK_YEARLY:
        message += f'<a href="{STRIPE_LINK_MONTHLY}">Upgrade Monthly</a> | <a href="{STRIPE_LINK_YEARLY}">Upgrade Yearly</a>'
    elif STRIPE_LINK_MONTHLY:
        message += f'<a href="{STRIPE_LINK_MONTHLY}">Upgrade now</a>'

    return message


def post_telegram_vip(ws: gspread.Worksheet, headers: List[str], max_posts: int = 1) -> int:
    """
    Post to VIP Telegram (AM run only).
    Selects BEST deal from READY_TO_PUBLISH status.
    """
    if RUN_SLOT != "AM":
        log("Telegram VIP: Skipping (PM run)")
        return 0

    if not TELEGRAM_BOT_TOKEN_VIP or not TELEGRAM_CHANNEL_VIP:
        log("Telegram VIP: Missing credentials")
        return 0

    hmap = header_map(headers)
    rows = ws.get_all_values()

    # Find BEST deal with READY_TO_PUBLISH status
    log("Selecting best READY_TO_PUBLISH deal for VIP...")

    candidates: List[Tuple[int, float, Dict[str, str]]] = []

    for i in range(2, len(rows) + 1):
        row_vals = rows[i - 1]
        row = {headers[c]: (row_vals[c] if c < len(row_vals) else "") for c in range(len(headers))}

        status = safe_get(row, "status").upper()
        if status != STATUS_READY_TO_PUBLISH:
            continue

        # Skip if already posted to VIP
        if safe_get(row, "tg_monthly_timestamp") or safe_get(row, "posted_to_vip"):
            continue

        # Get score
        ai_score_str = safe_get(row, "ai_score")
        if not ai_score_str:
            continue

        try:
            ai_score = float(ai_score_str)
        except:
            continue

        candidates.append((i, ai_score, row))

    if not candidates:
        log("  No READY_TO_PUBLISH deals available for VIP")
        return 0

    # Sort by score descending
    candidates.sort(key=lambda x: x[1], reverse=True)

    log(f"  Found {len(candidates)} candidates, posting best {min(max_posts, len(candidates))}")

    posted = 0
    for row_idx, score, row_data in candidates[:max_posts]:
        log(f"  Posting row {row_idx} to VIP (score: {score})")
        log(f"    {safe_get(row_data, 'origin_city')}->{safe_get(row_data, 'destination_city')} £{safe_get(row_data, 'price_gbp')}")

        message = format_telegram_vip(row_data)

        try:
            tg_send(TELEGRAM_BOT_TOKEN_VIP, TELEGRAM_CHANNEL_VIP, message)

            ts = now_utc().replace(microsecond=0).isoformat() + "Z"
            updates = []
            if "tg_monthly_timestamp" in hmap:
                updates.append(gspread.Cell(row_idx, hmap["tg_monthly_timestamp"], ts))
            if "posted_to_vip" in hmap:
                updates.append(gspread.Cell(row_idx, hmap["posted_to_vip"], "TRUE"))
            if "status" in hmap:
                updates.append(gspread.Cell(row_idx, hmap["status"], STATUS_POSTED_TELEGRAM_VIP))

            if updates:
                ws.update_cells(updates, value_input_option="USER_ENTERED")

            posted += 1
            log(f"    ✓ Posted to VIP Telegram successfully")

        except Exception as e:
            log(f"    ✗ Telegram VIP error: {e}")

    return posted


def post_telegram_free(ws: gspread.Worksheet, headers: List[str], max_posts: int = 1) -> int:
    """
    Post to FREE Telegram (PM run only, after 24h delay).
    Enforces VIP_DELAY_HOURS before posting to FREE tier.
    """
    if RUN_SLOT != "PM":
        log("Telegram FREE: Skipping (AM run)")
        return 0

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHANNEL:
        log("Telegram FREE: Missing credentials")
        return 0

    hmap = header_map(headers)
    rows = ws.get_all_values()
    posted = 0

    log("Looking for deals ready for FREE tier (24h after VIP)...")

    for i in range(2, len(rows) + 1):
        if posted >= max_posts:
            break

        row_vals = rows[i - 1]
        row = {headers[c]: (row_vals[c] if c < len(row_vals) else "") for c in range(len(headers))}

        status = safe_get(row, "status").upper()
        if status != STATUS_POSTED_TELEGRAM_VIP:
            continue

        # Skip if already posted to FREE
        if safe_get(row, "tg_free_timestamp") or safe_get(row, "posted_for_free"):
            continue

        # Check telegram_tier - if VIP only, skip
        telegram_tier = safe_get(row, "telegram_tier").lower()
        if telegram_tier == "vip":
            log(f"  Row {i}: VIP-only deal, skipping FREE tier")
            continue

        # Enforce 24h delay
        vip_ts = parse_ts(safe_get(row, "tg_monthly_timestamp"))
        if not vip_ts:
            log(f"  Row {i}: Missing VIP timestamp, skipping")
            continue

        hours_since_vip = (now_utc() - vip_ts).total_seconds() / 3600.0
        if hours_since_vip < VIP_DELAY_HOURS:
            log(f"  Row {i}: VIP posted {hours_since_vip:.1f}h ago, need {VIP_DELAY_HOURS}h")
            continue

        log(f"  Posting row {i} to FREE (delay: {hours_since_vip:.1f}h)")
        log(f"    {safe_get(row, 'origin_city')}->{safe_get(row, 'destination_city')} £{safe_get(row, 'price_gbp')}")

        message = format_telegram_free(row)

        try:
            tg_send(TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL, message)

            ts = now_utc().replace(microsecond=0).isoformat() + "Z"
            updates = []
            if "tg_free_timestamp" in hmap:
                updates.append(gspread.Cell(i, hmap["tg_free_timestamp"], ts))
            if "posted_for_free" in hmap:
                updates.append(gspread.Cell(i, hmap["posted_for_free"], "TRUE"))
            if "status" in hmap:
                updates.append(gspread.Cell(i, hmap["status"], STATUS_POSTED_ALL))

            if updates:
                ws.update_cells(updates, value_input_option="USER_ENTERED")

            posted += 1
            log(f"    ✓ Posted to FREE Telegram successfully")

        except Exception as e:
            log(f"    ✗ Telegram FREE error: {e}")

    return posted


# =========================
# MAIN PIPELINE - Refactored Flow
# =========================
def main() -> None:
    log("=" * 70)
    log("TRAVELTXTER V4.2 REFACTORED - BEST-OF-BATCH ENGINE")
    log("=" * 70)
    log(f"Sheet: {SPREADSHEET_ID[:20]}...")
    log(f"RUN_SLOT: {RUN_SLOT} | VIP_DELAY: {VIP_DELAY_HOURS}h")
    log(f"Duffel: {'ENABLED' if DUFFEL_ENABLED else 'DISABLED'} | Max inserts/route: {DUFFEL_MAX_INSERTS}")
    log("=" * 70)

    try:
        ws, headers = get_ws()
        log(f"Connected to Google Sheets successfully")
        log(f"Columns: {len(headers)}")

        # STAGE 1: Duffel Feeder (Multi-Route Rotation)
        log("\n--- STAGE 1: DUFFEL FEEDER (Multi-Route) ---")
        inserted = run_duffel_feeder(ws, headers)
        log(f"✓ Inserted {inserted} new deals from {DUFFEL_ROUTES_PER_RUN} routes")

        # STAGE 2: Batch Score ALL NEW Deals
        log("\n--- STAGE 2: BATCH AI SCORING ---")
        scored = stage_score_all_new_deals(ws, headers)
        log(f"✓ Scored {scored} NEW deals")

        # STAGE 3: Select BEST for Publishing
        log("\n--- STAGE 3: SELECT BEST FOR PUBLISHING ---")
        promoted = stage_select_best_for_publishing(ws, headers, max_to_promote=1)
        log(f"✓ Promoted {promoted} best deals to READY_TO_PUBLISH")

        # STAGE 4: Telegram VIP (AM run - posts BEST deal)
        log("\n--- STAGE 4: TELEGRAM VIP (AM Only) ---")
        vip_posted = post_telegram_vip(ws, headers, max_posts=1)
        log(f"✓ Posted {vip_posted} to VIP")

        # STAGE 5: Telegram FREE (PM run - 24h delay)
        log("\n--- STAGE 5: TELEGRAM FREE (PM Only, 24h Delay) ---")
        free_posted = post_telegram_free(ws, headers, max_posts=1)
        log(f"✓ Posted {free_posted} to FREE")

        log("\n" + "=" * 70)
        log("PIPELINE COMPLETE")
        log("=" * 70)
        log(f"Summary: {inserted} inserted | {scored} scored | {promoted} selected | {vip_posted} VIP | {free_posted} FREE")

    except Exception as e:
        log(f"FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
