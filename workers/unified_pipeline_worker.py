#!/usr/bin/env python3
"""
TravelTxter V4.1 — UNIFIED PIPELINE WORKER (FIXED)

✅ Themed destinations (variety every day)
✅ Best-deal selection (not oldest first)
✅ UK English spelling
✅ Human-sounding copy (less AI)
✅ Freshness decay prioritisation

Pipeline stages:
1. AI Scoring (NEW → READY_TO_POST) - picks BEST from batch
2. Render (READY_TO_POST → READY_TO_PUBLISH)
3. Instagram (READY_TO_PUBLISH → POSTED_INSTAGRAM)
4. Telegram FREE (POSTED_INSTAGRAM → POSTED_TELEGRAM_FREE)
5. Telegram VIP (POSTED_TELEGRAM_FREE → POSTED_ALL)
"""

import os
import sys
import json
import ssl
import time
import uuid
import datetime as dt
from typing import Dict, Any, List, Optional, Tuple
from urllib.request import Request, urlopen

import gspread
from google.oauth2.service_account import Credentials
import requests


# =========================
# CONFIG-BASED ROUTING (V4.2)
# Reads routes from CONFIG spreadsheet tab
# =========================

def get_enabled_routes_from_config(ws_config) -> List[Dict[str, Any]]:
    """
    Read enabled routes from CONFIG sheet.
    
    Returns list of route configs with:
    - origin_iata, origin_city
    - destination_iata, destination_city, destination_country
    - trip_length_days, max_connections, cabin_class
    - days_ahead, theme
    """
    try:
        rows = ws_config.get_all_values()
        if len(rows) < 2:
            log("  ⚠️  CONFIG sheet has no data")
            return []
        
        headers = [h.strip().lower() for h in rows[0]]
        hmap = {h: i for i, h in enumerate(headers)}
        
        # Required columns
        required = ['enabled', 'origin_iata', 'destination_iata', 'theme']
        if not all(col in hmap for col in required):
            log(f"  ⚠️  CONFIG missing required columns: {required}")
            return []
        
        routes = []
        for row_idx in range(1, len(rows)):
            row = rows[row_idx]
            
            # Check if enabled
            enabled = (row[hmap['enabled']] if hmap['enabled'] < len(row) else "").strip().upper()
            if enabled != "TRUE":
                continue
            
            # Build route config
            route = {
                'origin_iata': (row[hmap['origin_iata']] if hmap['origin_iata'] < len(row) else "").strip().upper(),
                'origin_city': (row[hmap.get('origin_city', 0)] if 'origin_city' in hmap and hmap['origin_city'] < len(row) else "").strip(),
                'destination_iata': (row[hmap['destination_iata']] if hmap['destination_iata'] < len(row) else "").strip().upper(),
                'destination_city': (row[hmap.get('destination_city', 0)] if 'destination_city' in hmap and hmap['destination_city'] < len(row) else "").strip(),
                'destination_country': (row[hmap.get('destination_country', 0)] if 'destination_country' in hmap and hmap['destination_country'] < len(row) else "").strip(),
                'trip_length_days': int((row[hmap.get('trip_length_days', 0)] if 'trip_length_days' in hmap and hmap['trip_length_days'] < len(row) else "5").strip() or "5"),
                'max_connections': int((row[hmap.get('max_connections', 0)] if 'max_connections' in hmap and hmap['max_connections'] < len(row) else "1").strip() or "1"),
                'cabin_class': (row[hmap.get('cabin_class', 0)] if 'cabin_class' in hmap and hmap['cabin_class'] < len(row) else "economy").strip().lower(),
                'days_ahead': int((row[hmap.get('days_ahead', 0)] if 'days_ahead' in hmap and hmap['days_ahead'] < len(row) else "62").strip() or "62"),
                'theme': (row[hmap.get('theme', 0)] if 'theme' in hmap and hmap['theme'] < len(row) else "").strip().upper(),
            }
            
            # Validate required fields
            if route['origin_iata'] and route['destination_iata']:
                routes.append(route)
        
        return routes
        
    except Exception as e:
        log(f"  ⚠️  Error reading CONFIG: {e}")
        return []


def pick_route_from_config() -> Optional[Dict[str, Any]]:
    """
    Pick a route from CONFIG sheet based on deterministic rotation.
    
    Strategy:
    - Read all enabled routes from CONFIG tab
    - Group by theme (optional)
    - Rotate through routes deterministically (day of year + run slot)
    """
    try:
        # Get CONFIG sheet
        gc = gs_client()
        sh = gc.open_by_key(SPREADSHEET_ID)
        
        # Try to get CONFIG sheet
        try:
            ws_config = sh.worksheet("CONFIG")
        except:
            log("  ⚠️  CONFIG sheet not found, using fallback")
            return None
        
        # Get enabled routes
        routes = get_enabled_routes_from_config(ws_config)

        # Fallback: if CONFIG is missing/empty, use env route (keeps the pipeline alive)
        if not routes:
            fallback_origin = env_any(["ORIGIN_IATA"], default=None)
            fallback_dest   = env_any(["DEST_IATA"], default=None)
            if fallback_origin and fallback_dest:
                routes = [{
                    "origin_iata": fallback_origin.strip().upper(),
                    "dest_iata": fallback_dest.strip().upper(),
                    "days_ahead": int(env_any(["DAYS_AHEAD"], default="60")),
                    "trip_length_days": int(env_any(["TRIP_LENGTH_DAYS"], default="5")),
                    "max_inserts": int(env_any(["MAX_INSERTS", "DUFFEL_MAX_INSERTS"], default=str(DUFFEL_MAX_INSERTS))),
                    "theme": env_any(["THEME"], default="fallback"),
                }]
                log(f"⚠️ CONFIG empty — using fallback route: {routes[0]['origin_iata']} -> {routes[0]['dest_iata']}")
        
        if not routes:
            log("  ⚠️  No enabled routes in CONFIG")
            return None
        
        log(f"  📋 Found {len(routes)} enabled routes in CONFIG")
        
        # Deterministic selection based on day of year + run slot
        doy = int(dt.datetime.utcnow().strftime("%j"))
        slot = 0 if dt.datetime.utcnow().hour < 12 else 1
        
        idx = (doy * 2 + slot) % len(routes)
        selected = routes[idx]
        
        log(f"  ✅ Selected route {idx + 1}/{len(routes)}: {selected['origin_iata']} → {selected['destination_iata']}")
        
        return selected
        
    except Exception as e:
        log(f"  ❌ Error picking route from CONFIG: {e}")
        return None


# =========================
# CONFIG
# =========================

def env(key: str, default: str = "") -> str:
    return (os.getenv(key) or default).strip()

def env_any(keys: List[str], default: str = "") -> str:
    for k in keys:
        v = env(k)
        if v:
            return v
    return default

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
    print(f"{ts} | {msg}", flush=True)


SPREADSHEET_ID = env_any(["SPREADSHEET_ID", "SHEET_ID"])
RAW_DEALS_TAB = env_any(["RAW_DEALS_TAB", "DEALS_SHEET_NAME"], "RAW_DEALS")
STATUS_COLUMN = "status"

OPENAI_API_KEY = env("OPENAI_API_KEY")
OPENAI_MODEL = env("OPENAI_MODEL", "gpt-4o-mini")

# Duffel API (optional - for feeding new deals)
DUFFEL_API_KEY = env("DUFFEL_API_KEY")  # Optional
DUFFEL_VERSION = "v2"
DUFFEL_BASE_URL = "https://api.duffel.com/air"

# V4.2: MULTI-AIRPORT DEAL GENERATION
# Strategy: Get ALL offers from each search (not just 1)
# 2 searches/day × 20 offers = 40 offers/day = 1,200/month
# Then AI scorer picks best 10-20% = 120-240 quality deals/month
DUFFEL_ENABLED = bool(DUFFEL_API_KEY)
DUFFEL_MAX_INSERTS = int(env("DUFFEL_MAX_INSERTS", "20"))  # Get all offers
DUFFEL_MAX_INSERTS = min(DUFFEL_MAX_INSERTS, 20)  # Cap at 20 per search

RENDER_URL = env_any(["RENDER_URL", "RENDER_BASE_URL"])

IG_ACCESS_TOKEN = env_any(["IG_ACCESS_TOKEN", "FB_ACCESS_TOKEN"])
IG_USER_ID = env("IG_USER_ID")

# Telegram (Free vs VIP split)
TELEGRAM_FREE_TOKEN = env_any(["TELEGRAM_BOT_TOKEN", "TELEGRAM_FREE_BOT_TOKEN"])
TELEGRAM_VIP_TOKEN  = env_any(["TELEGRAM_BOT_TOKEN_VIP", "TELEGRAM_VIP_BOT_TOKEN"], default=TELEGRAM_FREE_TOKEN)

# Channels (defaults are your locked IDs)
TELEGRAM_FREE_CHANNEL   = env_any(["TELEGRAM_CHANNEL", "TELEGRAM_FREE_CHANNEL"], default="-1003505750272")
TELEGRAM_MONTHLY_CHANNEL = env_any(["TELEGRAM_CHANNEL_VIP", "TELEGRAM_MONTHLY_CHANNEL"], default="-1003517970522")
TELEGRAM_ANNUAL_CHANNEL  = env_any(["TELEGRAM_CHANNEL_VIP", "TELEGRAM_ANNUAL_CHANNEL"], default="-1003517970522")

# Delay (VIP early access window before Free can post)
TELEGRAM_VIP_DELAY_HOURS = int(env_any(["TELEGRAM_VIP_DELAY_HOURS"], default="24"))

# 3-Tier Subscription Model
STRIPE_RAMBLER_FREE = "https://buy.stripe.com/8x2eV60fIfps3Ik3qde7m09"        # £0
STRIPE_ADVENTURER_MONTHLY = "https://buy.stripe.com/3cI14g3rU4KOdiUbWJe7m08"  # £3/month
STRIPE_NOMAD_ANNUAL = "https://buy.stripe.com/9B67sE2nQa586Uw3qde7m07"        # £30/year

# Freshness decay (prioritise newer deals)
FRESHNESS_DECAY_PER_DAY = float(env("FRESHNESS_DECAY_PER_DAY", "2.0"))


# =========================
# THEME LABELS
# =========================

THEME_LABELS = {
    "WINTER_SUN": "Winter Sun",
    "SURF": "Surf Break",
    "SNOW": "Snow",
    "FOODIE": "Foodie Break",
    "CITY_BREAKS": "City Break",
    "LONG_HAUL": "Long-Haul",
    "SURPRISE": "Surprise Deal",
}

def theme_label(theme_key: str) -> str:
    k = (theme_key or "").strip().upper()
    return THEME_LABELS.get(k, "Travel Deal")


# =========================
# GOOGLE SHEETS
# =========================

def gs_client():
    sa_json = env_any(["GCP_SA_JSON", "GCP_SA_JSON_ONE_LINE"])
    if not sa_json:
        raise RuntimeError("Missing GCP_SA_JSON")
    
    info = json.loads(sa_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def get_ws():
    gc = gs_client()
    return gc.open_by_key(SPREADSHEET_ID).worksheet(RAW_DEALS_TAB)


def col_to_a1(n: int) -> str:
    out = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        out = chr(65 + r) + out
    return out


def update_cells(ws, row_num: int, headers: List[str], updates: Dict[str, str]) -> None:
    hmap = {h.strip(): i + 1 for i, h in enumerate(headers) if h.strip()}
    data = []
    for k, v in updates.items():
        if k in hmap:
            col = hmap[k]
            data.append({"range": f"{col_to_a1(col)}{row_num}", "values": [[v]]})
    if data:
        ws.batch_update(data)


# =========================
# DUFFEL FEEDER (Free-tier safe)
# =========================

def duffel_headers() -> Dict[str, str]:
    """Duffel API headers."""
    return {
        "Authorization": f"Bearer {DUFFEL_API_KEY}",
        "Duffel-Version": DUFFEL_VERSION,
        "Content-Type": "application/json",
    }


def feed_new_deals() -> int:
    """
    Feed new deals from Duffel API using CONFIG-based routing.
    
    V4.2 CONFIG STRATEGY:
    - Reads enabled routes from CONFIG spreadsheet tab
    - Rotates through routes deterministically
    - Insert ALL offers (up to 20) from each search
    - Let AI scorer pick best deals later
    
    Returns: Number of deals inserted
    """
    if not DUFFEL_ENABLED:
        log("  No DUFFEL_API_KEY, skipping feeder")
        return 0
    
    try:
        # Pick route from CONFIG
        route = pick_route_from_config()
        
        if not route:
            log("   ⚠️  No route selected from CONFIG")
            return 0
        
        origin_code = route['origin_iata']
        origin_city = route.get('origin_city') or origin_code
        dest_code = route['destination_iata']
        dest_city = route.get('destination_city') or dest_code
        dest_country = route.get('destination_country', '')
        days_ahead = route.get('days_ahead', 62)
        trip_length = route.get('trip_length_days', 5)
        cabin_class = route.get('cabin_class', 'economy')
        max_connections = route.get('max_connections', 1)
        theme = route.get('theme', '')
        
        # Calculate dates
        from datetime import date, timedelta
        out_date = date.today() + timedelta(days=days_ahead)
        ret_date = out_date + timedelta(days=trip_length)
        
        log(f"\n🔍 DUFFEL FEEDER (V4.2 CONFIG-Based)")
        log(f"   Theme: {theme}")
        log(f"   Route: {origin_code} ({origin_city}) → {dest_code} ({dest_city})")
        log(f"   Dates: {out_date} to {ret_date} ({trip_length} days)")
        log(f"   Search window: {days_ahead} days ahead")
        log(f"   Cabin: {cabin_class}, Max stops: {max_connections}")
        log(f"   Max offers: {DUFFEL_MAX_INSERTS}")
        
        # Create offer request
        payload = {
            "data": {
                "slices": [
                    {
                        "origin": origin_code,
                        "destination": dest_code,
                        "departure_date": out_date.isoformat()
                    },
                    {
                        "origin": dest_code,
                        "destination": origin_code,
                        "departure_date": ret_date.isoformat()
                    }
                ],
                "passengers": [{"type": "adult"}],
                "cabin_class": cabin_class,
                "max_connections": max_connections,
            }
        }
        
        # Request offers
        r1 = requests.post(
            f"{DUFFEL_BASE_URL}/offer_requests",
            headers=duffel_headers(),
            json=payload,
            timeout=30
        )
        r1.raise_for_status()
        offer_request_id = r1.json()["data"]["id"]
        
        # Get offers
        r2 = requests.get(
            f"{DUFFEL_BASE_URL}/offers?offer_request_id={offer_request_id}&limit=50",
            headers=duffel_headers(),
            timeout=30
        )
        r2.raise_for_status()
        offers = r2.json()["data"]
        
        if not offers:
            log("   ⚠️  No offers returned by Duffel")
            return 0
        
        log(f"   📦 Duffel returned {len(offers)} offers")
        
        # Get worksheet
        ws = get_ws()
        headers = ws.row_values(1)
        if not headers:
            log("   ❌ Sheet has no headers")
            return 0
        
        hmap = {h.strip(): i for i, h in enumerate(headers)}
        
        # Parse offers and insert (up to DUFFEL_MAX_INSERTS)
        inserted = 0
        rows_to_insert = []
        
        for offer in offers:
            if inserted >= DUFFEL_MAX_INSERTS:
                break
            
            try:
                slices = offer.get("slices", [])
                if len(slices) < 2:
                    continue
                
                # Parse flight details
                seg0 = slices[0].get("segments", [])
                seg1 = slices[1].get("segments", [])
                if not seg0 or not seg1:
                    continue
                
                out = seg0[0]["departing_at"][:10]
                ret = seg1[0]["departing_at"][:10]
                
                stops = (len(seg0) - 1) + (len(seg1) - 1)
                airline = (offer.get("owner") or {}).get("name", "")
                price = offer.get("total_amount", "")
                
                # Build deal record
                deal = {
                    "deal_id": str(uuid.uuid4()),
                    "origin_city": origin_city,
                    "destination_city": dest_city,
                    "destination_country": dest_country,
                    "price_gbp": price,
                    "outbound_date": out,
                    "return_date": ret,
                    "trip_length_days": str(trip_length),
                    "stops": str(stops),
                    "airline": airline,
                    "theme": theme,
                    "deal_source": f"DUFFEL_V4.2_CONFIG_{origin_code}",
                    "date_added": date.today().isoformat(),
                    "status": "NEW",
                }
                
                # Build row (only write columns that exist)
                row = [""] * len(headers)
                for key, val in deal.items():
                    if key in hmap:
                        row[hmap[key]] = str(val) if val else ""
                
                rows_to_insert.append(row)
                inserted += 1
                
            except Exception as e:
                log(f"   ⚠️  Failed to parse offer: {e}")
                continue
        
        # Insert rows
        if rows_to_insert:
            ws.append_rows(rows_to_insert, value_input_option="USER_ENTERED")
            log(f"   ✅ Inserted {len(rows_to_insert)} offer(s)")
            log(f"   📊 AI Scorer will pick best deals from this batch")
            return len(rows_to_insert)
        else:
            log("   ⚠️  No valid offers to insert")
            return 0
            
    except Exception as e:
        log(f"   ❌ Duffel feeder error: {e}")
        import traceback
        log(f"   Traceback: {traceback.format_exc()}")
        return 0


# =========================
# FRESHNESS DECAY
# =========================

def age_days(rec: Dict[str, str]) -> int:
    """Calculate days since deal was added."""
    date_added = rec.get("date_added", "").strip()
    if not date_added:
        return 0
    try:
        added = dt.date.fromisoformat(date_added[:10])
        return max(0, (dt.date.today() - added).days)
    except:
        return 0


def effective_score(raw_score: float, age: int) -> float:
    """Apply freshness decay to prioritise newer deals."""
    return raw_score - (FRESHNESS_DECAY_PER_DAY * float(age))


# =========================
# AI SCORING (UK English, Human tone)
# =========================

def score_deal(rec: Dict[str, str]) -> Dict[str, str]:
    """Score deal using OpenAI or fallback heuristic."""
    if not OPENAI_API_KEY:
        return score_heuristic(rec)
    
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
        
        theme = rec.get("theme", "").strip()
        origin = rec.get("origin_city", "London")
        dest = rec.get("destination_city", "")
        country = rec.get("destination_country", "")
        price = rec.get("price_gbp", "")
        
        system_prompt = """You're a UK travel editor. Write like a real person texting their mate, not a corporate robot.
Use British English. Be conversational and honest. Make people actually want to book.
Instagram = marketing (inspire them → send to Telegram). No hard selling, just good vibes."""

        user_prompt = f"""Score this honestly. Would YOU book it?

Deal:
- Theme: {theme_label(theme) if theme else 'General'}
- Route: {origin} → {dest} ({country})
- Price: £{price}
- Dates: {rec.get('outbound_date')} to {rec.get('return_date')}
- Trip: {rec.get('trip_length_days')} days
- Stops: {rec.get('stops')}

Return STRICT JSON:
{{
  "score": 1-10,
  "verdict": "GOOD" or "AVERAGE" or "POOR",
  "grading": "A" or "B" or "C",
  "caption": "Instagram caption (mate-to-mate tone, British, benefit-led, CTA to Telegram, max 180 chars)"
}}

Caption must sound like a real person wrote it. Lead with feeling, not features. No corporate waffle."""

        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.7,
            max_tokens=300,
            response_format={"type": "json_object"}
        )
        
        result = json.loads(response.choices[0].message.content)
        
        return {
            "ai_score": str(result.get("score", "5")),
            "ai_verdict": result.get("verdict", "AVERAGE").upper(),
            "ai_grading": result.get("grading", "B").upper(),
            "ai_caption": result.get("caption", "")[:700],
        }
        
    except Exception as e:
        log(f"  OpenAI failed: {e}, using heuristic")
        return score_heuristic(rec)


def score_heuristic(rec: Dict[str, str]) -> Dict[str, str]:
    """Fallback heuristic with human captions (British English)."""
    try:
        price = float(rec.get("price_gbp", "999").replace("£", ""))
        stops = int(rec.get("stops", "0"))
    except:
        return {"ai_score": "5", "ai_verdict": "AVERAGE", "ai_grading": "B", "ai_caption": ""}
    
    score = 5
    if price <= 60:
        score = 9
    elif price <= 100:
        score = 7
    elif price <= 150:
        score = 6
    
    if stops == 0:
        score = min(10, score + 1)
    
    verdict = "GOOD" if score >= 7 else "AVERAGE" if score >= 5 else "POOR"
    grading = "A" if score >= 8 else "B" if score >= 6 else "C"
    
    # Human captions (sound like a real person, British English)
    dest = rec.get("destination_city", "")
    price_str = rec.get("price_gbp", "")
    theme = theme_label(rec.get("theme", ""))
    
    # Mate-to-mate tone templates
    templates = [
        f"£{price_str} to {dest}. {theme} sorted. Check Telegram.",
        f"{dest} for £{price_str}. Not bad. Details on Telegram.",
        f"Found one: £{price_str} flights to {dest}. Link in bio.",
        f"{theme} — {dest} for £{price_str}. Full story on Telegram.",
        f"£{price_str} to {dest}. Because why not? Telegram's got details.",
    ]
    
    import random
    random.seed(rec.get("deal_id", ""))
    caption = random.choice(templates)
    
    return {
        "ai_score": str(score),
        "ai_verdict": verdict,
        "ai_grading": grading,
        "ai_caption": caption[:700],
    }


# =========================
# RENDER
# =========================

def render_deal(rec: Dict[str, str]) -> Optional[str]:
    """Call render service."""
    if not RENDER_URL:
        log("  No RENDER_URL, skipping")
        return None
    
    try:
        payload = {
            "deal_id": rec.get("deal_id", ""),
            "origin_city": rec.get("origin_city", ""),
            "destination_city": rec.get("destination_city", ""),
            "destination_country": rec.get("destination_country", ""),
            "price_gbp": rec.get("price_gbp", ""),
            "outbound_date": rec.get("outbound_date", ""),
            "return_date": rec.get("return_date", ""),
            "ai_grading": rec.get("ai_grading", "B"),
            "theme": rec.get("theme", ""),
        }
        
        body = json.dumps(payload).encode("utf-8")
        req = Request(RENDER_URL, data=body, headers={"Content-Type": "application/json"}, method="POST")
        ctx = ssl.create_default_context()
        
        with urlopen(req, timeout=30, context=ctx) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data.get("graphic_url") or data.get("image_url")
            
    except Exception as e:
        log(f"  Render failed: {e}")
        return None


# =========================
# INSTAGRAM
# =========================

def post_instagram(graphic_url: str, rec: Dict[str, str]) -> Optional[str]:
    """Post to Instagram."""
    if not IG_ACCESS_TOKEN or not IG_USER_ID:
        log("  No Instagram credentials, skipping")
        return None
    
    try:
        caption = rec.get("ai_caption", "").strip()
        
        if not caption:
            dest = rec.get("destination_city", "")
            price = rec.get("price_gbp", "")
            theme = theme_label(rec.get("theme", ""))
            
            caption = f"{theme} — £{price} to {dest}. Telegram's got details."
        
        # Ensure hashtags
        if "#TravelTxter" not in caption:
            caption += "\n\n#TravelTxter #CheapFlights #TravelDeals"
        
        # Create container
        r1 = requests.post(
            f"https://graph.facebook.com/v19.0/{IG_USER_ID}/media",
            data={
                "image_url": graphic_url,
                "caption": caption,
                "access_token": IG_ACCESS_TOKEN,
            },
            timeout=30
        )
        
        if r1.status_code != 200:
            log(f"  Instagram container failed: {r1.text[:200]}")
            return None
        
        container_id = r1.json().get("id")
        
        # Publish
        r2 = requests.post(
            f"https://graph.facebook.com/v19.0/{IG_USER_ID}/media_publish",
            data={
                "creation_id": container_id,
                "access_token": IG_ACCESS_TOKEN,
            },
            timeout=30
        )
        
        if r2.status_code != 200:
            log(f"  Instagram publish failed: {r2.text[:200]}")
            return None
        
        return r2.json().get("id", "unknown")
        
    except Exception as e:
        log(f"  Instagram error: {e}")
        return None


# =========================
# TELEGRAM (UK English, human tone)
# =========================

def _send_telegram(channel: str, rec: Dict[str, str], tier: str = "free") -> Optional[str]:
    """Post to Telegram with 3-tier subscription model (British English, mate-to-mate tone).
    
    Args:
        tier: "free", "monthly", or "annual"
    """
    token = TELEGRAM_FREE_TOKEN if tier == "free" else TELEGRAM_VIP_TOKEN
    if not token:
        log("  No Telegram token, skipping")
        return None
    
    try:
        origin = rec.get("origin_city", "London")
        dest = rec.get("destination_city", "")
        country = rec.get("destination_country", "")
        price = rec.get("price_gbp", "")
        out_date = rec.get("outbound_date", "")
        ret_date = rec.get("return_date", "")
        verdict = rec.get("ai_verdict", "")
        theme = rec.get("theme", "")
        graphic_url = rec.get("graphic_url", "")
        
        if tier == "free":
            # FREE tier (promote monthly AND annual)
            msg = f"<b>£{price} to {dest}</b>\n\n"
            msg += f"TO: {dest.upper()}\n"
            msg += f"FROM: {origin}\n\n"
            if out_date and ret_date:
                msg += f"OUT:  {out_date}\n"
                msg += f"BACK: {ret_date}\n"
            msg += "\nHeads up:\n"
            msg += "• Paid members saw this 24 hours ago\n"
            msg += "• Availability's running low\n"
            msg += "• Best deals go to subscribers first\n\n"
            msg += "<b>Want instant access?</b>\n\n"
            msg += "<b>Adventurer (Monthly)</b> — £3/month\n"
            msg += "• Deals 24 hours early\n"
            msg += "• Direct booking links\n"
            msg += "• Cancel anytime\n"
            msg += f'<a href="{STRIPE_ADVENTURER_MONTHLY}">Subscribe monthly</a>\n\n'
            msg += "<b>Nomad (Annual)</b> — £30/year\n"
            msg += "• Everything in Monthly\n"
            msg += "• Save £6 per year\n"
            msg += "• Exclusive mistake fares\n"
            msg += f'<a href="{STRIPE_NOMAD_ANNUAL}">Subscribe annually</a>'
            
        elif tier == "monthly":
            # MONTHLY tier (Adventurer - £3/month)
            msg = f"<b>ADVENTURER EARLY ACCESS</b>\n\n"
            msg += f"£{price} to {dest}"
            if country:
                msg += f", {country}"
            if theme:
                msg += f" ({theme_label(theme)})"
            msg += f"\n\n"
            msg += f"TO: {dest.upper()}\n"
            msg += f"FROM: {origin}\n\n"
            if out_date and ret_date:
                msg += f"OUT:  {out_date}\n"
                msg += f"BACK: {ret_date}\n"
            if verdict:
                msg += f"\nVERDICT: {verdict}\n"
            msg += "\nYou're seeing this 24 hours before free members."
            
        else:  # annual
            # ANNUAL tier (Nomad - £30/year)
            msg = f"<b>NOMAD EARLY ACCESS</b>\n\n"
            msg += f"£{price} to {dest}"
            if country:
                msg += f", {country}"
            if theme:
                msg += f" ({theme_label(theme)})"
            msg += f"\n\n"
            msg += f"TO: {dest.upper()}\n"
            msg += f"FROM: {origin}\n\n"
            if out_date and ret_date:
                msg += f"OUT:  {out_date}\n"
                msg += f"BACK: {ret_date}\n"
            if verdict:
                msg += f"\nVERDICT: {verdict}\n"
            msg += "\nYou're seeing this 24 hours before free members."
        
        # Send with photo if available
        if graphic_url:
            url = f"https://api.telegram.org/bot{token}/sendPhoto"
            payload = {
                "chat_id": channel,
                "photo": graphic_url,
                "caption": msg,
                "parse_mode": "HTML",
            }
        else:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            payload = {
                "chat_id": channel,
                "text": msg,
                "parse_mode": "HTML",
                "disable_web_page_preview": False,
            }
        
        response = requests.post(url, json=payload, timeout=30)
        
        if response.status_code == 200 and response.json().get("ok"):
            return str(response.json().get("result", {}).get("message_id", "unknown"))
        else:
            log(f"  Telegram error: {response.text[:200]}")
            return None
            
    except Exception as e:
        log(f"  Telegram error: {e}")
        return None


# =========================
# MAIN PIPELINE
# =========================


def post_telegram(ws, headers, row_idx: int, row: Dict[str, str], tier: str = "free") -> bool:
    """Sheet-aware wrapper used by main(): sends Telegram message for the correct tier."""
    if tier == "free":
        channel = TELEGRAM_FREE_CHANNEL
    elif tier in ("vip", "monthly"):
        channel = TELEGRAM_MONTHLY_CHANNEL
    elif tier == "annual":
        channel = TELEGRAM_ANNUAL_CHANNEL
    else:
        channel = TELEGRAM_FREE_CHANNEL

    msg_id = _send_telegram(channel=channel, rec=row, tier=tier)
    if not msg_id:
        return False

    # Best-effort writebacks (ignored if columns don't exist)
    stamp = now_iso()
    update_cells(ws, headers, row_idx, {
        "telegram_message_id": msg_id,
        "telegram_tier": tier,
        "telegram_timestamp": stamp,
    })
    return True

def main() -> int:
    log("=" * 60)
    log("🚀 TRAVELTXTER V4.1 UNIFIED PIPELINE")
    log("=" * 60)
    log(f"Sheet: {SPREADSHEET_ID}")
    log(f"Tab: {RAW_DEALS_TAB}")
    log(f"Duffel: {'ENABLED' if DUFFEL_ENABLED else 'DISABLED'}")
    log(f"Freshness decay: {FRESHNESS_DECAY_PER_DAY}/day")
    log("=" * 60)
    
    if not SPREADSHEET_ID:
        log("❌ Missing SPREADSHEET_ID")
        return 1
    
    # ============================================================
    # STAGE 0: DUFFEL FEEDER (Optional - only if API key set)
    # ============================================================
    
    if DUFFEL_ENABLED:
        feed_new_deals()  # Inserts max 1 deal (free-tier safe)
    
    # ============================================================
    # STAGE 1: SCORE & SELECT BEST NEW DEAL
    # ============================================================
    
    ws = get_ws()
    log(f"✅ Connected: {ws.title}")
    
    rows = ws.get_all_values()
    if len(rows) < 2:
        log("No data rows")
        return 0
    
    headers = [h.strip() for h in rows[0]]
    hmap = {h: i for i, h in enumerate(headers)}
    
    if STATUS_COLUMN not in hmap:
        log(f"❌ Column '{STATUS_COLUMN}' not found")
        return 1
    
    status_idx = hmap[STATUS_COLUMN]
    
    # ============================================================
    # STAGE 1: SCORE & SELECT BEST NEW DEAL
    # ============================================================
    
    new_candidates: List[Tuple[int, Dict[str, str], float]] = []
    
    for row_idx in range(1, len(rows)):
        row = rows[row_idx]
        row_num = row_idx + 1
        
        if status_idx >= len(row):
            continue
        
        current_status = row[status_idx].strip().upper()
        rec = {h: (row[hmap[h]] if hmap[h] < len(row) else "") for h in headers}
        
        if current_status == "NEW":
            raw_score = float(rec.get("ai_score", "0") or "0")
            
            # Score if not scored yet
            if raw_score == 0:
                deal_id = rec.get("deal_id", "")
                dest = rec.get("destination_city", "")
                theme = rec.get("theme", "")
                log(f"\n📊 Scoring NEW deal row {row_num} ({dest}, {theme})...")
                
                score_result = score_deal(rec)
                
                updates = {
                    "ai_score": score_result["ai_score"],
                    "ai_verdict": score_result["ai_verdict"],
                    "ai_grading": score_result["ai_grading"],
                    "ai_caption": score_result.get("ai_caption", ""),
                    "scored_timestamp": dt.datetime.utcnow().isoformat() + "Z",
                }
                
                update_cells(ws, row_num, headers, updates)
                log(f"   Scored: {score_result['ai_score']}/10 ({score_result['ai_verdict']})")
                
                rec.update(updates)
                raw_score = float(score_result["ai_score"])
            
            # Apply freshness decay
            age = age_days(rec)
            eff_score = effective_score(raw_score, age)
            
            new_candidates.append((row_num, rec, eff_score))
            log(f"   Row {row_num}: score={raw_score}, age={age}d, effective={eff_score:.1f}")
    
    # Promote BEST deal
    if new_candidates:
        new_candidates.sort(key=lambda x: x[2], reverse=True)
        best_row, best_rec, best_eff = new_candidates[0]
        
        log(f"\n✅ BEST DEAL: Row {best_row} (effective={best_eff:.1f})")
        log(f"   Destination: {best_rec.get('destination_city', '')}")
        log(f"   Theme: {best_rec.get('theme', '')}")
        log(f"   Promoting to READY_TO_POST...")
        
        update_cells(ws, best_row, headers, {STATUS_COLUMN: "READY_TO_POST"})
        return 0
    
    # ============================================================
    # OTHER STAGES
    # ============================================================
    
    for row_idx in range(1, len(rows)):
        row = rows[row_idx]
        row_num = row_idx + 1
        
        if status_idx >= len(row):
            continue
        
        current_status = row[status_idx].strip().upper()
        rec = {h: (row[hmap[h]] if hmap[h] < len(row) else "") for h in headers}
        deal_id = rec.get("deal_id", "")
        
        if current_status == "READY_TO_POST":
            log(f"\n🎨 Rendering row {row_num}...")
            graphic_url = render_deal(rec)
            
            if graphic_url:
                update_cells(ws, row_num, headers, {
                    STATUS_COLUMN: "READY_TO_PUBLISH",
                    "graphic_url": graphic_url,
                    "rendered_timestamp": dt.datetime.utcnow().isoformat() + "Z",
                })
                log(f"✅ Rendered")
            
            return 0
        
        elif current_status == "READY_TO_PUBLISH":
            log(f"\n📸 Instagram posting row {row_num}...")
            graphic_url = rec.get("graphic_url", "")
            
            if graphic_url:
                media_id = post_instagram(graphic_url, rec)
                if media_id:
                    update_cells(ws, row_num, headers, {
                        STATUS_COLUMN: "POSTED_INSTAGRAM",
                        "ig_media_id": media_id,
                        "ig_published_timestamp": dt.datetime.utcnow().isoformat() + "Z",
                    })
                    log(f"✅ Posted to Instagram")
            
            return 0
        
        elif current_status == "POSTED_INSTAGRAM":
        # VIP FIRST (Monthly/Annual both map to VIP channel)
        ok = post_telegram(ws, headers, row_idx, row, tier="monthly")
        if not ok:
            update_cells(ws, headers, row_idx, {"status": "ERROR_HARD", "ai_notes": "Telegram VIP (monthly) post failed"})
            return 1

        vip_ts = now_iso()
        update_cells(ws, headers, row_idx, {
            "status": "POSTED_TELEGRAM_VIP",
            "vip_published_timestamp": vip_ts,
            "published_timestamp": vip_ts,  # used for delay gating if vip_published_timestamp column doesn't exist
        })
        log(f"✅ Telegram VIP posted. Free will release after {TELEGRAM_VIP_DELAY_HOURS}h.")
        return 0

    elif current_status == "POSTED_TELEGRAM_VIP":
        # Enforce VIP delay window before Free release
        vip_ts_raw = row.get("vip_published_timestamp") or row.get("published_timestamp") or ""
        vip_dt = parse_iso(vip_ts_raw)
        if not vip_dt:
            # Safety: if timestamp missing, treat as "just posted" and hold Free
            log("⚠️ Missing VIP timestamp — holding Free release until next run.")
            return 0

        hours = (utcnow() - vip_dt).total_seconds() / 3600.0
        if hours < TELEGRAM_VIP_DELAY_HOURS:
            remaining = TELEGRAM_VIP_DELAY_HOURS - hours
            log(f"⏳ VIP window active ({hours:.1f}h elapsed). Free releases in ~{remaining:.1f}h.")
            return 0

        ok = post_telegram(ws, headers, row_idx, row, tier="free")
        if not ok:
            update_cells(ws, headers, row_idx, {"status": "ERROR_HARD", "ai_notes": "Telegram Free post failed"})
            return 1

        free_ts = now_iso()
        update_cells(ws, headers, row_idx, {
            "status": "POSTED_ALL",
            "free_published_timestamp": free_ts,
            "published_timestamp": free_ts,
        })
        log("✅ Telegram Free posted. Deal lifecycle complete (POSTED_ALL).")
        return 0
    
    log("\nNo deals to process")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        log(f"❌ FATAL: {e}")
        import traceback
        log(traceback.format_exc())
        sys.exit(1)
