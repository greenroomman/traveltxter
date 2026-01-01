#!/usr/bin/env python3
"""
TravelTxter V4.2 - PRODUCTION FINAL (100% Working)

Combines:
- Refactored Best-of-Batch engine
- Instagram status polling + cache-busting  
- Correct column names from spreadsheet
- Marketing formatting (city names, rounded prices, YYMMDD)
- Duffel multi-route rotation
- VIP early access with 24h delay
"""

import os
import json
import uuid
import datetime as dt
import math
import time
from typing import Dict, Any, List, Tuple, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials
from openai import OpenAI


# =========================
# ENV
# =========================
def env(name: str, default: str = "", required: bool = False) -> str:
    v = (os.getenv(name) or "").strip()
    if not v:
        v = default
    if required and not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


SPREADSHEET_ID = env("SPREADSHEET_ID", required=True)
RAW_DEALS_TAB = env("RAW_DEALS_TAB", "RAW_DEALS")
CONFIG_TAB = env("CONFIG_TAB", "CONFIG")

GCP_SA_JSON = env("GCP_SA_JSON", required=True)

DUFFEL_API_KEY = env("DUFFEL_API_KEY", "")
DUFFEL_VERSION = env("DUFFEL_VERSION", "v2")
DUFFEL_MAX_INSERTS = int(env("DUFFEL_MAX_INSERTS", "20"))
DUFFEL_ROUTES_PER_RUN = int(env("DUFFEL_ROUTES_PER_RUN", "2"))
DUFFEL_ENABLED = env("DUFFEL_ENABLED", "true").lower() in ("1", "true", "yes")

OPENAI_API_KEY = env("OPENAI_API_KEY", "")
OPENAI_MODEL = env("OPENAI_MODEL", "gpt-4o-mini")

RENDER_URL = env("RENDER_URL", required=True)

IG_ACCESS_TOKEN = env("IG_ACCESS_TOKEN", required=True)
IG_USER_ID = env("IG_USER_ID", required=True)

TELEGRAM_BOT_TOKEN_VIP = env("TELEGRAM_BOT_TOKEN_VIP", required=True)
TELEGRAM_VIP_CHANNEL = env("TELEGRAM_VIP_CHANNEL", required=True)

TELEGRAM_BOT_TOKEN = env("TELEGRAM_BOT_TOKEN", required=True)
TELEGRAM_FREE_CHANNEL = env("TELEGRAM_FREE_CHANNEL", required=True)

SKYSCANNER_AFFILIATE_ID = env("SKYSCANNER_AFFILIATE_ID", "")
STRIPE_LINK_MONTHLY = env("STRIPE_LINK_MONTHLY", "")
STRIPE_LINK_YEARLY = env("STRIPE_LINK_YEARLY", "")

VIP_DELAY_HOURS = int(env("VIP_DELAY_HOURS", "24"))
RUN_SLOT = env("RUN_SLOT", "AM").upper()


# =========================
# Status constants (match spreadsheet)
# =========================
STATUS_NEW = "NEW"
STATUS_READY_TO_POST = "READY_TO_POST"
STATUS_READY_TO_PUBLISH = "READY_TO_PUBLISH"
STATUS_POSTED_INSTAGRAM = "POSTED_INSTAGRAM"
STATUS_POSTED_TELEGRAM_VIP = "POSTED_TELEGRAM_VIP"
STATUS_POSTED_ALL = "POSTED_ALL"


# =========================
# Helpers
# =========================
def now_utc() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def log(msg: str) -> None:
    print(f"{now_utc()} | {msg}", flush=True)


def safe_get(row: Dict[str, str], key: str) -> str:
    return (row.get(key) or "").strip()


def round_price_up(price_str: str) -> int:
    """Round price UP to nearest whole pound"""
    try:
        return math.ceil(float(price_str))
    except:
        return 0


def format_date_yymmdd(date_str: str) -> str:
    """Convert YYYY-MM-DD to YYMMDD"""
    try:
        if not date_str:
            return ""
        date_str = date_str.strip()
        
        # Handle MM/DD/YY format from spreadsheet
        if '/' in date_str:
            parts = date_str.split('/')
            if len(parts) == 3:
                mm, dd, yy = parts
                return f"{yy.zfill(2)}{mm.zfill(2)}{dd.zfill(2)}"
        
        # Handle YYYY-MM-DD format
        if '-' in date_str:
            parts = date_str.split('-')
            if len(parts) == 3:
                yy = parts[0][2:]
                mm = parts[1].zfill(2)
                dd = parts[2].zfill(2)
                return f"{yy}{mm}{dd}"
        
        return ""
    except:
        return ""


def hours_since(ts: str) -> float:
    """Calculate hours since timestamp"""
    if not ts:
        return 9999.0
    try:
        t = dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return (dt.datetime.now(dt.timezone.utc) - t).total_seconds() / 3600.0
    except:
        return 9999.0


# =========================
# Google Sheets
# =========================
def get_ws() -> Tuple[gspread.Worksheet, List[str]]:
    creds = Credentials.from_service_account_info(
        json.loads(GCP_SA_JSON),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    client = gspread.authorize(creds)
    sh = client.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(RAW_DEALS_TAB)
    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("No headers found")
    return ws, headers


def header_map(headers: List[str]) -> Dict[str, int]:
    return {h.strip(): i + 1 for i, h in enumerate(headers) if h.strip()}


# =========================
# CONFIG Routes (if Duffel enabled)
# =========================
def load_routes_from_config(ws_parent: gspread.Spreadsheet) -> List[Tuple[int, str, str, str, str, int]]:
    """Load enabled routes from CONFIG tab"""
    try:
        cfg = ws_parent.worksheet(CONFIG_TAB)
    except:
        return []

    values = cfg.get_all_values()
    if not values or len(values) < 2:
        return []

    headers = [h.strip() for h in values[0]]
    idx = {h: i for i, h in enumerate(headers) if h}

    routes: List[Tuple[int, str, str, str, str, int]] = []
    
    for r in values[1:]:
        enabled = ""
        if "enabled" in idx and idx["enabled"] < len(r):
            enabled = (r[idx["enabled"]] or "").strip().upper()
        if enabled not in ("TRUE", "1", "YES"):
            continue

        priority = 1
        if "priority" in idx and idx["priority"] < len(r):
            try:
                priority = int(r[idx["priority"]] or "1")
            except:
                priority = 1

        origin_iata = (r[idx["origin_iata"]] if "origin_iata" in idx and idx["origin_iata"] < len(r) else "").strip().upper()
        origin_city = (r[idx["origin_city"]] if "origin_city" in idx and idx["origin_city"] < len(r) else "").strip()
        dest_iata = (r[idx["destination_iata"]] if "destination_iata" in idx and idx["destination_iata"] < len(r) else "").strip().upper()
        dest_city = (r[idx["destination_city"]] if "destination_city" in idx and idx["destination_city"] < len(r) else "").strip()
        
        days_ahead = 60
        if "days_ahead" in idx and idx["days_ahead"] < len(r):
            try:
                days_ahead = int(r[idx["days_ahead"]] or "60")
            except:
                pass

        if origin_iata and dest_iata:
            routes.append((priority, origin_iata, origin_city, dest_iata, dest_city, days_ahead))

    routes.sort(key=lambda x: x[0])
    return routes


def select_routes_rotating(routes: List[Tuple[int, str, str, str, str, int]], max_routes: int) -> List[Tuple[int, str, str, str, str, int]]:
    """Select routes deterministically"""
    if not routes or len(routes) <= max_routes:
        return routes
    
    day_of_year = dt.date.today().timetuple().tm_yday
    slot_offset = 0 if RUN_SLOT == "AM" else 1
    start_idx = ((day_of_year * 2) + slot_offset) % len(routes)
    
    selected = []
    for i in range(max_routes):
        idx = (start_idx + i) % len(routes)
        selected.append(routes[idx])
    
    return selected


# =========================
# Duffel (if enabled)
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
    """Feed new deals from Duffel (if enabled)"""
    if not DUFFEL_ENABLED or not DUFFEL_API_KEY:
        log("Duffel: DISABLED or no API key")
        return 0

    sh = ws.spreadsheet
    routes = load_routes_from_config(sh)
    if not routes:
        log("No enabled routes in CONFIG")
        return 0

    selected_routes = select_routes_rotating(routes, DUFFEL_ROUTES_PER_RUN)
    log(f"Duffel: Searching {len(selected_routes)} routes")

    hmap = header_map(headers)
    total_inserted = 0

    for priority, origin_iata, origin_city, dest_iata, dest_city, days_ahead in selected_routes:
        today = dt.date.today()
        out_date = today + dt.timedelta(days=days_ahead)
        ret_date = out_date + dt.timedelta(days=5)  # Default 5 days

        log(f"  {origin_city}->{dest_city} | Out: {out_date}")
        
        try:
            data = duffel_offer_request(origin_iata, dest_iata, str(out_date), str(ret_date))
        except Exception as e:
            log(f"  Duffel error: {e}")
            continue

        offers = (data.get("data") or {}).get("offers") or []
        if not offers:
            log(f"  0 offers")
            continue

        rows_to_append = []
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

            dest_country = ""
            try:
                if slices:
                    d = (slices[0].get("destination") or {})
                    dest_country = (d.get("country_name") or "").strip()
            except:
                pass

            deal_id = str(uuid.uuid4())

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
            row_obj["trip_length_days"] = "5"
            row_obj["deal_source"] = "DUFFEL"
            row_obj["date_added"] = now_utc()
            row_obj["airline"] = airline
            row_obj["stops"] = stops
            row_obj["status"] = STATUS_NEW

            # Generate Skyscanner link
            out_formatted = str(out_date).replace('-', '')
            ret_formatted = str(ret_date).replace('-', '')
            base_url = f"https://www.skyscanner.net/transport/flights/{origin_iata}/{dest_iata}/{out_formatted}/{ret_formatted}/"
            if SKYSCANNER_AFFILIATE_ID:
                booking_link = f"{base_url}?affiliateid={SKYSCANNER_AFFILIATE_ID}"
            else:
                booking_link = base_url
            
            row_obj["booking_link_vip"] = booking_link
            row_obj["booking_link_free"] = booking_link
            row_obj["affiliate_url"] = booking_link

            rows_to_append.append([row_obj.get(h, "") for h in headers])
            inserted_for_route += 1

        if rows_to_append:
            ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
            log(f"  ✓ Inserted {len(rows_to_append)} deals")
            total_inserted += len(rows_to_append)

    return total_inserted


# =========================
# AI Scoring (if OpenAI enabled)
# =========================
def score_deal_with_ai(row: Dict[str, str]) -> Dict[str, Any]:
    """Score deal with OpenAI"""
    if not OPENAI_API_KEY:
        return {
            "ai_score": 60,
            "ai_verdict": "AVERAGE",
            "ai_caption": f"Return flights to {safe_get(row, 'destination_city')}",
            "is_instagram_eligible": False,
            "telegram_priority": "Low"
        }

    client = OpenAI(api_key=OPENAI_API_KEY)

    origin_city = safe_get(row, "origin_city") or safe_get(row, "origin_iata")
    dest_city = safe_get(row, "destination_city") or safe_get(row, "destination_iata")
    price = safe_get(row, "price_gbp")

    prompt = f"""
Score this UK flight deal (0-100):
- {origin_city} to {dest_city}
- £{price}

Return JSON:
{{"ai_score": 0-100, "ai_verdict": "EXCELLENT/GOOD/AVERAGE/POOR", "ai_caption": "1 sentence why it's good (max 100 chars, NO EMOJIS)"}}
""".strip()

    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": "Return only valid JSON. Never use emojis."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=200
        )

        text_out = resp.choices[0].message.content.strip()
        text_out = text_out.replace("```json", "").replace("```", "").strip()
        data = json.loads(text_out)

        ai_score = int(data.get("ai_score", 60))
        return {
            "ai_score": ai_score,
            "ai_verdict": str(data.get("ai_verdict", "AVERAGE")).upper(),
            "ai_caption": str(data.get("ai_caption", "")).strip() or f"Flights to {dest_city}",
            "is_instagram_eligible": ai_score >= 90,
            "telegram_priority": "High" if ai_score >= 85 else ("Medium" if ai_score >= 70 else "Low")
        }
    except Exception as e:
        log(f"OpenAI error: {e}")
        return {
            "ai_score": 60,
            "ai_verdict": "AVERAGE",
            "ai_caption": f"Flights to {dest_city} for £{price}",
            "is_instagram_eligible": False,
            "telegram_priority": "Low"
        }


def stage_score_all_new(ws: gspread.Worksheet, headers: List[str]) -> int:
    """Score all NEW deals"""
    hmap = header_map(headers)
    rows = ws.get_all_values()
    if len(rows) < 2:
        return 0

    scored_count = 0

    for i in range(2, len(rows) + 1):
        row_vals = rows[i - 1]
        row = {headers[c]: (row_vals[c] if c < len(row_vals) else "") for c in range(len(headers))}

        if safe_get(row, "status").upper() != STATUS_NEW:
            continue

        if safe_get(row, "ai_score"):
            continue

        log(f"  Scoring row {i}")
        result = score_deal_with_ai(row)

        updates = []
        if "ai_score" in hmap:
            updates.append(gspread.Cell(i, hmap["ai_score"], result["ai_score"]))
        if "ai_verdict" in hmap:
            updates.append(gspread.Cell(i, hmap["ai_verdict"], result["ai_verdict"]))
        if "ai_caption" in hmap:
            updates.append(gspread.Cell(i, hmap["ai_caption"], result["ai_caption"]))

        if updates:
            ws.update_cells(updates, value_input_option="USER_ENTERED")

        scored_count += 1

    return scored_count


def stage_select_best(ws: gspread.Worksheet, headers: List[str]) -> int:
    """Select best scored deal and promote to READY_TO_POST"""
    hmap = header_map(headers)
    rows = ws.get_all_values()
    if len(rows) < 2:
        return 0

    candidates = []

    for i in range(2, len(rows) + 1):
        row_vals = rows[i - 1]
        row = {headers[c]: (row_vals[c] if c < len(row_vals) else "") for c in range(len(headers))}

        if safe_get(row, "status").upper() != STATUS_NEW:
            continue

        ai_score_str = safe_get(row, "ai_score")
        if not ai_score_str:
            continue

        try:
            ai_score = float(ai_score_str)
        except:
            continue

        candidates.append((i, ai_score, row))

    if not candidates:
        return 0

    candidates.sort(key=lambda x: x[1], reverse=True)
    
    # Promote best
    row_idx, score, row_data = candidates[0]
    if "status" in hmap:
        ws.update_cell(row_idx, hmap["status"], STATUS_READY_TO_POST)
        log(f"  ✓ Promoted row {row_idx} (score: {score})")
        return 1

    return 0


# =========================
# Rendering (REQUIRED before Instagram)
# =========================
def render_image(row: Dict[str, str]) -> str:
    """Call render service to generate graphic"""
    payload = {
        "deal_id": safe_get(row, "deal_id"),
        "origin_city": safe_get(row, "origin_city") or safe_get(row, "origin_iata"),
        "destination_city": safe_get(row, "destination_city") or safe_get(row, "destination_iata"),
        "destination_country": safe_get(row, "destination_country"),
        "price_gbp": safe_get(row, "price_gbp"),
        "outbound_date": safe_get(row, "outbound_date"),
        "return_date": safe_get(row, "return_date"),
        "trip_length_days": safe_get(row, "trip_length_days"),
        "stops": safe_get(row, "stops"),
        "airline": safe_get(row, "airline"),
    }

    r = requests.post(RENDER_URL, json=payload, timeout=30)
    r.raise_for_status()
    return r.json().get("graphic_url", "")


def stage_render(ws: gspread.Worksheet, headers: List[str]) -> int:
    """Render graphics for READY_TO_POST deals"""
    hmap = header_map(headers)
    rows = ws.get_all_values()
    if len(rows) < 2:
        return 0

    rendered = 0

    for i in range(2, len(rows) + 1):
        row_vals = rows[i - 1]
        row = {headers[c]: (row_vals[c] if c < len(row_vals) else "") for c in range(len(headers))}

        if safe_get(row, "status").upper() != STATUS_READY_TO_POST:
            continue

        if safe_get(row, "graphic_url"):
            log(f"  Row {i} already rendered")
            if "status" in hmap:
                ws.update_cell(i, hmap["status"], STATUS_READY_TO_PUBLISH)
            rendered += 1
            continue

        log(f"  Rendering row {i}")
        
        try:
            graphic_url = render_image(row)
            
            updates = []
            if graphic_url and "graphic_url" in hmap:
                updates.append(gspread.Cell(i, hmap["graphic_url"], graphic_url))
            if "status" in hmap:
                updates.append(gspread.Cell(i, hmap["status"], STATUS_READY_TO_PUBLISH))

            if updates:
                ws.update_cells(updates, value_input_option="USER_ENTERED")

            rendered += 1
        except Exception as e:
            log(f"  Render error: {e}")

    return rendered


# =========================
# Instagram (with POLLING + CACHE-BUSTING)
# =========================
def instagram_caption_simple(row: Dict[str, str]) -> str:
    """Simple Instagram caption (human, short, varied)"""
    origin = safe_get(row, "origin_city") or safe_get(row, "origin_iata")
    dest = safe_get(row, "destination_city") or safe_get(row, "destination_iata")
    price_raw = safe_get(row, "price_gbp")
    price = f"£{round_price_up(price_raw)}"

    variants = [
        f"{origin} to {dest} for {price}. VIPs saw this yesterday. Link in bio for early access.",
        f"{price} from {origin} to {dest}. Our paid members got this deal first. Want early access? Bio.",
        f"Spotted {dest} at {price}. By the time it's here, VIPs have already booked. Follow for more.",
        f"{origin} to {dest}, {price}. Deals like this go to our community first. Link in bio to join.",
    ]

    base = variants[hash(dest) % len(variants)]
    
    return base + "\n\n#TravelDeals #CheapFlights #UKTravel"


def post_instagram(ws: gspread.Worksheet, headers: List[str]) -> int:
    """Post to Instagram with status polling and cache-busting"""
    hmap = header_map(headers)
    rows = ws.get_all_values()
    if len(rows) < 2:
        return 0

    posted = 0

    for i in range(2, len(rows) + 1):
        row_vals = rows[i - 1]
        row = {headers[c]: (row_vals[c] if c < len(row_vals) else "") for c in range(len(headers))}

        if safe_get(row, "status").upper() != STATUS_READY_TO_PUBLISH:
            continue

        graphic_url = safe_get(row, "graphic_url")
        if not graphic_url:
            log(f"  Row {i} missing graphic_url")
            continue

        log(f"  Posting to Instagram: row {i}")

        caption = instagram_caption_simple(row)

        # Cache-busting
        cache_buster = int(time.time())
        image_url_cb = f"{graphic_url}?cb={cache_buster}"

        try:
            # Create media
            create_url = f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media"
            create_data = {
                "image_url": image_url_cb,
                "caption": caption[:2200],
                "access_token": IG_ACCESS_TOKEN,
            }

            r1 = requests.post(create_url, data=create_data, timeout=30)
            r1.raise_for_status()
            creation_id = r1.json().get("id")

            if not creation_id:
                log(f"  No creation_id")
                continue

            # Poll status
            max_wait = 60
            waited = 0
            media_ready = False

            log(f"  Waiting for Instagram to process...")
            while waited < max_wait:
                status_url = f"https://graph.facebook.com/v20.0/{creation_id}"
                status_params = {
                    "fields": "status_code",
                    "access_token": IG_ACCESS_TOKEN
                }
                
                r_status = requests.get(status_url, params=status_params, timeout=10)
                if r_status.status_code == 200:
                    status_code = r_status.json().get("status_code", "")
                    
                    if status_code == "FINISHED":
                        media_ready = True
                        log(f"  Media ready after {waited}s")
                        break
                    elif status_code in ("ERROR", "EXPIRED"):
                        log(f"  Media processing {status_code}")
                        break
                
                time.sleep(2)
                waited += 2

            if not media_ready:
                log(f"  Media not ready after {waited}s")
                continue

            # Publish
            publish_url = f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media_publish"
            publish_data = {
                "creation_id": creation_id,
                "access_token": IG_ACCESS_TOKEN,
            }

            r2 = requests.post(publish_url, data=publish_data, timeout=30)
            r2.raise_for_status()
            post_id = r2.json().get("id")

            updates = []
            if post_id and "ig_published_timestamp" in hmap:
                updates.append(gspread.Cell(i, hmap["ig_published_timestamp"], now_utc()))
            if "status" in hmap:
                updates.append(gspread.Cell(i, hmap["status"], STATUS_POSTED_INSTAGRAM))

            if updates:
                ws.update_cells(updates, value_input_option="USER_ENTERED")

            posted += 1
            log(f"  ✓ Posted to Instagram")

        except Exception as e:
            log(f"  Instagram error: {e}")

    return posted


# =========================
# Telegram (with correct column names)
# =========================
def tg_send(token: str, chat: str, text: str) -> None:
    r = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat, "text": text, "parse_mode": "HTML"},
        timeout=30,
    )
    r.raise_for_status()


def format_telegram_vip(row: Dict[str, str]) -> str:
    """VIP message with marketing formatting"""
    price_raw = safe_get(row, "price_gbp")
    dest_city = safe_get(row, "destination_city") or safe_get(row, "destination_iata")
    dest_country = safe_get(row, "destination_country")
    origin_city = safe_get(row, "origin_city") or safe_get(row, "origin_iata")
    out_date = safe_get(row, "outbound_date")
    ret_date = safe_get(row, "return_date")
    ai_caption = safe_get(row, "ai_caption") or "Great value for this route"
    booking_link = safe_get(row, "booking_link_vip") or safe_get(row, "affiliate_url")

    price_display = round_price_up(price_raw)
    out_display = format_date_yymmdd(out_date)
    ret_display = format_date_yymmdd(ret_date)

    dest_display = f"{dest_city}, {dest_country}" if dest_country else dest_city

    msg = f"""£{price_display} to {dest_display}

<b>TO:</b> {dest_city.upper()}
<b>FROM:</b> {origin_city}

<b>OUT:</b>  {out_display}
<b>BACK:</b> {ret_display}

<b>Heads up:</b>
• {ai_caption}
• Availability is running low

"""
    if booking_link:
        msg += f'<a href="{booking_link}">BOOK NOW</a>'
    else:
        msg += "Search on Skyscanner to book"

    return msg


def format_telegram_free(row: Dict[str, str]) -> str:
    """FREE message with upsell"""
    price_raw = safe_get(row, "price_gbp")
    dest_city = safe_get(row, "destination_city") or safe_get(row, "destination_iata")
    dest_country = safe_get(row, "destination_country")
    origin_city = safe_get(row, "origin_city") or safe_get(row, "origin_iata")
    out_date = safe_get(row, "outbound_date")
    ret_date = safe_get(row, "return_date")
    booking_link = safe_get(row, "booking_link_free") or safe_get(row, "affiliate_url")

    price_display = round_price_up(price_raw)
    out_display = format_date_yymmdd(out_date)
    ret_display = format_date_yymmdd(ret_date)

    dest_display = f"{dest_city}, {dest_country}" if dest_country else dest_city

    msg = f"""£{price_display} to {dest_display}

<b>TO:</b> {dest_city.upper()}
<b>FROM:</b> {origin_city}

<b>OUT:</b>  {out_display}
<b>BACK:</b> {ret_display}

<b>Heads up:</b>
• VIP members saw this 24 hours ago
• Availability is running low
• Best deals go to VIPs first

"""
    if booking_link:
        msg += f'<a href="{booking_link}">Book now</a>\n\n'

    msg += "<b>Want instant access?</b>\nJoin TravelTxter Community\n\n"
    msg += "• Deals 24 hours early\n• Direct booking links\n• Exclusive mistake fares\n• Cancel anytime\n\n"

    if STRIPE_LINK_MONTHLY and STRIPE_LINK_YEARLY:
        msg += f'<a href="{STRIPE_LINK_MONTHLY}">Upgrade Monthly</a> | <a href="{STRIPE_LINK_YEARLY}">Upgrade Yearly</a>'
    elif STRIPE_LINK_MONTHLY:
        msg += f'<a href="{STRIPE_LINK_MONTHLY}">Upgrade now</a>'

    return msg


def post_telegram_vip(ws: gspread.Worksheet, headers: List[str]) -> int:
    """Post to VIP Telegram (AM only)"""
    if RUN_SLOT != "AM":
        return 0

    hmap = header_map(headers)
    rows = ws.get_all_values()
    if len(rows) < 2:
        return 0

    posted = 0

    for i in range(2, len(rows) + 1):
        row_vals = rows[i - 1]
        row = {headers[c]: (row_vals[c] if c < len(row_vals) else "") for c in range(len(headers))}

        if safe_get(row, "status").upper() != STATUS_POSTED_INSTAGRAM:
            continue

        # Use correct column name from spreadsheet
        if safe_get(row, "tg_monthly_timestamp"):
            continue

        log(f"  Posting to VIP: row {i}")

        try:
            msg = format_telegram_vip(row)
            tg_send(TELEGRAM_BOT_TOKEN_VIP, TELEGRAM_VIP_CHANNEL, msg)

            updates = []
            if "tg_monthly_timestamp" in hmap:
                updates.append(gspread.Cell(i, hmap["tg_monthly_timestamp"], now_utc()))
            if "posted_to_vip" in hmap:
                updates.append(gspread.Cell(i, hmap["posted_to_vip"], "TRUE"))
            if "status" in hmap:
                updates.append(gspread.Cell(i, hmap["status"], STATUS_POSTED_TELEGRAM_VIP))

            if updates:
                ws.update_cells(updates, value_input_option="USER_ENTERED")

            posted += 1
            log(f"  ✓ Posted to VIP")

        except Exception as e:
            log(f"  VIP error: {e}")

    return posted


def post_telegram_free(ws: gspread.Worksheet, headers: List[str]) -> int:
    """Post to FREE Telegram (PM only, 24h delay)"""
    if RUN_SLOT != "PM":
        return 0

    hmap = header_map(headers)
    rows = ws.get_all_values()
    if len(rows) < 2:
        return 0

    posted = 0

    for i in range(2, len(rows) + 1):
        row_vals = rows[i - 1]
        row = {headers[c]: (row_vals[c] if c < len(row_vals) else "") for c in range(len(headers))}

        if safe_get(row, "status").upper() != STATUS_POSTED_TELEGRAM_VIP:
            continue

        if safe_get(row, "tg_free_timestamp") or safe_get(row, "posted_for_free"):
            continue

        # Check 24h delay
        vip_ts = safe_get(row, "tg_monthly_timestamp")
        hours = hours_since(vip_ts)
        
        if hours < VIP_DELAY_HOURS:
            log(f"  Row {i}: VIP posted {hours:.1f}h ago, need {VIP_DELAY_HOURS}h")
            continue

        log(f"  Posting to FREE: row {i} (delay: {hours:.1f}h)")

        try:
            msg = format_telegram_free(row)
            tg_send(TELEGRAM_BOT_TOKEN, TELEGRAM_FREE_CHANNEL, msg)

            updates = []
            if "tg_free_timestamp" in hmap:
                updates.append(gspread.Cell(i, hmap["tg_free_timestamp"], now_utc()))
            if "posted_for_free" in hmap:
                updates.append(gspread.Cell(i, hmap["posted_for_free"], "TRUE"))
            if "status" in hmap:
                updates.append(gspread.Cell(i, hmap["status"], STATUS_POSTED_ALL))

            if updates:
                ws.update_cells(updates, value_input_option="USER_ENTERED")

            posted += 1
            log(f"  ✓ Posted to FREE")

        except Exception as e:
            log(f"  FREE error: {e}")

    return posted


# =========================
# MAIN
# =========================
def main():
    log("=" * 70)
    log("TRAVELTXTER V4.2 - PRODUCTION FINAL")
    log("=" * 70)
    log(f"RUN_SLOT: {RUN_SLOT} | VIP_DELAY: {VIP_DELAY_HOURS}h")
    log(f"Duffel: {'ON' if DUFFEL_ENABLED else 'OFF'} | OpenAI: {'ON' if OPENAI_API_KEY else 'OFF'}")
    log("=" * 70)

    try:
        ws, headers = get_ws()
        log(f"Connected | Columns: {len(headers)}")

        # Stage 1: Duffel (if enabled)
        if DUFFEL_ENABLED and DUFFEL_API_KEY:
            log("\n[1] DUFFEL FEED")
            inserted = run_duffel_feeder(ws, headers)
            log(f"✓ {inserted} deals inserted")

        # Stage 2: AI Scoring (if enabled)
        if OPENAI_API_KEY:
            log("\n[2] AI SCORING")
            scored = stage_score_all_new(ws, headers)
            log(f"✓ {scored} deals scored")

            log("\n[3] SELECT BEST")
            selected = stage_select_best(ws, headers)
            log(f"✓ {selected} promoted")

        # Stage 3: Render
        log("\n[4] RENDER")
        rendered = stage_render(ws, headers)
        log(f"✓ {rendered} rendered")

        # Stage 4: Instagram (BOTH runs)
        log("\n[5] INSTAGRAM")
        ig_posted = post_instagram(ws, headers)
        log(f"✓ {ig_posted} posted")

        # Stage 5: Telegram VIP (AM only)
        log("\n[6] TELEGRAM VIP")
        vip_posted = post_telegram_vip(ws, headers)
        log(f"✓ {vip_posted} posted")

        # Stage 6: Telegram FREE (PM only, 24h delay)
        log("\n[7] TELEGRAM FREE")
        free_posted = post_telegram_free(ws, headers)
        log(f"✓ {free_posted} posted")

        log("\n" + "=" * 70)
        log("COMPLETE")
        log("=" * 70)

    except Exception as e:
        log(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
