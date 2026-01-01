#!/usr/bin/env python3
"""
TravelTxter V4.2 Unified Pipeline - Production Ready FINAL

Features:
1) Duffel FEED with deterministic route rotation
2) AI scoring with human-like copy (no emojis)
3) Rendering service integration (solari board graphics)
4) Instagram publishing (simple CTA captions - graphic shows all details)
5) Telegram VIP (24h early access with booking links)
6) Telegram FREE (with 24h delay + upsell)
7) City names (not airport codes) in all user-facing content
"""

import os
import json
import uuid
import datetime as dt
import re
import time
from typing import Dict, Any, List, Tuple, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials
from openai import OpenAI
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
RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", os.getenv("RAW_DEALS", "")).strip()
CONFIG_TAB = os.getenv("CONFIG_TAB", "CONFIG").strip()

GCP_SA_JSON = os.getenv("GCP_SA_JSON", "").strip()
GCP_SA_JSON_ONE_LINE = os.getenv("GCP_SA_JSON_ONE_LINE", "").strip()

DUFFEL_API_KEY = os.getenv("DUFFEL_API_KEY", "").strip()
DUFFEL_VERSION = os.getenv("DUFFEL_VERSION", "v2").strip()
DUFFEL_MAX_INSERTS = int(os.getenv("DUFFEL_MAX_INSERTS", "3"))
DUFFEL_ROUTES_PER_RUN = int(os.getenv("DUFFEL_ROUTES_PER_RUN", "1"))
DUFFEL_ENABLED = os.getenv("DUFFEL_ENABLED", "true").strip().lower() in ("1", "true", "yes", "y")

DEFAULT_ORIGIN_IATA = os.getenv("DEFAULT_ORIGIN_IATA", "LHR").strip().upper()
DEFAULT_DEST_IATA = os.getenv("DEFAULT_DEST_IATA", "BCN").strip().upper()
DAYS_AHEAD = int(os.getenv("DAYS_AHEAD", "60"))
TRIP_LENGTH_DAYS = int(os.getenv("TRIP_LENGTH_DAYS", "5"))

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()

# Rendering & Instagram
RENDER_URL = os.getenv("RENDER_URL", "").strip()
IG_ACCESS_TOKEN = os.getenv("IG_ACCESS_TOKEN", "").strip()
IG_USER_ID = os.getenv("IG_USER_ID", "").strip()

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHANNEL = (
    os.getenv("TELEGRAM_CHANNEL", "")
    or os.getenv("TELEGRAM_FREE_CHANNEL", "")
    or os.getenv("TELEGRAM_CHANNEL_FREE", "")
).strip()

TELEGRAM_BOT_TOKEN_VIP = os.getenv("TELEGRAM_BOT_TOKEN_VIP", "").strip()
TELEGRAM_CHANNEL_VIP = (
    os.getenv("TELEGRAM_CHANNEL_VIP", "")
    or os.getenv("TELEGRAM_VIP_CHANNEL", "")
).strip()

# Subscription links
STRIPE_LINK_MONTHLY = os.getenv("STRIPE_LINK_MONTHLY", "").strip()
STRIPE_LINK_YEARLY = os.getenv("STRIPE_LINK_YEARLY", "").strip()
SKYSCANNER_AFFILIATE_ID = os.getenv("SKYSCANNER_AFFILIATE_ID", "").strip()

VIP_DELAY_HOURS = int(os.getenv("VIP_DELAY_HOURS", "24"))
RUN_SLOT = os.getenv("RUN_SLOT", "AM").strip().upper()  # AM / PM


# =========================
# Status constants
# =========================
STATUS_NEW = "NEW"
STATUS_READY_TO_POST = "READY_TO_POST"
STATUS_READY_TO_PUBLISH = "READY_TO_PUBLISH"
STATUS_POSTED_INSTAGRAM = "POSTED_INSTAGRAM"
STATUS_POSTED_TELEGRAM_VIP = "POSTED_TELEGRAM_VIP"
STATUS_POSTED_TELEGRAM_FREE = "POSTED_TELEGRAM_FREE"
STATUS_POSTED_ALL = "POSTED_ALL"


# =========================
# Google Sheets helpers
# =========================
def load_sa_json() -> Dict[str, Any]:
    if GCP_SA_JSON:
        return json.loads(GCP_SA_JSON)
    if GCP_SA_JSON_ONE_LINE:
        return json.loads(GCP_SA_JSON_ONE_LINE)
    raise RuntimeError("Missing GCP_SA_JSON or GCP_SA_JSON_ONE_LINE")


def get_ws() -> Tuple[gspread.Worksheet, List[str]]:
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID")
    if not RAW_DEALS_TAB:
        raise RuntimeError("Missing RAW_DEALS_TAB")

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
        raise RuntimeError("RAW_DEALS has no headers (row 1)")
    return ws, headers


def header_map(headers: List[str]) -> Dict[str, int]:
    return {h.strip(): i + 1 for i, h in enumerate(headers) if h.strip()}


def iso_date(d: dt.date) -> str:
    return d.isoformat()


def safe_get(row: Dict[str, str], key: str) -> str:
    return (row.get(key) or "").strip()


# =========================
# CONFIG: route rotation
# =========================
def load_routes_from_config(ws_parent: gspread.Spreadsheet) -> List[Tuple[str, str]]:
    try:
        cfg = ws_parent.worksheet(CONFIG_TAB)
    except Exception:
        return []

    values = cfg.get_all_values()
    if not values or len(values) < 2:
        return []

    headers = [h.strip() for h in values[0]]
    idx = {h: i for i, h in enumerate(headers) if h}

    if "origin_iata" not in idx or "destination_iata" not in idx:
        return []

    out: List[Tuple[str, str]] = []
    for r in values[1:]:
        enabled = ""
        if "enabled" in idx and idx["enabled"] < len(r):
            enabled = (r[idx["enabled"]] or "").strip().lower()
        if enabled and enabled not in ("1", "true", "yes", "y"):
            continue

        o = ""
        d = ""
        if idx["origin_iata"] < len(r):
            o = (r[idx["origin_iata"]] or "").strip().upper()
        if idx["destination_iata"] < len(r):
            d = (r[idx["destination_iata"]] or "").strip().upper()

        if o and d:
            out.append((o, d))

    return out


def select_route_deterministic(routes: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
    """
    Select route deterministically based on day-of-year and AM/PM slot.
    Ensures full rotation through all 60 routes.
    """
    if not routes:
        return []
    
    day_of_year = dt.date.today().timetuple().tm_yday
    slot_offset = 0 if RUN_SLOT == "AM" else 1
    
    # (day * 2 + slot) ensures AM and PM get different routes
    # Modulo ensures we cycle through all routes
    index = (day_of_year * 2 + slot_offset) % len(routes)
    
    return [routes[index]]


# =========================
# Skyscanner Affiliate Links
# =========================
def generate_skyscanner_link(origin_iata: str, dest_iata: str, 
                            out_date: str, ret_date: str) -> str:
    """
    Generate Skyscanner affiliate link.
    Format: https://www.skyscanner.net/transport/flights/ORIGIN/DEST/OUTDATE/RETDATE/?affiliateid=XXX
    """
    if not SKYSCANNER_AFFILIATE_ID:
        # Fallback to non-affiliate link
        return f"https://www.skyscanner.net/transport/flights/{origin_iata}/{dest_iata}/{out_date.replace('-', '')}/{ret_date.replace('-', '')}/"
    
    return f"https://www.skyscanner.net/transport/flights/{origin_iata}/{dest_iata}/{out_date.replace('-', '')}/{ret_date.replace('-', '')}/?affiliateid={SKYSCANNER_AFFILIATE_ID}"


# =========================
# Duffel
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


def duffel_run_and_insert(ws: gspread.Worksheet, headers: List[str]) -> int:
    if not DUFFEL_ENABLED:
        log("Duffel: DISABLED")
        return 0
    if not DUFFEL_API_KEY:
        log("Duffel: ENABLED but missing DUFFEL_API_KEY -> skipping.")
        return 0

    sh = ws.spreadsheet
    routes = load_routes_from_config(sh)
    if not routes:
        routes = [(DEFAULT_ORIGIN_IATA, DEFAULT_DEST_IATA)]

    # FIXED: Deterministic route selection
    routes = select_route_deterministic(routes)
    log(f"Duffel: ENABLED | routes_per_run={len(routes)} | max_inserts={DUFFEL_MAX_INSERTS}")
    log(f"Routes selected (deterministic): {routes}")

    hmap = header_map(headers)

    required = ["deal_id", "origin_iata", "destination_iata", "outbound_date", "return_date", "price_gbp", "status"]
    missing = [c for c in required if c not in hmap]
    if missing:
        raise RuntimeError(f"RAW_DEALS missing required columns: {missing}")

    inserted = 0
    today = dt.date.today()

    for (origin, dest) in routes:
        # Search 21 days ahead (good balance for deals)
        out_date = today + dt.timedelta(days=21)
        ret_date = out_date + dt.timedelta(days=TRIP_LENGTH_DAYS)

        log(f"Duffel search: {origin}->{dest} {iso_date(out_date)}->{iso_date(ret_date)}")
        data = duffel_offer_request(origin, dest, iso_date(out_date), iso_date(ret_date))

        offers = []
        try:
            offers = (data.get("data") or {}).get("offers") or []
        except Exception:
            offers = []

        if not offers:
            log("Duffel returned 0 offers.")
            continue

        rows_to_append: List[List[Any]] = []
        for off in offers:
            if inserted >= DUFFEL_MAX_INSERTS:
                break

            price = off.get("total_amount") or ""
            currency = off.get("total_currency") or "GBP"
            if currency != "GBP":
                continue

            owner = (off.get("owner") or {}).get("name") or ""

            slices = off.get("slices") or []
            stops = "0"
            try:
                segs = (slices[0].get("segments") or [])
                stops = str(max(0, len(segs) - 1))
            except Exception:
                pass

            deal_id = str(uuid.uuid4())

            row_obj = {h: "" for h in headers}
            row_obj["deal_id"] = deal_id
            row_obj["origin_iata"] = origin
            row_obj["destination_iata"] = dest

            # Extract city/country names from Duffel
            origin_city = ""
            dest_city = ""
            dest_country = ""
            try:
                if slices:
                    o = (slices[0].get("origin") or {})
                    d = (slices[0].get("destination") or {})
                    origin_city = (o.get("city_name") or o.get("name") or "").strip()
                    dest_city = (d.get("city_name") or d.get("name") or "").strip()
                    dest_country = (d.get("country_name") or "").strip()
            except Exception:
                pass

            def _title(x: str) -> str:
                x = (x or "").strip()
                return x.title() if x else ""

            if "origin_city" in row_obj:
                row_obj["origin_city"] = _title(origin_city) or origin
            if "destination_city" in row_obj:
                row_obj["destination_city"] = _title(dest_city) or dest
            if "destination_country" in row_obj:
                row_obj["destination_country"] = _title(dest_country)

            row_obj["outbound_date"] = iso_date(out_date)
            row_obj["return_date"] = iso_date(ret_date)
            row_obj["price_gbp"] = price

            if "trip_length_days" in row_obj:
                row_obj["trip_length_days"] = str(TRIP_LENGTH_DAYS)
            if "deal_source" in row_obj:
                row_obj["deal_source"] = "DUFFEL"
            if "theme" in row_obj:
                row_obj["theme"] = ""
            if "date_added" in row_obj:
                row_obj["date_added"] = now_utc().replace(microsecond=0).isoformat() + "Z"

            if "airline" in row_obj:
                row_obj["airline"] = owner
            if "stops" in row_obj:
                row_obj["stops"] = stops

            row_obj["status"] = STATUS_NEW

            # FIXED: Generate Skyscanner affiliate links
            booking_link = generate_skyscanner_link(origin, dest, iso_date(out_date), iso_date(ret_date))
            if "booking_link_vip" in row_obj:
                row_obj["booking_link_vip"] = booking_link
            if "booking_link_free" in row_obj:
                row_obj["booking_link_free"] = booking_link

            rows_to_append.append([row_obj.get(h, "") for h in headers])
            inserted += 1

        if rows_to_append:
            ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
            log(f"Inserted {len(rows_to_append)} new rows from Duffel.")
        else:
            log("No insertable offers after filtering.")

    return inserted


# =========================
# AI SCORING
# =========================
def openai_client() -> OpenAI:
    if not OPENAI_API_KEY:
        raise RuntimeError("Missing OPENAI_API_KEY")
    return OpenAI(api_key=OPENAI_API_KEY)


def score_and_caption_deal(row: Dict[str, str]) -> Tuple[str, str, str]:
    """
    Score a deal and generate human-like explanation (NO EMOJIS).
    Returns: (ai_score, ai_verdict, why_its_good)
    """
    client = openai_client()

    origin = safe_get(row, "origin_city") or safe_get(row, "origin_iata") or "UK"
    dest = safe_get(row, "destination_city") or safe_get(row, "destination_iata") or "Somewhere"
    dest_country = safe_get(row, "destination_country") or ""
    price = safe_get(row, "price_gbp") or "?"
    out_date = safe_get(row, "outbound_date")
    ret_date = safe_get(row, "return_date")
    airline = safe_get(row, "airline") or "Unknown airline"
    stops = safe_get(row, "stops") or "0"

    prompt = f"""
You are a UK travel expert evaluating flight deals.

Deal Details:
- From: {origin}
- To: {dest}, {dest_country}
- Price: £{price}
- Dates: {out_date} to {ret_date}
- Airline: {airline}
- Stops: {stops}

Evaluate this deal and return JSON with:
1. ai_score (0-100): How good is this price for this route? Consider seasonality, typical prices, stops.
2. ai_verdict (GOOD/AVERAGE/POOR): Overall assessment
3. why_its_good (1-2 sentences, max 150 chars): Explain why someone should book this. Be specific, human, conversational. NO EMOJIS. Examples:
   - "Direct flights to Iceland for under £110 are rare outside January"
   - "This undercuts the usual £180 fare by nearly 40 percent"
   - "Weekend break in Barcelona for the price of a train ticket to Edinburgh"

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

        # Clean markdown fences if present
        if text_out.startswith("```json"):
            text_out = text_out[7:]
        if text_out.startswith("```"):
            text_out = text_out[3:]
        if text_out.endswith("```"):
            text_out = text_out[:-3]
        text_out = text_out.strip()

        data = json.loads(text_out)

        ai_score = str(data.get("ai_score", 60))
        ai_verdict = str(data.get("ai_verdict", "AVERAGE")).upper()
        why_its_good = str(data.get("why_its_good", "")).strip()

        # Fallback if empty
        if not why_its_good:
            why_its_good = f"Return flights to {dest} for £{price}"

        return ai_score, ai_verdict, why_its_good

    except Exception as e:
        log(f"OpenAI error: {e}")
        # Fallback values
        return "60", "AVERAGE", f"Return flights to {dest} for £{price}"


def stage_scoring(ws: gspread.Worksheet, headers: List[str], max_rows: int = 1) -> int:
    """
    Score NEW deals and promote BEST to READY_TO_POST (with freshness decay).
    """
    hmap = header_map(headers)

    if "status" not in hmap:
        raise RuntimeError("Missing 'status' column for scoring stage")

    rows = ws.get_all_values()
    if len(rows) < 2:
        return 0

    # Score all NEW deals
    deals_to_score = []
    for i in range(2, len(rows) + 1):
        row_vals = rows[i - 1]
        row = {headers[c]: (row_vals[c] if c < len(row_vals) else "") for c in range(len(headers))}

        status = safe_get(row, "status").upper()
        if status != STATUS_NEW:
            continue

        # Skip if already scored
        if safe_get(row, "ai_score"):
            # Already scored, calculate final score with decay
            ai_score = float(safe_get(row, "ai_score") or "0")
            date_added_str = safe_get(row, "date_added")
            age_days = 0
            if date_added_str:
                try:
                    date_added = dt.datetime.fromisoformat(date_added_str.replace("Z", "+00:00"))
                    age_days = (now_utc() - date_added).days
                except Exception:
                    pass
            
            # Apply freshness decay: 2 points per day
            final_score = ai_score - (2.0 * age_days)
            deals_to_score.append((i, final_score, row))
            continue

        # Score new deal
        log(f"Scoring row {i}: {safe_get(row, 'origin_city')}->{safe_get(row, 'destination_city')}")

        ai_score, ai_verdict, why_its_good = score_and_caption_deal(row)

        # Update sheet with scores
        updates = []
        if "ai_score" in hmap:
            updates.append(gspread.Cell(i, hmap["ai_score"], ai_score))
        if "ai_verdict" in hmap:
            updates.append(gspread.Cell(i, hmap["ai_verdict"], ai_verdict))
        if "why_its_good" in hmap:
            updates.append(gspread.Cell(i, hmap["why_its_good"], why_its_good))

        if updates:
            ws.update_cells(updates, value_input_option="USER_ENTERED")

        # Calculate final score with freshness decay
        date_added_str = safe_get(row, "date_added")
        age_days = 0
        if date_added_str:
            try:
                date_added = dt.datetime.fromisoformat(date_added_str.replace("Z", "+00:00"))
                age_days = (now_utc() - date_added).days
            except Exception:
                pass

        final_score = float(ai_score) - (2.0 * age_days)
        deals_to_score.append((i, final_score, row))

    # Promote BEST deal to READY_TO_POST
    if deals_to_score and max_rows > 0:
        # Sort by final score descending
        deals_to_score.sort(key=lambda x: x[1], reverse=True)
        
        promoted = 0
        for row_idx, final_score, row_data in deals_to_score:
            if promoted >= max_rows:
                break
            
            if "status" in hmap:
                ws.update_cell(row_idx, hmap["status"], STATUS_READY_TO_POST)
                log(f"Promoted row {row_idx} to READY_TO_POST (score: {final_score:.1f})")
                promoted += 1

        return promoted

    return 0


# =========================
# RENDERING
# =========================
def stage_rendering(ws: gspread.Worksheet, headers: List[str], max_rows: int = 1) -> int:
    """
    Call rendering service for deals with READY_TO_POST status.
    Sends: origin_city, destination_city, price_gbp, dates (for solari board graphic)
    Transitions to READY_TO_PUBLISH.
    """
    if not RENDER_URL:
        log("RENDER_URL not set - skipping rendering, auto-promoting to READY_TO_PUBLISH")
        # Auto-promote READY_TO_POST -> READY_TO_PUBLISH if no rendering
        hmap = header_map(headers)
        rows = ws.get_all_values()
        promoted = 0
        
        for i in range(2, len(rows) + 1):
            if promoted >= max_rows:
                break
            row_vals = rows[i - 1]
            row = {headers[c]: (row_vals[c] if c < len(row_vals) else "") for c in range(len(headers))}
            
            if safe_get(row, "status").upper() == STATUS_READY_TO_POST:
                if "status" in hmap:
                    ws.update_cell(i, hmap["status"], STATUS_READY_TO_PUBLISH)
                    promoted += 1
        
        return promoted

    hmap = header_map(headers)
    rows = ws.get_all_values()
    rendered = 0

    for i in range(2, len(rows) + 1):
        if rendered >= max_rows:
            break

        row_vals = rows[i - 1]
        row = {headers[c]: (row_vals[c] if c < len(row_vals) else "") for c in range(len(headers))}

        status = safe_get(row, "status").upper()
        if status != STATUS_READY_TO_POST:
            continue

        # Check if already rendered
        if safe_get(row, "graphic_url"):
            log(f"Row {i} already has graphic_url, promoting")
            if "status" in hmap:
                ws.update_cell(i, hmap["status"], STATUS_READY_TO_PUBLISH)
            rendered += 1
            continue

        log(f"Rendering row {i}")

        # Send payload matching render service expectations (V3 format)
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
            "ai_score": safe_get(row, "ai_score"),
            "ai_verdict": safe_get(row, "ai_verdict"),
            "ai_caption": safe_get(row, "ai_caption"),
        }

        try:
            r = requests.post(RENDER_URL, json=payload, timeout=30)
            if r.status_code == 200:
                data = r.json()
                graphic_url = data.get("graphic_url", "")

                updates = []
                if graphic_url and "graphic_url" in hmap:
                    updates.append(gspread.Cell(i, hmap["graphic_url"], graphic_url))
                    log(f"Rendered: {graphic_url}")

                if "status" in hmap:
                    updates.append(gspread.Cell(i, hmap["status"], STATUS_READY_TO_PUBLISH))

                if updates:
                    ws.update_cells(updates, value_input_option="USER_ENTERED")

                rendered += 1
            else:
                log(f"Render failed: {r.status_code} {r.text[:200]}")
        except Exception as e:
            log(f"Render error: {e}")

    return rendered


# =========================
# INSTAGRAM
# =========================
def generate_instagram_caption(row: Dict[str, str]) -> str:
    """
    Generate simple, enticing Instagram caption that drives sign-ups.
    The graphic (solari board) shows all the deal details, so copy is short and CTA-focused.
    NO EMOJIS. Human, conversational.
    """
    client = openai_client()

    dest_city = safe_get(row, "destination_city") or safe_get(row, "destination_iata")
    price = safe_get(row, "price_gbp")
    origin_city = safe_get(row, "origin_city") or safe_get(row, "origin_iata")

    prompt = f"""
You are writing a short Instagram caption for a flight deal post.

The graphic (solari board style) already shows all details:
- {origin_city} to {dest_city}
- £{price}
- Exact dates

Write a SHORT caption (2-4 sentences, max 60 words) that:
- Sounds natural and conversational (no emojis, no excessive excitement)
- Mentions VIP members get these deals 24 hours early
- Encourages people to sign up / link in bio
- Creates FOMO without being pushy

Good examples:
"London to Bordeaux for £105. Our VIP members saw this yesterday. Link in bio to join."

"Spotted Barcelona at £89. By the time it's on Instagram, our paid members have had 24 hours to book. Want early access? Bio link."

"Manchester to Reykjavik, £103. VIP members got this deal yesterday morning. Follow for more, or join the VIP list in bio for early access."

Write caption now (no hashtags, no emojis):
""".strip()

    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": "You are a copywriter. Write short, conversational captions. Never use emojis. Be natural and human."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=120
        )

        caption = resp.choices[0].message.content.strip()
        
        # Remove any emojis that might have slipped through
        caption = re.sub(r'[^\w\s.,!?£\'\"-]', '', caption)
        
        return caption

    except Exception as e:
        log(f"Instagram caption generation error: {e}")
        # Fallback
        return f"{origin_city} to {dest_city} for £{price}. Our VIP members saw this 24 hours ago. Link in bio to join TravelTxter for early access."


def stage_instagram(ws: gspread.Worksheet, headers: List[str], max_rows: int = 1) -> int:
    """
    Post deals to Instagram with simple, CTA-focused captions.
    Graphic shows all deal details (solari board style).
    Includes proper status checking before publishing.
    Transitions to POSTED_INSTAGRAM.
    """
    if not IG_ACCESS_TOKEN or not IG_USER_ID:
        log("Instagram credentials missing - skipping")
        return 0

    hmap = header_map(headers)
    rows = ws.get_all_values()
    posted = 0

    for i in range(2, len(rows) + 1):
        if posted >= max_rows:
            break

        row_vals = rows[i - 1]
        row = {headers[c]: (row_vals[c] if c < len(row_vals) else "") for c in range(len(headers))}

        status = safe_get(row, "status").upper()
        if status != STATUS_READY_TO_PUBLISH:
            continue

        graphic_url = safe_get(row, "graphic_url")
        if not graphic_url:
            log(f"Row {i} missing graphic_url - skipping Instagram")
            continue

        dest_city = safe_get(row, "destination_city") or safe_get(row, "destination_iata")

        log(f"Posting to Instagram: row {i} ({dest_city})")

        # Generate short, CTA-focused caption
        caption = generate_instagram_caption(row)

        # Add hashtags
        hashtags = "\n\n#TravelDeals #CheapFlights #FlightDeals #TravelTips #UKTravel #TravelCommunity"
        full_caption = caption + hashtags

        try:
            # Step 1: Create media container
            create_url = f"https://graph.facebook.com/v18.0/{IG_USER_ID}/media"
            create_payload = {
                "image_url": graphic_url,
                "caption": full_caption[:2200],  # Instagram limit
                "access_token": IG_ACCESS_TOKEN
            }

            log(f"Creating Instagram media container...")
            r1 = requests.post(create_url, data=create_payload, timeout=30)
            if r1.status_code != 200:
                log(f"IG create failed: {r1.status_code} {r1.text[:200]}")
                continue

            creation_id = r1.json().get("id")
            log(f"Media container created: {creation_id}")

            # Step 2: Poll for media readiness
            # time already imported at top
            max_wait_seconds = 60
            poll_interval = 2
            waited = 0
            media_ready = False

            log(f"Waiting for Instagram to process image...")
            while waited < max_wait_seconds:
                status_url = f"https://graph.facebook.com/v18.0/{creation_id}"
                status_params = {
                    "fields": "status_code",
                    "access_token": IG_ACCESS_TOKEN
                }
                
                r_status = requests.get(status_url, params=status_params, timeout=10)
                if r_status.status_code == 200:
                    status_data = r_status.json()
                    status_code = status_data.get("status_code", "")
                    
                    log(f"Media status: {status_code} (waited {waited}s)")
                    
                    if status_code == "FINISHED":
                        media_ready = True
                        log(f"Media ready after {waited}s")
                        break
                    elif status_code == "ERROR":
                        log(f"Media processing error - aborting")
                        break
                    elif status_code == "EXPIRED":
                        log(f"Media container expired - aborting")
                        break
                    # If IN_PROGRESS or not set, continue waiting
                
                time.sleep(poll_interval)
                waited += poll_interval

            if not media_ready:
                log(f"Media not ready after {waited}s - aborting publish")
                continue

            # Step 3: Publish
            publish_url = f"https://graph.facebook.com/v18.0/{IG_USER_ID}/media_publish"
            publish_payload = {
                "creation_id": creation_id,
                "access_token": IG_ACCESS_TOKEN
            }

            log(f"Publishing to Instagram...")
            r2 = requests.post(publish_url, data=publish_payload, timeout=30)
            if r2.status_code == 200:
                post_id = r2.json().get("id")
                log(f"Posted to Instagram: {post_id}")

                updates = []
                if "instagram_post_id" in hmap:
                    updates.append(gspread.Cell(i, hmap["instagram_post_id"], post_id))
                if "instagram_caption" in hmap:
                    updates.append(gspread.Cell(i, hmap["instagram_caption"], caption[:500]))
                if "status" in hmap:
                    updates.append(gspread.Cell(i, hmap["status"], STATUS_POSTED_INSTAGRAM))

                if updates:
                    ws.update_cells(updates, value_input_option="USER_ENTERED")

                posted += 1
            else:
                log(f"IG publish failed: {r2.status_code} {r2.text[:200]}")

        except Exception as e:
            log(f"Instagram error: {e}")

    return posted
def parse_ts(s: str) -> Optional[dt.datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1]
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None


def tg_send(bot_token: str, channel: str, msg: str, parse_mode: str = "HTML") -> None:
    if not bot_token or not channel:
        raise RuntimeError("Missing TELEGRAM token or channel")

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
    Format VIP Telegram message with booking link.
    Uses CITY NAMES (not airport codes).
    """
    price = safe_get(row, "price_gbp")
    dest_city = safe_get(row, "destination_city") or safe_get(row, "destination_iata")
    dest_country = safe_get(row, "destination_country")
    origin_city = safe_get(row, "origin_city") or safe_get(row, "origin_iata")
    out_date = safe_get(row, "outbound_date")
    ret_date = safe_get(row, "return_date")
    why_good = safe_get(row, "why_its_good") or "Great value for this route"
    booking_link = safe_get(row, "booking_link_vip")

    dest_display = f"{dest_city}, {dest_country}" if dest_country else dest_city

    message = f"""£{price} to {dest_display}

<b>TO:</b> {dest_city.upper()}
<b>FROM:</b> {origin_city}

<b>OUT:</b>  {out_date}
<b>BACK:</b> {ret_date}

<b>Heads up:</b>
• {why_good}
• Availability is running low

"""

    if booking_link:
        message += f'<a href="{booking_link}">BOOK NOW</a>'
    else:
        message += "Search on Skyscanner to book"

    return message


def format_telegram_free(row: Dict[str, str]) -> str:
    """
    Format FREE Telegram message with upsell.
    Uses CITY NAMES (not airport codes).
    """
    price = safe_get(row, "price_gbp")
    dest_city = safe_get(row, "destination_city") or safe_get(row, "destination_iata")
    dest_country = safe_get(row, "destination_country")
    origin_city = safe_get(row, "origin_city") or safe_get(row, "origin_iata")
    out_date = safe_get(row, "outbound_date")
    ret_date = safe_get(row, "return_date")
    booking_link = safe_get(row, "booking_link_free")

    dest_display = f"{dest_city}, {dest_country}" if dest_country else dest_city

    message = f"""£{price} to {dest_display}

<b>TO:</b> {dest_city.upper()}
<b>FROM:</b> {origin_city}

<b>OUT:</b>  {out_date}
<b>BACK:</b> {ret_date}

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


def stage_telegram_vip(ws: gspread.Worksheet, headers: List[str], max_rows: int = 1) -> int:
    """
    Post to VIP Telegram (AM run only).
    Transitions POSTED_INSTAGRAM -> POSTED_TELEGRAM_VIP.
    """
    if RUN_SLOT != "AM":
        log("Telegram VIP: Skipping (PM run)")
        return 0

    if not TELEGRAM_BOT_TOKEN_VIP or not TELEGRAM_CHANNEL_VIP:
        log("Telegram VIP: Missing credentials")
        return 0

    hmap = header_map(headers)
    rows = ws.get_all_values()
    posted = 0

    for i in range(2, len(rows) + 1):
        if posted >= max_rows:
            break

        row_vals = rows[i - 1]
        row = {headers[c]: (row_vals[c] if c < len(row_vals) else "") for c in range(len(headers))}

        status = safe_get(row, "status").upper()
        if status != STATUS_POSTED_INSTAGRAM:
            continue

        # Check if already posted
        if safe_get(row, "telegram_vip_posted_at"):
            continue

        log(f"Posting to Telegram VIP: row {i}")

        message = format_telegram_vip(row)

        try:
            tg_send(TELEGRAM_BOT_TOKEN_VIP, TELEGRAM_CHANNEL_VIP, message)

            ts = now_utc().replace(microsecond=0).isoformat() + "Z"
            updates = []
            if "telegram_vip_posted_at" in hmap:
                updates.append(gspread.Cell(i, hmap["telegram_vip_posted_at"], ts))
            if "status" in hmap:
                updates.append(gspread.Cell(i, hmap["status"], STATUS_POSTED_TELEGRAM_VIP))

            if updates:
                ws.update_cells(updates, value_input_option="USER_ENTERED")

            posted += 1
            log(f"Posted to VIP Telegram successfully")

        except Exception as e:
            log(f"Telegram VIP error: {e}")

    return posted


def stage_telegram_free(ws: gspread.Worksheet, headers: List[str], max_rows: int = 1) -> int:
    """
    Post to FREE Telegram (PM run only, after 24h delay).
    Transitions POSTED_TELEGRAM_VIP -> POSTED_ALL.
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

    for i in range(2, len(rows) + 1):
        if posted >= max_rows:
            break

        row_vals = rows[i - 1]
        row = {headers[c]: (row_vals[c] if c < len(row_vals) else "") for c in range(len(headers))}

        status = safe_get(row, "status").upper()
        if status != STATUS_POSTED_TELEGRAM_VIP:
            continue

        # Check if already posted to FREE
        if safe_get(row, "telegram_free_posted_at"):
            continue

        # Enforce 24h delay
        vip_ts = parse_ts(safe_get(row, "telegram_vip_posted_at"))
        if not vip_ts:
            log(f"Row {i}: Missing VIP timestamp, skipping")
            continue

        hours_since_vip = (now_utc() - vip_ts).total_seconds() / 3600.0
        if hours_since_vip < VIP_DELAY_HOURS:
            log(f"Row {i}: VIP posted {hours_since_vip:.1f}h ago, need {VIP_DELAY_HOURS}h")
            continue

        log(f"Posting to Telegram FREE: row {i} (delay: {hours_since_vip:.1f}h)")

        message = format_telegram_free(row)

        try:
            tg_send(TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL, message)

            ts = now_utc().replace(microsecond=0).isoformat() + "Z"
            updates = []
            if "telegram_free_posted_at" in hmap:
                updates.append(gspread.Cell(i, hmap["telegram_free_posted_at"], ts))
            if "status" in hmap:
                updates.append(gspread.Cell(i, hmap["status"], STATUS_POSTED_ALL))

            if updates:
                ws.update_cells(updates, value_input_option="USER_ENTERED")

            posted += 1
            log(f"Posted to FREE Telegram successfully")

        except Exception as e:
            log(f"Telegram FREE error: {e}")

    return posted


# =========================
# MAIN PIPELINE
# =========================
def main() -> None:
    log("=" * 70)
    log("TRAVELTXTER V4.2 UNIFIED PIPELINE - FINAL")
    log("=" * 70)
    log(f"Sheet: {SPREADSHEET_ID[:20]}...")
    log(f"Tab: {RAW_DEALS_TAB}")
    log(f"Duffel: {'ENABLED' if DUFFEL_ENABLED else 'DISABLED'}")
    log(f"RUN_SLOT: {RUN_SLOT} | VIP_DELAY: {VIP_DELAY_HOURS}h")
    log(f"Instagram: {'ENABLED' if (IG_ACCESS_TOKEN and IG_USER_ID) else 'DISABLED'}")
    log(f"Rendering: {'ENABLED' if RENDER_URL else 'DISABLED'}")
    log("=" * 70)

    try:
        ws, headers = get_ws()
        log(f"Connected to Google Sheets successfully")
        log(f"Columns: {len(headers)}")

        # Stage 1: Duffel Feed (insert NEW deals)
        log("\n--- STAGE 1: DUFFEL FEED ---")
        inserted = duffel_run_and_insert(ws, headers)
        log(f"✓ Inserted {inserted} new deals")

        # Stage 2: AI Scoring (NEW -> READY_TO_POST)
        log("\n--- STAGE 2: AI SCORING ---")
        scored = stage_scoring(ws, headers, max_rows=1)
        log(f"✓ Scored and promoted {scored} deals")

        # Stage 3: Rendering (READY_TO_POST -> READY_TO_PUBLISH)
        log("\n--- STAGE 3: RENDERING ---")
        rendered = stage_rendering(ws, headers, max_rows=1)
        log(f"✓ Rendered {rendered} graphics")

        # Stage 4: Instagram (READY_TO_PUBLISH -> POSTED_INSTAGRAM)
        log("\n--- STAGE 4: INSTAGRAM ---")
        ig_posted = stage_instagram(ws, headers, max_rows=1)
        log(f"✓ Posted {ig_posted} to Instagram")

        # Stage 5: Telegram VIP (POSTED_INSTAGRAM -> POSTED_TELEGRAM_VIP, AM only)
        log("\n--- STAGE 5: TELEGRAM VIP ---")
        vip_posted = stage_telegram_vip(ws, headers, max_rows=1)
        log(f"✓ Posted {vip_posted} to VIP")

        # Stage 6: Telegram FREE (POSTED_TELEGRAM_VIP -> POSTED_ALL, PM only, 24h delay)
        log("\n--- STAGE 6: TELEGRAM FREE ---")
        free_posted = stage_telegram_free(ws, headers, max_rows=1)
        log(f"✓ Posted {free_posted} to FREE")

        log("\n" + "=" * 70)
        log("PIPELINE COMPLETE")
        log("=" * 70)

    except Exception as e:
        log(f"FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
