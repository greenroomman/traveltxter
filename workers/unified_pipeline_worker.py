#!/usr/bin/env python3
"""
TravelTxter V4.5.1 — PRODUCTION READY (Matches Real Schema)

SCHEMA MATCH:
- CONFIG_SIGNALS: Uses actual columns (sun_score_m01, surf_score_m01, etc.)
- Keying: Uses iata_hint (your actual key column)
- Theme derivation: Based on highest activity score per month
- Safe defaults: MAX_INSERTS=3, 48h recency filter

DEPLOY CHECKLIST:
[ ] Fix CONFIG: Bergen = BGO (not BOD)
[ ] Set DUFFEL_MAX_INSERTS=3
[ ] Verify STRIPE_LINK_MONTHLY and STRIPE_LINK_YEARLY
"""

import os
import json
import uuid
import datetime as dt
import time
import math
import hashlib
from typing import Dict, Any, List, Tuple, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials

try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False


# =========================
# SECRET NAME COMPATIBILITY
# =========================
# Handle variations in secret names between different deployments
def normalize_secrets():
    """Map actual GitHub secret names to worker expected names"""
    mappings = {
        # Worker expects → Your actual GitHub secret name(s)
        'SPREADSHEET_ID': ['SHEET_ID'],
        'IG_ACCESS_TOKEN': ['META_ACCESS_TOKEN', 'FB_ACCESS_TOKEN'],
        'GCP_SA_JSON': ['GCP_SA_JSON_ONE_LINE'],
    }
    
    for target, aliases in mappings.items():
        if not os.getenv(target):
            for alias in aliases:
                if os.getenv(alias):
                    os.environ[target] = os.getenv(alias)
                    break

normalize_secrets()


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
CONFIG_SIGNALS_TAB = env("CONFIG_SIGNALS_TAB", "CONFIG_SIGNALS")

GCP_SA_JSON = env("GCP_SA_JSON", required=True)

DUFFEL_API_KEY = env("DUFFEL_API_KEY", "")
DUFFEL_VERSION = env("DUFFEL_VERSION", "v2")
DUFFEL_MAX_INSERTS = int(env("DUFFEL_MAX_INSERTS", "3"))
DUFFEL_ROUTES_PER_RUN = int(env("DUFFEL_ROUTES_PER_RUN", "2"))
DUFFEL_ENABLED = env("DUFFEL_ENABLED", "true").lower() in ("1", "true", "yes")

OPENAI_API_KEY = env("OPENAI_API_KEY", "")
OPENAI_MODEL = env("OPENAI_MODEL", "gpt-4o-mini")

RENDER_URL = env("RENDER_URL", required=True)

IG_ACCESS_TOKEN = env("IG_ACCESS_TOKEN", required=True)
IG_USER_ID = env("IG_USER_ID", required=True)

TELEGRAM_BOT_TOKEN_VIP = env("TELEGRAM_BOT_TOKEN_VIP", required=True)
TELEGRAM_CHANNEL_VIP = env("TELEGRAM_CHANNEL_VIP", required=True)

TELEGRAM_BOT_TOKEN = env("TELEGRAM_BOT_TOKEN", required=True)
TELEGRAM_CHANNEL = env("TELEGRAM_CHANNEL", required=True)

SKYSCANNER_AFFILIATE_ID = env("SKYSCANNER_AFFILIATE_ID", "")
STRIPE_LINK_MONTHLY = env("STRIPE_LINK_MONTHLY", "") or env("STRIPE_LINK", "")  # Fallback to STRIPE_LINK
STRIPE_LINK_YEARLY = env("STRIPE_LINK_YEARLY", "") or env("STRIPE_LINK", "")    # Fallback to STRIPE_LINK

VIP_DELAY_HOURS = int(env("VIP_DELAY_HOURS", "24"))
RUN_SLOT = env("RUN_SLOT", "AM").upper()

# Editorial constraints
VARIETY_LOOKBACK_HOURS = int(env("VARIETY_LOOKBACK_HOURS", "72"))
DEST_REPEAT_PENALTY = float(env("DEST_REPEAT_PENALTY", "25.0"))
THEME_REPEAT_PENALTY = float(env("THEME_REPEAT_PENALTY", "10.0"))
RECENCY_FILTER_HOURS = int(env("RECENCY_FILTER_HOURS", "48"))


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
# Helpers
# =========================
def now_utc() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def log(msg: str) -> None:
    print(f"{now_utc()} | {msg}", flush=True)


def safe_get(row: Dict[str, str], key: str) -> str:
    return (row.get(key) or "").strip()


def safe_float(val: Any, default: float = 0.0) -> float:
    try:
        return float(val) if val else default
    except:
        return default


def round_price_up(price_str: str) -> int:
    try:
        return math.ceil(float(price_str))
    except:
        return 0


def format_date_yymmdd(date_str: str) -> str:
    try:
        if not date_str:
            return ""
        date_str = date_str.strip()
        
        if '/' in date_str:
            parts = date_str.split('/')
            if len(parts) == 3:
                mm, dd, yy = parts
                return f"{yy.zfill(2)}{mm.zfill(2)}{dd.zfill(2)}"
        
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
    if not ts:
        return 9999.0
    try:
        t = dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return (dt.datetime.now(dt.timezone.utc) - t).total_seconds() / 3600.0
    except:
        return 9999.0


def stable_hash(text: str) -> int:
    """Stable hash (not per-process random)"""
    return int(hashlib.md5(text.encode()).hexdigest(), 16)


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
# CONFIG_SIGNALS (Real Schema)
# =========================
def load_config_signals(ws_parent: gspread.Spreadsheet) -> Dict[str, Dict[str, Any]]:
    """
    Load CONFIG_SIGNALS using actual schema:
    - Key column: iata_hint
    - Score columns: sun_score_m01, surf_score_m01, snow_score_m01, etc.
    """
    try:
        signals_ws = ws_parent.worksheet(CONFIG_SIGNALS_TAB)
    except:
        log("CONFIG_SIGNALS tab not found")
        return {}

    try:
        records = signals_ws.get_all_records()
    except:
        log("CONFIG_SIGNALS empty")
        return {}

    signals_map = {}
    for rec in records:
        # Use iata_hint as primary key
        iata = (rec.get('iata_hint') or "").strip().upper()
        
        if iata:
            signals_map[iata] = rec

    log(f"Loaded CONFIG_SIGNALS for {len(signals_map)} destinations")
    return signals_map


def get_activity_scores(signals: Dict[str, Any], month: int) -> Dict[str, float]:
    """
    Get sun/surf/snow scores for a given month.
    
    Returns: {'sun': 0-3, 'surf': 0-3, 'snow': 0-3}
    """
    month_str = f"m{month:02d}"
    
    sun_score = safe_float(signals.get(f'sun_score_{month_str}'), 0.0)
    surf_score = safe_float(signals.get(f'surf_score_{month_str}'), 0.0)
    snow_score = safe_float(signals.get(f'snow_score_{month_str}'), 0.0)
    
    return {
        'sun': sun_score,
        'surf': surf_score,
        'snow': snow_score
    }


def derive_theme_from_signals(signals: Dict[str, Any], month: int) -> str:
    """
    Derive theme from CONFIG_SIGNALS based on highest activity score.
    
    Logic:
    1. Get sun/surf/snow scores for the month
    2. Pick activity with highest score (>1.0 threshold)
    3. Map to theme:
       - snow → snow
       - surf → surf
       - sun (in winter months) → winter_sun
       - sun (other months) → shoulder
       - all low → city_breaks
    
    Returns: winter_sun | surf | snow | city_breaks | shoulder
    """
    if not signals:
        return "shoulder"
    
    scores = get_activity_scores(signals, month)
    
    # Find dominant activity (must be >1.0 to be relevant)
    max_activity = max(scores.items(), key=lambda x: x[1])
    activity_name, activity_score = max_activity
    
    if activity_score <= 1.0:
        # No strong activity signal, default to city_breaks
        return "city_breaks"
    
    # Theme mapping
    if activity_name == 'snow' and activity_score >= 2.0:
        return "snow"
    
    if activity_name == 'surf' and activity_score >= 2.0:
        return "surf"
    
    if activity_name == 'sun':
        # Winter sun if it's winter months (Nov-Mar) and good score
        if month in [11, 12, 1, 2, 3] and activity_score >= 2.0:
            return "winter_sun"
        elif activity_score >= 2.5:
            return "winter_sun"  # Strong sun score any time
        else:
            return "shoulder"
    
    return "city_breaks"


# =========================
# CONFIG Routes
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

    routes = []
    
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
                days_ahead = 60

        if origin_iata and dest_iata:
            routes.append((priority, origin_iata, origin_city, dest_iata, dest_city, days_ahead))

    routes.sort(key=lambda x: x[0])
    return routes


def select_routes_rotating(routes: List[Tuple[int, str, str, str, str, int]], max_routes: int) -> List[Tuple[int, str, str, str, str, int]]:
    """Deterministic route rotation"""
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


def run_duffel_feeder(ws: gspread.Worksheet, headers: List[str]) -> int:
    """Feed deals with safe defaults (MAX_INSERTS=3)"""
    if not DUFFEL_ENABLED or not DUFFEL_API_KEY:
        log("Duffel: DISABLED")
        return 0

    sh = ws.spreadsheet
    routes = load_routes_from_config(sh)
    if not routes:
        log("No enabled routes in CONFIG")
        return 0

    selected_routes = select_routes_rotating(routes, DUFFEL_ROUTES_PER_RUN)
    log(f"Duffel: Searching {len(selected_routes)} routes (MAX_INSERTS={DUFFEL_MAX_INSERTS})")

    hmap = header_map(headers)
    total_inserted = 0

    for priority, origin_iata, origin_city, dest_iata, dest_city, days_ahead in selected_routes:
        today = dt.date.today()
        out_date = today + dt.timedelta(days=days_ahead)
        ret_date = out_date + dt.timedelta(days=5)

        log(f"  {origin_city} ({origin_iata}) → {dest_city} ({dest_iata})")
        
        try:
            data = duffel_offer_request(origin_iata, dest_iata, str(out_date), str(ret_date))
        except Exception as e:
            log(f"  Duffel error: {e}")
            continue

        # Handle Duffel response structure
        offers = []
        if isinstance(data, dict):
            if "data" in data:
                data_content = data["data"]
                if isinstance(data_content, dict) and "offers" in data_content:
                    offers = data_content["offers"]
                elif isinstance(data_content, list):
                    offers = data_content
            elif "offers" in data:
                offers = data["offers"]

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
            row_obj["destination_key"] = dest_iata
            row_obj["outbound_date"] = str(out_date)
            row_obj["return_date"] = str(ret_date)
            row_obj["price_gbp"] = price
            row_obj["trip_length_days"] = "5"
            row_obj["deal_source"] = "DUFFEL"
            row_obj["date_added"] = now_utc()
            row_obj["airline"] = airline
            row_obj["stops"] = stops
            row_obj["status"] = STATUS_NEW

            # Booking link
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
# AI Scoring
# =========================
def score_deal_with_ai(row: Dict[str, str]) -> Dict[str, Any]:
    """Simple AI scoring"""
    if not OPENAI_API_KEY or not OPENAI_AVAILABLE:
        return {
            "ai_score": 70,
            "ai_verdict": "GOOD",
            "ai_caption": f"Flights to {safe_get(row, 'destination_city')}"
        }

    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
    except:
        return {
            "ai_score": 70,
            "ai_verdict": "GOOD",
            "ai_caption": f"Flights to {safe_get(row, 'destination_city')}"
        }

    origin_city = safe_get(row, "origin_city") or safe_get(row, "origin_iata")
    dest_city = safe_get(row, "destination_city") or safe_get(row, "destination_iata")
    price = safe_get(row, "price_gbp")

    prompt = f"""
Score this UK flight deal (0-100):
- {origin_city} to {dest_city}
- £{price}

Return JSON:
{{"ai_score": 0-100, "ai_verdict": "EXCELLENT/GOOD/AVERAGE/POOR", "ai_caption": "1 sentence (max 100 chars, NO EMOJIS)"}}
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

        return {
            "ai_score": int(data.get("ai_score", 70)),
            "ai_verdict": str(data.get("ai_verdict", "GOOD")).upper(),
            "ai_caption": str(data.get("ai_caption", "")).strip() or f"Flights to {dest_city}"
        }
    except Exception as e:
        log(f"OpenAI error: {e}")
        return {
            "ai_score": 70,
            "ai_verdict": "GOOD",
            "ai_caption": f"Flights to {dest_city} for £{price}"
        }


def stage_score_all_new(ws: gspread.Worksheet, headers: List[str], signals_map: Dict[str, Dict]) -> int:
    """Score NEW deals and derive theme from CONFIG_SIGNALS"""
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

        # Derive theme from CONFIG_SIGNALS
        dest_iata = safe_get(row, "destination_iata")
        out_date = safe_get(row, "outbound_date")
        
        try:
            if '-' in out_date:
                month = int(out_date.split('-')[1])
            elif '/' in out_date:
                month = int(out_date.split('/')[0])
            else:
                month = dt.date.today().month
        except:
            month = dt.date.today().month
        
        signals = signals_map.get(dest_iata, {})
        auto_theme = derive_theme_from_signals(signals, month)
        
        log(f"  Scoring row {i}: {safe_get(row, 'destination_city')} (theme: {auto_theme})")

        # Score with AI
        result = score_deal_with_ai(row)

        # Update sheet
        updates = []
        if "ai_score" in hmap:
            updates.append(gspread.Cell(i, hmap["ai_score"], result["ai_score"]))
        if "ai_verdict" in hmap:
            updates.append(gspread.Cell(i, hmap["ai_verdict"], result["ai_verdict"]))
        if "ai_caption" in hmap:
            updates.append(gspread.Cell(i, hmap["ai_caption"], result["ai_caption"]))
        if "auto_theme" in hmap:
            updates.append(gspread.Cell(i, hmap["auto_theme"], auto_theme))
        if "destination_key" in hmap and not safe_get(row, "destination_key"):
            updates.append(gspread.Cell(i, hmap["destination_key"], dest_iata))

        if updates:
            ws.update_cells(updates, value_input_option="USER_ENTERED")

        scored_count += 1

    return scored_count


# =========================
# EDITORIAL SELECTION
# =========================
def get_recent_posts(rows: List[List[str]], headers: List[str], lookback_hours: int) -> Tuple[set, set]:
    """Get recently posted destinations and themes"""
    cutoff = dt.datetime.utcnow() - dt.timedelta(hours=lookback_hours)
    recent_dests = set()
    recent_themes = set()
    
    for i in range(1, len(rows)):
        row = {headers[c]: (rows[i][c] if c < len(rows[i]) else "") for c in range(len(headers))}
        
        status = safe_get(row, "status").upper()
        if status not in ["POSTED_INSTAGRAM", "POSTED_TELEGRAM_VIP", "POSTED_ALL"]:
            continue
        
        posted_ts = safe_get(row, "ig_published_timestamp") or safe_get(row, "tg_monthly_timestamp")
        if not posted_ts:
            continue
        
        try:
            posted_dt = dt.datetime.fromisoformat(posted_ts.replace("Z", "+00:00"))
            if posted_dt > cutoff:
                dest_key = safe_get(row, "destination_key") or safe_get(row, "destination_iata")
                theme = safe_get(row, "auto_theme") or safe_get(row, "theme") or ""
                
                if dest_key:
                    recent_dests.add(dest_key.upper())
                if theme:
                    recent_themes.add(theme.lower())
        except:
            pass
    
    return recent_dests, recent_themes


def stage_select_best(ws: gspread.Worksheet, headers: List[str]) -> int:
    """
    Editorial selection with variety enforcement.
    
    Key: Groups by (destination_key, theme), applies penalties, selects winner.
    """
    hmap = header_map(headers)
    rows = ws.get_all_values()
    if len(rows) < 2:
        return 0

    # Get recent posts
    recent_dests, recent_themes = get_recent_posts(rows, headers, VARIETY_LOOKBACK_HOURS)
    
    if recent_dests:
        log(f"  Recent destinations: {sorted(list(recent_dests))[:5]}")
    if recent_themes:
        log(f"  Recent themes: {sorted(list(recent_themes))}")

    # Collect candidates (with recency filter)
    recency_cutoff = dt.datetime.utcnow() - dt.timedelta(hours=RECENCY_FILTER_HOURS)
    candidates = []
    
    for i in range(2, len(rows) + 1):
        row_vals = rows[i - 1]
        row = {headers[c]: (row_vals[c] if c < len(row_vals) else "") for c in range(len(headers))}

        if safe_get(row, "status").upper() != STATUS_NEW:
            continue

        # Recency filter: only consider recent deals
        date_added = safe_get(row, "date_added")
        if date_added:
            try:
                added_dt = dt.datetime.fromisoformat(date_added.replace("Z", "+00:00"))
                if added_dt < recency_cutoff:
                    continue
            except:
                pass

        ai_score_str = safe_get(row, "ai_score")
        if not ai_score_str:
            continue

        try:
            ai_score = float(ai_score_str)
        except:
            continue

        dest_key = (safe_get(row, "destination_key") or safe_get(row, "destination_iata") or "").upper()
        resolved_theme = (safe_get(row, "auto_theme") or safe_get(row, "theme") or "").lower()
        
        if not dest_key:
            continue

        # Apply penalties
        final_score = ai_score
        penalties = []
        
        if dest_key in recent_dests:
            final_score -= DEST_REPEAT_PENALTY
            penalties.append(f"dest:{dest_key}")
        
        if resolved_theme in recent_themes:
            final_score -= THEME_REPEAT_PENALTY
            penalties.append(f"theme:{resolved_theme}")
        
        penalty_str = ", ".join(penalties) if penalties else "none"

        candidates.append({
            'row_idx': i,
            'dest_key': dest_key,
            'dest_city': safe_get(row, 'destination_city'),
            'theme': resolved_theme,
            'ai_score': ai_score,
            'final_score': final_score,
            'penalties': penalty_str,
            'row_data': row
        })

    if not candidates:
        log("  No scored NEW deals found")
        return 0

    # Group by (destination_key, theme)
    groups = {}
    for c in candidates:
        group_key = (c['dest_key'], c['theme'])
        if group_key not in groups:
            groups[group_key] = []
        groups[group_key].append(c)
    
    log(f"  Candidates: {len(candidates)} deals in {len(groups)} groups")

    # Select best from each group
    group_winners = []
    for group_key, group_deals in groups.items():
        group_deals.sort(key=lambda x: x['final_score'], reverse=True)
        winner = group_deals[0]
        group_winners.append(winner)
        
        log(f"    {group_key}: {winner['dest_city']} "
            f"(score: {winner['ai_score']:.0f} → {winner['final_score']:.0f}, "
            f"penalties: {winner['penalties']})")

    # Select best group winner
    group_winners.sort(key=lambda x: x['final_score'], reverse=True)
    selected = group_winners[0]
    
    log(f"  ✓ SELECTED: {selected['dest_city']} ({selected['dest_key']})")
    log(f"    Theme: {selected['theme']}")
    log(f"    Score: {selected['ai_score']:.0f} → {selected['final_score']:.0f}")
    
    # Promote
    if "status" in hmap:
        ws.update_cell(selected['row_idx'], hmap["status"], STATUS_READY_TO_POST)
        return 1

    return 0


# =========================
# Rendering
# =========================
def render_image(row: Dict[str, str]) -> str:
    payload = {
        "deal_id": safe_get(row, "deal_id"),
        "origin_city": safe_get(row, "origin_city") or safe_get(row, "origin_iata"),
        "destination_city": safe_get(row, "destination_city") or safe_get(row, "destination_iata"),
        "destination_country": safe_get(row, "destination_country"),
        "price_gbp": safe_get(row, "price_gbp"),
        "outbound_date": safe_get(row, "outbound_date"),
        "return_date": safe_get(row, "return_date"),
    }

    r = requests.post(RENDER_URL, json=payload, timeout=30)
    r.raise_for_status()
    return r.json().get("graphic_url", "")


def stage_render(ws: gspread.Worksheet, headers: List[str]) -> int:
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
# Instagram
# =========================
def instagram_caption_simple(row: Dict[str, str]) -> str:
    origin = safe_get(row, "origin_city") or safe_get(row, "origin_iata")
    dest = safe_get(row, "destination_city") or safe_get(row, "destination_iata")
    price_raw = safe_get(row, "price_gbp")
    price = f"£{round_price_up(price_raw)}"

    variants = [
        f"{origin} to {dest} for {price}. VIPs saw this yesterday.",
        f"{price} from {origin} to {dest}. Early access for VIP members.",
        f"{dest} at {price}. Our community saw this first.",
        f"{origin} to {dest}, {price}. VIPs got this 24h early.",
    ]

    # Stable hash
    idx = stable_hash(dest) % len(variants)
    return variants[idx]


def post_instagram(ws: gspread.Worksheet, headers: List[str]) -> int:
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
            continue

        log(f"  Posting to Instagram: row {i}")

        caption = instagram_caption_simple(row)
        cache_buster = int(time.time())
        image_url_cb = f"{graphic_url}?cb={cache_buster}"

        try:
            # Create media
            create_url = f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media"
            r1 = requests.post(create_url, data={
                "image_url": image_url_cb,
                "caption": caption[:2200],
                "access_token": IG_ACCESS_TOKEN,
            }, timeout=30)
            r1.raise_for_status()
            creation_id = r1.json().get("id")

            if not creation_id:
                continue

            # Poll status
            max_wait = 60
            waited = 0
            media_ready = False

            while waited < max_wait:
                r_status = requests.get(
                    f"https://graph.facebook.com/v20.0/{creation_id}",
                    params={"fields": "status_code", "access_token": IG_ACCESS_TOKEN},
                    timeout=10
                )
                
                if r_status.status_code == 200:
                    status_code = r_status.json().get("status_code", "")
                    if status_code == "FINISHED":
                        media_ready = True
                        break
                    elif status_code in ("ERROR", "EXPIRED"):
                        break
                
                time.sleep(2)
                waited += 2

            if not media_ready:
                log(f"  Media not ready after {waited}s")
                continue

            # Publish
            r2 = requests.post(
                f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media_publish",
                data={"creation_id": creation_id, "access_token": IG_ACCESS_TOKEN},
                timeout=30
            )
            r2.raise_for_status()

            updates = []
            if "ig_published_timestamp" in hmap:
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
# Telegram
# =========================
def tg_send(token: str, chat: str, text: str) -> None:
    r = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat, "text": text, "parse_mode": "HTML"},
        timeout=30,
    )
    r.raise_for_status()


def format_telegram_vip(row: Dict[str, str]) -> str:
    price_raw = safe_get(row, "price_gbp")
    dest_city = safe_get(row, "destination_city") or safe_get(row, "destination_iata")
    dest_country = safe_get(row, "destination_country")
    origin_city = safe_get(row, "origin_city") or safe_get(row, "origin_iata")
    out_date = safe_get(row, "outbound_date")
    ret_date = safe_get(row, "return_date")
    ai_caption = safe_get(row, "ai_caption") or "Good value for this route"
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

"""
    if booking_link:
        msg += f'<a href="{booking_link}">BOOK NOW</a>'

    return msg


def format_telegram_free(row: Dict[str, str]) -> str:
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

"""
    if booking_link:
        msg += f'<a href="{booking_link}">Book now</a>\n\n'

    msg += "<b>Want earlier access?</b>\n"
    msg += "• See deals 24h early\n"
    msg += "• Cancel anytime\n\n"

    if STRIPE_LINK_MONTHLY:
        msg += f'<a href="{STRIPE_LINK_MONTHLY}">Upgrade to VIP</a>'

    return msg


def post_telegram_vip(ws: gspread.Worksheet, headers: List[str]) -> int:
    """Post to VIP (AM only)"""
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

        if safe_get(row, "tg_monthly_timestamp"):
            continue

        log(f"  Posting to VIP: row {i}")

        try:
            msg = format_telegram_vip(row)
            tg_send(TELEGRAM_BOT_TOKEN_VIP, TELEGRAM_CHANNEL_VIP, msg)

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
    """Post to FREE (PM only, 24h delay)"""
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

        if safe_get(row, "tg_free_timestamp"):
            continue

        vip_ts = safe_get(row, "tg_monthly_timestamp")
        hours = hours_since(vip_ts)
        
        if hours < VIP_DELAY_HOURS:
            continue

        log(f"  Posting to FREE: row {i} (delay: {hours:.1f}h)")

        try:
            msg = format_telegram_free(row)
            tg_send(TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL, msg)

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
    log("TRAVELTXTER V4.5.1 PRODUCTION")
    log("=" * 70)
    log(f"RUN_SLOT: {RUN_SLOT} | VIP_DELAY: {VIP_DELAY_HOURS}h")
    log(f"Variety: {VARIETY_LOOKBACK_HOURS}h lookback, -{DEST_REPEAT_PENALTY} dest, -{THEME_REPEAT_PENALTY} theme")
    log(f"Duffel: MAX_INSERTS={DUFFEL_MAX_INSERTS}, ROUTES_PER_RUN={DUFFEL_ROUTES_PER_RUN}")
    log("=" * 70)

    try:
        ws, headers = get_ws()
        sh = ws.spreadsheet
        log(f"Connected | Columns: {len(headers)}")

        # Load CONFIG_SIGNALS
        signals_map = load_config_signals(sh)

        # Stage 1: Duffel
        if DUFFEL_ENABLED and DUFFEL_API_KEY:
            log("\n[1] DUFFEL FEED")
            inserted = run_duffel_feeder(ws, headers)
            log(f"✓ {inserted} deals inserted")

        # Stage 2: Scoring + Theme
        log("\n[2] SCORING + THEME")
        scored = stage_score_all_new(ws, headers, signals_map)
        log(f"✓ {scored} deals scored")

        # Stage 3: Editorial Selection
        log("\n[3] EDITORIAL SELECTION")
        selected = stage_select_best(ws, headers)
        log(f"✓ {selected} promoted")

        # Stage 4: Render
        log("\n[4] RENDER")
        rendered = stage_render(ws, headers)
        log(f"✓ {rendered} rendered")

        # Stage 5: Instagram
        log("\n[5] INSTAGRAM")
        ig_posted = post_instagram(ws, headers)
        log(f"✓ {ig_posted} posted")

        # Stage 6: Telegram VIP
        log("\n[6] TELEGRAM VIP")
        vip_posted = post_telegram_vip(ws, headers)
        log(f"✓ {vip_posted} posted")

        # Stage 7: Telegram FREE
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
