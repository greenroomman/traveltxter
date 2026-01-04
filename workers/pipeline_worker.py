#!/usr/bin/env python3
"""
TravelTxter V4.5.2 — WATERWHEEL PRODUCTION READY (Non-tech safe)

KEY FIXES (Dragon-proof):
1) DEADLOCK FIX:
   - NEW is only for "unprocessed"
   - After scoring, rows become SCORED
   - Editorial selection ONLY considers SCORED
   => Stops "publishing ancient rows" (e.g., row 544)

2) WATERWHEEL LOGIC:
   - Themes are SOFT WEIGHTS (preference), not hard filters
   - Price outliers can override theme (honest "wildcard")
   - Strong diversity circuit breaker:
       - hard-avoid last 3 destinations if possible
       - DEST_REPEAT_PENALTY default recommended = 50

3) DAILY THEMES:
   - A simple theme schedule per weekday
   - Still allows wildcards when value is exceptional

4) LOGGING:
   - Logs EXACT reason for choice (theme match vs wildcard vs repeat penalty)

SCHEMA MATCH:
- CONFIG_SIGNALS: iata_hint keys, and columns like sun_score_m01, surf_score_m01, snow_score_m01
- RAW_DEALS: status, ai_score/stotal, auto_theme, theme_strength, angle_type, etc.

NOTE:
- We DO NOT rely on Google Sheets formulas for theme (no resolved_theme required).
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

RENDER_URL = env("RENDER_URL", required=True)

IG_ACCESS_TOKEN = env("IG_ACCESS_TOKEN", required=True)
IG_USER_ID = env("IG_USER_ID", required=True)

TELEGRAM_BOT_TOKEN_VIP = env("TELEGRAM_BOT_TOKEN_VIP", required=True)
TELEGRAM_CHANNEL_VIP = env("TELEGRAM_CHANNEL_VIP", required=True)

TELEGRAM_BOT_TOKEN = env("TELEGRAM_BOT_TOKEN", required=True)
TELEGRAM_CHANNEL = env("TELEGRAM_CHANNEL", required=True)

SKYSCANNER_AFFILIATE_ID = env("SKYSCANNER_AFFILIATE_ID", "")

# Stripe links (your repo sometimes uses STRIPE_LINK only)
STRIPE_LINK_MONTHLY = env("STRIPE_LINK_MONTHLY", "") or env("STRIPE_LINK", "")
STRIPE_LINK_YEARLY = env("STRIPE_LINK_YEARLY", "") or env("STRIPE_LINK", "")

# Timing / posting
VIP_DELAY_HOURS = int(env("VIP_DELAY_HOURS", "24"))
RUN_SLOT = env("RUN_SLOT", "AM").upper()  # AM = VIP/IG path, PM = free release path

# Editorial constraints
VARIETY_LOOKBACK_HOURS = int(env("VARIETY_LOOKBACK_HOURS", "72"))
DEST_REPEAT_PENALTY = float(env("DEST_REPEAT_PENALTY", "50.0"))   # IMPORTANT: boredom breaker
THEME_REPEAT_PENALTY = float(env("THEME_REPEAT_PENALTY", "10.0"))
RECENCY_FILTER_HOURS = int(env("RECENCY_FILTER_HOURS", "48"))

# Scoring weights (baseline; price can override when outlier)
THEME_WEIGHT_BASE = float(env("THEME_WEIGHT_BASE", "0.60"))
PRICE_WEIGHT_BASE = float(env("PRICE_WEIGHT_BASE", "0.40"))


# =========================
# Status constants
# =========================
STATUS_NEW = "NEW"
STATUS_SCORED = "SCORED"  # <-- DEADLOCK FIX
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
        if val is None:
            return default
        s = str(val).strip()
        if s == "":
            return default
        return float(s)
    except:
        return default


def round_price_up(price_str: str) -> int:
    try:
        return math.ceil(float(str(price_str).strip()))
    except:
        return 0


def format_date_yymmdd(date_str: str) -> str:
    try:
        if not date_str:
            return ""
        date_str = date_str.strip()
        if "-" in date_str:
            parts = date_str.split("-")
            if len(parts) == 3:
                yy = parts[0][2:]
                mm = parts[1].zfill(2)
                dd = parts[2].zfill(2)
                return f"{yy}{mm}{dd}"
        if "/" in date_str:
            parts = date_str.split("/")
            if len(parts) == 3:
                mm, dd, yy = parts
                return f"{yy.zfill(2)}{mm.zfill(2)}{dd.zfill(2)}"
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
    return int(hashlib.md5(text.encode()).hexdigest(), 16)


def today_theme() -> str:
    """
    Daily themes (simple schedule).
    You can adjust this later via CONFIG if you want.
    """
    # Monday=0 ... Sunday=6
    dow = dt.datetime.utcnow().weekday()
    schedule = {
        0: "city_breaks",  # Mon
        1: "winter_sun",   # Tue
        2: "surf",         # Wed
        3: "snow",         # Thu
        4: "city_breaks",  # Fri
        5: "winter_sun",   # Sat
        6: "winter_sun",   # Sun
    }
    return schedule.get(dow, "city_breaks")


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
        raise RuntimeError("No headers found in RAW_DEALS")
    return ws, headers


def header_map(headers: List[str]) -> Dict[str, int]:
    return {h.strip(): i + 1 for i, h in enumerate(headers) if h.strip()}


# =========================
# CONFIG_SIGNALS (Real Schema)
# =========================
def load_config_signals(ws_parent: gspread.Spreadsheet) -> Dict[str, Dict[str, Any]]:
    def _safe_text(v: Any) -> str:
        if v is None:
            return ""
        if isinstance(v, float):
            try:
                if v != v:  # NaN
                    return ""
            except:
                return ""
            return ""
        try:
            return str(v)
        except:
            return ""

    try:
        signals_ws = ws_parent.worksheet(CONFIG_SIGNALS_TAB)
    except Exception:
        log("CONFIG_SIGNALS tab not found")
        return {}

    try:
        records = signals_ws.get_all_records()
    except Exception:
        log("CONFIG_SIGNALS empty/unreadable")
        return {}

    signals_map: Dict[str, Dict[str, Any]] = {}
    bad_rows = 0

    for rec in records:
        raw_iata = _safe_text(rec.get("iata_hint"))
        iata = raw_iata.strip().upper()
        if len(iata) == 3 and iata.isalpha():
            signals_map[iata] = rec
        else:
            if raw_iata != "":
                bad_rows += 1

    log(f"Loaded CONFIG_SIGNALS: {len(signals_map)} destinations (skipped {bad_rows} bad iata_hint rows)")
    return signals_map


def get_activity_scores(signals: Dict[str, Any], month: int) -> Dict[str, float]:
    month_str = f"m{month:02d}"
    sun_score = safe_float(signals.get(f"sun_score_{month_str}"), 0.0)
    surf_score = safe_float(signals.get(f"surf_score_{month_str}"), 0.0)
    snow_score = safe_float(signals.get(f"snow_score_{month_str}"), 0.0)
    return {"sun": sun_score, "surf": surf_score, "snow": snow_score}


def derive_theme_from_signals(signals: Dict[str, Any], month: int) -> str:
    """
    Returns: winter_sun | surf | snow | city_breaks | shoulder
    """
    if not signals:
        return "shoulder"

    scores = get_activity_scores(signals, month)
    activity_name, activity_score = max(scores.items(), key=lambda x: x[1])

    if activity_score <= 1.0:
        return "city_breaks"

    if activity_name == "snow" and activity_score >= 2.0:
        return "snow"

    if activity_name == "surf" and activity_score >= 2.0:
        return "surf"

    if activity_name == "sun":
        if month in [11, 12, 1, 2, 3] and activity_score >= 2.0:
            return "winter_sun"
        if activity_score >= 2.5:
            return "winter_sun"
        return "shoulder"

    return "city_breaks"


# =========================
# CONFIG Routes
# =========================
def load_routes_from_config(ws_parent: gspread.Spreadsheet) -> List[Tuple[int, str, str, str, str, int]]:
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
                days_ahead = 60

        if origin_iata and dest_iata:
            routes.append((priority, origin_iata, origin_city, dest_iata, dest_city, days_ahead))

    routes.sort(key=lambda x: x[0])
    return routes


def select_routes_rotating(routes: List[Tuple[int, str, str, str, str, int]], max_routes: int) -> List[Tuple[int, str, str, str, str, int]]:
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
DEFAULT_ROUTES = [
    (1, "LGW", "London", "KEF", "Reykjavik", "Iceland", 180, 5),
    (1, "LGW", "London", "BGO", "Bergen", "Norway", 150, 5),  # Bergen fixed to BGO
    (1, "MAN", "Manchester", "KEF", "Reykjavik", "Iceland", 200, 5),
    (2, "LGW", "London", "FAO", "Faro", "Portugal", 120, 5),
    (2, "LGW", "London", "TFS", "Tenerife", "Spain", 150, 5),
    (2, "MAN", "Manchester", "FAO", "Faro", "Portugal", 140, 5),
    (2, "LGW", "London", "BCN", "Barcelona", "Spain", 90, 3),
    (2, "MAN", "Manchester", "BCN", "Barcelona", "Spain", 100, 3),
]

def populate_config_if_empty(ws_spreadsheet, config_tab_name: str) -> int:
    try:
        cfg = ws_spreadsheet.worksheet(config_tab_name)
    except:
        log("Creating CONFIG tab...")
        cfg = ws_spreadsheet.add_worksheet(title=config_tab_name, rows=100, cols=20)
        headers = ["enabled","priority","origin_iata","origin_city","destination_iata","destination_city","destination_country","days_ahead","trip_length_days"]
        cfg.append_row(headers)

    try:
        values = cfg.get_all_values()
        if len(values) < 2:
            enabled_count = 0
        else:
            headers = [h.strip() for h in values[0]]
            enabled_idx = headers.index("enabled") if "enabled" in headers else 0
            enabled_count = sum(1 for row in values[1:] if len(row) > enabled_idx and (row[enabled_idx] or "").upper() in ("TRUE","1","YES"))
    except:
        enabled_count = 0

    if enabled_count >= 3:
        log(f"CONFIG: {enabled_count} enabled routes found")
        return 0

    log(f"CONFIG: Only {enabled_count} routes - auto-populating defaults")
    rows_to_add = []
    for route in DEFAULT_ROUTES:
        priority, origin_iata, origin_city, dest_iata, dest_city, dest_country, days_ahead, trip_length = route
        rows_to_add.append(["TRUE", str(priority), origin_iata, origin_city, dest_iata, dest_city, dest_country, str(days_ahead), str(trip_length)])

    if rows_to_add:
        cfg.append_rows(rows_to_add, value_input_option="USER_ENTERED")
    return len(rows_to_add)


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
    if not DUFFEL_ENABLED or not DUFFEL_API_KEY:
        log("Duffel: DISABLED (or missing key)")
        return 0

    sh = ws.spreadsheet
    routes_added = populate_config_if_empty(sh, CONFIG_TAB)
    if routes_added > 0:
        log(f"Auto-populated CONFIG with {routes_added} routes")

    routes = load_routes_from_config(sh)
    if not routes:
        log("CONFIG empty after auto-population")
        return 0

    selected_routes = select_routes_rotating(routes, DUFFEL_ROUTES_PER_RUN)
    log(f"Duffel: Searching {len(selected_routes)} routes (MAX_INSERTS={DUFFEL_MAX_INSERTS})")

    total_inserted = 0

    for priority, origin_iata, origin_city, dest_iata, dest_city, days_ahead in selected_routes:
        today = dt.date.today()
        out_date = today + dt.timedelta(days=days_ahead)
        ret_date = out_date + dt.timedelta(days=5)

        log(f"  {origin_city or origin_iata} ({origin_iata}) → {dest_city or dest_iata} ({dest_iata})")

        try:
            data = duffel_offer_request(origin_iata, dest_iata, str(out_date), str(ret_date))
        except Exception as e:
            log(f"  Duffel error: {e}")
            continue

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
            log("  0 offers")
            continue

        rows_to_append = []
        inserted_for_route = 0

        for off in offers:
            if inserted_for_route >= DUFFEL_MAX_INSERTS:
                break

            currency = off.get("total_currency") or "GBP"
            if currency != "GBP":
                continue

            price = off.get("total_amount") or ""
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
            row_obj["status"] = STATUS_NEW  # NEW = truly unprocessed

            out_formatted = str(out_date).replace("-", "")
            ret_formatted = str(ret_date).replace("-", "")
            base_url = f"https://www.skyscanner.net/transport/flights/{origin_iata}/{dest_iata}/{out_formatted}/{ret_formatted}/"
            booking_link = f"{base_url}?affiliateid={SKYSCANNER_AFFILIATE_ID}" if SKYSCANNER_AFFILIATE_ID else base_url

            row_obj["booking_link_vip"] = booking_link
            row_obj["booking_link_free"] = booking_link
            row_obj["affiliate_url"] = booking_link

            rows_to_append.append([row_obj.get(h, "") for h in headers])
            inserted_for_route += 1

        if rows_to_append:
            ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
            log(f"  Inserted {len(rows_to_append)} deals")
            total_inserted += len(rows_to_append)

    return total_inserted


# =========================
# SCORING (Elastic / Soft Theme Weighting)
# =========================
def value_score_from_price(price: float) -> float:
    """
    0..1
    Designed for UK audience short-haul + some long-haul.
    Cheap flights score higher; clamps safe.
    """
    if price <= 0:
        return 0.0
    if price < 10 or price > 1200:
        return 0.0
    # Normalise around 300 being "meh"; below is better
    return max(0.0, min(1.0, (350.0 - price) / 340.0))


def route_score_from_stops(stops: float) -> float:
    if stops <= 0:
        return 0.90
    if stops <= 1:
        return 0.60
    return 0.30


def timing_score_from_outbound(outbound_date: str, today: dt.date) -> float:
    """
    0..1
    Prefers 2-12 weeks out but still allows other dates.
    """
    try:
        dep = dt.date.fromisoformat(outbound_date)
        days = (dep - today).days
        if 14 <= days <= 90:
            return 0.80
        if 7 <= days <= 180:
            return 0.60
        if 181 <= days <= 365:
            return 0.45
        return 0.30
    except:
        return 0.50


def theme_alignment_score(deal_theme: str, target_theme: str) -> float:
    """
    Soft alignment:
    - exact match: 1.0
    - shoulder/city can be adjacent to most: 0.55
    - otherwise mismatch: 0.25
    """
    d = (deal_theme or "").strip().lower()
    t = (target_theme or "").strip().lower()
    if not d or not t:
        return 0.25
    if d == t:
        return 1.0
    # Adjacent logic (simple, safe)
    if t == "winter_sun" and d in ("shoulder", "city_breaks"):
        return 0.55
    if t == "city_breaks" and d in ("shoulder",):
        return 0.55
    if t in ("surf", "snow") and d in ("shoulder", "city_breaks"):
        return 0.45
    return 0.25


def determine_strength_and_angle(align: float, val: float, route: float) -> Tuple[str, str]:
    """
    Returns (theme_strength, angle_type)
    theme_strength: primary | adjacent | wildcard
    angle_type: classic_theme | smart_alternative | price_led | routing_win | quiet_season
    """
    if align >= 0.90:
        return "primary", "classic_theme"

    if align >= 0.50:
        # adjacent
        return "adjacent", "smart_alternative"

    # wildcard
    if val >= 0.70:
        return "wildcard", "price_led"
    if route >= 0.80:
        return "wildcard", "routing_win"
    return "wildcard", "price_led"


def score_deal_elastic(row: Dict[str, str], deal_theme: str, target_theme: str) -> Dict[str, Any]:
    """
    Elastic scorer:
    - Base weights: Theme 0.60, Price 0.40
    - BUT if price is an outlier (very high value_score), price weight can dominate.
    """
    price = safe_float(safe_get(row, "price_gbp") or safe_get(row, "price"), 0.0)
    stops = safe_float(safe_get(row, "stops"), 0.0)

    today = dt.date.today()
    outbound_date = safe_get(row, "outbound_date")

    align = theme_alignment_score(deal_theme, target_theme)
    val = value_score_from_price(price)
    route = route_score_from_stops(stops)
    timing = timing_score_from_outbound(outbound_date, today)

    # Adaptive weighting:
    # If value score is very strong, allow it to override theme.
    theme_w = THEME_WEIGHT_BASE
    price_w = PRICE_WEIGHT_BASE
    if val >= 0.85:
        theme_w = 0.20
        price_w = 0.80
    elif val >= 0.70:
        theme_w = 0.40
        price_w = 0.60

    # Composite score (0..100)
    stotal = (
        (align * theme_w) +
        (val * price_w) +
        (route * 0.10) +
        (timing * 0.10)
    ) * 100.0

    strength, angle = determine_strength_and_angle(align, val, route)

    return {
        "stotal": round(stotal, 1),
        "ai_score": round(stotal, 1),
        "theme_alignment": round(align, 3),
        "value_score": round(val, 3),
        "route_score": round(route, 3),
        "timing_score": round(timing, 3),
        "theme_strength": strength,
        "angle_type": angle,
        "ai_verdict": "EXCELLENT" if stotal >= 90 else ("VERY_GOOD" if stotal >= 80 else ("GOOD" if stotal >= 70 else "ACCEPTABLE")),
    }


def stage_score_all_new(ws: gspread.Worksheet, headers: List[str], signals_map: Dict[str, Dict]) -> int:
    """
    DEADLOCK FIX:
    - Only reads NEW
    - Writes scores + auto_theme
    - Then sets status -> SCORED
    """
    hmap = header_map(headers)
    rows = ws.get_all_values()
    if len(rows) < 2:
        return 0

    scored_count = 0
    target = today_theme()

    for i in range(2, len(rows) + 1):
        row_vals = rows[i - 1]
        row = {headers[c]: (row_vals[c] if c < len(row_vals) else "") for c in range(len(headers))}

        if safe_get(row, "status").upper() != STATUS_NEW:
            continue

        # derive month
        out_date = safe_get(row, "outbound_date")
        try:
            month = int(out_date.split("-")[1]) if "-" in out_date else dt.date.today().month
        except:
            month = dt.date.today().month

        dest_iata = safe_get(row, "destination_iata").upper()
        signals = signals_map.get(dest_iata, {})
        deal_theme = derive_theme_from_signals(signals, month)

        result = score_deal_elastic(row, deal_theme=deal_theme, target_theme=target)

        log(f"  Scoring row {i}: {safe_get(row, 'destination_city')} | deal_theme={deal_theme} | target_theme={target} | score={result['ai_score']}")

        updates: List[gspread.Cell] = []

        def put(col: str, val: Any):
            if col in hmap:
                updates.append(gspread.Cell(i, hmap[col], val))

        put("stotal", result["stotal"])
        put("ai_score", result["ai_score"])
        put("ai_verdict", result["ai_verdict"])
        put("theme_alignment", result["theme_alignment"])
        put("value_score", result["value_score"])
        put("route_score", result["route_score"])
        put("timing_score", result["timing_score"])
        put("theme_strength", result["theme_strength"])
        put("angle_type", result["angle_type"])

        put("auto_theme", deal_theme)
        put("target_theme", target)  # optional helper column if you create it
        put("scored_timestamp", now_utc())

        # Ensure destination_key set
        if "destination_key" in hmap and not safe_get(row, "destination_key"):
            put("destination_key", dest_iata)

        # DEADLOCK FIX: move NEW -> SCORED
        put("status", STATUS_SCORED)

        if updates:
            ws.update_cells(updates, value_input_option="USER_ENTERED")
            scored_count += 1

    return scored_count


# =========================
# EDITORIAL SELECTION (Diversity Force + Soft Weight)
# =========================
def get_recent_posts(rows: List[List[str]], headers: List[str], lookback_hours: int) -> Tuple[set, set, List[str]]:
    """
    Returns:
      recent_dests: destinations posted within lookback
      recent_themes: themes posted within lookback
      last3_dests: most recent 3 destinations (hard avoid if possible)
    """
    cutoff = dt.datetime.utcnow() - dt.timedelta(hours=lookback_hours)
    recent_dests = set()
    recent_themes = set()
    timeline: List[Tuple[dt.datetime, str]] = []

    for i in range(1, len(rows)):
        row = {headers[c]: (rows[i][c] if c < len(rows[i]) else "") for c in range(len(headers))}
        status = safe_get(row, "status").upper()
        if status not in (STATUS_POSTED_INSTAGRAM, STATUS_POSTED_TELEGRAM_VIP, STATUS_POSTED_ALL):
            continue

        posted_ts = safe_get(row, "ig_published_timestamp") or safe_get(row, "tg_monthly_timestamp") or safe_get(row, "tg_free_timestamp")
        if not posted_ts:
            continue

        try:
            posted_dt = dt.datetime.fromisoformat(posted_ts.replace("Z", "+00:00"))
        except:
            continue

        dest_key = (safe_get(row, "destination_key") or safe_get(row, "destination_iata") or "").upper()
        theme = (safe_get(row, "auto_theme") or safe_get(row, "theme") or "").lower()

        # Timeline always for last-3
        if dest_key:
            timeline.append((posted_dt, dest_key))

        # lookback sets
        if posted_dt > cutoff:
            if dest_key:
                recent_dests.add(dest_key)
            if theme:
                recent_themes.add(theme)

    # last 3 destinations by most recent timestamp
    timeline.sort(key=lambda x: x[0], reverse=True)
    last3 = [d for _, d in timeline[:3]]

    return recent_dests, recent_themes, last3


def stage_select_best(ws: gspread.Worksheet, headers: List[str]) -> int:
    """
    Selection logic:
    - ONLY considers SCORED (deadlock fix)
    - Recency filter on date_added
    - Applies repeat penalties (DEST + THEME)
    - HARD avoid last 3 destinations if possible
    - Still allows price-led wildcard to break theme on big value
    """
    hmap = header_map(headers)
    rows = ws.get_all_values()
    if len(rows) < 2:
        return 0

    recent_dests, recent_themes, last3 = get_recent_posts(rows, headers, VARIETY_LOOKBACK_HOURS)

    if last3:
        log(f"  Last 3 destinations: {last3}")
    if recent_dests:
        log(f"  Recent destinations (lookback): {sorted(list(recent_dests))[:8]}")
    if recent_themes:
        log(f"  Recent themes (lookback): {sorted(list(recent_themes))}")

    recency_cutoff = dt.datetime.utcnow() - dt.timedelta(hours=RECENCY_FILTER_HOURS)
    target = today_theme()

    candidates = []

    for i in range(2, len(rows) + 1):
        row_vals = rows[i - 1]
        row = {headers[c]: (row_vals[c] if c < len(row_vals) else "") for c in range(len(headers))}

        if safe_get(row, "status").upper() != STATUS_SCORED:
            continue

        # Recency filter (only recently added deals)
        date_added = safe_get(row, "date_added")
        if date_added:
            try:
                added_dt = dt.datetime.fromisoformat(date_added.replace("Z", "+00:00"))
                if added_dt < recency_cutoff:
                    continue
            except:
                pass

        ai_score = safe_float(safe_get(row, "ai_score"), 0.0)
        if ai_score <= 0:
            continue

        dest_key = (safe_get(row, "destination_key") or safe_get(row, "destination_iata") or "").upper()
        deal_theme = (safe_get(row, "auto_theme") or "").lower()

        if not dest_key:
            continue

        # Base score is ai_score (already elastic)
        final_score = ai_score
        reasons = []

        # Diversity penalties
        if dest_key in recent_dests:
            final_score -= DEST_REPEAT_PENALTY
            reasons.append(f"repeat_dest(-{DEST_REPEAT_PENALTY})")

        if deal_theme and deal_theme in recent_themes:
            final_score -= THEME_REPEAT_PENALTY
            reasons.append(f"repeat_theme(-{THEME_REPEAT_PENALTY})")

        # Extra hard avoid for "last 3" boredom loop
        hard_block = dest_key in last3
        if hard_block:
            # we don't automatically discard yet; we mark it and only use if necessary
            reasons.append("hard_block(last3)")

        # Store
        candidates.append({
            "row_idx": i,
            "dest_key": dest_key,
            "dest_city": safe_get(row, "destination_city") or dest_key,
            "deal_theme": deal_theme or "unknown",
            "ai_score": ai_score,
            "final_score": final_score,
            "hard_block": hard_block,
            "reasons": ", ".join(reasons) if reasons else "none",
            "row_data": row
        })

    if not candidates:
        log("  No SCORED candidates found (nothing to select)")
        return 0

    # First try: any non-hard-block candidates?
    non_blocked = [c for c in candidates if not c["hard_block"]]
    pool = non_blocked if non_blocked else candidates

    # Rank by final_score
    pool.sort(key=lambda x: x["final_score"], reverse=True)
    selected = pool[0]

    # WHY chosen
    log(f"  Candidates total: {len(candidates)} | non-blocked: {len(non_blocked)}")
    log(f"  ✓ SELECTED: {selected['dest_city']} ({selected['dest_key']})")
    log(f"    deal_theme={selected['deal_theme']} | target_theme={target}")
    log(f"    score={selected['ai_score']:.1f} → final={selected['final_score']:.1f}")
    log(f"    selection_notes={selected['reasons']}")
    if selected["hard_block"] and non_blocked == []:
        log("    NOTE: Forced to pick from last-3 destinations because nothing else qualified")

    # Promote
    if "status" in hmap:
        ws.update_cell(selected["row_idx"], hmap["status"], STATUS_READY_TO_POST)
        if "selected_timestamp" in hmap:
            ws.update_cell(selected["row_idx"], hmap["selected_timestamp"], now_utc())
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
    price = f"£{round_price_up(safe_get(row, 'price_gbp'))}"

    # Use angle_type to keep it feeling varied + honest
    angle = (safe_get(row, "angle_type") or "").lower()
    variants = []

    if angle == "price_led":
        variants = [
            f"{origin} → {dest} from {price}. Price dipped into the good zone — worth a look.",
            f"{price} {origin} → {dest}. Not a promise, just a solid price right now.",
        ]
    elif angle == "routing_win":
        variants = [
            f"{origin} → {dest} from {price}. Good routing for the money (less faff).",
            f"{price} {origin} → {dest}. If you hate connections, this one’s tidy.",
        ]
    elif angle == "smart_alternative":
        variants = [
            f"{origin} → {dest} from {price}. Theme-adjacent, but the value is real.",
            f"{price} to {dest} from {origin}. Not the obvious pick — that’s the point.",
        ]
    else:
        variants = [
            f"{origin} → {dest} from {price}. Simple, bookable, and actually usable dates.",
            f"{price} from {origin} to {dest}. Clean little find.",
        ]

    idx = stable_hash(dest + angle) % len(variants)
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

            # Poll
            waited = 0
            media_ready = False
            while waited < 60:
                r_status = requests.get(
                    f"https://graph.facebook.com/v20.0/{creation_id}",
                    params={"fields": "status_code", "access_token": IG_ACCESS_TOKEN},
                    timeout=10
                )
                if r_status.status_code == 200:
                    sc = r_status.json().get("status_code", "")
                    if sc == "FINISHED":
                        media_ready = True
                        break
                    if sc in ("ERROR", "EXPIRED"):
                        break
                time.sleep(2)
                waited += 2

            if not media_ready:
                log(f"  Media not ready after {waited}s")
                continue

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
            log("  ✓ Posted to Instagram")

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
    price_display = round_price_up(safe_get(row, "price_gbp"))
    dest_city = safe_get(row, "destination_city") or safe_get(row, "destination_iata")
    dest_country = safe_get(row, "destination_country")
    origin_city = safe_get(row, "origin_city") or safe_get(row, "origin_iata")
    out_date = format_date_yymmdd(safe_get(row, "outbound_date"))
    ret_date = format_date_yymmdd(safe_get(row, "return_date"))
    booking_link = safe_get(row, "booking_link_vip") or safe_get(row, "affiliate_url")

    angle = (safe_get(row, "angle_type") or "").replace("_", " ").strip()
    theme = (safe_get(row, "auto_theme") or "").replace("_", " ").strip()

    dest_display = f"{dest_city}, {dest_country}" if dest_country else dest_city

    msg = f"""£{price_display} to {dest_display}

<b>FROM:</b> {origin_city}
<b>OUT:</b>  {out_date}
<b>BACK:</b> {ret_date}

<b>Angle:</b> {angle or "value"}
<b>Theme:</b> {theme or "wildcard"}

"""
    if booking_link:
        msg += f'<a href="{booking_link}">BOOK NOW</a>'

    return msg


def format_telegram_free(row: Dict[str, str]) -> str:
    price_display = round_price_up(safe_get(row, "price_gbp"))
    dest_city = safe_get(row, "destination_city") or safe_get(row, "destination_iata")
    dest_country = safe_get(row, "destination_country")
    origin_city = safe_get(row, "origin_city") or safe_get(row, "origin_iata")
    out_date = format_date_yymmdd(safe_get(row, "outbound_date"))
    ret_date = format_date_yymmdd(safe_get(row, "return_date"))
    booking_link = safe_get(row, "booking_link_free") or safe_get(row, "affiliate_url")

    dest_display = f"{dest_city}, {dest_country}" if dest_country else dest_city

    msg = f"""£{price_display} to {dest_display}

<b>FROM:</b> {origin_city}
<b>OUT:</b>  {out_date}
<b>BACK:</b> {ret_date}

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
    # VIP posts in AM run only
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
            tg_send(TELEGRAM_BOT_TOKEN_VIP, TELEGRAM_CHANNEL_VIP, format_telegram_vip(row))

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
            log("  ✓ Posted to VIP")

        except Exception as e:
            log(f"  VIP error: {e}")

    return posted


def post_telegram_free(ws: gspread.Worksheet, headers: List[str]) -> int:
    # Free posts in PM run only after VIP delay
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
        if hours_since(vip_ts) < VIP_DELAY_HOURS:
            continue

        log(f"  Posting to FREE: row {i}")
        try:
            tg_send(TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL, format_telegram_free(row))

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
            log("  ✓ Posted to FREE")

        except Exception as e:
            log(f"  FREE error: {e}")

    return posted


# =========================
# MAIN
# =========================
def main():
    log("=" * 70)
    log("TRAVELTXTER V4.5.2 — WATERWHEEL RUN")
    log("=" * 70)
    log(f"RUN_SLOT: {RUN_SLOT} | VIP_DELAY: {VIP_DELAY_HOURS}h")
    log(f"Daily Theme (target): {today_theme()}")
    log(f"Variety: lookback={VARIETY_LOOKBACK_HOURS}h | DEST_REPEAT_PENALTY={DEST_REPEAT_PENALTY} | THEME_REPEAT_PENALTY={THEME_REPEAT_PENALTY}")
    log(f"Duffel: ENABLED={DUFFEL_ENABLED} | MAX_INSERTS={DUFFEL_MAX_INSERTS} | ROUTES_PER_RUN={DUFFEL_ROUTES_PER_RUN}")
    log("=" * 70)

    ws, headers = get_ws()
    sh = ws.spreadsheet
    log(f"Connected to sheet | Columns: {len(headers)} | Tab: {RAW_DEALS_TAB}")

    signals_map = load_config_signals(sh)

    # [1] Duffel
    if DUFFEL_ENABLED and DUFFEL_API_KEY:
        log("\n[1] DUFFEL FEED")
        inserted = run_duffel_feeder(ws, headers)
        log(f"✓ {inserted} deals inserted")
    else:
        log("\n[1] DUFFEL FEED")
        log("✓ skipped (disabled or missing DUFFEL_API_KEY)")

    # [2] Score NEW -> SCORED
    log("\n[2] SCORING (NEW → SCORED)")
    scored = stage_score_all_new(ws, headers, signals_map)
    log(f"✓ {scored} deals scored")

    # [3] Select SCORED -> READY_TO_POST
    log("\n[3] EDITORIAL SELECTION (SCORED → READY_TO_POST)")
    selected = stage_select_best(ws, headers)
    log(f"✓ {selected} promoted")

    # [4] Render
    log("\n[4] RENDER (READY_TO_POST → READY_TO_PUBLISH)")
    rendered = stage_render(ws, headers)
    log(f"✓ {rendered} rendered")

    # [5] Instagram
    log("\n[5] INSTAGRAM (READY_TO_PUBLISH → POSTED_INSTAGRAM)")
    ig_posted = post_instagram(ws, headers)
    log(f"✓ {ig_posted} posted")

    # [6] Telegram VIP
    log("\n[6] TELEGRAM VIP (AM only)")
    vip_posted = post_telegram_vip(ws, headers)
    log(f"✓ {vip_posted} posted")

    # [7] Telegram FREE
    log("\n[7] TELEGRAM FREE (PM only, delayed)")
    free_posted = post_telegram_free(ws, headers)
    log(f"✓ {free_posted} posted")

    log("\n" + "=" * 70)
    log("COMPLETE")
    log("=" * 70)


if __name__ == "__main__":
    main()
