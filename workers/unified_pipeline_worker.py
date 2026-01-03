#!/usr/bin/env python3
"""
TravelTxter V4.5 - PRODUCTION (feature-led, human copy rules)

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

# =========================
# V4.5: CONFIG_SIGNALS + Performance + MailerLite feed (sheet-only)
# =========================

CONFIG_SIGNALS_TAB = env("CONFIG_SIGNALS_TAB", "CONFIG_SIGNALS")
PERFORMANCE_SIGNALS_TAB = env("PERFORMANCE_SIGNALS_TAB", "PERFORMANCE_SIGNALS")
PERFORMANCE_SUMMARY_TAB = env("PERFORMANCE_SUMMARY_TAB", "PERFORMANCE_SUMMARY")
MAILERLITE_FEED_TAB = env("MAILERLITE_FEED_TAB", "MAILERLITE_FEED")

ENABLE_PERF_AGG = env("ENABLE_PERF_AGG", "1") == "1"
ENABLE_EMAIL_FEED = env("ENABLE_EMAIL_FEED", "1") == "1"

# Global caches (loaded once per run)
_CONFIG_SIGNALS_BY_DESTKEY: Dict[str, Dict[str, str]] = {}
_CONFIG_SIGNALS_BY_IATA: Dict[str, Dict[str, str]] = {}
_PERF_BOOST_MAP: Dict[Tuple[str, str, str], float] = {}  # (destination_key, theme, channel) -> boost


def _month_from_date(date_str: str) -> int:
    """Return 1-12 month from YYYY-MM-DD (or empty -> 0)."""
    try:
        return int(date_str.strip()[5:7])
    except Exception:
        return 0


def _get_cfg_value(cfg: Dict[str, str], prefix: str, month: int) -> str:
    if not cfg or month < 1 or month > 12:
        return ""
    k = f"{prefix}_m{month:02d}"
    return (cfg.get(k) or "").strip()


def load_config_signals(sh: gspread.Spreadsheet) -> None:
    """Load CONFIG_SIGNALS into fast lookup maps."""
    global _CONFIG_SIGNALS_BY_DESTKEY, _CONFIG_SIGNALS_BY_IATA
    _CONFIG_SIGNALS_BY_DESTKEY = {}
    _CONFIG_SIGNALS_BY_IATA = {}

    try:
        ws = sh.worksheet(CONFIG_SIGNALS_TAB)
    except Exception:
        log(f"CONFIG_SIGNALS tab not found: {CONFIG_SIGNALS_TAB} (skipping)")
        return

    vals = ws.get_all_values()
    if len(vals) < 2:
        return

    headers = vals[0]
    for r in vals[1:]:
        row = {headers[i]: (r[i] if i < len(r) else "") for i in range(len(headers))}
        dest_key = (row.get("destination_key") or "").strip()
        iata = (row.get("iata_hint") or "").strip().upper()

        if dest_key:
            _CONFIG_SIGNALS_BY_DESTKEY[dest_key] = row
        if iata:
            _CONFIG_SIGNALS_BY_IATA[iata] = row


def _lookup_cfg(row: Dict[str, str]) -> Dict[str, str]:
    dest_key = (safe_get(row, "destination_key") or "").strip()
    dest_iata = (safe_get(row, "destination_iata") or "").strip().upper()
    if dest_key and dest_key in _CONFIG_SIGNALS_BY_DESTKEY:
        return _CONFIG_SIGNALS_BY_DESTKEY[dest_key]
    if dest_iata and dest_iata in _CONFIG_SIGNALS_BY_IATA:
        return _CONFIG_SIGNALS_BY_IATA[dest_iata]
    return {}


def _theme_from_signals(cfg: Dict[str, str], month: int) -> str:
    """Pick a single theme from signals for this month (order matters)."""
    try:
        snow = int(_get_cfg_value(cfg, "snow_score", month) or "0")
        surf = int(_get_cfg_value(cfg, "surf_score", month) or "0")
        sun = int(_get_cfg_value(cfg, "sun_score", month) or "0")
    except Exception:
        snow = surf = sun = 0

    # Priority: activity-led themes first
    if snow >= 2:
        return "SNOW"
    if surf >= 2:
        return "SURF"
    if sun >= 2 and month in (11, 12, 1, 2, 3):
        return "WINTER_SUN"
    return "CITY"


def _activity_context(cfg: Dict[str, str], theme: str, month: int) -> str:
    """Honest, historical-context blurb (not live conditions)."""
    if not cfg or month < 1:
        return ""

    if theme == "SURF":
        wt = _get_cfg_value(cfg, "surf_water_temp_c", month)
        wl = _get_cfg_value(cfg, "surf_wave_level", month)
        ws = _get_cfg_value(cfg, "surf_wetsuit_note", month)
        bits = []
        if wt:
            bits.append(f"Avg water ~{wt}°C")
        if wl:
            bits.append(f"Waves: {wl.lower()}")
        if ws:
            bits.append(f"Wetsuit: {ws.replace('_','/').lower()}")
        return " · ".join(bits)

    if theme == "SNOW":
        st = _get_cfg_value(cfg, "snow_temp_c", month)
        sf = _get_cfg_value(cfg, "snow_snowfall_level", month)
        br = _get_cfg_value(cfg, "snow_base_reliability", month)
        bits = []
        if st:
            bits.append(f"Avg temps ~{st}°C")
        if sf:
            bits.append(f"Snowfall: {sf.lower()}")
        if br:
            bits.append(f"Base: {br.lower()}")
        return " · ".join(bits)

    # WINTER_SUN or CITY (sun context)
    tt = _get_cfg_value(cfg, "sun_temp_c", month)
    rl = _get_cfg_value(cfg, "sun_rain_level", month)
    sl = _get_cfg_value(cfg, "sun_sunshine_level", month)
    bits = []
    if tt:
        bits.append(f"Avg daytime ~{tt}°C")
    if sl:
        bits.append(f"Sun: {sl.lower()}")
    if rl:
        bits.append(f"Rain: {rl.lower()}")
    return " · ".join(bits)


def _timing_score(cfg: Dict[str, str], theme: str, month: int) -> int:
    if not cfg or month < 1:
        return 0
    key = "sun_score" if theme in ("WINTER_SUN", "CITY") else ("surf_score" if theme == "SURF" else "snow_score")
    try:
        return int(_get_cfg_value(cfg, key, month) or "0")
    except Exception:
        return 0


def _value_score(row: Dict[str, str]) -> int:
    """Simple value proxy from price. Conservative and explainable."""
    try:
        p = float(safe_get(row, "price_gbp") or "0")
    except Exception:
        return 0
    if p <= 60:
        return 3
    if p <= 120:
        return 2
    if p <= 200:
        return 1
    return 0


def performance_aggregate(sh: gspread.Spreadsheet) -> None:
    """Aggregate PERFORMANCE_SIGNALS -> PERFORMANCE_SUMMARY (fast, idempotent)."""
    if not ENABLE_PERF_AGG:
        return

    try:
        ws = sh.worksheet(PERFORMANCE_SIGNALS_TAB)
    except Exception:
        # Optional tab
        return

    try:
        out = sh.worksheet(PERFORMANCE_SUMMARY_TAB)
    except Exception:
        out = sh.add_worksheet(title=PERFORMANCE_SUMMARY_TAB, rows=2000, cols=20)

    vals = ws.get_all_values()
    if len(vals) < 2:
        return

    headers = vals[0]
    h = {name: idx for idx, name in enumerate(headers)}

    def get(r, k):
        i = h.get(k)
        return (r[i] if i is not None and i < len(r) else "").strip()

    VALID_CHANNELS = {"INSTAGRAM", "TELEGRAM", "EMAIL"}
    WEIGHTS = {"VIEW": 1, "CLICK": 2, "SAVE": 5, "JOIN": 8, "VIP_JOIN": 12}

    rows = []
    bad = 0

    for r in vals[1:]:
        ts = get(r, "timestamp_utc")
        dest = get(r, "destination_key") or get(r, "destination_iata")
        theme = get(r, "theme") or "GENERAL"
        channel = get(r, "channel").upper()
        metric = get(r, "metric_type").upper()
        try:
            value = float(get(r, "metric_value") or "0")
        except Exception:
            value = 0.0

        if not ts or not dest or channel not in VALID_CHANNELS or metric not in WEIGHTS:
            bad += 1
            continue

        try:
            dt_ts = dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            bad += 1
            continue

        weight = WEIGHTS[metric]
        score = value * weight
        rows.append((dt_ts, dest, theme, channel, score))

    # 14d half-life decay (configurable)
    half_life_days = float(env("PERF_HALF_LIFE_DAYS", "14") or "14")
    now = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)

    def decay_factor(ts):
        age_days = (now - ts).total_seconds() / 86400.0
        if age_days <= 0:
            return 1.0
        # exponential decay where half-life = half_life_days
        return 0.5 ** (age_days / max(1e-6, half_life_days))

    agg = {}
    for ts, dest, theme, channel, score in rows:
        key = (dest, theme, channel)
        agg[key] = agg.get(key, 0.0) + score * decay_factor(ts)

    # write summary
    out_headers = ["destination_key", "theme", "channel", "boost_decay"]
    out_rows = [out_headers]
    for (dest, theme, channel), boost in sorted(agg.items(), key=lambda x: x[1], reverse=True):
        out_rows.append([dest, theme, channel, round(boost, 3)])

    out.clear()
    out.update("A1", out_rows, value_input_option="USER_ENTERED")
    log(f"Performance aggregation: {len(out_rows)-1} rows (skipped {bad})")


def load_perf_boost_map(sh: gspread.Spreadsheet) -> None:
    global _PERF_BOOST_MAP
    _PERF_BOOST_MAP = {}

    try:
        ws = sh.worksheet(PERFORMANCE_SUMMARY_TAB)
    except Exception:
        return

    vals = ws.get_all_values()
    if len(vals) < 2:
        return

    headers = vals[0]
    h = {name: idx for idx, name in enumerate(headers)}

    def get(r, k):
        i = h.get(k)
        return (r[i] if i is not None and i < len(r) else "").strip()

    for r in vals[1:]:
        dest = get(r, "destination_key")
        theme = get(r, "theme") or "GENERAL"
        channel = get(r, "channel").upper()
        try:
            boost = float(get(r, "boost_decay") or "0")
        except Exception:
            boost = 0.0
        if dest and channel:
            _PERF_BOOST_MAP[(dest, theme, channel)] = boost


def perf_boost(destination_key: str, theme: str, channel: str) -> float:
    if not destination_key:
        return 0.0
    return float(_PERF_BOOST_MAP.get((destination_key, theme or "GENERAL", channel.upper()), 0.0) or 0.0)



def stage_update_mailerlite_feed(ws: gspread.Worksheet, headers: List[str]) -> None:
    """Write a clean weekly email feed into MAILERLITE_FEED (sheet-only).

    This does NOT send emails. It only writes structured rows MailerLite can consume.
    """
    if not ENABLE_EMAIL_FEED:
        return

    sh = ws.spreadsheet
    try:
        out = sh.worksheet(MAILERLITE_FEED_TAB)
    except Exception:
        out = sh.add_worksheet(title=MAILERLITE_FEED_TAB, rows=2000, cols=30)

    rows = ws.get_all_values()
    if len(rows) < 2:
        return

    hmap = header_map(headers)

    def get(row, k):
        return safe_get(row, k)

    # Collect scored rows (prefer NEW, but include READY_TO_POST if needed)
    candidates = []
    for idx in range(2, len(rows) + 1):
        rv = rows[idx - 1]
        r = {headers[c]: (rv[c] if c < len(rv) else "") for c in range(len(headers))}
        st = (get(r, "status") or "").upper()
        if st not in {STATUS_NEW, STATUS_READY_TO_POST, STATUS_READY_TO_PUBLISH}:
            continue
        sc = get(r, "ai_score") or ""
        if not sc:
            continue
        try:
            score = float(sc)
        except Exception:
            continue
        theme = (get(r, "theme") or "").strip().upper() or "CITY"
        dest_key = (get(r, "destination_key") or get(r, "destination_iata") or "").strip()
        boost = perf_boost(dest_key, theme, "EMAIL")
        final_score = score + max(0.0, min(20.0, boost))
        candidates.append((final_score, idx, r))

    candidates.sort(key=lambda x: x[0], reverse=True)
    top = candidates[: int(env("EMAIL_FEED_TOP_N", "12") or "12")]

    out_rows = [[
        "generated_utc",
        "destination_key",
        "theme",
        "headline",
        "blurb",
        "price_gbp",
        "origin_iata",
        "destination_iata",
        "outbound_date",
        "return_date",
        "booking_url",
        "context_blurb",
        "score"
    ]]

    generated = now_utc()
    for score, idx, r in top:
        dest_city = get(r, "destination_city") or get(r, "destination_iata")
        origin_city = get(r, "origin_city") or get(r, "origin_iata")
        price = round_price_up(get(r, "price_gbp"))
        theme = (get(r, "theme") or "").strip().upper() or "CITY"
        context = get(r, "context_blurb") or ""
        url = get(r, "booking_url") or get(r, "deep_link") or ""

        headline = f"{origin_city} → {dest_city} from £{price}"
        blurb_bits = []
        if get(r, "outbound_date") and get(r, "return_date"):
            blurb_bits.append(f"{get(r,'outbound_date')} to {get(r,'return_date')}")
        if theme:
            blurb_bits.append(theme.replace("_", " ").title())
        if context:
            blurb_bits.append(context)
        blurb = " · ".join([b for b in blurb_bits if b])

        out_rows.append([
            generated,
            get(r, "destination_key") or "",
            theme,
            headline,
            blurb,
            get(r, "price_gbp") or "",
            get(r, "origin_iata") or "",
            get(r, "destination_iata") or "",
            get(r, "outbound_date") or "",
            get(r, "return_date") or "",
            url,
            context,
            round(score, 2),
        ])

    out.clear()
    out.update("A1", out_rows, value_input_option="USER_ENTERED")
    log(f"MailerLite feed updated: {len(out_rows)-1} rows")




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
    """V4.5 scoring: deterministic signals first, optional AI only for light classification.

    Output fields are intentionally simple:
      - ai_score (0-100)
      - ai_verdict (GOOD / AVERAGE / POOR)
      - theme (SURF / SNOW / WINTER_SUN / CITY)
      - context_blurb (historical averages, never live claims)
      - score_components (JSON string)
      - ai_caption (kept for backward compatibility; generated deterministically)
    """
    cfg = _lookup_cfg(row)
    month = _month_from_date(safe_get(row, "outbound_date"))

    # Theme: prefer existing theme column; else infer from signals
    theme = (safe_get(row, "theme") or "").strip().upper()
    if not theme:
        theme = _theme_from_signals(cfg, month)

    # Component scores (0-3 each)
    value = _value_score(row)
    timing = _timing_score(cfg, theme, month)

    # Conservative weighting into a 0-100 score
    # Value matters most; timing is a kicker; performance is applied later at selection time
    score = (value * 25) + (timing * 10)

    # Light AI assist (optional): only to refine verdict banding on ambiguous prices
    if OPENAI_API_KEY:
        try:
            client = OpenAI(api_key=OPENAI_API_KEY)
            origin = safe_get(row, "origin_city") or safe_get(row, "origin_iata")
            dest = safe_get(row, "destination_city") or safe_get(row, "destination_iata")
            price = safe_get(row, "price_gbp")
            out_date = safe_get(row, "outbound_date")
            ret_date = safe_get(row, "return_date")
            stops = safe_get(row, "stops") or "0"

            prompt = f"""Classify this flight deal into one theme: SURF, SNOW, WINTER_SUN, CITY.
Return JSON with keys: theme (one of those), value_hint (0-3), notes (max 12 words).
Facts:
- From: {origin}
- To: {dest}
- Price GBP: {price}
- Dates: {out_date} to {ret_date}
- Stops (outbound): {stops}
Rules:
- Do not write marketing copy.
- Do not claim live conditions or scarcity.
"""

            resp = client.chat.completions.create(
                model=OPENAI_MODEL,
                temperature=0.2,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
            )
            obj = json.loads(resp.choices[0].message.content)
            t2 = str(obj.get("theme", "")).strip().upper()
            if t2 in {"SURF", "SNOW", "WINTER_SUN", "CITY"}:
                theme = t2

            try:
                vh = int(obj.get("value_hint", value))
                if 0 <= vh <= 3:
                    value = vh
            except Exception:
                pass

            score = (value * 25) + (timing * 10)
        except Exception as e:
            log(f"AI classify failed (non-fatal): {e}")

    verdict = "POOR"
    if score >= 70:
        verdict = "GOOD"
    elif score >= 40:
        verdict = "AVERAGE"

    context = _activity_context(cfg, theme, month)

    # Backwards-compatible caption: feature-led, plain.
    dest_city = safe_get(row, "destination_city") or safe_get(row, "destination_iata")
    price_txt = safe_get(row, "price_gbp")
    caption = f"Return flights to {dest_city} from £{round_price_up(price_txt)}"
    if context:
        caption += f". {context}."

    components = {
        "value_score_0_3": value,
        "timing_score_0_3": timing,
        "theme": theme,
        "month": month
    }

    return {
        "ai_score": int(round(score)),
        "ai_verdict": verdict,
        "ai_caption": caption,
        "theme": theme,
        "context_blurb": context,
        "score_components": json.dumps(components, separators=(",", ":"))
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
        if "theme" in hmap and result.get("theme"):
            updates.append(gspread.Cell(i, hmap["theme"], result["theme"]))
        if "context_blurb" in hmap and result.get("context_blurb") is not None:
            updates.append(gspread.Cell(i, hmap["context_blurb"], result.get("context_blurb","")))
        if "score_components" in hmap and result.get("score_components"):
            updates.append(gspread.Cell(i, hmap["score_components"], result["score_components"]))

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

        # V4.5: performance boost (ranking only; never treated as a factual claim)
        dest_key = (safe_get(row, "destination_key") or safe_get(row, "destination_iata") or "").strip()
        theme = (safe_get(row, "theme") or "GENERAL").strip().upper()
        boost = perf_boost(dest_key, theme, "TELEGRAM")
        # keep boost bounded so it can't swamp price/timing
        boosted_score = ai_score + max(0.0, min(20.0, boost))

        candidates.append((i, boosted_score, row))

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
    log("TRAVELTXTER V4.5 - PRODUCTION")
    log("=" * 70)
    log(f"RUN_SLOT: {RUN_SLOT} | VIP_DELAY: {VIP_DELAY_HOURS}h")
    log(f"Duffel: {'ON' if DUFFEL_ENABLED else 'OFF'} | OpenAI: {'ON' if OPENAI_API_KEY else 'OFF'}")
    log("=" * 70)

    try:
        ws, headers = get_ws()
        log(f"Connected | Columns: {len(headers)}")

        # V4.5: load destination signals + performance boosts
        sh = ws.spreadsheet
        load_config_signals(sh)
        performance_aggregate(sh)
        load_perf_boost_map(sh)

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

        # Stage 3b: Email feed (sheet-only)
        log(\"\n[3b] EMAIL FEED\")
        stage_update_mailerlite_feed(ws, headers)

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
