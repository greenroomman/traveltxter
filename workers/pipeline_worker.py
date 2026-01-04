#!/usr/bin/env python3
"""
TravelTxter V4.5.3 — WATERWHEEL (Theme rotation + soft weighting + diversity force)

GOALS (non-tech summary):
- Feeder pulls enough variety (within Duffel free tier) so scoring can “sift like a human”
- Themes are a PREFERENCE, not a hard gate (price outliers can break through)
- Diversity penalties stop “boring Iceland loops”
- Works from CONFIG first (product), then pipeline backwards to IG/TG output

REQUIRED SHEET TABS:
- RAW_DEALS
- CONFIG
- CONFIG_SIGNALS (optional but supported)

CONFIG (expected columns; flexible aliases supported):
- enabled (TRUE/YES/1)
- priority (1..n)
- origin_iata (e.g., LGW)
- origin_city (optional)
- destination_iata (e.g., TFS)
- destination_city (optional)
- theme (e.g., winter_sun / snow / surf / city / foodie / longhaul_asia / etc.)
- region (optional: americas/asia/africa/australasia/europe)
- included_airlines (optional CSV: "U2,FR,W6,LS")
- max_connections (optional int; 0 = direct only)
- min_days_ahead / max_days_ahead (optional; else days_ahead)
- trip_length_days (optional; default 5)
"""

import os
import json
import uuid
import time
import math
import hashlib
import datetime as dt
from typing import Dict, Any, List, Tuple, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# ENV + COMPAT (your repo has mixed secret naming historically)
# ============================================================

def _set_if_missing(target: str, source: str) -> None:
    if not os.getenv(target) and os.getenv(source):
        os.environ[target] = os.getenv(source)

# Compatibility shims (safe)
_set_if_missing("SPREADSHEET_ID", "SHEET_ID")               # Some builds used SHEET_ID for spreadsheet key
_set_if_missing("IG_ACCESS_TOKEN", "META_ACCESS_TOKEN")     # Some builds used META_ACCESS_TOKEN
_set_if_missing("TELEGRAM_CHANNEL_VIP", "TELEGRAM_VIP_CHANNEL")
_set_if_missing("TELEGRAM_CHANNEL", "TELEGRAM_FREE_CHANNEL")

def env(name: str, default: str = "", required: bool = False) -> str:
    v = (os.getenv(name) or "").strip()
    if not v:
        v = default
    if required and not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v

# Core
SPREADSHEET_ID      = env("SPREADSHEET_ID", required=True)
RAW_DEALS_TAB       = env("RAW_DEALS_TAB", "RAW_DEALS")
CONFIG_TAB          = env("CONFIG_TAB", "CONFIG")
CONFIG_SIGNALS_TAB  = env("CONFIG_SIGNALS_TAB", "CONFIG_SIGNALS")
GCP_SA_JSON         = env("GCP_SA_JSON", required=True)

# Duffel
DUFFEL_API_KEY        = env("DUFFEL_API_KEY", "")
DUFFEL_VERSION        = env("DUFFEL_VERSION", "v2")
DUFFEL_ENABLED        = env("DUFFEL_ENABLED", "true").lower() in ("1", "true", "yes")
DUFFEL_MAX_INSERTS    = int(env("DUFFEL_MAX_INSERTS", "3"))        # safety for free tier
DUFFEL_ROUTES_PER_RUN = int(env("DUFFEL_ROUTES_PER_RUN", "3"))     # safety for free tier

# Posting
RENDER_URL   = env("RENDER_URL", required=True)
IG_ACCESS_TOKEN = env("IG_ACCESS_TOKEN", required=True)
IG_USER_ID      = env("IG_USER_ID", required=True)

TELEGRAM_BOT_TOKEN_VIP = env("TELEGRAM_BOT_TOKEN_VIP", required=True)
TELEGRAM_CHANNEL_VIP   = env("TELEGRAM_CHANNEL_VIP", required=True)
TELEGRAM_BOT_TOKEN     = env("TELEGRAM_BOT_TOKEN", required=True)
TELEGRAM_CHANNEL       = env("TELEGRAM_CHANNEL", required=True)

# Monetisation links
STRIPE_LINK_MONTHLY = env("STRIPE_LINK_MONTHLY", "") or env("STRIPE_LINK", "")
STRIPE_LINK_YEARLY  = env("STRIPE_LINK_YEARLY", "") or env("STRIPE_LINK", "")

# Timing + waterwheel behaviour
RUN_SLOT        = env("RUN_SLOT", "AM").upper()  # AM or PM (set by workflow)
VIP_DELAY_HOURS = int(env("VIP_DELAY_HOURS", "24"))

# Variety / anti-boring controls
VARIETY_LOOKBACK_HOURS = int(env("VARIETY_LOOKBACK_HOURS", "72"))
DEST_REPEAT_PENALTY    = float(env("DEST_REPEAT_PENALTY", "50.0"))     # strong circuit breaker
THEME_REPEAT_PENALTY   = float(env("THEME_REPEAT_PENALTY", "30.0"))
RECENCY_FILTER_HOURS   = int(env("RECENCY_FILTER_HOURS", "72"))

# Elastic scoring weights (theme is preferred, but outliers can win)
THEME_WEIGHT_BASE = float(env("THEME_WEIGHT_BASE", "0.60"))
PRICE_WEIGHT_BASE = float(env("PRICE_WEIGHT_BASE", "0.40"))
OUTLIER_BOOST_THRESHOLD = float(env("OUTLIER_BOOST_THRESHOLD", "0.35"))  # if deal is “very cheap” -> price dominates


# ============================================================
# Status constants (tolerant)
# ============================================================

STATUS_NEW               = "NEW"
STATUS_SCORED            = "SCORED"
STATUS_READY_TO_POST     = "READY_TO_POST"
STATUS_READY_TO_PUBLISH  = "READY_TO_PUBLISH"
STATUS_POSTED_INSTAGRAM  = "POSTED_INSTAGRAM"
STATUS_POSTED_TELEGRAM_VIP = "POSTED_TELEGRAM_VIP"
STATUS_POSTED_ALL        = "POSTED_ALL"


# ============================================================
# Utilities (safe + timezone correct)
# ============================================================

def now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def log(msg: str) -> None:
    print(f"{now_utc_iso()} | {msg}", flush=True)

def _safe_text(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float):
        # NaN
        if v != v:
            return ""
        # floats are not valid IATA anyway
        return ""
    try:
        return str(v)
    except Exception:
        return ""

def safe_get(row: Dict[str, Any], key: str) -> str:
    return (_safe_text(row.get(key))).strip()

def safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        s = _safe_text(v).strip()
        if not s:
            return default
        return float(s)
    except Exception:
        return default

def stable_hash(text: str) -> int:
    return int(hashlib.md5(text.encode("utf-8")).hexdigest(), 16)

def parse_iso_utc(ts: str) -> Optional[dt.datetime]:
    """
    Returns timezone-aware UTC datetime, or None.
    Accepts: "2026-01-04T11:44:38Z" or ISO with offset.
    """
    if not ts:
        return None
    s = ts.strip()
    try:
        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        d = dt.datetime.fromisoformat(s)
        if d.tzinfo is None:
            # treat as UTC if naive
            d = d.replace(tzinfo=dt.timezone.utc)
        return d.astimezone(dt.timezone.utc)
    except Exception:
        return None

def hours_since(ts: str) -> float:
    d = parse_iso_utc(ts)
    if not d:
        return 9999.0
    return (dt.datetime.now(dt.timezone.utc) - d).total_seconds() / 3600.0

def round_price_up(p: Any) -> int:
    try:
        return math.ceil(float(_safe_text(p)))
    except Exception:
        return 0


# ============================================================
# Google Sheets
# ============================================================

def gs_client() -> gspread.Client:
    creds = Credentials.from_service_account_info(
        json.loads(GCP_SA_JSON),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)

def get_sheet() -> Tuple[gspread.Spreadsheet, gspread.Worksheet, List[str]]:
    client = gs_client()
    sh = client.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(RAW_DEALS_TAB)
    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("RAW_DEALS has no headers row")
    return sh, ws, headers

def header_map(headers: List[str]) -> Dict[str, int]:
    return {h.strip(): i + 1 for i, h in enumerate(headers) if h.strip()}

def row_dict(headers: List[str], row_vals: List[str]) -> Dict[str, str]:
    d = {}
    for i, h in enumerate(headers):
        d[h] = row_vals[i] if i < len(row_vals) else ""
    return d


# ============================================================
# Theme Rotation (includes longhaul regions)
# ============================================================

def daily_theme(date: dt.date, run_slot: str) -> str:
    """
    Rotation that:
    - keeps core themes frequent
    - introduces longhaul region rotations (esp weekends)
    """
    dow = date.weekday()  # Mon=0
    # Two slots per day: AM “hero”, PM can be “secondary”
    schedule_am = {
        0: "city",            # Mon
        1: "winter_sun",      # Tue
        2: "surf",            # Wed
        3: "snow",            # Thu
        4: "foodie",          # Fri
        5: "longhaul_americas",  # Sat
        6: "longhaul_asia",      # Sun
    }
    schedule_pm = {
        0: "winter_sun",
        1: "city",
        2: "city",
        3: "winter_sun",
        4: "city",
        5: "longhaul_africa",
        6: "longhaul_australasia",
    }
    return (schedule_am if run_slot == "AM" else schedule_pm).get(dow, "city")


# ============================================================
# CONFIG Loader (robust to duplicates/messy headers)
# ============================================================

def load_config_routes(sh: gspread.Spreadsheet) -> List[Dict[str, Any]]:
    """
    Reads CONFIG using raw values so duplicate headers don't kill us.
    """
    try:
        ws = sh.worksheet(CONFIG_TAB)
    except Exception:
        log("CONFIG tab not found")
        return []

    values = ws.get_all_values()
    if not values or len(values) < 2:
        log("CONFIG empty")
        return []

    headers = [h.strip().lower() for h in values[0]]
    rows = values[1:]

    def col(*names: str) -> Optional[int]:
        for n in names:
            n = n.strip().lower()
            if n in headers:
                return headers.index(n)
        return None

    ix_enabled = col("enabled")
    ix_priority = col("priority")
    ix_origin_iata = col("origin_iata", "origin")
    ix_origin_city = col("origin_city")
    ix_dest_iata = col("destination_iata", "dest_iata", "destination")
    ix_dest_city = col("destination_city")
    ix_theme = col("theme")  # if duplicated, we take first
    ix_region = col("region")
    ix_airlines = col("included_airlines", "airlines", "carrier_bias")
    ix_max_conn = col("max_connections", "max_conn", "connections")
    ix_min_days = col("min_days_ahead", "days_ahead_min")
    ix_max_days = col("max_days_ahead", "days_ahead_max")
    ix_days_ahead = col("days_ahead")
    ix_trip_len = col("trip_length_days", "trip_length")

    out: List[Dict[str, Any]] = []

    for r in rows:
        enabled = (r[ix_enabled] if ix_enabled is not None and ix_enabled < len(r) else "").strip().upper()
        if enabled not in ("TRUE", "YES", "1"):
            continue

        origin = (r[ix_origin_iata] if ix_origin_iata is not None and ix_origin_iata < len(r) else "").strip().upper()
        dest   = (r[ix_dest_iata] if ix_dest_iata is not None and ix_dest_iata < len(r) else "").strip().upper()

        # Only real airport codes (3 letters)
        if len(origin) != 3 or len(dest) != 3:
            continue

        theme = (r[ix_theme] if ix_theme is not None and ix_theme < len(r) else "").strip().lower()
        region = (r[ix_region] if ix_region is not None and ix_region < len(r) else "").strip().lower()

        priority = 50
        if ix_priority is not None and ix_priority < len(r):
            try:
                priority = int((r[ix_priority] or "50").strip())
            except Exception:
                priority = 50

        airlines_raw = (r[ix_airlines] if ix_airlines is not None and ix_airlines < len(r) else "").strip()
        included_airlines = [a.strip().upper() for a in airlines_raw.split(",") if a.strip()]

        max_connections = None
        if ix_max_conn is not None and ix_max_conn < len(r):
            try:
                max_connections = int((r[ix_max_conn] or "").strip())
            except Exception:
                max_connections = None

        # days ahead window
        min_days = None
        max_days = None
        if ix_min_days is not None and ix_min_days < len(r):
            try:
                min_days = int((r[ix_min_days] or "").strip())
            except Exception:
                min_days = None
        if ix_max_days is not None and ix_max_days < len(r):
            try:
                max_days = int((r[ix_max_days] or "").strip())
            except Exception:
                max_days = None

        if (min_days is None or max_days is None) and ix_days_ahead is not None and ix_days_ahead < len(r):
            # fallback: single days_ahead means "depart around then"
            try:
                d = int((r[ix_days_ahead] or "").strip())
                min_days = min_days if min_days is not None else max(7, d - 10)
                max_days = max_days if max_days is not None else (d + 20)
            except Exception:
                pass

        # final defaults
        if min_days is None:
            min_days = 14
        if max_days is None:
            max_days = 60

        trip_len = 5
        if ix_trip_len is not None and ix_trip_len < len(r):
            try:
                trip_len = int((r[ix_trip_len] or "5").strip())
            except Exception:
                trip_len = 5

        origin_city = (r[ix_origin_city] if ix_origin_city is not None and ix_origin_city < len(r) else "").strip()
        dest_city   = (r[ix_dest_city] if ix_dest_city is not None and ix_dest_city < len(r) else "").strip()

        out.append({
            "priority": priority,
            "origin_iata": origin,
            "origin_city": origin_city,
            "destination_iata": dest,
            "destination_city": dest_city,
            "theme": theme,
            "region": region,
            "included_airlines": included_airlines,
            "max_connections": max_connections,
            "min_days_ahead": min_days,
            "max_days_ahead": max_days,
            "trip_length_days": trip_len
        })

    out.sort(key=lambda x: x["priority"])
    log(f"CONFIG: Loaded {len(out)} enabled routes")
    return out


def pick_routes_for_today(routes: List[Dict[str, Any]], target_theme: str, max_routes: int) -> List[Dict[str, Any]]:
    """
    Soft theme matching:
    - Prefer exact theme
    - Then adjacent themes
    - Then wildcard anything

    Also rotates deterministically so you don’t keep hammering same origins.
    """
    if not routes:
        return []

    # Define adjacency
    adjacent = {
        "winter_sun": ["surf", "city", "foodie"],
        "snow": ["city", "foodie"],
        "surf": ["winter_sun", "city"],
        "city": ["foodie", "winter_sun"],
        "foodie": ["city"],
        "longhaul_americas": ["winter_sun", "city"],
        "longhaul_asia": ["winter_sun", "city"],
        "longhaul_africa": ["winter_sun", "surf"],
        "longhaul_australasia": ["surf", "winter_sun"],
    }.get(target_theme, ["city"])

    exact = [r for r in routes if (r.get("theme") or "").strip().lower() == target_theme]
    adj   = [r for r in routes if (r.get("theme") or "").strip().lower() in adjacent]
    wild  = [r for r in routes if r not in exact and r not in adj]

    pool = exact + adj + wild
    if len(pool) <= max_routes:
        return pool

    day = dt.date.today().timetuple().tm_yday
    slot_offset = 0 if RUN_SLOT == "AM" else 1
    start = ((day * 2) + slot_offset) % len(pool)

    picked = []
    for i in range(max_routes):
        picked.append(pool[(start + i) % len(pool)])
    return picked


# ============================================================
# Duffel (tries quality params first; retries if Duffel rejects)
# NOTE: Duffel supports airline filtering in offer requests. :contentReference[oaicite:0]{index=0}
# ============================================================

def duffel_offer_request(payload: Dict[str, Any]) -> Dict[str, Any]:
    url = "https://api.duffel.com/air/offer_requests"
    headers = {
        "Authorization": f"Bearer {DUFFEL_API_KEY}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Duffel-Version": DUFFEL_VERSION,
    }
    r = requests.post(url, headers=headers, json={"data": payload}, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"Duffel {r.status_code}: {r.text[:500]}")
    return r.json()

def extract_offers(resp: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not isinstance(resp, dict):
        return []
    data = resp.get("data")
    if isinstance(data, dict):
        offers = data.get("offers")
        if isinstance(offers, list):
            return offers
    return []

def build_offer_payload(origin: str, dest: str, out_date: str, ret_date: str,
                        max_connections: Optional[int],
                        included_airlines: List[str]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "slices": [
            {"origin": origin, "destination": dest, "departure_date": out_date},
            {"origin": dest, "destination": origin, "departure_date": ret_date},
        ],
        "passengers": [{"type": "adult"}],
        "cabin_class": "economy",
    }

    # Quality constraint: direct-only if asked
    if max_connections is not None:
        payload["max_connections"] = max_connections

    # Carrier bias (Duffel supports included_airlines) :contentReference[oaicite:1]{index=1}
    if included_airlines:
        payload["included_airlines"] = included_airlines

    return payload

def run_duffel_feeder(ws: gspread.Worksheet, headers: List[str], routes_today: List[Dict[str, Any]]) -> int:
    if not DUFFEL_ENABLED or not DUFFEL_API_KEY:
        log("Duffel: DISABLED or missing DUFFEL_API_KEY")
        return 0

    hmap = header_map(headers)
    inserted_total = 0

    log(f"Duffel: Searching {len(routes_today)} routes (MAX_INSERTS={DUFFEL_MAX_INSERTS})")

    for rt in routes_today:
        origin = rt["origin_iata"]
        dest   = rt["destination_iata"]
        trip_len = int(rt.get("trip_length_days") or 5)

        # Pick a departure date inside the window (rotates inside range)
        today = dt.date.today()
        min_days = int(rt.get("min_days_ahead") or 14)
        max_days = int(rt.get("max_days_ahead") or 60)
        if max_days < min_days:
            max_days = min_days

        # Deterministic offset inside window
        span = max(1, (max_days - min_days))
        offset = stable_hash(f"{origin}-{dest}-{today.isoformat()}-{RUN_SLOT}") % span
        depart = today + dt.timedelta(days=(min_days + offset))
        ret    = depart + dt.timedelta(days=trip_len)

        out_date = str(depart)
        ret_date = str(ret)

        max_conn = rt.get("max_connections", None)
        airlines = rt.get("included_airlines", []) or []

        log(f"  {origin} → {dest} | theme={rt.get('theme','')} | airlines={','.join(airlines) or 'none'} | max_conn={max_conn}")

        # Try best-quality query first, then fail-open
        payload = build_offer_payload(origin, dest, out_date, ret_date, max_conn, airlines)

        try:
            resp = duffel_offer_request(payload)
        except Exception as e1:
            log(f"    Duffel rejected constrained query: {e1}")
            # retry without max_connections
            try:
                payload2 = build_offer_payload(origin, dest, out_date, ret_date, None, airlines)
                resp = duffel_offer_request(payload2)
                log("    Retried without max_connections: OK")
            except Exception as e2:
                log(f"    Duffel retry failed: {e2}")
                continue

        offers = extract_offers(resp)
        if not offers:
            log("    0 offers")
            continue

        rows_to_append = []
        for off in offers:
            if len(rows_to_append) >= DUFFEL_MAX_INSERTS:
                break

            price = off.get("total_amount")
            currency = off.get("total_currency") or "GBP"
            if currency != "GBP":
                continue

            owner = off.get("owner") or {}
            airline_name = owner.get("name") or ""

            # Stops proxy (outbound segments - 1)
            stops = "0"
            try:
                slices = off.get("slices") or []
                segs = (slices[0].get("segments") or [])
                stops = str(max(0, len(segs) - 1))
            except Exception:
                pass

            # Build row object with only known headers
            row_obj = {h: "" for h in headers}

            deal_id = str(uuid.uuid4())
            row_obj["deal_id"] = deal_id
            row_obj["origin_iata"] = origin
            row_obj["destination_iata"] = dest
            row_obj["origin_city"] = rt.get("origin_city") or origin
            row_obj["destination_city"] = rt.get("destination_city") or dest
            row_obj["price_gbp"] = str(price)
            row_obj["outbound_date"] = out_date
            row_obj["return_date"] = ret_date
            row_obj["trip_length_days"] = str(trip_len)
            row_obj["stops"] = stops
            row_obj["airline"] = airline_name
            row_obj["deal_source"] = "DUFFEL"
            row_obj["date_added"] = now_utc_iso()
            row_obj["status"] = STATUS_NEW

            # For downstream: theme hints from CONFIG
            if "theme" in row_obj:
                row_obj["theme"] = (rt.get("theme") or "").strip().lower()
            if "resolved_theme" in row_obj:
                row_obj["resolved_theme"] = (rt.get("theme") or "").strip().lower()
            if "destination_key" in row_obj:
                row_obj["destination_key"] = dest

            # Basic booking link (safe placeholder if you don’t have affiliate enrichment yet)
            out_fmt = out_date.replace("-", "")
            ret_fmt = ret_date.replace("-", "")
            booking = f"https://www.skyscanner.net/transport/flights/{origin}/{dest}/{out_fmt}/{ret_fmt}/"
            if "affiliate_url" in row_obj:
                row_obj["affiliate_url"] = booking
            if "booking_link_vip" in row_obj:
                row_obj["booking_link_vip"] = booking
            if "booking_link_free" in row_obj:
                row_obj["booking_link_free"] = booking

            rows_to_append.append([row_obj.get(h, "") for h in headers])

        if rows_to_append:
            ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
            inserted_total += len(rows_to_append)
            log(f"    Inserted {len(rows_to_append)} deals")

    return inserted_total


# ============================================================
# Elastic scoring (theme preferred, but outliers can win)
# ============================================================

def elastic_score(deal_theme: str, target_theme: str, price_gbp: float, stops: float) -> Tuple[float, Dict[str, float]]:
    """
    Returns score 0..100 and components.
    - Theme match drives brand consistency
    - Price can dominate if it’s an outlier
    - Stops penalized
    """
    # theme match score (soft)
    theme_match = 1.0 if deal_theme == target_theme else (0.55 if deal_theme else 0.40)

    # price score (rough but practical): cheaper is better
    # scale assumes typical short haul 30..250
    if price_gbp <= 0:
        price_score = 0.20
    else:
        price_score = max(0.0, min(1.0, (260.0 - price_gbp) / 230.0))  # 30->~1, 260->0

    # outlier boost: if very cheap, let price override theme a bit
    # (e.g., £59 to Jordan or £199 longhaul)
    outlier = 1.0 if price_score >= (1.0 - OUTLIER_BOOST_THRESHOLD) else 0.0
    theme_w = THEME_WEIGHT_BASE * (1.0 - 0.35 * outlier)
    price_w = PRICE_WEIGHT_BASE * (1.0 + 0.35 * outlier)

    # route score
    route_score = 1.0 if stops <= 0 else (0.75 if stops <= 1 else 0.45)

    # final
    score = (theme_w * theme_match + price_w * price_score + 0.15 * route_score) / (theme_w + price_w + 0.15)
    return round(score * 100.0, 1), {
        "theme_match": round(theme_match, 3),
        "price_score": round(price_score, 3),
        "route_score": round(route_score, 3),
        "theme_w": round(theme_w, 3),
        "price_w": round(price_w, 3),
    }


def stage_score(ws: gspread.Worksheet, headers: List[str], target_theme: str) -> int:
    hmap = header_map(headers)
    rows = ws.get_all_values()
    if len(rows) < 2:
        return 0

    scored = 0
    for i in range(2, len(rows) + 1):
        row = row_dict(headers, rows[i - 1])
        status = safe_get(row, "status").upper()

        if status not in (STATUS_NEW, STATUS_SCORED):
            continue

        # already scored?
        if safe_get(row, "ai_score") or safe_get(row, "stotal"):
            continue

        deal_theme = (safe_get(row, "resolved_theme") or safe_get(row, "theme")).lower()
        price = safe_float(safe_get(row, "price_gbp"), 0.0)
        stops = safe_float(safe_get(row, "stops"), 1.0)

        score, comps = elastic_score(deal_theme, target_theme, price, stops)

        updates = []
        def put(col: str, val: Any):
            if col in hmap:
                updates.append(gspread.Cell(i, hmap[col], str(val)))

        put("ai_score", score)
        put("stotal", score)
        put("theme_score", comps["theme_match"])
        put("value_score", comps["price_score"])
        put("route_score", comps["route_score"])
        put("score_components", json.dumps(comps))
        put("theme_final", deal_theme or target_theme)
        put("resolved_theme", deal_theme or target_theme)
        put("scored_timestamp", now_utc_iso())
        put("status", STATUS_SCORED)

        if updates:
            ws.update_cells(updates, value_input_option="USER_ENTERED")

        scored += 1

    return scored


# ============================================================
# Variety tracking (timezone-safe)
# ============================================================

def get_recent_posts(rows: List[List[str]], headers: List[str], lookback_hours: int) -> Tuple[set, set, List[str]]:
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=lookback_hours)
    recent_dests = set()
    recent_themes = set()
    last3_dests: List[str] = []

    # Walk newest->oldest for last3
    for i in range(len(rows) - 1, 0, -1):
        row = row_dict(headers, rows[i])
        status = safe_get(row, "status").upper()
        if status not in (STATUS_POSTED_INSTAGRAM, STATUS_POSTED_TELEGRAM_VIP, STATUS_POSTED_ALL):
            continue

        ts = safe_get(row, "ig_published_timestamp") or safe_get(row, "tg_monthly_timestamp") or safe_get(row, "published_timestamp")
        posted_dt = parse_iso_utc(ts)
        if not posted_dt:
            continue

        if posted_dt > cutoff:
            dest = (safe_get(row, "destination_key") or safe_get(row, "destination_iata")).upper()
            theme = (safe_get(row, "theme_final") or safe_get(row, "resolved_theme") or safe_get(row, "theme")).lower()

            if dest:
                recent_dests.add(dest)
                if len(last3_dests) < 3:
                    last3_dests.append(dest)
            if theme:
                recent_themes.add(theme)

        if len(last3_dests) >= 3 and posted_dt <= cutoff:
            break

    return recent_dests, recent_themes, last3_dests


# ============================================================
# Editorial selection (diversity force + fail-open)
# ============================================================

def stage_select_best(ws: gspread.Worksheet, headers: List[str], target_theme: str) -> int:
    hmap = header_map(headers)
    rows = ws.get_all_values()
    if len(rows) < 2:
        return 0

    recent_dests, recent_themes, last3 = get_recent_posts(rows, headers, VARIETY_LOOKBACK_HOURS)

    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=RECENCY_FILTER_HOURS)

    candidates = []
    for i in range(2, len(rows) + 1):
        row = row_dict(headers, rows[i - 1])

        status = safe_get(row, "status").upper()
        if status not in (STATUS_NEW, STATUS_SCORED):
            continue

        # Must be scored
        score = safe_float(safe_get(row, "ai_score") or safe_get(row, "stotal"), 0.0)
        if score <= 0:
            continue

        # Recency
        added = parse_iso_utc(safe_get(row, "date_added"))
        if added and added < cutoff:
            continue

        dest = (safe_get(row, "destination_key") or safe_get(row, "destination_iata")).upper()
        theme = (safe_get(row, "theme_final") or safe_get(row, "resolved_theme") or safe_get(row, "theme")).lower()
        if not dest:
            continue

        # Soft weighting: prefer target theme but allow break-through
        theme_match_bonus = 12.0 if theme == target_theme else (5.0 if theme else 0.0)

        final_score = score + theme_match_bonus

        penalties = []
        if dest in recent_dests:
            final_score -= DEST_REPEAT_PENALTY
            penalties.append(f"dest_repeat:{dest}")
        if theme in recent_themes and theme:
            final_score -= THEME_REPEAT_PENALTY
            penalties.append(f"theme_repeat:{theme}")
        if dest in last3:
            final_score -= (DEST_REPEAT_PENALTY * 0.75)
            penalties.append(f"last3_block:{dest}")

        candidates.append((final_score, i, dest, theme, score, ", ".join(penalties) if penalties else "none"))

    if not candidates:
        log("  No candidates found (scored NEW/SCORED).")
        return 0

    candidates.sort(key=lambda x: x[0], reverse=True)

    picked = candidates[0]
    final_score, row_idx, dest, theme, base_score, penalties = picked

    log(f"  ✓ SELECTED row {row_idx}: dest={dest} theme={theme or 'unknown'} base={base_score:.1f} final={final_score:.1f} penalties={penalties}")

    updates = []
    if "status" in hmap:
        updates.append(gspread.Cell(row_idx, hmap["status"], STATUS_READY_TO_POST))
    if "final_score" in hmap:
        updates.append(gspread.Cell(row_idx, hmap["final_score"], str(round(final_score, 1))))
    if "reasons" in hmap:
        updates.append(gspread.Cell(row_idx, hmap["reasons"], f"selected|target={target_theme}|penalties={penalties}"))

    if updates:
        ws.update_cells(updates, value_input_option="USER_ENTERED")
        return 1
    return 0


# ============================================================
# Render
# ============================================================

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
    r = requests.post(RENDER_URL, json=payload, timeout=45)
    r.raise_for_status()
    j = r.json()
    return (j.get("graphic_url") or "").strip()

def stage_render(ws: gspread.Worksheet, headers: List[str]) -> int:
    hmap = header_map(headers)
    rows = ws.get_all_values()
    if len(rows) < 2:
        return 0

    rendered = 0
    for i in range(2, len(rows) + 1):
        row = row_dict(headers, rows[i - 1])
        if safe_get(row, "status").upper() != STATUS_READY_TO_POST:
            continue

        log(f"  Rendering row {i}")
        try:
            graphic_url = render_image(row)
            updates = []
            if graphic_url and "graphic_url" in hmap:
                updates.append(gspread.Cell(i, hmap["graphic_url"], graphic_url))
            if "rendered_timestamp" in hmap:
                updates.append(gspread.Cell(i, hmap["rendered_timestamp"], now_utc_iso()))
            if "status" in hmap:
                updates.append(gspread.Cell(i, hmap["status"], STATUS_READY_TO_PUBLISH))
            if updates:
                ws.update_cells(updates, value_input_option="USER_ENTERED")
            rendered += 1
        except Exception as e:
            log(f"  Render error: {e}")
    return rendered


# ============================================================
# Instagram
# ============================================================

def instagram_caption(row: Dict[str, str]) -> str:
    origin = safe_get(row, "origin_city") or safe_get(row, "origin_iata")
    dest = safe_get(row, "destination_city") or safe_get(row, "destination_iata")
    price = f"£{round_price_up(safe_get(row,'price_gbp'))}"

    # Human, low-hype, UK tone
    variants = [
        f"{price} from {origin} to {dest}. If you’re flexible, this is a tidy one.",
        f"{origin} → {dest} for {price}. Handy dates if you can move quick.",
        f"{price} to {dest} from {origin}. Worth a look if that’s been on your list.",
        f"{origin} to {dest} for {price}. Prices move — double-check at checkout.",
    ]
    idx = stable_hash(dest) % len(variants)
    return variants[idx]

def post_instagram(ws: gspread.Worksheet, headers: List[str]) -> int:
    hmap = header_map(headers)
    rows = ws.get_all_values()
    if len(rows) < 2:
        return 0

    posted = 0
    for i in range(2, len(rows) + 1):
        row = row_dict(headers, rows[i - 1])
        if safe_get(row, "status").upper() != STATUS_READY_TO_PUBLISH:
            continue

        graphic_url = safe_get(row, "graphic_url")
        if not graphic_url:
            continue

        caption = instagram_caption(row)
        cache_buster = int(time.time())
        image_url_cb = f"{graphic_url}?cb={cache_buster}"

        log(f"  Posting IG row {i}")
        try:
            create_url = f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media"
            r1 = requests.post(create_url, data={
                "image_url": image_url_cb,
                "caption": caption[:2200],
                "access_token": IG_ACCESS_TOKEN,
            }, timeout=45)
            r1.raise_for_status()
            creation_id = r1.json().get("id")
            if not creation_id:
                continue

            # poll until finished
            waited = 0
            while waited < 90:
                rs = requests.get(
                    f"https://graph.facebook.com/v20.0/{creation_id}",
                    params={"fields": "status_code", "access_token": IG_ACCESS_TOKEN},
                    timeout=15
                )
                if rs.status_code == 200 and rs.json().get("status_code") == "FINISHED":
                    break
                time.sleep(3)
                waited += 3

            r2 = requests.post(
                f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media_publish",
                data={"creation_id": creation_id, "access_token": IG_ACCESS_TOKEN},
                timeout=45
            )
            r2.raise_for_status()
            media_id = r2.json().get("id", "")

            updates = []
            if "ig_creation_id" in hmap:
                updates.append(gspread.Cell(i, hmap["ig_creation_id"], creation_id))
            if "ig_media_id" in hmap:
                updates.append(gspread.Cell(i, hmap["ig_media_id"], media_id))
            if "ig_published_timestamp" in hmap:
                updates.append(gspread.Cell(i, hmap["ig_published_timestamp"], now_utc_iso()))
            if "published_timestamp" in hmap:
                updates.append(gspread.Cell(i, hmap["published_timestamp"], now_utc_iso()))
            if "status" in hmap:
                updates.append(gspread.Cell(i, hmap["status"], STATUS_POSTED_INSTAGRAM))

            if updates:
                ws.update_cells(updates, value_input_option="USER_ENTERED")

            posted += 1
            log("  ✓ IG posted")
        except Exception as e:
            log(f"  IG error: {e}")
    return posted


# ============================================================
# Telegram (VIP AM, Free PM after delay)
# ============================================================

def tg_send(token: str, chat: str, text: str) -> None:
    r = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True},
        timeout=45,
    )
    r.raise_for_status()

def format_tg_vip(row: Dict[str, str]) -> str:
    price = f"£{round_price_up(safe_get(row,'price_gbp'))}"
    dest_city = safe_get(row, "destination_city") or safe_get(row, "destination_iata")
    dest_country = safe_get(row, "destination_country")
    origin_city = safe_get(row, "origin_city") or safe_get(row, "origin_iata")
    out_date = safe_get(row, "outbound_date")
    ret_date = safe_get(row, "return_date")
    link = safe_get(row, "booking_link_vip") or safe_get(row, "affiliate_url")

    where = f"{dest_city}, {dest_country}" if dest_country else dest_city
    msg = f"""{price} to {where}

<b>FROM:</b> {origin_city}
<b>OUT:</b> {out_date}
<b>BACK:</b> {ret_date}

<b>VIP:</b> 24reduced noise — best pick for this run.
"""
    if link:
        msg += f'\n<a href="{link}">Book</a>'
    return msg

def format_tg_free(row: Dict[str, str]) -> str:
    price = f"£{round_price_up(safe_get(row,'price_gbp'))}"
    dest_city = safe_get(row, "destination_city") or safe_get(row, "destination_iata")
    dest_country = safe_get(row, "destination_country")
    origin_city = safe_get(row, "origin_city") or safe_get(row, "origin_iata")
    out_date = safe_get(row, "outbound_date")
    ret_date = safe_get(row, "return_date")
    link = safe_get(row, "booking_link_free") or safe_get(row, "affiliate_url")

    where = f"{dest_city}, {dest_country}" if dest_country else dest_city
    msg = f"""{price} to {where}

<b>FROM:</b> {origin_city}
<b>OUT:</b> {out_date}
<b>BACK:</b> {ret_date}

<b>Heads up:</b>
• VIP members saw this 24h early
"""
    if link:
        msg += f'\n<a href="{link}">Book</a>\n\n'

    if STRIPE_LINK_MONTHLY:
        msg += "<b>Want earlier access?</b>\n"
        msg += "• Deals 24h early\n"
        msg += "• Less noise, better picks\n\n"
        msg += f'<a href="{STRIPE_LINK_MONTHLY}">Upgrade to VIP</a>'
    return msg

def post_tg_vip(ws: gspread.Worksheet, headers: List[str]) -> int:
    if RUN_SLOT != "AM":
        return 0
    hmap = header_map(headers)
    rows = ws.get_all_values()
    if len(rows) < 2:
        return 0

    posted = 0
    for i in range(2, len(rows) + 1):
        row = row_dict(headers, rows[i - 1])
        if safe_get(row, "status").upper() != STATUS_POSTED_INSTAGRAM:
            continue
        if safe_get(row, "tg_monthly_timestamp"):
            continue

        msg = format_tg_vip(row)
        log(f"  TG VIP row {i}")
        try:
            tg_send(TELEGRAM_BOT_TOKEN_VIP, TELEGRAM_CHANNEL_VIP, msg)
            updates = []
            if "tg_monthly_timestamp" in hmap:
                updates.append(gspread.Cell(i, hmap["tg_monthly_timestamp"], now_utc_iso()))
            if "posted_to_vip" in hmap:
                updates.append(gspread.Cell(i, hmap["posted_to_vip"], "TRUE"))
            if "status" in hmap:
                updates.append(gspread.Cell(i, hmap["status"], STATUS_POSTED_TELEGRAM_VIP))
            if updates:
                ws.update_cells(updates, value_input_option="USER_ENTERED")
            posted += 1
        except Exception as e:
            log(f"  TG VIP error: {e}")
    return posted

def post_tg_free(ws: gspread.Worksheet, headers: List[str]) -> int:
    if RUN_SLOT != "PM":
        return 0
    hmap = header_map(headers)
    rows = ws.get_all_values()
    if len(rows) < 2:
        return 0

    posted = 0
    for i in range(2, len(rows) + 1):
        row = row_dict(headers, rows[i - 1])
        if safe_get(row, "status").upper() != STATUS_POSTED_TELEGRAM_VIP:
            continue
        if safe_get(row, "tg_free_timestamp"):
            continue

        vip_ts = safe_get(row, "tg_monthly_timestamp")
        if hours_since(vip_ts) < VIP_DELAY_HOURS:
            continue

        msg = format_tg_free(row)
        log(f"  TG FREE row {i}")
        try:
            tg_send(TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL, msg)
            updates = []
            if "tg_free_timestamp" in hmap:
                updates.append(gspread.Cell(i, hmap["tg_free_timestamp"], now_utc_iso()))
            if "posted_for_free" in hmap:
                updates.append(gspread.Cell(i, hmap["posted_for_free"], "TRUE"))
            if "status" in hmap:
                updates.append(gspread.Cell(i, hmap["status"], STATUS_POSTED_ALL))
            if updates:
                ws.update_cells(updates, value_input_option="USER_ENTERED")
            posted += 1
        except Exception as e:
            log(f"  TG FREE error: {e}")
    return posted


# ============================================================
# MAIN
# ============================================================

def main():
    log("=" * 70)
    log("TRAVELTXTER V4.5.3 — WATERWHEEL RUN")
    log("=" * 70)
    log(f"RUN_SLOT: {RUN_SLOT} | VIP_DELAY: {VIP_DELAY_HOURS}h")
    log(f"Variety: lookback={VARIETY_LOOKBACK_HOURS}h | DEST_REPEAT_PENALTY={DEST_REPEAT_PENALTY} | THEME_REPEAT_PENALTY={THEME_REPEAT_PENALTY}")
    log(f"Duffel: ENABLED={DUFFEL_ENABLED} | MAX_INSERTS={DUFFEL_MAX_INSERTS} | ROUTES_PER_RUN={DUFFEL_ROUTES_PER_RUN}")
    log("=" * 70)

    sh, ws, headers = get_sheet()
    log(f"Connected to sheet | Columns: {len(headers)} | Tab: {RAW_DEALS_TAB}")

    target = daily_theme(dt.date.today(), RUN_SLOT)
    log(f"Daily Theme (target): {target}")

    # Load CONFIG and pick today’s routes
    routes = load_config_routes(sh)
    routes_today = pick_routes_for_today(routes, target, DUFFEL_ROUTES_PER_RUN)
    log(f"Routes today: {len(routes_today)} (target={target})")

    # [1] Duffel feed
    if DUFFEL_ENABLED and DUFFEL_API_KEY and routes_today:
        log("\n[1] DUFFEL FEED")
        inserted = run_duffel_feeder(ws, headers, routes_today)
        log(f"✓ {inserted} deals inserted")
    else:
        log("\n[1] DUFFEL FEED")
        log("Skipped (missing key/routes or disabled)")

    # [2] Score (NEW/SCORED -> SCORED w scores)
    log("\n[2] SCORING (NEW → SCORED)")
    scored = stage_score(ws, headers, target)
    log(f"✓ {scored} deals scored")

    # [3] Select (SCORED -> READY_TO_POST)
    log("\n[3] EDITORIAL SELECTION (SCORED → READY_TO_POST)")
    selected = stage_select_best(ws, headers, target)
    log(f"✓ {selected} promoted")

    # [4] Render
    log("\n[4] RENDER (READY_TO_POST → READY_TO_PUBLISH)")
    rendered = stage_render(ws, headers)
    log(f"✓ {rendered} rendered")

    # [5] Instagram
    log("\n[5] INSTAGRAM (READY_TO_PUBLISH → POSTED_INSTAGRAM)")
    ig_posted = post_instagram(ws, headers)
    log(f"✓ {ig_posted} posted")

    # [6] Telegram VIP (AM)
    log("\n[6] TELEGRAM VIP (POSTED_INSTAGRAM → POSTED_TELEGRAM_VIP)")
    vip_posted = post_tg_vip(ws, headers)
    log(f"✓ {vip_posted} posted")

    # [7] Telegram FREE (PM after delay)
    log("\n[7] TELEGRAM FREE (POSTED_TELEGRAM_VIP → POSTED_ALL)")
    free_posted = post_tg_free(ws, headers)
    log(f"✓ {free_posted} posted")

    log("\n" + "=" * 70)
    log("COMPLETE")
    log("=" * 70)


if __name__ == "__main__":
    main()
