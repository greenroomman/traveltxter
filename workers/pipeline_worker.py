#!/usr/bin/env python3
"""
TravelTxter V4.5.3 — WATERWHEEL (Route-First + Theme Rotation + Direct/LCC Bias + Human Copy)

PASTE-READY SINGLE FILE
=======================
Drop this in:   workers/pipeline_worker.py
Then run via GitHub Actions with RUN_SLOT=AM and RUN_SLOT=PM.

What this version fixes / adds
------------------------------
1) ✅ Theme rotation (daily) + Long-haul region rotation (Americas/Asia/Africa/Australasia)
2) ✅ “Route-first” feeder: picks CONFIG routes that match today’s theme first (but never hard-blocks)
3) ✅ Reverse-engineered Duffel request: tries direct-only + LCC airline bias FIRST, then auto-fallbacks if Duffel rejects fields
4) ✅ Diversity circuit breaker: strong penalty if destination was posted recently (kills “Boring Iceland” loops)
5) ✅ Datetime bug fix: no more offset-aware vs naive comparisons
6) ✅ City names everywhere: feeder populates origin_city/destination_city from CONFIG (and signals fallback)
7) ✅ Outputs: Instagram + Telegram VIP then Telegram Free (delay) + optional MailerLite feed export tab

IMPORTANT: Sheet ID vs Spreadsheet ID
-------------------------------------
- SPREADSHEET_ID = the long ID in the Google Sheets URL (e.g. 1qTwHlCaTayPzvMFcXuDtJOLoAQ4mINMYPwuitogZeTE)
- SHEET_ID is NOT needed for Google Sheets access (people sometimes misuse it). We treat SHEET_ID as a fallback alias only.

Secrets expected (matches your list)
------------------------------------
DUFFEL_API_KEY
GCP_SA_JSON or GCP_SA_JSON_ONE_LINE
SPREADSHEET_ID (or SHEET_ID fallback)
RAW_DEALS_TAB
RENDER_URL
IG_ACCESS_TOKEN (or META_ACCESS_TOKEN fallback)
IG_USER_ID
TELEGRAM_BOT_TOKEN_VIP / TELEGRAM_CHANNEL_VIP
TELEGRAM_BOT_TOKEN / TELEGRAM_CHANNEL
STRIPE_LINK (or STRIPE_LINK_MONTHLY / STRIPE_LINK_YEARLY if you add them)
Optional: OPENAI_API_KEY (not used here), MAILERLITE_API_KEY (not used here), MAILERLITE_FEED_TAB

CONFIG / CONFIG_SIGNALS
-----------------------
- CONFIG must contain *real airport codes only*. No LON.
- This worker uses CONFIG columns if present:
  enabled, priority, origin_iata, origin_city, destination_iata, destination_city, destination_country,
  days_ahead, window_days, trip_length_days, max_connections, cabin_class, theme, included_airlines
- CONFIG_SIGNALS is used only as a fallback to get nicer names and for auto-theme derivation if you want it.

Run slots and posting SLA
-------------------------
- AM run: feeder -> score -> select -> render -> Instagram -> Telegram VIP
- PM run: feeder -> score -> select -> render -> Instagram (optional) -> Telegram FREE (only when VIP delay satisfied)

"""

from __future__ import annotations

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
# ENV / SECRET COMPAT
# ============================================================

# Map common alias secrets -> canonical names used by this worker
_SECRET_MAPPINGS = {
    # IG token
    "IG_ACCESS_TOKEN": "META_ACCESS_TOKEN",
    # Sheet ID alias (some repos used SHEET_ID)
    "SPREADSHEET_ID": "SHEET_ID",
}

for target, source in _SECRET_MAPPINGS.items():
    if not os.getenv(target) and os.getenv(source):
        os.environ[target] = os.getenv(source)


def env(name: str, default: str = "", required: bool = False) -> str:
    v = (os.getenv(name) or "").strip()
    if not v:
        v = default
    if required and not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


SPREADSHEET_ID = env("SPREADSHEET_ID", required=True)
RAW_DEALS_TAB = env("RAW_DEALS_TAB", "RAW_DEALS")
CONFIG_TAB = env("CONFIG_TAB", "CONFIG")
CONFIG_SIGNALS_TAB = env("CONFIG_SIGNALS_TAB", "CONFIG_SIGNALS")
MAILERLITE_FEED_TAB = env("MAILERLITE_FEED_TAB", "MAILERLITE_FEED")  # optional export tab

GCP_SA_JSON = env("GCP_SA_JSON", "") or env("GCP_SA_JSON_ONE_LINE", "")
if not GCP_SA_JSON:
    raise RuntimeError("Missing required env var: GCP_SA_JSON or GCP_SA_JSON_ONE_LINE")

DUFFEL_API_KEY = env("DUFFEL_API_KEY", "")
DUFFEL_VERSION = env("DUFFEL_VERSION", "v2")
DUFFEL_ENABLED = env("DUFFEL_ENABLED", "true").lower() in ("1", "true", "yes")

# Free-tier safety knobs (keep these conservative)
DUFFEL_ROUTES_PER_RUN = int(env("DUFFEL_ROUTES_PER_RUN", "2"))       # how many CONFIG routes to query per run
DUFFEL_MAX_INSERTS = int(env("DUFFEL_MAX_INSERTS", "3"))             # how many offers saved per route request
DUFFEL_MAX_SEARCHES_PER_RUN = int(env("DUFFEL_MAX_SEARCHES_PER_RUN", "4"))  # hard cap on offer_requests per run
DUFFEL_MIN_OFFERS_FLOOR = int(env("DUFFEL_MIN_OFFERS_FLOOR", "6"))   # if we inserted < this, do 1 extra widening step

# Request-level “quality” hints (auto-fallback if Duffel rejects fields)
ENFORCE_DIRECT_FIRST = env("ENFORCE_DIRECT_FIRST", "true").lower() in ("1", "true", "yes")
LCC_BIAS_FIRST = env("LCC_BIAS_FIRST", "true").lower() in ("1", "true", "yes")

RENDER_URL = env("RENDER_URL", required=True)

IG_ACCESS_TOKEN = env("IG_ACCESS_TOKEN", required=True)
IG_USER_ID = env("IG_USER_ID", required=True)

TELEGRAM_BOT_TOKEN_VIP = env("TELEGRAM_BOT_TOKEN_VIP", required=True)
TELEGRAM_CHANNEL_VIP = env("TELEGRAM_CHANNEL_VIP", required=True)

TELEGRAM_BOT_TOKEN = env("TELEGRAM_BOT_TOKEN", required=True)
TELEGRAM_CHANNEL = env("TELEGRAM_CHANNEL", required=True)

# Stripe
STRIPE_LINK = env("STRIPE_LINK", "")
STRIPE_LINK_MONTHLY = env("STRIPE_LINK_MONTHLY", "") or STRIPE_LINK
STRIPE_LINK_YEARLY = env("STRIPE_LINK_YEARLY", "") or STRIPE_LINK

# Timing / cadence
VIP_DELAY_HOURS = int(env("VIP_DELAY_HOURS", "24"))
RUN_SLOT = env("RUN_SLOT", "AM").upper()  # AM or PM

# Variety / boredom circuit breaker
VARIETY_LOOKBACK_HOURS = int(env("VARIETY_LOOKBACK_HOURS", "72"))
DEST_REPEAT_PENALTY = float(env("DEST_REPEAT_PENALTY", "50.0"))      # strong
THEME_REPEAT_PENALTY = float(env("THEME_REPEAT_PENALTY", "30.0"))
RECENCY_FILTER_HOURS = int(env("RECENCY_FILTER_HOURS", "48"))

# Status lifecycle (simple + stable)
STATUS_NEW = "NEW"
STATUS_SCORED = "SCORED"
STATUS_READY_TO_POST = "READY_TO_POST"
STATUS_READY_TO_PUBLISH = "READY_TO_PUBLISH"
STATUS_POSTED_INSTAGRAM = "POSTED_INSTAGRAM"
STATUS_POSTED_TELEGRAM_VIP = "POSTED_TELEGRAM_VIP"
STATUS_POSTED_ALL = "POSTED_ALL"


# ============================================================
# LOGGING / TIME
# ============================================================

def now_utc_dt() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def now_utc_str() -> str:
    return now_utc_dt().replace(microsecond=0).isoformat().replace("+00:00", "Z")


def log(msg: str) -> None:
    print(f"{now_utc_str()} | {msg}", flush=True)


def stable_hash(text: str) -> int:
    return int(hashlib.md5(text.encode("utf-8")).hexdigest(), 16)


def safe_text(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float):
        try:
            if v != v:  # NaN
                return ""
        except Exception:
            return ""
        return ""
    try:
        return str(v)
    except Exception:
        return ""


def safe_get(row: Dict[str, Any], key: str) -> str:
    return safe_text(row.get(key)).strip()


def safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def parse_iso_utc(s: str) -> Optional[dt.datetime]:
    """
    Parse timestamps like:
    - 2026-01-04T11:44:38Z
    - 2026-01-04T11:44:38+00:00
    Returns timezone-aware dt in UTC.
    """
    s = (s or "").strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(dt.timezone.utc)
        d = dt.datetime.fromisoformat(s)
        if d.tzinfo is None:
            # assume UTC if naive
            return d.replace(tzinfo=dt.timezone.utc)
        return d.astimezone(dt.timezone.utc)
    except Exception:
        return None


def hours_since(ts: str) -> float:
    d = parse_iso_utc(ts)
    if not d:
        return 9999.0
    return (now_utc_dt() - d).total_seconds() / 3600.0


def round_price_up(price_str: str) -> int:
    try:
        return int(math.ceil(float(price_str)))
    except Exception:
        return 0


# ============================================================
# THEME ROTATION (daily + longhaul region)
# ============================================================

THEMES = ["city", "winter_sun", "surf", "snow", "foodie", "longhaul"]

# Simple schedule you can tweak later
# Monday..Sunday = 0..6
WEEKLY_THEME_SCHEDULE = {
    0: "city",
    1: "winter_sun",
    2: "surf",
    3: "snow",
    4: "city",
    5: "winter_sun",
    6: "longhaul",
}

LONGHAUL_REGIONS = ["americas", "asia", "africa", "australasia"]

# LCC airline IATA codes (common)
LCC_DEFAULT = ["U2", "FR", "W6", "LS"]  # easyJet, Ryanair, Wizz, Jet2


def todays_theme_and_region(today: dt.date) -> Tuple[str, Optional[str]]:
    theme = WEEKLY_THEME_SCHEDULE.get(today.weekday(), "city")
    if theme != "longhaul":
        return theme, None

    # Rotate regions by ISO week number (stable + predictable)
    week = int(today.isocalendar().week)
    region = LONGHAUL_REGIONS[week % len(LONGHAUL_REGIONS)]
    return theme, region


# ============================================================
# GOOGLE SHEETS
# ============================================================

def gs_client() -> gspread.Client:
    creds = Credentials.from_service_account_info(
        json.loads(GCP_SA_JSON),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)


def get_spreadsheet() -> gspread.Spreadsheet:
    return gs_client().open_by_key(SPREADSHEET_ID)


def get_ws(tab_name: str) -> gspread.Worksheet:
    sh = get_spreadsheet()
    return sh.worksheet(tab_name)


def header_map(headers: List[str]) -> Dict[str, int]:
    return {h.strip(): i + 1 for i, h in enumerate(headers) if (h or "").strip()}


def ensure_tab(sh: gspread.Spreadsheet, title: str, headers: List[str]) -> gspread.Worksheet:
    try:
        ws = sh.worksheet(title)
        existing = ws.row_values(1)
        if existing:
            return ws
        ws.append_row(headers, value_input_option="USER_ENTERED")
        return ws
    except Exception:
        ws = sh.add_worksheet(title=title, rows=200, cols=max(20, len(headers) + 5))
        ws.append_row(headers, value_input_option="USER_ENTERED")
        return ws


# ============================================================
# CONFIG_SIGNALS (optional helper for nicer names)
# ============================================================

def load_config_signals(sh: gspread.Spreadsheet) -> Dict[str, Dict[str, Any]]:
    try:
        ws = sh.worksheet(CONFIG_SIGNALS_TAB)
    except Exception:
        log("CONFIG_SIGNALS tab not found (ok)")
        return {}

    try:
        recs = ws.get_all_records()
    except Exception:
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    for r in recs:
        iata = safe_text(r.get("iata_hint")).strip().upper()
        if len(iata) == 3 and iata.isalpha():
            out[iata] = r
    log(f"Loaded CONFIG_SIGNALS: {len(out)} destinations")
    return out


def signals_city_name(signals: Dict[str, Any]) -> str:
    # Try common column names people use
    for k in ["city", "city_name", "place_name", "destination_city", "name"]:
        v = safe_text(signals.get(k)).strip()
        if v:
            return v
    return ""


# ============================================================
# CONFIG LOADING (route-first)
# ============================================================

def normalize_theme(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace(" ", "_")
    return s


def load_routes_from_config(sh: gspread.Spreadsheet) -> List[Dict[str, Any]]:
    """
    Reads CONFIG as rows of dict.
    Very tolerant: ignores unknown columns; uses what exists.
    """
    try:
        cfg = sh.worksheet(CONFIG_TAB)
    except Exception:
        log("CONFIG tab missing — create it and add routes.")
        return []

    values = cfg.get_all_values()
    if not values or len(values) < 2:
        return []

    headers = [h.strip() for h in values[0]]
    rows = []
    for r in values[1:]:
        row = {}
        for i, h in enumerate(headers):
            row[h] = r[i] if i < len(r) else ""
        # enabled?
        enabled = safe_text(row.get("enabled")).strip().upper()
        if enabled not in ("TRUE", "1", "YES"):
            continue

        # require real IATA
        o = safe_text(row.get("origin_iata")).strip().upper()
        d = safe_text(row.get("destination_iata")).strip().upper()
        if len(o) != 3 or len(d) != 3 or (not o.isalpha()) or (not d.isalpha()):
            continue

        # priority
        pr = int(safe_float(row.get("priority"), 9999))
        row["_priority"] = pr

        # theme tag
        row["_theme"] = normalize_theme(row.get("theme"))
        row["_theme_name"] = safe_text(row.get("theme.1") or row.get("theme_name") or "").strip()

        rows.append(row)

    rows.sort(key=lambda x: x.get("_priority", 9999))
    return rows


def select_routes_for_today(
    routes: List[Dict[str, Any]],
    today_theme: str,
    longhaul_region: Optional[str],
    max_routes: int,
    run_slot: str,
) -> List[Dict[str, Any]]:
    """
    Route-first selection:
    - Prefer rows where CONFIG.theme matches today_theme (soft, not hard)
    - Then fill with any enabled routes (wildcards)
    - Deterministic rotation (day-of-year + slot)
    """

    if not routes:
        return []

    theme = normalize_theme(today_theme)

    # themed pool
    themed = [r for r in routes if r.get("_theme") == theme]

    # longhaul region pool (optional if you tag CONFIG rows like longhaul_asia, longhaul_americas, etc.)
    if theme == "longhaul" and longhaul_region:
        region_tag = f"longhaul_{normalize_theme(longhaul_region)}"
        regioned = [r for r in routes if r.get("_theme") == region_tag]
        if regioned:
            themed = regioned  # strong preference

    pool = themed if themed else routes

    if len(pool) <= max_routes:
        return pool

    day_of_year = dt.date.today().timetuple().tm_yday
    slot_offset = 0 if run_slot == "AM" else 1
    start = ((day_of_year * 2) + slot_offset) % len(pool)

    selected = []
    for i in range(max_routes):
        selected.append(pool[(start + i) % len(pool)])

    return selected


# ============================================================
# DUFFEL (route-first, direct + LCC bias first, auto-fallback)
# ============================================================

def build_offer_request_payload(
    origin: str,
    dest: str,
    out_date: str,
    ret_date: str,
    cabin_class: str = "economy",
    max_connections: Optional[int] = None,
    included_airlines: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Duffel offer_requests payload.
    IMPORTANT: Some Duffel fields vary by API version/account.
    We include optional fields, but we always auto-fallback if rejected.
    """
    data: Dict[str, Any] = {
        "slices": [
            {"origin": origin, "destination": dest, "departure_date": out_date},
            {"origin": dest, "destination": origin, "departure_date": ret_date},
        ],
        "passengers": [{"type": "adult"}],
        "cabin_class": cabin_class or "economy",
    }

    # Optional hints (guarded by fallback logic)
    if max_connections is not None:
        # Some Duffel implementations expect this in slice(s), some at root.
        # We try root first; if rejected we retry with a plain payload.
        data["max_connections"] = int(max_connections)

    if included_airlines:
        # Duffel commonly accepts 'included_airlines' on offer_requests
        data["included_airlines"] = included_airlines

    return {"data": data}


def duffel_offer_request(payload: Dict[str, Any]) -> Dict[str, Any]:
    url = "https://api.duffel.com/air/offer_requests"
    headers = {
        "Authorization": f"Bearer {DUFFEL_API_KEY}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Duffel-Version": DUFFEL_VERSION,
    }
    r = requests.post(url, headers=headers, json=payload, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"Duffel error {r.status_code}: {r.text[:500]}")
    return r.json()


def extract_offers(resp: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not isinstance(resp, dict):
        return []
    data = resp.get("data")
    if isinstance(data, dict) and isinstance(data.get("offers"), list):
        return data["offers"]
    if isinstance(data, list):
        return data
    if isinstance(resp.get("offers"), list):
        return resp["offers"]
    return []


def request_offers_with_fallbacks(
    origin: str,
    dest: str,
    out_date: str,
    ret_date: str,
    cabin_class: str,
    max_connections: Optional[int],
    included_airlines: Optional[List[str]],
) -> List[Dict[str, Any]]:
    """
    Try:
    1) direct + airline bias (if enabled)
    2) direct only
    3) airline bias only
    4) plain request
    """
    attempts = []

    base_max = max_connections
    base_air = included_airlines or None

    # Attempt 1: both
    if ENFORCE_DIRECT_FIRST or LCC_BIAS_FIRST:
        attempts.append((base_max, base_air))

    # Attempt 2: direct only
    if ENFORCE_DIRECT_FIRST:
        attempts.append((base_max, None))

    # Attempt 3: airlines only
    if LCC_BIAS_FIRST and base_air:
        attempts.append((None, base_air))

    # Attempt 4: plain
    attempts.append((None, None))

    last_err = None
    for mc, ia in attempts:
        try:
            payload = build_offer_request_payload(
                origin=origin,
                dest=dest,
                out_date=out_date,
                ret_date=ret_date,
                cabin_class=cabin_class,
                max_connections=mc,
                included_airlines=ia,
            )
            resp = duffel_offer_request(payload)
            offers = extract_offers(resp)
            return offers
        except Exception as e:
            last_err = e
            continue

    raise RuntimeError(f"Duffel request failed after fallbacks: {last_err}")


# ============================================================
# FEEDER (writes to RAW_DEALS)
# ============================================================

def compute_date_pair(days_ahead: int, window_offset_days: int, trip_len: int) -> Tuple[str, str]:
    today = dt.date.today()
    out = today + dt.timedelta(days=int(days_ahead) + int(window_offset_days))
    ret = out + dt.timedelta(days=int(trip_len))
    return str(out), str(ret)


def get_raw_headers(ws: gspread.Worksheet) -> List[str]:
    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("RAW_DEALS has no headers row")
    return headers


def get_city_name(
    iata: str,
    fallback_city: str,
    signals_map: Dict[str, Dict[str, Any]],
) -> str:
    fallback_city = (fallback_city or "").strip()
    if fallback_city and fallback_city.upper() != iata:
        return fallback_city

    sig = signals_map.get(iata.upper(), {})
    name = signals_city_name(sig)
    return name or fallback_city or iata.upper()


def run_duffel_feeder(
    raw_ws: gspread.Worksheet,
    headers: List[str],
    routes: List[Dict[str, Any]],
    today_theme: str,
    longhaul_region: Optional[str],
    signals_map: Dict[str, Dict[str, Any]],
) -> int:
    if not DUFFEL_ENABLED or not DUFFEL_API_KEY:
        log("Duffel: DISABLED")
        return 0

    hmap = header_map(headers)
    selected_routes = select_routes_for_today(
        routes=routes,
        today_theme=today_theme,
        longhaul_region=longhaul_region,
        max_routes=DUFFEL_ROUTES_PER_RUN,
        run_slot=RUN_SLOT,
    )

    if not selected_routes:
        log("Duffel: No routes available (CONFIG empty?)")
        return 0

    log(f"Duffel: Searching {len(selected_routes)} routes (MAX_INSERTS={DUFFEL_MAX_INSERTS}, SEARCH_CAP={DUFFEL_MAX_SEARCHES_PER_RUN})")

    total_inserted = 0
    searches_used = 0

    for r in selected_routes:
        if searches_used >= DUFFEL_MAX_SEARCHES_PER_RUN:
            break

        origin = safe_get(r, "origin_iata").upper()
        dest = safe_get(r, "destination_iata").upper()

        # Names from CONFIG (best), signals fallback
        origin_city = get_city_name(origin, safe_get(r, "origin_city"), signals_map)
        dest_city = get_city_name(dest, safe_get(r, "destination_city"), signals_map)
        dest_country = safe_get(r, "destination_country")

        # Window & trip settings
        days_ahead = int(safe_float(r.get("days_ahead"), 45))
        window_days = int(safe_float(r.get("window_days"), 0))
        trip_len = int(safe_float(r.get("trip_length_days"), 5))

        # request settings
        cabin_class = safe_get(r, "cabin_class") or "economy"
        max_conn = int(safe_float(r.get("max_connections"), 0))  # direct default

        # included airlines: from CONFIG or theme bias
        cfg_air = safe_get(r, "included_airlines")
        included_airlines: Optional[List[str]] = None
        if cfg_air:
            included_airlines = [a.strip().upper() for a in cfg_air.split(",") if a.strip()]
        else:
            # Theme-based bias (soft)
            if LCC_BIAS_FIRST and normalize_theme(today_theme) in ("winter_sun", "snow", "surf", "city", "foodie"):
                included_airlines = LCC_DEFAULT.copy()

        # First date attempt
        out_date, ret_date = compute_date_pair(days_ahead, 0, trip_len)
        log(f"  {origin_city} ({origin}) → {dest_city} ({dest}) | {out_date} +{trip_len}d")

        try:
            offers = request_offers_with_fallbacks(
                origin=origin,
                dest=dest,
                out_date=out_date,
                ret_date=ret_date,
                cabin_class=cabin_class,
                max_connections=max_conn if ENFORCE_DIRECT_FIRST else None,
                included_airlines=included_airlines if LCC_BIAS_FIRST else None,
            )
            searches_used += 1
        except Exception as e:
            log(f"  Duffel error: {e}")
            continue

        if not offers:
            log("  0 offers")
            continue

        # Insert up to DUFFEL_MAX_INSERTS offers
        inserted_here = append_offers_to_sheet(
            raw_ws=raw_ws,
            headers=headers,
            hmap=hmap,
            offers=offers,
            origin=origin,
            origin_city=origin_city,
            dest=dest,
            dest_city=dest_city,
            dest_country=dest_country,
            out_date=out_date,
            ret_date=ret_date,
            trip_len=trip_len,
            deal_source="DUFFEL",
        )
        total_inserted += inserted_here
        log(f"  ✓ Inserted {inserted_here} deals")

        # Elastic widening (only ONE extra attempt per route, and only if we’re under floor)
        if window_days > 0 and searches_used < DUFFEL_MAX_SEARCHES_PER_RUN:
            if total_inserted < DUFFEL_MIN_OFFERS_FLOOR:
                widen = min(window_days, 7)  # keep it cheap
                out2, ret2 = compute_date_pair(days_ahead, widen, trip_len)
                log(f"  ↻ Widening (+{widen}d): {out2}")

                try:
                    offers2 = request_offers_with_fallbacks(
                        origin=origin,
                        dest=dest,
                        out_date=out2,
                        ret_date=ret2,
                        cabin_class=cabin_class,
                        max_connections=max_conn if ENFORCE_DIRECT_FIRST else None,
                        included_airlines=included_airlines if LCC_BIAS_FIRST else None,
                    )
                    searches_used += 1
                except Exception as e:
                    log(f"  Duffel widening error: {e}")
                    continue

                if offers2:
                    inserted2 = append_offers_to_sheet(
                        raw_ws=raw_ws,
                        headers=headers,
                        hmap=hmap,
                        offers=offers2,
                        origin=origin,
                        origin_city=origin_city,
                        dest=dest,
                        dest_city=dest_city,
                        dest_country=dest_country,
                        out_date=out2,
                        ret_date=ret2,
                        trip_len=trip_len,
                        deal_source="DUFFEL",
                    )
                    total_inserted += inserted2
                    log(f"  ✓ Inserted {inserted2} (widened)")

    return total_inserted


def append_offers_to_sheet(
    raw_ws: gspread.Worksheet,
    headers: List[str],
    hmap: Dict[str, int],
    offers: List[Dict[str, Any]],
    origin: str,
    origin_city: str,
    dest: str,
    dest_city: str,
    dest_country: str,
    out_date: str,
    ret_date: str,
    trip_len: int,
    deal_source: str,
) -> int:
    rows_to_append: List[List[Any]] = []
    inserted = 0

    for off in offers:
        if inserted >= DUFFEL_MAX_INSERTS:
            break

        currency = safe_text(off.get("total_currency") or "GBP").strip().upper()
        if currency and currency != "GBP":
            continue

        price = safe_text(off.get("total_amount") or "").strip()
        if not price:
            continue

        airline = ""
        try:
            airline = safe_text((off.get("owner") or {}).get("name") or "").strip()
        except Exception:
            airline = ""

        stops = "0"
        try:
            slices = off.get("slices") or []
            segs = (slices[0].get("segments") or []) if slices else []
            stops = str(max(0, len(segs) - 1))
        except Exception:
            stops = "0"

        deal_id = str(uuid.uuid4())

        row_obj = {h: "" for h in headers}

        # Core fields (only write if header exists)
        row_obj["deal_id"] = deal_id
        row_obj["origin_iata"] = origin
        row_obj["origin_city"] = origin_city
        row_obj["destination_iata"] = dest
        row_obj["destination_city"] = dest_city
        row_obj["destination_country"] = dest_country
        row_obj["outbound_date"] = out_date
        row_obj["return_date"] = ret_date
        row_obj["trip_length_days"] = str(trip_len)
        row_obj["price_gbp"] = price
        row_obj["airline"] = airline
        row_obj["stops"] = stops
        row_obj["deal_source"] = deal_source
        row_obj["date_added"] = now_utc_str()
        row_obj["status"] = STATUS_NEW

        # Destination key (for repeat detection)
        row_obj["destination_key"] = dest

        # Booking link (Skyscanner deep link pattern)
        out_fmt = out_date.replace("-", "")
        ret_fmt = ret_date.replace("-", "")
        booking = f"https://www.skyscanner.net/transport/flights/{origin}/{dest}/{out_fmt}/{ret_fmt}/"
        row_obj["affiliate_url"] = booking
        row_obj["booking_link_vip"] = booking
        row_obj["booking_link_free"] = booking

        rows_to_append.append([row_obj.get(h, "") for h in headers])
        inserted += 1

    if rows_to_append:
        raw_ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
    return inserted


# ============================================================
# SCORING (Elastic Scorer: theme weight soft, price can break through)
# ============================================================

def calc_value_score(price_gbp: float) -> float:
    # 0..1 (rough, tune later)
    if price_gbp <= 0:
        return 0.0
    if price_gbp < 15:
        return 0.65  # suspiciously low, but not auto-0
    if price_gbp > 900:
        return 0.0
    # map 30..300 to 1..0-ish
    return max(0.0, min(1.0, (350.0 - price_gbp) / 320.0))


def calc_route_score(stops: int) -> float:
    if stops <= 0:
        return 1.0
    if stops == 1:
        return 0.65
    return 0.25


def calc_timing_score(out_date: str) -> float:
    try:
        d = dt.date.fromisoformat(out_date)
        days = (d - dt.date.today()).days
        if 14 <= days <= 60:
            return 1.0
        if 7 <= days <= 120:
            return 0.75
        if 121 <= days <= 240:
            return 0.55
        return 0.35
    except Exception:
        return 0.5


def determine_deal_theme_from_signals(dest_iata: str, out_date: str, signals_map: Dict[str, Dict[str, Any]]) -> str:
    """
    Lightweight: if you already have resolved_theme formulas you can ignore this,
    but we keep it deterministic and safe.

    If signals exist and have sun/surf/snow scores, you can extend this later.
    For now:
      - winter months => winter_sun bias only if you tag via CONFIG routes
      - default => shoulder
    """
    _ = signals_map.get(dest_iata.upper(), {})
    # Keep conservative; your CONFIG.theme is the primary “intent”
    return "shoulder"


def score_row_elastic(
    row: Dict[str, Any],
    target_theme: str,
    recent_dest_penalty: bool,
    recent_theme_penalty: bool,
) -> Dict[str, Any]:
    """
    “Elastic scorer”:
      - Theme is a soft weight
      - Price weight ramps up if value is an outlier
      - Diversity penalty is harsh (kills repeats)
    """

    dest = safe_get(row, "destination_iata").upper()
    deal_theme = normalize_theme(safe_get(row, "deal_theme") or safe_get(row, "auto_theme") or safe_get(row, "resolved_theme") or "")

    price = safe_float(safe_get(row, "price_gbp"), 0.0)
    stops = int(safe_float(safe_get(row, "stops"), 0.0))
    out_date = safe_get(row, "outbound_date")

    value = calc_value_score(price)
    route = calc_route_score(stops)
    timing = calc_timing_score(out_date)

    # Soft theme score: match -> 1.0, adjacent -> 0.6, wildcard -> 0.3
    theme_match = (deal_theme == normalize_theme(target_theme))
    theme_score = 1.0 if theme_match else (0.6 if deal_theme and normalize_theme(target_theme) in deal_theme else 0.3)

    # Adaptive weights: if value is very strong, let it override theme
    if value >= 0.85:
        w_theme, w_value = 0.25, 0.55
    else:
        w_theme, w_value = 0.55, 0.30

    w_route, w_timing = 0.10, 0.05

    stotal = (theme_score * w_theme + value * w_value + route * w_route + timing * w_timing) * 100.0

    # Apply boredom penalties
    if recent_dest_penalty:
        stotal -= DEST_REPEAT_PENALTY
    if recent_theme_penalty:
        stotal -= THEME_REPEAT_PENALTY

    # Strength & angle type (simple, deterministic)
    if theme_score >= 0.85:
        theme_strength = "primary"
        angle_type = "classic_theme"
    elif theme_score >= 0.55:
        theme_strength = "adjacent"
        angle_type = "smart_alternative"
    else:
        theme_strength = "wildcard"
        angle_type = "price_led" if value >= 0.6 else ("routing_win" if route >= 0.9 else "quiet_season")

    angle_reason = (
        "Theme match" if theme_strength == "primary" else
        "Theme-adjacent" if theme_strength == "adjacent" else
        "Wildcard value"
    )

    return {
        "stotal": round(stotal, 1),
        "theme_score": round(theme_score, 3),
        "value_score": round(value, 3),
        "route_score": round(route, 3),
        "timing_score": round(timing, 3),
        "theme_strength": theme_strength,
        "angle_type": angle_type,
        "angle_reason": angle_reason,
        "deal_theme": deal_theme or "shoulder",
    }


def stage_score_new(raw_ws: gspread.Worksheet, headers: List[str], target_theme: str, signals_map: Dict[str, Dict[str, Any]]) -> int:
    hmap = header_map(headers)
    rows = raw_ws.get_all_values()
    if len(rows) < 2:
        return 0

    scored = 0
    for i in range(2, len(rows) + 1):
        vals = rows[i - 1]
        row = {headers[c]: (vals[c] if c < len(vals) else "") for c in range(len(headers))}

        if safe_get(row, "status").upper() != STATUS_NEW:
            continue

        # If already scored, skip
        if safe_get(row, "stotal") or safe_get(row, "ai_score"):
            continue

        dest = safe_get(row, "destination_iata").upper()
        out_date = safe_get(row, "outbound_date")
        deal_theme = determine_deal_theme_from_signals(dest, out_date, signals_map)

        updates: List[gspread.Cell] = []
        if "deal_theme" in hmap:
            updates.append(gspread.Cell(i, hmap["deal_theme"], deal_theme))

        # placeholder penalties decided later at selection time
        result = score_row_elastic(row, target_theme, False, False)

        # write score components if columns exist
        for k in ["stotal", "theme_score", "value_score", "route_score", "timing_score", "theme_strength", "angle_type", "angle_reason"]:
            if k in hmap and k in result:
                updates.append(gspread.Cell(i, hmap[k], result[k]))

        # also mirror to ai_score if present (so older tabs still work)
        if "ai_score" in hmap:
            updates.append(gspread.Cell(i, hmap["ai_score"], result["stotal"]))
        if "status" in hmap:
            updates.append(gspread.Cell(i, hmap["status"], STATUS_SCORED))
        if "scored_timestamp" in hmap:
            updates.append(gspread.Cell(i, hmap["scored_timestamp"], now_utc_str()))

        if updates:
            raw_ws.update_cells(updates, value_input_option="USER_ENTERED")
            scored += 1

    return scored


# ============================================================
# SELECTION (diversity force + safe datetimes)
# ============================================================

def get_recent_posts(rows: List[List[str]], headers: List[str], lookback_hours: int) -> Tuple[set, set, List[str]]:
    cutoff = now_utc_dt() - dt.timedelta(hours=lookback_hours)

    recent_dests = set()
    recent_themes = set()
    last3: List[str] = []

    # Walk from newest to oldest
    for i in range(len(rows) - 1, 0, -1):
        row = {headers[c]: (rows[i][c] if c < len(rows[i]) else "") for c in range(len(headers))}
        status = safe_get(row, "status").upper()

        if status not in (STATUS_POSTED_INSTAGRAM, STATUS_POSTED_TELEGRAM_VIP, STATUS_POSTED_ALL):
            continue

        posted_ts = safe_get(row, "ig_published_timestamp") or safe_get(row, "tg_monthly_timestamp") or safe_get(row, "published_timestamp")
        posted_dt = parse_iso_utc(posted_ts)
        if not posted_dt:
            continue

        if posted_dt <= cutoff:
            break

        dest_key = (safe_get(row, "destination_key") or safe_get(row, "destination_iata") or "").upper()
        theme = normalize_theme(safe_get(row, "deal_theme") or safe_get(row, "resolved_theme") or safe_get(row, "auto_theme") or "")

        if dest_key:
            recent_dests.add(dest_key)
            if len(last3) < 3:
                last3.append(dest_key)

        if theme:
            recent_themes.add(theme)

    return recent_dests, recent_themes, last3


def stage_select_best(raw_ws: gspread.Worksheet, headers: List[str], target_theme: str) -> int:
    hmap = header_map(headers)
    rows = raw_ws.get_all_values()
    if len(rows) < 2:
        return 0

    recent_dests, recent_themes, last3 = get_recent_posts(rows, headers, VARIETY_LOOKBACK_HOURS)

    recency_cutoff = now_utc_dt() - dt.timedelta(hours=RECENCY_FILTER_HOURS)

    candidates = []
    for i in range(2, len(rows) + 1):
        vals = rows[i - 1]
        row = {headers[c]: (vals[c] if c < len(vals) else "") for c in range(len(headers))}

        if safe_get(row, "status").upper() != STATUS_SCORED:
            continue

        # recency filter on date_added
        added = parse_iso_utc(safe_get(row, "date_added"))
        if added and added < recency_cutoff:
            continue

        dest_key = (safe_get(row, "destination_key") or safe_get(row, "destination_iata") or "").upper()
        if not dest_key:
            continue

        # recompute score WITH penalties applied
        deal_theme = normalize_theme(safe_get(row, "deal_theme") or "")
        r_dest = dest_key in recent_dests or dest_key in last3
        r_theme = deal_theme in recent_themes if deal_theme else False

        scored = score_row_elastic(row, target_theme, r_dest, r_theme)
        final_score = scored["stotal"]

        candidates.append((final_score, i, dest_key, deal_theme, scored))

    if not candidates:
        log("  No scored candidates available")
        return 0

    candidates.sort(key=lambda x: x[0], reverse=True)
    best_score, best_row_idx, best_dest, best_theme, scored = candidates[0]

    log(f"  ✓ SELECTED row {best_row_idx}: {best_dest} | theme={best_theme or 'n/a'} | score={best_score}")

    updates: List[gspread.Cell] = []
    if "final_score" in hmap:
        updates.append(gspread.Cell(best_row_idx, hmap["final_score"], best_score))
    if "status" in hmap:
        updates.append(gspread.Cell(best_row_idx, hmap["status"], STATUS_READY_TO_POST))

    # also keep a “reasons” text if column exists
    if "reasons" in hmap:
        reason = f"Picked via elastic scorer. dest_repeat={'YES' if best_dest in recent_dests else 'NO'}; theme_repeat={'YES' if best_theme in recent_themes else 'NO'}"
        updates.append(gspread.Cell(best_row_idx, hmap["reasons"], reason))

    if updates:
        raw_ws.update_cells(updates, value_input_option="USER_ENTERED")
        return 1

    return 0


# ============================================================
# RENDER (ensure city names)
# ============================================================

def render_image(row: Dict[str, Any]) -> str:
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
    return safe_text(r.json().get("graphic_url")).strip()


def stage_render(raw_ws: gspread.Worksheet, headers: List[str]) -> int:
    hmap = header_map(headers)
    rows = raw_ws.get_all_values()
    if len(rows) < 2:
        return 0

    rendered = 0
    for i in range(2, len(rows) + 1):
        vals = rows[i - 1]
        row = {headers[c]: (vals[c] if c < len(vals) else "") for c in range(len(headers))}

        if safe_get(row, "status").upper() != STATUS_READY_TO_POST:
            continue

        log(f"  Rendering row {i}")
        try:
            url = render_image(row)
        except Exception as e:
            log(f"  Render error: {e}")
            if "render_error" in hmap:
                raw_ws.update_cell(i, hmap["render_error"], str(e)[:250])
            continue

        updates: List[gspread.Cell] = []
        if "graphic_url" in hmap:
            updates.append(gspread.Cell(i, hmap["graphic_url"], url))
        if "rendered_timestamp" in hmap:
            updates.append(gspread.Cell(i, hmap["rendered_timestamp"], now_utc_str()))
        if "status" in hmap:
            updates.append(gspread.Cell(i, hmap["status"], STATUS_READY_TO_PUBLISH))

        if updates:
            raw_ws.update_cells(updates, value_input_option="USER_ENTERED")
            rendered += 1

    return rendered


# ============================================================
# INSTAGRAM (human-sounding, no hype, no AI mentions)
# ============================================================

def instagram_caption(row: Dict[str, Any], target_theme: str) -> str:
    origin = safe_get(row, "origin_city") or safe_get(row, "origin_iata")
    dest = safe_get(row, "destination_city") or safe_get(row, "destination_iata")
    price = f"£{round_price_up(safe_get(row, 'price_gbp'))}"
    out_date = safe_get(row, "outbound_date")
    ret_date = safe_get(row, "return_date")

    angle = normalize_theme(safe_get(row, "angle_type"))
    theme_strength = safe_get(row, "theme_strength") or "wildcard"

    # Keep it honest + benefit-led
    if angle == "routing_win":
        hook = "Direct routing is doing the heavy lifting here."
    elif angle == "quiet_season":
        hook = "Quieter dates, usually easier on your wallet."
    elif angle == "smart_alternative":
        hook = "Not the obvious pick — that’s the point."
    else:
        hook = "Worth a look at this price."

    caption = [
        f"{origin} → {dest} for {price}",
        "",
        f"Dates: {out_date} to {ret_date}",
        "",
        hook,
        f"Theme today: {normalize_theme(target_theme).replace('_',' ')} • {theme_strength}",
        "",
        "Link in bio for the free Telegram channel (VIP gets deals 24h early).",
    ]
    return "\n".join(caption)[:2200]


def post_instagram(raw_ws: gspread.Worksheet, headers: List[str], target_theme: str) -> int:
    hmap = header_map(headers)
    rows = raw_ws.get_all_values()
    if len(rows) < 2:
        return 0

    posted = 0
    for i in range(2, len(rows) + 1):
        vals = rows[i - 1]
        row = {headers[c]: (vals[c] if c < len(vals) else "") for c in range(len(headers))}

        if safe_get(row, "status").upper() != STATUS_READY_TO_PUBLISH:
            continue

        graphic_url = safe_get(row, "graphic_url")
        if not graphic_url:
            continue

        caption = instagram_caption(row, target_theme)
        cache_buster = int(time.time())
        image_url_cb = f"{graphic_url}?cb={cache_buster}"

        log(f"  Posting to Instagram row {i}")

        try:
            # create media container
            create_url = f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media"
            r1 = requests.post(
                create_url,
                data={
                    "image_url": image_url_cb,
                    "caption": caption,
                    "access_token": IG_ACCESS_TOKEN,
                },
                timeout=30,
            )
            r1.raise_for_status()
            creation_id = safe_text(r1.json().get("id")).strip()
            if not creation_id:
                raise RuntimeError("No creation_id returned")

            # wait until finished
            waited = 0
            while waited < 60:
                rs = requests.get(
                    f"https://graph.facebook.com/v20.0/{creation_id}",
                    params={"fields": "status_code", "access_token": IG_ACCESS_TOKEN},
                    timeout=10,
                )
                if rs.status_code == 200:
                    sc = safe_text(rs.json().get("status_code")).strip()
                    if sc == "FINISHED":
                        break
                    if sc in ("ERROR", "EXPIRED"):
                        raise RuntimeError(f"Media status: {sc}")
                time.sleep(2)
                waited += 2

            # publish
            r2 = requests.post(
                f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media_publish",
                data={"creation_id": creation_id, "access_token": IG_ACCESS_TOKEN},
                timeout=30,
            )
            r2.raise_for_status()
            media_id = safe_text(r2.json().get("id")).strip()

            updates: List[gspread.Cell] = []
            if "ig_creation_id" in hmap:
                updates.append(gspread.Cell(i, hmap["ig_creation_id"], creation_id))
            if "ig_media_id" in hmap and media_id:
                updates.append(gspread.Cell(i, hmap["ig_media_id"], media_id))
            if "ig_published_timestamp" in hmap:
                updates.append(gspread.Cell(i, hmap["ig_published_timestamp"], now_utc_str()))
            if "published_timestamp" in hmap:
                updates.append(gspread.Cell(i, hmap["published_timestamp"], now_utc_str()))
            if "status" in hmap:
                updates.append(gspread.Cell(i, hmap["status"], STATUS_POSTED_INSTAGRAM))

            if updates:
                raw_ws.update_cells(updates, value_input_option="USER_ENTERED")

            posted += 1
            log("  ✓ Posted to Instagram")
        except Exception as e:
            log(f"  Instagram error: {e}")
            if "last_error" in hmap:
                raw_ws.update_cell(i, hmap["last_error"], str(e)[:250])

    return posted


# ============================================================
# TELEGRAM (VIP first, Free later)
# ============================================================

def tg_send(token: str, chat: str, text: str) -> str:
    r = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat, "text": text, "parse_mode": "HTML", "disable_web_page_preview": False},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    try:
        return str(data["result"]["message_id"])
    except Exception:
        return ""


def fmt_date_short(iso_date: str) -> str:
    try:
        d = dt.date.fromisoformat(iso_date)
        return d.strftime("%d %b %Y")
    except Exception:
        return iso_date


def format_telegram_vip(row: Dict[str, Any]) -> str:
    price = round_price_up(safe_get(row, "price_gbp"))
    dest_city = safe_get(row, "destination_city") or safe_get(row, "destination_iata")
    dest_country = safe_get(row, "destination_country")
    origin_city = safe_get(row, "origin_city") or safe_get(row, "origin_iata")
    out_date = fmt_date_short(safe_get(row, "outbound_date"))
    ret_date = fmt_date_short(safe_get(row, "return_date"))
    link = safe_get(row, "booking_link_vip") or safe_get(row, "affiliate_url")

    where = f"{dest_city}, {dest_country}" if dest_country else dest_city

    angle = normalize_theme(safe_get(row, "angle_type"))
    if angle == "routing_win":
        why = "Direct routing — less faff, more time there."
    elif angle == "quiet_season":
        why = "Quieter dates — often better value."
    elif angle == "smart_alternative":
        why = "A smarter alternative to the usual hotspots."
    else:
        why = "Good value for the route."

    msg = (
        f"<b>£{price} to {where}</b>\n\n"
        f"<b>From:</b> {origin_city}\n"
        f"<b>Out:</b> {out_date}\n"
        f"<b>Back:</b> {ret_date}\n\n"
        f"<b>Why it’s worth a look:</b>\n• {why}\n\n"
    )
    if link:
        msg += f'<a href="{link}">Book now</a>'
    return msg


def format_telegram_free(row: Dict[str, Any]) -> str:
    price = round_price_up(safe_get(row, "price_gbp"))
    dest_city = safe_get(row, "destination_city") or safe_get(row, "destination_iata")
    dest_country = safe_get(row, "destination_country")
    origin_city = safe_get(row, "origin_city") or safe_get(row, "origin_iata")
    out_date = fmt_date_short(safe_get(row, "outbound_date"))
    ret_date = fmt_date_short(safe_get(row, "return_date"))
    link = safe_get(row, "booking_link_free") or safe_get(row, "affiliate_url")

    where = f"{dest_city}, {dest_country}" if dest_country else dest_city

    msg = (
        f"<b>£{price} to {where}</b>\n\n"
        f"<b>From:</b> {origin_city}\n"
        f"<b>Out:</b> {out_date}\n"
        f"<b>Back:</b> {ret_date}\n\n"
        f"VIP members saw this <b>24 hours earlier</b>.\n\n"
    )
    if link:
        msg += f'<a href="{link}">Book now</a>\n\n'
    msg += (
        "<b>Want earlier access?</b>\n"
        "• Deals 24h early\n"
        "• Cancel anytime\n\n"
    )
    if STRIPE_LINK_MONTHLY:
        msg += f'<a href="{STRIPE_LINK_MONTHLY}">Upgrade to VIP</a>'
    return msg


def post_telegram_vip(raw_ws: gspread.Worksheet, headers: List[str]) -> int:
    if RUN_SLOT != "AM":
        return 0

    hmap = header_map(headers)
    rows = raw_ws.get_all_values()
    if len(rows) < 2:
        return 0

    posted = 0
    for i in range(2, len(rows) + 1):
        vals = rows[i - 1]
        row = {headers[c]: (vals[c] if c < len(vals) else "") for c in range(len(headers))}

        if safe_get(row, "status").upper() != STATUS_POSTED_INSTAGRAM:
            continue

        if safe_get(row, "tg_monthly_timestamp") or safe_get(row, "telegram_vip_msg_id"):
            continue

        log(f"  Telegram VIP post row {i}")
        try:
            msg = format_telegram_vip(row)
            mid = tg_send(TELEGRAM_BOT_TOKEN_VIP, TELEGRAM_CHANNEL_VIP, msg)

            updates: List[gspread.Cell] = []
            if "tg_monthly_timestamp" in hmap:
                updates.append(gspread.Cell(i, hmap["tg_monthly_timestamp"], now_utc_str()))
            if "telegram_vip_msg_id" in hmap and mid:
                updates.append(gspread.Cell(i, hmap["telegram_vip_msg_id"], mid))
            if "posted_to_vip" in hmap:
                updates.append(gspread.Cell(i, hmap["posted_to_vip"], "TRUE"))
            if "status" in hmap:
                updates.append(gspread.Cell(i, hmap["status"], STATUS_POSTED_TELEGRAM_VIP))

            if updates:
                raw_ws.update_cells(updates, value_input_option="USER_ENTERED")

            posted += 1
            log("  ✓ VIP posted")
        except Exception as e:
            log(f"  VIP error: {e}")
            if "last_error" in hmap:
                raw_ws.update_cell(i, hmap["last_error"], str(e)[:250])

    return posted


def post_telegram_free(raw_ws: gspread.Worksheet, headers: List[str]) -> int:
    if RUN_SLOT != "PM":
        return 0

    hmap = header_map(headers)
    rows = raw_ws.get_all_values()
    if len(rows) < 2:
        return 0

    posted = 0
    for i in range(2, len(rows) + 1):
        vals = rows[i - 1]
        row = {headers[c]: (vals[c] if c < len(vals) else "") for c in range(len(headers))}

        if safe_get(row, "status").upper() != STATUS_POSTED_TELEGRAM_VIP:
            continue

        if safe_get(row, "tg_free_timestamp") or safe_get(row, "telegram_free_msg_id"):
            continue

        vip_ts = safe_get(row, "tg_monthly_timestamp")
        if hours_since(vip_ts) < VIP_DELAY_HOURS:
            continue

        log(f"  Telegram FREE post row {i}")
        try:
            msg = format_telegram_free(row)
            mid = tg_send(TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL, msg)

            updates: List[gspread.Cell] = []
            if "tg_free_timestamp" in hmap:
                updates.append(gspread.Cell(i, hmap["tg_free_timestamp"], now_utc_str()))
            if "telegram_free_msg_id" in hmap and mid:
                updates.append(gspread.Cell(i, hmap["telegram_free_msg_id"], mid))
            if "posted_for_free" in hmap:
                updates.append(gspread.Cell(i, hmap["posted_for_free"], "TRUE"))
            if "status" in hmap:
                updates.append(gspread.Cell(i, hmap["status"], STATUS_POSTED_ALL))

            if updates:
                raw_ws.update_cells(updates, value_input_option="USER_ENTERED")

            posted += 1
            log("  ✓ FREE posted")
        except Exception as e:
            log(f"  FREE error: {e}")
            if "last_error" in hmap:
                raw_ws.update_cell(i, hmap["last_error"], str(e)[:250])

    return posted


# ============================================================
# OPTIONAL: MAILERLITE EXPORT TAB (for later integration)
# ============================================================

def stage_export_mailerlite(sh: gspread.Spreadsheet, raw_ws: gspread.Worksheet, headers: List[str]) -> int:
    """
    Writes a simple feed row into MAILERLITE_FEED for any deal that reached POSTED_ALL today.
    This does NOT call MailerLite API. It just prepares a clean feed tab for an email workflow.
    """
    try:
        ml_ws = ensure_tab(
            sh,
            MAILERLITE_FEED_TAB,
            headers=["timestamp", "origin", "destination", "price_gbp", "outbound", "return", "theme", "angle", "link"],
        )
    except Exception:
        return 0

    hmap = header_map(headers)
    rows = raw_ws.get_all_values()
    if len(rows) < 2:
        return 0

    wrote = 0
    today = dt.date.today()

    for i in range(2, len(rows) + 1):
        vals = rows[i - 1]
        row = {headers[c]: (vals[c] if c < len(vals) else "") for c in range(len(headers))}
        if safe_get(row, "status").upper() != STATUS_POSTED_ALL:
            continue

        ts = parse_iso_utc(safe_get(row, "tg_free_timestamp") or safe_get(row, "published_timestamp") or "")
        if not ts:
            continue
        if ts.date() != today:
            continue

        origin = safe_get(row, "origin_city") or safe_get(row, "origin_iata")
        dest = safe_get(row, "destination_city") or safe_get(row, "destination_iata")
        price = safe_get(row, "price_gbp")
        outd = safe_get(row, "outbound_date")
        retd = safe_get(row, "return_date")
        theme = safe_get(row, "deal_theme") or safe_get(row, "resolved_theme")
        angle = safe_get(row, "angle_type")
        link = safe_get(row, "affiliate_url") or safe_get(row, "booking_link_free")

        ml_ws.append_row(
            [now_utc_str(), origin, dest, price, outd, retd, theme, angle, link],
            value_input_option="USER_ENTERED",
        )
        wrote += 1

    return wrote


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    today = dt.date.today()
    daily_theme, longhaul_region = todays_theme_and_region(today)

    log("=" * 70)
    log("TRAVELTXTER V4.5.3 — WATERWHEEL RUN")
    log("=" * 70)
    log(f"RUN_SLOT: {RUN_SLOT} | VIP_DELAY: {VIP_DELAY_HOURS}h")
    log(f"Daily Theme (target): {daily_theme}" + (f" | Longhaul region: {longhaul_region}" if longhaul_region else ""))
    log(f"Variety: lookback={VARIETY_LOOKBACK_HOURS}h | DEST_REPEAT_PENALTY={DEST_REPEAT_PENALTY} | THEME_REPEAT_PENALTY={THEME_REPEAT_PENALTY}")
    log(f"Duffel: ENABLED={DUFFEL_ENABLED} | MAX_INSERTS={DUFFEL_MAX_INSERTS} | ROUTES_PER_RUN={DUFFEL_ROUTES_PER_RUN} | SEARCH_CAP={DUFFEL_MAX_SEARCHES_PER_RUN}")
    log("=" * 70)

    sh = get_spreadsheet()
    raw_ws = sh.worksheet(RAW_DEALS_TAB)
    headers = get_raw_headers(raw_ws)

    log(f"Connected to sheet | Columns: {len(headers)} | Tab: {RAW_DEALS_TAB}")

    signals_map = load_config_signals(sh)
    routes = load_routes_from_config(sh)
    log(f"Loaded CONFIG routes: {len(routes)} enabled")

    # 1) Feed
    log("\n[1] DUFFEL FEED")
    inserted = run_duffel_feeder(
        raw_ws=raw_ws,
        headers=headers,
        routes=routes,
        today_theme=daily_theme,
        longhaul_region=longhaul_region,
        signals_map=signals_map,
    )
    log(f"✓ {inserted} deals inserted")

    # 2) Score
    log("\n[2] SCORING (NEW → SCORED)")
    scored = stage_score_new(raw_ws, headers, daily_theme, signals_map)
    log(f"✓ {scored} deals scored")

    # 3) Select
    log("\n[3] EDITORIAL SELECTION (SCORED → READY_TO_POST)")
    selected = stage_select_best(raw_ws, headers, daily_theme)
    log(f"✓ {selected} promoted")

    # 4) Render
    log("\n[4] RENDER (READY_TO_POST → READY_TO_PUBLISH)")
    rendered = stage_render(raw_ws, headers)
    log(f"✓ {rendered} rendered")

    # 5) Instagram
    log("\n[5] INSTAGRAM (READY_TO_PUBLISH → POSTED_INSTAGRAM)")
    ig_posted = post_instagram(raw_ws, headers, daily_theme)
    log(f"✓ {ig_posted} posted")

    # 6) Telegram VIP (AM)
    log("\n[6] TELEGRAM VIP (POSTED_INSTAGRAM → POSTED_TELEGRAM_VIP)")
    vip_posted = post_telegram_vip(raw_ws, headers)
    log(f"✓ {vip_posted} posted")

    # 7) Telegram FREE (PM after delay)
    log("\n[7] TELEGRAM FREE (POSTED_TELEGRAM_VIP → POSTED_ALL)")
    free_posted = post_telegram_free(raw_ws, headers)
    log(f"✓ {free_posted} posted")

    # 8) Optional: export for MailerLite workflow
    log("\n[8] MAILERLITE FEED EXPORT (optional)")
    exported = stage_export_mailerlite(sh, raw_ws, headers)
    log(f"✓ {exported} exported rows")

    log("\n" + "=" * 70)
    log("COMPLETE")
    log("=" * 70)


if __name__ == "__main__":
    main()
