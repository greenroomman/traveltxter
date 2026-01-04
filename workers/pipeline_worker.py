#!/usr/bin/env python3
"""
TravelTxter V4.5.3 — WATERWHEEL (Route-first + Theme/Region rotation)

Locked design goals
-------------------
0) ✅ Operational continuity: always publish something honest.
1) ✅ Theme rotation (daily) + Long-haul region rotation (Americas/Asia/Africa/Australasia).
2) ✅ Route-first feeder (CONFIG-driven), true IATA codes only (no LON pseudo-codes).
3) ✅ Direct-first at API level (max_connections=0 where possible), with safe fallbacks.
4) ✅ Budget carrier bias where relevant (U2/FR/W6/LS), also guarded by fallback logic.
5) ✅ Elastic scorer (theme is soft weight; price can break through).
6) ✅ Strong diversity penalties (kills Iceland loops / repeats).
7) ✅ Guaranteed city names everywhere (render, Instagram, Telegram, MailerLite export).
8) ✅ VIP → Free delay enforced (AM VIP, PM Free; default 24h).
9) ✅ Timezone-safe datetime handling (no naive/aware crashes).

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


def format_price_gbp(price_str: str) -> str:
    """Format GBP price as '£103.35' (2dp when possible)."""
    try:
        v = float(str(price_str).strip())
        return f"
