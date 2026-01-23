#!/usr/bin/env python3
"""
workers/pipeline_worker.py

TravelTxter Pipeline Worker (FEEDER) — DESTINATION-FIRST FLOOR-FINDER (Appendix E)
Built as a full-file replacement from the existing V4.7.10+ feeder.

LOCKED PRINCIPLES:
- Replace full file only.
- Google Sheets is the single source of truth.
- Do NOT write to RAW_DEALS_VIEW.
- Do NOT touch other workers.

DESTINATION-FIRST INVARIANT (Appendix E):
For each selected destination:
- Search across allowed origins + small date jitter (±2 days),
- Pool offers across origin×date,
- Apply hard gates (GBP, benchmarks, capability, hygiene),
- Insert exactly ONE winner row (cheapest viable),
- Hard-cap total Duffel searches by DUFFEL_MAX_SEARCHES_PER_RUN.
"""

from __future__ import annotations

# ==================== PYTHONPATH GUARD ====================
import os
import sys

WORKERS_DIR = os.path.dirname(os.path.abspath(__file__))
if WORKERS_DIR not in sys.path:
    sys.path.insert(0, WORKERS_DIR)

# ==================== STANDARD IMPORTS ====================
import json
import time
import math
import hashlib
import datetime as dt
from typing import Any, Dict, List, Tuple, Optional, Set

import requests
import gspread
from google.oauth2.service_account import Credentials

# ==================== ENV / TABS ====================

RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS").strip() or "RAW_DEALS"
CONFIG_TAB = os.getenv("FEEDER_CONFIG_TAB", os.getenv("CONFIG_TAB", "CONFIG")).strip() or "CONFIG"
THEMES_TAB = os.getenv("THEMES_TAB", "THEMES").strip() or "THEMES"
SIGNALS_TAB = os.getenv("SIGNALS_TAB", os.getenv("CONFIG_SIGNALS_TAB", "CONFIG_SIGNALS")).strip() or "CONFIG_SIGNALS"
CAPABILITY_TAB = os.getenv("CAPABILITY_TAB", "ROUTE_CAPABILITY_MAP").strip() or "ROUTE_CAPABILITY_MAP"
BENCHMARKS_TAB = os.getenv("BENCHMARKS_TAB", "ZONE_THEME_BENCHMARKS").strip() or "ZONE_THEME_BENCHMARKS"

SPREADSHEET_ID = (os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID") or "").strip()

DUFFEL_API_KEY = os.getenv("DUFFEL_API_KEY", "").strip()
DUFFEL_API_BASE = os.getenv("DUFFEL_API_BASE", "https://api.duffel.com").strip()
DUFFEL_VERSION = os.getenv("DUFFEL_VERSION", "v2").strip() or "v2"

RUN_SLOT = (os.getenv("RUN_SLOT") or "").strip().upper()

# ==================== GOVERNORS (SAFE CASTING) ====================

def _get_int(primary: str, fallback: str, default: int) -> int:
    v = (os.getenv(primary) or "").strip()
    if v:
        try:
            return int(float(v))
        except Exception:
            pass
    v2 = (os.getenv(fallback) or "").strip()
    if v2:
        try:
            return int(float(v2))
        except Exception:
            pass
    return int(default)


def _get_float(primary: str, fallback: str, default: float) -> float:
    v = (os.getenv(primary) or "").strip()
    if v:
        try:
            return float(v)
        except Exception:
            pass
    v2 = (os.getenv(fallback) or "").strip()
    if v2:
        try:
            return float(v2)
        except Exception:
            pass
    return float(default)


DUFFEL_MAX_INSERTS = _get_int("DUFFEL_MAX_INSERTS", "FEEDER_MAX_INSERTS", 3)
DUFFEL_MAX_SEARCHES_PER_RUN = _get_int("DUFFEL_MAX_SEARCHES_PER_RUN", "FEEDER_MAX_SEARCHES", 4)
DUFFEL_ROUTES_PER_RUN = _get_int("DUFFEL_ROUTES_PER_RUN", "FEEDER_ROUTES_PER_RUN", 3)

DUFFEL_OFFERS_PER_SEARCH = _get_int("DUFFEL_OFFERS_PER_SEARCH", "OFFERS_PER_SEARCH", 50)

# Still respected, but destination-first inserts max 1 per destination.
DUFFEL_MAX_INSERTS_PER_ROUTE = _get_int("DUFFEL_MAX_INSERTS_PER_ROUTE", "MAX_INSERTS_PER_ROUTE", 10)
DUFFEL_MAX_INSERTS_PER_ORIGIN = _get_int("DUFFEL_MAX_INSERTS_PER_ORIGIN", "MAX_INSERTS_PER_ORIGIN", 10)

FEEDER_SLEEP_SECONDS = _get_float("FEEDER_SLEEP_SECONDS", "FEEDER_SLEEP_SECONDS", 0.0)

FEEDER_OPEN_ORIGINS = (os.getenv("FEEDER_OPEN_ORIGINS", "false").strip().lower() == "true")
STRICT_CAPABILITY_MAP = (os.getenv("STRICT_CAPABILITY_MAP", "true").strip().lower() == "true")

# Use existing switch as a compatibility lever:
# - If TRUE: origins per destination are constrained to CONFIG origins for that destination (theme rows).
# - If FALSE: origins per destination come from ORIGINS_<THEME> (or fallback pools).
RESPECT_CONFIG_ORIGIN = (os.getenv("RESPECT_CONFIG_ORIGIN", "true").strip().lower() == "true")

DEFAULT_DAYS_AHEAD_MIN = _get_int("DAYS_AHEAD_MIN", "DAYS_AHEAD_MIN", 21)
DEFAULT_DAYS_AHEAD_MAX = _get_int("DAYS_AHEAD_MAX", "DAYS_AHEAD_MAX", 84)
DEFAULT_TRIP_LENGTH_DAYS = _get_int("TRIP_LENGTH_DAYS", "TRIP_LENGTH_DAYS", 5)

INVENTORY_MIN_DAYS = _get_int("INVENTORY_MIN_DAYS", "INVENTORY_MIN_DAYS", 21)
INVENTORY_MAX_DAYS = _get_int("INVENTORY_MAX_DAYS", "INVENTORY_MAX_DAYS", 84)

FEEDER_EXPLORE_RUN_MOD = _get_int("FEEDER_EXPLORE_RUN_MOD", "FEEDER_EXPLORE_RUN_MOD", 10)
FEEDER_EXPLORE_SALT = (os.getenv("FEEDER_EXPLORE_SALT", "traveltxter") or "traveltxter").strip()

PRICE_GATE_ENABLED = (os.getenv("PRICE_GATE_ENABLED", "true").strip().lower() == "true")
PRICE_GATE_MULTIPLIER = _get_float("PRICE_GATE_MULTIPLIER", "PRICE_GATE_MULTIPLIER", 1.0)
PRICE_GATE_MIN_CAP_GBP = _get_float("PRICE_GATE_MIN_CAP_GBP", "PRICE_GATE_MIN_CAP_GBP", 80.0)
PRICE_GATE_FALLBACK_BEHAVIOR = (os.getenv("PRICE_GATE_FALLBACK_BEHAVIOR", "BLOCK").strip().upper() or "BLOCK")

ZERO_OFFER_RETRY_ENABLED = (os.getenv("ZERO_OFFER_RETRY_ENABLED", "true").strip().lower() == "true")
ZERO_OFFER_RETRY_MAX_DAYS = _get_int("ZERO_OFFER_RETRY_MAX_DAYS", "ZERO_OFFER_RETRY_MAX_DAYS", 60)

# DESTINATION-FIRST jitter window (Appendix E default ±2)
DATE_JITTER_DAYS = _get_int("DATE_JITTER_DAYS", "DATE_JITTER_DAYS", 2)

# ==================== HYGIENE GATE ====================
HYGIENE_ENABLED = (os.getenv("HYGIENE_ENABLED", "true").strip().lower() == "true")
OFFER_MAX_CONNECTIONS_SHORTHAUL = _get_int("OFFER_MAX_CONNECTIONS_SHORTHAUL", "OFFER_MAX_CONNECTIONS_SHORTHAUL", 1)
OFFER_MAX_CONNECTIONS_LONGHAUL = _get_int("OFFER_MAX_CONNECTIONS_LONGHAUL", "OFFER_MAX_CONNECTIONS_LONGHAUL", 2)
OFFER_MAX_DURATION_MINUTES_SHORTHAUL = _get_int("OFFER_MAX_DURATION_MINUTES_SHORTHAUL", "OFFER_MAX_DURATION_MINUTES_SHORTHAUL", 720)
OFFER_MAX_DURATION_MINUTES_LONGHAUL = _get_int("OFFER_MAX_DURATION_MINUTES_LONGHAUL", "OFFER_MAX_DURATION_MINUTES_LONGHAUL", 1200)
QUALITY_PRICE_BAND_SHORTHAUL = _get_float("QUALITY_PRICE_BAND_SHORTHAUL", "QUALITY_PRICE_BAND_SHORTHAUL", 0.85)
QUALITY_PRICE_BAND_LONGHAUL = _get_float("QUALITY_PRICE_BAND_LONGHAUL", "QUALITY_PRICE_BAND_LONGHAUL", 0.95)

# ==================== THEMES ====================

MASTER_THEMES = [
    "winter_sun",
    "summer_sun",
    "beach_break",
    "snow",
    "northern_lights",
    "surf",
    "adventure",
    "city_breaks",
    "culture_history",
    "long_haul",
    "luxury_value",
    "unexpected_value",
]

SPARSE_THEMES = {"northern_lights"}

# Pools
SW_ENGLAND_DEFAULT = ["BRS", "EXT", "NQY", "SOU", "CWL", "BOH"]
LONDON_LCC = ["STN", "LTN", "LGW"]
LONDON_FULL = ["LHR", "LGW", "STN", "LTN", "LCY", "SEN"]
MIDLANDS = ["BHX", "EMA"]
NORTH = ["MAN", "LBA", "NCL", "LPL"]
SCOTLAND = ["EDI", "GLA"]
NI = ["BFS", "BHD"]

LONGHAUL_PRIMARY_HUBS = ["LHR", "LGW"]
LONGHAUL_SECONDARY_HUBS = ["MAN"]

ORIGIN_CITY_FALLBACK = {
    "LHR": "London", "LGW": "London", "STN": "London", "LTN": "London", "LCY": "London", "SEN": "London",
    "MAN": "Manchester", "BHX": "Birmingham", "EMA": "East Midlands", "LPL": "Liverpool", "LBA": "Leeds",
    "NCL": "Newcastle", "EDI": "Edinburgh", "GLA": "Glasgow", "BFS": "Belfast", "BHD": "Belfast",
    "BRS": "Bristol", "EXT": "Exeter", "NQY": "Newquay", "SOU": "Southampton", "CWL": "Cardiff", "BOH": "Bournemouth",
}

# ==================== LOGGING ====================

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


def _clean_iata(x: Any) -> str:
    return str(x or "").strip().upper()[:3]


def _dedupe_keep_order(seq: List[str]) -> List[str]:
    out: List[str] = []
    for x in seq:
        xx = _clean_iata(x)
        if not xx:
            continue
        if xx not in out:
            out.append(xx)
    return out


def _stable_mod(key: str, mod: int) -> int:
    h = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return int(h[:8], 16) % max(1, mod)


def theme_of_day_utc() -> str:
    today = dt.datetime.utcnow().date()
    doy = int(today.strftime("%j"))
    return MASTER_THEMES[doy % len(MASTER_THEMES)]


def should_do_explore_this_run(theme_today: str) -> bool:
    today = dt.datetime.utcnow().date().isoformat()
    gh_run_id = (os.getenv("GITHUB_RUN_ID") or "").strip()
    gh_run_attempt = (os.getenv("GITHUB_RUN_ATTEMPT") or "").strip()
    key = f"{FEEDER_EXPLORE_SALT}|{today}|{RUN_SLOT}|{theme_today}|{gh_run_id}|{gh_run_attempt}"
    return _stable_mod(key, FEEDER_EXPLORE_RUN_MOD) == 0


def _sw_england_origins_from_env() -> List[str]:
    raw = (os.getenv("SW_ENGLAND_ORIGINS", "") or "").strip()
    if not raw:
        return SW_ENGLAND_DEFAULT[:]
    parts = [p.strip().upper() for p in raw.split(",")]
    parts = [p for p in parts if p]
    return parts or SW_ENGLAND_DEFAULT[:]


def _theme_origins_from_env(theme_today: str) -> List[str]:
    key = f"ORIGINS_{str(theme_today or '').strip().upper()}"
    raw = (os.getenv(key, "") or "").strip()
    if not raw:
        return []
    parts = [p.strip().upper() for p in raw.split(",")]
    parts = [p for p in parts if p]
    return _dedupe_keep_order(parts)


def origin_plan_for_theme(theme_today: str, plan_n: int) -> List[str]:
    sw = _dedupe_keep_order(_sw_england_origins_from_env())
    explicit = _theme_origins_from_env(theme_today)
    if explicit:
        return explicit[:plan_n]

    # fallback pools (legacy behaviour)
    if theme_today == "surf":
        pools = _dedupe_keep_order(sw + LONDON_LCC)
    elif theme_today in ("long_haul",):
        pools = _dedupe_keep_order(LONGHAUL_PRIMARY_HUBS + LONGHAUL_SECONDARY_HUBS)
    elif theme_today in ("luxury_value", "unexpected_value"):
        pools = _dedupe_keep_order(LONDON_FULL + MIDLANDS + ["MAN"] + SCOTLAND + sw)
    else:
        pools = _dedupe_keep_order(sw + LONDON_LCC + MIDLANDS + ["MAN"] + SCOTLAND)

    return pools[:plan_n]


def resolve_origin_city(iata: str, origin_city_map: Dict[str, str]) -> str:
    i = _clean_iata(iata)
    return (origin_city_map.get(i) or ORIGIN_CITY_FALLBACK.get(i) or "").strip()


# ==================== GOOGLE SHEETS ====================

def _parse_sa_json(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return json.loads(raw.replace("\\n", "\n"))


def gs_client() -> gspread.Client:
    raw = (os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON") or "").strip()
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
    info = _parse_sa_json(raw)
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds)


def load_config_rows(sheet: gspread.Spreadsheet) -> List[Dict[str, Any]]:
    ws = sheet.worksheet(CONFIG_TAB)
    rows = ws.get_all_records()
    out: List[Dict[str, Any]] = []
    for r in rows:
        enabled = str(r.get("enabled", "")).strip().lower()
        if enabled in ("true", "yes", "1", "y"):
            out.append(r)
    return out


def load_themes_dict(sheet: gspread.Spreadsheet) -> Dict[str, List[Dict[str, Any]]]:
    ws = sheet.worksheet(THEMES_TAB)
    rows = ws.get_all_records()
    themes: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        t = str(r.get("theme", "")).strip()
        if not t:
            continue
        themes.setdefault(t, []).append(r)
    return themes


def load_signals(sheet: gspread.Spreadsheet) -> Dict[str, Dict[str, Any]]:
    ws = sheet.worksheet(SIGNALS_TAB)
    rows = ws.get_all_records()
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        key = _clean_iata(r.get("destination_iata") or r.get("iata_hint") or r.get("iata"))
        if key:
            out[key] = r
    return out


def load_route_capability_map(sheet: gspread.Spreadsheet) -> Tuple[Set[Tuple[str, str]], Dict[str, str]]:
    ws = sheet.worksheet(CAPABILITY_TAB)
    rows = ws.get_all_records()

    allowed: Set[Tuple[str, str]] = set()
    origin_city_map: Dict[str, str] = {}

    for r in rows:
        o = _clean_iata(r.get("origin_iata"))
        d = _clean_iata(r.get("destination_iata"))
        oc = str(r.get("origin_city", "")).strip()
        if o and d:
            allowed.add((o, d))
            if oc and o not in origin_city_map:
                origin_city_map[o] = oc

    if not allowed:
        msg = f"{CAPABILITY_TAB} is empty or missing required headers"
        if STRICT_CAPABILITY_MAP:
            raise RuntimeError(msg)
        log(f"⚠️ {msg} — continuing WITHOUT capability filtering.")

    return allowed, origin_city_map


# ==================== BENCHMARKS ====================

def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def _split_examples(x: Any) -> List[str]:
    s = str(x or "").strip()
    if not s:
        return []
    parts = [p.strip().upper() for p in s.split(",")]
    return [p for p in parts if p]


def load_zone_theme_benchmarks(sheet: gspread.Spreadsheet) -> List[Dict[str, Any]]:
    ws = sheet.worksheet(BENCHMARKS_TAB)
    rows = ws.get_all_records()
    out: List[Dict[str, Any]] = []

    for r in rows:
        theme = str(r.get("theme") or "").strip()
        origin = _clean_iata(r.get("origin_iata") or "")
        examples = _split_examples(r.get("destination_examples") or "")

        max_price = _to_float(r.get("max_price"))
        error_price = _to_float(r.get("error_price")) or 0.0

        if not theme or not origin or max_price is None:
            continue

        out.append(
            {
                "theme": theme,
                "origin_iata": origin,
                "destination_examples": examples,
                "max_price": float(max_price),
                "error_price": float(error_price),
            }
        )
    return out


def compute_ingest_cap_gbp(benchmarks: List[Dict[str, Any]], theme: str, origin: str, destination: str) -> Optional[float]:
    t = str(theme or "").strip()
    o = _clean_iata(origin)
    d = _clean_iata(destination)

    best: Optional[Dict[str, Any]] = None
    fallback: Optional[Dict[str, Any]] = None

    for r in benchmarks:
        if str(r.get("theme", "")).strip() != t:
            continue
        if _clean_iata(r.get("origin_iata")) != o:
            continue
        if fallback is None:
            fallback = r

        examples = r.get("destination_examples") or []
        if examples and d in examples:
            best = r
            break

    chosen = best if best is not None else fallback
    if not chosen:
        return None

    base = float(chosen.get("max_price") or 0.0) + float(chosen.get("error_price") or 0.0)
    if base <= 0:
        return None

    cap = max(float(PRICE_GATE_MIN_CAP_GBP), base * float(PRICE_GATE_MULTIPLIER))
    return float(cap)


# ==================== DUFFEL ====================

def duffel_headers() -> Dict[str, str]:
    if not DUFFEL_API_KEY:
        raise RuntimeError("Missing DUFFEL_API_KEY")
    return {
        "Authorization": f"Bearer {DUFFEL_API_KEY}",
        "Duffel-Version": DUFFEL_VERSION,
        "Content-Type": "application/json",
    }


def duffel_search_offer_request(payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{DUFFEL_API_BASE}/air/offer_requests"
    r = requests.post(url, headers=duffel_headers(), json=payload, timeout=90)
    if r.status_code >= 400:
        raise RuntimeError(f"Duffel offer_requests failed: {r.status_code} {r.text[:500]}")
    return r.json()


def offer_price_gbp(offer: Dict[str, Any]) -> float:
    cur = str(offer.get("total_currency") or "").strip().upper()
    if cur != "GBP":
        return 0.0
    try:
        return float(offer.get("total_amount"))
    except Exception:
        return 0.0


def offer_connections_safe(offer: Dict[str, Any]) -> int:
    try:
        slices = offer.get("slices") or []
        if not slices:
            return 99
        if len(slices) >= 2:
            out_segs = len((slices[0] or {}).get("segments") or [])
            in_segs = len((slices[1] or {}).get("segments") or [])
            if out_segs <= 0 or in_segs <= 0:
                return 99
            return max(0, (out_segs - 1)) + max(0, (in_segs - 1))
        seg_total = 0
        for s in slices:
            seg_total += len((s or {}).get("segments") or [])
        return max(0, seg_total - 1) if seg_total > 0 else 99
    except Exception:
        return 99


def offer_duration_minutes_safe(offer: Dict[str, Any]) -> int:
    def parse_iso_dur(s: str) -> int:
        s = str(s or "").strip().upper()
        if not s.startswith("PT"):
            return 0
        s = s[2:]
        h = 0
        m = 0
        num = ""
        for ch in s:
            if ch.isdigit():
                num += ch
            elif ch == "H":
                h = int(num or "0"); num = ""
            elif ch == "M":
                m = int(num or "0"); num = ""
            else:
                num = ""
        return h * 60 + m

    try:
        slices = offer.get("slices") or []
        total = 0
        found = False
        for sl in slices:
            dur = (sl or {}).get("duration")
            if dur:
                found = True
                total += parse_iso_dur(dur)
        return total if found and total > 0 else 999999
    except Exception:
        return 999999


# ==================== ENRICH ====================

def enrich_deal(deal: Dict[str, Any], themes_dict: Dict[str, List[Dict[str, Any]]], signals: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    dest = _clean_iata(deal.get("destination_iata", ""))
    theme = str(deal.get("deal_theme") or deal.get("theme") or "").strip()

    if theme and theme in themes_dict:
        for d in themes_dict[theme]:
            if _clean_iata(d.get("destination_iata")) == dest:
                if d.get("destination_city"):
                    deal["destination_city"] = d["destination_city"]
                if d.get("destination_country"):
                    deal["destination_country"] = d["destination_country"]
                break

    if dest and dest in signals:
        s = signals[dest]
        if not deal.get("destination_city") and s.get("destination_city"):
            deal["destination_city"] = s["destination_city"]
        if not deal.get("destination_country") and s.get("destination_country"):
            deal["destination_country"] = s["destination_country"]

    return deal


# ==================== WRITE RAW_DEALS ====================

def append_rows_header_mapped(ws, deals: List[Dict[str, Any]]) -> int:
    if not deals:
        return 0
    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("RAW_DEALS header row is empty")

    rows: List[List[Any]] = []
    for d in deals:
        row: List[Any] = []
        for h in headers:
            key = str(h).strip()
            row.append(d.get(key, ""))
        rows.append(row)

    ws.append_rows(rows, value_input_option="RAW")
    return len(rows)


# ==================== DATE PICKING ====================

def clamp_window(days_min: int, days_max: int) -> Tuple[int, int]:
    mn = max(int(days_min), int(INVENTORY_MIN_DAYS))
    mx = min(int(days_max), int(INVENTORY_MAX_DAYS))
    if mx < mn:
        return int(days_min), int(days_max)
    return mn, mx


def pick_dates(seed: str, days_min: int, days_max: int, trip_len: int) -> Tuple[dt.date, dt.date]:
    hsh = int(hashlib.md5(seed.encode("utf-8")).hexdigest()[:8], 16)
    span = max(1, (days_max - days_min + 1))
    dep_offset = days_min + (hsh % span)
    dep_date = dt.datetime.utcnow().date() + dt.timedelta(days=dep_offset)
    ret_date = dep_date + dt.timedelta(days=int(trip_len))
    return dep_date, ret_date


def jitter_offsets(max_jitter: int) -> List[int]:
    j = max(0, int(max_jitter))
    return list(range(-j, j + 1))


# ==================== MAIN ====================

def main() -> int:
    log("=" * 80)
    log("TRAVELTXTTER PIPELINE WORKER (FEEDER) START")
    log("=" * 80)

    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")

    theme_today = theme_of_day_utc()
    log(f"🎯 Theme of the day (UTC): {theme_today}")

    sparse_theme_override = theme_today in SPARSE_THEMES
    open_origins_effective = bool(FEEDER_OPEN_ORIGINS or sparse_theme_override)

    # For destination-first, "routes_per_run" means "destinations_per_run".
    destinations_per_run = max(1, int(DUFFEL_ROUTES_PER_RUN))
    destinations_per_run = min(destinations_per_run, max(1, DUFFEL_MAX_SEARCHES_PER_RUN))

    log(f"DESTINATION_FIRST: destinations_per_run={destinations_per_run} | date_jitter=±{DATE_JITTER_DAYS} days")
    log(f"ORIGIN_POLICY: FEEDER_OPEN_ORIGINS={FEEDER_OPEN_ORIGINS} | sparse_override={sparse_theme_override} | effective_open={open_origins_effective}")
    log(f"CAPS: MAX_INSERTS={DUFFEL_MAX_INSERTS} | PER_ORIGIN={DUFFEL_MAX_INSERTS_PER_ORIGIN} | "
        f"MAX_SEARCHES={DUFFEL_MAX_SEARCHES_PER_RUN}")
    log(f"PRICE_GATE: enabled={PRICE_GATE_ENABLED} fallback={PRICE_GATE_FALLBACK_BEHAVIOR} | mult={PRICE_GATE_MULTIPLIER} | mincap={PRICE_GATE_MIN_CAP_GBP}")
    log(f"HYGIENE: enabled={HYGIENE_ENABLED} | conn_short={OFFER_MAX_CONNECTIONS_SHORTHAUL} conn_long={OFFER_MAX_CONNECTIONS_LONGHAUL} | "
        f"dur_short={OFFER_MAX_DURATION_MINUTES_SHORTHAUL} dur_long={OFFER_MAX_DURATION_MINUTES_LONGHAUL} | "
        f"band_short={QUALITY_PRICE_BAND_SHORTHAUL} band_long={QUALITY_PRICE_BAND_LONGHAUL}")
    log(f"INVENTORY_WINDOW_DAYS={INVENTORY_MIN_DAYS}-{INVENTORY_MAX_DAYS} | ZERO_OFFER_RETRY_ENABLED={ZERO_OFFER_RETRY_ENABLED} retry_window_max={ZERO_OFFER_RETRY_MAX_DAYS}")
    log(f"RESPECT_CONFIG_ORIGIN={RESPECT_CONFIG_ORIGIN} (if true: origins constrained by CONFIG per destination)")

    explore_run = should_do_explore_this_run(theme_today)
    theme_quota = destinations_per_run if not explore_run else max(0, destinations_per_run - 1)
    explore_quota = 0 if not explore_run else 1
    log(f"🧠 Strategy: 90/10 | explore_run={explore_run} | theme_quota={theme_quota} | explore_quota={explore_quota} | MOD={FEEDER_EXPLORE_RUN_MOD}")

    gc = gs_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_raw = sh.worksheet(RAW_DEALS_TAB)
    raw_headers = ws_raw.row_values(1)
    raw_header_set = {str(h).strip() for h in raw_headers if h}

    config_rows = load_config_rows(sh)
    themes_dict = load_themes_dict(sh)
    signals = load_signals(sh)
    allowed_pairs, origin_city_map = load_route_capability_map(sh)

    benchmarks: List[Dict[str, Any]] = []
    if PRICE_GATE_ENABLED:
        try:
            benchmarks = load_zone_theme_benchmarks(sh)
        except Exception as e:
            log(f"⚠️ PRICE_GATE: failed to load '{BENCHMARKS_TAB}': {e} | fallback={PRICE_GATE_FALLBACK_BEHAVIOR}")
            benchmarks = []

    # -------------------- DESTINATION SELECTION --------------------
    theme_rows = [r for r in config_rows if str(r.get("theme") or "").strip() == theme_today]
    explore_rows = [r for r in config_rows if str(r.get("theme") or "").strip() != theme_today]

    def sort_key_cfg(r: Dict[str, Any]) -> Tuple[float, float, str]:
        pr = float(r.get("priority") or 0)
        sw = float(r.get("search_weight") or 0)
        d = _clean_iata(r.get("destination_iata"))
        return (pr, sw, d)

    # Group rows by destination
    def group_by_destination(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for r in rows:
            d = _clean_iata(r.get("destination_iata"))
            if not d:
                continue
            grouped.setdefault(d, []).append(r)
        # sort rows within each destination deterministically
        for d in list(grouped.keys()):
            grouped[d] = sorted(grouped[d], key=sort_key_cfg, reverse=True)
        return grouped

    theme_by_dest = group_by_destination(theme_rows)
    explore_by_dest = group_by_destination(explore_rows)

    # pick top destinations (theme first)
    theme_dests = sorted(theme_by_dest.keys(), key=lambda d: sort_key_cfg(theme_by_dest[d][0]), reverse=True)
    selected_dests: List[Tuple[str, str]] = []  # (dest_iata, mode)
    for d in theme_dests:
        if len(selected_dests) >= theme_quota:
            break
        selected_dests.append((d, "theme"))

    # deterministic explore: rotate explore destinations
    if explore_quota > 0 and explore_by_dest:
        explore_dests = list(explore_by_dest.keys())
        today = dt.datetime.utcnow().date().isoformat()
        seed = f"{FEEDER_EXPLORE_SALT}|{today}|{RUN_SLOT}|{theme_today}|EXPLORE"
        offset = _stable_mod(seed, max(1, len(explore_dests)))
        explore_dests = explore_dests[offset:] + explore_dests[:offset]
        for d in explore_dests:
            if len(selected_dests) >= (theme_quota + explore_quota):
                break
            selected_dests.append((d, "explore"))

    if not selected_dests:
        log("⚠️ No eligible destinations in CONFIG after filtering.")
        return 0

    log(f"🧭 Selected destinations ({len(selected_dests)}): {[d for d,_ in selected_dests]}")

    # -------------------- HELPERS --------------------
    def is_longhaul_theme(deal_theme: str) -> bool:
        t = str(deal_theme or "").strip()
        return t in {"long_haul", "adventure", "luxury_value"}

    def hygiene_limits_for_theme(deal_theme: str) -> Tuple[int, int, float]:
        if is_longhaul_theme(deal_theme):
            return (
                max(0, int(OFFER_MAX_CONNECTIONS_LONGHAUL)),
                max(0, int(OFFER_MAX_DURATION_MINUTES_LONGHAUL)),
                float(QUALITY_PRICE_BAND_LONGHAUL),
            )
        return (
            max(0, int(OFFER_MAX_CONNECTIONS_SHORTHAUL)),
            max(0, int(OFFER_MAX_DURATION_MINUTES_SHORTHAUL)),
            float(QUALITY_PRICE_BAND_SHORTHAUL),
        )

    def route_allowed(origin: str, dest: str) -> bool:
        if allowed_pairs and (origin, dest) not in allowed_pairs:
            return False
        return True

    def cap_for(origin: str, dest: str, deal_theme: str) -> Optional[float]:
        if not PRICE_GATE_ENABLED:
            return None
        if not benchmarks:
            return None
        return compute_ingest_cap_gbp(benchmarks, deal_theme, origin, dest)

    def build_origin_pool_for_destination(dest: str, cfg_rows_for_dest: List[Dict[str, Any]], deal_theme: str) -> List[str]:
        """
        Destination-first origin pool:
        - If RESPECT_CONFIG_ORIGIN and NOT open origins: use origins present in CONFIG rows for this destination.
        - Else: use ORIGINS_<THEME> allowlist (or fallback pool).
        Always dedupe, always capability-filter later.
        """
        if RESPECT_CONFIG_ORIGIN and not open_origins_effective:
            cfg_origins = [_clean_iata(r.get("origin_iata")) for r in cfg_rows_for_dest]
            cfg_origins = [o for o in cfg_origins if o]
            pool = _dedupe_keep_order(cfg_origins)
            return pool

        # use theme allowlist / fallback pools
        explicit = _theme_origins_from_env(deal_theme)
        if explicit:
            return explicit[:]
        return origin_plan_for_theme(deal_theme, 25)  # safe upper bound; we still hard-cap searches

    def cfg_for_destination(dest: str, mode: str) -> Dict[str, Any]:
        # pick the best cfg row (sorted already)
        if mode == "theme":
            rows = theme_by_dest.get(dest) or []
        else:
            rows = explore_by_dest.get(dest) or []
        if rows:
            return rows[0]
        # fallback: search all rows
        for r in config_rows:
            if _clean_iata(r.get("destination_iata")) == dest:
                return r
        return {"theme": theme_today, "destination_iata": dest}

    # -------------------- SEARCH + WINNER SELECTION --------------------
    searches_done = 0
    inserted_total = 0
    inserted_by_origin: Dict[str, int] = {}
    winners: List[Dict[str, Any]] = []

    def best_key(price: float, conn: int, dur: int) -> Tuple[float, int, int]:
        return (price, conn, dur)

    def search_best_offer(
        origin: str,
        dest: str,
        dep_date: dt.date,
        ret_date: dt.date,
        cfg: Dict[str, Any],
        deal_theme: str,
        cap_gbp: Optional[float],
    ) -> Tuple[int, Optional[Dict[str, Any]]]:
        nonlocal searches_done

        max_conn_cfg = int(cfg.get("max_connections") or 0)
        payload = {
            "data": {
                "slices": [
                    {"origin": origin, "destination": dest, "departure_date": dep_date.isoformat()},
                    {"origin": dest, "destination": origin, "departure_date": ret_date.isoformat()},
                ],
                "passengers": [{"type": "adult"}],
                "cabin_class": (cfg.get("cabin_class") or "economy"),
                "max_connections": max_conn_cfg,
                "return_offers": True,
            }
        }

        log(f"Duffel: {origin}->{dest} {dep_date.isoformat()}/{ret_date.isoformat()}")
        resp = duffel_search_offer_request(payload)
        searches_done += 1

        offers = (resp.get("data") or {}).get("offers") or []
        if not offers:
            return 0, None

        max_conn_hard, max_dur_hard, band = hygiene_limits_for_theme(deal_theme)

        # Evaluate all offers, keep the best that passes gates.
        best_offer: Optional[Dict[str, Any]] = None
        best_tuple: Optional[Tuple[float, int, int]] = None

        # band cap only makes sense if cap exists
        band_cap = (float(cap_gbp) * float(band)) if (HYGIENE_ENABLED and PRICE_GATE_ENABLED and cap_gbp is not None and 0 < band < 1.0) else None

        for off in offers:
            price = offer_price_gbp(off)
            if price <= 0:
                continue

            # price gate
            if PRICE_GATE_ENABLED and cap_gbp is not None and price > cap_gbp:
                continue

            # hygiene gate
            conn = offer_connections_safe(off)
            dur = offer_duration_minutes_safe(off)

            if HYGIENE_ENABLED:
                if conn > max_conn_hard:
                    continue
                if dur > max_dur_hard:
                    continue
                if band_cap is not None and price > band_cap:
                    continue

            tup = best_key(price, conn, dur)
            if best_tuple is None or tup < best_tuple:
                best_tuple = tup
                best_offer = off

        return len(offers), best_offer

    def build_deal_row(
        origin: str,
        dest: str,
        dep_date: dt.date,
        ret_date: dt.date,
        offer: Dict[str, Any],
        deal_theme: str,
    ) -> Dict[str, Any]:
        price = offer_price_gbp(offer)
        off_id = str(offer.get("id") or "")
        deal_id_seed = f"{origin}->{dest}|{dep_date.isoformat()}|{ret_date.isoformat()}|{price:.2f}|{off_id}"
        deal_id = hashlib.sha256(deal_id_seed.encode("utf-8")).hexdigest()[:24]

        now_iso = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

        row: Dict[str, Any] = {
            "status": "NEW",
            "deal_id": deal_id,
            "price_gbp": int(math.ceil(price)),
            "origin_iata": origin,
            "origin_city": resolve_origin_city(origin, origin_city_map),
            "destination_iata": dest,
            "outbound_date": dep_date.strftime("%Y-%m-%d"),
            "return_date": ret_date.strftime("%Y-%m-%d"),
            "deal_theme": deal_theme,
            "theme": deal_theme,
            "destination_city": "",
            "destination_country": "",
        }

        # only fill timestamp columns if they exist
        if "ingested_at_utc" in raw_header_set:
            row["ingested_at_utc"] = now_iso
        if "created_utc" in raw_header_set:
            row["created_utc"] = now_iso
        if "created_at" in raw_header_set:
            row["created_at"] = now_iso
        if "timestamp" in raw_header_set:
            row["timestamp"] = now_iso

        return enrich_deal(row, themes_dict, signals)

    # -------------------- DESTINATION LOOP --------------------
    jitters = jitter_offsets(DATE_JITTER_DAYS)

    for dest, mode in selected_dests:
        if inserted_total >= DUFFEL_MAX_INSERTS:
            break
        if searches_done >= DUFFEL_MAX_SEARCHES_PER_RUN:
            break

        cfg = cfg_for_destination(dest, mode)
        deal_theme = str(cfg.get("theme") or theme_today).strip() or theme_today

        # trip + window (from CONFIG if present)
        days_min = int(cfg.get("days_ahead_min") or DEFAULT_DAYS_AHEAD_MIN)
        days_max = int(cfg.get("days_ahead_max") or DEFAULT_DAYS_AHEAD_MAX)
        trip_len = int(cfg.get("trip_length_days") or DEFAULT_TRIP_LENGTH_DAYS)
        days_min, days_max = clamp_window(days_min, days_max)

        # base dates (deterministic)
        base_seed = f"{dt.datetime.utcnow().date().isoformat()}|{dest}|{deal_theme}|{trip_len}|{RUN_SLOT}"
        base_dep, base_ret = pick_dates(base_seed, days_min, days_max, trip_len)

        # origin pool (destination-first)
        cfg_rows_for_dest = (theme_by_dest.get(dest) if mode == "theme" else explore_by_dest.get(dest)) or []
        origin_pool = build_origin_pool_for_destination(dest, cfg_rows_for_dest, deal_theme)
        origin_pool = _dedupe_keep_order(origin_pool)

        if not origin_pool:
            log(f"⚠️ DESTINATION_SKIP: {dest} no origins in pool (mode={mode})")
            continue

        # dynamic origin trimming to stay within remaining search budget
        remaining_searches = max(0, DUFFEL_MAX_SEARCHES_PER_RUN - searches_done)
        if remaining_searches <= 0:
            break

        # Each origin consumes up to len(jitters) searches (worst case).
        max_origins_now = max(1, remaining_searches // max(1, len(jitters)))
        if len(origin_pool) > max_origins_now:
            origin_pool = origin_pool[:max_origins_now]

        log(f"DESTINATION_FIRST: {dest} mode={mode} theme={deal_theme} base={base_dep.isoformat()}/{base_ret.isoformat()} "
            f"window={days_min}-{days_max} trip_len={trip_len} origins={origin_pool} jitters={jitters}")

        # find best across origin×jitter
        best: Optional[Tuple[Tuple[float, int, int], str, dt.date, dt.date, Dict[str, Any]]] = None
        zero_offers_seen = False

        for origin in origin_pool:
            if inserted_total >= DUFFEL_MAX_INSERTS:
                break
            if searches_done >= DUFFEL_MAX_SEARCHES_PER_RUN:
                break

            # origin cap
            if inserted_by_origin.get(origin, 0) >= DUFFEL_MAX_INSERTS_PER_ORIGIN:
                continue

            # capability pre-filter: skip origin entirely if no route
            if not route_allowed(origin, dest):
                continue

            # benchmark cap: if missing and BLOCK, do not search this origin->dest for this theme
            cap = cap_for(origin, dest, deal_theme)
            if PRICE_GATE_ENABLED:
                if not benchmarks and PRICE_GATE_FALLBACK_BEHAVIOR == "BLOCK":
                    log(f"⛔ BENCHMARKS_MISSING: cannot precheck {origin}->{dest} fallback=BLOCK => skip")
                    continue
                if benchmarks and cap is None and PRICE_GATE_FALLBACK_BEHAVIOR == "BLOCK":
                    log(f"⛔ BENCHMARK_MISS: {origin}->{dest} theme={deal_theme} fallback=BLOCK => skip")
                    continue

            for j in jitters:
                if searches_done >= DUFFEL_MAX_SEARCHES_PER_RUN:
                    break

                dep = base_dep + dt.timedelta(days=j)
                ret = dep + dt.timedelta(days=int(trip_len))

                # guard: keep dep within configured window bounds (relative to today)
                days_out = (dep - dt.datetime.utcnow().date()).days
                if days_out < days_min or days_out > days_max:
                    continue

                try:
                    offers_returned, best_offer = search_best_offer(origin, dest, dep, ret, cfg, deal_theme, cap)
                except Exception as e:
                    log(f"❌ Duffel error: {e}")
                    continue

                if offers_returned == 0:
                    zero_offers_seen = True
                    continue

                if best_offer is None:
                    continue

                price = offer_price_gbp(best_offer)
                conn = offer_connections_safe(best_offer)
                dur = offer_duration_minutes_safe(best_offer)
                tup = best_key(price, conn, dur)

                if best is None or tup < best[0]:
                    best = (tup, origin, dep, ret, best_offer)

            if FEEDER_SLEEP_SECONDS > 0:
                time.sleep(FEEDER_SLEEP_SECONDS)

        # Optional zero-offer retry (broader window) if we saw zero offers and still have budget.
        if best is None and ZERO_OFFER_RETRY_ENABLED and zero_offers_seen and searches_done < DUFFEL_MAX_SEARCHES_PER_RUN:
            retry_max = min(ZERO_OFFER_RETRY_MAX_DAYS, INVENTORY_MAX_DAYS)
            retry_min = INVENTORY_MIN_DAYS
            if retry_max >= retry_min:
                retry_seed = base_seed + "|retry"
                dep2, ret2 = pick_dates(retry_seed, retry_min, retry_max, trip_len)

                # try a single best-effort retry on the first allowed origin (trimmed to budget)
                origin = origin_pool[0]
                if route_allowed(origin, dest):
                    cap = cap_for(origin, dest, deal_theme)
                    if not (PRICE_GATE_ENABLED and benchmarks and cap is None and PRICE_GATE_FALLBACK_BEHAVIOR == "BLOCK"):
                        log(f"🔄 ZERO_OFFER_RETRY: {origin}->{dest} window={retry_min}-{retry_max} => {dep2.isoformat()}/{ret2.isoformat()}")
                        try:
                            _, best_offer = search_best_offer(origin, dest, dep2, ret2, cfg, deal_theme, cap)
                            if best_offer is not None:
                                price = offer_price_gbp(best_offer)
                                conn = offer_connections_safe(best_offer)
                                dur = offer_duration_minutes_safe(best_offer)
                                best = (best_key(price, conn, dur), origin, dep2, ret2, best_offer)
                        except Exception as e:
                            log(f"❌ Duffel retry error: {e}")

        if best is None:
            log(f"⚠️ NO_WINNER: {dest} (searched within caps; none passed gates)")
            continue

        _, win_origin, win_dep, win_ret, win_offer = best

        # enforce origin insert caps (again)
        if inserted_by_origin.get(win_origin, 0) >= DUFFEL_MAX_INSERTS_PER_ORIGIN:
            log(f"⏭️ ORIGIN_CAP blocks winner: {win_origin} for {dest}")
            continue

        deal_row = build_deal_row(win_origin, dest, win_dep, win_ret, win_offer, deal_theme)
        winners.append(deal_row)
        inserted_total += 1
        inserted_by_origin[win_origin] = inserted_by_origin.get(win_origin, 0) + 1

        log(f"🏁 WINNER: {dest} from {win_origin} {win_dep.isoformat()}/{win_ret.isoformat()} £{deal_row.get('price_gbp')} "
            f"(inserted_total={inserted_total}/{DUFFEL_MAX_INSERTS} searches={searches_done}/{DUFFEL_MAX_SEARCHES_PER_RUN})")

        if FEEDER_SLEEP_SECONDS > 0:
            time.sleep(FEEDER_SLEEP_SECONDS)

    log(f"✓ Searches completed: {searches_done}/{DUFFEL_MAX_SEARCHES_PER_RUN}")
    log(f"✓ Winners collected: {len(winners)} (cap {DUFFEL_MAX_INSERTS})")

    if not winners:
        log("⚠️ No winners passed gates for this run.")
        return 0

    inserted = append_rows_header_mapped(ws_raw, winners)
    log(f"✅ Inserted {inserted} rows into {RAW_DEALS_TAB}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
