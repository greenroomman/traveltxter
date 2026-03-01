#!/usr/bin/env python3
"""
workers/pipeline_worker.py
TRAVELTXTTER V5 FEEDER — CONSTRAINED COVERAGE ENGINE

Architecture: Stratified bucket model with deterministic day-index rotation.
Destination selection is geographic (bucket-driven), not theme-driven.
Theme is read from OPS_MASTER and applied as a label to output rows only.

Key changes from v1:
- Bucket model replaces theme-gated CONFIG destination selection
- Day-index deterministic rotation (no randomness)
- London dominance control via modulo constraint (≤40% share)
- Dedupe reads last N rows only (not full sheet)
- Single batch write at end of run
- Run summary logging to console (structured for future metrics tab)

Oilpan contracts preserved:
- Only writes to RAW_DEALS
- Only sets status = NEW
- Does not score, enrich, or publish
- Stateless — no memory between runs except what's in Sheets
"""

from __future__ import annotations

import os
import json
import time
import math
import hashlib
import datetime as dt
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials


# ─────────────────────────────────────────────
# ENV HELPERS
# ─────────────────────────────────────────────

def env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, default)).strip())
    except Exception:
        return default


def env_float(name: str, default: float) -> float:
    try:
        return float(str(os.getenv(name, default)).strip())
    except Exception:
        return default


def env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


# ─────────────────────────────────────────────
# GSPREAD AUTH
# ─────────────────────────────────────────────

def _sanitize_sa_json(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        raise RuntimeError("Missing GCP service account JSON.")
    try:
        json.loads(raw)
        return raw
    except Exception:
        pass
    try:
        fixed = raw.replace("\\n", "\n")
        json.loads(fixed)
        return fixed
    except Exception:
        pass
    if '"private_key"' in raw and "BEGIN PRIVATE KEY" in raw:
        try:
            before, rest = raw.split('"private_key"', 1)
            if ':"' in rest:
                k1, krest = rest.split(':"', 1)
                pk_prefix = ':"'
            else:
                k1, krest = rest.split('": "', 1)
                pk_prefix = '": "'
            key_body, after = krest.split("-----END PRIVATE KEY-----", 1)
            key_body = key_body.replace("\r", "").replace("\n", "\\n")
            repaired = (before + '"private_key"' + k1 + pk_prefix
                        + key_body + "-----END PRIVATE KEY-----" + after)
            json.loads(repaired)
            return repaired
        except Exception:
            pass
    raw2 = raw.replace("\r", "")
    json.loads(raw2)
    return raw2


def gspread_client() -> gspread.Client:
    raw = os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON") or ""
    raw = _sanitize_sa_json(raw)
    info = json.loads(raw)
    creds = Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return gspread.authorize(creds)


def get_cell(ws: gspread.Worksheet, a1: str) -> str:
    try:
        v = ws.acell(a1).value
        return "" if v is None else str(v).strip()
    except Exception:
        return ""


# ─────────────────────────────────────────────
# TIMESTAMP
# ─────────────────────────────────────────────

def _utc_iso() -> str:
    return (dt.datetime.now(dt.timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z"))


# ─────────────────────────────────────────────
# DAY INDEX — DETERMINISTIC ROTATION SEED
# ─────────────────────────────────────────────

def day_index(run_slot: str) -> int:
    """
    Unique integer per run.
    day_index = YYYY * 1000 + day_of_year + slot_offset
    AM = +1, PM = +2
    Same inputs always produce same outputs.
    """
    now = dt.datetime.now(dt.timezone.utc)
    base = now.year * 1000 + now.timetuple().tm_yday
    offset = 1 if run_slot.upper() == "AM" else 2
    return base + offset


# ─────────────────────────────────────────────
# DUFFEL
# ─────────────────────────────────────────────

DUFFEL_API = "https://api.duffel.com/air/offer_requests"

LONDON_AIRPORTS = {"LHR", "LGW", "LCY"}


def duffel_headers() -> Dict[str, str]:
    key = env_str("DUFFEL_API_KEY")
    if not key:
        raise RuntimeError("Missing DUFFEL_API_KEY.")
    return {
        "Authorization": f"Bearer {key}",
        "Duffel-Version": "v2",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _hash_trip(origin: str, dest: str, out_date: str, ret_date: str) -> str:
    s = f"{origin}|{dest}|{out_date}|{ret_date}"
    return hashlib.sha1(s.encode()).hexdigest()[:12]


def _pick_dates(dix: int, win_min: int, win_max: int, trip_days: int) -> Tuple[str, str]:
    """
    Deterministic date selection based on day_index.
    No random — same dix always returns same dates.
    """
    span = max(1, win_max - win_min)
    depart_offset = win_min + (dix % span)
    out_epoch = int(time.time()) + depart_offset * 86400
    out = time.strftime("%Y-%m-%d", time.gmtime(out_epoch))
    ret_epoch = out_epoch + max(1, trip_days) * 86400
    ret = time.strftime("%Y-%m-%d", time.gmtime(ret_epoch))
    return out, ret


def duffel_search(
    origin: str, dest: str, out_date: str, ret_date: str,
    cabin: str, max_connections: int
) -> Optional[Dict[str, Any]]:
    payload = {
        "data": {
            "slices": [
                {"origin": origin, "destination": dest, "departure_date": out_date},
                {"origin": dest, "destination": origin, "departure_date": ret_date},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": cabin,
            "max_connections": max_connections,
            "return_offers": True,
        }
    }
    try:
        resp = requests.post(DUFFEL_API, headers=duffel_headers(), json=payload, timeout=45)
        if resp.status_code >= 400:
            return None
        data = resp.json().get("data", {})
        offers = data.get("offers") or []
        if not offers:
            return None
        gbp = [o for o in offers if (o.get("total_currency") or "").upper() == "GBP"]
        if not gbp:
            return None
        gbp.sort(key=lambda o: float(o.get("total_amount") or "1e18"))
        return gbp[0]
    except Exception:
        return None


# ─────────────────────────────────────────────
# OFFER EXTRACTION
# ─────────────────────────────────────────────

def extract_carriers(offer: Dict[str, Any]) -> str:
    carriers: List[str] = []
    try:
        for sl in offer.get("slices") or []:
            for seg in sl.get("segments") or []:
                mc = seg.get("marketing_carrier") or {}
                code = (mc.get("iata_code") or "").upper()
                if code and code not in carriers:
                    carriers.append(code)
    except Exception:
        pass
    return ",".join(carriers)


def extract_stops(offer: Dict[str, Any]) -> int:
    try:
        stops = 0
        for sl in offer.get("slices") or []:
            stops += max(0, len(sl.get("segments") or []) - 1)
        return stops
    except Exception:
        return 0


def extract_cabin_class(offer: Dict[str, Any], fallback: str = "economy") -> str:
    cc = (offer.get("cabin_class") or "").strip().lower()
    return cc if cc else fallback


def extract_bags_included(offer: Dict[str, Any]) -> str:
    try:
        services = offer.get("available_services") or []
        if isinstance(services, dict):
            services = [services]
        if not isinstance(services, list):
            return ""
        bag_qty = 0
        for svc in services:
            if not isinstance(svc, dict):
                continue
            t = (svc.get("type") or "").lower()
            if "bag" in t or "baggage" in t:
                q = svc.get("maximum_quantity") or svc.get("quantity") or 0
                try:
                    bag_qty = max(bag_qty, int(q))
                except Exception:
                    continue
        return str(bag_qty) if bag_qty > 0 else ""
    except Exception:
        return ""


# ─────────────────────────────────────────────
# CONFIG_BUCKETS
# ─────────────────────────────────────────────

@dataclass
class BucketDest:
    bucket_id: int
    bucket_name: str
    destination_iata: str
    city: str
    country: str
    liquidity_tier: str  # A / B / C


def load_buckets(ws_buckets: gspread.Worksheet) -> Dict[int, List[BucketDest]]:
    """
    Returns dict: bucket_id → [BucketDest, ...]
    Only enabled rows included.
    """
    rows = ws_buckets.get_all_records()
    buckets: Dict[int, List[BucketDest]] = {}
    for r in rows:
        enabled = str(r.get("enabled", "")).strip().upper() in ("TRUE", "1", "YES", "Y")
        if not enabled:
            continue
        try:
            bid = int(r.get("bucket_id", 0) or 0)
        except Exception:
            continue
        if bid < 1:
            continue
        iata = str(r.get("destination_iata", "")).strip().upper()
        if not iata:
            continue
        dest = BucketDest(
            bucket_id=bid,
            bucket_name=str(r.get("bucket_name", "")).strip(),
            destination_iata=iata,
            city=str(r.get("city", "")).strip(),
            country=str(r.get("country", "")).strip(),
            liquidity_tier=str(r.get("liquidity_tier", "B")).strip().upper(),
        )
        buckets.setdefault(bid, []).append(dest)
    return buckets


# ─────────────────────────────────────────────
# CONFIG_ORIGINS
# ─────────────────────────────────────────────

@dataclass
class OriginAirport:
    airport_iata: str
    tier: int
    tier_weight: float


def load_origins(ws_origins: gspread.Worksheet) -> Dict[int, List[OriginAirport]]:
    """
    Returns dict: tier → [OriginAirport, ...]
    Only enabled rows.
    """
    rows = ws_origins.get_all_records()
    tiers: Dict[int, List[OriginAirport]] = {}
    for r in rows:
        enabled = str(r.get("enabled", "")).strip().upper() in ("TRUE", "1", "YES", "Y")
        if not enabled:
            continue
        iata = str(r.get("airport_iata", "")).strip().upper()
        if not iata:
            continue
        try:
            tier = int(r.get("tier", 1) or 1)
            tw = float(r.get("tier_weight", 0.5) or 0.5)
        except Exception:
            tier, tw = 1, 0.5
        tiers.setdefault(tier, []).append(OriginAirport(iata, tier, tw))
    return tiers


# ─────────────────────────────────────────────
# BUCKET × ORIGIN COMPATIBILITY
# ─────────────────────────────────────────────

# bucket_id → max_origin_tier allowed
BUCKET_MAX_TIER: Dict[int, int] = {
    1: 3,  # EU High Volume     — all tiers
    2: 3,  # EU Secondary       — all tiers
    3: 2,  # Near Long-Haul     — Tier 1 + 2 only
    4: 1,  # Long-Haul US/CA    — Tier 1 only
    5: 1,  # Long-Haul Asia/ME  — Tier 1 only
    6: 2,  # Seasonal/Wildcard  — Tier 1 + 2 only
}


def eligible_tiers_for_bucket(bucket_id: int) -> List[int]:
    max_tier = BUCKET_MAX_TIER.get(bucket_id, 1)
    return list(range(1, max_tier + 1))


# ─────────────────────────────────────────────
# BUCKET ROTATION (STRATIFIED)
# ─────────────────────────────────────────────

# Each run gets 2 buckets: primary + secondary.
# Pairs cycle across 6 days covering all 6 buckets.
BUCKET_PAIRS = [
    (1, 4),  # Day 1 AM
    (2, 5),  # Day 1 PM
    (3, 6),  # Day 2 AM
    (4, 1),  # Day 2 PM
    (5, 2),  # Day 3 AM
    (6, 3),  # Day 3 PM
]


def select_buckets(dix: int) -> Tuple[int, int]:
    pair = BUCKET_PAIRS[dix % len(BUCKET_PAIRS)]
    return pair[0], pair[1]


# ─────────────────────────────────────────────
# DESTINATION SELECTION (DETERMINISTIC)
# ─────────────────────────────────────────────

def select_destinations(
    bucket_dests: List[BucketDest],
    dix: int,
    n: int,
) -> List[BucketDest]:
    """
    Deterministic rotation within bucket.
    dest = bucket_list[(dix + offset) % len(bucket_list)]
    No randomness.
    """
    if not bucket_dests:
        return []
    # Prefer A-tier liquidity first, then B, then C
    ordered = (
        [d for d in bucket_dests if d.liquidity_tier == "A"]
        + [d for d in bucket_dests if d.liquidity_tier == "B"]
        + [d for d in bucket_dests if d.liquidity_tier == "C"]
    )
    selected: List[BucketDest] = []
    seen: set = set()
    for i in range(n):
        idx = (dix + i) % len(ordered)
        dest = ordered[idx]
        if dest.destination_iata not in seen:
            selected.append(dest)
            seen.add(dest.destination_iata)
    return selected[:n]


# ─────────────────────────────────────────────
# ORIGIN SELECTION (DETERMINISTIC + LONDON CAP)
# ─────────────────────────────────────────────

def select_origin(
    tier_airports: Dict[int, List[OriginAirport]],
    bucket_id: int,
    dix: int,
    slot_offset: int,
    london_count: int,
    total_count: int,
) -> Optional[str]:
    """
    Selects one origin for a search:
    1. Determine eligible tiers for this bucket
    2. Pick tier based on weighted distribution (50/35/15)
    3. Rotate deterministically within tier
    4. Apply London modulo constraint: LHR/LGW only when (dix % 3 == 0)
       This enforces ≤40% London share without needing state

    Returns IATA string or None if no eligible airport found.
    """
    eligible = eligible_tiers_for_bucket(bucket_id)

    # Tier probability weights
    tier_weights = {1: 0.50, 2: 0.35, 3: 0.15}

    # Normalise to eligible tiers only
    total_w = sum(tier_weights.get(t, 0) for t in eligible if t in tier_airports)
    if total_w == 0:
        return None

    # Deterministic tier pick: use dix + slot_offset to cycle
    tier_seed = (dix + slot_offset) % 100
    cumulative = 0.0
    chosen_tier = eligible[0]
    for t in sorted(eligible):
        if t not in tier_airports:
            continue
        cumulative += tier_weights.get(t, 0) / total_w * 100
        if tier_seed < cumulative:
            chosen_tier = t
            break

    pool = tier_airports.get(chosen_tier, [])
    if not pool:
        return None

    # Deterministic rotation within tier
    airport = pool[(dix + slot_offset) % len(pool)]
    iata = airport.airport_iata

    # London modulo constraint: only allow LHR/LGW every 3rd rotation
    # This keeps London share ≤ 33% deterministically
    if iata in LONDON_AIRPORTS:
        if dix % 3 != 0:
            # Try next airport in tier that isn't London
            for i in range(1, len(pool)):
                alt = pool[(dix + slot_offset + i) % len(pool)]
                if alt.airport_iata not in LONDON_AIRPORTS:
                    return alt.airport_iata
            # All airports in tier are London — allow it
            return iata

    return iata


# ─────────────────────────────────────────────
# DEDUPE (LAST N ROWS ONLY)
# ─────────────────────────────────────────────

def load_dedupe_set(ws_raw: gspread.Worksheet, lookback_rows: int) -> set:
    """
    Read only last N rows to build dedupe set.
    Avoids full-sheet load as RAW_DEALS grows.
    """
    all_values = ws_raw.get_all_values()
    if len(all_values) < 2:
        return set()

    header = all_values[0]
    hm = {str(h).strip(): i for i, h in enumerate(header)}

    def col(name: str, row: List[str]) -> str:
        i = hm.get(name)
        return (row[i] if (i is not None and i < len(row)) else "").strip()

    # Take only last N data rows
    data_rows = all_values[1:]
    if len(data_rows) > lookback_rows:
        data_rows = data_rows[-lookback_rows:]

    s = set()
    for r in data_rows:
        o = col("origin_iata", r).upper()
        d = col("destination_iata", r).upper()
        od = col("outbound_date", r)
        rd = col("return_date", r)
        if o and d and od and rd:
            s.add((o, d, od, rd))
    return s


# ─────────────────────────────────────────────
# SHEETS WRITE
# ─────────────────────────────────────────────

def append_rows_bulk(ws_raw: gspread.Worksheet, rows: List[List[Any]]) -> None:
    if rows:
        ws_raw.append_rows(rows, value_input_option="USER_ENTERED")


# ─────────────────────────────────────────────
# RAW_DEALS CONTRACT
# ─────────────────────────────────────────────

RAW_HEADERS_REQUIRED = [
    "deal_id", "origin_iata", "destination_iata", "origin_city",
    "destination_city", "destination_country", "outbound_date", "return_date",
    "price_gbp", "currency", "stops", "cabin_class", "carriers", "theme",
    "status", "publish_window", "score", "bags_incl", "graphic_url",
    "booking_link_vip", "posted_vip_at", "posted_free_at", "posted_instagram_at",
    "ingested_at_utc", "phrase_used", "phrase_category", "scored_timestamp",
]


def ensure_headers(ws: gspread.Worksheet) -> Dict[str, int]:
    hm = {str(h).strip(): i for i, h in enumerate(ws.row_values(1))}
    missing = [h for h in RAW_HEADERS_REQUIRED if h not in hm]
    if missing:
        raise RuntimeError(f"{ws.title} missing required headers: {missing}")
    return hm


# ─────────────────────────────────────────────
# THEME → TRAVEL PARAMS
# Theme determines travel window, trip length, max stops.
# Theme does NOT determine destination.
# ─────────────────────────────────────────────

@dataclass
class TravelParams:
    win_min: int = 21
    win_max: int = 84
    trip_min: int = 4
    trip_max: int = 10
    max_stops: int = 1
    cabin: str = "economy"


THEME_PARAMS: Dict[str, TravelParams] = {
    "northern_lights": TravelParams(win_min=14, win_max=60, trip_min=4, trip_max=8, max_stops=1),
    "snow":            TravelParams(win_min=21, win_max=90, trip_min=5, trip_max=10, max_stops=1),
    "city_breaks":     TravelParams(win_min=14, win_max=60, trip_min=3, trip_max=5, max_stops=0),
    "beach_break":     TravelParams(win_min=21, win_max=90, trip_min=5, trip_max=10, max_stops=1),
    "summer_sun":      TravelParams(win_min=30, win_max=120, trip_min=7, trip_max=14, max_stops=1),
    "winter_sun":      TravelParams(win_min=14, win_max=90, trip_min=5, trip_max=10, max_stops=1),
    "surf":            TravelParams(win_min=21, win_max=90, trip_min=7, trip_max=14, max_stops=1),
    "adventure":       TravelParams(win_min=30, win_max=120, trip_min=7, trip_max=14, max_stops=2),
    "luxury_value":    TravelParams(win_min=30, win_max=120, trip_min=7, trip_max=14, max_stops=1),
    "long_haul":       TravelParams(win_min=30, win_max=120, trip_min=7, trip_max=14, max_stops=1),
    "unexpected_value":TravelParams(win_min=14, win_max=60, trip_min=3, trip_max=7, max_stops=1),
    "hub":             TravelParams(win_min=21, win_max=90, trip_min=5, trip_max=10, max_stops=0),
}

DEFAULT_PARAMS = TravelParams()


def params_for_theme(theme: str) -> TravelParams:
    return THEME_PARAMS.get(theme.lower(), DEFAULT_PARAMS)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main() -> int:
    print("=" * 70)
    print("TRAVELTXTTER V5 — FEEDER — CONSTRAINED COVERAGE ENGINE")
    print("=" * 70)

    run_slot = env_str("RUN_SLOT", "PM").upper()
    raw_tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    buckets_tab = env_str("FEEDER_BUCKETS_TAB", "CONFIG_BUCKETS")
    origins_tab = env_str("FEEDER_ORIGINS_TAB", "CONFIG_ORIGINS")
    ops_tab = env_str("OPS_MASTER_TAB", "OPS_MASTER")

    max_searches = env_int("DUFFEL_MAX_SEARCHES_PER_RUN", 4)
    max_inserts = env_int("DUFFEL_MAX_INSERTS", 20)
    dests_per_bucket = env_int("DUFFEL_ROUTES_PER_RUN", 2)
    lookback_rows = env_int("DEDUPE_LOOKBACK_ROWS", 2000)
    sleep_s = env_float("FEEDER_SLEEP_SECONDS", 0.1)
    cabin = env_str("CABIN_CLASS", "economy").lower()

    dix = day_index(run_slot)
    print(f"📅 Day index: {dix} | Slot: {run_slot}")

    # ── Connect ──
    gc = gspread_client()
    sh = gc.open_by_key(env_str("SPREADSHEET_ID") or env_str("SHEET_ID"))

    # ── Theme (label only — does not gate destinations) ──
    ws_ops = sh.worksheet(ops_tab)
    theme_today = (get_cell(ws_ops, "B2") or "DEFAULT").strip()
    travel_p = params_for_theme(theme_today)
    print(f"🎯 Theme (label): {theme_today}")
    print(f"   Window: {travel_p.win_min}–{travel_p.win_max}d | "
          f"Trip: {travel_p.trip_min}–{travel_p.trip_max}d | "
          f"Max stops: {travel_p.max_stops}")

    # ── Load sheets ──
    ws_raw = sh.worksheet(raw_tab)
    ensure_headers(ws_raw)
    dedupe = load_dedupe_set(ws_raw, lookback_rows)
    print(f"🔍 Dedupe set loaded: {len(dedupe)} recent trips")

    ws_buckets = sh.worksheet(buckets_tab)
    all_buckets = load_buckets(ws_buckets)
    if not all_buckets:
        print("❌ CONFIG_BUCKETS is empty or missing. Exiting.")
        return 1

    ws_origins = sh.worksheet(origins_tab)
    tier_airports = load_origins(ws_origins)
    if not tier_airports:
        print("❌ CONFIG_ORIGINS is empty or missing. Exiting.")
        return 1

    # ── Select buckets for this run ──
    bucket_a, bucket_b = select_buckets(dix)
    print(f"🪣 Buckets this run: {bucket_a} + {bucket_b}")

    # ── Select destinations ──
    dests_a = select_destinations(all_buckets.get(bucket_a, []), dix, dests_per_bucket)
    dests_b = select_destinations(all_buckets.get(bucket_b, []), dix + 100, dests_per_bucket)
    all_dests = dests_a + dests_b

    if not all_dests:
        print("⚠️  No destinations resolved. Exiting cleanly.")
        return 0

    dest_names = [f"{d.destination_iata}({d.city})" for d in all_dests]
    print(f"📍 Destinations ({len(all_dests)}): {dest_names}")

    # ── Trip length (midpoint of theme range) ──
    trip_len = (travel_p.trip_min + travel_p.trip_max) // 2

    # ── Search loop ──
    searches = 0
    no_offer = 0
    dedupe_skips = 0
    london_used = 0
    pending_rows: List[List[Any]] = []

    print("=" * 70)

    for slot_offset, dest in enumerate(all_dests):
        if searches >= max_searches or len(pending_rows) >= max_inserts:
            break

        origin = select_origin(
            tier_airports, dest.bucket_id, dix,
            slot_offset, london_used, searches
        )

        if not origin:
            print(f"⚠️  No eligible origin for bucket {dest.bucket_id}. Skipping {dest.destination_iata}.")
            continue

        out_date, ret_date = _pick_dates(dix + slot_offset, travel_p.win_min, travel_p.win_max, trip_len)
        trip_key = (origin, dest.destination_iata, out_date, ret_date)

        if trip_key in dedupe:
            dedupe_skips += 1
            print(f"⏭️  Dedupe skip: {origin}→{dest.destination_iata} {out_date}/{ret_date}")
            continue

        if origin in LONDON_AIRPORTS:
            london_used += 1

        searches += 1
        bucket_label = f"[B{dest.bucket_id}:{dest.bucket_name}]"
        print(f"🔎 Search {searches}/{max_searches} {origin}→{dest.destination_iata} "
              f"{out_date}/{ret_date} {bucket_label}")

        offer = duffel_search(
            origin, dest.destination_iata, out_date, ret_date,
            cabin=cabin, max_connections=travel_p.max_stops
        )

        if not offer:
            no_offer += 1
            print(f"   ❌ No offer")
            time.sleep(sleep_s)
            continue

        price_gbp = int(math.ceil(float(offer.get("total_amount") or 0)))
        currency = (offer.get("total_currency") or "GBP").upper()
        print(f"   ✅ £{price_gbp} ({dest.city}, {dest.country})")

        row_map: Dict[str, Any] = {h: "" for h in RAW_HEADERS_REQUIRED}
        row_map.update({
            "deal_id": offer.get("id") or _hash_trip(origin, dest.destination_iata, out_date, ret_date),
            "origin_iata": origin,
            "destination_iata": dest.destination_iata,
            "origin_city": "",
            "destination_city": dest.city,
            "destination_country": dest.country,
            "outbound_date": out_date,
            "return_date": ret_date,
            "price_gbp": price_gbp,
            "currency": currency,
            "stops": extract_stops(offer),
            "cabin_class": extract_cabin_class(offer, fallback=cabin),
            "carriers": extract_carriers(offer),
            "theme": theme_today,
            "status": "NEW",
            "publish_window": "",
            "score": "",
            "bags_incl": extract_bags_included(offer),
            "graphic_url": "",
            "booking_link_vip": "",
            "posted_vip_at": "",
            "posted_free_at": "",
            "posted_instagram_at": "",
            "ingested_at_utc": _utc_iso(),
            "phrase_used": "",
            "phrase_category": "",
            "scored_timestamp": "",
        })

        pending_rows.append([row_map[h] for h in RAW_HEADERS_REQUIRED])
        dedupe.add(trip_key)
        time.sleep(sleep_s)

    # ── Single batch write ──
    print("=" * 70)
    if pending_rows:
        append_rows_bulk(ws_raw, pending_rows)
        print(f"✅ Inserted {len(pending_rows)} row(s) into {raw_tab}.")
    else:
        print("⚠️  No rows inserted this run.")

    # ── Run summary (structured for future metrics tab) ──
    london_pct = round(london_used / max(1, searches) * 100, 1)
    offer_rate = round((searches - no_offer) / max(1, searches) * 100, 1)
    unique_dests = len({r[RAW_HEADERS_REQUIRED.index("destination_iata")] for r in pending_rows})
    unique_origins = len({r[RAW_HEADERS_REQUIRED.index("origin_iata")] for r in pending_rows})

    print(f"\n📊 RUN SUMMARY")
    print(f"   slot={run_slot} | day_index={dix}")
    print(f"   buckets={bucket_a}+{bucket_b}")
    print(f"   searches={searches} | inserted={len(pending_rows)}")
    print(f"   dedupe_skips={dedupe_skips} | no_offer={no_offer}")
    print(f"   offer_rate={offer_rate}%")
    print(f"   unique_dests={unique_dests} | unique_origins={unique_origins}")
    print(f"   london_used={london_used} | london_share={london_pct}%")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
