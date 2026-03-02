#!/usr/bin/env python3
"""
workers/atlas_snapshot_capture.py
ATLAS SNAPSHOT CAPTURE — v1.2 (Per-Origin Cap)

Built against Atlas Snapshot Oilpan v1.0.

WHAT CHANGED FROM v1.1:
- Per-origin search cap added (max_searches_per_origin)
  Prevents any single origin consuming the full daily cap.
  Guarantees all 9 origins get daily SNAPSHOT_LOG coverage.
  Defaults to max_searches // len(origins) if not explicitly set.
  Override via ATLAS_MAX_SEARCHES_PER_ORIGIN env var.

WHAT CHANGED FROM v1.0:
- compatible_routes shuffled before search loop (random.shuffle)
- BUCKET_MAX_TIER aligned with oilpan spec: Buckets 1+2 max tier = 2

WHAT CHANGED FROM v0:
- Origins loaded from CONFIG_ORIGINS sheet (Tier 1 + 2 only)
- Destinations loaded from CONFIG_BUCKETS sheet (Buckets 1–5 only)
- Bucket × origin compatibility enforced (Buckets 4+5 → Tier 1 only)
- ROUTE_CATEGORY replaced by bucket-derived category_for_bucket()

WHAT IS UNCHANGED:
- SNAPSHOT_LOG schema — all columns preserved in exact order
- shi_variance_flag() logic
- School holiday / bank holiday logic
- Duffel search implementation
- Batch write pattern
- Dedupe via existing_keys
- atlas_snapshot_config.json retained for non-route parameters

OILPAN CONTRACT:
- Never touches RAW_DEALS
- Writes only to SNAPSHOT_LOG
- Stateless — no memory between runs
"""

from __future__ import annotations

import os
import json
import time
import random
import datetime as dt
import statistics
from uuid import uuid4
from dataclasses import dataclass
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


def env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


# ─────────────────────────────────────────────
# GSPREAD AUTH
# ─────────────────────────────────────────────

def _sanitize_sa_json(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE or GCP_SA_JSON.")
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
            repaired = (
                before + '"private_key"' + k1 + pk_prefix
                + key_body + "-----END PRIVATE KEY-----" + after
            )
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
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


# ─────────────────────────────────────────────
# UK SCHOOL HOLIDAYS + BANK HOLIDAYS (unchanged)
# ─────────────────────────────────────────────

UK_SCHOOL_HOLIDAYS = [
    ("2025-02-17", "2025-02-21"),
    ("2025-04-11", "2025-04-25"),
    ("2025-05-26", "2025-05-30"),
    ("2025-07-22", "2025-09-03"),
    ("2025-10-27", "2025-10-31"),
    ("2025-12-20", "2026-01-05"),
    ("2026-02-16", "2026-02-20"),
    ("2026-04-02", "2026-04-17"),
    ("2026-05-25", "2026-05-29"),
    ("2026-07-21", "2026-09-02"),
    ("2026-10-26", "2026-10-30"),
    ("2026-12-21", "2027-01-04"),
]

UK_BANK_HOLIDAYS = {
    "2025-04-18", "2025-04-21",
    "2025-05-05", "2025-05-26",
    "2025-08-25",
    "2025-12-25", "2025-12-26",
    "2026-01-01",
    "2026-04-03", "2026-04-06",
    "2026-05-04", "2026-05-25",
    "2026-08-31",
    "2026-12-25", "2026-12-26",
    "2027-01-01",
}


def check_school_holiday(departure_date: dt.date) -> bool:
    for start_str, end_str in UK_SCHOOL_HOLIDAYS:
        if dt.date.fromisoformat(start_str) <= departure_date <= dt.date.fromisoformat(end_str):
            return True
    return False


def check_bank_holiday_adjacent(departure_date: dt.date) -> bool:
    for delta in (-1, 0, 1):
        if (departure_date + dt.timedelta(days=delta)).isoformat() in UK_BANK_HOLIDAYS:
            return True
    return False


# ─────────────────────────────────────────────
# TIME HELPERS
# ─────────────────────────────────────────────

def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0)


def _utc_date() -> str:
    return _utc_now().strftime("%Y-%m-%d")


def _utc_time() -> str:
    return _utc_now().strftime("%H:%M")


# ─────────────────────────────────────────────
# LCC SET (unchanged)
# ─────────────────────────────────────────────

LCC_IATA_CODES = {
    "FR", "U2", "W6", "VY", "PC", "HV", "LS", "BE", "EN",
    "WX", "ZB", "TOM", "BY", "X3", "4U", "DE", "EW", "HG",
    "DY", "D8", "SK", "FI", "WF", "DX",
    "F9", "G4", "NK", "B6", "WN", "WS", "G3", "VT", "NX",
}


# ─────────────────────────────────────────────
# DUFFEL (unchanged)
# ─────────────────────────────────────────────

DUFFEL_API = "https://api.duffel.com/air/offer_requests"


def duffel_headers() -> Dict[str, str]:
    key = env_str("DUFFEL_API_KEY")
    if not key:
        raise RuntimeError("Missing DUFFEL_API_KEY.")
    return {
        "Authorization": "Bearer " + key,
        "Duffel-Version": "v2",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def duffel_search(
    origin: str, dest: str, out_date: str, ret_date: str,
    cabin: str = "economy", max_connections: int = 1,
) -> Optional[Any]:
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


def extract_carriers(offer: Any) -> List[str]:
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
    return carriers


def extract_stops(offer: Any) -> int:
    try:
        stops = 0
        for sl in offer.get("slices") or []:
            stops += max(0, len(sl.get("segments") or []) - 1)
        return stops
    except Exception:
        return 0


# ─────────────────────────────────────────────
# SHEET-DRIVEN ORIGIN LOADING
# ─────────────────────────────────────────────

@dataclass
class OriginRow:
    airport_iata: str
    tier: int


def load_origins_from_sheet(sh: gspread.Spreadsheet, tab: str) -> List[OriginRow]:
    """
    Reads CONFIG_ORIGINS. Returns Tier 1 + Tier 2 airports only.
    Tier 3 excluded — structural no-offer rate too high for daily monitoring.
    """
    try:
        ws = sh.worksheet(tab)
    except Exception:
        raise RuntimeError(f"CONFIG_ORIGINS tab '{tab}' not found.")

    rows = ws.get_all_records()
    origins: List[OriginRow] = []
    for r in rows:
        enabled = str(r.get("enabled", "")).strip().upper() in ("TRUE", "1", "YES", "Y")
        if not enabled:
            continue
        iata = str(r.get("airport_iata", "")).strip().upper()
        try:
            tier = int(r.get("tier", 1) or 1)
        except Exception:
            tier = 1
        if iata and tier in (1, 2):
            origins.append(OriginRow(airport_iata=iata, tier=tier))

    if not origins:
        raise RuntimeError("No Tier 1/2 origins found in CONFIG_ORIGINS.")

    print(f"✅ Origins loaded from {tab}: {len(origins)} airports "
          f"(T1={sum(1 for o in origins if o.tier==1)}, "
          f"T2={sum(1 for o in origins if o.tier==2)})")
    return origins


# ─────────────────────────────────────────────
# SHEET-DRIVEN DESTINATION LOADING
# ─────────────────────────────────────────────

@dataclass
class DestRow:
    destination_iata: str
    bucket_id: int
    city: str
    country: str


def load_destinations_from_sheet(sh: gspread.Spreadsheet, tab: str) -> List[DestRow]:
    """
    Reads CONFIG_BUCKETS. Returns Buckets 1–5 only.
    Bucket 6 (Seasonal/Wildcard) excluded — high no-offer rate from regional airports.
    C-tier liquidity destinations excluded — structural no-offer for monitoring purposes.
    """
    try:
        ws = sh.worksheet(tab)
    except Exception:
        raise RuntimeError(f"CONFIG_BUCKETS tab '{tab}' not found.")

    rows = ws.get_all_records()
    dests: List[DestRow] = []
    seen: set = set()
    for r in rows:
        enabled = str(r.get("enabled", "")).strip().upper() in ("TRUE", "1", "YES", "Y")
        if not enabled:
            continue
        try:
            bid = int(r.get("bucket_id", 0) or 0)
        except Exception:
            bid = 0
        if bid not in (1, 2, 3, 4, 5):
            continue
        iata = str(r.get("destination_iata", "")).strip().upper()
        if not iata or iata in seen:
            continue
        liq = str(r.get("liquidity_tier", "B")).strip().upper()
        if liq == "C":
            continue
        dests.append(DestRow(
            destination_iata=iata,
            bucket_id=bid,
            city=str(r.get("city", "")).strip(),
            country=str(r.get("country", "")).strip(),
        ))
        seen.add(iata)

    if not dests:
        raise RuntimeError("No destinations found in CONFIG_BUCKETS (Buckets 1–5).")

    by_bucket = {}
    for d in dests:
        by_bucket.setdefault(d.bucket_id, 0)
        by_bucket[d.bucket_id] += 1
    print(f"✅ Destinations loaded from {tab}: {len(dests)} total "
          f"(by bucket: {dict(sorted(by_bucket.items()))})")
    return dests


# ─────────────────────────────────────────────
# BUCKET × ORIGIN COMPATIBILITY
# ─────────────────────────────────────────────

BUCKET_MAX_TIER: Dict[int, int] = {
    1: 2,  # EU High Volume     — Tier 1 + 2
    2: 2,  # EU Secondary       — Tier 1 + 2
    3: 2,  # Near Long-Haul     — Tier 1 + 2
    4: 1,  # Long-Haul US/CA    — Tier 1 only
    5: 1,  # Long-Haul Asia/ME  — Tier 1 only
}


def origin_eligible_for_bucket(origin: OriginRow, bucket_id: int) -> bool:
    max_tier = BUCKET_MAX_TIER.get(bucket_id, 1)
    return origin.tier <= max_tier


# ─────────────────────────────────────────────
# BUCKET-DERIVED CATEGORY
# ─────────────────────────────────────────────

def category_for_bucket(bucket_id: int) -> str:
    return {
        1: "leisure",
        2: "leisure",
        3: "mixed",
        4: "long_haul",
        5: "long_haul",
        6: "leisure",
    }.get(bucket_id, "leisure")


# ─────────────────────────────────────────────
# SHI VARIANCE FLAG (unchanged logic)
# ─────────────────────────────────────────────

def shi_variance_flag(
    route_key: str, today_price: float, history: Dict[str, List[float]]
) -> str:
    baseline = history.get(route_key, [])
    if len(baseline) < 5:
        return "INSUFFICIENT_DATA"
    try:
        mean = statistics.mean(baseline)
        stdev = statistics.stdev(baseline)
        if stdev == 0:
            return "FLAG"
        z = abs(today_price - mean) / stdev
        return "FLAG" if z > 2.5 else "OK"
    except Exception:
        return "INSUFFICIENT_DATA"


# ─────────────────────────────────────────────
# DATE GENERATION (unchanged)
# ─────────────────────────────────────────────

def generate_target_dates(
    lookahead_min: int, lookahead_max: int,
    outbound_weekdays: List[int], trip_lengths: List[int],
    max_per_dest: int = 1,
) -> List[Tuple[str, str, int]]:
    today = _utc_now().date()
    results = []
    weekday_hits = 0
    for delta in range(lookahead_min, lookahead_max + 1):
        candidate = today + dt.timedelta(days=delta)
        if candidate.weekday() not in outbound_weekdays:
            continue
        weekday_hits += 1
        if weekday_hits > max_per_dest:
            break
        for tl in trip_lengths:
            ret = candidate + dt.timedelta(days=tl)
            results.append((
                candidate.strftime("%Y-%m-%d"),
                ret.strftime("%Y-%m-%d"),
                delta,
            ))
    return results


# ─────────────────────────────────────────────
# SNAPSHOT_LOG SCHEMA (unchanged)
# ─────────────────────────────────────────────

SNAPSHOT_HEADERS = [
    "snapshot_id",
    "snapshot_date", "capture_time_utc",
    "origin_iata", "destination_iata",
    "outbound_date", "return_date",
    "dtd",
    "day_of_week_departure", "day_of_week_snapshot",
    "is_school_holiday_window", "is_bank_holiday_adjacent",
    "price_gbp", "currency",
    "carrier_count", "lcc_present", "direct", "stops", "cabin_class",
    "seats_remaining",
    "price_t7", "price_t14", "rose_10pct", "fell_10pct",
    "snapshot_key", "notes",
    "origin_type", "shi_variance_flag",
]


def make_snapshot_key(
    origin: str, dest: str, out_date: str, ret_date: str,
    snapshot_date: str, capture_time: str,
) -> str:
    t = capture_time.replace(":", "")
    return f"{origin}_{dest}_{out_date}_{ret_date}_{snapshot_date}_{t}"


def ensure_snapshot_headers(ws: gspread.Worksheet) -> None:
    first_row = ws.row_values(1)
    if not first_row or first_row[0] != "snapshot_id":
        ws.update("A1", [SNAPSHOT_HEADERS])
        print("SNAPSHOT_LOG headers written.")


def load_existing_keys(ws: gspread.Worksheet) -> Tuple[set, Dict[str, List[float]]]:
    values = ws.get_all_values()
    if len(values) < 2:
        return set(), {}
    try:
        hdr = values[0]
        key_col = hdr.index("snapshot_key")
    except (ValueError, IndexError):
        return set(), {}

    today = _utc_now().date()
    cutoff = (today - dt.timedelta(days=7)).isoformat()
    price_history: Dict[str, List[float]] = {}
    try:
        snap_col = hdr.index("snapshot_date")
        orig_col = hdr.index("origin_iata")
        dest_col = hdr.index("destination_iata")
        price_col = hdr.index("price_gbp")
        for row in values[1:]:
            def _get(col: int) -> str:
                return row[col].strip() if col < len(row) else ""
            if _get(snap_col) < cutoff:
                continue
            route_k = _get(orig_col) + "-" + _get(dest_col)
            try:
                p = float(_get(price_col))
                if p > 0:
                    price_history.setdefault(route_k, []).append(p)
            except ValueError:
                pass
    except (ValueError, IndexError):
        pass

    keys = {
        row[key_col]
        for row in values[1:]
        if len(row) > key_col and row[key_col]
    }
    return keys, price_history


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main() -> int:
    print("=" * 70)
    print("ATLAS SNAPSHOT CAPTURE v1.2 — Per-Origin Cap")
    print("=" * 70)

    config_path = env_str("ATLAS_CONFIG_PATH", "config/atlas_snapshot_config.json")
    snapshot_tab = env_str("SNAPSHOT_LOG_TAB", "SNAPSHOT_LOG")
    origins_tab = env_str("FEEDER_ORIGINS_TAB", "CONFIG_ORIGINS")
    buckets_tab = env_str("FEEDER_BUCKETS_TAB", "CONFIG_BUCKETS")
    sleep_s = float(env_str("FEEDER_SLEEP_SECONDS", "0.5"))
    max_searches = env_int("ATLAS_MAX_SEARCHES", 160)
    max_searches_per_origin = env_int("ATLAS_MAX_SEARCHES_PER_ORIGIN", 0)

    # Load non-route config from JSON
    with open(config_path, "r") as f:
        cfg = json.load(f)
    trip_lengths = cfg.get("trip_length_days", [4, 7, 14])
    outbound_weekdays = cfg.get("outbound_weekdays", [3, 5])
    lookahead_min = cfg.get("lookahead_min_days", 14)
    lookahead_max = cfg.get("lookahead_max_days", 84)
    cabin = cfg.get("cabin_class", "economy")
    max_connections = cfg.get("max_connections", 1)
    max_per_dest = cfg.get("max_date_combos_per_dest", 1)

    gc = gspread_client()
    sh = gc.open_by_key(env_str("SPREADSHEET_ID") or env_str("SHEET_ID"))

    # Sheet-driven origins and destinations
    origins = load_origins_from_sheet(sh, origins_tab)
    destinations = load_destinations_from_sheet(sh, buckets_tab)

    ws = sh.worksheet(snapshot_tab)
    ensure_snapshot_headers(ws)
    existing_keys, price_history = load_existing_keys(ws)

    snapshot_date = _utc_date()
    capture_time = _utc_time()

    date_combos = generate_target_dates(
        lookahead_min, lookahead_max, outbound_weekdays, trip_lengths, max_per_dest
    )

    # Build compatible routes then shuffle so the per-origin cap fills
    # evenly across destinations rather than always taking the same ones.
    compatible_routes = [
        (o, d)
        for o in origins
        for d in destinations
        if origin_eligible_for_bucket(o, d.bucket_id)
    ]
    random.shuffle(compatible_routes)

    total_routes = len(origins) * len(destinations)
    skipped_compat = total_routes - len(compatible_routes)

    # Per-origin cap — guarantees every origin gets daily coverage.
    # If not explicitly set, divide global cap evenly across origins.
    if max_searches_per_origin <= 0:
        max_searches_per_origin = max_searches // len(origins)

    print(f"📅 Snapshot date: {snapshot_date} | Capture time: {capture_time}")
    print(f"🗓  Date combos per route: {len(date_combos)}")
    print(f"🛫 Compatible origin × destination routes: {len(compatible_routes)}")
    print(f"🔢 Max potential searches: {len(compatible_routes) * len(date_combos)}")
    print(f"🔒 Global search cap: {max_searches}")
    print(f"🎯 Per-origin search cap: {max_searches_per_origin}")
    print(f"⏭️  Compatibility filter: {skipped_compat} routes skipped "
          f"(tier mismatch — e.g. GLA→JFK, BRS→BKK)")
    print(f"🔀 Route order: shuffled")
    print("-" * 70)

    searches = 0
    captured = 0
    skipped_dedupe = 0
    no_offer = 0
    pending: List[List[Any]] = []
    searches_by_origin: Dict[str, int] = {}

    for origin, dest in compatible_routes:
        if searches >= max_searches:
            print(f"⚠️  Global search cap reached ({max_searches}).")
            break

        origin_count = searches_by_origin.get(origin.airport_iata, 0)
        if origin_count >= max_searches_per_origin:
            continue  # This origin is done for today, move to next route

        for out_date, ret_date, dtd in date_combos:
            if searches >= max_searches:
                break
            if searches_by_origin.get(origin.airport_iata, 0) >= max_searches_per_origin:
                break

            snap_key = make_snapshot_key(
                origin.airport_iata, dest.destination_iata,
                out_date, ret_date, snapshot_date, capture_time
            )
            if snap_key in existing_keys:
                skipped_dedupe += 1
                continue

            searches += 1
            searches_by_origin[origin.airport_iata] = (
                searches_by_origin.get(origin.airport_iata, 0) + 1
            )
            category = category_for_bucket(dest.bucket_id)
            route_key = f"{origin.airport_iata}-{dest.destination_iata}"

            print(
                f"[{searches}/{max_searches}] "
                f"{origin.airport_iata}(T{origin.tier})→{dest.destination_iata} "
                f"{out_date}/{ret_date} DTD={dtd} "
                f"[B{dest.bucket_id}:{category}] "
                f"[origin {searches_by_origin[origin.airport_iata]}/{max_searches_per_origin}]"
            )

            offer = duffel_search(
                origin.airport_iata, dest.destination_iata,
                out_date, ret_date,
                cabin=cabin, max_connections=max_connections,
            )

            row = {h: "" for h in SNAPSHOT_HEADERS}
            out_date_obj = dt.date.fromisoformat(out_date)
            snap_date_obj = dt.date.fromisoformat(snapshot_date)

            row.update({
                "snapshot_id": str(uuid4()),
                "snapshot_date": snapshot_date,
                "capture_time_utc": capture_time,
                "origin_iata": origin.airport_iata,
                "destination_iata": dest.destination_iata,
                "outbound_date": out_date,
                "return_date": ret_date,
                "dtd": dtd,
                "day_of_week_departure": out_date_obj.strftime("%A"),
                "day_of_week_snapshot": snap_date_obj.strftime("%A"),
                "is_school_holiday_window": str(check_school_holiday(out_date_obj)).upper(),
                "is_bank_holiday_adjacent": str(check_bank_holiday_adjacent(out_date_obj)).upper(),
                "snapshot_key": snap_key,
                "origin_type": category,
            })

            if not offer:
                no_offer += 1
                row["notes"] = "no_offer"
                row["shi_variance_flag"] = ""
                print(f"   ❌ No offer")
            else:
                carriers = extract_carriers(offer)
                stops = extract_stops(offer)
                lcc_present = any(c in LCC_IATA_CODES for c in carriers)
                price_gbp = round(float(offer.get("total_amount") or 0), 2)

                seats_remaining = None
                try:
                    slices = offer.get("slices") or []
                    if slices:
                        first_seg = (slices[0].get("segments") or [{}])[0]
                        seats_remaining = first_seg.get("available_seats")
                except Exception:
                    pass

                shi_flag = shi_variance_flag(route_key, price_gbp, price_history)
                if shi_flag == "FLAG":
                    print(f"   ⚠️  SHI variance FLAG")

                row.update({
                    "price_gbp": price_gbp,
                    "currency": "GBP",
                    "carrier_count": len(carriers),
                    "lcc_present": str(lcc_present).upper(),
                    "direct": str(stops == 0).upper(),
                    "stops": stops,
                    "cabin_class": cabin,
                    "seats_remaining": seats_remaining if seats_remaining is not None else "",
                    "notes": "",
                    "shi_variance_flag": shi_flag,
                })
                captured += 1
                print(
                    f"   ✅ £{price_gbp} | "
                    f"{','.join(carriers)} | "
                    f"direct={stops == 0} | "
                    f"SHI={shi_flag}"
                )

                price_history.setdefault(route_key, []).append(price_gbp)

            pending.append([row[h] for h in SNAPSHOT_HEADERS])
            existing_keys.add(snap_key)
            time.sleep(sleep_s)

    print("-" * 70)

    if pending:
        for attempt in range(1, 4):
            try:
                ws.append_rows(pending, value_input_option="USER_ENTERED")
                print(f"✅ Written {len(pending)} rows to {snapshot_tab}.")
                break
            except Exception as e:
                print(f"append_rows attempt {attempt}/3 failed: {e}")
                if attempt < 3:
                    time.sleep(10 * attempt)
                else:
                    raise
    else:
        print("⚠️  No rows written.")

    origin_iatas = list(dict.fromkeys(
        r[SNAPSHOT_HEADERS.index("origin_iata")]
        for r in pending
        if r[SNAPSHOT_HEADERS.index("origin_iata")]
    ))

    print(
        f"\n📊 RUN SUMMARY\n"
        f"   searches={searches} | captured={captured} | "
        f"no_offer={no_offer} | dedupe_skipped={skipped_dedupe}\n"
        f"   offer_rate={round(captured / max(1, searches) * 100, 1)}%\n"
        f"   origins_used={origin_iatas}\n"
        f"   searches_by_origin={dict(sorted(searches_by_origin.items()))}\n"
        f"   compat_routes={len(compatible_routes)} | "
        f"compat_skipped={skipped_compat}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
