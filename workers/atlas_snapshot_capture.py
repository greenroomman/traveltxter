#!/usr/bin/env python3
"""
workers/atlas_snapshot_capture.py
ATLAS SNAPSHOT CAPTURE — v0

Captures daily price snapshots for RegretRisk model training.
Writes to SNAPSHOT_LOG tab. Never touches RAW_DEALS.

Run daily at fixed UTC time (07:10 recommended).
Config: config/atlas_snapshot_config.json
"""

from __future__ import annotations

import os
import json
import time
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials


# ----------------------------
# LCC registry (proxy for lcc_present feature)
# ----------------------------

LCC_IATA_CODES = {
    # UK / Europe LCC
    "FR", "U2", "W6", "VY", "PC", "HV", "LS", "BE", "EN",
    "WX", "ZB", "TOM", "BY", "X3", "4U", "DE", "EW", "HG",
    # Nordic carriers (MAN routes)
    "DY", "D8", "SK", "FI", "WF", "DX",
    # US LCC (future proofing)
    "F9", "G4", "NK", "B6", "WN", "WS", "G3", "VT", "NX",
}


# ----------------------------
# Env helpers
# ----------------------------

def env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, default)).strip())
    except Exception:
        return default

def env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


# ----------------------------
# GSpread auth
# ----------------------------

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


# ----------------------------
# Time helpers
# ----------------------------

def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0)

def _utc_date() -> str:
    return _utc_now().strftime("%Y-%m-%d")

def _utc_time() -> str:
    return _utc_now().strftime("%H:%M")


# ----------------------------
# Duffel
# ----------------------------

DUFFEL_API = "https://api.duffel.com/air/offer_requests"

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

def duffel_search(
    origin: str,
    dest: str,
    out_date: str,
    ret_date: str,
    cabin: str = "economy",
    max_connections: int = 1,
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


# ----------------------------
# Offer extraction
# ----------------------------

def extract_carriers(offer: Dict[str, Any]) -> List[str]:
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

def extract_stops(offer: Dict[str, Any]) -> int:
    try:
        stops = 0
        for sl in offer.get("slices") or []:
            segs = sl.get("segments") or []
            stops += max(0, len(segs) - 1)
        return stops
    except Exception:
        return 0


# ----------------------------
# Snapshot key
# ----------------------------

def make_snapshot_key(
    origin: str,
    dest: str,
    out_date: str,
    ret_date: str,
    snapshot_date: str,
    capture_time: str,
) -> str:
    t = capture_time.replace(":", "")
    return f"{origin}_{dest}_{out_date}_{ret_date}_{snapshot_date}_{t}"


# ----------------------------
# Config
# ----------------------------

def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r") as f:
        return json.load(f)


# ----------------------------
# Date generation
# ----------------------------

def generate_target_dates(
    lookahead_min: int,
    lookahead_max: int,
    outbound_weekdays: List[int],
    trip_lengths: List[int],
    max_per_dest: int = 2,
) -> List[Tuple[str, str, int]]:
    today = _utc_now().date()
    results: List[Tuple[str, str, int]] = []
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


# ----------------------------
# SNAPSHOT_LOG schema
# ----------------------------

SNAPSHOT_HEADERS = [
    "snapshot_date", "capture_time_utc", "origin_iata", "destination_iata",
    "outbound_date", "return_date", "dtd", "price_gbp", "currency",
    "carrier_count", "lcc_present", "direct", "stops", "cabin_class",
    "price_t7", "price_t14", "rose_10pct", "fell_10pct", "snapshot_key", "notes",
]

def ensure_snapshot_headers(ws: gspread.Worksheet) -> None:
    first_row = ws.row_values(1)
    if not first_row or first_row[0] != "snapshot_date":
        ws.update("A1", [SNAPSHOT_HEADERS])
        print("📋 SNAPSHOT_LOG headers written.")

def load_existing_keys(ws: gspread.Worksheet) -> set:
    values = ws.get_all_values()
    if len(values) < 2:
        return set()
    try:
        hdr = values[0]
        key_col = hdr.index("snapshot_key")
    except (ValueError, IndexError):
        return set()
    return {row[key_col] for row in values[1:] if len(row) > key_col and row[key_col]}


# ----------------------------
# Main
# ----------------------------

def main() -> int:
    print("=" * 70)
    print("ATLAS SNAPSHOT CAPTURE v0")
    print("=" * 70)

    config_path = env_str("ATLAS_CONFIG_PATH", "config/atlas_snapshot_config.json")
    snapshot_tab = env_str("SNAPSHOT_LOG_TAB", "SNAPSHOT_LOG")
    sleep_s = float(env_str("FEEDER_SLEEP_SECONDS", "0.5"))
    max_searches = env_int("ATLAS_MAX_SEARCHES", 30)

    cfg = load_config(config_path)
    origins: List[str] = cfg.get("origins", ["MAN"])
    destinations: List[str] = cfg.get("destinations", [])
    trip_lengths: List[int] = cfg.get("trip_length_days", [3, 4, 5])
    outbound_weekdays: List[int] = cfg.get("outbound_weekdays", [3, 5])
    lookahead_min: int = cfg.get("lookahead_min_days", 45)
    lookahead_max: int = cfg.get("lookahead_max_days", 140)
    cabin: str = cfg.get("cabin_class", "economy")
    max_connections: int = cfg.get("max_connections", 1)
    max_per_dest: int = cfg.get("max_date_combos_per_dest", 2)

    if not destinations:
        raise RuntimeError("No destinations in atlas_snapshot_config.json.")

    gc = gspread_client()
    sh = gc.open_by_key(env_str("SPREADSHEET_ID") or env_str("SHEET_ID"))
    ws = sh.worksheet(snapshot_tab)
    ensure_snapshot_headers(ws)
    existing_keys = load_existing_keys(ws)

    snapshot_date = _utc_date()
    capture_time = _utc_time()

    date_combos = generate_target_dates(
        lookahead_min, lookahead_max, outbound_weekdays, trip_lengths, max_per_dest
    )

    print(f"📅 Date combos per dest: {len(date_combos)}")
    print(f"🌍 Destinations ({len(destinations)}): {destinations}")
    print(f"📍 Origins: {origins}")
    print(f"🔢 Max searches this run: {max_searches}")
    print("-" * 70)

    searches = 0
    captured = 0
    skipped = 0
    no_offer = 0
    pending: List[List[Any]] = []

    for origin in origins:
        for dest in destinations:
            for out_date, ret_date, dtd in date_combos:
                if searches >= max_searches:
                    break

                snap_key = make_snapshot_key(
                    origin, dest, out_date, ret_date, snapshot_date, capture_time
                )
                if snap_key in existing_keys:
                    skipped += 1
                    continue

                searches += 1
                print(f"🔎 [{searches}/{max_searches}] {origin}→{dest}  "
                      f"{out_date}/{ret_date}  DTD={dtd}")

                offer = duffel_search(
                    origin, dest, out_date, ret_date,
                    cabin=cabin, max_connections=max_connections
                )

                row: Dict[str, Any] = {h: "" for h in SNAPSHOT_HEADERS}
                row.update({
                    "snapshot_date": snapshot_date,
                    "capture_time_utc": capture_time,
                    "origin_iata": origin,
                    "destination_iata": dest,
                    "outbound_date": out_date,
                    "return_date": ret_date,
                    "dtd": dtd,
                    "snapshot_key": snap_key,
                })

                if not offer:
                    no_offer += 1
                    row["notes"] = "no_offer"
                    print(f"   ❌ No offer — logging null row")
                else:
                    carriers = extract_carriers(offer)
                    stops = extract_stops(offer)
                    lcc_present = any(c in LCC_IATA_CODES for c in carriers)
                    price_gbp = round(float(offer.get("total_amount") or 0), 2)

                    row.update({
                        "price_gbp": price_gbp,
                        "currency": "GBP",
                        "carrier_count": len(carriers),
                        "lcc_present": str(lcc_present).upper(),
                        "direct": str(stops == 0).upper(),
                        "stops": stops,
                        "cabin_class": cabin,
                        "notes": "",
                    })
                    captured += 1
                    print(f"   ✅ £{price_gbp} | {','.join(carriers)} | direct={stops == 0}")

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
                print(f"⚠️ append_rows attempt {attempt}/3 failed: {e}")
                if attempt < 3:
                    time.sleep(10 * attempt)
                else:
                    raise
    else:
        print("⚠️ No rows written.")

    print(
        f"📊 SUMMARY: searches={searches} captured={captured} "
        f"no_offer={no_offer} skipped={skipped}"
    )
    return 0
