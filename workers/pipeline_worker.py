#!/usr/bin/env python3
"""
workers/pipeline_worker.py
TRAVELTXTTER V5 FEEDER

Fixes in this version:
- ISO timestamp format for ingested_at_utc (was epoch int, now ISO string)
- Enhanced logging to see dedupe skips and destination progression
- Preserved all other V5 contract requirements
"""

from __future__ import annotations

import os
import json
import time
import math
import hashlib
import random
import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials


# ----------------------------
# Env helpers
# ----------------------------

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


def parse_csv_list(s: str) -> List[str]:
    out: List[str] = []
    for part in (s or "").split(","):
        p = part.strip().upper()
        if p:
            out.append(p)
    return out


# ----------------------------
# GSpread auth (robust)
# ----------------------------

def _sanitize_sa_json(raw: str) -> str:
    """
    Handles common GitHub Secrets failure mode where SA JSON contains literal newlines
    inside the private_key value (invalid JSON).
    """
    raw = (raw or "").strip()
    if not raw:
        raise RuntimeError("Missing GCP service account JSON (GCP_SA_JSON_ONE_LINE or GCP_SA_JSON).")

    # Fast path: already valid JSON
    try:
        json.loads(raw)
        return raw
    except Exception:
        pass

    # Common path: one-line JSON with \n escapes
    try:
        fixed = raw.replace("\\n", "\n")
        json.loads(fixed)
        return fixed
    except Exception:
        pass

    # Repair literal newlines inside private_key value
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
            repaired = before + '"private_key"' + k1 + pk_prefix + key_body + "-----END PRIVATE KEY-----" + after
            json.loads(repaired)
            return repaired
        except Exception:
            pass

    # Last resort: remove CR and re-raise
    raw2 = raw.replace("\r", "")
    json.loads(raw2)  # will raise with clearer message
    return raw2


def gspread_client() -> gspread.Client:
    raw = os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON") or ""
    raw = _sanitize_sa_json(raw)
    info = json.loads(raw)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def get_cell(ws: gspread.Worksheet, a1: str) -> str:
    try:
        v = ws.acell(a1).value
        return "" if v is None else str(v).strip()
    except Exception:
        return ""


# ----------------------------
# Timestamp helper (ISO format)
# ----------------------------

def _utc_iso() -> str:
    """
    CRITICAL FIX: Return ISO format timestamp string (not epoch int)
    IG Publisher expects: "2026-02-08T07:34:52Z"
    """
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def _hash_trip(origin: str, dest: str, out_date: str, ret_date: str) -> str:
    s = f"{origin}|{dest}|{out_date}|{ret_date}"
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:12]


def _pick_dates(days_ahead_min: int, days_ahead_max: int, trip_length_days: int) -> Tuple[str, str]:
    # deterministic per hour bucket
    seed = int(time.time() // 3600)
    rnd = random.Random(seed + trip_length_days + days_ahead_min * 31 + days_ahead_max)
    depart_in = rnd.randint(days_ahead_min, max(days_ahead_min, days_ahead_max))
    out_epoch = int(time.time()) + depart_in * 86400
    out = time.strftime("%Y-%m-%d", time.gmtime(out_epoch))
    ret_epoch = out_epoch + max(1, trip_length_days) * 86400
    ret = time.strftime("%Y-%m-%d", time.gmtime(ret_epoch))
    return out, ret


def duffel_search(origin: str, dest: str, out_date: str, ret_date: str, cabin: str, max_connections: int) -> Optional[Dict[str, Any]]:
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


# ----------------------------
# Offer extraction (never crash)
# ----------------------------

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
            segs = sl.get("segments") or []
            stops += max(0, len(segs) - 1)
        return int(stops)
    except Exception:
        return 0


def extract_cabin_class(offer: Dict[str, Any], fallback: str = "economy") -> str:
    cc = (offer.get("cabin_class") or "").strip().lower()
    return cc if cc else fallback


def extract_bags_included(offer: Dict[str, Any]) -> str:
    """
    Fix for crash: offer.get("available_services") can be None
    """
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


# ----------------------------
# Sheets: CONFIG + RAW_DEALS
# ----------------------------

@dataclass
class DestRow:
    destination_iata: str
    weight: float


def load_config_dests(ws_cfg: gspread.Worksheet, theme_today: str) -> List[DestRow]:
    rows = ws_cfg.get_all_records()
    out: List[DestRow] = []
    for r in rows:
        enabled = str(r.get("enabled", "")).strip().upper() in ("TRUE", "1", "YES", "Y")
        if not enabled:
            continue
        theme = str(r.get("theme", "")).strip()
        if theme.lower() != theme_today.lower():
            continue
        dest = str(r.get("destination_iata", "")).strip().upper()
        if not dest:
            continue
        try:
            w = float(r.get("weight", 1.0) or 1.0)
        except Exception:
            w = 1.0
        out.append(DestRow(dest, w))
    out.sort(key=lambda x: x.weight, reverse=True)
    return out


def header_map(ws: gspread.Worksheet) -> Dict[str, int]:
    hdr = ws.row_values(1)
    return {str(h).strip(): i for i, h in enumerate(hdr)}


def ensure_headers(ws: gspread.Worksheet, required: List[str]) -> Dict[str, int]:
    hm = header_map(ws)
    missing = [h for h in required if h not in hm]
    if missing:
        raise RuntimeError(f"{ws.title} missing required headers: {missing}")
    return hm


def load_dedupe_set(ws_raw: gspread.Worksheet, hm: Dict[str, int]) -> set:
    values = ws_raw.get_all_values()
    if len(values) < 2:
        return set()
    rows = values[1:]

    def col(name: str, row: List[str]) -> str:
        i = hm.get(name)
        return (row[i] if (i is not None and i < len(row)) else "").strip()

    s = set()
    for r in rows:
        o = col("origin_iata", r).upper()
        d = col("destination_iata", r).upper()
        od = col("outbound_date", r)
        rd = col("return_date", r)
        if o and d and od and rd:
            s.add((o, d, od, rd))
    return s


def append_rows_bulk(ws_raw: gspread.Worksheet, rows: List[List[Any]]) -> None:
    if rows:
        ws_raw.append_rows(rows, value_input_option="USER_ENTERED")


# ----------------------------
# Origin selection
# ----------------------------

def origins_for(theme: str, run_slot: str) -> List[str]:
    tkey = theme.upper()
    slot = run_slot.upper() if run_slot else "PM"

    # Priority: AM_/PM_ -> legacy ORIGINS_ -> ORIGINS_DEFAULT
    key1 = f"{slot}_ORIGINS_{tkey}"
    key2 = f"ORIGINS_{tkey}"
    origins = parse_csv_list(env_str(key1))
    if not origins:
        origins = parse_csv_list(env_str(key2))
    if not origins:
        origins = parse_csv_list(env_str("ORIGINS_DEFAULT", "LHR,LGW,MAN"))

    seen = set()
    out: List[str] = []
    for o in origins:
        if o not in seen:
            out.append(o)
            seen.add(o)
    return out


def max_stops_for(theme: str) -> int:
    tkey = theme.upper()
    return env_int(f"MAX_STOPS_{tkey}", env_int("MAX_STOPS_DEFAULT", 1))


def window_for(theme: str) -> Tuple[int, int]:
    tkey = theme.upper()
    w = env_str(f"WINDOW_{tkey}_MIN/MAX")
    if w and "/" in w:
        a, b = w.split("/", 1)
        return int(a.strip()), int(b.strip())
    return 21, 84


def trip_for(theme: str) -> Tuple[int, int]:
    tkey = theme.upper()
    s = env_str(f"TRIP_{tkey}_MIN/MAX")
    if s and "/" in s:
        a, b = s.split("/", 1)
        return int(a.strip()), int(b.strip())
    return 4, 10


# ----------------------------
# RAW_DEALS contract
# ----------------------------

RAW_HEADERS_REQUIRED = [
    "deal_id",
    "origin_iata",
    "destination_iata",
    "origin_city",
    "destination_city",
    "destination_country",
    "outbound_date",
    "return_date",
    "price_gbp",
    "currency",
    "stops",
    "cabin_class",
    "carriers",
    "theme",
    "status",
    "publish_window",
    "score",
    "bags_incl",
    "graphic_url",
    "booking_link_vip",
    "posted_vip_at",
    "posted_free_at",
    "posted_instagram_at",
    "ingested_at_utc",
    "phrase_used",
    "phrase_category",
    "scored_timestamp",
]


# ----------------------------
# Main
# ----------------------------

def main() -> int:
    print("======================================================================")
    print("TRAVELTXTTER V5 — FEEDER START (FIXED: ISO TIMESTAMPS)")
    print("======================================================================")

    run_slot = env_str("RUN_SLOT", "PM").upper()
    cfg_tab = env_str("FEEDER_CONFIG_TAB", "CONFIG")
    raw_tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    ops_tab = env_str("OPS_MASTER_TAB", "OPS_MASTER")

    max_searches = env_int("DUFFEL_MAX_SEARCHES_PER_RUN", env_int("FEEDER_MAX_SEARCHES", 12))
    max_inserts = env_int("DUFFEL_MAX_INSERTS", env_int("FEEDER_MAX_INSERTS", 20))
    routes_per_run = env_int("DUFFEL_ROUTES_PER_RUN", 4)
    sleep_s = env_float("FEEDER_SLEEP_SECONDS", 0.1)

    gc = gspread_client()
    sh = gc.open_by_key(env_str("SPREADSHEET_ID") or env_str("SHEET_ID"))

    ws_ops = sh.worksheet(ops_tab)
    theme_today = (get_cell(ws_ops, "B2") or "DEFAULT").strip()
    print(f"🎯 Theme of day: {theme_today}")

    ws_raw = sh.worksheet(raw_tab)
    hm = ensure_headers(ws_raw, RAW_HEADERS_REQUIRED)
    dedupe = load_dedupe_set(ws_raw, hm)

    ws_cfg = sh.worksheet(cfg_tab)
    dests = load_config_dests(ws_cfg, theme_today)
    if not dests:
        print("⚠️ No CONFIG routes eligible for theme.")
        return 0

    chosen = dests[:max(1, routes_per_run)]
    origins = origins_for(theme_today, run_slot)
    if not origins:
        raise RuntimeError("No origins resolved (check AM_/PM_/ORIGINS_* env vars).")

    max_conn = max_stops_for(theme_today)
    win_min, win_max = window_for(theme_today)
    trip_min, trip_max = trip_for(theme_today)

    cabin = env_str("CABIN_CLASS", "economy").lower()

    print(f"📍 Origins: {origins}")
    print(f"📍 Destinations (top {len(chosen)}): {[d.destination_iata for d in chosen]}")
    print(f"📍 Max searches: {max_searches} | Max inserts: {max_inserts}")
    print("=" * 60)

    searches = 0
    no_offer = 0
    dedupe_skips = 0
    pending_rows: List[List[Any]] = []

    for d in chosen:
        if searches >= max_searches or len(pending_rows) >= max_inserts:
            break

        for o in origins:
            if searches >= max_searches or len(pending_rows) >= max_inserts:
                break

            trip_len = max(trip_min, min(trip_max, int(round((trip_min + trip_max) / 2))))
            out_date, ret_date = _pick_dates(win_min, win_max, trip_len)

            trip_key = (o, d.destination_iata, out_date, ret_date)
            if trip_key in dedupe:
                dedupe_skips += 1
                print(f"⏭️  Dedupe skip: {o}→{d.destination_iata} {out_date}/{ret_date}")
                continue

            searches += 1
            print(f"🔎 Search {searches}/{max_searches} {o}→{d.destination_iata} {out_date}/{ret_date}")

            offer = duffel_search(o, d.destination_iata, out_date, ret_date, cabin=cabin, max_connections=max_conn)
            if not offer:
                no_offer += 1
                print(f"   ❌ No offer found")
                time.sleep(sleep_s)
                continue

            total_amount = float(offer.get("total_amount") or 0.0)
            price_gbp = int(math.ceil(total_amount))
            currency = (offer.get("total_currency") or "GBP").upper()

            print(f"   ✅ Found offer: £{price_gbp}")

            row_map: Dict[str, Any] = {h: "" for h in RAW_HEADERS_REQUIRED}
            row_map.update({
                "deal_id": offer.get("id") or _hash_trip(o, d.destination_iata, out_date, ret_date),
                "origin_iata": o,
                "destination_iata": d.destination_iata,
                "origin_city": "",
                "destination_city": "",
                "destination_country": "",
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
                "ingested_at_utc": _utc_iso(),  # FIXED: ISO format
                "phrase_used": "",
                "phrase_category": "",
                "scored_timestamp": "",
            })

            pending_rows.append([row_map[h] for h in RAW_HEADERS_REQUIRED])
            dedupe.add(trip_key)
            time.sleep(sleep_s)

    print("=" * 60)
    if pending_rows:
        append_rows_bulk(ws_raw, pending_rows)
        print(f"✅ Inserted {len(pending_rows)} row(s) into {raw_tab}.")
    else:
        print("⚠️ No rows inserted.")

    print(f"📊 SUMMARY: searches={searches} inserted={len(pending_rows)} dedupe_skips={dedupe_skips} no_offer={no_offer}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
