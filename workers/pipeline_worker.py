#!/usr/bin/env python3
"""
TravelTxter V4.5x — pipeline_worker.py (FEEDER ONLY)

Purpose:
- Insert NEW deals into RAW_DEALS using Duffel offer_requests.
- Do NOT score, render, or publish.

Hard rules:
- Google Sheets is the state machine.
- Status written by this file: NEW only.
- Writes by HEADER NAME ONLY (no column numbers).
- Duffel-Version: v2.

Reads (best-effort):
- RAW_DEALS (for dedupe/variety)
- CONFIG routes (optional)
- CONFIG_ORIGIN_POOLS (optional)
- CONFIG_SIGNALS (optional: IATA → City/Country lookup)

Writes (if headers exist in RAW_DEALS):
- status
- deal_id
- price_gbp
- origin_iata
- destination_iata
- origin_city
- destination_city
- destination_country
- outbound_date
- return_date
- stops
- deal_theme
- affiliate_url
- booking_link_vip
- affiliate_source

Env required:
- SPREADSHEET_ID
- GCP_SA_JSON_ONE_LINE (service account json, single line)
- RAW_DEALS_TAB (default RAW_DEALS)
- DUFFEL_API_KEY

Env optional (sane defaults):
- DUFFEL_ROUTES_PER_RUN (default 3)
- DUFFEL_MAX_SEARCHES_PER_RUN (default 4)
- DUFFEL_MAX_INSERTS (default 3)
- DAYS_AHEAD_MIN (default 30)
- TRIP_LEN_DAYS (default 5)
- VARIETY_LOOKBACK_HOURS (default 120)
- THEME (default DEFAULT)
- FEEDER_ROUTES (override list "BRS:AGP,BRS:PMI,MAN:TFS")
"""

from __future__ import annotations

import os
import json
import time
import math
import hashlib
import datetime as dt
from typing import Dict, Any, List, Tuple, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# Logging
# ============================================================

def utcnow() -> dt.datetime:
    return dt.datetime.utcnow()

def ts() -> str:
    return utcnow().replace(microsecond=0).isoformat() + "Z"

def log(msg: str) -> None:
    print(f"{ts()} | {msg}", flush=True)


# ============================================================
# Env
# ============================================================

def env_str(k: str, default: str = "") -> str:
    return os.environ.get(k, default).strip()

def env_int(k: str, default: int) -> int:
    try:
        return int(env_str(k, str(default)))
    except Exception:
        return default


# ============================================================
# Google Sheets
# ============================================================

def get_gspread_client() -> gspread.Client:
    sa = env_str("GCP_SA_JSON_ONE_LINE") or env_str("GCP_SA_JSON")
    if not sa:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE (recommended) or GCP_SA_JSON")

    try:
        info = json.loads(sa)
    except json.JSONDecodeError:
        info = json.loads(sa.replace("\\n", "\n"))

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def ws_all(ws: gspread.Worksheet) -> List[List[str]]:
    return ws.get_all_values() or []


# ============================================================
# Config lookups (best-effort)
# ============================================================

def load_signals_lookup(spread: gspread.Spreadsheet) -> Dict[str, Dict[str, str]]:
    """
    CONFIG_SIGNALS best-effort:
    maps IATA -> {city, country}

    Accepts any of these header patterns:
      destination_iata / destination_city / destination_country
      iata / city / country
      airport_iata / city / country
    """
    try:
        ws = spread.worksheet("CONFIG_SIGNALS")
    except Exception:
        return {}

    data = ws_all(ws)
    if len(data) < 2:
        return {}

    headers = [h.strip() for h in data[0]]
    h = {name: i for i, name in enumerate(headers)}

    def pick(*cands: str) -> Optional[int]:
        for c in cands:
            if c in h:
                return h[c]
        return None

    i_iata = pick("destination_iata", "iata", "airport_iata", "dest_iata")
    i_city = pick("destination_city", "city", "dest_city")
    i_country = pick("destination_country", "country", "dest_country")

    if i_iata is None:
        return {}

    out: Dict[str, Dict[str, str]] = {}
    for r in data[1:]:
        code = (r[i_iata] if i_iata < len(r) else "").strip().upper()
        if not code:
            continue
        city = (r[i_city] if (i_city is not None and i_city < len(r)) else "").strip()
        country = (r[i_country] if (i_country is not None and i_country < len(r)) else "").strip()
        out[code] = {"city": city, "country": country}
    return out


def parse_feeder_routes(s: str) -> List[Tuple[str, str]]:
    routes: List[Tuple[str, str]] = []
    for part in (s or "").split(","):
        part = part.strip()
        if not part or ":" not in part:
            continue
        o, d = part.split(":", 1)
        o = o.strip().upper()
        d = d.strip().upper()
        if len(o) == 3 and len(d) == 3:
            routes.append((o, d))
    return routes


def pick_routes(spread: gspread.Spreadsheet) -> List[Tuple[str, str]]:
    """
    Priority:
    1) FEEDER_ROUTES env override
    2) CONFIG tab with origin_iata + destination_iata columns
    3) fallback list
    """
    env_routes = parse_feeder_routes(env_str("FEEDER_ROUTES"))
    if env_routes:
        return env_routes

    try:
        ws = spread.worksheet("CONFIG")
        data = ws_all(ws)
        if len(data) >= 2:
            headers = [h.strip() for h in data[0]]
            h = {name: i for i, name in enumerate(headers)}
            if "origin_iata" in h and "destination_iata" in h:
                routes: List[Tuple[str, str]] = []
                for r in data[1:]:
                    o = (r[h["origin_iata"]] if h["origin_iata"] < len(r) else "").strip().upper()
                    d = (r[h["destination_iata"]] if h["destination_iata"] < len(r) else "").strip().upper()
                    if len(o) == 3 and len(d) == 3:
                        routes.append((o, d))
                if routes:
                    return routes
    except Exception:
        pass

    return [
        ("BRS", "AGP"),
        ("BRS", "PMI"),
        ("MAN", "TFS"),
        ("LHR", "BCN"),
        ("LGW", "FAO"),
        ("BRS", "GVA"),
        ("MAN", "GVA"),
    ]


# ============================================================
# Variety / dedupe
# ============================================================

def deal_hash(origin: str, dest: str, out_date: str, ret_date: str, price_gbp: str) -> str:
    raw = f"{origin}|{dest}|{out_date}|{ret_date}|{price_gbp}".encode("utf-8")
    return hashlib.sha1(raw).hexdigest()[:12]

def parse_iso(s: str) -> Optional[dt.datetime]:
    s = (s or "").strip()
    # Accept timestamp "....Z" or date
    try:
        if s.endswith("Z"):
            return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            # date only
            return dt.datetime.fromisoformat(s + "T00:00:00")
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None

def recently_seen(existing_rows: List[Dict[str, str]], origin: str, dest: str, lookback_hours: int) -> bool:
    cutoff = utcnow() - dt.timedelta(hours=lookback_hours)
    for r in existing_rows:
        oi = (r.get("origin_iata") or "").strip().upper()
        di = (r.get("destination_iata") or "").strip().upper()
        if oi != origin or di != dest:
            continue
        t = parse_iso(r.get("scanned_at") or r.get("created_at") or r.get("inserted_at") or r.get("timestamp") or "")
        # If no timestamp field, we still treat it as seen to reduce repeats
        if t is None:
            return True
        if t >= cutoff:
            return True
    return False


# ============================================================
# Duffel
# ============================================================

DUFFEL_OFFER_REQUESTS_URL = "https://api.duffel.com/air/offer_requests"

def duffel_offer_request(duffel_key: str, origin: str, dest: str, out_date: str, ret_date: str) -> Dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {duffel_key}",
        "Content-Type": "application/json",
        "Duffel-Version": "v2",
    }
    body = {
        "data": {
            "slices": [
                {"origin": origin, "destination": dest, "departure_date": out_date},
                {"origin": dest, "destination": origin, "departure_date": ret_date},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": "economy",
        }
    }
    r = requests.post(DUFFEL_OFFER_REQUESTS_URL, headers=headers, json=body, timeout=45)
    try:
        j = r.json()
    except Exception:
        j = {"_raw": r.text}

    if r.status_code >= 400:
        raise RuntimeError(f"Duffel error {r.status_code}: {j}")
    return j


def pick_best_offers(offers: List[Dict[str, Any]], max_n: int) -> List[Dict[str, Any]]:
    def price(o: Dict[str, Any]) -> float:
        try:
            return float(o.get("total_amount") or 1e9)
        except Exception:
            return 1e9
    return sorted(offers, key=price)[:max_n]


def offer_stops(offer: Dict[str, Any]) -> int:
    # outbound segments - 1
    try:
        slices = offer.get("slices") or []
        if not slices:
            return 0
        segs = slices[0].get("segments") or []
        return max(0, len(segs) - 1)
    except Exception:
        return 0


# ============================================================
# Sheet write (header-mapped)
# ============================================================

def safe_set(row: List[str], h: Dict[str, int], col: str, val: str) -> None:
    if col not in h:
        return
    i = h[col]
    if i >= len(row):
        row.extend([""] * (i - len(row) + 1))
    row[i] = val


def best_effort_city(signals: Dict[str, Dict[str, str]], iata: str) -> str:
    rec = signals.get((iata or "").upper(), {})
    return (rec.get("city") or "").strip()

def best_effort_country(signals: Dict[str, Dict[str, str]], iata: str) -> str:
    rec = signals.get((iata or "").upper(), {})
    return (rec.get("country") or "").strip()


# ============================================================
# Main
# ============================================================

def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    duffel_key = env_str("DUFFEL_API_KEY")

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")
    if not duffel_key:
        raise RuntimeError("Missing DUFFEL_API_KEY")
    if not (env_str("GCP_SA_JSON_ONE_LINE") or env_str("GCP_SA_JSON")):
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE")

    routes_per_run = env_int("DUFFEL_ROUTES_PER_RUN", 3)
    max_searches = env_int("DUFFEL_MAX_SEARCHES_PER_RUN", 4)
    max_inserts = env_int("DUFFEL_MAX_INSERTS", 3)
    days_ahead_min = env_int("DAYS_AHEAD_MIN", 30)
    trip_len_days = env_int("TRIP_LEN_DAYS", 5)
    lookback_hours = env_int("VARIETY_LOOKBACK_HOURS", 120)
    theme = env_str("THEME", "DEFAULT")

    gc = get_gspread_client()
    spread = gc.open_by_key(spreadsheet_id)
    ws = spread.worksheet(tab)

    # Headers
    data = ws_all(ws)
    if not data:
        raise RuntimeError(f"{tab} is empty (must have a header row).")
    headers = [c.strip() for c in data[0]]
    h = {name: i for i, name in enumerate(headers)}

    # Existing rows for dedupe/variety (best-effort)
    existing_rows: List[Dict[str, str]] = []
    for r in data[1:]:
        rowd = {}
        for name, idx in h.items():
            rowd[name] = (r[idx] if idx < len(r) else "")
        existing_rows.append(rowd)

    signals = load_signals_lookup(spread)
    routes = pick_routes(spread)

    # Single date pair for this run (simple + deterministic)
    out_date = (utcnow().date() + dt.timedelta(days=days_ahead_min)).isoformat()
    ret_date = (utcnow().date() + dt.timedelta(days=days_ahead_min + trip_len_days)).isoformat()

    searches = 0
    inserted = 0

    for (origin, dest) in routes[:routes_per_run]:
        if searches >= max_searches or inserted >= max_inserts:
            break

        # Variety guard: skip if we’ve seen this route recently
        if recently_seen(existing_rows, origin, dest, lookback_hours):
            log(f"⏭️  Variety skip: {origin}->{dest} (seen within {lookback_hours}h)")
            continue

        searches += 1
        log(f"Duffel: Searching {origin}->{dest} {out_date}/{ret_date} (theme={theme})")

        try:
            j = duffel_offer_request(duffel_key, origin, dest, out_date, ret_date)
        except Exception as e:
            log(f"❌ Duffel error: {e}")
            continue

        offers = (((j or {}).get("data") or {}).get("offers") or [])
        if not offers:
            log("No offers returned.")
            continue

        # Insert cheapest offers, up to remaining allowance
        remaining = max(0, max_inserts - inserted)
        chosen = pick_best_offers(offers, max_n=min(remaining, 3))

        for offer in chosen:
            if inserted >= max_inserts:
                break

            currency = (offer.get("total_currency") or "").upper()
            if currency and currency != "GBP":
                continue

            try:
                price = float(offer.get("total_amount"))
            except Exception:
                continue

            price_gbp = f"{price:.2f}"
            stops = str(offer_stops(offer))

            # Create deterministic deal_id
            deal_id = deal_hash(origin, dest, out_date, ret_date, price_gbp)

            # Build row aligned to RAW_DEALS headers
            row_out = [""] * len(headers)

            origin_city = best_effort_city(signals, origin) or origin
            dest_city = best_effort_city(signals, dest) or dest
            dest_country = best_effort_country(signals, dest) or ""

            safe_set(row_out, h, "status", "NEW")
            safe_set(row_out, h, "deal_id", deal_id)
            safe_set(row_out, h, "price_gbp", price_gbp)
            safe_set(row_out, h, "origin_iata", origin)
            safe_set(row_out, h, "destination_iata", dest)
            safe_set(row_out, h, "origin_city", origin_city)
            safe_set(row_out, h, "destination_city", dest_city)
            safe_set(row_out, h, "destination_country", dest_country)
            safe_set(row_out, h, "outbound_date", out_date)
            safe_set(row_out, h, "return_date", ret_date)
            safe_set(row_out, h, "stops", stops)
            safe_set(row_out, h, "deal_theme", theme)

            # Links (link_router will upgrade booking_link_vip where eligible)
            deep_link = (offer.get("deep_link") or "").strip()
            safe_set(row_out, h, "affiliate_url", deep_link)
            safe_set(row_out, h, "booking_link_vip", "")
            safe_set(row_out, h, "affiliate_source", "skyscanner")

            # Optional timestamp columns if you have them
            if "scanned_at" in h:
                safe_set(row_out, h, "scanned_at", ts())
            if "created_at" in h:
                safe_set(row_out, h, "created_at", ts())

            # Append row
            ws.append_row(row_out, value_input_option="USER_ENTERED")
            inserted += 1
            log(f"✅ Inserted NEW: {deal_id} {origin}->{dest} £{price_gbp} (inserted={inserted})")

            time.sleep(0.35)

    log(f"Done. searches={searches} inserted={inserted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
