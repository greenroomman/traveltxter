#!/usr/bin/env python3
"""
TravelTxter V4.5x — pipeline_worker.py (FEEDER, config-driven)

Reads control plane tabs:
- CONFIG
- CONFIG_ORIGIN_POOLS
- THEMES
- CONFIG_SIGNALS
- CONFIG_CARRIER_BIAS
- MVP_RULES
Writes:
- RAW_DEALS (append NEW rows)
- DUFFEL_SEARCH_LOG (append one row per Duffel search)

Does NOT score, render, publish.

Env required:
- SPREADSHEET_ID
- GCP_SA_JSON_ONE_LINE (or GCP_SA_JSON)
- RAW_DEALS_TAB (default RAW_DEALS)
- DUFFEL_API_KEY

Env optional:
- RUN_SLOT (AM/PM)   (used only for logging)
- DUFFEL_ROUTES_PER_RUN (default 3)
- DUFFEL_MAX_SEARCHES_PER_RUN (default 4)
- DUFFEL_MAX_INSERTS (default 3)
- THEME (override theme rotation)
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

from lib.sheet_config import (
    load_config_bundle,
    pick_theme_for_today,
    active_config_routes,
    origins_for_today,
    theme_destinations,
    iata_signal_maps,
    mvp_hard_limits,
)

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
# Sheets auth
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

def ensure_columns(ws: gspread.Worksheet, headers: List[str], required: List[str]) -> List[str]:
    missing = [c for c in required if c not in headers]
    if not missing:
        return headers
    new_headers = headers + missing
    ws.update([new_headers], "A1")
    log(f"🛠️  Added missing columns to header: {missing}")
    return new_headers

# ============================================================
# Duffel
# ============================================================

DUFFEL_OFFER_REQUESTS_URL = "https://api.duffel.com/air/offer_requests"

def duffel_offer_request(duffel_key: str, origin: str, dest: str, out_date: str, ret_date: str, max_connections: int = 1) -> Dict[str, Any]:
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
            "max_connections": max_connections,
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

def offer_stops(offer: Dict[str, Any]) -> int:
    try:
        slices = offer.get("slices") or []
        segs = (slices[0].get("segments") or []) if slices else []
        return max(0, len(segs) - 1)
    except Exception:
        return 0

def offer_primary_carrier(offer: Dict[str, Any]) -> str:
    """
    Best-effort: returns marketing carrier IATA code for first segment of first slice.
    """
    try:
        slices = offer.get("slices") or []
        segs = (slices[0].get("segments") or []) if slices else []
        if not segs:
            return ""
        carrier = (segs[0].get("marketing_carrier") or segs[0].get("operating_carrier") or {})
        return str(carrier.get("iata_code") or "").strip().upper()
    except Exception:
        return ""

def offer_city_country(offer: Dict[str, Any]) -> Tuple[str, str]:
    """
    Best-effort: destination city + country from Duffel objects if present.
    """
    try:
        sl = (offer.get("slices") or [{}])[0]
        dest = sl.get("destination") or {}
        city = str(dest.get("city_name") or dest.get("city") or "").strip()
        country = str(dest.get("country_name") or dest.get("country") or "").strip()
        return city, country
    except Exception:
        return "", ""

def pick_best_offers(offers: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    def price(o: Dict[str, Any]) -> float:
        try:
            return float(o.get("total_amount") or 1e9)
        except Exception:
            return 1e9
    return sorted(offers, key=price)[:limit]

# ============================================================
# Dedupe key
# ============================================================

def deal_hash(origin: str, dest: str, out_date: str, ret_date: str, price_gbp: str) -> str:
    raw = f"{origin}|{dest}|{out_date}|{ret_date}|{price_gbp}".encode("utf-8")
    return hashlib.sha1(raw).hexdigest()[:12]

# ============================================================
# Main
# ============================================================

def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    duffel_key = env_str("DUFFEL_API_KEY")
    run_slot = env_str("RUN_SLOT", "AM").upper()

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")
    if not duffel_key:
        raise RuntimeError("Missing DUFFEL_API_KEY")

    routes_per_run = env_int("DUFFEL_ROUTES_PER_RUN", 3)
    max_searches = env_int("DUFFEL_MAX_SEARCHES_PER_RUN", 4)
    max_inserts = env_int("DUFFEL_MAX_INSERTS", 3)
    theme_override = env_str("THEME", "")

    gc = get_gspread_client()
    sh = gc.open_by_key(spreadsheet_id)

    # Load control plane
    cfg = load_config_bundle(sh)
    theme = pick_theme_for_today(cfg.themes, override=theme_override)
    limits = mvp_hard_limits(cfg.mvp_rules)
    sig_city, sig_country = iata_signal_maps(cfg.signals)

    # Get worksheets
    ws_raw = sh.worksheet(tab)

    # Ensure RAW_DEALS has required columns
    raw_values = ws_raw.get_all_values()
    if not raw_values:
        raise RuntimeError("RAW_DEALS must have a header row.")

    headers = [h.strip() for h in raw_values[0]]
    required_cols = [
        "status", "deal_id", "price_gbp",
        "origin_iata", "destination_iata",
        "origin_city", "destination_city", "destination_country",
        "outbound_date", "return_date", "stops",
        "deal_theme",
        "affiliate_url", "booking_link_vip", "affiliate_source",
        "created_utc",
    ]
    headers = ensure_columns(ws_raw, headers, required_cols)

    # Re-read after header update
    raw_values = ws_raw.get_all_values()
    headers = [h.strip() for h in raw_values[0]]
    h = {name: i for i, name in enumerate(headers)}

    # Search log tab (must exist in sheet)
    try:
        ws_log = sh.worksheet("DUFFEL_SEARCH_LOG")
    except Exception:
        ws_log = None

    # Build route list: prefer CONFIG routes for theme, else build from THEMES + ORIGIN_POOLS
    config_routes = active_config_routes(cfg.config_routes, theme)
    routes: List[Dict[str, Any]] = []

    if config_routes:
        routes = config_routes[: max(1, routes_per_run)]
        log(f"🎛️  Using CONFIG routes for theme={theme} count={len(routes)} (RUN_SLOT={run_slot})")
    else:
        origins = origins_for_today(cfg.origin_pools) or ["LGW", "LHR", "MAN", "BRS"]
        dests = theme_destinations(cfg.themes, theme, limit=25)
        if not dests:
            dests = ["TFS", "ACE", "LPA"]  # safe fallback
        # Create small grid
        for o in origins[:3]:
            for d in dests[:10]:
                routes.append({
                    "enabled": "TRUE",
                    "priority": "1",
                    "theme": theme,
                    "origin_iata": o,
                    "destination_iata": d,
                    "days_ahead_min": "30",
                    "days_ahead_max": "45",
                    "trip_length_days": "5",
                    "max_connections": "1",
                    "included_airlines": "",
                    "cabin_class": "economy",
                })
        routes = routes[: max(1, routes_per_run)]
        log(f"🧩 Built routes from THEMES+ORIGIN_POOLS for theme={theme} count={len(routes)} (RUN_SLOT={run_slot})")

    searches = 0
    inserted = 0

    for rconf in routes:
        if searches >= max_searches or inserted >= max_inserts:
            break

        origin = str(rconf.get("origin_iata","")).strip().upper()
        dest = str(rconf.get("destination_iata","")).strip().upper()
        if len(origin) != 3 or len(dest) != 3:
            continue

        days_min = int(float(rconf.get("days_ahead_min","30") or 30))
        days_max = int(float(rconf.get("days_ahead_max","45") or 45))
        trip_len = int(float(rconf.get("trip_length_days","5") or 5))
        max_conn = int(float(rconf.get("max_connections","1") or 1))

        # Deterministic date pick: min bound (keeps it simple)
        out_date = (utcnow().date() + dt.timedelta(days=days_min)).isoformat()
        ret_date = (utcnow().date() + dt.timedelta(days=days_min + trip_len)).isoformat()

        dedupe_key = f"{origin}-{dest}-{out_date}-{ret_date}-{theme}"

        searches += 1
        log(f"🔎 Duffel search {searches}/{max_searches}: {origin}->{dest} {out_date}/{ret_date} theme={theme}")

        offers_count = 0
        search_ok = False

        try:
            j = duffel_offer_request(duffel_key, origin, dest, out_date, ret_date, max_connections=max_conn)
            offers = (((j or {}).get("data") or {}).get("offers") or [])
            offers_count = len(offers)
            search_ok = True
        except Exception as e:
            offers = []
            log(f"❌ Duffel error: {e}")

        # Log search (accounting)
        if ws_log is not None:
            # Ensure header exists
            vals = ws_log.get_all_values()
            if vals:
                log_headers = [x.strip() for x in vals[0]]
                needed = ["ts_utc","origin","dest","outbound_date","return_date","theme","dedupe_key","skipped_dedupe","search_ok","offers_count"]
                missing = [c for c in needed if c not in log_headers]
                if missing:
                    ws_log.update([log_headers + missing], "A1")
                row = ["" for _ in range(len(log_headers) + len(missing))]
                # rebuild header map after potential expansion
                vals2 = ws_log.get_all_values()
                log_headers2 = [x.strip() for x in vals2[0]]
                lh = {name:i for i,name in enumerate(log_headers2)}
                def setv(col, v):
                    i = lh[col]
                    row[i] = str(v)
                setv("ts_utc", ts())
                setv("origin", origin)
                setv("dest", dest)
                setv("outbound_date", out_date)
                setv("return_date", ret_date)
                setv("theme", theme)
                setv("dedupe_key", dedupe_key)
                setv("skipped_dedupe", "FALSE")
                setv("search_ok", "TRUE" if search_ok else "FALSE")
                setv("offers_count", str(offers_count))
                ws_log.append_row(row, value_input_option="USER_ENTERED")

        if not offers:
            continue

        remaining = max(0, max_inserts - inserted)
        chosen = pick_best_offers(offers, limit=min(remaining, 3))

        for offer in chosen:
            if inserted >= max_inserts:
                break

            cur = str(offer.get("total_currency") or "").upper()
            if cur and cur != "GBP":
                continue

            try:
                price = float(offer.get("total_amount"))
            except Exception:
                continue

            stops = offer_stops(offer)

            # MVP hard rules (enforced here, at ingestion)
            if price < float(limits["min_price_gbp"]) or price > float(limits["max_price_gbp"]):
                continue
            if stops > int(limits["max_stops"]):
                continue

            price_gbp = f"{price:.2f}"
            deal_id = deal_hash(origin, dest, out_date, ret_date, price_gbp)

            # City/country enrichment: Duffel first, then CONFIG_SIGNALS fallback
            dest_city, dest_country = offer_city_country(offer)
            if not dest_city:
                dest_city = sig_city.get(dest, dest)
            if not dest_country:
                dest_country = sig_country.get(dest, "")

            origin_city = origin  # safe default
            # If we have a signal city for origin (rare), use it. Otherwise keep IATA (you can add origin map later)
            origin_city = sig_city.get(origin, origin_city)

            # Link (Duffel deep_link if present)
            deep_link = str(offer.get("deep_link") or "").strip()

            row_out = [""] * len(headers)
            def setc(col: str, val: str):
                if col not in h:
                    return
                i = h[col]
                if i >= len(row_out):
                    row_out.extend([""] * (i - len(row_out) + 1))
                row_out[i] = val

            setc("status", "NEW")
            setc("deal_id", deal_id)
            setc("price_gbp", price_gbp)
            setc("origin_iata", origin)
            setc("destination_iata", dest)
            setc("origin_city", origin_city)
            setc("destination_city", dest_city)
            setc("destination_country", dest_country)
            setc("outbound_date", out_date)
            setc("return_date", ret_date)
            setc("stops", str(stops))
            setc("deal_theme", theme)
            setc("affiliate_url", deep_link)
            setc("booking_link_vip", "")
            setc("affiliate_source", "skyscanner")
            setc("created_utc", ts())

            ws_raw.append_row(row_out, value_input_option="USER_ENTERED")
            inserted += 1
            log(f"✅ Inserted NEW: {deal_id} {origin}->{dest} £{price_gbp} stops={stops} theme={theme} (inserted={inserted})")
            time.sleep(0.35)

    log(f"Done. theme={theme} searches={searches} inserted={inserted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
