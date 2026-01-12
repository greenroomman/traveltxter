# workers/pipeline_worker.py
#!/usr/bin/env python3
"""
TravelTxter V4.5x — pipeline_worker.py (LOCKED)

ROLE:
- Reads CONFIG (enabled origins/themes) and THEMES (destination pools)
- Applies ROUTE_CAPABILITY_MAP filtering
- Calls Duffel to discover offers (capped for free tier)
- Inserts NEW rows into RAW_DEALS (facts only)
- NEVER publishes, NEVER renders, NEVER scores beyond supply heuristics

This file is production-locked. Do not refactor unless the pipeline spine is changing.
"""

from __future__ import annotations

import os
import json
import time
import math
import random
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# Theme rotation (LOCKED — must match other workers)
# ============================================================

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


def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


def env_int(k: str, default: int) -> int:
    v = os.getenv(k, "").strip()
    return int(v) if v else int(default)


def env_float(k: str, default: float) -> float:
    v = os.getenv(k, "").strip()
    return float(v) if v else float(default)


def env_str(k: str, default: str = "") -> str:
    v = os.getenv(k, "").strip()
    return v if v else default


def norm(s: str) -> str:
    return (s or "").strip()


def low(s: str) -> str:
    return norm(s).lower().replace(" ", "_")


def today_theme_utc() -> str:
    today = dt.datetime.utcnow().date()
    doy = int(today.strftime("%j"))
    return MASTER_THEMES[doy % len(MASTER_THEMES)]


def parse_sa_json(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return json.loads(raw.replace("\\n", "\n"))


def gs_client() -> gspread.Client:
    raw = env_str("GCP_SA_JSON_ONE_LINE") or env_str("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
    info = parse_sa_json(raw)
    creds = Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


def duffel_headers() -> Dict[str, str]:
    key = env_str("DUFFEL_API_KEY")
    if not key:
        raise RuntimeError("Missing DUFFEL_API_KEY")
    return {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Duffel-Version": "v2",
    }


def duffel_offer_request(origin: str, dest: str, out_date: str, in_date: str, max_connections: int) -> Dict[str, Any]:
    url = "https://api.duffel.com/air/offer_requests"
    payload = {
        "data": {
            "slices": [
                {"origin": origin, "destination": dest, "departure_date": out_date},
                {"origin": dest, "destination": origin, "departure_date": in_date},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": "economy",
            "max_connections": max_connections,
        }
    }
    r = requests.post(url, headers=duffel_headers(), json=payload, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"Duffel offer_requests failed: {r.status_code} {r.text[:400]}")
    return r.json()


def duffel_offers(offer_request_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    url = f"https://api.duffel.com/air/offers?offer_request_id={offer_request_id}&limit={limit}"
    r = requests.get(url, headers=duffel_headers(), timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"Duffel offers fetch failed: {r.status_code} {r.text[:400]}")
    j = r.json()
    return (j.get("data") or [])


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        s = str(x).strip().replace("£", "").replace(",", "")
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def iso_now() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def load_sheet_rows(ws: gspread.Worksheet) -> Tuple[List[str], List[List[str]]]:
    values = ws.get_all_values()
    if not values:
        return [], []
    headers = [str(h).strip() for h in values[0]]
    rows = values[1:]
    return headers, rows


def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID") or env_str("SHEET_ID")
    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")

    raw_tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    max_inserts = env_int("DUFFEL_MAX_INSERTS", 15)
    routes_per_run = env_int("DUFFEL_ROUTES_PER_RUN", 4)
    max_searches = env_int("DUFFEL_MAX_SEARCHES_PER_RUN", 4)
    days_ahead = env_int("DAYS_AHEAD", 60)
    trip_len_min = env_int("TRIP_LEN_MIN", 3)
    trip_len_max = env_int("TRIP_LEN_MAX", 7)
    max_connections = env_int("MAX_CONNECTIONS", 1)

    gc = gs_client()
    sh = gc.open_by_key(spreadsheet_id)

    ws_raw = sh.worksheet(raw_tab)
    ws_themes = sh.worksheet("THEMES")
    ws_cap = sh.worksheet("ROUTE_CAPABILITY_MAP")

    theme_today = today_theme_utc()
    log(f"🎯 Theme of the day (UTC): {theme_today}")

    # Load destination pool for theme_today
    th_headers, th_rows = load_sheet_rows(ws_themes)
    th = {h: i for i, h in enumerate(th_headers)}

    dest_pool: List[Dict[str, str]] = []
    for r in th_rows:
        if low(r[th["theme"]] if th["theme"] < len(r) else "") != theme_today:
            continue
        dest_pool.append({
            "destination_iata": norm(r[th["destination_iata"]] if th["destination_iata"] < len(r) else ""),
            "destination_city": norm(r[th["destination_city"]] if th["destination_city"] < len(r) else ""),
            "destination_country": norm(r[th["destination_country"]] if th["destination_country"] < len(r) else ""),
        })

    if not dest_pool:
        log("⚠️ No destinations found for theme today (THEMES tab). Exiting.")
        return 0

    # Load route capability map (allowed origin/dest)
    cap_headers, cap_rows = load_sheet_rows(ws_cap)
    cap = {h: i for i, h in enumerate(cap_headers)}
    allowed_pairs = set()
    for r in cap_rows:
        o = norm(r[cap.get("origin_iata", 0)] if cap.get("origin_iata", 0) < len(r) else "")
        d = norm(r[cap.get("destination_iata", 1)] if cap.get("destination_iata", 1) < len(r) else "")
        if o and d:
            allowed_pairs.add((o, d))

    # Load CONFIG origins for this theme (simple table: enabled/priority/theme/origin_iata)
    cfg = sh.worksheet("CONFIG")
    cfg_headers, cfg_rows = load_sheet_rows(cfg)
    ci = {h: i for i, h in enumerate(cfg_headers)}
    origins: List[str] = []
    for r in cfg_rows:
        enabled = r[ci["enabled"]] if ci["enabled"] < len(r) else ""
        theme = low(r[ci["theme"]] if ci["theme"] < len(r) else "")
        origin_iata = norm(r[ci["origin_iata"]] if ci["origin_iata"] < len(r) else "")
        if str(enabled).strip().lower() in ("true", "yes", "1") and theme == theme_today and origin_iata:
            origins.append(origin_iata)

    if not origins:
        log("⚠️ No enabled origins for today’s theme in CONFIG. Exiting.")
        return 0

    # Build candidate routes (origin × dest pool) filtered by allowed_pairs
    routes: List[Tuple[str, Dict[str, str]]] = []
    for o in origins:
        for d in dest_pool:
            di = d["destination_iata"]
            if (o, di) in allowed_pairs:
                routes.append((o, d))

    random.shuffle(routes)
    routes = routes[: max(1, routes_per_run)]
    log(f"Built {len(routes)} candidate routes for theme={theme_today}")

    # Prepare RAW_DEALS headers
    raw_values = ws_raw.get_all_values()
    if not raw_values:
        raise RuntimeError("RAW_DEALS has no headers.")
    raw_headers = [str(h).strip() for h in raw_values[0]]
    rh = {h: i for i, h in enumerate(raw_headers)}

    required_cols = [
        "status","deal_id","price_gbp","origin_city","origin_iata","destination_country","destination_city","destination_iata",
        "outbound_date","return_date","stops","deal_theme","created_utc","affiliate_url","currency","carriers","deeplink","theme",
        "banked_utc","reason_banked",
    ]
    missing = [c for c in required_cols if c not in rh]
    if missing:
        raise RuntimeError(f"RAW_DEALS missing required columns: {missing}")

    inserts: List[List[Any]] = []
    searches_used = 0

    for origin_iata, dest in routes:
        if searches_used >= max_searches:
            break

        # choose dates
        out = (dt.datetime.utcnow().date() + dt.timedelta(days=random.randint(14, days_ahead))).strftime("%Y-%m-%d")
        trip_len = random.randint(trip_len_min, trip_len_max)
        inn = (dt.datetime.strptime(out, "%Y-%m-%d").date() + dt.timedelta(days=trip_len)).strftime("%Y-%m-%d")

        log(f"Duffel: Searching {origin_iata}->{dest['destination_iata']} {out}/{inn}")
        searches_used += 1

        try:
            req = duffel_offer_request(origin_iata, dest["destination_iata"], out, inn, max_connections=max_connections)
            offer_request_id = req["data"]["id"]
            offers = duffel_offers(offer_request_id, limit=50)
        except Exception as e:
            log(f"❌ Duffel error: {e}")
            continue

        # Insert top offers (capped)
        for off in offers:
            if len(inserts) >= max_inserts:
                break

            total = safe_float(off.get("total_amount"))
            if total is None:
                continue

            carriers = []
            try:
                carriers = [s.get("marketing_carrier", {}).get("name", "") for s in (off.get("slices") or [])]
            except Exception:
                carriers = []

            stops = 0
            try:
                # crude: if any slice has >1 segment, count stops
                for s in (off.get("slices") or []):
                    segs = s.get("segments") or []
                    if len(segs) > 1:
                        stops = max(stops, len(segs) - 1)
            except Exception:
                stops = 0

            deal_id = str(off.get("id") or f"{origin_iata}-{dest['destination_iata']}-{out}-{inn}-{total}")
            row = [""] * len(raw_headers)

            def setv(col: str, val: Any) -> None:
                row[rh[col]] = val

            setv("status", "NEW")
            setv("deal_id", deal_id)
            setv("price_gbp", round(float(total), 2))
            setv("origin_city", "")  # optional; downstream can enrich
            setv("origin_iata", origin_iata)
            setv("destination_country", dest["destination_country"])
            setv("destination_city", dest["destination_city"])
            setv("destination_iata", dest["destination_iata"])
            setv("outbound_date", out)
            setv("return_date", inn)
            setv("stops", int(stops))
            setv("deal_theme", theme_today)
            setv("created_utc", iso_now())
            setv("currency", "GBP")
            setv("carriers", ", ".join([c for c in carriers if c]))
            setv("deeplink", "")  # link_router may fill later
            setv("theme", theme_today)  # compatibility
            setv("banked_utc", "")
            setv("reason_banked", "")

            inserts.append(row)

        if len(inserts) >= max_inserts:
            break

    if not inserts:
        log("No inserts.")
        return 0

    # Append to RAW_DEALS
    ws_raw.append_rows(inserts, value_input_option="USER_ENTERED")
    log(f"✅ Inserted {len(inserts)} NEW rows into RAW_DEALS. Searches used: {searches_used}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
