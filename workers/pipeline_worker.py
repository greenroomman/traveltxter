#!/usr/bin/env python3
"""
TravelTxter V4.5x — pipeline_worker.py (LOCKED FEEDER)

ROLE:
- ONLY ingests (Duffel) and writes NEW rows into RAW_DEALS
- NEVER scores
- NEVER renders
- NEVER publishes

Reads these tabs (if present):
- CONFIG
- CONFIG_ORIGIN_POOLS
- THEMES
- CONFIG_SIGNALS
- CONFIG_CARRIER_BIAS (optional)

Writes:
- RAW_DEALS (canonical columns)

Canonical columns written (must match the handover):
status, deal_id, price_gbp, origin_iata, destination_iata, origin_city, destination_city,
destination_country, outbound_date, return_date, stops, deal_theme,
deal_score, dest_variety_score, theme_variety_score,
graphic_url, rendered_timestamp, render_error, render_response_snippet,
posted_instagram_at, posted_telegram_vip_at, posted_telegram_free_at,
affiliate_url, booking_link_vip, affiliate_source

Notes:
- Hardened service-account JSON parsing (fixes secret formatting issues)
- Duffel-Version: v2
- Inserts up to DUFFEL_MAX_INSERTS per run
- Searches up to DUFFEL_MAX_SEARCHES_PER_RUN per run
"""

from __future__ import annotations

import os
import json
import time
import random
import datetime as dt
from typing import Dict, Any, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError


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
# Env helpers
# ============================================================

def env_str(k: str, default: str = "") -> str:
    return os.environ.get(k, default).strip()

def env_int(k: str, default: int) -> int:
    try:
        return int(env_str(k, str(default)))
    except Exception:
        return default


# ============================================================
# Robust SA JSON parsing (fixes messy GitHub secrets)
# ============================================================

def _extract_json_object(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()

    # Fast path
    try:
        return json.loads(raw)
    except Exception:
        pass

    # Repair escaped newlines
    try:
        return json.loads(raw.replace("\\n", "\n"))
    except Exception:
        pass

    # Extract first {...}
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise RuntimeError("Invalid GCP_SA_JSON_ONE_LINE: no JSON object found")

    candidate = raw[start:end + 1]

    try:
        return json.loads(candidate)
    except Exception:
        pass

    try:
        return json.loads(candidate.replace("\\n", "\n"))
    except Exception as e:
        raise RuntimeError("Invalid GCP_SA_JSON_ONE_LINE: JSON parse failed") from e


def get_gspread_client() -> gspread.Client:
    sa = env_str("GCP_SA_JSON_ONE_LINE") or env_str("GCP_SA_JSON")
    if not sa:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
    info = _extract_json_object(sa)
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds)


def open_sheet_with_backoff(gc: gspread.Client, spreadsheet_id: str, attempts: int = 8) -> gspread.Spreadsheet:
    delay = 4.0
    for i in range(1, attempts + 1):
        try:
            return gc.open_by_key(spreadsheet_id)
        except APIError as e:
            msg = str(e)
            if "429" in msg or "Quota exceeded" in msg:
                log(f"⏳ Sheets quota (429). Retry {i}/{attempts} in {int(delay)}s...")
                time.sleep(delay)
                delay = min(delay * 1.6, 45.0)
                continue
            raise
    raise RuntimeError("Sheets quota still exceeded after retries (429). Try again shortly.")


# ============================================================
# A1 helpers
# ============================================================

def col_letter(n1: int) -> str:
    s = ""
    n = n1
    while n:
        n, rr = divmod(n - 1, 26)
        s = chr(65 + rr) + s
    return s

def a1(rownum: int, col0: int) -> str:
    return f"{col_letter(col0 + 1)}{rownum}"


# ============================================================
# Sheet helpers
# ============================================================

def safe_get(row: List[str], idx: int) -> str:
    return row[idx].strip() if 0 <= idx < len(row) else ""

def ensure_columns(ws: gspread.Worksheet, headers: List[str], required: List[str]) -> List[str]:
    missing = [c for c in required if c not in headers]
    if not missing:
        return headers
    ws.update([headers + missing], "A1")
    log(f"🛠️  Added missing columns: {missing}")
    return headers + missing

def get_ws_optional(sh: gspread.Spreadsheet, title: str) -> Optional[gspread.Worksheet]:
    try:
        return sh.worksheet(title)
    except Exception:
        return None

def read_table(ws: Optional[gspread.Worksheet]) -> Tuple[List[str], List[List[str]]]:
    if ws is None:
        return [], []
    vals = ws.get_all_values()
    if not vals:
        return [], []
    headers = [h.strip() for h in vals[0]]
    rows = vals[1:]
    return headers, rows

def as_bool(s: str) -> bool:
    return (s or "").strip().lower() in {"1", "true", "yes", "y", "on"}

def as_bool_default_true(s: str) -> bool:
    """Treat blank/empty as TRUE (sheet-friendly)."""
    if (s or "").strip() == "":
        return True
    return as_bool(s)

def norm_theme(s: str) -> str:
    """Normalise theme strings so CITY BREAK / City-Break / city_break => CITY_BREAK."""
    s = (s or "").strip().upper()
    if not s:
        return ""
    s = s.replace("-", " ")
    s = "_".join([p for p in s.split() if p])
    while "__" in s:
        s = s.replace("__", "_")
    return s


# ============================================================
# Config loading (flexible headers)
# ============================================================

def load_signals_map(sh: gspread.Spreadsheet) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Returns:
      city_by_iata, country_by_iata
    Accepts flexible header names:
      iata / IATA / destination_iata / iata_hint
      city / city_name / destination_city
      country / country_name / destination_country
    """
    ws = get_ws_optional(sh, "CONFIG_SIGNALS")
    h, rows = read_table(ws)
    if not h:
        return {}, {}

    idx = {name.strip(): i for i, name in enumerate(h)}

    def find(*names: str) -> Optional[int]:
        for n in names:
            for k, _v in idx.items():
                if k.lower() == n.lower():
                    return idx[k]
        return None

    i_iata = find("iata", "IATA", "destination_iata", "iata_hint")
    i_city = find("city", "city_name", "destination_city")
    i_country = find("country", "country_name", "destination_country")

    city_by: Dict[str, str] = {}
    country_by: Dict[str, str] = {}

    for r in rows:
        iata = safe_get(r, i_iata) if i_iata is not None else ""
        if not iata:
            continue
        key = iata.strip().upper()
        if i_city is not None:
            city_by[key] = safe_get(r, i_city)
        if i_country is not None:
            country_by[key] = safe_get(r, i_country)

    return city_by, country_by


def pick_theme(sh: gspread.Spreadsheet) -> str:
    override = env_str("THEME")
    if override:
        return norm_theme(override) or override

    ws = get_ws_optional(sh, "THEMES")
    h, rows = read_table(ws)
    if not h or not rows:
        return "CITY_BREAK"

    idx = {name.strip().lower(): i for i, name in enumerate(h)}

    i_name = idx.get("theme") or idx.get("name") or idx.get("theme_name")
    i_active = idx.get("active") or idx.get("is_active") or idx.get("enabled")

    themes: List[str] = []
    for r in rows:
        name = safe_get(r, i_name) if i_name is not None else ""
        if not name:
            continue
        if i_active is not None:
            # Blank active/enabled means TRUE (sheet-friendly)
            if not as_bool_default_true(safe_get(r, i_active)):
                continue
        themes.append(norm_theme(name.strip()) or name.strip())

    if not themes:
        return "CITY_BREAK"

    day = int(utcnow().strftime("%j"))
    return themes[day % len(themes)]


def load_routes(sh: gspread.Spreadsheet, theme: str) -> List[Tuple[str, str]]:
    """
    Prefer CONFIG routes if present. Flexible headers:
      origin_iata / origin
      destination_iata / destination / dest
      active / enabled (blank treated as TRUE)
      theme / deal_theme (optional; blanks treated as "all themes")
    """
    ws = get_ws_optional(sh, "CONFIG")
    h, rows = read_table(ws)
    if not h or not rows:
        return []

    idx = {name.strip().lower(): i for i, name in enumerate(h)}

    def col(*names: str) -> Optional[int]:
        for n in names:
            if n.lower() in idx:
                return idx[n.lower()]
        return None

    i_origin = col("origin_iata", "origin")
    i_dest = col("destination_iata", "destination", "dest")
    i_active = col("active", "enabled", "is_active")
    i_theme = col("theme", "deal_theme")

    theme_n = norm_theme(theme)
    routes: List[Tuple[str, str]] = []
    for r in rows:
        if i_active is not None and not as_bool_default_true(safe_get(r, i_active)):
            continue
        if i_theme is not None:
            t_raw = safe_get(r, i_theme).strip()
            t = norm_theme(t_raw)
            if t and t != theme_n:
                continue

        o = safe_get(r, i_origin).strip().upper() if i_origin is not None else ""
        d = safe_get(r, i_dest).strip().upper() if i_dest is not None else ""
        if o and d:
            routes.append((o, d))

    return routes


def load_origin_pool(sh: gspread.Spreadsheet) -> List[str]:
    """
    Flexible headers:
      origin_iata / iata / origin
      active / enabled (blank treated as TRUE)
    """
    ws = get_ws_optional(sh, "CONFIG_ORIGIN_POOLS")
    h, rows = read_table(ws)
    if not h or not rows:
        return ["LON", "MAN"]  # safe default

    idx = {name.strip().lower(): i for i, name in enumerate(h)}
    i_iata = idx.get("origin_iata") or idx.get("iata") or idx.get("origin")
    i_active = idx.get("active") or idx.get("enabled") or idx.get("is_active")

    out: List[str] = []
    for r in rows:
        if i_active is not None and not as_bool_default_true(safe_get(r, i_active)):
            continue
        iata = safe_get(r, i_iata).strip().upper() if i_iata is not None else ""
        if iata:
            out.append(iata)

    return out or ["LON", "MAN"]


# ============================================================
# Duffel search (v2 header)
# ============================================================

DUFFEL_API_BASE = "https://api.duffel.com"

def duffel_headers(api_key: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {api_key}",
        "Duffel-Version": "v2",
        "Content-Type": "application/json",
    }

def duffel_search_return(
    api_key: str,
    origin: str,
    dest: str,
    out_date: str,
    ret_date: str,
    cabin: str = "economy",
    max_connections: int = 1,
) -> List[Dict[str, Any]]:
    url = f"{DUFFEL_API_BASE}/air/offer_requests"
    payload = {
        "data": {
            "slices": [
                {"origin": origin, "destination": dest, "departure_date": out_date},
                {"origin": dest, "destination": origin, "departure_date": ret_date},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": cabin,
            "max_connections": max_connections,
        }
    }

    r = requests.post(url, headers=duffel_headers(api_key), json=payload, timeout=40)
    if r.status_code >= 300:
        raise RuntimeError(f"Duffel offer_requests failed: {r.status_code} {r.text[:400]}")

    offer_request_id = r.json().get("data", {}).get("id")
    if not offer_request_id:
        raise RuntimeError("Duffel offer_requests missing data.id")

    url2 = f"{DUFFEL_API_BASE}/air/offers"
    params = {"offer_request_id": offer_request_id, "limit": 50}
    r2 = requests.get(url2, headers=duffel_headers(api_key), params=params, timeout=40)
    if r2.status_code >= 300:
        raise RuntimeError(f"Duffel offers failed: {r2.status_code} {r2.text[:400]}")

    return r2.json().get("data", [])


def offer_price_gbp(offer: Dict[str, Any]) -> Optional[float]:
    try:
        amt = float(offer.get("total_amount", ""))
        cur = str(offer.get("total_currency", "")).upper()
        if cur != "GBP":
            return None
        return amt
    except Exception:
        return None


def offer_stops(offer: Dict[str, Any]) -> int:
    try:
        slices = offer.get("slices", [])
        if not slices:
            return 0
        segs = slices[0].get("segments", [])
        return max(0, len(segs) - 1)
    except Exception:
        return 0


# ============================================================
# Date helpers
# ============================================================

def pick_dates() -> Tuple[str, str]:
    base = utcnow().date()
    out = base + dt.timedelta(days=random.randint(21, 50))
    ret = out + dt.timedelta(days=random.randint(3, 7))
    return out.strftime("%Y-%m-%d"), ret.strftime("%Y-%m-%d")


# ============================================================
# Main
# ============================================================

def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    raw_tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    api_key = env_str("DUFFEL_API_KEY")

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")
    if not api_key:
        raise RuntimeError("Missing DUFFEL_API_KEY")

    ROUTES_PER_RUN = env_int("DUFFEL_ROUTES_PER_RUN", 3)
    MAX_SEARCHES = env_int("DUFFEL_MAX_SEARCHES_PER_RUN", 4)
    MAX_INSERTS = env_int("DUFFEL_MAX_INSERTS", 3)

    gc = get_gspread_client()
    sh = open_sheet_with_backoff(gc, spreadsheet_id)
    raw_ws = sh.worksheet(raw_tab)

    raw_vals = raw_ws.get_all_values()
    headers = [h.strip() for h in raw_vals[0]] if raw_vals else []

    required_cols = [
        "status",
        "deal_id",
        "price_gbp",
        "origin_iata",
        "destination_iata",
        "origin_city",
        "destination_city",
        "destination_country",
        "outbound_date",
        "return_date",
        "stops",
        "deal_theme",
        "deal_score",
        "dest_variety_score",
        "theme_variety_score",
        "graphic_url",
        "rendered_timestamp",
        "render_error",
        "render_response_snippet",
        "posted_instagram_at",
        "posted_telegram_vip_at",
        "posted_telegram_free_at",
        "affiliate_url",
        "booking_link_vip",
        "affiliate_source",
    ]

    if not headers:
        raw_ws.update([required_cols], "A1")
        headers = required_cols[:]
        log("🛠️  Created RAW_DEALS header row")
    else:
        headers = ensure_columns(raw_ws, headers, required_cols)

    h = {name: i for i, name in enumerate(headers)}

    theme = pick_theme(sh)
    origins = load_origin_pool(sh)
    city_by_iata, country_by_iata = load_signals_map(sh)

    routes = load_routes(sh, theme)

    # NEVER starve: if CONFIG routes are filtered out, synthesize from origins + signals.
    if not routes:
        if not origins:
            origins = ["LON", "MAN", "BRS", "EDI", "GLA"]

        dests = [d for d in list(city_by_iata.keys()) if len(d) == 3]
        random.shuffle(dests)

        if not dests:
            dests = ["BCN", "AGP", "FCO", "LIS", "AMS", "CDG", "DUB", "PRG", "BUD", "KRK"]

        dests = dests[: max(10, ROUTES_PER_RUN * 4)]

        for o in origins:
            for d in dests:
                if o != d:
                    routes.append((o, d))
        random.shuffle(routes)

    random.shuffle(routes)
    routes = routes[: max(1, ROUTES_PER_RUN)]

    if not routes:
        routes = [("LON", "BCN")]

    log(f"🎯 Theme selected: {theme}")
    log(f"🧭 Routes this run: {len(routes)} (cap {ROUTES_PER_RUN})")

    inserted = 0
    searches = 0

    for origin, dest in routes:
        if inserted >= MAX_INSERTS or searches >= MAX_SEARCHES:
            break

        out_date, ret_date = pick_dates()

        log(f"✈️  Duffel search: {origin}->{dest} {out_date}/{ret_date}")
        searches += 1

        try:
            offers = duffel_search_return(
                api_key=api_key,
                origin=origin,
                dest=dest,
                out_date=out_date,
                ret_date=ret_date,
                cabin="economy",
                max_connections=1,
            )
        except Exception as e:
            log(f"❌ Duffel error: {e}")
            continue

        best_offer = None
        best_price = None
        for off in offers:
            p = offer_price_gbp(off)
            if p is None:
                continue
            if best_price is None or p < best_price:
                best_price = p
                best_offer = off

        if best_offer is None or best_price is None:
            log("⏭️  No GBP offers found")
            continue

        deal_id = f"{origin}-{dest}-{out_date}-{ret_date}-{int(utcnow().timestamp())}"

        origin_city = city_by_iata.get(origin, origin)
        dest_city = city_by_iata.get(dest, dest)
        dest_country = country_by_iata.get(dest, "")

        stops = offer_stops(best_offer)

        affiliate_url = f"https://www.skyscanner.net/transport/flights/{origin}/{dest}/{out_date.replace('-','')}/{ret_date.replace('-','')}/"

        row = [""] * len(headers)
        row[h["status"]] = "NEW"
        row[h["deal_id"]] = deal_id
        row[h["price_gbp"]] = str(round(best_price, 2))
        row[h["origin_iata"]] = origin
        row[h["destination_iata"]] = dest
        row[h["origin_city"]] = origin_city
        row[h["destination_city"]] = dest_city
        row[h["destination_country"]] = dest_country
        row[h["outbound_date"]] = out_date
        row[h["return_date"]] = ret_date
        row[h["stops"]] = str(stops)
        row[h["deal_theme"]] = theme

        row[h["affiliate_url"]] = affiliate_url
        row[h["affiliate_source"]] = "skyscanner_fallback"

        raw_ws.append_row(row, value_input_option="USER_ENTERED")
        inserted += 1
        log(f"✅ Inserted NEW: {origin}->{dest} £{best_price:.2f} (deal_id={deal_id})")

        time.sleep(1)

    log(f"Done. searches={searches} inserted={inserted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
