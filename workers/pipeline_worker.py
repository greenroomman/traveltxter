# TRAVELTXTTER V5 — FEEDER (MIN CONFIG, VOLUME-FIRST)
# Purpose: Insert fresh inventory into RAW_DEALS
# CONFIG headers: enabled, destination_iata, theme, weight
# OPS_MASTER!B2 defines theme of day
# RAW_DEALS is the ONLY writable database

from __future__ import annotations

import datetime as dt
import json
import os
import random
import time
from typing import Any, Dict, List, Tuple, Optional

import gspread
import requests
from google.oauth2.service_account import Credentials


# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


# ------------------------------------------------------------------------------
# Environment helpers
# ------------------------------------------------------------------------------

def env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v)

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

def truthy(v: Any) -> bool:
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


# ------------------------------------------------------------------------------
# Google auth (robust against broken secrets)
# ------------------------------------------------------------------------------

def _parse_sa_json(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()
    if not raw:
        raise RuntimeError("Missing GCP service account JSON")

    # attempt 1
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    # attempt 2: escaped newlines
    try:
        return json.loads(raw.replace("\\n", "\n"))
    except json.JSONDecodeError:
        pass

    # attempt 3: literal newlines inside strings
    try:
        return json.loads(raw.replace("\n", "\\n"))
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Cannot parse GCP_SA_JSON: {e}") from e


def gspread_client() -> gspread.Client:
    raw = env("GCP_SA_JSON_ONE_LINE") or env("GCP_SA_JSON")
    info = _parse_sa_json(raw)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


# ------------------------------------------------------------------------------
# Sheet helpers
# ------------------------------------------------------------------------------

def open_sheet(gc: gspread.Client) -> gspread.Spreadsheet:
    sid = env("SPREADSHEET_ID") or env("SHEET_ID")
    if not sid:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")
    return gc.open_by_key(sid)

def open_ws(sh: gspread.Spreadsheet, name: str) -> gspread.Worksheet:
    if not name:
        raise RuntimeError("Worksheet name empty")
    try:
        return sh.worksheet(name)
    except Exception as e:
        raise RuntimeError(f"WorksheetNotFound: {name}") from e

def get_all_records(ws: gspread.Worksheet) -> List[Dict[str, Any]]:
    values = ws.get_all_values()
    if len(values) < 2:
        return []
    headers = [h.strip() for h in values[0]]
    out: List[Dict[str, Any]] = []
    for row in values[1:]:
        if not any(c.strip() for c in row):
            continue
        d: Dict[str, Any] = {}
        for i, h in enumerate(headers):
            if h:
                d[h] = row[i] if i < len(row) else ""
        out.append(d)
    return out

def header_map(headers: List[str]) -> Dict[str, int]:
    m: Dict[str, int] = {}
    for i, h in enumerate(headers):
        h = h.strip()
        if h and h not in m:
            m[h] = i
    return m


# ------------------------------------------------------------------------------
# Time helpers
# ------------------------------------------------------------------------------

def now_utc() -> dt.datetime:
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)

def iso(ts: dt.datetime) -> str:
    return ts.replace(microsecond=0).isoformat().replace("+00:00", "Z")


# ------------------------------------------------------------------------------
# Theme policy (VARIABLES only)
# ------------------------------------------------------------------------------

def read_theme(ws_ops: gspread.Worksheet) -> str:
    v = ws_ops.acell("B2").value
    return (v or "DEFAULT").strip()

def origins_for_theme(theme: str) -> List[str]:
    raw = env(f"ORIGINS_{theme.upper()}") or env("ORIGINS_DEFAULT", "LHR,LGW,MAN")
    return [x.strip().upper() for x in raw.split(",") if x.strip()]

def max_stops(theme: str) -> int:
    return env_int(f"MAX_STOPS_{theme.upper()}", env_int("MAX_STOPS_DEFAULT", 1))

def trip_bounds(theme: str) -> Tuple[int, int]:
    mn = env_int(f"TRIP_{theme.upper()}_MIN", env_int("TRIP_DEFAULT_MIN", 4))
    mx = env_int(f"TRIP_{theme.upper()}_MAX", env_int("TRIP_DEFAULT_MAX", 10))
    return mn, max(mx, mn)

def window_bounds(theme: str) -> Tuple[int, int]:
    mn = env_int(f"WINDOW_{theme.upper()}_MIN", env_int("WINDOW_DEFAULT_MIN", 21))
    mx = env_int(f"WINDOW_{theme.upper()}_MAX", env_int("WINDOW_DEFAULT_MAX", 84))
    return mn, max(mx, mn)


# ------------------------------------------------------------------------------
# Duffel API
# ------------------------------------------------------------------------------

DUFFEL_URL = "https://api.duffel.com/air/offer_requests"

def duffel_headers() -> Dict[str, str]:
    key = env("DUFFEL_API_KEY")
    if not key:
        raise RuntimeError("Missing DUFFEL_API_KEY")
    return {
        "Authorization": f"Bearer {key}",
        "Duffel-Version": "v2",
        "Content-Type": "application/json",
    }

def duffel_search(
    origin: str,
    dest: str,
    depart: dt.date,
    ret: dt.date,
    cabin: str,
    max_conn: int,
) -> Optional[Dict[str, Any]]:
    payload = {
        "data": {
            "slices": [
                {"origin": origin, "destination": dest, "departure_date": depart.isoformat()},
                {"origin": dest, "destination": origin, "departure_date": ret.isoformat()},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": cabin,
            "max_connections": max_conn,
        }
    }

    r = requests.post(DUFFEL_URL, headers=duffel_headers(), json=payload, timeout=45)
    if r.status_code >= 400:
        return None

    offers = (r.json().get("data") or {}).get("offers") or []
    if not offers:
        return None

    offers.sort(key=lambda o: float(o.get("total_amount", 1e18)))
    return offers[0]


def extract_stops_and_carriers(offer: Dict[str, Any]) -> Tuple[int, str]:
    stops = 0
    carriers = set()
    for sl in offer.get("slices", []):
        segs = sl.get("segments", [])
        stops = max(stops, max(0, len(segs) - 1))
        for s in segs:
            code = (s.get("marketing_carrier") or {}).get("iata_code")
            if code:
                carriers.add(code)
    return stops, ",".join(sorted(carriers))


def extract_cabin_and_bags(offer: Dict[str, Any]) -> Tuple[str, str]:
    cabin = offer.get("cabin_class") or "economy"
    bags = ""
    try:
        bags = str(offer.get("baggages", ""))
    except Exception:
        pass
    return cabin, bags


# ------------------------------------------------------------------------------
# RAW_DEALS requirements
# ------------------------------------------------------------------------------

REQUIRED_HEADERS = [
    "deal_id","origin_iata","destination_iata",
    "origin_city","destination_city","destination_country",
    "outbound_date","return_date","price_gbp","currency",
    "stops","cabin_class","carriers","theme","status",
    "publish_window","score","phrase_used","graphic_url",
    "booking_link_vip","posted_vip_at","posted_free_at",
    "posted_instagram_at","ingested_at_utc",
    "phrase_category","scored_timestamp"
]


# ------------------------------------------------------------------------------
# Dedupe (theme-aware)
# ------------------------------------------------------------------------------

def load_dedupe(ws, hmap: Dict[str,int]) -> set:
    vals = ws.get_all_values()
    if len(vals) < 2:
        return set()

    rows = vals[1:]
    s = set()

    def v(col, row):
        i = hmap.get(col)
        return (row[i] if i is not None and i < len(row) else "").strip()

    for r in rows:
        o = v("origin_iata", r).upper()
        d = v("destination_iata", r).upper()
        od = v("outbound_date", r)
        rd = v("return_date", r)
        t = v("theme", r)
        if o and d and od and rd and t:
            s.add((o, d, od, rd, t))
    return s


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------

def main() -> int:
    log("======================================================================")
    log("TRAVELTXTTER V5 — FEEDER START (MIN CONFIG, VOLUME)")
    log("======================================================================")

    RAW_TAB = env("RAW_DEALS_TAB", "RAW_DEALS")
    CFG_TAB = env("FEEDER_CONFIG_TAB", "CONFIG")
    OPS_TAB = env("OPS_MASTER_TAB", "OPS_MASTER")

    MAX_SEARCHES = env_int("DUFFEL_MAX_SEARCHES_PER_RUN", 12)
    MAX_INSERTS = env_int("DUFFEL_MAX_INSERTS", 50)
    ROUTES_PER_RUN = env_int("DUFFEL_ROUTES_PER_RUN", 4)
    SLEEP = env_float("FEEDER_SLEEP_SECONDS", 0.1)

    gc = gspread_client()
    sh = open_sheet(gc)

    ws_raw = open_ws(sh, RAW_TAB)
    ws_cfg = open_ws(sh, CFG_TAB)
    ws_ops = open_ws(sh, OPS_TAB)

    headers = ws_raw.get_all_values()[0]
    hmap = header_map(headers)
    missing = [h for h in REQUIRED_HEADERS if h not in hmap]
    if missing:
        raise RuntimeError(f"RAW_DEALS missing headers: {missing}")

    theme = read_theme(ws_ops)
    log(f"🎯 Theme of day: {theme}")

    dedupe = load_dedupe(ws_raw, hmap)

    cfg = [r for r in get_all_records(ws_cfg) if truthy(r.get("enabled"))]
    cfg = [r for r in cfg if str(r.get("theme","")).lower() == theme.lower()]

    if not cfg:
        log("⚠️ No CONFIG routes eligible for theme.")
        return 0

    cfg.sort(key=lambda r: float(r.get("weight", 0)), reverse=True)
    dests = []
    for r in cfg:
        d = r.get("destination_iata","").upper()
        if d and d not in dests:
            dests.append(d)
        if len(dests) >= ROUTES_PER_RUN:
            break

    origins = origins_for_theme(theme)
    max_conn = max_stops(theme)
    wmin, wmax = window_bounds(theme)
    tmin, tmax = trip_bounds(theme)

    rows_to_insert = []
    searches = 0
    no_offer = 0

    seed = int(dt.date.today().strftime("%Y%m%d"))

    for dest in dests:
        for origin in origins:
            if searches >= MAX_SEARCHES or len(rows_to_insert) >= MAX_INSERTS:
                break

            rng = random.Random(seed + searches * 31)
            depart = dt.date.today() + dt.timedelta(days=rng.randint(wmin, wmax))
            ret = depart + dt.timedelta(days=rng.randint(tmin, tmax))

            key = (origin, dest, depart.isoformat(), ret.isoformat(), theme)
            if key in dedupe:
                continue

            log(f"🔎 Search {searches+1}/{MAX_SEARCHES} {origin}→{dest}")
            searches += 1

            offer = duffel_search(origin, dest, depart, ret, "economy", max_conn)
            time.sleep(SLEEP)

            if not offer:
                no_offer += 1
                continue

            if (offer.get("total_currency") or "").upper() != "GBP":
                continue

            price = float(offer.get("total_amount", 0))
            stops, carriers = extract_stops_and_carriers(offer)
            cabin, bags = extract_cabin_and_bags(offer)

            row = [""] * len(headers)
            def setv(c,v):
                i=hmap.get(c)
                if i is not None: row[i]=v

            setv("deal_id", f"{origin}_{dest}_{depart:%Y%m%d}_{ret:%Y%m%d}")
            setv("origin_iata", origin)
            setv("destination_iata", dest)
            setv("outbound_date", depart.isoformat())
            setv("return_date", ret.isoformat())
            setv("price_gbp", round(price,2))
            setv("currency", "GBP")
            setv("stops", stops)
            setv("cabin_class", cabin)
            setv("carriers", carriers)
            setv("theme", theme)
            setv("status", "NEW")
            setv("ingested_at_utc", iso(now_utc()))

            rows_to_insert.append(row)
            dedupe.add(key)

    if not rows_to_insert:
        log("⚠️ No rows inserted.")
        log(f"SKIPS: no_offer={no_offer}")
        return 0

    ws_raw.append_rows(rows_to_insert, value_input_option="RAW")
    log(f"✅ Inserted {len(rows_to_insert)} row(s) into RAW_DEALS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
