from __future__ import annotations

import datetime as dt
import json
import os
import random
import time
from typing import Any, Dict, List, Tuple

import gspread
import requests
from google.oauth2.service_account import Credentials


# ==============================================================================
# Logging
# ==============================================================================

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


# ==============================================================================
# Env helpers
# ==============================================================================

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


# ==============================================================================
# Google auth
# ==============================================================================

def _parse_sa_json(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()
    if not raw:
        raise RuntimeError("Missing GCP service account JSON")

    for attempt in (raw, raw.replace("\\n", "\n"), raw.replace("\n", "\\n")):
        try:
            return json.loads(attempt)
        except json.JSONDecodeError:
            continue

    raise RuntimeError("Cannot parse GCP_SA_JSON")


def gspread_client() -> gspread.Client:
    raw = env("GCP_SA_JSON_ONE_LINE") or env("GCP_SA_JSON")
    info = _parse_sa_json(raw)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


# ==============================================================================
# Sheet helpers
# ==============================================================================

def open_sheet(gc: gspread.Client) -> gspread.Spreadsheet:
    sid = env("SPREADSHEET_ID") or env("SHEET_ID")
    if not sid:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")
    return gc.open_by_key(sid)

def open_ws(sh: gspread.Spreadsheet, name: str) -> gspread.Worksheet:
    try:
        return sh.worksheet(name)
    except Exception as e:
        raise RuntimeError(f"WorksheetNotFound: {name}") from e

def header_map(headers: List[str]) -> Dict[str, int]:
    return {h: i for i, h in enumerate(headers) if h}


# ==============================================================================
# Time helpers
# ==============================================================================

def now_utc() -> dt.datetime:
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)

def iso(ts: dt.datetime) -> str:
    return ts.replace(microsecond=0).isoformat().replace("+00:00", "Z")


# ==============================================================================
# Theme + policy (VARIABLES ONLY)
# ==============================================================================

def read_theme(ws_ops: gspread.Worksheet) -> str:
    return (ws_ops.acell("B2").value or "DEFAULT").strip()

def origins() -> List[str]:
    raw = env("ORIGINS_DEFAULT", "LHR,LGW,MAN")
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


# ==============================================================================
# Duffel
# ==============================================================================

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

def duffel_search(origin, dest, depart, ret, max_conn):
    payload = {
        "data": {
            "slices": [
                {"origin": origin, "destination": dest, "departure_date": depart.isoformat()},
                {"origin": dest, "destination": origin, "departure_date": ret.isoformat()},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": "economy",
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


# ==============================================================================
# Offer extractors (RESTORED)
# ==============================================================================

def extract_carriers(offer: Dict[str, Any]) -> str:
    carriers = set()
    for s in offer.get("slices", []):
        for seg in s.get("segments", []):
            mc = seg.get("marketing_carrier", {})
            if mc.get("iata_code"):
                carriers.add(mc["iata_code"])
    return ",".join(sorted(carriers))

def extract_stops(offer: Dict[str, Any]) -> int:
    # Max stops across slices
    max_stops = 0
    for s in offer.get("slices", []):
        segs = s.get("segments", [])
        max_stops = max(max_stops, max(0, len(segs) - 1))
    return max_stops

def extract_bags_included(offer: Dict[str, Any]) -> str:
    services = offer.get("available_services")
    if not isinstance(services, list):
        return ""
    for svc in services:
        if svc.get("type") == "baggage":
            return "YES"
    return ""


# ==============================================================================
# Main
# ==============================================================================

def main() -> int:
    log("======================================================================")
    log("TRAVELTXTTER V5 — FEEDER START (MIN CONFIG, VOLUME)")
    log("======================================================================")

    RAW_TAB = env("RAW_DEALS_TAB", "RAW_DEALS")
    CFG_TAB = env("FEEDER_CONFIG_TAB", "CONFIG")
    OPS_TAB = env("OPS_MASTER_TAB", "OPS_MASTER")

    MAX_SEARCHES = env_int("DUFFEL_MAX_SEARCHES_PER_RUN", 12)
    ROUTES_PER_RUN = env_int("DUFFEL_ROUTES_PER_RUN", 4)
    SLEEP = env_float("FEEDER_SLEEP_SECONDS", 0.1)

    gc = gspread_client()
    sh = open_sheet(gc)

    ws_raw = open_ws(sh, RAW_TAB)
    ws_cfg = open_ws(sh, CFG_TAB)
    ws_ops = open_ws(sh, OPS_TAB)

    headers = ws_raw.get_all_values()[0]
    hmap = header_map(headers)

    theme = read_theme(ws_ops)
    log(f"🎯 Theme of day: {theme}")

    cfg = [
        r for r in ws_cfg.get_all_records()
        if truthy(r.get("enabled"))
        and str(r.get("theme","")).lower() == theme.lower()
    ]

    if not cfg:
        log("⚠️ No CONFIG routes eligible for theme.")
        return 0

    cfg.sort(key=lambda r: float(r.get("weight", 0)), reverse=True)
    dests = [r["destination_iata"].upper() for r in cfg[:ROUTES_PER_RUN]]

    origins_list = origins()
    wmin, wmax = window_bounds(theme)
    tmin, tmax = trip_bounds(theme)
    max_conn = max_stops(theme)

    rows = []
    searches = 0
    seed = int(dt.date.today().strftime("%Y%m%d"))

    for dest in dests:
        for origin in origins_list:
            if searches >= MAX_SEARCHES:
                break

            rng = random.Random(seed + searches * 37)
            depart = dt.date.today() + dt.timedelta(days=rng.randint(wmin, wmax))
            ret = depart + dt.timedelta(days=rng.randint(tmin, tmax))

            log(f"🔎 Search {searches+1}/{MAX_SEARCHES} {origin}→{dest}")
            searches += 1

            offer = duffel_search(origin, dest, depart, ret, max_conn)
            time.sleep(SLEEP)

            if not offer:
                continue

            if (offer.get("total_currency") or "").upper() != "GBP":
                continue

            row = [""] * len(headers)
            def setv(c,v):
                i=hmap.get(c)
                if i is not None: row[i]=v

            setv("deal_id", f"{origin}_{dest}_{depart:%Y%m%d}_{ret:%Y%m%d}")
            setv("origin_iata", origin)
            setv("destination_iata", dest)
            setv("outbound_date", depart.isoformat())
            setv("return_date", ret.isoformat())
            setv("price_gbp", round(float(offer["total_amount"]),2))
            setv("currency", "GBP")
            setv("theme", theme)
            setv("status", "NEW")
            setv("carriers", extract_carriers(offer))
            setv("stops", extract_stops(offer))
            setv("bags_incl", extract_bags_included(offer))
            setv("ingested_at_utc", iso(now_utc()))

            rows.append(row)

    if not rows:
        log("⚠️ No rows inserted.")
        return 0

    ws_raw.append_rows(rows, value_input_option="RAW")
    log(f"✅ Inserted {len(rows)} row(s) into RAW_DEALS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
