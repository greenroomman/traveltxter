# workers/pipeline_worker.py
# FULL FILE REPLACEMENT
#
# TravelTxter Feeder (destination-first, CONFIG-only)
# - Reads: CONFIG, ROUTE_CAPABILITY_MAP (geo enrichment)
# - Writes: RAW_DEALS (status=NEW) with real offer data only
# - Never inserts "stub" rows without deal_id/price/dates.
#
# ENV (required):
#   SPREADSHEET_ID, GCP_SA_JSON_ONE_LINE (or GCP_SA_JSON), DUFFEL_API_KEY
#
# ENV (optional / existing knobs):
#   RAW_DEALS_TAB (default RAW_DEALS)
#   CONFIG_TAB (default CONFIG)
#   CAPABILITY_TAB (default ROUTE_CAPABILITY_MAP)
#   DUFFEL_MAX_SEARCHES_PER_RUN (default 12)
#   DUFFEL_ROUTES_PER_RUN (default 6)
#   DUFFEL_MAX_INSERTS (default 20)
#   FEEDER_SLEEP_SECONDS (default 0.1)
#   DATE_JITTER_DAYS (default 2)
#   THEME (optional override)
#   PRICE_GATE_FALLBACK_BEHAVIOR (ALLOW|BLOCK) (default ALLOW)
#
# Theme origin pools (optional):
#   ORIGINS_<THEME> (comma-separated IATA), ORIGINS_DEFAULT fallback

import os
import json
import time
import hashlib
import datetime as dt
from typing import Dict, Any, List, Tuple, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials


# ------------------ env helpers ------------------

def env(k: str, d: str = "") -> str:
    return (os.getenv(k, d) or "").strip()


def env_int(k: str, d: int) -> int:
    v = env(k, "")
    try:
        return int(v)
    except Exception:
        return d


def truthy(v) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in ("true", "1", "yes", "y", "t")


def must_env(k: str) -> str:
    v = env(k)
    if not v:
        raise RuntimeError(f"Missing required env var: {k}")
    return v


# ------------------ auth / sheets ------------------

def sa_creds():
    raw = env("GCP_SA_JSON_ONE_LINE") or env("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
    try:
        info = json.loads(raw)
    except json.JSONDecodeError:
        info = json.loads(raw.replace("\\n", "\n"))

    return Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )


def open_sheet():
    gc = gspread.authorize(sa_creds())
    sh = gc.open_by_key(must_env("SPREADSHEET_ID"))
    return sh


# ------------------ logging ------------------

def log(msg: str):
    print(f"{dt.datetime.utcnow().isoformat()}Z | {msg}")


# ------------------ capability map ------------------

def load_capability_map(sh) -> Dict[Tuple[str, str], Dict[str, str]]:
    tab = env("CAPABILITY_TAB", "ROUTE_CAPABILITY_MAP")
    ws = sh.worksheet(tab)
    rows = ws.get_all_records()

    # "enabled" may be TRUE/"TRUE"/True; treat truthy as enabled.
    enabled_rows = [r for r in rows if truthy(r.get("enabled"))]
    log(f"✅ ROUTE_CAPABILITY_MAP loaded: {len(enabled_rows)} enabled routes")

    m = {}
    for r in enabled_rows:
        o = (r.get("origin_iata") or "").strip().upper()
        d = (r.get("destination_iata") or "").strip().upper()
        if not o or not d:
            continue
        m[(o, d)] = {
            "origin_city": (r.get("origin_city") or "").strip(),
            "origin_country": (r.get("origin_country") or "").strip(),
            "destination_city": (r.get("destination_city") or "").strip(),
            "destination_country": (r.get("destination_country") or "").strip(),
        }
    return m


# ------------------ config (CONFIG-only) ------------------

def load_active_config(sh) -> List[Dict[str, Any]]:
    tab = env("CONFIG_TAB", "CONFIG")
    ws = sh.worksheet(tab)
    rows = ws.get_all_records()

    active = [r for r in rows if truthy(r.get("active_in_feeder"))]
    log(f"✅ CONFIG loaded: {len(active)} active routes (of {len(rows)} total)")

    # normalize key fields
    out = []
    for r in active:
        o = (r.get("origin_iata") or "").strip().upper()
        d = (r.get("destination_iata") or "").strip().upper()
        theme = (r.get("theme") or "").strip()
        theme_of_day = (r.get("theme_of_day") or "").strip()
        if not o or not d:
            continue
        out.append({**r,
                    "origin_iata": o,
                    "destination_iata": d,
                    "theme": theme,
                    "theme_of_day": theme_of_day})
    return out


def pick_theme_of_day(config_rows: List[Dict[str, Any]]) -> str:
    # Priority:
    # 1) explicit env THEME
    # 2) rotate deterministically across available theme_of_day values by UTC day index
    t = env("THEME", "")
    if t:
        return t

    themes = sorted({(r.get("theme_of_day") or "").strip() for r in config_rows if (r.get("theme_of_day") or "").strip()})
    if not themes:
        return "default"

    # deterministic rotation based on date (UTC) so it’s stable across runs that day
    today = dt.datetime.utcnow().date()
    idx = (today.toordinal() % len(themes))
    return themes[idx]


def origins_for_theme(theme: str) -> List[str]:
    key = f"ORIGINS_{theme.upper()}"
    v = env(key, "")
    if not v:
        v = env("ORIGINS_DEFAULT", "")
    origins = [x.strip().upper() for x in v.split(",") if x.strip()]
    # safety fallback
    if not origins:
        origins = ["LHR", "LGW", "MAN", "BRS", "STN"]
    return origins


# ------------------ duffel search ------------------

DUFFEL_BASE = "https://api.duffel.com"


def duffel_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {must_env('DUFFEL_API_KEY')}",
        "Duffel-Version": "v2",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def create_offer_request(origin: str, dest: str, out_date: str, ret_date: str, max_connections: int) -> Optional[Dict[str, Any]]:
    payload = {
        "data": {
            "slices": [
                {"origin": origin, "destination": dest, "departure_date": out_date},
                {"origin": dest, "destination": origin, "departure_date": ret_date},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": "economy",
            # Duffel has "max_connections" on the request (0 = direct)
            "max_connections": max_connections,
        }
    }

    r = requests.post(f"{DUFFEL_BASE}/air/offer_requests", headers=duffel_headers(), data=json.dumps(payload), timeout=60)
    if not r.ok:
        return None
    return r.json().get("data")


def pick_cheapest_offer(offer_request_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    offers = offer_request_data.get("offers") or []
    if not offers:
        return None

    def to_float(x):
        try:
            return float(x)
        except Exception:
            return 1e18

    offers_sorted = sorted(offers, key=lambda o: to_float(o.get("total_amount")))
    return offers_sorted[0] if offers_sorted else None


def stops_from_offer(offer: Dict[str, Any]) -> int:
    # stops = total segments - slices
    slices = offer.get("slices") or []
    segs = 0
    for s in slices:
        segs += len(s.get("segments") or [])
    # 2 slices (out + back)
    return max(0, (segs - len(slices)))


def parse_dates_from_offer(offer: Dict[str, Any]) -> Tuple[str, str]:
    slices = offer.get("slices") or []
    if len(slices) < 2:
        return ("", "")
    out = (slices[0].get("departure_date") or "").strip()
    ret = (slices[1].get("departure_date") or "").strip()
    return out, ret


# ------------------ deterministic date selection ------------------

def stable_hash_int(s: str) -> int:
    h = hashlib.md5(s.encode("utf-8")).hexdigest()
    return int(h[:8], 16)


def select_base_dates(dest: str, days_ahead_min: int, days_ahead_max: int, trip_len: int) -> Tuple[dt.date, dt.date]:
    today = dt.datetime.utcnow().date()
    span = max(0, days_ahead_max - days_ahead_min)
    offset = days_ahead_min + (stable_hash_int(dest) % (span + 1 if span > 0 else 1))
    out = today + dt.timedelta(days=offset)
    ret = out + dt.timedelta(days=max(1, trip_len))
    return out, ret


# ------------------ RAW_DEALS insert ------------------

def ensure_headers(ws) -> Dict[str, int]:
    headers = ws.row_values(1)
    idx = {h: i for i, h in enumerate(headers)}
    return idx


def write_new_deal(ws, header_idx: Dict[str, int], row: Dict[str, Any]) -> None:
    headers = ws.row_values(1)

    out = [""] * len(headers)
    for k, v in row.items():
        if k not in header_idx:
            continue
        out[header_idx[k]] = v

    ws.append_row(out, value_input_option="RAW")


# ------------------ main ------------------

def main():
    log("=" * 78)
    log("TRAVELTXTTER PIPELINE WORKER (FEEDER) START")
    log("=" * 78)

    sh = open_sheet()

    raw_tab = env("RAW_DEALS_TAB", "RAW_DEALS")
    ws_raw = sh.worksheet(raw_tab)
    header_idx = ensure_headers(ws_raw)

    required_cols = [
        "status",
        "origin_iata",
        "destination_iata",
        "origin_city",
        "destination_city",
        "destination_country",
        "price_gbp",
        "outbound_date",
        "return_date",
        "deal_id",
        "stops",
        "bags_incl",
        "created_utc",
        "deal_theme",
    ]
    for c in required_cols:
        if c not in header_idx:
            raise RuntimeError(f"RAW_DEALS missing required header: {c}")

    config_rows = load_active_config(sh)
    if not config_rows:
        log("⚠️ No active routes in CONFIG (active_in_feeder)")
        return 0

    cap_map = load_capability_map(sh)

    theme_today = pick_theme_of_day(config_rows)
    log(f"🎯 Theme of the day (UTC): {theme_today}")

    # Filter to today's theme_of_day
    todays = [r for r in config_rows if (r.get("theme_of_day") or "").strip() == theme_today]
    if not todays:
        # fallback: use rows where r.theme matches theme_today
        todays = [r for r in config_rows if (r.get("theme") or "").strip() == theme_today]

    if not todays:
        log(f"⚠️ No CONFIG rows match theme_of_day/theme = {theme_today}")
        return 0

    # Destination-first: pick distinct destinations, limited by DUFFEL_ROUTES_PER_RUN
    routes_per_run = env_int("DUFFEL_ROUTES_PER_RUN", 6)
    max_searches = env_int("DUFFEL_MAX_SEARCHES_PER_RUN", 12)
    max_inserts = env_int("DUFFEL_MAX_INSERTS", 20)
    sleep_s = float(env("FEEDER_SLEEP_SECONDS", "0.1") or "0.1")
    jitter_days = env_int("DATE_JITTER_DAYS", 2)

    # Group by destination, keep highest priority rows first
    def prio(r):
        try:
            return int(r.get("priority") or 0)
        except Exception:
            return 0

    todays_sorted = sorted(todays, key=prio, reverse=True)

    by_dest: Dict[str, List[Dict[str, Any]]] = {}
    for r in todays_sorted:
        by_dest.setdefault(r["destination_iata"], []).append(r)

    dests = list(by_dest.keys())[:routes_per_run]
    if not dests:
        log("⚠️ No destinations selected")
        return 0

    inserts = 0
    searches = 0

    for dest in dests:
        # For each destination, search across allowed origins for theme_today (pool),
        # plus small date jitter around a deterministic base date.
        sample_row = by_dest[dest][0]
        days_min = int(sample_row.get("days_ahead_min") or 21)
        days_max = int(sample_row.get("days_ahead_max") or 84)
        trip_len = int(sample_row.get("trip_length_days") or 5)
        max_conn = int(sample_row.get("max_connections") or 1)

        allowed_origins = origins_for_theme(theme_today)

        out_base, ret_base = select_base_dates(dest, days_min, days_max, trip_len)

        best_offer = None
        best_origin = None
        best_out = None
        best_ret = None

        # Search budget-capped across origin × jitter
        for origin in allowed_origins:
            if searches >= max_searches:
                break

            # Skip physically impossible routes if not in capability map
            if (origin, dest) not in cap_map:
                continue

            for j in range(-jitter_days, jitter_days + 1):
                if searches >= max_searches:
                    break

                out_d = out_base + dt.timedelta(days=j)
                ret_d = ret_base + dt.timedelta(days=j)

                out_s = out_d.isoformat()
                ret_s = ret_d.isoformat()

                searches += 1
                req = create_offer_request(origin, dest, out_s, ret_s, max_conn)
                time.sleep(sleep_s)

                if not req:
                    continue

                offer = pick_cheapest_offer(req)
                if not offer:
                    continue

                # Stops sanity (Duffel max_connections is request-level, but we compute anyway)
                st = stops_from_offer(offer)
                if st > max_conn:
                    continue

                if best_offer is None:
                    best_offer = offer
                    best_origin = origin
                    best_out, best_ret = parse_dates_from_offer(offer)
                else:
                    try:
                        if float(offer.get("total_amount")) < float(best_offer.get("total_amount")):
                            best_offer = offer
                            best_origin = origin
                            best_out, best_ret = parse_dates_from_offer(offer)
                    except Exception:
                        pass

        if not best_offer or not best_origin:
            continue

        # Build insert row (REAL deal only)
        enrich = cap_map.get((best_origin, dest), {})
        price_gbp = ""
        try:
            # Duffel returns currency + amount; we assume GBP offers as per your system
            price_gbp = str(int(round(float(best_offer.get("total_amount") or 0))))
        except Exception:
            price_gbp = ""

        deal_id = (best_offer.get("id") or "").strip()
        if not deal_id or not price_gbp or not best_out or not best_ret:
            # Never insert stubs
            continue

        row = {
            "status": "NEW",
            "deal_id": deal_id,
            "price_gbp": price_gbp,
            "origin_city": enrich.get("origin_city", ""),
            "origin_iata": best_origin,
            "destination_country": enrich.get("destination_country", ""),
            "destination_city": enrich.get("destination_city", ""),
            "destination_iata": dest,
            "outbound_date": best_out,
            "return_date": best_ret,
            "stops": str(stops_from_offer(best_offer)),
            "bags_incl": "",  # Duffel offer includes baggage info per segment; left blank unless you want parsing added.
            "created_utc": dt.datetime.utcnow().isoformat() + "Z",
            "deal_theme": (sample_row.get("theme") or theme_today or "default"),
        }

        write_new_deal(ws_raw, header_idx, row)
        inserts += 1
        log(f"✅ Inserted 1 rows into {raw_tab}: {best_origin}->{dest} £{price_gbp} OUT {best_out} BACK {best_ret}")

        if inserts >= max_inserts:
            break

    if inserts == 0:
        log("⚠️ No winners to insert")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

