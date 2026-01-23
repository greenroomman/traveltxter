# workers/pipeline_worker.py
# FULL FILE REPLACEMENT
#
# TravelTxter Feeder (CONFIG-only brain)
# - Reads: CONFIG (merged), ROUTE_CAPABILITY_MAP (enrichment)
# - Writes: RAW_DEALS using the EXACT RD schema you supplied
# - Inserts ONLY real Duffel offers (never inserts stub rows)
#
# REQUIRED ENV:
#   SPREADSHEET_ID (or SHEET_ID), GCP_SA_JSON_ONE_LINE (or GCP_SA_JSON), DUFFEL_API_KEY
#
# OPTIONAL ENV (defaults shown):
#   RAW_DEALS_TAB=RAW_DEALS
#   CONFIG_TAB=CONFIG
#   RCM_TAB=ROUTE_CAPABILITY_MAP
#   DUFFEL_ROUTES_PER_RUN=6
#   DUFFEL_MAX_SEARCHES_PER_RUN=12
#   DUFFEL_MAX_INSERTS=20
#   FEEDER_SLEEP_SECONDS=0.15
#   DATE_JITTER_DAYS=2
#   CABIN_CLASS=economy
#   CURRENCY=GBP
#   ORIGINS_DEFAULT="LHR,LGW,MAN,BRS,STN"
#   ORIGINS_<THEME>="..."
#   THEME=""  (optional override for theme_of_day)

import os
import json
import time
import hashlib
import datetime as dt
from typing import Dict, Any, List, Tuple, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials


# ------------------ ENV helpers ------------------

def env(k: str, d: str = "") -> str:
    return (os.getenv(k, d) or "").strip()

def env_int(k: str, d: int) -> int:
    try:
        return int(env(k, str(d)))
    except Exception:
        return d

def env_float(k: str, d: float) -> float:
    try:
        return float(env(k, str(d)))
    except Exception:
        return d

def must_env(k: str) -> str:
    v = env(k)
    if not v:
        raise RuntimeError(f"Missing required env var: {k}")
    return v

def truthy(v) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in ("true", "1", "yes", "y", "t")

def now_utc_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def log(msg: str):
    print(f"{dt.datetime.utcnow().isoformat()}Z | {msg}")


# ------------------ Sheets auth ------------------

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
    sid = env("SPREADSHEET_ID") or env("SHEET_ID")
    if not sid:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")
    return gc.open_by_key(sid)


# ------------------ Duffel API (v2) ------------------

DUFFEL_BASE = "https://api.duffel.com"

def duffel_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {must_env('DUFFEL_API_KEY')}",
        "Duffel-Version": "v2",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def duffel_create_offer_request(origin: str, dest: str, out_date: str, ret_date: str,
                               max_connections: int, cabin_class: str, currency: str) -> Optional[Dict[str, Any]]:
    payload = {
        "data": {
            "slices": [
                {"origin": origin, "destination": dest, "departure_date": out_date},
                {"origin": dest, "destination": origin, "departure_date": ret_date},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": cabin_class,
            "max_connections": max_connections,
            "return_offers": True,
            "currency": currency,
        }
    }
    r = requests.post(
        f"{DUFFEL_BASE}/air/offer_requests",
        headers=duffel_headers(),
        data=json.dumps(payload),
        timeout=60,
    )
    if not r.ok:
        return None
    return r.json().get("data")

def duffel_list_offers(offer_request_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    # Fallback if return_offers isn't present in the create response.
    r = requests.get(
        f"{DUFFEL_BASE}/air/offers",
        headers=duffel_headers(),
        params={"offer_request_id": offer_request_id, "limit": limit},
        timeout=60,
    )
    if not r.ok:
        return []
    return (r.json().get("data") or [])

def pick_cheapest_offer(offers: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not offers:
        return None
    def to_float(o):
        try:
            return float(o.get("total_amount"))
        except Exception:
            return 1e18
    offers_sorted = sorted(offers, key=to_float)
    return offers_sorted[0] if offers_sorted else None

def offer_stops(offer: Dict[str, Any]) -> int:
    slices = offer.get("slices") or []
    segs = 0
    for s in slices:
        segs += len(s.get("segments") or [])
    return max(0, segs - len(slices))

def offer_dates(offer: Dict[str, Any]) -> Tuple[str, str]:
    slices = offer.get("slices") or []
    if len(slices) < 2:
        return "", ""
    out = (slices[0].get("departure_date") or "").strip()
    ret = (slices[1].get("departure_date") or "").strip()
    return out, ret

def offer_durations_minutes(offer: Dict[str, Any]) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    # outbound_duration_minutes, inbound_duration_minutes, total_duration_minutes
    slices = offer.get("slices") or []
    out_m = None
    in_m = None
    try:
        if len(slices) >= 1:
            out_m = int(slices[0].get("duration") or 0)
        if len(slices) >= 2:
            in_m = int(slices[1].get("duration") or 0)
    except Exception:
        out_m, in_m = None, None
    tot = (out_m + in_m) if (out_m is not None and in_m is not None) else None
    return out_m, in_m, tot

def offer_carriers(offer: Dict[str, Any]) -> str:
    # Collect unique marketing carrier names/codes
    carriers = []
    for sl in (offer.get("slices") or []):
        for seg in (sl.get("segments") or []):
            mk = seg.get("marketing_carrier") or {}
            name = (mk.get("name") or "").strip()
            iata = (mk.get("iata_code") or "").strip()
            label = name or iata
            if label and label not in carriers:
                carriers.append(label)
    return ", ".join(carriers)


# ------------------ Deterministic dates ------------------

def stable_int(s: str) -> int:
    h = hashlib.md5(s.encode("utf-8")).hexdigest()
    return int(h[:8], 16)

def select_dates(origin: str, dest: str, days_min: int, days_max: int, trip_len: int) -> Tuple[dt.date, dt.date]:
    today = dt.datetime.utcnow().date()
    span = max(0, days_max - days_min)
    seed = f"{origin}-{dest}-{today.isoformat()}"
    offset = days_min + (stable_int(seed) % (span + 1 if span > 0 else 1))
    out = today + dt.timedelta(days=offset)
    ret = out + dt.timedelta(days=max(1, trip_len))
    return out, ret


# ------------------ Read CONFIG & RCM ------------------

def load_config(sh) -> List[Dict[str, Any]]:
    tab = env("CONFIG_TAB", "CONFIG")
    ws = sh.worksheet(tab)
    rows = ws.get_all_records()
    active = [r for r in rows if truthy(r.get("active_in_feeder"))]
    log(f"✅ CONFIG loaded: {len(active)} active rows (of {len(rows)} total)")
    # normalize
    out = []
    for r in active:
        o = (r.get("origin_iata") or "").strip().upper()
        d = (r.get("destination_iata") or "").strip().upper()
        if not o or not d:
            continue
        out.append({**r, "origin_iata": o, "destination_iata": d})
    return out

def pick_theme_of_day(config_rows: List[Dict[str, Any]]) -> str:
    forced = env("THEME", "")
    if forced:
        return forced
    themes = sorted({(r.get("theme_of_day") or "").strip() for r in config_rows if (r.get("theme_of_day") or "").strip()})
    if not themes:
        return "default"
    today = dt.datetime.utcnow().date()
    return themes[today.toordinal() % len(themes)]

def origins_for_theme(theme: str) -> List[str]:
    v = env(f"ORIGINS_{theme.upper()}", "")
    if not v:
        v = env("ORIGINS_DEFAULT", "LHR,LGW,MAN,BRS,STN")
    return [x.strip().upper() for x in v.split(",") if x.strip()]

def load_rcm(sh) -> Dict[Tuple[str, str], Dict[str, str]]:
    tab = env("RCM_TAB", "ROUTE_CAPABILITY_MAP")
    ws = sh.worksheet(tab)
    rows = ws.get_all_records()
    enabled = [r for r in rows if truthy(r.get("enabled")) or str(r.get("enabled") or "").strip().upper() == "TRUE"]
    log(f"✅ ROUTE_CAPABILITY_MAP loaded: {len(enabled)} enabled routes")
    m: Dict[Tuple[str, str], Dict[str, str]] = {}
    for r in enabled:
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


# ------------------ RAW_DEALS write ------------------

def get_headers(ws) -> List[str]:
    return ws.row_values(1)

def header_index(headers: List[str]) -> Dict[str, int]:
    return {h: i for i, h in enumerate(headers)}

def require_headers(idx: Dict[str, int], required: List[str], sheet_name: str):
    for c in required:
        if c not in idx:
            raise RuntimeError(f"{sheet_name} missing required header: {c}")

def fetch_existing_deal_ids(ws, idx: Dict[str, int], max_rows: int = 2000) -> set:
    # To avoid duplicate inserts; cheap scan of deal_id column
    deal_col = idx.get("deal_id")
    if deal_col is None:
        return set()
    col_vals = ws.col_values(deal_col + 1)
    # drop header
    col_vals = col_vals[1:max_rows]
    return {v.strip() for v in col_vals if v and v.strip()}

def append_row_by_headers(ws, headers: List[str], row_map: Dict[str, Any]):
    row = [""] * len(headers)
    for k, v in row_map.items():
        if k in row_map and k in headers:
            pass
    idx = header_index(headers)
    for k, v in row_map.items():
        if k not in idx:
            continue
        row[idx[k]] = "" if v is None else str(v)
    ws.append_row(row, value_input_option="RAW")


# ------------------ Build RD schema row ------------------

def build_rd_row(schema_defaults: Dict[str, Any],
                 offer: Dict[str, Any],
                 origin_iata: str,
                 dest_iata: str,
                 theme: str,
                 theme_of_day: str,
                 enrich: Dict[str, str],
                 cabin_class: str,
                 currency: str) -> Dict[str, Any]:

    out_date, ret_date = offer_dates(offer)
    st = offer_stops(offer)

    # Duffel amount is already in chosen currency; store in price_gbp as rounded integer string (per your renderer expectations)
    price_val = ""
    try:
        price_val = str(int(round(float(offer.get("total_amount") or 0))))
    except Exception:
        price_val = ""

    out_m, in_m, tot_m = offer_durations_minutes(offer)
    total_hours = ""
    if tot_m is not None:
        try:
            total_hours = f"{(tot_m / 60.0):.1f}"
        except Exception:
            total_hours = ""

    connection_type = "direct" if st == 0 else ("1_stop" if st == 1 else "multi_stop")
    carriers = offer_carriers(offer)

    row = dict(schema_defaults)

    row.update({
        "status": "NEW",
        "deal_id": (offer.get("id") or "").strip(),
        "price_gbp": price_val,
        "origin_city": enrich.get("origin_city", ""),
        "origin_iata": origin_iata,
        "origin_country": enrich.get("origin_country", ""),
        "destination_country": enrich.get("destination_country", ""),
        "destination_city": enrich.get("destination_city", ""),
        "destination_iata": dest_iata,
        "outbound_date": out_date,
        "return_date": ret_date,
        "stops": str(st),
        "deal_theme": (theme or theme_of_day or "default"),
        "theme": (theme or ""),  # keep if your sheet uses it
        "cabin_class": cabin_class,
        "currency": currency,
        "connection_type": connection_type,
        "carriers": carriers,
        "outbound_duration_minutes": "" if out_m is None else str(out_m),
        "inbound_duration_minutes": "" if in_m is None else str(in_m),
        "total_duration_hours": total_hours,
        "trip_length_days": "" if (out_date == "" or ret_date == "") else "",  # optional; RDV can compute
        "ingested_at_utc": now_utc_iso(),
        "created_utc": now_utc_iso(),
    })

    return row


# ------------------ Main ------------------

def main():
    log("=" * 78)
    log("TRAVELTXTTER PIPELINE WORKER (FEEDER) START")
    log("=" * 78)

    sh = open_sheet()

    raw_tab = env("RAW_DEALS_TAB", "RAW_DEALS")
    ws_raw = sh.worksheet(raw_tab)

    headers = get_headers(ws_raw)
    idx = header_index(headers)

    # Minimal required RD schema fields for insertion to be "real"
    require_headers(idx, [
        "status", "deal_id", "price_gbp", "origin_iata", "destination_iata",
        "origin_city", "destination_city", "destination_country",
        "outbound_date", "return_date", "stops", "deal_theme",
        "ingested_at_utc", "created_utc"
    ], raw_tab)

    existing_ids = fetch_existing_deal_ids(ws_raw, idx)

    config_rows = load_config(sh)
    if not config_rows:
        log("⚠️ No active CONFIG rows (active_in_feeder).")
        return 0

    rcm = load_rcm(sh)

    theme_today = pick_theme_of_day(config_rows)
    log(f"🎯 Theme of the day (UTC): {theme_today}")

    # Choose rows for today's theme_of_day (fallback: theme matches)
    todays = [r for r in config_rows if (r.get("theme_of_day") or "").strip() == theme_today]
    if not todays:
        todays = [r for r in config_rows if (r.get("theme") or "").strip() == theme_today]

    if not todays:
        log(f"⚠️ No CONFIG rows match theme_of_day/theme = {theme_today}")
        return 0

    # Sort by priority/search_weight if present
    def score_row(r):
        try:
            pr = float(r.get("priority") or 0)
        except Exception:
            pr = 0.0
        try:
            sw = float(r.get("search_weight") or 1)
        except Exception:
            sw = 1.0
        return pr * sw

    todays = sorted(todays, key=score_row, reverse=True)

    routes_per_run = env_int("DUFFEL_ROUTES_PER_RUN", 6)
    max_searches = env_int("DUFFEL_MAX_SEARCHES_PER_RUN", 12)
    max_inserts = env_int("DUFFEL_MAX_INSERTS", 20)
    sleep_s = env_float("FEEDER_SLEEP_SECONDS", 0.15)
    jitter_days = env_int("DATE_JITTER_DAYS", 2)

    cabin_class = env("CABIN_CLASS", "economy")
    currency = env("CURRENCY", "GBP")

    # Destination-first: pick distinct destinations, then search origins pool
    by_dest: Dict[str, List[Dict[str, Any]]] = {}
    for r in todays:
        by_dest.setdefault(r["destination_iata"], []).append(r)

    dests = list(by_dest.keys())[:routes_per_run]
    if not dests:
        log("⚠️ No destinations selected.")
        return 0

    origins_pool = origins_for_theme(theme_today)

    inserts = 0
    searches = 0

    # Default values for any optional RD columns (we never write status side-effects)
    schema_defaults = {h: "" for h in headers}

    for dest in dests:
        # choose best config row for this dest
        cfg = by_dest[dest][0]
        # CONFIG route still gives a default origin_iata, but we prefer theme pool origins and RCM availability
        base_origin = (cfg.get("origin_iata") or "").strip().upper()

        try:
            days_min = int(cfg.get("days_ahead_min") or 21)
        except Exception:
            days_min = 21
        try:
            days_max = int(cfg.get("days_ahead_max") or 84)
        except Exception:
            days_max = 84
        try:
            trip_len = int(cfg.get("trip_length_days") or 5)
        except Exception:
            trip_len = 5
        try:
            max_conn = int(cfg.get("max_connections") or 1)
        except Exception:
            max_conn = 1

        theme = (cfg.get("theme") or "").strip()
        theme_of_day = (cfg.get("theme_of_day") or "").strip()

        best_offer = None
        best_origin = None

        # Put base origin at the front if it exists, then the rest of the pool
        origins = [base_origin] if base_origin else []
        for o in origins_pool:
            if o and o not in origins:
                origins.append(o)

        out_base, ret_base = select_dates("X", dest, days_min, days_max, trip_len)  # origin-specific hash later

        for origin in origins:
            if searches >= max_searches:
                break

            # Only try if RCM says route exists/enabled
            if (origin, dest) not in rcm:
                continue

            # origin-specific deterministic dates
            out_base, ret_base = select_dates(origin, dest, days_min, days_max, trip_len)

            for j in range(-jitter_days, jitter_days + 1):
                if searches >= max_searches:
                    break

                out_d = out_base + dt.timedelta(days=j)
                ret_d = ret_base + dt.timedelta(days=j)

                searches += 1
                req = duffel_create_offer_request(
                    origin=origin,
                    dest=dest,
                    out_date=out_d.isoformat(),
                    ret_date=ret_d.isoformat(),
                    max_connections=max_conn,
                    cabin_class=cabin_class,
                    currency=currency,
                )
                time.sleep(sleep_s)

                if not req:
                    continue

                offers = (req.get("offers") or [])
                if not offers:
                    # fallback: list offers endpoint
                    orid = (req.get("id") or "").strip()
                    if orid:
                        offers = duffel_list_offers(orid, limit=50)

                offer = pick_cheapest_offer(offers)
                if not offer:
                    continue

                # Respect max_connections sanity
                if offer_stops(offer) > max_conn:
                    continue

                # We have a candidate
                best_offer = offer
                best_origin = origin
                break

            if best_offer:
                break

        if not best_offer or not best_origin:
            continue

        deal_id = (best_offer.get("id") or "").strip()
        if not deal_id or deal_id in existing_ids:
            continue

        out_date, ret_date = offer_dates(best_offer)
        try:
            price_ok = float(best_offer.get("total_amount") or 0) > 0
        except Exception:
            price_ok = False

        # HARD RULE: never insert stubs
        if not out_date or not ret_date or not price_ok:
            continue

        enrich = rcm.get((best_origin, dest), {})
        log(f"🧩 CAPABILITY_ENRICH: {best_origin}->{dest} | {enrich.get('origin_city','')}, {enrich.get('origin_country','')} → {enrich.get('destination_city','')}, {enrich.get('destination_country','')}")

        rd_row = build_rd_row(
            schema_defaults=schema_defaults,
            offer=best_offer,
            origin_iata=best_origin,
            dest_iata=dest,
            theme=theme,
            theme_of_day=theme_of_day or theme_today,
            enrich=enrich,
            cabin_class=cabin_class,
            currency=currency,
        )

        # Final sanity: schema-required fields present
        if not rd_row.get("deal_id") or not rd_row.get("price_gbp") or not rd_row.get("outbound_date") or not rd_row.get("return_date"):
            continue

        append_row_by_headers(ws_raw, headers, rd_row)
        existing_ids.add(deal_id)
        inserts += 1

        log(f"✅ Inserted 1 rows into {raw_tab}: {best_origin}->{dest} £{rd_row.get('price_gbp')} OUT {rd_row.get('outbound_date')} BACK {rd_row.get('return_date')}")

        if inserts >= max_inserts:
            break

    if inserts == 0:
        log("⚠️ No winners to insert")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
