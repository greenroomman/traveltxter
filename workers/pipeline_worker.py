#!/usr/bin/env python3
"""
TravelTxter V4.5x — pipeline_worker.py (FEEDER + DISCOVERY) — FIXED CITY/COUNTRY ENRICHMENT

Fix:
- Stop inserting IATA into origin_city / destination_city.
- Populate destination_country reliably.
- If metadata cannot be resolved, bank the row (status=BANKED) so publishers never see junk.

Non-negotiables preserved:
- Duffel-safe IATA ONLY (no LON/NYC/PAR)
- Uses your existing sheet tabs:
  CONFIG, CONFIG_THEME_DESTINATIONS, CONFIG_SIGNALS, CONFIG_ORIGIN_POOLS
- Keeps free-tier caps via env vars
"""

from __future__ import annotations

import os
import json
import time
import math
import random
import datetime as dt
from typing import Dict, Any, List, Tuple, Optional, Set

import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# Logging
# ============================================================

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


# ============================================================
# Cell helpers (robust against NaN/float)
# ============================================================

def cell_str(v) -> str:
    if v is None:
        return ""
    if isinstance(v, float):
        if math.isnan(v):
            return ""
        if v.is_integer():
            return str(int(v))
        return str(v)
    return str(v)

def cell_upper(v) -> str:
    return cell_str(v).strip().upper()

def cell_lower(v) -> str:
    return cell_str(v).strip().lower()


# ============================================================
# Validation
# ============================================================

def is_iata3(s: str) -> bool:
    ss = (s or "").strip().upper()
    return len(ss) == 3 and ss.isalpha()

def is_trueish(v) -> bool:
    s = cell_lower(v)
    if s in ("", "true", "1", "yes", "y", "on", "enabled"):
        return True
    if s in ("false", "0", "no", "n", "off", "disabled"):
        return False
    return True


# ============================================================
# Theme Route Packs (fallback)
# ============================================================

ORIGIN_CLUSTERS = {
    "SW": ["BRS", "EXT", "NQY", "SOU", "CWL"],
    "LON": ["LHR", "LGW", "STN", "LTN", "LCY", "SEN"],
    "NORTH": ["MAN", "BHX", "LPL", "NCL", "EDI", "GLA"],
}

THEME_DEFAULTS = {
    "SNOW": {"lead_days": 55, "trip_days": 4},
    "SURF": {"lead_days": 70, "trip_days": 6},
    "WINTER_SUN": {"lead_days": 70, "trip_days": 7},
    "CITY_BREAKS": {"lead_days": 45, "trip_days": 3},
}

ROUTE_PACKS = {
    "SNOW": {"dests": ["GVA", "BGY", "MXP", "TRN", "MUC", "ZRH", "BSL", "INN", "SZG"]},
    "SURF": {"dests": ["AGA", "RAK", "AGP", "FAO", "FUE", "ACE", "LPA", "TFS"]},
    "WINTER_SUN": {"dests": ["TFS", "LPA", "FUE", "ACE", "RAK", "AGA", "FNC", "PDL"]},
    "CITY_BREAKS": {"dests": ["BUD", "PRG", "KRK", "WAW", "BCN", "PMI", "OPO", "LIS", "AMS", "DUB", "CPH"]},
}

def normalize_theme_name(theme: str) -> str:
    t = (theme or "").strip().upper()
    if t in ("SKI",):
        return "SNOW"
    if t in ("CITY", "CITYBREAK", "CITYBREAKS"):
        return "CITY_BREAKS"
    if t in ("SUN", "BEACH"):
        return "WINTER_SUN"
    return t

def today_utc() -> dt.date:
    return dt.datetime.utcnow().date()

def pick_origin_cluster(run_slot: str) -> str:
    salt = 0 if (run_slot or "").upper() == "AM" else 1
    idx = (today_utc().timetuple().tm_yday + salt) % 3
    return ["SW", "LON", "NORTH"][idx]

def build_route_pack_fallback(theme: str, origins: List[str], cap: int = 40) -> List[Tuple[str, str, str, int, int]]:
    t = normalize_theme_name(theme)
    pack = ROUTE_PACKS.get(t)
    if not pack:
        return []

    defaults = THEME_DEFAULTS.get(t, {"lead_days": 55, "trip_days": 5})
    lead_days = int(defaults.get("lead_days", 55))
    trip_days = int(defaults.get("trip_days", 5))

    olist = [cell_upper(o) for o in (origins or []) if is_iata3(cell_upper(o))]
    if not olist:
        olist = ["LHR", "LGW", "STN", "LTN", "MAN", "BRS", "EDI", "GLA", "BHX"]

    dests = [cell_upper(d) for d in pack.get("dests", []) if is_iata3(cell_upper(d))]
    if not dests:
        return []

    routes: List[Tuple[str, str, str, int, int]] = []
    seen = set()

    rnd = random.Random(t + "_" + str(today_utc()))
    rnd.shuffle(olist)
    rnd.shuffle(dests)

    for d in dests:
        for o in olist:
            if o == d:
                continue
            k = (o, d, t)
            if k in seen:
                continue
            routes.append((o, d, t, lead_days, trip_days))
            seen.add(k)
            if len(routes) >= cap:
                return routes

    return routes


# ============================================================
# Auth / Sheets
# ============================================================

def env_str(k: str, default: str = "") -> str:
    return (os.environ.get(k, default) or "").strip()

def env_int(k: str, default: int) -> int:
    try:
        return int(env_str(k, str(default)))
    except Exception:
        return default

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
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds)

def ensure_headers(ws: gspread.Worksheet, required: List[str]) -> Dict[str, int]:
    existing = ws.row_values(1)
    existing = [cell_str(x).strip() for x in existing if cell_str(x).strip()]

    if not existing:
        ws.update([required], "A1")
        existing = required[:]
        log(f"🛠️ Created headers for {ws.title}")

    missing = [c for c in required if c not in existing]
    if missing:
        new_headers = existing + missing
        ws.update([new_headers], "A1")
        log(f"🛠️ Added missing columns to {ws.title}: {missing}")
        existing = new_headers

    return {h: i for i, h in enumerate(existing)}

def load_records(ws: gspread.Worksheet) -> List[Dict[str, Any]]:
    recs = ws.get_all_records(default_blank="")
    out = []
    for r in recs:
        rr = {}
        for k, v in (r or {}).items():
            rr[cell_str(k).strip()] = v
        out.append(rr)
    return out


# ============================================================
# Metadata enrichment (CONFIG_SIGNALS + fallbacks)
# ============================================================

UK_AIRPORT_CITY_FALLBACK = {
    "LHR": "London",
    "LGW": "London",
    "STN": "London",
    "LTN": "London",
    "LCY": "London",
    "SEN": "London",
    "MAN": "Manchester",
    "BRS": "Bristol",
    "BHX": "Birmingham",
    "EDI": "Edinburgh",
    "GLA": "Glasgow",
    "NCL": "Newcastle",
    "LPL": "Liverpool",
    "NQY": "Newquay",
    "SOU": "Southampton",
    "CWL": "Cardiff",
    "EXT": "Exeter",
}

def _pick_header(headers: List[str], *names: str) -> Optional[int]:
    idx = {h.strip(): i for i, h in enumerate(headers)}
    for n in names:
        if n in idx:
            return idx[n]
    return None

def load_signals_maps(sh: gspread.Spreadsheet) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Returns (iata->city, iata->country) from CONFIG_SIGNALS.
    Supports header variants:
      iata_hint / destination_iata / iata
      destination_city / city
      destination_country / country
    """
    try:
        ws = sh.worksheet("CONFIG_SIGNALS")
    except Exception:
        return {}, {}

    values = ws.get_all_values()
    if not values or len(values) < 2:
        return {}, {}

    headers = [h.strip() for h in values[0]]

    i_iata = _pick_header(headers, "iata_hint", "destination_iata", "iata", "airport_iata", "dest_iata")
    i_city = _pick_header(headers, "destination_city", "city", "dest_city", "airport_city")
    i_country = _pick_header(headers, "destination_country", "country", "dest_country")

    if i_iata is None:
        return {}, {}

    iata_to_city: Dict[str, str] = {}
    iata_to_country: Dict[str, str] = {}

    for r in values[1:]:
        code = (r[i_iata] if i_iata < len(r) else "").strip().upper()
        if not is_iata3(code):
            continue
        city = (r[i_city] if (i_city is not None and i_city < len(r)) else "").strip()
        country = (r[i_country] if (i_country is not None and i_country < len(r)) else "").strip()
        if city:
            iata_to_city[code] = city
        if country:
            iata_to_country[code] = country

    return iata_to_city, iata_to_country

def resolve_origin_city(origin_iata: str, iata_to_city: Dict[str, str]) -> str:
    o = cell_upper(origin_iata)
    if not is_iata3(o):
        return ""
    return iata_to_city.get(o) or UK_AIRPORT_CITY_FALLBACK.get(o) or o

def resolve_dest_city_country(dest_iata: str, iata_to_city: Dict[str, str], iata_to_country: Dict[str, str]) -> Tuple[str, str]:
    d = cell_upper(dest_iata)
    if not is_iata3(d):
        return "", ""
    return (iata_to_city.get(d, ""), iata_to_country.get(d, ""))


# ============================================================
# Duffel
# ============================================================

DUFFEL_API = "https://api.duffel.com/air/offer_requests"

def duffel_headers():
    key = env_str("DUFFEL_API_KEY")
    if not key:
        raise RuntimeError("Missing DUFFEL_API_KEY")
    return {
        "Authorization": f"Bearer {key}",
        "Duffel-Version": "v2",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def _next_weekday(d: dt.date, weekday: int) -> dt.date:
    delta = (weekday - d.weekday()) % 7
    return d + dt.timedelta(days=delta)

def pick_dates_for_theme(theme: str, avg_lead_days: int = 45, avg_trip_days: int = 5) -> Tuple[str, str]:
    t = (theme or "").strip().upper()
    lead_jitter = random.randint(-10, 15)
    trip_jitter = random.randint(-1, 3)

    base_out = today_utc() + dt.timedelta(days=max(10, avg_lead_days + lead_jitter))

    if t in ("SNOW", "SKI"):
        preferred = [3, 4, 5]
        trip_min, trip_max = 3, 5
    elif t in ("SURF", "WINTER_SUN", "SUN", "BEACH"):
        preferred = [0, 1]
        trip_min, trip_max = 4, 7
    elif t in ("CITY", "CITY_BREAKS", "CITYBREAK", "FOODIE", "CULTURE"):
        preferred = [3, 4]
        trip_min, trip_max = 2, 4
    else:
        preferred = [1, 3, 5]
        trip_min, trip_max = 3, 6

    outs = [_next_weekday(base_out, w) for w in preferred]
    out = min(outs)

    trip = max(trip_min, min(trip_max, avg_trip_days + trip_jitter))
    ret = out + dt.timedelta(days=trip)
    return out.isoformat(), ret.isoformat()

def duffel_offer_request(origin: str, dest: str, out_date: str, ret_date: str) -> Dict[str, Any]:
    return {
        "data": {
            "slices": [
                {"origin": origin, "destination": dest, "departure_date": out_date},
                {"origin": dest, "destination": origin, "departure_date": ret_date},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": "economy",
        }
    }

def duffel_search(origin: str, dest: str, out_date: str, ret_date: str) -> Dict[str, Any]:
    r = requests.post(DUFFEL_API, headers=duffel_headers(), json=duffel_offer_request(origin, dest, out_date, ret_date), timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"Duffel offer_requests failed: {r.status_code} {r.text[:300]}")
    return r.json()


# ============================================================
# Config helpers
# ============================================================

def origins_from_origin_pools(rows: List[Dict]) -> List[str]:
    out = []
    for r in rows:
        if not is_trueish(r.get("enabled", True)):
            continue
        o = cell_upper(r.get("origin_iata"))
        if is_iata3(o):
            out.append(o)
    return list(dict.fromkeys(out))

def build_week_theme_plan(themes_rows: List[Dict]) -> List[str]:
    mapping = {}
    for r in themes_rows:
        dow = cell_lower(r.get("day_of_week"))
        theme = cell_upper(r.get("theme"))
        if dow and theme:
            mapping[dow] = theme
    days = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
    default = ["CITY_BREAKS", "SURF", "SNOW", "WINTER_SUN", "CITY_BREAKS", "SURF", "SNOW"]
    return [mapping.get(d, default[i]) for i, d in enumerate(days)]

def pick_today_theme(week_plan: List[str]) -> str:
    idx = dt.datetime.utcnow().weekday()
    try:
        return cell_upper(week_plan[idx])
    except Exception:
        return "CITY_BREAKS"

def theme_destinations(themes_rows: List[Dict], theme: str) -> List[Dict]:
    t = cell_upper(theme)
    out = []
    for r in themes_rows:
        if cell_upper(r.get("theme")) != t:
            continue
        if not is_trueish(r.get("enabled", True)):
            continue
        di = cell_upper(r.get("destination_iata"))
        if not is_iata3(di):
            continue
        out.append(r)

    def keyfun(rr):
        pr = rr.get("priority", 9999)
        try:
            pr_i = int(float(pr))
        except Exception:
            pr_i = 9999
        return (pr_i, cell_upper(rr.get("destination_iata")))

    out.sort(key=keyfun)
    return out


# ============================================================
# Main
# ============================================================

def main() -> None:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    raw_tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")

    run_slot = env_str("RUN_SLOT", "AM").upper()
    weekly_sweep = env_str("WEEKLY_THEME_SWEEP", "true").strip().lower() in ("true", "1", "yes")
    allow_publish = env_str("ALLOW_PUBLISH", "true").strip().lower() in ("true", "1", "yes")

    routes_cap = env_int("DUFFEL_ROUTES_PER_RUN", 4)
    searches_cap = env_int("DUFFEL_MAX_SEARCHES_PER_RUN", 4)
    inserts_cap = env_int("DUFFEL_MAX_INSERTS", 3)

    gc = gs_client()
    sh = gc.open_by_key(spreadsheet_id)
    raw_ws = sh.worksheet(raw_tab)

    # Keep compatible with your sheet (adds if missing)
    h = ensure_headers(raw_ws, [
        "status",
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
        "carriers",
        "deeplink",
        "deal_theme",
        "timestamp",
        # discovery / banking (safe add)
        "banked_utc",
        "reason_banked",
    ])

    sig_city, sig_country = load_signals_maps(sh)

    # Load tabs (best-effort)
    try:
        config_rows = load_records(sh.worksheet("CONFIG"))
    except Exception:
        config_rows = []

    try:
        themes_rows = load_records(sh.worksheet("CONFIG_THEME_DESTINATIONS"))
    except Exception:
        themes_rows = []

    try:
        origin_rows = load_records(sh.worksheet("CONFIG_ORIGIN_POOLS"))
        origins = origins_from_origin_pools(origin_rows)
    except Exception:
        origins = []

    if not origins:
        origins = ["LHR", "LGW", "STN", "LTN", "MAN", "BRS", "EDI", "GLA", "BHX"]

    week_plan = build_week_theme_plan(themes_rows)
    today_theme = pick_today_theme(week_plan)
    log(f"RUN_SLOT={run_slot} | Today theme: {today_theme}")

    # Build route plan
    route_plan: List[Tuple[str, str, str, int, int]] = []

    if weekly_sweep and run_slot == "AM":
        for t in week_plan:
            dest_rows = theme_destinations(themes_rows, t)
            for rr in dest_rows[:25]:
                d = cell_upper(rr.get("destination_iata"))
                if not is_iata3(d):
                    continue
                lead_days = int(float(rr.get("lead_days") or 45))
                trip_days = int(float(rr.get("trip_days") or 5))
                for o in origins[:4]:
                    if o != d:
                        route_plan.append((o, d, cell_upper(t), lead_days, trip_days))

        if len(route_plan) < 10:
            cluster = pick_origin_cluster(run_slot)
            cluster_origins = [o for o in origins if o in set(ORIGIN_CLUSTERS.get(cluster, []))]
            route_plan.extend(build_route_pack_fallback(today_theme, cluster_origins or origins, cap=80))
    else:
        for r in config_rows:
            if not is_trueish(r.get("enabled", True)):
                continue
            if cell_upper(r.get("theme")) != cell_upper(today_theme):
                continue
            o = cell_upper(r.get("origin_iata"))
            d = cell_upper(r.get("destination_iata"))
            if not (is_iata3(o) and is_iata3(d)):
                continue
            lead_days = int(float(r.get("lead_days") or 45))
            trip_days = int(float(r.get("trip_days") or 5))
            route_plan.append((o, d, cell_upper(today_theme), lead_days, trip_days))

        if not route_plan:
            for rr in theme_destinations(themes_rows, today_theme)[:40]:
                d = cell_upper(rr.get("destination_iata"))
                if not is_iata3(d):
                    continue
                lead_days = int(float(rr.get("lead_days") or 45))
                trip_days = int(float(rr.get("trip_days") or 5))
                for o in origins[:4]:
                    if o != d:
                        route_plan.append((o, d, cell_upper(today_theme), lead_days, trip_days))

        if not route_plan or len(route_plan) < 10:
            cluster = pick_origin_cluster(run_slot)
            cluster_origins = [o for o in origins if o in set(ORIGIN_CLUSTERS.get(cluster, []))]
            route_plan.extend(build_route_pack_fallback(today_theme, cluster_origins or origins, cap=60))

    if not route_plan:
        log("❌ No routes available after CONFIG + fallbacks.")
        return

    route_plan = route_plan[:routes_cap]

    searches = 0
    inserted = 0
    seen_run: Set[Tuple[str, str, str, str, str]] = set()

    for origin, dest, theme, lead_days, trip_days in route_plan:
        if searches >= searches_cap or inserted >= inserts_cap:
            break

        if not (is_iata3(origin) and is_iata3(dest)):
            continue

        out_date, ret_date = pick_dates_for_theme(theme, avg_lead_days=lead_days, avg_trip_days=trip_days)
        log(f"✈️ Duffel search: {origin}->{dest} ({theme}) {out_date}/{ret_date}")

        try:
            res = duffel_search(origin, dest, out_date, ret_date)
            searches += 1
        except Exception as e:
            log(f"❌ Duffel error: {e}")
            continue

        offers = (((res or {}).get("data") or {}).get("offers") or [])
        if not offers:
            continue

        offers_sorted = []
        for off in offers:
            total = (off.get("total_amount") or "").strip()
            try:
                price = float(total)
            except Exception:
                continue
            offers_sorted.append((price, off))
        offers_sorted.sort(key=lambda x: x[0])

        for price, off in offers_sorted:
            if inserted >= inserts_cap:
                break

            try:
                stops = max(0, len(off["slices"][0]["segments"]) - 1)
            except Exception:
                stops = 0

            carriers = set()
            try:
                for sl in off.get("slices", []):
                    for seg in sl.get("segments", []):
                        oc = seg.get("operating_carrier", {}) or {}
                        code = (oc.get("iata_code") or "").strip().upper()
                        if code:
                            carriers.add(code)
            except Exception:
                pass
            carriers_str = ",".join(sorted(carriers))[:80]

            deal_seed = f"{origin}{dest}{out_date.replace('-','')}{ret_date.replace('-','')}{int(price*100)}"
            deal_id = str(abs(hash(deal_seed)))[:12]

            dk = (origin, dest, out_date, ret_date, f"{price:.2f}")
            if dk in seen_run:
                continue
            seen_run.add(dk)

            origin_city = resolve_origin_city(origin, sig_city)
            dest_city, dest_country = resolve_dest_city_country(dest, sig_city, sig_country)

            # If we cannot resolve destination metadata, bank it.
            meta_ok = bool(dest_city) and bool(dest_country)

            status = "NEW" if meta_ok else "BANKED"
            reason = "" if meta_ok else "missing_signals_city_or_country"

            row = [""] * len(h)

            def setv(col: str, val: str) -> None:
                idx = h.get(col)
                if idx is None:
                    return
                if idx >= len(row):
                    row.extend([""] * (idx - len(row) + 1))
                row[idx] = val

            setv("status", status)
            setv("deal_id", deal_id)
            setv("origin_iata", origin)
            setv("destination_iata", dest)
            setv("origin_city", origin_city)
            setv("destination_city", dest_city or dest)  # keep IATA only if banked
            setv("destination_country", dest_country)
            setv("outbound_date", out_date)
            setv("return_date", ret_date)
            setv("price_gbp", f"{price:.2f}")
            setv("currency", "GBP")
            setv("stops", str(stops))
            setv("carriers", carriers_str)
            setv("deal_theme", theme)
            setv("timestamp", dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")

            if status == "BANKED":
                setv("banked_utc", dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
                setv("reason_banked", reason)

            if allow_publish:
                raw_ws.append_row(row, value_input_option="USER_ENTERED")
                inserted += 1
                log(f"✅ Inserted {status}: {origin}->{dest} £{price:.2f} ({theme})")
            else:
                log(f"🧪 Dry-run: would insert {status}: {origin}->{dest} £{price:.2f} ({theme})")

    log(f"Done. searches={searches} inserted={inserted}")


if __name__ == "__main__":
    main()
