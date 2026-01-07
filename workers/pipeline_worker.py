#!/usr/bin/env python3
"""
TravelTxter V4.5x — PIPELINE WORKER (FEEDER + DISCOVERY) — WEEKLY THEME SWEEP FALLBACK

FIX INCLUDED:
- Robustly handles Google Sheets NaN / floats from get_all_records()
  (prevents: AttributeError: 'float' object has no attribute 'strip')

UPGRADE (Jan 2026):
- Theme Route Packs fallback: when CONFIG_SIGNALS / priors are weak, still get real themed deals.
- UK origin cluster rotation: SW / London / North to prevent hammering one region.
- Theme-aware date picking (single-shot, no extra searches).

Core principles:
- REAL IATA ONLY (Duffel-safe) — never LON/PAR/NYC etc
- Theme-aware
- Weekly sweep across all themes (Mon→Sun) in AM slot if enabled
- Insert NEW deals when eligible, otherwise bank to DISCOVERY_BANK
"""

import os
import sys
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
# Theme Route Packs (fallback when CONFIG_SIGNALS / priors are weak)
# ============================================================

ORIGIN_CLUSTERS = {
    # South West / Wales
    "SW": ["BRS", "EXT", "NQY", "SOU", "CWL"],
    # London airports
    "LON": ["LHR", "LGW", "STN", "LTN", "LCY", "SEN"],
    # Midlands + North
    "NORTH": ["MAN", "BHX", "LPL", "NCL", "EDI", "GLA"],
}

THEME_DEFAULTS = {
    "SNOW": {"lead_days": 55, "trip_days": 4},
    "SURF": {"lead_days": 70, "trip_days": 6},
    "WINTER_SUN": {"lead_days": 70, "trip_days": 7},
    "CITY_BREAKS": {"lead_days": 45, "trip_days": 3},
}

# High-probability destinations by theme (Duffel-safe IATA only).
# Keep this list curated and fairly small: quality > quantity.
ROUTE_PACKS = {
    "SNOW": {
        "dests": ["GVA", "BGY", "MXP", "TRN", "MUC", "ZRH", "BSL", "INN", "SZG"],
    },
    "SURF": {
        "dests": ["AGA", "RAK", "AGP", "FAO", "FUE", "ACE", "LPA", "TFS"],
    },
    "WINTER_SUN": {
        "dests": ["TFS", "LPA", "FUE", "ACE", "RAK", "AGA", "FNC", "PDL"],
    },
    "CITY_BREAKS": {
        "dests": ["BUD", "PRG", "KRK", "WAW", "BCN", "PMI", "OPO", "LIS", "AMS", "DUB", "CPH"],
    },
}

def normalize_theme_name(theme: str) -> str:
    t = (theme or "").strip().upper()
    # Allow a few common variants
    if t in ("SKI",):
        return "SNOW"
    if t in ("CITY", "CITYBREAK", "CITYBREAKS"):
        return "CITY_BREAKS"
    if t in ("SUN", "BEACH"):
        return "WINTER_SUN"
    return t

def pick_origin_cluster(run_slot: str) -> str:
    """
    Deterministic cluster rotation so we don't hammer London every run.
    Uses day-of-year + slot to pick SW/LON/NORTH.
    """
    d = today_utc()
    salt = 0 if (run_slot or "").upper() == "AM" else 1
    idx = (d.timetuple().tm_yday + salt) % 3
    return ["SW", "LON", "NORTH"][idx]

def build_route_pack_fallback(
    theme: str,
    origins: List[str],
    config_route_set: set,
    cap: int = 40,
) -> List[Tuple[str, str, str, int, int]]:
    """
    Build (origin, dest, theme, lead_days, trip_days) routes from curated packs.
    Filters to real IATA only; does not require CONFIG_SIGNALS to be populated.
    """
    t = normalize_theme_name(theme)
    pack = ROUTE_PACKS.get(t)
    if not pack:
        return []

    defaults = THEME_DEFAULTS.get(t, {"lead_days": 55, "trip_days": 5})
    lead_days = int(defaults.get("lead_days", 55))
    trip_days = int(defaults.get("trip_days", 5))

    # Use only valid IATA origins/dests
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

    # Prefer configured routes when present, otherwise allow pack routes freely
    for d in dests:
        for o in olist:
            if o == d:
                continue
            k = (o, d, t)
            if k in seen:
                continue

            if config_route_set and (o, d) not in config_route_set:
                # If CONFIG exists, treat it as preference but not a hard block
                pass

            routes.append((o, d, t, lead_days, trip_days))
            seen.add(k)
            if len(routes) >= cap:
                return routes

    return routes


# ============================================================
# Auth / Sheets
# ================================================

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
    """
    Uses get_all_records() but normalises NaN floats and missing fields.
    """
    recs = ws.get_all_records(default_blank="")
    out = []
    for r in recs:
        rr = {}
        for k, v in (r or {}).items():
            rr[cell_str(k).strip()] = v
        out.append(rr)
    return out


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
    # default permissive
    return True


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


def today_utc() -> dt.date:
    return dt.datetime.utcnow().date()


def _next_weekday(d: dt.date, weekday: int) -> dt.date:
    """weekday: Monday=0..Sunday=6"""
    delta = (weekday - d.weekday()) % 7
    return d + dt.timedelta(days=delta)


def pick_dates_for_theme(theme: str, avg_lead_days: int = 45, avg_trip_days: int = 5) -> Tuple[str, str]:
    """
    Theme-aware date picking (single-shot, no extra searches).
    Keeps your free-tier caps safe while increasing hit-rate for "real" themed trips.
    """
    t = (theme or "").strip().upper()

    # baseline jitter
    lead_jitter = random.randint(-10, 15)
    trip_jitter = random.randint(-1, 3)

    base_out = today_utc() + dt.timedelta(days=max(10, avg_lead_days + lead_jitter))

    # Preferred outbound weekdays per theme
    # SNOW: Thu/Fri/Sat (long weekends)
    # SURF/WINTER_SUN: Mon/Tue (cheaper shoulder days)
    # CITY: Thu/Fri (weekenders)
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
        preferred = [1, 3, 5]  # mixed
        trip_min, trip_max = 3, 6

    # Snap base_out to the next preferred weekday (choose the soonest)
    outs = [_next_weekday(base_out, w) for w in preferred]
    out = min(outs)

    # Trip length: keep avg as centre, clamp to theme ranges
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
    payload = duffel_offer_request(origin, dest, out_date, ret_date)
    r = requests.post(DUFFEL_API, headers=duffel_headers(), json=payload, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"Duffel offer_requests failed: {r.status_code} {r.text[:300]}")
    return r.json()


# ============================================================
# Config / Discovery helpers
# ============================================================

def origins_from_origin_pools(rows: List[Dict]) -> List[str]:
    """
    CONFIG_ORIGIN_POOLS: expected columns:
      - origin_iata
      - enabled
      - priority
    """
    out = []
    for r in rows:
        if not is_trueish(r.get("enabled", True)):
            continue
        o = cell_upper(r.get("origin_iata"))
        if is_iata3(o):
            out.append(o)
    # stable order
    out = list(dict.fromkeys(out))
    return out


def build_week_theme_plan(themes_rows: List[Dict]) -> List[str]:
    """
    CONFIG_THEMES expects (flexible):
      - day_of_week (Mon..Sun) and theme
    If missing, provide a stable default.
    """
    mapping = {}
    for r in themes_rows:
        dow = cell_lower(r.get("day_of_week"))
        theme = cell_upper(r.get("theme"))
        if dow and theme:
            mapping[dow] = theme

    days = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
    default = ["CITY_BREAKS", "SURF", "SNOW", "WINTER_SUN", "CITY_BREAKS", "SURF", "SNOW"]

    out = []
    for i, d in enumerate(days):
        out.append(mapping.get(d, default[i]))
    return out


def pick_today_theme(week_plan: List[str]) -> str:
    idx = dt.datetime.utcnow().weekday()  # Monday=0
    try:
        return cell_upper(week_plan[idx])
    except Exception:
        return "CITY_BREAKS"


def theme_destinations(themes_rows: List[Dict], theme: str) -> List[Dict]:
    """
    CONFIG_THEME_DESTINATIONS:
      - theme
      - destination_iata
      - lead_days
      - trip_days
      - priority
      - enabled
    """
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


def signals_map(signals_rows: List[Dict]) -> Dict[str, Dict]:
    """
    CONFIG_SIGNALS: expects iata_hint column, but may contain NaN floats.
    Returns map keyed by iata_hint.
    """
    m = {}
    for r in signals_rows:
        code = cell_upper(r.get("iata_hint"))
        if is_iata3(code):
            m[code] = r
    return m


def build_massive_weekly_fallback_routes(
    week_plan: List[str],
    themes_rows: List[Dict],
    signals_rows: List[Dict],
    origins: List[str],
    per_theme_cap: int = 25,
) -> List[Tuple[str, str, str, int, int]]:
    sig = signals_map(signals_rows)
    routes: List[Tuple[str, str, str, int, int]] = []
    seen: Set[Tuple[str, str, str]] = set()

    for t in week_plan:
        dest_rows = theme_destinations(themes_rows, t)
        if not dest_rows:
            continue

        dest_rows = dest_rows[:per_theme_cap]

        rnd = random.Random(t)
        dests = [cell_upper(r.get("destination_iata")) for r in dest_rows if is_iata3(cell_upper(r.get("destination_iata")))]
        rnd.shuffle(dests)

        olist = origins[:] if origins else ["LHR", "LGW", "STN", "LTN", "MAN", "BRS", "EDI", "GLA", "BHX"]
        if not olist:
            olist = ["LHR", "LGW", "STN", "LTN", "MAN", "BRS", "EDI", "GLA", "BHX"]

        for d in dests:
            _ = sig.get(d)  # keep preference but don't hard-block

            chosen_origins = []
            # rotate origins deterministically for variety
            for o in olist:
                if o != d and is_iata3(o):
                    chosen_origins.append(o)
                if len(chosen_origins) >= 4:
                    break

            for o in chosen_origins:
                lead_days = 45
                trip_days = 5
                # allow overrides if present in destination rows
                for rr in dest_rows:
                    if cell_upper(rr.get("destination_iata")) == d:
                        try:
                            lead_days = int(float(rr.get("lead_days") or lead_days))
                        except Exception:
                            pass
                        try:
                            trip_days = int(float(rr.get("trip_days") or trip_days))
                        except Exception:
                            pass
                        break

                k = (o, d, cell_upper(t))
                if k in seen:
                    continue
                routes.append((o, d, cell_upper(t), lead_days, trip_days))
                seen.add(k)

    return routes


# ============================================================
# Main
# ============================================================

def main() -> None:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    raw_tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")

    run_slot = env_str("RUN_SLOT", "AM").upper()

    routes_cap = env_int("DUFFEL_ROUTES_PER_RUN", 4)
    searches_cap = env_int("DUFFEL_MAX_SEARCHES_PER_RUN", 4)
    inserts_cap = env_int("DUFFEL_MAX_INSERTS", 3)

    weekly_sweep = env_str("WEEKLY_THEME_SWEEP", "true").strip().lower() in ("true", "1", "yes")

    gc = gs_client()
    sh = gc.open_by_key(spreadsheet_id)

    raw_ws = sh.worksheet(raw_tab)

    # Required RAW_DEALS headers for feeder insertion
    raw_headers = ensure_headers(raw_ws, [
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
        "theme",
        "timestamp",
    ])

    # Load config
    try:
        config_ws = sh.worksheet("CONFIG")
        config_rows = load_records(config_ws)
    except Exception:
        config_rows = []

    try:
        themes_ws = sh.worksheet("CONFIG_THEME_DESTINATIONS")
        themes_rows = load_records(themes_ws)
    except Exception:
        themes_rows = []

    try:
        signals_ws = sh.worksheet("CONFIG_SIGNALS")
        signals_rows = load_records(signals_ws)
    except Exception:
        signals_rows = []

    # Origins
    try:
        origin_pools_ws = sh.worksheet("CONFIG_ORIGIN_POOLS")
        origin_rows = load_records(origin_pools_ws)
        origins = origins_from_origin_pools(origin_rows)
    except Exception:
        origins = []

    if not origins:
        origins = ["LHR", "LGW", "STN", "LTN", "MAN", "BRS", "EDI", "GLA", "BHX"]

    # Week plan + today theme
    week_plan = build_week_theme_plan(themes_rows)
    today_theme = pick_today_theme(week_plan)
    log(f"RUN_SLOT={run_slot} | Today theme: {today_theme}")

    # CONFIG route set
    config_route_set = set()
    for r in config_rows:
        if not is_trueish(r.get("enabled", True)):
            continue
        o = cell_upper(r.get("origin_iata"))
        d = cell_upper(r.get("destination_iata"))
        if is_iata3(o) and is_iata3(d):
            config_route_set.add((o, d))

    # Build route plan
    route_plan: List[Tuple[str, str, str, int, int]] = []

    if weekly_sweep and run_slot == "AM":
        log("🗓️ Weekly sweep enabled (AM). Building fallback plan across week.")
        route_plan = build_massive_weekly_fallback_routes(
            week_plan=week_plan,
            themes_rows=themes_rows,
            signals_rows=signals_rows,
            origins=origins,
            per_theme_cap=25,
        )

        # If sweep is thin, top up with route packs for today's theme
        if len(route_plan) < 10:
            cluster = pick_origin_cluster(run_slot)
            cluster_origins = [o for o in origins if cell_upper(o) in set(ORIGIN_CLUSTERS.get(cluster, []))]
            use_origins = cluster_origins if cluster_origins else origins
            rp = build_route_pack_fallback(today_theme, use_origins, config_route_set, cap=80)
            if rp:
                log(f"🧭 Final route-pack top-up for '{today_theme}' (cluster={cluster}) → +{len(rp)} routes")
                route_plan = route_plan + rp

    else:
        # Normal day: use CONFIG routes for today's theme if present
        for r in config_rows:
            if not is_trueish(r.get("enabled", True)):
                continue
            if cell_upper(r.get("theme")) != cell_upper(today_theme):
                continue
            o = cell_upper(r.get("origin_iata"))
            d = cell_upper(r.get("destination_iata"))
            if not (is_iata3(o) and is_iata3(d)):
                continue
            try:
                lead_days = int(float(r.get("lead_days") or 45))
            except Exception:
                lead_days = 45
            try:
                trip_days = int(float(r.get("trip_days") or 5))
            except Exception:
                trip_days = 5
            route_plan.append((o, d, cell_upper(today_theme), lead_days, trip_days))

        if not route_plan:
            log(f"⚠️ CONFIG empty for theme '{today_theme}' → using theme fallback")
            fb = build_massive_weekly_fallback_routes(
                week_plan=[today_theme],
                themes_rows=themes_rows,
                signals_rows=signals_rows,
                origins=origins,
                per_theme_cap=40,
            )
            route_plan = fb

            # If theme destinations/signals are weak, use curated route-pack fallback
            if not route_plan or len(route_plan) < 10:
                cluster = pick_origin_cluster(run_slot)
                cluster_origins = [o for o in origins if cell_upper(o) in set(ORIGIN_CLUSTERS.get(cluster, []))]
                use_origins = cluster_origins if cluster_origins else origins
                rp = build_route_pack_fallback(today_theme, use_origins, config_route_set, cap=60)
                if rp:
                    log(f"🧭 Route-pack fallback engaged for theme '{today_theme}' (cluster={cluster}) → {len(rp)} routes")
                    route_plan = (route_plan or []) + rp

    if not route_plan:
        log("❌ No routes available after CONFIG + fallback. Nothing to do.")
        return

    route_plan = route_plan[:routes_cap]

    # Run searches
    searches = 0
    published = 0

    allow_publish = env_str("ALLOW_PUBLISH", "true").strip().lower() in ("true", "1", "yes")

    for origin, dest, theme, lead_days, trip_days in route_plan:
        if searches >= searches_cap:
            break
        if published >= inserts_cap:
            break

        if not (is_iata3(origin) and is_iata3(dest)):
            log(f"⏭️ Skip invalid IATA route: {origin}->{dest}")
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

        # Insert cheapest offers up to inserts_cap
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
            if published >= inserts_cap:
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

            # deal_id stable-ish
            deal_id = f"{origin}{dest}{out_date.replace('-','')}{ret_date.replace('-','')}{int(price*100)}"
            deal_id = str(abs(hash(deal_id)))[:12]

            row = [""] * len(raw_headers)
            row[raw_headers["status"]] = "NEW"
            row[raw_headers["deal_id"]] = deal_id
            row[raw_headers["origin_iata"]] = origin
            row[raw_headers["destination_iata"]] = dest
            row[raw_headers["origin_city"]] = origin  # resolved later by render/publishers
            row[raw_headers["destination_city"]] = dest
            row[raw_headers["outbound_date"]] = out_date
            row[raw_headers["return_date"]] = ret_date
            row[raw_headers["price_gbp"]] = f"{price:.2f}"
            row[raw_headers["currency"]] = "GBP"
            row[raw_headers["stops"]] = str(stops)
            row[raw_headers["carriers"]] = carriers_str
            row[raw_headers["theme"]] = theme
            row[raw_headers["timestamp"]] = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

            if allow_publish:
                raw_ws.append_row(row, value_input_option="USER_ENTERED")
                published += 1
                log(f"✅ Inserted NEW deal {origin}->{dest} £{price:.2f} ({theme})")
            else:
                log(f"🧪 Dry-run: would insert {origin}->{dest} £{price:.2f} ({theme})")

    log(f"Done. searches={searches} inserted={published}")


if __name__ == "__main__":
    main()
