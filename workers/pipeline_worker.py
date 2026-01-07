#!/usr/bin/env python3
"""
TravelTxter V4.5x — PIPELINE WORKER (FEEDER + DISCOVERY) — WEEKLY THEME SWEEP FALLBACK

FIX INCLUDED:
- Robustly handles Google Sheets NaN / floats from get_all_records()
  (prevents: AttributeError: 'float' object has no attribute 'strip')

YOU ASKED FOR:
- Massive fallback (strong enough to be real pipeline)
- REAL IATA ONLY (Duffel-safe) — never LON/PAR/NYC etc
- Theme-aware
- Weekly sweep across all themes (Mon→Sun)
- Insert NEW deals when eligible, otherwise bank to DISCOVERY_BANK

SAFEGUARDS:
- NEVER clears sheets
- Adds missing columns only (appends)
"""

from __future__ import annotations

import os
import json
import time
import uuid
import random
import math
import datetime as dt
from typing import Dict, List, Tuple, Set, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# Logging
# ============================================================

def log(msg: str):
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


# ============================================================
# Safe cell handling (CRITICAL)
# ============================================================

def cell_str(v) -> str:
    """
    Google Sheets via get_all_records() can yield floats (NaN) for blanks.
    This converts any cell value safely to a clean string.
    """
    if v is None:
        return ""
    if isinstance(v, float):
        if math.isnan(v):
            return ""
        # avoid "123.0" when it was an integer in Sheets
        if v.is_integer():
            return str(int(v))
        return str(v)
    return str(v)


def cell_upper(v) -> str:
    return cell_str(v).strip().upper()


def cell_lower(v) -> str:
    return cell_str(v).strip().lower()


# ============================================================
# Auth / Sheets
# ============================================================

def _parse_sa_json(raw: str) -> Dict:
    raw = (raw or "").strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return json.loads(raw.replace("\\n", "\n"))


def get_client():
    raw = os.environ.get("GCP_SA_JSON_ONE_LINE", "")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE")
    info = _parse_sa_json(raw)
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds)


def get_ws(sh, title: str):
    return sh.worksheet(title)


def get_or_create_ws(sh, title: str, rows: int = 2000, cols: int = 30):
    try:
        return sh.worksheet(title)
    except Exception:
        ws = sh.add_worksheet(title=title, rows=rows, cols=cols)
        return ws


def ensure_columns(ws, required_cols: List[str]) -> Dict[str, int]:
    """
    Ensures required columns exist; appends missing to the end.
    Returns header index map (0-based).
    """
    headers = ws.row_values(1)
    if not headers:
        ws.update([required_cols], "A1")
        headers = required_cols[:]
        log(f"🛠️ Initialised headers for {ws.title} (blank sheet)")

    existing = [h.strip() for h in headers]
    missing = [c for c in required_cols if c not in existing]
    if missing:
        new_headers = existing + missing
        ws.update([new_headers], "A1")
        log(f"🛠️ Added missing columns to {ws.title}: {missing}")
        existing = new_headers

    return {h: i for i, h in enumerate(existing)}


# ============================================================
# Duffel
# ============================================================

DUFFEL_API = "https://api.duffel.com/air/offer_requests"


def duffel_headers():
    return {
        "Authorization": f"Bearer {os.environ['DUFFEL_API_KEY']}",
        "Duffel-Version": "v2",
        "Content-Type": "application/json",
    }


def duffel_search(origin: str, dest: str, out_date: str, ret_date: str):
    payload = {
        "data": {
            "slices": [
                {"origin": origin, "destination": dest, "departure_date": out_date},
                {"origin": dest, "destination": origin, "departure_date": ret_date},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": "economy",
        }
    }
    r = requests.post(DUFFEL_API, headers=duffel_headers(), json=payload, timeout=45)
    r.raise_for_status()
    return r.json()["data"]["offers"]


# ============================================================
# Helpers
# ============================================================

def today_utc() -> dt.date:
    return dt.datetime.utcnow().date()


def pick_dates(avg_lead_days: int = 45, avg_trip_days: int = 5) -> Tuple[str, str]:
    lead_jitter = random.randint(-10, 15)
    trip_jitter = random.randint(-1, 3)
    out = today_utc() + dt.timedelta(days=max(10, avg_lead_days + lead_jitter))
    ret = out + dt.timedelta(days=max(2, avg_trip_days + trip_jitter))
    return out.isoformat(), ret.isoformat()


def norm(s: str) -> str:
    return (s or "").strip().lower().replace(" ", "_")


def is_trueish(v) -> bool:
    if v is None:
        return True
    return cell_lower(v) not in ("false", "0", "no", "off")


def is_iata3(code: str) -> bool:
    c = cell_upper(code)
    return len(c) == 3 and c.isalpha()


def safe_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        s = cell_str(x).strip().replace("£", "").replace(",", "")
        if not s:
            return None
        return float(s)
    except Exception:
        return None


# ============================================================
# Load tabs
# ============================================================

def load_records(ws) -> List[Dict]:
    return ws.get_all_records()


# ============================================================
# Weekly theme plan + massive fallback
# ============================================================

def build_week_theme_plan(themes_rows: List[Dict]) -> List[str]:
    by_theme: Dict[str, int] = {}
    for r in themes_rows:
        t = norm(cell_str(r.get("theme")))
        if not t:
            continue
        pr = r.get("priority", 9999)
        try:
            pr_i = int(float(pr))
        except Exception:
            pr_i = 9999
        by_theme[t] = min(by_theme.get(t, 9999), pr_i)

    if not by_theme:
        return ["city_break"] * 7

    ordered = sorted(by_theme.keys(), key=lambda k: (by_theme[k], k))
    plan = []
    i = 0
    while len(plan) < 7:
        plan.append(ordered[i % len(ordered)])
        i += 1
    return plan


def pick_today_theme(week_plan: List[str]) -> str:
    dow = dt.datetime.utcnow().weekday()  # Mon=0..Sun=6
    return week_plan[dow] if week_plan else "city_break"


def theme_destinations(themes_rows: List[Dict], theme: str) -> List[Dict]:
    t = norm(theme)
    out = []
    for r in themes_rows:
        if norm(cell_str(r.get("theme"))) != t:
            continue
        d = cell_upper(r.get("destination_iata"))
        if is_iata3(d):
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
    """
    m = {}
    for r in signals_rows:
        iata = cell_upper(r.get("iata_hint"))
        if is_iata3(iata):
            m[iata] = r
    return m


def origins_from_origin_pools(origin_rows: List[Dict]) -> List[str]:
    origins = []
    for r in origin_rows:
        o = cell_upper(r.get("origin_iata"))
        if is_iata3(o):
            origins.append(o)

    def pri_for(o: str) -> int:
        best = 0
        for r in origin_rows:
            if cell_upper(r.get("origin_iata")) == o:
                try:
                    best = max(best, int(float(r.get("priority", 0))))
                except Exception:
                    pass
        return best

    origins = sorted(list(set(origins)), key=lambda o: (-pri_for(o), o))
    return origins


def config_routes_for_theme(config_rows: List[Dict], theme: str) -> List[Tuple[str, str]]:
    t = norm(theme)
    routes = []
    for r in config_rows:
        if not is_trueish(r.get("enabled", True)):
            continue
        if norm(cell_str(r.get("theme"))) != t:
            continue
        o = cell_upper(r.get("origin_iata"))
        d = cell_upper(r.get("destination_iata"))
        if is_iata3(o) and is_iata3(d):
            routes.append((o, d))

    def pr_for(rt: Tuple[str, str]) -> int:
        o, d = rt
        pr = 9999
        for r in config_rows:
            if cell_upper(r.get("origin_iata")) == o and cell_upper(r.get("destination_iata")) == d:
                try:
                    pr = int(float(r.get("priority", 9999)))
                except Exception:
                    pr = 9999
                break
        return pr

    routes.sort(key=lambda rt: (pr_for(rt), rt[0], rt[1]))
    return routes


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
            for o in olist:
                if o == d:
                    continue
                chosen_origins.append(o)
                if len(chosen_origins) >= 2:
                    break

            for o in chosen_origins:
                key = (o, d, t)
                if key in seen:
                    continue
                seen.add(key)

                lead = 45
                trip = 5
                for rr in dest_rows:
                    if cell_upper(rr.get("destination_iata")) == d:
                        try:
                            lead = int(float(rr.get("booking_lead_days", lead)))
                        except Exception:
                            pass
                        try:
                            trip = int(float(rr.get("avg_trip_days", trip)))
                        except Exception:
                            pass
                        break

                routes.append((o, d, t, lead, trip))

    return routes


# ============================================================
# Publish eligibility + discovery reasons
# ============================================================

def publish_reason(
    origin: str,
    dest: str,
    theme: str,
    config_route_set: Set[Tuple[str, str]],
    theme_dest_set: Set[str],
    currency: str,
    price: float,
    price_max_gbp: float,
) -> Optional[str]:
    if (origin, dest) not in config_route_set:
        return "outside_config"
    if dest not in theme_dest_set:
        return "outside_theme"
    if currency != "GBP":
        return "non_gbp"
    if price > price_max_gbp:
        return "too_expensive"
    return None


# ============================================================
# Main
# ============================================================

def main():
    # Controls
    routes_cap = int(os.environ.get("DUFFEL_ROUTES_PER_RUN", "6"))
    searches_cap = int(os.environ.get("DUFFEL_MAX_SEARCHES_PER_RUN", "4"))
    inserts_cap = int(os.environ.get("DUFFEL_MAX_INSERTS", "3"))

    price_max_gbp = float(os.environ.get("PRICE_MAX_GBP", "300"))

    weekly_sweep = os.environ.get("WEEKLY_THEME_SWEEP", "true").strip().lower() not in ("false", "0", "no")
    run_slot = os.environ.get("RUN_SLOT", "AM").strip().upper()

    gc = get_client()
    sh = gc.open_by_key(os.environ["SPREADSHEET_ID"])

    raw_tab = os.environ.get("RAW_DEALS_TAB", "RAW_DEALS")
    raw_ws = get_ws(sh, raw_tab)
    disc_ws = get_or_create_ws(sh, "DISCOVERY_BANK", rows=5000, cols=30)

    config_ws = get_ws(sh, "CONFIG")
    themes_ws = get_ws(sh, "THEMES")
    signals_ws = get_ws(sh, "CONFIG_SIGNALS")

    config_rows = load_records(config_ws)
    themes_rows = load_records(themes_ws)
    signals_rows = load_records(signals_ws)

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

    # Ensure schemas (non-destructive)
    DISC_COLS = [
        "found_at_utc",
        "origin_iata",
        "destination_iata",
        "destination_city",
        "destination_country",
        "outbound_date",
        "return_date",
        "price",
        "currency",
        "stops",
        "carrier_codes",
        "raw_theme_guess",
        "reason_flag",
        "search_context",
    ]
    ensure_columns(disc_ws, DISC_COLS)

    RAW_REQUIRED = [
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
        "created_utc",
    ]
    ensure_columns(raw_ws, RAW_REQUIRED)

    # Signals lookup
    sig = signals_map(signals_rows)

    # Theme destinations set
    def theme_dest_set(theme: str) -> Set[str]:
        return set(cell_upper(r.get("destination_iata")) for r in theme_destinations(themes_rows, theme))

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
        log(f"🗓️ Weekly theme sweep enabled (AM). Week plan (Mon→Sun): {week_plan}")
        fallback_routes = build_massive_weekly_fallback_routes(
            week_plan=week_plan,
            themes_rows=themes_rows,
            signals_rows=signals_rows,
            origins=origins,
            per_theme_cap=25,
        )

        rnd = random.Random(dt.datetime.utcnow().isocalendar().week)
        rnd.shuffle(fallback_routes)
        route_plan = fallback_routes[: max(routes_cap, searches_cap)]
        log(f"🧰 Massive fallback pool built. Candidate routes: {len(fallback_routes)}. Using: {len(route_plan)}")

    else:
        log(f"🎯 Theme selected for run: {today_theme}")
        cfg_routes = config_routes_for_theme(config_rows, today_theme)

        if cfg_routes:
            log(f"✅ Using CONFIG routes for theme '{today_theme}': {len(cfg_routes)}")
            lead = 45
            trip = 5
            trows = theme_destinations(themes_rows, today_theme)
            if trows:
                try:
                    lead = int(float(trows[0].get("booking_lead_days", lead)))
                except Exception:
                    pass
                try:
                    trip = int(float(trows[0].get("avg_trip_days", trip)))
                except Exception:
                    pass
            route_plan = [(o, d, today_theme, lead, trip) for (o, d) in cfg_routes]
        else:
            log(f"⚠️ CONFIG empty for theme '{today_theme}' → using theme fallback")
            fb = build_massive_weekly_fallback_routes(
                week_plan=[today_theme],
                themes_rows=themes_rows,
                signals_rows=signals_rows,
                origins=origins,
                per_theme_cap=40,
            )
            route_plan = fb

    if not route_plan:
        log("❌ No routes available after CONFIG + fallback. Nothing to do.")
        return

    route_plan = route_plan[:routes_cap]
    log(f"🧭 Routes planned this run: {len(route_plan)} (DUFFEL_ROUTES_PER_RUN={routes_cap})")

    published = 0
    banked = 0
    searches = 0

    fallback_can_publish = os.environ.get("FALLBACK_CAN_PUBLISH", "false").strip().lower() in ("true", "1", "yes")

    for origin, dest, theme, lead_days, trip_days in route_plan:
        if searches >= searches_cap:
            break
        if published >= inserts_cap:
            break

        if not (is_iata3(origin) and is_iata3(dest)):
            log(f"⏭️ Skip invalid IATA route: {origin}->{dest}")
            continue

        out_date, ret_date = pick_dates(avg_lead_days=lead_days, avg_trip_days=trip_days)
        log(f"✈️ Duffel search: {origin}->{dest} ({theme}) {out_date}/{ret_date}")

        try:
            offers = duffel_search(origin, dest, out_date, ret_date)
            searches += 1
        except Exception as e:
            log(f"❌ Duffel error {origin}->{dest}: {e}")
            continue

        if not offers:
            continue

        for off in offers[:3]:
            price = safe_float(off.get("total_amount"))
            currency = (off.get("total_currency") or "").strip().upper()
            if price is None:
                continue

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
            carrier_codes = ",".join(sorted(list(carriers)))

            sigrow = sig.get(dest, {})
            dest_city = cell_str(sigrow.get("destination_city")).strip()
            dest_country = cell_str(sigrow.get("destination_country")).strip()

            t_dest_set = theme_dest_set(theme)

            reason = publish_reason(
                origin=origin,
                dest=dest,
                theme=theme,
                config_route_set=config_route_set,
                theme_dest_set=t_dest_set,
                currency=currency,
                price=price,
                price_max_gbp=price_max_gbp,
            )

            if fallback_can_publish and reason in ("outside_config", "outside_theme"):
                if currency == "GBP" and price <= price_max_gbp and dest in t_dest_set:
                    reason = None

            if reason is None and published < inserts_cap:
                raw_ws.append_row([
                    "NEW",
                    uuid.uuid4().hex[:12],
                    price,
                    origin,
                    dest,
                    origin,  # origin_city fallback
                    dest_city or dest,
                    dest_country or "",
                    out_date,
                    ret_date,
                    stops,
                    theme,
                    dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
                ])
                published += 1
            else:
                if reason not in ("outside_config", "outside_theme", "non_gbp", "too_expensive", "duplicate_candidate"):
                    reason = "duplicate_candidate"

                disc_ws.append_row([
                    dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
                    origin,
                    dest,
                    dest_city or dest,
                    dest_country or "",
                    out_date,
                    ret_date,
                    price,
                    currency,
                    stops,
                    carrier_codes,
                    theme,
                    reason,
                    f"run_slot:{run_slot}|week:{dt.datetime.utcnow().isocalendar().week}|theme:{theme}",
                ])
                banked += 1

    log(f"✅ Done. searches={searches} published={published} banked={banked} (caps: searches={searches_cap}, inserts={inserts_cap})")


if __name__ == "__main__":
    main()
