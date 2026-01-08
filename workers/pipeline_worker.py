#!/usr/bin/env python3
"""
TravelTxter V4.5.x — pipeline_worker.py (FEEDER + DISCOVERY) — PHASE 1 HARDENED

PHASE 1 GOAL (LOCKED):
- Always restore supply: write rows into RAW_DEALS every run when Duffel returns offers.
- NEVER write IATA codes into origin_city / destination_city (prevents Telegram/IG junk).
- If city/country cannot be resolved via CONFIG_SIGNALS, the row is still written but marked BANKED.
- Header-based writes only (safe across schema changes).
- Duffel-safe airport IATA only (no LON/NYC/PAR).

Reads tabs:
- CONFIG
- CONFIG_THEME_DESTINATIONS
- CONFIG_ORIGIN_POOLS
- CONFIG_SIGNALS

Writes tab:
- RAW_DEALS (append rows only)

Outputs:
- NEW rows (publish-eligible later)
- BANKED rows (learning buffer; never published)
"""

from __future__ import annotations

import os
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
# Env helpers
# ============================================================

def env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v.strip() if isinstance(v, str) and v.strip() else default

def env_int(name: str, default: int) -> int:
    v = env_str(name, "")
    if not v:
        return default
    try:
        return int(float(v))
    except Exception:
        return default


# ============================================================
# Google Sheets
# ============================================================

def gs_client() -> gspread.Client:
    sa = env_str("GCP_SA_JSON_ONE_LINE") or env_str("GCP_SA_JSON")
    if not sa:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE (or GCP_SA_JSON)")
    info = __import__("json").loads(sa)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def load_records(ws: gspread.Worksheet) -> List[Dict[str, Any]]:
    # get_all_records can coerce empty cells to NaN floats; our cell_* helpers handle it.
    return ws.get_all_records()

def ensure_headers(ws: gspread.Worksheet, required: List[str]) -> Dict[str, int]:
    values = ws.get_all_values()
    headers = values[0] if values else []
    headers = [h.strip() for h in headers]
    existing = set(headers)
    missing = [h for h in required if h not in existing]
    if missing:
        ws.update([headers + missing], "A1")
        headers = headers + missing
        log(f"🛠️ Added missing RAW_DEALS columns: {missing}")
    return {h: i for i, h in enumerate(headers)}


# ============================================================
# Signals (IATA -> City/Country)
# ============================================================

UK_AIRPORT_CITY_FALLBACK = {
    "LHR": "London", "LGW": "London", "STN": "London", "LTN": "London", "LCY": "London", "SEN": "London",
    "MAN": "Manchester", "BRS": "Bristol", "BHX": "Birmingham", "EDI": "Edinburgh", "GLA": "Glasgow",
    "NCL": "Newcastle", "LPL": "Liverpool", "NQY": "Newquay", "SOU": "Southampton", "CWL": "Cardiff", "EXT": "Exeter",
}

def _pick_header(headers: List[str], *names: str) -> Optional[int]:
    idx = {h.strip(): i for i, h in enumerate(headers)}
    for n in names:
        if n in idx:
            return idx[n]
    return None

def load_signals_maps(sh: gspread.Spreadsheet) -> Tuple[Dict[str, str], Dict[str, str]]:
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
    # OK to use UK fallback (but NEVER return raw IATA as a city value)
    return iata_to_city.get(o) or UK_AIRPORT_CITY_FALLBACK.get(o) or ""

def resolve_dest_city_country(dest_iata: str, iata_to_city: Dict[str, str], iata_to_country: Dict[str, str]) -> Tuple[str, str]:
    d = cell_upper(dest_iata)
    if not is_iata3(d):
        return "", ""
    return (iata_to_city.get(d, ""), iata_to_country.get(d, ""))


# ============================================================
# Themes + fallback route packs (Duffel-safe)
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

def build_week_theme_plan(themes_rows: List[Dict[str, Any]]) -> List[str]:
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
    idx = dt.datetime.utcnow().weekday()  # Mon=0
    try:
        return cell_upper(week_plan[idx])
    except Exception:
        return "CITY_BREAKS"

def pick_dates_for_theme(theme: str, avg_lead_days: int, avg_trip_days: int) -> Tuple[str, str]:
    lead_jitter = random.randint(-10, 15)
    trip_jitter = random.randint(-1, 3)
    out = today_utc() + dt.timedelta(days=max(10, avg_lead_days + lead_jitter))
    ret = out + dt.timedelta(days=max(2, avg_trip_days + trip_jitter))
    return out.isoformat(), ret.isoformat()

def origins_from_origin_pools(rows: List[Dict[str, Any]]) -> List[str]:
    out: List[str] = []
    for r in rows:
        if not is_trueish(r.get("enabled", True)):
            continue
        o = cell_upper(r.get("origin_iata"))
        if is_iata3(o):
            out.append(o)
    return list(dict.fromkeys(out))

def theme_destinations(rows: List[Dict[str, Any]], theme: str) -> List[Dict[str, Any]]:
    t = cell_upper(theme)
    out: List[Dict[str, Any]] = []
    for r in rows:
        if cell_upper(r.get("theme")) != t:
            continue
        if not is_trueish(r.get("enabled", True)):
            continue
        di = cell_upper(r.get("destination_iata"))
        if not is_iata3(di):
            continue
        out.append(r)

    def keyfun(rr: Dict[str, Any]) -> Tuple[int, str]:
        pr = rr.get("priority", 9999)
        try:
            pr_i = int(float(pr))
        except Exception:
            pr_i = 9999
        return (pr_i, cell_upper(rr.get("destination_iata")))

    out.sort(key=keyfun)
    return out

def build_route_pack_fallback(theme: str, origins: List[str], cap: int) -> List[Tuple[str, str, str, int, int]]:
    t = normalize_theme_name(theme)
    pack = ROUTE_PACKS.get(t)
    if not pack:
        return []
    dd = THEME_DEFAULTS.get(t, {"lead_days": 45, "trip_days": 5})
    lead_days = int(dd["lead_days"])
    trip_days = int(dd["trip_days"])

    origins = [o for o in origins if is_iata3(o)]
    if not origins:
        return []

    out: List[Tuple[str, str, str, int, int]] = []
    for o in origins:
        for d in pack["dests"]:
            if o == d:
                continue
            out.append((o, d, t, lead_days, trip_days))
            if len(out) >= cap:
                return out
    return out


# ============================================================
# Duffel
# ============================================================

DUFFEL_API = "https://api.duffel.com/air/offer_requests"

def duffel_headers() -> Dict[str, str]:
    key = env_str("DUFFEL_API_KEY")
    if not key:
        raise RuntimeError("Missing DUFFEL_API_KEY")
    return {
        "Authorization": f"Bearer {key}",
        "Duffel-Version": "v2",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

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
# Main
# ============================================================

def main() -> None:
    spreadsheet_id = env_str("SPREADSHEET_ID") or env_str("SHEET_ID")
    raw_tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")

    run_slot = env_str("RUN_SLOT", "AM").upper()
    weekly_sweep = env_str("WEEKLY_THEME_SWEEP", "true").strip().lower() in ("true", "1", "yes")
    allow_write = env_str("ALLOW_PUBLISH", "true").strip().lower() in ("true", "1", "yes")

    routes_cap = env_int("DUFFEL_ROUTES_PER_RUN", 4)
    searches_cap = env_int("DUFFEL_MAX_SEARCHES_PER_RUN", 4)
    inserts_cap = env_int("DUFFEL_MAX_INSERTS", 3)

    gc = gs_client()
    sh = gc.open_by_key(spreadsheet_id)
    raw_ws = sh.worksheet(raw_tab)

    h = ensure_headers(raw_ws, [
        "status", "deal_id", "origin_iata", "destination_iata",
        "origin_city", "destination_city", "destination_country",
        "outbound_date", "return_date", "price_gbp", "currency",
        "stops", "carriers", "deeplink", "deal_theme", "timestamp",
        "banked_utc", "reason_banked",
    ])

    sig_city, sig_country = load_signals_maps(sh)

    try:
        theme_rows = load_records(sh.worksheet("CONFIG_THEME_DESTINATIONS"))
    except Exception:
        theme_rows = []

    try:
        origin_rows = load_records(sh.worksheet("CONFIG_ORIGIN_POOLS"))
        origins = origins_from_origin_pools(origin_rows)
    except Exception:
        origins = []

    if not origins:
        origins = ["LHR", "LGW", "STN", "LTN", "MAN", "BRS", "EDI", "GLA", "BHX"]

    week_plan = build_week_theme_plan(theme_rows)
    today_theme = pick_today_theme(week_plan)
    log(f"RUN_SLOT={run_slot} | Today theme: {today_theme}")

    route_plan: List[Tuple[str, str, str, int, int]] = []

    dest_rows = theme_destinations(theme_rows, today_theme)
    if dest_rows:
        dd = THEME_DEFAULTS.get(normalize_theme_name(today_theme), {"lead_days": 45, "trip_days": 5})
        for o in origins:
            for rr in dest_rows:
                d = cell_upper(rr.get("destination_iata"))
                if not (is_iata3(o) and is_iata3(d)) or o == d:
                    continue
                lead_days = int(float(rr.get("lead_days") or dd["lead_days"])) if cell_str(rr.get("lead_days")) else int(dd["lead_days"])
                trip_days = int(float(rr.get("trip_days") or dd["trip_days"])) if cell_str(rr.get("trip_days")) else int(dd["trip_days"])
                route_plan.append((cell_upper(o), d, normalize_theme_name(today_theme), lead_days, trip_days))
    else:
        log(f"⚠️ No CONFIG_THEME_DESTINATIONS rows for '{today_theme}' → using ROUTE_PACKS fallback")

    if len(route_plan) < routes_cap:
        route_plan += build_route_pack_fallback(today_theme, origins, cap=40)

    if weekly_sweep and run_slot == "AM":
        for t in week_plan:
            if t == today_theme:
                continue
            route_plan += build_route_pack_fallback(t, origins, cap=12)

    cleaned: List[Tuple[str, str, str, int, int]] = []
    seen: Set[Tuple[str, str, str]] = set()
    for o, d, t, ld, td in route_plan:
        o, d, t = cell_upper(o), cell_upper(d), normalize_theme_name(t)
        if not (is_iata3(o) and is_iata3(d)) or o == d:
            continue
        k = (o, d, t)
        if k in seen:
            continue
        seen.add(k)
        cleaned.append((o, d, t, int(ld), int(td)))

    if not cleaned:
        log("❌ No routes available after CONFIG + fallback. Nothing to do.")
        return

    cleaned = cleaned[:routes_cap]
    log(f"Route plan size: {len(cleaned)} (cap={routes_cap})")

    searches = 0
    wrote = 0
    wrote_new = 0
    wrote_banked = 0
    skipped_no_offers = 0
    skipped_dupes = 0

    seen_run: Set[Tuple[str, str, str, str, str]] = set()

    for origin, dest, theme, lead_days, trip_days in cleaned:
        if searches >= searches_cap or wrote >= inserts_cap:
            break

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
            skipped_no_offers += 1
            log(f"⏭️ No offers: {origin}->{dest} {out_date}/{ret_date}")
            continue

        offers_sorted: List[Tuple[float, Dict[str, Any]]] = []
        for off in offers:
            total = (off.get("total_amount") or "").strip()
            try:
                price = float(total)
            except Exception:
                continue
            offers_sorted.append((price, off))
        offers_sorted.sort(key=lambda x: x[0])

        for price, off in offers_sorted:
            if wrote >= inserts_cap:
                break

            dk = (origin, dest, out_date, ret_date, f"{price:.2f}")
            if dk in seen_run:
                skipped_dupes += 1
                continue
            seen_run.add(dk)

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

            origin_city = resolve_origin_city(origin, sig_city)
            dest_city, dest_country = resolve_dest_city_country(dest, sig_city, sig_country)

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
            setv("origin_city", origin_city)          # blank is OK
            setv("destination_city", dest_city)       # IMPORTANT: never IATA fallback
            setv("destination_country", dest_country) # blank is OK
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

            if allow_write:
                raw_ws.append_row(row, value_input_option="USER_ENTERED")
                wrote += 1
                if status == "NEW":
                    wrote_new += 1
                else:
                    wrote_banked += 1
                log(f"✅ Inserted {status}: {origin}->{dest} £{price:.2f} ({theme})")
            else:
                log(f"🧪 Dry-run: would insert {status}: {origin}->{dest} £{price:.2f} ({theme})")

    log(
        "Done. "
        f"searches={searches} "
        f"inserted_total={wrote} "
        f"inserted_new={wrote_new} "
        f"inserted_banked={wrote_banked} "
        f"skipped_no_offers={skipped_no_offers} "
        f"skipped_dupes={skipped_dupes}"
    )


if __name__ == "__main__":
    main()
