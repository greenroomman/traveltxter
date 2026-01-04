#!/usr/bin/env python3
"""
Traveltxter V4.5.3 — Pipeline Worker (Post-Waterwheel, Canonical Schema Fix)

This version FIXES the contract mismatch that caused:
- deals being inserted but no scoring/rendering/publishing
- link_router "Updated 0 rows"

Root cause: feeder was writing legacy fields (origin/dest/out_date/price/deep_link)
instead of canonical fields used by the rest of the pipeline:
- origin_iata, destination_iata, outbound_date, return_date, price_gbp, affiliate_url, deal_id, status

This file:
- Writes CANONICAL columns (and optionally legacy tail fields for debugging)
- Keeps Duffel-Version: v2
- Keeps gspread v6 safe update wrappers
- Keeps dedupe + "force 1 search" safety
- Keeps budget governor + DUFFEL_BUDGET and DUFFEL_SEARCH_LOG tabs

No pipeline redesign.
"""

import os
import json
import random
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# Logging / Time
# ============================================================

def now_utc_dt() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def now_utc_str() -> str:
    return now_utc_dt().replace(microsecond=0).isoformat().replace("+00:00", "Z")

def log(msg: str) -> None:
    print(f"{now_utc_str()} | {msg}", flush=True)

def month_key_utc() -> str:
    d = now_utc_dt().date()
    return f"{d.year:04d}-{d.month:02d}"


# ============================================================
# Env helpers
# ============================================================

def env_str(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()

def env_int(name: str, default: int) -> int:
    v = env_str(name, "")
    try:
        return int(v) if v != "" else default
    except Exception:
        return default

def env_float(name: str, default: float) -> float:
    v = env_str(name, "")
    try:
        return float(v) if v != "" else default
    except Exception:
        return default

def env_bool(name: str, default: bool = False) -> bool:
    v = env_str(name, "")
    if v == "":
        return default
    return v.lower() in ("1", "true", "yes", "y", "on")


# ============================================================
# gspread v6 safe updates
# ============================================================

def a1_update(ws: gspread.Worksheet, a1: str, value: Any) -> None:
    # Stable signature for gspread v6: update(matrix, range)
    ws.update([[value]], a1)

def a1_row_update(ws: gspread.Worksheet, a1_range: str, row_values: List[Any]) -> None:
    ws.update([row_values], a1_range)


# ============================================================
# Google Sheets auth
# ============================================================

def get_gspread_client() -> gspread.Client:
    sa_json = env_str("GCP_SA_JSON_ONE_LINE")
    if not sa_json:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE")

    try:
        info = json.loads(sa_json)
    except Exception as e:
        raise RuntimeError(f"GCP_SA_JSON_ONE_LINE is not valid JSON: {e}")

    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)


# ============================================================
# Canonical schema helpers
# ============================================================

CANONICAL_MIN_COLS = [
    "deal_id",
    "origin_iata",
    "destination_iata",
    "outbound_date",
    "return_date",
    "price_gbp",
    "affiliate_url",
    "status",
]

CANONICAL_NICE_COLS = [
    "origin_city",
    "destination_city",
    "destination_country",
    "trip_length_days",
    "stops",
    "airline",
    "theme",
    "deal_source",
    "date_added",
]

# Optional legacy/debug tail fields (safe to keep; downstream ignores them)
LEGACY_DEBUG_COLS = [
    "origin",
    "dest",
    "out_date",
    "ret_date",
    "price",
    "currency",
    "deep_link",
    "created_utc",
]


def ensure_columns(ws: gspread.Worksheet, required: List[str]) -> Dict[str, int]:
    """
    Ensures required columns exist in header row.
    Returns header_map: col_name -> index.
    """
    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("RAW_DEALS_TAB has no header row")

    changed = False
    for c in required:
        if c not in headers:
            headers.append(c)
            changed = True

    if changed:
        ws.update([headers], "A1")

    return {h: i for i, h in enumerate(headers)}


# Small, safe IATA->(city,country) fallback map.
# Extend over time; blanks are fine (do not block pipeline).
IATA_MAP: Dict[str, Tuple[str, str]] = {
    # UK / SW England bias origins
    "BRS": ("Bristol", "United Kingdom"),
    "EXT": ("Exeter", "United Kingdom"),
    "NQY": ("Newquay", "United Kingdom"),
    "SOU": ("Southampton", "United Kingdom"),
    "CWL": ("Cardiff", "United Kingdom"),
    "BOH": ("Bournemouth", "United Kingdom"),
    "MAN": ("Manchester", "United Kingdom"),
    "LHR": ("London", "United Kingdom"),
    "LGW": ("London", "United Kingdom"),

    # Common short-haul
    "BCN": ("Barcelona", "Spain"),
    "AGP": ("Málaga", "Spain"),
    "PMI": ("Palma", "Spain"),
    "FAO": ("Faro", "Portugal"),
    "LIS": ("Lisbon", "Portugal"),
    "OPO": ("Porto", "Portugal"),
    "TFS": ("Tenerife", "Spain"),
    "GVA": ("Geneva", "Switzerland"),
    "AMS": ("Amsterdam", "Netherlands"),
    "DUB": ("Dublin", "Ireland"),
    "CDG": ("Paris", "France"),
    "ORY": ("Paris", "France"),
    "FCO": ("Rome", "Italy"),
    "MXP": ("Milan", "Italy"),
    "ATH": ("Athens", "Greece"),
}


def convert_to_gbp(amount_str: str, currency: str) -> Optional[float]:
    """
    Convert Duffel total_amount/total_currency to GBP.

    - If currency == GBP: direct
    - Supports EUR and USD via env overrides:
        FX_EUR_TO_GBP (default 0.86)
        USD_TO_GBP (default 0.79) or FX_USD_TO_GBP
    - If unknown currency, returns None (skip offer)
    """
    try:
        amt = float(amount_str)
    except Exception:
        return None

    cur = (currency or "").upper().strip()
    if cur == "GBP":
        return round(amt, 2)

    if cur == "EUR":
        fx = env_float("FX_EUR_TO_GBP", 0.86)
        return round(amt * fx, 2)

    if cur == "USD":
        fx = env_float("FX_USD_TO_GBP", env_float("USD_TO_GBP", 0.79))
        return round(amt * fx, 2)

    return None


def trip_length_days(out_date: str, ret_date: str) -> Optional[int]:
    try:
        o = dt.date.fromisoformat(out_date)
        r = dt.date.fromisoformat(ret_date)
        return (r - o).days
    except Exception:
        return None


# ============================================================
# Budget tabs + search log tabs
# ============================================================

def ensure_budget_tabs(spread: gspread.Spreadsheet) -> Tuple[gspread.Worksheet, gspread.Worksheet]:
    try:
        budget_ws = spread.worksheet("DUFFEL_BUDGET")
    except Exception:
        budget_ws = spread.add_worksheet("DUFFEL_BUDGET", rows=200, cols=12)
        budget_ws.update([[
            "month", "orders_this_month", "free_searches_per_order",
            "searches_this_month", "budget_gbp", "excess_search_usd",
            "usd_to_gbp", "est_cost_gbp"
        ]], "A1")

    try:
        log_ws = spread.worksheet("DUFFEL_SEARCH_LOG")
    except Exception:
        log_ws = spread.add_worksheet("DUFFEL_SEARCH_LOG", rows=2000, cols=12)
        log_ws.update([[
            "ts_utc", "origin", "dest", "out_date", "ret_date",
            "theme", "key", "skipped_dedupe", "searched", "offers_count"
        ]], "A1")

    return budget_ws, log_ws


def get_budget_row_index(budget_ws: gspread.Worksheet, month: str) -> Optional[int]:
    rows = budget_ws.get_all_values()
    if len(rows) <= 1:
        return None
    for i in range(1, len(rows)):
        if rows[i] and rows[i][0].strip() == month:
            return i + 1
    return None


def est_monthly_cost_gbp(
    searches_this_month: int,
    orders_this_month: int,
    free_searches_per_order: int,
    excess_search_usd: float,
    usd_to_gbp: float,
) -> float:
    free = max(0, orders_this_month * free_searches_per_order)
    paid = max(0, searches_this_month - free)
    return paid * (excess_search_usd * usd_to_gbp)


def budget_commit_searches(
    budget_ws: gspread.Worksheet,
    month: str,
    searches_add: int,
    orders_this_month: int,
    free_searches_per_order: int,
    budget_gbp: float,
    excess_search_usd: float,
    usd_to_gbp: float,
) -> float:
    row_i = get_budget_row_index(budget_ws, month)

    if row_i is None:
        searches_this_month = searches_add
        est_cost = est_monthly_cost_gbp(searches_this_month, orders_this_month, free_searches_per_order, excess_search_usd, usd_to_gbp)
        budget_ws.append_row([
            month, str(orders_this_month), str(free_searches_per_order),
            str(searches_this_month), str(budget_gbp), str(excess_search_usd),
            str(usd_to_gbp), f"{est_cost:.2f}"
        ])
        return est_cost

    try:
        searches_this_month = int(budget_ws.cell(row_i, 4).value or "0")
    except Exception:
        searches_this_month = 0

    searches_this_month += searches_add
    est_cost = est_monthly_cost_gbp(searches_this_month, orders_this_month, free_searches_per_order, excess_search_usd, usd_to_gbp)

    a1_update(budget_ws, f"D{row_i}", str(searches_this_month))
    a1_update(budget_ws, f"H{row_i}", f"{est_cost:.2f}")
    a1_update(budget_ws, f"B{row_i}", str(orders_this_month))
    a1_update(budget_ws, f"C{row_i}", str(free_searches_per_order))
    a1_update(budget_ws, f"E{row_i}", str(budget_gbp))
    a1_update(budget_ws, f"F{row_i}", str(excess_search_usd))
    a1_update(budget_ws, f"G{row_i}", str(usd_to_gbp))

    return est_cost


# ============================================================
# Duffel API (v2)
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

    offer_request_id = r.json()["data"]["id"]

    url2 = f"{DUFFEL_API_BASE}/air/offers"
    params = {"offer_request_id": offer_request_id, "limit": 50}
    r2 = requests.get(url2, headers=duffel_headers(api_key), params=params, timeout=40)
    if r2.status_code >= 300:
        raise RuntimeError(f"Duffel offers failed: {r2.status_code} {r2.text[:400]}")

    return r2.json().get("data", [])


# ============================================================
# Skyscanner fallback link
# ============================================================

def build_skyscanner_url(origin: str, dest: str, out_date: str, ret_date: str) -> str:
    base = "https://www.skyscanner.net/transport/flights"
    return f"{base}/{origin}/{dest}/{out_date.replace('-','')}/{ret_date.replace('-','')}/"


# ============================================================
# Dedupe via DUFFEL_SEARCH_LOG
# ============================================================

def dedupe_key(origin: str, dest: str, out_date: str, ret_date: str) -> str:
    return f"{origin}|{dest}|{out_date}|{ret_date}"

def was_recently_searched(log_ws: gspread.Worksheet, key: str, dedupe_hours: int) -> bool:
    if dedupe_hours <= 0:
        return False
    try:
        rows = log_ws.get_all_values()
        if len(rows) <= 1:
            return False
        data = rows[1:]
        tail = data[-300:] if len(data) > 300 else data
        cutoff = now_utc_dt() - dt.timedelta(hours=dedupe_hours)
        for r in reversed(tail):
            if len(r) < 7:
                continue
            ts = r[0].strip()
            k = r[6].strip()
            if k != key:
                continue
            try:
                t = dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except Exception:
                continue
            if t >= cutoff:
                return True
        return False
    except Exception:
        return False

def log_search(
    log_ws: gspread.Worksheet,
    origin: str,
    dest: str,
    out_date: str,
    ret_date: str,
    theme: str,
    key: str,
    skipped_dedupe: bool,
    searched: bool,
    offers_count: int,
) -> None:
    try:
        log_ws.append_row([
            now_utc_str(),
            origin, dest, out_date, ret_date,
            theme, key,
            "1" if skipped_dedupe else "0",
            "1" if searched else "0",
            str(offers_count)
        ])
    except Exception:
        pass


# ============================================================
# Routes + Dates
# ============================================================

def pick_routes() -> List[Tuple[str, str]]:
    # Keep existing behaviour; route-first grid will be expanded via CONFIG elsewhere.
    return [
        ("LHR", "BCN"),
        ("LGW", "FAO"),
        ("MAN", "TFS"),
        ("BRS", "PMI"),
        ("BRS", "AGP"),
        ("MAN", "AGP"),
        ("BRS", "GVA"),
        ("MAN", "GVA"),
    ]

def date_pair(days_ahead_min: int = 30, trip_len: int = 5) -> Tuple[str, str]:
    start = now_utc_dt().date() + dt.timedelta(days=days_ahead_min)
    out = start.isoformat()
    ret = (start + dt.timedelta(days=trip_len)).isoformat()
    return out, ret


# ============================================================
# CANONICAL Insert
# ============================================================

def append_offers_to_sheet(
    raw_ws: gspread.Worksheet,
    hm: Dict[str, int],
    offers: List[Dict[str, Any]],
    origin: str,
    dest: str,
    out_date: str,
    ret_date: str,
    theme: str,
    max_inserts: int,
) -> int:
    """
    Writes CANONICAL columns (the rest of pipeline consumes these).
    """
    headers = raw_ws.row_values(1)
    inserted = 0

    deal_id = f"{origin}-{dest}-{out_date}"

    # City/country fallback
    origin_city, origin_country = IATA_MAP.get(origin, ("", ""))
    dest_city, dest_country = IATA_MAP.get(dest, ("", ""))

    tlen = trip_length_days(out_date, ret_date)

    for offer in offers:
        if inserted >= max_inserts:
            break

        total_amount = offer.get("total_amount")
        total_currency = (offer.get("total_currency") or "").upper().strip()

        gbp = None
        if total_amount is not None:
            gbp = convert_to_gbp(str(total_amount), total_currency)

        # If we cannot confidently produce price_gbp, skip this offer (prevents dead rows)
        if gbp is None:
            continue

        # Airline + stops from first slice outbound segments
        airline = ""
        stops = "0"
        slices = offer.get("slices") or []
        if slices:
            segs = (slices[0].get("segments") or [])
            if segs:
                airline = ((segs[0].get("marketing_carrier") or {}).get("name") or "")
                stops = str(max(0, len(segs) - 1))

        affiliate_url = build_skyscanner_url(origin, dest, out_date, ret_date)

        row = [""] * len(headers)

        def setv(col: str, val: Any) -> None:
            if col in hm:
                row[hm[col]] = "" if val is None else str(val)

        # ---- CANONICAL REQUIRED ----
        setv("deal_id", deal_id)
        setv("origin_iata", origin)
        setv("destination_iata", dest)
        setv("outbound_date", out_date)
        setv("return_date", ret_date)
        setv("price_gbp", gbp)
        setv("affiliate_url", affiliate_url)
        setv("status", "NEW")

        # ---- CANONICAL NICE ----
        setv("origin_city", origin_city)
        setv("destination_city", dest_city)
        setv("destination_country", dest_country)
        setv("trip_length_days", tlen)
        setv("stops", stops)
        setv("airline", airline)
        setv("theme", theme)
        setv("deal_source", "duffel")
        setv("date_added", now_utc_str())

        # ---- Optional legacy debug tail (harmless) ----
        setv("origin", origin)
        setv("dest", dest)
        setv("out_date", out_date)
        setv("ret_date", ret_date)
        setv("price", total_amount)
        setv("currency", total_currency or "GBP")
        setv("deep_link", affiliate_url)
        setv("created_utc", now_utc_str())

        raw_ws.append_row(row, value_input_option="USER_ENTERED")
        inserted += 1

    return inserted


# ============================================================
# Main
# ============================================================

def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    raw_tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")

    duffel_key = env_str("DUFFEL_API_KEY")
    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")
    if not duffel_key:
        raise RuntimeError("Missing DUFFEL_API_KEY")

    DUFFEL_ROUTES_PER_RUN = env_int("DUFFEL_ROUTES_PER_RUN", 3)
    DUFFEL_MAX_SEARCHES_PER_RUN = env_int("DUFFEL_MAX_SEARCHES_PER_RUN", 4)
    DUFFEL_MAX_INSERTS = env_int("DUFFEL_MAX_INSERTS", 3)
    DUFFEL_SEARCH_DEDUPE_HOURS = env_int("DUFFEL_SEARCH_DEDUPE_HOURS", 24)

    DUFFEL_BUDGET_GBP = env_float("DUFFEL_BUDGET_GBP", 25.0)
    DUFFEL_EXCESS_SEARCH_USD = env_float("DUFFEL_EXCESS_SEARCH_USD", 0.005)
    USD_TO_GBP = env_float("USD_TO_GBP", 0.79)
    DUFFEL_ORDERS_THIS_MONTH = env_int("DUFFEL_ORDERS_THIS_MONTH", 0)
    DUFFEL_FREE_SEARCHES_PER_ORDER = env_int("DUFFEL_FREE_SEARCHES_PER_ORDER", 1500)

    RUN_SLOT = env_str("RUN_SLOT", "AM").upper()
    theme = env_str("THEME", "DEFAULT")

    log("============================================================")
    log("🚀 TravelTxter Pipeline Worker Starting")
    log(f"RUN_SLOT: {RUN_SLOT}")
    log("============================================================")

    gc = get_gspread_client()
    spread = gc.open_by_key(spreadsheet_id)
    raw_ws = spread.worksheet(raw_tab)

    # Ensure canonical columns exist (and keep legacy tail if present)
    hm = ensure_columns(raw_ws, CANONICAL_MIN_COLS + CANONICAL_NICE_COLS + LEGACY_DEBUG_COLS)

    budget_ws, log_ws = ensure_budget_tabs(spread)
    month = month_key_utc()

    # Budget headroom check
    row_i = get_budget_row_index(budget_ws, month)
    searches_total = 0
    if row_i is not None:
        try:
            searches_total = int(budget_ws.cell(row_i, 4).value or "0")
        except Exception:
            searches_total = 0

    est_cost = est_monthly_cost_gbp(
        searches_total,
        DUFFEL_ORDERS_THIS_MONTH,
        DUFFEL_FREE_SEARCHES_PER_ORDER,
        DUFFEL_EXCESS_SEARCH_USD,
        USD_TO_GBP,
    )
    if est_cost >= DUFFEL_BUDGET_GBP:
        log(f"🛑 Budget governor: est_cost_gbp={est_cost:.2f} >= budget_gbp={DUFFEL_BUDGET_GBP:.2f}. Skipping Duffel searches.")
        return 0

    # FEEDER
    log("[1] FEEDER (Elastic Supply)")

    routes = pick_routes()
    random.shuffle(routes)
    routes = routes[:max(1, DUFFEL_ROUTES_PER_RUN)]

    base_days_ahead = 30
    trip_len = 5
    slot_offset_days = 0 if RUN_SLOT == "AM" else 2

    inserted_total = 0
    searches_done = 0
    dedupe_skips = 0

    for i, (origin, dest) in enumerate(routes):
        if searches_done >= DUFFEL_MAX_SEARCHES_PER_RUN:
            break

        # Rotate dates across slot + route index to avoid dedupe starvation
        days_ahead = base_days_ahead + slot_offset_days + (i % 2)
        out_date, ret_date = date_pair(days_ahead_min=days_ahead, trip_len=trip_len)
        key = dedupe_key(origin, dest, out_date, ret_date)

        if was_recently_searched(log_ws, key, DUFFEL_SEARCH_DEDUPE_HOURS):
            log(f"⏭️  Dedupe skip: {origin}->{dest} {out_date}/{ret_date}")
            dedupe_skips += 1
            log_search(log_ws, origin, dest, out_date, ret_date, theme, key, True, False, 0)
            continue

        try:
            log(f"Duffel: Searching {origin}->{dest} {out_date}/{ret_date}")
            offers = duffel_search_return(duffel_key, origin, dest, out_date, ret_date)
            searches_done += 1

            ins = append_offers_to_sheet(
                raw_ws, hm, offers,
                origin, dest, out_date, ret_date,
                theme, DUFFEL_MAX_INSERTS
            )
            inserted_total += ins
            log_search(log_ws, origin, dest, out_date, ret_date, theme, key, False, True, len(offers))

            log(f"✓ Inserted {ins} deals for {origin}->{dest}")
        except Exception as e:
            searches_done += 1
            log(f"❌ Duffel error: {e}")
            log_search(log_ws, origin, dest, out_date, ret_date, theme, key, False, False, 0)

    # Force exactly 1 search if dedupe blocked everything
    if searches_done == 0 and routes:
        origin, dest = routes[0]
        forced_days_ahead = base_days_ahead + slot_offset_days + 7
        out_date, ret_date = date_pair(days_ahead_min=forced_days_ahead, trip_len=trip_len)
        key = dedupe_key(origin, dest, out_date, ret_date)

        try:
            log(f"⚠️  All routes deduped (skips={dedupe_skips}/{len(routes)}). Forcing 1 Duffel search: {origin}->{dest} {out_date}/{ret_date}")
            offers = duffel_search_return(duffel_key, origin, dest, out_date, ret_date)
            searches_done += 1

            ins = append_offers_to_sheet(
                raw_ws, hm, offers,
                origin, dest, out_date, ret_date,
                theme, DUFFEL_MAX_INSERTS
            )
            inserted_total += ins
            log_search(log_ws, origin, dest, out_date, ret_date, theme, key, False, True, len(offers))

            log(f"✓ Forced insert: {ins} deals for {origin}->{dest}")
        except Exception as e:
            searches_done += 1
            log(f"❌ Forced Duffel error: {e}")
            log_search(log_ws, origin, dest, out_date, ret_date, theme, key, False, False, 0)

    # Commit searches to budget
    if searches_done > 0:
        bc = budget_commit_searches(
            budget_ws,
            month,
            searches_done,
            DUFFEL_ORDERS_THIS_MONTH,
            DUFFEL_FREE_SEARCHES_PER_ORDER,
            DUFFEL_BUDGET_GBP,
            DUFFEL_EXCESS_SEARCH_USD,
            USD_TO_GBP,
        )
        log(f"Budget: searches_add={searches_done} est_cost_gbp={bc:.2f}/{DUFFEL_BUDGET_GBP:.2f}")

    log(f"✅ FEEDER complete. Searches={searches_done}, Inserts={inserted_total}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        log(f"FATAL: {e}")
        raise
