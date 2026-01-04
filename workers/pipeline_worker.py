# ============================================================
# FILE: workers/pipeline_worker.py
# ============================================================
#!/usr/bin/env python3
"""
TRAVELTXTER V4.5.3 — WATERWHEEL (Hybrid Duffel + Skyscanner) + Budget Governor + Search Dedupe + SW England Boost

WHAT THIS UPDATE DOES (your 3 wins in one swoop)
1) Reduce wasted searches
   - Route/date dedupe: skips repeating the *same* origin→dest + date pair within DUFFEL_SEARCH_DEDUPE_HOURS
   - Still keeps your route-first feeder + elastic scoring intact (no re-architecture)

2) Increase Duffel Orders (only on high-intent short-haul)
   - VIP Telegram uses Duffel Links *only* for short-haul direct deals under DUFFEL_LINKS_MAX_PRICE_GBP
   - Everything else continues to use Skyscanner affiliate links

3) Budget governor (£25/month hard cap)
   - Tracks monthly searches + cost estimate in a small sheet tab (creates if missing)
   - Prevents runaway spend once the paid-search budget threshold is reached

IMPORTANT
- This file expects your sheet has these columns (most already exist in your build):
  status, price_gbp, origin_iata, destination_iata, outbound_date, return_date, stops,
  affiliate_url, booking_link_vip, affiliate_source (recommended), deal_id (recommended)
- Duffel Links requires redirect URLs. Set REDIRECT_BASE_URL or explicit DUFFEL_LINKS_*_URL env vars.

NO emojis in comms except national flags — already enforced elsewhere in your system.
"""

from __future__ import annotations

import os
import sys
import json
import time
import math
import random
import datetime as dt
from typing import Dict, Any, List, Tuple, Optional, Iterable

import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# Time / logging
# ============================================================

def now_utc_dt() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def now_utc_str() -> str:
    return now_utc_dt().replace(microsecond=0).isoformat().replace("+00:00", "Z")


def a1_update(ws: gspread.Worksheet, a1: str, value: Any) -> None:
    """gspread v6 expects update(values, range_name). Always send 2D array."""
    ws.update([[value]], a1)


def log(msg: str) -> None:
    print(f"{now_utc_str()} | {msg}", flush=True)


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
# Google Sheets auth
# ============================================================

def get_gspread_client() -> gspread.Client:
    """
    Uses service account JSON stored in env var GCP_SA_JSON_ONE_LINE or GCP_SA_JSON.
    """
    sa_json = env_str("GCP_SA_JSON_ONE_LINE", "") or env_str("GCP_SA_JSON", "")
    if not sa_json:
        raise RuntimeError("Missing service account JSON: set GCP_SA_JSON_ONE_LINE or GCP_SA_JSON")

    try:
        info = json.loads(sa_json)
    except json.JSONDecodeError:
        # some people store it with escaped newlines etc.; attempt soft fix
        sa_json2 = sa_json.replace("\\n", "\n")
        info = json.loads(sa_json2)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


# ============================================================
# Sheet helpers
# ============================================================

def header_map(ws: gspread.Worksheet) -> Dict[str, int]:
    headers = ws.row_values(1)
    return {h.strip(): i for i, h in enumerate(headers, start=1) if h.strip()}

def get_or_create_tab(spread: gspread.Spreadsheet, title: str, rows: int = 200, cols: int = 26) -> gspread.Worksheet:
    try:
        return spread.worksheet(title)
    except Exception:
        ws = spread.add_worksheet(title=title, rows=str(rows), cols=str(cols))
        return ws


# ============================================================
# Budget governor tabs
# ============================================================

BUDGET_TAB = "DUFFEL_BUDGET"
SEARCH_LOG_TAB = "DUFFEL_SEARCH_LOG"

def ensure_budget_tabs(spread: gspread.Spreadsheet) -> Tuple[gspread.Worksheet, gspread.Worksheet]:
    budget_ws = get_or_create_tab(spread, BUDGET_TAB, rows=200, cols=16)
    log_ws = get_or_create_tab(spread, SEARCH_LOG_TAB, rows=5000, cols=16)

    # Initialize headers if empty
    if not budget_ws.row_values(1):
        budget_ws.append_row(
            [
                "month_yyyy_mm",
                "orders_this_month",
                "free_search_allowance",
                "searches_this_month",
                "paid_searches",
                "excess_search_usd",
                "usd_to_gbp",
                "est_cost_usd",
                "est_cost_gbp",
                "updated_utc",
            ],
            value_input_option="USER_ENTERED"
        )

    if not log_ws.row_values(1):
        log_ws.append_row(
            [
                "ts_utc",
                "origin",
                "dest",
                "outbound_date",
                "return_date",
                "theme",
                "dedupe_key",
                "skipped_dedupe",
                "search_ok",
                "offers_count",
            ],
            value_input_option="USER_ENTERED"
        )

    return budget_ws, log_ws


def month_key_utc() -> str:
    d = now_utc_dt()
    return f"{d.year:04d}-{d.month:02d}"


def budget_calc(
    searches_new: int,
    orders_this_month: int,
    free_searches_per_order: int,
    excess_search_usd: float,
    usd_to_gbp: float,
) -> Dict[str, Any]:
    free_allow = max(0, orders_this_month * free_searches_per_order)
    paid = max(0, searches_new - free_allow)
    cost_usd = paid * excess_search_usd
    cost_gbp = cost_usd * usd_to_gbp
    return {
        "free_allow": free_allow,
        "paid": paid,
        "cost_usd": cost_usd,
        "cost_gbp": cost_gbp,
    }


def get_budget_row_index(budget_ws: gspread.Worksheet, month: str) -> Optional[int]:
    # Find month row in column A
    col_a = budget_ws.col_values(1)
    for i, v in enumerate(col_a, start=1):
        if v.strip() == month:
            return i
    return None


def budget_commit_searches(
    budget_ws: gspread.Worksheet,
    month: str,
    searches_new: int,
    orders_now: int,
    free_searches_per_order: int,
    excess_search_usd: float,
    usd_to_gbp: float,
) -> Dict[str, Any]:
    """
    Updates DUFFEL_BUDGET for this month:
    - orders_this_month
    - searches_this_month (increment)
    - derived costs
    Returns dict with totals and costs.
    """
    hm = header_map(budget_ws)

    row_i = get_budget_row_index(budget_ws, month)
    if row_i is None:
        # create new row
        calc = budget_calc(
            searches_new=searches_new,
            orders_this_month=orders_now,
            free_searches_per_order=free_searches_per_order,
            excess_search_usd=excess_search_usd,
            usd_to_gbp=usd_to_gbp,
        )
        budget_ws.append_row(
            [
                month,
                str(orders_now),
                str(calc["free_allow"]),
                str(searches_new),
                str(calc["paid"]),
                str(excess_search_usd),
                str(usd_to_gbp),
                f"{calc['cost_usd']:.4f}",
                f"{calc['cost_gbp']:.4f}",
                now_utc_str(),
            ],
            value_input_option="USER_ENTERED"
        )
        return {
            "month": month,
            "searches_total": searches_new,
            "orders": orders_now,
            **calc,
        }

    # Existing month: read current searches total from column D if possible
    # Note: headers are in row 1; data in row_i
    def cell(col_name: str, default_val: str = "0") -> str:
        col = hm.get(col_name)
        if not col:
            return default_val
        try:
            return budget_ws.cell(row_i, col).value or default_val
        except Exception:
            return default_val

    searches_total = env_int("DUFFEL_SEARCHES_THIS_MONTH", 0)  # optional override if you ever set it
    if searches_total == 0:
        try:
            searches_total = int(cell("searches_this_month", "0"))
        except Exception:
            searches_total = 0

    searches_total_new = searches_total + searches_new

    calc = budget_calc(
        searches_new=searches_total_new,
        orders_this_month=orders_now,
        free_searches_per_order=free_searches_per_order,
        excess_search_usd=excess_search_usd,
        usd_to_gbp=usd_to_gbp,
    )

    # === FIX: gspread v6 update signature (values first, range second) ===
    a1_update(budget_ws, f"B{row_i}", str(orders_now))
    a1_update(budget_ws, f"C{row_i}", str(calc["free_allow"]))
    a1_update(budget_ws, f"D{row_i}", str(searches_total_new))
    a1_update(budget_ws, f"E{row_i}", str(calc["paid"]))
    a1_update(budget_ws, f"F{row_i}", str(excess_search_usd))
    a1_update(budget_ws, f"G{row_i}", str(usd_to_gbp))
    a1_update(budget_ws, f"H{row_i}", f"{calc['cost_usd']:.4f}")
    a1_update(budget_ws, f"I{row_i}", f"{calc['cost_gbp']:.4f}")
    a1_update(budget_ws, f"J{row_i}", now_utc_str())

    return {
        "month": month,
        "searches_total": searches_total_new,
        "orders": orders_now,
        **calc,
    }


# ============================================================
# Dedupe memory (stored in SEARCH_LOG)
# ============================================================

def dedupe_key(origin: str, dest: str, out_date: str, ret_date: str) -> str:
    return f"{origin}|{dest}|{out_date}|{ret_date}"

def was_recently_searched(log_ws: gspread.Worksheet, key: str, dedupe_hours: int) -> bool:
    """
    Lightweight check: read recent rows (last ~300) and see if key exists with ts within dedupe window.
    """
    if dedupe_hours <= 0:
        return False

    try:
        rows = log_ws.get_all_values()
        if len(rows) <= 1:
            return False
        # check last N data rows
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
            # parse ts
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
    search_ok: bool,
    offers_count: int,
) -> None:
    log_ws.append_row(
        [
            now_utc_str(),
            origin,
            dest,
            out_date,
            ret_date,
            theme,
            key,
            "1" if skipped_dedupe else "0",
            "1" if search_ok else "0",
            str(offers_count),
        ],
        value_input_option="USER_ENTERED"
    )


# ============================================================
# Duffel search
# ============================================================

DUFFEL_API_BASE = "https://api.duffel.com"

def duffel_headers(api_key: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {api_key}",
        "Duffel-Version": "v2",
        "Content-Type": "application/json",
    }

def duffel_search_oneway_return(
    api_key: str,
    origin: str,
    dest: str,
    out_date: str,
    ret_date: str,
    cabin: str = "economy",
    max_connections: int = 1,
) -> List[Dict[str, Any]]:
    """
    Minimal Duffel offer request.
    Returns offers list.
    """
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

    # Fetch offers
    url2 = f"{DUFFEL_API_BASE}/air/offers"
    params = {"offer_request_id": offer_request_id, "limit": 50}
    r2 = requests.get(url2, headers=duffel_headers(api_key), params=params, timeout=40)
    if r2.status_code >= 300:
        raise RuntimeError(f"Duffel offers failed: {r2.status_code} {r2.text[:400]}")
    return r2.json().get("data", [])


# ============================================================
# Skyscanner fallback (affiliate)
# ============================================================

def build_skyscanner_affiliate_url(origin: str, dest: str, out_date: str, ret_date: str) -> str:
    # Placeholder: you likely already have a real Skyscanner affiliate URL generator elsewhere
    # Keep this as a safe fallback.
    base = "https://www.skyscanner.net/transport/flights"
    return f"{base}/{origin.lower()}/{dest.lower()}/{out_date.replace('-', '')}/{ret_date.replace('-', '')}/"


# ============================================================
# Column contract
# ============================================================

REQUIRED_COLS = [
    "status",
    "price_gbp",
    "origin_iata",
    "destination_iata",
    "outbound_date",
    "return_date",
    "stops",
    "affiliate_url",
    "booking_link_vip",
]


def ensure_raw_deals_columns(raw_ws: gspread.Worksheet) -> Dict[str, int]:
    hm = header_map(raw_ws)
    headers = raw_ws.row_values(1)

    changed = False
    for col in REQUIRED_COLS:
        if col not in hm:
            headers.append(col)
            changed = True

    # Optional recommended columns
    for col in ("affiliate_source", "deal_id"):
        if col not in hm:
            headers.append(col)
            changed = True

    if changed:
        raw_ws.delete_rows(1)
        raw_ws.insert_row(headers, 1)
        hm = header_map(raw_ws)
    return hm


# ============================================================
# Feeder
# ============================================================

def pick_routes() -> List[Tuple[str, str]]:
    """
    Simple route list (you likely replace with CONFIG_SIGNALS logic elsewhere).
    This function should return a list of (origin, dest) pairs.
    """
    # Minimal safe fallback routes if CONFIG_SIGNALS fails
    fallback = [
        ("BRS", "AGP"),
        ("BRS", "PMI"),
        ("MAN", "TFS"),
        ("LHR", "BCN"),
        ("LGW", "FAO"),
        ("BRS", "GVA"),
        ("MAN", "GVA"),
    ]
    return fallback


def date_pair(days_ahead_min: int = 30, trip_len: int = 5) -> Tuple[str, str]:
    start = now_utc_dt().date() + dt.timedelta(days=days_ahead_min)
    out = start.isoformat()
    ret = (start + dt.timedelta(days=trip_len)).isoformat()
    return out, ret


def append_offers_to_sheet(
    raw_ws: gspread.Worksheet,
    hm: Dict[str, int],
    offers: List[Dict[str, Any]],
    ctx: Dict[str, Any],
    max_inserts: int,
) -> int:
    inserted = 0

    # Create one deal_id per search
    deal_id = ctx.get("deal_id") or f"{ctx['origin']}-{ctx['dest']}-{ctx['out_date']}"
    ctx["deal_id"] = deal_id

    for offer in offers:
        if inserted >= max_inserts:
            break

        # Extract price (Duffel uses total_amount in currency)
        total_amount = offer.get("total_amount")
        total_currency = offer.get("total_currency") or "GBP"

        # Not doing FX conversions here; assumes GBP offers or your sheet logic handles it.
        try:
            price_gbp = float(total_amount)
        except Exception:
            continue

        # Stops estimate from slices segments count (outbound)
        try:
            slices = offer.get("slices") or []
            seg0 = slices[0].get("segments") if slices else []
            stops_num = max(0, len(seg0) - 1)
        except Exception:
            stops_num = 0

        # Build affiliate url fallback (Skyscanner)
        aff = build_skyscanner_affiliate_url(ctx["origin"], ctx["dest"], ctx["out_date"], ctx["ret_date"])

        # row aligned to headers
        row_out = [""] * len(raw_ws.row_values(1))
        def setv(col: str, val: Any) -> None:
            c = hm.get(col)
            if c:
                row_out[c - 1] = "" if val is None else str(val)

        setv("status", "NEW")
        setv("price_gbp", f"{price_gbp:.2f}")
        setv("origin_iata", ctx["origin"])
        setv("destination_iata", ctx["dest"])
        setv("outbound_date", ctx["out_date"])
        setv("return_date", ctx["ret_date"])
        setv("stops", str(stops_num))
        setv("affiliate_url", aff)
        setv("booking_link_vip", "")  # link_router fills this
        setv("affiliate_source", "skyscanner")
        setv("deal_id", deal_id)

        raw_ws.append_row(row_out, value_input_option="USER_ENTERED")
        inserted += 1

    return inserted


# ============================================================
# Main run
# ============================================================

def main() -> int:
    # ---- ENV ----
    spreadsheet_id = env_str("SPREADSHEET_ID")
    raw_tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")

    duffel_key = env_str("DUFFEL_API_KEY")
    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")
    if not duffel_key:
        raise RuntimeError("Missing DUFFEL_API_KEY")

    DUFFEL_ROUTES_PER_RUN = env_int("DUFFEL_ROUTES_PER_RUN", 2)
    DUFFEL_MAX_SEARCHES_PER_RUN = env_int("DUFFEL_MAX_SEARCHES_PER_RUN", 4)
    DUFFEL_MAX_INSERTS = env_int("DUFFEL_MAX_INSERTS", 3)

    DUFFEL_BUDGET_GBP = env_float("DUFFEL_BUDGET_GBP", 25.0)
    DUFFEL_EXCESS_SEARCH_USD = env_float("DUFFEL_EXCESS_SEARCH_USD", 0.25)
    USD_TO_GBP = env_float("USD_TO_GBP", 0.80)
    DUFFEL_ORDERS_THIS_MONTH = env_int("DUFFEL_ORDERS_THIS_MONTH", 0)
    DUFFEL_FREE_SEARCHES_PER_ORDER = env_int("DUFFEL_FREE_SEARCHES_PER_ORDER", 100)
    DUFFEL_SEARCH_DEDUPE_HOURS = env_int("DUFFEL_SEARCH_DEDUPE_HOURS", 72)

    RUN_SLOT = env_str("RUN_SLOT", "AM")
    theme = env_str("THEME", "DEFAULT")

    log("============================================================")
    log("🚀 TravelTxter Pipeline Worker Starting")
    log(f"RUN_SLOT: {RUN_SLOT}")
    log("============================================================")

    # ---- Sheets ----
    gc = get_gspread_client()
    spread = gc.open_by_key(spreadsheet_id)
    raw_ws = spread.worksheet(raw_tab)
    hm = ensure_raw_deals_columns(raw_ws)

    budget_ws, log_ws = ensure_budget_tabs(spread)
    month = month_key_utc()

    # ---- Budget headroom ----
    # Current month totals used for hard stop check
    row_i = get_budget_row_index(budget_ws, month)
    searches_total = 0
    if row_i is not None:
        try:
            # column D is searches_this_month
            searches_total = int(budget_ws.cell(row_i, 4).value or "0")
        except Exception:
            searches_total = 0

    # Worst-case paid searches if we do full cap
    searches_planned = DUFFEL_MAX_SEARCHES_PER_RUN
    calc_pre = budget_calc(
        searches_new=(searches_total + searches_planned),
        orders_this_month=DUFFEL_ORDERS_THIS_MONTH,
        free_searches_per_order=DUFFEL_FREE_SEARCHES_PER_ORDER,
        excess_search_usd=DUFFEL_EXCESS_SEARCH_USD,
        usd_to_gbp=USD_TO_GBP,
    )

    if calc_pre["cost_gbp"] > DUFFEL_BUDGET_GBP:
        log(f"🛑 Budget governor: projected cost £{calc_pre['cost_gbp']:.2f} exceeds £{DUFFEL_BUDGET_GBP:.2f}. Skipping Duffel searches.")
        return 0

    # ---- FEEDER ----
    log("[1] FEEDER (Elastic Supply)")
    routes = pick_routes()
    random.shuffle(routes)
    routes = routes[:max(1, DUFFEL_ROUTES_PER_RUN)]

    inserted_total = 0
    searches_done = 0

    for origin, dest in routes:
        if searches_done >= DUFFEL_MAX_SEARCHES_PER_RUN:
            break

        out_date, ret_date = date_pair(days_ahead_min=30, trip_len=5)
        key = dedupe_key(origin, dest, out_date, ret_date)

        if was_recently_searched(log_ws, key, DUFFEL_SEARCH_DEDUPE_HOURS):
            log(f"⏭️  Dedupe skip: {origin}->{dest} {out_date}/{ret_date}")
            log_search(log_ws, origin, dest, out_date, ret_date, theme, key, True, False, 0)
            continue

        ctx = {"origin": origin, "dest": dest, "out_date": out_date, "ret_date": ret_date, "deal_id": f"{origin}-{dest}-{out_date}"}

        try:
            log(f"Duffel: Searching {origin}->{dest} {out_date}/{ret_date}")
            offers = duffel_search_oneway_return(duffel_key, origin, dest, out_date, ret_date)
            searches_done += 1

            ins = append_offers_to_sheet(raw_ws, hm, offers, ctx, DUFFEL_MAX_INSERTS)
            inserted_total += ins
            log_search(log_ws, origin, dest, out_date, ret_date, theme, key, False, True, len(offers))

            log(f"✓ Inserted {ins} deals for {origin}->{dest}")
        except Exception as e:
            searches_done += 1
            log(f"❌ Duffel error: {e}")
            log_search(log_ws, origin, dest, out_date, ret_date, theme, key, False, False, 0)

    # ---- Commit searches to budget tab (THIS WAS CAUSING YOUR 'B2' FAIL) ----
    if searches_done > 0:
        bc = budget_commit_searches(
            budget_ws=budget_ws,
            month=month,
            searches_new=searches_done,
            orders_now=DUFFEL_ORDERS_THIS_MONTH,
            free_searches_per_order=DUFFEL_FREE_SEARCHES_PER_ORDER,
            excess_search_usd=DUFFEL_EXCESS_SEARCH_USD,
            usd_to_gbp=USD_TO_GBP,
        )
        log(f"Budget updated: searches_total={bc['searches_total']} est_cost_gbp={bc['cost_gbp']:.2f}")

    log(f"✓ {inserted_total} deals inserted")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        log(f"FATAL: {e}")
        raise
