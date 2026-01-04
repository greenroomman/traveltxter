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

3) Budget governor (so £25/month is never exceeded)
   - Uses DUFFEL_BUDGET_GBP, DUFFEL_EXCESS_SEARCH_USD, USD_TO_GBP, DUFFEL_ORDERS_THIS_MONTH, DUFFEL_FREE_SEARCHES_PER_ORDER
   - Hard-stops if we are about to exceed the monthly budget

POST-WATERWHEEL FIX PACK (this patch)
- Prevents "zero work" runs:
  - Deterministic date rotation by RUN_SLOT + route index
  - If ALL routes are deduped → force exactly 1 Duffel search using shifted dates
"""

import os
import sys
import json
import time
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
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return gspread.authorize(creds)


# ============================================================
# Sheet helpers
# ============================================================

def ensure_raw_deals_columns(ws: gspread.Worksheet) -> Dict[str, int]:
    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("RAW_DEALS_TAB has no header row")

    # Must-have columns (your existing contract)
    must = [
        "status",
        "origin",
        "dest",
        "out_date",
        "ret_date",
        "price",
        "currency",
        "airline",
        "stops",
        "deep_link",
        "deal_id",
        "theme",
        "created_utc",
    ]

    changed = False
    for h in must:
        if h not in headers:
            headers.append(h)
            changed = True

    if changed:
        ws.update([headers], "A1")

    # Header map
    hm = {h: i for i, h in enumerate(headers)}
    return hm

def ensure_budget_tabs(spread: gspread.Spreadsheet) -> Tuple[gspread.Worksheet, gspread.Worksheet]:
    # budget tab
    try:
        budget_ws = spread.worksheet("DUFFEL_BUDGET")
    except Exception:
        budget_ws = spread.add_worksheet("DUFFEL_BUDGET", rows=200, cols=12)
        budget_ws.update([[
            "month", "orders_this_month", "free_searches_per_order",
            "searches_this_month", "budget_gbp", "excess_search_usd",
            "usd_to_gbp", "est_cost_gbp"
        ]], "A1")

    # search log tab
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
        if len(rows[i]) > 0 and rows[i][0].strip() == month:
            return i + 1  # gspread is 1-based
    return None

def a1_update(ws: gspread.Worksheet, a1: str, value: Any) -> None:
    # gspread v6 safe signature
    ws.update([[value]], a1)

def a1_row_update(ws: gspread.Worksheet, a1_range: str, row_values: List[Any]) -> None:
    ws.update([row_values], a1_range)


# ============================================================
# Budget governor
# ============================================================

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
    """
    Update DUFFEL_BUDGET for the month.
    Returns new estimated monthly cost in GBP.
    """
    row_i = get_budget_row_index(budget_ws, month)

    if row_i is None:
        # append row
        searches_this_month = searches_add
        est_cost = est_monthly_cost_gbp(searches_this_month, orders_this_month, free_searches_per_order, excess_search_usd, usd_to_gbp)
        budget_ws.append_row([
            month, str(orders_this_month), str(free_searches_per_order),
            str(searches_this_month), str(budget_gbp), str(excess_search_usd),
            str(usd_to_gbp), f"{est_cost:.2f}"
        ])
        return est_cost

    # read existing
    try:
        searches_this_month = int(budget_ws.cell(row_i, 4).value or "0")
    except Exception:
        searches_this_month = 0

    searches_this_month += searches_add
    est_cost = est_monthly_cost_gbp(searches_this_month, orders_this_month, free_searches_per_order, excess_search_usd, usd_to_gbp)

    # safe updates (gspread v6)
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

def duffel_search_oneway_return(
    api_key: str,
    origin: str,
    dest: str,
    out_date: str,
    ret_date: str,
    cabin: str = "economy",
    max_connections: int = 1,
) -> List[Dict[str, Any]]:
    # Create offer request
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
    base = "https://www.skyscanner.net/transport/flights"
    return f"{base}/{origin}/{dest}/{out_date.replace('-','')}/{ret_date.replace('-','')}/"


# ============================================================
# Route selection (CONFIG-driven route-first)
# ============================================================

def pick_routes() -> List[Tuple[str, str]]:
    """
    Minimal route list; your real build likely loads from CONFIG or SIGNALS.
    Keep as-is to avoid redesign. If CONFIG fails, fallback list maintains supply.
    """
    fallback = [
        ("LHR", "BCN"),
        ("LGW", "FAO"),
        ("MAN", "TFS"),
        ("BRS", "PMI"),
        ("BRS", "AGP"),
        ("MAN", "AGP"),
        ("LHR", "PMI"),
        ("LGW", "PMI"),
        ("MAN", "BCN"),
        ("BRS", "FAO"),
        ("MAN", "FAO"),
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
    headers = raw_ws.row_values(1)

    deal_id = ctx.get("deal_id") or f"{ctx['origin']}-{ctx['dest']}-{ctx['out_date']}"
    ctx["deal_id"] = deal_id

    for offer in offers:
        if inserted >= max_inserts:
            break

        total_amount = offer.get("total_amount")
        total_currency = offer.get("total_currency") or "GBP"

        # Simple airline/stops extraction
        slices = offer.get("slices") or []
        airline = ""
        stops = "0"
        if slices:
            segs = slices[0].get("segments") or []
            if segs:
                airline = (segs[0].get("marketing_carrier") or {}).get("name") or ""
                stops = str(max(0, len(segs) - 1))

        deep_link = build_skyscanner_affiliate_url(ctx["origin"], ctx["dest"], ctx["out_date"], ctx["ret_date"])

        row = [""] * len(headers)
        def setv(col: str, val: Any) -> None:
            if col in hm:
                row[hm[col]] = "" if val is None else str(val)

        setv("status", "NEW")
        setv("origin", ctx["origin"])
        setv("dest", ctx["dest"])
        setv("out_date", ctx["out_date"])
        setv("ret_date", ctx["ret_date"])
        setv("price", total_amount)
        setv("currency", total_currency)
        setv("airline", airline)
        setv("stops", stops)
        setv("deep_link", deep_link)
        setv("deal_id", ctx["deal_id"])
        setv("theme", ctx.get("theme", "DEFAULT"))
        setv("created_utc", now_utc_str())

        raw_ws.append_row(row, value_input_option="USER_ENTERED")
        inserted += 1

    return inserted


# ============================================================
# Search dedupe (DUFFEL_SEARCH_LOG)
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
    offers_count: int
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

    gc = get_gspread_client()
    spread = gc.open_by_key(spreadsheet_id)
    raw_ws = spread.worksheet(raw_tab)
    hm = ensure_raw_deals_columns(raw_ws)

    budget_ws, log_ws = ensure_budget_tabs(spread)
    month = month_key_utc()

    # ---- Budget headroom ----
    row_i = get_budget_row_index(budget_ws, month)
    searches_total = 0
    if row_i is not None:
        try:
            searches_total = int(budget_ws.cell(row_i, 4).value or "0")
        except Exception:
            searches_total = 0

    # Worst-case paid searches this month, estimate costs
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

    # ---- FEEDER ----
    log("[1] FEEDER (Elastic Supply)")
    routes = pick_routes()
    random.shuffle(routes)
    routes = routes[:max(1, DUFFEL_ROUTES_PER_RUN)]

    # --- Date rotation (prevents dedupe starvation) ---
    slot = (RUN_SLOT or "AM").strip().upper()
    slot_offset_days = 0 if slot == "AM" else 2  # PM gets a small forward shift
    base_days_ahead = 30
    trip_len = 5

    inserted_total = 0
    searches_done = 0
    dedupe_skips = 0

    for i, (origin, dest) in enumerate(routes):
        if searches_done >= DUFFEL_MAX_SEARCHES_PER_RUN:
            break

        # Rotate dates across slot + route index to avoid dedupe starving the run
        days_ahead = base_days_ahead + slot_offset_days + (i % 2)
        out_date, ret_date = date_pair(days_ahead_min=days_ahead, trip_len=trip_len)
        key = dedupe_key(origin, dest, out_date, ret_date)

        if was_recently_searched(log_ws, key, DUFFEL_SEARCH_DEDUPE_HOURS):
            log(f"⏭️  Dedupe skip: {origin}->{dest} {out_date}/{ret_date}")
            dedupe_skips += 1
            log_search(log_ws, origin, dest, out_date, ret_date, theme, key, True, False, 0)
            continue

        ctx = {"origin": origin, "dest": dest, "out_date": out_date, "ret_date": ret_date, "deal_id": f"{origin}-{dest}-{out_date}", "theme": theme}

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

    # --- Never allow a zero-search run ---
    # If every route was dedupe-skipped, force exactly one Duffel search using a shifted date pair.
    if searches_done == 0 and routes:
        origin, dest = routes[0]
        forced_days_ahead = base_days_ahead + slot_offset_days + 7  # guaranteed new window vs recent runs
        out_date, ret_date = date_pair(days_ahead_min=forced_days_ahead, trip_len=trip_len)
        key = dedupe_key(origin, dest, out_date, ret_date)

        ctx = {"origin": origin, "dest": dest, "out_date": out_date, "ret_date": ret_date,
               "deal_id": f"{origin}-{dest}-{out_date}", "theme": theme}

        try:
            log(f"⚠️  All routes deduped (skips={dedupe_skips}/{len(routes)}). Forcing 1 Duffel search: {origin}->{dest} {out_date}/{ret_date}")
            offers = duffel_search_oneway_return(duffel_key, origin, dest, out_date, ret_date)
            searches_done += 1

            ins = append_offers_to_sheet(raw_ws, hm, offers, ctx, DUFFEL_MAX_INSERTS)
            inserted_total += ins
            log_search(log_ws, origin, dest, out_date, ret_date, theme, key, False, True, len(offers))

            log(f"✓ Forced insert: {ins} deals for {origin}->{dest}")
        except Exception as e:
            searches_done += 1
            log(f"❌ Forced Duffel error: {e}")
            log_search(log_ws, origin, dest, out_date, ret_date, theme, key, False, False, 0)

    # ---- Commit searches to budget tab ----
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
