from __future__ import annotations

import os
import json
import time
import math
import hashlib
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# TRAVELTXTTER V5 — FEEDER (MINIMAL, CONFIG-DRIVEN)
# PURPOSE:
#   Insert NEW raw inventory rows into RAW_DEALS using Duffel.
#
# READS:
#   - OPS_MASTER!B2 (theme_of_the_day)
#   - CONFIG (route constraints + Pi inputs)
#   - RAW_DEALS (dedupe + caps)
#
# WRITES:
#   - RAW_DEALS: append NEW rows (status="NEW", ingested_at_utc ISO)
#
# DOES NOT:
#   - score
#   - enrich
#   - render
#   - publish
# ============================================================

DUFFEL_API = "https://api.duffel.com"


# ------------------------- ENV -------------------------
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID") or ""
RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS")
CONFIG_TAB = os.getenv("FEEDER_CONFIG_TAB", "CONFIG")
OPS_TAB = os.getenv("OPS_MASTER_TAB", "OPS_MASTER")

DUFFEL_API_KEY = os.getenv("DUFFEL_API_KEY", "")

MAX_SEARCHES = int(os.getenv("DUFFEL_MAX_SEARCHES_PER_RUN", "12"))
MAX_INSERTS = int(os.getenv("DUFFEL_MAX_INSERTS", "50"))
DESTS_PER_RUN = int(os.getenv("DUFFEL_ROUTES_PER_RUN", "6"))

# When CONFIG has multiple eligible rows, we sample some variety
RANDOM_SEED = os.getenv("FEEDER_RANDOM_SEED", "")
SLEEP_SECONDS = float(os.getenv("FEEDER_SLEEP_SECONDS", "0.05"))

# Cabin default if CONFIG blank
DEFAULT_CABIN = os.getenv("DEFAULT_CABIN_CLASS", "economy")

# ------------------------- RAW_DEALS CONTRACT -------------------------
# Must exist as headers in row 1 of RAW_DEALS.
RAW_DEALS_HEADERS = [
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
    "cabin_class",
    "carriers",
    "theme",
    "status",
    "publish_window",
    "score",
    "phrase_used",
    "graphic_url",
    "posted_vip_at",
    "posted_free_at",
    "posted_instagram_at",
    "ingested_at_utc",
    # NOTE: do NOT include duplicate headers like phrasse_used, etc.
]

# ------------------------- CONFIG CONTRACT -------------------------
# Your CONFIG headers (you pasted these) are supported:
# enabled,priority,origin_iata,destination_iata,days_ahead_min,days_ahead_max,
# trip_length_days,max_connections,included_airlines,cabin_class,search_weight,
# audience_type,content_priority,seasonality_boost,active_in_feeder,gateway_type,
# is_long_haul,primary_theme,slot_hint,reachability,short_stay_theme,
# long_stay_winter_theme,long_stay_summer_theme,value_score


def log(msg: str) -> None:
    print(f"{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')} | {msg}", flush=True)


def _normalize_sa_json(raw: str) -> Dict[str, Any]:
    """
    Robustly parse service account JSON from GitHub Secrets.
    Handles either:
      - one-line JSON
      - JSON with escaped newlines \\n in private_key
      - JSON with literal newlines in private_key
    """
    raw = (raw or "").strip()
    if not raw:
        raise RuntimeError("Missing service account JSON (GCP_SA_JSON or GCP_SA_JSON_ONE_LINE).")

    # Try as-is first
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    # Try replacing escaped newlines
    try:
        return json.loads(raw.replace("\\n", "\n"))
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Service account JSON decode failed: {e}") from e


def gspread_client() -> gspread.Client:
    raw = os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON") or ""
    info = _normalize_sa_json(raw)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/drive.file",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def open_sheet(gc: gspread.Client) -> gspread.Spreadsheet:
    if not SPREADSHEET_ID:
        raise RuntimeError("SPREADSHEET_ID (or SHEET_ID) env var is missing.")
    return gc.open_by_key(SPREADSHEET_ID)


def get_ops_theme(sh: gspread.Spreadsheet) -> str:
    ws = sh.worksheet(OPS_TAB)
    theme = (ws.acell("B2").value or "").strip()
    if not theme:
        raise RuntimeError("OPS_MASTER!B2 (theme_of_the_day) is blank.")
    return theme


def _header_map(headers: List[str]) -> Dict[str, int]:
    # maps header -> 0-based index
    return {h.strip(): i for i, h in enumerate(headers) if h and h.strip()}


def read_table(ws: gspread.Worksheet) -> Tuple[List[str], List[List[Any]]]:
    values = ws.get_all_values()
    if not values or not values[0]:
        return [], []
    headers = [h.strip() for h in values[0]]
    rows = values[1:]
    return headers, rows


def ensure_raw_deals_headers(ws: gspread.Worksheet) -> Dict[str, int]:
    headers, _ = read_table(ws)
    if not headers:
        raise RuntimeError("RAW_DEALS is empty. Row 1 must contain headers.")
    hm = _header_map(headers)

    missing = [h for h in RAW_DEALS_HEADERS if h not in hm]
    if missing:
        raise RuntimeError(f"RAW_DEALS missing required headers: {missing}")

    # Also protect against accidental duplicate “phrase” columns etc.
    if len(set(headers)) != len(headers):
        raise RuntimeError("RAW_DEALS header row contains duplicates. Fix before running workers.")

    return hm


def build_existing_keys(raw_rows: List[List[Any]], hm: Dict[str, int]) -> set:
    """
    Dedupe key: origin|dest|out|in|cabin
    """
    keys = set()
    for r in raw_rows:
        try:
            o = (r[hm["origin_iata"]] or "").strip()
            d = (r[hm["destination_iata"]] or "").strip()
            outd = (r[hm["outbound_date"]] or "").strip()
            ind = (r[hm["return_date"]] or "").strip()
            cab = (r[hm["cabin_class"]] or "").strip()
        except Exception:
            continue
        if o and d and outd and ind and cab:
            keys.add(f"{o}|{d}|{outd}|{ind}|{cab}")
    return keys


@dataclass
class ConfigRoute:
    origin: str
    dest: str
    days_min: int
    days_max: int
    trip_len: int
    max_conn: int
    included_airlines: str
    cabin: str
    search_weight: float   # Fi
    value_score: float     # Vi
    slot_hint: str
    primary_theme: str
    short_stay_theme: str
    long_winter_theme: str
    long_summer_theme: str

    @property
    def pi(self) -> float:
        return float(self.search_weight) * float(self.value_score)


def _to_int(x: Any, default: int) -> int:
    try:
        if x is None or str(x).strip() == "":
            return default
        return int(float(str(x).strip()))
    except Exception:
        return default


def _to_float(x: Any, default: float) -> float:
    try:
        if x is None or str(x).strip() == "":
            return default
        return float(str(x).strip())
    except Exception:
        return default


def load_config_routes(sh: gspread.Spreadsheet, theme_today: str) -> List[ConfigRoute]:
    ws = sh.worksheet(CONFIG_TAB)
    headers, rows = read_table(ws)
    if not headers:
        raise RuntimeError("CONFIG is empty / missing headers.")

    hm = _header_map(headers)

    def col(name: str) -> int:
        if name not in hm:
            raise RuntimeError(f"CONFIG missing required header: {name}")
        return hm[name]

    eligible: List[ConfigRoute] = []

    for r in rows:
        # Guard: rows can be shorter than headers
        if len(r) < len(headers):
            r = r + [""] * (len(headers) - len(r))

        enabled = str(r[col("enabled")]).strip().upper() == "TRUE"
        active = str(r[col("active_in_feeder")]).strip().upper() == "TRUE"
        if not (enabled and active):
            continue

        origin = str(r[col("origin_iata")]).strip()
        dest = str(r[col("destination_iata")]).strip()
        if not origin or not dest:
            continue  # CONFIG is the brain: if origin/dest missing, route doesn't exist

        # Theme match: accept if theme_today matches any of the theme columns.
        primary_theme = str(r[col("primary_theme")]).strip()
        short_theme = str(r[col("short_stay_theme")]).strip()
        winter_theme = str(r[col("long_stay_winter_theme")]).strip()
        summer_theme = str(r[col("long_stay_summer_theme")]).strip()

        theme_match = theme_today in {primary_theme, short_theme, winter_theme, summer_theme}
        if not theme_match:
            continue

        days_min = _to_int(r[col("days_ahead_min")], 21)
        days_max = _to_int(r[col("days_ahead_max")], 84)
        trip_len = _to_int(r[col("trip_length_days")], 7)
        max_conn = _to_int(r[col("max_connections")], 1)

        inc_air = str(r[col("included_airlines")]).strip()  # "ANY" or "BA,U2,FR"
        cabin = str(r[col("cabin_class")]).strip().lower() or DEFAULT_CABIN

        Fi = _to_float(r[col("search_weight")], 0.5)
        Vi = _to_float(r[col("value_score")], 0.5)

        slot_hint = str(r[col("slot_hint")]).strip().upper()  # optional: AM/PM/BOTH/blank

        eligible.append(
            ConfigRoute(
                origin=origin,
                dest=dest,
                days_min=days_min,
                days_max=days_max,
                trip_len=trip_len,
                max_conn=max_conn,
                included_airlines=inc_air,
                cabin=cabin,
                search_weight=Fi,
                value_score=Vi,
                slot_hint=slot_hint or "BOTH",
                primary_theme=primary_theme,
                short_stay_theme=short_theme,
                long_winter_theme=winter_theme,
                long_summer_theme=summer_theme,
            )
        )

    return eligible


def mmdd_now_utc() -> int:
    return int(datetime.now(timezone.utc).strftime("%m%d"))


def pick_dates(days_min: int, days_max: int, trip_len: int, k: int = 1) -> List[Tuple[str, str]]:
    """
    Deterministic date picks: evenly spaced within window.
    Returns ISO yyyy-mm-dd for out/in.
    """
    today = datetime.now(timezone.utc).date()
    days_min = max(1, days_min)
    days_max = max(days_min, days_max)
    span = days_max - days_min

    outs: List[int] = []
    if k <= 1 or span == 0:
        outs = [days_min + span // 2]
    else:
        step = max(1, span // (k - 1))
        outs = [days_min + i * step for i in range(k)]
        outs = [min(days_max, x) for x in outs]

    pairs: List[Tuple[str, str]] = []
    for d in outs:
        out_date = today + timedelta(days=int(d))
        in_date = out_date + timedelta(days=int(trip_len))
        pairs.append((out_date.isoformat(), in_date.isoformat()))
    return pairs


def duffel_headers() -> Dict[str, str]:
    if not DUFFEL_API_KEY:
        raise RuntimeError("DUFFEL_API_KEY is missing.")
    return {
        "Authorization": f"Bearer {DUFFEL_API_KEY}",
        "Duffel-Version": "v2",
        "Content-Type": "application/json",
    }


def duffel_search_offer(origin: str, dest: str, out_date: str, in_date: str,
                        cabin: str, max_conn: int, included_airlines: str) -> Optional[Dict[str, Any]]:
    """
    Returns the cheapest offer dict (Duffel offer) or None.
    """
    # 1) create offer request
    payload: Dict[str, Any] = {
        "data": {
            "slices": [
                {"origin": origin, "destination": dest, "departure_date": out_date},
                {"origin": dest, "destination": origin, "departure_date": in_date},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": cabin,
        }
    }

    # Airline filters: if "ANY" or blank, omit it
    inc = (included_airlines or "").strip().upper()
    if inc and inc != "ANY":
        # Duffel supports "allowed_carriers" on offer requests in some shapes; keep minimal:
        # We'll pass it via "carrier_filter" if present; if Duffel rejects, it will fail and we treat as no-offer.
        payload["data"]["carrier_filter"] = {"allowed_carriers": [c.strip() for c in inc.split(",") if c.strip()]}

    # max connections: Duffel does not support a strict "max_conn" parameter universally.
    # We enforce it post-filter by counting stops on returned offers.
    r = requests.post(f"{DUFFEL_API}/air/offer_requests", headers=duffel_headers(), json=payload, timeout=60)
    if r.status_code >= 300:
        return None

    offer_request_id = r.json()["data"]["id"]

    # 2) list offers
    r2 = requests.get(
        f"{DUFFEL_API}/air/offers",
        headers=duffel_headers(),
        params={"offer_request_id": offer_request_id, "limit": 50},
        timeout=60,
    )
    if r2.status_code >= 300:
        return None

    offers = r2.json().get("data", []) or []
    if not offers:
        return None

    # Filter by max_conn (stops)
    filtered: List[Dict[str, Any]] = []
    for off in offers:
        try:
            slices = off.get("slices", [])
            # stops per slice = segments-1; total stops = max over slices or sum; we use max(slice stops)
            slice_stops = []
            for s in slices:
                segs = s.get("segments", []) or []
                slice_stops.append(max(0, len(segs) - 1))
            stops = max(slice_stops) if slice_stops else 99
            if stops <= max_conn:
                filtered.append(off)
        except Exception:
            continue

    if not filtered:
        return None

    # Cheapest by total_amount (string)
    def price(off: Dict[str, Any]) -> float:
        try:
            return float(off.get("total_amount", "999999"))
        except Exception:
            return 999999.0

    filtered.sort(key=price)
    return filtered[0]


def safe_deal_id(origin: str, dest: str, out_date: str, in_date: str, cabin: str, price_gbp: int) -> str:
    base = f"{origin}|{dest}|{out_date}|{in_date}|{cabin}|{price_gbp}"
    h = hashlib.sha1(base.encode("utf-8")).hexdigest()[:12]
    return f"d_{out_date.replace('-','')}_{h}"


def parse_offer_to_row(offer: Dict[str, Any], theme: str, publish_window: str, ingested_iso: str) -> Optional[List[Any]]:
    """
    Convert Duffel offer to RAW_DEALS row.
    We keep enrichment minimal: city/country blank (enrich fills later if you want).
    """
    try:
        total_amount = float(offer["total_amount"])
        currency = offer.get("total_currency", "") or offer.get("total_currency", "")
        # We only want GBP rows. If Duffel returns non-GBP, skip.
        if str(currency).upper() != "GBP":
            return None

        origin = offer["slices"][0]["origin"]["iata_code"]
        dest = offer["slices"][0]["destination"]["iata_code"]
        out_date = offer["slices"][0]["segments"][0]["departing_at"][:10]
        in_date = offer["slices"][1]["segments"][0]["departing_at"][:10]

        # Stops (max stops across slices)
        slice_stops = []
        carriers = set()
        for s in offer.get("slices", []):
            segs = s.get("segments", []) or []
            slice_stops.append(max(0, len(segs) - 1))
            for seg in segs:
                carrier = (seg.get("marketing_carrier") or {}).get("iata_code")
                if carrier:
                    carriers.add(carrier)
        stops = max(slice_stops) if slice_stops else 0

        cabin = (offer.get("cabin_class") or DEFAULT_CABIN).lower()
        price_gbp = int(math.ceil(total_amount))

        deal_id = safe_deal_id(origin, dest, out_date, in_date, cabin, price_gbp)

        row = [
            deal_id,
            origin,
            dest,
            "",  # origin_city (enrich later)
            "",  # destination_city
            "",  # destination_country
            out_date,
            in_date,
            price_gbp,
            "GBP",
            stops,
            cabin,
            ",".join(sorted(carriers)) if carriers else "",
            theme,
            "NEW",
            publish_window,
            "",   # score (scorer owns)
            "",   # phrase_used (enrich owns)
            "",   # graphic_url (render owns)
            "",   # posted_vip_at
            "",   # posted_free_at
            "",   # posted_instagram_at
            ingested_iso,
        ]
        return row
    except Exception:
        return None


def bulk_append(ws: gspread.Worksheet, rows: List[List[Any]]) -> None:
    # gspread v6: append_rows is fast
    ws.append_rows(rows, value_input_option="USER_ENTERED")


def main() -> int:
    if RANDOM_SEED:
        random.seed(RANDOM_SEED)

    log("===============================================================================")
    log("TRAVELTXTTER V5 — FEEDER START (CONFIG-DRIVEN, Pi RANKING)")
    log("===============================================================================")

    gc = gspread_client()
    sh = open_sheet(gc)

    theme_today = get_ops_theme(sh)
    log(f"🎯 Theme of day: {theme_today}")

    ws_raw = sh.worksheet(RAW_DEALS_TAB)
    hm_raw = ensure_raw_deals_headers(ws_raw)

    # Load RAW_DEALS for dedupe
    _, raw_rows = read_table(ws_raw)
    existing = build_existing_keys(raw_rows, hm_raw)

    # Load config candidates
    routes = load_config_routes(sh, theme_today)
    if not routes:
        log("⚠️ No CONFIG routes eligible for theme (enabled+active_in_feeder+theme match).")
        return 0

    # Rank by Pi and keep top pool for variety
    routes.sort(key=lambda r: r.pi, reverse=True)
    top_pool = routes[: max(DESTS_PER_RUN * 3, DESTS_PER_RUN)]
    # Variety: sample from top_pool but preserve Pi bias
    chosen = top_pool[:DESTS_PER_RUN]

    log(f"CAPS: MAX_SEARCHES={MAX_SEARCHES} | MAX_INSERTS={MAX_INSERTS} | DESTS_PER_RUN={DESTS_PER_RUN}")
    log(f"Eligible CONFIG routes: {len(routes)} | using: {len(chosen)} (top-ranked by Pi)")

    ingested_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    inserts: List[List[Any]] = []
    searches = 0
    skips_dedupe = 0
    skips_no_offer = 0

    # For each selected route, try 1 date-pair (simple and cheap).
    # (You can raise K later if budget allows.)
    for route in chosen:
        if searches >= MAX_SEARCHES or len(inserts) >= MAX_INSERTS:
            break

        # Publish window: if CONFIG gives AM/PM/BOTH use it; else BOTH.
        publish_window = route.slot_hint if route.slot_hint in {"AM", "PM", "BOTH"} else "BOTH"

        (out_date, in_date) = pick_dates(route.days_min, route.days_max, route.trip_len, k=1)[0]

        key = f"{route.origin}|{route.dest}|{out_date}|{in_date}|{route.cabin}"
        if key in existing:
            skips_dedupe += 1
            continue

        searches += 1
        log(
            f"🔎 Search {searches}/{MAX_SEARCHES}: {route.origin}→{route.dest} "
            f"| Pi={route.pi:.2f} | max_conn={route.max_conn} | cabin={route.cabin} "
            f"| trip={route.trip_len}d | window={route.days_min}-{route.days_max}"
        )

        offer = duffel_search_offer(
            origin=route.origin,
            dest=route.dest,
            out_date=out_date,
            in_date=in_date,
            cabin=route.cabin,
            max_conn=route.max_conn,
            included_airlines=route.included_airlines,
        )

        if not offer:
            skips_no_offer += 1
            time.sleep(SLEEP_SECONDS)
            continue

        row = parse_offer_to_row(
            offer=offer,
            theme=theme_today,
            publish_window=publish_window,
            ingested_iso=ingested_iso,
        )
        if not row:
            skips_no_offer += 1
            time.sleep(SLEEP_SECONDS)
            continue

        # update dedupe set for run-local duplicates
        existing.add(key)
        inserts.append(row)

        time.sleep(SLEEP_SECONDS)

    if not inserts:
        log("⚠️ No rows inserted.")
        log(f"SKIPS: dedupe={skips_dedupe} no_offer={skips_no_offer}")
        return 0

    # Bulk append
    bulk_append(ws_raw, inserts)
    log(f"✅ Inserted {len(inserts)} row(s) into {RAW_DEALS_TAB}.")
    log(f"SUMMARY: searches={searches} inserted={len(inserts)} dedupe_skips={skips_dedupe} no_offer_skips={skips_no_offer}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
