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
#
# GOAL:
#   Insert NEW raw inventory into RAW_DEALS using Duffel.
#
# READS (Sheets):
#   - OPS_MASTER!B2            theme_of_the_day
#   - OPS_MASTER!C2:C          active_themes (optional, for 90/10 drift)
#   - CONFIG                   enabled,destination_iata,theme,weight
#   - RAW_DEALS                dedupe (recent) + schema check
#
# WRITES:
#   - RAW_DEALS: append NEW rows
#
# DOES NOT:
#   - score, enrich, render, publish
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

# 90/10 drift (secondary theme budget). Set to 0 to fully lock.
PRIMARY_PCT = float(os.getenv("FEEDER_PRIMARY_PCT", "0.90"))  # 0.90 -> 90/10
PRIMARY_PCT = min(1.0, max(0.0, PRIMARY_PCT))

# Dedupe window: only look at the most recent N rows to avoid reading thousands
DEDUP_READ_ROWS = int(os.getenv("FEEDER_DEDUP_READ_ROWS", "1500"))

SLEEP_SECONDS = float(os.getenv("FEEDER_SLEEP_SECONDS", "0.05"))
RANDOM_SEED = os.getenv("FEEDER_RANDOM_SEED", "")
DEFAULT_CABIN = os.getenv("DEFAULT_CABIN_CLASS", "economy").lower().strip() or "economy"

# ------------------------- RAW_DEALS CONTRACT -------------------------
RAW_DEALS_HEADERS_REQUIRED = [
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
]

# ------------------------- CONFIG CONTRACT (MINIMAL) -------------------------
# enabled, destination_iata, theme, weight
CONFIG_HEADERS_REQUIRED = ["enabled", "destination_iata", "theme", "weight"]


# ------------------------- THEME DEFAULTS (minimal “brain”) -------------------------
# If you want to tune these later, do it here (or move to ZTB later),
# but keep CONFIG minimal.
THEME_DEFAULTS: Dict[str, Dict[str, Any]] = {
    "adventure": {"window": (21, 84), "trip": (6, 31), "max_conn": 1, "cabin": "economy"},
    "beach_break": {"window": (21, 84), "trip": (5, 7), "max_conn": 0, "cabin": "economy"},
    "city_breaks": {"window": (10, 84), "trip": (2, 5), "max_conn": 0, "cabin": "economy"},
    "culture_history": {"window": (21, 84), "trip": (4, 7), "max_conn": 0, "cabin": "economy"},
    "long_haul": {"window": (21, 84), "trip": (7, 14), "max_conn": 1, "cabin": "economy"},
    "luxury_value": {"window": (21, 84), "trip": (4, 14), "max_conn": 1, "cabin": "economy"},
    "northern_lights": {"window": (21, 84), "trip": (4, 8), "max_conn": 1, "cabin": "economy"},
    "snow": {"window": (14, 84), "trip": (4, 8), "max_conn": 1, "cabin": "economy"},
    "summer_sun": {"window": (21, 84), "trip": (5, 14), "max_conn": 1, "cabin": "economy"},
    "surf": {"window": (21, 84), "trip": (5, 15), "max_conn": 1, "cabin": "economy"},
    "unexpected_value": {"window": (7, 84), "trip": (3, 14), "max_conn": 1, "cabin": "economy"},
    "winter_sun": {"window": (14, 84), "trip": (4, 10), "max_conn": 1, "cabin": "economy"},
    "default": {"window": (21, 84), "trip": (4, 10), "max_conn": 1, "cabin": "economy"},
}


def log(msg: str) -> None:
    print(f"{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')} | {msg}", flush=True)


def _normalize_sa_json(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()
    if not raw:
        raise RuntimeError("Missing service account JSON (GCP_SA_JSON or GCP_SA_JSON_ONE_LINE).")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass
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


def _hm(headers: List[str]) -> Dict[str, int]:
    return {h.strip(): i for i, h in enumerate(headers) if h and h.strip()}


def _is_true(v: Any) -> bool:
    return str(v).strip().upper() == "TRUE"


def _safe_float(v: Any, default: float) -> float:
    try:
        if v is None or str(v).strip() == "":
            return default
        return float(str(v).strip())
    except Exception:
        return default


def _theme_key(s: str) -> str:
    return (s or "").strip().lower()


def get_origins_for_theme(theme: str) -> List[str]:
    """
    Origins live in ENV (your variables). Example: ORIGINS_NORTHERN_LIGHTS.
    If missing, fall back to ORIGINS_DEFAULT.
    """
    t = theme.upper()
    key = f"ORIGINS_{t}"
    raw = os.getenv(key) or os.getenv("ORIGINS_DEFAULT") or "LHR,MAN,LGW"
    origins = [o.strip().upper() for o in raw.split(",") if o.strip()]
    # de-dupe while preserving order
    seen = set()
    out: List[str] = []
    for o in origins:
        if o not in seen:
            seen.add(o)
            out.append(o)
    return out


def get_theme_defaults(theme: str) -> Dict[str, Any]:
    t = _theme_key(theme)
    return THEME_DEFAULTS.get(t, THEME_DEFAULTS["default"])


def duffel_headers() -> Dict[str, str]:
    if not DUFFEL_API_KEY:
        raise RuntimeError("DUFFEL_API_KEY is missing.")
    return {
        "Authorization": f"Bearer {DUFFEL_API_KEY}",
        "Duffel-Version": "v2",
        "Content-Type": "application/json",
    }


def pick_dates(days_min: int, days_max: int, trip_len: int, k: int = 1) -> List[Tuple[str, str]]:
    """
    Deterministic date picks: evenly spaced within the window.
    """
    today = datetime.now(timezone.utc).date()
    days_min = max(1, days_min)
    days_max = max(days_min, days_max)
    span = days_max - days_min

    if k <= 1 or span == 0:
        outs = [days_min + span // 2]
    else:
        step = max(1, span // (k - 1))
        outs = [min(days_max, days_min + i * step) for i in range(k)]

    pairs: List[Tuple[str, str]] = []
    for d in outs:
        out_date = today + timedelta(days=int(d))
        in_date = out_date + timedelta(days=int(trip_len))
        pairs.append((out_date.isoformat(), in_date.isoformat()))
    return pairs


def duffel_search_offer(
    origin: str,
    dest: str,
    out_date: str,
    in_date: str,
    cabin: str,
    max_conn: int,
    included_airlines_csv: str = "ANY",
) -> Optional[Dict[str, Any]]:
    """
    Returns cheapest offer dict or None.
    We enforce max_conn by filtering returned offers.
    """
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

    inc = (included_airlines_csv or "").strip().upper()
    if inc and inc != "ANY":
        payload["data"]["carrier_filter"] = {
            "allowed_carriers": [c.strip() for c in inc.split(",") if c.strip()]
        }

    r = requests.post(f"{DUFFEL_API}/air/offer_requests", headers=duffel_headers(), json=payload, timeout=60)
    if r.status_code >= 300:
        return None

    offer_request_id = r.json()["data"]["id"]

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

    def stops_of(offer: Dict[str, Any]) -> int:
        slice_stops = []
        for s in offer.get("slices", []) or []:
            segs = s.get("segments", []) or []
            slice_stops.append(max(0, len(segs) - 1))
        return max(slice_stops) if slice_stops else 99

    filtered = [o for o in offers if stops_of(o) <= max_conn]
    if not filtered:
        return None

    def price(o: Dict[str, Any]) -> float:
        try:
            return float(o.get("total_amount", "999999"))
        except Exception:
            return 999999.0

    filtered.sort(key=price)
    return filtered[0]


def safe_deal_id(origin: str, dest: str, out_date: str, in_date: str, cabin: str, price_gbp: int) -> str:
    base = f"{origin}|{dest}|{out_date}|{in_date}|{cabin}|{price_gbp}"
    h = hashlib.sha1(base.encode("utf-8")).hexdigest()[:12]
    return f"d_{out_date.replace('-','')}_{h}"


def offer_to_row(
    offer: Dict[str, Any],
    theme: str,
    publish_window: str,
    ingested_iso: str,
) -> Optional[List[Any]]:
    """
    Convert offer -> RAW_DEALS row. Keep city/country blank (enrich later).
    NOTE: currency must be GBP for pipeline simplicity.
    """
    try:
        total_amount = float(offer["total_amount"])
        currency = (offer.get("total_currency") or "").upper()
        if currency != "GBP":
            return None

        origin = offer["slices"][0]["origin"]["iata_code"]
        dest = offer["slices"][0]["destination"]["iata_code"]
        out_date = offer["slices"][0]["segments"][0]["departing_at"][:10]
        in_date = offer["slices"][1]["segments"][0]["departing_at"][:10]

        carriers = set()
        slice_stops = []
        for s in offer.get("slices", []) or []:
            segs = s.get("segments", []) or []
            slice_stops.append(max(0, len(segs) - 1))
            for seg in segs:
                c = (seg.get("marketing_carrier") or {}).get("iata_code")
                if c:
                    carriers.add(c)

        stops = max(slice_stops) if slice_stops else 0
        cabin = (offer.get("cabin_class") or DEFAULT_CABIN).lower()
        price_gbp = int(math.ceil(total_amount))
        deal_id = safe_deal_id(origin, dest, out_date, in_date, cabin, price_gbp)

        return [
            deal_id,
            origin,
            dest,
            "",  # origin_city
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
            "",  # score
            "",  # phrase_used
            "",  # graphic_url
            "",  # posted_vip_at
            "",  # posted_free_at
            "",  # posted_instagram_at
            ingested_iso,
        ]
    except Exception:
        return None


def bulk_append(ws: gspread.Worksheet, rows: List[List[Any]]) -> None:
    # Fast append (gspread v6)
    ws.append_rows(rows, value_input_option="USER_ENTERED")


def ensure_headers(headers: List[str], required: List[str], tab_name: str) -> Dict[str, int]:
    if not headers:
        raise RuntimeError(f"{tab_name} is empty. Row 1 must contain headers.")
    if len(set(headers)) != len(headers):
        raise RuntimeError(f"{tab_name} header row contains duplicates. Fix before running workers.")
    hm = _hm(headers)
    missing = [h for h in required if h not in hm]
    if missing:
        raise RuntimeError(f"{tab_name} missing required headers: {missing}")
    return hm


def build_dedupe_keys(raw_headers: List[str], raw_rows: List[List[Any]]) -> set:
    hm = _hm(raw_headers)
    # key: origin|dest|out|in|cabin
    need = ["origin_iata", "destination_iata", "outbound_date", "return_date", "cabin_class"]
    if any(k not in hm for k in need):
        return set()

    keys = set()
    for r in raw_rows:
        if len(r) < len(raw_headers):
            r = r + [""] * (len(raw_headers) - len(r))
        o = str(r[hm["origin_iata"]]).strip()
        d = str(r[hm["destination_iata"]]).strip()
        outd = str(r[hm["outbound_date"]]).strip()
        ind = str(r[hm["return_date"]]).strip()
        cab = str(r[hm["cabin_class"]]).strip().lower()
        if o and d and outd and ind and cab:
            keys.add(f"{o}|{d}|{outd}|{ind}|{cab}")
    return keys


@dataclass
class DestCandidate:
    dest: str
    theme: str
    weight: float  # “Fi-ish” ranking weight from CONFIG

    @property
    def pi(self) -> float:
        # Minimal Pi: weight is the only ranking signal in this ultra-min config.
        # If you later re-add value_score, change this to Fi*Vi.
        return float(self.weight)


def read_ops_and_config_and_raw(sh: gspread.Spreadsheet) -> Tuple[str, List[str], List[str], List[List[Any]], List[str], List[List[Any]]]:
    """
    One batch_get call to reduce Sheets API latency.
    Returns:
      theme_today,
      active_themes,
      raw_headers, raw_recent_rows,
      config_headers, config_rows
    """
    # RAW_DEALS: grab headers + last N rows (fast + dedupe-safe enough)
    # We don’t know last row index without extra call; simplest is to read a reasonably bounded range.
    # If your sheet grows beyond this, increase DEDUP_READ_ROWS or move dedupe to a FEEDER_LOG tab later.
    raw_max = 1 + DEDUP_READ_ROWS
    ranges = [
        f"{OPS_TAB}!B2",             # theme_of_the_day
        f"{OPS_TAB}!C2:C",           # active_themes list (optional)
        f"{RAW_DEALS_TAB}!A1:W{raw_max}",  # headers + recent
        f"{CONFIG_TAB}!A1:D",        # minimal config
    ]
    res = sh.values_batch_get(ranges=ranges)
    value_ranges = res.get("valueRanges", [])

    # Unpack safely by index
    ops_theme_vals = (value_ranges[0].get("values") or [[""]])
    theme_today = (ops_theme_vals[0][0] if ops_theme_vals and ops_theme_vals[0] else "").strip()

    active_vals = value_ranges[1].get("values") or []
    active_themes = [str(r[0]).strip() for r in active_vals if r and str(r[0]).strip()]

    raw_vals = value_ranges[2].get("values") or []
    raw_headers = [h.strip() for h in (raw_vals[0] if raw_vals else [])]
    raw_rows = raw_vals[1:] if len(raw_vals) > 1 else []

    cfg_vals = value_ranges[3].get("values") or []
    cfg_headers = [h.strip() for h in (cfg_vals[0] if cfg_vals else [])]
    cfg_rows = cfg_vals[1:] if len(cfg_vals) > 1 else []

    return theme_today, active_themes, raw_headers, raw_rows, cfg_headers, cfg_rows


def load_candidates(theme_today: str, cfg_headers: List[str], cfg_rows: List[List[Any]]) -> List[DestCandidate]:
    hm = ensure_headers(cfg_headers, CONFIG_HEADERS_REQUIRED, "CONFIG")

    cands: List[DestCandidate] = []
    for r in cfg_rows:
        if len(r) < len(cfg_headers):
            r = r + [""] * (len(cfg_headers) - len(r))

        if not _is_true(r[hm["enabled"]]):
            continue

        dest = str(r[hm["destination_iata"]]).strip().upper()
        theme = _theme_key(str(r[hm["theme"]]))
        if not dest or not theme:
            continue
        if theme != _theme_key(theme_today):
            continue

        w = _safe_float(r[hm["weight"]], 1.0)
        cands.append(DestCandidate(dest=dest, theme=theme, weight=w))

    cands.sort(key=lambda c: c.pi, reverse=True)
    return cands


def pick_secondary_themes(theme_today: str, active_themes: List[str]) -> List[str]:
    t = _theme_key(theme_today)
    out = []
    for x in active_themes:
        k = _theme_key(x)
        if k and k != t:
            out.append(k)
    return out


def main() -> int:
    if RANDOM_SEED:
        random.seed(RANDOM_SEED)

    log("===============================================================================")
    log("TRAVELTXTTER V5 — FEEDER START (MIN CONFIG, WEIGHT RANKING)")
    log("===============================================================================")

    gc = gspread_client()
    sh = open_sheet(gc)

    theme_today, active_themes, raw_headers, raw_rows, cfg_headers, cfg_rows = read_ops_and_config_and_raw(sh)

    if not theme_today:
        raise RuntimeError("OPS_MASTER!B2 (theme_of_the_day) is blank.")
    theme_today = _theme_key(theme_today)
    log(f"🎯 Theme of day: {theme_today}")

    # Validate RAW_DEALS headers
    ensure_headers(raw_headers, RAW_DEALS_HEADERS_REQUIRED, "RAW_DEALS")
    existing_keys = build_dedupe_keys(raw_headers, raw_rows)

    # Load config candidates for theme
    candidates = load_candidates(theme_today, cfg_headers, cfg_rows)
    if not candidates:
        log("⚠️ No CONFIG routes eligible for theme (enabled + theme match).")
        return 0

    # Determine budgets
    primary_budget = max(1, int(round(MAX_SEARCHES * PRIMARY_PCT)))
    secondary_budget = max(0, MAX_SEARCHES - primary_budget)

    # If no drift desired, set secondary_budget = 0
    if PRIMARY_PCT >= 1.0:
        secondary_budget = 0

    log(f"CAPS: MAX_SEARCHES={MAX_SEARCHES} | MAX_INSERTS={MAX_INSERTS} | DESTS_PER_RUN={DESTS_PER_RUN}")
    log(f"Budget: primary={primary_budget} secondary={secondary_budget}")

    # Pick top destinations (variety)
    chosen_primary = candidates[: max(1, min(DESTS_PER_RUN, len(candidates)))]

    # Optional secondary drift: pick from other active themes (OPS_MASTER column C)
    secondary_themes = pick_secondary_themes(theme_today, active_themes)
    chosen_secondary: List[DestCandidate] = []
    if secondary_budget > 0 and secondary_themes:
        # Build secondary pool from CONFIG (enabled rows with theme in secondary_themes)
        hm = ensure_headers(cfg_headers, CONFIG_HEADERS_REQUIRED, "CONFIG")
        pool: List[DestCandidate] = []
        for r in cfg_rows:
            if len(r) < len(cfg_headers):
                r = r + [""] * (len(cfg_headers) - len(r))
            if not _is_true(r[hm["enabled"]]):
                continue
            dest = str(r[hm["destination_iata"]]).strip().upper()
            theme = _theme_key(str(r[hm["theme"]]))
            if not dest or not theme:
                continue
            if theme not in set(secondary_themes):
                continue
            w = _safe_float(r[hm["weight"]], 1.0)
            pool.append(DestCandidate(dest=dest, theme=theme, weight=w))
        pool.sort(key=lambda c: c.pi, reverse=True)
        chosen_secondary = pool[: max(1, min(2, len(pool)))] if pool else []

    # Build search plan: (theme, origin, dest, out_date, in_date, max_conn, cabin, publish_window)
    ingested_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    inserts: List[List[Any]] = []

    def run_searches(chosen: List[DestCandidate], budget: int, lane: str) -> Tuple[int, int, int]:
        searches = 0
        dedupe_skips = 0
        no_offer_skips = 0

        for cand in chosen:
            if searches >= budget or len(inserts) >= MAX_INSERTS:
                break

            # Theme defaults drive constraints
            defaults = get_theme_defaults(cand.theme)
            days_min, days_max = defaults["window"]
            trip_min, trip_max = defaults["trip"]
            max_conn = int(defaults["max_conn"])
            cabin = str(defaults.get("cabin") or DEFAULT_CABIN).lower()

            # deterministic “middle” trip length
            trip_len = int(round((trip_min + trip_max) / 2))

            # origins are ENV-driven
            origins = get_origins_for_theme(cand.theme)
            if not origins:
                origins = get_origins_for_theme("default")

            # Choose 1 origin per destination (rotate for variety)
            origin = origins[searches % len(origins)]

            out_date, in_date = pick_dates(days_min, days_max, trip_len, k=1)[0]
            key = f"{origin}|{cand.dest}|{out_date}|{in_date}|{cabin}"
            if key in existing_keys:
                dedupe_skips += 1
                continue

            searches += 1
            publish_window = "BOTH"  # minimal contract; publishers can route by slot later

            log(
                f"🔎 Search {searches}/{budget} [{lane}] {origin}→{cand.dest} "
                f"| weight={cand.weight:.2f} | max_conn={max_conn} | cabin={cabin} "
                f"| trip={trip_len}d | window={days_min}-{days_max}"
            )

            offer = duffel_search_offer(
                origin=origin,
                dest=cand.dest,
                out_date=out_date,
                in_date=in_date,
                cabin=cabin,
                max_conn=max_conn,
                included_airlines_csv="ANY",  # you said this is set to ANY
            )
            if not offer:
                no_offer_skips += 1
                time.sleep(SLEEP_SECONDS)
                continue

            row = offer_to_row(
                offer=offer,
                theme=cand.theme,
                publish_window=publish_window,
                ingested_iso=ingested_iso,
            )
            if not row:
                no_offer_skips += 1
                time.sleep(SLEEP_SECONDS)
                continue

            existing_keys.add(key)
            inserts.append(row)
            time.sleep(SLEEP_SECONDS)

        return searches, dedupe_skips, no_offer_skips

    # Primary lane (theme locked)
    p_searches, p_dedupe, p_no_offer = run_searches(chosen_primary, primary_budget, "PRIMARY")

    # Secondary lane (drift)
    s_searches, s_dedupe, s_no_offer = (0, 0, 0)
    if secondary_budget > 0 and chosen_secondary:
        s_searches, s_dedupe, s_no_offer = run_searches(chosen_secondary, secondary_budget, "SECONDARY")

    total_searches = p_searches + s_searches
    total_dedupe = p_dedupe + s_dedupe
    total_no_offer = p_no_offer + s_no_offer

    if not inserts:
        log("⚠️ No rows inserted.")
        log(f"SKIPS: dedupe={total_dedupe} no_offer={total_no_offer}")
        return 0

    # Bulk append
    ws_raw = sh.worksheet(RAW_DEALS_TAB)
    bulk_append(ws_raw, inserts)
    log(f"✅ Inserted {len(inserts)} row(s) into {RAW_DEALS_TAB}.")
    log(f"SUMMARY: searches={total_searches} inserted={len(inserts)} dedupe_skips={total_dedupe} no_offer_skips={total_no_offer}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
