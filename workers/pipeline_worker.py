# workers/pipeline_worker.py
# FULL FILE REPLACEMENT — FEEDER v4.8c
# Fix: ensure RAW_DEALS.deal_id is always populated (deterministic) + populate stops
# Also: CONFIG_CARRIER_BIAS loader accepts your current headers (carrier_code, carrier_name, theme, destination_iata, bias_weight, notes)
# Governance: writes ONLY to RAW_DEALS. No schema renames. No RDV writes.

from __future__ import annotations

import os
import sys
import time
import json
import math
import hashlib
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
if THIS_DIR not in sys.path:
    sys.path.insert(0, THIS_DIR)


def log(msg: str) -> None:
    ts = dt.datetime.utcnow().isoformat(timespec="microseconds") + "Z"
    print(f"{ts} | {msg}", flush=True)


def _env(k: str, default: str = "") -> str:
    return str(os.getenv(k, default) or "").strip()


def _env_int(k: str, default: int) -> int:
    v = _env(k, "")
    try:
        return int(v)
    except Exception:
        return default


def _safe_int(v: Any, default: int) -> int:
    try:
        if v is None:
            return default
        if isinstance(v, (int, float)):
            return int(v)
        s = str(v).strip()
        if s == "":
            return default
        return int(float(s))
    except Exception:
        return default


def _safe_float(v: Any, default: float) -> float:
    try:
        if v is None:
            return default
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        if s == "":
            return default
        return float(s)
    except Exception:
        return default


def _is_true(v: Any) -> bool:
    s = str(v or "").strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def _csv_list(v: str) -> List[str]:
    out: List[str] = []
    for x in (v or "").replace(";", ",").split(","):
        x = x.strip().upper()
        if x:
            out.append(x)
    return list(dict.fromkeys(out))


def _today_utc() -> dt.date:
    return dt.datetime.utcnow().date()


def _mmdd(d: dt.date) -> int:
    return int(d.strftime("%m%d"))


def _in_window(mmdd: int, start_mmdd: int, end_mmdd: int) -> bool:
    if start_mmdd <= end_mmdd:
        return start_mmdd <= mmdd <= end_mmdd
    return (mmdd >= start_mmdd) or (mmdd <= end_mmdd)


def _sha12(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:12]


def _deal_id(origin: str, dest: str, od: dt.date, rd: dt.date, price_gbp: float, cabin: str, max_conn: int) -> str:
    # Deterministic + stable across retries for the same search outcome.
    # IMPORTANT: must not be blank — scorer and downstream logic depend on it.
    price_i = int(math.ceil(float(price_gbp)))
    base = f"{origin}|{dest}|{od.isoformat()}|{rd.isoformat()}|{price_i}|{cabin}|{max_conn}"
    return f"D{_sha12(base)}"


def _normalize_header(h: Any) -> str:
    return str(h or "").strip().lower().replace(" ", "_")


def _build_header_index(headers: List[str]) -> Dict[str, int]:
    idx: Dict[str, int] = {}
    for i, h in enumerate(headers):
        hh = str(h or "").strip()
        if hh:
            idx[hh] = i
    return idx


def _append_rows(ws: gspread.Worksheet, rows: List[List[Any]]) -> None:
    if rows:
        ws.append_rows(rows, value_input_option="USER_ENTERED")


def _get_gspread_client() -> gspread.Client:
    sa_json = _env("GCP_SA_JSON_ONE_LINE") or _env("GCP_SA_JSON")
    if not sa_json:
        raise RuntimeError("Missing GCP_SA_JSON or GCP_SA_JSON_ONE_LINE")
    try:
        creds_info = json.loads(sa_json)
    except Exception:
        creds_info = json.loads(sa_json.replace("\\n", "\n"))
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)


# -----------------------------
# Duffel request helpers
# -----------------------------

DUFFEL_API_KEY = _env("DUFFEL_API_KEY")

DUFFEL_ENDPOINT = "https://api.duffel.com/air/offer_requests"
DUFFEL_HEADERS = {
    "Authorization": f"Bearer {DUFFEL_API_KEY}",
    "Duffel-Version": "v2",
    "Content-Type": "application/json",
}


def _duffel_offer_request(
    origin: str,
    destination: str,
    outbound_date: dt.date,
    return_date: dt.date,
    cabin: str,
    max_connections: int,
    included_airlines: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    passengers = [{"type": "adult"}]
    slices = [
        {"origin": origin, "destination": destination, "departure_date": outbound_date.isoformat()},
        {"origin": destination, "destination": origin, "departure_date": return_date.isoformat()},
    ]

    data: Dict[str, Any] = {
        "data": {
            "slices": slices,
            "passengers": passengers,
            "cabin_class": cabin.lower() if cabin else "economy",
        }
    }

    # Duffel uses "max_connections" on the offer request payload
    if max_connections is not None:
        data["data"]["max_connections"] = int(max_connections)

    if included_airlines:
        data["data"]["allowed_carrier_codes"] = [c for c in included_airlines if c]

    try:
        r = requests.post(DUFFEL_ENDPOINT, headers=DUFFEL_HEADERS, json=data, timeout=60)
        if r.status_code >= 400:
            log(f"❌ Duffel offer_request failed {origin}->{destination}: {r.status_code} {r.text[:300]}")
            return None
        return r.json()
    except Exception as e:
        log(f"❌ Duffel request exception {origin}->{destination}: {e}")
        return None


def _min_price_gbp(offers_json: Dict[str, Any]) -> Optional[float]:
    try:
        offers = offers_json.get("data", {}).get("offers") or []
        if not offers:
            return None
        prices = []
        for o in offers:
            total = o.get("total_amount")
            cur = o.get("total_currency")
            if not total:
                continue
            if cur and str(cur).upper() != "GBP":
                continue
            prices.append(float(total))
        if not prices:
            return None
        return min(prices)
    except Exception:
        return None


def _candidate_outbounds_seasonal(
    min_d: int,
    max_d: int,
    trip_len: int,
    start_mmdd: int,
    end_mmdd: int,
    n: int = 3,
) -> List[dt.date]:
    today = _today_utc()
    max_start = max(min_d, max_d - trip_len)
    if max_start < min_d:
        max_start = min_d

    # Year-round => normal
    if start_mmdd <= 101 and end_mmdd >= 1231:
        pts = [min_d]
        if n > 1 and max_start != min_d:
            mid = min_d + (max_start - min_d) // 2
            pts = [min_d, mid, max_start]
        return [(today + dt.timedelta(days=p)) for p in pts[:n]]

    eligible: List[dt.date] = []
    for d in range(min_d, max_start + 1):
        od = today + dt.timedelta(days=d)
        if _in_window(_mmdd(od), start_mmdd, end_mmdd):
            eligible.append(od)

    if not eligible:
        return []

    if n <= 1 or len(eligible) == 1:
        return [eligible[0]]

    first = eligible[0]
    last = eligible[-1]
    mid = eligible[len(eligible) // 2]
    out: List[dt.date] = []
    for x in (first, mid, last):
        if x not in out:
            out.append(x)
    return out[:n]


# -----------------------------
# Sheet loaders
# -----------------------------

def _ws_or_none(sh: gspread.Spreadsheet, name: str) -> Optional[gspread.Worksheet]:
    try:
        return sh.worksheet(name)
    except Exception:
        return None


def _load_records(ws: gspread.Worksheet) -> Tuple[List[str], List[Dict[str, Any]]]:
    headers = ws.row_values(1)
    rows = ws.get_all_records()  # uses header row
    return headers, rows


def _load_ztb(sh: gspread.Spreadsheet, tab_name: str) -> List[Dict[str, Any]]:
    ws = _ws_or_none(sh, tab_name)
    if not ws and tab_name != "ZONE_THEME_BENCHMARKS":
        ws = _ws_or_none(sh, "ZONE_THEME_BENCHMARKS")
    if not ws:
        log("⚠️ ZTB tab not found. No themes eligible.")
        return []
    _, rows = _load_records(ws)
    return rows or []


def _load_config(sh: gspread.Spreadsheet, tab_name: str) -> List[Dict[str, Any]]:
    ws = _ws_or_none(sh, tab_name)
    if not ws:
        log(f"❌ CONFIG tab not found: {tab_name}")
        return []
    _, rows = _load_records(ws)
    return rows or []


def _load_geo_dict(sh: gspread.Spreadsheet, rcm_tab: str, iata_tab: str) -> Dict[str, Dict[str, str]]:
    geo: Dict[str, Dict[str, str]] = {}

    # ROUTE_CAPABILITY_MAP (expected: origin_city, origin_iata, origin_country, destination_city, destination_iata, destination_country)
    ws_rcm = _ws_or_none(sh, rcm_tab)
    if ws_rcm:
        _, rows = _load_records(ws_rcm)
        for r in rows or []:
            oi = str(r.get("origin_iata") or "").strip().upper()
            di = str(r.get("destination_iata") or "").strip().upper()
            if oi:
                geo.setdefault(oi, {})
                if r.get("origin_city"):
                    geo[oi]["city"] = str(r.get("origin_city")).strip()
                if r.get("origin_country"):
                    geo[oi]["country"] = str(r.get("origin_country")).strip()
            if di:
                geo.setdefault(di, {})
                if r.get("destination_city"):
                    geo[di]["city"] = str(r.get("destination_city")).strip()
                if r.get("destination_country"):
                    geo[di]["country"] = str(r.get("destination_country")).strip()

    # IATA_MASTER (expected at least: iata, city, country)
    ws_iata = _ws_or_none(sh, iata_tab)
    if ws_iata:
        _, rows = _load_records(ws_iata)
        for r in rows or []:
            code = str(r.get("iata") or r.get("IATA") or "").strip().upper()
            if not code:
                continue
            geo.setdefault(code, {})
            city = r.get("city") or r.get("City")
            country = r.get("country") or r.get("Country")
            if city and not geo[code].get("city"):
                geo[code]["city"] = str(city).strip()
            if country and not geo[code].get("country"):
                geo[code]["country"] = str(country).strip()

    return geo


def _load_carrier_bias(sh: gspread.Spreadsheet, tab_name: str) -> List[Dict[str, Any]]:
    ws = _ws_or_none(sh, tab_name)
    if not ws:
        return []

    # Prefer get_all_values so we can normalize headers ourselves.
    values = ws.get_all_values()
    if not values or len(values) < 2:
        return []

    raw_headers = values[0]
    norm_headers = [_normalize_header(h) for h in raw_headers]
    expected = {"carrier_code", "theme", "destination_iata", "bias_weight"}

    if not expected.issubset(set(norm_headers)):
        # fallback: use get_all_records, but only if it returns meaningful dicts
        recs = ws.get_all_records()
        if recs:
            return recs
        return []

    out: List[Dict[str, Any]] = []
    for row in values[1:]:
        d: Dict[str, Any] = {}
        for i, h in enumerate(norm_headers):
            d[h] = row[i] if i < len(row) else ""
        out.append(d)
    return out


# -----------------------------
# Selection + scoring helpers
# -----------------------------

def _ztb_enabled(r: Dict[str, Any]) -> bool:
    v = r.get("enabled")
    if isinstance(v, bool):
        return bool(v)
    return _is_true(v) or str(v or "").strip().lower() in ("yes", "y")


def _ztb_theme(r: Dict[str, Any]) -> str:
    return str(r.get("theme") or "").strip()


def _ztb_window(r: Dict[str, Any]) -> Tuple[int, int]:
    s = _safe_int(r.get("start_mmdd"), 101)
    e = _safe_int(r.get("end_mmdd"), 1231)
    return s, e


def _ztb_max_conn(r: Dict[str, Any]) -> int:
    # connection_tolerance can be 0/1/2 (direct/1-stop/2-stop)
    return _safe_int(r.get("connection_tolerance"), 1)


def _ztb_price_bounds(r: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    mn = r.get("price_psychology_min")
    mx = r.get("price_psychology_max")
    fmn = _safe_float(mn, -1.0)
    fmx = _safe_float(mx, -1.0)
    if fmn <= 0:
        fmn = None
    if fmx <= 0:
        fmx = None
    return fmn, fmx


def _cfg_enabled(r: Dict[str, Any]) -> bool:
    v = r.get("enabled")
    if isinstance(v, bool):
        return bool(v)
    return _is_true(v) or str(v or "").strip().lower() in ("yes", "y")


def _cfg_origin(r: Dict[str, Any]) -> str:
    return str(r.get("origin_iata") or "").strip().upper()


def _cfg_dest(r: Dict[str, Any]) -> str:
    return str(r.get("destination_iata") or "").strip().upper()


def _cfg_theme(r: Dict[str, Any]) -> str:
    return str(r.get("primary_theme") or r.get("theme") or "").strip()


def _cfg_trip_len(r: Dict[str, Any]) -> int:
    return _safe_int(r.get("trip_length_days"), 5)


def _cfg_days_min(r: Dict[str, Any]) -> int:
    return _safe_int(r.get("days_ahead_min"), 45)


def _cfg_days_max(r: Dict[str, Any]) -> int:
    return _safe_int(r.get("days_ahead_max"), 120)


def _cfg_max_conn(r: Dict[str, Any], theme_max_conn: int) -> int:
    # config max_connections can override theme tolerance; otherwise theme tolerance wins
    v = r.get("max_connections")
    if v is None or str(v).strip() == "":
        return theme_max_conn
    return _safe_int(v, theme_max_conn)


def _cfg_cabin(r: Dict[str, Any]) -> str:
    c = str(r.get("cabin_class") or "economy").strip().lower()
    if c not in ("economy", "premium_economy", "business", "first"):
        c = "economy"
    return c


def _cfg_included_airlines(r: Dict[str, Any]) -> List[str]:
    return _csv_list(str(r.get("included_airlines") or ""))


def _pick_weighted_deterministic(items: List[Tuple[str, float]], salt: str) -> Optional[str]:
    # Deterministic "random" pick based on SHA1(salt), weighted by bias_weight
    if not items:
        return None
    cleaned: List[Tuple[str, float]] = []
    for k, w in items:
        kk = str(k or "").strip().upper()
        ww = float(w) if w is not None else 0.0
        if kk and ww > 0:
            cleaned.append((kk, ww))
    if not cleaned:
        return None

    total = sum(w for _, w in cleaned)
    if total <= 0:
        return None

    h = hashlib.sha1(salt.encode("utf-8")).hexdigest()
    # map first 8 hex chars -> [0,1)
    x = int(h[:8], 16) / float(0xFFFFFFFF)
    tgt = x * total

    acc = 0.0
    for k, w in cleaned:
        acc += w
        if tgt <= acc:
            return k
    return cleaned[-1][0]


def main() -> int:
    log("================================================================================")
    log("TRAVELTXTTER PIPELINE WORKER (FEEDER) START")
    log("================================================================================")

    if not DUFFEL_API_KEY:
        log("❌ Missing DUFFEL_API_KEY")
        return 1

    SPREADSHEET_ID = (_env("SPREADSHEET_ID") or _env("SHEET_ID")).strip()
    if not SPREADSHEET_ID:
        log("❌ Missing SPREADSHEET_ID / SHEET_ID env var")
        return 1

    RAW_DEALS_TAB = _env("RAW_DEALS_TAB", "RAW_DEALS")
    CONFIG_TAB = _env("FEEDER_CONFIG_TAB", _env("CONFIG_TAB", "CONFIG"))
    ZTB_TAB = _env("ZTB_TAB", "ZTB")
    RCM_TAB = _env("RCM_TAB", "ROUTE_CAPABILITY_MAP")
    IATA_TAB = _env("IATA_TAB", "IATA_MASTER")
    CARRIER_BIAS_TAB = _env("CONFIG_CARRIER_BIAS_TAB", "CONFIG_CARRIER_BIAS")

    DUFFEL_MAX_INSERTS = _env_int("DUFFEL_MAX_INSERTS", _env_int("FEEDER_MAX_INSERTS", 20))
    DUFFEL_MAX_INSERTS_PER_ORIGIN = _env_int("DUFFEL_MAX_INSERTS_PER_ORIGIN", 15)
    DUFFEL_MAX_INSERTS_PER_ROUTE = _env_int("DUFFEL_MAX_INSERTS_PER_ROUTE", 5)
    DUFFEL_MAX_SEARCHES_PER_RUN = _env_int("DUFFEL_MAX_SEARCHES_PER_RUN", _env_int("FEEDER_MAX_SEARCHES", 12))
    DESTS_PER_RUN = _env_int("DUFFEL_ROUTES_PER_RUN", 4)
    ORIGINS_PER_DEST = _env_int("ORIGINS_PER_DEST", 3)

    log(
        f"CAPS: MAX_INSERTS={DUFFEL_MAX_INSERTS} | PER_ORIGIN={DUFFEL_MAX_INSERTS_PER_ORIGIN} | "
        f"PER_ROUTE={DUFFEL_MAX_INSERTS_PER_ROUTE} | MAX_SEARCHES={DUFFEL_MAX_SEARCHES_PER_RUN} | "
        f"DESTS_PER_RUN={DESTS_PER_RUN} | ORIGINS_PER_DEST={ORIGINS_PER_DEST}"
    )

    gc = _get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_raw = sh.worksheet(RAW_DEALS_TAB)
    headers = ws_raw.row_values(1)
    idx = _build_header_index(headers)

    # Load ZTB (theme policy)
    ztb_rows = _load_ztb(sh, ZTB_TAB)
    eligible: List[Dict[str, Any]] = []
    pool: List[str] = []

    today = _today_utc()
    today_mmdd = _mmdd(today)

    for r in ztb_rows:
        if not _ztb_enabled(r):
            continue
        theme = _ztb_theme(r)
        if not theme:
            continue
        start_mmdd, end_mmdd = _ztb_window(r)
        if _in_window(today_mmdd, start_mmdd, end_mmdd):
            eligible.append(r)
            pool.append(theme)

    log(f"✅ ZTB loaded: {len(ztb_rows)} rows | eligible_today={len(eligible)} | pool={pool}")

    if not eligible:
        log("⚠️ No eligible themes today (ZTB). Exiting.")
        return 0

    # Theme of the day (deterministic by date)
    theme_today = pool[today.toordinal() % len(pool)]
    log(f"🎯 Theme of the day (UTC): {theme_today}")

    # Load CONFIG
    cfg_rows = _load_config(sh, CONFIG_TAB)
    cfg_active = [r for r in cfg_rows if _cfg_enabled(r)]
    log(f"✅ CONFIG loaded: {len(cfg_active)} active rows (of {len(cfg_rows)} total)")

    # Load carrier bias (optional) — your current headers supported
    carrier_bias_rows = _load_carrier_bias(sh, CARRIER_BIAS_TAB)
    carrier_bias_by_theme_dest: Dict[Tuple[str, str], List[Tuple[str, float]]] = {}
    if carrier_bias_rows:
        for r in carrier_bias_rows:
            # Support both normalized and original keys
            theme = str(r.get("theme") or r.get("Theme") or "").strip()
            dest = str(r.get("destination_iata") or r.get("destination") or r.get("Destination_IATA") or "").strip().upper()
            carrier = str(r.get("carrier_code") or r.get("carrier") or r.get("Carrier_Code") or "").strip().upper()
            w = _safe_float(r.get("bias_weight") or r.get("weight") or r.get("Bias_Weight"), 0.0)
            if theme and dest and carrier and w > 0:
                carrier_bias_by_theme_dest.setdefault((theme, dest), []).append((carrier, w))

    if not carrier_bias_by_theme_dest:
        log("⚠️ CONFIG_CARRIER_BIAS not loaded or empty. Airline hints will not be applied.")
    else:
        log(f"✅ CONFIG_CARRIER_BIAS loaded: {sum(len(v) for v in carrier_bias_by_theme_dest.values())} usable rows")

    # Geo dictionary for enrichment (origin/dest city/country)
    geo = _load_geo_dict(sh, RCM_TAB, IATA_TAB)
    log(f"✅ Geo dictionary loaded: {len(geo)} IATA entries (RCM + IATA_MASTER)")

    # Filter config for today’s theme
    cfg_theme_rows = [c for c in cfg_active if _cfg_theme(c) == theme_today]
    if not cfg_theme_rows:
        log(f"⚠️ No CONFIG rows found for theme_today={theme_today}. Exiting.")
        return 0

    # Select up to DESTS_PER_RUN destinations to attempt (by CONFIG priority if present)
    def _prio(c: Dict[str, Any]) -> int:
        return _safe_int(c.get("priority"), 999)

    cfg_theme_rows = sorted(cfg_theme_rows, key=_prio)
    selected = cfg_theme_rows[: max(1, DESTS_PER_RUN)]
    log(f"🧭 Selected destinations to attempt: {len(selected)} (cap DESTS_PER_RUN={DESTS_PER_RUN})")

    inserted_by_origin: Dict[str, int] = {}
    inserted_by_route: Dict[Tuple[str, str], int] = {}

    searches = 0
    deals_out: List[List[Any]] = []

    # Precompute theme policy row for today
    theme_policy = None
    for r in eligible:
        if _ztb_theme(r) == theme_today:
            theme_policy = r
            break
    if not theme_policy:
        theme_policy = eligible[0]

    theme_start_mmdd, theme_end_mmdd = _ztb_window(theme_policy)
    theme_max_conn_default = _ztb_max_conn(theme_policy)
    price_min, price_max = _ztb_price_bounds(theme_policy)

    for cfg in selected:
        dest = _cfg_dest(cfg)
        if not dest:
            continue

        # Build origins list:
        # - If CONFIG specifies origin_iata, use it.
        # - Otherwise skip (no open origin scanning in this feeder mode).
        origin = _cfg_origin(cfg)
        if not origin:
            continue

        # Caps per origin/route
        if inserted_by_origin.get(origin, 0) >= DUFFEL_MAX_INSERTS_PER_ORIGIN:
            continue
        if inserted_by_route.get((origin, dest), 0) >= DUFFEL_MAX_INSERTS_PER_ROUTE:
            continue

        # Build date candidates
        days_min = _cfg_days_min(cfg)
        days_max = _cfg_days_max(cfg)
        trip_len = _cfg_trip_len(cfg)
        cabin = _cfg_cabin(cfg)
        max_conn = _cfg_max_conn(cfg, theme_max_conn_default)

        outbounds = _candidate_outbounds_seasonal(
            min_d=days_min,
            max_d=days_max,
            trip_len=trip_len,
            start_mmdd=theme_start_mmdd,
            end_mmdd=theme_end_mmdd,
            n=3,
        )
        if not outbounds:
            continue

        # Airline hints:
        included_airlines = _cfg_included_airlines(cfg)
        if not included_airlines:
            # Apply carrier bias if we have it for (theme, destination_iata)
            key = (theme_today, dest)
            choices = carrier_bias_by_theme_dest.get(key, [])
            if choices:
                salt = f"{theme_today}|{dest}|{origin}|{today.isoformat()}"
                picked = _pick_weighted_deterministic(choices, salt)
                if picked:
                    included_airlines = [picked]

        for od in outbounds:
            rd = od + dt.timedelta(days=trip_len)

            # Final safety (still within window for outbound)
            if not _in_window(_mmdd(od), theme_start_mmdd, theme_end_mmdd):
                continue

            if searches >= DUFFEL_MAX_SEARCHES_PER_RUN:
                break

            offers_json = _duffel_offer_request(
                origin=origin,
                destination=dest,
                outbound_date=od,
                return_date=rd,
                cabin=cabin,
                max_connections=max_conn,
                included_airlines=included_airlines,
            )
            searches += 1
            if not offers_json:
                continue

            price = _min_price_gbp(offers_json)
            if price is None:
                continue

            # Optional price psychology gating (if present)
            if price_min is not None and float(price) < float(price_min):
                continue
            if price_max is not None and float(price) > float(price_max):
                continue

            # Build row aligned to RAW_DEALS headers
            row = [""] * len(headers)

            def set_if(col: str, val: Any) -> None:
                j = idx.get(col)
                if j is None:
                    return
                row[j] = val

            # Enrichment via geo dict
            oc = geo.get(origin, {}).get("city", "")
            ok = geo.get(origin, {}).get("country", "")
            dc = geo.get(dest, {}).get("city", "")
            dk = geo.get(dest, {}).get("country", "")

            # REQUIRED: deterministic deal_id
            did = _deal_id(origin, dest, od, rd, float(price), cabin, max_conn)

            # Populate canonical columns (only if they exist in sheet)
            set_if("status", "NEW")
            set_if("deal_id", did)

            set_if("origin_iata", origin)
            set_if("destination_iata", dest)

            set_if("origin_city", oc)
            set_if("origin_country", ok)
            set_if("destination_city", dc)
            set_if("destination_country", dk)

            set_if("outbound_date", od.isoformat())
            set_if("return_date", rd.isoformat())

            set_if("price_gbp", float(price))

            # Stops: use max_connections as a practical proxy for the inserted deal row
            set_if("stops", int(max_conn))

            set_if("theme", theme_today)
            set_if("primary_theme", _cfg_theme(cfg))
            set_if("ingested_at", dt.datetime.utcnow().isoformat() + "Z")
            set_if("source", "duffel")
            set_if("max_connections", int(max_conn))
            set_if("cabin_class", cabin)
            set_if("included_airlines", ",".join(included_airlines) if included_airlines else "")

            deals_out.append(row)
            inserted_by_origin[origin] = inserted_by_origin.get(origin, 0) + 1
            inserted_by_route[(origin, dest)] = inserted_by_route.get((origin, dest), 0) + 1

            if len(deals_out) >= DUFFEL_MAX_INSERTS:
                break

        if searches >= DUFFEL_MAX_SEARCHES_PER_RUN:
            break

        if len(deals_out) >= DUFFEL_MAX_INSERTS:
            break

        # light sleep to avoid hammering
        time.sleep(float(_env("FEEDER_SLEEP_SECONDS", "0.1") or "0.1"))

    log(f"✓ Searches completed: {searches}")
    log(f"✓ Deals collected: {len(deals_out)} (cap {DUFFEL_MAX_INSERTS})")

    if not deals_out:
        log("⚠️ No rows to insert (no winners)")
        return 0

    _append_rows(ws_raw, deals_out)
    log(f"✅ Inserted rows into {RAW_DEALS_TAB}: {len(deals_out)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
