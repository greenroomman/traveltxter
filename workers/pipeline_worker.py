# workers/pipeline_worker.py
# FULL FILE REPLACEMENT — FEEDER vNext (CONFIG + ZTB + IATA_MASTER fallback)
#
# Contract:
# - CONFIG = inventory permissions (destination-first; origin_iata may be "ANY")
# - ZTB = theme constraints (season windows, connection tolerance, price psychology ranges)
# - Theme-of-day = rotation over eligible ZTB themes (enabled + in-season), with env override
# - Feeder samples destinations for theme_today and discovers viable origins when origin_iata="ANY"
# - Feeder NEVER blocks on price psychology; RDV/scorer enforce promotion rules.
# - RAW_DEALS is the only writable state.
#
# Non-negotiables:
# - Header-based writes only (no A/B/AA column letters)
# - No stub inserts (core fields required)
# - No invented geography:
#     - Prefer ROUTE_CAPABILITY_MAP as observed truth
#     - Fallback to IATA_MASTER as canonical reference
#     - If still missing geo -> skip (never guess)
#
# Requires env:
# - DUFFEL_API_KEY
# - SPREADSHEET_ID or SHEET_ID
# - GCP_SA_JSON_ONE_LINE or GCP_SA_JSON
#
# Optional env (sane defaults):
# - RAW_DEALS_TAB (default RAW_DEALS)
# - CONFIG_TAB / FEEDER_CONFIG_TAB (default CONFIG)
# - ZTB_TAB (default ZTB, fallback ZONE_THEME_BENCHMARKS)
# - RCM_TAB (default ROUTE_CAPABILITY_MAP)
# - IATA_TAB (default IATA_MASTER)
# - THEME_OVERRIDE (force a theme)
# - ORIGINS_DEFAULT (CSV; default LHR,LGW,MAN,BRS,STN)
# - ORIGINS_<THEME> (CSV; e.g. ORIGINS_SNOW)
# - ORIGINS_PER_DEST_PER_RUN (default 3)
# - DUFFEL_ROUTES_PER_RUN (default 6)  -> number of CONFIG rows (destinations) to attempt
# - DUFFEL_MAX_SEARCHES_PER_RUN (default 12)
# - DUFFEL_MAX_INSERTS (default 50)
# - DUFFEL_MAX_INSERTS_PER_ORIGIN (default 15)
# - DUFFEL_MAX_INSERTS_PER_ROUTE (default 5)
# - FEEDER_SLEEP_SECONDS (default 0.15)

from __future__ import annotations

import os
import json
import time
import math
import hashlib
import datetime as dt
from typing import Any, Dict, List, Tuple, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials


# ----------------------------
# Logging
# ----------------------------

def utc_now() -> dt.datetime:
    return dt.datetime.utcnow()


def log(msg: str) -> None:
    print(f"{utc_now().isoformat()}Z | {msg}", flush=True)


def _today_utc() -> dt.date:
    return utc_now().date()


# ----------------------------
# Env helpers
# ----------------------------

def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v)


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(_env(name, str(default)).strip() or default))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(_env(name, str(default)).strip() or default)
    except Exception:
        return default


def _csv_list(s: str) -> List[str]:
    return [x.strip().upper() for x in (s or "").split(",") if x.strip()]


def _is_true(v: Any) -> bool:
    return str(v).strip().upper() == "TRUE"


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(str(v).strip()))
    except Exception:
        return default


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(str(v).strip().replace(",", ""))
    except Exception:
        return default


# ----------------------------
# Google Sheets
# ----------------------------

def get_gspread_client() -> gspread.Client:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    raw = (_env("GCP_SA_JSON_ONE_LINE", "") or _env("GCP_SA_JSON", "")).strip()
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")

    try:
        info = json.loads(raw)
    except json.JSONDecodeError:
        # sometimes secrets are stored with literal "\n"
        info = json.loads(raw.replace("\\n", "\n"))

    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


# ----------------------------
# Duffel
# ----------------------------

DUFFEL_API_KEY = _env("DUFFEL_API_KEY").strip()
DUFFEL_VERSION = "v2"
DUFFEL_OFFER_REQUEST_URL = "https://api.duffel.com/air/offer_requests"


def duffel_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {DUFFEL_API_KEY}",
        "Duffel-Version": DUFFEL_VERSION,
        "Content-Type": "application/json",
    }


def duffel_create_offer_request(
    origin: str,
    dest: str,
    out_date: str,
    in_date: str,
    cabin_class: str = "economy",
) -> List[Dict[str, Any]]:
    payload = {
        "data": {
            "slices": [
                {"origin": origin, "destination": dest, "departure_date": out_date},
                {"origin": dest, "destination": origin, "departure_date": in_date},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": cabin_class or "economy",
        }
    }

    r = requests.post(
        DUFFEL_OFFER_REQUEST_URL,
        headers=duffel_headers(),
        json=payload,
        timeout=35,
    )
    if r.status_code >= 300:
        log(f"❌ Duffel error {r.status_code}: {r.text[:220]}")
        return []
    data = r.json().get("data") or {}
    return data.get("offers") or []


# ----------------------------
# ZTB: season windows + theme rotation
# ----------------------------

def _mmdd(d: dt.date) -> int:
    return int(d.strftime("%m%d"))


def _in_window(today_mmdd: int, start_mmdd: int, end_mmdd: int) -> bool:
    """Handles wrap windows (e.g. 1101-0331)."""
    if start_mmdd <= end_mmdd:
        return start_mmdd <= today_mmdd <= end_mmdd
    return (today_mmdd >= start_mmdd) or (today_mmdd <= end_mmdd)


def _eligible_themes_from_ztb(ztb_rows: List[Dict[str, Any]]) -> List[str]:
    today = _today_utc()
    t = _mmdd(today)
    eligible: List[str] = []
    for r in ztb_rows:
        theme = str(r.get("theme") or "").strip()
        if not theme:
            continue
        if not _is_true(r.get("enabled")):
            continue
        start = _safe_int(r.get("start_mmdd"), 101)
        end = _safe_int(r.get("end_mmdd"), 1231)
        if _in_window(t, start, end):
            eligible.append(theme)
    return sorted(list(dict.fromkeys(eligible)))


def _theme_of_day(eligible: List[str]) -> str:
    if not eligible:
        return "unexpected_value"
    anchor = dt.date(2026, 1, 1)  # deterministic anchor; change only deliberately
    idx = (_today_utc() - anchor).days % len(eligible)
    return eligible[idx]


def _theme_override(eligible: List[str]) -> Optional[str]:
    o = _env("THEME_OVERRIDE", "").strip()
    if not o:
        return None
    for t in eligible:
        if t.lower() == o.lower():
            return t
    log(f"⚠️ THEME_OVERRIDE '{o}' not in eligible pool today. Ignoring override.")
    return None


def _ztb_row_for_theme(ztb_rows: List[Dict[str, Any]], theme: str) -> Optional[Dict[str, Any]]:
    for r in ztb_rows:
        if str(r.get("theme") or "").strip().lower() == theme.lower():
            return r
    return None


def _connections_from_tolerance(tol: str) -> int:
    """
    ZTB connection_tolerance:
      - direct / 0
      - 1_stop / one_stop / 1
      - 2_stop / two_stop / 2
      - any => 2 (safe default)
    """
    s = (tol or "").strip().lower()
    if s in ("direct", "0", "zero"):
        return 0
    if s in ("1_stop", "one_stop", "1", "single"):
        return 1
    if s in ("2_stop", "two_stop", "2", "double"):
        return 2
    return 2


# ----------------------------
# Inventory window + trip length
# ----------------------------

def _window_days(cfg_row: Dict[str, Any]) -> Tuple[int, int]:
    min_d = _safe_int(cfg_row.get("days_ahead_min"), 21)
    max_d = _safe_int(cfg_row.get("days_ahead_max"), 84)
    max_d = min(max_d, 180)
    min_d = max(0, min_d)
    if max_d < min_d:
        max_d = min_d
    return min_d, max_d


def _trip_len_days(cfg_row: Dict[str, Any]) -> int:
    tl = _safe_int(cfg_row.get("trip_length_days"), 5)
    return max(1, tl)


def _candidate_outbounds(min_d: int, max_d: int, trip_len: int, n: int = 3) -> List[dt.date]:
    today = _today_utc()
    max_start = max(min_d, max_d - trip_len)
    if max_start < min_d:
        max_start = min_d
    if n <= 1 or max_start == min_d:
        points = [min_d]
    else:
        mid = min_d + (max_start - min_d) // 2
        points = [min_d, mid, max_start]
    out: List[dt.date] = []
    seen = set()
    for d in points:
        if d in seen:
            continue
        seen.add(d)
        out.append(today + dt.timedelta(days=d))
    return out


# ----------------------------
# Offer parsing (GBP + segments)
# ----------------------------

def _offer_total_amount_gbp(offer: Dict[str, Any]) -> Optional[float]:
    cur = (offer.get("total_currency") or "").upper().strip()
    if cur != "GBP":
        return None
    amt = _safe_float(offer.get("total_amount"), 0.0)
    if amt <= 0:
        return None
    return amt


def _slice_segments(offer: Dict[str, Any], slice_idx: int) -> List[Dict[str, Any]]:
    slices = offer.get("slices") or []
    if slice_idx >= len(slices):
        return []
    return slices[slice_idx].get("segments") or []


def _extract_dates_from_offer(offer: Dict[str, Any]) -> Tuple[str, str]:
    slices = offer.get("slices") or []
    out = ""
    inn = ""
    if len(slices) >= 1:
        out = (slices[0].get("departure_date") or "").strip()
    if len(slices) >= 2:
        inn = (slices[1].get("departure_date") or "").strip()
    if not out:
        segs = _slice_segments(offer, 0)
        if segs and segs[0].get("departing_at"):
            out = str(segs[0]["departing_at"])[:10]
    if not inn:
        segs = _slice_segments(offer, 1)
        if segs and segs[0].get("departing_at"):
            inn = str(segs[0]["departing_at"])[:10]
    return out, inn


def _via_hub_from_offer(offer: Dict[str, Any]) -> str:
    segs = _slice_segments(offer, 0)
    if len(segs) <= 1:
        return "na"
    hub = (segs[0].get("destination") or {}).get("iata_code")
    return str(hub).upper() if hub else "na"


def _carriers_from_offer(offer: Dict[str, Any]) -> str:
    carriers = set()
    for seg in (_slice_segments(offer, 0) + _slice_segments(offer, 1)):
        mc = (seg.get("marketing_carrier") or {}).get("iata_code")
        if mc:
            carriers.add(str(mc).upper())
    return ",".join(sorted(carriers)) if carriers else "na"


def _normalise_offer_fields(offer: Dict[str, Any]) -> Dict[str, Any]:
    segs_out = _slice_segments(offer, 0)
    stops = max(len(segs_out) - 1, 0)
    cabin = (offer.get("cabin_class") or "").strip() or "na"
    return {
        "stops": int(stops),
        "connection_type": "direct" if stops == 0 else "indirect",
        "cabin_class": cabin,
        "bags_incl": 0,
        "via_hub": _via_hub_from_offer(offer),
        "carriers": _carriers_from_offer(offer),
        "currency": "GBP",
    }


# ----------------------------
# RAW_DEALS write by header
# ----------------------------

def _build_header_index(headers: List[str]) -> Dict[str, int]:
    return {h: i for i, h in enumerate(headers)}


def _row_put(row: List[Any], idx: Dict[str, int], key: str, val: Any) -> None:
    if key in idx:
        row[idx[key]] = val


def _round_price_gbp(p: float) -> float:
    return float(math.ceil(p * 100) / 100.0)


def _ingested_iso() -> str:
    return utc_now().replace(microsecond=0).isoformat() + "Z"


# ----------------------------
# Main
# ----------------------------

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

    DUFFEL_MAX_INSERTS = _env_int("DUFFEL_MAX_INSERTS", 50)
    DUFFEL_MAX_INSERTS_PER_ORIGIN = _env_int("DUFFEL_MAX_INSERTS_PER_ORIGIN", 15)
    DUFFEL_MAX_INSERTS_PER_ROUTE = _env_int("DUFFEL_MAX_INSERTS_PER_ROUTE", 5)
    DUFFEL_MAX_SEARCHES_PER_RUN = _env_int("DUFFEL_MAX_SEARCHES_PER_RUN", 12)
    DUFFEL_ROUTES_PER_RUN = max(1, _env_int("DUFFEL_ROUTES_PER_RUN", 6))
    ORIGINS_PER_DEST = max(1, _env_int("ORIGINS_PER_DEST_PER_RUN", 3))
    FEEDER_SLEEP_SECONDS = _env_float("FEEDER_SLEEP_SECONDS", 0.15)

    PROCESSED_PER_SEARCH = 5

    log(
        f"CAPS: MAX_INSERTS={DUFFEL_MAX_INSERTS} | PER_ORIGIN={DUFFEL_MAX_INSERTS_PER_ORIGIN} | "
        f"PER_ROUTE={DUFFEL_MAX_INSERTS_PER_ROUTE} | MAX_SEARCHES={DUFFEL_MAX_SEARCHES_PER_RUN} | "
        f"DESTS_PER_RUN={DUFFEL_ROUTES_PER_RUN} | ORIGINS_PER_DEST={ORIGINS_PER_DEST}"
    )

    gc = get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_cfg = sh.worksheet(CONFIG_TAB)
    ws_raw = sh.worksheet(RAW_DEALS_TAB)

    try:
        ws_ztb = sh.worksheet(ZTB_TAB)
    except Exception:
        ws_ztb = sh.worksheet("ZONE_THEME_BENCHMARKS")

    ws_rcm = sh.worksheet(RCM_TAB)

    # IATA master is required for long-haul themes to function
    ws_iata = sh.worksheet(IATA_TAB)

    # Load ZTB
    ztb_rows_all = ws_ztb.get_all_records()
    eligible_themes = _eligible_themes_from_ztb(ztb_rows_all)
    theme_today = _theme_of_day(eligible_themes)
    override = _theme_override(eligible_themes)
    if override:
        theme_today = override

    log(f"✅ ZTB loaded: {len(ztb_rows_all)} rows | eligible_today={len(eligible_themes)} | pool={eligible_themes}")
    log(f"🎯 Theme of the day (UTC): {theme_today}")

    ztb_today = _ztb_row_for_theme(ztb_rows_all, theme_today) or {"connection_tolerance": "any"}

    # Load CONFIG (active rows only)
    cfg_all = ws_cfg.get_all_records()
    cfg_active = [r for r in cfg_all if _is_true(r.get("active_in_feeder")) and _is_true(r.get("enabled"))]
    log(f"✅ CONFIG loaded: {len(cfg_active)} active rows (of {len(cfg_all)} total)")

    def cfg_theme(r: Dict[str, Any]) -> str:
        return (str(r.get("primary_theme") or "").strip()
                or str(r.get("audience_type") or "").strip())

    theme_rows = [r for r in cfg_active if cfg_theme(r).lower() == theme_today.lower()]
    if not theme_rows:
        fallback = "unexpected_value"
        theme_rows = [r for r in cfg_active if cfg_theme(r).lower() == fallback.lower()] or cfg_active[:]
        log(f"⚠️ No CONFIG rows for '{theme_today}'. Falling back to '{fallback}' (rows={len(theme_rows)}).")

    def sort_key(r: Dict[str, Any]) -> Tuple[int, float]:
        return (_safe_int(r.get("priority"), 0), _safe_float(r.get("search_weight"), 0.0))

    theme_rows.sort(key=sort_key, reverse=True)
    theme_rows = theme_rows[:DUFFEL_ROUTES_PER_RUN]
    log(f"🧭 Selected destinations to attempt: {len(theme_rows)} (cap DESTS_PER_RUN={DUFFEL_ROUTES_PER_RUN})")

    # Build IATA geo dictionary:
    # 1) Prefer ROUTE_CAPABILITY_MAP (observed truth)
    # 2) Fallback to IATA_MASTER (canonical reference)
    rcm_all = ws_rcm.get_all_records()
    iata_geo: Dict[str, Tuple[str, str]] = {}
    for r in rcm_all:
        o = str(r.get("origin_iata") or "").strip().upper()
        oc = str(r.get("origin_city") or "").strip()
        ok = str(r.get("origin_country") or "").strip()
        d = str(r.get("destination_iata") or "").strip().upper()
        dc = str(r.get("destination_city") or "").strip()
        dk = str(r.get("destination_country") or "").strip()
        if o and oc and ok and o not in iata_geo:
            iata_geo[o] = (oc, ok)
        if d and dc and dk and d not in iata_geo:
            iata_geo[d] = (dc, dk)

    iata_rows = ws_iata.get_all_records()
    for r in iata_rows:
        code = str(r.get("iata_code") or "").strip().upper()
        city = str(r.get("city") or "").strip()
        country = str(r.get("country") or "").strip()
        if code and city and country and code not in iata_geo:
            iata_geo[code] = (city, country)

    log(f"✅ Geo dictionary loaded: {len(iata_geo)} IATA entries (RCM + {IATA_TAB})")

    # Origins policy
    origins_default = _csv_list(_env("ORIGINS_DEFAULT", "LHR,LGW,MAN,BRS,STN"))
    origins_theme = _csv_list(_env(f"ORIGINS_{theme_today.upper()}", ""))
    origins_pool = origins_theme or origins_default
    if not origins_pool:
        log("❌ No origins available (ORIGINS_DEFAULT empty).")
        return 1

    ztb_max_conn = _connections_from_tolerance(str(ztb_today.get("connection_tolerance") or "any"))

    headers = ws_raw.row_values(1)
    idx = _build_header_index(headers)

    inserted_by_origin: Dict[str, int] = {}
    inserted_by_route: Dict[Tuple[str, str], int] = {}
    deals_out: List[List[Any]] = []
    searches_done = 0

    def can_insert(origin: str, dest: str) -> bool:
        if len(deals_out) >= DUFFEL_MAX_INSERTS:
            return False
        if inserted_by_origin.get(origin, 0) >= DUFFEL_MAX_INSERTS_PER_ORIGIN:
            return False
        if inserted_by_route.get((origin, dest), 0) >= DUFFEL_MAX_INSERTS_PER_ROUTE:
            return False
        return True

    def geo_for(iata: str) -> Optional[Tuple[str, str]]:
        return iata_geo.get(iata.upper())

    def choose_origins_for_dest(dest: str, cap: int) -> List[str]:
        base = origins_pool[:]
        seed = f"{_today_utc().isoformat()}|{theme_today}|{dest}|{_env('RUN_SLOT','')}"
        h = int(hashlib.md5(seed.encode("utf-8")).hexdigest(), 16)
        if base:
            rot = h % len(base)
            base = base[rot:] + base[:rot]
        return base[:cap]

    for cfg in theme_rows:
        if searches_done >= DUFFEL_MAX_SEARCHES_PER_RUN:
            break

        dest = str(cfg.get("destination_iata") or "").strip().upper()
        if not dest:
            continue

        cabin = (str(cfg.get("cabin_class") or "economy").strip() or "economy")
        min_d, max_d = _window_days(cfg)
        trip_len = _trip_len_days(cfg)

        cfg_max_conn_raw = str(cfg.get("max_connections") or "").strip()
        max_conn = _safe_int(cfg_max_conn_raw, ztb_max_conn)

        origin_cfg = str(cfg.get("origin_iata") or "").strip().upper()
        if origin_cfg and origin_cfg != "ANY":
            origins = [origin_cfg]
        else:
            origins = choose_origins_for_dest(dest, ORIGINS_PER_DEST)

        dest_geo = geo_for(dest)
        if not dest_geo:
            log(f"⚠️ Missing geo for destination_iata={dest}. Skipping (no invented geo).")
            continue

        log(f"🧩 Target: theme={theme_today} dest={dest} origins={origins} window={min_d}-{max_d} trip_len={trip_len} max_conn={max_conn}")
        out_dates = _candidate_outbounds(min_d, max_d, trip_len, n=3)

        for origin in origins:
            if searches_done >= DUFFEL_MAX_SEARCHES_PER_RUN:
                break
            if not can_insert(origin, dest):
                continue

            origin_geo = geo_for(origin)
            if not origin_geo:
                log(f"⚠️ Missing geo for origin_iata={origin}. Skipping origin (no invented geo).")
                continue

            best_offers: List[Dict[str, Any]] = []
            offers_returned_total = 0

            for out_d in out_dates:
                if searches_done >= DUFFEL_MAX_SEARCHES_PER_RUN:
                    break
                out_s = out_d.isoformat()
                in_s = (out_d + dt.timedelta(days=trip_len)).isoformat()
                log(f"Duffel: Searching {origin}->{dest} {out_s}/{in_s} cabin={cabin}")
                searches_done += 1
                offers = duffel_create_offer_request(origin, dest, out_s, in_s, cabin_class=cabin)
                offers_returned_total += len(offers)
                best_offers.extend(offers)
                time.sleep(FEEDER_SLEEP_SECONDS)

            rejected_non_gbp = 0
            rejected_conn = 0
            ranked: List[Tuple[float, Dict[str, Any]]] = []

            for off in best_offers:
                amt = _offer_total_amount_gbp(off)
                if amt is None:
                    rejected_non_gbp += 1
                    continue
                stops = max(len(_slice_segments(off, 0)) - 1, 0)
                if stops > max_conn:
                    rejected_conn += 1
                    continue
                ranked.append((amt, off))

            ranked.sort(key=lambda x: x[0])
            if not ranked:
                log(f"Duffel: offers_returned={offers_returned_total} gbp_ranked=0 rej_non_gbp={rejected_non_gbp} rej_conn={rejected_conn} inserted=0")
                continue

            processed = 0
            inserted_here = 0

            for amt, off in ranked:
                if processed >= PROCESSED_PER_SEARCH:
                    break
                if not can_insert(origin, dest):
                    break

                deal_id = (off.get("id") or "").strip()
                out_date, ret_date = _extract_dates_from_offer(off)

                if not deal_id or not out_date or not ret_date:
                    continue

                row = [""] * len(headers)

                oc, ok = origin_geo
                dc, dk = dest_geo

                _row_put(row, idx, "status", "NEW")
                _row_put(row, idx, "deal_id", deal_id)
                _row_put(row, idx, "price_gbp", _round_price_gbp(float(amt)))
                _row_put(row, idx, "origin_iata", origin)
                _row_put(row, idx, "destination_iata", dest)
                _row_put(row, idx, "origin_city", oc)
                _row_put(row, idx, "origin_country", ok)
                _row_put(row, idx, "destination_city", dc)
                _row_put(row, idx, "destination_country", dk)
                _row_put(row, idx, "outbound_date", out_date)
                _row_put(row, idx, "return_date", ret_date)

                _row_put(row, idx, "deal_theme", theme_today)
                _row_put(row, idx, "theme", theme_today)

                _row_put(row, idx, "ingested_at_utc", _ingested_iso())
                _row_put(row, idx, "created_utc", _ingested_iso())

                norm = _normalise_offer_fields(off)
                _row_put(row, idx, "stops", norm.get("stops", 0))
                _row_put(row, idx, "connection_type", norm.get("connection_type", "na"))
                _row_put(row, idx, "cabin_class", norm.get("cabin_class", "na"))
                _row_put(row, idx, "bags_incl", norm.get("bags_incl", 0))
                _row_put(row, idx, "via_hub", norm.get("via_hub", "na"))
                _row_put(row, idx, "carriers", norm.get("carriers", "na"))
                _row_put(row, idx, "currency", norm.get("currency", "GBP"))

                _row_put(row, idx, "gateway_type", str(cfg.get("gateway_type") or "").strip())
                _row_put(row, idx, "is_long_haul", str(cfg.get("is_long_haul") or "").strip())
                _row_put(row, idx, "primary_theme", str(cfg.get("primary_theme") or cfg.get("audience_type") or "").strip())
                _row_put(row, idx, "content_priority", str(cfg.get("content_priority") or "").strip())
                _row_put(row, idx, "search_weight", _safe_float(cfg.get("search_weight"), 0.0))

                core_required = [
                    "deal_id", "price_gbp", "origin_iata", "destination_iata",
                    "outbound_date", "return_date",
                    "destination_city", "destination_country", "origin_city", "origin_country",
                    "deal_theme",
                ]
                ok_core = True
                for k in core_required:
                    if k not in idx:
                        continue
                    v = row[idx[k]]
                    if v in ("", None):
                        ok_core = False
                        break
                if not ok_core:
                    continue

                deals_out.append(row)
                processed += 1
                inserted_here += 1

                inserted_by_origin[origin] = inserted_by_origin.get(origin, 0) + 1
                inserted_by_route[(origin, dest)] = inserted_by_route.get((origin, dest), 0) + 1

            log(
                f"Duffel: origin={origin} dest={dest} offers_returned={offers_returned_total} "
                f"gbp_ranked={len(ranked)} processed={processed} rej_non_gbp={rejected_non_gbp} rej_conn={rejected_conn} "
                f"inserted={inserted_here} origin_total={inserted_by_origin.get(origin,0)}/{DUFFEL_MAX_INSERTS_PER_ORIGIN} "
                f"running_total={len(deals_out)}/{DUFFEL_MAX_INSERTS}"
            )

    log(f"✓ Searches completed: {searches_done}")
    log(f"✓ Deals collected: {len(deals_out)} (cap {DUFFEL_MAX_INSERTS})")

    if not deals_out:
        log("⚠️ No rows to insert (no deals collected).")
        return 0

    ws_raw.append_rows(deals_out, value_input_option="USER_ENTERED")
    log(f"✅ Inserted {len(deals_out)} rows into {RAW_DEALS_TAB}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
