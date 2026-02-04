# workers/pipeline_worker.py
# FULL FILE REPLACEMENT — DEAL FEEDER v5.1 (theme-locked 90/10 within-theme)
#
# FIX APPLIED:
# - Insert caps are RUN-scoped only.
# - We DO NOT preload origin_counts / route_counts from historical RAW_DEALS,
#   because that can permanently deadlock the feeder (skipped_can_insert=240, duffel_calls=0).
#
# Locked constraints respected:
# - No redesign / no schema changes
# - RAW_DEALS only writable source of truth
# - RDV never written
# - OPS_MASTER!B5 governs theme (fallback deterministic)
#
# Key behaviour (V5.1 DEAL FEEDER):
# - Theme is locked to OPS_MASTER theme_today.
# - 90/10 split is within the same theme (no AM/PM cross-slot drift).
# - PRIMARY(90%) = strict feasibility (shorthaul direct-only).
# - SECONDARY(10%) = controlled feasibility drift within same theme:
#       - shorthaul may allow 1 connection (still same theme).
#
# Notes:
# - Layout (AM/PM) is NOT a feeder concern anymore. Render derives AM/PM from ingested_at_utc.
# - RUN_SLOT is retained only for backward compatible logs, but selection does not use it.

from __future__ import annotations

import os
import json
import hashlib
import datetime as dt
import math
import re
from typing import Any, Dict, List, Optional, Tuple, Set

import requests
import gspread
from google.oauth2.service_account import Credentials


def log(msg: str) -> None:
    ts = dt.datetime.utcnow().isoformat(timespec="microseconds") + "Z"
    print(f"{ts} | {msg}", flush=True)


def _unique_headers(headers: list[str]) -> list[str]:
    """Make headers unique and non-empty for get_all_values-derived records."""
    seen: dict[str, int] = {}
    out: list[str] = []
    for i, h in enumerate(headers):
        base = (str(h).strip() if h is not None else "")
        if base == "":
            base = f"__col_{i+1}"
        seen[base] = seen.get(base, 0) + 1
        key = base if seen[base] == 1 else f"{base}__{seen[base]}"
        out.append(key)
    return out


def _get_records(ws) -> list[dict]:
    """Fast batch read using get_all_values() - single API call per sheet."""
    all_values = ws.get_all_values()
    if not all_values:
        return []
    headers = _unique_headers(all_values[0])
    records = []
    for row in all_values[1:]:
        padded = row + [""] * (len(headers) - len(row))
        records.append(dict(zip(headers, padded[: len(headers)])))
    return records


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


def _slot_norm(s: str) -> str:
    s = (s or "").strip().upper()
    return s if s in ("AM", "PM") else ""


def _today_utc() -> dt.date:
    return dt.datetime.utcnow().date()


def _mmdd_today() -> int:
    d = _today_utc()
    return d.month * 100 + d.day


def _mmdd_in_range(mmdd: int, start: int, end: int) -> bool:
    # handles wrap-around e.g. 1101..331
    if start <= end:
        return start <= mmdd <= end
    return (mmdd >= start) or (mmdd <= end)


def _connections_from_tolerance(tol: str) -> int:
    tol = (tol or "").strip().lower()
    if tol in ("0", "direct", "none"):
        return 0
    if tol in ("1", "one"):
        return 1
    if tol in ("2", "two"):
        return 2
    return 2


def _eligible_themes_from_ztb(rows: List[Dict[str, Any]]) -> List[str]:
    mmdd = _mmdd_today()
    out: List[str] = []
    for r in rows or []:
        theme = str(r.get("theme") or "").strip()
        if not theme:
            continue
        enabled = str(r.get("enabled") or "").strip().lower()
        if enabled not in ("true", "1", "yes", "y"):
            continue
        start = _safe_int(r.get("start_mmdd"), 101)
        end = _safe_int(r.get("end_mmdd"), 1231)
        if _mmdd_in_range(mmdd, start, end):
            out.append(theme)
    return out


def _theme_of_day(pool: List[str]) -> str:
    # deterministic daily theme from pool
    if not pool:
        return "adventure"
    seed = _today_utc().isoformat()
    h = int(hashlib.md5(seed.encode("utf-8")).hexdigest(), 16)
    return pool[h % len(pool)]


def _ztb_row_for_theme(rows: List[Dict[str, Any]], theme: str) -> Optional[Dict[str, Any]]:
    t = (theme or "").strip().lower()
    for r in rows or []:
        if str(r.get("theme") or "").strip().lower() == t:
            return r
    return None


def _norm_iata(s: Any) -> str:
    s = str(s or "").strip().upper()
    s = re.sub(r"[^A-Z]", "", s)
    return s[:3] if len(s) >= 3 else s


def _get_gspread_client() -> gspread.Client:
    sa_json = _env("GCP_SA_JSON_ONE_LINE") or _env("GCP_SA_JSON")
    if not sa_json:
        raise RuntimeError("Missing GCP_SA_JSON / GCP_SA_JSON_ONE_LINE")
    try:
        info = json.loads(sa_json)
    except Exception:
        info = json.loads(sa_json.replace("\\n", "\n"))

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def cfg_theme(r: Dict[str, Any]) -> str:
    # supports both legacy and newer config headers
    return str(r.get("theme") or r.get("primary_theme") or r.get("intent_theme") or "").strip()


def is_longhaul(r: Dict[str, Any]) -> bool:
    v = str(r.get("is_long_haul") or "").strip().lower()
    if v in ("true", "1", "yes", "y"):
        return True
    rc = str(r.get("route_class_primary") or "").strip().lower()
    return "long" in rc


def carriers_from_cfg(r: Dict[str, Any]) -> str:
    return str(r.get("included_airlines") or "").strip()


def _read_geo_dict(iata_rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, str]]:
    d: Dict[str, Dict[str, str]] = {}
    for r in iata_rows or []:
        code = _norm_iata(r.get("iata_code") or r.get("iata") or r.get("code"))
        if not code:
            continue
        d[code] = {
            "city": str(r.get("city") or "").strip(),
            "country": str(r.get("country") or "").strip(),
        }
    return d


def _duffel_headers() -> Dict[str, str]:
    api_key = _env("DUFFEL_API_KEY")
    if not api_key:
        raise RuntimeError("Missing DUFFEL_API_KEY")
    return {
        "Authorization": f"Bearer {api_key}",
        "Duffel-Version": "v2",
        "Content-Type": "application/json",
    }


def _duffel_search(
    origin: str,
    dest: str,
    out_date: dt.date,
    ret_date: dt.date,
    max_conn: int,
    cabin: str,
    included_airlines: str,
) -> Optional[Dict[str, Any]]:
    url = "https://api.duffel.com/air/offer_requests"
    body: Dict[str, Any] = {
        "data": {
            "slices": [
                {"origin": origin, "destination": dest, "departure_date": out_date.isoformat()},
                {"origin": dest, "destination": origin, "departure_date": ret_date.isoformat()},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": cabin,
            "max_connections": max_conn,
        }
    }
    if included_airlines:
        body["data"]["included_airlines"] = [a.strip().upper() for a in included_airlines.split(",") if a.strip()]

    try:
        r = requests.post(url, headers=_duffel_headers(), json=body, timeout=30)
        if not r.ok:
            log(f"⚠️ Duffel error {r.status_code}: {r.text[:200]}")
            return None
        return r.json()
    except Exception as e:
        log(f"⚠️ Duffel request failed: {e}")
        return None


def _best_offer_gbp_with_meta(offer_request: Dict[str, Any]) -> Optional[Tuple[float, int, Dict[str, Any]]]:
    try:
        data = offer_request.get("data") or {}
        offers = data.get("offers") or []
        if not offers:
            return None

        best = None
        best_price = 1e18
        best_stops = 99

        for o in offers:
            total = float(o.get("total_amount") or "0")
            currency = str(o.get("total_currency") or "").upper()
            if currency != "GBP":
                continue

            stops = 0
            for s in (o.get("slices") or []):
                segs = s.get("segments") or []
                stops = max(stops, max(0, len(segs) - 1))

            if total < best_price:
                best_price = total
                best_stops = stops
                best = o

        if best is None:
            return None
        return best_price, best_stops, best
    except Exception:
        return None


def _now_utc_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _write_rows(ws_raw: gspread.Worksheet, headers: List[str], rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    values = [[str(r.get(h, "") or "") for h in headers] for r in rows]
    ws_raw.append_rows(values, value_input_option="RAW")
    return len(rows)


def main() -> int:
    SPREADSHEET_ID = _env("SPREADSHEET_ID") or _env("SHEET_ID")
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")

    RAW_DEALS_TAB = _env("RAW_DEALS_TAB", "RAW_DEALS")
    CONFIG_TAB = _env("CONFIG_TAB", "CONFIG")
    ZTB_TAB = _env("ZTB_TAB", "ZONE_THEME_BENCHMARKS")
    RCM_TAB = _env("RCM_TAB", "ROUTE_CAPABILITY_MAP")
    IATA_TAB = _env("IATA_TAB", "IATA_MASTER")
    CONFIG_CARRIER_BIAS_TAB = _env("CONFIG_CARRIER_BIAS_TAB", "CONFIG_CARRIER_BIAS")
    OPS_MASTER_TAB = _env("OPS_MASTER_TAB", "OPS_MASTER")

    RUN_SLOT = _slot_norm(_env("RUN_SLOT", "")) or "AM"
    FEEDER_KIND = (_env("FEEDER_KIND", "DEAL") or "DEAL").strip().upper()
    RUN_SLOT_EFFECTIVE = "DEAL" if FEEDER_KIND == "DEAL" else "DREAM"

    DUFFEL_MAX_INSERTS = _env_int("DUFFEL_MAX_INSERTS", 50)
    DUFFEL_MAX_INSERTS_PER_ORIGIN = _env_int("DUFFEL_MAX_INSERTS_PER_ORIGIN", 15)
    DUFFEL_MAX_INSERTS_PER_ROUTE = _env_int("DUFFEL_MAX_INSERTS_PER_ROUTE", 5)
    DUFFEL_MAX_SEARCHES_PER_RUN = _env_int("DUFFEL_MAX_SEARCHES_PER_RUN", 12)

    DESTS_PER_RUN = _env_int("DESTS_PER_RUN", 4)
    ORIGINS_PER_DEST = _env_int("ORIGINS_PER_DEST", 3)

    SLOT_PRIMARY_PCT = _env_int("SLOT_PRIMARY_PCT", 90)
    K_OVERRIDE = _env_int("K_DATES_PER_DEST", 0)

    log("================================================================================")
    log("TRAVELTXTTER DEAL FEEDER (THEME-LOCKED 90/10) START")
    log("================================================================================")

    K_DATES_PER_DEST = max(
        1,
        K_OVERRIDE
        or max(1, int(math.floor(max(1, DUFFEL_MAX_SEARCHES_PER_RUN) / max(1, DESTS_PER_RUN)))),
    )

    log(
        f"CAPS: MAX_INSERTS={DUFFEL_MAX_INSERTS} | PER_ORIGIN={DUFFEL_MAX_INSERTS_PER_ORIGIN} | "
        f"PER_ROUTE={DUFFEL_MAX_INSERTS_PER_ROUTE} | MAX_SEARCHES={DUFFEL_MAX_SEARCHES_PER_RUN} | "
        f"DESTS_PER_RUN={DESTS_PER_RUN} | ORIGINS_PER_DEST={ORIGINS_PER_DEST} | RUN_SLOT={RUN_SLOT_EFFECTIVE} | "
        f"SLOT_SPLIT={min(100,max(1,SLOT_PRIMARY_PCT))}/{100 - min(100,max(1,SLOT_PRIMARY_PCT))} | K_DATES_PER_DEST={K_DATES_PER_DEST}"
    )

    gc = _get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_raw = sh.worksheet(RAW_DEALS_TAB)
    ws_cfg = sh.worksheet(CONFIG_TAB)
    ws_ztb = sh.worksheet(ZTB_TAB)
    ws_rcm = sh.worksheet(RCM_TAB)
    ws_iata = sh.worksheet(IATA_TAB)

    try:
        ws_ops = sh.worksheet(OPS_MASTER_TAB)
    except Exception:
        ws_ops = None
        log("⚠️ OPS_MASTER worksheet not found - will use calculated theme")

    # Load carrier bias (continuity only)
    try:
        log("📥 Loading CONFIG_CARRIER_BIAS...")
        t0 = dt.datetime.utcnow()
        ws_bias = sh.worksheet(CONFIG_CARRIER_BIAS_TAB)
        bias_rows = _get_records(ws_bias) or []
        usable_bias = sum(1 for r in bias_rows if str(r.get("carrier_code") or "").strip())
        log(f"✅ CONFIG_CARRIER_BIAS loaded: {usable_bias} usable rows ({(dt.datetime.utcnow()-t0).total_seconds():.1f}s)")
    except Exception as e:
        log(f"⚠️ CONFIG_CARRIER_BIAS not loaded: {e}")

    # Theme of day
    log("📥 Loading ZTB...")
    t0 = dt.datetime.utcnow()
    ztb_rows_all = _get_records(ws_ztb)
    eligible_themes = _eligible_themes_from_ztb(ztb_rows_all)

    theme_today = None
    if ws_ops:
        try:
            v = ws_ops.acell("B5").value
            if v and str(v).strip():
                theme_today = str(v).strip()
                log(f"✅ Theme read from OPS_MASTER!B5: {theme_today}")
        except Exception as e:
            log(f"⚠️ Could not read OPS_MASTER!B5: {e}")

    if not theme_today:
        theme_today = _theme_of_day(eligible_themes)
        log(f"ℹ️ Using calculated theme (B5 was empty or errored): {theme_today}")

    log(
        f"✅ ZTB loaded: {len(ztb_rows_all)} rows | eligible_today={len(eligible_themes)} | pool={eligible_themes} ({(dt.datetime.utcnow()-t0).total_seconds():.1f}s)"
    )
    log(f"🎯 Theme of the day (UTC): {theme_today}")

    ztb_today = _ztb_row_for_theme(ztb_rows_all, theme_today) or {"connection_tolerance": "any"}
    ztb_start = _safe_int(ztb_today.get("start_mmdd"), 101)
    ztb_end = _safe_int(ztb_today.get("end_mmdd"), 1231)
    ztb_max_conn = _connections_from_tolerance(str(ztb_today.get("connection_tolerance") or "any"))

    # Load CONFIG
    log("📥 Loading CONFIG...")
    t0 = dt.datetime.utcnow()
    cfg_all = _get_records(ws_cfg)
    cfg_active = [
        r for r in (cfg_all or [])
        if str(r.get("enabled") or "").strip().lower() in ("true", "1", "yes", "y")
    ]
    log(f"✅ CONFIG loaded: {len(cfg_active)} active rows (of {len(cfg_all)} total) ({(dt.datetime.utcnow()-t0).total_seconds():.1f}s)")

    # Load ROUTE_CAPABILITY_MAP
    log("📥 Loading ROUTE_CAPABILITY_MAP...")
    t0 = dt.datetime.utcnow()
    rcm_rows = _get_records(ws_rcm)
    enabled_routes: Set[Tuple[str, str]] = set()
    for r in rcm_rows or []:
        o = _norm_iata(r.get("origin_iata"))
        d = _norm_iata(r.get("destination_iata"))
        en = str(r.get("enabled") or "").strip().lower()
        if en in ("true", "1", "yes", "y") and o and d:
            enabled_routes.add((o, d))
    log(f"✅ ROUTE_CAPABILITY_MAP loaded: {len(enabled_routes)} enabled routes ({(dt.datetime.utcnow()-t0).total_seconds():.1f}s)")

    # Load IATA master
    log("📥 Loading IATA_MASTER (6000+ rows, may take time)...")
    t0 = dt.datetime.utcnow()
    iata_rows = _get_records(ws_iata)
    geo = _read_geo_dict(iata_rows)
    log(f"✅ Geo dictionary loaded: {len(geo)} IATA entries (IATA_MASTER only) ({(dt.datetime.utcnow()-t0).total_seconds():.1f}s)")

    def geo_for(code: str) -> Dict[str, str]:
        return geo.get(code, {})

    # ✅ RUN-SCOPED caps only (FIX)
    origin_counts: Dict[str, int] = {}
    route_counts: Dict[Tuple[str, str], int] = {}

    def can_insert(origin: str, dest: str) -> bool:
        if origin_counts.get(origin, 0) >= DUFFEL_MAX_INSERTS_PER_ORIGIN:
            return False
        if route_counts.get((origin, dest), 0) >= DUFFEL_MAX_INSERTS_PER_ROUTE:
            return False
        return True

    # Seasonal gate note (do not block run)
    mmdd = _mmdd_today()
    if not _mmdd_in_range(mmdd, ztb_start, ztb_end):
        log(f"⚠️ Theme {theme_today} not in-season today ({mmdd} not in {ztb_start}..{ztb_end}). Proceeding anyway (theme-locked), but expect fewer hits.")

    # Theme-filter CONFIG (theme-locked)
    theme_rows_all = [r for r in cfg_active if cfg_theme(r).lower() == theme_today.lower()]
    if not theme_rows_all:
        fallback = "unexpected_value"
        log(f"⚠️ No CONFIG rows for theme={theme_today}. Falling back to {fallback}")
        theme_today = fallback
        ztb_today = _ztb_row_for_theme(ztb_rows_all, theme_today) or {"connection_tolerance": "any"}
        ztb_start = _safe_int(ztb_today.get("start_mmdd"), 101)
        ztb_end = _safe_int(ztb_today.get("end_mmdd"), 1231)
        ztb_max_conn = _connections_from_tolerance(str(ztb_today.get("connection_tolerance") or "any"))
        theme_rows_all = [r for r in cfg_active if cfg_theme(r).lower() == theme_today.lower()]

    rows_primary = list(theme_rows_all)
    rows_explore = list(theme_rows_all)

    def sort_key(r: Dict[str, Any]) -> Tuple[float, float, float]:
        return (
            _safe_float(r.get("priority"), 0),
            _safe_float(r.get("search_weight"), 0),
            _safe_float(r.get("content_priority"), 0),
        )

    rows_primary.sort(key=sort_key, reverse=True)
    rows_explore.sort(key=sort_key, reverse=True)

    primary_period = max(1, min(100, SLOT_PRIMARY_PCT))
    explore_period = 100 - primary_period

    skipped_no_dest = 0
    skipped_missing_geo_dest = 0
    skipped_missing_geo_origin = 0
    skipped_no_out_dates = 0
    skipped_no_viable_origins = 0
    skipped_can_insert = 0

    deals_out: List[Dict[str, Any]] = []
    dest_attempts: Dict[str, int] = {}
    duffel_calls = 0
    attempts = 0
    max_attempts = max(50, DUFFEL_MAX_SEARCHES_PER_RUN * 20)

    log(f"PLAN: intended_routes={max(1, DESTS_PER_RUN)} | dates_per_dest(K)={max(1, K_DATES_PER_DEST)} | max_searches={DUFFEL_MAX_SEARCHES_PER_RUN}")
    log(f"🔧 90/10 budgets: PRIMARY={primary_period}% | SECONDARY={explore_period}% (theme-locked)")

    def _parse_days_ahead(v: Any, default: int) -> int:
        return max(0, _safe_int(v, default))

    def _parse_trip_len(v: Any, default: int) -> int:
        return max(1, _safe_int(v, default))

    def _candidate_out_dates(cfg_row: Dict[str, Any]) -> List[dt.date]:
        min_a = _parse_days_ahead(cfg_row.get("days_ahead_min"), 30)
        max_a = _parse_days_ahead(cfg_row.get("days_ahead_max"), 120)
        if max_a < min_a:
            max_a = min_a
        today = dt.datetime.utcnow().date()
        span = max(0, max_a - min_a)
        if span == 0:
            return [today + dt.timedelta(days=min_a)]
        offsets = []
        for i in range(max(1, K_DATES_PER_DEST)):
            frac = (i + 1) / (max(1, K_DATES_PER_DEST) + 1)
            offsets.append(min_a + int(round(span * frac)))
        return [today + dt.timedelta(days=o) for o in offsets]

    def choose_viable_origins(cfg_row: Dict[str, Any], dest: str, cap: int) -> List[str]:
        o = str(cfg_row.get("origin_iata") or "").strip().upper()
        if o and o != "ANY":
            o = _norm_iata(o)
            return [o] if (o, dest) in enabled_routes else []

        viable = sorted({oo for (oo, dd) in enabled_routes if dd == dest})
        if not viable:
            return []
        seed = f"{_today_utc().isoformat()}|{theme_today}|{dest}|{RUN_SLOT_EFFECTIVE}"
        h = int(hashlib.md5(seed.encode("utf-8")).hexdigest(), 16)
        rot = h % len(viable)
        viable = viable[rot:] + viable[:rot]
        return viable[: max(1, cap)]

    # Fetch RAW_DEALS headers only (needed for append order)
    raw_values = ws_raw.get_all_values()
    raw_headers = raw_values[0] if raw_values else []
    if not raw_headers:
        raise RuntimeError("RAW_DEALS has no header row")

    last_progress_log = 0
    consecutive_skips = 0

    while duffel_calls < DUFFEL_MAX_SEARCHES_PER_RUN and len(deals_out) < DUFFEL_MAX_INSERTS and attempts < max_attempts:
        attempt_idx = attempts
        attempts += 1

        if attempts - last_progress_log >= 50:
            last_progress_log = attempts
            log(f"🔄 Loop progress: attempts={attempts}/{max_attempts} | duffel_calls={duffel_calls}/{DUFFEL_MAX_SEARCHES_PER_RUN} | deals={len(deals_out)}")

        use_explore = False
        if explore_period > 0 and ((attempt_idx % 100) >= primary_period):
            use_explore = True

        if use_explore and rows_explore:
            cfg_pick = rows_explore[attempt_idx % len(rows_explore)]
            mode = "SECONDARY(10%)"
        else:
            cfg_pick = rows_primary[attempt_idx % len(rows_primary)] if rows_primary else None
            mode = "PRIMARY(90%)"

        if cfg_pick is None:
            log("⚠️ No CONFIG rows available to pick from. Ending run.")
            break

        dest = _norm_iata(cfg_pick.get("destination_iata"))
        if not dest:
            skipped_no_dest += 1
            consecutive_skips += 1
            continue

        if not geo_for(dest):
            skipped_missing_geo_dest += 1
            consecutive_skips += 1
            continue

        out_dates_all = _candidate_out_dates(cfg_pick)
        if not out_dates_all:
            skipped_no_out_dates += 1
            consecutive_skips += 1
            continue
        out_dates = out_dates_all[: max(1, min(K_DATES_PER_DEST, len(out_dates_all)))]

        da = dest_attempts.get(dest, 0)
        if da >= len(out_dates):
            consecutive_skips += 1
            continue

        is_long = is_longhaul(cfg_pick)
        cfg_max_conn = _safe_int(cfg_pick.get("max_connections"), 2)
        max_conn = min(cfg_max_conn, ztb_max_conn)

        if not is_long:
            # Deal feeder: PRIMARY direct, SECONDARY allows 1 conn as controlled drift.
            max_conn = 1 if use_explore else 0

        cabin = str(cfg_pick.get("cabin_class") or "economy").strip().lower() or "economy"
        included_airlines = carriers_from_cfg(cfg_pick)

        origins = choose_viable_origins(cfg_pick, dest, ORIGINS_PER_DEST)
        if not origins:
            skipped_no_viable_origins += 1
            consecutive_skips += 1
            continue

        origin = origins[attempt_idx % len(origins)]
        if not geo_for(origin):
            skipped_missing_geo_origin += 1
            consecutive_skips += 1
            continue

        if not can_insert(origin, dest):
            skipped_can_insert += 1
            consecutive_skips += 1
            continue

        od = out_dates[da]
        trip_len = _parse_trip_len(cfg_pick.get("trip_length_days"), 4)
        rd = od + dt.timedelta(days=trip_len)

        log(
            f"🔎 Search[{duffel_calls+1}/{DUFFEL_MAX_SEARCHES_PER_RUN}] mode={mode} dest={dest} origin={origin} "
            f"haul={'LONG' if is_long else 'SHORT'} max_conn={max_conn} "
            f"date_try={da+1}/{len(out_dates)} out={od.isoformat()} in={rd.isoformat()}"
        )

        duffel_calls += 1
        consecutive_skips = 0

        resp = _duffel_search(origin, dest, od, rd, max_conn, cabin, included_airlines)
        dest_attempts[dest] = da + 1

        if not resp:
            continue

        best = _best_offer_gbp_with_meta(resp)
        if best is None:
            continue

        price, offer_stops, _offer_obj = best

        now_ts = _now_utc_iso()
        deal_id = hashlib.md5(f"{origin}|{dest}|{od}|{rd}|{price}".encode("utf-8")).hexdigest()[:12]

        row = {
            "status": "NEW",
            "deal_id": deal_id,
            "price_gbp": f"{price:.2f}",
            "origin_city": geo_for(origin).get("city", ""),
            "origin_iata": origin,
            "destination_country": geo_for(dest).get("country", ""),
            "destination_city": geo_for(dest).get("city", ""),
            "destination_iata": dest,
            "outbound_date": od.isoformat(),
            "return_date": rd.isoformat(),
            "stops": str(offer_stops),
            "deal_theme": theme_today,
            "ingested_at_utc": now_ts,
            "cabin_class": cabin,
            "connection_type": "direct" if offer_stops == 0 else "connection",
            "trip_length_days": str(trip_len),
        }

        deals_out.append(row)

        # RUN-SCOPED caps increment here
        origin_counts[origin] = origin_counts.get(origin, 0) + 1
        route_counts[(origin, dest)] = route_counts.get((origin, dest), 0) + 1

        if consecutive_skips >= 50:
            log("⚠️ Loop exhaustion detected: 50 consecutive skips. Ending run early.")
            break

    log(f"✓ Attempts (internal loop): {attempts} (cap {max_attempts})")
    log(f"✓ Duffel calls made: {duffel_calls} (cap {DUFFEL_MAX_SEARCHES_PER_RUN})")
    log(f"✓ Deals collected: {len(deals_out)} (cap {DUFFEL_MAX_INSERTS})")
    log(
        f"⚠️ skipped_no_dest={skipped_no_dest} skipped_missing_geo_dest={skipped_missing_geo_dest} "
        f"skipped_no_out_dates={skipped_no_out_dates} skipped_no_viable_origins={skipped_no_viable_origins} "
        f"skipped_missing_geo_origin={skipped_missing_geo_origin} skipped_can_insert={skipped_can_insert}"
    )

    if not deals_out:
        log("⚠️ No rows to insert (no winners)")
        return 0

    inserted = _write_rows(ws_raw, raw_headers, deals_out)
    log(f"✅ Inserted rows into {RAW_DEALS_TAB}: {inserted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
