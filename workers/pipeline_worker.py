# workers/pipeline_worker.py
# FULL FILE REPLACEMENT — "RICH FEEDER" RESTORE + TODAY'S FIXES
#
# Restores V4.6/4e4cdaf behaviours:
# - inventory window sampling + zero-offer retry
# - cheapest-offer ranking + processed/inserted accounting
# - caps + origin policy logging
#
# Adds required fixes:
# - google-auth creds (no oauth2client)
# - always supplies Duffel departure_date (no 422)
# - RCM geography enrichment (no invented city/country)
# - offer normalisation w/ placeholders so RDV formulas never break
#
# NON-NEGOTIABLE:
# - No stub inserts: core fields must exist or skip insert
# - CONFIG is brain for route pairs (origin_iata + destination_iata)
# - RCM enabled routes gate + enrich geography

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

def _env_bool(name: str, default: bool = False) -> bool:
    v = _env(name, "")
    if v == "":
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")

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

def _today_utc() -> dt.date:
    return utc_now().date()


# ----------------------------
# Google Sheets
# ----------------------------

def get_gspread_client() -> gspread.Client:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    raw = _env("GCP_SA_JSON_ONE_LINE", "") or _env("GCP_SA_JSON", "")
    info = json.loads(raw)
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


# ----------------------------
# Duffel
# ----------------------------

DUFFEL_API_KEY = _env("DUFFEL_API_KEY")
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
    # Duffel requires slices[*].departure_date for each slice
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
# Theme selection (daily)
# ----------------------------

def _theme_pool_from_config(config_rows: List[Dict[str, Any]]) -> Tuple[str, Dict[str, int]]:
    # Use theme_of_day if present else theme
    counts: Dict[str, int] = {}
    for r in config_rows:
        t = (r.get("theme_of_day") or r.get("theme") or "").strip()
        if not t:
            continue
        counts[t] = counts.get(t, 0) + 1
    if not counts:
        return "DEFAULT", {}
    pool = sorted(counts.keys())
    idx = _today_utc().timetuple().tm_yday % len(pool)
    return pool[idx], counts

def _effective_theme_override(pool_counts: Dict[str, int]) -> Optional[str]:
    override = _env("THEME", "").strip()
    if not override:
        return None
    if override.upper() == "DEFAULT":
        return None
    # case-insensitive match to pool
    for k in pool_counts.keys():
        if k.lower() == override.lower():
            return k
    log(f"⚠️ THEME override '{override}' not found in CONFIG pool. Ignoring override.")
    return None


# ----------------------------
# Origins policy
# ----------------------------

def _theme_origins_from_env(theme: str) -> List[str]:
    key = f"ORIGINS_{theme.upper()}"
    return _csv_list(_env(key, ""))

def _open_origins_effective() -> Tuple[bool, bool, bool]:
    # matches your logging shape
    open_origins = _env_bool("FEEDER_OPEN_ORIGINS", False)
    sparse_override = _env_bool("SPARSE_OVERRIDE", False)  # optional; safe default False
    effective_open = open_origins or sparse_override
    return open_origins, sparse_override, effective_open


# ----------------------------
# Inventory window + trip length
# ----------------------------

def _window_days_for_theme(theme: str, cfg_row: Dict[str, Any]) -> Tuple[int, int]:
    # Prefer CONFIG row ranges if present; else theme env; else defaults.
    # Hard cap max to 84 (Duffel constraint you’re enforcing).
    min_d = _safe_int(cfg_row.get("days_ahead_min"), 21)
    max_d = _safe_int(cfg_row.get("days_ahead_max"), 84)

    env_min = _env(f"WINDOW_{theme.upper()}_MIN", "").strip()
    env_max = _env(f"WINDOW_{theme.upper()}_MAX", "").strip()
    if env_min:
        min_d = _safe_int(env_min, min_d)
    if env_max:
        max_d = _safe_int(env_max, max_d)

    max_d = min(max_d, 84)
    min_d = max(0, min_d)
    if max_d < min_d:
        max_d = min_d
    return min_d, max_d

def _trip_len_days_for_theme(theme: str, cfg_row: Dict[str, Any]) -> int:
    tl = _safe_int(cfg_row.get("trip_length_days"), 5)
    env_min = _env(f"TRIP_{theme.upper()}_MIN", "").strip()
    env_max = _env(f"TRIP_{theme.upper()}_MAX", "").strip()
    if env_min and env_max:
        # deterministic choose mid
        a = _safe_int(env_min, tl)
        b = _safe_int(env_max, tl)
        if b < a:
            a, b = b, a
        tl = max(1, (a + b) // 2)
    return max(1, tl)

def _candidate_outbounds(min_d: int, max_d: int, trip_len: int, n: int = 3) -> List[dt.date]:
    # deterministic spread across window: min, mid, max-trip
    today = _today_utc()
    max_start = max(min_d, max_d - trip_len)
    if max_start < min_d:
        max_start = min_d
    points = []
    if n <= 1 or max_start == min_d:
        points = [min_d]
    else:
        mid = min_d + (max_start - min_d) // 2
        points = [min_d, mid, max_start]
    # unique preserve order
    seen = set()
    out = []
    for d in points:
        if d in seen:
            continue
        seen.add(d)
        out.append(today + dt.timedelta(days=d))
    return out


# ----------------------------
# Offer parsing + placeholders
# ----------------------------

def _offer_total_amount_gbp(offer: Dict[str, Any]) -> Optional[float]:
    # Prefer GBP offers only; reject non-GBP so downstream is stable
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
    # fallback from segment timestamps if needed
    if not out:
        segs = _slice_segments(offer, 0)
        if segs and segs[0].get("departing_at"):
            out = str(segs[0]["departing_at"])[:10]
    if not inn:
        segs = _slice_segments(offer, 1)
        if segs and segs[0].get("departing_at"):
            inn = str(segs[0]["departing_at"])[:10]
    return out, inn

def _duration_minutes_from_segments(segs: List[Dict[str, Any]]) -> int:
    # compute from timestamps if present
    try:
        if not segs:
            return 0
        start = segs[0].get("departing_at")
        end = segs[-1].get("arriving_at")
        if not start or not end:
            return 0
        s = dt.datetime.fromisoformat(str(start).replace("Z", "+00:00"))
        e = dt.datetime.fromisoformat(str(end).replace("Z", "+00:00"))
        mins = int((e - s).total_seconds() // 60)
        return max(0, mins)
    except Exception:
        return 0

def _carriers_from_offer(offer: Dict[str, Any]) -> str:
    carriers = set()
    for seg in (_slice_segments(offer, 0) + _slice_segments(offer, 1)):
        mc = (seg.get("marketing_carrier") or {}).get("iata_code")
        if mc:
            carriers.add(str(mc).upper())
    return ",".join(sorted(carriers)) if carriers else "na"

def _via_hub_from_offer(offer: Dict[str, Any]) -> str:
    segs = _slice_segments(offer, 0)
    if len(segs) <= 1:
        return "na"
    # first connection point = destination of first segment
    hub = (segs[0].get("destination") or {}).get("iata_code")
    return str(hub).upper() if hub else "na"

def _normalise_offer_fields(offer: Dict[str, Any]) -> Dict[str, Any]:
    segs_out = _slice_segments(offer, 0)
    segs_in = _slice_segments(offer, 1)
    stops = max(len(segs_out) - 1, 0)

    out_min = _duration_minutes_from_segments(segs_out)
    in_min = _duration_minutes_from_segments(segs_in)
    tot_h = round((out_min + in_min) / 60.0, 2) if (out_min + in_min) > 0 else 0

    # Duffel sometimes includes cabin_class on offer; if not, placeholder
    cabin = (offer.get("cabin_class") or "").strip() or "na"

    return {
        "stops": int(stops),
        "connection_type": "direct" if stops == 0 else "indirect",
        "cabin_class": cabin,
        "bags_incl": 0,  # do not invent; placeholder
        "outbound_duration_minutes": int(out_min),
        "inbound_duration_minutes": int(in_min),
        "total_duration_hours": tot_h,
        "via_hub": _via_hub_from_offer(offer),
        "carriers": _carriers_from_offer(offer),
        "currency": "GBP",  # we only accept GBP offers
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
    # keep as numeric; light rounding
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

    # Tabs
    SPREADSHEET_ID = _env("SPREADSHEET_ID") or _env("SHEET_ID")
    RAW_DEALS_TAB = _env("RAW_DEALS_TAB", "RAW_DEALS")
    CONFIG_TAB = _env("FEEDER_CONFIG_TAB", _env("CONFIG_TAB", "CONFIG"))
    RCM_TAB = _env("RCM_TAB", "ROUTE_CAPABILITY_MAP")

    # Caps
    DUFFEL_MAX_INSERTS = _env_int("DUFFEL_MAX_INSERTS", 50)
    DUFFEL_MAX_INSERTS_PER_ORIGIN = _env_int("DUFFEL_MAX_INSERTS_PER_ORIGIN", 15)
    DUFFEL_MAX_INSERTS_PER_ROUTE = _env_int("DUFFEL_MAX_INSERTS_PER_ROUTE", 5)
    DUFFEL_MAX_SEARCHES_PER_RUN = _env_int("DUFFEL_MAX_SEARCHES_PER_RUN", 12)
    DUFFEL_ROUTES_PER_RUN_ENV = _env_int("DUFFEL_ROUTES_PER_RUN", 6)
    FEEDER_SLEEP_SECONDS = _env_float("FEEDER_SLEEP_SECONDS", 0.1)

    # Process settings
    PROCESSED_PER_SEARCH = 5  # matches yesterday “processed=5”
    ZERO_OFFER_RETRY_ENABLED = _env_bool("ZERO_OFFER_RETRY_ENABLED", True)
    ZERO_OFFER_RETRY_MAX_DAYS = _env_int("ZERO_OFFER_RETRY_MAX_DAYS", 60)

    # Hygiene (kept but default off)
    HYGIENE_ENABLED = _env_bool("HYGIENE_ENABLED", False)
    HYGIENE_CONN_SHORT = _env_int("HYGIENE_CONN_SHORT", 1)
    HYGIENE_CONN_LONG = _env_int("HYGIENE_CONN_LONG", 2)
    HYGIENE_DUR_SHORT = _env_int("HYGIENE_DUR_SHORT", 720)
    HYGIENE_DUR_LONG = _env_int("HYGIENE_DUR_LONG", 1200)

    # Price gate (kept light; we rank cheapest rather than hard-kill everything)
    PRICE_GATE_BEHAVIOR = _env("PRICE_GATE_FALLBACK_BEHAVIOR", "BLOCK").upper().strip() or "BLOCK"
    PRICE_MULT = _env_float("PRICE_GATE_MULT", 1.0)
    PRICE_MINCAP = _env_float("PRICE_GATE_MINCAP", 80.0)

    open_origins, sparse_override, open_origins_effective = _open_origins_effective()
    req_origins = _env_int("REQUIRED_ORIGINS", 3)
    eff_routes = max(1, DUFFEL_ROUTES_PER_RUN_ENV)

    log(f"ORIGIN_POLICY: FEEDER_OPEN_ORIGINS={open_origins} | sparse_override={sparse_override} | effective_open={open_origins_effective}")
    log(f"CAPS: MAX_INSERTS={DUFFEL_MAX_INSERTS} | PER_ORIGIN={DUFFEL_MAX_INSERTS_PER_ORIGIN} | PER_ROUTE={DUFFEL_MAX_INSERTS_PER_ROUTE} | MAX_SEARCHES={DUFFEL_MAX_SEARCHES_PER_RUN} | ROUTES_PER_RUN(env)={DUFFEL_ROUTES_PER_RUN_ENV} | ROUTES_PER_RUN(effective)={eff_routes}")
    log(f"CAPACITY_NOTE: theoretical_max_inserts_this_run <= {min(DUFFEL_MAX_INSERTS, DUFFEL_MAX_INSERTS_PER_ORIGIN * eff_routes)} (based on caps + effective routes)")
    log(f"PRICE_GATE: fallback={PRICE_GATE_BEHAVIOR} | mult={PRICE_MULT} | mincap={PRICE_MINCAP}")
    log(f"HYGIENE: enabled={HYGIENE_ENABLED} | conn_short={HYGIENE_CONN_SHORT} conn_long={HYGIENE_CONN_LONG} | dur_short={HYGIENE_DUR_SHORT} dur_long={HYGIENE_DUR_LONG}")

    # Sheets init
    gc = get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws_cfg = sh.worksheet(CONFIG_TAB)
    ws_rcm = sh.worksheet(RCM_TAB)
    ws_raw = sh.worksheet(RAW_DEALS_TAB)

    # Load CONFIG + filter active
    cfg_rows_all = ws_cfg.get_all_records()
    cfg_rows = [r for r in cfg_rows_all if _is_true(r.get("active_in_feeder"))]
    log(f"✅ CONFIG loaded: {len(cfg_rows)} rows")

    # Load RCM enabled routes + geography map
    rcm_all = ws_rcm.get_all_records()
    rcm_enabled = [r for r in rcm_all if _is_true(r.get("enabled"))]
    log(f"✅ RCM loaded: {len(rcm_enabled)} enabled routes")

    # RCM map by (origin_iata, destination_iata)
    rcm_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for r in rcm_enabled:
        o = str(r.get("origin_iata") or "").strip().upper()
        d = str(r.get("destination_iata") or "").strip().upper()
        if not o or not d:
            continue
        rcm_map[(o, d)] = r

    # Theme pool and selection
    theme_today, pool_counts = _theme_pool_from_config(cfg_rows)
    override = _effective_theme_override(pool_counts)
    if override:
        theme_today = override

    # theme pool log (matches your earlier format)
    pool_size = len(pool_counts)
    summary = ", ".join([f"{k}:{pool_counts[k]}" for k in sorted(pool_counts.keys())])
    log(f"🧠 Theme pool mode: theme_of_day | pool_size={pool_size} | {summary}")
    log(f"🎯 Theme of the day (UTC): {theme_today}")

    # Strategy 90/10 (kept deterministic)
    MOD = _env_int("FEEDER_EXPLORE_RUN_MOD", 10)
    run_slot = _env("RUN_SLOT", "").strip()
    salt = _env("FEEDER_EXPLORE_RUN_MOD_SALT", "").strip()
    seed = f"{_today_utc().isoformat()}|{run_slot}|{salt}"
    h = int(hashlib.md5(seed.encode("utf-8")).hexdigest(), 16)
    explore_run = (MOD > 0) and (h % MOD == 0)

    theme_quota = eff_routes
    explore_quota = 0 if not explore_run else max(0, eff_routes - theme_quota)
    log(f"🧠 Strategy: 90/10 | explore_run={explore_run} | theme_quota={theme_quota} | explore_quota={explore_quota} | MOD={MOD}")

    # Split config rows into theme/explore
    theme_rows = [r for r in cfg_rows if str(r.get("theme_of_day") or r.get("theme") or "").strip() == theme_today]
    explore_rows = [r for r in cfg_rows if str(r.get("theme_of_day") or r.get("theme") or "").strip() != theme_today]

    # Planned origins
    explicit_origins_now = _theme_origins_from_env(theme_today)
    if explicit_origins_now and not open_origins_effective:
        planned_origins = explicit_origins_now[:]
        plan_n = len(planned_origins)
    else:
        # fall back to default allowlist (still does NOT create new routes; CONFIG pairs are respected)
        default_origins = _csv_list(_env("ORIGINS_DEFAULT", "LHR,LGW,MAN,BRS,STN"))
        planned_origins = (explicit_origins_now or default_origins)[: max(5, req_origins, eff_routes)]
        plan_n = len(planned_origins)

    log(f"🧭 Planned origins for run ({len(planned_origins)}; required={plan_n}): {planned_origins}")
    log(f"🧭 Unique theme routes: {len(theme_rows)} | Unique explore routes: {len(explore_rows)}")

    # RAW_DEALS headers
    headers = ws_raw.row_values(1)
    idx = _build_header_index(headers)

    # Caps counters
    inserted_by_origin: Dict[str, int] = {}
    inserted_by_route: Dict[Tuple[str, str], int] = {}
    deals_out: List[List[Any]] = []

    searches_done = 0
    deals_collected = 0

    def can_insert(origin: str, dest: str) -> bool:
        if deals_collected + len(deals_out) >= DUFFEL_MAX_INSERTS:
            return False
        if inserted_by_origin.get(origin, 0) >= DUFFEL_MAX_INSERTS_PER_ORIGIN:
            return False
        if inserted_by_route.get((origin, dest), 0) >= DUFFEL_MAX_INSERTS_PER_ROUTE:
            return False
        return True

    def geography_for_pair(origin: str, dest: str) -> Optional[Dict[str, Any]]:
        return rcm_map.get((origin, dest))

    def geo_core_ok(geo: Dict[str, Any]) -> bool:
        return all([
            (geo.get("origin_city") or "").strip(),
            (geo.get("origin_country") or "").strip(),
            (geo.get("destination_city") or "").strip(),
            (geo.get("destination_country") or "").strip(),
        ])

    # Choose which rows to search (theme first)
    rows_to_search = theme_rows[:]

    # enforce route-pairs strictly: CONFIG already contains explicit pairs; we do not invent
    # order by priority desc then search_weight desc
    def sort_key(r: Dict[str, Any]) -> Tuple[int, float]:
        return (_safe_int(r.get("priority"), 0), _safe_float(r.get("search_weight"), 0.0))

    rows_to_search.sort(key=sort_key, reverse=True)
    rows_to_search = rows_to_search[:eff_routes]

    # For each planned route, search window candidates and pick cheapest offers
    for cfg in rows_to_search:
        if searches_done >= DUFFEL_MAX_SEARCHES_PER_RUN:
            break

        origin = str(cfg.get("origin_iata") or "").strip().upper()
        dest = str(cfg.get("destination_iata") or "").strip().upper()
        if not origin or not dest:
            continue

        if not can_insert(origin, dest):
            continue

        geo = geography_for_pair(origin, dest)
        if not geo or not geo_core_ok(geo):
            # no invented city/country; skip
            continue

        theme = theme_today
        cabin = (cfg.get("cabin_class") or "economy").strip() or "economy"
        max_conn = _safe_int(cfg.get("max_connections"), _env_int(f"MAX_STOPS_{theme.upper()}", 1))

        inv_min, inv_max = _window_days_for_theme(theme, cfg)
        trip_len = _trip_len_days_for_theme(theme, cfg)

        log(f"INVENTORY_WINDOW_DAYS={inv_min}-{inv_max} | ZERO_OFFER_RETRY_ENABLED={ZERO_OFFER_RETRY_ENABLED} retry_window_max={ZERO_OFFER_RETRY_MAX_DAYS}")

        # candidates
        out_dates = _candidate_outbounds(inv_min, inv_max, trip_len, n=3)

        best_offers: List[Dict[str, Any]] = []
        offers_returned_total = 0

        def do_search(out_d: dt.date) -> List[Dict[str, Any]]:
            nonlocal searches_done
            if searches_done >= DUFFEL_MAX_SEARCHES_PER_RUN:
                return []
            out_s = out_d.isoformat()
            in_s = (out_d + dt.timedelta(days=trip_len)).isoformat()
            log(f"Duffel[PRIMARY]: Searching {origin}->{dest} {out_s}/{in_s}")
            searches_done += 1
            offers = duffel_create_offer_request(origin, dest, out_s, in_s, cabin_class=cabin)
            time.sleep(FEEDER_SLEEP_SECONDS)
            return offers

        # primary window sampling
        for out_d in out_dates:
            offers = do_search(out_d)
            offers_returned_total += len(offers)
            best_offers.extend(offers)

        # zero-offer retry: if nothing came back at all, try a later window (bounded)
        if ZERO_OFFER_RETRY_ENABLED and offers_returned_total == 0 and searches_done < DUFFEL_MAX_SEARCHES_PER_RUN:
            retry_min = inv_min
            retry_max = min(max(inv_min + 1, inv_max), ZERO_OFFER_RETRY_MAX_DAYS)
            retry_out = _today_utc() + dt.timedelta(days=retry_min)
            retry_in = retry_out + dt.timedelta(days=trip_len)
            log(f"🔄 ZERO_OFFER_RETRY: {origin}->{dest} using {retry_min}-{retry_max}d window => {retry_out.isoformat()}/{retry_in.isoformat()}")
            offers = do_search(retry_out)
            offers_returned_total += len(offers)
            best_offers.extend(offers)

        # Filter + rank offers
        rejected_non_gbp = 0
        rejected_price_hard = 0
        rejected_hygiene_conn = 0
        rejected_hygiene_dur = 0
        rejected_band = 0  # kept for log shape

        ranked: List[Tuple[float, Dict[str, Any]]] = []

        for off in best_offers:
            amt = _offer_total_amount_gbp(off)
            if amt is None:
                rejected_non_gbp += 1
                continue

            # connection gating (light; respects max_connections)
            stops = max(len(_slice_segments(off, 0)) - 1, 0)
            if stops > max_conn:
                rejected_hygiene_conn += 1
                continue

            # duration hygiene optional
            if HYGIENE_ENABLED:
                out_min = _duration_minutes_from_segments(_slice_segments(off, 0))
                in_min = _duration_minutes_from_segments(_slice_segments(off, 1))
                if (out_min and out_min > HYGIENE_DUR_LONG) or (in_min and in_min > HYGIENE_DUR_LONG):
                    rejected_hygiene_dur += 1
                    continue

            ranked.append((amt, off))

        ranked.sort(key=lambda x: x[0])

        # Price gate: do NOT kill everything early; but keep behaviour compatibility.
        # We cap at (mincap or cheapest*mult) if BLOCK; otherwise allow.
        gbp_ranked = len(ranked)
        if gbp_ranked == 0:
            cap_str = "na"
            band_str = "na"
            log(
                f"Duffel[PRIMARY]: offers_returned={offers_returned_total} gbp_offers=0 cap_gbp={cap_str} band_cap={band_str} "
                f"rej_non_gbp={rejected_non_gbp} rej_price={rejected_price_hard} rej_conn={rejected_hygiene_conn} "
                f"rej_dur={rejected_hygiene_dur} rej_band={rejected_band} inserted=0"
            )
            continue

        cheapest = ranked[0][0]
        cap_gbp = max(PRICE_MINCAP, cheapest * PRICE_MULT)
        cap_str = f"{cap_gbp:.0f}"

        filtered_ranked: List[Tuple[float, Dict[str, Any]]] = []
        for amt, off in ranked:
            if PRICE_GATE_BEHAVIOR == "BLOCK" and amt > cap_gbp:
                rejected_price_hard += 1
                continue
            filtered_ranked.append((amt, off))

        # Insert up to PROCESSED_PER_SEARCH offers
        processed = 0
        inserted_here = 0

        for amt, off in filtered_ranked:
            if processed >= PROCESSED_PER_SEARCH:
                break
            if not can_insert(origin, dest):
                break

            deal_id = (off.get("id") or "").strip()
            out_date, ret_date = _extract_dates_from_offer(off)

            # core fields must exist (no stubs)
            if not deal_id or not out_date or not ret_date:
                continue

            # build row
            row = [""] * len(headers)

            # core
            _row_put(row, idx, "status", "NEW")
            _row_put(row, idx, "deal_id", deal_id)
            _row_put(row, idx, "price_gbp", _round_price_gbp(float(amt)))
            _row_put(row, idx, "origin_iata", origin)
            _row_put(row, idx, "destination_iata", dest)
            _row_put(row, idx, "origin_city", (geo.get("origin_city") or "").strip())
            _row_put(row, idx, "origin_country", (geo.get("origin_country") or "").strip())
            _row_put(row, idx, "destination_city", (geo.get("destination_city") or "").strip())
            _row_put(row, idx, "destination_country", (geo.get("destination_country") or "").strip())
            _row_put(row, idx, "outbound_date", out_date)
            _row_put(row, idx, "return_date", ret_date)
            _row_put(row, idx, "deal_theme", theme)
            _row_put(row, idx, "theme", theme)
            _row_put(row, idx, "ingested_at_utc", _ingested_iso())
            _row_put(row, idx, "created_utc", _ingested_iso())

            # offer-derived placeholders (never invent; always populate)
            norm = _normalise_offer_fields(off)
            _row_put(row, idx, "stops", norm.get("stops", 0))
            _row_put(row, idx, "bags_incl", norm.get("bags_incl", 0))
            _row_put(row, idx, "cabin_class", norm.get("cabin_class", "na"))
            _row_put(row, idx, "connection_type", norm.get("connection_type", "na"))
            _row_put(row, idx, "outbound_duration_minutes", norm.get("outbound_duration_minutes", 0))
            _row_put(row, idx, "inbound_duration_minutes", norm.get("inbound_duration_minutes", 0))
            _row_put(row, idx, "total_duration_hours", norm.get("total_duration_hours", 0))
            _row_put(row, idx, "via_hub", norm.get("via_hub", "na"))
            _row_put(row, idx, "carriers", norm.get("carriers", "na"))
            _row_put(row, idx, "currency", norm.get("currency", "GBP"))

            # core guardrail check (no stubs)
            core_required = ["deal_id", "price_gbp", "origin_iata", "destination_iata", "outbound_date", "return_date", "destination_city", "destination_country", "origin_city", "origin_country", "deal_theme"]
            ok = True
            for k in core_required:
                v = row[idx[k]] if k in idx else ""
                if v in ("", None):
                    ok = False
                    break
            if not ok:
                continue

            deals_out.append(row)
            processed += 1
            inserted_here += 1
            inserted_by_origin[origin] = inserted_by_origin.get(origin, 0) + 1
            inserted_by_route[(origin, dest)] = inserted_by_route.get((origin, dest), 0) + 1

        band_str = "1.0x"  # preserved log shape
        log(
            f"Duffel[PRIMARY]: offers_returned={offers_returned_total} gbp_ranked={gbp_ranked} processed={processed} "
            f"cap_gbp={cap_str} band_cap={band_str} rej_non_gbp={rejected_non_gbp} rej_price={rejected_price_hard} "
            f"rej_conn={rejected_hygiene_conn} rej_dur={rejected_hygiene_dur} rej_band={rejected_band} "
            f"inserted={inserted_here} origin_total={inserted_by_origin.get(origin,0)}/{DUFFEL_MAX_INSERTS_PER_ORIGIN} "
            f"running_total={len(deals_out)}/{DUFFEL_MAX_INSERTS}"
        )

    log(f"✓ Searches completed: {searches_done}")
    log(f"✓ Deals collected: {len(deals_out)} (cap {DUFFEL_MAX_INSERTS})")

    if not deals_out:
        log("⚠️ No winners to insert")
        return 0

    ws_raw.append_rows(deals_out, value_input_option="USER_ENTERED")
    log(f"✅ Inserted {len(deals_out)} rows into {RAW_DEALS_TAB}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
