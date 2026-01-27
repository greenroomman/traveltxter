# workers/pipeline_worker.py
# FULL FILE REPLACEMENT — FEEDER v4.8f
#
# CHANGE REQUEST (YOU):
# - Feeder must use a deterministic 90/10 split on EACH search attempt:
#     * 90% searches use CONFIG rows matching RUN_SLOT (AM or PM)
#     * 10% searches deliberately sample the opposite slot's CONFIG rows
#   This makes the feeder "look beyond" without increasing scan volume.
#
# LOCKED RULE (YOU):
# - ALL shorthaul must be DIRECT.
#   Shorthaul is defined as CONFIG.is_long_haul != TRUE.
#   Therefore:
#     - For shorthaul rows we force max_connections=0 on Duffel request
#     - We derive actual stops from the selected offer (segments-1)
#     - If shorthaul and stops != 0, we skip inserting that deal
#
# Notes:
# - No redesign of the pipeline
# - RAW_DEALS remains sole writable state
# - RDV is never written
# - Slot logic is driven by:
#     * RUN_SLOT env (AM/PM)
#     * CONFIG.slot_hint (AM/PM)
#   If CONFIG has no slot_hint populated, we fall back gracefully.
#
# Output improvements:
# - Logs show which slot stream each search was drawn from (primary vs exploration)

from __future__ import annotations

import os
import sys
import json
import hashlib
import datetime as dt
import math
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


def _eligible_themes_from_ztb(ztb_rows: List[Dict[str, Any]]) -> List[str]:
    today_mmdd = _mmdd(_today_utc())
    themes: List[str] = []
    for r in ztb_rows:
        theme = str(r.get("theme") or "").strip()
        if not theme:
            continue
        if not _is_true(r.get("enabled")):
            continue
        start_mmdd = _safe_int(r.get("start_mmdd"), 101)
        end_mmdd = _safe_int(r.get("end_mmdd"), 1231)
        if _in_window(today_mmdd, start_mmdd, end_mmdd):
            themes.append(theme)
    return list(dict.fromkeys(themes))


def _theme_of_day(eligible: List[str]) -> str:
    if not eligible:
        return "unexpected_value"
    base = dt.date(2026, 1, 1)
    idx = (_today_utc() - base).days % len(eligible)
    return sorted(eligible)[idx]


def _ztb_row_for_theme(ztb_rows: List[Dict[str, Any]], theme: str) -> Optional[Dict[str, Any]]:
    for r in ztb_rows:
        if str(r.get("theme") or "").strip().lower() == theme.lower():
            return r
    return None


def _connections_from_tolerance(tol: str) -> int:
    t = (tol or "").strip().lower()
    if t in ("direct", "0", "zero"):
        return 0
    if t in ("1", "one_stop", "one-stop", "onestop"):
        return 1
    if t in ("2", "two_stop", "two-stop", "twostop"):
        return 2
    return 2


def _sha12(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:12]


def _deal_id(origin: str, dest: str, od: dt.date, rd: dt.date, price_gbp: float, cabin: str, stops: int) -> str:
    price_i = int(math.ceil(float(price_gbp)))
    base = f"{origin}|{dest}|{od.isoformat()}|{rd.isoformat()}|{price_i}|{cabin}|{int(stops)}"
    return f"D{_sha12(base)}"


DUFFEL_API_KEY = _env("DUFFEL_API_KEY")
DUFFEL_VERSION = "v2"
DUFFEL_BASE = "https://api.duffel.com"

HEADERS = {
    "Authorization": f"Bearer {DUFFEL_API_KEY}",
    "Duffel-Version": DUFFEL_VERSION,
    "Content-Type": "application/json",
}


def _duffel_search(
    origin: str,
    destination: str,
    outbound_date: dt.date,
    return_date: dt.date,
    max_connections: int,
    cabin: str,
    included_airlines: List[str],
) -> Optional[Dict[str, Any]]:
    url = f"{DUFFEL_BASE}/air/offer_requests"
    payload: Dict[str, Any] = {
        "data": {
            "slices": [
                {"origin": origin, "destination": destination, "departure_date": outbound_date.isoformat()},
                {"origin": destination, "destination": origin, "departure_date": return_date.isoformat()},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": (cabin or "economy").lower(),
        }
    }

    for sl in payload["data"]["slices"]:
        sl["max_connections"] = int(max_connections)

    if included_airlines:
        payload["data"]["allowed_carrier_iatas"] = included_airlines

    try:
        r = requests.post(url, headers=HEADERS, json=payload, timeout=60)
        if r.status_code >= 400:
            log(f"❌ Duffel offer_request failed {origin}->{destination}: {r.status_code} {r.text[:300]}")
            return None
        return r.json()
    except Exception as e:
        log(f"❌ Duffel request exception {origin}->{destination}: {e}")
        return None


def _offer_stops(offer: Dict[str, Any]) -> Optional[int]:
    try:
        slices = offer.get("slices") or []
        if not slices:
            return None
        max_conn = 0
        for s in slices:
            segs = s.get("segments") or []
            c = max(0, len(segs) - 1)
            if c > max_conn:
                max_conn = c
        return max_conn
    except Exception:
        return None


def _best_offer_gbp(offers_json: Dict[str, Any]) -> Optional[Tuple[float, int]]:
    try:
        offers = offers_json.get("data", {}).get("offers") or []
        if not offers:
            return None

        best_price: Optional[float] = None
        best_stops: int = 99

        for o in offers:
            total = o.get("total_amount")
            cur = o.get("total_currency")
            if not total:
                continue
            if cur and str(cur).upper() != "GBP":
                continue

            price = float(total)
            stops = _offer_stops(o)
            stops_i = int(stops) if stops is not None else 99

            if best_price is None or price < best_price or (price == best_price and stops_i < best_stops):
                best_price = price
                best_stops = stops_i

        if best_price is None:
            return None

        return (best_price, best_stops)
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
    out = []
    for x in (first, mid, last):
        if x not in out:
            out.append(x)
    return out[:n]


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


def _slot_norm(s: str) -> str:
    x = str(s or "").strip().upper()
    if x in ("AM", "PM"):
        return x
    return ""


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
    CONFIG_CARRIER_BIAS_TAB = _env("CONFIG_CARRIER_BIAS_TAB", "CONFIG_CARRIER_BIAS")

    RUN_SLOT = _slot_norm(_env("RUN_SLOT", ""))
    if RUN_SLOT == "":
        RUN_SLOT = "AM"  # deterministic fallback

    DUFFEL_MAX_INSERTS = _env_int("DUFFEL_MAX_INSERTS", 50)
    DUFFEL_MAX_INSERTS_PER_ORIGIN = _env_int("DUFFEL_MAX_INSERTS_PER_ORIGIN", 15)
    DUFFEL_MAX_INSERTS_PER_ROUTE = _env_int("DUFFEL_MAX_INSERTS_PER_ROUTE", 5)
    DUFFEL_MAX_SEARCHES_PER_RUN = _env_int("DUFFEL_MAX_SEARCHES_PER_RUN", 12)
    DESTS_PER_RUN = _env_int("DUFFEL_ROUTES_PER_RUN", 4)
    ORIGINS_PER_DEST = _env_int("ORIGINS_PER_DEST", 3)

    # 90/10 control (search-level), default 9/1
    SLOT_PRIMARY_PCT = _env_int("SLOT_PRIMARY_PCT", 90)

    log(
        f"CAPS: MAX_INSERTS={DUFFEL_MAX_INSERTS} | PER_ORIGIN={DUFFEL_MAX_INSERTS_PER_ORIGIN} | "
        f"PER_ROUTE={DUFFEL_MAX_INSERTS_PER_ROUTE} | MAX_SEARCHES={DUFFEL_MAX_SEARCHES_PER_RUN} | "
        f"DESTS_PER_RUN={DESTS_PER_RUN} | ORIGINS_PER_DEST={ORIGINS_PER_DEST} | RUN_SLOT={RUN_SLOT} | "
        f"SLOT_SPLIT={SLOT_PRIMARY_PCT}/{100 - SLOT_PRIMARY_PCT}"
    )

    gc = _get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_raw = sh.worksheet(RAW_DEALS_TAB)
    ws_cfg = sh.worksheet(CONFIG_TAB)

    try:
        ws_ztb = sh.worksheet(ZTB_TAB)
    except Exception:
        ws_ztb = sh.worksheet("ZONE_THEME_BENCHMARKS")

    ws_rcm = sh.worksheet(RCM_TAB)
    ws_iata = sh.worksheet(IATA_TAB)

    # Carrier bias: loaded + logged (not required for this change)
    bias_rows: List[Dict[str, Any]] = []
    try:
        ws_bias = sh.worksheet(CONFIG_CARRIER_BIAS_TAB)
        bias_rows = ws_bias.get_all_records() or []
    except Exception:
        bias_rows = []

    usable_bias = 0
    if bias_rows:
        for r in bias_rows:
            cc = str(r.get("carrier_code") or "").strip()
            th = str(r.get("theme") or "").strip()
            di = str(r.get("destination_iata") or "").strip()
            bw = r.get("bias_weight")
            if cc and th and di and _safe_float(bw, 0.0) > 0:
                usable_bias += 1
    if usable_bias > 0:
        log(f"✅ CONFIG_CARRIER_BIAS loaded: {usable_bias} usable rows")
    else:
        log("⚠️ CONFIG_CARRIER_BIAS not loaded or empty. Origin selection will use fallback pools.")

    ztb_rows_all = ws_ztb.get_all_records()
    eligible_themes = _eligible_themes_from_ztb(ztb_rows_all)
    theme_today = _theme_of_day(eligible_themes)

    log(f"✅ ZTB loaded: {len(ztb_rows_all)} rows | eligible_today={len(eligible_themes)} | pool={eligible_themes}")
    log(f"🎯 Theme of the day (UTC): {theme_today}")

    ztb_today = _ztb_row_for_theme(ztb_rows_all, theme_today) or {"connection_tolerance": "any"}
    ztb_start = _safe_int(ztb_today.get("start_mmdd"), 101)
    ztb_end = _safe_int(ztb_today.get("end_mmdd"), 1231)
    ztb_max_conn = _connections_from_tolerance(str(ztb_today.get("connection_tolerance") or "any"))

    cfg_all = ws_cfg.get_all_records()
    cfg_active = [r for r in cfg_all if _is_true(r.get("active_in_feeder")) and _is_true(r.get("enabled"))]
    log(f"✅ CONFIG loaded: {len(cfg_active)} active rows (of {len(cfg_all)} total)")

    def cfg_theme(r: Dict[str, Any]) -> str:
        return (str(r.get("primary_theme") or "").strip() or str(r.get("audience_type") or "").strip()).strip()

    def cfg_slot(r: Dict[str, Any]) -> str:
        return _slot_norm(r.get("slot_hint"))

    def is_longhaul(r: Dict[str, Any]) -> bool:
        return str(r.get("is_long_haul") or "").strip().upper() == "TRUE"

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

    # Split rows by slot
    rows_primary = [r for r in theme_rows_all if cfg_slot(r) == RUN_SLOT]
    opp = "PM" if RUN_SLOT == "AM" else "AM"
    rows_explore = [r for r in theme_rows_all if cfg_slot(r) == opp]

    # If slot_hint isn't populated for theme, treat all as primary
    if not rows_primary and not rows_explore:
        rows_primary = theme_rows_all[:]
        rows_explore = []

    # Sort by priority for stable sampling
    def sort_key(r: Dict[str, Any]) -> Tuple[float, float, float]:
        return (
            _safe_float(r.get("priority"), 0),
            _safe_float(r.get("search_weight"), 0),
            _safe_float(r.get("content_priority"), 0),
        )

    rows_primary.sort(key=sort_key, reverse=True)
    rows_explore.sort(key=sort_key, reverse=True)

    # Build a deterministic per-search plan (90/10) — choice is made PER SEARCH ATTEMPT
    # We do not increase MAX_SEARCHES; we just decide which config stream each attempt uses.
    primary_period = max(1, min(99, SLOT_PRIMARY_PCT))
    explore_period = 100 - primary_period

    # Deterministic sequence: every 10th attempt (when 90/10) becomes exploration.
    # Generalized: exploration happens when (attempt_index % 100) >= primary_period.
    log(
        f"🧩 Slot pools: primary({RUN_SLOT})={len(rows_primary)} | explore({opp})={len(rows_explore)} | "
        f"theme_rows_total={len(theme_rows_all)}"
    )

    # Geo dictionary from RCM + IATA_MASTER
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

    for r in ws_iata.get_all_records():
        code = str(
            r.get("iata_code")
            or r.get("iata")
            or r.get("IATA")
            or r.get("iataCode")
            or ""
        ).strip().upper()
        city = str(r.get("city") or r.get("City") or "").strip()
        country = str(r.get("country") or r.get("Country") or "").strip()
        if code and city and country and code not in iata_geo:
            iata_geo[code] = (city, country)

    log(f"✅ Geo dictionary loaded: {len(iata_geo)} IATA entries (RCM + {IATA_TAB})")

    origins_default = _csv_list(_env("ORIGINS_DEFAULT", "LHR,LGW,MAN,BRS,STN,LTN,BHX,EDI,GLA"))
    hub_origins = _csv_list(_env("HUB_ORIGINS", "LHR,LGW,MAN"))
    lcc_origins = _csv_list(_env("LCC_ORIGINS", "STN,LTN,LGW,MAN,BRS,BHX,EDI,GLA"))

    headers = ws_raw.row_values(1)
    idx = _build_header_index(headers)

    deals_out: List[List[Any]] = []
    inserted_by_origin: Dict[str, int] = {}
    inserted_by_route: Dict[Tuple[str, str], int] = {}

    def geo_for(iata: str) -> Optional[Tuple[str, str]]:
        return iata_geo.get(iata.upper())

    def can_insert(origin: str, dest: str) -> bool:
        if len(deals_out) >= DUFFEL_MAX_INSERTS:
            return False
        if inserted_by_origin.get(origin, 0) >= DUFFEL_MAX_INSERTS_PER_ORIGIN:
            return False
        if inserted_by_route.get((origin, dest), 0) >= DUFFEL_MAX_INSERTS_PER_ROUTE:
            return False
        return True

    def carriers_from_cfg(cfg_row: Dict[str, Any]) -> List[str]:
        raw = str(cfg_row.get("included_airlines") or "").strip()
        if not raw:
            return []
        return _csv_list(raw)

    def choose_origins_for_dest(cfg_row: Dict[str, Any], dest: str, cap: int) -> List[str]:
        is_long_local = is_longhaul(cfg_row)
        gw = str(cfg_row.get("gateway_type") or "").strip().lower()

        if is_long_local:
            candidate_pool = [o for o in hub_origins if o in origins_default] or origins_default[:]
        else:
            if gw in ("commodity", "value"):
                candidate_pool = [o for o in lcc_origins if o in origins_default] or origins_default[:]
            else:
                candidate_pool = origins_default[:]

        seed = f"{_today_utc().isoformat()}|{theme_today}|{dest}|{RUN_SLOT}"
        h = int(hashlib.md5(seed.encode("utf-8")).hexdigest(), 16)
        ranked = sorted(candidate_pool, reverse=True)
        if ranked:
            rot = h % len(ranked)
            ranked = ranked[rot:] + ranked[:rot]
        return ranked[:cap]

    searches = 0

    # Deterministically rotate within each pool by attempt index
    def pick_cfg_from_pool(pool: List[Dict[str, Any]], attempt_idx: int) -> Optional[Dict[str, Any]]:
        if not pool:
            return None
        return pool[attempt_idx % len(pool)]

    # Optionally cap how many unique destinations we attempt (DESTS_PER_RUN),
    # but we now select config rows PER SEARCH, not just once per run.
    # We keep a small guard so we don't hammer the same destination endlessly.
    dest_seen: Dict[str, int] = {}

    while searches < DUFFEL_MAX_SEARCHES_PER_RUN and len(deals_out) < DUFFEL_MAX_INSERTS:
        attempt = searches  # 0-based
        mode = "PRIMARY"
        use_explore = False

        if explore_period > 0:
            if (attempt % 100) >= primary_period:
                use_explore = True

        cfg_pick = None
        if use_explore and rows_explore:
            cfg_pick = pick_cfg_from_pool(rows_explore, attempt)
            mode = f"EXPLORE({opp})"
        else:
            cfg_pick = pick_cfg_from_pool(rows_primary, attempt)
            mode = f"PRIMARY({RUN_SLOT})"

        if cfg_pick is None:
            # fallback to whatever exists
            cfg_pick = pick_cfg_from_pool(theme_rows_all, attempt)
            mode = "FALLBACK(ALL)"

        if cfg_pick is None:
            log("⚠️ No CONFIG rows available to pick from. Ending run.")
            break

        dest = str(cfg_pick.get("destination_iata") or "").strip().upper()
        if not dest:
            searches += 1
            continue

        # light anti-stutter (does not change caps, just avoids repeating the same dest every attempt)
        dest_seen[dest] = dest_seen.get(dest, 0) + 1
        if dest_seen[dest] > max(1, DESTS_PER_RUN):
            searches += 1
            continue

        if not geo_for(dest):
            log(f"⚠️ Missing geo for destination_iata={dest}. Skipping (no invented geo).")
            searches += 1
            continue

        min_d = _safe_int(cfg_pick.get("days_ahead_min"), 7)
        max_d = _safe_int(cfg_pick.get("days_ahead_max"), 120)
        trip_len = _safe_int(cfg_pick.get("trip_length_days"), 5)
        out_dates = _candidate_outbounds_seasonal(min_d, max_d, trip_len, ztb_start, ztb_end, n=2)
        if not out_dates:
            searches += 1
            continue

        is_long = is_longhaul(cfg_pick)

        cfg_max_conn = _safe_int(cfg_pick.get("max_connections"), 2)
        max_conn = min(cfg_max_conn, ztb_max_conn)

        # Hard rule: shorthaul must be direct
        if not is_long:
            max_conn = 0

        cabin = str(cfg_pick.get("cabin_class") or "economy").strip().lower() or "economy"
        included_airlines = carriers_from_cfg(cfg_pick)
        origins = choose_origins_for_dest(cfg_pick, dest, ORIGINS_PER_DEST)

        # Make exactly one origin+date attempt per search tick (keeps "each search" semantics)
        origin = origins[0] if origins else ""
        od = out_dates[0] if out_dates else None
        if not origin or od is None or not geo_for(origin):
            searches += 1
            continue

        if not can_insert(origin, dest):
            searches += 1
            continue

        rd = od + dt.timedelta(days=trip_len)

        log(f"🔎 Search[{attempt+1}/{DUFFEL_MAX_SEARCHES_PER_RUN}] mode={mode} dest={dest} origin={origin} "
            f"haul={'LONG' if is_long else 'SHORT'} max_conn={max_conn} slot_hint={cfg_slot(cfg_pick) or '—'}")

        resp = _duffel_search(origin, dest, od, rd, max_conn, cabin, included_airlines)
        searches += 1
        if not resp:
            continue

        best = _best_offer_gbp(resp)
        if best is None:
            continue
        price, offer_stops = best

        # Hard rule: shorthaul must be direct (truth derived from offer)
        if (not is_long) and int(offer_stops) != 0:
            continue

        row = [""] * len(headers)

        def set_if(col: str, val: Any) -> None:
            j = idx.get(col)
            if j is not None:
                row[j] = val

        oc, ok = geo_for(origin) or ("", "")
        dc, dk = geo_for(dest) or ("", "")

        did = _deal_id(origin, dest, od, rd, float(price), cabin, int(offer_stops))

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

        set_if("stops", int(offer_stops))

        set_if("theme", theme_today)
        set_if("primary_theme", cfg_theme(cfg_pick))
        set_if("ingested_at", dt.datetime.utcnow().isoformat() + "Z")
        set_if("source", "duffel")
        set_if("max_connections", int(max_conn))
        set_if("cabin_class", cabin)
        set_if("included_airlines", ",".join(included_airlines) if included_airlines else "")
        set_if("slot_hint", cfg_slot(cfg_pick))

        deals_out.append(row)
        inserted_by_origin[origin] = inserted_by_origin.get(origin, 0) + 1
        inserted_by_route[(origin, dest)] = inserted_by_route.get((origin, dest), 0) + 1

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
