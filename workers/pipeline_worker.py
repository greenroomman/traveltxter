# workers/pipeline_worker.py
# FULL FILE REPLACEMENT — FEEDER vNext (CONFIG + ZTB + carrier-bias + seasonal clamp)
# Patch: if ZTB tab isn't literally named "ZTB", fallback to "ZONE_THEME_BENCHMARKS"
# Patch: CONFIG_CARRIER_BIAS load supports BOTH schemas:
#   A) carrier_iata, origin_iata, weight   (origin weighting)
#   B) carrier_code, carrier_name, theme, destination_iata, bias_weight, notes  (destination carrier bias)
# Patch: PRICE PSYCHOLOGY REMOVED from feeder (no hard-gating on price_psychology_*)

from __future__ import annotations

import os
import sys
import json
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


def _norm_header(h: str) -> str:
    return "".join(ch for ch in (h or "").strip().lower() if ch.isalnum() or ch == "_")


def _find_first_nonempty_row(values: List[List[str]], max_scan: int = 30) -> int:
    for i, row in enumerate(values[:max_scan]):
        if any(str(c).strip() for c in row):
            return i
    return -1


def _col_index(headers_norm: List[str], candidates: List[str]) -> Optional[int]:
    cand = set(candidates)
    for j, hn in enumerate(headers_norm):
        if hn in cand:
            return j
    return None


def _load_bias_schema_A(values: List[List[str]]) -> List[Dict[str, Any]]:
    # Schema A: carrier_iata, origin_iata, weight
    hdr_i = _find_first_nonempty_row(values)
    if hdr_i < 0:
        return []
    raw_headers = values[hdr_i]
    headers_norm = [_norm_header(h) for h in raw_headers]

    j_carrier = _col_index(headers_norm, ["carrier_iata", "carrier", "airline_iata", "airline", "carrieriata"])
    j_origin = _col_index(headers_norm, ["origin_iata", "origin", "originiata"])
    j_weight = _col_index(headers_norm, ["weight", "w", "bias_weight", "wt", "score"])

    if j_carrier is None or j_origin is None or j_weight is None:
        return []

    out: List[Dict[str, Any]] = []
    for row in values[hdr_i + 1 :]:
        if not any(str(c).strip() for c in row):
            continue
        c = (row[j_carrier] if j_carrier < len(row) else "").strip()
        o = (row[j_origin] if j_origin < len(row) else "").strip()
        w = (row[j_weight] if j_weight < len(row) else "").strip()
        if not c or not o or not w:
            continue
        out.append({"carrier_iata": c, "origin_iata": o, "weight": w})
    return out


def _load_bias_schema_B(values: List[List[str]]) -> List[Dict[str, Any]]:
    # Schema B (your current): carrier_code, carrier_name, theme, destination_iata, bias_weight, notes
    hdr_i = _find_first_nonempty_row(values)
    if hdr_i < 0:
        return []
    raw_headers = values[hdr_i]
    headers_norm = [_norm_header(h) for h in raw_headers]

    j_carrier = _col_index(headers_norm, ["carrier_code", "carrier_iata", "carrier", "airline_iata", "airline"])
    j_theme = _col_index(headers_norm, ["theme", "primary_theme"])
    j_dest = _col_index(headers_norm, ["destination_iata", "dest_iata", "destination", "to_iata"])
    j_weight = _col_index(headers_norm, ["bias_weight", "weight", "w", "score", "wt"])

    if j_carrier is None or j_theme is None or j_dest is None or j_weight is None:
        return []

    out: List[Dict[str, Any]] = []
    for row in values[hdr_i + 1 :]:
        if not any(str(c).strip() for c in row):
            continue
        c = (row[j_carrier] if j_carrier < len(row) else "").strip()
        t = (row[j_theme] if j_theme < len(row) else "").strip()
        d = (row[j_dest] if j_dest < len(row) else "").strip()
        w = (row[j_weight] if j_weight < len(row) else "").strip()
        if not c or not t or not d or not w:
            continue
        out.append({"carrier_code": c, "theme": t, "destination_iata": d, "bias_weight": w})
    return out


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

    DUFFEL_MAX_INSERTS = _env_int("DUFFEL_MAX_INSERTS", 50)
    DUFFEL_MAX_INSERTS_PER_ORIGIN = _env_int("DUFFEL_MAX_INSERTS_PER_ORIGIN", 15)
    DUFFEL_MAX_INSERTS_PER_ROUTE = _env_int("DUFFEL_MAX_INSERTS_PER_ROUTE", 5)
    DUFFEL_MAX_SEARCHES_PER_RUN = _env_int("DUFFEL_MAX_SEARCHES_PER_RUN", 12)
    DUFFEL_ROUTES_PER_RUN = _env_int("DUFFEL_ROUTES_PER_RUN", 4)
    ORIGINS_PER_DEST = _env_int("ORIGINS_PER_DEST", 3)

    # How many carriers to apply (Schema B) when CONFIG.included_airlines is empty
    BIAS_CARRIERS_PER_ROUTE = max(0, _env_int("BIAS_CARRIERS_PER_ROUTE", 2))

    log(
        f"CAPS: MAX_INSERTS={DUFFEL_MAX_INSERTS} | PER_ORIGIN={DUFFEL_MAX_INSERTS_PER_ORIGIN} | "
        f"PER_ROUTE={DUFFEL_MAX_INSERTS_PER_ROUTE} | MAX_SEARCHES={DUFFEL_MAX_SEARCHES_PER_RUN} | "
        f"DESTS_PER_RUN={DUFFEL_ROUTES_PER_RUN} | ORIGINS_PER_DEST={ORIGINS_PER_DEST}"
    )

    gc = _get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_raw = sh.worksheet(RAW_DEALS_TAB)
    ws_cfg = sh.worksheet(CONFIG_TAB)

    # ✅ FIX: allow either "ZTB" or legacy "ZONE_THEME_BENCHMARKS"
    try:
        ws_ztb = sh.worksheet(ZTB_TAB)
    except Exception:
        ws_ztb = sh.worksheet("ZONE_THEME_BENCHMARKS")

    ws_rcm = sh.worksheet(RCM_TAB)
    ws_iata = sh.worksheet(IATA_TAB)

    # -------------------------
    # CONFIG_CARRIER_BIAS (two schemas)
    # -------------------------
    carrier_origin_weight: Dict[str, Dict[str, float]] = {}  # Schema A
    dest_carrier_bias: Dict[Tuple[str, str], Dict[str, float]] = {}  # Schema B: (theme, dest)-> carrier->weight

    try:
        ws_bias = sh.worksheet(CONFIG_CARRIER_BIAS_TAB)
        values = ws_bias.get_all_values()

        rows_A = _load_bias_schema_A(values)
        rows_B = _load_bias_schema_B(values)

        # Build Schema A mapping
        for r in rows_A:
            c = str(r.get("carrier_iata") or "").strip().upper()
            o = str(r.get("origin_iata") or "").strip().upper()
            w = _safe_float(r.get("weight"), 0.0)
            if c and o and w > 0:
                carrier_origin_weight.setdefault(c, {})[o] = w

        # Build Schema B mapping
        for r in rows_B:
            c = str(r.get("carrier_code") or "").strip().upper()
            t = str(r.get("theme") or "").strip().lower()
            d = str(r.get("destination_iata") or "").strip().upper()
            w = _safe_float(r.get("bias_weight"), 0.0)
            if c and t and d and w > 0:
                dest_carrier_bias.setdefault((t, d), {})[c] = w

        if carrier_origin_weight or dest_carrier_bias:
            log(
                "✅ CONFIG_CARRIER_BIAS loaded: "
                f"tab={CONFIG_CARRIER_BIAS_TAB} | "
                f"schemaA_carriers={len(carrier_origin_weight)} | "
                f"schemaB_theme_dest_pairs={len(dest_carrier_bias)}"
            )
        else:
            # fall back to get_all_records just for diagnostics (but don't depend on its header assumptions)
            recs = ws_bias.get_all_records()
            hdrs = ws_bias.row_values(1)
            log(
                "⚠️ CONFIG_CARRIER_BIAS not usable (no recognized schema rows). "
                f"tab={CONFIG_CARRIER_BIAS_TAB} | row1_headers={hdrs} | get_all_records_rows={len(recs)}"
            )
    except Exception as e:
        try:
            tabs = [w.title for w in sh.worksheets()]
            log(f"⚠️ CONFIG_CARRIER_BIAS load failed for tab={CONFIG_CARRIER_BIAS_TAB}: {e}. Available tabs={tabs}")
        except Exception:
            log(f"⚠️ CONFIG_CARRIER_BIAS load failed for tab={CONFIG_CARRIER_BIAS_TAB}: {e}. (Could not list tabs)")

    if not (carrier_origin_weight or dest_carrier_bias):
        log("⚠️ CONFIG_CARRIER_BIAS not loaded or empty. Origin selection will use fallback pools.")

    # -------------------------
    # ZTB + theme of day
    # -------------------------
    ztb_rows_all = ws_ztb.get_all_records()
    eligible_themes = _eligible_themes_from_ztb(ztb_rows_all)
    theme_today = _theme_of_day(eligible_themes)

    log(f"✅ ZTB loaded: {len(ztb_rows_all)} rows | eligible_today={len(eligible_themes)} | pool={eligible_themes}")
    log(f"🎯 Theme of the day (UTC): {theme_today}")

    ztb_today = _ztb_row_for_theme(ztb_rows_all, theme_today) or {"connection_tolerance": "any"}
    ztb_start = _safe_int(ztb_today.get("start_mmdd"), 101)
    ztb_end = _safe_int(ztb_today.get("end_mmdd"), 1231)
    ztb_max_conn = _connections_from_tolerance(str(ztb_today.get("connection_tolerance") or "any"))

    # -------------------------
    # CONFIG attempts
    # -------------------------
    cfg_all = ws_cfg.get_all_records()
    cfg_active = [r for r in cfg_all if _is_true(r.get("active_in_feeder")) and _is_true(r.get("enabled"))]
    log(f"✅ CONFIG loaded: {len(cfg_active)} active rows (of {len(cfg_all)} total)")

    def cfg_theme(r: Dict[str, Any]) -> str:
        return (str(r.get("primary_theme") or "").strip() or str(r.get("audience_type") or "").strip())

    theme_rows = [r for r in cfg_active if cfg_theme(r).lower() == theme_today.lower()]
    if not theme_rows:
        fallback = "unexpected_value"
        log(f"⚠️ No CONFIG rows for theme={theme_today}. Falling back to {fallback}")
        theme_today = fallback
        ztb_today = _ztb_row_for_theme(ztb_rows_all, theme_today) or {"connection_tolerance": "any"}
        ztb_start = _safe_int(ztb_today.get("start_mmdd"), 101)
        ztb_end = _safe_int(ztb_today.get("end_mmdd"), 1231)
        ztb_max_conn = _connections_from_tolerance(str(ztb_today.get("connection_tolerance") or "any"))
        theme_rows = [r for r in cfg_active if cfg_theme(r).lower() == theme_today.lower()]

    theme_rows.sort(
        key=lambda r: (
            _safe_float(r.get("priority"), 0),
            _safe_float(r.get("search_weight"), 0),
            _safe_float(r.get("content_priority"), 0),
        ),
        reverse=True,
    )

    # Quotas
    DUFFEL_ROUTES_PER_RUN = max(1, DUFFEL_ROUTES_PER_RUN)
    COMMODITY_CAP = _env_int("COMMODITY_CAP", max(1, DUFFEL_ROUTES_PER_RUN // 3))
    MIN_EXOTIC = _env_int("MIN_EXOTIC", 1)

    def gw_type(r: Dict[str, Any]) -> str:
        return str(r.get("gateway_type") or "").strip().lower()

    non_comm = [r for r in theme_rows if gw_type(r) not in ("commodity", "")]
    comm = [r for r in theme_rows if gw_type(r) in ("commodity", "")]

    selected: List[Dict[str, Any]] = []
    for r in non_comm:
        if len(selected) >= DUFFEL_ROUTES_PER_RUN:
            break
        if len(selected) < MIN_EXOTIC:
            selected.append(r)

    comm_used = 0
    for r in (non_comm + comm):
        if len(selected) >= DUFFEL_ROUTES_PER_RUN:
            break
        if r in selected:
            continue
        if gw_type(r) in ("commodity", ""):
            if comm_used >= COMMODITY_CAP:
                continue
            comm_used += 1
        selected.append(r)

    theme_rows = selected
    log(
        f"🧭 Selected destinations to attempt: {len(theme_rows)} (cap DESTS_PER_RUN={DUFFEL_ROUTES_PER_RUN}) "
        f"| commodity_cap={COMMODITY_CAP} commodity_used={comm_used} min_exotic={MIN_EXOTIC}"
    )

    # -------------------------
    # Geo dictionary from RCM + IATA_MASTER
    # -------------------------
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
        code = str(r.get("iata_code") or "").strip().upper()
        city = str(r.get("city") or "").strip()
        country = str(r.get("country") or "").strip()
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

    def carriers_from_cfg(cfg_row: Dict[str, Any], theme: str, dest: str) -> List[str]:
        raw = str(cfg_row.get("included_airlines") or "").strip()
        if raw:
            return _csv_list(raw)

        # If CONFIG doesn't specify, Schema B can supply "allowed carriers" per theme+dest.
        if BIAS_CARRIERS_PER_ROUTE > 0 and dest_carrier_bias:
            key = (str(theme or "").strip().lower(), str(dest or "").strip().upper())
            cmap = dest_carrier_bias.get(key) or {}
            if cmap:
                ranked = sorted(cmap.items(), key=lambda kv: (kv[1], kv[0]), reverse=True)
                return [c for c, _w in ranked[:BIAS_CARRIERS_PER_ROUTE]]

        return []

    def choose_origins_for_dest(cfg_row: Dict[str, Any], dest: str, cap: int, theme: str) -> List[str]:
        is_long = str(cfg_row.get("is_long_haul") or "").strip().upper() == "TRUE"
        gw = str(cfg_row.get("gateway_type") or "").strip().lower()

        carriers = carriers_from_cfg(cfg_row, theme, dest)

        if is_long:
            candidate_pool = [o for o in hub_origins if o in origins_default] or origins_default[:]
        else:
            if gw in ("commodity", "value"):
                candidate_pool = [o for o in lcc_origins if o in origins_default] or origins_default[:]
            else:
                candidate_pool = origins_default[:]

        scores: Dict[str, float] = {o: 0.0 for o in candidate_pool}

        # Schema A: carriers -> origin weighting
        if carriers and carrier_origin_weight:
            for c in carriers:
                cmap = carrier_origin_weight.get(c)
                if not cmap:
                    continue
                for o, w in cmap.items():
                    if o in scores:
                        scores[o] += float(w)

        # Fallback base reality (unchanged)
        if all(v == 0.0 for v in scores.values()):
            if is_long:
                for o in hub_origins:
                    if o in scores:
                        scores[o] += 1.0
            elif gw in ("commodity", "value"):
                for o in lcc_origins:
                    if o in scores:
                        scores[o] += 0.5

        seed = f"{_today_utc().isoformat()}|{theme}|{dest}|{_env('RUN_SLOT','')}"
        h = int(hashlib.md5(seed.encode("utf-8")).hexdigest(), 16)
        ranked = sorted(candidate_pool, key=lambda o: (scores.get(o, 0.0), o), reverse=True)
        if ranked:
            rot = h % len(ranked)
            ranked = ranked[rot:] + ranked[:rot]
        return ranked[:cap]

    searches = 0

    for cfg in theme_rows:
        if searches >= DUFFEL_MAX_SEARCHES_PER_RUN:
            break

        dest = str(cfg.get("destination_iata") or "").strip().upper()
        if not dest:
            continue
        if not geo_for(dest):
            log(f"⚠️ Missing geo for destination_iata={dest}. Skipping (no invented geo).")
            continue

        min_d = _safe_int(cfg.get("days_ahead_min"), 7)
        max_d = _safe_int(cfg.get("days_ahead_max"), 120)
        trip_len = _safe_int(cfg.get("trip_length_days"), 5)

        out_dates = _candidate_outbounds_seasonal(min_d, max_d, trip_len, ztb_start, ztb_end, n=3)
        if not out_dates:
            log(f"⚠️ No in-season outbound dates available for dest={dest}. Skipping.")
            continue

        cfg_max_conn = _safe_int(cfg.get("max_connections"), 2)
        max_conn = min(cfg_max_conn, ztb_max_conn)

        cabin = str(cfg.get("cabin_class") or "economy").strip().lower() or "economy"
        included_airlines = carriers_from_cfg(cfg, theme_today, dest)

        origins = choose_origins_for_dest(cfg, dest, ORIGINS_PER_DEST, theme_today)

        for origin in origins:
            if searches >= DUFFEL_MAX_SEARCHES_PER_RUN:
                break
            if not geo_for(origin):
                continue
            if not can_insert(origin, dest):
                continue

            for od in out_dates:
                if searches >= DUFFEL_MAX_SEARCHES_PER_RUN:
                    break

                rd = od + dt.timedelta(days=trip_len)

                resp = _duffel_search(origin, dest, od, rd, max_conn, cabin, included_airlines)
                searches += 1
                if not resp:
                    continue

                price = _min_price_gbp(resp)
                if price is None:
                    continue

                row = [""] * len(headers)

                def set_if(col: str, val: Any) -> None:
                    j = idx.get(col)
                    if j is not None:
                        row[j] = val

                oc, ok = geo_for(origin) or ("", "")
                dc, dk = geo_for(dest) or ("", "")

                set_if("status", "NEW")
                set_if("origin_iata", origin)
                set_if("destination_iata", dest)
                set_if("origin_city", oc)
                set_if("origin_country", ok)
                set_if("destination_city", dc)
                set_if("destination_country", dk)
                set_if("outbound_date", od.isoformat())
                set_if("return_date", rd.isoformat())
                set_if("price_gbp", float(price))
                set_if("theme", theme_today)
                set_if("primary_theme", cfg_theme(cfg))
                set_if("ingested_at", dt.datetime.utcnow().isoformat() + "Z")
                set_if("source", "duffel")
                set_if("max_connections", max_conn)
                set_if("cabin_class", cabin)
                set_if("included_airlines", ",".join(included_airlines) if included_airlines else "")

                deals_out.append(row)
                inserted_by_origin[origin] = inserted_by_origin.get(origin, 0) + 1
                inserted_by_route[(origin, dest)] = inserted_by_route.get((origin, dest), 0) + 1

                if len(deals_out) >= DUFFEL_MAX_INSERTS:
                    break

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
