# workers/pipeline_worker.py
# FULL FILE REPLACEMENT — TravelTxter V5 Feeder
#
# Targeted fix: populate RAW_DEALS using the ACTUAL RD headers:
# status, deal_id, price_gbp, origin_city, origin_iata, destination_country, destination_city,
# destination_iata, outbound_date, return_date, stops, deal_theme, ingested_at_utc, ...
#
# Also:
# - Avoid gspread get_all_records() header uniqueness crash for CONFIG by using safe values reader
# - Use IATA_MASTER only for geo enrichment (no RCM geo dependency)
# - Respect PM direct-only (RCM connection_type gate)
# - Deterministic + capped (MAX_SEARCHES etc.)
# - Minimal dedupe by deal_id

from __future__ import annotations

import os
import json
import math
import random
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple, Set

import gspread
import requests


# -------------------------
# Logging
# -------------------------
def log(msg: str) -> None:
    ts = dt.datetime.utcnow().isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


# -------------------------
# Env / tabs
# -------------------------
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID") or os.environ.get("SHEET_ID") or ""
RAW_DEALS_TAB = os.environ.get("RAW_DEALS_TAB") or "RAW_DEALS"
CONFIG_TAB = os.environ.get("CONFIG_TAB") or "CONFIG"
ROUTE_CAPABILITY_MAP_TAB = os.environ.get("ROUTE_CAPABILITY_MAP_TAB") or "ROUTE_CAPABILITY_MAP"
IATA_MASTER_TAB = os.environ.get("IATA_MASTER_TAB") or "IATA_MASTER"
OPS_MASTER_TAB = os.environ.get("OPS_MASTER_TAB") or "OPS_MASTER"
ZONE_THEME_BENCHMARKS_TAB = os.environ.get("ZONE_THEME_BENCHMARKS_TAB") or "ZONE_THEME_BENCHMARKS"
CONFIG_CARRIER_BIAS_TAB = os.environ.get("CONFIG_CARRIER_BIAS_TAB") or "CONFIG_CARRIER_BIAS"

# Caps
MAX_INSERTS = int(os.environ.get("MAX_INSERTS") or "50")
MAX_SEARCHES = int(os.environ.get("MAX_SEARCHES") or "12")
DESTS_PER_RUN = int(os.environ.get("DESTS_PER_RUN") or "4")
ORIGINS_PER_DEST = int(os.environ.get("ORIGINS_PER_DEST") or "3")
K_DATES_PER_DEST = int(os.environ.get("K_DATES_PER_DEST") or "3")

PER_ORIGIN = int(os.environ.get("PER_ORIGIN") or "15")
PER_ROUTE = int(os.environ.get("PER_ROUTE") or "5")

RUN_SLOT = (os.environ.get("RUN_SLOT") or "").strip().upper()  # AM/PM or blank
SLOT_SPLIT = os.environ.get("SLOT_SPLIT") or "90/10"

# Duffel
DUFFEL_API_KEY = os.environ.get("DUFFEL_API_KEY") or ""
DUFFEL_VERSION = os.environ.get("DUFFEL_VERSION") or "v2"
DUFFEL_API_BASE = "https://api.duffel.com"

# Sheets auth
GCP_SA_JSON = os.environ.get("GCP_SA_JSON") or ""
GCP_SA_JSON_ONE_LINE = os.environ.get("GCP_SA_JSON_ONE_LINE") or ""


# -------------------------
# Helpers
# -------------------------
def _norm(v: Any) -> str:
    return str(v or "").strip()


def _norm_iata(v: Any) -> str:
    return _norm(v).upper()


def _is_true(v: Any) -> bool:
    s = _norm(v).lower()
    return s in ("true", "1", "yes", "y", "on")


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, (int, float)):
            return int(v)
        s = _norm(v)
        if s == "":
            return default
        return int(float(s))
    except Exception:
        return default


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if isinstance(v, bool):
            return float(int(v))
        if isinstance(v, (int, float)):
            return float(v)
        s = _norm(v)
        if s == "":
            return default
        return float(s)
    except Exception:
        return default


def _csv_list(v: Any) -> List[str]:
    s = _norm(v)
    if not s:
        return []
    return [x.strip().upper() for x in s.split(",") if x.strip()]


def _utc_now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _date_iso(d: dt.date) -> str:
    return d.isoformat()


def _parse_money(amount_str: str) -> float:
    # Duffel returns amount as string like "123.45"
    try:
        return float(str(amount_str).strip())
    except Exception:
        return 0.0


def _gbp(amount: float) -> int:
    # round up to nearest pound for price_gbp
    return int(math.ceil(amount))


def _deal_id(origin: str, dest: str, out_date: str, ret_date: str, price_gbp: int, stops: int, cabin: str) -> str:
    # deterministic ID
    return f"{origin}-{dest}-{out_date}-{ret_date}-GBP{price_gbp}-S{stops}-{cabin}".upper()


def _service_account_dict() -> Dict[str, Any]:
    if GCP_SA_JSON:
        return json.loads(GCP_SA_JSON)
    if GCP_SA_JSON_ONE_LINE:
        return json.loads(GCP_SA_JSON_ONE_LINE)
    raise RuntimeError("Missing GCP service account JSON (GCP_SA_JSON or GCP_SA_JSON_ONE_LINE)")


def _open_sheet() -> gspread.Spreadsheet:
    sa = _service_account_dict()
    gc = gspread.service_account_from_dict(sa)
    return gc.open_by_key(SPREADSHEET_ID)


def _get_all_records_safe(ws, *, label: str = "") -> List[Dict[str, Any]]:
    """
    Robust sheet->records reader that does NOT fail on non-unique headers.
    We only use this for CONFIG (the known pain point).
    """
    try:
        return ws.get_all_records() or []
    except Exception as e:
        log(f"⚠️ {label} get_all_records failed ({type(e).__name__}: {e}). Falling back to get_all_values().")
        values = ws.get_all_values() or []
        if not values:
            return []
        raw_headers = [str(h).strip() for h in values[0]]

        # Make unique keys for duplicates/blanks
        seen: Dict[str, int] = {}
        headers: List[str] = []
        for h in raw_headers:
            base = h if h else "__blank__"
            if base in seen:
                seen[base] += 1
                headers.append(f"{base}__{seen[base]}")
            else:
                seen[base] = 1
                headers.append(base)

        rows: List[Dict[str, Any]] = []
        for r in values[1:]:
            if not any(str(x).strip() for x in r):
                continue
            d: Dict[str, Any] = {}
            for i, k in enumerate(headers):
                if i < len(r):
                    d[k] = r[i]
            rows.append(d)
        return rows


# -------------------------
# Duffel
# -------------------------
def _duffel_headers() -> Dict[str, str]:
    h = {
        "Accept": "application/json",
        "Duffel-Version": DUFFEL_VERSION,
    }
    if DUFFEL_API_KEY:
        h["Authorization"] = f"Bearer {DUFFEL_API_KEY}"
    return h


def _duffel_offer_request(origin: str, dest: str, out_date: str, ret_date: str,
                          cabin: str, airlines: List[str]) -> Optional[Dict[str, Any]]:
    payload: Dict[str, Any] = {
        "data": {
            "slices": [
                {"origin": origin, "destination": dest, "departure_date": out_date},
                {"origin": dest, "destination": origin, "departure_date": ret_date},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": cabin,
        }
    }
    if airlines:
        payload["data"]["allowed_carrier_codes"] = airlines

    try:
        r = requests.post(
            f"{DUFFEL_API_BASE}/air/offer_requests",
            headers=_duffel_headers(),
            json=payload,
            timeout=60,
        )
        if r.status_code >= 400:
            return None
        return r.json()
    except Exception:
        return None


def _extract_best_offer(offer_request_json: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Return a flattened "best offer" dict from Duffel offer_request response.
    We pick the lowest total_amount offer if present.
    """
    try:
        offers = offer_request_json.get("data", {}).get("offers", []) or []
        if not offers:
            return None

        def offer_key(o: Dict[str, Any]) -> float:
            return _parse_money(o.get("total_amount", "0") or "0")

        best = min(offers, key=offer_key)
        return best
    except Exception:
        return None


def _offer_durations_minutes(best_offer: Dict[str, Any]) -> Tuple[int, int]:
    """
    Try to compute outbound/inbound durations (minutes) from slices.
    Duffel offers often include slices with segments times; we compute from departure/arrival.
    If not possible, return (0,0).
    """
    try:
        slices = best_offer.get("slices", []) or []
        if len(slices) < 2:
            return 0, 0

        def duration_for_slice(slc: Dict[str, Any]) -> int:
            segs = slc.get("segments", []) or []
            if not segs:
                return 0
            dep = segs[0].get("departing_at")
            arr = segs[-1].get("arriving_at")
            if not dep or not arr:
                return 0
            # ISO timestamps
            dep_dt = dt.datetime.fromisoformat(dep.replace("Z", "+00:00"))
            arr_dt = dt.datetime.fromisoformat(arr.replace("Z", "+00:00"))
            mins = int((arr_dt - dep_dt).total_seconds() // 60)
            return max(mins, 0)

        out_m = duration_for_slice(slices[0])
        in_m = duration_for_slice(slices[1])
        return out_m, in_m
    except Exception:
        return 0, 0


def _offer_stops(best_offer: Dict[str, Any]) -> int:
    """
    Stops: max(segments per slice - 1) across slices.
    """
    try:
        stops = 0
        for slc in best_offer.get("slices", []) or []:
            segs = slc.get("segments", []) or []
            stops = max(stops, max(len(segs) - 1, 0))
        return stops
    except Exception:
        return 0


def _offer_carriers(best_offer: Dict[str, Any]) -> str:
    """
    Return comma-separated unique carrier codes from segments.
    """
    try:
        carriers: List[str] = []
        for slc in best_offer.get("slices", []) or []:
            for seg in slc.get("segments", []) or []:
                c = seg.get("marketing_carrier", {}).get("iata_code") or seg.get("operating_carrier", {}).get("iata_code")
                c = _norm_iata(c)
                if c and c not in carriers:
                    carriers.append(c)
        return ", ".join(carriers)
    except Exception:
        return ""


# -------------------------
# Main
# -------------------------
def main() -> int:
    log("==============================================================================")
    log("TRAVELTXTTER PIPELINE WORKER (FEEDER) START")
    log("==============================================================================")

    # Run slot
    slot = RUN_SLOT
    if slot not in ("AM", "PM"):
        slot = "AM" if dt.datetime.utcnow().hour < 12 else "PM"
    want_long = slot == "AM"

    log(
        f"CAPS: MAX_INSERTS={MAX_INSERTS} | PER_ORIGIN={PER_ORIGIN} | PER_ROUTE={PER_ROUTE} | "
        f"MAX_SEARCHES={MAX_SEARCHES} | DESTS_PER_RUN={DESTS_PER_RUN} | ORIGINS_PER_DEST={ORIGINS_PER_DEST} | "
        f"RUN_SLOT={slot} | SLOT_SPLIT={SLOT_SPLIT} | K_DATES_PER_DEST={K_DATES_PER_DEST}"
    )

    sh = _open_sheet()
    ws_raw = sh.worksheet(RAW_DEALS_TAB)
    ws_cfg = sh.worksheet(CONFIG_TAB)
    ws_rcm = sh.worksheet(ROUTE_CAPABILITY_MAP_TAB)
    ws_iata = sh.worksheet(IATA_MASTER_TAB)

    # Optional worksheets
    ws_ops = None
    try:
        ws_ops = sh.worksheet(OPS_MASTER_TAB)
    except Exception:
        ws_ops = None

    # Carrier bias (informational continuity only)
    try:
        ws_bias = sh.worksheet(CONFIG_CARRIER_BIAS_TAB)
        bias_rows = ws_bias.get_all_records() or []
        usable = sum(1 for r in bias_rows if _norm(r.get("theme")) and _norm(r.get("carrier_code")))
        log(f"✅ CONFIG_CARRIER_BIAS loaded: {usable} usable rows")
    except Exception:
        log("⚠️ CONFIG_CARRIER_BIAS not loaded (ok)")

    # Theme authority: OPS_MASTER!B5
    theme = None
    if ws_ops:
        try:
            theme = _norm(ws_ops.acell("B5").value)
            if theme:
                log(f"✅ Theme read from OPS_MASTER!B5: {theme}")
        except Exception:
            theme = None
    if not theme:
        # deterministic fallback (should rarely be used in your system)
        theme = "unexpected_value"
        log(f"⚠️ Theme fallback used: {theme}")

    log(f"🎯 Theme of the day (UTC): {theme}")

    # Build geo dictionary from IATA_MASTER (authoritative)
    iata_geo: Dict[str, Tuple[str, str]] = {}
    for r in ws_iata.get_all_records() or []:
        code = _norm_iata(r.get("iata_code") or r.get("iata") or r.get("IATA"))
        city = _norm(r.get("city"))
        country = _norm(r.get("country"))
        if code:
            iata_geo[code] = (city, country)
    log(f"✅ Geo dictionary loaded: {len(iata_geo)} IATA entries (IATA_MASTER only)")

    # Load CONFIG safely (avoid header uniqueness crash)
    cfg_all = _get_all_records_safe(ws_cfg, label="CONFIG")
    # Only rows enabled + active_in_feeder
    cfg_active = [r for r in cfg_all if _is_true(r.get("enabled")) and _is_true(r.get("active_in_feeder"))]
    log(f"✅ CONFIG loaded: {len(cfg_active)} active rows (of {len(cfg_all)} total)")

    # Load RCM and build allowed origins by destination subject to slot rules
    rcm_rows = ws_rcm.get_all_records() or []
    origins_by_dest: Dict[str, List[Tuple[str, str, str]]] = {}
    # tuple: (origin_iata, connection_type, via_hub)
    enabled_routes = 0
    for r in rcm_rows:
        if not _is_true(r.get("enabled")):
            continue
        origin = _norm_iata(r.get("origin_iata"))
        dest = _norm_iata(r.get("destination_iata"))
        if not origin or not dest:
            continue
        conn = _norm(r.get("connection_type")).lower()
        via_hub = _norm_iata(r.get("via_hub")) if conn == "via_hub" else ""
        # PM direct-only
        if slot == "PM" and conn != "direct":
            continue
        origins_by_dest.setdefault(dest, [])
        origins_by_dest[dest].append((origin, conn, via_hub))
        enabled_routes += 1
    log(f"✅ ROUTE_CAPABILITY_MAP loaded: {enabled_routes} enabled routes")

    # Candidate destinations from CONFIG for today theme + slot + longhaul match
    candidates: Dict[str, Dict[str, Any]] = {}
    weights: Dict[str, float] = {}
    for r in cfg_active:
        if _norm(r.get("primary_theme")).lower() != theme.lower():
            continue
        if _norm(r.get("slot_hint")).upper() != slot:
            continue
        is_lh = _is_true(r.get("is_long_haul"))
        if is_lh != want_long:
            continue
        dest = _norm_iata(r.get("destination_iata"))
        if not dest:
            continue
        if dest not in iata_geo:
            continue
        candidates[dest] = r
        weights[dest] = max(weights.get(dest, 0.0), _safe_float(r.get("search_weight"), 1.0))

    if not candidates:
        log("⚠️ Gate 1 fail: no eligible CONFIG destinations for theme/slot/haul")
        return 0

    # Choose destinations (weighted, deterministic)
    rnd = random.Random(f"{theme}:{slot}:{_date_iso(dt.datetime.utcnow().date())}")
    dest_pool = list(candidates.keys())
    chosen: List[str] = []
    while dest_pool and len(chosen) < DESTS_PER_RUN:
        total = sum(weights.get(d, 1.0) for d in dest_pool) or 1.0
        pick = rnd.random() * total
        acc = 0.0
        sel = dest_pool[0]
        for d in dest_pool:
            acc += weights.get(d, 1.0)
            if acc >= pick:
                sel = d
                break
        chosen.append(sel)
        dest_pool.remove(sel)

    log(f"PLAN: intended_routes={len(chosen)} | dates_per_dest(K)={K_DATES_PER_DEST} | max_searches={MAX_SEARCHES}")

    # Build date candidates (within CONFIG window)
    today = dt.datetime.utcnow().date()

    # Prepare de-dupe set from existing deal_id values
    rd_hdr = ws_raw.row_values(1)
    rd_idx = {h: i for i, h in enumerate(rd_hdr)}
    deal_id_col = rd_idx.get("deal_id", None)
    existing_ids: Set[str] = set()
    if deal_id_col is not None:
        # pull a limited range to avoid huge reads
        col_vals = ws_raw.col_values(deal_id_col + 1)  # 1-indexed
        for v in col_vals[1:]:
            vv = _norm(v)
            if vv:
                existing_ids.add(vv.upper())

    searches = 0
    duffel_calls = 0
    inserted_rows: List[List[Any]] = []

    for dest in chosen:
        if searches >= MAX_SEARCHES:
            break

        cfg = candidates[dest]
        # Origins available for dest from RCM
        origin_rows = origins_by_dest.get(dest, [])
        if not origin_rows:
            continue

        # Deterministic origin selection
        origin_rows = sorted(origin_rows, key=lambda x: (x[0], x[1], x[2]))
        origin_rows = origin_rows[:ORIGINS_PER_DEST]

        cabin = (_norm(cfg.get("cabin_class")) or "economy").lower()
        trip_len = _safe_int(cfg.get("trip_length_days"), 7)
        days_min = _safe_int(cfg.get("days_ahead_min"), 14)
        days_max = _safe_int(cfg.get("days_ahead_max"), 120)
        days_max = max(days_max, days_min)

        airlines = _csv_list(cfg.get("included_airlines"))

        # K dates spread across [min,max]
        date_offsets: List[int] = []
        if K_DATES_PER_DEST <= 1:
            date_offsets = [days_min]
        else:
            span = max(days_max - days_min, 0)
            for k in range(K_DATES_PER_DEST):
                frac = k / (K_DATES_PER_DEST - 1)
                date_offsets.append(days_min + int(round(span * frac)))

        for (origin, conn_type, via_hub) in origin_rows:
            if searches >= MAX_SEARCHES:
                break

            for off in date_offsets:
                if searches >= MAX_SEARCHES:
                    break

                out_d = today + dt.timedelta(days=off)
                ret_d = out_d + dt.timedelta(days=trip_len)
                out_date = _date_iso(out_d)
                ret_date = _date_iso(ret_d)

                searches += 1
                if not DUFFEL_API_KEY:
                    continue

                duffel_calls += 1
                offer_req = _duffel_offer_request(origin, dest, out_date, ret_date, cabin, airlines)
                if not offer_req:
                    continue

                best = _extract_best_offer(offer_req)
                if not best:
                    continue

                currency = _norm(best.get("total_currency")) or "GBP"
                amt = _parse_money(best.get("total_amount") or "0")
                price_gbp = _gbp(amt) if currency.upper() == "GBP" else _gbp(amt)  # keep numeric; convert later if needed

                stops = _offer_stops(best)
                carriers = _offer_carriers(best)
                out_mins, in_mins = _offer_durations_minutes(best)
                total_hours = 0.0
                if out_mins and in_mins:
                    total_hours = round((out_mins + in_mins) / 60.0, 2)

                did = _deal_id(origin, dest, out_date, ret_date, price_gbp, stops, cabin)
                if did in existing_ids:
                    continue  # de-dupe

                existing_ids.add(did)

                # Geo
                o_city, o_country = iata_geo.get(origin, ("", ""))
                d_city, d_country = iata_geo.get(dest, ("", ""))

                now_iso = _utc_now_iso()

                # Build a row aligned to RAW_DEALS headers
                row = [""] * len(rd_hdr)

                def put(col: str, val: Any) -> None:
                    idx = rd_idx.get(col)
                    if idx is not None and idx < len(row):
                        row[idx] = val

                # Mandatory/core
                put("status", "NEW")
                put("deal_id", did)
                put("price_gbp", price_gbp)
                put("currency", currency.upper())

                put("origin_city", o_city)
                put("origin_country", o_country)
                put("origin_iata", origin)

                put("destination_city", d_city)
                put("destination_country", d_country)
                put("destination_iata", dest)

                put("outbound_date", out_date)
                put("return_date", ret_date)

                put("stops", stops)
                put("trip_length_days", trip_len)

                # Theme fields (you have both)
                put("deal_theme", theme)
                put("theme", theme)

                # Timing / lifecycle timestamps
                put("ingested_at_utc", now_iso)
                put("created_utc", now_iso)
                put("timestamp", now_iso)
                put("created_at", now_iso)

                # Connection info (from RCM)
                put("connection_type", conn_type)
                put("via_hub", via_hub)

                # Durations
                put("outbound_duration_minutes", out_mins or "")
                put("inbound_duration_minutes", in_mins or "")
                put("total_duration_hours", total_hours or "")

                # Carriers (best effort)
                put("carriers", carriers)

                # Cabin
                put("cabin_class", cabin)

                # Keep the run bounded
                inserted_rows.append(row)
                if len(inserted_rows) >= MAX_INSERTS:
                    break

            if len(inserted_rows) >= MAX_INSERTS:
                break

        if len(inserted_rows) >= MAX_INSERTS:
            break

    log(f"✓ Searches completed: {searches}")
    log(f"✓ Duffel calls made: {duffel_calls} (cap {MAX_SEARCHES})")
    log(f"✓ Deals collected: {len(inserted_rows)} (cap {MAX_INSERTS})")

    if not inserted_rows:
        log("⚠️ No rows to insert (no winners)")
        return 0

    ws_raw.append_rows(inserted_rows, value_input_option="USER_ENTERED")
    log(f"✅ Inserted {len(inserted_rows)} rows into {RAW_DEALS_TAB}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
