# workers/pipeline_worker.py
# FULL FILE REPLACEMENT — TravelTxter V5 Feeder (hotfix: import random)

from __future__ import annotations

import os
import sys
import json
import hashlib
import datetime as dt
import math
import re
import random  # ✅ HOTFIX: required for random.Random(...)
from typing import Any, Dict, List, Optional, Tuple, Set

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


def _get_all_records_safe(ws, label: str) -> List[Dict[str, Any]]:
    """Read worksheet records without failing on non-unique headers."""
    try:
        return ws.get_all_records() or []
    except Exception as e:
        log(f"⚠️ {label}: get_all_records failed ({type(e).__name__}: {e}). Falling back to get_all_values().")
        values = ws.get_all_values() or []
        if not values:
            return []
        raw_headers = [str(h).strip() for h in values[0]]
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
            for idx, key in enumerate(headers):
                if idx < len(r):
                    d[key] = r[idx]
            rows.append(d)
        return rows


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
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:12]


def _norm_iata(x: Any) -> str:
    return str(x or "").strip().upper()


def _to_iso_utc(ts: Optional[dt.datetime] = None) -> str:
    if ts is None:
        ts = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    return ts.isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse_iso(s: str) -> Optional[dt.datetime]:
    try:
        if not s:
            return None
        s = s.replace("Z", "+00:00")
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None


def _minutes_between(a: str, b: str) -> int:
    da = _parse_iso(a)
    db = _parse_iso(b)
    if not da or not db:
        return 0
    return max(0, int((db - da).total_seconds() // 60))


def _offer_stops(offer: Dict[str, Any]) -> int:
    try:
        slices = offer.get("slices") or []
        stops = 0
        for slc in slices:
            segs = slc.get("segments") or []
            stops = max(stops, max(0, len(segs) - 1))
        return stops
    except Exception:
        return 0


def _offer_carriers(offer: Dict[str, Any]) -> str:
    carriers: List[str] = []
    try:
        for slc in (offer.get("slices") or []):
            for seg in (slc.get("segments") or []):
                c = (
                    (seg.get("marketing_carrier") or {}).get("iata_code")
                    or (seg.get("operating_carrier") or {}).get("iata_code")
                )
                c = _norm_iata(c)
                if c and c not in carriers:
                    carriers.append(c)
    except Exception:
        pass
    return ", ".join(carriers)


def _offer_durations(offer: Dict[str, Any]) -> Tuple[int, int, float]:
    out_m = 0
    in_m = 0
    try:
        slices = offer.get("slices") or []
        if len(slices) >= 1:
            segs = slices[0].get("segments") or []
            if segs:
                out_m = _minutes_between(segs[0].get("departing_at", ""), segs[-1].get("arriving_at", ""))
        if len(slices) >= 2:
            segs = slices[1].get("segments") or []
            if segs:
                in_m = _minutes_between(segs[0].get("departing_at", ""), segs[-1].get("arriving_at", ""))
    except Exception:
        pass
    total_h = 0.0
    if out_m or in_m:
        total_h = round((out_m + in_m) / 60.0, 2)
    return out_m, in_m, total_h


def _best_offer(offers: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not offers:
        return None

    def key(o: Dict[str, Any]) -> float:
        try:
            return float(str(o.get("total_amount") or "0").strip())
        except Exception:
            return 0.0

    return min(offers, key=key)


def _sa_creds_from_env() -> Credentials:
    sa_json = _env("GCP_SA_JSON_ONE_LINE") or _env("GCP_SA_JSON")
    if not sa_json:
        raise RuntimeError("Missing GCP_SA_JSON / GCP_SA_JSON_ONE_LINE")
    info = json.loads(sa_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    return Credentials.from_service_account_info(info, scopes=scopes)


def _gs_client() -> gspread.Client:
    return gspread.authorize(_sa_creds_from_env())


def _open_sheet(gc: gspread.Client) -> gspread.Spreadsheet:
    sid = _env("SPREADSHEET_ID") or _env("SHEET_ID")
    if not sid:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")
    return gc.open_by_key(sid)


def _ws(sh: gspread.Spreadsheet, name: str) -> gspread.Worksheet:
    return sh.worksheet(name)


def _read_headers(ws: gspread.Worksheet) -> List[str]:
    return [str(x or "").strip() for x in ws.row_values(1)]


def _append_rows(ws: gspread.Worksheet, rows: List[List[Any]]) -> None:
    if rows:
        ws.append_rows(rows, value_input_option="USER_ENTERED")


def _http_post(url: str, headers: Dict[str, str], payload: Dict[str, Any], timeout: int = 45) -> Optional[Dict[str, Any]]:
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=timeout)
        if r.status_code >= 400:
            return None
        return r.json()
    except Exception:
        return None


def _duffel_headers() -> Dict[str, str]:
    v = _env("DUFFEL_VERSION", "v2")
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {_env('DUFFEL_API_KEY')}",
        "Duffel-Version": v,
    }


def _duffel_offer_request(origin: str, dest: str, out_date: str, in_date: str, cabin: str, airlines: List[str]) -> Optional[List[Dict[str, Any]]]:
    url = "https://api.duffel.com/air/offer_requests"
    data: Dict[str, Any] = {
        "data": {
            "slices": [
                {"origin": origin, "destination": dest, "departure_date": out_date},
                {"origin": dest, "destination": origin, "departure_date": in_date},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": cabin,
        }
    }
    if airlines:
        data["data"]["allowed_carrier_codes"] = airlines
    resp = _http_post(url, _duffel_headers(), data, timeout=60)
    if not resp:
        return None
    return (resp.get("data") or {}).get("offers") or []


def _trip_len_days(out_date: str, in_date: str) -> int:
    try:
        d1 = dt.date.fromisoformat(out_date)
        d2 = dt.date.fromisoformat(in_date)
        return max(0, (d2 - d1).days)
    except Exception:
        return 0


def main() -> int:
    log("==============================================================================")
    log("TRAVELTXTTER PIPELINE WORKER (FEEDER) START")
    log("==============================================================================")

    run_slot = _env("RUN_SLOT", "").upper()
    if run_slot not in ("AM", "PM"):
        run_slot = "AM" if dt.datetime.utcnow().hour < 12 else "PM"
    want_long = run_slot == "AM"

    max_inserts = _env_int("MAX_INSERTS", 50)
    per_origin = _env_int("PER_ORIGIN", 15)
    per_route = _env_int("PER_ROUTE", 5)
    max_searches = _env_int("MAX_SEARCHES", 12)
    dests_per_run = _env_int("DESTS_PER_RUN", 4)
    origins_per_dest = _env_int("ORIGINS_PER_DEST", 3)
    slot_split = _env("SLOT_SPLIT", "90/10")
    k_dates_per_dest = _env_int("K_DATES_PER_DEST", 3)

    log(
        f"CAPS: MAX_INSERTS={max_inserts} | PER_ORIGIN={per_origin} | PER_ROUTE={per_route} | "
        f"MAX_SEARCHES={max_searches} | DESTS_PER_RUN={dests_per_run} | ORIGINS_PER_DEST={origins_per_dest} | "
        f"RUN_SLOT={run_slot} | SLOT_SPLIT={slot_split} | K_DATES_PER_DEST={k_dates_per_dest}"
    )

    gc = _gs_client()
    sh = _open_sheet(gc)

    ws_raw = _ws(sh, _env("RAW_DEALS_TAB", "RAW_DEALS"))
    ws_cfg = _ws(sh, _env("CONFIG_TAB", "CONFIG"))
    ws_rcm = _ws(sh, _env("ROUTE_CAPABILITY_MAP_TAB", "ROUTE_CAPABILITY_MAP"))
    ws_iata = _ws(sh, _env("IATA_MASTER_TAB", "IATA_MASTER"))
    ws_ztb = _ws(sh, _env("ZONE_THEME_BENCHMARKS_TAB", "ZONE_THEME_BENCHMARKS"))

    # Bias (optional)
    try:
        ws_bias = _ws(sh, _env("CONFIG_CARRIER_BIAS_TAB", "CONFIG_CARRIER_BIAS"))
        bias_rows = ws_bias.get_all_records() or []
        usable_bias = sum(1 for r in bias_rows if str(r.get("carrier_code") or "").strip())
        log(f"✅ CONFIG_CARRIER_BIAS loaded: {usable_bias} usable rows")
    except Exception:
        log("⚠️ CONFIG_CARRIER_BIAS not loaded (ok)")

    # Theme from OPS_MASTER!B5
    theme = ""
    try:
        ws_ops = _ws(sh, _env("OPS_MASTER_TAB", "OPS_MASTER"))
        theme = str(ws_ops.acell("B5").value or "").strip()
        if theme:
            log(f"✅ Theme read from OPS_MASTER!B5: {theme}")
    except Exception:
        theme = ""

    ztb_rows = ws_ztb.get_all_records() or []
    eligible_today = _eligible_themes_from_ztb(ztb_rows)
    log(f"✅ ZTB loaded: {len(ztb_rows)} rows | eligible_today={len(eligible_today)} | pool={eligible_today}")

    if not theme:
        theme = _theme_of_day(eligible_today)
    log(f"🎯 Theme of the day (UTC): {theme}")

    # CONFIG (safe)
    cfg_all = _get_all_records_safe(ws_cfg, "CONFIG")
    cfg_active = [r for r in cfg_all if _is_true(r.get("active_in_feeder")) and _is_true(r.get("enabled"))]
    log(f"✅ CONFIG loaded: {len(cfg_active)} active rows (of {len(cfg_all)} total)")

    # RCM gate
    rcm_rows = ws_rcm.get_all_records() or []
    enabled_routes = 0
    origins_by_dest: Dict[str, List[Tuple[str, str, str]]] = {}
    for r in rcm_rows:
        if not _is_true(r.get("enabled")):
            continue
        o = _norm_iata(r.get("origin_iata"))
        d = _norm_iata(r.get("destination_iata"))
        if not o or not d:
            continue
        conn = str(r.get("connection_type") or "").strip().lower()
        via = _norm_iata(r.get("via_hub")) if conn == "via_hub" else ""
        if run_slot == "PM" and conn != "direct":
            continue
        origins_by_dest.setdefault(d, [])
        origins_by_dest[d].append((o, conn, via))
        enabled_routes += 1
    log(f"✅ ROUTE_CAPABILITY_MAP loaded: {enabled_routes} enabled routes")

    # IATA geo
    iata_rows = ws_iata.get_all_records() or []
    geo: Dict[str, Tuple[str, str]] = {}
    for r in iata_rows:
        code = _norm_iata(r.get("iata") or r.get("iata_code") or r.get("IATA"))
        if not code:
            continue
        city = str(r.get("city") or "").strip()
        country = str(r.get("country") or "").strip()
        geo[code] = (city, country)
    log(f"✅ Geo dictionary loaded: {len(geo)} IATA entries (IATA_MASTER only)")

    # Build candidate dests from CONFIG
    candidates: Dict[str, Dict[str, Any]] = {}
    weights: Dict[str, float] = {}
    for r in cfg_active:
        if str(r.get("primary_theme") or "").strip().lower() != theme.lower():
            continue
        if str(r.get("slot_hint") or "").strip().upper() != run_slot:
            continue
        is_lh = _is_true(r.get("is_long_haul"))
        if is_lh != want_long:
            continue
        dest = _norm_iata(r.get("destination_iata"))
        if not dest or dest not in geo:
            continue
        candidates[dest] = r
        weights[dest] = max(weights.get(dest, 0.0), _safe_float(r.get("search_weight"), 1.0))

    if not candidates:
        log("⚠️ No eligible destinations from CONFIG (Gate 1 fail)")
        return 0

    # ✅ This was crashing before because random wasn't imported
    rnd = random.Random(f"{theme}:{run_slot}:{_today_utc().isoformat()}")

    # choose destinations (weighted)
    pool = list(candidates.keys())
    chosen: List[str] = []
    while pool and len(chosen) < dests_per_run:
        total = sum(weights.get(d, 1.0) for d in pool) or 1.0
        pick = rnd.random() * total
        acc = 0.0
        sel = pool[0]
        for d in pool:
            acc += weights.get(d, 1.0)
            if acc >= pick:
                sel = d
                break
        chosen.append(sel)
        pool.remove(sel)

    log(f"PLAN: intended_routes={len(chosen)} | dates_per_dest(K)={k_dates_per_dest} | max_searches={max_searches}")

    # Dedup
    rd_hdr = _read_headers(ws_raw)
    rd_idx = {h: i for i, h in enumerate(rd_hdr)}
    existing_ids: Set[str] = set()
    if "deal_id" in rd_idx:
        col = rd_idx["deal_id"] + 1
        vals = ws_raw.col_values(col)
        for v in vals[1:]:
            v = str(v or "").strip().upper()
            if v:
                existing_ids.add(v)

    def put(row: List[Any], col: str, val: Any) -> None:
        i = rd_idx.get(col)
        if i is not None and i < len(row):
            row[i] = val

    searches = 0
    duffel_calls = 0
    out_rows: List[List[Any]] = []

    today = _today_utc()

    for dest in chosen:
        if searches >= max_searches or len(out_rows) >= max_inserts:
            break

        cfg_row = candidates[dest]
        origin_rows = sorted(origins_by_dest.get(dest, []), key=lambda x: (x[0], x[1], x[2]))[:origins_per_dest]
        if not origin_rows:
            continue

        cabin = (str(cfg_row.get("cabin_class") or "economy").strip().lower())
        trip_len = _safe_int(cfg_row.get("trip_length_days"), 7)
        airlines = _csv_list(str(cfg_row.get("included_airlines") or ""))

        dmin = _safe_int(cfg_row.get("days_ahead_min"), 14)
        dmax = _safe_int(cfg_row.get("days_ahead_max"), 120)
        dmax = max(dmax, dmin)
        offsets: List[int] = []
        if k_dates_per_dest <= 1:
            offsets = [dmin]
        else:
            span = dmax - dmin
            for k in range(k_dates_per_dest):
                frac = k / (k_dates_per_dest - 1)
                offsets.append(dmin + int(round(span * frac)))

        for (origin, conn_type, via_hub) in origin_rows:
            if searches >= max_searches or len(out_rows) >= max_inserts:
                break
            if origin not in geo:
                continue

            for off in offsets:
                if searches >= max_searches or len(out_rows) >= max_inserts:
                    break

                out_date = (today + dt.timedelta(days=off)).isoformat()
                in_date = (today + dt.timedelta(days=off + trip_len)).isoformat()

                searches += 1
                offers = _duffel_offer_request(origin, dest, out_date, in_date, cabin, airlines)
                duffel_calls += 1
                if not offers:
                    continue

                best = _best_offer(offers)
                if not best:
                    continue

                stops = _offer_stops(best)
                if run_slot == "PM" and stops != 0:
                    continue

                currency = str(best.get("total_currency") or "").strip().upper() or "GBP"
                try:
                    total_amount = float(str(best.get("total_amount") or "0").strip())
                except Exception:
                    total_amount = 0.0
                price_gbp = int(math.ceil(total_amount))

                did_seed = f"{origin}-{dest}-{out_date}-{in_date}-{price_gbp}-{stops}-{cabin}".upper()
                deal_id = _sha12(did_seed)
                if deal_id in existing_ids:
                    continue
                existing_ids.add(deal_id)

                o_city, o_country = geo.get(origin, ("", ""))
                d_city, d_country = geo.get(dest, ("", ""))

                out_mins, in_mins, total_hours = _offer_durations(best)
                carriers = _offer_carriers(best)

                now_iso = _to_iso_utc()

                row = [""] * len(rd_hdr)
                put(row, "status", "NEW")
                put(row, "deal_id", deal_id)
                put(row, "price_gbp", price_gbp)
                put(row, "currency", currency)

                put(row, "origin_city", o_city)
                put(row, "origin_country", o_country)
                put(row, "origin_iata", origin)

                put(row, "destination_city", d_city)
                put(row, "destination_country", d_country)
                put(row, "destination_iata", dest)

                put(row, "outbound_date", out_date)
                put(row, "return_date", in_date)

                put(row, "stops", stops)
                put(row, "trip_length_days", _trip_len_days(out_date, in_date))

                put(row, "deal_theme", theme)
                put(row, "theme", theme)

                put(row, "cabin_class", cabin)

                put(row, "connection_type", conn_type)
                put(row, "via_hub", via_hub)

                put(row, "outbound_duration_minutes", out_mins)
                put(row, "inbound_duration_minutes", in_mins)
                put(row, "total_duration_hours", total_hours)

                put(row, "carriers", carriers)

                put(row, "ingested_at_utc", now_iso)
                put(row, "created_utc", now_iso)
                put(row, "timestamp", now_iso)
                put(row, "created_at", now_iso)

                out_rows.append(row)

    log(f"✓ Searches completed: {searches}")
    log(f"✓ Duffel calls made: {duffel_calls} (cap {max_searches})")
    log(f"✓ Deals collected: {len(out_rows)} (cap {max_inserts})")

    if not out_rows:
        log("⚠️ No rows to insert (no winners)")
        return 0

    _append_rows(ws_raw, out_rows)
    log(f"✅ Inserted {len(out_rows)} rows into {RAW_DEALS_TAB}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
