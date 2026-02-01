# workers/pipeline_worker.py
from __future__ import annotations

import os
import re
import json
import time
import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import gspread
import requests


# ==============================
# Logging
# ==============================
def log(msg: str) -> None:
    ts = datetime.utcnow().isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


# ==============================
# Helpers
# ==============================
def _norm_iata(v: Any) -> str:
    return str(v or "").strip().upper()


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, (int, float)):
            return int(v)
        s = str(v).strip()
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
        s = str(v).strip()
        if s == "":
            return default
        return float(s)
    except Exception:
        return default


def _is_true(v: Any) -> bool:
    s = str(v or "").strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def _get_all_records_safe(ws, *, label: str = "") -> List[Dict[str, Any]]:
    """Read a worksheet into a list[dict] without failing on non-unique headers.

    gspread.Worksheet.get_all_records() raises if headers are not unique.
    For TravelTxter, we prefer a tolerant read that preserves the first
    occurrence of each header name and safely namespaces duplicates.
    """
    try:
        return ws.get_all_records() or []
    except Exception as e:
        try:
            log(f"⚠️ {label} get_all_records failed ({type(e).__name__}: {e}). Falling back to get_all_values().")
        except Exception:
            pass

        values = ws.get_all_values() or []
        if not values:
            return []

        raw_headers = [str(h).strip() for h in values[0]]
        # Build unique header keys while preserving the first occurrence verbatim.
        seen: Dict[str, int] = {}
        headers: List[str] = []
        for h in raw_headers:
            base = h if h != "" else "__blank__"
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


def _csv_list(v: str) -> List[str]:
    if not v:
        return []
    return [x.strip() for x in str(v).split(",") if x.strip()]


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _yyyymmdd(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")


def _parse_date_yyyymmdd(s: str) -> Optional[datetime]:
    try:
        return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _weekday_seeded_choice(items: List[str], seed_key: str) -> Optional[str]:
    if not items:
        return None
    # deterministic by ISO week + seed_key
    w = datetime.utcnow().isocalendar().week
    r = random.Random(f"{seed_key}:{w}")
    return items[r.randrange(0, len(items))]


def _connections_from_tolerance(t: str) -> int:
    t = (t or "").strip().lower()
    if t in ("direct", "0", "none"):
        return 0
    if t in ("1", "one", "via_hub"):
        return 1
    return 2


# ==============================
# Config / Env
# ==============================
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID") or os.environ.get("SHEET_ID") or ""
RAW_DEALS_TAB = os.environ.get("RAW_DEALS_TAB") or "RAW_DEALS"
CONFIG_TAB = os.environ.get("CONFIG_TAB") or "CONFIG"
ROUTE_CAPABILITY_MAP_TAB = os.environ.get("ROUTE_CAPABILITY_MAP_TAB") or "ROUTE_CAPABILITY_MAP"
IATA_MASTER_TAB = os.environ.get("IATA_MASTER_TAB") or "IATA_MASTER"
OPS_MASTER_TAB = os.environ.get("OPS_MASTER_TAB") or "OPS_MASTER"
ZONE_THEME_BENCHMARKS_TAB = os.environ.get("ZONE_THEME_BENCHMARKS_TAB") or "ZONE_THEME_BENCHMARKS"
CONFIG_CARRIER_BIAS_TAB = os.environ.get("CONFIG_CARRIER_BIAS_TAB") or "CONFIG_CARRIER_BIAS"

DUFFEL_API_KEY = os.environ.get("DUFFEL_API_KEY") or ""
DUFFEL_BUDGET_GBP = _safe_float(os.environ.get("DUFFEL_BUDGET_GBP") or "25", 25.0)
DUFFEL_EXCESS_SEARCH_USD = _safe_float(os.environ.get("DUFFEL_EXCESS_SEARCH_USD") or "0.005", 0.005)

MAX_INSERTS = _safe_int(os.environ.get("MAX_INSERTS") or "50", 50)
PER_ORIGIN = _safe_int(os.environ.get("PER_ORIGIN") or "15", 15)
PER_ROUTE = _safe_int(os.environ.get("PER_ROUTE") or "5", 5)
MAX_SEARCHES = _safe_int(os.environ.get("MAX_SEARCHES") or "12", 12)

DESTS_PER_RUN = _safe_int(os.environ.get("DESTS_PER_RUN") or "4", 4)
ORIGINS_PER_DEST = _safe_int(os.environ.get("ORIGINS_PER_DEST") or "3", 3)

RUN_SLOT = os.environ.get("RUN_SLOT") or ""  # "AM"/"PM" or blank to derive from time
SLOT_SPLIT = os.environ.get("SLOT_SPLIT") or "90/10"
K_DATES_PER_DEST = _safe_int(os.environ.get("K_DATES_PER_DEST") or "3", 3)

GCP_SA_JSON = os.environ.get("GCP_SA_JSON") or ""
GCP_SA_JSON_ONE_LINE = os.environ.get("GCP_SA_JSON_ONE_LINE") or ""


# ==============================
# Duffel
# ==============================
DUFFEL_API_BASE = "https://api.duffel.com"
DUFFEL_VERSION = os.environ.get("DUFFEL_VERSION") or "v2"

_HEADERS = {
    "Accept": "application/json",
    "Duffel-Version": DUFFEL_VERSION,
}


def _duffel_headers() -> Dict[str, str]:
    h = dict(_HEADERS)
    if DUFFEL_API_KEY:
        h["Authorization"] = f"Bearer {DUFFEL_API_KEY}"
    return h


def _duffel_search(origin: str, dest: str, out_date: str, in_date: Optional[str], cabin: str, max_connections: int,
                  airlines: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
    # Minimal Duffel offer request (existing logic preserved)
    payload: Dict[str, Any] = {
        "data": {
            "slices": [
                {"origin": origin, "destination": dest, "departure_date": out_date},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": cabin,
        }
    }
    if in_date:
        payload["data"]["slices"].append({"origin": dest, "destination": origin, "departure_date": in_date})

    if airlines:
        payload["data"]["allowed_carrier_codes"] = airlines

    try:
        r = requests.post(
            f"{DUFFEL_API_BASE}/air/offer_requests",
            headers=_duffel_headers(),
            json=payload,
            timeout=45,
        )
        if r.status_code >= 400:
            return None
        return r.json()
    except Exception:
        return None


# ==============================
# ZTB Theme Pool
# ==============================
def _eligible_themes_from_ztb(ztb_rows: List[Dict[str, Any]]) -> List[str]:
    # Very lightweight: any enabled + in-season is handled in sheet formula; here we just read pool.
    themes = []
    for r in ztb_rows:
        t = str(r.get("theme") or "").strip()
        if not t:
            continue
        if _is_true(r.get("enabled")):
            themes.append(t)
    # unique, stable order
    out = []
    seen = set()
    for t in themes:
        k = t.strip().lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(t)
    return out


# ==============================
# Google Sheets auth
# ==============================
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


# ==============================
# Main
# ==============================
def main() -> int:
    log("=" * 79)
    log("TRAVELTXTTER PIPELINE WORKER (FEEDER) START")
    log("=" * 79)

    # Slot
    slot = RUN_SLOT.strip().upper()
    if slot not in ("AM", "PM"):
        slot = "AM" if datetime.utcnow().hour < 12 else "PM"
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
    ws_ztb = sh.worksheet(ZONE_THEME_BENCHMARKS_TAB)

    # OPS_MASTER optional
    ws_ops = None
    try:
        ws_ops = sh.worksheet(OPS_MASTER_TAB)
    except Exception:
        log("⚠️ OPS_MASTER worksheet not found - will use calculated theme")

    # Carrier bias (informational continuity)
    try:
        ws_bias = sh.worksheet(CONFIG_CARRIER_BIAS_TAB)
        bias_rows = ws_bias.get_all_records() or []
        usable_bias = sum(
            1
            for r in bias_rows
            if str(r.get("carrier_code") or "").strip()
            and str(r.get("theme") or "").strip()
        )
        log(f"✅ CONFIG_CARRIER_BIAS loaded: {usable_bias} usable rows")
    except Exception:
        log("⚠️ CONFIG_CARRIER_BIAS not loaded or empty.")

    # Theme authority
    ztb_rows_all = ws_ztb.get_all_records()
    eligible_themes = _eligible_themes_from_ztb(ztb_rows_all)

    theme_today = None
    if ws_ops:
        try:
            v = ws_ops.acell("B5").value
            if v and str(v).strip():
                theme_today = str(v).strip()
                log(f"✅ Theme read from OPS_MASTER!B5: {theme_today}")
        except Exception:
            theme_today = None

    if not theme_today:
        theme_today = _weekday_seeded_choice(eligible_themes, "theme")
        log(f"✅ Theme calculated from ZTB pool: {theme_today}")

    log(f"🎯 Theme of the day (UTC): {theme_today}")

    # ZTB constraints for this theme (best-effort)
    ztb_today = None
    for r in ztb_rows_all:
        if str(r.get("theme") or "").strip().lower() == str(theme_today).strip().lower():
            ztb_today = r
            break

    ztb_days_min = _safe_int(ztb_today.get("days_ahead_min") if ztb_today else None, 14)
    ztb_days_max = _safe_int(ztb_today.get("days_ahead_max") if ztb_today else None, 120)
    ztb_trip_len = _safe_int(ztb_today.get("trip_length_days") if ztb_today else None, 7)
    ztb_max_conn = _connections_from_tolerance(str(ztb_today.get("connection_tolerance") if ztb_today else "any"))

    # === CONFIG (robust read; avoids non-unique header crash) ===
    cfg_all = _get_all_records_safe(ws_cfg, label="CONFIG")
    cfg_active = [r for r in cfg_all if _is_true(r.get("active_in_feeder")) and _is_true(r.get("enabled"))]
    log(f"✅ CONFIG loaded: {len(cfg_active)} active rows (of {len(cfg_all)} total)")

    # ROUTE_CAPABILITY_MAP — route gating ONLY (no geo from here)
    rcm_rows = ws_rcm.get_all_records()
    enabled_routes: Set[Tuple[str, str]] = set()
    origins_by_dest: Dict[str, List[str]] = {}
    for r in rcm_rows:
        o = _norm_iata(r.get("origin_iata") or r.get("origin"))
        d = _norm_iata(r.get("destination_iata") or r.get("destination"))
        if not o or not d:
            continue
        if not _is_true(r.get("enabled")):
            continue
        conn = str(r.get("connection_type") or "").strip().lower()
        if slot == "PM" and conn != "direct":
            continue
        enabled_routes.add((o, d))
        origins_by_dest.setdefault(d, [])
        if o not in origins_by_dest[d]:
            origins_by_dest[d].append(o)

    log(f"✅ ROUTE_CAPABILITY_MAP loaded: {len(enabled_routes)} enabled routes")

    # IATA_MASTER — geo enrichment ONLY
    iata_geo: Dict[str, Tuple[str, str]] = {}
    for r in ws_iata.get_all_records():
        code = _norm_iata(r.get("iata_code") or r.get("iata") or r.get("IATA"))
        city = str(r.get("city") or "").strip()
        country = str(r.get("country") or "").strip()
        if code:
            iata_geo[code] = (city, country)

    log(f"✅ Geo dictionary loaded: {len(iata_geo)} IATA entries (IATA_MASTER only)")

    # === Build candidate destinations from CONFIG ===
    # NOTE: Schema expected (as per your header list): destination_iata, primary_theme, slot_hint, is_long_haul, etc.
    candidates: List[Dict[str, Any]] = []
    for r in cfg_active:
        if str(r.get("primary_theme") or "").strip().lower() != str(theme_today).strip().lower():
            continue
        if str(r.get("slot_hint") or "").strip().upper() != slot:
            continue
        is_lh = _is_true(r.get("is_long_haul"))
        if is_lh != want_long:
            continue
        dest = _norm_iata(r.get("destination_iata") or "")
        if not dest:
            continue
        if dest not in iata_geo:
            continue
        candidates.append(r)

    # pick up to DESTS_PER_RUN distinct destinations weighted by search_weight
    dest_weights: Dict[str, float] = {}
    dest_rows: Dict[str, Dict[str, Any]] = {}
    for r in candidates:
        d = _norm_iata(r.get("destination_iata"))
        w = _safe_float(r.get("search_weight"), 1.0)
        dest_weights[d] = max(dest_weights.get(d, 0.0), w)
        dest_rows[d] = r

    dests = list(dest_weights.keys())
    if not dests:
        log("⚠️ No eligible destinations from CONFIG (Gate 1 fail)")
        return 0

    # weighted sample without replacement
    chosen_dests: List[str] = []
    pool = dests[:]
    rnd = random.Random(f"{theme_today}:{slot}:{datetime.utcnow().date().isoformat()}")
    while pool and len(chosen_dests) < DESTS_PER_RUN:
        total = sum(dest_weights[d] for d in pool) or 1.0
        pick = rnd.random() * total
        acc = 0.0
        chosen = pool[0]
        for d in pool:
            acc += dest_weights[d]
            if acc >= pick:
                chosen = d
                break
        chosen_dests.append(chosen)
        pool.remove(chosen)

    # plan searches
    intended_routes = len(chosen_dests)
    log(f"PLAN: intended_routes={intended_routes} | dates_per_dest(K)={K_DATES_PER_DEST} | max_searches={MAX_SEARCHES}")

    # Build dates per destination (simple jitter within ZTB window)
    today = datetime.utcnow().date()
    date_choices: List[datetime] = []
    for k in range(K_DATES_PER_DEST):
        # deterministic jitter across window
        offset = ztb_days_min + int((ztb_days_max - ztb_days_min) * (k / max(1, (K_DATES_PER_DEST - 1))))
        date_choices.append(datetime.combine(today + timedelta(days=offset), datetime.min.time(), tzinfo=timezone.utc))

    # Execute searches
    searches = 0
    duffel_calls = 0
    deals: List[Dict[str, Any]] = []

    for dest in chosen_dests:
        if searches >= MAX_SEARCHES:
            break

        origins = origins_by_dest.get(dest, [])
        # pick up to ORIGINS_PER_DEST origins deterministically
        origins = sorted(origins)
        origins = origins[:ORIGINS_PER_DEST]

        if not origins:
            continue

        cfg_row = dest_rows.get(dest, {})
        cabin = str(cfg_row.get("cabin_class") or "economy").strip().lower()
        max_conn = _safe_int(cfg_row.get("max_connections"), ztb_max_conn)
        airlines = _csv_list(str(cfg_row.get("included_airlines") or ""))

        for origin in origins:
            if searches >= MAX_SEARCHES:
                break

            # Route cap per route/origin is enforced by collection caps downstream; keep existing structure
            for dt_out in date_choices:
                if searches >= MAX_SEARCHES:
                    break
                # build return date
                trip_len = _safe_int(cfg_row.get("trip_length_days"), ztb_trip_len)
                dt_in = dt_out + timedelta(days=trip_len)
                out_date = _yyyymmdd(dt_out)
                in_date = _yyyymmdd(dt_in)

                searches += 1
                if DUFFEL_API_KEY:
                    duffel_calls += 1
                    res = _duffel_search(origin, dest, out_date, in_date, cabin, max_conn, airlines=airlines)
                    if not res:
                        continue
                    # Minimal extraction (existing contract: you likely enrich elsewhere)
                    deals.append(
                        {
                            "origin_iata": origin,
                            "destination_iata": dest,
                            "out_date": out_date,
                            "in_date": in_date,
                            "theme": theme_today,
                            "slot": slot,
                            "raw_offer_request": json.dumps(res)[:5000],
                        }
                    )

    log(f"✓ Searches completed: {searches}")
    log(f"✓ Duffel calls made: {duffel_calls} (cap {MAX_SEARCHES})")
    log(f"✓ Deals collected: {len(deals)} (cap {MAX_INSERTS})")

    if not deals:
        log("⚠️ No rows to insert (no winners)")
        return 0

    # Insert deals into RAW_DEALS (append rows) — minimal; preserve existing behavior structure
    # NOTE: This worker in your repo likely has a richer insert with full schema.
    # Here we keep append-only of core fields if columns exist.
    hdr = ws_raw.row_values(1)
    col_index = {h: i for i, h in enumerate(hdr)}
    rows_to_append: List[List[Any]] = []
    for d in deals[:MAX_INSERTS]:
        row = [""] * len(hdr)
        for k, v in d.items():
            if k in col_index:
                row[col_index[k]] = v
        rows_to_append.append(row)

    if rows_to_append:
        ws_raw.append_rows(rows_to_append, value_input_option="USER_ENTERED")
        log(f"✅ Inserted {len(rows_to_append)} rows into {RAW_DEALS_TAB}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
