# workers/pipeline_worker.py
# TRAVELTXTTER V5 — FEEDER (MIN CONFIG, WEIGHT RANKING)
# CONFIG headers (required): enabled, destination_iata, theme, weight
# OPS_MASTER theme cell: B2 (theme_of_the-day)
# RAW_DEALS is the only writable DB.

from __future__ import annotations

import datetime as dt
import json
import os
import random
import time
from typing import Any, Dict, List, Optional, Tuple

import gspread
import requests
from google.oauth2.service_account import Credentials


# ----------------------------
# Logging
# ----------------------------

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


# ----------------------------
# Env helpers
# ----------------------------

def env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v)

def env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, default)).strip())
    except Exception:
        return default

def env_float(name: str, default: float) -> float:
    try:
        return float(str(os.getenv(name, default)).strip())
    except Exception:
        return default

def truthy(v: Any) -> bool:
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "on")


# ----------------------------
# Google auth (robust SA JSON)
# ----------------------------

def _parse_sa_json(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()
    if not raw:
        raise RuntimeError("Missing service account JSON (GCP_SA_JSON or GCP_SA_JSON_ONE_LINE).")

    # Try 1: direct
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    # Try 2: secrets often store \\n sequences
    try:
        return json.loads(raw.replace("\\n", "\n"))
    except json.JSONDecodeError:
        pass

    # Try 3: secrets sometimes contain literal newlines inside JSON strings (invalid control chars)
    # Convert literal newlines to escaped \n so JSON parses.
    try:
        return json.loads(raw.replace("\n", "\\n"))
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Service account JSON could not be parsed: {e}") from e


def gspread_client() -> gspread.Client:
    raw = env_str("GCP_SA_JSON_ONE_LINE") or env_str("GCP_SA_JSON")
    info = _parse_sa_json(raw)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


# ----------------------------
# Sheet helpers
# ----------------------------

def open_sheet(gc: gspread.Client) -> gspread.Spreadsheet:
    sid = env_str("SPREADSHEET_ID") or env_str("SHEET_ID")
    if not sid:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID env var.")
    return gc.open_by_key(sid)

def open_ws(sh: gspread.Spreadsheet, name: str) -> gspread.Worksheet:
    name = (name or "").strip()
    if not name:
        raise RuntimeError("Worksheet name was empty.")
    try:
        return sh.worksheet(name)
    except Exception as e:
        raise RuntimeError(f"WorksheetNotFound: '{name}'") from e

def get_all_records_fast(ws: gspread.Worksheet) -> List[Dict[str, Any]]:
    # gspread get_all_records can be slow + brittle. We do our own.
    values = ws.get_all_values()
    if not values or len(values) < 2:
        return []
    headers = [h.strip() for h in values[0]]
    out: List[Dict[str, Any]] = []
    for row in values[1:]:
        if not any(cell.strip() for cell in row):
            continue
        d: Dict[str, Any] = {}
        for i, h in enumerate(headers):
            if not h:
                continue
            d[h] = row[i] if i < len(row) else ""
        out.append(d)
    return out

def header_map_first(headers: List[str]) -> Dict[str, int]:
    # If duplicate header names exist, keep FIRST occurrence.
    m: Dict[str, int] = {}
    for i, h in enumerate(headers):
        k = (h or "").strip()
        if k and k not in m:
            m[k] = i
    return m


# ----------------------------
# Date / time parsing
# ----------------------------

def now_utc() -> dt.datetime:
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)

def iso_utc(ts: dt.datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    return ts.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def parse_iso_or_serial(v: Any) -> Optional[dt.datetime]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        # Google Sheets serial date (days since 1899-12-30)
        base = dt.datetime(1899, 12, 30, tzinfo=dt.timezone.utc)
        return base + dt.timedelta(days=float(v))
    s = str(v).strip()
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    try:
        d = dt.datetime.fromisoformat(s)
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        return d.astimezone(dt.timezone.utc)
    except Exception:
        return None


# ----------------------------
# Theme + policy from VARIABLES
# ----------------------------

def read_theme_today(ws_ops: gspread.Worksheet) -> str:
    # Locked: theme lives in OPS_MASTER!B2
    v = ws_ops.acell("B2").value
    t = (v or "").strip()
    return t if t else "DEFAULT"

def origins_for_theme(theme: str) -> List[str]:
    key = f"ORIGINS_{theme.upper()}"
    raw = env_str(key) or env_str("ORIGINS_DEFAULT")
    out = [x.strip().upper() for x in raw.split(",") if x.strip()]
    return out if out else [x.strip().upper() for x in env_str("ORIGINS_DEFAULT", "LHR,LGW,MAN").split(",") if x.strip()]

def max_stops_for_theme(theme: str) -> int:
    key = f"MAX_STOPS_{theme.upper()}"
    return env_int(key, env_int("MAX_STOPS_DEFAULT", 1))

def trip_bounds_for_theme(theme: str) -> Tuple[int, int]:
    # Accept either TRIP_THEME_MIN/MAX vars or your legacy "TRIP_THEME_MIN/MAX = a / b" (not parseable).
    # So we expect two env vars:
    # TRIP_<THEME>_MIN and TRIP_<THEME>_MAX
    mn = env_int(f"TRIP_{theme.upper()}_MIN", env_int("TRIP_DEFAULT_MIN", 4))
    mx = env_int(f"TRIP_{theme.upper()}_MAX", env_int("TRIP_DEFAULT_MAX", 10))
    if mx < mn:
        mx = mn
    return mn, mx

def window_bounds_for_theme(theme: str) -> Tuple[int, int]:
    mn = env_int(f"WINDOW_{theme.upper()}_MIN", env_int("WINDOW_DEFAULT_MIN", 21))
    mx = env_int(f"WINDOW_{theme.upper()}_MAX", env_int("WINDOW_DEFAULT_MAX", 84))
    if mx < mn:
        mx = mn
    return mn, mx


# ----------------------------
# Duffel API (minimal)
# ----------------------------

DUFFEL_API = "https://api.duffel.com/air/offer_requests"

def duffel_headers() -> Dict[str, str]:
    key = env_str("DUFFEL_API_KEY")
    if not key:
        raise RuntimeError("Missing DUFFEL_API_KEY.")
    # Duffel-Version v2 fix already learned
    return {
        "Authorization": f"Bearer {key}",
        "Duffel-Version": "v2",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

def duffel_search_cheapest(
    origin_iata: str,
    dest_iata: str,
    depart: dt.date,
    ret: dt.date,
    cabin: str,
    max_connections: int,
) -> Optional[Dict[str, Any]]:
    payload = {
        "data": {
            "slices": [
                {"origin": origin_iata, "destination": dest_iata, "departure_date": depart.isoformat()},
                {"origin": dest_iata, "destination": origin_iata, "departure_date": ret.isoformat()},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": cabin,
            "max_connections": max_connections,
            # Keep it cheap: don't add bags here; RDV/scorer can handle later if needed
        }
    }

    r = requests.post(DUFFEL_API, headers=duffel_headers(), json=payload, timeout=45)
    if r.status_code >= 400:
        # Non-fatal; treat as no offer
        return None

    data = r.json().get("data") or {}
    offers = data.get("offers") or []
    if not offers:
        return None

    # Cheapest by total_amount (string)
    def amt(o: Dict[str, Any]) -> float:
        try:
            return float(o.get("total_amount") or 1e18)
        except Exception:
            return 1e18

    offers.sort(key=amt)
    return offers[0]


def extract_carriers(offer: Dict[str, Any]) -> str:
    codes: List[str] = []
    slices = offer.get("slices") or []
    for sl in slices:
        for seg in (sl.get("segments") or []):
            m = (seg.get("marketing_carrier") or {}).get("iata_code") or ""
            if m and m not in codes:
                codes.append(m)
    return ",".join(codes)


# ----------------------------
# Feeder core
# ----------------------------

RAW_REQUIRED_HEADERS = [
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
    "booking_link_vip",
    "posted_vip_at",
    "posted_free_at",
    "posted_instagram_at",
    "ingested_at_utc",
    "phrase_category",
    "scored_timestamp",
]

def ensure_raw_headers(ws_raw: gspread.Worksheet) -> Tuple[List[str], Dict[str, int]]:
    values = ws_raw.get_all_values()
    if not values:
        raise RuntimeError("RAW_DEALS is empty (no header row).")
    headers = values[0]
    norm = [(h or "").strip() for h in headers]
    missing = [h for h in RAW_REQUIRED_HEADERS if h not in norm]
    if missing:
        raise RuntimeError(f"RAW_DEALS missing required headers: {missing}")
    return norm, header_map_first(norm)

def load_dedupe_set(ws_raw: gspread.Worksheet, hmap: Dict[str, int]) -> set:
    values = ws_raw.get_all_values()
    if len(values) < 2:
        return set()
    rows = values[1:]
    def col(name: str, row: List[str]) -> str:
        i = hmap.get(name)
        return (row[i] if (i is not None and i < len(row)) else "").strip()

    s = set()
    for r in rows:
        o = col("origin_iata", r).upper()
        d = col("destination_iata", r).upper()
        od = col("outbound_date", r)
        rd = col("return_date", r)
        if o and d and od and rd:
            s.add((o, d, od, rd))
    return s

def pick_depart_return(theme: str, seed: int) -> Tuple[dt.date, dt.date, int]:
    wmin, wmax = window_bounds_for_theme(theme)
    tmin, tmax = trip_bounds_for_theme(theme)

    rng = random.Random(seed)
    days_ahead = rng.randint(wmin, wmax)
    trip_len = rng.randint(tmin, tmax)

    depart = (dt.date.today() + dt.timedelta(days=days_ahead))
    ret = depart + dt.timedelta(days=trip_len)
    return depart, ret, trip_len

def deal_id(origin: str, dest: str, depart: dt.date, ret: dt.date) -> str:
    return f"{origin}_{dest}_{depart.strftime('%Y%m%d')}_{ret.strftime('%Y%m%d')}"

def main() -> int:
    log("==============================================================================")
    log("TRAVELTXTTER V5 — FEEDER START (MIN CONFIG, WEIGHT RANKING)")
    log("==============================================================================")

    RAW_TAB = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    CFG_TAB = env_str("FEEDER_CONFIG_TAB", "CONFIG")
    OPS_TAB = env_str("OPS_MASTER_TAB", "OPS_MASTER")

    MAX_SEARCHES = env_int("DUFFEL_MAX_SEARCHES_PER_RUN", env_int("FEEDER_MAX_SEARCHES", 12))
    MAX_INSERTS = env_int("DUFFEL_MAX_INSERTS", env_int("FEEDER_MAX_INSERTS", 50))
    ROUTES_PER_RUN = env_int("DUFFEL_ROUTES_PER_RUN", 4)
    ORIGINS_PER_DEST = 3
    SLEEP_S = env_float("FEEDER_SLEEP_SECONDS", 0.1)
    CABIN = "economy"  # economy-first lock for V5

    gc = gspread_client()
    sh = open_sheet(gc)

    ws_raw = open_ws(sh, RAW_TAB)
    ws_cfg = open_ws(sh, CFG_TAB)
    ws_ops = open_ws(sh, OPS_TAB)

    theme_today = read_theme_today(ws_ops)
    log(f"🎯 Theme of day: {theme_today}")

    headers, hmap = ensure_raw_headers(ws_raw)
    dedupe = load_dedupe_set(ws_raw, hmap)

    cfg_rows = get_all_records_fast(ws_cfg)
    active = []
    for r in cfg_rows:
        if truthy(r.get("enabled")):
            active.append(r)

    theme_rows = [r for r in active if str(r.get("theme") or "").strip().lower() == theme_today.lower()]
    if not theme_rows:
        log("⚠️ No CONFIG routes eligible for theme. Exiting 0.")
        return 0

    # Sort by weight desc
    def w(r: Dict[str, Any]) -> float:
        try:
            return float(str(r.get("weight") or "0").strip())
        except Exception:
            return 0.0

    theme_rows.sort(key=w, reverse=True)
    dests = []
    seen = set()
    for r in theme_rows:
        d = str(r.get("destination_iata") or "").strip().upper()
        if not d or d in seen:
            continue
        seen.add(d)
        dests.append((d, w(r)))
        if len(dests) >= ROUTES_PER_RUN:
            break

    log(f"CAPS: MAX_SEARCHES={MAX_SEARCHES} | MAX_INSERTS={MAX_INSERTS} | DESTS_PER_RUN={len(dests)}")

    origins = origins_for_theme(theme_today)
    max_conn = max_stops_for_theme(theme_today)

    # Budget split (primary/secondary) is optional; keep simple for V5.
    planned: List[Tuple[str, str, float]] = []
    for dest, weight in dests:
        for o in origins[:ORIGINS_PER_DEST]:
            planned.append((o, dest, weight))

    planned = planned[:MAX_SEARCHES]

    inserted_rows: List[List[Any]] = []
    searches = 0
    no_offer = 0
    dedupe_skips = 0

    # Stable daily seed
    day_seed = int(dt.date.today().strftime("%Y%m%d"))

    for i, (o, d, weight) in enumerate(planned, start=1):
        if len(inserted_rows) >= MAX_INSERTS:
            break

        depart, ret, trip_len = pick_depart_return(theme_today, seed=day_seed + i * 97)
        key = (o, d, depart.isoformat(), ret.isoformat())
        if key in dedupe:
            dedupe_skips += 1
            continue

        log(f"🔎 Search {searches+1}/{len(planned)} {o}→{d} | weight={weight:.2f} | max_conn={max_conn} | cabin={CABIN} | trip={trip_len}d")
        searches += 1

        offer = duffel_search_cheapest(
            origin_iata=o,
            dest_iata=d,
            depart=depart,
            ret=ret,
            cabin=CABIN,
            max_connections=max_conn,
        )
        time.sleep(SLEEP_S)

        if not offer:
            no_offer += 1
            continue

        currency = (offer.get("total_currency") or "").upper()
        if currency != "GBP":
            # Economy-first; keep it deterministic: only GBP passes feeder
            continue

        try:
            price_gbp = float(offer.get("total_amount") or 0.0)
        except Exception:
            continue

        stops = 0
        try:
            # Duffel offers have slices[0].segments length etc.
            slices = offer.get("slices") or []
            # approximate stops as max(segments-1) across slices
            for sl in slices:
                segs = sl.get("segments") or []
                stops = max(stops, max(0, len(segs) - 1))
        except Exception:
            pass

        row = [""] * len(headers)

        def setv(col: str, val: Any) -> None:
            idx = hmap.get(col)
            if idx is not None and idx < len(row):
                row[idx] = val

        setv("deal_id", deal_id(o, d, depart, ret))
        setv("origin_iata", o)
        setv("destination_iata", d)
        setv("origin_city", "")                 # filled later by enrich_router via IATA_MASTER
        setv("destination_city", "")            # filled later by enrich_router
        setv("destination_country", "")         # filled later by enrich_router
        setv("outbound_date", depart.isoformat())
        setv("return_date", ret.isoformat())
        setv("price_gbp", round(price_gbp, 2))
        setv("currency", "GBP")
        setv("stops", stops)
        setv("cabin_class", CABIN)
        setv("carriers", extract_carriers(offer))
        setv("theme", theme_today)
        setv("status", "NEW")
        setv("publish_window", "")              # scorer sets later
        setv("score", "")                       # scorer sets later
        setv("phrase_used", "")                 # enrich sets later
        setv("graphic_url", "")                 # render_client sets later
        setv("booking_link_vip", "")            # link_router sets later
        setv("posted_vip_at", "")
        setv("posted_free_at", "")
        setv("posted_instagram_at", "")
        setv("ingested_at_utc", iso_utc(now_utc()))
        setv("phrase_category", "")
        setv("scored_timestamp", "")

        inserted_rows.append(row)
        dedupe.add(key)

    if not inserted_rows:
        log("⚠️ No rows inserted.")
        log(f"SKIPS: dedupe={dedupe_skips} no_offer={no_offer}")
        return 0

    # Bulk append (single API call)
    ws_raw.append_rows(inserted_rows, value_input_option="RAW")
    log(f"✅ Inserted {len(inserted_rows)} row(s) into {RAW_TAB}.")
    log(f"SUMMARY: searches={searches} inserted={len(inserted_rows)} dedupe_skips={dedupe_skips} no_offer_skips={no_offer}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
