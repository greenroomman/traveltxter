# workers/pipeline_worker.py
# ==============================================================================
# TRAVELTXTTER V5 — FEEDER (MIN CONFIG, WEIGHT RANKING, OPS_MASTER THEME)
#
# CONTRACT (LOCKED)
# - Theme source of truth: OPS_MASTER!B2 (normalised)
# - CONFIG schema (minimal): enabled, destination_iata, theme, weight
# - Workers are dumb. Sheets is control plane. No hidden defaults.
# - Insert NEW inventory rows into RAW_DEALS (facts only, no scoring).
# - ingested_at_utc MUST be a NUMBER timestamp (epoch seconds) to support RDV math.
#
# ENV (expected)
# - SPREADSHEET_ID or SHEET_ID
# - GCP_SA_JSON or GCP_SA_JSON_ONE_LINE
# - RAW_DEALS_TAB (default RAW_DEALS)
# - FEEDER_CONFIG_TAB (default CONFIG)
# - OPS_MASTER_TAB (default OPS_MASTER)
# - DUFFEL_API_KEY
#
# Optional env
# - THEME_OVERRIDE (explicit only; if set, overrides OPS_MASTER!B2)
# - DUFFEL_MAX_SEARCHES_PER_RUN (default 12)
# - DUFFEL_MAX_INSERTS (default 50)
# - DUFFEL_ROUTES_PER_RUN (default 4)
# - FEEDER_SLEEP_SECONDS (default 0.1)
# - K_DATES_PER_DEST (default 3)
# - ORIGINS_<THEME> (e.g. ORIGINS_NORTHERN_LIGHTS="LGW,LHR,MAN")
# - ORIGINS_DEFAULT
# - MAX_STOPS_<THEME> (default 1)  (interpreted as max_connections for Duffel)
# - MAX_STOPS_DEFAULT
# - WINDOW_<THEME>_MIN/MAX (days ahead) defaults 21/84
# - WINDOW_DEFAULT_MIN/MAX
# - TRIP_<THEME>_MIN/MAX (trip length days) defaults 4/10
# - TRIP_DEFAULT_MIN/MAX
# ==============================================================================

from __future__ import annotations

import json
import os
import random
import time
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

import gspread
import requests
from google.oauth2.service_account import Credentials


# ----------------------------- logging ---------------------------------


def log(msg: str) -> None:
    print(f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} | {msg}", flush=True)


# ----------------------------- config ----------------------------------


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def env_csv(name: str, default: str) -> List[str]:
    raw = os.getenv(name, default).strip()
    if not raw:
        return []
    return [x.strip().upper() for x in raw.split(",") if x.strip()]


def norm(s: str) -> str:
    return (s or "").strip().lower()


def norm_header(h: str) -> str:
    return norm(h).replace("\u200b", "").replace("\ufeff", "")


# ----------------------------- gspread ---------------------------------


def _load_sa_json() -> Dict[str, Any]:
    """
    Robust Service Account JSON loading:
    - Supports GCP_SA_JSON_ONE_LINE and GCP_SA_JSON
    - Handles escaped newlines (\\n) and real newlines
    - Avoids "Invalid control character" failures by normalising
    """
    raw = (os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON") or "").strip()
    if not raw:
        raise RuntimeError("Missing service account JSON (GCP_SA_JSON_ONE_LINE or GCP_SA_JSON).")

    # Common patterns:
    # - one-line JSON with \n in private_key
    # - multi-line JSON with actual newlines in private_key (rare but happens)
    # We normalise by:
    # 1) If it's a file path, read it
    if raw.startswith("{") is False and os.path.exists(raw):
        with open(raw, "r", encoding="utf-8") as f:
            raw = f.read().strip()

    # 2) First attempt: JSON as-is
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    # 3) Replace escaped newlines with real newlines
    try:
        return json.loads(raw.replace("\\n", "\n"))
    except json.JSONDecodeError:
        pass

    # 4) If the private_key got real newlines but JSON wasn’t escaped properly,
    # we try a conservative fix: re-escape raw newlines inside JSON by converting
    # any unescaped newline chars to \\n globally.
    try:
        compact = raw.replace("\r\n", "\n").replace("\r", "\n")
        compact = compact.replace("\n", "\\n")
        return json.loads(compact)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Service account JSON could not be parsed: {e}") from e


def gspread_client() -> gspread.Client:
    sa = _load_sa_json()
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(sa, scopes=scopes)
    return gspread.authorize(creds)


def open_sheet(gc: gspread.Client) -> gspread.Spreadsheet:
    sid = (os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID") or "").strip()
    if not sid:
        raise RuntimeError("Missing SPREADSHEET_ID (or SHEET_ID).")
    return gc.open_by_key(sid)


# ----------------------------- Duffel ----------------------------------


DUFFEL_BASE = "https://api.duffel.com/air"


def duffel_headers() -> Dict[str, str]:
    key = (os.getenv("DUFFEL_API_KEY") or "").strip()
    if not key:
        raise RuntimeError("Missing DUFFEL_API_KEY.")
    # Duffel version header: V5 expects v2 (you previously hotfixed this)
    return {
        "Authorization": f"Bearer {key}",
        "Duffel-Version": "v2",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def duffel_offer_request(
    origin: str,
    dest: str,
    depart: date,
    ret: date,
    cabin: str,
    max_connections: int,
) -> Optional[Dict[str, Any]]:
    """
    Returns the *cheapest* offer found (dict) or None.
    We keep this deterministic and lightweight.
    """
    url = f"{DUFFEL_BASE}/offer_requests"
    payload = {
        "data": {
            "slices": [
                {"origin": origin, "destination": dest, "departure_date": depart.isoformat()},
                {"origin": dest, "destination": origin, "departure_date": ret.isoformat()},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": cabin,
            "max_connections": int(max_connections),
        }
    }

    r = requests.post(url, headers=duffel_headers(), json=payload, timeout=60)
    if r.status_code >= 300:
        # Non-fatal; treat as no offer for this query
        return None

    data = r.json().get("data") or {}
    offers = data.get("offers") or []
    if not offers:
        return None

    # Choose cheapest by total_amount
    def amt(o: Dict[str, Any]) -> float:
        try:
            return float(o.get("total_amount") or "999999")
        except ValueError:
            return 999999.0

    offers.sort(key=amt)
    return offers[0]


# ----------------------------- domain ----------------------------------


@dataclass(frozen=True)
class ConfigRow:
    destination_iata: str
    theme: str
    weight: float


def read_ops_theme(sh: gspread.Spreadsheet, ops_tab: str = "OPS_MASTER") -> str:
    ws = sh.worksheet(ops_tab)
    val = ws.acell("B2").value  # LOCKED: Theme lives in OPS_MASTER!B2
    theme = norm(val)
    if not theme:
        raise RuntimeError("OPS_MASTER!B2 is empty — theme is required.")
    return theme


def read_theme_today(sh: gspread.Spreadsheet) -> str:
    override = norm(os.getenv("THEME_OVERRIDE", ""))
    if override:
        log(f"🧪 THEME_OVERRIDE set -> using '{override}'")
        return override
    theme = read_ops_theme(sh, os.getenv("OPS_MASTER_TAB", "OPS_MASTER"))
    return theme


def load_min_config(sh: gspread.Spreadsheet, theme_today: str) -> List[ConfigRow]:
    tab = os.getenv("FEEDER_CONFIG_TAB", "CONFIG")
    ws = sh.worksheet(tab)
    values = ws.get_all_values()
    if not values or len(values) < 2:
        return []

    headers = [norm_header(h) for h in values[0]]
    hm: Dict[str, int] = {h: i for i, h in enumerate(headers) if h}

    required = ["enabled", "destination_iata", "theme", "weight"]
    missing = [c for c in required if c not in hm]
    if missing:
        raise RuntimeError(f"{tab} missing required headers: {missing}")

    out: List[ConfigRow] = []
    for row in values[1:]:
        # pad
        if len(row) < len(headers):
            row = row + [""] * (len(headers) - len(row))

        enabled_raw = norm(row[hm["enabled"]])
        if enabled_raw not in ("true", "1", "yes", "y"):
            continue

        dest = (row[hm["destination_iata"]] or "").strip().upper()
        if not dest or len(dest) != 3:
            continue

        row_theme = norm(row[hm["theme"]])
        # Strict theme match; no "DEFAULT" backdoor in V5
        if row_theme != theme_today:
            continue

        try:
            w = float((row[hm["weight"]] or "0").strip())
        except ValueError:
            w = 0.0
        if w <= 0:
            continue

        out.append(ConfigRow(destination_iata=dest, theme=row_theme, weight=w))

    # Weight ranking (desc)
    out.sort(key=lambda r: r.weight, reverse=True)
    return out


def theme_env_key(theme: str) -> str:
    return theme.upper().replace("-", "_").replace(" ", "_")


def get_theme_origins(theme_today: str) -> List[str]:
    key = theme_env_key(theme_today)
    origins = env_csv(f"ORIGINS_{key}", "")
    if origins:
        return origins
    return env_csv("ORIGINS_DEFAULT", "LHR,LGW,MAN")


def get_theme_max_connections(theme_today: str) -> int:
    key = theme_env_key(theme_today)
    raw = os.getenv(f"MAX_STOPS_{key}", "").strip()
    if raw:
        try:
            return int(raw)
        except ValueError:
            pass
    return env_int("MAX_STOPS_DEFAULT", 1)


def get_window(theme_today: str) -> Tuple[int, int]:
    key = theme_env_key(theme_today)
    mn = os.getenv(f"WINDOW_{key}_MIN", "").strip()
    mx = os.getenv(f"WINDOW_{key}_MAX", "").strip()
    if mn and mx:
        try:
            return int(mn), int(mx)
        except ValueError:
            pass
    return env_int("WINDOW_DEFAULT_MIN", 21), env_int("WINDOW_DEFAULT_MAX", 84)


def get_trip_len(theme_today: str) -> int:
    key = theme_env_key(theme_today)
    mn = os.getenv(f"TRIP_{key}_MIN", "").strip()
    mx = os.getenv(f"TRIP_{key}_MAX", "").strip()
    if mn and mx:
        try:
            a, b = int(mn), int(mx)
            if a > b:
                a, b = b, a
            return max(1, int(round((a + b) / 2)))
        except ValueError:
            pass
    a = env_int("TRIP_DEFAULT_MIN", 4)
    b = env_int("TRIP_DEFAULT_MAX", 10)
    if a > b:
        a, b = b, a
    return max(1, int(round((a + b) / 2)))


def build_date_candidates(window_min: int, window_max: int, k: int) -> List[date]:
    k = max(1, k)
    start = date.today() + timedelta(days=window_min)
    end = date.today() + timedelta(days=window_max)
    span = max(1, (end - start).days)

    # Evenly spaced k points, deterministic-ish but not identical daily
    points: List[date] = []
    for i in range(k):
        offset = int(round((span * i) / max(1, (k - 1))))
        points.append(start + timedelta(days=offset))

    # Add tiny shuffle based on day-of-year so it’s not identical every run
    seed = int(date.today().strftime("%j"))
    rnd = random.Random(seed)
    rnd.shuffle(points)
    return points


def get_raw_headers(ws: gspread.Worksheet) -> List[str]:
    return ws.row_values(1)


def header_index_map(headers: List[str]) -> Dict[str, int]:
    # map normalised header -> first index
    hm: Dict[str, int] = {}
    for i, h in enumerate(headers):
        nh = norm_header(h)
        if nh and nh not in hm:
            hm[nh] = i
    return hm


def pick_ingest_header(hm: Dict[str, int]) -> str:
    # Support both new and legacy naming
    for candidate in ("ingested_at_utc", "ingested_dt", "ingested_at", "ingested_utc"):
        if candidate in hm:
            return candidate
    raise RuntimeError("RAW_DEALS missing an ingest timestamp column (expected ingested_at_utc).")


def read_existing_deal_ids(ws: gspread.Worksheet, hm: Dict[str, int], limit_rows: int = 3000) -> set:
    if "deal_id" not in hm:
        return set()
    # Read last N rows of deal_id only (cheap)
    all_vals = ws.get_all_values()
    if len(all_vals) <= 1:
        return set()
    start = max(2, len(all_vals) - limit_rows)
    idx = hm["deal_id"]
    ids = set()
    for r in all_vals[start - 1 :]:
        if len(r) > idx:
            v = (r[idx] or "").strip()
            if v:
                ids.add(v)
    return ids


def append_rows(ws: gspread.Worksheet, rows: List[List[Any]]) -> None:
    if not rows:
        return
    ws.append_rows(rows, value_input_option="USER_ENTERED")


# ----------------------------- main ------------------------------------


def main() -> int:
    log("======================================================================")
    log("TRAVELTXTTER V5 — FEEDER START (MIN CONFIG, OPS_MASTER THEME)")
    log("======================================================================")

    gc = gspread_client()
    sh = open_sheet(gc)

    raw_tab = os.getenv("RAW_DEALS_TAB", "RAW_DEALS")
    cfg_tab = os.getenv("FEEDER_CONFIG_TAB", "CONFIG")
    ops_tab = os.getenv("OPS_MASTER_TAB", "OPS_MASTER")

    # Theme (LOCKED)
    theme_today = read_theme_today(sh)
    log(f"🎯 Theme of day (OPS_MASTER!B2): {theme_today}")

    # Caps
    max_searches = env_int("DUFFEL_MAX_SEARCHES_PER_RUN", 12)
    max_inserts = env_int("DUFFEL_MAX_INSERTS", 50)
    dests_per_run = env_int("DUFFEL_ROUTES_PER_RUN", 4)
    sleep_s = env_float("FEEDER_SLEEP_SECONDS", 0.1)
    k_dates = env_int("K_DATES_PER_DEST", 3)

    # Primary / Secondary (11/1 default for 12)
    secondary_budget = 1 if max_searches >= 2 else 0
    primary_budget = max_searches - secondary_budget

    origins = get_theme_origins(theme_today)
    if not origins:
        raise RuntimeError("No origins available (ORIGINS_<THEME> or ORIGINS_DEFAULT).")

    cabin = norm(os.getenv("CABIN_CLASS", "economy")) or "economy"
    max_conn_primary = get_theme_max_connections(theme_today)

    window_min, window_max = get_window(theme_today)
    trip_len = get_trip_len(theme_today)

    log(
        f"CAPS: MAX_SEARCHES={max_searches} | MAX_INSERTS={max_inserts} | DESTS_PER_RUN={dests_per_run}"
    )
    log(f"Budget: primary={primary_budget} secondary={secondary_budget}")
    log(
        f"Params: cabin={cabin} | max_conn_primary={max_conn_primary} | trip_len={trip_len}d | window={window_min}-{window_max}d"
    )

    # CONFIG (minimal)
    try:
        cfg_rows = load_min_config(sh, theme_today)
    except gspread.exceptions.WorksheetNotFound as e:
        raise RuntimeError(f"WorksheetNotFound: {cfg_tab}") from e

    if not cfg_rows:
        log("⚠️ No CONFIG routes eligible for theme (enabled + theme match).")
        return 0

    # Select top destinations by weight (dests_per_run)
    top_dests = cfg_rows[: max(1, dests_per_run)]

    # RAW_DEALS headers
    ws_raw = sh.worksheet(raw_tab)
    raw_headers = get_raw_headers(ws_raw)
    hm = header_index_map(raw_headers)

    # Minimal required for feeder insertion
    required = ["deal_id", "origin_iata", "destination_iata", "outbound_date", "return_date", "price_gbp", "currency", "status", "theme"]
    missing_req = [c for c in required if c not in hm]
    if missing_req:
        raise RuntimeError(f"{raw_tab} missing required headers: {missing_req}")

    ingest_col = pick_ingest_header(hm)

    # Dedupe by deal_id
    existing_ids = read_existing_deal_ids(ws_raw, hm, limit_rows=4000)

    # Build date candidates
    date_candidates = build_date_candidates(window_min, window_max, k_dates)

    inserted_rows: List[List[Any]] = []
    searches_done = 0
    no_offer_skips = 0
    dedupe_skips = 0

    def make_row_template() -> List[Any]:
        # Row length must match sheet header width exactly
        return [""] * len(raw_headers)

    def set_cell(row: List[Any], col: str, value: Any) -> None:
        if col in hm:
            row[hm[col]] = value

    # Primary lane
    for dest in top_dests:
        for origin in origins:
            for depart_dt in date_candidates:
                if searches_done >= primary_budget:
                    break
                ret_dt = depart_dt + timedelta(days=trip_len)

                searches_done += 1
                log(
                    f"🔎 Search {searches_done}/{primary_budget} [PRIMARY] {origin}→{dest.destination_iata} | weight={dest.weight:.2f} | max_conn={max_conn_primary} | cabin={cabin} | trip={trip_len}d | window={window_min}-{window_max}"
                )

                offer = duffel_offer_request(
                    origin=origin,
                    dest=dest.destination_iata,
                    depart=depart_dt,
                    ret=ret_dt,
                    cabin=cabin,
                    max_connections=max_conn_primary,
                )

                time.sleep(sleep_s)

                if not offer:
                    no_offer_skips += 1
                    continue

                deal_id = (offer.get("id") or "").strip()
                if not deal_id:
                    # If Duffel ever returns no offer id, skip (shouldn’t)
                    no_offer_skips += 1
                    continue

                if deal_id in existing_ids:
                    dedupe_skips += 1
                    continue

                # Basic fields
                currency = (offer.get("total_currency") or "").strip().upper()
                total_amount = offer.get("total_amount")

                # Insert row
                r = make_row_template()
                set_cell(r, "deal_id", deal_id)
                set_cell(r, "origin_iata", origin)
                set_cell(r, "destination_iata", dest.destination_iata)
                set_cell(r, "outbound_date", depart_dt.isoformat())
                set_cell(r, "return_date", ret_dt.isoformat())
                set_cell(r, "currency", currency or "GBP")
                set_cell(r, "price_gbp", total_amount)  # keep raw; scorer/rdv can convert if needed
                set_cell(r, "status", "NEW")
                set_cell(r, "theme", theme_today)

                # Numeric ingest timestamp (epoch seconds)
                now_epoch = int(time.time())
                set_cell(r, ingest_col, now_epoch)

                inserted_rows.append(r)
                existing_ids.add(deal_id)

                if len(inserted_rows) >= max_inserts:
                    break

            if searches_done >= primary_budget or len(inserted_rows) >= max_inserts:
                break
        if searches_done >= primary_budget or len(inserted_rows) >= max_inserts:
            break

    # Secondary lane (optional): widen feasibility slightly
    if secondary_budget > 0 and len(inserted_rows) < max_inserts:
        # Secondary adjustments: slightly earlier window +1 connections (bounded)
        sec_window_min = max(0, window_min - 7)
        sec_window_max = window_max
        sec_dates = build_date_candidates(sec_window_min, sec_window_max, 1)
        max_conn_secondary = max_conn_primary + 1

        # pick next best destination not already tried heavily
        sec_dest = top_dests[0]  # highest weight again (minimal system)
        sec_origin = origins[0]

        searches_done_sec = 0
        for depart_dt in sec_dates:
            if searches_done_sec >= secondary_budget:
                break
            ret_dt = depart_dt + timedelta(days=trip_len)
            searches_done_sec += 1
            log(
                f"🔎 Search {searches_done_sec}/{secondary_budget} [SECONDARY] {sec_origin}→{sec_dest.destination_iata} | weight={sec_dest.weight:.2f} | max_conn={max_conn_secondary} | cabin={cabin} | trip={trip_len}d | window={sec_window_min}-{sec_window_max}"
            )

            offer = duffel_offer_request(
                origin=sec_origin,
                dest=sec_dest.destination_iata,
                depart=depart_dt,
                ret=ret_dt,
                cabin=cabin,
                max_connections=max_conn_secondary,
            )

            time.sleep(sleep_s)

            if not offer:
                no_offer_skips += 1
                continue

            deal_id = (offer.get("id") or "").strip()
            if not deal_id:
                no_offer_skips += 1
                continue
            if deal_id in existing_ids:
                dedupe_skips += 1
                continue

            currency = (offer.get("total_currency") or "").strip().upper()
            total_amount = offer.get("total_amount")

            r = make_row_template()
            set_cell(r, "deal_id", deal_id)
            set_cell(r, "origin_iata", sec_origin)
            set_cell(r, "destination_iata", sec_dest.destination_iata)
            set_cell(r, "outbound_date", depart_dt.isoformat())
            set_cell(r, "return_date", ret_dt.isoformat())
            set_cell(r, "currency", currency or "GBP")
            set_cell(r, "price_gbp", total_amount)
            set_cell(r, "status", "NEW")
            set_cell(r, "theme", theme_today)
            set_cell(r, ingest_col, int(time.time()))

            inserted_rows.append(r)
            existing_ids.add(deal_id)

    if inserted_rows:
        append_rows(ws_raw, inserted_rows)
        log(f"✅ Inserted {len(inserted_rows)} row(s) into {raw_tab}.")
    else:
        log("⚠️ No rows inserted.")

    log(
        f"SUMMARY: searches={searches_done} inserted={len(inserted_rows)} dedupe_skips={dedupe_skips} no_offer_skips={no_offer_skips}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
