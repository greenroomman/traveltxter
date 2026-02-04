# ============================================================
# TRAVELTXTER — FEEDER v5.2 (Economy-Only, Theme-Locked, Pi Ranked, 90/10)
#
# Canonical behaviour:
# - Reads theme from OPS_MASTER!B5 (theme-locked)
# - Economy-only searches (premium deferred)
# - Destination ranking optimises Pi = Fi × Vi (CONFIG: search_weight × value_score)
# - ZTB provides connection_tolerance (PRIMARY lane); SECONDARY lane drifts +1 connection (capped)
# - 90/10 implemented as PRIMARY vs SECONDARY query budgets
# - Airline restriction is OPTIONAL; "ANY/ALL/*" is treated as blank (no restriction)
# - RAW_DEALS is sole writable source of truth
# - Inserts are capped + deduped (deal_id, per-origin, per-route) to avoid sheet spam
#
# Notes:
# - Does NOT write to RAW_DEALS_VIEW (read-only)
# - Does NOT assume tab names; uses env where provided, defaults to canonical names
# ============================================================

from __future__ import annotations

import os
import json
import time
import hashlib
import datetime as dt
from typing import Dict, List, Any, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials


# ------------------------
# Logging
# ------------------------
def log(msg: str) -> None:
    ts = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    print(f"{ts} | {msg}", flush=True)


# ------------------------
# Env helpers
# ------------------------
def env(k: str, d: str = "") -> str:
    return (os.getenv(k, d) or "").strip()


def env_int(k: str, d: int) -> int:
    try:
        return int(env(k))
    except Exception:
        return d


def fnum(v: Any, d: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return d


# ------------------------
# Sheets client
# ------------------------
def gspread_client():
    raw = env("GCP_SA_JSON_ONE_LINE") or env("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")

    # Try as-is first (valid one-line JSON with \\n)
    try:
        info = json.loads(raw)
    except json.JSONDecodeError:
        # Fallback: if platform expanded newlines
        info = json.loads(raw.replace("\n", "\\n"))

    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)


def records(ws) -> List[Dict[str, str]]:
    rows = ws.get_all_values()
    if not rows:
        return []
    header = rows[0]
    out: List[Dict[str, str]] = []
    for r in rows[1:]:
        rr = r + [""] * (len(header) - len(r))
        out.append(dict(zip(header, rr)))
    return out


def header(ws) -> List[str]:
    return ws.row_values(1)


def a1(ws, cell: str) -> str:
    v = ws.acell(cell).value
    return (v or "").strip()


def open_ws(sh, name: str):
    try:
        return sh.worksheet(name)
    except Exception as e:
        raise RuntimeError(f"WorksheetNotFound: '{name}'") from e


# ------------------------
# Duffel
# ------------------------
def duffel_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {env('DUFFEL_API_KEY')}",
        "Duffel-Version": "v2",
        "Content-Type": "application/json",
    }


def normalise_airlines_csv(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    if s.upper() in ("ANY", "ALL", "*", "NONE", "NULL"):
        return ""
    # keep comma-separated airline IATA codes
    parts = [p.strip().upper() for p in s.split(",") if p.strip()]
    # Defensive: drop obviously invalid tokens
    parts = [p for p in parts if 1 <= len(p) <= 3 and p.isalnum()]
    return ",".join(parts)


def duffel_offer_request(
    origin: str,
    dest: str,
    out_date: str,
    ret_date: str,
    max_connections: int,
    cabin_class: str,
    included_airlines_csv: str = "",
) -> Optional[Dict[str, Any]]:
    body: Dict[str, Any] = {
        "data": {
            "slices": [
                {"origin": origin, "destination": dest, "departure_date": out_date},
                {"origin": dest, "destination": origin, "departure_date": ret_date},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": cabin_class,
            "max_connections": int(max_connections),
        }
    }

    air = normalise_airlines_csv(included_airlines_csv)
    if air:
        body["data"]["included_airlines"] = [x for x in air.split(",") if x]

    r = requests.post(
        "https://api.duffel.com/air/offer_requests",
        headers=duffel_headers(),
        json=body,
        timeout=35,
    )
    if not r.ok:
        return None
    return r.json()


def best_offer(resp: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    data = (resp or {}).get("data") or {}
    offers = data.get("offers") or []
    return offers[0] if offers else None


def offer_stops(offer: Dict[str, Any]) -> int:
    try:
        slices = offer.get("slices") or []
        if not slices:
            return 0
        segs = slices[0].get("segments") or []
        return max(0, len(segs) - 1)
    except Exception:
        return 0


# ------------------------
# ZTB helpers
# ------------------------
def ztb_row(ztb_rows: List[Dict[str, str]], theme: str) -> Dict[str, str]:
    for r in ztb_rows:
        if (r.get("theme") or "").strip().lower() == theme.lower():
            return r
    return {}


def ztb_connection_tolerance(ztb_rows: List[Dict[str, str]], theme: str) -> int:
    r = ztb_row(ztb_rows, theme)
    try:
        return int(float(r.get("connection_tolerance") or "0"))
    except Exception:
        return 0


# ------------------------
# Deterministic date candidates (simple + stable)
# ------------------------
def date_candidates(today: dt.date, k: int, base_days_ahead: int, step_days: int) -> List[dt.date]:
    out: List[dt.date] = []
    k = max(1, int(k))
    for i in range(k):
        out.append(today + dt.timedelta(days=base_days_ahead + i * step_days))
    return out


# ------------------------
# Insert governance (caps + dedupe)
# ------------------------
def make_deal_id(origin: str, dest: str, out_date: str, ret_date: str) -> str:
    key = f"{origin}|{dest}|{out_date}|{ret_date}|econ"
    return hashlib.md5(key.encode("utf-8")).hexdigest()[:12]


def safe_get(d: Dict[str, str], *keys: str) -> str:
    for k in keys:
        if k in d and (d.get(k) or "").strip():
            return (d.get(k) or "").strip()
    return ""


# ------------------------
# Main
# ------------------------
def main() -> int:
    # ===== Caps / knobs =====
    MAX_SEARCHES = env_int("DUFFEL_MAX_SEARCHES_PER_RUN", 12)
    MAX_INSERTS = env_int("DUFFEL_MAX_INSERTS", 50)
    MAX_PER_ORIGIN = env_int("DUFFEL_MAX_INSERTS_PER_ORIGIN", 15)
    MAX_PER_ROUTE = env_int("DUFFEL_MAX_INSERTS_PER_ROUTE", 5)

    DESTS_PER_RUN = env_int("DUFFEL_ROUTES_PER_RUN", 4)
    ORIGINS_PER_DEST = env_int("ORIGINS_PER_DEST", 3)
    K_DATES_PER_DEST = env_int("K_DATES_PER_DEST", 3)

    PRIMARY_PCT = env_int("SLOT_PRIMARY_PCT", 90)  # 90 means 90/10
    BASE_DAYS_AHEAD = env_int("BASE_DAYS_AHEAD", 30)
    DATE_STEP_DAYS = env_int("DATE_STEP_DAYS", 14)

    CABIN_CLASS = "economy"  # V5 economy-only lock

    # ===== Tabs (do not assume env blanks are safe) =====
    RAW_TAB = env("RAW_DEALS_TAB", "RAW_DEALS")
    CFG_TAB = env("FEEDER_CONFIG_TAB", "CONFIG")
    ZTB_TAB = env("ZTB_TAB", "ZONE_THEME_BENCHMARKS")
    OPS_TAB = env("OPS_MASTER_TAB", "OPS_MASTER")
    RCM_TAB = env("ROUTE_CAPABILITY_TAB", "ROUTE_CAPABILITY_MAP")
    IATA_TAB = env("IATA_MASTER_TAB", "IATA_MASTER")  # env may be blank; default is canonical

    # ===== Connect =====
    gc = gspread_client()
    sh = gc.open_by_key(env("SPREADSHEET_ID"))

    RAW = open_ws(sh, RAW_TAB)
    CFG = open_ws(sh, CFG_TAB)
    ZTB = open_ws(sh, ZTB_TAB)
    OPS = open_ws(sh, OPS_TAB)
    RCM = open_ws(sh, RCM_TAB)
    IATA = open_ws(sh, IATA_TAB)

    theme = a1(OPS, "B5")
    log(f"🎯 Theme of day: {theme}")

    # ===== ZTB constraints =====
    ztb_rows = records(ZTB)
    max_conn_primary = ztb_connection_tolerance(ztb_rows, theme)
    max_conn_secondary = min(max_conn_primary + 1, 2)  # controlled feasibility drift

    primary_budget = int(MAX_SEARCHES * (PRIMARY_PCT / 100.0))
    primary_budget = max(0, min(MAX_SEARCHES, primary_budget))
    secondary_budget = MAX_SEARCHES - primary_budget

    log(
        f"CAPS: MAX_SEARCHES={MAX_SEARCHES} | MAX_INSERTS={MAX_INSERTS} | "
        f"PER_ORIGIN={MAX_PER_ORIGIN} | PER_ROUTE={MAX_PER_ROUTE} | "
        f"DESTS_PER_RUN={DESTS_PER_RUN} | ORIGINS_PER_DEST={ORIGINS_PER_DEST} | "
        f"K_DATES_PER_DEST={K_DATES_PER_DEST} | PRIMARY/SECONDARY={primary_budget}/{secondary_budget}"
    )
    log(f"ZTB: max_conn_primary={max_conn_primary} | max_conn_secondary={max_conn_secondary} | cabin={CABIN_CLASS}")

    # ===== Geo map (city/country fill) =====
    geo = {(r.get("iata_code") or "").strip(): r for r in records(IATA) if (r.get("iata_code") or "").strip()}

    # ===== Route map: destination -> origins (stable order) =====
    dest_to_origins: Dict[str, List[str]] = {}
    for r in records(RCM):
        if (r.get("enabled") or "").strip().lower() not in ("true", "1", "yes"):
            continue
        o = (r.get("origin_iata") or "").strip()
        d = (r.get("destination_iata") or "").strip()
        if not o or not d:
            continue
        dest_to_origins.setdefault(d, [])
        if o not in dest_to_origins[d]:
            dest_to_origins[d].append(o)

    # ===== CONFIG candidates: theme-locked (primary_theme) =====
    cfg_all = records(CFG)
    cfg_rows = [
        r for r in cfg_all
        if (r.get("enabled") or "").strip().lower() in ("true", "1", "yes")
        and (r.get("primary_theme") or "").strip().lower() == theme.lower()
        and (r.get("destination_iata") or "").strip()
    ]
    if not cfg_rows:
        log("⚠️ No CONFIG rows for theme (primary_theme match).")
        return 0

    # ===== Build best row per destination by Pi =====
    best_by_dest: Dict[str, Dict[str, Any]] = {}
    for r in cfg_rows:
        dest = (r.get("destination_iata") or "").strip()
        Fi = fnum(safe_get(r, "search_weight", "feasibility_score", "Fi"), 0.0)
        Vi = fnum(safe_get(r, "value_score", "Vi"), 0.5)
        Pi = Fi * Vi

        rr: Dict[str, Any] = dict(r)
        rr["_Fi"] = Fi
        rr["_Vi"] = Vi
        rr["_Pi"] = Pi
        rr["_queries"] = 0

        if dest not in best_by_dest or Pi > float(best_by_dest[dest].get("_Pi", 0.0)):
            best_by_dest[dest] = rr

    candidates = list(best_by_dest.values())
    candidates.sort(key=lambda x: float(x.get("_Pi", 0.0)), reverse=True)
    candidates = candidates[:max(1, DESTS_PER_RUN)]

    # ===== Existing deal_ids for dedupe =====
    raw_hdr = header(RAW)
    deal_id_col = raw_hdr.index("deal_id") if "deal_id" in raw_hdr else -1
    existing_deal_ids = set()
    if deal_id_col >= 0:
        # pull the deal_id column only
        col_vals = RAW.col_values(deal_id_col + 1)[1:]
        existing_deal_ids = {v.strip() for v in col_vals if v and v.strip()}

    # ===== Date candidates =====
    today = dt.date.today()
    outs = date_candidates(today, K_DATES_PER_DEST, BASE_DAYS_AHEAD, DATE_STEP_DAYS)

    # ===== Insert counters (run-scoped governance) =====
    inserted = 0
    inserts_by_origin: Dict[str, int] = {}
    inserts_by_route: Dict[Tuple[str, str], int] = {}

    used_triplets: set[Tuple[str, str, str]] = set()

    def can_insert(origin_iata: str, dest_iata: str, deal_id: str) -> bool:
        if inserted >= MAX_INSERTS:
            return False
        if deal_id in existing_deal_ids:
            return False
        if inserts_by_origin.get(origin_iata, 0) >= MAX_PER_ORIGIN:
            return False
        if inserts_by_route.get((origin_iata, dest_iata), 0) >= MAX_PER_ROUTE:
            return False
        return True

    # ===== Search loop =====
    out_rows: List[Dict[str, str]] = []
    searches = 0
    primary_used = 0
    secondary_used = 0

    def pick_next_dest() -> Optional[Dict[str, Any]]:
        # Diminishing returns: prefer spreading across destinations
        candidates.sort(
            key=lambda r: float(r["_Pi"]) * ((1.0 - float(r["_Pi"])) ** int(r["_queries"])),
            reverse=True,
        )
        return candidates[0] if candidates else None

    while searches < MAX_SEARCHES and inserted < MAX_INSERTS:
        lane = "PRIMARY" if primary_used < primary_budget else "SECONDARY"
        max_conn = max_conn_primary if lane == "PRIMARY" else max_conn_secondary

        r = pick_next_dest()
        if not r:
            break

        dest = (r.get("destination_iata") or "").strip()
        if not dest:
            r["_queries"] += 1
            continue

        # cap queries per destination (variety)
        if int(r["_queries"]) >= 3:
            # remove exhausted destination
            candidates = [x for x in candidates if (x.get("destination_iata") or "").strip() != dest]
            continue

        origins = dest_to_origins.get(dest, [])[:max(1, ORIGINS_PER_DEST)]
        if not origins:
            r["_queries"] += 1
            continue

        q = int(r["_queries"])
        origin = origins[q % len(origins)]
        out_date = outs[q % len(outs)]

        # trip length: use CONFIG if present, else default 4
        trip_len = int(float((safe_get(r, "trip_length_days", "trip_days") or "4")))
        trip_len = max(2, min(21, trip_len))
        ret_date = out_date + dt.timedelta(days=trip_len)

        triplet = (origin, dest, out_date.isoformat())
        if triplet in used_triplets:
            r["_queries"] += 1
            continue
        used_triplets.add(triplet)

        deal_id = make_deal_id(origin, dest, out_date.isoformat(), ret_date.isoformat())

        # IMPORTANT: included_airlines may be "ANY" in CONFIG; normaliser makes it blank.
        included_airlines = safe_get(r, "included_airlines", "included_airlines_csv", "carriers")

        # Pre-insert gate (don’t waste Duffel on things we cannot insert)
        if not can_insert(origin, dest, deal_id):
            r["_queries"] += 1
            continue

        log(
            f"🔎 Search {searches+1}/{MAX_SEARCHES} [{lane}] {origin} → {dest} "
            f"| Pi={float(r['_Pi']):.2f} | max_conn={max_conn} | cabin={CABIN_CLASS}"
        )

        resp = duffel_offer_request(
            origin=origin,
            dest=dest,
            out_date=out_date.isoformat(),
            ret_date=ret_date.isoformat(),
            max_connections=max_conn,
            cabin_class=CABIN_CLASS,
            included_airlines_csv=included_airlines,
        )

        searches += 1
        if lane == "PRIMARY":
            primary_used += 1
        else:
            secondary_used += 1

        r["_queries"] += 1

        offer = best_offer(resp or {})
        if not offer:
            continue

        total_currency = (offer.get("total_currency") or "").strip().upper()
        total_amount = (offer.get("total_amount") or "").strip()

        # Economy-only pipeline expects GBP; if Duffel returns other currency, skip safely.
        if total_currency and total_currency != "GBP":
            continue

        try:
            price = float(total_amount)
        except Exception:
            continue

        stops = offer_stops(offer)
        connection_type = "direct" if stops == 0 else "via"

        oc = geo.get(origin, {}) or {}
        dc = geo.get(dest, {}) or {}

        now_utc = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

        row: Dict[str, str] = {
            "status": "NEW",
            "deal_id": deal_id,
            "price_gbp": f"{price:.2f}",
            "origin_city": (oc.get("city") or "").strip(),
            "origin_iata": origin,
            "origin_country": (oc.get("country") or "").strip(),
            "destination_country": (dc.get("country") or "").strip(),
            "destination_city": (dc.get("city") or "").strip(),
            "destination_iata": dest,
            "outbound_date": out_date.isoformat(),
            "return_date": ret_date.isoformat(),
            "stops": str(stops),
            "deal_theme": theme,
            "ingested_at_utc": now_utc,
            "created_utc": now_utc,
            "timestamp": now_utc,
            "cabin_class": CABIN_CLASS,
            "connection_type": connection_type,
            "trip_length_days": str(trip_len),
        }

        # Final insert gate (dedupe + caps)
        if not can_insert(origin, dest, deal_id):
            continue

        out_rows.append(row)
        inserted += 1
        existing_deal_ids.add(deal_id)
        inserts_by_origin[origin] = inserts_by_origin.get(origin, 0) + 1
        inserts_by_route[(origin, dest)] = inserts_by_route.get((origin, dest), 0) + 1

    if not out_rows:
        log("⚠️ No rows inserted (no publishable offers within caps).")
        return 0

    # Append in RAW_DEALS header order
    values: List[List[str]] = []
    for r in out_rows:
        values.append([r.get(h, "") for h in raw_hdr])

    RAW.append_rows(values, value_input_option="RAW")
    log(f"✅ Inserted {len(out_rows)} row(s) into {RAW_TAB}.")
    log(f"SUMMARY: searches={searches} | primary={primary_used} | secondary={secondary_used} | inserted={len(out_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
