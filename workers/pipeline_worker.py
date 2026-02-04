# ============================================================
# TRAVELTXTER — DEAL FEEDER v5.1 (Economy-Only, Pi-Optimised)
#
# CANONICAL RULES (V5 economy-first):
# - Optimise Pi = Fi × Vi (publishable probability)
# - Vi lives in CONFIG as value_score
# - Theme locked to OPS_MASTER!B5
# - 90/10 = feasibility drift WITHIN theme only
# - Economy-only for now (premium deferred)
# - Skip luxury_value while premium layer is not implemented
# - RAW_DEALS is sole writable source of truth
# ============================================================

from __future__ import annotations

import os
import json
import hashlib
import datetime as dt
from typing import Dict, List, Any, Tuple, Optional

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
# Environment helpers
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
# Google Sheets helpers
# ------------------------
def gspread_client():
    raw = env("GCP_SA_JSON_ONE_LINE") or env("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")

    # Try as-is first (valid one-line JSON with \\n)
    try:
        info = json.loads(raw)
    except json.JSONDecodeError:
        # Fallback: if env system expanded newlines
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
        row = r + [""] * (len(header) - len(r))
        out.append(dict(zip(header, row)))
    return out


def get_header(ws) -> List[str]:
    return ws.row_values(1)


def a1(ws, cell: str) -> str:
    v = ws.acell(cell).value
    return (v or "").strip()


# ------------------------
# Duffel
# ------------------------
def duffel_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {env('DUFFEL_API_KEY')}",
        "Duffel-Version": "v2",
        "Content-Type": "application/json",
    }


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

    if included_airlines_csv.strip():
        body["data"]["included_airlines"] = [
            x.strip() for x in included_airlines_csv.split(",") if x.strip()
        ]

    r = requests.post(
        "https://api.duffel.com/air/offer_requests",
        headers=duffel_headers(),
        json=body,
        timeout=30,
    )
    if not r.ok:
        return None
    return r.json()


def best_offer_payload(resp: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    data = (resp or {}).get("data") or {}
    offers = data.get("offers") or []
    if not offers:
        return None
    # Duffel usually returns sorted; we take first as "best" for now.
    return offers[0]


def offer_stops(offer: Dict[str, Any]) -> int:
    # Stops = segments - 1 on outbound slice
    try:
        slices = offer.get("slices") or []
        if not slices:
            return 0
        segs = slices[0].get("segments") or []
        return max(0, len(segs) - 1)
    except Exception:
        return 0


# ------------------------
# ZTB (constraints)
# ------------------------
def ztb_connection_tolerance(ztb_rows: List[Dict[str, str]], theme: str) -> int:
    # default: 0 connections if not found
    for r in ztb_rows:
        if (r.get("theme") or "").strip().lower() == theme.lower():
            try:
                return int(float(r.get("connection_tolerance") or "0"))
            except Exception:
                return 0
    return 0


# ------------------------
# Deterministic date candidates
# ------------------------
def date_candidates(today: dt.date, k: int, base_days_ahead: int, step_days: int) -> List[dt.date]:
    out = []
    for i in range(max(1, k)):
        out.append(today + dt.timedelta(days=base_days_ahead + i * step_days))
    return out


# ------------------------
# Main
# ------------------------
def main() -> int:
    # Caps / knobs
    MAX_SEARCHES = env_int("DUFFEL_MAX_SEARCHES_PER_RUN", 12)
    PRIMARY_PCT = env_int("SLOT_PRIMARY_PCT", 90)  # 90 means 90/10
    DESTS_PER_RUN = env_int("DUFFEL_ROUTES_PER_RUN", 4)
    ORIGINS_PER_DEST = env_int("ORIGINS_PER_DEST", 3)
    K_DATES_PER_DEST = env_int("K_DATES_PER_DEST", 3)

    BASE_DAYS_AHEAD = env_int("BASE_DAYS_AHEAD", 30)
    DATE_STEP_DAYS = env_int("DATE_STEP_DAYS", 14)

    # Economy-first lock
    CABIN_CLASS = "economy"

    gc = gspread_client()
    sh = gc.open_by_key(env("SPREADSHEET_ID"))

    RAW = sh.worksheet("RAW_DEALS")
    CFG = sh.worksheet("CONFIG")
    ZTB = sh.worksheet("ZONE_THEME_BENCHMARKS")
    OPS = sh.worksheet("OPS_MASTER")
    RCM = sh.worksheet("ROUTE_CAPABILITY_MAP")
    IATA = sh.worksheet("IATA_MASTER")

    theme = a1(OPS, "B5")
    log(f"🎯 Theme of day: {theme}")

    # Economy-first: pause luxury_value until premium layer exists
    if theme.lower() == "luxury_value":
        log("⏸️ luxury_value skipped (economy-first lock). Premium layer deferred.")
        return 0

    # Load ZTB constraints (connection tolerance)
    ztb_rows = records(ZTB)
    max_conn_primary = ztb_connection_tolerance(ztb_rows, theme)
    # Secondary lane: feasibility drift within theme ( +1 connection, capped at 2 )
    max_conn_secondary = min(max_conn_primary + 1, 2)

    primary_budget = int(MAX_SEARCHES * (PRIMARY_PCT / 100.0))
    primary_budget = max(0, min(MAX_SEARCHES, primary_budget))
    secondary_budget = MAX_SEARCHES - primary_budget

    log(f"CAPS: MAX_SEARCHES={MAX_SEARCHES} | PRIMARY={primary_budget} | SECONDARY={secondary_budget} | CABIN={CABIN_CLASS}")
    log(f"ZTB: max_conn_primary={max_conn_primary} | max_conn_secondary={max_conn_secondary}")

    # Build geo map for city/country fill (optional, safe)
    geo = { (r.get("iata_code") or "").strip(): r for r in records(IATA) if (r.get("iata_code") or "").strip() }

    # Build destination -> origins list (stable ordering, not sets)
    rcm_rows = records(RCM)
    dest_to_origins: Dict[str, List[str]] = {}
    for r in rcm_rows:
        if (r.get("enabled") or "").strip().lower() not in ("true", "1", "yes"):
            continue
        o = (r.get("origin_iata") or "").strip()
        d = (r.get("destination_iata") or "").strip()
        if not o or not d:
            continue
        dest_to_origins.setdefault(d, [])
        if o not in dest_to_origins[d]:
            dest_to_origins[d].append(o)

    # Load CONFIG candidates theme-locked (primary_theme only, per your current contract)
    cfg_rows = [
        r for r in records(CFG)
        if (r.get("enabled") or "").strip().lower() in ("true", "1", "yes")
        and (r.get("primary_theme") or "").strip().lower() == theme.lower()
    ]
    if not cfg_rows:
        log("❌ No CONFIG rows for theme (primary_theme match).")
        return 0

    # Compute Pi = Fi × Vi
    scored: List[Dict[str, Any]] = []
    for r in cfg_rows:
        Fi = fnum(r.get("search_weight"), 0.0)
        Vi = fnum(r.get("value_score"), 0.5)
        Pi = Fi * Vi

        rr = dict(r)
        rr["_Fi"] = Fi
        rr["_Vi"] = Vi
        rr["_Pi"] = Pi
        rr["_queries"] = 0
        scored.append(rr)

    # Take top destination rows by Pi (but don’t assume unique dests in CONFIG)
    scored.sort(key=lambda x: x["_Pi"], reverse=True)

    # Reduce to unique destinations (keep best row per destination)
    best_by_dest: Dict[str, Dict[str, Any]] = {}
    for r in scored:
        dest = (r.get("destination_iata") or "").strip()
        if not dest:
            continue
        if dest not in best_by_dest:
            best_by_dest[dest] = r

    candidates = list(best_by_dest.values())
    candidates.sort(key=lambda x: x["_Pi"], reverse=True)

    # Limit destination set for this run
    candidates = candidates[:max(1, DESTS_PER_RUN)]

    # Precompute dates
    today = dt.date.today()
    outs = date_candidates(today, K_DATES_PER_DEST, BASE_DAYS_AHEAD, DATE_STEP_DAYS)

    # Prepare insert rows
    raw_header = get_header(RAW)
    out_rows: List[Dict[str, str]] = []

    searches = 0
    primary_used = 0
    secondary_used = 0

    # Variety guard: no repeat (origin, dest, out_date)
    used_triplets: set[Tuple[str, str, str]] = set()

    def pick_next_dest() -> Optional[Dict[str, Any]]:
        # Diminishing returns per-destination
        candidates.sort(
            key=lambda r: r["_Pi"] * ((1.0 - r["_Pi"]) ** int(r["_queries"])),
            reverse=True,
        )
        if not candidates:
            return None
        return candidates[0]

    while searches < MAX_SEARCHES:
        lane = "PRIMARY" if primary_used < primary_budget else "SECONDARY"
        max_conn = max_conn_primary if lane == "PRIMARY" else max_conn_secondary

        r = pick_next_dest()
        if not r:
            break

        dest = (r.get("destination_iata") or "").strip()
        if not dest:
            r["_queries"] += 1
            continue

        # Hard cap queries per destination (variety): max 3
        if int(r["_queries"]) >= 3:
            # If top dest exhausted, remove it and continue
            candidates = [x for x in candidates if x.get("destination_iata") != dest]
            continue

        origins = dest_to_origins.get(dest, [])[:max(1, ORIGINS_PER_DEST)]
        if not origins:
            r["_queries"] += 1
            continue

        # Deterministic origin + date selection
        q = int(r["_queries"])
        origin = origins[q % len(origins)]
        out_date = outs[q % len(outs)]
        trip_len = int(float((r.get("trip_length_days") or "4").strip() or "4"))
        trip_len = max(2, min(21, trip_len))  # sanity bounds
        ret_date = out_date + dt.timedelta(days=trip_len)

        key = (origin, dest, out_date.isoformat())
        if key in used_triplets:
            r["_queries"] += 1
            continue
        used_triplets.add(key)

        log(f"🔎 Search {searches+1}/{MAX_SEARCHES} [{lane}] {origin} → {dest} | Pi={r['_Pi']:.2f} | max_conn={max_conn} | cabin={CABIN_CLASS}")

        resp = duffel_offer_request(
            origin=origin,
            dest=dest,
            out_date=out_date.isoformat(),
            ret_date=ret_date.isoformat(),
            max_connections=max_conn,
            cabin_class=CABIN_CLASS,
            included_airlines_csv=(r.get("included_airlines") or "").strip(),
        )

        searches += 1
        if lane == "PRIMARY":
            primary_used += 1
        else:
            secondary_used += 1

        r["_queries"] += 1

        offer = best_offer_payload(resp or {})
        if not offer:
            continue

        # Only accept GBP offers (economy-first pipeline assumes GBP)
        total_currency = (offer.get("total_currency") or "").strip().upper()
        total_amount = (offer.get("total_amount") or "").strip()
        if total_currency and total_currency != "GBP":
            log(f"↪️ Skip non-GBP offer: {total_currency} {total_amount}")
            continue

        try:
            price = float(total_amount)
        except Exception:
            continue

        # Derive simple connection metadata
        stops = offer_stops(offer)
        connection_type = "direct" if stops == 0 else "via"

        # Fill city/country from IATA_MASTER when possible
        oc = geo.get(origin, {}) or {}
        dc = geo.get(dest, {}) or {}

        now_utc = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        deal_id = hashlib.md5(f"{origin}|{dest}|{out_date.isoformat()}|{ret_date.isoformat()}|{CABIN_CLASS}|{max_conn}".encode()).hexdigest()[:12]

        out_rows.append({
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
        })

        # Respect MAX_INSERTS-like behaviour if you want later; for now keep bounded by searches.

    if not out_rows:
        log("⚠️ No deals found (no offers returned within caps).")
        return 0

    # Append rows in RAW_DEALS header order
    values = []
    for row in out_rows:
        values.append([row.get(h, "") for h in raw_header])

    RAW.append_rows(values, value_input_option="RAW")
    log(f"✅ Inserted {len(out_rows)} row(s) into RAW_DEALS.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
