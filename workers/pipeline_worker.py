# ============================================================
# TRAVELTXTER — DEAL FEEDER v5.0 (Pi-Optimised, Theme-Locked)
#
# CANONICAL RULES:
# - Optimise Pi = Fi × Vi (publishable probability)
# - Vi lives in CONFIG as value_score
# - Theme locked to OPS_MASTER!B5
# - 90/10 = feasibility drift WITHIN theme
# - No AM/PM logic (render decides)
# - RAW_DEALS is sole writable source
# ============================================================

from __future__ import annotations

import os, json, math, hashlib, datetime as dt
from typing import Dict, List, Any
import requests, gspread
from google.oauth2.service_account import Credentials

# ------------------------
# Logging
# ------------------------
def log(msg: str):
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

def f(v, d=0.0): 
    try: return float(v)
    except Exception: return d

# ------------------------
# Google Sheets
# ------------------------
def gspread_client():
    raw = env("GCP_SA_JSON_ONE_LINE") or env("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")

    # Try as-is first (works for proper one-line JSON with \\n)
    try:
        info = json.loads(raw)
    except json.JSONDecodeError:
        # Fallback for secrets where newlines were already expanded
        info = json.loads(raw.replace("\n", "\\n"))

    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return gspread.authorize(creds)

# ------------------------
# Duffel
# ------------------------
def duffel_headers():
    return {
        "Authorization": f"Bearer {env('DUFFEL_API_KEY')}",
        "Duffel-Version": "v2",
        "Content-Type": "application/json",
    }

def duffel_search(o, d, out, ret, max_conn, cabin, airlines):
    body = {
        "data": {
            "slices": [
                {"origin": o, "destination": d, "departure_date": out},
                {"origin": d, "destination": o, "departure_date": ret},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": cabin,
            "max_connections": max_conn,
        }
    }
    if airlines:
        body["data"]["included_airlines"] = airlines.split(",")

    r = requests.post(
        "https://api.duffel.com/air/offer_requests",
        headers=duffel_headers(),
        json=body,
        timeout=30,
    )
    return r.json() if r.ok else None

# ------------------------
# Main
# ------------------------
def main():
    gc = gspread_client()
    sh = gc.open_by_key(env("SPREADSHEET_ID"))

    RAW = sh.worksheet("RAW_DEALS")
    CFG = sh.worksheet("CONFIG")
    ZTB = sh.worksheet("ZONE_THEME_BENCHMARKS")
    OPS = sh.worksheet("OPS_MASTER")
    RCM = sh.worksheet("ROUTE_CAPABILITY_MAP")
    IATA = sh.worksheet("IATA_MASTER")

    MAX_SEARCHES = env_int("DUFFEL_MAX_SEARCHES_PER_RUN", 12)
    PRIMARY_PCT = env_int("SLOT_PRIMARY_PCT", 90)

    # ------------------------
    # Theme
    # ------------------------
    theme = OPS.acell("B5").value.strip()
    log(f"🎯 Theme of day: {theme}")

    # ------------------------
    # CONFIG rows (theme locked)
    # ------------------------
    cfg = [
        r for r in records(CFG)
        if r.get("enabled","").lower() in ("true","1","yes")
        and r.get("primary_theme","").lower() == theme.lower()
    ]

    if not cfg:
        log("❌ No CONFIG rows for theme")
        return 0

    # ------------------------
    # Compute Pi per destination
    # ------------------------
    scored = []
    for r in cfg:
        Fi = f(r.get("search_weight"), 0.0)
        Vi = f(r.get("value_score"), 0.5)

        Pi = Fi * Vi
        r["_Fi"] = Fi
        r["_Vi"] = Vi
        r["_Pi"] = Pi
        r["_queries"] = 0
        scored.append(r)

    scored.sort(key=lambda r: r["_Pi"], reverse=True)

    # ------------------------
    # Feeder loop
    # ------------------------
    routes = {
        (r["origin_iata"], r["destination_iata"])
        for r in records(RCM)
        if r.get("enabled","").lower() in ("true","1")
    }

    geo = {
        r["iata_code"]: r
        for r in records(IATA)
        if r.get("iata_code")
    }

    out_rows = []
    searches = 0
    today = dt.date.today()

    while searches < MAX_SEARCHES:
        # diminishing returns
        scored.sort(
            key=lambda r: r["_Pi"] * ((1 - r["_Pi"]) ** r["_queries"]),
            reverse=True,
        )

        r = scored[0]
        if r["_queries"] >= 3:
            break

        dest = r["destination_iata"]
        origins = [o for (o,d) in routes if d == dest][:3]
        if not origins:
            r["_queries"] += 1
            continue

        origin = origins[r["_queries"] % len(origins)]
        out = today + dt.timedelta(days=30 + r["_queries"]*10)
        ret = out + dt.timedelta(days=int(r.get("trip_length_days",4)))

        log(f"🔎 Search {searches+1}: {origin} → {dest} | Pi={r['_Pi']:.2f}")

        resp = duffel_search(
            origin, dest,
            out.isoformat(), ret.isoformat(),
            0, "economy",
            r.get("included_airlines","")
        )

        searches += 1
        r["_queries"] += 1

        if not resp or not resp.get("data",{}).get("offers"):
            continue

        price = float(resp["data"]["offers"][0]["total_amount"])

        out_rows.append({
            "status": "NEW",
            "deal_id": hashlib.md5(f"{origin}{dest}{out}".encode()).hexdigest()[:10],
            "price_gbp": f"{price:.2f}",
            "origin_iata": origin,
            "destination_iata": dest,
            "outbound_date": out.isoformat(),
            "return_date": ret.isoformat(),
            "deal_theme": theme,
            "ingested_at_utc": dt.datetime.utcnow().isoformat()+"Z",
            "trip_length_days": str((ret-out).days),
        })

    if out_rows:
        RAW.append_rows(
            [[r.get(h,"") for h in RAW.row_values(1)] for r in out_rows],
            value_input_option="RAW"
        )
        log(f"✅ Inserted {len(out_rows)} deals")

    else:
        log("⚠️ No deals found")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
