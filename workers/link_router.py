#!/usr/bin/env python3
"""
TravelTxter V4.5x — link_router.py (VIP booking link routing)

Purpose:
- Populate booking_link_vip for rows that need it (idempotent).
- Uses Duffel Links session URL for eligible short-haul/direct deals.
- Falls back to existing affiliate_url if not eligible or any error occurs.
- Never changes statuses.

Reads:
- RAW_DEALS rows (any status) where booking_link_vip is blank and affiliate_url exists or Duffel can be used.

Writes:
- booking_link_vip
- affiliate_source  (duffel_links / skyscanner / existing)
- (optional) render_response_snippet is not used here

Env required:
- SPREADSHEET_ID
- GCP_SA_JSON_ONE_LINE
- RAW_DEALS_TAB (default RAW_DEALS)

Duffel Links (optional):
- DUFFEL_API_KEY
- REDIRECT_BASE_URL   (must be a real URL base, e.g. https://greenroomman.pythonanywhere.com)

Limits / controls:
- MAX_LINK_ROWS_PER_RUN (default 5)
- DUFFEL_LINKS_PRICE_CAP_GBP (default 120)
- DUFFEL_LINKS_REQUIRE_DIRECT (default 1)

Eligibility:
- stops == 0 (if require direct)
- price_gbp <= cap
- origin_iata + destination_iata exist
"""

from __future__ import annotations

import os
import json
import datetime as dt
from typing import Dict, Any, List, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# Logging
# ============================================================

def utcnow() -> dt.datetime:
    return dt.datetime.utcnow()

def ts() -> str:
    return utcnow().replace(microsecond=0).isoformat() + "Z"

def log(msg: str) -> None:
    print(f"{ts()} | {msg}", flush=True)


# ============================================================
# Env
# ============================================================

def env_str(k: str, default: str = "") -> str:
    return os.environ.get(k, default).strip()

def env_int(k: str, default: int) -> int:
    try:
        return int(env_str(k, str(default)))
    except Exception:
        return default

def env_float(k: str, default: float) -> float:
    try:
        return float(env_str(k, str(default)))
    except Exception:
        return default

def clean_url(u: str) -> str:
    return (u or "").strip().replace(" ", "")


# ============================================================
# Sheets
# ============================================================

def get_client() -> gspread.Client:
    sa = env_str("GCP_SA_JSON_ONE_LINE") or env_str("GCP_SA_JSON")
    if not sa:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE (recommended) or GCP_SA_JSON")
    try:
        info = json.loads(sa)
    except json.JSONDecodeError:
        info = json.loads(sa.replace("\\n", "\n"))

    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)

def col_letter(n1: int) -> str:
    s = ""
    n = n1
    while n:
        n, rr = divmod(n - 1, 26)
        s = chr(65 + rr) + s
    return s

def a1(rownum: int, col0: int) -> str:
    return f"{col_letter(col0 + 1)}{rownum}"


# ============================================================
# Duffel Links
# ============================================================

DUFFEL_LINKS_URL = "https://api.duffel.com/air/order_links"

def parse_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(str(x).replace("£", "").strip())
    except Exception:
        return None

def parse_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(float(str(x).strip()))
    except Exception:
        return None

def create_duffel_link(duffel_key: str, offer_id: str, redirect_url: str) -> str:
    headers = {
        "Authorization": f"Bearer {duffel_key}",
        "Content-Type": "application/json",
        "Duffel-Version": "v2",
    }
    body = {
        "data": {
            "selected_offers": [offer_id],
            "redirect_url": redirect_url,
        }
    }
    r = requests.post(DUFFEL_LINKS_URL, headers=headers, json=body, timeout=45)
    j = r.json()
    if r.status_code >= 400:
        raise RuntimeError(f"Duffel Links failed {r.status_code}: {j}")
    url = (((j.get("data") or {}).get("url")) or "").strip()
    if not url:
        raise RuntimeError(f"Duffel Links missing url: {j}")
    return url


# ============================================================
# Main
# ============================================================

def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")

    max_rows = env_int("MAX_LINK_ROWS_PER_RUN", 5)
    price_cap = env_float("DUFFEL_LINKS_PRICE_CAP_GBP", 120.0)
    require_direct = env_int("DUFFEL_LINKS_REQUIRE_DIRECT", 1) == 1

    duffel_key = env_str("DUFFEL_API_KEY")
    redirect_base = clean_url(env_str("REDIRECT_BASE_URL"))

    gc = get_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(tab)

    values = ws.get_all_values()
    if not values or len(values) < 2:
        log("Sheet empty. Nothing to route.")
        return 0

    headers = [h.strip() for h in values[0]]
    rows = values[1:]
    h = {name: i for i, name in enumerate(headers)}

    needed = ["booking_link_vip", "affiliate_url", "affiliate_source", "price_gbp", "stops", "deal_id"]
    for c in needed:
        if c not in h:
            raise RuntimeError(f"Missing required column in RAW_DEALS: {c}")

    # Optional but strongly recommended if you want Duffel Links eligibility:
    # offer_id from Duffel would need to be stored by the feeder in a column like "duffel_offer_id"
    offer_id_col = h.get("duffel_offer_id")
    origin_iata_col = h.get("origin_iata")
    dest_iata_col = h.get("destination_iata")

    routed = 0

    for rownum, r in enumerate(rows, start=2):
        if routed >= max_rows:
            break

        booking = (r[h["booking_link_vip"]] if h["booking_link_vip"] < len(r) else "").strip()
        if booking:
            continue

        affiliate = (r[h["affiliate_url"]] if h["affiliate_url"] < len(r) else "").strip()
        price = parse_float(r[h["price_gbp"]] if h["price_gbp"] < len(r) else "")
        stops = parse_int(r[h["stops"]] if h["stops"] < len(r) else "") or 0
        deal_id = (r[h["deal_id"]] if h["deal_id"] < len(r) else "").strip()

        # Default route: fall back to affiliate_url if present
        chosen_url = affiliate
        chosen_source = "existing" if affiliate else ""

        # Duffel Links attempt only if we have what we need
        eligible = True
        if not duffel_key or not redirect_base:
            eligible = False
        if price is None or price > price_cap:
            eligible = False
        if require_direct and stops != 0:
            eligible = False
        if offer_id_col is None:
            eligible = False
        if origin_iata_col is None or dest_iata_col is None:
            eligible = False

        if eligible:
            offer_id = (r[offer_id_col] if offer_id_col < len(r) else "").strip()
            origin_iata = (r[origin_iata_col] if origin_iata_col < len(r) else "").strip().upper()
            dest_iata = (r[dest_iata_col] if dest_iata_col < len(r) else "").strip().upper()
            if offer_id and origin_iata and dest_iata and deal_id:
                redirect_url = f"{redirect_base}/r/vip/{deal_id}?url={affiliate}" if affiliate else f"{redirect_base}/r/vip/{deal_id}?url=https://skyscanner.net"
                try:
                    chosen_url = create_duffel_link(duffel_key, offer_id, redirect_url=redirect_url)
                    chosen_source = "duffel_links"
                    log(f"✅ Duffel Links for row {rownum} deal_id={deal_id}")
                except Exception as e:
                    # Soft-fail to affiliate
                    log(f"⚠️  Duffel Links failed row {rownum}: {e}")

        # If we still have no URL, skip
        if not chosen_url:
            log(f"⏭️  Skip row {rownum}: no link available")
            continue

        ws.batch_update([
            {"range": a1(rownum, h["booking_link_vip"]), "values": [[chosen_url]]},
            {"range": a1(rownum, h["affiliate_source"]), "values": [[chosen_source or "skyscanner"]]},
        ])
        routed += 1

    log(f"Done. routed={routed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
