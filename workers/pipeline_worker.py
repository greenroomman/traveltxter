#!/usr/bin/env python3
"""
Traveltxter — Hybrid Link Router (Duffel short-haul, Skyscanner everything else)

What this does (simple):
- Reads RAW_DEALS
- For rows that are publishable (SCORED / READY_TO_POST / READY_TO_PUBLISH) AND missing booking_link_vip:
    - If the deal is "Duffel-eligible" (short-haul, direct, good price), create a Duffel Links Session URL
      and write it into booking_link_vip + affiliate_source="DUFFEL_LINKS"
    - Otherwise, use the existing affiliate_url (Skyscanner) for booking_link_vip + affiliate_source="SKYSCANNER"

Why:
- Duffel has a 1500:1 look-to-book expectation and charges "Excess Search" if you exceed it. :contentReference[oaicite:3]{index=3}
- Using Duffel for short-haul, high-intent routes increases the chance of Orders and keeps searches "worth it".
- Everything else stays Skyscanner so you don’t burn Duffel searches on low-conversion long-haul.
"""

from __future__ import annotations

import os
import json
import datetime as dt
from typing import Dict, Any, List, Tuple, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# Helpers
# ============================================================

def env(name: str, default: str = "", required: bool = False) -> str:
    v = (os.getenv(name) or "").strip()
    if not v:
        v = default
    if required and not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)

def now_utc_str() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def safe_text(v: Any) -> str:
    if v is None:
        return ""
    try:
        return str(v)
    except Exception:
        return ""

def safe_get(row: Dict[str, Any], key: str) -> str:
    return safe_text(row.get(key)).strip()

def normalize_status(s: str) -> str:
    return (s or "").strip().upper()

def safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(str(v).strip())
    except Exception:
        return default

def safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(str(v).strip()))
    except Exception:
        return default


# ============================================================
# ENV
# ============================================================

SPREADSHEET_ID = env("SPREADSHEET_ID", required=True)
RAW_DEALS_TAB = env("RAW_DEALS_TAB", "RAW_DEALS")

GCP_SA_JSON = env("GCP_SA_JSON", "") or env("GCP_SA_JSON_ONE_LINE", "")
if not GCP_SA_JSON:
    raise RuntimeError("Missing GCP_SA_JSON or GCP_SA_JSON_ONE_LINE")

# Duffel token (use your existing DUFFEL_API_KEY secret)
DUFFEL_API_KEY = env("DUFFEL_API_KEY", required=True)

# Where Duffel Links returns users after booking / failure / abandon.
# You can set REDIRECT_BASE_URL to your landing page domain.
REDIRECT_BASE_URL = env("REDIRECT_BASE_URL", "https://traveltxter.com")
LINKS_SUCCESS_URL = env("LINKS_SUCCESS_URL", f"{REDIRECT_BASE_URL.rstrip('/')}/success")
LINKS_FAILURE_URL = env("LINKS_FAILURE_URL", f"{REDIRECT_BASE_URL.rstrip('/')}/failure")
LINKS_ABANDON_URL = env("LINKS_ABANDON_URL", f"{REDIRECT_BASE_URL.rstrip('/')}/abandon")

# How many rows to process per run (keeps it fast + safe)
MAX_ROWS_PER_RUN = safe_int(env("LINK_ROUTER_MAX_ROWS", "8"), 8)

# Duffel eligibility thresholds (tweak safely)
MAX_PRICE_DUFFEL_GBP = safe_float(env("DUFFEL_MAX_PRICE_GBP", "220"), 220.0)
MIN_TRIP_DAYS = safe_int(env("DUFFEL_MIN_TRIP_DAYS", "2"), 2)
MAX_TRIP_DAYS = safe_int(env("DUFFEL_MAX_TRIP_DAYS", "10"), 10)

# If you want to force Duffel only for certain themes, set:
# DUFFEL_THEMES="CITY,WINTER SUN,SURF,FOODIE"
DUFFEL_THEMES = [t.strip().upper() for t in env("DUFFEL_THEMES", "").split(",") if t.strip()]

# Destination country allowlist for short-haul Duffel lanes (broad, UK-centric)
# (Keeps Duffel focused on likely conversion routes.)
DUFFEL_COUNTRY_ALLOWLIST = {
    # Europe
    "ICELAND","IRELAND","FRANCE","SPAIN","PORTUGAL","ITALY","GERMANY","NETHERLANDS","BELGIUM",
    "DENMARK","NORWAY","SWEDEN","FINLAND","POLAND","CZECHIA","AUSTRIA","SWITZERLAND",
    "HUNGARY","GREECE","CROATIA","SLOVENIA","SLOVAKIA","BULGARIA","ROMANIA","LITHUANIA",
    "LATVIA","ESTONIA","MALTA","CYPRUS","TURKEY",
    # Near/Med sun
    "MOROCCO","EGYPT","TUNISIA",
}


# ============================================================
# Google Sheets
# ============================================================

def get_spreadsheet() -> gspread.Spreadsheet:
    creds_json = json.loads(GCP_SA_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID)

def get_headers(ws: gspread.Worksheet) -> List[str]:
    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("No headers found in sheet")
    return headers

def header_map(headers: List[str]) -> Dict[str, int]:
    return {h: i + 1 for i, h in enumerate(headers) if h}


# ============================================================
# Duffel Links
# ============================================================

def create_duffel_links_session(reference: str) -> str:
    """
    Creates a Duffel Links session and returns session URL.
    Sessions endpoint: POST https://api.duffel.com/links/sessions :contentReference[oaicite:4]{index=4}
    """
    url = "https://api.duffel.com/links/sessions"
    payload = {
        "data": {
            "traveller_currency": "GBP",
            "success_url": LINKS_SUCCESS_URL,
            "failure_url": LINKS_FAILURE_URL,
            "abandonment_url": LINKS_ABANDON_URL,
            "reference": reference,
            "flights": {"enabled": True},
            # Keep it simple / clean
            "should_hide_traveller_currency_selector": True,
            "checkout_display_text": "Thanks for booking — have an unreal trip.",
        }
    }

    r = requests.post(
        url,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Duffel-Version": "v2",
            "Authorization": f"Bearer {DUFFEL_API_KEY}",
        },
        json=payload,
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    session_url = safe_text((data.get("data") or {}).get("url")).strip()
    if not session_url:
        raise RuntimeError("Duffel Links session created but no URL returned")
    return session_url


# ============================================================
# Routing rules
# ============================================================

def is_duffel_eligible(row: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Decide if this deal should use Duffel Links (short-haul, direct, good price).
    """
    stops = safe_int(safe_get(row, "stops"), 99)
    if stops != 0:
        return False, "not_direct"

    price = safe_float(safe_get(row, "price_gbp"), 999999.0)
    if price > MAX_PRICE_DUFFEL_GBP:
        return False, "too_expensive_for_duffel"

    trip_days = safe_int(safe_get(row, "trip_length_days"), 0)
    if trip_days and (trip_days < MIN_TRIP_DAYS or trip_days > MAX_TRIP_DAYS):
        return False, "trip_length_out_of_range"

    country = safe_get(row, "destination_country").upper().strip()
    if country and country not in DUFFEL_COUNTRY_ALLOWLIST:
        return False, "country_not_in_shorthaul_allowlist"

    # Optional theme gating
    if DUFFEL_THEMES:
        theme = (safe_get(row, "theme_final") or safe_get(row, "resolved_theme") or safe_get(row, "theme")).upper().strip()
        if theme and theme not in DUFFEL_THEMES:
            return False, "theme_not_duffel_enabled"

    return True, "duffel_ok"


# ============================================================
# Main
# ============================================================

def main() -> None:
    log("=" * 72)
    log("Hybrid Link Router: Duffel short-haul, Skyscanner everything else")
    log(f"MAX_ROWS_PER_RUN={MAX_ROWS_PER_RUN} | MAX_PRICE_DUFFEL_GBP={MAX_PRICE_DUFFEL_GBP}")
    log("=" * 72)

    sh = get_spreadsheet()
    ws = sh.worksheet(RAW_DEALS_TAB)
    headers = get_headers(ws)
    hmap = header_map(headers)

    required_cols = ["status", "affiliate_url", "booking_link_vip", "affiliate_source", "deal_id"]
    for c in required_cols:
        if c not in hmap:
            raise RuntimeError(f"RAW_DEALS missing required column: {c}")

    all_values = ws.get_all_values()
    if len(all_values) < 2:
        log("No data rows in RAW_DEALS.")
        return

    publishable_statuses = {"SCORED", "READY_TO_POST", "READY_TO_PUBLISH"}

    processed = 0
    updates: List[gspread.Cell] = []

    for r_idx in range(2, len(all_values) + 1):
        if processed >= MAX_ROWS_PER_RUN:
            break

        row_vals = all_values[r_idx - 1]
        row = {headers[c]: (row_vals[c] if c < len(row_vals) else "") for c in range(len(headers))}

        status = normalize_status(safe_get(row, "status"))
        if status not in publishable_statuses:
            continue

        existing_vip_link = safe_get(row, "booking_link_vip")
        if existing_vip_link:
            continue  # already routed

        affiliate_url = safe_get(row, "affiliate_url")
        deal_id = safe_get(row, "deal_id") or f"row_{r_idx}"

        # Decide routing
        ok, reason = is_duffel_eligible(row)

        if ok:
            # Create Duffel Links session URL
            # Reference helps you reconcile sessions later. :contentReference[oaicite:5]{index=5}
            reference = f"tx_{deal_id}"
            try:
                session_url = create_duffel_links_session(reference=reference)
                updates.append(gspread.Cell(r_idx, hmap["booking_link_vip"], session_url))
                updates.append(gspread.Cell(r_idx, hmap["affiliate_source"], "DUFFEL_LINKS"))
                log(f"Row {r_idx}: DUFFEL_LINKS set ({reason}) | {deal_id}")
            except Exception as e:
                # Fallback to Skyscanner if Duffel Links fails
                if affiliate_url:
                    updates.append(gspread.Cell(r_idx, hmap["booking_link_vip"], affiliate_url))
                    updates.append(gspread.Cell(r_idx, hmap["affiliate_source"], "SKYSCANNER_FALLBACK"))
                log(f"Row {r_idx}: Duffel Links failed, fell back to Skyscanner | {deal_id} | {e}")
        else:
            if affiliate_url:
                updates.append(gspread.Cell(r_idx, hmap["booking_link_vip"], affiliate_url))
                updates.append(gspread.Cell(r_idx, hmap["affiliate_source"], "SKYSCANNER"))
                log(f"Row {r_idx}: SKYSCANNER set ({reason}) | {deal_id}")
            else:
                log(f"Row {r_idx}: No affiliate_url available ({reason}) | {deal_id}")

        processed += 1

    if updates:
        ws.update_cells(updates, value_input_option="USER_ENTERED")
        log(f"Updates written: {len(updates)} cells")
    else:
        log("No updates needed (nothing publishable missing booking_link_vip).")

    log("Done.")


if __name__ == "__main__":
    main()
