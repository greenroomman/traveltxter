# ============================================================
# FILE: workers/link_router.py
# ============================================================
#!/usr/bin/env python3
"""
Traveltxter — Hybrid Link Router (Duffel short-haul, Skyscanner everything else)

Why you got "on:" SyntaxError:
- You accidentally pasted YAML (the workflow file) into workers/link_router.py.
- Python sees "on:" and crashes.

Fix:
- Replace the ENTIRE contents of workers/link_router.py with this Python file only.
"""

from __future__ import annotations

import os
import json
import datetime as dt
from typing import Dict, Any, List, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials


# =========================
# Helpers
# =========================

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

def safe_text(v: Any) -> str:
    if v is None:
        return ""
    try:
        return str(v)
    except Exception:
        return ""

def safe_get(row: Dict[str, Any], key: str) -> str:
    return safe_text(row.get(key)).strip()

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

def normalize_status(s: str) -> str:
    return (s or "").strip().upper()


# =========================
# ENV
# =========================

SPREADSHEET_ID = env("SPREADSHEET_ID", env("SHEET_ID", ""), required=True)
RAW_DEALS_TAB = env("RAW_DEALS_TAB", "RAW_DEALS")

GCP_SA_JSON = env("GCP_SA_JSON", "") or env("GCP_SA_JSON_ONE_LINE", "")
if not GCP_SA_JSON:
    raise RuntimeError("Missing GCP_SA_JSON or GCP_SA_JSON_ONE_LINE")

DUFFEL_API_KEY = env("DUFFEL_API_KEY", required=True)
DUFFEL_VERSION = env("DUFFEL_VERSION", "v2")

REDIRECT_BASE_URL = env("REDIRECT_BASE_URL", "").strip()
DUFFEL_LINKS_SUCCESS_URL = env(
    "DUFFEL_LINKS_SUCCESS_URL",
    (REDIRECT_BASE_URL.rstrip("/") + "/success") if REDIRECT_BASE_URL else ""
)
DUFFEL_LINKS_FAILURE_URL = env(
    "DUFFEL_LINKS_FAILURE_URL",
    (REDIRECT_BASE_URL.rstrip("/") + "/failure") if REDIRECT_BASE_URL else ""
)
DUFFEL_LINKS_ABANDON_URL = env(
    "DUFFEL_LINKS_ABANDON_URL",
    (REDIRECT_BASE_URL.rstrip("/") + "/abandon") if REDIRECT_BASE_URL else ""
)

MAX_ROWS_PER_RUN = safe_int(env("LINK_ROUTER_MAX_ROWS", "12"), 12)

MAX_PRICE_DUFFEL_GBP = safe_float(env("DUFFEL_LINKS_MAX_PRICE_GBP", "220"), 220.0)
MIN_TRIP_DAYS = safe_int(env("DUFFEL_MIN_TRIP_DAYS", "2"), 2)
MAX_TRIP_DAYS = safe_int(env("DUFFEL_MAX_TRIP_DAYS", "10"), 10)

DUFFEL_THEMES = [t.strip().upper() for t in env("DUFFEL_THEMES", "").split(",") if t.strip()]

DUFFEL_COUNTRY_ALLOWLIST = set(
    x.strip().upper()
    for x in env(
        "DUFFEL_COUNTRY_ALLOWLIST",
        "ICELAND,IRELAND,FRANCE,SPAIN,PORTUGAL,ITALY,GERMANY,NETHERLANDS,BELGIUM,DENMARK,"
        "NORWAY,SWEDEN,FINLAND,POLAND,CZECHIA,AUSTRIA,SWITZERLAND,HUNGARY,GREECE,CROATIA,"
        "SLOVENIA,SLOVAKIA,BULGARIA,ROMANIA,LITHUANIA,LATVIA,ESTONIA,MALTA,CYPRUS,TURKEY,"
        "MOROCCO,EGYPT,TUNISIA"
    ).split(",")
    if x.strip()
)


# =========================
# Google Sheets
# =========================

def get_spreadsheet() -> gspread.Spreadsheet:
    creds_json = json.loads(GCP_SA_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID)

def header_map(headers: List[str]) -> Dict[str, int]:
    return {h: i + 1 for i, h in enumerate(headers) if h}


# =========================
# Duffel Links
# =========================

def create_duffel_links_session(reference: str) -> str:
    """
    Creates Duffel Links session and returns hosted session URL.
    """
    if not (DUFFEL_LINKS_SUCCESS_URL and DUFFEL_LINKS_FAILURE_URL and DUFFEL_LINKS_ABANDON_URL):
        raise RuntimeError("Duffel Links redirect URLs not set. Set REDIRECT_BASE_URL or DUFFEL_LINKS_*_URL")

    url = "https://api.duffel.com/links/sessions"
    payload = {
        "data": {
            "traveller_currency": "GBP",
            "success_url": DUFFEL_LINKS_SUCCESS_URL,
            "failure_url": DUFFEL_LINKS_FAILURE_URL,
            "abandonment_url": DUFFEL_LINKS_ABANDON_URL,
            "reference": reference,
            "flights": {"enabled": True},
            "should_hide_traveller_currency_selector": True,
        }
    }

    r = requests.post(
        url,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Duffel-Version": DUFFEL_VERSION,
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


# =========================
# Eligibility rules
# =========================

def is_duffel_eligible(row: Dict[str, Any]) -> Tuple[bool, str]:
    stops = safe_int(safe_get(row, "stops"), 99)
    if stops != 0:
        return False, "not_direct"

    price = safe_float(safe_get(row, "price_gbp"), 999999.0)
    if price > MAX_PRICE_DUFFEL_GBP:
        return False, "too_expensive"

    trip_days = safe_int(safe_get(row, "trip_length_days"), 0)
    if trip_days and (trip_days < MIN_TRIP_DAYS or trip_days > MAX_TRIP_DAYS):
        return False, "trip_length_out_of_range"

    country = safe_get(row, "destination_country").upper().strip()
    if country and country not in DUFFEL_COUNTRY_ALLOWLIST:
        return False, "country_not_allowed"

    if DUFFEL_THEMES:
        theme = (safe_get(row, "theme_final") or safe_get(row, "resolved_theme") or safe_get(row, "theme")).upper().strip()
        if theme and theme not in DUFFEL_THEMES:
            return False, "theme_not_allowed"

    return True, "ok"


# =========================
# Main
# =========================

def main() -> None:
    log("=" * 72)
    log("Hybrid Link Router starting")
    log(f"MAX_ROWS_PER_RUN={MAX_ROWS_PER_RUN} | MAX_PRICE_DUFFEL_GBP={MAX_PRICE_DUFFEL_GBP}")
    log("=" * 72)

    sh = get_spreadsheet()
    ws = sh.worksheet(RAW_DEALS_TAB)
    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("RAW_DEALS has no headers row")

    hmap = header_map(headers)

    for col in ("status", "affiliate_url", "booking_link_vip"):
        if col not in hmap:
            raise RuntimeError(f"RAW_DEALS missing required column: {col}")

    has_source_col = "affiliate_source" in hmap

    all_values = ws.get_all_values()
    if len(all_values) < 2:
        log("No data rows in RAW_DEALS.")
        return

    publishable = {"SCORED", "READY_TO_POST", "READY_TO_PUBLISH"}
    processed = 0
    updates: List[gspread.Cell] = []

    for r_idx in range(2, len(all_values) + 1):
        if processed >= MAX_ROWS_PER_RUN:
            break

        vals = all_values[r_idx - 1]
        row = {headers[c]: (vals[c] if c < len(vals) else "") for c in range(len(headers))}

        if normalize_status(safe_get(row, "status")) not in publishable:
            continue

        if safe_get(row, "booking_link_vip"):
            continue

        affiliate_url = safe_get(row, "affiliate_url")
        deal_id = safe_get(row, "deal_id") or f"row_{r_idx}"

        ok, reason = is_duffel_eligible(row)
        if ok:
            try:
                session_url = create_duffel_links_session(reference=f"tx_{deal_id}")
                updates.append(gspread.Cell(r_idx, hmap["booking_link_vip"], session_url))
                if has_source_col:
                    updates.append(gspread.Cell(r_idx, hmap["affiliate_source"], "DUFFEL_LINKS"))
                log(f"Row {r_idx}: DUFFEL_LINKS ({reason}) | {deal_id}")
            except Exception as e:
                if affiliate_url:
                    updates.append(gspread.Cell(r_idx, hmap["booking_link_vip"], affiliate_url))
                    if has_source_col:
                        updates.append(gspread.Cell(r_idx, hmap["affiliate_source"], "SKYSCANNER_FALLBACK"))
                    log(f"Row {r_idx}: Duffel Links failed -> SKYSCANNER_FALLBACK | {deal_id} | {e}")
                else:
                    log(f"Row {r_idx}: Duffel Links failed and no affiliate_url | {deal_id} | {e}")
        else:
            if affiliate_url:
                updates.append(gspread.Cell(r_idx, hmap["booking_link_vip"], affiliate_url))
                if has_source_col:
                    updates.append(gspread.Cell(r_idx, hmap["affiliate_source"], "SKYSCANNER"))
                log(f"Row {r_idx}: SKYSCANNER ({reason}) | {deal_id}")
            else:
                log(f"Row {r_idx}: No affiliate_url ({reason}) | {deal_id}")

        processed += 1

    if updates:
        ws.update_cells(updates, value_input_option="USER_ENTERED")
        log(f"Updated {len(updates)} cells across {processed} routed rows.")
    else:
        log("No updates needed (nothing publishable missing booking_link_vip).")

    log("Done.")


if __name__ == "__main__":
    main()
