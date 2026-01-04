# ============================================================
# FILE: workers/link_router.py
# (PASTE OVER YOUR EXISTING FILE)
# ============================================================

#!/usr/bin/env python3
"""
TravelTxter — Hybrid Link Router (Duffel Links for short-haul, Skyscanner fallback)

Reads:  RAW_DEALS rows
Writes: booking_link_vip (+ optional affiliate_source)

Goal:
- For eligible short-haul, direct-ish, price-capped deals → create Duffel Links session URL (VIP link)
- Otherwise → leave booking_link_vip empty (Skyscanner remains in affiliate_url)

Notes:
- This script should NEVER crash on SA JSON formatting quirks; it should handle:
  - one-line JSON
  - JSON with literal newlines
  - JSON with escaped "\\n" inside private_key
"""

from __future__ import annotations

import os
import json
import datetime as dt
from typing import Any, Dict, List, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials


# -----------------------------
# Logging / time
# -----------------------------

def now_utc_str() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def log(msg: str) -> None:
    print(f"{now_utc_str()} | {msg}", flush=True)


# -----------------------------
# Env helpers
# -----------------------------

def env_str(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def env_int(name: str, default: int) -> int:
    v = env_str(name, "")
    try:
        return int(v) if v != "" else default
    except Exception:
        return default


def env_float(name: str, default: float) -> float:
    v = env_str(name, "")
    try:
        return float(v) if v != "" else default
    except Exception:
        return default


# -----------------------------
# Google Sheets auth (ROBUST)
# -----------------------------

def get_gspread_client() -> gspread.Client:
    """
    Robust loader for SA JSON stored in env vars.

    Supports:
    - GCP_SA_JSON_ONE_LINE (preferred)
    - GCP_SA_JSON (fallback)

    Handles:
    - Escaped newlines (\\n) inside private_key
    - Literal newlines in the secret value
    """
    sa_json = env_str("GCP_SA_JSON_ONE_LINE", "") or env_str("GCP_SA_JSON", "")
    if not sa_json:
        raise RuntimeError("Missing service account JSON: set GCP_SA_JSON_ONE_LINE (or GCP_SA_JSON).")

    # Try strict parse first
    try:
        info = json.loads(sa_json)
    except json.JSONDecodeError:
        # Common case: escaped newlines in private_key
        try:
            info = json.loads(sa_json.replace("\\n", "\n"))
        except json.JSONDecodeError as e:
            # Do not print secret content. Provide a deterministic fix instruction.
            raise RuntimeError(
                "Service account JSON secret is not valid JSON in this runner.\n"
                "Fix (recommended): recreate GCP_SA_JSON_ONE_LINE as true one-line JSON:\n"
                "1) Save the raw SA JSON to sa.json\n"
                "2) Run: python -c \"import json;print(json.dumps(json.load(open('sa.json'))))\"\n"
                "3) Paste the output into the GitHub Secret: GCP_SA_JSON_ONE_LINE"
            ) from e

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


# -----------------------------
# Duffel Links (best-effort)
# -----------------------------

DUFFEL_API_BASE = "https://api.duffel.com"


def duffel_headers(api_key: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {api_key}",
        "Duffel-Version": "v2",
        "Content-Type": "application/json",
    }


def looks_like_valid_url(base: str) -> bool:
    if not base:
        return False
    if base in ("http://", "https://"):
        return False
    return base.startswith("http://") or base.startswith("https://")


def create_duffel_link(
    api_key: str,
    origin: str,
    destination: str,
    outbound_date: str,
    return_date: str,
    redirect_base_url: str,
) -> Optional[str]:
    """
    Creates a Duffel Links session URL (if enabled in your Duffel account).
    Returns URL string or None.
    """
    if not looks_like_valid_url(redirect_base_url):
        return None

    url = f"{DUFFEL_API_BASE}/air/links/sessions"
    payload = {
        "data": {
            "slices": [
                {"origin": origin, "destination": destination, "departure_date": outbound_date},
                {"origin": destination, "destination": origin, "departure_date": return_date},
            ],
            "passengers": [{"type": "adult"}],
            "redirect_urls": {
                "success": f"{redirect_base_url.rstrip('/')}/success",
                "cancel": f"{redirect_base_url.rstrip('/')}/cancel",
            },
        }
    }

    r = requests.post(url, headers=duffel_headers(api_key), json=payload, timeout=40)
    if r.status_code >= 300:
        return None

    data = r.json().get("data") or {}
    return data.get("url")


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    raw_tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    api_key = env_str("DUFFEL_API_KEY")
    redirect_base = env_str("REDIRECT_BASE_URL", "")

    max_rows = env_int("LINK_ROUTER_MAX_ROWS", 12)
    max_price = env_float("DUFFEL_LINKS_MAX_PRICE_GBP", 220.0)

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")
    if not api_key:
        raise RuntimeError("Missing DUFFEL_API_KEY")

    log("============================================================")
    log("🔗 Hybrid Link Router starting")
    log("============================================================")

    if not looks_like_valid_url(redirect_base):
        log("⚠️ REDIRECT_BASE_URL is missing/invalid. Duffel Links will be skipped until set correctly.")

    gc = get_gspread_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(raw_tab)

    values = ws.get_all_values()
    if len(values) <= 1:
        log(f"No data rows in {raw_tab}")
        return 0

    headers = values[0]
    rows = values[1:]

    def idx(col: str) -> int:
        try:
            return headers.index(col)
        except ValueError:
            return -1

    i_origin = idx("origin_iata")
    i_dest = idx("destination_iata")
    i_out = idx("outbound_date")
    i_ret = idx("return_date")
    i_price = idx("price_gbp")
    i_aff = idx("affiliate_url")
    i_vip = idx("booking_link_vip")

    required = {
        "origin_iata": i_origin,
        "destination_iata": i_dest,
        "outbound_date": i_out,
        "return_date": i_ret,
        "price_gbp": i_price,
        "affiliate_url": i_aff,
        "booking_link_vip": i_vip,
    }
    missing = [k for k, v in required.items() if v < 0]
    if missing:
        raise RuntimeError(f"Missing required columns in {raw_tab}: {', '.join(missing)}")

    updated = 0

    # Process oldest-first so links fill in a stable order
    for sheet_row_idx, r in enumerate(rows[:max_rows], start=2):  # sheet rows start at 2
        vip_link = (r[i_vip] or "").strip()
        if vip_link:
            continue

        origin = (r[i_origin] or "").strip()
        dest = (r[i_dest] or "").strip()
        out_date = (r[i_out] or "").strip()
        ret_date = (r[i_ret] or "").strip()

        try:
            price = float((r[i_price] or "0").strip())
        except Exception:
            price = 0.0

        if not origin or not dest or not out_date or not ret_date:
            continue

        # Price gate
        if price <= 0 or price > max_price:
            continue

        link = create_duffel_link(api_key, origin, dest, out_date, ret_date, redirect_base)
        if not link:
            continue

        # gspread v6 update(values, range_name)
        cell_a1 = gspread.utils.rowcol_to_a1(sheet_row_idx, i_vip + 1)
        ws.update([[link]], cell_a1)

        updated += 1
        log(f"✓ VIP link set row {sheet_row_idx}: {origin}->{dest} £{price:.0f}")

    log(f"Done. Updated {updated} rows.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
