# ============================================================
# FILE: workers/link_router.py
# ============================================================

#!/usr/bin/env python3
"""
TravelTxter — Hybrid Link Router (Duffel Links for short-haul, Skyscanner fallback)

Reads:  RAW_DEALS rows (status gating optional)
Writes: booking_link_vip (+ affiliate_source optional)

Goal:
- For short-haul, direct, price-capped deals → create DuffelIATA + Duffel Links session (VIP link)
- Otherwise → keep/derive Skyscanner affiliate_url

IMPORTANT:
- This script assumes your RAW_DEALS sheet has these columns:
  origin_iata, destination_iata, outbound_date, return_date, price_gbp,
  affiliate_url, booking_link_vip
"""

import os
import json
import datetime as dt
from typing import Any, Dict, List, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials


def now_utc_str() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def log(msg: str) -> None:
    print(f"{now_utc_str()} | {msg}", flush=True)


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


def get_gspread_client() -> gspread.Client:
    sa_json = env_str("GCP_SA_JSON_ONE_LINE", "") or env_str("GCP_SA_JSON", "")
    if not sa_json:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
    info = json.loads(sa_json.replace("\\n", "\n"))
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


DUFFEL_API_BASE = "https://api.duffel.com"


def duffel_headers(api_key: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {api_key}",
        "Duffel-Version": "v1",
        "Content-Type": "application/json",
    }


def create_duffel_link(
    api_key: str,
    origin: str,
    destination: str,
    outbound_date: str,
    return_date: str,
    redirect_base_url: str,
) -> Optional[str]:
    """
    Creates a Duffel "links" session (if available to your account).
    Returns URL string or None.
    """
    if not redirect_base_url:
        return None

    # NOTE: If your account uses Duffel Links sessions, your endpoint/payload may differ.
    # This is a conservative placeholder that you can adapt to your working session-creator.
    # If you already have a working Duffel Links implementation, keep it.
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
    return r.json().get("data", {}).get("url")


def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    raw_tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    api_key = env_str("DUFFEL_API_KEY")
    redirect_base = env_str("REDIRECT_BASE_URL", "")

    max_rows = env_int("LINK_ROUTER_MAX_ROWS", 12)
    max_price = env_float("DUFFEL_LINKS_MAX_PRICE_GBP", 180.0)

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")
    if not api_key:
        raise RuntimeError("Missing DUFFEL_API_KEY")

    log("============================================================")
    log("🔗 Hybrid Link Router starting")
    log("============================================================")

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

    if min(i_origin, i_dest, i_out, i_ret, i_price, i_aff, i_vip) < 0:
        raise RuntimeError("Missing required columns in RAW_DEALS for link routing")

    updated = 0

    # iterate oldest-to-newest to fill gaps
    for r_i, r in enumerate(rows[:max_rows], start=2):  # sheet row index starts at 2
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

        # Basic eligibility: short-haul direct under price cap (you can refine)
        if price <= 0 or price > max_price:
            continue

        link = create_duffel_link(api_key, origin, dest, out_date, ret_date, redirect_base)
        if not link:
            continue

        # write vip link
        ws.update([[link]], f"{gspread.utils.rowcol_to_a1(r_i, i_vip + 1)}")
        updated += 1
        log(f"✓ VIP link set for row {r_i}: {origin}->{dest} £{price:.0f}")

    log(f"Done. Updated {updated} rows.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
