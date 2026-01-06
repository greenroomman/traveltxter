#!/usr/bin/env python3
"""
TravelTxter V4.5x — instagram_publisher.py

Reads:
- RAW_DEALS where status == READY_TO_PUBLISH

Requires:
- graphic_url present (render step completed)

Writes:
- posted_instagram_at
- status -> POSTED_INSTAGRAM

Key feature:
- Google Sheets 429 (rate limit) retry/backoff to prevent workflow failure.

Env required:
- SPREADSHEET_ID
- GCP_SA_JSON_ONE_LINE (or GCP_SA_JSON)
- RAW_DEALS_TAB (default RAW_DEALS)
- IG_ACCESS_TOKEN
- IG_USER_ID

Env optional:
- STRIPE_MONTHLY_LINK
- STRIPE_YEARLY_LINK
- IG_MAX_ROWS (default 1)
"""

from __future__ import annotations

import os
import json
import time
import datetime as dt
from typing import Dict, Any, List, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError


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


# ============================================================
# Sheets auth with retry
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
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds)

def open_sheet_with_backoff(gc: gspread.Client, spreadsheet_id: str, attempts: int = 8) -> gspread.Spreadsheet:
    """
    Retries on Google Sheets 429 quota exceeded with exponential backoff.
    """
    delay = 4.0
    for i in range(1, attempts + 1):
        try:
            return gc.open_by_key(spreadsheet_id)
        except APIError as e:
            msg = str(e)
            if "429" in msg or "Quota exceeded" in msg:
                log(f"⏳ Sheets quota (429). Retry {i}/{attempts} in {int(delay)}s...")
                time.sleep(delay)
                delay = min(delay * 1.6, 45.0)
                continue
            raise
    raise RuntimeError("Google Sheets quota still exceeded after retries (429). Try again in 1–2 minutes.")


# ============================================================
# A1 helpers
# ============================================================

def col_letter(n1: int) -> str:
    s = ""
    n = n1
    while n:
        n, rr = divmod(n - 1, 26)
        s = chr(65 + rr) + s
    return s

def a1(rownum: int, col0: int) -> str:
    return f"{col_letter(col0 + 1)}{rownum}"

def ensure_columns(ws: gspread.Worksheet, headers: List[str], required: List[str]) -> List[str]:
    missing = [c for c in required if c not in headers]
    if not missing:
        return headers
    ws.update([headers + missing], "A1")
    log(f"🛠️  Added missing columns to header: {missing}")
    return headers + missing


# ============================================================
# Caption building (LOCKED TEMPLATE)
# ============================================================

FLAG_MAP = {
    # minimal common flags; extend safely later
    "ICELAND": "🇮🇸",
    "SPAIN": "🇪🇸",
    "PORTUGAL": "🇵🇹",
    "FRANCE": "🇫🇷",
    "ITALY": "🇮🇹",
    "GREECE": "🇬🇷",
    "MOROCCO": "🇲🇦",
    "TURKEY": "🇹🇷",
    "THAILAND": "🇹🇭",
    "JAPAN": "🇯🇵",
    "USA": "🇺🇸",
    "UNITED STATES": "🇺🇸",
}

def country_flag(country: str) -> str:
    c = (country or "").strip().upper()
    return FLAG_MAP.get(c, "")

def build_caption(price_gbp: str, destination_country: str, destination_city: str, origin_city: str,
                  outbound_date: str, return_date: str, stripe_monthly: str, stripe_yearly: str) -> str:
    flag = country_flag(destination_country)
    dest_upper = (destination_city or "").strip().upper()
    lines = [
        f"£{price_gbp} to {destination_country}{(' ' + flag) if flag else ''}".strip(),
        f"TO: {dest_upper}",
        f"FROM: {origin_city}",
        f"OUT: {outbound_date}",
        f"BACK: {return_date}",
        "",
        "Heads up:",
        "• VIP members saw this 24 hours ago",
        "• Availability is running low",
        "• Best deals go to VIPs first",
        "",
        "Want instant access?",
        "Join TravelTxter Nomad for £7.99 / month:",
        "",
        "• Deals 24 hours early",
        "• Direct booking links",
        "• Exclusive mistake fares",
        "• Cancel anytime",
        "",
        f"Upgrade now (Monthly): {stripe_monthly}".strip(),
        f"Upgrade now (Yearly): {stripe_yearly}".strip(),
    ]
    # Remove any accidental blank URL lines if links not set
    out = []
    for ln in lines:
        if "Upgrade now" in ln and ln.endswith(":"):
            continue
        out.append(ln)
    return "\n".join(out).strip() + "\n"


# ============================================================
# Instagram Graph API
# ============================================================

def ig_create_container(ig_user_id: str, access_token: str, image_url: str, caption: str) -> str:
    url = f"https://graph.facebook.com/v20.0/{ig_user_id}/media"
    r = requests.post(url, data={
        "image_url": image_url,
        "caption": caption,
        "access_token": access_token,
    }, timeout=45)
    j = r.json()
    if r.status_code >= 400 or "id" not in j:
        raise RuntimeError(f"IG create container failed: {j}")
    return j["id"]

def ig_publish(ig_user_id: str, access_token: str, creation_id: str) -> str:
    url = f"https://graph.facebook.com/v20.0/{ig_user_id}/media_publish"
    r = requests.post(url, data={
        "creation_id": creation_id,
        "access_token": access_token,
    }, timeout=45)
    j = r.json()
    if r.status_code >= 400 or "id" not in j:
        raise RuntimeError(f"IG publish failed: {j}")
    return j["id"]

def ig_publish_with_wait(ig_user_id: str, access_token: str, creation_id: str) -> str:
    """
    Fixes 'Media ID is not available' by waiting and retrying publish.
    """
    delays = [5, 10, 20, 30, 45]
    last_err = None
    for d in delays:
        try:
            return ig_publish(ig_user_id, access_token, creation_id)
        except Exception as e:
            last_err = str(e)
            # only retry on the known transient error
            if "Media ID is not available" in last_err or "2207027" in last_err:
                log(f"⏳ IG media not ready. Waiting {d}s then retry...")
                time.sleep(d)
                continue
            raise
    raise RuntimeError(f"IG publish failed after retries: {last_err}")


# ============================================================
# Main
# ============================================================

def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")

    ig_token = env_str("IG_ACCESS_TOKEN")
    ig_user_id = env_str("IG_USER_ID")

    stripe_monthly = env_str("STRIPE_MONTHLY_LINK")
    stripe_yearly = env_str("STRIPE_YEARLY_LINK")

    max_rows = env_int("IG_MAX_ROWS", 1)

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")
    if not ig_token or not ig_user_id:
        raise RuntimeError("Missing IG_ACCESS_TOKEN or IG_USER_ID")

    gc = get_client()
    sh = open_sheet_with_backoff(gc, spreadsheet_id)

    ws = sh.worksheet(tab)

    # Reduce read pressure: one read for everything
    values = ws.get_all_values()
    if not values or len(values) < 2:
        log("No rows to publish.")
        return 0

    headers = [h.strip() for h in values[0]]
    required = [
        "status", "graphic_url",
        "price_gbp", "destination_country", "destination_city", "origin_city",
        "outbound_date", "return_date",
        "posted_instagram_at",
    ]
    headers = ensure_columns(ws, headers, required)
    values = ws.get_all_values()
    headers = [h.strip() for h in values[0]]
    h = {name: i for i, name in enumerate(headers)}
    rows = values[1:]

    published = 0

    for idx, row in enumerate(rows, start=2):
        if published >= max_rows:
            break

        status = (row[h["status"]] if h["status"] < len(row) else "").strip().upper()
        if status != "READY_TO_PUBLISH":
            continue

        image_url = (row[h["graphic_url"]] if h["graphic_url"] < len(row) else "").strip()
        if not image_url:
            continue

        price_gbp = (row[h["price_gbp"]] if h["price_gbp"] < len(row) else "").strip()
        destination_country = (row[h["destination_country"]] if h["destination_country"] < len(row) else "").strip()
        destination_city = (row[h["destination_city"]] if h["destination_city"] < len(row) else "").strip()
        origin_city = (row[h["origin_city"]] if h["origin_city"] < len(row) else "").strip()
        outbound_date = (row[h["outbound_date"]] if h["outbound_date"] < len(row) else "").strip()
        return_date = (row[h["return_date"]] if h["return_date"] < len(row) else "").strip()

        caption = build_caption(
            price_gbp=price_gbp,
            destination_country=destination_country,
            destination_city=destination_city,
            origin_city=origin_city,
            outbound_date=outbound_date,
            return_date=return_date,
            stripe_monthly=stripe_monthly,
            stripe_yearly=stripe_yearly,
        )

        log(f"📸 Posting IG for row {idx}")

        creation_id = ig_create_container(ig_user_id, ig_token, image_url, caption)
        media_id = ig_publish_with_wait(ig_user_id, ig_token, creation_id)

        # Write back with minimal updates
        updates = [
            {"range": a1(idx, h["posted_instagram_at"]), "values": [[ts()]]},
            {"range": a1(idx, h["status"]), "values": [["POSTED_INSTAGRAM"]]},
        ]
        ws.batch_update(updates)

        published += 1
        log(f"✅ IG posted media_id={media_id} row={idx} -> POSTED_INSTAGRAM")

        # small sleep to reduce rate-limit pressure before next worker
        time.sleep(3)

    log(f"Done. IG published {published}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
