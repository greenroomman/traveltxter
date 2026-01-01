#!/usr/bin/env python3
"""
TravelTxter V4 Unified Pipeline (Practical Fix Version)

Fixes:
1) Duffel FEED now runs inside this worker (no silent 'DISABLED' stage).
2) VIP vs FREE Telegram gating:
   - VIP posts on AM run
   - FREE posts on PM run AND only if VIP was posted >= VIP_DELAY_HOURS ago
3) Works with an empty sheet (header-only): it will still insert deals.

This file is “colour-by-numbers” copy/paste safe (indentation preserved).
"""

import os
import json
import uuid
import datetime as dt
from typing import Dict, Any, List, Tuple, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials
from openai import OpenAI


# =========================
# Logging
# =========================
def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


def now_utc() -> dt.datetime:
    return dt.datetime.utcnow()


# =========================
# ENV
# =========================
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "").strip()
RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", os.getenv("RAW_DEALS", "")).strip()

CONFIG_TAB = os.getenv("CONFIG_TAB", "CONFIG").strip()

GCP_SA_JSON = os.getenv("GCP_SA_JSON", "").strip()
GCP_SA_JSON_ONE_LINE = os.getenv("GCP_SA_JSON_ONE_LINE", "").strip()

DUFFEL_API_KEY = os.getenv("DUFFEL_API_KEY", "").strip()
DUFFEL_VERSION = os.getenv("DUFFEL_VERSION", "v2").strip()
DUFFEL_MAX_INSERTS = int(os.getenv("DUFFEL_MAX_INSERTS", "3"))
DUFFEL_ROUTES_PER_RUN = int(os.getenv("DUFFEL_ROUTES_PER_RUN", "1"))
DUFFEL_ENABLED = os.getenv("DUFFEL_ENABLED", "true").strip().lower() in ("1", "true", "yes", "y")

DEFAULT_ORIGIN_IATA = os.getenv("ORIGIN_IATA", "LHR").strip().upper()
DEFAULT_DEST_IATA = os.getenv("DEST_IATA", "BCN").strip().upper()
DAYS_AHEAD = int(os.getenv("DAYS_AHEAD", "60"))
TRIP_LENGTH_DAYS = int(os.getenv("TRIP_LENGTH_DAYS", "5"))

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()

# Telegram (support both the old + new env var names)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHANNEL = (
    os.getenv("TELEGRAM_CHANNEL", "")
    or os.getenv("TELEGRAM_FREE_CHANNEL", "")
    or os.getenv("TELEGRAM_CHANNEL_FREE", "")
).strip()

TELEGRAM_BOT_TOKEN_VIP = os.getenv("TELEGRAM_BOT_TOKEN_VIP", "").strip()
TELEGRAM_CHANNEL_VIP = (
    os.getenv("TELEGRAM_CHANNEL_VIP", "")
    or os.getenv("TELEGRAM_VIP_CHANNEL", "")
).strip()

VIP_DELAY_HOURS = int(os.getenv("VIP_DELAY_HOURS", "24"))
RUN_SLOT = os.getenv("RUN_SLOT", "AM").strip().upper()  # AM / PM

STRIPE_LINK = os.getenv("STRIPE_LINK", "").strip()


# =========================
# Status constants
# =========================
STATUS_NEW = "NEW"
STATUS_READY_TO_PUBLISH = "READY_TO_PUBLISH"
STATUS_POSTED_ALL = "POSTED_ALL"


# =========================
# Google Sheets helpers
# =========================
def load_sa_json() -> Dict[str, Any]:
    if GCP_SA_JSON:
        return json.loads(GCP_SA_JSON)
    if GCP_SA_JSON_ONE_LINE:
        return json.loads(GCP_SA_JSON_ONE_LINE)
    raise RuntimeError("Missing GCP_SA_JSON or GCP_SA_JSON_ONE_LINE")


def get_ws() -> Tuple[gspread.Worksheet, List[str]]:
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID")
    if not RAW_DEALS_TAB:
        raise RuntimeError("Missing RAW_DEALS_TAB")

    sa = load_sa_json()
    creds = Credentials.from_service_account_info(
        sa,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(RAW_DEALS_TAB)
    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("RAW_DEALS has no headers (row 1)")
    return ws, headers


def header_map(headers: List[str]) -> Dict[str, int]:
    return {h.strip(): i + 1 for i, h in enumerate(headers) if h.strip()}


def iso_date(d: dt.date) -> str:
    return d.isoformat()


def safe_get(row: Dict[str, str], key: str) -> str:
    return (row.get(key) or "").strip()


# =========================
# CONFIG: route rotation
# =========================
def load_routes_from_config(ws_parent: gspread.Spreadsheet) -> List[Tuple[str, str]]:
    try:
        cfg = ws_parent.worksheet(CONFIG_TAB)
    except Exception:
        return []

    values = cfg.get_all_values()
    if not values or len(values) < 2:
        return []

    headers = [h.strip() for h in values[0]]
    idx = {h: i for i, h in enumerate(headers) if h}

    if "origin_iata" not in idx or "destination_iata" not in idx:
        return []

    out: List[Tuple[str, str]] = []
    for r in values[1:]:
        enabled = ""
        if "enabled" in idx and idx["enabled"] < len(r):
            enabled = (r[idx["enabled"]] or "").strip().lower()
        if enabled and enabled not in ("1", "true", "yes", "y"):
            continue

        o = ""
        d = ""
        if idx["origin_iata"] < len(r):
            o = (r[idx["origin_iata"]] or "").strip().upper()
        if idx["destination_iata"] < len(r):
            d = (r[idx["destination_iata"]] or "").strip().upper()

        if o and d:
            out.append((o, d))

    return out


# =========================
# Duffel
# =========================
def duffel_offer_request(origin: str, dest: str, out_date: str, ret_date: str) -> Dict[str, Any]:
    url = "https://api.duffel.com/air/offer_requests"
    headers = {
        "Authorization": f"Bearer {DUFFEL_API_KEY}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Duffel-Version": DUFFEL_VERSION,  # v2 required
    }
    payload = {
        "data": {
            "slices": [
                {"origin": origin, "destination": dest, "departure_date": out_date},
                {"origin": dest, "destination": origin, "departure_date": ret_date},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": "economy",
        }
    }
    r = requests.post(url, headers=headers, json=payload, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"Duffel error {r.status_code}: {r.text[:500]}")
    return r.json()


def duffel_run_and_insert(ws: gspread.Worksheet, headers: List[str]) -> int:
    if not DUFFEL_ENABLED:
        log("Duffel: DISABLED")
        return 0
    if not DUFFEL_API_KEY:
        log("Duffel: ENABLED but missing DUFFEL_API_KEY -> skipping.")
        return 0

    sh = ws.spreadsheet
    routes = load_routes_from_config(sh)
    if not routes:
        routes = [(DEFAULT_ORIGIN_IATA, DEFAULT_DEST_IATA)]

    routes = routes[: max(1, DUFFEL_ROUTES_PER_RUN)]
    log(f"Duffel: ENABLED | routes_per_run={DUFFEL_ROUTES_PER_RUN} | max_inserts={DUFFEL_MAX_INSERTS}")
    log(f"Routes selected: {routes}")

    hmap = header_map(headers)

    required = ["deal_id", "origin_iata", "destination_iata", "outbound_date", "return_date", "price_gbp", "status"]
    missing = [c for c in required if c not in hmap]
    if missing:
        raise RuntimeError(f"RAW_DEALS missing required columns: {missing}")

    inserted = 0
    today = dt.date.today()

    for (origin, dest) in routes:
        out_date = today + dt.timedelta(days=min(21, DAYS_AHEAD))
        ret_date = out_date + dt.timedelta(days=TRIP_LENGTH_DAYS)

        log(f"Duffel search: {origin}->{dest} {iso_date(out_date)}->{iso_date(ret_date)}")
        data = duffel_offer_request(origin, dest, iso_date(out_date), iso_date(ret_date))

        offers = []
        try:
            offers = (data.get("data") or {}).get("offers") or []
        except Exception:
            offers = []

        if not offers:
            log("Duffel returned 0 offers.")
            continue

        rows_to_append: List[List[Any]] = []
        for off in offers:
            if inserted >= DUFFEL_MAX_INSERTS:
                break

            price = off.get("total_amount") or ""
            currency = off.get("total_currency") or "GBP"
            if currency != "GBP":
                continue

            owner = (off.get("owner") or {}).get("name") or ""

            slices = off.get("slices") or []
            stops = "0"
            try:
                segs = (slices[0].get("segments") or [])
                stops = str(max(0, len(segs) - 1))
            except Exception:
                pass

            deal_id = str(uuid.uuid4())

            row_obj = {h: "" for h in headers}
            row_obj["deal_id"] = deal_id
            row_obj["origin_iata"] = origin
            row_obj["destination_iata"] = dest

            # Human-readable city/country if Duffel provides it
            origin_city = ""
            dest_city = ""
            dest_country = ""
            try:
                if slices:
                    o = (slices[0].get("origin") or {})
                    d = (slices[0].get("destination") or {})
                    origin_city = (o.get("city_name") or o.get("name") or "").strip()
                    dest_city = (d.get("city_name") or d.get("name") or "").strip()
                    dest_country = (d.get("country_name") or "").strip()
            except Exception:
                pass

            def _title(x: str) -> str:
                x = (x or "").strip()
                return x.title() if x else ""

            if "origin_city" in row_obj:
                row_obj["origin_city"] = _title(origin_city) or origin
            if "destination_city" in row_obj:
                row_obj["destination_city"] = _title(dest_city) or dest
            if "destination_country" in row_obj:
                row_obj["destination_country"] = _title(dest_country)

            row_obj["outbound_date"] = iso_date(out_date)
            row_obj["return_date"] = iso_date(ret_date)
            row_obj["price_gbp"] = price

            if "trip_length_days" in row_obj:
                row_obj["trip_length_days"] = str(TRIP_LENGTH_DAYS)
            if "deal_source" in row_obj:
                row_obj["deal_source"] = "DUFFEL"
            if "theme" in row_obj:
                row_obj["theme"] = ""
            if "date_added" in row_obj:
                row_obj["date_added"] = now_utc().replace(microsecond=0).isoformat() + "Z"

            if "airline" in row_obj:
                row_obj["airline"] = owner
            if "stops" in row_obj:
                row_obj["stops"] = stops

            row_obj["status"] = STATUS_NEW

            if "booking_link_free" in row_obj:
                row_obj["booking_link_free"] = ""
            if "booking_link_vip" in row_obj:
                row_obj["booking_link_vip"] = ""

            rows_to_append.append([row_obj.get(h, "") for h in headers])
            inserted += 1

        if rows_to_append:
            ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
            log(f"Inserted {len(rows_to_append)} new rows from Duffel.")
        else:
            log("No insertable offers after filtering.")

    return inserted


# =========================
# SCORING
# =========================
def openai_client() -> OpenAI:
    if not OPENAI_API_KEY:
        raise RuntimeError("Missing OPENAI_API_KEY")
    return OpenAI(api_key=OPENAI_API_KEY)


def score_caption(row: Dict[str, str]) -> Tuple[str, str, str]:
    """
    returns: (ai_score, ai_verdict, ai_caption)
    """
    client = openai_client()

    origin = safe_get(row, "origin_city") or safe_get(row, "origin_iata") or "UK"
    dest = safe_get(row, "destination_city") or safe_get(row, "destination_iata") or "Somewhere"
    price = safe_get(row, "price_gbp") or "?"
    out_date = safe_get(row, "outbound_date")
    ret_date = safe_get(row, "return_date")

    prompt = f"""
You are a UK travel-deals copywriter.
Write a short, punchy caption for a flight deal.

Deal:
- From: {origin}
- To: {dest}
- Price: £{price}
- Dates: {out_date} to {ret_date}

Return JSON with keys:
ai_score (0-100 integer),
ai_ve_
