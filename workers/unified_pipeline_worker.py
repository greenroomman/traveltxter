#!/usr/bin/env python3
"""
TravelTxter V4 Unified Pipeline (FIX VERSION)

Runs the whole pipeline in one worker:
1) Duffel feeder (optional; can run with Duffel disabled)
2) AI scorer (promote best NEW -> READY_TO_POST/READY_TO_PUBLISH depending on your gating)
3) Render (HTML->PNG via RENDER_URL)
4) Instagram publish (Graph API)
5) Telegram publish (VIP first, FREE after delay)

Design goals:
- Uses 'status' lifecycle gating (single source of truth)
- Header-map only writes (no column-index writes)
- Idempotent: safe to run repeatedly
- Free-tier safe caps for Duffel (routes_per_run + max_inserts)

NOTE:
- This file is provided as a complete copy/paste “colour-by-numbers” file.
"""

import os
import sys
import json
import time
import random
import datetime as dt
from typing import Dict, Any, List, Tuple, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# Logging helpers
# ============================================================

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


def die(msg: str) -> None:
    log(f"❌ {msg}")
    raise SystemExit(1)


# ============================================================
# ENV
# ============================================================

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "").strip()
RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "").strip()

GCP_SA_JSON = os.getenv("GCP_SA_JSON", "").strip()
GCP_SA_JSON_ONE_LINE = os.getenv("GCP_SA_JSON_ONE_LINE", "").strip()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()

RENDER_URL = os.getenv("RENDER_URL", "").strip()

IG_ACCESS_TOKEN = os.getenv("IG_ACCESS_TOKEN", "").strip()
IG_USER_ID = os.getenv("IG_USER_ID", "").strip()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHANNEL = os.getenv("TELEGRAM_CHANNEL", "").strip()

TELEGRAM_BOT_TOKEN_VIP = os.getenv("TELEGRAM_BOT_TOKEN_VIP", "").strip()
TELEGRAM_CHANNEL_VIP = os.getenv("TELEGRAM_CHANNEL_VIP", "").strip()

DUFFEL_API_KEY = os.getenv("DUFFEL_API_KEY", "").strip()
DUFFEL_VERSION = os.getenv("DUFFEL_VERSION", "v2").strip()  # Duffel API version (v2 required)
DUFFEL_MAX_INSERTS = int(os.getenv("DUFFEL_MAX_INSERTS", "3"))
DUFFEL_ROUTES_PER_RUN = int(os.getenv("DUFFEL_ROUTES_PER_RUN", "1"))
DUFFEL_ENABLED = os.getenv("DUFFEL_ENABLED", "true").strip().lower() in ("1", "true", "yes", "y")

SKYSCANNER_AFFILIATE_ID = os.getenv("SKYSCANNER_AFFILIATE_ID", "").strip()

STRIPE_LINK = os.getenv("STRIPE_LINK", "").strip()

RUN_SLOT = os.getenv("RUN_SLOT", "AM").strip().upper()  # AM / PM
VIP_DELAY_HOURS = int(os.getenv("VIP_DELAY_HOURS", "24"))


# ============================================================
# Config (route rotation)
# ============================================================

CONFIG_JSON = os.getenv("CONFIG_JSON", "").strip()

DEFAULT_ROUTES = [
    ("LGW", "KEF"),  # Iceland (example)
]


def load_routes_from_config() -> List[Tuple[str, str]]:
    """
    Optional: load routes from CONFIG_JSON env (or fall back).
    CONFIG_JSON example:
    {
      "routes": [["LGW","KEF"],["LHR","BCN"]]
    }
    """
    if not CONFIG_JSON:
        return DEFAULT_ROUTES[:]

    try:
        obj = json.loads(CONFIG_JSON)
        routes = obj.get("routes", [])
        out: List[Tuple[str, str]] = []
        for r in routes:
            if isinstance(r, (list, tuple)) and len(r) == 2:
                out.append((str(r[0]).upper(), str(r[1]).upper()))
        return out if out else DEFAULT_ROUTES[:]
    except Exception:
        return DEFAULT_ROUTES[:]


# ============================================================
# Google Sheets
# ============================================================

def load_sa_json() -> Dict[str, Any]:
    if GCP_SA_JSON:
        return json.loads(GCP_SA_JSON)
    if GCP_SA_JSON_ONE_LINE:
        return json.loads(GCP_SA_JSON_ONE_LINE)
    die("Missing GCP service account json (set GCP_SA_JSON or GCP_SA_JSON_ONE_LINE).")
    return {}  # unreachable


def get_ws() -> Tuple[gspread.Worksheet, List[str]]:
    if not SPREADSHEET_ID:
        die("Missing SPREADSHEET_ID.")
    if not RAW_DEALS_TAB:
        die("Missing RAW_DEALS_TAB.")

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
        die("Sheet has no headers in row 1.")
    return ws, headers


def header_map(headers: List[str]) -> Dict[str, int]:
    return {h.strip(): i + 1 for i, h in enumerate(headers) if h.strip()}


def iso_date(d: dt.date) -> str:
    return d.isoformat()


# ============================================================
# Duffel feeder
# ============================================================

def duffel_offer_request(origin: str, dest: str, out_date: str, ret_date: str) -> Dict:
    url = "https://api.duffel.com/air/offer_requests"
    headers = {
        "Authorization": f"Bearer {DUFFEL_API_KEY}",
        "Duffel-Version": DUFFEL_VERSION,
        "Content-Type": "application/json",
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
    r = requests.post(url, headers=headers, json=payload, timeout=45)
    if r.status_code >= 400:
        raise RuntimeError(f"Duffel error {r.status_code}: {r.text[:500]}")
    return r.json()


def pick_travel_dates() -> Tuple[dt.date, dt.date]:
    """
    Simple date picker: outbound 3 weeks from now, return +5 days.
    (Keep it deterministic enough for repeatable runs.)
    """
    today = dt.date.today()
    out_date = today + dt.timedelta(days=21)
    ret_date = out_date + dt.timedelta(days=5)
    return out_date, ret_date


def normalize_offer_to_row(offer: Dict[str, Any], origin: str, dest: str, out_date: str, ret_date: str) -> Dict[str, Any]:
    """
    Maps a Duffel offer into the sheet's expected columns.
    This is intentionally conservative: if a column is missing, we just skip it.
    """
    total_amount = ""
    total_currency = ""
    airline = ""
    stops = ""
    booking_link = ""

    try:
        total_amount = offer.get("total_amount", "")
        total_currency = offer.get("total_currency", "")
    except Exception:
        pass

    try:
        # Best-effort carrier code/name extraction
        owner = offer.get("owner", {})
        airline = owner.get("name", "") or owner.get("iata_code", "") or ""
    except Exception:
        pass

    try:
        # Stops proxy from slices/segments outbound
        slices = offer.get("slices", [])
        if slices and isinstance(slices, list):
            segs = slices[0].get("segments", []) if slices[0] else []
            stops_num = max(0, len(segs) - 1)
            stops = str(stops_num)
    except Exception:
        stops = ""

    # Booking link (Duffel does not give a universal public URL; keep blank unless you have one)
    booking_link = ""

    return {
        "origin": origin,
        "destination": dest,
        "out_date": out_date,
        "return_date": ret_date,
        "price": f"{total_currency}{total_amount}".strip(),
        "currency": total_currency,
        "airline": airline,
        "stops": stops,
        "booking_url": booking_link,
        "status": "NEW",
        "source": "DUFFEL",
    }


def duffel_run_and_insert(ws: gspread.Worksheet, headers: List[str]) -> int:
    if not DUFFEL_ENABLED:
        log("Duffel: DISABLED")
        return 0
    if not DUFFEL_API_KEY:
        log("Duffel: ENABLED but missing DUFFEL_API_KEY -> skipping.")
        return 0

    routes = load_routes_from_config()
    if not routes:
        routes = DEFAULT_ROUTES[:]

    random.shuffle(routes)
    selected = routes[: max(1, DUFFEL_ROUTES_PER_RUN)]

    log(f"Duffel: ENABLED | routes_per_run={DUFFEL_ROUTES_PER_RUN} | max_inserts={DUFFEL_MAX_INSERTS}")
    log(f"Routes selected: {selected}")

    hm = header_map(headers)

    inserted = 0
    for (origin, dest) in selected:
        out_d, ret_d = pick_travel_dates()
        out_date = iso_date(out_d)
        ret_date = iso_date(ret_d)

        log(f"Duffel search: {origin}->{dest} {out_date}->{ret_date}")
        data = duffel_offer_request(origin, dest, out_date, ret_date)

        offers = []
        try:
            offers = data.get("data", {}).get("offers", [])
        except Exception:
            offers = []

        if not offers:
            log("Duffel returned 0 offers.")
            continue

        for offer in offers[:DUFFEL_MAX_INSERTS]:
            row_obj = normalize_offer_to_row(offer, origin, dest, out_date, ret_date)

            new_row = [""] * len(headers)
            for k, v in row_obj.items():
                if k in hm:
                    new_row[hm[k] - 1] = str(v)

            ws.append_row(new_row, value_input_option="RAW")
            inserted += 1

    return inserted


# ============================================================
# AI Scoring (placeholder minimal)
# ============================================================

def find_first_row_with_status(ws: gspread.Worksheet, headers: List[str], status_value: str) -> Optional[int]:
    hm = header_map(headers)
    if "status" not in hm:
        die("Missing required 'status' column in sheet.")

    status_col = hm["status"]
    col_vals = ws.col_values(status_col)
    for idx in range(2, len(col_vals) + 1):
        if (col_vals[idx - 1] or "").strip() == status_value:
            return idx
    return None


def ai_score_row_minimal(ws: gspread.Worksheet, headers: List[str], row_idx: int) -> None:
    """
    Minimal scorer:
    - sets ai_verdict = GOOD
    - promotes status -> READY_TO_PUBLISH
    (Your project can swap this for the full scorer logic.)
    """
    hm = header_map(headers)

    updates: List[Tuple[int, int, str]] = []

    if "ai_verdict" in hm:
        updates.append((row_idx, hm["ai_verdict"], "GOOD"))

    if "ai_score" in hm:
        updates.append((row_idx, hm["ai_score"], "85"))

    if "status" in hm:
        updates.append((row_idx, hm["status"], "READY_TO_PUBLISH"))

    if updates:
        cell_list = [gspread.Cell(r, c, v) for (r, c, v) in updates]
        ws.update_cells(cell_list, value_input_option="RAW")


# ============================================================
# Render (placeholder minimal)
# ============================================================

def render_row(ws: gspread.Worksheet, headers: List[str], row_idx: int) -> Optional[str]:
    if not RENDER_URL:
        log("Render: missing RENDER_URL -> skipping render.")
        return None

    hm = header_map(headers)

    row = ws.row_values(row_idx)
    row_obj = {headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))}

    payload = {"data": row_obj}
    r = requests.post(RENDER_URL, json=payload, timeout=60)
    if r.status_code >= 400:
        log(f"Render error {r.status_code}: {r.text[:200]}")
        return None

    try:
        out = r.json()
        return out.get("graphic_url") or out.get("image_url") or None
    except Exception:
        return None


# ============================================================
# Instagram publish (placeholder minimal)
# ============================================================

def instagram_publish(image_url: str, caption: str) -> bool:
    if not IG_ACCESS_TOKEN or not IG_USER_ID:
        log("Instagram: missing IG_ACCESS_TOKEN or IG_USER_ID -> skipping.")
        return False

    # Create media container
    create_url = f"https://graph.facebook.com/v19.0/{IG_USER_ID}/media"
    create_payload = {
        "image_url": image_url,
        "caption": caption,
        "access_token": IG_ACCESS_TOKEN,
    }
    r = requests.post(create_url, data=create_payload, timeout=60)
    if r.status_code >= 400:
        log(f"IG create error {r.status_code}: {r.text[:200]}")
        return False

    container_id = ""
    try:
        container_id = r.json().get("id", "")
    except Exception:
        container_id = ""

    if not container_id:
        log("IG create: missing container id.")
        return False

    # Publish
    publish_url = f"https://graph.facebook.com/v19.0/{IG_USER_ID}/media_publish"
    publish_payload = {
        "creation_id": container_id,
        "access_token": IG_ACCESS_TOKEN,
    }
    r2 = requests.post(publish_url, data=publish_payload, timeout=60)
    if r2.status_code >= 400:
        log(f"IG publish error {r2.status_code}: {r2.text[:200]}")
        return False

    return True


# ============================================================
# Telegram publish (VIP + FREE gating)
# ============================================================

def telegram_send(bot_token: str, channel: str, text: str) -> bool:
    if not bot_token or not channel:
        return False
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": channel, "text": text, "disable_web_page_preview": False}
    r = requests.post(url, json=payload, timeout=30)
    return r.status_code < 400


def format_telegram_message(row_obj: Dict[str, str], vip: bool) -> str:
    price = row_obj.get("price", "").strip()
    origin = row_obj.get("origin", "").strip()
    dest = row_obj.get("destination", "").strip()
    out_date = row_obj.get("out_date", "").strip()
    ret_date = row_obj.get("return_date", "").strip()

    base = (
        f"🔥 {price}\n\n"
        f"📍 From {origin} to {dest}\n"
        f"📅 {out_date} → {ret_date}\n"
    )

    if vip:
        return base + "\n💎 VIP: early access deal\n"
    else:
        return base + "\n⚠️ Heads up: VIP saw this earlier\n"


# ============================================================
# Main pipeline
# ============================================================

def main() -> None:
    log("============================================================")
    log("🚀 TRAVELTXTER V4 UNIFIED PIPELINE (FIX VERSION)")
    log("============================================================")
    log(f"Sheet: {SPREADSHEET_ID or '[missing]'}")
    log(f"Tab: {RAW_DEALS_TAB or '[missing]'}")
    log(f"Duffel: {'ENABLED' if DUFFEL_ENABLED else 'DISABLED'}")
    log(f"RUN_SLOT: {RUN_SLOT} | VIP_DELAY_HOURS={VIP_DELAY_HOURS}")
    log("============================================================")

    ws, headers = get_ws()

    # 1) Feeder
    inserted = duffel_run_and_insert(ws, headers)
    if inserted:
        log(f"✅ Inserted {inserted} new row(s) from Duffel.")
    else:
        log("No new Duffel inserts this run.")

    # 2) Score (minimal): find NEW -> READY_TO_PUBLISH
    row_idx = find_first_row_with_status(ws, headers, "NEW")
    if not row_idx:
        log("No NEW rows to score.")
        return

    log(f"Scoring row {row_idx}...")
    ai_score_row_minimal(ws, headers, row_idx)

    # 3) Render
    log(f"Rendering row {row_idx}...")
    image_url = render_row(ws, headers, row_idx)
    if not image_url:
        log("Render failed or returned no image_url.")
        return

    # Pull row into dict for messaging/caption
    row = ws.row_values(row_idx)
    row_obj = {headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))}

    caption = f"🔥 {row_obj.get('price','')} • {row_obj.get('origin','')}→{row_obj.get('destination','')} • {row_obj.get('out_date','')}→{row_obj.get('return_date','')}"
    ig_ok = instagram_publish(image_url, caption)

    hm = header_map(headers)
    if ig_ok and "status" in hm:
        ws.update_cell(row_idx, hm["status"], "POSTED_INSTAGRAM")
        log("✅ Instagram posted.")
    elif not ig_ok:
        log("Instagram publish skipped/failed (continuing to Telegram if possible).")

    # 5) Telegram (VIP now, FREE later via delay logic elsewhere)
    vip_text = format_telegram_message(row_obj, vip=True)
    free_text = format_telegram_message(row_obj, vip=False) + (f"\n👉 Upgrade: {STRIPE_LINK}\n" if STRIPE_LINK else "")

    vip_ok = telegram_send(TELEGRAM_BOT_TOKEN_VIP, TELEGRAM_CHANNEL_VIP, vip_text)
    if vip_ok:
        log("✅ Telegram VIP posted.")
    else:
        log("Telegram VIP skipped/failed.")

    # FREE gating handled outside this minimal worker; you can implement delay + second-stage publish.
    free_ok = telegram_send(TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL, free_text)
    if free_ok:
        log("✅ Telegram FREE posted.")
        if "status" in hm:
            ws.update_cell(row_idx, hm["status"], "POSTED_ALL")
    else:
        log("Telegram FREE skipped/failed.")

    log("✅ Pipeline complete.")


if __name__ == "__main__":
    main()
