#!/usr/bin/env python3
"""
TravelTxter V4.2 — Unified Pipeline (Best-of-batch + Render + Instagram + Telegram)

Pipeline:
1) Duffel Feeder (CONFIG-driven) -> inserts NEW
2) AI Scoring (batch score all NEW) -> writes ai_score/ai_verdict/ai_caption + flags
3) Select Best -> promotes top deal(s) to READY_TO_POST
4) Render -> calls PythonAnywhere RENDER_URL -> writes graphic_url -> promotes READY_TO_PUBLISH
5) Instagram (MARKETING) -> posts on BOTH AM + PM -> promotes POSTED_INSTAGRAM
6) Telegram VIP (PRODUCT) -> AM only -> promotes POSTED_TELEGRAM_VIP
7) Telegram Free (PRODUCT preview) -> PM only after delay -> promotes POSTED_ALL

Key rules:
- Instagram must post twice a day if there is eligible content (marketing channel).
- Telegram is paid product: VIP-first, free delayed.
- City names preferred over IATA codes for public-facing content.
- Dates displayed as YYMMDD for public-facing (render + telegram).
- Duffel-Version must be v2 (v1 retired 23-01-2025 per Duffel docs).
"""

import os
import json
import uuid
import math
import time
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
GCP_SA_JSON = os.getenv("GCP_SA_JSON", "").strip()
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "").strip()
RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS").strip()
CONFIG_TAB = os.getenv("CONFIG_TAB", "CONFIG").strip()

# Duffel
DUFFEL_API_KEY = os.getenv("DUFFEL_API_KEY", "").strip()
DUFFEL_VERSION = os.getenv("DUFFEL_VERSION", "v2").strip()  # IMPORTANT: v2
DUFFEL_MAX_INSERTS = int(os.getenv("DUFFEL_MAX_INSERTS", "3"))
DUFFEL_ROUTES_PER_RUN = int(os.getenv("DUFFEL_ROUTES_PER_RUN", "1"))
DAYS_AHEAD = int(os.getenv("DAYS_AHEAD", "60"))
TRIP_LENGTH_DAYS = int(os.getenv("TRIP_LENGTH_DAYS", "5"))

DEFAULT_ORIGIN_IATA = os.getenv("DEFAULT_ORIGIN_IATA", "LON").strip().upper()
DEFAULT_DEST_IATA = os.getenv("DEFAULT_DEST_IATA", "KEF").strip().upper()

# OpenAI
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()

# Rendering & Instagram
RENDER_URL = os.getenv("RENDER_URL", "").strip()
RENDER_TIMEOUT = int(os.getenv("RENDER_TIMEOUT", "30"))

IG_ACCESS_TOKEN = os.getenv("IG_ACCESS_TOKEN", "").strip()
IG_USER_ID = os.getenv("IG_USER_ID", "").strip()

# IG gating (but we still post best available)
IG_MIN_SCORE = int(os.getenv("IG_MIN_SCORE", "80"))

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHANNEL = os.getenv("TELEGRAM_CHANNEL", "").strip()

TELEGRAM_BOT_TOKEN_VIP = os.getenv("TELEGRAM_BOT_TOKEN_VIP", "").strip()
TELEGRAM_CHANNEL_VIP = os.getenv("TELEGRAM_CHANNEL_VIP", "").strip()

VIP_DELAY_HOURS = int(os.getenv("VIP_DELAY_HOURS", "24"))
RUN_SLOT = os.getenv("RUN_SLOT", "AM").strip().upper()  # AM / PM

# Stripe links (locked)
STRIPE_LINK_MONTHLY = os.getenv("STRIPE_LINK_MONTHLY", "").strip()
STRIPE_LINK_YEARLY = os.getenv("STRIPE_LINK_YEARLY", "").strip()

# Optional Skyscanner affiliate
SKYSCANNER_AFFILIATE_ID = os.getenv("SKYSCANNER_AFFILIATE_ID", "").strip()


# =========================
# Status constants
# =========================
STATUS_NEW = "NEW"
STATUS_READY_TO_POST = "READY_TO_POST"
STATUS_READY_TO_PUBLISH = "READY_TO_PUBLISH"
STATUS_POSTED_INSTAGRAM = "POSTED_INSTAGRAM"
STATUS_POSTED_TELEGRAM_VIP = "POSTED_TELEGRAM_VIP"
STATUS_POSTED_ALL = "POSTED_ALL"


# =========================
# Helpers
# =========================
def safe_get(row: Dict[str, str], key: str) -> str:
    return (row.get(key) or "").strip()


def header_map(headers: List[str]) -> Dict[str, int]:
    return {h.strip(): i + 1 for i, h in enumerate(headers) if h.strip()}


def parse_ts(s: str) -> Optional[dt.datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1]
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None


def round_price_up(price_str: str) -> int:
    try:
        return int(math.ceil(float(price_str)))
    except Exception:
        return 0


def format_date_yymmdd(date_str: str) -> str:
    """Convert YYYY-MM-DD to YYMMDD (or handle already formatted)."""
    try:
        if not date_str:
            return ""
        s = date_str.strip()

        # YYYYMMDD
        if len(s) == 8 and s.isdigit():
            return s[2:]  # YYMMDD

        # YYMMDD
        if len(s) == 6 and s.isdigit():
            return s

        # ISO
        if "-" in s:
            parts = s.split("-")
            if len(parts) == 3:
                yy = parts[0][2:]
                mm = parts[1].zfill(2)
                dd = parts[2].zfill(2)
                return f"{yy}{mm}{dd}"

        return ""
    except Exception:
        return ""


def openai_client() -> OpenAI:
    if not OPENAI_API_KEY:
        raise RuntimeError("Missing OPENAI_API_KEY")
    return OpenAI(api_key=OPENAI_API_KEY)


# =========================
# Google Sheets
# =========================
def get_ws() -> Tuple[gspread.Worksheet, List[str]]:
    if not (GCP_SA_JSON and SPREADSHEET_ID):
        raise RuntimeError("Missing GCP_SA_JSON or SPREADSHEET_ID")

    creds_dict = json.loads(GCP_SA_JSON)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(RAW_DEALS_TAB)
    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("Sheet missing header row (row 1).")
    return ws, [h.strip() for h in headers]


def load_routes_from_config(sh) -> List[Tuple[str, str]]:
    try:
        cfg = sh.worksheet(CONFIG_TAB)
    except Exception:
        return []

    values = cfg.get_all_values()
    if len(values) < 2:
        return []

    headers = [h.strip() for h in values[0]]
    idx = {h: i for i, h in enumerate(headers)}

    if "origin_iata" not in idx or "destination_iata" not in idx:
        return []

    routes: List[Tuple[str, str]] = []
    for r in values[1:]:
        enabled_raw = (r[idx.get("enabled", -1)] if "enabled" in idx else "TRUE").strip().upper()
        if enabled_raw in {"FALSE", "0", "NO", "N"}:
            continue

        o = (r[idx["origin_iata"]] or "").strip().upper()
        d = (r[idx["destination_iata"]] or "").strip().upper()
        if o and d:
            routes.append((o, d))
    return routes


# =========================
# Duffel
# =========================
def iso_date(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")


def duffel_offer_request(origin: str, dest: str, out_date: str, ret_date: str) -> Dict[str, Any]:
    url = "https://api.duffel.com/air/offer_requests"
    headers = {
        "Authorization": f"Bearer {DUFFEL_API_KEY}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Duffel-Version": DUFFEL_VERSION,  # v2
    }
    payload = {
        "data": {
            "slices": [
                {"origin": origin, "destination": dest, "departure_date": out_date},
                {"origin": dest, "destination": origin, "departure_date": ret_date},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": "economy",
            "return_offers": True,
            "currency": "GBP",
        }
    }
    r = requests.post(url, headers=headers, json=payload, timeout=60)
    if r.status_code >= 300:
        raise RuntimeError(f"Duffel error {r.status_code}: {r.text[:500]}")
    return r.json()


def pick_offers(offer_request_json: Dict[str, Any], max_n: int) -> List[Dict[str, Any]]:
    offers = (((offer_request_json or {}).get("data") or {}).get("offers") or [])
    def amt(o: Dict[str, Any]) -> float:
        try:
            return float(o.get("total_amount") or "1e9")
        except Exception:
            return 1e9
    offers_sorted = sorted(offers, key=amt)
    return offers_sorted[: max(0, max_n)]


def generate_skyscanner_link(origin_iata: str, dest_iata: str, out_date: str, ret_date: str) -> str:
    out_formatted = out_date.replace("-", "")
    ret_formatted = ret_date.replace("-", "")
    base_url = f"https://www.skyscanner.net/transport/flights/{origin_iata}/{dest_iata}/{out_formatted}/{ret_formatted}/"
    if SKYSCANNER_AFFILIATE_ID:
        return f"{base_url}?affiliateid={SKYSCANNER_AFFILIATE_ID}"
    return base_url


def stage_duffel_feeder(ws: gspread.Worksheet, headers: List[str]) -> int:
    if not DUFFEL_API_KEY:
        log("Duffel: DISABLED (no DUFFEL_API_KEY)")
        return 0

    sh = ws.spreadsheet
    routes = load_routes_from_config(sh)
    if not routes:
        routes = [(DEFAULT_ORIGIN_IATA, DEFAULT_DEST_IATA)]

    routes = routes[: max(1, DUFFEL_ROUTES_PER_RUN)]
    log(f"Duffel: ENABLED | routes_per_run={len(routes)} | max_inserts={DUFFEL_MAX_INSERTS}")

    hmap = header_map(headers)
    required = ["deal_id", "origin_iata", "destination_iata", "outbound_date", "return_date", "price_gbp", "status"]
    missing = [c for c in required if c not in hmap]
    if missing:
        raise RuntimeError(f"RAW_DEALS missing required columns: {missing}")

    total_inserted = 0
    today = dt.date.today()

    for (origin, dest) in routes:
        out_date = today + dt.timedelta(days=DAYS_AHEAD)
        ret_date = out_date + dt.timedelta(days=TRIP_LENGTH_DAYS)

        log(f"Duffel search: {origin}->{dest} {iso_date(out_date)}->{iso_date(ret_date)}")
        data = duffel_offer_request(origin, dest, iso_date(out_date), iso_date(ret_date))
        offers = pick_offers(data, DUFFEL_MAX_INSERTS)

        # Try to pull city names from Duffel response
        origin_city = origin
        dest_city = dest
        try:
            slices = (((data or {}).get("data") or {}).get("slices") or [])
            if slices:
                origin_city = ((slices[0].get("origin") or {}).get("city_name") or origin).strip()
                dest_city = ((slices[0].get("destination") or {}).get("city_name") or dest).strip()
        except Exception:
            pass

        rows_to_append: List[List[Any]] = []
        for off in offers:
            price = off.get("total_amount") or ""
            currency = off.get("total_currency") or "GBP"
            if currency != "GBP":
                continue

            airline = (off.get("owner") or {}).get("name") or ""

            # Stops (outbound slice)
            stops = "0"
            try:
                slices = off.get("slices") or []
                segs = (slices[0].get("segments") or [])
                stops = str(max(0, len(segs) - 1))
            except Exception:
                pass

            dest_country = ""
            try:
                slices = off.get("slices") or []
                if slices:
                    d = (slices[0].get("destination") or {})
                    dest_country = (d.get("country_name") or "").strip()
            except Exception:
                pass

            deal_id = str(uuid.uuid4())

            row_obj = {h: "" for h in headers}
            row_obj["deal_id"] = deal_id
            row_obj["origin_iata"] = origin
            row_obj["origin_city"] = origin_city or origin
            row_obj["destination_iata"] = dest
            row_obj["destination_city"] = dest_city or dest
            row_obj["destination_country"] = dest_country
            row_obj["outbound_date"] = iso_date(out_date)
            row_obj["return_date"] = iso_date(ret_date)
            row_obj["price_gbp"] = str(price)
            row_obj["trip_length_days"] = str(TRIP_LENGTH_DAYS)
            row_obj["airline"] = airline
            row_obj["stops"] = stops
            row_obj["deal_source"] = "DUFFEL"
            row_obj["date_added"] = now_utc().replace(microsecond=0).isoformat() + "Z"
            row_obj["status"] = STATUS_NEW

            booking_link = generate_skyscanner_link(origin, dest, iso_date(out_date), iso_date(ret_date))
            row_obj["booking_link_vip"] = booking_link
            row_obj["booking_link_free"] = booking_link
            row_obj["affiliate_url"] = booking_link

            rows_to_append.append([row_obj.get(h, "") for h in headers])

        if rows_to_append:
            ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
            log(f"  ✓ Inserted {len(rows_to_append)} deals for {origin_city}->{dest_city}")
            total_inserted += len(rows_to_append)
        else:
            log("  No valid GBP offers to insert")

    return total_inserted


# =========================
# AI Scoring
# =========================
def score_deal_with_ai(row: Dict[str, str]) -> Dict[str, Any]:
    client = openai_client()

    origin_city = safe_get(row, "origin_city") or safe_get(row, "origin_iata")
    dest_city = safe_get(row, "destination_city") or safe_get(row, "destination_iata")
    dest_country = safe_get(row, "destination_country")
    price = safe_get(row, "price_gbp")
    out_date = safe_get(row, "outbound_date")
    ret_date = safe_get(row, "return_date")
    airline = safe_get(row, "airline") or "Unknown"
    stops = safe_get(row, "stops") or "0"
    trip_length = safe_get(row, "trip_length_days") or "5"

    prompt = f"""
You are a UK travel-deals evaluator for TravelTxter.

Score this deal 0-100 based on:
- Price value for route/season
- Stops (prefer direct)
- Airline reputation (basic common-sense)
- Practicality (trip length)

Deal:
From: {origin_city}
To: {dest_city} ({dest_country})
Price: £{price}
Dates: {out_date} to {ret_date}
Trip length: {trip_length} days
Stops: {stops}
Airline: {airline}

Return JSON only with:
ai_score (integer 0-100),
ai_verdict (EXCELLENT/GOOD/AVERAGE/POOR),
ai_caption (max 240 chars).
""".strip()

    resp = client.responses.create(model=OPENAI_MODEL, input=prompt)
    text_out = (resp.output_text or "").strip()

    ai_score = 60
    ai_verdict = "AVERAGE"
    ai_caption = text_out[:240]

    try:
        obj = json.loads(text_out)
        ai_score = int(obj.get("ai_score", ai_score))
        ai_verdict = str(obj.get("ai_verdict", ai_verdict)).upper().strip()
        ai_caption = str(obj.get("ai_caption", ai_caption)).strip()[:240]
    except Exception:
        pass

    # Flags (DO NOT choke posting; just hints)
    is_instagram_eligible = ai_score >= IG_MIN_SCORE
    telegram_priority = "High" if ai_score >= 85 else ("Medium" if ai_score >= 70 else "Low")

    return {
        "ai_score": ai_score,
        "ai_verdict": ai_verdict,
        "ai_caption": ai_caption,
        "is_instagram_eligible": str(is_instagram_eligible),
        "telegram_priority": telegram_priority,
    }


def stage_score_all_new(ws: gspread.Worksheet, headers: List[str]) -> int:
    hmap = header_map(headers)
    if "status" not in hmap:
        raise RuntimeError("Missing 'status' column")

    rows = ws.get_all_values()
    if len(rows) < 2:
        return 0

    scored = 0
    for i in range(2, len(rows) + 1):
        row_vals = rows[i - 1]
        row = {headers[c]: (row_vals[c] if c < len(row_vals) else "") for c in range(len(headers))}

        if safe_get(row, "status").upper() != STATUS_NEW:
            continue

        # skip if already scored
        if safe_get(row, "ai_score"):
            continue

        log(f"Scoring row {i}: {safe_get(row, 'origin_city')}->{safe_get(row, 'destination_city')} £{safe_get(row, 'price_gbp')}")
        result = score_deal_with_ai(row)

        updates = []
        if "ai_score" in hmap:
            updates.append(gspread.Cell(i, hmap["ai_score"], str(result["ai_score"])))
        if "ai_verdict" in hmap:
            updates.append(gspread.Cell(i, hmap["ai_verdict"], result["ai_verdict"]))
        if "ai_caption" in hmap:
            updates.append(gspread.Cell(i, hmap["ai_caption"], result["ai_caption"]))
        if "is_instagram_eligible" in hmap:
            updates.append(gspread.Cell(i, hmap["is_instagram_eligible"], result["is_instagram_eligible"]))
        if "telegram_priority" in hmap:
            updates.append(gspread.Cell(i, hmap["telegram_priority"], result["telegram_priority"]))
        if "scored_timestamp" in hmap:
            updates.append(gspread.Cell(i, hmap["scored_timestamp"], now_utc().replace(microsecond=0).isoformat() + "Z"))

        if updates:
            ws.update_cells(updates, value_input_option="USER_ENTERED")

        log(f"  ✓ Score {result['ai_score']}/100 | {result['ai_verdict']} | IG flag={result['is_instagram_eligible']}")
        scored += 1

    return scored


# =========================
# Select Best (NEW -> READY_TO_POST)
# =========================
def stage_select_best(ws: gspread.Worksheet, headers: List[str], max_to_promote: int = 1) -> int:
    hmap = header_map(headers)
    rows = ws.get_all_values()
    if len(rows) < 2:
        return 0

    candidates: List[Tuple[int, float, Dict[str, str]]] = []

    for i in range(2, len(rows) + 1):
        row_vals = rows[i - 1]
        row = {headers[c]: (row_vals[c] if c < len(row_vals) else "") for c in range(len(headers))}

        if safe_get(row, "status").upper() != STATUS_NEW:
            continue

        s = safe_get(row, "ai_score")
        if not s:
            continue

        try:
            ai_score = float(s)
        except Exception:
            continue

        # freshness decay
        age_days = 0
        d = safe_get(row, "date_added")
        if d:
            try:
                dt_added = dt.datetime.fromisoformat(d.replace("Z", "+00:00"))
                age_days = (now_utc() - dt_added).days
            except Exception:
                pass

        final_score = ai_score - (2.0 * age_days)
        candidates.append((i, final_score, row))

    if not candidates:
        log("No scored candidates to promote.")
        return 0

    candidates.sort(key=lambda x: x[1], reverse=True)

    promoted = 0
    for row_idx, final_score, row_data in candidates[:max_to_promote]:
        if "status" in hmap:
            ws.update_cell(row_idx, hmap["status"], STATUS_READY_TO_POST)
            log(f"Promoted row {row_idx} -> READY_TO_POST (score {final_score:.1f})")
            log(f"  {safe_get(row_data, 'origin_city')}->{safe_get(row_data, 'destination_city')} £{safe_get(row_data, 'price_gbp')}")
            promoted += 1

    return promoted


# =========================
# Render (READY_TO_POST -> READY_TO_PUBLISH)
# =========================
def call_render(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not RENDER_URL:
        raise RuntimeError("Missing RENDER_URL")
    r = requests.post(RENDER_URL, json=payload, timeout=RENDER_TIMEOUT)
    if r.status_code >= 300:
        raise RuntimeError(f"Render failed {r.status_code}: {r.text[:300]}")
    try:
        return r.json()
    except Exception:
        raise RuntimeError("Render response was not JSON.")


def stage_render(ws: gspread.Worksheet, headers: List[str], max_rows: int = 1) -> int:
    hmap = header_map(headers)
    rows = ws.get_all_values()
    if len(rows) < 2:
        return 0

    rendered = 0
    for i in range(2, len(rows) + 1):
        if rendered >= max_rows:
            break

        row_vals = rows[i - 1]
        row = {headers[c]: (row_vals[c] if c < len(row_vals) else "") for c in range(len(headers))}

        if safe_get(row, "status").upper() != STATUS_READY_TO_POST:
            continue

        # If already has graphic_url, just promote
        if safe_get(row, "graphic_url"):
            if "status" in hmap:
                ws.update_cell(i, hmap["status"], STATUS_READY_TO_PUBLISH)
            rendered += 1
            continue

        # Build a V3-compatible payload shape (city names, GBP, YYMMDD)
        origin_city = safe_get(row, "origin_city") or safe_get(row, "origin_iata")
        dest_city = safe_get(row, "destination_city") or safe_get(row, "destination_iata")

        payload = {
            "deal_id": safe_get(row, "deal_id"),
            "origin_city": origin_city,
            "destination_city": dest_city,
            "destination_country": safe_get(row, "destination_country"),
            "price_gbp": str(round_price_up(safe_get(row, "price_gbp"))),
            "outbound_date": format_date_yymmdd(safe_get(row, "outbound_date")),
            "return_date": format_date_yymmdd(safe_get(row, "return_date")),
            "trip_length_days": safe_get(row, "trip_length_days"),
            "stops": safe_get(row, "stops"),
            "airline": safe_get(row, "airline"),
            "ai_caption": safe_get(row, "ai_caption"),
        }

        log(f"Rendering row {i}: {origin_city}->{dest_city} £{payload['price_gbp']} OUT {payload['outbound_date']}")

        try:
            data = call_render(payload)
            graphic_url = (data.get("graphic_url") or data.get("image_url") or "").strip()
            if not graphic_url:
                raise RuntimeError("Render returned no graphic_url/image_url")

            updates = []
            if "graphic_url" in hmap:
                updates.append(gspread.Cell(i, hmap["graphic_url"], graphic_url))
            if "rendered_timestamp" in hmap:
                updates.append(gspread.Cell(i, hmap["rendered_timestamp"], now_utc().replace(microsecond=0).isoformat() + "Z"))
            if "render_error" in hmap:
                updates.append(gspread.Cell(i, hmap["render_error"], ""))
            if "status" in hmap:
                updates.append(gspread.Cell(i, hmap["status"], STATUS_READY_TO_PUBLISH))

            if updates:
                ws.update_cells(updates, value_input_option="USER_ENTERED")

            log(f"  ✓ Render OK: {graphic_url}")
            rendered += 1

        except Exception as e:
            msg = str(e)[:200]
            log(f"  ✗ Render error: {msg}")
            updates = []
            if "render_error" in hmap:
                updates.append(gspread.Cell(i, hmap["render_error"], msg))
            if "status" in hmap:
                # leave as READY_TO_POST so it retries next run
                updates.append(gspread.Cell(i, hmap["status"], STATUS_READY_TO_POST))
            if updates:
                ws.update_cells(updates, value_input_option="USER_ENTERED")
            rendered += 1

    return rendered


# =========================
# Instagram (READY_TO_PUBLISH -> POSTED_INSTAGRAM)
# =========================
def create_ig_container(image_url: str, caption: str) -> str:
    url = f"https://graph.facebook.com/v19.0/{IG_USER_ID}/media"
    payload = {"image_url": image_url, "caption": caption, "access_token": IG_ACCESS_TOKEN}
    r = requests.post(url, data=payload, timeout=60)
    if r.status_code >= 300:
        raise RuntimeError(f"IG create container failed {r.status_code}: {r.text[:300]}")
    return (r.json() or {}).get("id", "")


def publish_ig_container(creation_id: str) -> str:
    url = f"https://graph.facebook.com/v19.0/{IG_USER_ID}/media_publish"
    payload = {"creation_id": creation_id, "access_token": IG_ACCESS_TOKEN}
    r = requests.post(url, data=payload, timeout=60)
    if r.status_code >= 300:
        raise RuntimeError(f"IG publish failed {r.status_code}: {r.text[:300]}")
    return (r.json() or {}).get("id", "")


def stage_instagram(ws: gspread.Worksheet, headers: List[str], max_rows: int = 1) -> int:
    if not (IG_ACCESS_TOKEN and IG_USER_ID):
        log("Instagram: Missing IG_ACCESS_TOKEN or IG_USER_ID (skipping)")
        return 0

    hmap = header_map(headers)
    rows = ws.get_all_values()
    if len(rows) < 2:
        return 0

    posted = 0
    for i in range(2, len(rows) + 1):
        if posted >= max_rows:
            break

        row_vals = rows[i - 1]
        row = {headers[c]: (row_vals[c] if c < len(row_vals) else "") for c in range(len(headers))}

        if safe_get(row, "status").upper() != STATUS_READY_TO_PUBLISH:
            continue

        graphic_url = safe_get(row, "graphic_url")
        if not graphic_url:
            log(f"Row {i}: No graphic_url yet (skipping IG)")
            continue

        # Always short + human; image carries the detail
        origin_city = safe_get(row, "origin_city") or safe_get(row, "origin_iata")
        dest_city = safe_get(row, "destination_city") or safe_get(row, "destination_iata")
        price = round_price_up(safe_get(row, "price_gbp"))
        out_d = format_date_yymmdd(safe_get(row, "outbound_date"))
        back_d = format_date_yymmdd(safe_get(row, "return_date"))

        caption = (
            f"£{price} {origin_city} → {dest_city}\n"
            f"OUT {out_d} / BACK {back_d}\n\n"
            f"{safe_get(row, 'ai_caption')}\n\n"
            f"Get deals 24h early in VIP. Link in bio."
        ).strip()

        log(f"Instagram posting row {i}: {origin_city}->{dest_city} £{price}")
        try:
            creation_id = create_ig_container(graphic_url, caption)
            media_id = publish_ig_container(creation_id)

            updates = []
            if "ig_media_id" in hmap:
                updates.append(gspread.Cell(i, hmap["ig_media_id"], media_id))
            if "ig_published_timestamp" in hmap:
                updates.append(gspread.Cell(i, hmap["ig_published_timestamp"], now_utc().replace(microsecond=0).isoformat() + "Z"))
            if "status" in hmap:
                updates.append(gspread.Cell(i, hmap["status"], STATUS_POSTED_INSTAGRAM))

            if updates:
                ws.update_cells(updates, value_input_option="USER_ENTERED")

            log("  ✓ IG posted")
            posted += 1

        except Exception as e:
            log(f"  ✗ IG error: {str(e)[:200]}")
            # leave status as READY
