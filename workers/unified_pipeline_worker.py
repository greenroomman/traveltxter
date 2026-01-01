#!/usr/bin/env python3
"""
TravelTxter V4.2 — Unified Pipeline Worker (Production)

State machine:
NEW
  -> READY_TO_POST        (AI scoring + caption)
  -> READY_TO_PUBLISH     (Render via PythonAnywhere, writes graphic_url)
  -> POSTED_INSTAGRAM     (Instagram Graph API)
  -> POSTED_TELEGRAM_VIP  (VIP Telegram)
  -> POSTED_ALL           (FREE Telegram after VIP delay)

Key fixes (per V4 message spec):
- Captions sound human, UK English, no emojis; short (2–4 sentences) :contentReference[oaicite:1]{index=1}
- Render stage exists and calls RENDER_URL, writes graphic_url, advances status
- City names used in payload and messages (not just IATA)
- Duffel-Version is v2 (avoids Unsupported version 400)
"""

import os
import sys
import json
import uuid
import datetime as dt
from typing import Dict, Any, List, Tuple, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# CONFIG / ENV
# ============================================================

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def env(name: str, default: str = "", required: bool = False) -> str:
    v = (os.getenv(name, "") or "").strip()
    if not v:
        v = default
    if required and not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v

GCP_SA_JSON = env("GCP_SA_JSON", required=True)

SPREADSHEET_ID = env("SPREADSHEET_ID", default=env("SHEET_ID"))
RAW_DEALS_TAB = env("RAW_DEALS_TAB", default=env("WORKSHEET_NAME", "RAW_DEALS"))
CONFIG_TAB = env("CONFIG_TAB", "CONFIG")

DUFFEL_API_KEY = env("DUFFEL_API_KEY")
DUFFEL_MAX_INSERTS = int(env("DUFFEL_MAX_INSERTS", "3") or "3")
DUFFEL_ROUTES_PER_RUN = int(env("DUFFEL_ROUTES_PER_RUN", "1") or "1")
DAYS_AHEAD = int(env("DAYS_AHEAD", "60") or "60")
TRIP_LENGTH_DAYS = int(env("TRIP_LENGTH_DAYS", "5") or "5")

RENDER_URL = env("RENDER_URL")
RENDER_TIMEOUT = int(env("RENDER_TIMEOUT", "45") or "45")

IG_ACCESS_TOKEN = env("IG_ACCESS_TOKEN")
IG_USER_ID = env("IG_USER_ID")

TELEGRAM_BOT_TOKEN = env("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL = env("TELEGRAM_CHANNEL", default=env("TELEGRAM_FREE_CHANNEL"))

TELEGRAM_BOT_TOKEN_VIP = env("TELEGRAM_BOT_TOKEN_VIP")
TELEGRAM_CHANNEL_VIP = env("TELEGRAM_CHANNEL_VIP", default=env("TELEGRAM_VIP_CHANNEL"))

STRIPE_LINK = env("STRIPE_LINK")
RUN_SLOT = env("RUN_SLOT", "AM").upper()  # AM/PM (only affects VIP/FREE release)
VIP_DELAY_HOURS = int(env("VIP_DELAY_HOURS", "24") or "24")

MAX_SCORE_PER_RUN = int(env("MAX_SCORE_PER_RUN", "1") or "1")
MAX_RENDER_PER_RUN = int(env("MAX_RENDER_PER_RUN", "1") or "1")
MAX_IG_PER_RUN = int(env("MAX_IG_PER_RUN", "1") or "1")
MAX_TG_VIP_PER_RUN = int(env("MAX_TG_VIP_PER_RUN", "1") or "1")
MAX_TG_FREE_PER_RUN = int(env("MAX_TG_FREE_PER_RUN", "1") or "1")


# ============================================================
# STATUSES
# ============================================================

STATUS_NEW = "NEW"
STATUS_READY_TO_POST = "READY_TO_POST"
STATUS_READY_TO_PUBLISH = "READY_TO_PUBLISH"
STATUS_POSTED_INSTAGRAM = "POSTED_INSTAGRAM"
STATUS_POSTED_TELEGRAM_VIP = "POSTED_TELEGRAM_VIP"
STATUS_POSTED_ALL = "POSTED_ALL"

STATUS_RENDER_AGAIN = "RENDER_AGAIN"
STATUS_ERROR_SOFT = "ERROR_SOFT"


# ============================================================
# LOGGING
# ============================================================

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)

def die(msg: str, code: int = 1) -> None:
    log(msg)
    sys.exit(code)

def utc_now() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def safe_strip(v: Any) -> str:
    if v is None:
        return ""
    s = str(v)
    s = s.replace("\u00A0", " ").replace("\u200B", "").replace("\uFEFF", "")
    return s.strip()

def norm_status(v: Any) -> str:
    return safe_strip(v).upper()


# ============================================================
# SHEETS HELPERS
# ============================================================

def header_map(headers: List[str]) -> Dict[str, int]:
    return {safe_strip(h): i + 1 for i, h in enumerate(headers) if safe_strip(h)}

def col_to_a1(n: int) -> str:
    out = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        out = chr(65 + r) + out
    return out

def a1(row: int, col: int) -> str:
    return f"{col_to_a1(col)}{row}"

def pad_row(row: List[str], n: int) -> List[str]:
    return (row + [""] * n)[:n]

def record_from_row(headers: List[str], row: List[str]) -> Dict[str, str]:
    return {headers[i]: row[i] for i in range(len(headers))}

def pick(rec: Dict[str, Any], *keys: str) -> str:
    for k in keys:
        v = rec.get(k)
        if v is not None and safe_strip(v) != "":
            return safe_strip(v)
    return ""

def batch_update_row(ws: gspread.Worksheet, row_num: int, hmap: Dict[str, int], updates: Dict[str, Any]) -> None:
    data = []
    for k, v in updates.items():
        if k in hmap:
            data.append({"range": a1(row_num, hmap[k]), "values": [[v]]})
    if data:
        ws.batch_update(data)

def get_sheet() -> gspread.Spreadsheet:
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID (or SHEET_ID).")
    try:
        sa = json.loads(GCP_SA_JSON)
    except Exception:
        raise RuntimeError("GCP_SA_JSON must be valid JSON (single line).")
    creds = Credentials.from_service_account_info(sa, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open_by_key(SPREADSHEET_ID)

def get_raw_ws(sh: gspread.Spreadsheet) -> gspread.Worksheet:
    return sh.worksheet(RAW_DEALS_TAB)


# ============================================================
# FORMATTING (CITY, DATES, GBP)
# ============================================================

IATA_CITY_FALLBACK = {
    "LHR": "London",
    "LGW": "London",
    "STN": "London",
    "LTN": "London",
    "LCY": "London",
    "MAN": "Manchester",
    "BHX": "Birmingham",
    "BRS": "Bristol",
    "EDI": "Edinburgh",
    "GLA": "Glasgow",
    "NCL": "Newcastle",
    "LPL": "Liverpool",
    "EMA": "East Midlands",
    "KEF": "Reykjavik",
    "BCN": "Barcelona",
    "AMS": "Amsterdam",
    "DUB": "Dublin",
    "CDG": "Paris",
    "ORY": "Paris",
    "FCO": "Rome",
    "MAD": "Madrid",
}

def iso_date(d: dt.date) -> str:
    return d.isoformat()

def to_date(s: str) -> Optional[dt.date]:
    t = safe_strip(s)
    if not t:
        return None
    try:
        return dt.date.fromisoformat(t[:10])
    except Exception:
        return None

def yymmdd(s: str) -> str:
    d = to_date(s)
    if not d:
        return ""
    return d.strftime("%y%m%d")

def gbp(price: Any) -> str:
    s = safe_strip(price)
    if not s:
        return ""
    try:
        t = s.replace("£", "").replace(",", "").strip()
        v = float(t)
        if abs(v - round(v)) < 1e-9:
            return f"£{int(round(v))}"
        return f"£{v:.2f}"
    except Exception:
        return s if s.startswith("£") else f"£{s}"

def round_up_price_str(price_gbp: str) -> str:
    s = safe_strip(price_gbp).replace("£", "")
    try:
        v = float(s)
        rounded = int(v) if abs(v - int(v)) < 1e-9 else int(v) + 1
        return f"£{rounded}"
    except Exception:
        return gbp(price_gbp)


# ============================================================
# CONFIG ROUTES
# ============================================================

def load_routes_from_config(sh: gspread.Spreadsheet) -> List[Tuple[str, str]]:
    """
    CONFIG columns expected (case-insensitive):
    enabled, origin_iata, destination_iata
    """
    try:
        ws = sh.worksheet(CONFIG_TAB)
    except Exception:
        return []

    values = ws.get_all_values()
    if len(values) < 2:
        return []

    headers = [safe_strip(h).lower() for h in values[0]]
    idx = {h: i for i, h in enumerate(headers)}

    def cell(row: List[str], key: str, default: str = "") -> str:
        if key not in idx:
            return default
        i = idx[key]
        return safe_strip(row[i]) if i < len(row) else default

    routes: List[Tuple[str, str]] = []
    for r in values[1:]:
        enabled = cell(r, "enabled", "TRUE").upper()
        if enabled in {"FALSE", "0", "NO", "N"}:
            continue
        o = cell(r, "origin_iata", "").upper()
        d = cell(r, "destination_iata", "").upper()
        if o and d:
            routes.append((o, d))

    return routes


# ============================================================
# DUFFEL (v2)
# ============================================================

def duffel_offer_request(origin: str, dest: str, out_iso: str, ret_iso: str) -> Dict[str, Any]:
    url = "https://api.duffel.com/air/offer_requests"
    headers = {
        "Authorization": f"Bearer {DUFFEL_API_KEY}",
        "Duffel-Version": "v2",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {
        "data": {
            "slices": [
                {"origin": origin, "destination": dest, "departure_date": out_iso},
                {"origin": dest, "destination": origin, "departure_date": ret_iso},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": "economy",
        }
    }
    r = requests.post(url, headers=headers, json=payload, timeout=60)
    if r.status_code >= 300:
        raise RuntimeError(f"Duffel error {r.status_code}: {r.text[:600]}")
    return r.json()

def duffel_get_offers(offer_request_id: str, limit: int = 30) -> List[Dict[str, Any]]:
    url = "https://api.duffel.com/air/offers"
    headers = {
        "Authorization": f"Bearer {DUFFEL_API_KEY}",
        "Duffel-Version": "v2",
        "Accept": "application/json",
    }
    params = {"offer_request_id": offer_request_id, "limit": str(limit)}
    r = requests.get(url, headers=headers, params=params, timeout=60)
    if r.status_code >= 300:
        raise RuntimeError(f"Duffel offers error {r.status_code}: {r.text[:600]}")
    j = r.json()
    return j.get("data") or []

def offer_price_gbp(offer: Dict[str, Any]) -> Optional[float]:
    total = offer.get("total_amount")
    currency = offer.get("total_currency")
    if not total or currency != "GBP":
        return None
    try:
        return float(str(total))
    except Exception:
        return None

def offer_city_names(offer: Dict[str, Any], origin_iata: str, dest_iata: str) -> Tuple[str, str]:
    origin_city = ""
    dest_city = ""
    try:
        slices = offer.get("slices") or []
        if slices:
            seg0 = slices[0].get("segments") or []
            if seg0:
                o0 = seg0[0].get("origin") or {}
                d0 = seg0[-1].get("destination") or {}
                origin_city = safe_strip(o0.get("city_name") or o0.get("city") or o0.get("name"))
                dest_city = safe_strip(d0.get("city_name") or d0.get("city") or d0.get("name"))
    except Exception:
        pass

    if not origin_city:
        origin_city = IATA_CITY_FALLBACK.get(origin_iata, origin_iata)
    if not dest_city:
        dest_city = IATA_CITY_FALLBACK.get(dest_iata, dest_iata)

    return origin_city, dest_city

def stage_feed(ws: gspread.Worksheet, headers: List[str]) -> int:
    if not DUFFEL_API_KEY:
        log("Duffel: DISABLED (no DUFFEL_API_KEY)")
        return 0

    hmap = header_map(headers)
    required = [
        "deal_id",
        "origin_iata",
        "destination_iata",
        "origin_city",
        "destination_city",
        "destination_country",
        "outbound_date",
        "return_date",
        "trip_length_days",
        "price_gbp",
        "currency",
        "status",
        "created_timestamp",
    ]
    missing = [c for c in required if c not in hmap]
    if missing:
        raise RuntimeError("RAW_DEALS missing required feed columns: " + ", ".join(missing))

    sh = get_sheet()
    routes = load_routes_from_config(sh)

    # legacy fallback
    if not routes:
        o = env("ORIGIN_IATA", "").upper()
        d = env("DEST_IATA", "").upper()
        if o and d:
            routes = [(o, d)]

    if not routes:
        log("Duffel: No routes found (CONFIG empty and ORIGIN_IATA/DEST_IATA not set).")
        return 0

    routes = routes[: max(1, DUFFEL_ROUTES_PER_RUN)]
    inserted = 0

    today = dt.date.today()
    out_date = today + dt.timedelta(days=DAYS_AHEAD)
    ret_date = out_date + dt.timedelta(days=TRIP_LENGTH_DAYS)
    out_iso = iso_date(out_date)
    ret_iso = iso_date(ret_date)

    for (origin, dest) in routes:
        if inserted >= DUFFEL_MAX_INSERTS:
            break

        log(f"Duffel search: {origin}->{dest} {out_iso}->{ret_iso}")

        data = duffel_offer_request(origin, dest, out_iso, ret_iso)
        offer_request_id = (data.get("data") or {}).get("id")
        if not offer_request_id:
            raise RuntimeError("Duffel returned no offer_request id.")

        offers = duffel_get_offers(offer_request_id, limit=30)
        gbp_offers: List[Tuple[float, Dict[str, Any]]] = []
        for off in offers:
            p = offer_price_gbp(off)
            if p is not None:
                gbp_offers.append((p, off))
        gbp_offers.sort(key=lambda x: x[0])

        if not gbp_offers:
            log("Duffel: offers returned but none in GBP (skipping route).")
            continue

        for p, off in gbp_offers[: max(1, DUFFEL_MAX_INSERTS - inserted)]:
            origin_city, dest_city = offer_city_names(off, origin, dest)

            deal_id = f"{origin}{dest}-{out_date.strftime('%y%m%d')}-{uuid.uuid4().hex[:8]}"

            updates = {
                "deal_id": deal_id,
                "origin_iata": origin,
                "destination_iata": dest,
                "origin_city": origin_city,
                "destination_city": dest_city,
                "destination_country": pick({}, "destination_country"),  # optional, filled later
                "outbound_date": out_iso,
                "return_date": ret_iso,
                "trip_length_days": str(TRIP_LENGTH_DAYS),
                "price_gbp": f"{p:.2f}",
                "currency": "GBP",
                "status": STATUS_NEW,
                "created_timestamp": utc_now(),
            }

            row = [""] * len(headers)
            for k, v in updates.items():
                row[hmap[k] - 1] = str(v)
            ws.append_row(row, value_input_option="RAW")

            inserted += 1
            log(f"✅ Inserted NEW: {origin_city}->{dest_city} £{p:.2f}")
            if inserted >= DUFFEL_MAX_INSERTS:
                break

    return inserted


# ============================================================
# AI SCORING + CAPTION (HUMAN UK ENGLISH)
# ============================================================

def build_instagram_caption(rec: Dict[str, str]) -> str:
    """
    V4 Instagram caption style:
    - Short, conversational, British English
    - 2–4 sentences
    - No emojis
    - Mention VIP early access + CTA
    - Graphic contains details, so caption stays light :contentReference[oaicite:2]{index=2}
    """
    origin = pick(rec, "origin_city", "origin_iata")
    dest = pick(rec, "destination_city", "destination_iata")
    price = round_up_price_str(pick(rec, "price_gbp"))

    # Keep it natural, not salesy, not corporate.
    return (
        f"{origin} to {dest} at {price}. "
        f"Our VIP members saw this yesterday. "
        f"Want early access? Use the link in bio to join."
    )

def build_why_good(rec: Dict[str, str]) -> str:
    """
    V4 'why it's good' should be specific and factual.
    Keep it simple + safe: price-context based.
    """
    try:
        p = float(safe_strip(pick(rec, "price_gbp")).replace("£", ""))
    except Exception:
        p = 9999.0

    dest = pick(rec, "destination_city", "destination_iata")
    # A few generic-but-not-empty heuristics (no hype, no emojis).
    if p <= 80:
        return f"Weekend break to {dest} for less than a train ticket across the country"
    if p <= 110:
        return f"Return flights to {dest} for under £110 are uncommon outside off-peak weeks"
    if p <= 150:
        return f"This undercuts the usual fares to {dest} by a decent margin for this time of year"
    return f"Not the cheapest you'll ever see, but solid value if your dates line up"

def stage_score(ws: gspread.Worksheet, headers: List[str], max_rows: int) -> int:
    values = ws.get_all_values()
    if len(values) < 2:
        return 0

    headers = [safe_strip(h) for h in values[0]]
    hmap = header_map(headers)
    rows = values[1:]

    required = ["status", "ai_score", "ai_verdict", "ai_caption", "why_its_good", "scored_timestamp"]
    missing = [c for c in required if c not in hmap]
    if missing:
        raise RuntimeError("RAW_DEALS missing scoring columns: " + ", ".join(missing))

    done = 0
    for row_i, row in enumerate(rows, start=2):
        if done >= max_rows:
            break

        padded = pad_row(row, len(headers))
        status = norm_status(padded[hmap["status"] - 1])
        if status != STATUS_NEW:
            continue

        rec = record_from_row(headers, padded)

        # Deterministic, robust score (keeps system running even if OpenAI changes)
        try:
            price = float(safe_strip(pick(rec, "price_gbp")).replace("£", ""))
        except Exception:
            price = 9999.0
        score = max(1, min(100, int(round(100 - (price / 10.0)))))

        verdict = "GOOD" if score >= 75 else ("AVERAGE" if score >= 55 else "POOR")
        caption = build_instagram_caption(rec)
        why = build_why_good(rec)

        batch_update_row(
            ws,
            row_i,
            hmap,
            {
                "ai_score": str(score),
                "ai_verdict": verdict,
                "ai_caption": caption,
                "why_its_good": why,
                "scored_timestamp": utc_now(),
                "status": STATUS_READY_TO_POST,
            },
        )
        log(f"✅ Scored row {row_i}: {verdict} -> {STATUS_READY_TO_POST}")
        done += 1

    return done


# ============================================================
# RENDER (PYTHONANYWHERE)
# ============================================================

def build_render_payload(rec: Dict[str, str]) -> Dict[str, Any]:
    """
    Payload matches V3/V4 render service expectation (city names + ISO dates),
    and includes extra YYMMDD fields as a safe fallback.
    """
    origin_iata = pick(rec, "origin_iata")
    dest_iata = pick(rec, "destination_iata")

    origin_city = pick(rec, "origin_city") or IATA_CITY_FALLBACK.get(origin_iata, origin_iata)
    dest_city = pick(rec, "destination_city") or IATA_CITY_FALLBACK.get(dest_iata, dest_iata)

    out_iso = pick(rec, "outbound_date")
    ret_iso = pick(rec, "return_date")

    return {
        "deal_id": pick(rec, "deal_id"),
        "origin_city": origin_city,
        "destination_city": dest_city,
        "destination_country": pick(rec, "destination_country"),
        "price_gbp": safe_strip(pick(rec, "price_gbp")),
        "outbound_date": out_iso,             # YYYY-MM-DD
        "return_date": ret_iso,               # YYYY-MM-DD
        "outbound_yymmdd": yymmdd(out_iso),   # 260122 etc (optional)
        "return_yymmdd": yymmdd(ret_iso),
        "trip_length_days": pick(rec, "trip_length_days"),
        "stops": pick(rec, "stops"),
        "airline": pick(rec, "airline"),
        "ai_score": pick(rec, "ai_score"),
        "ai_verdict": pick(rec, "ai_verdict"),
        "ai_caption": pick(rec, "ai_caption"),
        "why_its_good": pick(rec, "why_its_good"),
    }

def call_render(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not RENDER_URL:
        raise RuntimeError("Missing RENDER_URL")
    r = requests.post(RENDER_URL, json=payload, timeout=RENDER_TIMEOUT)
    if r.status_code >= 300:
        raise RuntimeError(f"Render error {r.status_code}: {r.text[:600]}")
    try:
        return r.json()
    except Exception:
        raise RuntimeError("Render response was not JSON. First 300 chars: " + (r.text or "")[:300])

def stage_render(ws: gspread.Worksheet, headers: List[str], max_rows: int) -> int:
    values = ws.get_all_values()
    if len(values) < 2:
        return 0

    headers = [safe_strip(h) for h in values[0]]
    hmap = header_map(headers)
    rows = values[1:]

    required = [
        "status",
        "graphic_url",
        "rendered_timestamp",
        "render_error",
        "render_http_status",
        "render_response_snippet",
    ]
    missing = [c for c in required if c not in hmap]
    if missing:
        raise RuntimeError("RAW_DEALS missing render columns: " + ", ".join(missing))

    done = 0
    for row_i, row in enumerate(rows, start=2):
        if done >= max_rows:
            break

        padded = pad_row(row, len(headers))
        status = norm_status(padded[hmap["status"] - 1])
        if status != STATUS_READY_TO_POST:
            continue

        rec = record_from_row(headers, padded)
        payload = build_render_payload(rec)

        try:
            result = call_render(payload)
            graphic_url = safe_strip(result.get("graphic_url") or result.get("image_url") or "")
            if not graphic_url:
                raise RuntimeError("Render returned no graphic_url/image_url.")

            batch_update_row(
                ws,
                row_i,
                hmap,
                {
                    "graphic_url": graphic_url,
                    "rendered_timestamp": utc_now(),
                    "render_error": "",
                    "render_http_status": "200",
                    "render_response_snippet": "",
                    "status": STATUS_READY_TO_PUBLISH,
                },
            )
            log(f"✅ Render OK row {row_i}: -> {STATUS_READY_TO_PUBLISH}")
            done += 1

        except Exception as e:
            msg = safe_strip(e)
            batch_update_row(
                ws,
                row_i,
                hmap,
                {
                    "rendered_timestamp": utc_now(),
                    "render_error": f"ERROR: {msg[:240]}",
                    "render_http_status": "",
                    "render_response_snippet": msg[:240],
                    "status": STATUS_RENDER_AGAIN,
                },
            )
            log(f"❌ Render FAILED row {row_i}: {msg[:160]}")
            done += 1

    return done


# ============================================================
# INSTAGRAM (GRAPH API) — MARKETING, RUNS EVERY WORKFLOW RUN
# ============================================================

def ig_enabled() -> bool:
    return bool(IG_ACCESS_TOKEN and IG_USER_ID)

def ig_create_media(image_url: str, caption: str) -> str:
    url = f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media"
    payload = {"image_url": image_url, "caption": caption, "access_token": IG_ACCESS_TOKEN}
    r = requests.post(url, data=payload, timeout=60)
    if r.status_code >= 300:
        raise RuntimeError(f"IG create error {r.status_code}: {r.text[:600]}")
    j = r.json()
    cid = j.get("id")
    if not cid:
        raise RuntimeError("IG create returned no id.")
    return str(cid)

def ig_publish_media(creation_id: str) -> str:
    url = f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media_publish"
    payload = {"creation_id": creation_id, "access_token": IG_ACCESS_TOKEN}
    r = requests.post(url, data=payload, timeout=60)
    if r.status_code >= 300:
        raise RuntimeError(f"IG publish error {r.status_code}: {r.text[:600]}")
    j = r.json()
    mid = j.get("id")
    if not mid:
        raise RuntimeError("IG publish returned no id.")
    return str(mid)

def stage_instagram(ws: gspread.Worksheet, headers: List[str], max_rows: int) -> int:
    if not ig_enabled():
        log("Instagram: DISABLED (missing IG_ACCESS_TOKEN / IG_USER_ID)")
        return 0

    values = ws.get_all_values()
    if len(values) < 2:
        return 0

    headers = [safe_strip(h) for h in values[0]]
    hmap = header_map(headers)
    rows = values[1:]

    required = ["status", "graphic_url", "ig_media_id", "ig_posted_timestamp", "ig_error"]
    missing = [c for c in required if c not in hmap]
    if missing:
        raise RuntimeError("RAW_DEALS missing IG columns: " + ", ".join(missing))

    done = 0
    for row_i, row in enumerate(rows, start=2):
        if done >= max_rows:
            break

        padded = pad_row(row, len(headers))
        status = norm_status(padded[hmap["status"] - 1])
        if status != STATUS_READY_TO_PUBLISH:
            continue

        rec = record_from_row(headers, padded)
        image_url = pick(rec, "graphic_url")
        if not image_url:
            batch_update_row(
                ws,
                row_i,
                hmap,
                {
                    "ig_posted_timestamp": utc_now(),
                    "ig_media_id": "",
                    "ig_error": "Missing graphic_url",
                    "status": STATUS_ERROR_SOFT,
                },
            )
            log(f"❌ IG skipped row {row_i}: missing graphic_url")
            done += 1
            continue

        caption = pick(rec, "ai_caption") or build_instagram_caption(rec)

        try:
            creation_id = ig_create_media(image_url, caption)
            media_id = ig_publish_media(creation_id)

            batch_update_row(
                ws,
                row_i,
                hmap,
                {
                    "ig_posted_timestamp": utc_now(),
                    "ig_media_id": media_id,
                    "ig_error": "",
                    "status": STATUS_POSTED_INSTAGRAM,
                },
            )
            log(f"✅ IG posted row {row_i}: -> {STATUS_POSTED_INSTAGRAM}")
            done += 1

        except Exception as e:
            msg = safe_strip(e)
            batch_update_row(
                ws,
                row_i,
                hmap,
                {
                    "ig_posted_timestamp": utc_now(),
                    "ig_media_id": "",
                    "ig_error": msg[:300],
                    "status": STATUS_ERROR_SOFT,
                },
            )
            log(f"❌ IG FAILED row {row_i}: {msg[:180]}")
            done += 1

    return done


# ============================================================
# TELEGRAM (VIP FIRST, FREE AFTER DELAY)
# ============================================================

def tg_send(bot_token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": False, "parse_mode": "HTML"}
    r = requests.post(url, json=payload, timeout=45)
    if r.status_code >= 300:
        raise RuntimeError(f"Telegram error {r.status_code}: {r.text[:600]}")

def hours_since(iso_ts: str) -> float:
    s = safe_strip(iso_ts)
    if not s:
        return 999999.0
    try:
        t = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        now = dt.datetime.now(dt.timezone.utc)
        return (now - t.astimezone(dt.timezone.utc)).total_seconds() / 3600.0
    except Exception:
        return 999999.0

def format_telegram_vip(rec: Dict[str, str]) -> str:
    price = gbp(pick(rec, "price_gbp"))
    country = pick(rec, "destination_country") or pick(rec, "destination_city", "destination_iata")
    to_city = pick(rec, "destination_city", "destination_iata").upper()
    from_city = pick(rec, "origin_city", "origin_iata")
    out_iso = pick(rec, "outbound_date")
    ret_iso = pick(rec, "return_date")
    why = pick(rec, "why_its_good") or build_why_good(rec)

    return (
        f"{price} to {country}\n\n"
        f"<b>TO:</b> {to_city}\n"
        f"<b>FROM:</b> {from_city}\n\n"
        f"<b>OUT:</b>  {out_iso}\n"
        f"<b>BACK:</b> {ret_iso}\n\n"
        f"<b>Heads up:</b>\n"
        f"• {why}\n"
        f"• Availability is running low\n\n"
        f"BOOK NOW"
    )

def format_telegram_free(rec: Dict[str, str]) -> str:
    price = gbp(pick(rec, "price_gbp"))
    country = pick(rec, "destination_country") or pick(rec, "destination_city", "destination_iata")
    to_city = pick(rec, "destination_city", "destination_iata").upper()
    from_city = pick(rec, "origin_city", "origin_iata")
    out_iso = pick(rec, "outbound_date")
    ret_iso = pick(rec, "return_date")

    monthly = STRIPE_LINK
    yearly = env("STRIPE_LINK_YEARLY", "")

    upsell_links = ""
    if monthly and yearly:
        upsell_links = f'<a href="{monthly}">Upgrade Monthly</a> | <a href="{yearly}">Upgrade Yearly</a>'
    elif monthly:
        upsell_links = f'<a href="{monthly}">Upgrade</a>'

    return (
        f"{price} to {country}\n\n"
        f"<b>TO:</b> {to_city}\n"
        f"<b>FROM:</b> {from_city}\n\n"
        f"<b>OUT:</b>  {out_iso}\n"
        f"<b>BACK:</b> {ret_iso}\n\n"
        f"<b>Heads up:</b>\n"
        f"• VIP members saw this 24 hours ago\n"
        f"• Availability is running low\n"
        f"• Best deals go to VIPs first\n\n"
        f"Book now\n\n"
        f"<b>Want instant access?</b>\n"
        f"Join TravelTxter Community\n\n"
        f"• Deals 24 hours early\n"
        f"• Direct booking links\n"
        f"• Exclusive mistake fares\n"
        f"• Cancel anytime\n\n"
        f"{upsell_links}".strip()
    )

def stage_tg_vip(ws: gspread.Worksheet, headers: List[str], max_rows: int) -> int:
    if not (TELEGRAM_BOT_TOKEN_VIP and TELEGRAM_CHANNEL_VIP):
        log("Telegram VIP: DISABLED")
        return 0

    values = ws.get_all_values()
    if len(values) < 2:
        return 0

    headers = [safe_strip(h) for h in values[0]]
    hmap = header_map(headers)
    rows = values[1:]

    required = ["status", "tg_vip_timestamp", "tg_vip_error"]
    missing = [c for c in required if c not in hmap]
    if missing:
        raise RuntimeError("RAW_DEALS missing TG VIP columns: " + ", ".join(missing))

    done = 0
    for row_i, row in enumerate(rows, start=2):
        if done >= max_rows:
            break

        padded = pad_row(row, len(headers))
        status = norm_status(padded[hmap["status"] - 1])
        if status != STATUS_POSTED_INSTAGRAM:
            continue

        rec = record_from_row(headers, padded)
        text = format_telegram_vip(rec)

        try:
            tg_send(TELEGRAM_BOT_TOKEN_VIP, TELEGRAM_CHANNEL_VIP, text)
            batch_update_row(
                ws, row_i, hmap,
                {"tg_vip_timestamp": utc_now(), "tg_vip_error": "", "status": STATUS_POSTED_TELEGRAM_VIP},
            )
            log(f"✅ TG VIP posted row {row_i}: -> {STATUS_POSTED_TELEGRAM_VIP}")
            done += 1

        except Exception as e:
            msg = safe_strip(e)
            batch_update_row(ws, row_i, hmap, {"tg_vip_timestamp": utc_now(), "tg_vip_error": msg[:300]})
            log(f"❌ TG VIP FAILED row {row_i}: {msg[:180]}")
            done += 1

    return done

def stage_tg_free(ws: gspread.Worksheet, headers: List[str], max_rows: int) -> int:
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHANNEL):
        log("Telegram FREE: DISABLED")
        return 0

    values = ws.get_all_values()
    if len(values) < 2:
        return 0

    headers = [safe_strip(h) for h in values[0]]
    hmap = header_map(headers)
    rows = values[1:]

    required = ["status", "tg_free_timestamp", "tg_free_error", "tg_vip_timestamp"]
    missing = [c for c in required if c not in hmap]
    if missing:
        raise RuntimeError("RAW_DEALS missing TG FREE columns: " + ", ".join(missing))

    done = 0
    for row_i, row in enumerate(rows, start=2):
        if done >= max_rows:
            break

        padded = pad_row(row, len(headers))
        status = norm_status(padded[hmap["status"] - 1])
        if status != STATUS_POSTED_TELEGRAM_VIP:
            continue

        rec = record_from_row(headers, padded)
        vip_ts = pick(rec, "tg_vip_timestamp")

        if VIP_DELAY_HOURS > 0 and hours_since(vip_ts) < VIP_DELAY_HOURS:
            continue

        text = format_telegram_free(rec)

        try:
            tg_send(TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL, text)
            batch_update_row(
                ws, row_i, hmap,
                {"tg_free_timestamp": utc_now(), "tg_free_error": "", "status": STATUS_POSTED_ALL},
            )
            log(f"✅ TG FREE posted row {row_i}: -> {STATUS_POSTED_ALL}")
            done += 1

        except Exception as e:
            msg = safe_strip(e)
            batch_update_row(ws, row_i, hmap, {"tg_free_timestamp": utc_now(), "tg_free_error": msg[:300]})
            log(f"❌ TG FREE FAILED row {row_i}: {msg[:180]}")
            done += 1

    return done


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    try:
        sh = get_sheet()
        ws = get_raw_ws(sh)
    except Exception as e:
        die(f"ERROR: Sheets auth/open failed: {safe_strip(e)}")

    values = ws.get_all_values()
    if not values:
        die("ERROR: RAW_DEALS is empty (no headers).")

    headers = [safe_strip(h) for h in values[0]]

    log("============================================================")
    log("🚀 TRAVELTXTER V4.2 UNIFIED PIPELINE")
    log("============================================================")
    log(f"Sheet: {SPREADSHEET_ID}")
    log(f"Tab: {RAW_DEALS_TAB}")
    log(f"Duffel: {'ENABLED' if DUFFEL_API_KEY else 'DISABLED'} | max_inserts={DUFFEL_MAX_INSERTS}")
    log(f"Render: {'ENABLED' if RENDER_URL else 'DISABLED'}")
    log(f"Instagram: {'ENABLED' if ig_enabled() else 'DISABLED'}")
    log(f"RUN_SLOT: {RUN_SLOT} | VIP_DELAY_HOURS={VIP_DELAY_HOURS}")
    log("============================================================")

    inserted = 0
    scored = 0
    rendered = 0
    ig_posted = 0
    vip_posted = 0
    free_posted = 0

    # 1) FEED
    try:
        inserted = stage_feed(ws, headers)
    except Exception as e:
        log(f"❌ FEED error: {safe_strip(e)[:240]}")

    # Refresh headers after potential changes
    values = ws.get_all_values()
    if values:
        headers = [safe_strip(h) for h in values[0]]

    # 2) SCORE
    try:
        scored = stage_score(ws, headers, MAX_SCORE_PER_RUN)
    except Exception as e:
        log(f"❌ SCORE error: {safe_strip(e)[:240]}")

    # 3) RENDER
    try:
        rendered = stage_render(ws, headers, MAX_RENDER_PER_RUN)
    except Exception as e:
        log(f"❌ RENDER error: {safe_strip(e)[:240]}")

    # 4) INSTAGRAM (marketing: runs every run if READY_TO_PUBLISH exists)
    try:
        ig_posted = stage_instagram(ws, headers, MAX_IG_PER_RUN)
    except Exception as e:
        log(f"❌ IG error: {safe_strip(e)[:240]}")

    # 5) TG VIP (AM only by policy)
    if RUN_SLOT == "AM":
        try:
            vip_posted = stage_tg_vip(ws, headers, MAX_TG_VIP_PER_RUN)
        except Exception as e:
            log(f"❌ TG VIP error: {safe_strip(e)[:240]}")

    # 6) TG FREE (PM only by policy)
    if RUN_SLOT == "PM":
        try:
            free_posted = stage_tg_free(ws, headers, MAX_TG_FREE_PER_RUN)
        except Exception as e:
            log(f"❌ TG FREE error: {safe_strip(e)[:240]}")

    log("------------------------------------------------------------")
    log(f"Done. inserted={inserted} scored={scored} rendered={rendered} ig_posted={ig_posted} vip_posted={vip_posted} free_posted={free_posted}")
    log("------------------------------------------------------------")


if __name__ == "__main__":
    main()
