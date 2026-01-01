#!/usr/bin/env python3
"""
TravelTxter V4.2 — Unified Pipeline Worker (FIXED)

Stages (state machine):
1) FEED (scheduled): inserts NEW rows from Duffel (v2) into RAW_DEALS
2) AI SCORE:        NEW -> READY_TO_POST
3) RENDER:          READY_TO_POST -> READY_TO_PUBLISH (PythonAnywhere RENDER_URL)
4) INSTAGRAM:       READY_TO_PUBLISH -> POSTED_INSTAGRAM (Graph API)
5) TELEGRAM VIP:    POSTED_INSTAGRAM -> POSTED_TELEGRAM_VIP (VIP first)
6) TELEGRAM FREE:   POSTED_TELEGRAM_VIP -> POSTED_ALL (after VIP_DELAY_HOURS)

Key fixes:
- Duffel-Version header updated to v2 (v1/beta now rejected)
- Render payload uses CITY names (not IATA), YYMMDD dates, GBP price
- Instagram runs (marketing) and posts 2x/day (one per run)
- Supports both old/new env var names (TELEGRAM_* vs TELEGRAM_*_CHANNEL etc.)
"""

import os
import sys
import json
import time
import uuid
import datetime as dt
from typing import Dict, Any, List, Tuple, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials

# ============================================================
# ENV (with backward-compatible fallbacks)
# ============================================================

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

GCP_SA_JSON = (os.getenv("GCP_SA_JSON", "") or "").strip()

SPREADSHEET_ID = (os.getenv("SPREADSHEET_ID", "") or os.getenv("SHEET_ID", "") or "").strip()
RAW_DEALS_TAB = (os.getenv("RAW_DEALS_TAB", "") or os.getenv("WORKSHEET_NAME", "") or "RAW_DEALS").strip()
CONFIG_TAB = (os.getenv("CONFIG_TAB", "") or "CONFIG").strip()

DUFFEL_API_KEY = (os.getenv("DUFFEL_API_KEY", "") or "").strip()
DUFFEL_MAX_INSERTS = int(os.getenv("DUFFEL_MAX_INSERTS", "3") or "3")
DUFFEL_ROUTES_PER_RUN = int(os.getenv("DUFFEL_ROUTES_PER_RUN", "1") or "1")
DAYS_AHEAD = int(os.getenv("DAYS_AHEAD", "60") or "60")
TRIP_LENGTH_DAYS = int(os.getenv("TRIP_LENGTH_DAYS", "5") or "5")

OPENAI_API_KEY = (os.getenv("OPENAI_API_KEY", "") or "").strip()
OPENAI_MODEL = (os.getenv("OPENAI_MODEL", "") or "gpt-4o-mini").strip()

RENDER_URL = (os.getenv("RENDER_URL", "") or "").strip()
RENDER_TIMEOUT = int(os.getenv("RENDER_TIMEOUT", "45") or "45")

IG_ACCESS_TOKEN = (os.getenv("IG_ACCESS_TOKEN", "") or "").strip()
IG_USER_ID = (os.getenv("IG_USER_ID", "") or "").strip()

# Telegram: support both naming styles
TELEGRAM_BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN", "") or "").strip()
TELEGRAM_CHANNEL = (os.getenv("TELEGRAM_CHANNEL", "") or os.getenv("TELEGRAM_FREE_CHANNEL", "") or "").strip()

TELEGRAM_BOT_TOKEN_VIP = (os.getenv("TELEGRAM_BOT_TOKEN_VIP", "") or "").strip()
TELEGRAM_CHANNEL_VIP = (os.getenv("TELEGRAM_CHANNEL_VIP", "") or os.getenv("TELEGRAM_VIP_CHANNEL", "") or "").strip()

STRIPE_LINK = (os.getenv("STRIPE_LINK", "") or "").strip()

RUN_SLOT = (os.getenv("RUN_SLOT", "") or "AM").strip().upper()  # AM/PM used only for messaging flavour
VIP_DELAY_HOURS = int(os.getenv("VIP_DELAY_HOURS", "24") or "24")

MAX_SCORE_PER_RUN = int(os.getenv("MAX_SCORE_PER_RUN", "1") or "1")
MAX_RENDER_PER_RUN = int(os.getenv("MAX_RENDER_PER_RUN", "1") or "1")
MAX_IG_PER_RUN = int(os.getenv("MAX_IG_PER_RUN", "1") or "1")
MAX_TG_VIP_PER_RUN = int(os.getenv("MAX_TG_VIP_PER_RUN", "1") or "1")
MAX_TG_FREE_PER_RUN = int(os.getenv("MAX_TG_FREE_PER_RUN", "1") or "1")

# ============================================================
# STATUSES
# ============================================================

STATUS_NEW = "NEW"
STATUS_READY_TO_POST = "READY_TO_POST"
STATUS_READY_TO_PUBLISH = "READY_TO_PUBLISH"
STATUS_POSTED_INSTAGRAM = "POSTED_INSTAGRAM"
STATUS_POSTED_TELEGRAM_VIP = "POSTED_TELEGRAM_VIP"
STATUS_POSTED_ALL = "POSTED_ALL"

STATUS_ERROR_HARD = "ERROR_HARD"
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

def utc_now_iso() -> str:
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

def get_ws() -> gspread.Worksheet:
    if not GCP_SA_JSON:
        die("ERROR: Missing GCP_SA_JSON")
    if not SPREADSHEET_ID:
        die("ERROR: Missing SPREADSHEET_ID (or SHEET_ID)")
    try:
        sa = json.loads(GCP_SA_JSON)
    except Exception:
        die("ERROR: GCP_SA_JSON must be valid JSON (single line).")

    creds = Credentials.from_service_account_info(sa, scopes=SCOPES)
    client = gspread.authorize(creds)
    sh = client.open_by_key(SPREADSHEET_ID)
    return sh.worksheet(RAW_DEALS_TAB)

def get_parent_sheet() -> gspread.Spreadsheet:
    if not GCP_SA_JSON:
        die("ERROR: Missing GCP_SA_JSON")
    if not SPREADSHEET_ID:
        die("ERROR: Missing SPREADSHEET_ID (or SHEET_ID)")
    sa = json.loads(GCP_SA_JSON)
    creds = Credentials.from_service_account_info(sa, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open_by_key(SPREADSHEET_ID)

# ============================================================
# FORMATTING (CITY / DATES / GBP)
# ============================================================

# Very small fallback mapping (keeps output human when API doesn't provide city_name)
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
    "KEF": "Reykjavík",
    "BCN": "Barcelona",
    "AMS": "Amsterdam",
    "DUB": "Dublin",
    "CDG": "Paris",
    "ORY": "Paris",
    "FCO": "Rome",
    "MAD": "Madrid",
}

def yymmdd(date_str: str) -> str:
    s = safe_strip(date_str)
    if not s:
        return ""
    # Accept YYYY-MM-DD or ISO datetime; output YYMMDD
    try:
        d = dt.date.fromisoformat(s[:10])
        return d.strftime("%y%m%d")
    except Exception:
        return s  # if unknown format, return as-is

def gbp(price: Any) -> str:
    s = safe_strip(price)
    if not s:
        return ""
    try:
        # allow "123.45" or "£123.45"
        t = s.replace("£", "").replace(",", "").strip()
        v = float(t)
        # show no trailing .00 when whole
        if abs(v - round(v)) < 1e-9:
            return f"£{int(round(v))}"
        return f"£{v:.2f}"
    except Exception:
        return s if s.startswith("£") else f"£{s}"

# ============================================================
# CONFIG ROUTES
# ============================================================

def load_routes_from_config(sh: gspread.Spreadsheet) -> List[Tuple[str, str]]:
    """
    CONFIG columns expected: enabled, origin_iata, destination_iata
    enabled defaults TRUE if missing.
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

    routes: List[Tuple[str, str]] = []
    for r in values[1:]:
        def cell(name: str, default: str = "") -> str:
            if name not in idx:
                return default
            i = idx[name]
            return safe_strip(r[i]) if i < len(r) else default

        enabled = cell("enabled", "TRUE").upper()
        if enabled in {"FALSE", "0", "NO", "N"}:
            continue

        o = cell("origin_iata", "").upper()
        d = cell("destination_iata", "").upper()
        if o and d:
            routes.append((o, d))

    return routes

# ============================================================
# DUFFEL (v2)
# ============================================================

def duffel_offer_request(origin: str, dest: str, out_date: str, ret_date: str) -> Dict[str, Any]:
    """
    Calls Duffel Offer Requests API.
    IMPORTANT FIX: Duffel-Version must be v2 (v1/beta rejected now).
    """
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
                {"origin": origin, "destination": dest, "departure_date": out_date},
                {"origin": dest, "destination": origin, "departure_date": ret_date},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": "economy",
        }
    }

    r = requests.post(url, headers=headers, json=payload, timeout=60)
    if r.status_code >= 300:
        raise RuntimeError(f"Duffel error {r.status_code}: {r.text[:600]}")
    return r.json()

def duffel_get_offers(offer_request_id: str, limit: int = 25) -> List[Dict[str, Any]]:
    """
    Fetch offers for an offer_request.
    """
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

def _offer_best_price_gbp(offer: Dict[str, Any]) -> Optional[float]:
    """
    Duffel returns money as strings; currency may be GBP or not.
    We'll only accept GBP offers for now to keep the pipeline simple and correct.
    """
    total = offer.get("total_amount")
    currency = offer.get("total_currency")
    if not total or currency != "GBP":
        return None
    try:
        return float(str(total))
    except Exception:
        return None

def _offer_city_names(offer: Dict[str, Any], origin_iata: str, dest_iata: str) -> Tuple[str, str]:
    """
    Try to pull city names from Duffel offer structures (varies by response),
    otherwise fallback to a small mapping.
    """
    origin_city = ""
    dest_city = ""

    # Common patterns: slices -> segments -> origin/destination objects
    try:
        slices = offer.get("slices") or []
        if slices:
            seg0 = (slices[0].get("segments") or [])
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

    # Clean: keep it title case, not shouty
    origin_city = origin_city.strip()
    dest_city = dest_city.strip()
    return origin_city, dest_city

def duffel_run_and_insert(ws: gspread.Worksheet, headers: List[str]) -> int:
    """
    Inserts up to DUFFEL_MAX_INSERTS new rows (status=NEW).
    Requires columns:
      deal_id, origin_iata, destination_iata, origin_city, destination_city,
      outbound_date, return_date, price_gbp, currency, status, created_timestamp
    """
    if not DUFFEL_API_KEY:
        log("Duffel: DISABLED (no DUFFEL_API_KEY)")
        return 0

    sh = get_parent_sheet()
    routes = load_routes_from_config(sh)
    if not routes:
        # fallback single route from env (legacy)
        o = (os.getenv("ORIGIN_IATA", "") or "").strip().upper()
        d = (os.getenv("DEST_IATA", "") or "").strip().upper()
        if o and d:
            routes = [(o, d)]

    if not routes:
        log("Duffel: No routes found (CONFIG empty and ORIGIN_IATA/DEST_IATA not set).")
        return 0

    routes = routes[: max(1, DUFFEL_ROUTES_PER_RUN)]
    log(f"Duffel: ENABLED | routes_per_run={DUFFEL_ROUTES_PER_RUN} | max_inserts={DUFFEL_MAX_INSERTS}")
    log(f"Routes selected: {routes}")

    hmap = header_map(headers)

    required = [
        "deal_id",
        "origin_iata",
        "destination_iata",
        "origin_city",
        "destination_city",
        "outbound_date",
        "return_date",
        "price_gbp",
        "currency",
        "status",
        "created_timestamp",
    ]
    missing = [c for c in required if c not in hmap]
    if missing:
        raise RuntimeError(f"RAW_DEALS missing required columns: {missing}")

    inserted = 0
    today = dt.date.today()

    for (origin, dest) in routes:
        if inserted >= DUFFEL_MAX_INSERTS:
            break

        out_date = today + dt.timedelta(days=DAYS_AHEAD)
        ret_date = out_date + dt.timedelta(days=TRIP_LENGTH_DAYS)

        out_iso = out_date.isoformat()
        ret_iso = ret_date.isoformat()

        log(f"Duffel search: {origin}->{dest} {out_iso}->{ret_iso}")

        data = duffel_offer_request(origin, dest, out_iso, ret_iso)
        offer_request_id = (data.get("data") or {}).get("id")
        if not offer_request_id:
            raise RuntimeError("Duffel returned no offer_request id")

        offers = duffel_get_offers(offer_request_id, limit=30)
        if not offers:
            log("Duffel: no offers returned.")
            continue

        # pick cheapest GBP offers only
        gbp_offers: List[Tuple[float, Dict[str, Any]]] = []
        for off in offers:
            p = _offer_best_price_gbp(off)
            if p is not None:
                gbp_offers.append((p, off))
        gbp_offers.sort(key=lambda x: x[0])

        if not gbp_offers:
            log("Duffel: offers found but none in GBP (skipping).")
            continue

        # insert up to remaining slots
        for p, off in gbp_offers[: max(1, DUFFEL_MAX_INSERTS - inserted)]:
            deal_id = f"{origin}{dest}-{out_date.strftime('%y%m%d')}-{uuid.uuid4().hex[:8]}"
            origin_city, dest_city = _offer_city_names(off, origin, dest)

            row_updates = {
                "deal_id": deal_id,
                "origin_iata": origin,
                "destination_iata": dest,
                "origin_city": origin_city,
                "destination_city": dest_city,
                "outbound_date": out_iso,
                "return_date": ret_iso,
                "price_gbp": f"{p:.2f}",
                "currency": "GBP",
                "status": STATUS_NEW,
                "created_timestamp": utc_now_iso(),
            }

            # build full row in correct order
            row = [""] * len(headers)
            for k, v in row_updates.items():
                row[hmap[k] - 1] = str(v)

            ws.append_row(row, value_input_option="RAW")
            inserted += 1
            log(f"✅ Inserted NEW: {origin_city}->{dest_city} £{p:.2f} ({origin}->{dest})")

            if inserted >= DUFFEL_MAX_INSERTS:
                break

    return inserted

# ============================================================
# AI SCORING (simple + reliable)
# ============================================================

def ai_score_deal_stub(row: Dict[str, str]) -> Dict[str, str]:
    """
    Minimal deterministic scoring to keep pipeline robust.
    You can swap in OpenAI scoring later; this is safe for production.
    """
    price_raw = pick(row, "price_gbp")
    stops_raw = pick(row, "stops")
    airline = pick(row, "airline")
    origin_city = pick(row, "origin_city", "origin_iata")
    dest_city = pick(row, "destination_city", "destination_iata")
    out_d = yymmdd(pick(row, "outbound_date"))
    ret_d = yymmdd(pick(row, "return_date"))

    try:
        price = float(price_raw.replace("£", "").strip())
    except Exception:
        price = 9999.0

    # crude: cheaper => higher score
    score = max(1, min(100, int(round(100 - (price / 10.0)))))  # £100 => 90, £300 => 70
    verdict = "GOOD" if score >= 75 else ("AVERAGE" if score >= 55 else "POOR")

    caption = (
        f"🔥 {gbp(price)} return to {dest_city}\n\n"
        f"📍 From {origin_city}\n"
        f"📅 {out_d} → {ret_d}\n\n"
        f"Quick one — prices move fast. ✈️"
    )

    return {
        "ai_score": str(score),
        "ai_verdict": verdict,
        "ai_caption": caption,
    }

def stage_ai_scoring(ws: gspread.Worksheet, headers: List[str], max_rows: int = 1) -> int:
    values = ws.get_all_values()
    if len(values) < 2:
        return 0

    headers = [safe_strip(h) for h in values[0]]
    hmap = header_map(headers)
    rows = values[1:]

    required = ["status", "ai_score", "ai_verdict", "ai_caption"]
    missing = [c for c in required if c not in hmap]
    if missing:
        raise RuntimeError(f"RAW_DEALS missing scoring columns: {missing}")

    done = 0
    for row_i, row in enumerate(rows, start=2):
        if done >= max_rows:
            break
        padded = pad_row(row, len(headers))
        status = norm_status(padded[hmap["status"] - 1])
        if status != STATUS_NEW:
            continue

        rec = record_from_row(headers, padded)
        scored = ai_score_deal_stub(rec)

        batch_update_row(
            ws,
            row_i,
            hmap,
            {
                **scored,
                "scored_timestamp": utc_now_iso(),
                "status": STATUS_READY_TO_POST,
            },
        )
        log(f"✅ AI scored row {row_i}: status={STATUS_READY_TO_POST} verdict={scored['ai_verdict']}")
        done += 1

    return done

# ============================================================
# RENDER (PythonAnywhere)
# ============================================================

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

def build_render_payload(rec: Dict[str, str]) -> Dict[str, Any]:
    """
    REQUIRED BY YOU:
    - CITY names (TO/FROM), not IATA
    - Dates in YYMMDD
    - Price in GBP
    """
    origin_city = pick(rec, "origin_city") or IATA_CITY_FALLBACK.get(pick(rec, "origin_iata"), pick(rec, "origin_iata"))
    dest_city = pick(rec, "destination_city") or IATA_CITY_FALLBACK.get(pick(rec, "destination_iata"), pick(rec, "destination_iata"))

    payload = {
        "deal_id": pick(rec, "deal_id"),
        "origin_city": origin_city,
        "destination_city": dest_city,
        "origin_iata": pick(rec, "origin_iata"),
        "destination_iata": pick(rec, "destination_iata"),
        "price_gbp": gbp(pick(rec, "price_gbp")),
        "outbound_date": yymmdd(pick(rec, "outbound_date")),
        "return_date": yymmdd(pick(rec, "return_date")),
        "ai_score": pick(rec, "ai_score"),
        "ai_verdict": pick(rec, "ai_verdict"),
        "ai_caption": pick(rec, "ai_caption"),
    }
    return payload

def stage_render(ws: gspread.Worksheet, headers: List[str], max_rows: int = 1) -> int:
    values = ws.get_all_values()
    if len(values) < 2:
        return 0

    headers = [safe_strip(h) for h in values[0]]
    hmap = header_map(headers)
    rows = values[1:]

    required = ["status", "graphic_url", "rendered_timestamp", "render_error", "render_http_status", "render_response_snippet"]
    missing = [c for c in required if c not in hmap]
    if missing:
        raise RuntimeError(f"RAW_DEALS missing render columns: {missing}")

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
                raise RuntimeError("Render returned no graphic_url/image_url")

            batch_update_row(
                ws,
                row_i,
                hmap,
                {
                    "graphic_url": graphic_url,
                    "rendered_timestamp": utc_now_iso(),
                    "render_error": "",
                    "render_http_status": "200",
                    "render_response_snippet": "",
                    "status": STATUS_READY_TO_PUBLISH,
                },
            )
            log(f"✅ Render OK row {row_i}: status={STATUS_READY_TO_PUBLISH}")
            done += 1

        except Exception as e:
            msg = safe_strip(e)
            batch_update_row(
                ws,
                row_i,
                hmap,
                {
                    "rendered_timestamp": utc_now_iso(),
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
# INSTAGRAM (Graph API)
# ============================================================

def ig_enabled() -> bool:
    return bool(IG_ACCESS_TOKEN and IG_USER_ID)

def ig_create_media(image_url: str, caption: str) -> str:
    url = f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media"
    payload = {
        "image_url": image_url,
        "caption": caption,
        "access_token": IG_ACCESS_TOKEN,
    }
    r = requests.post(url, data=payload, timeout=60)
    if r.status_code >= 300:
        raise RuntimeError(f"IG media create error {r.status_code}: {r.text[:600]}")
    j = r.json()
    creation_id = j.get("id")
    if not creation_id:
        raise RuntimeError("IG media create returned no id")
    return str(creation_id)

def ig_publish_media(creation_id: str) -> str:
    url = f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media_publish"
    payload = {"creation_id": creation_id, "access_token": IG_ACCESS_TOKEN}
    r = requests.post(url, data=payload, timeout=60)
    if r.status_code >= 300:
        raise RuntimeError(f"IG publish error {r.status_code}: {r.text[:600]}")
    j = r.json()
    media_id = j.get("id")
    if not media_id:
        raise RuntimeError("IG publish returned no id")
    return str(media_id)

def stage_instagram(ws: gspread.Worksheet, headers: List[str], max_rows: int = 1) -> int:
    if not ig_enabled():
        log("Instagram: DISABLED (missing IG_ACCESS_TOKEN/IG_USER_ID)")
        return 0

    values = ws.get_all_values()
    if len(values) < 2:
        return 0

    headers = [safe_strip(h) for h in values[0]]
    hmap = header_map(headers)
    rows = values[1:]

    required = ["status", "graphic_url", "ig_posted_timestamp", "ig_media_id", "ig_error"]
    missing = [c for c in required if c not in hmap]
    if missing:
        raise RuntimeError(f"RAW_DEALS missing Instagram columns: {missing}")

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
                ws, row_i, hmap,
                {"ig_error": "Missing graphic_url", "status": STATUS_ERROR_SOFT, "ig_posted_timestamp": utc_now_iso()}
            )
            log(f"❌ Instagram skipped row {row_i}: missing graphic_url")
            done += 1
            continue

        caption = pick(rec, "ai_caption")
        if not caption:
            # safe fallback caption (city names)
            origin_city = pick(rec, "origin_city", "origin_iata")
            dest_city = pick(rec, "destination_city", "destination_iata")
            caption = (
                f"🔥 {gbp(pick(rec,'price_gbp'))} return to {dest_city}\n\n"
                f"📍 From {origin_city}\n"
                f"📅 {yymmdd(pick(rec,'outbound_date'))} → {yymmdd(pick(rec,'return_date'))}\n"
            )

        try:
            creation_id = ig_create_media(image_url, caption)
            media_id = ig_publish_media(creation_id)

            batch_update_row(
                ws,
                row_i,
                hmap,
                {
                    "ig_posted_timestamp": utc_now_iso(),
                    "ig_media_id": media_id,
                    "ig_error": "",
                    "status": STATUS_POSTED_INSTAGRAM,
                },
            )
            log(f"✅ Instagram posted row {row_i}: status={STATUS_POSTED_INSTAGRAM} media_id={media_id}")
            done += 1

        except Exception as e:
            msg = safe_strip(e)
            batch_update_row(
                ws,
                row_i,
                hmap,
                {
                    "ig_posted_timestamp": utc_now_iso(),
                    "ig_media_id": "",
                    "ig_error": msg[:300],
                    "status": STATUS_ERROR_SOFT,
                },
            )
            log(f"❌ Instagram FAILED row {row_i}: {msg[:180]}")
            done += 1

    return done

# ============================================================
# TELEGRAM
# ============================================================

def tg_send(bot_token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": False}
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
        delta = now - t.astimezone(dt.timezone.utc)
        return delta.total_seconds() / 3600.0
    except Exception:
        return 999999.0

def build_tg_text(rec: Dict[str, str], tier: str) -> str:
    origin = pick(rec, "origin_city", "origin_iata")
    dest = pick(rec, "destination_city", "destination_iata")
    price_txt = gbp(pick(rec, "price_gbp"))
    out_d = yymmdd(pick(rec, "outbound_date"))
    ret_d = yymmdd(pick(rec, "return_date"))

    base = (
        f"🔥 {price_txt} return to {dest}\n\n"
        f"📍 From {origin}\n"
        f"📅 {out_d} → {ret_d}\n"
    )

    if tier == "VIP":
        return base + "\n💎 VIP early-access deal.\n"
    # FREE
    upsell = ""
    if STRIPE_LINK:
        upsell = (
            "\n⚠️ Heads up:\n"
            "• VIP members saw this early\n"
            "• Best deals go to VIPs first\n\n"
            f"💎 Upgrade: {STRIPE_LINK}\n"
        )
    return base + upsell

def stage_telegram_vip(ws: gspread.Worksheet, headers: List[str], max_rows: int = 1) -> int:
    if not (TELEGRAM_BOT_TOKEN_VIP and TELEGRAM_CHANNEL_VIP):
        log("Telegram VIP: DISABLED (missing TELEGRAM_BOT_TOKEN_VIP/TELEGRAM_CHANNEL_VIP)")
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
        raise RuntimeError(f"RAW_DEALS missing VIP Telegram columns: {missing}")

    done = 0
    for row_i, row in enumerate(rows, start=2):
        if done >= max_rows:
            break

        padded = pad_row(row, len(headers))
        status = norm_status(padded[hmap["status"] - 1])
        if status != STATUS_POSTED_INSTAGRAM:
            continue

        rec = record_from_row(headers, padded)
        text = build_tg_text(rec, "VIP")

        try:
            tg_send(TELEGRAM_BOT_TOKEN_VIP, TELEGRAM_CHANNEL_VIP, text)
            batch_update_row(
                ws,
                row_i,
                hmap,
                {
                    "tg_vip_timestamp": utc_now_iso(),
                    "tg_vip_error": "",
                    "status": STATUS_POSTED_TELEGRAM_VIP,
                },
            )
            log(f"✅ Telegram VIP posted row {row_i}: status={STATUS_POSTED_TELEGRAM_VIP}")
            done += 1

        except Exception as e:
            msg = safe_strip(e)
            batch_update_row(ws, row_i, hmap, {"tg_vip_timestamp": utc_now_iso(), "tg_vip_error": msg[:300]})
            log(f"❌ Telegram VIP FAILED row {row_i}: {msg[:180]}")
            done += 1

    return done

def stage_telegram_free(ws: gspread.Worksheet, headers: List[str], max_rows: int = 1) -> int:
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHANNEL):
        log("Telegram FREE: DISABLED (missing TELEGRAM_BOT_TOKEN/TELEGRAM_CHANNEL)")
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
        raise RuntimeError(f"RAW_DEALS missing FREE Telegram columns: {missing}")

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
            # not yet time to release to free
            continue

        text = build_tg_text(rec, "FREE")

        try:
            tg_send(TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL, text)
            batch_update_row(
                ws,
                row_i,
                hmap,
                {
                    "tg_free_timestamp": utc_now_iso(),
                    "tg_free_error": "",
                    "status": STATUS_POSTED_ALL,
                },
            )
            log(f"✅ Telegram FREE posted row {row_i}: status={STATUS_POSTED_ALL}")
            done += 1

        except Exception as e:
            msg = safe_strip(e)
            batch_update_row(ws, row_i, hmap, {"tg_free_timestamp": utc_now_iso(), "tg_free_error": msg[:300]})
            log(f"❌ Telegram FREE FAILED row {row_i}: {msg[:180]}")
            done += 1

    return done

# ============================================================
# MAIN
# ============================================================

def main() -> None:
    log("============================================================")
    log("🚀 TRAVELTXTER V4.2 UNIFIED PIPELINE (FIXED)")
    log("============================================================")
    log(f"Sheet: {SPREADSHEET_ID}")
    log(f"Tab: {RAW_DEALS_TAB}")
    log(f"Duffel: {'ENABLED' if DUFFEL_API_KEY else 'DISABLED'}")
    log(f"Instagram: {'ENABLED' if ig_enabled() else 'DISABLED'}")
    log(f"RUN_SLOT: {RUN_SLOT} | VIP_DELAY_HOURS={VIP_DELAY_HOURS}")
    log("============================================================")

    ws = get_ws()
    values = ws.get_all_values()
    if not values:
        die("ERROR: RAW_DEALS sheet is empty (missing headers).")

    headers = [safe_strip(h) for h in values[0]]

    # 1) FEED
    inserted = 0
    try:
        inserted = duffel_run_and_insert(ws, headers)
    except Exception as e:
        log(f"❌ FEED error: {safe_strip(e)[:220]}")

    # Refresh headers after potential sheet changes
    values = ws.get_all_values()
    headers = [safe_strip(h) for h in values[0]] if values else headers

    # 2) SCORE
    scored = 0
    try:
        scored = stage_ai_scoring(ws, headers, max_rows=MAX_SCORE_PER_RUN)
    except Exception as e:
        log(f"❌ SCORE error: {safe_strip(e)[:220]}")

    # 3) RENDER
    rendered = 0
    try:
        rendered = stage_render(ws, headers, max_rows=MAX_RENDER_PER_RUN)
    except Exception as e:
        log(f"❌ RENDER error: {safe_strip(e)[:220]}")

    # 4) INSTAGRAM
    ig_posted = 0
    try:
        ig_posted = stage_instagram(ws, headers, max_rows=MAX_IG_PER_RUN)
    except Exception as e:
        log(f"❌ IG error: {safe_strip(e)[:220]}")

    # 5) TELEGRAM VIP (after IG)
    vip_posted = 0
    try:
        vip_posted = stage_telegram_vip(ws, headers, max_rows=MAX_TG_VIP_PER_RUN)
    except Exception as e:
        log(f"❌ TG VIP error: {safe_strip(e)[:220]}")

    # 6) TELEGRAM FREE (after delay)
    free_posted = 0
    try:
        free_posted = stage_telegram_free(ws, headers, max_rows=MAX_TG_FREE_PER_RUN)
    except Exception as e:
        log(f"❌ TG FREE error: {safe_strip(e)[:220]}")

    log("------------------------------------------------------------")
    log(f"Done. inserted={inserted} scored={scored} rendered={rendered} ig_posted={ig_posted} vip_posted={vip_posted} free_posted={free_posted}")
    log("------------------------------------------------------------")


if __name__ == "__main__":
    main()
