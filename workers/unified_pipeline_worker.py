# workers/unified_pipeline_worker.py
#!/usr/bin/env python3
"""
Traveltxter V4 Unified Pipeline Worker

Stages:
0) Duffel feeder (optional) -> inserts NEW rows into RAW_DEALS when empty/low
1) AI scorer -> NEW -> READY_TO_PUBLISH
2) Renderer -> adds graphic_url (or equivalent) and promotes READY_TO_PUBLISH -> READY_TO_POST
3) Instagram publisher -> posts one READY_TO_POST -> POSTED_INSTAGRAM
4) Telegram FREE -> posts one POSTED_INSTAGRAM -> POSTED_ALL
5) Telegram VIP (optional) -> posts one READY_TO_POST (or VIP logic) -> POSTED_* (implementation dependent)

Key rules:
- Uses single column: status
- Header-map only (no hardcoded column numbers)
- Guarded writes, idempotent behavior
"""

import os
import sys
import json
import time
import math
import hashlib
import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials

# OpenAI SDK (v1+)
try:
    from openai import OpenAI
except Exception:
    OpenAI = None


# =========================
# Logging
# =========================

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


def die(msg: str, code: int = 1) -> None:
    log(f"❌ {msg}")
    raise SystemExit(code)


# =========================
# Env helpers
# =========================

def env(name: str, default: Optional[str] = None) -> str:
    val = os.getenv(name, default)
    return "" if val is None else str(val)


def env_int(name: str, default: int) -> int:
    try:
        return int(env(name, str(default)).strip() or str(default))
    except Exception:
        return default


def env_float(name: str, default: float) -> float:
    try:
        return float(env(name, str(default)).strip() or str(default))
    except Exception:
        return default


def env_bool(name: str, default: bool = False) -> bool:
    v = env(name, "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "y", "on")


# =========================
# Google Sheets
# =========================

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
STATUS_COLUMN = "status"

RAW_STATUS_NEW = env("RAW_STATUS_NEW", "NEW").strip() or "NEW"
RAW_DEALS_TAB = env("RAW_DEALS_TAB", "RAW_DEALS").strip() or "RAW_DEALS"

STATUS_READY_TO_PUBLISH = "READY_TO_PUBLISH"
STATUS_READY_TO_POST = "READY_TO_POST"
STATUS_POSTED_INSTAGRAM = "POSTED_INSTAGRAM"
STATUS_POSTED_ALL = "POSTED_ALL"
STATUS_ERROR_HARD = "ERROR_HARD"

# Processing caps
MAX_SCORE_PER_RUN = env_int("MAX_SCORE_PER_RUN", 1)
MAX_RENDER_PER_RUN = env_int("MAX_RENDER_PER_RUN", 1)
MAX_IG_PER_RUN = env_int("MAX_IG_PER_RUN", 1)
MAX_TG_FREE_PER_RUN = env_int("MAX_TG_FREE_PER_RUN", 1)
MAX_TG_VIP_PER_RUN = env_int("MAX_TG_VIP_PER_RUN", 1)

# Duffel caps
DUFFEL_MAX_INSERTS = env_int("DUFFEL_MAX_INSERTS", 3)
DUFFEL_MAX_INSERTS = min(DUFFEL_MAX_INSERTS, 20)  # hard safety ceiling

FRESHNESS_DECAY_PER_DAY = env_float("FRESHNESS_DECAY_PER_DAY", 2.0)

# Keys/tokens
GCP_SA_JSON = env("GCP_SA_JSON", "").strip()
SPREADSHEET_ID = env("SPREADSHEET_ID", "").strip()

DUFFEL_API_KEY = env("DUFFEL_API_KEY", "").strip()

OPENAI_API_KEY = env("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = env("OPENAI_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"

RENDER_URL = env("RENDER_URL", "").strip()

IG_ACCESS_TOKEN = env("IG_ACCESS_TOKEN", "").strip()
IG_USER_ID = env("IG_USER_ID", "").strip()

TELEGRAM_BOT_TOKEN = env("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_FREE_CHANNEL = env("TELEGRAM_FREE_CHANNEL", env("TELEGRAM_CHANNEL", "")).strip()

TELEGRAM_BOT_TOKEN_VIP = env("TELEGRAM_BOT_TOKEN_VIP", "").strip()
TELEGRAM_VIP_CHANNEL = env("TELEGRAM_VIP_CHANNEL", env("TELEGRAM_CHANNEL_VIP", "")).strip()

STRIPE_LINK = env("STRIPE_LINK", "").strip()

SKYSCANNER_AFFILIATE_ID = env("SKYSCANNER_AFFILIATE_ID", "").strip()


def get_gspread_client() -> gspread.Client:
    if not GCP_SA_JSON:
        die("Missing GCP_SA_JSON secret/env")
    info = json.loads(GCP_SA_JSON)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)


def get_ws(tab_name: str) -> gspread.Worksheet:
    if not SPREADSHEET_ID:
        die("Missing SPREADSHEET_ID")
    gc = get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    return sh.worksheet(tab_name)


def read_headers(ws: gspread.Worksheet) -> List[str]:
    headers = ws.row_values(1)
    return [h.strip() for h in headers]


def header_map(headers: List[str]) -> Dict[str, int]:
    # map lower(header)->1-based col index
    m = {}
    for i, h in enumerate(headers, start=1):
        if h:
            m[h.strip().lower()] = i
    return m


def find_col(headers_map: Dict[str, int], name: str) -> int:
    key = name.strip().lower()
    if key not in headers_map:
        die(f"Missing required column in sheet: {name}")
    return headers_map[key]


def get_all_values(ws: gspread.Worksheet) -> List[List[str]]:
    return ws.get_all_values()


def now_utc_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


# =========================
# UTIL
# =========================

def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(str(x).replace("£", "").replace(",", "").strip())
    except Exception:
        return default


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def clamp(n: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, n))


# =========================
# Duffel feeder (Stage 0)
# =========================

DUFFEL_BASE = "https://api.duffel.com"


def duffel_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {DUFFEL_API_KEY}",
        "Duffel-Version": "v1",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def pick_route_from_config() -> Optional[Dict[str, Any]]:
    """
    Reads CONFIG tab and picks the next enabled route.

    Expected headers (case-insensitive):
      enabled, origin_iata, origin_city, destination_iata, destination_city, theme, max_connections, cabin_class

    If CONFIG missing/empty, caller will handle fallback.
    """
    ws_cfg = get_ws("CONFIG")
    vals = ws_cfg.get_all_values()
    if len(vals) < 2:
        return None

    headers = [h.strip().lower() for h in vals[0]]
    idx = {h: i for i, h in enumerate(headers)}
    required = ["enabled", "origin_iata", "destination_iata"]
    for r in required:
        if r not in idx:
            die(f"CONFIG tab missing header: {r}")

    enabled_routes: List[Dict[str, Any]] = []
    for row in vals[1:]:
        if not row or len(row) < len(headers):
            continue
        enabled_raw = (row[idx["enabled"]] or "").strip().lower()
        enabled = enabled_raw in ("true", "1", "yes", "y", "on")
        if not enabled:
            continue

        route = {
            "origin_iata": (row[idx["origin_iata"]] or "").strip(),
            "origin_city": (row[idx.get("origin_city", -1)] if idx.get("origin_city", -1) >= 0 else "") or "",
            "destination_iata": (row[idx["destination_iata"]] or "").strip(),
            "destination_city": (row[idx.get("destination_city", -1)] if idx.get("destination_city", -1) >= 0 else "") or "",
            "theme": (row[idx.get("theme", -1)] if idx.get("theme", -1) >= 0 else "") or "",
            "max_connections": int((row[idx.get("max_connections", -1)] if idx.get("max_connections", -1) >= 0 else "1") or "1"),
            "cabin_class": ((row[idx.get("cabin_class", -1)] if idx.get("cabin_class", -1) >= 0 else "economy") or "economy").strip().lower(),
        }
        if route["origin_iata"] and route["destination_iata"]:
            enabled_routes.append(route)

    if not enabled_routes:
        return None

    # Simple rotation: pick by day index
    day_idx = int(dt.datetime.utcnow().strftime("%j"))  # 1..366
    return enabled_routes[(day_idx - 1) % len(enabled_routes)]


def duffel_search_roundtrip(
    origin: str,
    destination: str,
    days_ahead: int,
    trip_length_days: int,
    cabin_class: str = "economy",
    max_connections: int = 1,
) -> List[Dict[str, Any]]:
    """
    Creates an offer request and returns offers.
    """
    depart_date = (dt.date.today() + dt.timedelta(days=max(1, days_ahead))).isoformat()
    return_date = (dt.date.fromisoformat(depart_date) + dt.timedelta(days=max(1, trip_length_days))).isoformat()

    payload = {
        "data": {
            "slices": [
                {"origin": origin, "destination": destination, "departure_date": depart_date},
                {"origin": destination, "destination": origin, "departure_date": return_date},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": cabin_class,
            "max_connections": max_connections,
        }
    }

    r = requests.post(f"{DUFFEL_BASE}/air/offer_requests", headers=duffel_headers(), json=payload, timeout=60)
    if r.status_code >= 300:
        raise RuntimeError(f"Duffel offer_requests failed ({r.status_code}): {r.text[:400]}")
    offer_request = r.json()["data"]
    offers = offer_request.get("offers", []) or []
    return offers


def upsert_rows(ws: gspread.Worksheet, headers: List[str], rows: List[Dict[str, Any]]) -> int:
    """
    Appends NEW rows to RAW_DEALS using header mapping.
    """
    hmap = header_map(headers)

    def set_cell(buf: List[str], col: str, val: Any) -> None:
        key = col.lower()
        if key not in hmap:
            return
        idx = hmap[key] - 1
        if idx >= len(buf):
            buf.extend([""] * (idx + 1 - len(buf)))
        buf[idx] = "" if val is None else str(val)

    inserted = 0
    for r in rows[:DUFFEL_MAX_INSERTS]:
        buf = [""] * len(headers)
        for k, v in r.items():
            set_cell(buf, k, v)
        ws.append_row(buf, value_input_option="USER_ENTERED")
        inserted += 1
    return inserted


def run_duffel_feeder_if_needed(ws: gspread.Worksheet) -> int:
    """
    If RAW_DEALS has no data rows, try to seed from Duffel.
    Requires DUFFEL_API_KEY (and CONFIG or ORIGIN_IATA/DEST_IATA fallback).
    """
    if not DUFFEL_API_KEY:
        return 0

    headers = read_headers(ws)
    vals = ws.get_all_values()
    data_rows = max(0, len(vals) - 1)
    if data_rows > 0:
        return 0

    log("🧪 Stage 0: Duffel feeder (sheet empty)")

    try:
        # Pick route from CONFIG (preferred).
        # Fallback: ORIGIN_IATA + DEST_IATA envs for quick-start / debugging.
        route = None
        try:
            route = pick_route_from_config()
        except Exception as e:
            log(f"   ⚠️  CONFIG route selection failed: {e}")
            route = None

        if not route:
            origin_env = os.getenv("ORIGIN_IATA", "").strip()
            dest_env = os.getenv("DEST_IATA", "").strip()
            if origin_env and dest_env:
                route = {
                    "origin_iata": origin_env,
                    "origin_city": origin_env,
                    "destination_iata": dest_env,
                    "destination_city": dest_env,
                    "theme": os.getenv("THEME", "default"),
                    "max_connections": int(os.getenv("MAX_CONNECTIONS", "1")),
                    "cabin_class": os.getenv("CABIN_CLASS", "economy"),
                }
                log("   ℹ️  Using ORIGIN_IATA/DEST_IATA fallback route (CONFIG missing/empty)")

        if not route:
            log("   ⚠️  No route selected (CONFIG empty/missing and no ORIGIN_IATA/DEST_IATA provided)")
            return 0

        origin_code = route['origin_iata']
        dest_code = route["destination_iata"]
        cabin_class = route.get("cabin_class", "economy") or "economy"
        max_connections = int(route.get("max_connections", 1) or 1)

        days_ahead = env_int("DAYS_AHEAD", 60)
        trip_length = env_int("TRIP_LENGTH_DAYS", 5)

        log(f"   🔎 Searching {origin_code} -> {dest_code} (days_ahead={days_ahead}, trip={trip_length}d)")
        offers = duffel_search_roundtrip(
            origin=origin_code,
            destination=dest_code,
            days_ahead=days_ahead,
            trip_length_days=trip_length,
            cabin_class=cabin_class,
            max_connections=max_connections,
        )
        if not offers:
            log("   ⚠️  Duffel returned 0 offers")
            return 0

        # Minimal normalization -> rows
        rows: List[Dict[str, Any]] = []
        for off in offers[:DUFFEL_MAX_INSERTS]:
            total_amount = off.get("total_amount") or ""
            total_currency = off.get("total_currency") or ""
            price_gbp = total_amount  # leave as amount; your sheet formulas/currency handling can adjust

            slices = off.get("slices") or []
            out_depart = ""
            ret_depart = ""
            stops = "0"
            airline = ""

            if slices:
                segs0 = (slices[0].get("segments") or [])
                if segs0:
                    out_depart = segs0[0].get("departing_at", "")[:10]
                    airline = (segs0[0].get("marketing_carrier", {}) or {}).get("name", "") or airline
                    stops = str(max(0, len(segs0) - 1))
                if len(slices) > 1:
                    segs1 = (slices[1].get("segments") or [])
                    if segs1:
                        ret_depart = segs1[0].get("departing_at", "")[:10]

            deal_id = sha1(json.dumps(off, sort_keys=True)[:2000])

            row = {
                "deal_id": deal_id,
                "origin_city": route.get("origin_city") or origin_code,
                "destination_city": route.get("destination_city") or dest_code,
                "destination_country": "",
                "price_gbp": price_gbp,
                "outbound_date": out_depart,
                "return_date": ret_depart,
                "trip_length_days": env_int("TRIP_LENGTH_DAYS", 5),
                "stops": stops,
                "baggage_included": "",
                "airline": airline,
                "deal_source": "Duffel",
                "notes": "",
                "date_added": now_utc_iso(),
                "status": RAW_STATUS_NEW,
            }
            rows.append(row)

        inserted = upsert_rows(ws, headers, rows)
        log(f"   ✅ Inserted {inserted} new rows")
        return inserted

    except Exception as e:
        log(f"   ❌ Duffel feeder failed: {e}")
        return 0


# =========================
# AI Scoring (Stage 1)
# =========================

def openai_client() -> Optional[Any]:
    if not OPENAI_API_KEY or OpenAI is None:
        return None
    return OpenAI(api_key=OPENAI_API_KEY)


def score_deal_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returns fields to write back: ai_score, ai_grading, ai_verdict, ai_caption, ai_notes, scored_timestamp
    """
    client = openai_client()
    if client is None:
        # deterministic fallback scoring
        price = safe_float(row.get("price_gbp", 0.0), 9999.0)
        stops = safe_float(row.get("stops", 0), 0.0)
        score = clamp(100 - price * 0.2 - stops * 10, 0, 100)
        verdict = "GOOD" if score >= 70 else ("AVERAGE" if score >= 45 else "POOR")
        return {
            "ai_score": round(score, 1),
            "ai_grading": verdict,
            "ai_verdict": verdict,
            "ai_caption": "",
            "ai_notes": "OpenAI disabled - fallback scoring",
            "scored_timestamp": now_utc_iso(),
        }

    prompt = {
        "role": "system",
        "content": (
            "You are a UK travel deal editor. "
            "Given deal fields, output strict JSON with keys: "
            "ai_score (0-100 number), ai_verdict (GOOD|AVERAGE|POOR), ai_caption (short IG caption), ai_notes (one line). "
            "No extra keys."
        ),
    }
    user = {
        "role": "user",
        "content": json.dumps(
            {
                "origin": row.get("origin_city"),
                "destination": row.get("destination_city"),
                "price_gbp": row.get("price_gbp"),
                "outbound_date": row.get("outbound_date"),
                "return_date": row.get("return_date"),
                "trip_length_days": row.get("trip_length_days"),
                "stops": row.get("stops"),
                "airline": row.get("airline"),
            },
            ensure_ascii=False,
        ),
    }

    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[prompt, user],
        temperature=0.4,
    )
    txt = resp.choices[0].message.content.strip()
    try:
        data = json.loads(txt)
    except Exception:
        data = {"ai_score": 50, "ai_verdict": "AVERAGE", "ai_caption": "", "ai_notes": "Parse fail"}

    score = float(data.get("ai_score", 50))
    verdict = str(data.get("ai_verdict", "AVERAGE")).upper()
    if verdict not in ("GOOD", "AVERAGE", "POOR"):
        verdict = "AVERAGE"

    return {
        "ai_score": round(score, 1),
        "ai_grading": verdict,
        "ai_verdict": verdict,
        "ai_caption": str(data.get("ai_caption", "")).strip(),
        "ai_notes": str(data.get("ai_notes", "")).strip(),
        "scored_timestamp": now_utc_iso(),
    }


def stage_score(ws: gspread.Worksheet, headers: List[str]) -> int:
    vals = ws.get_all_values()
    if len(vals) <= 1:
        return 0
    h = vals[0]
    hmap = header_map(h)
    status_col = find_col(hmap, STATUS_COLUMN)

    updated = 0
    for r_idx in range(2, len(vals) + 1):
        row_vals = vals[r_idx - 1]
        status = (row_vals[status_col - 1] if status_col - 1 < len(row_vals) else "").strip()
        if status != RAW_STATUS_NEW:
            continue

        row_obj = {h[i].strip(): (row_vals[i] if i < len(row_vals) else "") for i in range(len(h))}
        scored = score_deal_row(row_obj)

        updates = []
        for k, v in scored.items():
            if k.lower() in hmap:
                updates.append((r_idx, hmap[k.lower()], v))
        # promote
        updates.append((r_idx, status_col, STATUS_READY_TO_PUBLISH))

        for rr, cc, vv in updates:
            ws.update_cell(rr, cc, vv)

        log(f"✅ Scored row {r_idx} -> {STATUS_READY_TO_PUBLISH}")
        updated += 1
        if updated >= MAX_SCORE_PER_RUN:
            break

    return updated


# =========================
# Render (Stage 2)
# =========================

def render_image(row: Dict[str, Any]) -> Optional[str]:
    if not RENDER_URL:
        return None
    payload = {
        "origin": row.get("origin_city", ""),
        "destination": row.get("destination_city", ""),
        "price_gbp": row.get("price_gbp", ""),
        "outbound_date": row.get("outbound_date", ""),
        "return_date": row.get("return_date", ""),
        "trip_length_days": row.get("trip_length_days", ""),
    }
    r = requests.post(RENDER_URL, json=payload, timeout=60)
    if r.status_code >= 300:
        raise RuntimeError(f"Render failed ({r.status_code}): {r.text[:300]}")
    data = r.json()
    return data.get("graphic_url") or data.get("image_url") or data.get("url")


def stage_render(ws: gspread.Worksheet, headers: List[str]) -> int:
    vals = ws.get_all_values()
    if len(vals) <= 1:
        return 0
    h = vals[0]
    hmap = header_map(h)
    status_col = find_col(hmap, STATUS_COLUMN)

    # common output columns (safe if absent)
    graphic_col = hmap.get("graphic_url", hmap.get("image_url", 0))

    updated = 0
    for r_idx in range(2, len(vals) + 1):
        row_vals = vals[r_idx - 1]
        status = (row_vals[status_col - 1] if status_col - 1 < len(row_vals) else "").strip()
        if status != STATUS_READY_TO_PUBLISH:
            continue

        row_obj = {h[i].strip(): (row_vals[i] if i < len(row_vals) else "") for i in range(len(h))}
        try:
            url = render_image(row_obj)
        except Exception as e:
            log(f"⚠️ Render failed for row {r_idx}: {e}")
            continue

        if url and graphic_col:
            ws.update_cell(r_idx, graphic_col, url)

        ws.update_cell(r_idx, status_col, STATUS_READY_TO_POST)
        log(f"✅ Rendered row {r_idx} -> {STATUS_READY_TO_POST}")
        updated += 1
        if updated >= MAX_RENDER_PER_RUN:
            break

    return updated


# =========================
# Instagram publish (Stage 3)
# =========================

def ig_create_container(image_url: str, caption: str) -> str:
    url = f"https://graph.facebook.com/v19.0/{IG_USER_ID}/media"
    r = requests.post(
        url,
        data={"image_url": image_url, "caption": caption, "access_token": IG_ACCESS_TOKEN},
        timeout=60,
    )
    if r.status_code >= 300:
        raise RuntimeError(f"IG media create failed ({r.status_code}): {r.text[:300]}")
    return r.json()["id"]


def ig_publish_container(creation_id: str) -> str:
    url = f"https://graph.facebook.com/v19.0/{IG_USER_ID}/media_publish"
    r = requests.post(url, data={"creation_id": creation_id, "access_token": IG_ACCESS_TOKEN}, timeout=60)
    if r.status_code >= 300:
        raise RuntimeError(f"IG publish failed ({r.status_code}): {r.text[:300]}")
    return r.json().get("id", "")


def stage_instagram(ws: gspread.Worksheet, headers: List[str]) -> int:
    if not (IG_ACCESS_TOKEN and IG_USER_ID):
        return 0

    vals = ws.get_all_values()
    if len(vals) <= 1:
        return 0
    h = vals[0]
    hmap = header_map(h)
    status_col = find_col(hmap, STATUS_COLUMN)

    graphic_col = hmap.get("graphic_url", hmap.get("image_url", 0))
    caption_col = hmap.get("ai_caption", 0)
    published_ts_col = hmap.get("published_timestamp", 0)

    if not graphic_col:
        log("⚠️ No graphic_url/image_url column found; IG stage cannot run")
        return 0

    updated = 0
    for r_idx in range(2, len(vals) + 1):
        row_vals = vals[r_idx - 1]
        status = (row_vals[status_col - 1] if status_col - 1 < len(row_vals) else "").strip()
        if status != STATUS_READY_TO_POST:
            continue

        image_url = row_vals[graphic_col - 1] if graphic_col - 1 < len(row_vals) else ""
        caption = row_vals[caption_col - 1] if caption_col and caption_col - 1 < len(row_vals) else ""
        if not image_url:
            log(f"⚠️ Row {r_idx} missing image_url; skipping")
            continue

        creation_id = ig_create_container(image_url=image_url, caption=caption)
        ig_publish_container(creation_id=creation_id)

        if published_ts_col:
            ws.update_cell(r_idx, published_ts_col, now_utc_iso())
        ws.update_cell(r_idx, status_col, STATUS_POSTED_INSTAGRAM)
        log(f"✅ Instagram posted row {r_idx} -> {STATUS_POSTED_INSTAGRAM}")

        updated += 1
        if updated >= MAX_IG_PER_RUN:
            break

    return updated


# =========================
# Telegram (Stages 4/5)
# =========================

def tg_send(token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    r = requests.post(url, data={"chat_id": chat_id, "text": text, "parse_mode": "HTML"}, timeout=60)
    if r.status_code >= 300:
        raise RuntimeError(f"Telegram send failed ({r.status_code}): {r.text[:300]}")


def format_tg_message(row: Dict[str, Any], vip: bool = False) -> str:
    price = row.get("price_gbp", "")
    origin = row.get("origin_city", "")
    dest = row.get("destination_city", "")
    out_date = row.get("outbound_date", "")
    ret_date = row.get("return_date", "")

    if vip:
        upsell = ""
    else:
        upsell = f"\n\n💎 Want instant access?\nUpgrade: {STRIPE_LINK}" if STRIPE_LINK else ""

    return (
        f"🔥 <b>£{price}</b> to <b>{dest}</b>\n\n"
        f"📍 From {origin}\n"
        f"📅 {out_date} → {ret_date}"
        f"{upsell}"
    )


def stage_telegram_free(ws: gspread.Worksheet, headers: List[str]) -> int:
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_FREE_CHANNEL):
        return 0

    vals = ws.get_all_values()
    if len(vals) <= 1:
        return 0
    h = vals[0]
    hmap = header_map(h)
    status_col = find_col(hmap, STATUS_COLUMN)

    updated = 0
    for r_idx in range(2, len(vals) + 1):
        row_vals = vals[r_idx - 1]
        status = (row_vals[status_col - 1] if status_col - 1 < len(row_vals) else "").strip()
        if status != STATUS_POSTED_INSTAGRAM:
            continue

        row_obj = {h[i].strip(): (row_vals[i] if i < len(row_vals) else "") for i in range(len(h))}
        msg = format_tg_message(row_obj, vip=False)
        tg_send(TELEGRAM_BOT_TOKEN, TELEGRAM_FREE_CHANNEL, msg)

        ws.update_cell(r_idx, status_col, STATUS_POSTED_ALL)
        log(f"✅ Telegram FREE posted row {r_idx} -> {STATUS_POSTED_ALL}")

        updated += 1
        if updated >= MAX_TG_FREE_PER_RUN:
            break

    return updated


def stage_telegram_vip(ws: gspread.Worksheet, headers: List[str]) -> int:
    if not (TELEGRAM_BOT_TOKEN_VIP and TELEGRAM_VIP_CHANNEL):
        return 0

    vals = ws.get_all_values()
    if len(vals) <= 1:
        return 0
    h = vals[0]
    hmap = header_map(h)
    status_col = find_col(hmap, STATUS_COLUMN)

    updated = 0
    for r_idx in range(2, len(vals) + 1):
        row_vals = vals[r_idx - 1]
        status = (row_vals[status_col - 1] if status_col - 1 < len(row_vals) else "").strip()
        if status != STATUS_READY_TO_POST:
            continue

        row_obj = {h[i].strip(): (row_vals[i] if i < len(row_vals) else "") for i in range(len(h))}
        msg = format_tg_message(row_obj, vip=True)
        tg_send(TELEGRAM_BOT_TOKEN_VIP, TELEGRAM_VIP_CHANNEL, msg)

        # VIP does not advance the main status by default (leave for IG->FREE progression)
        log(f"✅ Telegram VIP posted row {r_idx}")

        updated += 1
        if updated >= MAX_TG_VIP_PER_RUN:
            break

    return updated


# =========================
# Main
# =========================

def main() -> None:
    log("============================================================")
    log("🚀 TRAVELTXTER V4 UNIFIED PIPELINE")
    log("============================================================")
    log(f"Sheet: {SPREADSHEET_ID[:6]}***" if SPREADSHEET_ID else "Sheet: (missing)")
    log(f"Tab: {RAW_DEALS_TAB}")
    log(f"Duffel: {'ENABLED' if DUFFEL_API_KEY else 'DISABLED'}")
    log(f"Freshness decay: {FRESHNESS_DECAY_PER_DAY}/day")
    log("============================================================")

    ws = get_ws(RAW_DEALS_TAB)
    log(f"✅ Connected: {RAW_DEALS_TAB}")

    vals = ws.get_all_values()
    data_rows = max(0, len(vals) - 1)

    if data_rows == 0 and not DUFFEL_API_KEY:
        die("No data rows and Duffel is DISABLED. Ensure DUFFEL_API_KEY is passed to this step in GitHub Actions.")

    # Stage 0: seed if empty
    inserted = run_duffel_feeder_if_needed(ws)

    # Refresh view after insert
    headers = read_headers(ws)

    # Stage 1: score
    n1 = stage_score(ws, headers)

    # Stage 2: render
    n2 = stage_render(ws, headers)

    # Stage 5: VIP (optional, before IG if you want early access)
    n5 = stage_telegram_vip(ws, headers)

    # Stage 3: IG
    n3 = stage_instagram(ws, headers)

    # Stage 4: Telegram FREE
    n4 = stage_telegram_free(ws, headers)

    log("============================================================")
    log("✅ Pipeline run complete")
    log(f"Inserted: {inserted} | Scored: {n1} | Rendered: {n2} | VIP TG: {n5} | IG: {n3} | FREE TG: {n4}")
    log("============================================================")


if __name__ == "__main__":
    main()
