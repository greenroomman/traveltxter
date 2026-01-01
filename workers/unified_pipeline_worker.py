#!/usr/bin/env python3
"""
TravelTxter V4 Unified Pipeline (Practical Fix Version)

Fixes:
1) Duffel FEED now runs inside this worker (no silent 'DISABLED' stage).
2) VIP vs FREE Telegram gating:
   - VIP posts on AM run
   - FREE posts on PM run AND only if VIP was posted >= VIP_DELAY_HOURS ago
3) Works with an empty sheet (header-only): it will still insert deals.

Assumptions:
- RAW_DEALS has headers similar to your CSV (status, deal_id, origin_iata, destination_iata, price_gbp,
  outbound_date, return_date, airline, stops, booking_link_free, booking_link_vip,
  tg_monthly_timestamp, tg_annual_timestamp, tg_free_timestamp, posted_to_vip, posted_for_free, etc.)
- CONFIG tab is optional. If present, it should include enabled/origin_iata/destination_iata.
"""

import os
import json
import time
import uuid
import math
import datetime as dt
from typing import Dict, List, Tuple, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials

from openai import OpenAI


# =========================
# ENV
# =========================
GCP_SA_JSON = os.getenv("GCP_SA_JSON", "").strip()
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "").strip()
RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS").strip()

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
DUFFEL_MAX_INSERTS = int(os.getenv("DUFFEL_MAX_INSERTS", "3"))
DUFFEL_ROUTES_PER_RUN = int(os.getenv("DUFFEL_ROUTES_PER_RUN", "1"))
CONFIG_TAB = os.getenv("CONFIG_TAB", "CONFIG").strip()

DEFAULT_ORIGIN_IATA = os.getenv("DEFAULT_ORIGIN_IATA", "LON").strip().upper()
DEFAULT_DEST_IATA = os.getenv("DEFAULT_DEST_IATA", "BCN").strip().upper()
DAYS_AHEAD = int(os.getenv("DAYS_AHEAD", "60"))
TRIP_LENGTH_DAYS = int(os.getenv("TRIP_LENGTH_DAYS", "5"))

RUN_SLOT = os.getenv("RUN_SLOT", "AM").strip().upper()  # AM or PM
VIP_DELAY_HOURS = int(os.getenv("VIP_DELAY_HOURS", "24"))

STATUS_NEW = "NEW"
STATUS_READY_TO_PUBLISH = "READY_TO_PUBLISH"
STATUS_POSTED_ALL = "POSTED_ALL"

DUFFEL_ENABLED = bool(DUFFEL_API_KEY)


# =========================
# LOG
# =========================
def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


# =========================
# SHEETS
# =========================
def get_sheet() -> gspread.Worksheet:
    if not GCP_SA_JSON:
        raise RuntimeError("Missing GCP_SA_JSON")
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID")

    creds_dict = json.loads(GCP_SA_JSON)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(RAW_DEALS_TAB)
    return ws


def get_headers(ws: gspread.Worksheet) -> List[str]:
    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("Sheet missing header row (row 1).")
    return headers


def header_map(headers: List[str]) -> Dict[str, int]:
    # 1-based indexes for gspread
    return {h.strip(): i + 1 for i, h in enumerate(headers) if h.strip()}


def safe_get(row: Dict[str, str], key: str) -> str:
    return (row.get(key) or "").strip()


# =========================
# CONFIG ROUTES
# =========================
def load_routes_from_config(ws_parent) -> List[Tuple[str, str]]:
    """
    Reads CONFIG tab if it exists:
    expects columns: enabled, origin_iata, destination_iata
    """
    try:
        cfg = ws_parent.worksheet(CONFIG_TAB)
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
# DUFFEL
# =========================
def duffel_offer_request(origin: str, dest: str, out_date: str, ret_date: str) -> Dict:
    url = "https://api.duffel.com/air/offer_requests"
    headers = {
        "Authorization": f"Bearer {DUFFEL_API_KEY}",
        "Duffel-Version": "beta",
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
            "return_offers": True,
            "currency": "GBP",
        }
    }
    r = requests.post(url, headers=headers, json=payload, timeout=60)
    if r.status_code >= 300:
        raise RuntimeError(f"Duffel error {r.status_code}: {r.text[:500]}")
    return r.json()


def pick_offers(offer_request_json: Dict, max_n: int) -> List[Dict]:
    offers = (((offer_request_json or {}).get("data") or {}).get("offers") or [])
    # sort by total_amount (GBP)
    def amt(o: Dict) -> float:
        try:
            return float(o.get("total_amount") or "1e9")
        except Exception:
            return 1e9

    offers_sorted = sorted(offers, key=amt)
    return offers_sorted[: max(0, max_n)]


def iso_date(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")


def duffel_run_and_insert(ws: gspread.Worksheet, headers: List[str]) -> int:
    if not DUFFEL_ENABLED:
        log("Duffel is DISABLED (no DUFFEL_API_KEY). Skipping search.")
        return 0

    # Load routes: CONFIG > fallback
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
        # simple: choose one departure date window = today + 21d, return = +TRIP_LENGTH_DAYS
        out_date = today + dt.timedelta(days=min(21, DAYS_AHEAD))
        ret_date = out_date + dt.timedelta(days=TRIP_LENGTH_DAYS)

        log(f"Duffel search: {origin}->{dest} {iso_date(out_date)}->{iso_date(ret_date)}")
        data = duffel_offer_request(origin, dest, iso_date(out_date), iso_date(ret_date))
        offers = pick_offers(data, max_n=DUFFEL_MAX_INSERTS)

        if not offers:
            log("Duffel returned 0 offers.")
            continue

        rows_to_append: List[List[str]] = []
        for off in offers:
            if inserted >= DUFFEL_MAX_INSERTS:
                break

            # Very defensive parsing
            price = off.get("total_amount") or ""
            currency = off.get("total_currency") or "GBP"
            if currency != "GBP":
                continue

            # airline guess
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
            row_obj["outbound_date"] = iso_date(out_date)
            row_obj["return_date"] = iso_date(ret_date)
            row_obj["price_gbp"] = price
            row_obj["airline"] = owner
            row_obj["stops"] = stops
            row_obj["status"] = STATUS_NEW

            # Optional link placeholders
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

    origin = safe_get(row, "origin_iata") or safe_get(row, "origin_city") or "UK"
    dest = safe_get(row, "destination_iata") or safe_get(row, "destination_city") or "Somewhere"
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
ai_verdict (GOOD/AVERAGE/POOR),
ai_caption (max 320 chars).
""".strip()

    resp = client.responses.create(
        model=OPENAI_MODEL,
        input=prompt,
    )

    text_out = (resp.output_text or "").strip()
    # best-effort JSON parse
    ai_score, ai_verdict, ai_caption = "60", "AVERAGE", ""
    try:
        obj = json.loads(text_out)
        ai_score = str(obj.get("ai_score", ai_score))
        ai_verdict = str(obj.get("ai_verdict", ai_verdict)).upper()
        ai_caption = str(obj.get("ai_caption", "")).strip()
    except Exception:
        ai_caption = text_out[:320]

    return ai_score, ai_verdict, ai_caption


def stage_score(ws: gspread.Worksheet, headers: List[str], max_rows: int = 1) -> int:
    hmap = header_map(headers)

    needed_cols = ["status", "ai_score", "ai_verdict", "ai_caption"]
    for c in needed_cols:
        if c not in hmap:
            log(f"Score stage: missing column '{c}', skipping.")
            return 0

    values = ws.get_all_values()
    if len(values) < 2:
        log("Score stage: no data rows.")
        return 0

    count = 0
    for i in range(2, len(values) + 1):  # row index in sheet
        row_vals = values[i - 1]
        row = {headers[j]: (row_vals[j] if j < len(row_vals) else "") for j in range(len(headers))}
        status = safe_get(row, "status").upper()
        if status != STATUS_NEW:
            continue

        ai_score, ai_verdict, ai_caption = score_caption(row)
        updates = {
            "ai_score": ai_score,
            "ai_verdict": ai_verdict,
            "ai_caption": ai_caption,
            "status": STATUS_READY_TO_PUBLISH,
        }
        # write row (single batch)
        cell_range = []
        for k, v in updates.items():
            col = hmap.get(k)
            if col:
                cell_range.append(gspread.Cell(i, col, v))
        if cell_range:
            ws.update_cells(cell_range, value_input_option="USER_ENTERED")
            count += 1
            log(f"Scored row {i}: score={ai_score} verdict={ai_verdict}")
        if count >= max_rows:
            break

    return count


# =========================
# TELEGRAM
# =========================
def tg_send(bot_token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": False}
    r = requests.post(url, json=payload, timeout=30)
    if r.status_code >= 300:
        raise RuntimeError(f"Telegram error {r.status_code}: {r.text[:300]}")


def parse_ts(s: str) -> Optional[dt.datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        # accept "2026-01-01T10:18:47Z" or isoformat
        s2 = s.replace("Z", "+00:00")
        return dt.datetime.fromisoformat(s2).astimezone(dt.timezone.utc).replace(tzinfo=None)
    except Exception:
        return None


def now_utc() -> dt.datetime:
    return dt.datetime.utcnow().replace(tzinfo=None)


def stage_telegram_vip(ws: gspread.Worksheet, headers: List[str], max_rows: int = 1) -> int:
    if RUN_SLOT != "AM":
        log("VIP Telegram: skipped (RUN_SLOT != AM).")
        return 0
    if not TELEGRAM_BOT_TOKEN_VIP or not TELEGRAM_CHANNEL_VIP:
        log("VIP Telegram: missing creds, skipped.")
        return 0

    hmap = header_map(headers)
    for c in ["status", "tg_monthly_timestamp", "tg_annual_timestamp", "posted_to_vip"]:
        if c not in hmap:
            log(f"VIP Telegram: missing '{c}', skipped.")
            return 0

    values = ws.get_all_values()
    if len(values) < 2:
        return 0

    posted = 0
    for i in range(2, len(values) + 1):
        row_vals = values[i - 1]
        row = {headers[j]: (row_vals[j] if j < len(row_vals) else "") for j in range(len(headers))}
        if safe_get(row, "status").upper() != STATUS_READY_TO_PUBLISH:
            continue

        already = safe_get(row, "posted_to_vip").upper() in {"TRUE", "1", "YES", "Y"}
        if already:
            continue

        caption = safe_get(row, "ai_caption") or "🔥 New VIP deal"
        link = safe_get(row, "booking_link_vip")
        msg = caption + (f"\n\nBook: {link}" if link else "")

        tg_send(TELEGRAM_BOT_TOKEN_VIP, TELEGRAM_CHANNEL_VIP, msg)

        ts = now_utc().replace(microsecond=0).isoformat() + "Z"
        updates = {
            "tg_monthly_timestamp": ts,   # treat as VIP sent marker
            "posted_to_vip": "TRUE",
        }
        cells = [gspread.Cell(i, hmap[k], v) for k, v in updates.items() if k in hmap]
        ws.update_cells(cells, value_input_option="USER_ENTERED")
        posted += 1
        log(f"VIP Telegram posted row {i}")
        if posted >= max_rows:
            break

    return posted


def stage_telegram_free(ws: gspread.Worksheet, headers: List[str], max_rows: int = 1) -> int:
    if RUN_SLOT != "PM":
        log("FREE Telegram: skipped (RUN_SLOT != PM).")
        return 0
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHANNEL:
        log("FREE Telegram: missing creds, skipped.")
        return 0

    hmap = header_map(headers)
    for c in ["status", "tg_free_timestamp", "posted_for_free", "posted_to_vip", "tg_monthly_timestamp"]:
        if c not in hmap:
            log(f"FREE Telegram: missing '{c}', skipped.")
            return 0

    values = ws.get_all_values()
    if len(values) < 2:
        return 0

    posted = 0
    for i in range(2, len(values) + 1):
        row_vals = values[i - 1]
        row = {headers[j]: (row_vals[j] if j < len(row_vals) else "") for j in range(len(headers))}
        if safe_get(row, "status").upper() != STATUS_READY_TO_PUBLISH:
            continue

        already_free = safe_get(row, "posted_for_free").upper() in {"TRUE", "1", "YES", "Y"}
        if already_free:
            continue

        # Must have been posted to VIP first
        posted_to_vip = safe_get(row, "posted_to_vip").upper() in {"TRUE", "1", "YES", "Y"}
        if not posted_to_vip:
            continue

        vip_ts = parse_ts(safe_get(row, "tg_monthly_timestamp"))
        if not vip_ts:
            continue

        # enforce VIP delay
        age_hours = (now_utc() - vip_ts).total_seconds() / 3600.0
        if age_hours < VIP_DELAY_HOURS:
            continue

        caption = safe_get(row, "ai_caption") or "🔥 New deal"
        link = safe_get(row, "booking_link_free")
        msg = caption + (f"\n\nBook: {link}" if link else "")

        tg_send(TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL, msg)

        ts = now_utc().replace(microsecond=0).isoformat() + "Z"
        updates = {
            "tg_free_timestamp": ts,
            "posted_for_free": "TRUE",
            # Once both tiers posted, mark fully done
            "status": STATUS_POSTED_ALL,
        }
        cells = [gspread.Cell(i, hmap[k], v) for k, v in updates.items() if k in hmap]
        ws.update_cells(cells, value_input_option="USER_ENTERED")
        posted += 1
        log(f"FREE Telegram posted row {i}")
        if posted >= max_rows:
            break

    return posted


# =========================
# MAIN
# =========================
def main() -> None:
    log("============================================================")
    log("🚀 TRAVELTXTER V4 UNIFIED PIPELINE (FIX VERSION)")
    log("============================================================")
    log(f"Sheet: {SPREADSHEET_ID}")
    log(f"Tab: {RAW_DEALS_TAB}")
    log(f"Duffel: {'ENABLED' if DUFFEL_ENABLED else 'DISABLED'}")
    log(f"RUN_SLOT: {RUN_SLOT} | VIP_DELAY_HOURS={VIP_DELAY_HOURS}")
    log("============================================================")

    ws = get_sheet()
    headers = get_headers(ws)

    # 1) Duffel insert (works even if sheet has no rows yet)
    inserted = duffel_run_and_insert(ws, headers)
    log(f"Duffel inserted: {inserted}")

    # 2) Score NEW -> READY_TO_PUBLISH
    scored = stage_score(ws, headers, max_rows=1)
    log(f"Scored: {scored}")

    # 3) Telegram VIP (AM)
    vip_posted = stage_telegram_vip(ws, headers, max_rows=1)
    log(f"VIP posted: {vip_posted}")

    # 4) Telegram FREE (PM) after delay
    free_posted = stage_telegram_free(ws, headers, max_rows=1)
    log(f"FREE posted: {free_posted}")

    log("✅ Pipeline complete.")


if __name__ == "__main__":
    main()
