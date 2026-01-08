#!/usr/bin/env python3
"""
TravelTxter V4.5x — telegram_publisher.py (BEST PICK + METADATA FIX)

Fix:
- If destination_city is IATA (e.g., FNC) or country blank, resolve using CONFIG_SIGNALS.
- Do NOT shout in ALL CAPS for city names.
- If still missing destination metadata, dead-letter the row (ERROR_HARD) so it can't loop.

AM (RUN_SLOT=AM):
  consumes: status == POSTED_INSTAGRAM
  writes:   posted_telegram_vip_at
  promotes: POSTED_INSTAGRAM -> POSTED_TELEGRAM_VIP

PM (RUN_SLOT=PM):
  consumes: status == POSTED_TELEGRAM_VIP AND posted_telegram_vip_at <= now-24h
  writes:   posted_telegram_free_at
  promotes: POSTED_TELEGRAM_VIP -> POSTED_ALL
"""

from __future__ import annotations

import os
import json
import datetime as dt
import time
import re
from typing import Any, Dict, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError


IATA_RE = re.compile(r"^[A-Z]{3}$")


def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)

def ts() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def env_str(k: str, default: str = "") -> str:
    return (os.environ.get(k, default) or "").strip()

def env_int(k: str, default: int) -> int:
    try:
        return int(env_str(k, str(default)))
    except Exception:
        return default


# -----------------------------
# Sheets auth
# -----------------------------

def _extract_sa(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()
    try:
        return json.loads(raw)
    except Exception:
        return json.loads(raw.replace("\\n", "\n"))

def get_client() -> gspread.Client:
    sa_raw = env_str("GCP_SA_JSON_ONE_LINE") or env_str("GCP_SA_JSON")
    if not sa_raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
    info = _extract_sa(sa_raw)
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds)

def open_sheet_with_backoff(gc: gspread.Client, spreadsheet_id: str, attempts: int = 8) -> gspread.Spreadsheet:
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
    raise RuntimeError("Sheets quota still exceeded after retries (429). Try again shortly.")


# -----------------------------
# A1 helpers
# -----------------------------

def col_letter(n1: int) -> str:
    s = ""
    n = n1
    while n:
        n, rr = divmod(n - 1, 26)
        s = chr(65 + rr) + s
    return s

def a1(rownum: int, col0: int) -> str:
    return f"{col_letter(col0 + 1)}{rownum}"


# -----------------------------
# Sheet helpers
# -----------------------------

def ensure_columns(ws: gspread.Worksheet, headers: List[str], required: List[str]) -> List[str]:
    missing = [c for c in required if c not in headers]
    if not missing:
        return headers
    ws.update([headers + missing], "A1")
    log(f"🛠️  Added missing columns: {missing}")
    return headers + missing

def safe_get(row: List[str], idx: int) -> str:
    return row[idx].strip() if 0 <= idx < len(row) else ""


# -----------------------------
# Metadata enrichment (CONFIG_SIGNALS)
# -----------------------------

def is_iata3(s: str) -> bool:
    return bool(IATA_RE.match((s or "").strip().upper()))

UK_AIRPORT_CITY_FALLBACK = {
    "LHR": "London", "LGW": "London", "STN": "London", "LTN": "London", "LCY": "London", "SEN": "London",
    "MAN": "Manchester", "BRS": "Bristol", "BHX": "Birmingham", "EDI": "Edinburgh", "GLA": "Glasgow",
    "NCL": "Newcastle", "LPL": "Liverpool", "NQY": "Newquay", "SOU": "Southampton", "CWL": "Cardiff", "EXT": "Exeter",
}

def load_config_signals_maps(sh: gspread.Spreadsheet) -> Tuple[Dict[str, str], Dict[str, str]]:
    try:
        ws = sh.worksheet("CONFIG_SIGNALS")
    except Exception:
        return {}, {}

    values = ws.get_all_values()
    if not values or len(values) < 2:
        return {}, {}

    headers = [h.strip() for h in values[0]]
    idx = {h: i for i, h in enumerate(headers)}

    def pick(*names: str) -> Optional[int]:
        for n in names:
            if n in idx:
                return idx[n]
        return None

    i_iata = pick("iata_hint", "destination_iata", "iata", "airport_iata")
    i_city = pick("destination_city", "city", "dest_city", "airport_city")
    i_country = pick("destination_country", "country", "dest_country")

    if i_iata is None:
        return {}, {}

    iata_to_city: Dict[str, str] = {}
    iata_to_country: Dict[str, str] = {}

    for r in values[1:]:
        code = (r[i_iata] if i_iata < len(r) else "").strip().upper()
        if not is_iata3(code):
            continue
        city = (r[i_city] if (i_city is not None and i_city < len(r)) else "").strip()
        country = (r[i_country] if (i_country is not None and i_country < len(r)) else "").strip()
        if city:
            iata_to_city[code] = city
        if country:
            iata_to_country[code] = country

    return iata_to_city, iata_to_country

def resolve_city(maybe_city: str, maybe_iata: str, iata_to_city: Dict[str, str]) -> str:
    c = (maybe_city or "").strip()
    if c and not is_iata3(c):
        return c
    code = (maybe_iata or c or "").strip().upper()
    if is_iata3(code):
        return iata_to_city.get(code) or UK_AIRPORT_CITY_FALLBACK.get(code) or code
    return c

def resolve_country(maybe_country: str, dest_iata: str, iata_to_country: Dict[str, str]) -> str:
    c = (maybe_country or "").strip()
    if c:
        return c
    code = (dest_iata or "").strip().upper()
    if is_iata3(code):
        return iata_to_country.get(code, "")
    return ""

def nice_city(s: str) -> str:
    t = (s or "").strip()
    if not t:
        return ""
    if is_iata3(t):
        return t
    # Keep internal casing for things like "São Paulo"
    return t


# -----------------------------
# Telegram sender
# -----------------------------

def tg_send(bot_token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    r = requests.post(url, json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True}, timeout=60)
    j = r.json()
    if not j.get("ok"):
        raise RuntimeError(f"Telegram sendMessage failed: {j}")


# -----------------------------
# Row selection
# -----------------------------

def parse_iso(s: str) -> Optional[dt.datetime]:
    t = (s or "").strip()
    if not t:
        return None
    try:
        return dt.datetime.fromisoformat(t.replace("Z", ""))
    except Exception:
        return None

def parse_num(s: str) -> Optional[float]:
    t = (s or "").strip().replace("£", "").replace(",", "")
    if not t:
        return None
    try:
        return float(t)
    except Exception:
        return None

def pick_best(rows: List[List[str]], h: Dict[str, int], run_slot: str) -> Optional[Tuple[int, List[str]]]:
    candidates: List[Tuple[int, List[str]]] = []
    now = dt.datetime.utcnow()

    for rownum, r in enumerate(rows, start=2):
        status = safe_get(r, h["status"]).upper()

        if run_slot == "AM":
            if status != "POSTED_INSTAGRAM":
                continue
            if safe_get(r, h["posted_telegram_vip_at"]):
                continue
        else:
            if status != "POSTED_TELEGRAM_VIP":
                continue
            vip_at = parse_iso(safe_get(r, h["posted_telegram_vip_at"]))
            if not vip_at:
                continue
            if now - vip_at < dt.timedelta(hours=24):
                continue
            if safe_get(r, h["posted_telegram_free_at"]):
                continue

        candidates.append((rownum, r))

    if not candidates:
        return None

    def key(item: Tuple[int, List[str]]):
        rownum, r = item
        score = parse_num(safe_get(r, h["deal_score"])) if "deal_score" in h else None
        scored_ts = parse_iso(safe_get(r, h["scored_timestamp"])) if "scored_timestamp" in h else None
        created_ts = parse_iso(safe_get(r, h["timestamp"])) if "timestamp" in h else None
        if created_ts is None and "created_at" in h:
            created_ts = parse_iso(safe_get(r, h["created_at"]))
        return (
            -(score if score is not None else -1e18),
            -(scored_ts.timestamp() if scored_ts else -1e18),
            -(created_ts.timestamp() if created_ts else -1e18),
            -rownum,
        )

    candidates.sort(key=key)
    return candidates[0]


# -----------------------------
# Message builders
# -----------------------------

def build_message(price_gbp: str, to_city: str, from_city: str, out_d: str, back_d: str, country: str, vip: bool) -> str:
    line1 = f"£{price_gbp} to {to_city}"
    if country:
        line1 = f"£{price_gbp} to {to_city}, {country}"

    lines = [
        line1,
        "",
        f"TO: {to_city}",
        f"FROM: {from_city}",
        f"OUT: {out_d}",
        f"BACK: {back_d}",
        "",
    ]

    if vip:
        lines += [
            "Heads up:",
            "• VIP members saw this 24 hours ago",
            "",
            "Want instant access?",
            "Join TravelTxter VIP for £3/month:",
            "• Deals 24 hours early",
            "• Direct booking links",
            "Upgrade now: " + env_str("STRIPE_LINK_VIP", env_str("STRIPE_LINK", "")),
        ]
    else:
        lines += [
            "Want deals 24 hours early?",
            "Upgrade to VIP: " + env_str("STRIPE_LINK_VIP", env_str("STRIPE_LINK", "")),
        ]

    return "\n".join([x for x in lines if x is not None]).strip()


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    run_slot = env_str("RUN_SLOT", "AM").upper()

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")

    # Tokens/Channels
    if run_slot == "AM":
        bot_token = env_str("TELEGRAM_BOT_TOKEN_VIP") or env_str("TELEGRAM_BOT_TOKEN")
        chat_id = env_str("TELEGRAM_CHANNEL_VIP") or env_str("TELEGRAM_CHANNEL")
        vip_mode = True
    else:
        bot_token = env_str("TELEGRAM_BOT_TOKEN")
        chat_id = env_str("TELEGRAM_CHANNEL")
        vip_mode = False

    if not bot_token or not chat_id:
        raise RuntimeError("Missing Telegram bot/channel env vars for this slot")

    gc = get_client()
    sh = open_sheet_with_backoff(gc, spreadsheet_id)
    ws = sh.worksheet(tab)

    values = ws.get_all_values()
    if not values or len(values) < 2:
        log("No rows.")
        return 0

    headers = [h.strip() for h in values[0]]
    required_cols = [
        "status",
        "deal_id",
        "price_gbp",
        "origin_iata",
        "destination_iata",
        "origin_city",
        "destination_city",
        "destination_country",
        "outbound_date",
        "return_date",
        "deal_score",
        "scored_timestamp",
        "timestamp",
        "created_at",
        "posted_telegram_vip_at",
        "posted_telegram_free_at",
        # dead-letter fields (safe add)
        "publish_error",
        "publish_error_at",
    ]
    headers = ensure_columns(ws, headers, required_cols)

    values = ws.get_all_values()
    headers = [h.strip() for h in values[0]]
    rows = values[1:]
    h = {name: i for i, name in enumerate(headers)}

    iata_to_city, iata_to_country = load_config_signals_maps(sh)

    best = pick_best(rows, h, run_slot)
    if not best:
        log("No eligible rows for this slot.")
        return 0

    rownum, r = best

    # Resolve metadata
    origin_city = resolve_city(safe_get(r, h["origin_city"]), safe_get(r, h["origin_iata"]), iata_to_city)
    dest_city = resolve_city(safe_get(r, h["destination_city"]), safe_get(r, h["destination_iata"]), iata_to_city)
    dest_country = resolve_country(safe_get(r, h["destination_country"]), safe_get(r, h["destination_iata"]), iata_to_country)

    origin_city = nice_city(origin_city)
    dest_city = nice_city(dest_city)

    if (not dest_country) or (not dest_city) or is_iata3(dest_city):
        ws.batch_update(
            [
                {"range": a1(rownum, h["status"]), "values": [["ERROR_HARD"]]},
                {"range": a1(rownum, h["publish_error"]), "values": [["missing_destination_metadata"]]},
                {"range": a1(rownum, h["publish_error_at"]), "values": [[ts()]]},
            ],
            value_input_option="USER_ENTERED",
        )
        log(f"🧯 Dead-lettered row {rownum} (missing destination metadata)")
        return 0

    msg = build_message(
        price_gbp=safe_get(r, h["price_gbp"]),
        to_city=dest_city,
        from_city=origin_city,
        out_d=safe_get(r, h["outbound_date"]),
        back_d=safe_get(r, h["return_date"]),
        country=dest_country,
        vip=vip_mode,
    )

    tg_send(bot_token, chat_id, msg)

    # Update sheet status/timestamps
    if run_slot == "AM":
        ws.batch_update(
            [
                {"range": a1(rownum, h["posted_telegram_vip_at"]), "values": [[ts()]]},
                {"range": a1(rownum, h["status"]), "values": [["POSTED_TELEGRAM_VIP"]]},
            ],
            value_input_option="USER_ENTERED",
        )
        log(f"✅ Telegram VIP posted row {rownum}")
    else:
        ws.batch_update(
            [
                {"range": a1(rownum, h["posted_telegram_free_at"]), "values": [[ts()]]},
                {"range": a1(rownum, h["status"]), "values": [["POSTED_ALL"]]},
            ],
            value_input_option="USER_ENTERED",
        )
        log(f"✅ Telegram FREE posted row {rownum}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
