#!/usr/bin/env python3
"""
TravelTxter V4.5.x — Telegram Publisher (VIP-first then FREE after delay)

WHAT THIS FIXES
- No all-caps city names in output
- Telegram hyperlinks (HTML parse_mode)
- FREE message uses Monthly + Annual upgrade links as clickable hyperlinks
- VIP message includes clickable booking link (Duffel/Skyscanner)
- VIP message includes "why it's good" bullets from available sheet columns

PIPELINE CONTRACT (unchanged)
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
import time
import datetime as dt
import re
import hashlib
from typing import Any, Dict, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError


IATA_RE = re.compile(r"^[A-Z]{3}$")


# -----------------------------
# Logging
# -----------------------------
def now_utc_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def log(msg: str) -> None:
    print(f"{now_utc_iso()} | {msg}", flush=True)


# -----------------------------
# Env
# -----------------------------
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
    raise RuntimeError("Sheets quota still exceeded after retries (429).")


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
    log(f"🛠️ Added missing columns: {missing}")
    return headers + missing

def safe_get(row: List[str], idx: int) -> str:
    return row[idx].strip() if 0 <= idx < len(row) else ""


# -----------------------------
# HTML safety (Telegram parse_mode=HTML)
# -----------------------------
def html_escape(s: str) -> str:
    s = s or ""
    return (
        s.replace("&", "&amp;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
    )

def html_link(url: str, text: str) -> str:
    url = (url or "").strip()
    text = (text or "").strip()
    if not url or not text:
        return html_escape(text or url)
    return f'<a href="{html_escape(url)}">{html_escape(text)}</a>'


# -----------------------------
# Metadata helpers
# -----------------------------
def is_iata3(s: str) -> bool:
    return bool(IATA_RE.match((s or "").strip().upper()))

UK_AIRPORT_CITY_FALLBACK = {
    "LHR": "London", "LGW": "London", "STN": "London", "LTN": "London", "LCY": "London", "SEN": "London",
    "MAN": "Manchester", "BRS": "Bristol", "BHX": "Birmingham", "EDI": "Edinburgh", "GLA": "Glasgow",
    "NCL": "Newcastle", "LPL": "Liverpool", "NQY": "Newquay", "SOU": "Southampton", "CWL": "Cardiff", "EXT": "Exeter",
}

FLAG_MAP = {
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
    "CANADA": "🇨🇦",
}

def country_flag(country: str) -> str:
    c = (country or "").strip().upper()
    return FLAG_MAP.get(c, "")

def load_config_signals_maps(sh: gspread.Spreadsheet) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Returns (iata->city, iata->country) from CONFIG_SIGNALS with flexible headers.
    """
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


# -----------------------------
# Phrase bank (optional)
# -----------------------------
def _truthy(x: str) -> bool:
    v = (x or "").strip().lower()
    return v in ("true", "yes", "1", "y", "on", "enabled")

def load_phrase_bank(sh: gspread.Spreadsheet) -> List[Dict[str, str]]:
    try:
        ws = sh.worksheet("PHRASE_BANK")
    except Exception:
        return []

    values = ws.get_all_values()
    if not values or len(values) < 2:
        return []

    headers = [h.strip() for h in values[0]]
    out: List[Dict[str, str]] = []
    for r in values[1:]:
        d: Dict[str, str] = {}
        for i, h in enumerate(headers):
            d[h] = (r[i] if i < len(r) else "").strip()
        if any(d.values()):
            out.append(d)
    return out

def pick_phrase(phrases: List[Dict[str, str]], theme: str, deal_id: str) -> str:
    th = (theme or "").strip().upper()
    approved = [r for r in phrases if _truthy(r.get("approved", "")) and (r.get("phrase", "").strip() != "")]
    if not approved:
        return ""
    themed = [r for r in approved if (r.get("theme", "").strip().upper() == th)] if th else []
    pool = themed if themed else approved
    h = hashlib.md5((deal_id or "noid").encode("utf-8")).hexdigest()
    idx = int(h[:8], 16) % len(pool)
    return (pool[idx].get("phrase", "") or "").strip()


# -----------------------------
# Telegram sender
# -----------------------------
def tg_send(bot_token: str, chat_id: str, text_html: str) -> None:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text_html,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    r = requests.post(url, json=payload, timeout=60)
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
        score = parse_num(safe_get(r, h.get("deal_score", -1))) if "deal_score" in h else None
        scored_ts = parse_iso(safe_get(r, h.get("scored_timestamp", -1))) if "scored_timestamp" in h else None
        created_ts = parse_iso(safe_get(r, h.get("timestamp", -1))) if "timestamp" in h else None
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
def format_price(price_gbp: str) -> str:
    p = (price_gbp or "").strip()
    return p if p else "?"

def build_free_message(price: str, country: str, flag: str, to_city: str, from_city: str, out_d: str, back_d: str, phrase: str,
                       monthly_url: str, annual_url: str) -> str:
    header = f"£{price} to {to_city}"
    if country:
        header = f"£{price} to {country} {flag}".strip()

    parts: List[str] = []
    parts.append(f"<b>{html_escape(header)}</b>")
    parts.append("")
    parts.append(f"<b>TO:</b> {html_escape(to_city)}")
    parts.append(f"<b>FROM:</b> {html_escape(from_city)}")
    parts.append(f"<b>OUT:</b> {html_escape(out_d)}")
    parts.append(f"<b>BACK:</b> {html_escape(back_d)}")
    parts.append("")

    if phrase:
        parts.append(html_escape(phrase))
        parts.append("")

    parts.append("<b>Want instant access?</b>")
    parts.append("Join TravelTxter for early access:")
    parts.append("")
    parts.append("• VIP members saw this 24 hours ago")
    parts.append("• Deals 24 hours early")
    parts.append("• Direct booking links")
    parts.append("• Exclusive mistake fares")
    parts.append("• £3 p/m or £30 p/a")
    parts.append("• Cancel anytime")
    parts.append("")
    parts.append(f"👉 {html_link(monthly_url, 'Upgrade Monthly (£3)')}")
    parts.append(f"👉 {html_link(annual_url, 'Upgrade Annual (£30)')}")
    return "\n".join(parts).strip()

def build_vip_message(price: str, country: str, flag: str, to_city: str, from_city: str, out_d: str, back_d: str,
                      phrase: str, why_bullets: List[str], booking_url: str) -> str:
    header = f"£{price} to {to_city}"
    if country:
        header = f"£{price} to {country} {flag}".strip()

    parts: List[str] = []
    parts.append(f"<b>{html_escape(header)}</b>")
    parts.append("")
    parts.append(f"<b>TO:</b> {html_escape(to_city)}")
    parts.append(f"<b>FROM:</b> {html_escape(from_city)}")
    parts.append(f"<b>OUT:</b> {html_escape(out_d)}")
    parts.append(f"<b>BACK:</b> {html_escape(back_d)}")
    parts.append("")

    if phrase:
        parts.append(html_escape(phrase))
        parts.append("")

    if why_bullets:
        parts.append("<b>Why it’s good:</b>")
        for b in why_bullets[:3]:
            bb = (b or "").strip()
            if bb:
                parts.append(f"• {html_escape(bb)}")
        parts.append("")

    if booking_url:
        parts.append(f"✅ {html_link(booking_url, 'Book this deal')}")
    else:
        parts.append("✅ Booking link coming soon")
    return "\n".join(parts).strip()

def extract_why_bullets(row: List[str], h: Dict[str, int]) -> List[str]:
    """
    Pulls benefit bullets from whichever columns exist.
    We try, in order:
      benefit_1/benefit_2/benefit_3
      benefit_summary
      ai_notes
    """
    bullets: List[str] = []

    for k in ("benefit_1", "benefit_2", "benefit_3"):
        if k in h:
            v = safe_get(row, h[k])
            if v:
                bullets.append(v)

    if not bullets and "benefit_summary" in h:
        v = safe_get(row, h["benefit_summary"])
        if v:
            # split into bullets if user wrote multiple lines
            bullets.extend([x.strip("• ").strip() for x in v.splitlines() if x.strip()])

    if not bullets and "ai_notes" in h:
        v = safe_get(row, h["ai_notes"])
        if v:
            bullets.extend([x.strip("• ").strip() for x in v.splitlines() if x.strip()])

    # Last resort: try "ai_grading" (some builds use it)
    if not bullets and "ai_grading" in h:
        v = safe_get(row, h["ai_grading"])
        if v:
            bullets.append(v)

    # Remove duplicates while preserving order
    seen = set()
    out = []
    for b in bullets:
        bb = b.strip()
        if not bb:
            continue
        key = bb.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(bb)
    return out


# -----------------------------
# Main
# -----------------------------
def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    run_slot = env_str("RUN_SLOT", "AM").upper()

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")

    # Telegram routing by slot
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

    # Upgrade links (hyperlinks)
    # Prefer env vars, but include safe defaults matching your locked Stripe links.
    monthly_url = env_str("STRIPE_LINK_MONTHLY", "https://buy.stripe.com/3cI14g3rU4KOdiUbWJe7m08")
    annual_url  = env_str("STRIPE_LINK_ANNUAL",  "https://buy.stripe.com/9B67sE2nQa586Uw3qde7m07")

    gc = get_client()
    sh = open_sheet_with_backoff(gc, spreadsheet_id)
    ws = sh.worksheet(tab)

    values = ws.get_all_values()
    if not values or len(values) < 2:
        log("No rows.")
        return 0

    headers = [h.strip() for h in values[0]]
    required_cols = [
        "status", "deal_id", "price_gbp",
        "origin_iata", "destination_iata",
        "origin_city", "destination_city", "destination_country",
        "outbound_date", "return_date",
        "deal_theme",
        "deal_score", "scored_timestamp", "timestamp", "created_at",
        "posted_telegram_vip_at", "posted_telegram_free_at",
        # booking links (either may exist)
        "booking_link_vip", "booking_link",
        # error fields (safe)
        "publish_error", "publish_error_at",
        # optional benefits
        "benefit_1", "benefit_2", "benefit_3", "benefit_summary", "ai_notes", "ai_grading",
    ]
    headers = ensure_columns(ws, headers, required_cols)

    values = ws.get_all_values()
    headers = [h.strip() for h in values[0]]
    rows = values[1:]
    h = {name: i for i, name in enumerate(headers)}

    iata_to_city, iata_to_country = load_config_signals_maps(sh)
    phrases = load_phrase_bank(sh)

    best = pick_best(rows, h, run_slot)
    if not best:
        log("No eligible rows for this slot.")
        return 0

    rownum, r = best

    deal_id = safe_get(r, h["deal_id"])
    theme = safe_get(r, h.get("deal_theme", -1))
    phrase = pick_phrase(phrases, theme, deal_id)

    origin_city = resolve_city(safe_get(r, h["origin_city"]), safe_get(r, h["origin_iata"]), iata_to_city)
    dest_city = resolve_city(safe_get(r, h["destination_city"]), safe_get(r, h["destination_iata"]), iata_to_city)
    dest_country = resolve_country(safe_get(r, h["destination_country"]), safe_get(r, h["destination_iata"]), iata_to_country)

    # If missing metadata, dead-letter so it cannot loop spam
    if (not dest_country) or (not dest_city) or is_iata3(dest_city):
        ws.batch_update(
            [
                {"range": a1(rownum, h["status"]), "values": [["ERROR_HARD"]]},
                {"range": a1(rownum, h["publish_error"]), "values": [["missing_destination_metadata"]]},
                {"range": a1(rownum, h["publish_error_at"]), "values": [[now_utc_iso()]]},
            ],
            value_input_option="USER_ENTERED",
        )
        log(f"🧯 Dead-lettered row {rownum} (missing destination metadata)")
        return 0

    price = format_price(safe_get(r, h["price_gbp"]))
    out_d = safe_get(r, h["outbound_date"])
    back_d = safe_get(r, h["return_date"])

    flag = country_flag(dest_country)
    why_bullets = extract_why_bullets(r, h)

    # Booking link: prefer booking_link_vip, fallback booking_link
    booking_url = safe_get(r, h.get("booking_link_vip", -1)) if "booking_link_vip" in h else ""
    if not booking_url and "booking_link" in h:
        booking_url = safe_get(r, h["booking_link"])

    if vip_mode:
        msg = build_vip_message(
            price=price,
            country=dest_country,
            flag=flag,
            to_city=dest_city,
            from_city=origin_city,
            out_d=out_d,
            back_d=back_d,
            phrase=phrase,
            why_bullets=why_bullets,
            booking_url=booking_url,
        )
    else:
        msg = build_free_message(
            price=price,
            country=dest_country,
            flag=flag,
            to_city=dest_city,
            from_city=origin_city,
            out_d=out_d,
            back_d=back_d,
            phrase=phrase,
            monthly_url=monthly_url,
            annual_url=annual_url,
        )

    tg_send(bot_token, chat_id, msg)

    # Update sheet status/timestamps
    if run_slot == "AM":
        ws.batch_update(
            [
                {"range": a1(rownum, h["posted_telegram_vip_at"]), "values": [[now_utc_iso()]]},
                {"range": a1(rownum, h["status"]), "values": [["POSTED_TELEGRAM_VIP"]]},
            ],
            value_input_option="USER_ENTERED",
        )
        log(f"✅ Telegram VIP posted row {rownum}")
    else:
        ws.batch_update(
            [
                {"range": a1(rownum, h["posted_telegram_free_at"]), "values": [[now_utc_iso()]]},
                {"range": a1(rownum, h["status"]), "values": [["POSTED_ALL"]]},
            ],
            value_input_option="USER_ENTERED",
        )
        log(f"✅ Telegram FREE posted row {rownum}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
