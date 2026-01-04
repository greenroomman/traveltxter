#!/usr/bin/env python3
"""
Traveltxter V4.5.3_Waterwheel — Publisher (Layout + Status Compatible)

Fixes the "no output" issue:
- AM slot selects best candidate from statuses: READY_TO_PUBLISH -> READY_TO_POST -> SCORED
- PM slot selects best candidate from status: POSTED_TELEGRAM_VIP (after VIP_DELAY_HOURS)

Locked outputs:
- Telegram VIP layout (BOOK NOW Skyscanner/affiliate link)
- Telegram FREE layout (VIP upsell + Monthly/Yearly Stripe hyperlinks)
- Instagram caption layout
- NO emojis anywhere except national flags
"""

from __future__ import annotations

import os
import json
import time
import hashlib
import datetime as dt
from typing import Dict, Any, List, Tuple, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# ENV
# ============================================================

def env(name: str, default: str = "", required: bool = False) -> str:
    v = (os.getenv(name) or "").strip()
    if not v:
        v = default
    if required and not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


SPREADSHEET_ID = env("SPREADSHEET_ID", required=True)
RAW_DEALS_TAB = env("RAW_DEALS_TAB", "RAW_DEALS")

PHRASE_BANK_TAB = env("PHRASE_BANK_TAB", "PHRASE_BANK")

GCP_SA_JSON = env("GCP_SA_JSON", "") or env("GCP_SA_JSON_ONE_LINE", "")
if not GCP_SA_JSON:
    raise RuntimeError("Missing required env var: GCP_SA_JSON or GCP_SA_JSON_ONE_LINE")

RENDER_URL = env("RENDER_URL", required=True)

IG_ACCESS_TOKEN = env("IG_ACCESS_TOKEN", required=True)
IG_USER_ID = env("IG_USER_ID", required=True)

TELEGRAM_BOT_TOKEN_VIP = env("TELEGRAM_BOT_TOKEN_VIP", required=True)
TELEGRAM_CHANNEL_VIP = env("TELEGRAM_CHANNEL_VIP", required=True)

TELEGRAM_BOT_TOKEN = env("TELEGRAM_BOT_TOKEN", required=True)
TELEGRAM_CHANNEL = env("TELEGRAM_CHANNEL", required=True)

STRIPE_LINK_MONTHLY = env("STRIPE_LINK_MONTHLY", "")
STRIPE_LINK_YEARLY = env("STRIPE_LINK_YEARLY", "")

RUN_SLOT = env("RUN_SLOT", "AM").upper()  # AM or PM
VIP_DELAY_HOURS = int(env("VIP_DELAY_HOURS", "24"))

# Instagram in PM is optional; default ON (matches earlier behavior)
POST_INSTAGRAM_ON_PM = env("POST_INSTAGRAM_ON_PM", "true").lower() in ("1", "true", "yes")


# ============================================================
# TIME / LOG
# ============================================================

def now_utc_dt() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def now_utc_str() -> str:
    return now_utc_dt().replace(microsecond=0).isoformat().replace("+00:00", "Z")

def log(msg: str) -> None:
    print(f"{now_utc_str()} | {msg}", flush=True)

def parse_iso_utc(s: str) -> Optional[dt.datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(dt.timezone.utc)
        d = dt.datetime.fromisoformat(s)
        if d.tzinfo is None:
            return d.replace(tzinfo=dt.timezone.utc)
        return d.astimezone(dt.timezone.utc)
    except Exception:
        return None

def hours_since(ts: str) -> float:
    d = parse_iso_utc(ts)
    if not d:
        return 9999.0
    return (now_utc_dt() - d).total_seconds() / 3600.0


# ============================================================
# SAFE TEXT
# ============================================================

def safe_text(v: Any) -> str:
    if v is None:
        return ""
    try:
        return str(v)
    except Exception:
        return ""

def safe_get(row: Dict[str, Any], key: str) -> str:
    return safe_text(row.get(key)).strip()

def safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(str(v).strip())
    except Exception:
        return default

def stable_hash(text: str) -> int:
    return int(hashlib.md5(text.encode("utf-8")).hexdigest(), 16)


# ============================================================
# EMOJI RULE: NO EMOJIS EXCEPT FLAGS
# ============================================================

def strip_nonflag_emojis(text: str) -> str:
    if not text:
        return ""
    out = []
    for ch in text:
        cp = ord(ch)
        # keep regional indicators (flags)
        if 0x1F1E6 <= cp <= 0x1F1FF:
            out.append(ch)
            continue
        # strip common emoji ranges
        if (
            0x1F300 <= cp <= 0x1FAFF or
            0x2600 <= cp <= 0x26FF or
            0x2700 <= cp <= 0x27BF or
            0xFE00 <= cp <= 0xFE0F
        ):
            continue
        out.append(ch)
    return "".join(out)

def clean_line(s: str) -> str:
    return strip_nonflag_emojis((s or "").strip())


# ============================================================
# PRICE FORMAT
# ============================================================

def format_price_gbp(price_str: str) -> str:
    try:
        v = float(str(price_str).strip())
        return f"£{v:,.2f}".replace(",", "")
    except Exception:
        s = (price_str or "").strip()
        if not s:
            return "£0.00"
        if s.startswith("£"):
            return s
        return f"£{s}"


# ============================================================
# FLAGS (ONLY EMOJI ALLOWED)
# ============================================================

_COUNTRY_TO_ISO2 = {
    "UNITED KINGDOM": "GB", "UK": "GB", "GREAT BRITAIN": "GB",
    "IRELAND": "IE",
    "ICELAND": "IS",
    "SPAIN": "ES",
    "PORTUGAL": "PT",
    "FRANCE": "FR",
    "ITALY": "IT",
    "GREECE": "GR",
    "TURKEY": "TR",
    "MOROCCO": "MA",
    "EGYPT": "EG",
    "TUNISIA": "TN",
    "CAPE VERDE": "CV",
    "MALTA": "MT",
    "CROATIA": "HR",
    "MONTENEGRO": "ME",
    "ALBANIA": "AL",
    "NETHERLANDS": "NL",
    "BELGIUM": "BE",
    "GERMANY": "DE",
    "AUSTRIA": "AT",
    "SWITZERLAND": "CH",
    "POLAND": "PL",
    "CZECHIA": "CZ", "CZECH REPUBLIC": "CZ",
    "HUNGARY": "HU",
    "ROMANIA": "RO",
    "BULGARIA": "BG",
    "SERBIA": "RS",
    "SLOVENIA": "SI",
    "SLOVAKIA": "SK",
    "SWEDEN": "SE",
    "NORWAY": "NO",
    "DENMARK": "DK",
    "FINLAND": "FI",
    "ESTONIA": "EE",
    "LATVIA": "LV",
    "LITHUANIA": "LT",
    "USA": "US", "UNITED STATES": "US", "UNITED STATES OF AMERICA": "US",
    "CANADA": "CA",
    "MEXICO": "MX",
    "BRAZIL": "BR",
    "COLOMBIA": "CO",
    "PERU": "PE",
    "ARGENTINA": "AR",
    "CHILE": "CL",
    "THAILAND": "TH",
    "VIETNAM": "VN",
    "CAMBODIA": "KH",
    "LAOS": "LA",
    "MALAYSIA": "MY",
    "SINGAPORE": "SG",
    "INDONESIA": "ID",
    "PHILIPPINES": "PH",
    "JAPAN": "JP",
    "SOUTH KOREA": "KR", "KOREA": "KR",
    "CHINA": "CN",
    "HONG KONG": "HK",
    "TAIWAN": "TW",
    "INDIA": "IN",
    "SRI LANKA": "LK",
    "NEPAL": "NP",
    "AUSTRALIA": "AU",
    "NEW ZEALAND": "NZ",
    "SOUTH AFRICA": "ZA",
    "KENYA": "KE",
    "TANZANIA": "TZ",
    "NAMIBIA": "NA",
    "BOTSWANA": "BW",
    "ZAMBIA": "ZM",
    "ZIMBABWE": "ZW",
    "GHANA": "GH",
    "NIGERIA": "NG",
    "GAMBIA": "GM",
    "SENEGAL": "SN",
    "UAE": "AE", "UNITED ARAB EMIRATES": "AE",
    "OMAN": "OM",
    "JORDAN": "JO",
    "ISRAEL": "IL",
}

def country_to_flag(country_name: str) -> str:
    name = (country_name or "").strip().upper()
    if not name:
        return ""
    iso2 = _COUNTRY_TO_ISO2.get(name, "")
    if not iso2 or len(iso2) != 2:
        return ""
    return chr(127397 + ord(iso2[0])) + chr(127397 + ord(iso2[1]))


# ============================================================
# GOOGLE SHEETS
# ============================================================

def get_spreadsheet() -> gspread.Spreadsheet:
    creds_json = json.loads(GCP_SA_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID)

def get_headers(ws: gspread.Worksheet) -> List[str]:
    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("Sheet has no headers row")
    return headers

def header_map(headers: List[str]) -> Dict[str, int]:
    return {h: i + 1 for i, h in enumerate(headers) if h}


# ============================================================
# PHRASE BANK (Approved only)
# ============================================================

def theme_key(theme: str) -> str:
    return (theme or "").strip().upper().replace(" ", "_")

def load_phrase_bank(sh: gspread.Spreadsheet) -> List[Dict[str, Any]]:
    try:
        ws = sh.worksheet(PHRASE_BANK_TAB)
    except Exception:
        return []
    values = ws.get_all_values()
    if len(values) < 2:
        return []
    headers = [h.strip() for h in values[0]]
    hmap = {h.lower(): idx for idx, h in enumerate(headers)}

    def g(row: List[str], col: str) -> str:
        idx = hmap.get(col.lower())
        if idx is None or idx >= len(row):
            return ""
        return (row[idx] or "").strip()

    out: List[Dict[str, Any]] = []
    for r in values[1:]:
        phrase = g(r, "phrase")
        if not phrase:
            continue
        approved_raw = g(r, "approved").lower()
        approved = approved_raw in ("true", "1", "yes", "y")
        out.append({
            "theme": g(r, "theme"),
            "category": g(r, "category") or "theme_phrase",
            "phrase": clean_line(phrase),
            "approved": approved,
        })
    return out

def build_phrase_index(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, List[str]]]:
    index: Dict[str, Dict[str, List[str]]] = {}

    def add(t: str, c: str, p: str):
        if not t:
            t = "ANY"
        if not c:
            c = "theme_phrase"
        index.setdefault(t, {}).setdefault(c, []).append(p)

    for r in rows:
        if not r.get("approved"):
            continue
        t = theme_key(r.get("theme", "")) or "ANY"
        c = (r.get("category") or "theme_phrase").strip().lower()
        p = clean_line(r.get("phrase", ""))
        if not p:
            continue
        add(t, c, p)
        add("ANY", c, p)
    return index

BLOGGER_OPENERS = [
    "If you fancy a quick reset,",
    "If you’ve been itching to get away,",
    "If your calendar’s looking flexible,",
    "If you want something easy to pull off,",
    "If you’re chasing a change of scene,",
]
BLOGGER_FINISHERS = [
    "Worth a quick look before it shifts.",
    "This is the kind of deal that doesn’t hang around.",
    "Good one to keep in your back pocket.",
    "If the dates work, it’s a strong shout.",
    "If you’re even half-tempted, check it now.",
]

def pick_phrase(index: Dict[str, Dict[str, List[str]]], theme: str, category: str, seed: str) -> str:
    t = theme_key(theme)
    c = (category or "theme_phrase").strip().lower()
    pool = index.get(t, {}).get(c, []) or index.get("ANY", {}).get(c, [])
    if not pool:
        return ""
    h = stable_hash(seed + "|" + t + "|" + c)
    return pool[h % len(pool)]

def build_vip_description(raw_phrase: str, seed: str) -> str:
    p = clean_line(raw_phrase)
    if not p:
        return "Nice, simple dates and a straightforward route — worth a quick look."
    if len(p) >= 55:
        return p
    h = stable_hash(seed)
    opener = BLOGGER_OPENERS[h % len(BLOGGER_OPENERS)]
    finisher = BLOGGER_FINISHERS[(h // 7) % len(BLOGGER_FINISHERS)]
    p2 = p.rstrip(".")
    return clean_line(f"{opener} {p2.lower()}. {finisher}")


# ============================================================
# TELEGRAM SEND (HTML hyperlinks)
# ============================================================

def tg_send(token: str, chat: str, text: str) -> str:
    r = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={
            "chat_id": chat,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": False
        },
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    try:
        return str(data["result"]["message_id"])
    except Exception:
        return ""


# ============================================================
# RENDER
# ============================================================

def render_image(row: Dict[str, Any]) -> str:
    payload = {
        "deal_id": safe_get(row, "deal_id"),
        "origin_city": safe_get(row, "origin_city") or safe_get(row, "origin_iata"),
        "destination_city": safe_get(row, "destination_city") or safe_get(row, "destination_iata"),
        "destination_country": safe_get(row, "destination_country"),
        "price_gbp": safe_get(row, "price_gbp"),
        "outbound_date": safe_get(row, "outbound_date"),
        "return_date": safe_get(row, "return_date"),
    }
    r = requests.post(RENDER_URL, json=payload, timeout=30)
    r.raise_for_status()
    url = safe_text(r.json().get("graphic_url")).strip()
    if not url:
        raise RuntimeError("Render did not return graphic_url")
    return url


# ============================================================
# INSTAGRAM
# ============================================================

def instagram_caption(row: Dict[str, Any]) -> str:
    country = clean_line(safe_get(row, "destination_country"))
    flag = country_to_flag(country)
    title = f"{country} {flag}".strip() if country else flag

    dest = clean_line(safe_get(row, "destination_city") or safe_get(row, "destination_iata"))
    origin = clean_line(safe_get(row, "origin_city") or safe_get(row, "origin_iata"))
    price = clean_line(format_price_gbp(safe_get(row, "price_gbp")))
    outd = clean_line(safe_get(row, "outbound_date"))
    retd = clean_line(safe_get(row, "return_date"))

    hook = "Quieter dates, usually easier on your wallet."

    lines = [
        title,
        "",
        f"To: {dest}",
        f"From: {origin}",
        f"Price: {price}",
        f"Out: {outd}",
        f"Return: {retd}",
        "",
        hook,
        "",
        "Link in bio…",
    ]
    return "\n".join([l for l in lines if l is not None]).strip()[:2200]

def post_instagram(image_url: str, caption: str) -> Tuple[str, str]:
    create_url = f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media"
    r1 = requests.post(
        create_url,
        data={"image_url": image_url, "caption": caption, "access_token": IG_ACCESS_TOKEN},
        timeout=30
    )
    r1.raise_for_status()
    creation_id = safe_text(r1.json().get("id")).strip()
    if not creation_id:
        raise RuntimeError("No creation_id returned from Instagram")

    publish_url = f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media_publish"
    r2 = requests.post(
        publish_url,
        data={"creation_id": creation_id, "access_token": IG_ACCESS_TOKEN},
        timeout=30
    )
    r2.raise_for_status()
    media_id = safe_text(r2.json().get("id")).strip()
    return creation_id, media_id


# ============================================================
# MESSAGE FORMATS (YOUR LOCKED LAYOUTS)
# ============================================================

def format_telegram_vip(row: Dict[str, Any], description_line: str) -> str:
    price = clean_line(format_price_gbp(safe_get(row, "price_gbp")))
    country = clean_line(safe_get(row, "destination_country"))
    flag = country_to_flag(country)
    country_with_flag = f"{country} {flag}".strip() if country else flag

    dest_city = clean_line(safe_get(row, "destination_city") or safe_get(row, "destination_iata"))
    origin_city = clean_line(safe_get(row, "origin_city") or safe_get(row, "origin_iata"))
    out_date = clean_line(safe_get(row, "outbound_date"))
    back_date = clean_line(safe_get(row, "return_date"))

    link = clean_line(
        safe_get(row, "booking_link_vip") or
        safe_get(row, "affiliate_url") or
        safe_get(row, "booking_link_free")
    )

    desc = clean_line(description_line)

    lines = [
        f"<b>{price} to {country_with_flag or dest_city}</b>",
        f"TO: {dest_city.upper()}",
        f"FROM: {origin_city}",
        f"OUT: {out_date}",
        f"BACK: {back_date}",
        "",
        desc,
        "",
    ]
    if link:
        lines.append(f'<a href="{link}"><b>BOOK NOW</b></a>')
    return "\n".join(lines).strip()

def format_telegram_free(row: Dict[str, Any]) -> str:
    price = clean_line(format_price_gbp(safe_get(row, "price_gbp")))
    country = clean_line(safe_get(row, "destination_country"))
    flag = country_to_flag(country)
    country_with_flag = f"{country} {flag}".strip() if country else flag

    dest_city = clean_line(safe_get(row, "destination_city") or safe_get(row, "destination_iata"))
    origin_city = clean_line(safe_get(row, "origin_city") or safe_get(row, "origin_iata"))
    out_date = clean_line(safe_get(row, "outbound_date"))
    back_date = clean_line(safe_get(row, "return_date"))

    lines = [
        f"<b>{price} to {country_with_flag or dest_city}</b>",
        f"TO: {dest_city}",
        f"FROM: {origin_city}",
        f"OUT: {out_date}",
        f"BACK: {back_date}",
        "",
        "Heads up:",
        "• VIP members saw this 24 hours ago",
        "• Availability is running low",
        "• Best deals go to VIPs first",
        "",
        "Want instant access?",
        "Join TravelTxter Nomad",
        "for £7.99 / month:",
        "- Live deals",
        "- Direct booking links",
        "- Exclusive mistake fares",
        "",
    ]

    if STRIPE_LINK_MONTHLY:
        lines.append(f'<a href="{STRIPE_LINK_MONTHLY}">Upgrade Monthly</a>')
    if STRIPE_LINK_YEARLY:
        lines.append(f'<a href="{STRIPE_LINK_YEARLY}">Upgrade Yearly</a>')

    return "\n".join(lines).strip()


# ============================================================
# STATUS COMPATIBILITY
# ============================================================

STATUS_SCORED = "SCORED"
STATUS_READY_TO_POST = "READY_TO_POST"
STATUS_READY_TO_PUBLISH = "READY_TO_PUBLISH"

STATUS_POSTED_INSTAGRAM = "POSTED_INSTAGRAM"
STATUS_POSTED_TELEGRAM_VIP = "POSTED_TELEGRAM_VIP"
STATUS_POSTED_ALL = "POSTED_ALL"


def normalize_status(s: str) -> str:
    return (s or "").strip().upper()


def pick_best_candidate(rows: List[Tuple[int, Dict[str, Any]]], allowed_statuses: List[str]) -> Optional[Tuple[int, Dict[str, Any]]]:
    allowed = {normalize_status(x) for x in allowed_statuses}
    candidates = []
    for idx, row in rows:
        if normalize_status(safe_get(row, "status")) in allowed:
            score = safe_float(safe_get(row, "ai_score"), 0.0)
            price = safe_float(safe_get(row, "price_gbp"), 999999.0)
            candidates.append(( -score, price, idx, row ))
    if not candidates:
        return None
    candidates.sort()
    return candidates[0][2], candidates[0][3]


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    log("=" * 72)
    log("Traveltxter V4.5.3 — Layout Publisher (Status Compatible)")
    log(f"RUN_SLOT={RUN_SLOT} | VIP_DELAY_HOURS={VIP_DELAY_HOURS}")
    log("=" * 72)

    sh = get_spreadsheet()
    raw_ws = sh.worksheet(RAW_DEALS_TAB)
    headers = get_headers(raw_ws)
    hmap = header_map(headers)

    all_values = raw_ws.get_all_values()
    if len(all_values) < 2:
        log("No rows in RAW_DEALS.")
        return

    # Build row dicts
    data_rows: List[Tuple[int, Dict[str, Any]]] = []
    for r_idx in range(2, len(all_values) + 1):
        vals = all_values[r_idx - 1]
        row = {headers[c]: (vals[c] if c < len(vals) else "") for c in range(len(headers))}
        data_rows.append((r_idx, row))

    # Load phrase bank (sheet tab)
    phrase_rows = load_phrase_bank(sh)
    phrase_index = build_phrase_index(phrase_rows)
    log(f"Phrase bank loaded from Sheet tab '{PHRASE_BANK_TAB}': {len(phrase_rows)} rows")

    # --------------------------
    # PM SLOT: Post FREE (after delay)
    # --------------------------
    if RUN_SLOT == "PM":
        eligible = []
        for idx, row in data_rows:
            if normalize_status(safe_get(row, "status")) != STATUS_POSTED_TELEGRAM_VIP:
                continue
            vip_ts = safe_get(row, "tg_monthly_timestamp")
            if hours_since(vip_ts) < VIP_DELAY_HOURS:
                continue
            eligible.append((idx, row))

        if not eligible:
            log("No eligible POSTED_TELEGRAM_VIP rows past delay. Nothing to post.")
            return

        # pick best by ai_score (fallback by price)
        chosen = pick_best_candidate(eligible, [STATUS_POSTED_TELEGRAM_VIP])
        if not chosen:
            log("No eligible row found after filtering.")
            return

        row_idx, row = chosen
        log(f"Posting Telegram FREE for row {row_idx}…")
        try:
            msg = format_telegram_free(row)
            mid = tg_send(TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL, msg)

            updates: List[gspread.Cell] = []
            if "tg_free_timestamp" in hmap:
                updates.append(gspread.Cell(row_idx, hmap["tg_free_timestamp"], now_utc_str()))
            if "tg_free_message_id" in hmap:
                updates.append(gspread.Cell(row_idx, hmap["tg_free_message_id"], mid))
            if "status" in hmap:
                updates.append(gspread.Cell(row_idx, hmap["status"], STATUS_POSTED_ALL))
            if updates:
                raw_ws.update_cells(updates, value_input_option="USER_ENTERED")

            log("Telegram FREE posted.")
        except Exception as e:
            log(f"Telegram FREE error: {e}")
            if "last_error" in hmap:
                raw_ws.update_cell(row_idx, hmap["last_error"], clean_line(str(e))[:250])
        return

    # --------------------------
    # AM SLOT: Post IG + VIP
    # --------------------------
    # Pick the best publishable candidate
    chosen = pick_best_candidate(data_rows, [STATUS_READY_TO_PUBLISH, STATUS_READY_TO_POST, STATUS_SCORED])
    if not chosen:
        log("No READY_TO_PUBLISH / READY_TO_POST / SCORED rows found. Nothing to post.")
        return

    row_idx, row = chosen
    log(f"Selected row {row_idx} for AM publish (status={safe_get(row,'status')}, ai_score={safe_get(row,'ai_score')}).")

    # 1) Instagram (AM always)
    try:
        graphic_url = safe_get(row, "graphic_url")
        if not graphic_url:
            graphic_url = render_image(row)
            if "graphic_url" in hmap:
                raw_ws.update_cell(row_idx, hmap["graphic_url"], graphic_url)

        image_url = f"{graphic_url}?cb={int(time.time())}"
        caption = instagram_caption(row)
        creation_id, media_id = post_instagram(image_url, caption)

        updates: List[gspread.Cell] = []
        if "ig_media_id" in hmap:
            updates.append(gspread.Cell(row_idx, hmap["ig_media_id"], media_id))
        if "ig_published_timestamp" in hmap:
            updates.append(gspread.Cell(row_idx, hmap["ig_published_timestamp"], now_utc_str()))
        if "status" in hmap:
            updates.append(gspread.Cell(row_idx, hmap["status"], STATUS_POSTED_INSTAGRAM))
        if updates:
            raw_ws.update_cells(updates, value_input_option="USER_ENTERED")

        log("Instagram posted.")
    except Exception as e:
        log(f"Instagram error: {e}")
        if "last_error" in hmap:
            raw_ws.update_cell(row_idx, hmap["last_error"], clean_line(str(e))[:250])
        return

    # Re-read updated row for VIP stage
    refreshed = raw_ws.row_values(row_idx)
    row = {headers[c]: (refreshed[c] if c < len(refreshed) else "") for c in range(len(headers))}

    # 2) Telegram VIP (AM)
    try:
        theme = safe_get(row, "theme_final") or safe_get(row, "resolved_theme") or safe_get(row, "theme") or "CITY"
        deal_id = safe_get(row, "deal_id") or f"row_{row_idx}"
        seed = f"{deal_id}|{theme_key(theme)}|AM"
        raw_phrase = pick_phrase(phrase_index, theme, "theme_phrase", seed)
        vip_desc = build_vip_description(raw_phrase, seed)

        msg = format_telegram_vip(row, vip_desc)
        mid = tg_send(TELEGRAM_BOT_TOKEN_VIP, TELEGRAM_CHANNEL_VIP, msg)

        updates = []
        if "tg_monthly_timestamp" in hmap:
            updates.append(gspread.Cell(row_idx, hmap["tg_monthly_timestamp"], now_utc_str()))
        if "tg_monthly_message_id" in hmap:
            updates.append(gspread.Cell(row_idx, hmap["tg_monthly_message_id"], mid))
        if "vip_description_used" in hmap:
            updates.append(gspread.Cell(row_idx, hmap["vip_description_used"], vip_desc))
        if "status" in hmap:
            updates.append(gspread.Cell(row_idx, hmap["status"], STATUS_POSTED_TELEGRAM_VIP))

        if updates:
            raw_ws.update_cells(updates, value_input_option="USER_ENTERED")

        log("Telegram VIP posted.")
    except Exception as e:
        log(f"Telegram VIP error: {e}")
        if "last_error" in hmap:
            raw_ws.update_cell(row_idx, hmap["last_error"], clean_line(str(e))[:250])


if __name__ == "__main__":
    main()
