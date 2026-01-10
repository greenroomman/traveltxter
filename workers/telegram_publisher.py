#!/usr/bin/env python3
"""
TravelTxter V4.5x — telegram_publisher.py (LOCKED + BEST PICK)

VIP post:
  - Consumes: status == POSTED_INSTAGRAM
  - Writes:   posted_telegram_vip_at
  - Promotes: POSTED_INSTAGRAM -> POSTED_TELEGRAM_VIP

FREE post (24h after VIP):
  - Consumes: status == POSTED_TELEGRAM_VIP AND posted_telegram_vip_at <= now-24h
  - Writes:   posted_telegram_free_at
  - Promotes: POSTED_TELEGRAM_VIP -> POSTED_ALL

RUN_SLOT support:
  - VIP:  RUN_SLOT in (VIP, AM, TEST)  -> VIP behaviour
  - FREE: RUN_SLOT in (FREE, PM)       -> FREE behaviour

LOCKED OUTPUT RULE:
- Telegram must show phrase_bank.
- If RAW_DEALS.phrase_bank is blank, this worker loads PHRASE_BANK and writes it into RAW_DEALS.phrase_bank.
- Flag emoji must use COUNTRY_FLAG_MAP (fallback 🌍 if unknown).
"""

from __future__ import annotations

import os
import json
import datetime as dt
import hashlib
from typing import Any, Dict, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials


# -----------------------------
# Logging
# -----------------------------

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


# -----------------------------
# Env helpers
# -----------------------------

def env_str(k: str, default: str = "") -> str:
    return (os.environ.get(k, default) or "").strip()

def env_any(keys: List[str], default: str = "") -> str:
    for k in keys:
        v = env_str(k, "")
        if v:
            return v
    return default

def clean_url(u: str) -> str:
    return (u or "").strip().replace(" ", "")

def utcnow() -> dt.datetime:
    return dt.datetime.utcnow()

def iso_now() -> str:
    return utcnow().replace(microsecond=0).isoformat() + "Z"


# -----------------------------
# Sheets auth
# -----------------------------

def _parse_sa_json(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return json.loads(raw.replace("\\n", "\n"))

def gs_client() -> gspread.Client:
    raw = env_any(["GCP_SA_JSON_ONE_LINE", "GCP_SA_JSON"])
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
    info = _parse_sa_json(raw)
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds)

def ensure_columns(ws: gspread.Worksheet, required_cols: List[str]) -> Dict[str, int]:
    headers = ws.row_values(1)
    if not headers:
        ws.update([required_cols], "A1")
        headers = required_cols[:]
        log(f"🛠️  Initialised headers for {ws.title}")

    headers = [h.strip() for h in headers]
    missing = [c for c in required_cols if c not in headers]
    if missing:
        headers = headers + missing
        ws.update([headers], "A1")
        log(f"🛠️  Added missing columns: {missing}")
    return {h: i for i, h in enumerate(headers)}


# -----------------------------
# Telegram send
# -----------------------------

def tg_send(bot_token: str, chat_id: str, message_html: str) -> None:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    r = requests.post(
        url,
        data={
            "chat_id": chat_id,
            "text": message_html,
            "parse_mode": "HTML",
            "disable_web_page_preview": "true",
        },
        timeout=60,
    )
    j = r.json()
    if not j.get("ok"):
        raise RuntimeError(f"Telegram send failed: {j}")


# -----------------------------
# Flags (LOCKED)
# -----------------------------

# Complete country-to-flag emoji mapping (195 UN member states + common territories)
COUNTRY_FLAG_MAP = {
    # Europe
    "albania": "🇦🇱",
    "andorra": "🇦🇩",
    "armenia": "🇦🇲",
    "austria": "🇦🇹",
    "azerbaijan": "🇦🇿",
    "belarus": "🇧🇾",
    "belgium": "🇧🇪",
    "bosnia and herzegovina": "🇧🇦",
    "bosnia": "🇧🇦",
    "bulgaria": "🇧🇬",
    "croatia": "🇭🇷",
    "cyprus": "🇨🇾",
    "czech republic": "🇨🇿",
    "czechia": "🇨🇿",
    "denmark": "🇩🇰",
    "estonia": "🇪🇪",
    "finland": "🇫🇮",
    "france": "🇫🇷",
    "georgia": "🇬🇪",
    "germany": "🇩🇪",
    "greece": "🇬🇷",
    "hungary": "🇭🇺",
    "iceland": "🇮🇸",
    "ireland": "🇮🇪",
    "italy": "🇮🇹",
    "kosovo": "🇽🇰",
    "latvia": "🇱🇻",
    "liechtenstein": "🇱🇮",
    "lithuania": "🇱🇹",
    "luxembourg": "🇱🇺",
    "malta": "🇲🇹",
    "moldova": "🇲🇩",
    "monaco": "🇲🇨",
    "montenegro": "🇲🇪",
    "netherlands": "🇳🇱",
    "north macedonia": "🇲🇰",
    "macedonia": "🇲🇰",
    "norway": "🇳🇴",
    "poland": "🇵🇱",
    "portugal": "🇵🇹",
    "romania": "🇷🇴",
    "russia": "🇷🇺",
    "russian federation": "🇷🇺",
    "san marino": "🇸🇲",
    "serbia": "🇷🇸",
    "slovakia": "🇸🇰",
    "slovenia": "🇸🇮",
    "spain": "🇪🇸",
    "sweden": "🇸🇪",
    "switzerland": "🇨🇭",
    "ukraine": "🇺🇦",
    "united kingdom": "🇬🇧",
    "uk": "🇬🇧",
    "great britain": "🇬🇧",
    "england": "🏴",
    "scotland": "🏴",
    "wales": "🏴",
    "northern ireland": "🇬🇧",
    "vatican city": "🇻🇦",

    # Asia
    "afghanistan": "🇦🇫",
    "bahrain": "🇧🇭",
    "bangladesh": "🇧🇩",
    "bhutan": "🇧🇹",
    "brunei": "🇧🇳",
    "cambodia": "🇰🇭",
    "china": "🇨🇳",
    "india": "🇮🇳",
    "indonesia": "🇮🇩",
    "iran": "🇮🇷",
    "iraq": "🇮🇶",
    "israel": "🇮🇱",
    "japan": "🇯🇵",
    "jordan": "🇯🇴",
    "kazakhstan": "🇰🇿",
    "kuwait": "🇰🇼",
    "kyrgyzstan": "🇰🇬",
    "laos": "🇱🇦",
    "lebanon": "🇱🇧",
    "malaysia": "🇲🇾",
    "maldives": "🇲🇻",
    "mongolia": "🇲🇳",
    "myanmar": "🇲🇲",
    "burma": "🇲🇲",
    "nepal": "🇳🇵",
    "north korea": "🇰🇵",
    "oman": "🇴🇲",
    "pakistan": "🇵🇰",
    "palestine": "🇵🇸",
    "philippines": "🇵🇭",
    "qatar": "🇶🇦",
    "saudi arabia": "🇸🇦",
    "singapore": "🇸🇬",
    "south korea": "🇰🇷",
    "korea": "🇰🇷",
    "sri lanka": "🇱🇰",
    "syria": "🇸🇾",
    "taiwan": "🇹🇼",
    "tajikistan": "🇹🇯",
    "thailand": "🇹🇭",
    "timor-leste": "🇹🇱",
    "east timor": "🇹🇱",
    "turkey": "🇹🇷",
    "turkmenistan": "🇹🇲",
    "united arab emirates": "🇦🇪",
    "uae": "🇦🇪",
    "uzbekistan": "🇺🇿",
    "vietnam": "🇻🇳",
    "yemen": "🇾🇪",

    # Africa
    "algeria": "🇩🇿",
    "angola": "🇦🇴",
    "benin": "🇧🇯",
    "botswana": "🇧🇼",
    "burkina faso": "🇧🇫",
    "burundi": "🇧🇮",
    "cameroon": "🇨🇲",
    "cape verde": "🇨🇻",
    "central african republic": "🇨🇫",
    "chad": "🇹🇩",
    "comoros": "🇰🇲",
    "congo": "🇨🇬",
    "democratic republic of the congo": "🇨🇩",
    "drc": "🇨🇩",
    "djibouti": "🇩🇯",
    "egypt": "🇪🇬",
    "equatorial guinea": "🇬🇶",
    "eritrea": "🇪🇷",
    "eswatini": "🇸🇿",
    "swaziland": "🇸🇿",
    "ethiopia": "🇪🇹",
    "gabon": "🇬🇦",
    "gambia": "🇬🇲",
    "ghana": "🇬🇭",
    "guinea": "🇬🇳",
    "guinea-bissau": "🇬🇼",
    "ivory coast": "🇨🇮",
    "côte d'ivoire": "🇨🇮",
    "kenya": "🇰🇪",
    "lesotho": "🇱🇸",
    "liberia": "🇱🇷",
    "libya": "🇱🇾",
    "madagascar": "🇲🇬",
    "malawi": "🇲🇼",
    "mali": "🇲🇱",
    "mauritania": "🇲🇷",
    "mauritius": "🇲🇺",
    "morocco": "🇲🇦",
    "mozambique": "🇲🇿",
    "namibia": "🇳🇦",
    "niger": "🇳🇪",
    "nigeria": "🇳🇬",
    "rwanda": "🇷🇼",
    "sao tome and principe": "🇸🇹",
    "senegal": "🇸🇳",
    "seychelles": "🇸🇨",
    "sierra leone": "🇸🇱",
    "somalia": "🇸🇴",
    "south africa": "🇿🇦",
    "south sudan": "🇸🇸",
    "sudan": "🇸🇩",
    "tanzania": "🇹🇿",
    "togo": "🇹🇬",
    "tunisia": "🇹🇳",
    "uganda": "🇺🇬",
    "zambia": "🇿🇲",
    "zimbabwe": "🇿🇼",

    # North America
    "antigua and barbuda": "🇦🇬",
    "bahamas": "🇧🇸",
    "barbados": "🇧🇧",
    "belize": "🇧🇿",
    "canada": "🇨🇦",
    "costa rica": "🇨🇷",
    "cuba": "🇨🇺",
    "dominica": "🇩🇲",
    "dominican republic": "🇩🇴",
    "el salvador": "🇸🇻",
    "grenada": "🇬🇩",
    "guatemala": "🇬🇹",
    "haiti": "🇭🇹",
    "honduras": "🇭🇳",
    "jamaica": "🇯🇲",
    "mexico": "🇲🇽",
    "nicaragua": "🇳🇮",
    "panama": "🇵🇦",
    "saint kitts and nevis": "🇰🇳",
    "saint lucia": "🇱🇨",
    "saint vincent and the grenadines": "🇻🇨",
    "trinidad and tobago": "🇹🇹",
    "united states": "🇺🇸",
    "united states of america": "🇺🇸",
    "usa": "🇺🇸",
    "us": "🇺🇸",
    "america": "🇺🇸",

    # South America
    "argentina": "🇦🇷",
    "bolivia": "🇧🇴",
    "brazil": "🇧🇷",
    "chile": "🇨🇱",
    "colombia": "🇨🇴",
    "ecuador": "🇪🇨",
    "guyana": "🇬🇾",
    "paraguay": "🇵🇾",
    "peru": "🇵🇪",
    "suriname": "🇸🇷",
    "uruguay": "🇺🇾",
    "venezuela": "🇻🇪",

    # Oceania
    "australia": "🇦🇺",
    "fiji": "🇫🇯",
    "kiribati": "🇰🇮",
    "marshall islands": "🇲🇭",
    "micronesia": "🇫🇲",
    "nauru": "🇳🇷",
    "new zealand": "🇳🇿",
    "palau": "🇵🇼",
    "papua new guinea": "🇵🇬",
    "samoa": "🇼🇸",
    "solomon islands": "🇸🇧",
    "tonga": "🇹🇴",
    "tuvalu": "🇹🇻",
    "vanuatu": "🇻🇺",

    # Common territories / dependencies (non-UN members)
    "hong kong": "🇭🇰",
    "macau": "🇲🇴",
    "puerto rico": "🇵🇷",
    "greenland": "🇬🇱",
    "faroe islands": "🇫🇴",
    "bermuda": "🇧🇲",
    "cayman islands": "🇰🇾",
    "french polynesia": "🇵🇫",
    "tahiti": "🇵🇫",
    "guam": "🇬🇺",
    "new caledonia": "🇳🇨",
    "aruba": "🇦🇼",
    "curaçao": "🇨🇼",
    "gibraltar": "🇬🇮",
    "isle of man": "🇮🇲",
    "jersey": "🇯🇪",
    "guernsey": "🇬🇬",
}

def get_country_flag(country_name: str) -> str:
    if not country_name:
        return ""
    normalized = country_name.lower().strip()
    return COUNTRY_FLAG_MAP.get(normalized, "")

def get_country_flag_with_fallback(country_name: str, fallback: str = "🌍") -> str:
    flag = get_country_flag(country_name)
    return flag if flag else fallback


# -----------------------------
# Formatting helpers
# -----------------------------

def safe_float(x: str) -> Optional[float]:
    try:
        return float(str(x).strip())
    except Exception:
        return None

def parse_iso_z(s: str) -> Optional[dt.datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1]
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None

def title_case_city(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    if len(s) == 3 and s.isalpha() and s.upper() == s:
        return s
    return " ".join([w[:1].upper() + w[1:].lower() for w in s.split()])

def fmt_price_gbp(p: str) -> str:
    p = (p or "").strip()
    if not p:
        return "£?"
    try:
        v = float(p)
        return f"£{v:,.2f}"
    except Exception:
        return p if p.startswith("£") else f"£{p}"

def pick_first_present(row: Dict[str, str], keys: List[str]) -> str:
    for k in keys:
        v = (row.get(k, "") or "").strip()
        if v:
            return v
    return ""


# -----------------------------
# PHRASE BANK loader + picker (LOCKED)
# -----------------------------

def _truthy(x: Any) -> bool:
    if x is True:
        return True
    if x is False or x is None:
        return False
    return str(x).strip().lower() in ("true", "yes", "1", "y", "approved")

def load_phrase_bank(sh: gspread.Spreadsheet) -> List[Dict[str, str]]:
    try:
        ws = sh.worksheet("PHRASE_BANK")
    except Exception:
        return []
    vals = ws.get_all_values()
    if len(vals) < 2:
        return []
    headers = [h.strip() for h in vals[0]]
    idx = {h: i for i, h in enumerate(headers)}
    out: List[Dict[str, str]] = []
    for r in vals[1:]:
        d: Dict[str, str] = {}
        for h in headers:
            i = idx[h]
            d[h] = (r[i] if i < len(r) else "").strip()
        if any(d.values()):
            out.append(d)
    return out

def _pick_from_pool(pool: List[Dict[str, str]], deal_id: str) -> str:
    if not pool:
        return ""
    h = hashlib.md5((deal_id or "x").encode()).hexdigest()
    return (pool[int(h[:8], 16) % len(pool)].get("phrase", "") or "").strip()

def pick_phrase(bank: List[Dict[str, str]], theme: str, deal_id: str) -> str:
    theme_u = (theme or "").strip().upper()
    themed = [
        r for r in bank
        if (r.get("phrase") or "").strip()
        and _truthy(r.get("approved", ""))
        and (r.get("theme") or "").strip().upper() == theme_u
    ]
    chosen = _pick_from_pool(themed, deal_id)
    if chosen:
        return chosen

    any_ok = [
        r for r in bank
        if (r.get("phrase") or "").strip()
        and _truthy(r.get("approved", ""))
    ]
    return _pick_from_pool(any_ok, deal_id)


# -----------------------------
# Message builders (LOCKED OUTPUT)
# -----------------------------

def build_vip_message(row: Dict[str, str]) -> str:
    price = fmt_price_gbp(row.get("price_gbp", ""))
    country = (row.get("destination_country", "") or "").strip()
    flag = get_country_flag_with_fallback(country, fallback="🌍")

    dest_city_raw = (row.get("destination_city", "") or "").strip() or (row.get("destination_iata", "") or "").strip()
    origin_city_raw = (row.get("origin_city", "") or "").strip() or (row.get("origin_iata", "") or "").strip()

    dest_upper = dest_city_raw.upper()
    origin_title = title_case_city(origin_city_raw)

    out_d = (row.get("outbound_date", "") or "").strip()
    ret_d = (row.get("return_date", "") or "").strip()

    phrase = (row.get("phrase_bank", "") or "").strip()
    booking = clean_url(pick_first_present(row, ["booking_link_vip", "deeplink", "affiliate_url"]))

    lines: List[str] = []
    lines.append(f"{price} to {country} {flag}".strip())
    lines.append(f"TO: {dest_upper}")
    lines.append(f"FROM: {origin_title}")
    lines.append(f"OUT:  {out_d}")
    lines.append(f"BACK: {ret_d}")

    if phrase:
        lines.append("")
        lines.append(phrase)

    if booking:
        lines.append("")
        lines.append(f'<a href="{booking}">BOOKING LINK</a>')

    return "\n".join(lines).strip()

def build_free_message(row: Dict[str, str], stripe_monthly: str, stripe_yearly: str) -> str:
    price = fmt_price_gbp(row.get("price_gbp", ""))
    country = (row.get("destination_country", "") or "").strip()
    flag = get_country_flag_with_fallback(country, fallback="🌍")

    dest_city_raw = (row.get("destination_city", "") or "").strip() or (row.get("destination_iata", "") or "").strip()
    origin_city_raw = (row.get("origin_city", "") or "").strip() or (row.get("origin_iata", "") or "").strip()

    dest_upper = dest_city_raw.upper()
    origin_title = title_case_city(origin_city_raw)

    out_d = (row.get("outbound_date", "") or "").strip()
    ret_d = (row.get("return_date", "") or "").strip()

    phrase = (row.get("phrase_bank", "") or "").strip()

    m = clean_url(stripe_monthly)
    y = clean_url(stripe_yearly)

    lines: List[str] = []
    lines.append(f"{price} to {country} {flag}".strip())
    lines.append(f"TO: {dest_upper}")
    lines.append(f"FROM: {origin_title}")
    lines.append(f"OUT:  {out_d}")
    lines.append(f"BACK: {ret_d}")

    if phrase:
        lines.append("")
        lines.append(phrase)

    lines.append("")
    lines.append("Want instant access?")
    lines.append("Join TravelTxter for early access")
    lines.append("")
    lines.append("* VIP members saw this 24 hours ago")
    lines.append("* Direct booking links")
    lines.append("* We find exclusive mistake fares")
    lines.append("* Subscription: £3 p/m or £30 p/a")

    links: List[str] = []
    if m:
        links.append(f'<a href="{m}">Upgrade now (Monthly)</a>')
    if y:
        links.append(f'<a href="{y}">Upgrade now (Annual)</a>')
    if links:
        lines.append("")
        lines.extend(links)

    return "\n".join(lines).strip()


# -----------------------------
# Best row selection
# -----------------------------

def row_dict(headers: List[str], vals: List[str]) -> Dict[str, str]:
    return {headers[i]: (vals[i] if i < len(vals) else "") for i in range(len(headers))}

def rank_key(rownum: int, row: Dict[str, str]) -> Tuple[float, dt.datetime, dt.datetime, int]:
    ds = safe_float(row.get("deal_score", "")) or 0.0
    scored = parse_iso_z(row.get("scored_timestamp", "")) or dt.datetime(1970, 1, 1)
    created = (
        parse_iso_z(row.get("created_at", "")) or
        parse_iso_z(row.get("timestamp", "")) or
        dt.datetime(1970, 1, 1)
    )
    return (ds, scored, created, rownum)

def pick_best_eligible(
    headers: List[str],
    rows: List[List[str]],
    h: Dict[str, int],
    mode: str,
    consume_status: str,
    ts_col: str,
    free_delay_hours: int = 24,
) -> Optional[Tuple[int, Dict[str, str]]]:
    now = utcnow()
    eligible: List[Tuple[int, Dict[str, str]]] = []

    for rownum in range(2, len(rows) + 2):
        vals = rows[rownum - 2]
        status = (vals[h["status"]] if h["status"] < len(vals) else "").strip().upper()
        if status != consume_status:
            continue

        already = (vals[h[ts_col]] if h[ts_col] < len(vals) else "").strip()
        if already:
            continue

        if mode == "FREE":
            vip_ts = (vals[h["posted_telegram_vip_at"]] if h["posted_telegram_vip_at"] < len(vals) else "").strip()
            vip_dt = parse_iso_z(vip_ts)
            if not vip_dt:
                continue
            if (now - vip_dt) < dt.timedelta(hours=free_delay_hours):
                continue

        eligible.append((rownum, row_dict(headers, vals)))

    if not eligible:
        return None

    eligible.sort(key=lambda it: rank_key(it[0], it[1]), reverse=True)
    return eligible[0]


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    run_slot = env_str("RUN_SLOT", "VIP").upper().strip()
    mode = "FREE" if run_slot in ("FREE", "PM") else "VIP"

    spreadsheet_id = env_any(["SPREADSHEET_ID", "SHEET_ID"])
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")

    stripe_monthly = env_any(["STRIPE_MONTHLY_LINK", "STRIPE_LINK_MONTHLY"], "")
    stripe_yearly = env_any(["STRIPE_YEARLY_LINK", "STRIPE_LINK_YEARLY"], "")

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID (or SHEET_ID)")

    bot_vip = env_any(["TELEGRAM_BOT_TOKEN_VIP", "TG_BOT_TOKEN_VIP"])
    chan_vip = env_any(["TELEGRAM_CHANNEL_VIP", "TG_CHANNEL_VIP"])
    bot_free = env_any(["TELEGRAM_BOT_TOKEN", "TG_BOT_TOKEN"])
    chan_free = env_any(["TELEGRAM_CHANNEL", "TG_CHANNEL"])

    if mode == "VIP":
        if not bot_vip or not chan_vip:
            raise RuntimeError("Missing TELEGRAM_BOT_TOKEN_VIP or TELEGRAM_CHANNEL_VIP")
        bot_token, chat_id = bot_vip, chan_vip
        consume_status = "POSTED_INSTAGRAM"
        promote_status = "POSTED_TELEGRAM_VIP"
        ts_col = "posted_telegram_vip_at"
    else:
        if not bot_free or not chan_free:
            raise RuntimeError("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHANNEL")
        bot_token, chat_id = bot_free, chan_free
        consume_status = "POSTED_TELEGRAM_VIP"
        promote_status = "POSTED_ALL"
        ts_col = "posted_telegram_free_at"

    gc = gs_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(tab)

    required = [
        "status","deal_id","deal_theme","theme","price_gbp","origin_city","destination_city",
        "origin_iata","destination_iata","destination_country","outbound_date","return_date",
        "booking_link_vip","deeplink","affiliate_url","phrase_bank","deal_score","scored_timestamp",
        "timestamp","created_at","posted_telegram_vip_at","posted_telegram_free_at","posted_instagram_at",
    ]
    ensure_columns(ws, required)

    all_vals = ws.get_all_values()
    if len(all_vals) < 2:
        log("No rows.")
        return 0

    headers = [h.strip() for h in all_vals[0]]
    rows = all_vals[1:]
    h = {name: i for i, name in enumerate(headers)}

    picked = pick_best_eligible(headers, rows, h, mode, consume_status, ts_col, free_delay_hours=24)
    if not picked:
        log(f"Done. Telegram posted 0. (No rows eligible for mode={mode} status={consume_status})")
        return 0

    rownum, row = picked
    deal_id = (row.get("deal_id") or "").strip()
    theme = pick_first_present(row, ["deal_theme", "theme"])

    phrase = (row.get("phrase_bank") or "").strip()
    if not phrase:
        bank = load_phrase_bank(sh)
        chosen = pick_phrase(bank, theme, deal_id)
        if chosen:
            ws.update([[chosen]], gspread.utils.rowcol_to_a1(rownum, h["phrase_bank"] + 1))
            row["phrase_bank"] = chosen
            log(f"🧩 Filled phrase_bank for row {rownum} (theme={theme or 'UNKNOWN'})")
        else:
            log(f"⚠️ No approved phrases found in PHRASE_BANK (theme={theme or 'UNKNOWN'})")

    log(f"📨 Telegram best-pick row {rownum} deal_id={deal_id} MODE={mode} RUN_SLOT={run_slot}")

    msg = build_vip_message(row) if mode == "VIP" else build_free_message(row, stripe_monthly, stripe_yearly)
    tg_send(bot_token, chat_id, msg)

    ws.batch_update(
        [
            {"range": gspread.utils.rowcol_to_a1(rownum, h[ts_col] + 1), "values": [[iso_now()]]},
            {"range": gspread.utils.rowcol_to_a1(rownum, h["status"] + 1), "values": [[promote_status]]},
        ],
        value_input_option="USER_ENTERED",
    )

    log(f"✅ Telegram posted row {rownum} -> {promote_status}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
