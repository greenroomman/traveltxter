#!/usr/bin/env python3
"""
TravelTxter V4.5x — telegram_publisher.py (AM = FREE after 24h, then VIP)

AM (RUN_SLOT=AM):
  1) FREE catch-up:
     consumes: status == POSTED_TELEGRAM_VIP AND (now - posted_telegram_vip_at) >= FREE_DELAY_HOURS (default 24)
     writes:   posted_telegram_free_at
     promotes: POSTED_TELEGRAM_VIP -> POSTED_ALL

  2) VIP today:
     consumes: status == POSTED_INSTAGRAM
     writes:   posted_telegram_vip_at
     promotes: POSTED_INSTAGRAM -> POSTED_TELEGRAM_VIP

PM (RUN_SLOT=PM):
  No-op (exit cleanly).  Free is handled next-day AM by design.
"""

from __future__ import annotations

import os
import json
import hashlib
import datetime as dt
from typing import Any, Dict, List, Optional

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

def env_int(k: str, default: int) -> int:
    try:
        return int(env_str(k, str(default)))
    except Exception:
        return default

def clean_url(u: str) -> str:
    return (u or "").strip().replace(" ", "")


# -----------------------------
# Time helpers
# -----------------------------

def now_utc() -> dt.datetime:
    return dt.datetime.utcnow().replace(microsecond=0)

def parse_iso_utc(s: str) -> Optional[dt.datetime]:
    """
    Accepts:
      2026-01-07T07:53:14Z
      2026-01-07T07:53:14.123Z
    Returns naive UTC datetime.
    """
    t = (s or "").strip()
    if not t:
        return None
    try:
        t = t.replace("Z", "")
        return dt.datetime.fromisoformat(t)
    except Exception:
        return None


# -----------------------------
# HTML helpers (Telegram hyperlinks)
# -----------------------------

def html_escape(s: str) -> str:
    s = s or ""
    return (
        s.replace("&", "&amp;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
         .replace('"', "&quot;")
    )

def html_link(url: str, label: str) -> str:
    u = clean_url(url)
    if not u:
        return ""
    return f'<a href="{html_escape(u)}">{html_escape(label)}</a>'


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
    raw = env_str("GCP_SA_JSON_ONE_LINE")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE")
    info = _parse_sa_json(raw)
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds)

def ensure_columns(ws, required_cols: List[str]) -> Dict[str, int]:
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

def tg_send(bot_token: str, chat_id: str, text_html: str) -> None:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    r = requests.post(
        url,
        json={
            "chat_id": chat_id,
            "text": text_html,
            "parse_mode": "HTML",
            "disable_web_page_preview": False,
        },
        timeout=30,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"Telegram send failed HTTP {r.status_code}: {r.text[:250]}")


# -----------------------------
# Formatting helpers
# -----------------------------

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
}

def country_flag(country: str) -> str:
    return FLAG_MAP.get((country or "").strip().upper(), "")

def fmt_price_gbp(x: str) -> str:
    s = (x or "").strip().replace(",", "").replace("£", "")
    if not s:
        return ""
    try:
        v = float(s)
        if v.is_integer():
            return f"£{int(v)}"
        return f"£{v:.2f}"
    except Exception:
        return f"£{s}"

def normalize_origin_city(x: str) -> str:
    s = (x or "").strip()
    u = s.upper()
    UK_AIRPORT_CITY_FALLBACK = {
        "LHR": "London", "LGW": "London", "STN": "London", "LTN": "London",
        "LCY": "London", "SEN": "London",
        "MAN": "Manchester",
        "BRS": "Bristol",
        "BHX": "Birmingham",
        "EDI": "Edinburgh",
        "GLA": "Glasgow",
        "NCL": "Newcastle",
        "LPL": "Liverpool",
        "NQY": "Newquay",
        "SOU": "Southampton",
        "CWL": "Cardiff",
        "EXT": "Exeter",
    }
    return UK_AIRPORT_CITY_FALLBACK.get(u, s)


# -----------------------------
# Phrase bank loader (your schema)
# theme, category, phrase, approved, channel_hint, max_per_month, notes
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

def pick_theme_phrase(pb_rows: List[Dict[str, str]], deal_theme: str, deal_id: str) -> str:
    th = (deal_theme or "").strip().upper()

    approved = [
        r for r in pb_rows
        if _truthy(r.get("approved", "")) and (r.get("phrase", "").strip() != "")
    ]
    if not approved:
        return ""

    themed = [r for r in approved if (r.get("theme", "").strip().upper() == th)] if th else []
    pool = themed if themed else approved

    key = (deal_id or "no_deal_id").encode("utf-8")
    h = hashlib.md5(key).hexdigest()
    idx = int(h[:8], 16) % len(pool)
    return (pool[idx].get("phrase", "") or "").strip()


# -----------------------------
# Definitive templates
# -----------------------------

def pick_booking_link(row: Dict[str, str]) -> str:
    vip = (row.get("booking_link_vip", "") or "").strip()
    aff = (row.get("affiliate_url", "") or "").strip()
    return clean_url(vip or aff)

def build_vip(row: Dict[str, str], phrase: str, booking_url: str) -> str:
    price = fmt_price_gbp(row.get("price_gbp", ""))
    country = (row.get("destination_country", "") or "").strip()
    flag = country_flag(country)

    dest_city = (row.get("destination_city", "") or "").strip() or (row.get("destination_iata", "") or "").strip()
    origin_city = normalize_origin_city((row.get("origin_city", "") or "").strip() or (row.get("origin_iata", "") or "").strip())
    out_d = (row.get("outbound_date", "") or "").strip()
    back_d = (row.get("return_date", "") or "").strip()

    headline = f"{price} to {country}{(' ' + flag) if flag else ''}".strip()

    lines: List[str] = [
        html_escape(headline),
        f"To: {html_escape(dest_city)}",
        f"From: {html_escape(origin_city)}",
        f"Out: {html_escape(out_d)}",
        f"Back: {html_escape(back_d)}",
        "",
    ]
    if phrase:
        lines.append(html_escape(phrase))
        lines.append("")

    if booking_url:
        lines.append(html_link(booking_url, "BOOKING LINK"))
    else:
        lines.append("BOOKING LINK: (missing)")

    return "\n".join(lines).strip()

def build_free(row: Dict[str, str], phrase: str, monthly_link: str, yearly_link: str) -> str:
    price = fmt_price_gbp(row.get("price_gbp", ""))
    country = (row.get("destination_country", "") or "").strip()
    flag = country_flag(country)

    dest_city = (row.get("destination_city", "") or "").strip() or (row.get("destination_iata", "") or "").strip()
    origin_city = normalize_origin_city((row.get("origin_city", "") or "").strip() or (row.get("origin_iata", "") or "").strip())
    out_d = (row.get("outbound_date", "") or "").strip()
    back_d = (row.get("return_date", "") or "").strip()

    headline = f"{price} to {country}{(' ' + flag) if flag else ''}".strip()

    lines: List[str] = [
        html_escape(headline),
        f"To: {html_escape(dest_city)}",
        f"From: {html_escape(origin_city)}",
        f"Out: {html_escape(out_d)}",
        f"Back: {html_escape(back_d)}",
        "",
    ]
    if phrase:
        lines.append(html_escape(phrase))
        lines.append("")

    lines += [
        "Want instant access?",
        "Join TravelTxter for early access",
        "",
        "* VIP members saw this 24 hours ago",
        "* Deals 24 hours early",
        "* Direct booking links",
        "* Exclusive mistake fares",
        "* £3 p/m or £30 p/a",
        "* Cancel anytime",
        "",
    ]

    upsell_bits = []
    if monthly_link:
        upsell_bits.append(html_link(monthly_link, "Upgrade now (Monthly)"))
    if yearly_link:
        upsell_bits.append(html_link(yearly_link, "Upgrade now (Yearly)"))
    if upsell_bits:
        lines.append(" | ".join(upsell_bits))

    return "\n".join(lines).strip()


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    run_slot = env_str("RUN_SLOT", "AM").upper()

    # PM no-op by design (FREE is next-day AM)
    if run_slot == "PM":
        log("Telegram PM slot: no-op (FREE posts are handled next-day AM).")
        return 0

    spreadsheet_id = env_str("SPREADSHEET_ID")
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")

    # Delay gate for FREE catch-up
    free_delay_hours = env_int("FREE_DELAY_HOURS", 24)

    # Tokens/channels
    bot_vip = env_str("TELEGRAM_BOT_TOKEN_VIP")
    chan_vip = env_str("TELEGRAM_CHANNEL_VIP")
    bot_free = env_str("TELEGRAM_BOT_TOKEN")
    chan_free = env_str("TELEGRAM_CHANNEL")

    if not bot_vip or not chan_vip:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN_VIP or TELEGRAM_CHANNEL_VIP")
    if not bot_free or not chan_free:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHANNEL")

    stripe_monthly = env_str("STRIPE_MONTHLY_LINK")
    stripe_yearly = env_str("STRIPE_YEARLY_LINK")

    gc = gs_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(tab)

    required = [
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
        "affiliate_url",
        "booking_link_vip",
        "deal_theme",
        "posted_telegram_vip_at",
        "posted_telegram_free_at",
    ]
    h = ensure_columns(ws, required)

    values = ws.get_all_values()
    if len(values) < 2:
        log("No rows.")
        return 0

    headers = values[0]
    rows = values[1:]

    def row_dict(vals: List[str]) -> Dict[str, str]:
        d: Dict[str, str] = {}
        for k, idx in h.items():
            d[k] = vals[idx].strip() if idx < len(vals) else ""
        return d

    pb = load_phrase_bank(sh)
    now = now_utc()

    # ------------------------------------------------
    # 1) FREE catch-up (POSTED_TELEGRAM_VIP older than 24h)
    # ------------------------------------------------
    for rownum, vals in enumerate(rows, start=2):
        status = (vals[h["status"]] if h["status"] < len(vals) else "").strip()
        posted_free = (vals[h["posted_telegram_free_at"]] if h["posted_telegram_free_at"] < len(vals) else "").strip()

        if status != "POSTED_TELEGRAM_VIP" or posted_free:
            continue

        vip_ts = (vals[h["posted_telegram_vip_at"]] if h["posted_telegram_vip_at"] < len(vals) else "").strip()
        vip_dt = parse_iso_utc(vip_ts)
        if not vip_dt:
            log(f"⏭️  FREE skip row {rownum}: missing/invalid posted_telegram_vip_at")
            continue

        age_hours = (now - vip_dt).total_seconds() / 3600.0
        if age_hours < float(free_delay_hours):
            log(f"⏭️  FREE skip row {rownum}: VIP age {age_hours:.1f}h < {free_delay_hours}h")
            continue

        row = row_dict(vals)
        phrase = pick_theme_phrase(pb, row.get("deal_theme", ""), row.get("deal_id", ""))
        msg = build_free(row, phrase=phrase, monthly_link=stripe_monthly, yearly_link=stripe_yearly)

        tg_send(bot_free, chan_free, msg)

        ws.update_cell(rownum, h["posted_telegram_free_at"] + 1, now.isoformat() + "Z")
        ws.update_cell(rownum, h["status"] + 1, "POSTED_ALL")

        log(f"✅ FREE posted row {rownum} -> POSTED_ALL")
        break  # max 1 per run

    # ------------------------------------------------
    # 2) VIP today (POSTED_INSTAGRAM)
    # ------------------------------------------------
    for rownum, vals in enumerate(rows, start=2):
        status = (vals[h["status"]] if h["status"] < len(vals) else "").strip()
        posted_vip = (vals[h["posted_telegram_vip_at"]] if h["posted_telegram_vip_at"] < len(vals) else "").strip()

        if status != "POSTED_INSTAGRAM" or posted_vip:
            continue

        row = row_dict(vals)
        phrase = pick_theme_phrase(pb, row.get("deal_theme", ""), row.get("deal_id", ""))
        booking = pick_booking_link(row)
        msg = build_vip(row, phrase=phrase, booking_url=booking)

        tg_send(bot_vip, chan_vip, msg)

        ws.update_cell(rownum, h["posted_telegram_vip_at"] + 1, now.isoformat() + "Z")
        ws.update_cell(rownum, h["status"] + 1, "POSTED_TELEGRAM_VIP")

        log(f"✅ VIP posted row {rownum} -> POSTED_TELEGRAM_VIP")
        break  # max 1 per run

    log("Done. Telegram AM slot complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
