#!/usr/bin/env python3
"""
TravelTxter V4.5x — telegram_publisher.py (LOCKED)

AM (RUN_SLOT=AM):
  consumes: status == POSTED_INSTAGRAM
  writes:   posted_telegram_vip_at
  promotes: POSTED_INSTAGRAM -> POSTED_TELEGRAM_VIP

PM (RUN_SLOT=PM):
  consumes: status == POSTED_TELEGRAM_VIP AND posted_telegram_vip_at <= now-24h
  writes:   posted_telegram_free_at
  promotes: POSTED_TELEGRAM_VIP -> POSTED_ALL

Key rules:
- NO weird trailing line: disable_web_page_preview=True
- Headline destination is Title Case (Budapest), but TO: stays UPPER (BUDAPEST)
- Uses Telegram HTML hyperlinks for booking + upgrades
"""

from __future__ import annotations

import os
import json
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
# Env
# -----------------------------

def env_str(k: str, default: str = "") -> str:
    return (os.environ.get(k, default) or "").strip()

def clean_url(u: str) -> str:
    return (u or "").strip().replace(" ", "")


# -----------------------------
# Sheets auth
# -----------------------------

def _parse_sa_json(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return json.loads(raw.replace("\\n", "\n"))

def gs_client():
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
# Telegram
# -----------------------------

def tg_send(bot_token: str, chat_id: str, html_text: str) -> None:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    r = requests.post(
        url,
        json={
            "chat_id": chat_id,
            "text": html_text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,   # <- kills the weird preview line
        },
        timeout=30,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"Telegram send failed HTTP {r.status_code}: {r.text[:200]}")


# -----------------------------
# Copy helpers
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
    "HUNGARY": "🇭🇺",
    "CANADA": "🇨🇦",
}

def country_flag(country: str) -> str:
    c = (country or "").strip().upper()
    return FLAG_MAP.get(c, "")

def title_case_city(x: str) -> str:
    s = (x or "").strip()
    if not s:
        return ""
    # Keep hyphens and accents reasonably intact
    return " ".join([w[:1].upper() + w[1:].lower() if w else "" for w in s.split(" ")]).strip()


def build_vip_message(row: Dict[str, str]) -> str:
    price = (row.get("price_gbp", "") or "").strip().replace("£", "")
    country = (row.get("destination_country", "") or "").strip()
    flag = country_flag(country)

    dest_city_raw = (row.get("destination_city", "") or "").strip() or (row.get("destination_iata", "") or "").strip()
    origin_city_raw = (row.get("origin_city", "") or "").strip() or (row.get("origin_iata", "") or "").strip()

    dest_headline = title_case_city(dest_city_raw)
    dest_upper = dest_city_raw.upper()
    origin_title = title_case_city(origin_city_raw)

    out_d = (row.get("outbound_date", "") or "").strip()
    ret_d = (row.get("return_date", "") or "").strip()

    phrase = (row.get("phrase_bank", "") or "").strip()
    booking = clean_url(row.get("booking_link_vip", "") or row.get("affiliate_url", "") or "")

    # VIP template (your spec)
    lines = []
    lines.append(f"£{price} to {country}{(' ' + flag) if flag else ''}".strip())
    lines.append(f"TO: {dest_upper}")
    lines.append(f"FROM: {origin_title}")
    lines.append(f"OUT:  {out_d}")
    lines.append(f"BACK: {ret_d}")
    lines.append("")
    if phrase:
        lines.append(phrase)
        lines.append("")
    if booking:
        lines.append(f'<a href="{booking}">BOOKING LINK</a>')
    return "\n".join([ln for ln in lines if ln is not None]).strip()


def build_free_message(row: Dict[str, str], stripe_monthly: str, stripe_yearly: str) -> str:
    price = (row.get("price_gbp", "") or "").strip().replace("£", "")
    country = (row.get("destination_country", "") or "").strip()
    flag = country_flag(country)

    dest_city_raw = (row.get("destination_city", "") or "").strip() or (row.get("destination_iata", "") or "").strip()
    origin_city_raw = (row.get("origin_city", "") or "").strip() or (row.get("origin_iata", "") or "").strip()

    dest_upper = dest_city_raw.upper()
    origin_title = title_case_city(origin_city_raw)

    out_d = (row.get("outbound_date", "") or "").strip()
    ret_d = (row.get("return_date", "") or "").strip()

    phrase = (row.get("phrase_bank", "") or "").strip()

    m = clean_url(stripe_monthly)
    y = clean_url(stripe_yearly)

    lines = []
    lines.append(f"£{price} to {country}{(' ' + flag) if flag else ''}".strip())
    lines.append(f"TO: {dest_upper}")
    lines.append(f"FROM: {origin_title}")
    lines.append(f"OUT:  {out_d}")
    lines.append(f"BACK: {ret_d}")
    lines.append("")
    if phrase:
        lines.append(phrase)
        lines.append("")
    lines.append("Want instant access?")
    lines.append("Join TravelTxter for early access")
    lines.append("")
    lines.append("• VIP members saw this 24 hours ago")
    lines.append("• Deals 24 hours early")
    lines.append("• Direct booking links")
    lines.append("• Exclusive mistake fares")
    lines.append("• £3 p/m or £30 p/a")
    lines.append("• Cancel anytime")
    lines.append("")
    # Hyperlinks (only include if present)
    if m:
        lines.append(f'<a href="{m}">Upgrade now (Monthly)</a>')
    if y:
        lines.append(f'<a href="{y}">Upgrade now (Yearly)</a>')
    return "\n".join(lines).strip()


def parse_iso_z(s: str) -> Optional[dt.datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        # "2026-01-07T07:53:14Z" or without Z
        if s.endswith("Z"):
            s = s[:-1]
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    run_slot = env_str("RUN_SLOT", "AM").upper()
    spreadsheet_id = env_str("SPREADSHEET_ID")
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")

    stripe_monthly = env_str("STRIPE_MONTHLY_LINK")
    stripe_yearly = env_str("STRIPE_YEARLY_LINK")

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")

    # Tokens/channels
    bot_vip = env_str("TELEGRAM_BOT_TOKEN_VIP")
    chan_vip = env_str("TELEGRAM_CHANNEL_VIP")
    bot_free = env_str("TELEGRAM_BOT_TOKEN")
    chan_free = env_str("TELEGRAM_CHANNEL")

    if run_slot == "AM" and (not bot_vip or not chan_vip):
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN_VIP or TELEGRAM_CHANNEL_VIP")
    if run_slot == "PM" and (not bot_free or not chan_free):
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHANNEL")

    gc = gs_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(tab)

    required = [
        "status",
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
        "phrase_bank",
        "posted_instagram_at",
        "posted_telegram_vip_at",
        "posted_telegram_free_at",
    ]
    h = ensure_columns(ws, required)

    rows = ws.get_all_values()
    if len(rows) < 2:
        log("No rows.")
        return 0

    now_dt = dt.datetime.utcnow().replace(microsecond=0)
    now_z = now_dt.isoformat() + "Z"

    if run_slot == "AM":
        consume_status = "POSTED_INSTAGRAM"
        promote_status = "POSTED_TELEGRAM_VIP"
        ts_col = "posted_telegram_vip_at"
    else:
        consume_status = "POSTED_TELEGRAM_VIP"
        promote_status = "POSTED_ALL"
        ts_col = "posted_telegram_free_at"

    # Find first eligible row (deterministic)
    target_row_idx = None
    target_row_vals = None

    for i in range(2, len(rows) + 1):
        vals = rows[i - 1]
        status = (vals[h["status"]] if h["status"] < len(vals) else "").strip()

        # already posted in this slot?
        ts_val = (vals[h[ts_col]] if h[ts_col] < len(vals) else "").strip()
        if status != consume_status or ts_val:
            continue

        # PM gate: VIP must be >= 24h ago
        if run_slot == "PM":
            vip_ts = (vals[h["posted_telegram_vip_at"]] if h["posted_telegram_vip_at"] < len(vals) else "").strip()
            vip_dt = parse_iso_z(vip_ts)
            if not vip_dt:
                continue
            if (now_dt - vip_dt) < dt.timedelta(hours=24):
                continue

        target_row_idx = i
        target_row_vals = vals
        break

    if not target_row_idx:
        if run_slot == "PM":
            log("Done. Telegram posted 0. (No rows eligible for FREE: status=POSTED_TELEGRAM_VIP and VIP>= 24h ago)")
        else:
            log(f"Done. Telegram posted 0. (No rows with status={consume_status})")
        return 0

    # Build row dict
    row: Dict[str, str] = {}
    for col, idx in h.items():
        row[col] = target_row_vals[idx] if idx < len(target_row_vals) else ""

    # Send
    if run_slot == "AM":
        msg = build_vip_message(row)
        tg_send(bot_vip, chan_vip, msg)
    else:
        msg = build_free_message(row, stripe_monthly=stripe_monthly, stripe_yearly=stripe_yearly)
        tg_send(bot_free, chan_free, msg)

    # Write back
    ws.update_cell(target_row_idx, h[ts_col] + 1, now_z)
    ws.update_cell(target_row_idx, h["status"] + 1, promote_status)

    log(f"✅ Telegram posted 1. Row {target_row_idx} -> {promote_status}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
