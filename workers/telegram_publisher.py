#!/usr/bin/env python3
"""
TravelTxter V4.5x — telegram_publisher.py (LOCKED + BEST PICK)

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

def gs_client():
    raw = env_any(["GCP_SA_JSON_ONE_LINE", "GCP_SA_JSON"])
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
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

def tg_send(bot_token: str, chat_id: str, html_text: str) -> None:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    r = requests.post(
        url,
        json={
            "chat_id": chat_id,
            "text": html_text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        },
        timeout=30,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"Telegram send failed HTTP {r.status_code}: {r.text[:300]}")


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
    "HUNGARY": "🇭🇺",
    "CANADA": "🇨🇦",
    "MEXICO": "🇲🇽",
}

def country_flag(country: str) -> str:
    c = (country or "").strip().upper()
    return FLAG_MAP.get(c, "")

def title_case_city(x: str) -> str:
    s = (x or "").strip()
    if not s:
        return ""
    return " ".join([w[:1].upper() + w[1:].lower() if w else "" for w in s.split(" ")]).strip()

def safe_float(s: str) -> Optional[float]:
    try:
        x = (s or "").strip().replace("£", "").replace(",", "")
        return float(x) if x else None
    except Exception:
        return None

def fmt_price_gbp(price_gbp: str) -> str:
    p = (price_gbp or "").strip()
    if not p:
        return ""
    if p.startswith("£"):
        p = p[1:]
    f = safe_float(p)
    if f is None:
        return f"£{p}"
    return f"£{f:.2f}"

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


def build_vip_message(row: Dict[str, str]) -> str:
    price = fmt_price_gbp(row.get("price_gbp", ""))
    country = (row.get("destination_country", "") or "").strip()
    flag = country_flag(country)

    dest_city_raw = (row.get("destination_city", "") or "").strip() or (row.get("destination_iata", "") or "").strip()
    origin_city_raw = (row.get("origin_city", "") or "").strip() or (row.get("origin_iata", "") or "").strip()

    dest_upper = dest_city_raw.upper()
    origin_title = title_case_city(origin_city_raw)

    out_d = (row.get("outbound_date", "") or "").strip()
    ret_d = (row.get("return_date", "") or "").strip()

    phrase = (row.get("phrase_bank", "") or row.get("why_good", "") or row.get("ai_notes", "") or "").strip()
    booking = clean_url(row.get("booking_link_vip", "") or row.get("affiliate_url", "") or "")

    lines = []
    lines.append(f"{price} to {country}{(' ' + flag) if flag else ''}".strip())
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
    flag = country_flag(country)

    dest_city_raw = (row.get("destination_city", "") or "").strip() or (row.get("destination_iata", "") or "").strip()
    origin_city_raw = (row.get("origin_city", "") or "").strip() or (row.get("origin_iata", "") or "").strip()

    dest_upper = dest_city_raw.upper()
    origin_title = title_case_city(origin_city_raw)

    out_d = (row.get("outbound_date", "") or "").strip()
    ret_d = (row.get("return_date", "") or "").strip()

    phrase = (row.get("phrase_bank", "") or row.get("why_good", "") or row.get("ai_notes", "") or "").strip()

    m = clean_url(stripe_monthly)
    y = clean_url(stripe_yearly)

    lines = []
    lines.append(f"{price} to {country}{(' ' + flag) if flag else ''}".strip())
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
    lines.append("• VIP members saw this 24 hours ago")
    lines.append("• Deals 24 hours early")
    lines.append("• Direct booking links")
    lines.append("• Exclusive mistake fares")
    lines.append("• £3 p/m or £30 p/a")
    lines.append("• Cancel anytime")

    links = []
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
    d: Dict[str, str] = {}
    for i, h in enumerate(headers):
        d[h] = (vals[i] if i < len(vals) else "") or ""
    return d

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
    run_slot: str,
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

        if run_slot == "PM":
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
    run_slot = env_str("RUN_SLOT", "AM").upper().strip()
    if run_slot not in ("AM", "PM"):
        run_slot = "AM"

    spreadsheet_id = env_any(["SPREADSHEET_ID", "SHEET_ID"])
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")

    stripe_monthly = env_any(["STRIPE_MONTHLY_LINK", "STRIPE_LINK_MONTHLY"], "")
    stripe_yearly = env_any(["STRIPE_YEARLY_LINK", "STRIPE_LINK_YEARLY"], "")

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID (or SHEET_ID)")

    # Accept a couple of aliases as fallback (in case secrets were named differently)
    bot_vip = env_any(["TELEGRAM_BOT_TOKEN_VIP", "TG_BOT_TOKEN_VIP"])
    chan_vip = env_any(["TELEGRAM_CHANNEL_VIP", "TG_CHANNEL_VIP"])
    bot_free = env_any(["TELEGRAM_BOT_TOKEN", "TG_BOT_TOKEN"])
    chan_free = env_any(["TELEGRAM_CHANNEL", "TG_CHANNEL"])

    if run_slot == "AM":
        missing = []
        if not bot_vip: missing.append("TELEGRAM_BOT_TOKEN_VIP")
        if not chan_vip: missing.append("TELEGRAM_CHANNEL_VIP")
        if missing:
            raise RuntimeError(f"Missing Telegram VIP env vars for AM: {', '.join(missing)}")
        bot_token, chat_id = bot_vip, chan_vip
        consume_status = "POSTED_INSTAGRAM"
        promote_status = "POSTED_TELEGRAM_VIP"
        ts_col = "posted_telegram_vip_at"
    else:
        missing = []
        if not bot_free: missing.append("TELEGRAM_BOT_TOKEN")
        if not chan_free: missing.append("TELEGRAM_CHANNEL")
        if missing:
            raise RuntimeError(f"Missing Telegram FREE env vars for PM: {', '.join(missing)}")
        bot_token, chat_id = bot_free, chan_free
        consume_status = "POSTED_TELEGRAM_VIP"
        promote_status = "POSTED_ALL"
        ts_col = "posted_telegram_free_at"

    gc = gs_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(tab)

    required = [
        "status","deal_id","deal_theme","price_gbp","origin_city","destination_city",
        "origin_iata","destination_iata","destination_country","outbound_date","return_date",
        "affiliate_url","booking_link_vip","phrase_bank","ai_notes","why_good","deal_score",
        "scored_timestamp","created_at","timestamp","posted_telegram_vip_at","posted_telegram_free_at","posted_instagram_at",
    ]
    ensure_columns(ws, required)

    all_vals = ws.get_all_values()
    if len(all_vals) < 2:
        log("No rows.")
        return 0

    headers = [h.strip() for h in all_vals[0]]
    rows = all_vals[1:]
    h = {name: i for i, name in enumerate(headers)}

    picked = pick_best_eligible(headers, rows, h, run_slot, consume_status, ts_col, free_delay_hours=24)
    if not picked:
        log(f"Done. Telegram posted 0. (No rows eligible for slot={run_slot} status={consume_status})")
        return 0

    rownum, row = picked
    deal_id = (row.get("deal_id") or "").strip()
    log(f"📨 Telegram best-pick row {rownum} deal_id={deal_id} RUN_SLOT={run_slot}")

    msg = build_vip_message(row) if run_slot == "AM" else build_free_message(row, stripe_monthly, stripe_yearly)

    tg_send(bot_token, chat_id, msg)

    now_z = iso_now()
    ws.update([[now_z]], f"{_col_letter(h[ts_col] + 1)}{rownum}")
    ws.update([[promote_status]], f"{_col_letter(h['status'] + 1)}{rownum}")

    log(f"✅ Telegram posted 1. Row {rownum} -> {promote_status}")
    return 0


def _col_letter(n1: int) -> str:
    s = ""
    n = n1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


if __name__ == "__main__":
    raise SystemExit(main())
