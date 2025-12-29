#!/usr/bin/env python3
"""
TravelTxter — Telegram Publisher (V4 CLEAN)

Safe, conflict-free, syntax-clean version.
Supports:
- FREE + VIP Telegram modes
- Legacy + V4 templates
- Header-based Google Sheets writes
"""

import os
import re
import json
import html
import time
import logging
import datetime as dt
from typing import Dict, List, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials


# =========================
# Logging
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("telegram_publisher")


def utc_now() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v)


def env_first(names: List[str], default: str = "") -> str:
    for n in names:
        v = os.getenv(n)
        if v:
            return str(v)
    return default


def truthy(v: str) -> bool:
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def safe(s: str) -> str:
    return html.escape(str(s or "").strip())


def clean(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "").strip())


# =========================
# Telegram
# =========================

def send_telegram(bot_token: str, chat_id: str, text: str) -> Tuple[bool, 
str]:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        r = requests.post(url, json=payload, timeout=20)
        if r.status_code != 200:
            return False, r.text
        j = r.json()
        if not j.get("ok"):
            return False, str(j)
        return True, "ok"
    except Exception as e:
        return False, str(e)


# =========================
# Templates
# =========================

def legacy_message(row: Dict[str, str]) -> str:
    bits = [
        "✈️ <b>Flight deal</b>",
        f"🇬🇧 {safe(row.get('origin_city'))} → 
{safe(row.get('destination_city'))}",
        f"💰 £{safe(row.get('price_gbp'))}",
    ]
    link = row.get("affiliate_url") or ""
    if link:
        bits.append(f"\n👉 <b>Book now:</b> {safe(link)}")
    return "\n".join(bits).strip()


def vip_message(row: Dict[str, str]) -> str:
    ai_grade = safe(row.get("ai_grading", "")).upper()
    reason = clean(row.get("ai_notes") or row.get("notes") or "")
    header = "✈️ <b>A-GRADE DEAL</b>" if ai_grade == "A" else "✈️ 
<b>PREMIUM DEAL</b>"

    lines = [
        header,
        f"🌍 {safe(row.get('origin_city'))} → 
{safe(row.get('destination_city'))}",
        f"📅 {safe(row.get('outbound_date'))} → 
{safe(row.get('return_date'))}",
        f"💰 <b>£{safe(row.get('price_gbp'))}</b>",
    ]

    if reason:
        lines.append("")
        lines.append("<b>Why this is special:</b>")
        for p in re.split(r"[.;]", reason)[:3]:
            if p.strip():
                lines.append(f"• {safe(p)}")

    link = row.get("affiliate_url") or ""
    if link:
        lines.append(f"\n👉 <b>Book now:</b> {safe(link)}")

    return "\n".join(lines).strip()


def free_message(row: Dict[str, str], stripe: str) -> str:
    lines = [
        f"🔥 <b>£{safe(row.get('price_gbp'))} to 
{safe(row.get('destination_city'))}</b>",
        "",
        "⚠️ VIP members saw this first.",
        "",
        "💎 <b>Want instant access?</b>",
        "Join Traveltxter VIP 👇",
    ]
    if stripe:
        lines.append(safe(stripe))
    return "\n".join(lines).strip()


def build_message(row: Dict[str, str], mode: str, template: str, stripe: 
str) -> str:
    if template == "v4":
        return vip_message(row) if mode == "vip" else free_message(row, 
stripe)
    return legacy_message(row)


# =========================
# Sheets
# =========================

def gs_client() -> gspread.Client:
    info = json.loads(env("GCP_SA_JSON"))
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


# =========================
# Main
# =========================

def main() -> int:
    sheet_id = env_first(["SPREADSHEET_ID", "SHEET_ID"])
    tab = env_first(["RAW_DEALS_TAB", "DEALS_SHEET_NAME"], "RAW_DEALS")
    status_col = env_first(["TELEGRAM_STATUS_COLUMN", 
"RAW_STATUS_COLUMN"], "raw_status")
    required = env_first(["TELEGRAM_REQUIRED_STATUS"], 
"POSTED_INSTAGRAM").upper()
    posted = env_first(["TELEGRAM_POSTED_STATUS"], 
"POSTED_TELEGRAM").upper()

    bot = env("TELEGRAM_BOT_TOKEN")
    chat = env("TELEGRAM_CHANNEL")

    mode = env_first(["TELEGRAM_MODE", "TG_MODE"], "free")
    template = env_first(["TELEGRAM_TEMPLATE_VERSION", 
"TG_TEMPLATE_VERSION"], "legacy")
    stripe = env("STRIPE_LINK")

    max_posts = int(env_first(["TELEGRAM_MAX_POSTS_PER_RUN"], "1"))

    gc = gs_client()
    ws = gc.open_by_key(sheet_id).worksheet(tab)
    rows = ws.get_all_values()
    headers = rows[0]
    idx = {h: i for i, h in enumerate(headers)}

    sent = 0

    for r in range(1, len(rows)):
        if sent >= max_posts:
            break

        row = rows[r]
        if row[idx[status_col]].upper() != required:
            continue

        data = {h: row[i] for h, i in idx.items()}
        msg = build_message(data, mode, template, stripe)

        ok, err = send_telegram(bot, chat, msg)
        if not ok:
            log.error(err)
            continue

        ws.update_cell(r + 1, idx[status_col] + 1, posted)
        sent += 1
        time.sleep(0.5)

    log.info(f"Published {sent} messages")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

