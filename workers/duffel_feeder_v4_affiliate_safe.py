#!/usr/bin/env python3
"""
TravelTxter — Telegram Publisher V4 (Dual Post, Affiliate-SAFE)

- Posts BOTH FREE + VIP in a single run.
- Uses booking_link_free / booking_link_vip if present, otherwise falls back to affiliate_url.
- Does NOT require Skyscanner affiliate approval yet.
- Optional click tracking via REDIRECT_BASE_URL (PythonAnywhere /r endpoint).
- Resume-safe if posted_to_free / posted_to_vip columns exist.

Env:
- GCP_SA_JSON, SPREADSHEET_ID
- TELEGRAM_BOT_TOKEN_FREE, TELEGRAM_CHAT_ID_FREE
- TELEGRAM_BOT_TOKEN_VIP, TELEGRAM_CHAT_ID_VIP
- STRIPE_LINK
- REDIRECT_BASE_URL (optional)
"""

import os
import re
import json
import html
import time
import logging
import datetime as dt
import urllib.parse
from typing import Dict, List, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("telegram_publisher_v4")


def env(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip()

def safe(s: str) -> str:
    return html.escape((s or "").strip())

def clean(s: str) -> str:
    s = (s or "").strip()
    return re.sub(r"\s+", " ", s)

def send_telegram(bot_token: str, chat_id: str, text: str) -> Tuple[bool, str, str]:
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": False,
        }
        r = requests.post(url, json=payload, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data.get("ok"):
            return False, str(data), ""
        msg_id = str((data.get("result") or {}).get("message_id") or "")
        return True, "", msg_id
    except Exception as e:
        return False, str(e), ""

def wrap_link(deal_id: str, tier: str, url: str, redirect_base: str) -> str:
    if not url:
        return ""
    if not redirect_base:
        return url
    q = {"deal_id": deal_id, "tier": tier, "url": url}
    return redirect_base + "?" + urllib.parse.urlencode(q)


def free_message(row: Dict[str, str], stripe: str, free_link: str) -> str:
    origin = safe(row.get("origin_city", ""))
    dest = safe(row.get("destination_city", ""))
    country = safe(row.get("destination_country", ""))
    price = clean(row.get("price_gbp", ""))
    out_date = safe(row.get("outbound_date", ""))
    ret_date = safe(row.get("return_date", ""))

    lines = []
    if price and dest:
        lines.append(f"🔥 <b>£{price} to {dest}{(', ' + country) if country else ''}</b>")
    else:
        lines.append("🔥 <b>DEAL SPOTTED</b>")

    lines.append("")
    if origin:
        lines.append(f"📍 From {origin}")
    if out_date and ret_date:
        lines.append(f"📅 {out_date} → {ret_date}")

    lines.append("")
    lines.append("⚠️ Heads up:")
    lines.append("• VIP members saw this 24 hours ago")
    lines.append("• Availability is running low")
    lines.append("• Best deals go to VIPs first")
    lines.append("")
    lines.append("<b>Want instant access?</b>")
    lines.append("Join TravelTxter Nomad")
    lines.append("for £7.99 / month:")
    lines.append("")
    lines.append("* Deals 24 hours early")
    lines.append("* Direct booking links")
    lines.append("* Exclusive mistake fares")
    lines.append("* Cancel anytime")
    lines.append("")

    # FREE link is optional; keep it for now (non-affiliate)
    if free_link:
        lines.append("<b>Book (FREE):</b>")
        lines.append(safe(free_link))
        lines.append("")

    if stripe:
        lines.append("<b>Upgrade now:</b>")
        lines.append(safe(stripe))
    else:
        lines.append("<b>Upgrade:</b> traveltxter.com/vip")

    return "\n".join(lines).strip()


def vip_message(row: Dict[str, str], vip_link: str) -> str:
    ai_grade = safe(row.get("ai_grading", "")).upper()
    origin = safe(row.get("origin_city", ""))
    dest = safe(row.get("destination_city", ""))
    country = safe(row.get("destination_country", ""))
    price = clean(row.get("price_gbp", ""))
    out_date = safe(row.get("outbound_date", ""))
    ret_date = safe(row.get("return_date", ""))

    lines = []
    head = "💎 <b>VIP EARLY ACCESS</b>"
    if ai_grade:
        head += f" — {ai_grade}"
    lines.append(head)

    if price and dest:
        lines.append(f"🔥 <b>£{price} to {dest}{(', ' + country) if country else ''}</b>")

    lines.append("")
    if origin:
        lines.append(f"📍 From {origin}")
    if out_date and ret_date:
        lines.append(f"📅 {out_date} → {ret_date}")

    lines.append("")
    if vip_link:
        lines.append("🔗 <b>Direct booking link:</b>")
        lines.append(safe(vip_link))
    else:
        lines.append("🔗 Booking link unavailable (try again later).")

    lines.append("")
    lines.append("✅ You’re seeing this first because you’re VIP.")
    return "\n".join(lines).strip()


def gs_client() -> gspread.Client:
    sa_json = env("GCP_SA_JSON")
    if not sa_json:
        raise RuntimeError("Missing GCP_SA_JSON")
    info = json.loads(sa_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def update_cells(ws, row_num: int, headers: List[str], updates: Dict[str, str]) -> None:
    idx = {h: i + 1 for i, h in enumerate(headers)}
    cells = []
    for k, v in updates.items():
        if k in idx:
            cells.append(gspread.Cell(row_num, idx[k], v))
    if cells:
        ws.update_cells(cells, value_input_option="USER_ENTERED")


def main() -> int:
    log.info("=" * 60)
    log.info("🚀 TravelTxter Telegram Publisher (V4 DUAL, SAFE)")
    log.info("=" * 60)

    sheet_id = env("SPREADSHEET_ID")
    if not sheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")

    tab = env("RAW_DEALS_TAB", "RAW_DEALS")
    status_col = env("TELEGRAM_STATUS_COLUMN", "status")
    required = env("TELEGRAM_REQUIRED_STATUS", "READY_TO_POST").upper()
    posted = env("TELEGRAM_POSTED_STATUS", "POSTED_TELEGRAM").upper()

    bot_free = env("TELEGRAM_BOT_TOKEN_FREE")
    chat_free = env("TELEGRAM_CHAT_ID_FREE")
    bot_vip = env("TELEGRAM_BOT_TOKEN_VIP")
    chat_vip = env("TELEGRAM_CHAT_ID_VIP")

    if not bot_free or not chat_free:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN_FREE or TELEGRAM_CHAT_ID_FREE")
    if not bot_vip or not chat_vip:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN_VIP or TELEGRAM_CHAT_ID_VIP")

    stripe = env("STRIPE_LINK")
    redirect_base = env("REDIRECT_BASE_URL")  # optional
    max_posts = int(env("TELEGRAM_MAX_POSTS_PER_RUN", "1"))

    gc = gs_client()
    ws = gc.open_by_key(sheet_id).worksheet(tab)

    rows = ws.get_all_values()
    if not rows or len(rows) < 2:
        log.info("No data rows")
        return 0

    headers = rows[0]
    idx = {h: i for i, h in enumerate(headers)}
    if status_col not in idx:
        raise RuntimeError(f"Column '{status_col}' not found")

    sent = 0

    for r in range(1, len(rows)):
        if sent >= max_posts:
            break

        row = rows[r]
        row_num = r + 1

        current_status = (row[idx[status_col]] if idx[status_col] < len(row) else "").strip().upper()
        if current_status != required:
            continue

        data: Dict[str, str] = {h: (row[i] if i < len(row) else "") for h, i in idx.items()}
        deal_id = (data.get("deal_id") or "").strip()

        already_free = (data.get("posted_to_free") or "").strip().upper() == "TRUE"
        already_vip = (data.get("posted_to_vip") or "").strip().upper() == "TRUE"

        raw_free = (data.get("booking_link_free") or "").strip() or (data.get("affiliate_url") or "").strip()
        raw_vip = (data.get("booking_link_vip") or "").strip() or (data.get("affiliate_url") or "").strip()

        free_link = wrap_link(deal_id, "free", raw_free, redirect_base) if deal_id else raw_free
        vip_link = wrap_link(deal_id, "vip", raw_vip, redirect_base) if deal_id else raw_vip

        free_text = free_message(data, stripe, free_link)
        vip_text = vip_message(data, vip_link)

        updates: Dict[str, str] = {}

        try:
            if not already_free:
                ok, err, msg_id = send_telegram(bot_free, chat_free, free_text)
                if not ok:
                    raise RuntimeError(f"FREE send failed: {err}")
                updates["posted_to_free"] = "TRUE"
                updates["telegram_free_msg_id"] = msg_id
                time.sleep(0.5)

            if not already_vip:
                ok, err, msg_id = send_telegram(bot_vip, chat_vip, vip_text)
                if not ok:
                    raise RuntimeError(f"VIP send failed: {err}")
                updates["posted_to_vip"] = "TRUE"
                updates["telegram_vip_msg_id"] = msg_id

            final_free = already_free or updates.get("posted_to_free") == "TRUE"
            final_vip = already_vip or updates.get("posted_to_vip") == "TRUE"

            if final_free and final_vip:
                updates[status_col] = posted
                updates["published_timestamp"] = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

            update_cells(ws, row_num, headers, updates)
            sent += 1
            log.info(f"✅ Posted row {row_num} (FREE+VIP)")
            time.sleep(0.6)

        except Exception as e:
            log.error(f"❌ Row {row_num}: {e}")
            # Only set error status if you use it; otherwise leave row for retry.
            if status_col in idx:
                try:
                    update_cells(ws, row_num, headers, {status_col: "ERROR_TELEGRAM"})
                except Exception:
                    pass

    log.info(f"🏁 Done. Published {sent} row(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
