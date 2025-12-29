#!/usr/bin/env python3
"""
TravelTxter — Telegram Publisher (V4 CLEAN)
Clean, structured messaging with appropriate emojis.
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
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

def send_telegram(bot_token: str, chat_id: str, text: str) -> Tuple[bool, str]:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        r = requests.post(url, json=payload, timeout=20)
        if r.status_code != 200:
            return False, f"HTTP {r.status_code}"
        j = r.json()
        if not j.get("ok"):
            return False, str(j)
        return True, "ok"
    except Exception as e:
        return False, str(e)

def legacy_message(row: Dict[str, str]) -> str:
    bits = ["✈️ <b>Flight Deal</b>"]
    origin = safe(row.get('origin_city', ''))
    dest = safe(row.get('destination_city', ''))
    price = safe(row.get('price_gbp', ''))
    if origin and dest:
        bits.append(f"→ {origin} to {dest}")
    if price:
        bits.append(f"£{price}")
    link = row.get("affiliate_url", "").strip()
    if link:
        bits.append(f"\n🔗 Book: {safe(link)}")
    return "\n".join(bits).strip()

def vip_message(row: Dict[str, str]) -> str:
    ai_grade = safe(row.get("ai_grading", "")).upper()
    reason = clean(row.get("ai_notes") or row.get("notes") or "")
    origin = safe(row.get('origin_city', ''))
    dest = safe(row.get('destination_city', ''))
    country = safe(row.get('destination_country', ''))
    price = safe(row.get('price_gbp', ''))
    out_date = safe(row.get('outbound_date', ''))
    ret_date = safe(row.get('return_date', ''))
    
    lines = []
    
    if price and dest:
        dest_display = f"{dest}, {country}" if country else dest
        lines.append(f"<b>£{price} to {dest_display}</b>")
    else:
        lines.append("<b>DEAL ALERT</b>")
    
    lines.append("")
    
    if dest:
        lines.append(f"TO: {dest.upper()}")
    if origin:
        lines.append(f"FROM: {origin}")
    
    lines.append("")
    
    if out_date:
        lines.append(f"OUT:  {out_date}")
    if ret_date:
        lines.append(f"BACK: {ret_date}")
    
    if reason:
        lines.append("")
        lines.append("<b>Why book this:</b>")
        parts = [p.strip() for p in re.split(r"[.;]", reason) if p.strip()]
        for p in parts[:3]:
            lines.append(f"• {safe(p)}")
    
    lines.append("")
    lines.append("⏱ Availability limited")
    
    link = row.get("affiliate_url", "").strip()
    if link:
        lines.append("")
        lines.append("<b>Book now:</b>")
        lines.append(safe(link))
    
    return "\n".join(lines).strip()

def free_message(row: Dict[str, str], stripe: str) -> str:
    dest = safe(row.get('destination_city', ''))
    country = safe(row.get('destination_country', ''))
    origin = safe(row.get('origin_city', ''))
    price = safe(row.get('price_gbp', ''))
    out_date = safe(row.get('outbound_date', ''))
    ret_date = safe(row.get('return_date', ''))
    
    lines = []
    
    if price and dest:
        dest_display = f"{dest}, {country}" if country else dest
        lines.append(f"<b>£{price} to {dest_display}</b>")
    else:
        lines.append("<b>DEAL SPOTTED</b>")
    
    lines.append("")
    
    if dest:
        lines.append(f"TO: {dest.upper()}")
    if origin:
        lines.append(f"FROM: {origin}")
    
    lines.append("")
    
    if out_date:
        lines.append(f"OUT:  {out_date}")
    if ret_date:
        lines.append(f"BACK: {ret_date}")
    
    lines.append("")
    
    lines.append("<b>Heads up:</b>")
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
    
    if stripe:
        lines.append("<b>Upgrade now:</b>")
        lines.append(safe(stripe))
    else:
        lines.append("<b>Upgrade:</b> traveltxter.com/vip")
    
    return "\n".join(lines).strip()

def build_message(row: Dict[str, str], mode: str, template: str, stripe: str) -> str:
    if template == "v4":
        return vip_message(row) if mode == "vip" else free_message(row, stripe)
    return legacy_message(row)

def gs_client() -> gspread.Client:
    sa_json = env("GCP_SA_JSON")
    if not sa_json:
        raise RuntimeError("Missing GCP_SA_JSON")
    info = json.loads(sa_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def main() -> int:
    log.info("="*60)
    log.info("🚀 TravelTxter Telegram Publisher")
    log.info("="*60)
    
    sheet_id = env_first(["SPREADSHEET_ID", "SHEET_ID"])
    if not sheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")
    
    tab = env_first(["RAW_DEALS_TAB", "DEALS_SHEET_NAME"], "RAW_DEALS")
    status_col = env_first(["TELEGRAM_STATUS_COLUMN", "RAW_STATUS_COLUMN"], "raw_status")
    required = env_first(["TELEGRAM_REQUIRED_STATUS"], "POSTED_INSTAGRAM").upper()
    posted = env_first(["TELEGRAM_POSTED_STATUS"], "POSTED_TELEGRAM").upper()
<<<<<<< HEAD
    bot = env("TELEGRAM_BOT_TOKEN").strip()
    chat = env("TELEGRAM_CHANNEL").strip()
    log.info(f"Bot token length: {len(bot)}")
    log.info(f"Channel value: '{chat}'")
    if not bot or not chat:
    raise RuntimeError(f"Missing credentials - bot:{len(bot)} chars, channel:{len(chat)} chars")
=======
    
    bot = env("TELEGRAM_BOT_TOKEN")
    chat = env("TELEGRAM_CHANNEL")
    
    # Debug logging
    log.info(f"DEBUG: Bot token length = {len(bot)}")
    log.info(f"DEBUG: Channel = '{chat}'")
    
    if not bot or not chat:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHANNEL")
    
>>>>>>> b148469 (Fix telegram publisher with debug logging)
    mode = env_first(["TELEGRAM_MODE", "TG_MODE"], "free").lower()
    template = env_first(["TELEGRAM_TEMPLATE_VERSION", "TG_TEMPLATE_VERSION"], "legacy").lower()
    stripe = env("STRIPE_LINK")
    max_posts = int(env_first(["TELEGRAM_MAX_POSTS_PER_RUN"], "1"))
    
    log.info(f"📄 Tab: {tab}")
    log.info(f"🔎 Filter: {status_col} == {required}")
    log.info(f"✅ Promote to: {posted}")
    log.info(f"📱 Template: {template} (mode: {mode})")
    log.info(f"📊 Max posts: {max_posts}")
    log.info("="*60)
    
    gc = gs_client()
    ws = gc.open_by_key(sheet_id).worksheet(tab)
    log.info(f"✅ Connected to: {ws.title}")
    
    rows = ws.get_all_values()
    if not rows or len(rows) < 2:
        log.info("No data rows")
        return 0
    
    headers = rows[0]
    idx = {h: i for i, h in enumerate(headers)}
    
    if status_col not in idx:
        raise RuntimeError(f"Column '{status_col}' not found")
    
    sent = 0
    considered = 0
    failed = 0
    
    for r in range(1, len(rows)):
        if sent >= max_posts:
            break
        
        row = rows[r]
        row_num = r + 1
        
        current_status = row[idx[status_col]].strip().upper()
        if current_status != required:
            continue
        
        considered += 1
        
        data = {}
        for h, i in idx.items():
            data[h] = row[i] if i < len(row) else ""
        
        msg = build_message(data, mode, template, stripe)
        ok, err = send_telegram(bot, chat, msg)
        
        if not ok:
            failed += 1
            log.error(f"❌ Row {row_num}: {err}")
            continue
        
        try:
            ws.update_cell(row_num, idx[status_col] + 1, posted)
            sent += 1
            log.info(f"✅ Posted row {row_num}")
            time.sleep(0.6)
        except Exception as e:
            failed += 1
            log.error(f"❌ Sheet update failed row {row_num}: {e}")
    
    log.info("="*60)
    log.info("📊 SUMMARY")
    log.info("="*60)
    log.info(f"🔎 Considered: {considered}")
    log.info(f"✅ Published: {sent}")
    log.info(f"❌ Failed: {failed}")
    log.info("="*60)
    
    return 0

if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        log.error(f"❌ Worker failed: {e}")
        raise
