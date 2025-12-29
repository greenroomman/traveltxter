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

logging.basicConfig(level=logging.INFO, format="%(asctime)s - 
%(levelname)s - %(message)s")
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
    """HTML-escape for Telegram"""
    return html.escape(str(s or "").strip())


def clean(s: str) -> str:
    """Remove excessive whitespace"""
    return re.sub(r"\s+", " ", str(s or "").strip())


# =========================
# Telegram
# =========================

def send_telegram(bot_token: str, chat_id: str, text: str) -> Tuple[bool, 
str]:
    """Send message to Telegram channel"""
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", 
"disable_web_page_preview": True}
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


# =========================
# Templates
# =========================

def legacy_message(row: Dict[str, str]) -> str:
    """Simple legacy message format"""
    origin = safe(row.get('origin_city', ''))
    dest = safe(row.get('destination_city', ''))
    price = safe(row.get('price_gbp', ''))
    out_date = safe(row.get('outbound_date', ''))
    ret_date = safe(row.get('return_date', ''))
    
    bits = ["✈️ <b>Flight Deal</b>"]
    
    if origin and dest:
        route_msg = f"🇬🇧 {origin} → {dest}"
        bits.append(route_msg)
    
    if price:
        price_msg = f"💰 £{price}"
        bits.append(price_msg)
    
    if out_date and ret_date:
        date_msg = f"📅 {out_date} → {ret_date}"
        bits.append(date_msg)
    elif out_date or ret_date:
        date_msg = f"📅 {out_date or ret_date}"
        bits.append(date_msg)
    
    link = row.get("affiliate_url", "").strip()
    if link:
        link_msg = f"\n👉 <b>Book now:</b> {safe(link)}"
        bits.append(link_msg)
    
    return "\n".join(bits).strip()


def vip_message(row: Dict[str, str]) -> str:
    """VIP/Premium message with full details"""
    ai_grade = safe(row.get("ai_grading", "")).upper()
    reason = clean(row.get("ai_notes") or row.get("notes") or "")
    
    origin = safe(row.get('origin_city', ''))
    dest = safe(row.get('destination_city', ''))
    country = safe(row.get('destination_country', ''))
    price = safe(row.get('price_gbp', ''))
    out_date = safe(row.get('outbound_date', ''))
    ret_date = safe(row.get('return_date', ''))
    
    # Header based on AI grade
    if ai_grade == "A":
        header = "✈️ <b>A-GRADE DEAL</b>"
    else:
        header = "✈️ <b>PREMIUM DEAL</b>"
    
    lines = [header, ""]
    
    # Route with country
    if origin and dest:
        if country:
            route_msg = f"🌍 {origin} → {dest}, {country}"
        else:
            route_msg = f"🌍 {origin} → {dest}"
        lines.append(route_msg)
    
    # Dates
    if out_date and ret_date:
        date_msg = f"📅 {out_date} → {ret_date}"
        lines.append(date_msg)
    elif out_date or ret_date:
        date_msg = f"📅 {out_date or ret_date}"
        lines.append(date_msg)
    
    # Price
    if price:
        price_msg = f"💰 <b>£{price}</b>"
        lines.append(price_msg)
    
    # AI reasoning (keep short)
    if reason:
        lines.append("")
        lines.append("<b>Why this is special:</b>")
        parts = [p.strip() for p in re.split(r"[.;]", reason) if 
p.strip()]
        for p in parts[:3]:
            bullet_msg = f"• {safe(p)}"
            lines.append(bullet_msg)
    
    lines.append("")
    lines.append("⏳ <i>Likely to disappear fast. Book now.</i>")
    
    # Affiliate link
    link = row.get("affiliate_url", "").strip()
    if link:
        link_msg = f"\n👉 <b>Book this deal:</b> {safe(link)}"
        lines.append(link_msg)
    else:
        lines.append("\n⚠️ <i>Booking link unavailable</i>")
    
    return "\n".join(lines).strip()


def free_message(row: Dict[str, str], stripe: str) -> str:
    """Free tier teaser to drive VIP conversions"""
    dest = safe(row.get('destination_city', ''))
    country = safe(row.get('destination_country', ''))
    origin = safe(row.get('origin_city', ''))
    price = safe(row.get('price_gbp', ''))
    out_date = safe(row.get('outbound_date', ''))
    ret_date = safe(row.get('return_date', ''))
    
    lines = []
    
    # Eye-catching headline
    if price and dest:
        if country:
            dest_display = f"{dest}, {country}"
        else:
            dest_display = dest
        headline = f"🔥 <b>£{price} to {dest_display}</b>"
        lines.append(headline)
    else:
        lines.append("🔥 <b>Deal Alert</b>")
    
    lines.append("")
    
    # Basic details
    if origin:
        origin_msg = f"📍 From {origin}"
        lines.append(origin_msg)
    
    if out_date and ret_date:
        date_msg = f"📅 {out_date} → {ret_date}"
        lines.append(date_msg)
    elif out_date or ret_date:
        date_msg = f"📅 {out_date or ret_date}"
        lines.append(date_msg)
    
    lines.append("")
    
    # FOMO messaging
    lines.append("⚠️ <b>Heads up:</b>")
    lines.append("• VIP members saw this 24 hours ago")
    lines.append("• Availability is running low")
    lines.append("• Best deals go to VIPs first")
    
    lines.append("")
    
    # CTA
    lines.append("💎 <b>Want instant access to deals like this?</b>")
    lines.append("Join TravelTxter VIP for just £7/month:")
    lines.append("✓ Deals 24 hours early")
    lines.append("✓ Direct booking links")
    lines.append("✓ Exclusive mistake fares")
    lines.append("✓ Cancel anytime")
    
    if stripe:
        cta_msg = f"\n👉 <b>Upgrade now:</b> {safe(stripe)}"
        lines.append(cta_msg)
    else:
        lines.append("\n👉 Upgrade at traveltxter.com")
    
    return "\n".join(lines).strip()


def build_message(row: Dict[str, str], mode: str, template: str, stripe: 
str) -> str:
    """Build message based on mode and template version"""
    if template == "v4":
        if mode == "vip":
            return vip_message(row)
        else:
            return free_message(row, stripe)
    return legacy_message(row)


# =========================
# Sheets
# =========================

def gs_client() -> gspread.Client:
    """Create Google Sheets client"""
    sa_json = env("GCP_SA_JSON")
    if not sa_json:
        raise RuntimeError("Missing GCP_SA_JSON environment variable")
    
    try:
        info = json.loads(sa_json)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"GCP_SA_JSON is not valid JSON: {e}")
    
    scopes = ["https://www.googleapis.com/auth/spreadsheets", 
"https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


# =========================
# Main
# =========================

def main() -> int:
    log.info("\n" + "="*60)
    log.info("🚀 TravelTxter Telegram Publisher Starting")
    log.info("="*60)
    
    # Config
    sheet_id = env_first(["SPREADSHEET_ID", "SHEET_ID"])
    if not sheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID or SHEET_ID")
    
    tab = env_first(["RAW_DEALS_TAB", "DEALS_SHEET_NAME"], "RAW_DEALS")
    status_col = env_first(["TELEGRAM_STATUS_COLUMN", 
"RAW_STATUS_COLUMN"], "raw_status")
    required = env_first(["TELEGRAM_REQUIRED_STATUS"], 
"POSTED_INSTAGRAM").upper()
    posted = env_first(["TELEGRAM_POSTED_STATUS"], 
"POSTED_TELEGRAM").upper()
    
    bot = env("TELEGRAM_BOT_TOKEN")
    chat = env("TELEGRAM_CHANNEL")
    if not bot or not chat:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN or 
TELEGRAM_CHANNEL")
    
    mode = env_first(["TELEGRAM_MODE", "TG_MODE"], "free").lower()
    template = env_first(["TELEGRAM_TEMPLATE_VERSION", 
"TG_TEMPLATE_VERSION"], "legacy").lower()
    stripe = env("STRIPE_LINK")
    max_posts = int(env_first(["TELEGRAM_MAX_POSTS_PER_RUN"], "1"))
    
    log.info(f"📄 Tab: {tab}")
    log.info(f"🔎 Filter: {status_col} == {required}")
    log.info(f"✅ Promote to: {posted}")
    log.info(f"📱 Template: {template} (mode: {mode})")
    log.info(f"📊 Max posts: {max_posts}")
    log.info("="*60 + "\n")
    
    # Connect to sheet
    gc = gs_client()
    ws = gc.open_by_key(sheet_id).worksheet(tab)
    log.info(f"✅ Connected to worksheet: '{ws.title}'")
    
    rows = ws.get_all_values()
    if not rows or len(rows) < 2:
        log.info("No data rows found")
        return 0
    
    headers = rows[0]
    idx = {h: i for i, h in enumerate(headers)}
    
    if status_col not in idx:
        raise RuntimeError(f"Status column '{status_col}' not found in 
sheet")
    
    # Process rows
    sent = 0
    considered = 0
    failed = 0
    
    for r in range(1, len(rows)):
        if sent >= max_posts:
            break
        
        row = rows[r]
        row_num = r + 1
        
        # Check status
        current_status = row[idx[status_col]].strip().upper()
        if current_status != required:
            continue
        
        considered += 1
        
        # Build row dict
        data = {}
        for h, i in idx.items():
            if i < len(row):
                data[h] = row[i]
            else:
                data[h] = ""
        
        # Build and send message
        msg = build_message(data, mode, template, stripe)
        ok, err = send_telegram(bot, chat, msg)
        
        if not ok:
            failed += 1
            log.error(f"❌ Failed to send row {row_num}: {err}")
            continue
        
        # Update status
        try:
            col_num = idx[status_col] + 1
            ws.update_cell(row_num, col_num, posted)
            sent += 1
            log.info(f"✅ Posted row {row_num} and promoted to {posted}")
            time.sleep(0.6)
        except Exception as e:
            failed += 1
            log.error(f"❌ Posted but failed to update sheet row 
{row_num}: {e}")
    
    # Summary
    log.info("\n" + "="*60)
    log.info("📊 PUBLISH SUMMARY")
    log.info("="*60)
    log.info(f"🔎 Considered: {considered}")
    log.info(f"✅ Published:  {sent}")
    log.info(f"❌ Failed:     {failed}")
    log.info("="*60 + "\n")
    
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        log.error(f"❌ Worker failed: {e}")
        raise
