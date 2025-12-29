#!/usr/bin/env python3
"""
Traveltxter V3_beta_b_final — Telegram Publisher (CLEAN + PIPELINE-CORRECT)

✅ Reads from RAW_DEALS
✅ Filters using raw_status == POSTED_INSTAGRAM (or READY_TO_POST)
✅ Posts to Telegram channel
✅ Writes back: tg_status, tg_message_id, tg_published_timestamp
✅ Promotes raw_status -> POSTED_TELEGRAM

Environment variables required:
- SHEET_ID
- GCP_SA_JSON
- TELEGRAM_BOT_TOKEN
- TELEGRAM_CHANNEL (e.g., @yourtraveltxterchannel or -1001234567890)

Optional:
- RAW_DEALS_TAB (default RAW_DEALS)
- TG_REQUIRED_STATUS (default POSTED_INSTAGRAM)
- TG_POSTED_STATUS (default POSTED_TELEGRAM)
- MAX_POSTS_PER_RUN (default 1)
"""

import os
import sys
import json
import logging
import traceback
from datetime import datetime
from typing import List, Dict, Optional, Tuple

from google.oauth2 import service_account
import gspread
import requests


# =============================================================================
# Logging
# =============================================================================

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/telegram_publisher.log", mode="a"),
    ],
)
logger = logging.getLogger(__name__)


# =============================================================================
# Config / Env
# =============================================================================

DRY_RUN = os.getenv("DRY_RUN", "false").strip().lower() == "true"
WORKER_ID = os.getenv("WORKER_ID", "telegram_publisher")
GITHUB_RUN_ID = os.getenv("GITHUB_RUN_ID", "local")
GITHUB_RUN_NUMBER = os.getenv("GITHUB_RUN_NUMBER", "0")

RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS")
RAW_STATUS_COLUMN = os.getenv("RAW_STATUS_COLUMN", "raw_status").strip()
TG_STATUS_COLUMN = os.getenv("TG_STATUS_COLUMN", "tg_status").strip()
TG_REQUIRED_STATUS = os.getenv("TG_REQUIRED_STATUS", "POSTED_INSTAGRAM").strip().upper()
TG_POSTED_STATUS = os.getenv("TG_POSTED_STATUS", "POSTED_TELEGRAM").strip().upper()

MAX_POSTS_PER_RUN = int(os.getenv("MAX_POSTS_PER_RUN", "1").strip())


# =============================================================================
# Sheets helpers
# =============================================================================

def get_sheets_credentials():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    
    gcp_json_str = os.getenv("GCP_SA_JSON")
    if not gcp_json_str:
        raise ValueError("Missing GCP_SA_JSON")
    
    try:
        info = json.loads(gcp_json_str)
    except json.JSONDecodeError as e:
        raise ValueError(f"GCP_SA_JSON is not valid JSON: {e}")
    
    return service_account.Credentials.from_service_account_info(info, scopes=scopes)


def get_worksheet():
    sheet_id = os.getenv("SHEET_ID")
    if not sheet_id:
        raise ValueError("Missing SHEET_ID")
    
    creds = get_sheets_credentials()
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(RAW_DEALS_TAB)
    
    logger.info(f"✅ Using worksheet: '{ws.title}' ({ws.row_count} rows)")
    return ws


def col_index(headers: List[str], name: str) -> Optional[int]:
    name_l = name.strip().lower()
    for i, h in enumerate(headers):
        if str(h).strip().lower() == name_l:
            return i
    return None


def pad_row(row: List[str], n: int) -> List[str]:
    return (row + [""] * n)[:n]


def batch_update_cells(ws, headers: List[str], row_number: int, updates: Dict[str, str]) -> None:
    if DRY_RUN:
        logger.info(f"🧪 [DRY RUN] Would update row {row_number}: {updates}")
        return
    
    hmap = {h.strip(): i + 1 for i, h in enumerate(headers) if h.strip()}
    data = []
    
    for k, v in updates.items():
        if k in hmap:
            a1 = gspread.utils.rowcol_to_a1(row_number, hmap[k])
            data.append({"range": a1, "values": [[v]]})
    
    if data:
        ws.batch_update(data)


# =============================================================================
# Telegram posting
# =============================================================================

def format_message(deal: Dict[str, str]) -> str:
    """Format deal as Telegram message with HTML markup."""
    origin = deal.get("origin_city", "UK").strip()
    destination = deal.get("destination_city", "Unknown").strip()
    country = deal.get("destination_country", "").strip()
    price = deal.get("price_gbp", "???").strip()
    out_date = deal.get("outbound_date", "").strip()
    ret_date = deal.get("return_date", "").strip()
    days = deal.get("trip_length_days", "?").strip()
    stops = deal.get("stops", "?").strip()
    baggage = deal.get("baggage_included", "Unknown").strip()
    airline = deal.get("airline", "Various").strip()
    verdict = deal.get("ai_verdict", "").strip()
    
    # Build message
    country_text = f" ({country})" if country else ""
    
    msg = f"✈️ <b>{origin} → {destination}</b>{country_text}\n\n"
    msg += f"💷 <b>From £{price}</b>\n"
    
    if out_date and ret_date:
        msg += f"📅 {out_date} → {ret_date} ({days} days)\n"
    elif out_date:
        msg += f"📅 {out_date}\n"
    
    msg += f"🧳 Bag: {baggage} | Stops: {stops}\n"
    msg += f"🏷 Airline: {airline}\n"
    
    if verdict:
        msg += f"\n🔥 <b>{verdict}</b>\n"
    
    msg += "\n#TravelTxter #CheapFlights #BudgetTravel"
    
    return msg


def post_to_telegram(message: str, photo_url: str = None) -> Tuple[bool, str]:
    """
    Post message to Telegram channel.
    
    Args:
        message: Message text (HTML format)
        photo_url: Optional image URL
        
    Returns:
        Tuple of (success: bool, message_id or error: str)
    """
    if DRY_RUN:
        logger.info("🧪 [DRY RUN] Would post to Telegram")
        return True, "dry_run_tg_123"
    
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    channel = os.getenv("TELEGRAM_CHANNEL")
    
    if not bot_token or not channel:
        return False, "Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHANNEL"
    
    try:
        if photo_url:
            # Send photo with caption
            url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
            payload = {
                "chat_id": channel,
                "photo": photo_url,
                "caption": message,
                "parse_mode": "HTML",
            }
        else:
            # Send text message
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            payload = {
                "chat_id": channel,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": False,
            }
        
        logger.info(f"   Sending to Telegram channel: {channel}")
        response = requests.post(url, json=payload, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            if result.get("ok"):
                message_id = result.get("result", {}).get("message_id", "unknown")
                return True, str(message_id)
            else:
                error = result.get("description", "Unknown error")
                return False, f"Telegram API error: {error}"
        else:
            return False, f"HTTP {response.status_code}: {response.text[:200]}"
            
    except Exception as e:
        logger.error(f"❌ Failed to post to Telegram: {e}")
        return False, str(e)


# =============================================================================
# Main
# =============================================================================

def main() -> int:
    logger.info("\n" + "=" * 60)
    logger.info("🚀 TravelTxter Telegram Publisher Starting")
    logger.info("=" * 60)
    logger.info(f"⏰ Timestamp: {datetime.utcnow().isoformat()}Z")
    logger.info(f"🆔 Worker ID: {WORKER_ID}")
    logger.info(f"📋 Run: #{GITHUB_RUN_NUMBER} (ID: {GITHUB_RUN_ID})")
    logger.info(f"🧪 Dry Run: {DRY_RUN}")
    logger.info(f"📄 Tab: {RAW_DEALS_TAB}")
    logger.info(f"🔎 Filter: {RAW_STATUS_COLUMN} == {TG_REQUIRED_STATUS}")
    logger.info(f"✅ Promote on success: {RAW_STATUS_COLUMN} -> {TG_POSTED_STATUS}")
    logger.info(f"📊 Max posts per run: {MAX_POSTS_PER_RUN}")
    logger.info("=" * 60 + "\n")
    
    # Validate env
    required_vars = ["SHEET_ID", "GCP_SA_JSON", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHANNEL"]
    missing = [v for v in required_vars if not os.getenv(v)]
    
    if missing:
        logger.error(f"❌ Missing required environment variables: {missing}")
        return 1
    
    try:
        ws = get_worksheet()
        data = ws.get_all_values()
        
        if len(data) < 2:
            logger.info("ℹ️ No rows to publish.")
            return 0
        
        headers = [h.strip() for h in data[0]]
        rows = data[1:]
        header_len = len(headers)
        
        # Required columns
        required_cols = [
            "deal_id",
            "origin_city",
            "destination_city",
            "price_gbp",
            RAW_STATUS_COLUMN,
            TG_STATUS_COLUMN,
        ]
        
        missing_cols = [c for c in required_cols if col_index(headers, c) is None]
        if missing_cols:
            raise ValueError(f"Missing required columns in '{RAW_DEALS_TAB}': {missing_cols}")
        
        idx = {c: col_index(headers, c) for c in required_cols}
        
        # Optional columns
        optional_cols = [
            "destination_country", "outbound_date", "return_date", 
            "trip_length_days", "stops", "baggage_included", 
            "airline", "ai_verdict", "graphic_url"
        ]
        for col in optional_cols:
            idx[col] = col_index(headers, col)
        
        published = 0
        failed = 0
        considered = 0
        
        for row_num, row in enumerate(rows, start=2):
            if published >= MAX_POSTS_PER_RUN:
                logger.info(f"✋ Reached MAX_POSTS_PER_RUN ({MAX_POSTS_PER_RUN}), stopping")
                break
            
            row = pad_row(row, header_len)
            
            raw_status = (row[idx[RAW_STATUS_COLUMN]] or "").strip().upper()
            if raw_status != TG_REQUIRED_STATUS:
                continue
            
            tg_status = (row[idx[TG_STATUS_COLUMN]] or "").strip().upper()
            if tg_status in ("POSTED", "PUBLISHED", "DONE"):
                logger.debug(f"Row {row_num}: Already posted to Telegram, skipping")
                continue
            
            # Build deal dict
            deal = {}
            for col_name, col_idx in idx.items():
                if col_idx is not None and col_idx < len(row):
                    deal[col_name] = row[col_idx]
                else:
                    deal[col_name] = ""
            
            considered += 1
            message = format_message(deal)
            photo_url = deal.get("graphic_url", "").strip() or None
            
            logger.info(f"\n{'='*60}")
            logger.info(f"📱 Posting row {row_num}: {deal['origin_city']} → {deal['destination_city']} (£{deal['price_gbp']})")
            logger.info(f"   Deal ID: {deal['deal_id']}")
            if photo_url:
                logger.info(f"   Photo: {photo_url}")
            
            ok, result = post_to_telegram(message, photo_url)
            
            if ok:
                published += 1
                logger.info(f"✅ Posted successfully: message_id={result}")
                
                batch_update_cells(
                    ws,
                    headers,
                    row_num,
                    {
                        TG_STATUS_COLUMN: "POSTED",
                        "tg_message_id": result,
                        "tg_published_timestamp": datetime.utcnow().isoformat() + "Z",
                        RAW_STATUS_COLUMN: TG_POSTED_STATUS,
                    },
                )
            else:
                failed += 1
                logger.error(f"❌ Post failed: {result}")
                
                batch_update_cells(
                    ws,
                    headers,
                    row_num,
                    {
                        TG_STATUS_COLUMN: "FAILED",
                        "tg_error": result[:200],
                        "tg_published_timestamp": datetime.utcnow().isoformat() + "Z",
                    },
                )
            
            # Rate limiting
            if published < MAX_POSTS_PER_RUN:
                import time
                time.sleep(2)  # 2 seconds between posts
        
        logger.info("\n" + "=" * 60)
        logger.info("📊 PUBLISH SUMMARY")
        logger.info("=" * 60)
        logger.info(f"🔎 Considered: {considered}")
        logger.info(f"✅ Published:  {published}")
        logger.info(f"❌ Failed:     {failed}")
        logger.info("=" * 60 + "\n")
        
        # Save stats
        stats = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "run_id": GITHUB_RUN_ID,
            "run_number": GITHUB_RUN_NUMBER,
            "considered": considered,
            "published": published,
            "failed": failed,
            "dry_run": DRY_RUN,
        }
        
        with open("logs/telegram_stats.json", "w") as f:
            json.dump(stats, f, indent=2)
        
        logger.info("📊 Stats saved to logs/telegram_stats.json")
        
        return 1 if failed > 0 else 0
        
    except Exception as e:
        logger.error(f"❌ Worker failed with error: {e}")
        logger.debug(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
