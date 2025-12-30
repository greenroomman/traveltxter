#!/usr/bin/env python3
"""
TravelTxter V4.1 — UNIFIED PIPELINE WORKER

Single worker that handles entire pipeline:
1. Duffel Feeder (optional, can run separately)
2. AI Scoring (NEW → READY_TO_POST)
3. Render (READY_TO_POST → READY_TO_PUBLISH)
4. Instagram (READY_TO_PUBLISH → POSTED_INSTAGRAM)
5. Telegram FREE (POSTED_INSTAGRAM → POSTED_TELEGRAM_FREE)
6. Telegram VIP (POSTED_TELEGRAM_FREE → POSTED_ALL)

State machine:
  NEW → READY_TO_POST → READY_TO_PUBLISH → POSTED_INSTAGRAM 
    → POSTED_TELEGRAM_FREE → POSTED_ALL

Environment variables:
  SPREADSHEET_ID, GCP_SA_JSON (required)
  OPENAI_API_KEY, OPENAI_MODEL
  RENDER_URL
  IG_ACCESS_TOKEN, IG_USER_ID
  TELEGRAM_BOT_TOKEN
  STRIPE_LINK
"""

import os
import sys
import json
import ssl
import time
import datetime as dt
from typing import Dict, Any, List, Optional
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

import gspread
from google.oauth2.service_account import Credentials
import requests


# =========================
# CONFIG
# =========================

def env(key: str, default: str = "") -> str:
    return (os.getenv(key) or default).strip()

def env_any(keys: List[str], default: str = "") -> str:
    for k in keys:
        v = env(k)
        if v:
            return v
    return default

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
    print(f"{ts} | {msg}", flush=True)


SPREADSHEET_ID = env_any(["SPREADSHEET_ID", "SHEET_ID"])
RAW_DEALS_TAB = env_any(["RAW_DEALS_TAB", "DEALS_SHEET_NAME"], "RAW_DEALS")
STATUS_COLUMN = "status"

OPENAI_API_KEY = env("OPENAI_API_KEY")
OPENAI_MODEL = env("OPENAI_MODEL", "gpt-4o-mini")

RENDER_URL = env_any(["RENDER_URL", "RENDER_BASE_URL"])

IG_ACCESS_TOKEN = env_any(["IG_ACCESS_TOKEN", "FB_ACCESS_TOKEN"])
IG_USER_ID = env("IG_USER_ID")

TELEGRAM_BOT_TOKEN = env("TELEGRAM_BOT_TOKEN")
TELEGRAM_FREE_CHANNEL = env("TELEGRAM_FREE_CHANNEL", "-1003505750272")
TELEGRAM_VIP_CHANNEL = env("TELEGRAM_VIP_CHANNEL", "-1003517970522")

STRIPE_LINK = env("STRIPE_LINK", "https://buy.stripe.com/7sYeV6faCa583IkaSFe7m04")


# =========================
# GOOGLE SHEETS
# =========================

def gs_client():
    sa_json = env_any(["GCP_SA_JSON", "GCP_SA_JSON_ONE_LINE"])
    if not sa_json:
        raise RuntimeError("Missing GCP_SA_JSON")
    
    info = json.loads(sa_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def get_ws():
    gc = gs_client()
    return gc.open_by_key(SPREADSHEET_ID).worksheet(RAW_DEALS_TAB)


def col_to_a1(n: int) -> str:
    out = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        out = chr(65 + r) + out
    return out


def update_cells(ws, row_num: int, headers: List[str], updates: Dict[str, str]) -> None:
    hmap = {h.strip(): i + 1 for i, h in enumerate(headers) if h.strip()}
    data = []
    for k, v in updates.items():
        if k in hmap:
            col = hmap[k]
            data.append({"range": f"{col_to_a1(col)}{row_num}", "values": [[v]]})
    if data:
        ws.batch_update(data)


# =========================
# STAGE 1: AI SCORING
# =========================

def score_deal(rec: Dict[str, str]) -> Dict[str, str]:
    """Score deal using OpenAI or fallback heuristic."""
    if not OPENAI_API_KEY:
        return score_heuristic(rec)
    
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
        
        prompt = f"""Score this flight deal (1-10) on whether a real person would book it.

Deal:
- Route: {rec.get('origin_city')} → {rec.get('destination_city')}
- Price: £{rec.get('price_gbp')}
- Dates: {rec.get('outbound_date')} to {rec.get('return_date')}
- Trip: {rec.get('trip_length_days')} days
- Stops: {rec.get('stops')}

Respond JSON only:
{{"score": 1-10, "verdict": "GOOD|AVERAGE|POOR", "grading": "A+|A|B|C"}}"""

        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": "Travel expert. JSON only."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=100
        )
        
        result = json.loads(response.choices[0].message.content)
        return {
            "ai_score": str(result.get("score", "5")),
            "ai_verdict": result.get("verdict", "AVERAGE").upper(),
            "ai_grading": result.get("grading", "B"),
        }
    except Exception as e:
        log(f"  OpenAI failed: {e}, using heuristic")
        return score_heuristic(rec)


def score_heuristic(rec: Dict[str, str]) -> Dict[str, str]:
    """Fallback heuristic scoring."""
    try:
        price = float(rec.get("price_gbp", "999").replace("£", ""))
        stops = int(rec.get("stops", "0"))
        days = int(rec.get("trip_length_days", "0"))
    except:
        return {"ai_score": "5", "ai_verdict": "AVERAGE", "ai_grading": "B"}
    
    score = 5
    if price <= 60:
        score = 9
    elif price <= 100:
        score = 7
    elif price <= 150:
        score = 6
    
    if stops == 0:
        score = min(10, score + 1)
    
    verdict = "GOOD" if score >= 7 else "AVERAGE" if score >= 5 else "POOR"
    grading = "A" if score >= 8 else "B" if score >= 6 else "C"
    
    return {
        "ai_score": str(score),
        "ai_verdict": verdict,
        "ai_grading": grading,
    }


# =========================
# STAGE 2: RENDER
# =========================

def render_deal(rec: Dict[str, str]) -> Optional[str]:
    """Call render service, return image URL."""
    if not RENDER_URL:
        log("  No RENDER_URL, skipping")
        return None
    
    try:
        payload = {
            "deal_id": rec.get("deal_id", ""),
            "origin_city": rec.get("origin_city", ""),
            "destination_city": rec.get("destination_city", ""),
            "destination_country": rec.get("destination_country", ""),
            "price_gbp": rec.get("price_gbp", ""),
            "outbound_date": rec.get("outbound_date", ""),
            "return_date": rec.get("return_date", ""),
            "ai_grading": rec.get("ai_grading", "B"),
        }
        
        body = json.dumps(payload).encode("utf-8")
        req = Request(RENDER_URL, data=body, headers={"Content-Type": "application/json"}, method="POST")
        ctx = ssl.create_default_context()
        
        with urlopen(req, timeout=30, context=ctx) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data.get("graphic_url") or data.get("image_url")
            
    except Exception as e:
        log(f"  Render failed: {e}")
        return None


# =========================
# STAGE 3: INSTAGRAM
# =========================

def post_instagram(graphic_url: str, rec: Dict[str, str]) -> Optional[str]:
    """Post to Instagram, return media_id."""
    if not IG_ACCESS_TOKEN or not IG_USER_ID:
        log("  No Instagram credentials, skipping")
        return None
    
    try:
        origin = rec.get("origin_city", "UK")
        dest = rec.get("destination_city", "Unknown")
        price = rec.get("price_gbp", "???")
        out_date = rec.get("outbound_date", "")
        
        caption = (
            f"✈️ {origin} → {dest}\n\n"
            f"💰 From £{price}\n"
            f"📅 {out_date}\n\n"
            "🔥 Limited availability!\n\n"
            "Join Telegram for ALL deals (link in bio) 👆\n\n"
            "#TravelTxter #CheapFlights #TravelDeals"
        )
        
        # Create container
        r1 = requests.post(
            f"https://graph.facebook.com/v19.0/{IG_USER_ID}/media",
            data={
                "image_url": graphic_url,
                "caption": caption,
                "access_token": IG_ACCESS_TOKEN,
            },
            timeout=30
        )
        
        if r1.status_code != 200:
            log(f"  Instagram container failed: {r1.text[:200]}")
            return None
        
        container_id = r1.json().get("id")
        
        # Publish
        r2 = requests.post(
            f"https://graph.facebook.com/v19.0/{IG_USER_ID}/media_publish",
            data={
                "creation_id": container_id,
                "access_token": IG_ACCESS_TOKEN,
            },
            timeout=30
        )
        
        if r2.status_code != 200:
            log(f"  Instagram publish failed: {r2.text[:200]}")
            return None
        
        return r2.json().get("id", "unknown")
        
    except Exception as e:
        log(f"  Instagram error: {e}")
        return None


# =========================
# STAGE 4 & 5: TELEGRAM
# =========================

def post_telegram(channel: str, rec: Dict[str, str], is_vip: bool = False) -> Optional[str]:
    """Post to Telegram, return message_id."""
    if not TELEGRAM_BOT_TOKEN:
        log("  No Telegram token, skipping")
        return None
    
    try:
        origin = rec.get("origin_city", "UK")
        dest = rec.get("destination_city", "Unknown")
        country = rec.get("destination_country", "")
        price = rec.get("price_gbp", "???")
        out_date = rec.get("outbound_date", "")
        ret_date = rec.get("return_date", "")
        days = rec.get("trip_length_days", "?")
        verdict = rec.get("ai_verdict", "")
        graphic_url = rec.get("graphic_url", "")
        
        if is_vip:
            # VIP message (full details)
            msg = f"💎 <b>VIP EARLY ACCESS</b>\n\n"
            msg += f"✈️ <b>{origin} → {dest}</b>"
            if country:
                msg += f" ({country})"
            msg += f"\n\n💷 <b>£{price}</b>\n"
            if out_date and ret_date:
                msg += f"📅 {out_date} → {ret_date} ({days} days)\n"
            if verdict:
                msg += f"\n🔥 <b>{verdict}</b>\n"
            msg += "\n✅ You're seeing this first because you're VIP."
        else:
            # FREE message (teaser + CTA)
            msg = f"🔥 <b>£{price} to {dest}</b>"
            if country:
                msg += f" ({country})"
            msg += f"\n\n📍 From {origin}\n"
            if out_date:
                msg += f"📅 {out_date}\n"
            msg += "\n⚠️ Heads up:\n"
            msg += "• VIP members saw this 24 hours ago\n"
            msg += "• Availability running low\n\n"
            msg += "<b>Want instant access?</b>\n"
            msg += f"Join TravelTxter Nomad for £7.99/month:\n{STRIPE_LINK}"
        
        # Send with photo if available
        if graphic_url:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
            payload = {
                "chat_id": channel,
                "photo": graphic_url,
                "caption": msg,
                "parse_mode": "HTML",
            }
        else:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {
                "chat_id": channel,
                "text": msg,
                "parse_mode": "HTML",
            }
        
        response = requests.post(url, json=payload, timeout=30)
        
        if response.status_code == 200 and response.json().get("ok"):
            return str(response.json().get("result", {}).get("message_id", "unknown"))
        else:
            log(f"  Telegram error: {response.text[:200]}")
            return None
            
    except Exception as e:
        log(f"  Telegram error: {e}")
        return None


# =========================
# MAIN PIPELINE
# =========================

def main() -> int:
    log("=" * 60)
    log("🚀 TRAVELTXTER V4.1 UNIFIED PIPELINE")
    log("=" * 60)
    log(f"Sheet: {SPREADSHEET_ID}")
    log(f"Tab: {RAW_DEALS_TAB}")
    log(f"Status column: {STATUS_COLUMN}")
    log("=" * 60)
    
    if not SPREADSHEET_ID:
        log("❌ Missing SPREADSHEET_ID")
        return 1
    
    ws = get_ws()
    log(f"✅ Connected to: {ws.title}")
    
    rows = ws.get_all_values()
    if len(rows) < 2:
        log("No data rows")
        return 0
    
    headers = [h.strip() for h in rows[0]]
    hmap = {h: i for i, h in enumerate(headers)}
    
    if STATUS_COLUMN not in hmap:
        log(f"❌ Column '{STATUS_COLUMN}' not found")
        return 1
    
    status_idx = hmap[STATUS_COLUMN]
    log(f"✅ Status column found at index {status_idx}")
    
    # Process ONE deal through entire pipeline
    for row_idx in range(1, len(rows)):
        row = rows[row_idx]
        row_num = row_idx + 1
        
        if status_idx >= len(row):
            continue
        
        current_status = row[status_idx].strip().upper()
        
        # Build record
        rec = {h: (row[hmap[h]] if hmap[h] < len(row) else "") for h in headers}
        deal_id = rec.get("deal_id", "")
        
        # STAGE 1: NEW → READY_TO_POST (Score)
        if current_status == "NEW":
            log(f"\n📊 Scoring row {row_num} (deal_id={deal_id})...")
            score_result = score_deal(rec)
            
            updates = {
                STATUS_COLUMN: "READY_TO_POST",
                "ai_score": score_result["ai_score"],
                "ai_verdict": score_result["ai_verdict"],
                "ai_grading": score_result["ai_grading"],
                "scored_timestamp": dt.datetime.utcnow().isoformat() + "Z",
            }
            
            update_cells(ws, row_num, headers, updates)
            log(f"✅ Scored: {score_result['ai_score']}/10 ({score_result['ai_verdict']})")
            log(f"   Status: NEW → READY_TO_POST")
            return 0  # Process one at a time
        
        # STAGE 2: READY_TO_POST → READY_TO_PUBLISH (Render)
        elif current_status == "READY_TO_POST":
            log(f"\n🎨 Rendering row {row_num} (deal_id={deal_id})...")
            graphic_url = render_deal(rec)
            
            if graphic_url:
                updates = {
                    STATUS_COLUMN: "READY_TO_PUBLISH",
                    "graphic_url": graphic_url,
                    "rendered_timestamp": dt.datetime.utcnow().isoformat() + "Z",
                }
                update_cells(ws, row_num, headers, updates)
                log(f"✅ Rendered: {graphic_url}")
                log(f"   Status: READY_TO_POST → READY_TO_PUBLISH")
            else:
                log(f"❌ Render failed")
            
            return 0
        
        # STAGE 3: READY_TO_PUBLISH → POSTED_INSTAGRAM (Instagram)
        elif current_status == "READY_TO_PUBLISH":
            log(f"\n📸 Instagram posting row {row_num} (deal_id={deal_id})...")
            graphic_url = rec.get("graphic_url", "")
            
            if not graphic_url:
                log("❌ No graphic_url, skipping")
                continue
            
            media_id = post_instagram(graphic_url, rec)
            
            if media_id:
                updates = {
                    STATUS_COLUMN: "POSTED_INSTAGRAM",
                    "ig_media_id": media_id,
                    "ig_published_timestamp": dt.datetime.utcnow().isoformat() + "Z",
                }
                update_cells(ws, row_num, headers, updates)
                log(f"✅ Posted to Instagram: {media_id}")
                log(f"   Status: READY_TO_PUBLISH → POSTED_INSTAGRAM")
            else:
                log(f"❌ Instagram failed")
            
            return 0
        
        # STAGE 4: POSTED_INSTAGRAM → POSTED_TELEGRAM_FREE (Telegram FREE)
        elif current_status == "POSTED_INSTAGRAM":
            log(f"\n📱 Telegram FREE posting row {row_num} (deal_id={deal_id})...")
            
            msg_id = post_telegram(TELEGRAM_FREE_CHANNEL, rec, is_vip=False)
            
            if msg_id:
                updates = {
                    STATUS_COLUMN: "POSTED_TELEGRAM_FREE",
                    "tg_free_message_id": msg_id,
                    "tg_free_timestamp": dt.datetime.utcnow().isoformat() + "Z",
                }
                update_cells(ws, row_num, headers, updates)
                log(f"✅ Posted to Telegram FREE: {msg_id}")
                log(f"   Status: POSTED_INSTAGRAM → POSTED_TELEGRAM_FREE")
            else:
                log(f"❌ Telegram FREE failed")
            
            return 0
        
        # STAGE 5: POSTED_TELEGRAM_FREE → POSTED_ALL (Telegram VIP)
        elif current_status == "POSTED_TELEGRAM_FREE":
            log(f"\n💎 Telegram VIP posting row {row_num} (deal_id={deal_id})...")
            
            msg_id = post_telegram(TELEGRAM_VIP_CHANNEL, rec, is_vip=True)
            
            if msg_id:
                updates = {
                    STATUS_COLUMN: "POSTED_ALL",
                    "tg_vip_message_id": msg_id,
                    "tg_vip_timestamp": dt.datetime.utcnow().isoformat() + "Z",
                    "published_timestamp": dt.datetime.utcnow().isoformat() + "Z",
                }
                update_cells(ws, row_num, headers, updates)
                log(f"✅ Posted to Telegram VIP: {msg_id}")
                log(f"   Status: POSTED_TELEGRAM_FREE → POSTED_ALL")
                log(f"🎉 PIPELINE COMPLETE for deal_id={deal_id}")
            else:
                log(f"❌ Telegram VIP failed")
            
            return 0
    
    log("\nNo deals found to process")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        log(f"❌ FATAL: {e}")
        import traceback
        log(traceback.format_exc())
        sys.exit(1)
