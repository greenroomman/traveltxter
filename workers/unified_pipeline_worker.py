#!/usr/bin/env python3
"""
TravelTxter V4.1 — UNIFIED PIPELINE WORKER (FIXED)

✅ Themed destinations (variety every day)
✅ Best-deal selection (not oldest first)
✅ UK English spelling
✅ Human-sounding copy (less AI)
✅ Freshness decay prioritisation

Pipeline stages:
1. AI Scoring (NEW → READY_TO_POST) - picks BEST from batch
2. Render (READY_TO_POST → READY_TO_PUBLISH)
3. Instagram (READY_TO_PUBLISH → POSTED_INSTAGRAM)
4. Telegram FREE (POSTED_INSTAGRAM → POSTED_TELEGRAM_FREE)
5. Telegram VIP (POSTED_TELEGRAM_FREE → POSTED_ALL)
"""

import os
import sys
import json
import ssl
import datetime as dt
from typing import Dict, Any, List, Optional, Tuple
from urllib.request import Request, urlopen

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
TELEGRAM_MONTHLY_CHANNEL = env("TELEGRAM_MONTHLY_CHANNEL", "-1003517970522")  # Was VIP, now Monthly
TELEGRAM_ANNUAL_CHANNEL = env("TELEGRAM_ANNUAL_CHANNEL", "-1003517970522")    # Same for now, can separate later

# 3-Tier Subscription Model
STRIPE_RAMBLER_FREE = "https://buy.stripe.com/8x2eV60fIfps3Ik3qde7m09"        # £0
STRIPE_ADVENTURER_MONTHLY = "https://buy.stripe.com/3cI14g3rU4KOdiUbWJe7m08"  # £3/month
STRIPE_NOMAD_ANNUAL = "https://buy.stripe.com/9B67sE2nQa586Uw3qde7m07"        # £30/year

# Freshness decay (prioritise newer deals)
FRESHNESS_DECAY_PER_DAY = float(env("FRESHNESS_DECAY_PER_DAY", "2.0"))


# =========================
# THEME LABELS
# =========================

THEME_LABELS = {
    "WINTER_SUN": "Winter Sun",
    "SURF": "Surf Break",
    "SNOW": "Snow",
    "FOODIE": "Foodie Break",
    "CITY_BREAKS": "City Break",
    "LONG_HAUL": "Long-Haul",
    "SURPRISE": "Surprise Deal",
}

def theme_label(theme_key: str) -> str:
    k = (theme_key or "").strip().upper()
    return THEME_LABELS.get(k, "Travel Deal")


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
# FRESHNESS DECAY
# =========================

def age_days(rec: Dict[str, str]) -> int:
    """Calculate days since deal was added."""
    date_added = rec.get("date_added", "").strip()
    if not date_added:
        return 0
    try:
        added = dt.date.fromisoformat(date_added[:10])
        return max(0, (dt.date.today() - added).days)
    except:
        return 0


def effective_score(raw_score: float, age: int) -> float:
    """Apply freshness decay to prioritise newer deals."""
    return raw_score - (FRESHNESS_DECAY_PER_DAY * float(age))


# =========================
# AI SCORING (UK English, Human tone)
# =========================

def score_deal(rec: Dict[str, str]) -> Dict[str, str]:
    """Score deal using OpenAI or fallback heuristic."""
    if not OPENAI_API_KEY:
        return score_heuristic(rec)
    
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
        
        theme = rec.get("theme", "").strip()
        origin = rec.get("origin_city", "London")
        dest = rec.get("destination_city", "")
        country = rec.get("destination_country", "")
        price = rec.get("price_gbp", "")
        
        system_prompt = """You're a UK travel editor. Write like a real person texting their mate, not a corporate robot.
Use British English. Be conversational and honest. Make people actually want to book.
Instagram = marketing (inspire them → send to Telegram). No hard selling, just good vibes."""

        user_prompt = f"""Score this honestly. Would YOU book it?

Deal:
- Theme: {theme_label(theme) if theme else 'General'}
- Route: {origin} → {dest} ({country})
- Price: £{price}
- Dates: {rec.get('outbound_date')} to {rec.get('return_date')}
- Trip: {rec.get('trip_length_days')} days
- Stops: {rec.get('stops')}

Return STRICT JSON:
{{
  "score": 1-10,
  "verdict": "GOOD" or "AVERAGE" or "POOR",
  "grading": "A" or "B" or "C",
  "caption": "Instagram caption (mate-to-mate tone, British, benefit-led, CTA to Telegram, max 180 chars)"
}}

Caption must sound like a real person wrote it. Lead with feeling, not features. No corporate waffle."""

        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.7,
            max_tokens=300,
            response_format={"type": "json_object"}
        )
        
        result = json.loads(response.choices[0].message.content)
        
        return {
            "ai_score": str(result.get("score", "5")),
            "ai_verdict": result.get("verdict", "AVERAGE").upper(),
            "ai_grading": result.get("grading", "B").upper(),
            "ai_caption": result.get("caption", "")[:700],
        }
        
    except Exception as e:
        log(f"  OpenAI failed: {e}, using heuristic")
        return score_heuristic(rec)


def score_heuristic(rec: Dict[str, str]) -> Dict[str, str]:
    """Fallback heuristic with human captions (British English)."""
    try:
        price = float(rec.get("price_gbp", "999").replace("£", ""))
        stops = int(rec.get("stops", "0"))
    except:
        return {"ai_score": "5", "ai_verdict": "AVERAGE", "ai_grading": "B", "ai_caption": ""}
    
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
    
    # Human captions (sound like a real person, British English)
    dest = rec.get("destination_city", "")
    price_str = rec.get("price_gbp", "")
    theme = theme_label(rec.get("theme", ""))
    
    # Mate-to-mate tone templates
    templates = [
        f"£{price_str} to {dest}. {theme} sorted. Check Telegram.",
        f"{dest} for £{price_str}. Not bad. Details on Telegram.",
        f"Found one: £{price_str} flights to {dest}. Link in bio.",
        f"{theme} — {dest} for £{price_str}. Full story on Telegram.",
        f"£{price_str} to {dest}. Because why not? Telegram's got details.",
    ]
    
    import random
    random.seed(rec.get("deal_id", ""))
    caption = random.choice(templates)
    
    return {
        "ai_score": str(score),
        "ai_verdict": verdict,
        "ai_grading": grading,
        "ai_caption": caption[:700],
    }


# =========================
# RENDER
# =========================

def render_deal(rec: Dict[str, str]) -> Optional[str]:
    """Call render service."""
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
            "theme": rec.get("theme", ""),
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
# INSTAGRAM
# =========================

def post_instagram(graphic_url: str, rec: Dict[str, str]) -> Optional[str]:
    """Post to Instagram."""
    if not IG_ACCESS_TOKEN or not IG_USER_ID:
        log("  No Instagram credentials, skipping")
        return None
    
    try:
        caption = rec.get("ai_caption", "").strip()
        
        if not caption:
            dest = rec.get("destination_city", "")
            price = rec.get("price_gbp", "")
            theme = theme_label(rec.get("theme", ""))
            
            caption = f"{theme} — £{price} to {dest}. Telegram's got details."
        
        # Ensure hashtags
        if "#TravelTxter" not in caption:
            caption += "\n\n#TravelTxter #CheapFlights #TravelDeals"
        
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
# TELEGRAM (UK English, human tone)
# =========================

def post_telegram(channel: str, rec: Dict[str, str], tier: str = "free") -> Optional[str]:
    """Post to Telegram with 3-tier subscription model (British English, mate-to-mate tone).
    
    Args:
        tier: "free", "monthly", or "annual"
    """
    if not TELEGRAM_BOT_TOKEN:
        log("  No Telegram token, skipping")
        return None
    
    try:
        origin = rec.get("origin_city", "London")
        dest = rec.get("destination_city", "")
        country = rec.get("destination_country", "")
        price = rec.get("price_gbp", "")
        out_date = rec.get("outbound_date", "")
        ret_date = rec.get("return_date", "")
        verdict = rec.get("ai_verdict", "")
        theme = rec.get("theme", "")
        graphic_url = rec.get("graphic_url", "")
        
        if tier == "free":
            # FREE tier (promote monthly AND annual)
            msg = f"<b>£{price} to {dest}</b>\n\n"
            msg += f"TO: {dest.upper()}\n"
            msg += f"FROM: {origin}\n\n"
            if out_date and ret_date:
                msg += f"OUT:  {out_date}\n"
                msg += f"BACK: {ret_date}\n"
            msg += "\nHeads up:\n"
            msg += "• Paid members saw this 24 hours ago\n"
            msg += "• Availability's running low\n"
            msg += "• Best deals go to subscribers first\n\n"
            msg += "<b>Want instant access?</b>\n\n"
            msg += "<b>Adventurer (Monthly)</b> — £3/month\n"
            msg += "• Deals 24 hours early\n"
            msg += "• Direct booking links\n"
            msg += "• Cancel anytime\n"
            msg += f'<a href="{STRIPE_ADVENTURER_MONTHLY}">Subscribe monthly</a>\n\n'
            msg += "<b>Nomad (Annual)</b> — £30/year\n"
            msg += "• Everything in Monthly\n"
            msg += "• Save £6 per year\n"
            msg += "• Exclusive mistake fares\n"
            msg += f'<a href="{STRIPE_NOMAD_ANNUAL}">Subscribe annually</a>'
            
        elif tier == "monthly":
            # MONTHLY tier (Adventurer - £3/month)
            msg = f"<b>ADVENTURER EARLY ACCESS</b>\n\n"
            msg += f"£{price} to {dest}"
            if country:
                msg += f", {country}"
            if theme:
                msg += f" ({theme_label(theme)})"
            msg += f"\n\n"
            msg += f"TO: {dest.upper()}\n"
            msg += f"FROM: {origin}\n\n"
            if out_date and ret_date:
                msg += f"OUT:  {out_date}\n"
                msg += f"BACK: {ret_date}\n"
            if verdict:
                msg += f"\nVERDICT: {verdict}\n"
            msg += "\nYou're seeing this 24 hours before free members."
            
        else:  # annual
            # ANNUAL tier (Nomad - £30/year)
            msg = f"<b>NOMAD EARLY ACCESS</b>\n\n"
            msg += f"£{price} to {dest}"
            if country:
                msg += f", {country}"
            if theme:
                msg += f" ({theme_label(theme)})"
            msg += f"\n\n"
            msg += f"TO: {dest.upper()}\n"
            msg += f"FROM: {origin}\n\n"
            if out_date and ret_date:
                msg += f"OUT:  {out_date}\n"
                msg += f"BACK: {ret_date}\n"
            if verdict:
                msg += f"\nVERDICT: {verdict}\n"
            msg += "\nYou're seeing this 24 hours before free members."
        
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
                "disable_web_page_preview": False,
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
    log(f"Freshness decay: {FRESHNESS_DECAY_PER_DAY}/day")
    log("=" * 60)
    
    if not SPREADSHEET_ID:
        log("❌ Missing SPREADSHEET_ID")
        return 1
    
    ws = get_ws()
    log(f"✅ Connected: {ws.title}")
    
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
    
    # ============================================================
    # STAGE 1: SCORE & SELECT BEST NEW DEAL
    # ============================================================
    
    new_candidates: List[Tuple[int, Dict[str, str], float]] = []
    
    for row_idx in range(1, len(rows)):
        row = rows[row_idx]
        row_num = row_idx + 1
        
        if status_idx >= len(row):
            continue
        
        current_status = row[status_idx].strip().upper()
        rec = {h: (row[hmap[h]] if hmap[h] < len(row) else "") for h in headers}
        
        if current_status == "NEW":
            raw_score = float(rec.get("ai_score", "0") or "0")
            
            # Score if not scored yet
            if raw_score == 0:
                deal_id = rec.get("deal_id", "")
                dest = rec.get("destination_city", "")
                theme = rec.get("theme", "")
                log(f"\n📊 Scoring NEW deal row {row_num} ({dest}, {theme})...")
                
                score_result = score_deal(rec)
                
                updates = {
                    "ai_score": score_result["ai_score"],
                    "ai_verdict": score_result["ai_verdict"],
                    "ai_grading": score_result["ai_grading"],
                    "ai_caption": score_result.get("ai_caption", ""),
                    "scored_timestamp": dt.datetime.utcnow().isoformat() + "Z",
                }
                
                update_cells(ws, row_num, headers, updates)
                log(f"   Scored: {score_result['ai_score']}/10 ({score_result['ai_verdict']})")
                
                rec.update(updates)
                raw_score = float(score_result["ai_score"])
            
            # Apply freshness decay
            age = age_days(rec)
            eff_score = effective_score(raw_score, age)
            
            new_candidates.append((row_num, rec, eff_score))
            log(f"   Row {row_num}: score={raw_score}, age={age}d, effective={eff_score:.1f}")
    
    # Promote BEST deal
    if new_candidates:
        new_candidates.sort(key=lambda x: x[2], reverse=True)
        best_row, best_rec, best_eff = new_candidates[0]
        
        log(f"\n✅ BEST DEAL: Row {best_row} (effective={best_eff:.1f})")
        log(f"   Destination: {best_rec.get('destination_city', '')}")
        log(f"   Theme: {best_rec.get('theme', '')}")
        log(f"   Promoting to READY_TO_POST...")
        
        update_cells(ws, best_row, headers, {STATUS_COLUMN: "READY_TO_POST"})
        return 0
    
    # ============================================================
    # OTHER STAGES
    # ============================================================
    
    for row_idx in range(1, len(rows)):
        row = rows[row_idx]
        row_num = row_idx + 1
        
        if status_idx >= len(row):
            continue
        
        current_status = row[status_idx].strip().upper()
        rec = {h: (row[hmap[h]] if hmap[h] < len(row) else "") for h in headers}
        deal_id = rec.get("deal_id", "")
        
        if current_status == "READY_TO_POST":
            log(f"\n🎨 Rendering row {row_num}...")
            graphic_url = render_deal(rec)
            
            if graphic_url:
                update_cells(ws, row_num, headers, {
                    STATUS_COLUMN: "READY_TO_PUBLISH",
                    "graphic_url": graphic_url,
                    "rendered_timestamp": dt.datetime.utcnow().isoformat() + "Z",
                })
                log(f"✅ Rendered")
            
            return 0
        
        elif current_status == "READY_TO_PUBLISH":
            log(f"\n📸 Instagram posting row {row_num}...")
            graphic_url = rec.get("graphic_url", "")
            
            if graphic_url:
                media_id = post_instagram(graphic_url, rec)
                if media_id:
                    update_cells(ws, row_num, headers, {
                        STATUS_COLUMN: "POSTED_INSTAGRAM",
                        "ig_media_id": media_id,
                        "ig_published_timestamp": dt.datetime.utcnow().isoformat() + "Z",
                    })
                    log(f"✅ Posted to Instagram")
            
            return 0
        
        elif current_status == "POSTED_INSTAGRAM":
            log(f"\n📱 Telegram FREE posting row {row_num}...")
            
            msg_id = post_telegram(TELEGRAM_FREE_CHANNEL, rec, tier="free")
            if msg_id:
                update_cells(ws, row_num, headers, {
                    STATUS_COLUMN: "POSTED_TELEGRAM_FREE",
                    "tg_free_message_id": msg_id,
                    "tg_free_timestamp": dt.datetime.utcnow().isoformat() + "Z",
                })
                log(f"✅ Posted to Telegram FREE (Rambler)")
            
            return 0
        
        elif current_status == "POSTED_TELEGRAM_FREE":
            log(f"\n💳 Telegram MONTHLY posting row {row_num}...")
            
            msg_id = post_telegram(TELEGRAM_MONTHLY_CHANNEL, rec, tier="monthly")
            if msg_id:
                update_cells(ws, row_num, headers, {
                    STATUS_COLUMN: "POSTED_TELEGRAM_MONTHLY",
                    "tg_monthly_message_id": msg_id,
                    "tg_monthly_timestamp": dt.datetime.utcnow().isoformat() + "Z",
                })
                log(f"✅ Posted to Telegram MONTHLY (Adventurer - £3/month)")
            
            return 0
        
        elif current_status == "POSTED_TELEGRAM_MONTHLY":
            log(f"\n🌍 Telegram ANNUAL posting row {row_num}...")
            
            msg_id = post_telegram(TELEGRAM_ANNUAL_CHANNEL, rec, tier="annual")
            if msg_id:
                update_cells(ws, row_num, headers, {
                    STATUS_COLUMN: "POSTED_ALL",
                    "tg_annual_message_id": msg_id,
                    "tg_annual_timestamp": dt.datetime.utcnow().isoformat() + "Z",
                    "published_timestamp": dt.datetime.utcnow().isoformat() + "Z",
                })
                log(f"✅ Posted to Telegram ANNUAL (Nomad - £30/year)")
                log(f"🎉 PIPELINE COMPLETE")
            
            return 0
    
    log("\nNo deals to process")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        log(f"❌ FATAL: {e}")
        import traceback
        log(traceback.format_exc())
        sys.exit(1)
