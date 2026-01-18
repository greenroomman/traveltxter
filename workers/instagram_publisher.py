# workers/telegram_publisher.py
# V4.8 - CORRECTED to match exact schematic
# Two-stage publisher: VIP first, then FREE with upgrade CTA

import os
import json
import datetime as dt
import requests
import gspread
from google.oauth2.service_account import Credentials


def env(k, d=""):
    return (os.getenv(k, d) or "").strip()


def _sa_creds():
    raw = env("GCP_SA_JSON_ONE_LINE") or env("GCP_SA_JSON")
    try:
        info = json.loads(raw)
    except json.JSONDecodeError:
        info = json.loads(raw.replace("\\n", "\n"))
    return Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )


def get_country_flag(country_name):
    """Get emoji flag for country name"""
    flag_map = {
        "Iceland": "🇮🇸",
        "Spain": "🇪🇸",
        "Portugal": "🇵🇹",
        "Greece": "🇬🇷",
        "Turkey": "🇹🇷",
        "Morocco": "🇲🇦",
        "Egypt": "🇪🇬",
        "UAE": "🇦🇪",
        "United Arab Emirates": "🇦🇪",
        "Tunisia": "🇹🇳",
        "Cape Verde": "🇨🇻",
        "Gambia": "🇬🇲",
        "Jordan": "🇯🇴",
        "Madeira": "🇵🇹",
        "Canary Islands": "🇪🇸",
        "Tenerife": "🇪🇸",
        "Lanzarote": "🇪🇸",
        "Fuerteventura": "🇪🇸",
        "Gran Canaria": "🇪🇸",
        "Croatia": "🇭🇷",
        "Italy": "🇮🇹",
        "Cyprus": "🇨🇾",
        "Malta": "🇲🇹",
        "Bulgaria": "🇧🇬",
        "Barbados": "🇧🇧",
        "Jamaica": "🇯🇲",
        "Antigua": "🇦🇬",
        "St Lucia": "🇱🇨",
        "Mexico": "🇲🇽",
        "Thailand": "🇹🇭",
        "Indonesia": "🇮🇩",
        "Bali": "🇮🇩",
        "Malaysia": "🇲🇾",
        "Maldives": "🇲🇻",
        "Mauritius": "🇲🇺",
        "Seychelles": "🇸🇨",
        "Azores": "🇵🇹",
        "Switzerland": "🇨🇭",
        "Austria": "🇦🇹",
        "France": "🇫🇷",
        "Norway": "🇳🇴",
        "Sweden": "🇸🇪",
        "Finland": "🇫🇮",
        "Czech Republic": "🇨🇿",
        "Hungary": "🇭🇺",
        "Poland": "🇵🇱",
        "Germany": "🇩🇪",
        "Belgium": "🇧🇪",
        "Netherlands": "🇳🇱",
        "Denmark": "🇩🇰",
        "Estonia": "🇪🇪",
        "Latvia": "🇱🇻",
        "Lithuania": "🇱🇹",
        "Romania": "🇷🇴",
        "Israel": "🇮🇱",
        "USA": "🇺🇸",
        "United States": "🇺🇸",
        "Canada": "🇨🇦",
        "Qatar": "🇶🇦",
        "South Africa": "🇿🇦",
        "Singapore": "🇸🇬",
        "Hong Kong": "🇭🇰",
        "India": "🇮🇳",
        "Japan": "🇯🇵",
        "South Korea": "🇰🇷",
        "China": "🇨🇳",
        "Australia": "🇦🇺",
        "New Zealand": "🇳🇿",
        "Brazil": "🇧🇷",
        "Argentina": "🇦🇷",
        "Colombia": "🇨🇴",
        "Slovakia": "🇸🇰",
        "Bosnia": "🇧🇦",
        "North Macedonia": "🇲🇰",
        "Armenia": "🇦🇲",
        "Georgia": "🇬🇪",
    }
    return flag_map.get(country_name, "🌍")


def phrase_from_row(row):
    """Get phrase_used first, fallback to phrase_bank"""
    return (row.get("phrase_used") or row.get("phrase_bank") or "").strip()


def tg_send(token, chat_id, text, disable_preview=True):
    """Send Telegram message"""
    r = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": str(disable_preview).lower(),
        },
    )
    if not r.json().get("ok"):
        raise RuntimeError(f"Telegram send failed: {r.text}")


def publish_vip(ws, i, headers, row, h):
    """Publish to VIP channel with booking link"""
    country = row.get("destination_country", "")
    city = row.get("destination_city", "")
    origin = row.get("origin_city", "")
    price = row.get("price_gbp", "")
    outbound = row.get("outbound_date", "")
    return_date = row.get("return_date", "")
    phrase = phrase_from_row(row)
    booking_link = row.get("booking_link_vip", "")
    
    # Get country flag
    flag = get_country_flag(country)
    
    # Build VIP message exactly as schematic
    msg = "\n".join([
        f"£{price} to {country} {flag}",
        f"TO: {city}",
        f"FROM: {origin}",
        f"OUT: {outbound}",
        f"BACK: {return_date}",
        "",
        phrase,
        "",
        f'<a href="{booking_link}">Book now</a>',
    ]).strip()
    
    # Send to VIP channel
    tg_send(
        env("TELEGRAM_BOT_TOKEN_VIP"),
        env("TELEGRAM_CHANNEL_VIP"),
        msg,
        disable_preview=True
    )
    
    # Update status to POSTED_TELEGRAM_VIP
    ws.update_cell(i, h["status"] + 1, "POSTED_TELEGRAM_VIP")
    ws.update_cell(i, h["posted_telegram_vip_at"] + 1, dt.datetime.utcnow().isoformat() + "Z")
    
    print(f"✅ Published to Telegram VIP: {city} £{price}")


def publish_free(ws, i, headers, row, h):
    """Publish to FREE channel with upgrade CTA (24h after VIP)"""
    country = row.get("destination_country", "")
    city = row.get("destination_city", "")
    origin = row.get("origin_city", "")
    price = row.get("price_gbp", "")
    outbound = row.get("outbound_date", "")
    return_date = row.get("return_date", "")
    phrase = phrase_from_row(row)
    
    # Get country flag
    flag = get_country_flag(country)
    
    # Subscription links (these should be environment variables or config)
    monthly_link = env("SUBSCRIPTION_LINK_MONTHLY", "https://buy.stripe.com/monthly")
    yearly_link = env("SUBSCRIPTION_LINK_YEARLY", "https://buy.stripe.com/yearly")
    
    # Build FREE message exactly as schematic
    msg = "\n".join([
        f"£{price} to {country} {flag}",
        f"TO: {city}",
        f"FROM: {origin}",
        f"OUT: {outbound}",
        f"BACK: {return_date}",
        "",
        phrase,
        "",
        "<b>Want instant access?</b>",
        "Join TravelTxter for early access",
        "• VIP members saw this 24 hours ago",
        "• Direct booking links",
        "• We find exclusive mistake fares",
        "• Subscription: £3 p/m or £30 p/a",
        "",
        f'<a href="{monthly_link}">Upgrade now (Monthly £3)</a> | <a href="{yearly_link}">Yearly £30</a>',
    ]).strip()
    
    # Send to FREE channel
    tg_send(
        env("TELEGRAM_BOT_TOKEN"),
        env("TELEGRAM_CHANNEL"),
        msg,
        disable_preview=True
    )
    
    # Update status to POSTED_TELEGRAM_FREE
    ws.update_cell(i, h["status"] + 1, "POSTED_TELEGRAM_FREE")
    ws.update_cell(i, h["posted_telegram_free_at"] + 1, dt.datetime.utcnow().isoformat() + "Z")
    
    print(f"✅ Published to Telegram FREE: {city} £{price}")


def main():
    gc = gspread.authorize(_sa_creds())
    sh = gc.open_by_key(env("SPREADSHEET_ID") or env("SHEET_ID"))
    ws = sh.worksheet(env("RAW_DEALS_TAB", "RAW_DEALS"))

    values = ws.get_all_values()
    headers = values[0]
    h = {k: i for i, k in enumerate(headers)}

    # STAGE 1: Publish to VIP (status = POSTED_INSTAGRAM)
    for i, r in enumerate(values[1:], start=2):
        if r[h["status"]] == "POSTED_INSTAGRAM":
            row = {headers[j]: r[j] for j in range(len(headers))}
            publish_vip(ws, i, headers, row, h)
            return 0  # Exit after publishing one deal

    # STAGE 2: Publish to FREE (status = POSTED_TELEGRAM_VIP, 24h later)
    for i, r in enumerate(values[1:], start=2):
        if r[h["status"]] == "POSTED_TELEGRAM_VIP":
            row = {headers[j]: r[j] for j in range(len(headers))}
            
            # Check if 24 hours have passed since VIP post
            vip_posted_at = row.get("posted_telegram_vip_at", "")
            if vip_posted_at:
                try:
                    vip_time = dt.datetime.fromisoformat(vip_posted_at.replace("Z", "+00:00"))
                    now = dt.datetime.now(dt.timezone.utc)
                    hours_elapsed = (now - vip_time).total_seconds() / 3600
                    
                    if hours_elapsed >= 24:
                        publish_free(ws, i, headers, row, h)
                        return 0  # Exit after publishing one deal
                    else:
                        print(f"⏳ Deal not ready for FREE (only {hours_elapsed:.1f}h elapsed, need 24h)")
                except Exception as e:
                    print(f"⚠️ Error checking VIP timestamp: {e}")
                    # If timestamp parsing fails, publish anyway
                    publish_free(ws, i, headers, row, h)
                    return 0

    print("No deals ready to publish")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
