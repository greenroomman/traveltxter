# workers/instagram_publisher.py
# UPDATED VERSION - Publishing Gate Implementation
#
# Locked constraints (DO NOT VIOLATE):
# - DO NOT change how Instagram posts look (caption lines) or how graphics are used (image_url from graphic_url).
# - Instagram is marketing-only: it MUST NOT write status.
# - It SHOULD timestamp posted_instagram_at (Column AA) on successful publish.
# - Idempotent: if posted_instagram_at already set, skip.
#
# CHANGES FROM PREVIOUS VERSION:
# - NOW READS FROM RAW_DEALS_VIEW (where instagram_ok gate lives)
# - Filters on instagram_ok=TRUE (primary gate - prevents £400+ beach breaks)
# - Logs blocked deals (monitoring/debugging)
# - Still writes timestamp back to RAW_DEALS (source table)

import os
import json
import time
import datetime as dt
import requests
import gspread
from google.oauth2.service_account import Credentials


def env(k, d=""):
    return (os.getenv(k, d) or "").strip()


def _sa_creds():
    raw = env("GCP_SA_JSON_ONE_LINE") or env("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
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
    # Common country to flag emoji mapping
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


def main():
    gc = gspread.authorize(_sa_creds())
    sheet_id = env("SPREADSHEET_ID") or env("SHEET_ID")
    if not sheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")
    sh = gc.open_by_key(sheet_id)
    
    # CRITICAL CHANGE: Read from RAW_DEALS_VIEW (where instagram_ok gate lives)
    ws_view = sh.worksheet("RAW_DEALS_VIEW")
    ws_source = sh.worksheet("RAW_DEALS")  # Still need this for writing timestamps

    values = ws_view.get_all_values()
    if not values or len(values) < 2:
        print("No rows found in RAW_DEALS_VIEW")
        return 0

    headers = values[0]
    h = {k: i for i, k in enumerate(headers)}

    # Required headers (from RAW_DEALS_VIEW)
    for col in [
        "deal_id",
        "status",
        "graphic_url",
        "destination_country",
        "destination_city",
        "origin_city",
        "price_gbp",
        "outbound_date",
        "return_date",
        "posted_instagram_at",
        "instagram_ok",  # NEW: Publishing gate
        "block_reason",  # NEW: For logging
    ]:
        if col not in h:
            raise RuntimeError(f"RAW_DEALS_VIEW missing required header: {col}")

    ig_user_id = env("IG_USER_ID")
    ig_access_token = env("IG_ACCESS_TOKEN")
    graph_api_version = env("GRAPH_API_VERSION", "v20.0")

    if not ig_user_id:
        raise RuntimeError("Missing IG_USER_ID")
    if not ig_access_token:
        raise RuntimeError("Missing IG_ACCESS_TOKEN")

    # Collect stats for logging
    total_ready = 0
    total_passed_gate = 0
    total_already_posted = 0
    total_published = 0
    blocked_deals = []

    print("=" * 70)
    print("📣 Instagram Publisher - Gate Enforcement Enabled")
    print("=" * 70)

    for i, r in enumerate(values[1:], start=2):
        # First check: Must be READY_TO_PUBLISH
        if r[h["status"]] != "READY_TO_PUBLISH":
            continue
        
        total_ready += 1
        
        row = {headers[j]: r[j] for j in range(len(headers))}
        deal_id = row.get("deal_id", "unknown")
        
        # CRITICAL NEW CHECK: instagram_ok gate
        instagram_ok = row.get("instagram_ok", "").strip().upper()
        if instagram_ok != "TRUE":
            # Deal blocked by gate - log it
            block_reason = row.get("block_reason", "Unknown reason")
            price = row.get("price_gbp", "?")
            theme = row.get("theme", "unknown")
            dest = row.get("destination_city", "unknown")
            
            blocked_deals.append({
                "deal_id": deal_id,
                "dest": dest,
                "theme": theme,
                "price": price,
                "reason": block_reason,
            })
            continue
        
        total_passed_gate += 1

        # Idempotency: if already posted, skip
        if row.get("posted_instagram_at", "").strip():
            total_already_posted += 1
            continue

        # Extract deal details
        country = row.get("destination_country", "")
        city = row.get("destination_city", "")
        origin = row.get("origin_city", "")
        price = row.get("price_gbp", "")
        outbound = row.get("outbound_date", "")
        return_date = row.get("return_date", "")
        phrase = phrase_from_row(row)
        image_url = row.get("graphic_url", "")

        if not image_url:
            print(f"⚠️  Skipping {deal_id}: missing graphic_url")
            continue

        # Get country flag (flags or globe only)
        flag = get_country_flag(country)

        # Build caption (DO NOT EDIT COPY OR STRUCTURE - LOCKED CONSTRAINT)
        caption = "\n".join([
            f"{country} {flag}",
            "",
            f"London to {city} from £{price}",
            f"Out: {outbound}",
            f"Return: {return_date}",
            "",
            phrase,
            "",
            "VIP members saw this first. We post here later, and the free channel gets it after that.",
            "",
            "Link in bio.",
        ]).strip()

        # Create Instagram media
        try:
            create = requests.post(
                f"https://graph.facebook.com/{graph_api_version}/{ig_user_id}/media",
                data={
                    "image_url": image_url,
                    "caption": caption,
                    "access_token": ig_access_token,
                },
            ).json()

            cid = create.get("id")
            if not cid:
                raise RuntimeError(f"Instagram media creation failed: {create}")

            time.sleep(2)

            # Publish Instagram media
            pub = requests.post(
                f"https://graph.facebook.com/{graph_api_version}/{ig_user_id}/media_publish",
                data={
                    "creation_id": cid,
                    "access_token": ig_access_token,
                },
            ).json()

            if "id" not in pub:
                raise RuntimeError(f"Instagram publish failed: {pub}")

            # Marketing-only writeback: timestamp to RAW_DEALS (source table)
            # We need to find the matching row in RAW_DEALS by deal_id
            source_values = ws_source.get_all_values()
            source_headers = source_values[0]
            source_h = {k: i for i, k in enumerate(source_headers)}
            
            for source_i, source_r in enumerate(source_values[1:], start=2):
                if source_r[source_h["deal_id"]] == deal_id:
                    ws_source.update_cell(
                        source_i, 
                        source_h["posted_instagram_at"] + 1, 
                        dt.datetime.utcnow().isoformat() + "Z"
                    )
                    break

            print(f"✅ Published: {deal_id} - {city} £{price}")
            total_published += 1
            
            # Only publish one deal per run (rate limiting)
            break

        except Exception as e:
            print(f"❌ Failed to publish {deal_id}: {e}")
            raise

    # Summary logging
    print("=" * 70)
    print("📊 Publishing Summary")
    print("=" * 70)
    print(f"Deals READY_TO_PUBLISH: {total_ready}")
    print(f"Passed instagram_ok gate: {total_passed_gate}")
    print(f"Already posted (skipped): {total_already_posted}")
    print(f"Published this run: {total_published}")
    print(f"Blocked by gate: {len(blocked_deals)}")
    
    if blocked_deals:
        print("\n⚠️  BLOCKED DEALS (instagram_ok=FALSE):")
        for deal in blocked_deals[:10]:  # Show first 10
            print(f"   - {deal['deal_id']}: {deal['dest']} {deal['theme']} £{deal['price']}")
            print(f"     Reason: {deal['reason']}")
        if len(blocked_deals) > 10:
            print(f"   ... and {len(blocked_deals)-10} more")
    
    if total_published == 0:
        print("\n⚠️  No deals published this run")
        if total_passed_gate == 0:
            print("   → No deals passed instagram_ok gate")
        elif total_already_posted == total_passed_gate:
            print("   → All deals already posted")
    
    print("=" * 70)
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
