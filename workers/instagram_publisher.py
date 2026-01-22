# workers/instagram_publisher.py
# V4.8 - CORRECTED to match exact schematic
# Instagram posts with country flag, phrase_bank, and exact format

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
    sh = gc.open_by_key(env("SPREADSHEET_ID") or env("SHEET_ID"))
    ws = sh.worksheet(env("RAW_DEALS_TAB", "RAW_DEALS"))

    values = ws.get_all_values()
    headers = values[0]
    h = {k: i for i, k in enumerate(headers)}

    for i, r in enumerate(values[1:], start=2):
        if r[h["status"]] != "READY_TO_PUBLISH":
            continue

        row = {headers[j]: r[j] for j in range(len(headers))}

        country = row.get("destination_country", "")
        city = row.get("destination_city", "")
        origin = row.get("origin_city", "")
        price = row.get("price_gbp", "")
        outbound = row.get("outbound_date", "")
        return_date = row.get("return_date", "")
        phrase = phrase_from_row(row)
        image_url = row.get("graphic_url", "")

        # Get country flag (flags or globe only)
        flag = get_country_flag(country)

        # Build caption (UPDATED COPY ONLY)
        # - human tone
        # - feature -> benefit
        # - explains VIP-first timing without hype
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
        create = requests.post(
            f"https://graph.facebook.com/v20.0/{env('IG_USER_ID')}/media",
            data={
                "image_url": image_url,
                "caption": caption,
                "access_token": env("IG_ACCESS_TOKEN"),
            },
        ).json()

        cid = create.get("id")
        if not cid:
            raise RuntimeError(f"Instagram media creation failed: {create}")

        time.sleep(2)

        # Publish Instagram media
        pub = requests.post(
            f"https://graph.facebook.com/v20.0/{env('IG_USER_ID')}/media_publish",
            data={
                "creation_id": cid,
                "access_token": env("IG_ACCESS_TOKEN"),
            },
        ).json()

        if "id" not in pub:
            raise RuntimeError(f"Instagram publish failed: {pub}")

        # Update status
        ws.update_cell(i, h["status"] + 1, "POSTED_INSTAGRAM")
        ws.update_cell(i, h["posted_instagram_at"] + 1, dt.datetime.utcnow().isoformat() + "Z")

        print(f"✅ Published to Instagram: {city} £{price}")
        return 0

    print("No deals with status READY_TO_PUBLISH found")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
