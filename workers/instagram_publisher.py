# workers/instagram_publisher.py
# FINAL HARDENED VERSION — RDV SAFE / SCHEMA TOLERANT
#
# LOCKED CONSTRAINTS (DO NOT VIOLATE):
# - DO NOT change caption wording or structure
# - DO NOT change graphic usage (graphic_url)
# - Instagram is marketing-only
# - MUST timestamp posted_instagram_at in RAW_DEALS
# - MUST be idempotent
# - MUST enforce instagram_ok gate
#
# This version:
# - Reads from RAW_DEALS_VIEW
# - Writes timestamps to RAW_DEALS
# - Tolerates missing optional columns (block_reason, posted_instagram_at in RDV)

import os
import json
import time
import datetime as dt
import requests
import gspread
from google.oauth2.service_account import Credentials


# ------------------------
# Utilities
# ------------------------

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


def phrase_from_row(row):
    return (row.get("phrase_used") or row.get("phrase_bank") or "").strip()


def get_country_flag(country):
    if not country:
        return "🌍"
    return {
        "Iceland": "🇮🇸",
        "Spain": "🇪🇸",
        "Portugal": "🇵🇹",
        "Greece": "🇬🇷",
        "Turkey": "🇹🇷",
        "Morocco": "🇲🇦",
        "Jordan": "🇯🇴",
        "Canada": "🇨🇦",
        "USA": "🇺🇸",
        "Indonesia": "🇮🇩",
        "Thailand": "🇹🇭",
        "Japan": "🇯🇵",
        "Australia": "🇦🇺",
        "France": "🇫🇷",
        "Italy": "🇮🇹",
        "Germany": "🇩🇪",
    }.get(country, "🌍")


# ------------------------
# Main
# ------------------------

def main():
    gc = gspread.authorize(_sa_creds())

    sheet_id = env("SPREADSHEET_ID") or env("SHEET_ID")
    if not sheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")

    sh = gc.open_by_key(sheet_id)

    ws_view = sh.worksheet("RAW_DEALS_VIEW")
    ws_raw = sh.worksheet("RAW_DEALS")

    values = ws_view.get_all_values()
    if len(values) < 2:
        print("No rows in RAW_DEALS_VIEW")
        return 0

    headers = values[0]
    h = {k: i for i, k in enumerate(headers)}

    # ---- REQUIRED RDV HEADERS (HARD FAIL) ----
    required = [
        "deal_id",
        "status",
        "graphic_url",
        "destination_country",
        "destination_city",
        "origin_city",
        "price_gbp",
        "outbound_date",
        "return_date",
        "instagram_ok",
    ]

    for col in required:
        if col not in h:
            raise RuntimeError(f"RAW_DEALS_VIEW missing required header: {col}")

    # ---- OPTIONAL HEADERS (SOFT) ----
    has_block_reason = "block_reason" in h

    ig_user_id = env("IG_USER_ID")
    ig_access_token = env("IG_ACCESS_TOKEN")
    api_ver = env("GRAPH_API_VERSION", "v20.0")

    if not ig_user_id or not ig_access_token:
        raise RuntimeError("Missing Instagram credentials")

    raw_vals = ws_raw.get_all_values()
    raw_headers = raw_vals[0]
    raw_h = {k: i for i, k in enumerate(raw_headers)}

    if "posted_instagram_at" not in raw_h:
        raise RuntimeError("RAW_DEALS missing required column: posted_instagram_at")

    print("=" * 70)
    print("📣 Instagram Publisher — RDV Gate Enforced")
    print("=" * 70)

    published = 0
    blocked = 0
    ready = 0

    for idx, r in enumerate(values[1:], start=2):
        if r[h["status"]] != "READY_TO_PUBLISH":
            continue

        ready += 1

        instagram_ok = r[h["instagram_ok"]].strip().upper()
        if instagram_ok != "TRUE":
            blocked += 1
            if has_block_reason:
                print(
                    f"⛔ BLOCKED {r[h['deal_id']]} — "
                    f"{r[h['destination_city']]} £{r[h['price_gbp']]} — "
                    f"{r[h['block_reason']]}"
                )
            continue

        deal_id = r[h["deal_id"]]

        # Idempotency check (RAW_DEALS)
        for raw_i, raw_r in enumerate(raw_vals[1:], start=2):
            if raw_r[raw_h["deal_id"]] == deal_id:
                if raw_r[raw_h["posted_instagram_at"]]:
                    print(f"↩️  Skipping already posted: {deal_id}")
                    break
                else:
                    # Publish
                    country = r[h["destination_country"]]
                    city = r[h["destination_city"]]
                    price = r[h["price_gbp"]]
                    outbound = r[h["outbound_date"]]
                    ret = r[h["return_date"]]
                    phrase = phrase_from_row(dict(zip(headers, r)))
                    image_url = r[h["graphic_url"]]

                    flag = get_country_flag(country)

                    caption = "\n".join([
                        f"{country} {flag}",
                        "",
                        f"London to {city} from £{price}",
                        f"Out: {outbound}",
                        f"Return: {ret}",
                        "",
                        phrase,
                        "",
                        "VIP members saw this first. We post here later, and the free channel gets it after that.",
                        "",
                        "Link in bio.",
                    ]).strip()

                    create = requests.post(
                        f"https://graph.facebook.com/{api_ver}/{ig_user_id}/media",
                        data={
                            "image_url": image_url,
                            "caption": caption,
                            "access_token": ig_access_token,
                        },
                    ).json()

                    cid = create.get("id")
                    if not cid:
                        raise RuntimeError(f"IG create failed: {create}")

                    time.sleep(2)

                    pub = requests.post(
                        f"https://graph.facebook.com/{api_ver}/{ig_user_id}/media_publish",
                        data={
                            "creation_id": cid,
                            "access_token": ig_access_token,
                        },
                    ).json()

                    if "id" not in pub:
                        raise RuntimeError(f"IG publish failed: {pub}")

                    ws_raw.update_cell(
                        raw_i,
                        raw_h["posted_instagram_at"] + 1,
                        dt.datetime.utcnow().isoformat() + "Z",
                    )

                    print(f"✅ Published {deal_id} — {city} £{price}")
                    published += 1
                    return 0  # one post per run

    print("=" * 70)
    print(f"READY_TO_PUBLISH: {ready}")
    print(f"BLOCKED BY GATE: {blocked}")
    print(f"PUBLISHED THIS RUN: {published}")
    print("=" * 70)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
