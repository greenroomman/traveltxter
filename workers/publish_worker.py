#!/usr/bin/env python3
"""
Traveltxter V3_beta_b_final — Instagram Publish Worker (CLEAN + PIPELINE-CORRECT)

✅ Reads from RAW_DEALS (or whatever RAW_DEALS_TAB is set to)
✅ Filters using raw_status == READY_TO_POST (NOT workflow)
✅ Requires graphic_url present (so we only post rendered deals)
✅ Posts to Instagram via Graph API
✅ Writes back: ig_status, ig_media_id, ig_published_timestamp
✅ Optionally promotes raw_status -> POSTED_INSTAGRAM (default ON)

Environment variables required:
- SHEET_ID
- GCP_SA_JSON
- FB_ACCESS_TOKEN
- IG_USER_ID

Optional:
- RAW_DEALS_TAB (default RAW_DEALS)
- GRAPH_VERSION (default v19.0)
- DRY_RUN (true/false)
- IG_REQUIRED_STATUS (default READY_TO_POST)
- IG_POSTED_STATUS (default POSTED_INSTAGRAM)
- RAW_STATUS_COLUMN (default raw_status)
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
        logging.FileHandler("logs/publish_worker.log", mode="a"),
    ],
)

logger = logging.getLogger(__name__)


# =============================================================================
# Config / Env
# =============================================================================

DRY_RUN = os.getenv("DRY_RUN", "false").strip().lower() == "true"

WORKER_ID = os.getenv("WORKER_ID", "publish_worker")
GITHUB_RUN_ID = os.getenv("GITHUB_RUN_ID", "local")
GITHUB_RUN_NUMBER = os.getenv("GITHUB_RUN_NUMBER", "0")

RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS")
RAW_STATUS_COLUMN = os.getenv("RAW_STATUS_COLUMN", "raw_status").strip()
IG_STATUS_COLUMN = os.getenv("IG_STATUS_COLUMN", "ig_status").strip()

IG_REQUIRED_STATUS = os.getenv("IG_REQUIRED_STATUS", "READY_TO_POST").strip().upper()
IG_POSTED_STATUS = os.getenv("IG_POSTED_STATUS", "POSTED_INSTAGRAM").strip().upper()

GRAPH_VERSION = os.getenv("GRAPH_VERSION", "v19.0").strip()
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
# Instagram posting
# =============================================================================

def format_caption(deal: Dict[str, str]) -> str:
    ai_caption = (deal.get("ai_caption") or "").strip()
    if ai_caption:
        return ai_caption

    origin = (deal.get("origin_city") or "UK").strip()
    destination = (deal.get("destination_city") or "Somewhere").strip()
    price = (deal.get("price_gbp") or "").strip()
    out_date = (deal.get("outbound_date") or "").strip()

    if price and not price.startswith("£"):
        price = f"£{price}"
    if not price:
        price = "£???"

    return (
        f"✈️ {origin} → {destination}\n\n"
        f"💰 From {price}\n"
        f"📅 {out_date or 'Flexible dates'}\n\n"
        "🔥 Limited availability — book fast!\n\n"
        "Want ALL the best deals? Join our Telegram (link in bio) 👆\n\n"
        "#TravelTxter #CheapFlights #TravelDeals #BudgetTravel"
    ).strip()


def post_to_instagram(image_url: str, caption: str) -> Tuple[bool, str]:
    if DRY_RUN:
        logger.info("🧪 [DRY RUN] Would post to Instagram")
        return True, "dry_run_ig_123"

    access_token = os.getenv("FB_ACCESS_TOKEN")
    ig_user_id = os.getenv("IG_USER_ID")

    if not access_token or not ig_user_id:
        return False, "Missing FB_ACCESS_TOKEN or IG_USER_ID"

    # 1) Create container
    container_endpoint = f"https://graph.facebook.com/{GRAPH_VERSION}/{ig_user_id}/media"
    container_data = {
        "image_url": image_url,
        "caption": caption,
        "access_token": access_token,
    }

    r1 = requests.post(container_endpoint, data=container_data, timeout=30)
    if r1.status_code != 200:
        try:
            err = r1.json().get("error", {}).get("message", r1.text)
        except Exception:
            err = r1.text
        return False, f"Container error: {err}"

    container_id = r1.json().get("id")
    if not container_id:
        return False, "Container created but no id returned"

    # 2) Publish container
    publish_endpoint = f"https://graph.facebook.com/{GRAPH_VERSION}/{ig_user_id}/media_publish"
    publish_data = {
        "creation_id": container_id,
        "access_token": access_token,
    }

    r2 = requests.post(publish_endpoint, data=publish_data, timeout=30)
    if r2.status_code != 200:
        try:
            err = r2.json().get("error", {}).get("message", r2.text)
        except Exception:
            err = r2.text
        return False, f"Publish error: {err}"

    post_id = r2.json().get("id", "unknown")
    return True, post_id


# =============================================================================
# Main
# =============================================================================

def main() -> int:
    logger.info("\n" + "=" * 60)
    logger.info("🚀 TravelTxter Instagram Publisher Starting")
    logger.info("=" * 60)
    logger.info(f"⏰ Timestamp: {datetime.utcnow().isoformat()}Z")
    logger.info(f"🆔 Worker ID: {WORKER_ID}")
    logger.info(f"📋 Run: #{GITHUB_RUN_NUMBER} (ID: {GITHUB_RUN_ID})")
    logger.info(f"🧪 Dry Run: {DRY_RUN}")
    logger.info(f"📄 Tab: {RAW_DEALS_TAB}")
    logger.info(f"🔎 Filter: {RAW_STATUS_COLUMN} == {IG_REQUIRED_STATUS}")
    logger.info(f"✅ Promote on success: {RAW_STATUS_COLUMN} -> {IG_POSTED_STATUS}")
    logger.info("=" * 60 + "\n")

    # Validate env
    required_vars = ["SHEET_ID", "GCP_SA_JSON", "FB_ACCESS_TOKEN", "IG_USER_ID"]
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

        # Required columns (RAW_DEALS schema)
        required_cols = [
            "deal_id",
            "origin_city",
            "destination_city",
            "price_gbp",
            "outbound_date",
            "graphic_url",
            RAW_STATUS_COLUMN,
            IG_STATUS_COLUMN,
        ]

        missing_cols = [c for c in required_cols if col_index(headers, c) is None]
        if missing_cols:
            raise ValueError(f"Missing required columns in '{RAW_DEALS_TAB}': {missing_cols}")

        idx = {c: col_index(headers, c) for c in required_cols}
        idx_ai_caption = col_index(headers, "ai_caption")

        published = 0
        failed = 0
        considered = 0

        for row_num, row in enumerate(rows, start=2):
            if published >= MAX_POSTS_PER_RUN:
                break

            row = pad_row(row, header_len)

            raw_status = (row[idx[RAW_STATUS_COLUMN]] or "").strip().upper()
            if raw_status != IG_REQUIRED_STATUS:
                continue

            ig_status = (row[idx[IG_STATUS_COLUMN]] or "").strip().upper()
            if ig_status in ("POSTED", "PUBLISHED", "DONE"):
                continue

            graphic_url = (row[idx["graphic_url"]] or "").strip()
            if not graphic_url:
                logger.warning(f"Row {row_num}: Missing graphic_url, skipping")
                continue

            deal = {
                "deal_id": row[idx["deal_id"]],
                "origin_city": row[idx["origin_city"]],
                "destination_city": row[idx["destination_city"]],
                "price_gbp": row[idx["price_gbp"]],
                "outbound_date": row[idx["outbound_date"]],
                "graphic_url": graphic_url,
                "ai_caption": row[idx_ai_caption] if idx_ai_caption is not None else "",
            }

            considered += 1
            caption = format_caption(deal)

            logger.info(f"📸 Posting row {row_num}: {deal['origin_city']} → {deal['destination_city']} (£{deal['price_gbp']})")
            ok, result = post_to_instagram(deal["graphic_url"], caption)

            if ok:
                published += 1
                logger.info(f"✅ Posted: {result}")
                batch_update_cells(
                    ws,
                    headers,
                    row_num,
                    {
                        IG_STATUS_COLUMN: "POSTED",
                        "ig_media_id": result,
                        "ig_published_timestamp": datetime.utcnow().isoformat() + "Z",
                        RAW_STATUS_COLUMN: IG_POSTED_STATUS,
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
                        IG_STATUS_COLUMN: "FAILED",
                        "ig_published_timestamp": datetime.utcnow().isoformat() + "Z",
                    },
                )

        logger.info("\n" + "=" * 60)
        logger.info("📊 PUBLISH SUMMARY")
        logger.info("=" * 60)
        logger.info(f"🔎 Considered: {considered}")
        logger.info(f"✅ Published:  {published}")
        logger.info(f"❌ Failed:     {failed}")
        logger.info("=" * 60 + "\n")

        return 1 if failed > 0 else 0

    except Exception as e:
        logger.error(f"❌ Worker failed with error: {e}")
        logger.debug(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
