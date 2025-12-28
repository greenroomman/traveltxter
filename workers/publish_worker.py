import sys
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import time
from datetime import datetime, timezone

import requests
import gspread
from google.oauth2.service_account import Credentials

from lib.normalise_deal import normalise_deal_for_render
from lib.caption_builder import build_caption
from lib.renderer_client import render_deal_png


# =========================
# CONFIG (simple)
# =========================

# Google Sheet
SHEET_ID = os.environ.get("SHEET_ID", "").strip()
TAB_NAME = os.environ.get("TAB_NAME", "RAW_DEALS").strip()

# Service account JSON path
# You have this in your repo root already: service_account.json
SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json").strip()

# Instagram Graph API
IG_USER_ID = os.environ.get("IG_USER_ID", "").strip()
FB_ACCESS_TOKEN = os.environ.get("FB_ACCESS_TOKEN", "").strip()
GRAPH_VERSION = os.environ.get("GRAPH_VERSION", "v19.0").strip()

# Publishing rules
READY_STATUSES = [s.strip().upper() for s in os.environ.get("READY_STATUSES", "SCORED,READY").split(",")]

# Column names we expect to exist in the sheet
COL_DEAL_ID = "deal_id"
COL_ORIGIN = "origin_city"
COL_DEST = "destination_city"
COL_PRICE = "price_gbp"
COL_OUT = "outbound_date"
COL_RET = "return_date"
COL_STATUS = "status"
COL_PUBLISHED_TS = "published_timestamp"

# Optional columns (if present, we will write into them)
COL_IG_CREATION_ID = "ig_creation_id"
COL_IG_MEDIA_ID = "ig_media_id"
COL_PUBLISH_ERROR = "publish_error"


# =========================
# HELPERS
# =========================

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def require_env(name: str, value: str) -> None:
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")


def get_ws():
    """
    Connect to Google Sheet tab using service account.
    """
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    return sh.worksheet(TAB_NAME)


def get_header_map(ws):
    """
    Returns: dict {header_name: column_index (1-based)}
    """
    headers = ws.row_values(1)
    return {h.strip(): idx + 1 for idx, h in enumerate(headers) if h.strip()}


def safe_get(row: list, header_map: dict, col_name: str) -> str:
    col_idx = header_map.get(col_name)
    if not col_idx:
        return ""
    # row list is 0-based; col_idx is 1-based
    i = col_idx - 1
    return row[i] if i < len(row) else ""


def safe_set(ws, row_index: int, header_map: dict, col_name: str, value: str):
    col_idx = header_map.get(col_name)
    if not col_idx:
        return  # column not present, skip silently
    ws.update_cell(row_index, col_idx, value)


def publish_to_instagram(image_url: str, caption: str) -> tuple[str, str]:
    """
    Returns (creation_id, media_id).
    Uses IG Content Publishing API:
      1) POST /{ig_user_id}/media -> creation_id
      2) POST /{ig_user_id}/media_publish -> media_id
    Includes a small retry loop because containers sometimes need a moment.
    """
    base = f"https://graph.facebook.com/{GRAPH_VERSION}"

    # Step 1: create container
    create_url = f"{base}/{IG_USER_ID}/media"
    r1 = requests.post(
        create_url,
        data={
            "image_url": image_url,
            "caption": caption,
            "access_token": FB_ACCESS_TOKEN,
        },
        timeout=60,
    )
    r1.raise_for_status()
    creation_id = r1.json().get("id")
    if not creation_id:
        raise RuntimeError(f"Failed to create media container: {r1.text}")

    # Step 2: publish container (retry a few times)
    publish_url = f"{base}/{IG_USER_ID}/media_publish"
    last_err = None

    for attempt in range(1, 8):  # ~ up to ~35 seconds
        r2 = requests.post(
            publish_url,
            data={
                "creation_id": creation_id,
                "access_token": FB_ACCESS_TOKEN,
            },
            timeout=60,
        )

        if r2.status_code == 200:
            media_id = r2.json().get("id")
            if media_id:
                return creation_id, media_id

        last_err = r2.text
        time.sleep(5)

    raise RuntimeError(f"Failed to publish after retries. creation_id={creation_id} last_error={last_err}")


# =========================
# MAIN
# =========================

def main():
    # Required settings
    require_env("SHEET_ID", SHEET_ID)
    require_env("IG_USER_ID", IG_USER_ID)
    require_env("FB_ACCESS_TOKEN", FB_ACCESS_TOKEN)

    ws = get_ws()
    header_map = get_header_map(ws)

    # Basic column checks (friendly errors)
    for col in [COL_DEAL_ID, COL_ORIGIN, COL_DEST, COL_PRICE, COL_OUT, COL_RET, COL_STATUS]:
        if col not in header_map:
            raise RuntimeError(f"Missing required column in sheet header row: {col}")

    # Pull all rows (simple + reliable for small/medium sheets)
    all_rows = ws.get_all_values()
    if len(all_rows) < 2:
        print("No data rows found.")
        return

    headers = all_rows[0]
    data_rows = all_rows[1:]

    # Find first row that is ready AND not already published
    target_row_index = None
    target_row_values = None

    for i, row in enumerate(data_rows, start=2):  # sheet row numbers start at 1, header is row 1
        status = safe_get(row, header_map, COL_STATUS).strip().upper()
        published_ts = safe_get(row, header_map, COL_PUBLISHED_TS).strip()

        if status in READY_STATUSES and not published_ts:
            target_row_index = i
            target_row_values = row
            break

    if not target_row_index:
        print(f"No rows found with status in {READY_STATUSES} and empty {COL_PUBLISHED_TS}.")
        return

    # Extract deal fields
    deal_id = safe_get(target_row_values, header_map, COL_DEAL_ID).strip()
    origin_city = safe_get(target_row_values, header_map, COL_ORIGIN).strip()
    destination_city = safe_get(target_row_values, header_map, COL_DEST).strip()
    price_gbp = safe_get(target_row_values, header_map, COL_PRICE).strip()
    outbound_date = safe_get(target_row_values, header_map, COL_OUT).strip()
    return_date = safe_get(target_row_values, header_map, COL_RET).strip()

    print(f"Publishing row {target_row_index} deal_id={deal_id}")

    # Mark as publishing early (so double-runs don’t double-post)
    safe_set(ws, target_row_index, header_map, COL_STATUS, "PUBLISHING")
    safe_set(ws, target_row_index, header_map, COL_PUBLISH_ERROR, "")

    try:
        # 1) Enforce strict renderer contract (full city names, ddmmyy, price max 3 digits)
        deal_for_render = normalise_deal_for_render({
            "deal_id": deal_id,
            "origin_city": origin_city,
            "destination_city": destination_city,
            "outbound_date": outbound_date,
            "return_date": return_date,
            "price_gbp": price_gbp,
        })

        # 2) Render HTML -> PNG and get a public image URL
        image_url = render_deal_png(deal_for_render)

        # 3) Build the neutral, conscientious caption
        caption = build_caption(deal_for_render)

        # 4) Publish to Instagram
        creation_id, media_id = publish_to_instagram(image_url=image_url, caption=caption)

        # 5) Update sheet
        safe_set(ws, target_row_index, header_map, COL_STATUS, "PUBLISHED")
        safe_set(ws, target_row_index, header_map, COL_PUBLISHED_TS, now_utc_iso())
        safe_set(ws, target_row_index, header_map, COL_IG_CREATION_ID, creation_id)
        safe_set(ws, target_row_index, header_map, COL_IG_MEDIA_ID, media_id)

        print(f"SUCCESS deal_id={deal_id} media_id={media_id}")

    except Exception as e:
        err = str(e)
        print(f"FAILED deal_id={deal_id}: {err}")
        safe_set(ws, target_row_index, header_map, COL_STATUS, "PUBLISH_FAILED")
        safe_set(ws, target_row_index, header_map, COL_PUBLISH_ERROR, err)


if __name__ == "__main__":
    main()

