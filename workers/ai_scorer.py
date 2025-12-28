import os
import sys
import json
from typing import Dict, Any, Optional, List

import gspread
from google.oauth2.service_account import Credentials


# ---------- tiny logger ----------
def log(msg: str) -> None:
    print(msg, flush=True)


def get_env(name: str, required: bool = True) -> str:
    value = os.getenv(name)
    if required and not value:
        log(f"ERROR: Missing environment variable: {name}")
        sys.exit(1)
    return value or ""


# ---------- Google Sheets ----------
def get_worksheet():
    sheet_id = get_env("SHEET_ID", required=True)
    worksheet_name = get_env("WORKSHEET_NAME", required=False) or "RAW_DEALS"

    # Service Account JSON should be stored in GitHub Secrets as a JSON string
    gcp_sa_json = get_env("GCP_SA_JSON", required=True)

    try:
        sa_info = json.loads(gcp_sa_json)
    except json.JSONDecodeError:
        log("ERROR: GCP_SA_JSON is not valid JSON. It must be the full Service Account JSON.")
        sys.exit(1)

    creds = Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )

    client = gspread.authorize(creds)
    sheet = client.open_by_key(sheet_id)
    return sheet.worksheet(worksheet_name)


def _normalize_key(k: str) -> str:
    # Keep it robust if someone uses "Raw_Status" etc.
    return (k or "").strip().upper()


def find_first_new_by_raw_status(ws) -> Optional[Dict[str, Any]]:
    """
    Returns a dict with:
      - row_number (sheet row number, starting at 2 for first data row)
      - record (dict of header->value for that row)
    or None if not found.
    """
    records: List[Dict[str, Any]] = ws.get_all_records()  # uses row 1 as headers
    if not records:
        return None

    # Ensure RAW_STATUS header exists (case/spacing tolerant)
    headers = ws.row_values(1)
    header_map = {_normalize_key(h): h for h in headers}
    if "RAW_STATUS" not in header_map:
        log(f"ERROR: Could not find a header named RAW_STATUS. Headers seen: {headers}")
        sys.exit(1)

    raw_status_key = header_map["RAW_STATUS"]

    for idx, rec in enumerate(records):
        # idx=0 corresponds to sheet row 2
        val = str(rec.get(raw_status_key, "")).strip().upper()
        if val == "NEW":
            return {"row_number": idx + 2, "record": rec}

    return None


def main() -> None:
    log("AI SCORER STARTING")

    # Stage 1: env check (already proven in green run)
    get_env("SHEET_ID", required=True)
    get_env("GCP_SA_JSON", required=True)
    log("Environment OK")

    # Stage 1: connect
    ws = get_worksheet()
    log(f"Connected to worksheet: {ws.title}")

    # Stage 2: find the next NEW row
    hit = find_first_new_by_raw_status(ws)
    if not hit:
        log("Stage 2: No rows found where RAW_STATUS == NEW")
        log("AI SCORER STAGE 2 COMPLETE")
        return

    row_number = hit["row_number"]
    rec = hit["record"]

    # Print a compact summary (safe)
    deal_id = rec.get("deal_id") or rec.get("DEAL_ID") or ""
    origin = rec.get("origin_city") or rec.get("ORIGIN_CITY") or ""
    dest = rec.get("destination_city") or rec.get("DESTINATION_CITY") or ""
    price = rec.get("price_gbp") or rec.get("PRICE_GBP") or ""
    out_date = rec.get("outbound_date") or rec.get("OUTBOUND_DATE") or ""

    log(f"Stage 2: Found NEW row at sheet row #{row_number}")
    log(f"deal_id={deal_id} | {origin} -> {dest} | £{price} | outbound={out_date}")

    log("AI SCORER STAGE 2 COMPLETE")


if __name__ == "__main__":
    main()
