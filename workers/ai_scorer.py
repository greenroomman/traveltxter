#!/usr/bin/env python3

import os
import sys
import json
import gspread
from google.oauth2.service_account import Credentials


def log(msg: str) -> None:
    print(msg, flush=True)


def get_env(name: str, required: bool = True, default: str = "") -> str:
    val = os.getenv(name)
    if (val is None or val.strip() == "") and required:
        log(f"ERROR: Missing environment variable: {name}")
        sys.exit(1)
    return (val or default).strip()


def get_worksheet():
    sheet_id = get_env("SHEET_ID", required=True)
    worksheet_name = get_env("WORKSHEET_NAME", required=False, default="RAW_DEALS") or "RAW_DEALS"
    sa_json = get_env("GCP_SA_JSON", required=True)

    try:
        sa_info = json.loads(sa_json)
    except json.JSONDecodeError:
        log("ERROR: GCP_SA_JSON is not valid JSON. Paste the full service-account JSON into the secret as-is.")
        sys.exit(1)

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
    client = gspread.authorize(creds)

    sheet = client.open_by_key(sheet_id)
    return sheet.worksheet(worksheet_name)


def main() -> None:
    log("AI SCORER STARTING")

    # Stage 1: env check
    get_env("SHEET_ID", required=True)
    get_env("GCP_SA_JSON", required=True)
    log("Environment OK")

    # Stage 1: connect
    ws = get_worksheet()
    log(f"Connected to worksheet: {ws.title}")

    log("AI SCORER STAGE 1 COMPLETE")


if __name__ == "__main__":
    main()
