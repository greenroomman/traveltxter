#!/usr/bin/env python3
"""
Render Client — CANONICAL, RESTORED VERSION

Purpose:
- Call PythonAnywhere render endpoint (POST)
- Generate image
- Write graphic_url back to RAW_DEALS
- Advance status

DO NOT ADD LOGIC
DO NOT CHANGE RENDER PAYLOAD SHAPE
"""

import os
import sys
import json
import requests
import datetime as dt

import gspread
from google.oauth2.service_account import Credentials


# =========================
# Config
# =========================
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
RAW_DEALS_TAB = os.environ.get("RAW_DEALS_TAB", "RAW_DEALS")
RENDER_URL = os.environ["RENDER_URL"]
MAX_ROWS = int(os.environ.get("RENDER_MAX_ROWS", "1"))


# =========================
# Helpers
# =========================
def log(msg: str):
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


def get_ws():
    sa = json.loads(os.environ["GCP_SA_JSON_ONE_LINE"])
    creds = Credentials.from_service_account_info(
        sa,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID).worksheet(RAW_DEALS_TAB)


# =========================
# Main
# =========================
def main():
    ws = get_ws()
    rows = ws.get_all_records()

    rendered = 0

    for idx, row in enumerate(rows, start=2):  # header offset
        if rendered >= MAX_ROWS:
            break

        if row.get("status") != "READY_TO_POST":
            continue

        deal_id = row.get("deal_id")
        log(f"🖼️ Rendering row {idx} deal_id={deal_id}")

        payload = {
            "deal_id": deal_id,
            "to_city": row["destination_city"],
            "from_city": row["origin_city"],
            "out_date": row["out_date"],
            "in_date": row["in_date"],
            "price": str(row["price"]).replace("£", "").strip(),
        }

        # 🔑 THIS IS THE CRITICAL FIX — POST, NOT GET
        resp = requests.post(
            RENDER_URL,
            json=payload,
            timeout=60,
        )

        if resp.status_code != 200:
            raise RuntimeError(
                f"Renderer error {resp.status_code}: {resp.text[:400]}"
            )

        data = resp.json()

        graphic_url = data.get("graphic_url")
        if not graphic_url:
            raise RuntimeError(f"No graphic_url returned: {data}")

        ws.update([[graphic_url]], f"{ws.find('graphic_url').col}{idx}")

        ws.update(
            [["RENDERED"]],
            f"{ws.find('status').col}{idx}",
        )

        rendered += 1
        log(f"✅ Rendered + saved graphic_url")

    log("Render client complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
