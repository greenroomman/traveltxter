#!/usr/bin/env python3
import os
import sys
import json
import requests
import datetime as dt

import gspread
from google.oauth2.service_account import Credentials

# ============================================================
# Config
# ============================================================

SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
RAW_DEALS_TAB = os.environ.get("RAW_DEALS_TAB", "RAW_DEALS")
RENDER_URL = os.environ["RENDER_URL"]
MAX_ROWS = int(os.environ.get("RENDER_MAX_ROWS", "1"))

# ============================================================
# Auth
# ============================================================

def get_ws():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GCP_SA_JSON_ONE_LINE"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID).worksheet(RAW_DEALS_TAB)

# ============================================================
# Helpers
# ============================================================

def log(msg):
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)

# ============================================================
# Main
# ============================================================

def main():
    ws = get_ws()
    rows = ws.get_all_records()
    rendered = 0

    for idx, row in enumerate(rows, start=2):
        if rendered >= MAX_ROWS:
            break

        if row.get("status") != "READY_TO_POST":
            continue

        if row.get("graphic_url"):
            continue

        payload = {
            "deal_id": row["deal_id"],
            "to_city": row["destination_city"],
            "from_city": row["origin_city"],
            "out_date": row["out_date"],
            "in_date": row["in_date"],
            "price": str(row["price"]).replace("£", "")
        }

        log(f"🖼️ Rendering row {idx} deal_id={row['deal_id']}")

        resp = requests.post(
            RENDER_URL,
            json=payload,
            timeout=30
        )

        if resp.status_code != 200:
            raise RuntimeError(
                f"Renderer error {resp.status_code}: {resp.text[:400]}"
            )

        data = resp.json()
        graphic_url = data.get("graphic_url")

        if not graphic_url:
            raise RuntimeError("Renderer returned no graphic_url")

        ws.update_cell(idx, ws.find("graphic_url").col, graphic_url)
        rendered += 1

        log(f"✅ graphic_url written: {graphic_url}")

    log(f"Done. Rendered {rendered} row(s).")

if __name__ == "__main__":
    main()
