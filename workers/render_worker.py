#!/usr/bin/env python3

import os
import json
import math
import requests
import gspread
from datetime import datetime
from google.oauth2.service_account import Credentials


def ddmmyy(date_iso: str) -> str:
    # YYYY-MM-DD → ddmmyy
    y, m, d = date_iso.split("-")
    return f"{d}{m}{y[-2:]}"


def main():
    render_url = os.getenv("RENDER_URL")
    spreadsheet_id = os.getenv("SPREADSHEET_ID")
    sa_json = os.getenv("GCP_SA_JSON_ONE_LINE")

    if not all([render_url, spreadsheet_id, sa_json]):
        raise RuntimeError("Missing RENDER_URL / SPREADSHEET_ID / GCP_SA_JSON_ONE_LINE")

    gc = gspread.authorize(
        Credentials.from_service_account_info(
            json.loads(sa_json),
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
    )

    ws = gc.open_by_key(spreadsheet_id).worksheet("RAW_DEALS")
    rows = ws.get_all_values()
    headers = rows[0]
    h = {name: idx for idx, name in enumerate(headers)}

    for row_idx, row in enumerate(rows[1:], start=2):
        if row[h["status"]] != "READY_TO_POST":
            continue

        payload = {
            "TO": row[h["destination_city"]],
            "FROM": row[h["origin_city"]],
            "OUT": ddmmyy(row[h["outbound_date"]]),
            "IN": ddmmyy(row[h["return_date"]]),
            "PRICE": f"£{math.ceil(float(row[h['price_gbp']]))}",
        }

        print(f"🎨 Rendering row {row_idx} → {payload}", flush=True)

        r = requests.post(render_url, json=payload, timeout=60)
        if r.status_code != 200:
            print(f"❌ Render failed {r.status_code}: {r.text[:200]}")
            return

        image_url = r.json().get("image_url")
        if not image_url:
            print("❌ Render returned no image_url")
            return

        ws.update_cell(row_idx, h["image_url"] + 1, image_url)
        ws.update_cell(row_idx, h["status"] + 1, "READY_TO_PUBLISH")

        print(f"✅ Rendered row {row_idx}: {image_url}")
        break  # ONE render per run


if __name__ == "__main__":
    main()
