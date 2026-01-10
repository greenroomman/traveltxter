#!/usr/bin/env python3
import os
import json
import datetime as dt
import requests

import gspread
from google.oauth2.service_account import Credentials

# ============================================================
# CONFIG
# ============================================================

SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
RAW_DEALS_TAB = os.environ.get("RAW_DEALS_TAB", "RAW_DEALS")
RENDER_URL = os.environ["RENDER_URL"]
MAX_ROWS = int(os.environ.get("RENDER_MAX_ROWS", "1"))

# ============================================================
# AUTH
# ============================================================

def get_ws():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GCP_SA_JSON_ONE_LINE"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID).worksheet(RAW_DEALS_TAB)

# ============================================================
# HELPERS
# ============================================================

def log(msg):
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)

def to_dd_mm_yy(iso_date):
    if not iso_date:
        return ""
    d = dt.date.fromisoformat(iso_date.strip())
    return f"{d.strftime('%d')} {d.strftime('%m')} {d.strftime('%y')}"

# ============================================================
# MAIN
# ============================================================

def main():
    ws = get_ws()
    headers = ws.row_values(1)

    col_status = headers.index("status") + 1
    col_graphic = headers.index("graphic_url") + 1

    rows = ws.get_all_records()
    rendered = 0

    log(f"Render endpoint: {RENDER_URL}")
    log("HTTP method: GET (PythonAnywhere requirement)")

    for idx, row in enumerate(rows, start=2):
        if rendered >= MAX_ROWS:
            break

        if row.get("status") != "READY_TO_POST":
            continue

        if row.get("graphic_url"):
            continue

        # ---- REQUIRED FIELDS ----
        to_city = row.get("destination_city", "").strip()
        from_city = row.get("origin_city", "").strip()

        out_date = to_dd_mm_yy(row.get("outbound_date", ""))
        in_date = to_dd_mm_yy(row.get("return_date", ""))

        price_raw = str(row.get("price_gbp", "")).strip()
        price = price_raw.replace("£", "").replace("Â£", "").replace(",", "")

        params = {
            "TO": to_city,
            "FROM": from_city,
            "OUT": out_date,
            "IN": in_date,
            "PRICE": price,
        }

        log(f"🖼️ Rendering row {idx} | {from_city} → {to_city} | £{price}")

        resp = requests.get(RENDER_URL, params=params, timeout=60)

        if resp.status_code != 200:
            raise RuntimeError(
                f"Renderer error {resp.status_code}: {resp.text[:300]}"
            )

        data = resp.json()
        graphic_url = data.get("graphic_url")

        if not graphic_url:
            raise RuntimeError(f"Renderer returned no graphic_url: {data}")

        # ---- WRITE BACK ----
        ws.update_cell(idx, col_graphic, graphic_url)
        ws.update_cell(idx, col_status, "READY_TO_PUBLISH")

        rendered += 1
        log(f"✅ graphic_url written: {graphic_url}")

    log(f"Done. Rendered {rendered} row(s).")

if __name__ == "__main__":
    main()
