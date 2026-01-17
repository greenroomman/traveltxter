#!/usr/bin/env python3
"""
workers/render_client.py

Render Client — V4.6
FULL REPLACEMENT

LOCKED BEHAVIOUR
- Google Sheets is the single source of truth
- RAW_DEALS_VIEW is never written to
- Renderer is stateless
- ALWAYS prioritise newest eligible deals (NOT sheet order)

SELECTION RULE (CRITICAL)
1. Eligible status
2. Newest ingested_at_utc DESC
3. Row number DESC
"""

import os
import json
import time
import datetime as dt
import requests
import gspread
from google.oauth2.service_account import Credentials


# ==================== ENV ====================

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS")
RENDER_URL = os.getenv("RENDER_URL")
RENDER_MAX_ROWS = int(os.getenv("RENDER_MAX_ROWS", "1") or "1")

RUN_SLOT = os.getenv("RUN_SLOT", "UNKNOWN")

# Statuses allowed to render
ELIGIBLE_STATUSES = {
    "READY_TO_PUBLISH",
    "READY_TO_POST",
}


# ==================== LOGGING ====================

def log(msg: str):
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


# ==================== GOOGLE SHEETS ====================

def gs_client():
    raw = os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON")
    info = json.loads(raw.replace("\\n", "\n"))
    creds = Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


# ==================== HELPERS ====================

def parse_utc(ts: str):
    try:
        return dt.datetime.fromisoformat(ts.replace("Z", ""))
    except Exception:
        return None


# ==================== MAIN ====================

def main():
    log("=" * 60)
    log(f"🖼️ Render Client starting | RUN_SLOT={RUN_SLOT}")
    log("=" * 60)

    if not SPREADSHEET_ID or not RENDER_URL:
        raise RuntimeError("Missing SPREADSHEET_ID or RENDER_URL")

    gc = gs_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(RAW_DEALS_TAB)

    headers = ws.row_values(1)
    rows = ws.get_all_values()[1:]

    idx = {h: i for i, h in enumerate(headers)}

    required = ["status", "ingested_at_utc", "graphic_url"]
    for col in required:
        if col not in idx:
            raise RuntimeError(f"Missing required column: {col}")

    # --------------------
    # FILTER ELIGIBLE ROWS
    # --------------------
    eligible = []

    for i, row in enumerate(rows, start=2):  # sheet row number
        status = row[idx["status"]].strip()
        graphic_url = row[idx["graphic_url"]].strip()
        ingested = row[idx["ingested_at_utc"]].strip()

        if status not in ELIGIBLE_STATUSES:
            continue
        if graphic_url:
            continue  # already rendered

        ts = parse_utc(ingested)
        eligible.append(
            {
                "row_num": i,
                "ingested_at": ts,
                "row": row,
            }
        )

    if not eligible:
        log("No eligible rows to render.")
        return 0

    # --------------------
    # SORT: NEWEST FIRST
    # --------------------
    eligible.sort(
        key=lambda r: (
            r["ingested_at"] or dt.datetime.min,
            r["row_num"],
        ),
        reverse=True,
    )

    to_render = eligible[:RENDER_MAX_ROWS]

    log(f"Eligible rows found: {len(eligible)} | Rendering: {len(to_render)}")

    # --------------------
    # RENDER LOOP
    # --------------------
    for item in to_render:
        row_num = item["row_num"]
        ingested = item["ingested_at"]

        log(f"🖼️ Rendering row {row_num} (ingested_at_utc={ingested})")

        payload = {
            "row_number": row_num,
        }

        r = requests.post(RENDER_URL, json=payload, timeout=60)

        if r.status_code != 200:
            log(f"❌ Render failed for row {row_num}: {r.status_code}")
            continue

        data = r.json()
        graphic_url = data.get("graphic_url")

        if not graphic_url:
            log(f"❌ No graphic_url returned for row {row_num}")
            continue

        cell = gspread.utils.rowcol_to_a1(row_num, idx["graphic_url"] + 1)
        ws.update([[graphic_url]], cell)

        log(f"✅ Rendered row {row_num}")

        time.sleep(1)

    log("Done. Render cycle complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
