#!/usr/bin/env python3
"""
workers/render_client.py

Render Client — V4.6.1 HOTFIX
FULL REPLACEMENT

FIX:
- Robust GCP_SA_JSON parsing (matches pipeline_worker / link_router)
- Prevent JSONDecodeError when secret already contains real newlines

LOCKED BEHAVIOUR:
- Google Sheets is the single source of truth
- RAW_DEALS_VIEW is never written to
- Renderer is stateless
- ALWAYS prioritise newest eligible deals (fresh-first)
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

ELIGIBLE_STATUSES = {
    "READY_TO_PUBLISH",
    "READY_TO_POST",
}


# ==================== LOGGING ====================

def log(msg: str):
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


# ==================== GOOGLE SHEETS ====================

def parse_sa_json(raw: str) -> dict:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return json.loads(raw.replace("\\n", "\n"))


def gs_client():
    raw = os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON")
    info = parse_sa_json(raw)
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

    for col in ("status", "ingested_at_utc", "graphic_url"):
        if col not in idx:
            raise RuntimeError(f"Missing required column: {col}")

    eligible = []

    for i, row in enumerate(rows, start=2):
        status = row[idx["status"]].strip()
        graphic_url = row[idx["graphic_url"]].strip()
        ingested = row[idx["ingested_at_utc"]].strip()

        if status not in ELIGIBLE_STATUSES:
            continue
        if graphic_url:
            continue

        ts = parse_utc(ingested)
        eligible.append(
            {
                "row_num": i,
                "ingested_at": ts,
            }
        )

    if not eligible:
        log("No eligible rows to render.")
        return 0

    eligible.sort(
        key=lambda r: (r["ingested_at"] or dt.datetime.min, r["row_num"]),
        reverse=True,
    )

    to_render = eligible[:RENDER_MAX_ROWS]
    log(f"Eligible rows: {len(eligible)} | Rendering: {len(to_render)}")

    for item in to_render:
        row_num = item["row_num"]
        log(f"🖼️ Rendering row {row_num}")

        r = requests.post(
            RENDER_URL,
            json={"row_number": row_num},
            timeout=60,
        )

        if r.status_code != 200:
            log(f"❌ Render failed row {row_num}: HTTP {r.status_code}")
            continue

        graphic_url = r.json().get("graphic_url")
        if not graphic_url:
            log(f"❌ No graphic_url returned for row {row_num}")
            continue

        cell = gspread.utils.rowcol_to_a1(row_num, idx["graphic_url"] + 1)
        ws.update([[graphic_url]], cell)

        log(f"✅ Rendered row {row_num}")
        time.sleep(1)

    log("Render cycle complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
