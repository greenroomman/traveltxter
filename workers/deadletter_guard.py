#!/usr/bin/env python3
"""
Dead-letter Guard — prevents pipeline jams by marking rows as ERROR_HARD after N failures.

What it does:
- Finds rows where status == INPUT_STATUS
- If fail_count >= MAX_FAILS -> set status to DEADLETTER_STATUS
- Otherwise increments fail_count and stores last_error + last_attempt_ts
- Only writes columns if they exist (safe)

Env:
  GCP_SA_JSON, SPREADSHEET_ID, RAW_DEALS_TAB
  INPUT_STATUS (required)
  MAX_FAILS (default 3)
  DEADLETTER_STATUS (default ERROR_HARD)
  LAST_ERROR (optional) - message from failing step
  MAX_ROWS_PER_RUN (default 5)
"""

import os
import json
import datetime as dt
from typing import Dict, List

import gspread
from google.oauth2.service_account import Credentials


def env(k: str, default: str = "") -> str:
    return (os.getenv(k) or default).strip()


def get_ws():
    info = json.loads(env("GCP_SA_JSON"))
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(env("SPREADSHEET_ID"))
    return sh.worksheet(env("RAW_DEALS_TAB", "RAW_DEALS"))


def update_cells(ws, row_num: int, headers: List[str], updates: Dict[str, str]) -> None:
    idx = {h: i + 1 for i, h in enumerate(headers)}
    cells = []
    for k, v in updates.items():
        if k in idx:
            cells.append(gspread.Cell(row_num, idx[k], v))
    if cells:
        ws.update_cells(cells, value_input_option="USER_ENTERED")


def main() -> int:
    input_status = env("INPUT_STATUS").upper()
    if not input_status:
        print("Missing INPUT_STATUS", flush=True)
        return 0

    max_fails = int(env("MAX_FAILS", "3"))
    dead_status = env("DEADLETTER_STATUS", "ERROR_HARD").upper()
    last_error = env("LAST_ERROR", "")
    max_rows = int(env("MAX_ROWS_PER_RUN", "5"))

    ws = get_ws()
    values = ws.get_all_values()
    if len(values) < 2:
        print("No rows", flush=True)
        return 0

    headers = values[0]
    idx = {h: i for i, h in enumerate(headers)}

    if "status" not in idx:
        print("No 'status' column found", flush=True)
        return 0

    processed = 0
    now = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    for r in range(1, len(values)):
        if processed >= max_rows:
            break

        row = values[r]
        row_num = r + 1
        status = (row[idx["status"]] if idx["status"] < len(row) else "").strip().upper()
        if status != input_status:
            continue

        # Read fail_count if present
        fail_count = 0
        if "fail_count" in idx and idx["fail_count"] < len(row):
            raw = (row[idx["fail_count"]] or "").strip()
            try:
                fail_count = int(raw) if raw else 0
            except Exception:
                fail_count = 0

        updates: Dict[str, str] = {}

        if fail_count >= max_fails:
            updates["status"] = dead_status
            if "last_error" in idx and last_error:
                updates["last_error"] = last_error[:500]
            if "last_attempt_ts" in idx:
                updates["last_attempt_ts"] = now
            update_cells(ws, row_num, headers, updates)
            processed += 1
            continue

        # Increment + annotate
        fail_count += 1
        if "fail_count" in idx:
            updates["fail_count"] = str(fail_count)
        if "last_error" in idx and last_error:
            updates["last_error"] = last_error[:500]
        if "last_attempt_ts" in idx:
            updates["last_attempt_ts"] = now

        # If this increment hits max, dead-letter it now
        if fail_count >= max_fails:
            updates["status"] = dead_status

        update_cells(ws, row_num, headers, updates)
        processed += 1

    print(f"Dead-letter guard processed {processed} rows for status={input_status}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
