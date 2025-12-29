#!/usr/bin/env python3
"""
Traveltxter V3_beta_b_final — AI Scorer (Fixed)
Reads:    RAW_DEALS where status == NEW
Writes:   ai_score, ai_verdict, ai_notes, scored_timestamp
Promotes: status -> READY_TO_POST

Key fix:
- get_all_values() truncates rows; we MUST pad rows to header length
"""

import os
import sys
import json
import datetime as dt
from typing import Dict, Any, List, Tuple

import gspread
from google.oauth2.service_account import Credentials


def log(msg: str):
    ts = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
    print(f"{ts} | {msg}", flush=True)


def get_env(name: str, required: bool = True, default: str = "") -> str:
    v = os.getenv(name)
    if not v and required:
        print(f"ERROR: Missing {name}")
        sys.exit(1)
    return str(v or default).strip()


def col_to_a1(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def normalize_status(s: str) -> str:
    """
    Normalize status values robustly:
    - remove NBSP and zero-width chars
    - strip whitespace
    - uppercase
    """
    if s is None:
        return ""
    s = str(s)
    # common invisible troublemakers
    s = s.replace("\u00A0", " ")   # NBSP
    s = s.replace("\u200B", "")    # zero-width space
    s = s.replace("\uFEFF", "")    # BOM / zero-width no-break
    return s.strip().upper()


def score_deal(rec: Dict[str, Any]) -> Dict[str, Any]:
    def safe_val(key, default=0):
        try:
            val = str(rec.get(key, "")).replace("£", "").replace(",", "").strip()
            return float(val) if val else default
        except:
            return default

    price = safe_val("price_gbp", 9999)
    stops = safe_val("stops", 0)
    days = safe_val("trip_length_days", 0)

    score = 60
    notes = []

    if price <= 60:
        score += 25
        notes.append("Bargain")
    elif price <= 150:
        score += 10
        notes.append("Fair Price")

    if stops == 0:
        score += 15
        notes.append("Direct")
    elif stops > 1:
        score -= 5

    if 3 <= days <= 10:
        score += 5
        notes.append("Good Length")

    score = max(0, min(100, int(score)))

    if score >= 80:
        verdict = "GOOD"
    elif score >= 60:
        verdict = "AVERAGE"
    else:
        verdict = "POOR"

    return {
        "ai_score": score,
        "ai_verdict": verdict,
        "ai_notes": ", ".join(notes) if notes else "Standard"
    }


def main():
    log("🚀 AI SCORER STARTING (V3_beta_b_final FIXED)")

    sheet_id = get_env("SHEET_ID")
    ws_name = get_env("WORKSHEET_NAME", required=False, default="RAW_DEALS")
    sa_json = json.loads(get_env("GCP_SA_JSON"))
    max_rows = int(get_env("MAX_ROWS_PER_RUN", required=False, default="10"))

    creds = Credentials.from_service_account_info(
        sa_json,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    client = gspread.authorize(creds)
    ws = client.open_by_key(sheet_id).worksheet(ws_name)

    log(f"Connected to worksheet: {ws.title}")

    # Pull all values (may contain ragged rows)
    all_values = ws.get_all_values()
    if len(all_values) < 2:
        log("No data found in sheet.")
        return

    headers = [h.strip() for h in all_values[0]]
    header_len = len(headers)
    hmap = {h: i + 1 for i, h in enumerate(headers) if h}

    log(f"Found {header_len} columns, {len(all_values) - 1} data rows")

    # Use status column first (baseline), fall back if needed
    status_col_name = None
    for col_name in ["status", "raw_status", "RAW_STATUS"]:
        if col_name in hmap:
            status_col_name = col_name
            break

    if not status_col_name:
        log("❌ Critical Error: No status/raw_status column found.")
        log(f"Available columns (first 30): {list(hmap.keys())[:30]}")
        return

    status_col_idx0 = hmap[status_col_name] - 1
    status_col_letter = col_to_a1(hmap[status_col_name])

    log(f"Using status column: '{status_col_name}' at position {hmap[status_col_name]}")

    rows_to_process: List[Tuple[int, Dict[str, Any]]] = []

    # IMPORTANT FIX: pad every row to header length so status cell exists
    for row_num, row in enumerate(all_values[1:], start=2):
        padded = (row + [""] * header_len)[:header_len]
        status_raw = padded[status_col_idx0]
        status_val = normalize_status(status_raw)

        # light debug (don't spam everything unless needed)
        deal_id = padded[hmap["deal_id"] - 1] if "deal_id" in hmap else ""
        log(f"Row {row_num} deal_id={deal_id} status='{status_raw}' -> '{status_val}'")

        if status_val == "NEW":
            rec = dict(zip(headers, padded))
            rows_to_process.append((row_num, rec))
            log(f"  ✅ MATCH: row {row_num} (NEW) queued")

        if len(rows_to_process) >= max_rows:
            log(f"Reached MAX_ROWS_PER_RUN={max_rows}")
            break

    if not rows_to_process:
        log("No NEW rows found. Exiting.")
        return

    # Build updates
    updates = []
    now = dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    for row_num, rec in rows_to_process:
        res = score_deal(rec)
        log(f"Scoring row {row_num}: score={res['ai_score']} verdict={res['ai_verdict']}")

        def add(col: str, val: Any):
            if col in hmap:
                col_idx = hmap[col]
                updates.append({
                    "range": f"{col_to_a1(col_idx)}{row_num}", 
                    "values": [[val]]
                })

        add("ai_score", res["ai_score"])
        add("ai_verdict", res["ai_verdict"])
        add("ai_notes", res["ai_notes"])
        add("scored_timestamp", now)

        # Promote directly to READY_TO_POST (skipping SCORED step)
        add(status_col_name, "READY_TO_POST")

    if updates:
        log(f"Writing {len(updates)} cell updates...")
        ws.batch_update(updates)
        log(f"✅ Done: processed {len(rows_to_process)} rows. NEW → READY_TO_POST")


if __name__ == "__main__":
    main()
