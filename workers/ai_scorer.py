#!/usr/bin/env python3
"""
V3.2_Stage2 — workers/ai_scorer.py
Stage 1: Environment + Google Sheets connection
Stage 2: Find first row where RAW_STATUS/raw_status == "NEW" (safe, 
deterministic)
Stage 3: Score (heuristic for now) + write-back + promote status to 
"SCORED"

Design rules:
- NEVER write by column letters hardcoded. Always map by header row.
- Guard before write: only update if status is still NEW.
- Single batch_update per row for consistent write-back.
"""

from __future__ import annotations

import json
import os
import sys
import time
import datetime as dt
from typing import Dict, Any, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials


# ----------------------------
# Logging
# ----------------------------

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


def die(msg: str, code: int = 1) -> None:
    log(msg)
    sys.exit(code)


# ----------------------------
# Env helpers
# ----------------------------

def get_env(name: str, required: bool = True, default: str = "") -> str:
    v = os.getenv(name)
    if v is None or v == "":
        if required:
            die(f"ERROR: Missing environment variable: {name}")
        return default
    return v


# ----------------------------
# Google Sheets helpers
# ----------------------------

SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def get_worksheet():
    sheet_id = get_env("SHEET_ID", required=True)
    worksheet_name = get_env("WORKSHEET_NAME", required=False, 
default="RAW_DEALS") or "RAW_DEALS"
    sa_json = get_env("GCP_SA_JSON", required=True)

    try:
        sa_info = json.loads(sa_json)
    except json.JSONDecodeError:
        die("ERROR: GCP_SA_JSON is not valid JSON. It must be the full 
service account JSON (as one line).")

    creds = Credentials.from_service_account_info(sa_info, 
scopes=SHEETS_SCOPES)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(sheet_id)
    ws = sheet.worksheet(worksheet_name)
    return ws


def utc_now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def col_to_a1(col_index_1_based: int) -> str:
    # 1 -> A, 2 -> B, ... 27 -> AA
    result = ""
    n = col_index_1_based
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def a1_cell(row_1_based: int, col_1_based: int) -> str:
    return f"{col_to_a1(col_1_based)}{row_1_based}"


def build_header_map(headers: List[str]) -> Dict[str, int]:
    # header -> col index (1-based)
    m: Dict[str, int] = {}
    for i, h in enumerate(headers, start=1):
        key = (h or "").strip()
        if key:
            m[key] = i
    return m


def first_existing_col(hmap: Dict[str, int], candidates: List[str]) -> 
Optional[str]:
    for c in candidates:
        if c in hmap:
            return c
    return None


# ----------------------------
# Stage 2: find NEW row
# ----------------------------

def get_all_rows(ws) -> Tuple[List[str], List[List[str]]]:
    """
    Returns (headers, rows) where rows are raw values for each row 
(excluding header row).
    """
    values = ws.get_all_values()
    if not values or len(values) < 2:
        return [], []
    headers = values[0]
    rows = values[1:]
    return headers, rows


def normalize(s: Any) -> str:
    return ("" if s is None else str(s)).strip()


def find_first_new_row(ws) -> Optional[Dict[str, Any]]:
    """
    Finds the first row where raw_status/RAW_STATUS == "NEW".
    Returns:
      {
        "row_number": <1-based row number in sheet>,
        "record": {header: value, ...},
        "status_col_name": "raw_status" or "RAW_STATUS"
      }
    """
    headers, rows = get_all_rows(ws)
    if not headers:
        log("No headers found (sheet looks empty).")
        return None

    hmap = build_header_map(headers)

    status_col_name = first_existing_col(hmap, ["raw_status", 
"RAW_STATUS"])
    if not status_col_name:
        die("ERROR: Sheet must contain a status column named 'raw_status' 
or 'RAW_STATUS'.")

    status_col_idx = hmap[status_col_name] - 1  # 0-based for row lists

    for i, row in enumerate(rows, start=2):  # sheet row number starts at 
2 (row 1 is headers)
        # pad row to header length
        if len(row) < len(headers):
            row = row + [""] * (len(headers) - len(row))

        status_val = normalize(row[status_col_idx])
        if status_val == "NEW":
            record = {headers[j].strip(): row[j] for j in 
range(len(headers)) if headers[j].strip()}
            return {"row_number": i, "record": record, "status_col_name": 
status_col_name}

    return None


# ----------------------------
# Stage 3: scoring (heuristic v1)
# ----------------------------

def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        s = normalize(x).replace("£", "").replace(",", "")
        if s == "":
            return default
        return float(s)
    except Exception:
        return default


def safe_int(x: Any, default: int = 0) -> int:
    try:
        s = normalize(x).replace(",", "")
        if s == "":
            return default
        return int(float(s))
    except Exception:
        return default


def pick(rec: Dict[str, Any], *keys: str) -> str:
    for k in keys:
        if k in rec and normalize(rec.get(k)) != "":
            return normalize(rec.get(k))
    return ""


def heuristic_score(rec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Produces:
      ai_score: 0-100
      ai_grading: A/B/C/D
      ai_verdict: GOOD/AVERAGE/POOR
      ai_notes: short rationale
    """
    price = safe_float(pick(rec, "price_gbp", "PRICE_GBP"), 
default=9999.0)
    stops_raw = pick(rec, "stops", "STOPS")
    stops = safe_int(stops_raw, default=0)
    baggage = pick(rec, "baggage_included", "BAGGAGE_INCLUDED").lower()
    trip_len = safe_int(pick(rec, "trip_length_days", "TRIP_LENGTH_DAYS"), 
default=0)

    # Base score from price bands (tune later)
    score = 50.0
    notes: List[str] = []

    if price <= 35:
        score += 35
        notes.append("ultra-low fare")
    elif price <= 80:
        score += 25
        notes.append("very cheap")
    elif price <= 150:
        score += 15
        notes.append("good price")
    elif price <= 250:
        score += 5
        notes.append("fair price")
    else:
        score -= 10
        notes.append("pricey")

    # Stops
    if stops == 0:
        score += 10
        notes.append("direct")
    elif stops == 1:
        score += 3
        notes.append("1 stop")
    else:
        score -= 8
        notes.append(f"{stops} stops")

    # Baggage
    if baggage in ("yes", "true", "1", "included", "y"):
        score += 5
        notes.append("baggage included")
    elif baggage in ("no", "false", "0", "n"):
        score -= 2
        notes.append("no baggage")

    # Trip length (soft preference for 3–10 days)
    if 3 <= trip_len <= 10:
        score += 5
        notes.append("good trip length")
    elif trip_len >= 14:
        score -= 2
        notes.append("long trip")

    # Clamp
    score = max(0.0, min(100.0, score))
    ai_score = round(score, 1)

    # Grade + verdict
    if ai_score >= 85:
        grading, verdict = "A", "GOOD"
    elif ai_score >= 70:
        grading, verdict = "B", "GOOD"
    elif ai_score >= 55:
        grading, verdict = "C", "AVERAGE"
    else:
        grading, verdict = "D", "POOR"

    ai_notes = "; ".join(notes) if notes else "scored by heuristic"
    return {
        "ai_score": ai_score,
        "ai_grading": grading,
        "ai_verdict": verdict,
        "ai_notes": ai_notes,
    }


# ----------------------------
# Stage 3: write-back
# ----------------------------

REQUIRED_OUTPUT_COLS = ["ai_score", "ai_grading", "ai_verdict", 
"ai_notes", "scored_timestamp"]


def ensure_output_columns(ws, hmap: Dict[str, int], headers: List[str]) -> 
Tuple[Dict[str, int], List[str]]:
    """
    Ensures required output columns exist. If missing, appends them to 
header row.
    Returns updated (hmap, headers).
    """
    missing = [c for c in REQUIRED_OUTPUT_COLS if c not in hmap]
    if not missing:
        return hmap, headers

    log(f"Stage 3: Missing output columns {missing}. Adding to header 
row...")
    new_headers = headers[:] + missing
    ws.update("1:1", [new_headers])  # replace header row
    # Small delay to let Sheets settle
    time.sleep(0.6)

    # Re-read headers to be safe
    updated_headers = ws.row_values(1)
    updated_hmap = build_header_map(updated_headers)
    still_missing = [c for c in REQUIRED_OUTPUT_COLS if c not in 
updated_hmap]
    if still_missing:
        die(f"ERROR: Failed to create required output columns: 
{still_missing}")

    return updated_hmap, updated_headers


def stage3_writeback_row(ws, row_number: int, hmap: Dict[str, int], 
status_col_name: str, updates: Dict[str, Any]) -> bool:
    """
    Writes updates to a single row.
    Guard: only writes if raw_status/RAW_STATUS is still NEW.
    Returns True if updated, False if skipped due to guard.
    """
    status_col = hmap[status_col_name]
    current_status = normalize(ws.cell(row_number, status_col).value)
    if current_status != "NEW":
        log(f"Stage 3: Guard skip — row #{row_number} status is 
'{current_status}' (expected NEW).")
        return False

    data = []
    for field, value in updates.items():
        if field not in hmap:
            continue
        col = hmap[field]
        data.append({"range": a1_cell(row_number, col), "values": 
[[value]]})

    if not data:
        log(f"Stage 3: Nothing to write for row #{row_number}.")
        return False

    ws.batch_update(data)
    return True


# ----------------------------
# Main
# ----------------------------

def main() -> None:
    log("AI SCORER STARTING")

    # Stage 1: env check
    get_env("SHEET_ID", required=True)
    get_env("GCP_SA_JSON", required=True)
    # OPENAI_API_KEY kept for future stage; not required for heuristic 
scoring
    log("Environment OK")

    # Stage 1: connect
    ws = get_worksheet()
    log(f"Connected to worksheet: {ws.title}")
    log("AI SCORER STAGE 1 COMPLETE")

    # Stage 2: find first NEW row
    hit = find_first_new_row(ws)
    if not hit:
        log("Stage 2: No NEW rows found. Nothing to do.")
        log("AI SCORER STAGE 2 COMPLETE")
        return

    row_number = hit["row_number"]
    rec = hit["record"]
    status_col_name = hit["status_col_name"]

    def p(*keys: str) -> str:
        return pick(rec, *keys)

    deal_id = p("deal_id", "DEAL_ID")
    origin = p("origin_city", "ORIGIN_CITY")
    dest = p("destination_city", "DESTINATION_CITY")
    price = p("price_gbp", "PRICE_GBP")
    out_date = p("outbound_date", "OUTBOUND_DATE")

    log(f"Stage 2: Found NEW row at sheet row #{row_number}")
    log(f"deal_id={deal_id} | {origin} -> {dest} | £{price} | 
outbound={out_date}")
    log("AI SCORER STAGE 2 COMPLETE")

    # Stage 3: write-back + promote
    try:
        headers = ws.row_values(1)
        hmap = build_header_map(headers)

        # Ensure status column exists (already validated in Stage 2)
        if status_col_name not in hmap:
            die(f"ERROR: Status column '{status_col_name}' disappeared 
from header row.")

        # Ensure outputs exist
        hmap, headers = ensure_output_columns(ws, hmap, headers)

        # Score
        result = heuristic_score(rec)

        updates = {
            "ai_score": result["ai_score"],
            "ai_grading": result["ai_grading"],
            "ai_verdict": result["ai_verdict"],
            "ai_notes": result["ai_notes"],
            "scored_timestamp": utc_now_iso(),
            status_col_name: "SCORED",  # promote in same column that 
triggered it
        }

        wrote = stage3_writeback_row(ws, row_number, hmap, 
status_col_name, updates)
        if wrote:
            log(f"Stage 3: Wrote score + promoted row #{row_number} to 
SCORED.")
            log(f"ai_score={result['ai_score']} | 
ai_grading={result['ai_grading']} | ai_verdict={result['ai_verdict']}")
        log("AI SCORER STAGE 3 COMPLETE")

    except Exception as e:
        # Attempt to mark error (best-effort)
        try:
            headers = ws.row_values(1)
            hmap = build_header_map(headers)
            # ensure output cols so we can write notes/timestamp
            hmap, headers = ensure_output_columns(ws, hmap, headers)

            err_msg = (str(e) or "Unknown error")[:240]
            updates = {
                "ai_notes": f"ERROR_SCORING: {err_msg}",
                "scored_timestamp": utc_now_iso(),
                status_col_name: "ERROR_SCORING",
            }
            _ = stage3_writeback_row(ws, row_number, hmap, 
status_col_name, updates)
        except Exception:
            pass

        die(f"Stage 3: ERROR while scoring row #{row_number}: {e}")


if __name__ == "__main__":
    main()

