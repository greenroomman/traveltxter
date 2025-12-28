#!/usr/bin/env python3
"""
V3.2 — AI Scorer
Stage 1: Env + Sheets connect
Stage 2: Find first NEW row
Stage 3: Heuristic score + write-back + promote to SCORED
"""

import os
import sys
import json
import time
import datetime as dt
from typing import Dict, Any, List, Optional

import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# Logging
# ============================================================

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


def die(msg: str, code: int = 1) -> None:
    log(msg)
    sys.exit(code)


# ============================================================
# Environment helpers
# ============================================================

def get_env(name: str, required: bool = True, default: str = "") -> str:
    value = os.getenv(name)
    if not value:
        if required:
            die(f"ERROR: Missing environment variable: {name}")
        return default
    return value


# ============================================================
# Google Sheets helpers
# ============================================================

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def get_worksheet():
    sheet_id = get_env("SHEET_ID", required=True)
    worksheet_name = get_env("WORKSHEET_NAME", required=False, 
default="RAW_DEALS")
    sa_json = get_env("GCP_SA_JSON", required=True)

    try:
        sa_info = json.loads(sa_json)
    except Exception:
        die("ERROR: GCP_SA_JSON must be valid JSON on a single line.")

    creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(sheet_id)
    return sheet.worksheet(worksheet_name)


def utc_now() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def col_to_a1(n: int) -> str:
    result = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        result = chr(65 + r) + result
    return result


def a1(row: int, col: int) -> str:
    return f"{col_to_a1(col)}{row}"


def build_header_map(headers: List[str]) -> Dict[str, int]:
    return {h.strip(): i + 1 for i, h in enumerate(headers) if h.strip()}


# ============================================================
# Stage 2 — find NEW row
# ============================================================

def normalize(v: Any) -> str:
    return "" if v is None else str(v).strip()


def find_first_new(ws) -> Optional[Dict[str, Any]]:
    values = ws.get_all_values()
    if len(values) < 2:
        return None

    headers = values[0]
    rows = values[1:]
    hmap = build_header_map(headers)

    status_col = None
    for k in ("raw_status", "RAW_STATUS"):
        if k in hmap:
            status_col = k
            break

    if not status_col:
        die("ERROR: Missing raw_status or RAW_STATUS column.")

    status_idx = hmap[status_col] - 1

    for i, row in enumerate(rows, start=2):
        if len(row) < len(headers):
            row += [""] * (len(headers) - len(row))
        if normalize(row[status_idx]) == "NEW":
            record = {headers[j]: row[j] for j in range(len(headers))}
            return {"row_number": i, "record": record, "status_col": 
status_col}

    return None


# ============================================================
# Stage 3 — heuristic scorer
# ============================================================

def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        s = normalize(x).replace("£", "").replace(",", "")
        return float(s) if s else default
    except Exception:
        return default


def safe_int(x: Any, default: int = 0) -> int:
    try:
        s = normalize(x)
        return int(float(s)) if s else default
    except Exception:
        return default


def pick(rec: Dict[str, Any], *keys: str) -> str:
    for k in keys:
        if k in rec and normalize(rec[k]):
            return normalize(rec[k])
    return ""


def score_deal(rec: Dict[str, Any]) -> Dict[str, Any]:
    price = safe_float(pick(rec, "price_gbp", "PRICE_GBP"), 9999)
    stops = safe_int(pick(rec, "stops", "STOPS"), 0)
    baggage = pick(rec, "baggage_included", "BAGGAGE_INCLUDED").lower()
    days = safe_int(pick(rec, "trip_length_days", "TRIP_LENGTH_DAYS"), 0)

    score = 50
    notes = []

    if price <= 50:
        score += 30
        notes.append("cheap")
    elif price <= 120:
        score += 15
        notes.append("good value")
    elif price > 300:
        score -= 10
        notes.append("expensive")

    if stops == 0:
        score += 10
        notes.append("direct")
    elif stops > 1:
        score -= 5
        notes.append("multiple stops")

    if baggage in ("yes", "true", "included"):
        score += 5
        notes.append("baggage included")

    if 3 <= days <= 10:
        score += 5
        notes.append("good length")

    score = max(0, min(100, score))

    if score >= 80:
        grade, verdict = "A", "GOOD"
    elif score >= 65:
        grade, verdict = "B", "GOOD"
    elif score >= 50:
        grade, verdict = "C", "AVERAGE"
    else:
        grade, verdict = "D", "POOR"

    return {
        "ai_score": score,
        "ai_grading": grade,
        "ai_verdict": verdict,
        "ai_notes": "; ".join(notes),
    }


# ============================================================
# Stage 3 — write-back
# ============================================================

OUTPUT_COLS = ["ai_score", "ai_grading", "ai_verdict", "ai_notes", 
"scored_timestamp"]


def ensure_columns(ws, headers, hmap):
    missing = [c for c in OUTPUT_COLS if c not in hmap]
    if not missing:
        return headers, hmap

    ws.update("1:1", [headers + missing])
    time.sleep(0.5)

    new_headers = ws.row_values(1)
    return new_headers, build_header_map(new_headers)


def write_row(ws, row, hmap, status_col, updates):
    current = normalize(ws.cell(row, hmap[status_col]).value)
    if current != "NEW":
        log(f"Guard skip row {row} (status={current})")
        return

    data = []
    for k, v in updates.items():
        if k in hmap:
            data.append({"range": a1(row, hmap[k]), "values": [[v]]})

    ws.batch_update(data)


# ============================================================
# Main
# ============================================================

def main():
    log("AI SCORER STARTING")

    get_env("SHEET_ID")
    get_env("GCP_SA_JSON")
    log("Environment OK")

    ws = get_worksheet()
    log(f"Connected to worksheet: {ws.title}")
    log("AI SCORER STAGE 1 COMPLETE")

    hit = find_first_new(ws)
    if not hit:
        log("Stage 2: No NEW rows found")
        return

    row = hit["row_number"]
    rec = hit["record"]
    status_col = hit["status_col"]

    log(f"Stage 2: Found NEW row #{row}")

    headers = ws.row_values(1)
    hmap = build_header_map(headers)
    headers, hmap = ensure_columns(ws, headers, hmap)

    try:
        result = score_deal(rec)
        updates = {
            "ai_score": result["ai_score"],
            "ai_grading": result["ai_grading"],
            "ai_verdict": result["ai_verdict"],
            "ai_notes": result["ai_notes"],
            "scored_timestamp": utc_now(),
            status_col: "SCORED",
        }
        write_row(ws, row, hmap, status_col, updates)
        log(f"Stage 3: Row #{row} scored and promoted to SCORED")

    except Exception as e:
        write_row(
            ws,
            row,
            hmap,
            status_col,
            {
                "ai_notes": f"ERROR_SCORING: {str(e)[:200]}",
                "scored_timestamp": utc_now(),
                status_col: "ERROR_SCORING",
            },
        )
        die(f"Stage 3 failed: {e}")


if __name__ == "__main__":
    main()

