#!/usr/bin/env python3
"""
V3.2 — Render Worker (Stage B)

Trigger condition (first match):
- raw_status / RAW_STATUS == "SCORED"
- ai_verdict / AI_VERDICT == "GOOD"
- status is "" OR "RENDER_AGAIN" OR "NEEDS_IMAGE"

Actions:
1) Lock row by setting status = NEEDS_IMAGE (guarded)
2) POST payload to RENDER_URL (default PythonAnywhere /render)
3) On success:
   - write graphic_url, rendered_timestamp
   - set status = READY_TO_POST
4) On failure:
   - write render_error, rendered_timestamp
   - set status = RENDER_AGAIN

Safety:
- NEVER edits header row (works with protected headers)
- Uses header-name mapping (never column letters)
- Guards against races by re-checking status before locking
"""

import os
import sys
import json
import datetime as dt
from typing import Dict, Any, List, Optional, Tuple
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


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
# Env
# ============================================================

def get_env(name: str, required: bool = True, default: str = "") -> str:
    v = os.getenv(name)
    if not v:
        if required:
            die(f"ERROR: Missing environment variable: {name}")
        return default
    return v


# ============================================================
# Google Sheets
# ============================================================

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def get_worksheet():
    sheet_id = get_env("SHEET_ID", required=True)
    worksheet_name = get_env("WORKSHEET_NAME", required=False, default="RAW_DEALS")
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


def normalize(v: Any) -> str:
    return "" if v is None else str(v).strip()


def build_header_map(headers: List[str]) -> Dict[str, int]:
    return {h.strip(): i + 1 for i, h in enumerate(headers) if h.strip()}


def col_to_a1(n: int) -> str:
    result = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        result = chr(65 + r) + result
    return result


def a1(row: int, col: int) -> str:
    return f"{col_to_a1(col)}{row}"


# ============================================================
# Required columns (NO header edits)
# ============================================================

REQUIRED_COLS = [
    "status",
    "graphic_url",
    "rendered_timestamp",
    "render_error",
]


def require_columns(hmap: Dict[str, int], cols: List[str]) -> None:
    missing = [c for c in cols if c not in hmap]
    if missing:
        die(
            "ERROR: Your sheet is missing required columns: "
            + ", ".join(missing)
            + ". Add these headers to row 1 in RAW_DEALS (far right), then rerun."
        )


def find_col_name(hmap: Dict[str, int], candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in hmap:
            return c
    return None


# ============================================================
# HTTP Render call (stdlib)
# ============================================================

def call_render(render_url: str, payload: Dict[str, Any], timeout_seconds: int = 30) -> Dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    req = Request(
        render_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urlopen(req, timeout=timeout_seconds) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            try:
                return json.loads(raw)
            except Exception:
                raise RuntimeError(f"Render response was not JSON. Raw: {raw[:300]}")
    except HTTPError as e:
        msg = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else str(e)
        raise RuntimeError(f"HTTPError {e.code}: {msg[:300]}")
    except URLError as e:
        raise RuntimeError(f"URLError: {e}")
    except Exception as e:
        raise RuntimeError(str(e))


# ============================================================
# Row selection + update
# ============================================================

def get_all(ws) -> Tuple[List[str], List[List[str]]]:
    values = ws.get_all_values()
    if len(values) < 2:
        return [], []
    return values[0], values[1:]


def record_from_row(headers: List[str], row: List[str]) -> Dict[str, str]:
    # pad row to header length
    if len(row) < len(headers):
        row = row + [""] * (len(headers) - len(row))
    return {headers[i]: row[i] for i in range(len(headers))}


def find_first_render_candidate(ws) -> Optional[Dict[str, Any]]:
    headers, rows = get_all(ws)
    if not headers:
        return None

    hmap = build_header_map(headers)

    raw_status_col = find_col_name(hmap, ["raw_status", "RAW_STATUS"])
    ai_verdict_col = find_col_name(hmap, ["ai_verdict", "AI_VERDICT"])
    status_col = find_col_name(hmap, ["status", "STATUS"])

    if not raw_status_col:
        die("ERROR: Missing raw_status or RAW_STATUS column.")
    if not ai_verdict_col:
        die("ERROR: Missing ai_verdict or AI_VERDICT column.")
    if not status_col:
        die("ERROR: Missing status column.")

    raw_status_idx = hmap[raw_status_col] - 1
    ai_verdict_idx = hmap[ai_verdict_col] - 1
    status_idx = hmap[status_col] - 1

    allowed_statuses = {"", "RENDER_AGAIN", "NEEDS_IMAGE"}

    for i, row in enumerate(rows, start=2):  # sheet row number
        if len(row) < len(headers):
            row = row + [""] * (len(headers) - len(row))

        raw_status = normalize(row[raw_status_idx])
        ai_verdict = normalize(row[ai_verdict_idx])
        status = normalize(row[status_idx])

        if raw_status == "SCORED" and ai_verdict == "GOOD" and status in allowed_statuses:
            rec = record_from_row(headers, row)
            return {
                "row_number": i,
                "record": rec,
                "raw_status_col": raw_status_col,
                "ai_verdict_col": ai_verdict_col,
                "status_col": status_col,
                "headers": headers,
            }

    return None


def batch_update_row(ws, row: int, hmap: Dict[str, int], updates: Dict[str, Any]) -> None:
    data = []
    for k, v in updates.items():
        if k in hmap:
            data.append({"range": a1(row, hmap[k]), "values": [[v]]})
    if data:
        ws.batch_update(data)


def get_cell(ws, row: int, col_1_based: int) -> str:
    return normalize(ws.cell(row, col_1_based).value)


# ============================================================
# Main
# ============================================================

def main():
    log("RENDER WORKER STARTING")

    # Required env (same as scorer)
    get_env("SHEET_ID")
    get_env("GCP_SA_JSON")
    log("Environment OK")

    render_url = get_env("RENDER_URL", required=False, default="https://greenroomman.pythonanywhere.com/render")
    timeout_s = int(get_env("RENDER_TIMEOUT", required=False, default="30"))

    ws = get_worksheet()
    log(f"Connected to worksheet: {ws.title}")

    hit = find_first_render_candidate(ws)
    if not hit:
        log("No render candidates found. Nothing to do.")
        return

    row = hit["row_number"]
    rec = hit["record"]
    headers = hit["headers"]

    hmap = build_header_map(headers)

    # Require output columns (NO editing headers)
    require_columns(hmap, REQUIRED_COLS)

    status_col_name = hit["status_col"]
    status_col_index = hmap[status_col_name]

    deal_id = normalize(rec.get("deal_id") or rec.get("DEAL_ID"))
    origin = normalize(rec.get("origin_city") or rec.get("ORIGIN_CITY"))
    dest = normalize(rec.get("destination_city") or rec.get("DESTINATION_CITY"))
    price = normalize(rec.get("price_gbp") or rec.get("PRICE_GBP"))
    out_date = normalize(rec.get("outbound_date") or rec.get("OUTBOUND_DATE"))

    log(f"Found render candidate row #{row} | deal_id={deal_id} | {origin}->{dest} | £{price} | outbound={out_date}")

    # -------- Lock row (guard) --------
    current_status = get_cell(ws, row, status_col_index)
    if current_status not in ("", "RENDER_AGAIN", "NEEDS_IMAGE"):
        log(f"Guard skip row {row} (status changed to {current_status})")
        return

    batch_update_row(ws, row, hmap, {"status": "NEEDS_IMAGE", "render_error": ""})
    log(f"Row #{row} locked: status=NEEDS_IMAGE")

    # -------- Build payload to /render --------
    # Keep payload simple + explicit. PythonAnywhere can ignore fields it doesn't need.
    payload = {
        "deal_id": deal_id,
        "origin_city": origin,
        "destination_city": dest,
        "destination_country": normalize(rec.get("destination_country") or rec.get("DESTINATION_COUNTRY")),
        "price_gbp": price,
        "outbound_date": out_date,
        "return_date": normalize(rec.get("return_date") or rec.get("RETURN_DATE")),
        "trip_length_days": normalize(rec.get("trip_length_days") or rec.get("TRIP_LENGTH_DAYS")),
        "stops": normalize(rec.get("stops") or rec.get("STOPS")),
        "baggage_included": normalize(rec.get("baggage_included") or rec.get("BAGGAGE_INCLUDED")),
        "airline": normalize(rec.get("airline") or rec.get("AIRLINE")),
        "ai_score": normalize(rec.get("ai_score") or rec.get("AI_SCORE")),
        "ai_grading": normalize(rec.get("ai_grading") or rec.get("AI_GRADING")),
        "ai_verdict": normalize(rec.get("ai_verdict") or rec.get("AI_VERDICT")),
        "ai_caption": normalize(rec.get("ai_caption") or rec.get("AI_CAPTION")),
    }

    # -------- Call render --------
    try:
        result = call_render(render_url, payload, timeout_seconds=timeout_s)

        graphic_url = normalize(result.get("graphic_url") or result.get("image_url") or "")
        if not graphic_url:
            raise RuntimeError("Render succeeded but returned no graphic_url/image_url.")

        updates = {
            "graphic_url": graphic_url,
            "rendered_timestamp": utc_now(),
            "render_error": "",
            "status": "READY_TO_POST",
        }
        batch_update_row(ws, row, hmap, updates)
        log(f"Render OK: row #{row} status=READY_TO_POST | graphic_url set")

    except Exception as e:
        err = str(e)[:240]
        updates = {
            "rendered_timestamp": utc_now(),
            "render_error": err,
            "status": "RENDER_AGAIN",
        }
        batch_update_row(ws, row, hmap, updates)
        die(f"Render FAILED for row #{row}: {err}", code=2)


if __name__ == "__main__":
    main()

