#!/usr/bin/env python3
"""
V3.2 — Render Worker (Stage B) — Debug/Robust Edition

Trigger condition (first match):
- raw_status / RAW_STATUS == "SCORED"
- ai_verdict / AI_VERDICT == "GOOD"
- workflow in {"", "RENDER_AGAIN", "NEEDS_IMAGE"}

Actions:
1) Lock row by setting workflow = NEEDS_IMAGE (guarded)
2) POST payload to RENDER_URL
3) On success: write graphic_url, rendered_timestamp; set workflow=READY_TO_POST
4) On failure: write render_error + status codes; set workflow=RENDER_AGAIN

Important:
- NEVER edits header row (works with protected headers)
- Uses header mapping (never column letters)
- Uses 'workflow' column instead of 'status' to avoid confusion with raw_status
"""

import os
import sys
import json
import ssl
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


def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


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
    if v is None:
        return ""
    s = str(v)
    # Remove invisible characters
    s = s.replace("\u00A0", " ")   # NBSP
    s = s.replace("\u200B", "")    # zero-width space
    s = s.replace("\uFEFF", "")    # BOM
    return s.strip().upper()


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
    "workflow",
    "graphic_url",
    "rendered_timestamp",
    "render_error",
    "render_http_status",
    "render_response_snippet",
]


def require_columns(hmap: Dict[str, int], cols: List[str]) -> None:
    missing = [c for c in cols if c not in hmap]
    if missing:
        die(
            "ERROR: Sheet missing required columns: "
            + ", ".join(missing)
            + ". Add these headers to row 1 in RAW_DEALS (far right), then rerun."
        )


def find_col_name(hmap: Dict[str, int], candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in hmap:
            return c
    return None


# ============================================================
# HTTP Render call
# ============================================================

def _ssl_context() -> ssl.SSLContext:
    # After running Install Certificates.command this should work fine.
    return ssl.create_default_context()


def call_render(render_url: str, payload: Dict[str, Any], timeout_seconds: int) -> Dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    req = Request(
        render_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    ctx = _ssl_context()

    with urlopen(req, timeout=timeout_seconds, context=ctx) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
        try:
            return json.loads(raw)
        except Exception:
            raise RuntimeError("Render response was not JSON. First 300 chars: " + raw[:300])


# ============================================================
# Row selection + update
# ============================================================

def get_all(ws) -> Tuple[List[str], List[List[str]]]:
    values = ws.get_all_values()
    if len(values) < 2:
        return [], []
    return values[0], values[1:]


def record_from_row(headers: List[str], row: List[str]) -> Dict[str, str]:
    if len(row) < len(headers):
        row = row + [""] * (len(headers) - len(row))
    return {headers[i]: row[i] for i in range(len(headers))}


def find_first_render_candidate(ws) -> Optional[Dict[str, Any]]:
    headers, rows = get_all(ws)
    if not headers:
        return None

    hmap = build_header_map(headers)

    # Check for required columns (case-insensitive)
    raw_status_col = None
    ai_verdict_col = None
    workflow_col = None
    
    for header, idx in hmap.items():
        header_upper = header.upper()
        if header_upper == "RAW_STATUS":
            raw_status_col = header
        elif header_upper == "AI_VERDICT":
            ai_verdict_col = header
        elif header_upper == "WORKFLOW":
            workflow_col = header

    if not raw_status_col:
        log(f"Available headers: {list(hmap.keys())}")
        die("ERROR: Missing 'raw_status' column in sheet.")
    if not ai_verdict_col:
        die("ERROR: Missing 'ai_verdict' column in sheet.")
    if not workflow_col:
        log(f"Available headers: {list(hmap.keys())}")
        die("ERROR: Missing 'workflow' column in sheet. Please add a 'workflow' column to your sheet headers.")

    log(f"Using columns: raw_status='{raw_status_col}', ai_verdict='{ai_verdict_col}', workflow='{workflow_col}'")

    raw_status_idx = hmap[raw_status_col] - 1
    ai_verdict_idx = hmap[ai_verdict_col] - 1
    workflow_idx = hmap[workflow_col] - 1

    allowed_workflows = {"", "RENDER_AGAIN", "NEEDS_IMAGE"}

    for i, row in enumerate(rows, start=2):
        if len(row) < len(headers):
            row = row + [""] * (len(headers) - len(row))

        raw_status = normalize(row[raw_status_idx])
        ai_verdict = normalize(row[ai_verdict_idx])
        workflow = normalize(row[workflow_idx])
        
        log(f"DEBUG Row {i}: raw_status='{raw_status}' ai_verdict='{ai_verdict}' workflow='{workflow}'")

        if raw_status == "SCORED" and ai_verdict == "GOOD" and workflow in allowed_workflows:
            rec = record_from_row(headers, row)
            return {
                "row_number": i,
                "record": rec,
                "workflow_col": workflow_col,
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


def pick(rec: Dict[str, Any], *keys: str) -> str:
    for k in keys:
        if k in rec and normalize(rec.get(k)):
            return normalize(rec.get(k))
    return ""


# ============================================================
# Main
# ============================================================

def main():
    log("RENDER WORKER STARTING")

    get_env("SHEET_ID")
    get_env("GCP_SA_JSON")
    log("Environment OK")

    render_url = get_env("RENDER_URL", required=False, default="https://greenroomman.pythonanywhere.com/render")
    timeout_s = int(get_env("RENDER_TIMEOUT", required=False, default="90"))
    debug = env_bool("RENDER_DEBUG", default=False)

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

    require_columns(hmap, REQUIRED_COLS)

    workflow_col_name = hit["workflow_col"]
    workflow_col_index = hmap[workflow_col_name]

    deal_id = pick(rec, "deal_id", "DEAL_ID")
    origin = pick(rec, "origin_city", "ORIGIN_CITY")
    dest = pick(rec, "destination_city", "DESTINATION_CITY")
    price = pick(rec, "price_gbp", "PRICE_GBP")
    out_date = pick(rec, "outbound_date", "OUTBOUND_DATE")

    log(f"Found render candidate row #{row} | deal_id={deal_id} | {origin}->{dest} | £{price} | outbound={out_date}")

    # Lock row
    current_workflow = get_cell(ws, row, workflow_col_index)
    log(f"DEBUG: Row {row} workflow cell raw value: '{current_workflow}' | normalized: '{normalize(current_workflow)}' | len={len(current_workflow)}")
    if current_workflow not in ("", "RENDER_AGAIN", "NEEDS_IMAGE"):
        log(f"Guard skip row {row} (workflow changed to {current_workflow})")
        return

    batch_update_row(
        ws,
        row,
        hmap,
        {
            "workflow": "NEEDS_IMAGE",
            "render_error": "",
            "render_http_status": "",
            "render_response_snippet": "",
        },
    )
    log(f"Row #{row} locked: workflow=NEEDS_IMAGE")

    # Payload - keep small & predictable
    payload = {
        "deal_id": deal_id,
        "origin_city": origin,
        "destination_city": dest,
        "destination_country": pick(rec, "destination_country", "DESTINATION_COUNTRY"),
        "price_gbp": price,
        "outbound_date": out_date,
        "return_date": pick(rec, "return_date", "RETURN_DATE"),
        "trip_length_days": pick(rec, "trip_length_days", "TRIP_LENGTH_DAYS"),
        "stops": pick(rec, "stops", "STOPS"),
        "baggage_included": pick(rec, "baggage_included", "BAGGAGE_INCLUDED"),
        "airline": pick(rec, "airline", "AIRLINE"),
        "ai_score": pick(rec, "ai_score", "AI_SCORE"),
        "ai_grading": pick(rec, "ai_grading", "AI_GRADING"),
        "ai_verdict": pick(rec, "ai_verdict", "AI_VERDICT"),
        "ai_caption": pick(rec, "ai_caption", "AI_CAPTION"),
    }

    if debug:
        log("RENDER_DEBUG=1 enabled")
        log("Payload keys: " + ", ".join(sorted(payload.keys())))

    try:
        result = call_render(render_url, payload, timeout_seconds=timeout_s)

        graphic_url = normalize(result.get("graphic_url") or result.get("image_url") or "")
        if not graphic_url:
            raise RuntimeError("Render succeeded but returned no graphic_url/image_url.")

        batch_update_row(
            ws,
            row,
            hmap,
            {
                "graphic_url": graphic_url,
                "rendered_timestamp": utc_now(),
                "render_error": "",
                "render_http_status": "200",
                "render_response_snippet": "",
                "workflow": "READY_TO_POST",
            },
        )
        log(f"Render OK: row #{row} workflow=READY_TO_POST | graphic_url set")
        return

    except HTTPError as e:
        # Read server-provided HTML error body
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            body = ""
        snippet = (body or "")[:240]
        status = str(getattr(e, "code", "HTTPError"))
        batch_update_row(
            ws,
            row,
            hmap,
            {
                "rendered_timestamp": utc_now(),
                "render_error": "HTTPError " + status,
                "render_http_status": status,
                "render_response_snippet": snippet,
                "workflow": "RENDER_AGAIN",
            },
        )
        die(f"Render FAILED for row #{row}: HTTPError {status}: {snippet}", code=2)

    except URLError as e:
        msg = str(e)
        batch_update_row(
            ws,
            row,
            hmap,
            {
                "rendered_timestamp": utc_now(),
                "render_error": "URLError: " + msg[:200],
                "render_http_status": "",
                "render_response_snippet": "",
                "workflow": "RENDER_AGAIN",
            },
        )
        die(f"Render FAILED for row #{row}: URLError: {msg}", code=2)

    except Exception as e:
        msg = str(e)
        batch_update_row(
            ws,
            row,
            hmap,
            {
                "rendered_timestamp": utc_now(),
                "render_error": "ERROR: " + msg[:200],
                "render_http_status": "",
                "render_response_snippet": "",
                "workflow": "RENDER_AGAIN",
            },
        )
        die(f"Render FAILED for row #{row}: {msg}", code=2)


if __name__ == "__main__":
    main()
