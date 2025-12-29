#!/usr/bin/env python3
"""
TravelTxter - Google Sheets Helper Library (Option A: raw_status is source of truth)

✅ Standard:
- ALL pipeline state is stored in column: raw_status
- Telegram worker claims rows by raw_status, not status

Key improvements vs your version:
- Telegram helpers now use raw_status everywhere (release/claim/posted/error)
- Adds env overrides for wanted/claim/posted statuses so you can evolve the state machine
- Anti-repost can use telegram_published_timestamp if present, otherwise published_timestamp

Environment variables used:
- GCP_SA_JSON (preferred) OR SERVICE_ACCOUNT_JSON file
- SPREADSHEET_ID (preferred) OR SHEET_ID OR SPREADSHEET_NAME
- DEALS_SHEET_NAME (default RAW_DEALS)

Telegram-specific optional env overrides:
- TELEGRAM_WANTED_STATUS (default READY_TO_POST)
- TELEGRAM_SET_STATUS (default POSTING)
- TELEGRAM_POSTED_STATUS (default POSTED)
"""

import os
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Tuple
from google.oauth2 import service_account
import gspread


# =============================================================================
# AUTHENTICATION & CLIENT
# =============================================================================

def get_env(key: str) -> str:
    """
    Get environment variable or raise error if not set.
    """
    value = os.getenv(key)
    if not value:
        raise ValueError(f"{key} environment variable not set")
    return value.strip()


def get_gspread_client():
    """
    Get authenticated gspread client.
    Supports both environment variable (GitHub Actions) and file (local dev).
    """
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    # Try environment variable first (GitHub Actions)
    gcp_json_str = os.getenv("GCP_SA_JSON")

    if gcp_json_str:
        try:
            creds_info = json.loads(gcp_json_str)
            creds = service_account.Credentials.from_service_account_info(
                creds_info,
                scopes=scopes,
            )
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid GCP_SA_JSON format: {e}")
    else:
        # Local dev path
        service_account_file = os.getenv("SERVICE_ACCOUNT_JSON", "service_account.json")
        if not os.path.exists(service_account_file):
            raise FileNotFoundError(
                f"Service account file not found: {service_account_file}\n"
                f"Set GCP_SA_JSON env var or place service_account.json in project root"
            )
        creds = service_account.Credentials.from_service_account_file(
            service_account_file,
            scopes=scopes,
        )

    return gspread.authorize(creds)


def now_iso() -> str:
    """
    Get current UTC timestamp in ISO format.
    """
    return datetime.utcnow().isoformat() + "Z"


# =============================================================================
# WORKSHEET HELPERS
# =============================================================================

def get_ws():
    """
    Open worksheet using environment variables.

    Environment variables:
        SPREADSHEET_ID (preferred) OR SPREADSHEET_NAME OR SHEET_ID
        DEALS_SHEET_NAME (default: RAW_DEALS)
    """
    gc = get_gspread_client()
    sheet_name = os.getenv("DEALS_SHEET_NAME", "RAW_DEALS").strip() or "RAW_DEALS"

    # Try SPREADSHEET_ID first (preferred)
    sid = os.getenv("SPREADSHEET_ID", "").strip()

    # Fallback to SHEET_ID (compatibility)
    if not sid:
        sid = os.getenv("SHEET_ID", "").strip()

    # Fallback to SPREADSHEET_NAME
    sname = os.getenv("SPREADSHEET_NAME", "").strip()

    if sid:
        sh = gc.open_by_key(sid)
    elif sname:
        sh = gc.open(sname)
    else:
        raise ValueError(
            "Missing SPREADSHEET_ID (preferred), SHEET_ID, or SPREADSHEET_NAME environment variable"
        )

    return sh.worksheet(sheet_name)


def ensure_headers(ws, required_headers: List[str]) -> Dict[str, int]:
    """
    Ensure worksheet has required headers and return header map (1-based).
    """
    headers = ws.row_values(1)
    missing = [h for h in required_headers if h not in headers]
    if missing:
        raise ValueError(f"Sheet missing required columns: {missing}")

    return {h: headers.index(h) + 1 for h in headers}


def update_row_by_headers(ws, header_map: Dict[str, int], row_num: int, updates: Dict[str, Any]):
    """
    Update specific cells in a row using header names.

    NOTE: This uses update_cell per field (simple + reliable).
    If you later want performance, we can batch_update.
    """
    for header, value in updates.items():
        if header in header_map:
            col = header_map[header]
            ws.update_cell(row_num, col, str(value))


# =============================================================================
# TELEGRAM PUBLISHING HELPERS (Option A: raw_status)
# =============================================================================

# Single source of truth for state
STATUS_COL = "raw_status"

# Defaults (override via env if needed)
STATUS_READY = "READY_TO_POST"
STATUS_POSTING = "POSTING"
STATUS_POSTED = "POSTED"
STATUS_ERROR = "ERROR"


def _safe_float(x: Any) -> Optional[float]:
    try:
        return float(str(x).strip())
    except Exception:
        return None


def release_back_to_ready(ws, header_map: Dict[str, int], row_num: int):
    """
    Release a deal back to READY status and clear locks.
    """
    update_row_by_headers(
        ws,
        header_map,
        row_num,
        {
            STATUS_COL: os.getenv("TELEGRAM_WANTED_STATUS", STATUS_READY),
            "processing_lock": "",
            "locked_by": "",
        },
    )


def claim_first_available(
    ws,
    required_headers: List[str],
    status_col: str,
    wanted_status: str,
    set_status: str,
    worker_id: str,
    max_lock_age: timedelta = timedelta(minutes=30),
) -> Optional[Dict[str, Any]]:
    """
    Claim the first available row matching criteria.
    Uses a simple lock mechanism (processing_lock + locked_by).
    """
    all_data = ws.get_all_values()
    if len(all_data) < 2:
        return None

    headers = all_data[0]
    rows = all_data[1:]

    # Build header map (0-based indexes for row arrays)
    header_map_0 = {h: i for i, h in enumerate(headers)}

    # Check required headers exist
    for h in required_headers:
        if h not in header_map_0:
            raise ValueError(f"Required header missing: {h}")

    status_idx = header_map_0.get(status_col)
    lock_idx = header_map_0.get("processing_lock")
    locked_by_idx = header_map_0.get("locked_by")

    if status_idx is None:
        raise ValueError(f"Status column '{status_col}' not found")

    now = datetime.utcnow()

    for row_num, row in enumerate(rows, start=2):  # sheet rows start at 2 (row 1 is headers)
        # Ensure row has enough columns to read needed indexes
        needed_max = max(
            status_idx,
            lock_idx if lock_idx is not None else 0,
            locked_by_idx if locked_by_idx is not None else 0,
        )
        if len(row) <= needed_max:
            continue

        # Check status
        if row[status_idx].strip() != wanted_status:
            continue

        # Check lock (if lock columns exist)
        if lock_idx is not None:
            lock_time_str = row[lock_idx].strip()
            if lock_time_str:
                try:
                    lock_time = datetime.fromisoformat(lock_time_str.replace("Z", "+00:00"))
                    if now - lock_time.replace(tzinfo=None) < max_lock_age:
                        continue  # lock still fresh
                except Exception:
                    pass  # invalid lock timestamp => treat as stale

        # Claim this row
        updates = {
            status_col: set_status,
            "processing_lock": now_iso(),
            "locked_by": worker_id,
        }

        # 1-based header map for writing
        hm_1 = {h: headers.index(h) + 1 for h in headers}
        update_row_by_headers(ws, hm_1, row_num, updates)

        # Build return data
        result = {"_row_number": row_num}
        for h in required_headers:
            idx = header_map_0.get(h)
            result[h] = row[idx] if idx is not None and idx < len(row) else ""

        return result

    return None


def get_ready_deal(
    worker_id: str,
    allow_verdicts: Tuple[str, ...] = ("GOOD",),
    min_ai_score: Optional[float] = None,
    max_lock_age_minutes: int = 30,
) -> Optional[Dict[str, Any]]:
    """
    Claims and returns one deal ready for posting.

    Option A: uses raw_status as the state machine column.

    Default flow for Telegram:
      raw_status == READY_TO_POST  -> claim -> set raw_status=POSTING -> post -> raw_status=POSTED

    You can override with env:
      TELEGRAM_WANTED_STATUS (e.g. POSTED_INSTAGRAM)
      TELEGRAM_SET_STATUS
      TELEGRAM_POSTED_STATUS
    """
    ws = get_ws()

    wanted_status = os.getenv("TELEGRAM_WANTED_STATUS", STATUS_READY).strip() or STATUS_READY
    set_status = os.getenv("TELEGRAM_SET_STATUS", STATUS_POSTING).strip() or STATUS_POSTING

    headers = ws.row_values(1)

    # Required columns for safe claiming
    required = ["deal_id", STATUS_COL, "processing_lock", "locked_by"]

    # For anti-repost, prefer telegram_published_timestamp if present, else published_timestamp
    ts_col = "telegram_published_timestamp" if "telegram_published_timestamp" in headers else "published_timestamp"
    required.append(ts_col)

    # Optional filters if present
    optional = ["ai_verdict", "ai_score", "ai_caption"]

    for col in required:
        if col not in headers:
            raise ValueError(f"Sheet missing required column: {col}")

    hm_1 = {h: headers.index(h) + 1 for h in headers}

    deal = claim_first_available(
        ws=ws,
        required_headers=list(set(required + [c for c in optional if c in headers])),
        status_col=STATUS_COL,
        wanted_status=wanted_status,
        set_status=set_status,
        worker_id=worker_id,
        max_lock_age=timedelta(minutes=max_lock_age_minutes),
    )

    if not deal:
        return None

    # Anti-repost check
    if str(deal.get(ts_col, "")).strip():
        # Already posted, mark posted + mark timestamp preserved
        mark_posted(deal["deal_id"], keep_existing_timestamp=True)
        return None

    # Verdict filter (if column exists and has a value)
    verdict = str(deal.get("ai_verdict", "")).strip().upper()
    if allow_verdicts and verdict and verdict not in allow_verdicts:
        release_back_to_ready(ws, hm_1, deal["_row_number"])
        return None

    # Score filter
    if min_ai_score is not None:
        score = _safe_float(deal.get("ai_score", ""))
        if score is None or score < float(min_ai_score):
            release_back_to_ready(ws, hm_1, deal["_row_number"])
            return None

    # Attach which timestamp column we used (helps caller update the right one)
    deal["_timestamp_col"] = ts_col
    return deal


def mark_posted(deal_id: str, keep_existing_timestamp: bool = False):
    """
    Mark a deal as posted and clear locks (Option A: raw_status).
    Writes timestamp to telegram_published_timestamp if that column exists, else published_timestamp.
    """
    ws = get_ws()
    headers = ws.row_values(1)

    ts_col = "telegram_published_timestamp" if "telegram_published_timestamp" in headers else "published_timestamp"
    hm = ensure_headers(ws, ["deal_id", STATUS_COL, "processing_lock", "locked_by", ts_col])

    cell = ws.find(deal_id)
    if not cell:
        raise RuntimeError(f"Could not find deal_id in sheet: {deal_id}")

    posted_status = os.getenv("TELEGRAM_POSTED_STATUS", STATUS_POSTED).strip() or STATUS_POSTED

    updates = {
        STATUS_COL: posted_status,
        "processing_lock": "",
        "locked_by": "",
    }

    if not keep_existing_timestamp:
        updates[ts_col] = now_iso()

    update_row_by_headers(ws, hm, cell.row, updates)


def mark_error(deal_id: str, error_msg: str):
    """
    Mark a deal as errored and log the error message (Option A: raw_status).
    """
    ws = get_ws()
    headers = ws.row_values(1)
    hm = {h: headers.index(h) + 1 for h in headers}

    if "deal_id" not in hm or STATUS_COL not in hm:
        raise ValueError(f"Sheet missing required column(s): deal_id and/or {STATUS_COL}")

    cell = ws.find(deal_id)
    if not cell:
        raise RuntimeError(f"Could not find deal_id in sheet: {deal_id}")

    row_num = cell.row

    # Clear locks and set error status
    update_row_by_headers(
        ws,
        hm,
        row_num,
        {
            STATUS_COL: STATUS_ERROR,
            "processing_lock": "",
            "locked_by": "",
        },
    )

    # Log error to ai_notes/notes if present
    target = "ai_notes" if "ai_notes" in hm else ("notes" if "notes" in hm else None)
    if target:
        existing = ws.cell(row_num, hm[target]).value or ""
        appended = (existing + "\n" if existing else "") + f"[TELEGRAM_ERROR {now_iso()}] {error_msg}"
        update_row_by_headers(ws, hm, row_num, {target: appended[:45000]})
