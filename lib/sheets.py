#!/usr/bin/env python3
"""
TravelTxter - Google Sheets Helper Library
Provides authentication and utility functions for Google Sheets operations
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
    
    Args:
        key: Environment variable name
        
    Returns:
        Environment variable value
        
    Raises:
        ValueError: If environment variable not set
    """
    value = os.getenv(key)
    if not value:
        raise ValueError(f"{key} environment variable not set")
    return value.strip()


def get_gspread_client():
    """
    Get authenticated gspread client.
    Supports both environment variable (GitHub Actions) and file (local dev).
    
    Returns:
        Authorized gspread client
    """
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    
    # Try environment variable first (GitHub Actions)
    gcp_json_str = os.getenv('GCP_SA_JSON')
    
    if gcp_json_str:
        # GitHub Actions path
        try:
            creds_info = json.loads(gcp_json_str)
            creds = service_account.Credentials.from_service_account_info(
                creds_info,
                scopes=scopes
            )
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid GCP_SA_JSON format: {e}")
    else:
        # Local dev path
        service_account_file = os.getenv('SERVICE_ACCOUNT_JSON', 'service_account.json')
        if not os.path.exists(service_account_file):
            raise FileNotFoundError(
                f"Service account file not found: {service_account_file}\n"
                f"Set GCP_SA_JSON env var or place service_account.json in project root"
            )
        creds = service_account.Credentials.from_service_account_file(
            service_account_file,
            scopes=scopes
        )
    
    return gspread.authorize(creds)


def now_iso() -> str:
    """
    Get current timestamp in ISO format.
    
    Returns:
        ISO formatted timestamp string
    """
    return datetime.utcnow().isoformat() + 'Z'


# =============================================================================
# WORKSHEET HELPERS
# =============================================================================

def get_ws():
    """
    Open worksheet using environment variables.
    
    Environment variables:
        SPREADSHEET_ID (preferred) OR SPREADSHEET_NAME OR SHEET_ID
        DEALS_SHEET_NAME (default: RAW_DEALS)
    
    Returns:
        gspread.Worksheet object
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
    Ensure worksheet has required headers and return header map.
    
    Args:
        ws: Worksheet object
        required_headers: List of required header names
        
    Returns:
        Dictionary mapping header name to column number (1-based)
        
    Raises:
        ValueError: If required headers are missing
    """
    headers = ws.row_values(1)
    missing = [h for h in required_headers if h not in headers]
    
    if missing:
        raise ValueError(f"Sheet missing required columns: {missing}")
    
    return {h: headers.index(h) + 1 for h in headers}


def update_row_by_headers(ws, header_map: Dict[str, int], row_num: int, updates: Dict[str, Any]):
    """
    Update specific cells in a row using header names.
    
    Args:
        ws: Worksheet object
        header_map: Dictionary mapping header names to column numbers
        row_num: Row number to update (1-based)
        updates: Dictionary of {header_name: value} to update
    """
    for header, value in updates.items():
        if header in header_map:
            col = header_map[header]
            ws.update_cell(row_num, col, str(value))


# =============================================================================
# TELEGRAM PUBLISHING HELPERS
# =============================================================================

STATUS_READY = "READY_TO_POST"
STATUS_POSTING = "POSTING"
STATUS_POSTED = "POSTED"
STATUS_ERROR = "ERROR"


def _safe_float(x: Any) -> Optional[float]:
    """Safely convert value to float."""
    try:
        return float(str(x).strip())
    except Exception:
        return None


def release_back_to_ready(ws, header_map: Dict[str, int], row_num: int):
    """
    Release a deal back to READY status and clear locks.
    
    Args:
        ws: Worksheet object
        header_map: Dictionary mapping header names to column numbers
        row_num: Row number to update
    """
    update_row_by_headers(
        ws, header_map, row_num,
        {
            "status": STATUS_READY,
            "processing_lock": "",
            "locked_by": ""
        }
    )


def claim_first_available(
    ws,
    required_headers: List[str],
    status_col: str,
    wanted_status: str,
    set_status: str,
    worker_id: str,
    max_lock_age: timedelta = timedelta(minutes=30)
) -> Optional[Dict[str, Any]]:
    """
    Claim the first available row matching criteria.
    
    Args:
        ws: Worksheet object
        required_headers: List of required header names
        status_col: Name of status column
        wanted_status: Status value to look for
        set_status: Status to set when claiming
        worker_id: ID of the worker claiming the row
        max_lock_age: Maximum age of existing locks to consider stale
        
    Returns:
        Dictionary of row data with '_row_number' key, or None if no rows available
    """
    all_data = ws.get_all_values()
    if len(all_data) < 2:
        return None
    
    headers = all_data[0]
    rows = all_data[1:]
    
    # Build header map
    header_map = {h: i for i, h in enumerate(headers)}
    
    # Check required headers exist
    for h in required_headers:
        if h not in header_map:
            raise ValueError(f"Required header missing: {h}")
    
    status_idx = header_map.get(status_col)
    lock_idx = header_map.get("processing_lock")
    locked_by_idx = header_map.get("locked_by")
    
    if status_idx is None:
        raise ValueError(f"Status column '{status_col}' not found")
    
    now = datetime.utcnow()
    
    for row_num, row in enumerate(rows, start=2):  # Start at 2 (row 1 is headers)
        # Check if row has enough columns
        if len(row) <= max(status_idx, lock_idx or 0, locked_by_idx or 0):
            continue
        
        # Check status
        if row[status_idx].strip() != wanted_status:
            continue
        
        # Check lock
        if lock_idx is not None:
            lock_time_str = row[lock_idx].strip()
            if lock_time_str:
                try:
                    lock_time = datetime.fromisoformat(lock_time_str.replace('Z', '+00:00'))
                    if now - lock_time.replace(tzinfo=None) < max_lock_age:
                        # Lock is still fresh, skip this row
                        continue
                except Exception:
                    # Invalid lock timestamp, treat as stale
                    pass
        
        # Claim this row
        updates = {
            status_col: set_status,
            "processing_lock": now_iso(),
            "locked_by": worker_id
        }
        
        # Update the row
        hm = {h: headers.index(h) + 1 for h in headers}
        update_row_by_headers(ws, hm, row_num, updates)
        
        # Build return data
        result = {"_row_number": row_num}
        for h in required_headers:
            idx = header_map.get(h)
            if idx is not None and idx < len(row):
                result[h] = row[idx]
            else:
                result[h] = ""
        
        return result
    
    return None


def get_ready_deal(
    worker_id: str,
    allow_verdicts: Tuple[str, ...] = ("GOOD",),
    min_ai_score: Optional[float] = None,
    max_lock_age_minutes: int = 30
) -> Optional[Dict[str, Any]]:
    """
    Claims and returns one deal ready for posting.
    
    Uses locking mechanism and sets status -> POSTING.
    Prevents reposts by requiring published_timestamp empty.
    
    Args:
        worker_id: ID of the worker claiming the deal
        allow_verdicts: Tuple of acceptable AI verdict values
        min_ai_score: Minimum AI score required (if set)
        max_lock_age_minutes: Maximum age of locks in minutes
        
    Returns:
        Dictionary with deal data, or None if no deals available
    """
    ws = get_ws()

    # Require these columns for safe posting
    required = ["deal_id", "status", "published_timestamp", "processing_lock", "locked_by"]
    # These are optional filters if present
    optional = ["ai_verdict", "ai_score", "ai_caption"]

    headers = ws.row_values(1)
    for col in required:
        if col not in headers:
            raise ValueError(f"Sheet missing required column: {col}")

    hm = {h: headers.index(h) + 1 for h in headers}

    deal = claim_first_available(
        ws=ws,
        required_headers=list(set(required + [c for c in optional if c in headers])),
        status_col="status",
        wanted_status=STATUS_READY,
        set_status=STATUS_POSTING,
        worker_id=worker_id,
        max_lock_age=timedelta(minutes=max_lock_age_minutes),
    )
    
    if not deal:
        return None

    # Anti-repost check
    if str(deal.get("published_timestamp", "")).strip():
        # Already posted, mark as posted and clear lock
        mark_posted(deal["deal_id"], keep_existing_timestamp=True)
        return None

    # Verdict filter (if column exists and has a value)
    verdict = str(deal.get("ai_verdict", "")).strip().upper()
    if allow_verdicts and verdict and verdict not in allow_verdicts:
        release_back_to_ready(ws, hm, deal["_row_number"])
        return None

    # Score filter
    if min_ai_score is not None:
        score = _safe_float(deal.get("ai_score", ""))
        if score is None or score < float(min_ai_score):
            release_back_to_ready(ws, hm, deal["_row_number"])
            return None

    return deal


def mark_posted(deal_id: str, keep_existing_timestamp: bool = False):
    """
    Mark a deal as posted and clear locks.
    
    Args:
        deal_id: Deal ID to mark as posted
        keep_existing_timestamp: If True, don't overwrite existing timestamp
    """
    ws = get_ws()
    hm = ensure_headers(ws, ["deal_id", "status", "processing_lock", "locked_by", "published_timestamp"])

    cell = ws.find(deal_id)
    if not cell:
        raise RuntimeError(f"Could not find deal_id in sheet: {deal_id}")

    updates = {
        "status": STATUS_POSTED,
        "processing_lock": "",
        "locked_by": "",
    }
    
    if not keep_existing_timestamp:
        updates["published_timestamp"] = now_iso()

    update_row_by_headers(ws, hm, cell.row, updates)


def mark_error(deal_id: str, error_msg: str):
    """
    Mark a deal as errored and log the error message.
    
    Args:
        deal_id: Deal ID that errored
        error_msg: Error message to log
    """
    ws = get_ws()
    headers = ws.row_values(1)
    hm = {h: headers.index(h) + 1 for h in headers}

    cell = ws.find(deal_id)
    if not cell:
        raise RuntimeError(f"Could not find deal_id in sheet: {deal_id}")

    row_num = cell.row
    
    # Clear locks and set error status
    update_row_by_headers(
        ws, hm, row_num,
        {
            "status": STATUS_ERROR,
            "processing_lock": "",
            "locked_by": ""
        }
    )

    # Log error to notes column
    target = "ai_notes" if "ai_notes" in hm else ("notes" if "notes" in hm else None)
    if target:
        existing = ws.cell(row_num, hm[target]).value or ""
        appended = (existing + "\n" if existing else "") + f"[TELEGRAM_ERROR {now_iso()}] {error_msg}"
        # Truncate to avoid hitting Sheets cell size limit
        update_row_by_headers(ws, hm, row_num, {target: appended[:45000]})
