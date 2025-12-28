import os
import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# === Status values (keep consistent across workers) ===
STATUS_READY = "READY_TO_POST"
STATUS_POSTING = "POSTING"
STATUS_POSTED = "POSTED"
STATUS_ERROR = "ERROR"


def now_iso():
    # UTC ISO, no microseconds
    return datetime.utcnow().replace(microsecond=0).isoformat()


def get_env(name, optional=False):
    v = os.getenv(name, "").strip()
    if not v and not optional:
        raise ValueError(f"Missing required env var: {name}")
    return v


def get_gspread_client():
    info = json.loads(get_env("GCP_SA_JSON"))
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)


def get_ws() -> gspread.Worksheet:
    """
    Opens the spreadsheet + worksheet using env vars:
      - SPREADSHEET_ID (preferred) OR SPREADSHEET_NAME
      - DEALS_SHEET_NAME (default RAW_DEALS)
    """
    gc = get_gspread_client()

    sheet_name = os.getenv("DEALS_SHEET_NAME", "RAW_DEALS").strip() or "RAW_DEALS"

    spreadsheet_id = os.getenv("SPREADSHEET_ID", "").strip()
    spreadsheet_name = os.getenv("SPREADSHEET_NAME", "").strip()

    if spreadsheet_id:
        sh = gc.open_by_key(spreadsheet_id)
    elif spreadsheet_name:
        sh = gc.open(spreadsheet_name)
    else:
        raise ValueError("Missing required env var: SPREADSHEET_ID (preferred) or SPREADSHEET_NAME")

    return sh.worksheet(sheet_name)


def ensure_headers(ws, required_headers):
    actual = ws.row_values(1)
    missing = sorted(set(required_headers) - set(actual))
    if missing:
        raise ValueError(f"Sheet missing columns: {missing}")
    return {h: actual.index(h) + 1 for h in actual}  # 1-based col index


def validate_sheet_schema(ws, required_headers):
    actual = ws.row_values(1)
    missing = sorted(set(required_headers) - set(actual))
    if missing:
        raise ValueError(f"Sheet missing columns: {missing}")


def row_to_dict(headers, values, row_num):
    d = {}
    for i, h in enumerate(headers):
        d[h] = values[i] if i < len(values) else ""
    d["_row_number"] = row_num
    return d


def _parse_lock(lock_value):
    try:
        return datetime.fromisoformat(lock_value)
    except Exception:
        return None


def lock_is_stale(lock_value, max_age):
    if not lock_value:
        return False
    t = _parse_lock(lock_value)
    if not t:
        return True
    return (datetime.utcnow() - t) > max_age


def update_row_by_headers(ws, header_map, row_num, updates):
    cells = []
    for k, v in updates.items():
        if k in header_map:
            cells.append(gspread.Cell(row_num, header_map[k], str(v)))
    if cells:
        ws.update_cells(cells, value_input_option="USER_ENTERED")


def claim_first_available(
    ws,
    required_headers,
    status_col,
    wanted_status,
    set_status,
    worker_id,
    max_lock_age,
):
    """
    Claims first row that matches:
      - status_col == wanted_status
      - processing_lock empty OR stale beyond max_lock_age
    Then writes:
      - processing_lock = now_iso()
      - locked_by = worker_id
      - status_col = set_status
    Returns the freshly fetched row as dict + _row_number
    """
    headers = ws.row_values(1)
    hm = ensure_headers(ws, required_headers)

    status_idx = hm[status_col] - 1
    lock_col = hm.get("processing_lock")
    lock_idx = (lock_col - 1) if lock_col else None

    values = ws.get_all_values()
    if len(values) < 2:
        return None

    for row_num in range(2, len(values) + 1):
        row = values[row_num - 1]
        status_val = row[status_idx] if status_idx < len(row) else ""
        if status_val != wanted_status:
            continue

        lock_val = row[lock_idx] if (lock_idx is not None and lock_idx < len(row)) else ""
        if lock_val and not lock_is_stale(lock_val, max_lock_age):
            continue

        update_row_by_headers(
            ws,
            hm,
            row_num,
            {
                "processing_lock": now_iso(),
                "locked_by": worker_id,
                status_col: set_status,
            },
        )
        fresh = ws.row_values(row_num)
        return row_to_dict(headers, fresh, row_num)

    return None


# =============================================================================
# NEW: Telegram publishing helpers (Step 6)
# =============================================================================

def _safe_float(x: str) -> Optional[float]:
    try:
        return float(str(x).strip())
    except Exception:
        return None


def get_ready_deal(
    worker_id: str,
    allow_verdicts: Tuple[str, ...] = ("GOOD",),
    min_ai_score: Optional[float] = None,
    max_lock_age_minutes: int = 30,
) -> Optional[Dict[str, Any]]:
    """
    Finds + CLAIMS one deal for posting (locks it + flips status to POSTING).

    Filters:
      - status == READY_TO_POST
      - published_timestamp must be empty (anti-repost)
      - if allow_verdicts provided, ai_verdict must be in it (if present)
      - if min_ai_score set, ai_score must be parseable and >= min_ai_score

    Returns:
      dict of row data including _row_number, OR None.
    """

    ws = get_ws()

    required_headers = [
        "deal_id",
        "status",
        "published_timestamp",
        "processing_lock",
        "locked_by",
        "ai_verdict",
        "ai_score",
    ]
    # If some optional columns don't exist, we’ll tolerate it by removing them from required list
    actual = ws.row_values(1)
    required_headers = [h for h in required_headers if h in actual]
    ensure_headers(ws, required_headers + ["deal_id", "status", "published_timestamp"])

    # First claim a row by status/lock rules
    claimed = claim_first_available(
        ws=ws,
        required_headers=list(set(required_headers + ["deal_id", "status", "published_timestamp"])),
        status_col="status",
        wanted_status=STATUS_READY,
        set_status=STATUS_POSTING,
        worker_id=worker_id,
        max_lock_age=timedelta(minutes=max_lock_age_minutes),
    )

    # No available row
    if not claimed:
        return None

    # Anti-repost: if published_timestamp already filled, immediately skip & free it
    published_ts = str(claimed.get("published_timestamp", "")).strip()
    if published_ts:
        # Free it by setting status back to POSTED (it already has published timestamp)
        mark_posted(claimed.get("deal_id", ""), keep_existing_timestamp=True)
        return None

    # Verdict filter (if column exists)
    verdict = str(claimed.get("ai_verdict", "")).strip()
    if allow_verdicts and verdict and verdict not in allow_verdicts:
        # Put it back to READY_TO_POST (we claimed it but don't want it)
        release_back_to_ready(claimed["_row_number"])
        return None

    # Min score filter (if requested)
    if min_ai_score is not None:
        score = _safe_float(claimed.get("ai_score", ""))
        if score is None or score < float(min_ai_score):
            release_back_to_ready(claimed["_row_number"])
            return None

    return claimed


def release_back_to_ready(row_num: int):
    """
    If we claimed a row but decided not to post it (verdict/score filter), restore it.
    """
    ws = get_ws()
    hm = ensure_headers(ws, ["status", "processing_lock", "locked_by"])
    update_row_by_headers(
        ws,
        hm,
        row_num,
        {
            "status": STATUS_READY,
            "processing_lock": "",
            "locked_by": "",
        },
    )


def mark_posted(deal_id: str, keep_existing_timestamp: bool = False):
    """
    Marks row as POSTED and sets published_timestamp (unless keep_existing_timestamp True).
    Clears processing_lock + locked_by.
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
    Marks row as ERROR, clears lock fields, and appends error to ai_notes (or notes).
    """
    ws = get_ws()
    headers = ws.row_values(1)
    hm = {h: headers.index(h) + 1 for h in headers}

    cell = ws.find(deal_id)
    if not cell:
        raise RuntimeError(f"Could not find deal_id in sheet: {deal_id}")
    row_num = cell.row

    # Set ERROR + clear locks
    base_updates = {
        "status": STATUS_ERROR,
        "processing_lock": "",
        "locked_by": "",
    }
    update_row_by_headers(ws, hm, row_num, base_updates)

    # Append message into ai_notes or notes if present
    target = "ai_notes" if "ai_notes" in hm else ("notes" if "notes" in hm else None)
    if target:
        existing = ws.cell(row_num, hm[target]).value or ""
        appended = (existing + "\n" if existing else "") + f"[TELEGRAM_ERROR {now_iso()}] {error_msg}"
        update_row_by_headers(ws, hm, row_num, {target: appended[:45000]})
# 
=============================================================================
# Telegram publishing helpers
# 
=============================================================================

STATUS_READY = "READY_TO_POST"
STATUS_POSTING = "POSTING"
STATUS_POSTED = "POSTED"
STATUS_ERROR = "ERROR"


def get_ws():
    """
    Open worksheet using env vars:
      - SPREADSHEET_ID (preferred) OR SPREADSHEET_NAME
      - DEALS_SHEET_NAME (default RAW_DEALS)
    """
    gc = get_gspread_client()
    sheet_name = os.getenv("DEALS_SHEET_NAME", "RAW_DEALS").strip() or 
"RAW_DEALS"

    sid = os.getenv("SPREADSHEET_ID", "").strip()
    sname = os.getenv("SPREADSHEET_NAME", "").strip()

    if sid:
        sh = gc.open_by_key(sid)
    elif sname:
        sh = gc.open(sname)
    else:
        raise ValueError("Missing SPREADSHEET_ID (preferred) or 
SPREADSHEET_NAME")

    return sh.worksheet(sheet_name)


def _safe_float(x):
    try:
        return float(str(x).strip())
    except Exception:
        return None


def release_back_to_ready(ws, hm, row_num: int):
    update_row_by_headers(
        ws, hm, row_num,
        {"status": STATUS_READY, "processing_lock": "", "locked_by": ""}
    )


def get_ready_deal(worker_id: str, allow_verdicts=("GOOD",), 
min_ai_score=None, max_lock_age_minutes=30):
    """
    Claims + returns one deal ready for posting.
    Uses existing claim_first_available lock mechanism and sets status -> 
POSTING.
    Also prevents reposts by requiring published_timestamp empty.
    """
    ws = get_ws()

    # Require these columns for safe posting
    required = ["deal_id", "status", "published_timestamp", 
"processing_lock", "locked_by"]
    # These are optional filters if present
    optional = ["ai_verdict", "ai_score", "ai_caption"]

    headers = ws.row_values(1)
    for col in required:
        if col not in headers:
            raise ValueError(f"Sheet missing required column: {col}")

    hm = {h: headers.index(h) + 1 for h in headers}

    deal = claim_first_available(
        ws=ws,
        required_headers=list(set(required + [c for c in optional if c in 
headers])),
        status_col="status",
        wanted_status=STATUS_READY,
        set_status=STATUS_POSTING,
        worker_id=worker_id,
        max_lock_age=timedelta(minutes=max_lock_age_minutes),
    )
    if not deal:
        return None

    # Anti-repost
    if str(deal.get("published_timestamp", "")).strip():
        # Already posted, finalise state and clear lock
        mark_posted(deal["deal_id"], keep_existing_timestamp=True)
        return None

    # Verdict filter (if column exists and has a value)
    verdict = str(deal.get("ai_verdict", "")).strip()
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
    ws = get_ws()
    hm = ensure_headers(ws, ["deal_id", "status", "processing_lock", 
"locked_by", "published_timestamp"])

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
    ws = get_ws()
    headers = ws.row_values(1)
    hm = {h: headers.index(h) + 1 for h in headers}

    cell = ws.find(deal_id)
    if not cell:
        raise RuntimeError(f"Could not find deal_id in sheet: {deal_id}")

    row_num = cell.row
    update_row_by_headers(
        ws, hm, row_num,
        {"status": STATUS_ERROR, "processing_lock": "", "locked_by": ""}
    )

    target = "ai_notes" if "ai_notes" in hm else ("notes" if "notes" in hm 
else None)
    if target:
        existing = ws.cell(row_num, hm[target]).value or ""
        appended = (existing + "\n" if existing else "") + 
f"[TELEGRAM_ERROR {now_iso()}] {error_msg}"
        update_row_by_headers(ws, hm, row_num, {target: appended[:45000]})

