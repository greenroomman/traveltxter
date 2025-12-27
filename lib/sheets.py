import os
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def now_iso():
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


def ensure_headers(ws, required_headers):
    actual = ws.row_values(1)
    missing = sorted(set(required_headers) - set(actual))
    if missing:
        raise ValueError(f"Sheet missing columns: {missing}")
    return {h: actual.index(h) + 1 for h in actual}


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
