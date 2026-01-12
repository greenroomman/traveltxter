#!/usr/bin/env python3
"""
Graphic URL Guard (Production-safe)

Purpose:
- Prevent instagram_publisher from crashing on stale / broken graphic_url values.
- Repairs known URL formats:
    - missing scheme: greenroomman.pythonanywhere.com/... -> https://greenroomman.pythonanywhere.com/...
    - legacy static:  /static/renders/... -> /renders/...
- Verifies URL fetchability (HTTP HEAD/GET).
- If still not fetchable (e.g., 404), resets row for re-render:
    - status -> READY_TO_POST
    - graphic_url -> "" (cleared)

Design rules:
- Header-map only (no hard-coded column numbers).
- Idempotent.
- Does not invent new statuses.
"""

import os
import sys
import json
import datetime as dt
from typing import Dict, Any, List, Tuple, Optional

import gspread
import requests
from google.oauth2.service_account import Credentials


# =========================
# Logging
# =========================

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


def die(msg: str, code: int = 1) -> None:
    log(f"FATAL: {msg}")
    raise SystemExit(code)


# =========================
# Google auth
# =========================

def load_sa_creds() -> Credentials:
    raw = os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON")
    if not raw:
        die("Missing GCP_SA_JSON_ONE_LINE or GCP_SA_JSON env var")

    try:
        info = json.loads(raw)
    except json.JSONDecodeError:
        # Some users store the JSON with escaped newlines in env vars
        raw2 = raw.replace("\\n", "\n")
        info = json.loads(raw2)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    return Credentials.from_service_account_info(info, scopes=scopes)


def open_sheet():
    spreadsheet_id = os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID")
    if not spreadsheet_id:
        die("Missing SPREADSHEET_ID (or SHEET_ID) env var")

    raw_tab = os.getenv("RAW_DEALS_TAB") or "RAW_DEALS"
    creds = load_sa_creds()
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(raw_tab)
    return ws


# =========================
# URL repair + preflight
# =========================

def normalize_graphic_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return u

    # Add scheme if missing
    if u.startswith("greenroomman.pythonanywhere.com/"):
        u = "https://" + u

    # Normalize legacy path
    u = u.replace("/static/renders/", "/renders/")

    # Some old pipelines might store scheme-less with leading //
    if u.startswith("//"):
        u = "https:" + u

    return u


def is_fetchable(url: str, timeout: int = 10) -> Tuple[bool, int, str]:
    """
    Returns: (ok, status_code, snippet)
    """
    if not url:
        return False, 0, "empty-url"

    headers = {"User-Agent": "traveltxter-graphic-url-guard/1.0"}
    try:
        r = requests.head(url, allow_redirects=True, timeout=timeout, headers=headers)
        status = r.status_code
        if status == 405:
            # Some servers don't allow HEAD; fall back to GET
            r = requests.get(url, allow_redirects=True, timeout=timeout, headers=headers)
            status = r.status_code

        ok = 200 <= status < 300
        snippet = ""
        ct = (r.headers.get("Content-Type") or "").lower()
        if not ok:
            # capture small snippet for debugging
            try:
                snippet = (r.text or "")[:160].replace("\n", " ").replace("\r", " ")
            except Exception:
                snippet = ""
        else:
            snippet = ct or "ok"

        return ok, status, snippet

    except Exception as e:
        return False, 0, f"{type(e).__name__}: {str(e)[:160]}"


# =========================
# Main
# =========================

def main() -> int:
    # Behaviour controls
    target_status = os.getenv("GUARD_TARGET_STATUS") or "READY_TO_PUBLISH"
    reset_status = os.getenv("GUARD_RESET_STATUS") or "READY_TO_POST"
    max_rows = int(os.getenv("GUARD_MAX_ROWS", "50"))

    ws = open_sheet()
    values = ws.get_all_values()
    if not values or len(values) < 2:
        log("No data rows found.")
        return 0

    headers = values[0]
    rows = values[1:]

    # Required columns
    def col(name: str) -> Optional[int]:
        try:
            return headers.index(name)
        except ValueError:
            return None

    c_status = col("status")
    c_graphic = col("graphic_url")

    if c_status is None:
        die("RAW_DEALS missing required column: status")
    if c_graphic is None:
        die("RAW_DEALS missing required column: graphic_url")

    # Optional columns for diagnostics
    c_render_err = col("render_error")
    c_render_snip = col("render_response_snippet")
    c_rendered_ts = col("rendered_timestamp")

    # Collect candidate row indices (1-based in sheet; +1 for header)
    candidates: List[int] = []
    for i, r in enumerate(rows, start=2):
        if len(candidates) >= max_rows:
            break
        status = (r[c_status] if c_status < len(r) else "").strip()
        if status == target_status:
            candidates.append(i)

    if not candidates:
        log(f"No rows with status={target_status}. Nothing to do.")
        return 0

    log(f"Found {len(candidates)} rows with status={target_status} (processing up to {max_rows}).")

    updates: List[Tuple[str, List[List[Any]]]] = []
    fixed = 0
    reset = 0

    for row_idx in candidates:
        r = rows[row_idx - 2]
        old_url = (r[c_graphic] if c_graphic < len(r) else "").strip()
        new_url = normalize_graphic_url(old_url)

        # If URL changed, write it back immediately (but still verify)
        if new_url != old_url and new_url:
            a1 = gspread.utils.rowcol_to_a1(row_idx, c_graphic + 1)
            updates.append((a1, [[new_url]]))
            fixed += 1

        ok, status_code, snippet = is_fetchable(new_url if new_url else old_url)

        if ok:
            continue

        # Not fetchable: reset for re-render
        log(f"Row {row_idx}: graphic_url not fetchable (HTTP {status_code}) -> reset to {reset_status}")

        # Clear graphic_url
        a1_graphic = gspread.utils.rowcol_to_a1(row_idx, c_graphic + 1)
        updates.append((a1_graphic, [[""]]))

        # Set status back so render_client can regenerate
        a1_status = gspread.utils.rowcol_to_a1(row_idx, c_status + 1)
        updates.append((a1_status, [[reset_status]]))

        # Optional diagnostics
        if c_render_err is not None:
            a1 = gspread.utils.rowcol_to_a1(row_idx, c_render_err + 1)
            updates.append((a1, [[f"graphic_url not fetchable (HTTP {status_code})"]]))
        if c_render_snip is not None:
            a1 = gspread.utils.rowcol_to_a1(row_idx, c_render_snip + 1)
            updates.append((a1, [[snippet]]))
        if c_rendered_ts is not None:
            a1 = gspread.utils.rowcol_to_a1(row_idx, c_rendered_ts + 1)
            ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
            updates.append((a1, [[ts]]))

        reset += 1

    # Apply updates (A1 updates are safest with gspread v6)
    for a1, payload in updates:
        ws.update(payload, a1)

    log(f"Done. URL repaired on {fixed} rows; reset {reset} rows for re-render.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
