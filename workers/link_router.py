#!/usr/bin/env python3
"""
TravelTxter V4.5x — link_router.py (LOCKED)

ROLE:
- Populates booking_link_vip for rows that need it
- Prefers booking_link_vip if already set
- Falls back to affiliate_url if present
- NEVER changes status (status gating is handled elsewhere)
- Never crashes if Duffel Links config is missing

NOTE:
This file is deliberately "safe-first" so the pipeline can publish reliably.
If/when you want Duffel Links sessions, we can add that logic without touching
the rest of the pipeline.
"""

from __future__ import annotations

import os
import json
import time
import datetime as dt
from typing import Dict, Any, List, Optional

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError


# ============================================================
# Logging
# ============================================================

def ts() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def log(msg: str) -> None:
    print(f"{ts()} | {msg}", flush=True)


# ============================================================
# Env helpers
# ============================================================

def env_str(k: str, default: str = "") -> str:
    return os.environ.get(k, default).strip()

def env_int(k: str, default: int) -> int:
    try:
        return int(env_str(k, str(default)))
    except Exception:
        return default


# ============================================================
# Robust SA JSON parsing
# ============================================================

def _extract_json_object(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()

    try:
        return json.loads(raw)
    except Exception:
        pass

    try:
        return json.loads(raw.replace("\\n", "\n"))
    except Exception:
        pass

    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise RuntimeError("Invalid GCP_SA_JSON_ONE_LINE: no JSON object found")

    candidate = raw[start:end + 1]

    try:
        return json.loads(candidate)
    except Exception:
        pass

    try:
        return json.loads(candidate.replace("\\n", "\n"))
    except Exception as e:
        raise RuntimeError("Invalid GCP_SA_JSON_ONE_LINE: JSON parse failed") from e


def get_client() -> gspread.Client:
    sa = env_str("GCP_SA_JSON_ONE_LINE") or env_str("GCP_SA_JSON")
    if not sa:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")

    info = _extract_json_object(sa)

    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds)


def open_sheet_with_backoff(gc: gspread.Client, spreadsheet_id: str, attempts: int = 8) -> gspread.Spreadsheet:
    delay = 4.0
    for i in range(1, attempts + 1):
        try:
            return gc.open_by_key(spreadsheet_id)
        except APIError as e:
            msg = str(e)
            if "429" in msg or "Quota exceeded" in msg:
                log(f"⏳ Sheets quota (429). Retry {i}/{attempts} in {int(delay)}s...")
                time.sleep(delay)
                delay = min(delay * 1.6, 45.0)
                continue
            raise
    raise RuntimeError("Sheets quota still exceeded after retries (429). Try again shortly.")


# ============================================================
# A1 helpers
# ============================================================

def col_letter(n1: int) -> str:
    s = ""
    n = n1
    while n:
        n, rr = divmod(n - 1, 26)
        s = chr(65 + rr) + s
    return s

def a1(rownum: int, col0: int) -> str:
    return f"{col_letter(col0 + 1)}{rownum}"


# ============================================================
# Sheet helpers
# ============================================================

def ensure_columns(ws: gspread.Worksheet, headers: List[str], required: List[str]) -> List[str]:
    missing = [c for c in required if c not in headers]
    if not missing:
        return headers
    ws.update([headers + missing], "A1")
    log(f"🛠️  Added missing columns: {missing}")
    return headers + missing

def safe_get(row: List[str], idx: int) -> str:
    return row[idx].strip() if 0 <= idx < len(row) else ""


# ============================================================
# Main
# ============================================================

def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")

    max_rows = env_int("LINK_ROUTER_MAX_ROWS", 5)

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")

    gc = get_client()
    sh = open_sheet_with_backoff(gc, spreadsheet_id)
    ws = sh.worksheet(tab)

    values = ws.get_all_values()
    if not values or len(values) < 2:
        log("No rows.")
        return 0

    headers = [h.strip() for h in values[0]]

    required_cols = [
        "status",
        "affiliate_url",
        "booking_link_vip",
        "affiliate_source",
        "link_routed_at",
    ]
    headers = ensure_columns(ws, headers, required_cols)

    # Re-read once after header mutation
    values = ws.get_all_values()
    headers = [h.strip() for h in values[0]]
    rows = values[1:]
    h = {name: i for i, name in enumerate(headers)}

    processed = 0

    # We route links for rows that are in-flight through publishing
    eligible_statuses = {"READY_TO_POST", "READY_TO_PUBLISH", "POSTED_INSTAGRAM"}

    for rownum, r in enumerate(rows, start=2):
        if processed >= max_rows:
            break

        status = safe_get(r, h["status"]).upper()
        if status not in eligible_statuses:
            continue

        booking_link_vip = safe_get(r, h["booking_link_vip"])
        affiliate_url = safe_get(r, h["affiliate_url"])

        # If we already have a VIP link, keep it.
        if booking_link_vip:
            continue

        # Safe fallback: use affiliate_url if present
        if not affiliate_url:
            log(f"⏭️  Skip row {rownum}: no affiliate_url to use as fallback")
            continue

        batch = [
            {"range": a1(rownum, h["booking_link_vip"]), "values": [[affiliate_url]]},
            {"range": a1(rownum, h["affiliate_source"]), "values": [["affiliate_fallback"]]},
            {"range": a1(rownum, h["link_routed_at"]), "values": [[ts()]]},
        ]
        ws.batch_update(batch, value_input_option="USER_ENTERED")

        processed += 1
        log(f"🔗 Routed row {rownum}: booking_link_vip set from affiliate_url")

        time.sleep(1)

    log(f"Done. Routed {processed} link(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
