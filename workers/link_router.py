# workers/link_router.py
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


# =================================
# Logging
# =================================

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


# =================================
# Env helpers
# =================================

def env_str(k: str, default: str = "") -> str:
    v = os.environ.get(k, "")
    v = (v or "").strip()
    return v if v else default


def parse_sa_json(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return json.loads(raw.replace("\\n", "\n"))


def gs_client() -> gspread.Client:
    raw = env_str("GCP_SA_JSON_ONE_LINE") or env_str("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
    info = parse_sa_json(raw)
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds)


def iso_now() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def a1(row: int, col0: int) -> str:
    return gspread.utils.rowcol_to_a1(row, col0 + 1)


def safe_get(r: List[str], idx: int) -> str:
    if idx < 0:
        return ""
    if idx >= len(r):
        return ""
    return (r[idx] or "").strip()


def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID") or env_str("SHEET_ID")
    raw_tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID/SHEET_ID")

    gc = gs_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(raw_tab)

    values = ws.get_all_values()
    if len(values) < 2:
        log("No rows.")
        return 0

    headers = [h.strip() for h in values[0]]
    h = {k: i for i, k in enumerate(headers)}

    required = ["status", "deal_id", "affiliate_url", "booking_link_vip", "affiliate_source", "link_routed_at"]
    missing = [c for c in required if c not in h]
    if missing:
        raise RuntimeError(f"RAW_DEALS missing required columns: {missing}")

    eligible_statuses = {"READY_TO_POST", "READY_TO_PUBLISH", "POSTED_INSTAGRAM"}

    updated = 0
    for rownum, r in enumerate(values[1:], start=2):
        status = safe_get(r, h["status"])
        if status not in eligible_statuses:
            continue

        deal_id = safe_get(r, h["deal_id"])
        if not deal_id:
            continue

        existing = safe_get(r, h["booking_link_vip"])
        if existing:
            continue

        affiliate_url = safe_get(r, h["affiliate_url"])
        if not affiliate_url:
            continue

        ws.update([[affiliate_url]], a1(rownum, h["booking_link_vip"]))
        ws.update([["affiliate_url"]], a1(rownum, h["affiliate_source"]))
        ws.update([[iso_now()]], a1(rownum, h["link_routed_at"]))
        updated += 1

    log(f"Done. booking_link_vip populated for {updated} rows.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
