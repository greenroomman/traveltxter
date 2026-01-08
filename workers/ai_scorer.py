#!/usr/bin/env python3
"""
TravelTxter — AI Scorer (Spreadsheet-Driven Worthiness)

LOCKED RULE:
- Spreadsheet is the brain.
- This worker does NOT compute quality.
- It only PROMOTES based on RAW_DEALS.worthiness_verdict and worthiness_score.

Consumes:
- RAW_DEALS rows with status == NEW

Promotes:
- If worthiness_verdict == "PUBLISH" and passes variety guard:
    status -> READY_TO_POST
    scored_timestamp -> now

Leaves alone:
- HOLD / REJECT rows (status remains NEW by default; spreadsheet remains source of truth)

Environment:
- SPREADSHEET_ID (required; accepts SHEET_ID as fallback)
- RAW_DEALS_TAB (default RAW_DEALS)
- GCP_SA_JSON_ONE_LINE or GCP_SA_JSON (required)
- WINNERS_PER_RUN (default 1)
- VARIETY_LOOKBACK_HOURS (default 120)
"""

from __future__ import annotations

import os
import json
import time
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError


# ============================================================
# Logging
# ============================================================

def now_utc() -> dt.datetime:
    return dt.datetime.utcnow()

def now_utc_iso() -> str:
    return now_utc().replace(microsecond=0).isoformat() + "Z"

def log(msg: str) -> None:
    print(f"{now_utc_iso()} | {msg}", flush=True)


# ============================================================
# Env helpers
# ============================================================

def env_str(k: str, default: str = "") -> str:
    return (os.environ.get(k, default) or "").strip()

def env_any(keys: List[str], default: str = "") -> str:
    for k in keys:
        v = env_str(k, "")
        if v:
            return v
    return default

def env_int(k: str, default: int) -> int:
    try:
        return int(env_str(k, str(default)))
    except Exception:
        return default

def parse_iso_z(s: str) -> Optional[dt.datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1]
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None

def safe_float(s: str) -> float:
    try:
        x = (s or "").strip().replace("£", "").replace(",", "")
        return float(x) if x else 0.0
    except Exception:
        return 0.0


# ============================================================
# Sheets auth
# ============================================================

def _extract_sa(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()
    try:
        return json.loads(raw)
    except Exception:
        return json.loads(raw.replace("\\n", "\n"))

def get_client() -> gspread.Client:
    sa_raw = env_any(["GCP_SA_JSON_ONE_LINE", "GCP_SA_JSON"])
    if not sa_raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
    info = _extract_sa(sa_raw)
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
    raise RuntimeError("Sheets quota still exceeded after retries (429).")


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

def safe_get(row: List[str], idx: int) -> str:
    return row[idx].strip() if 0 <= idx < len(row) else ""

def ensure_columns(ws: gspread.Worksheet, headers: List[str], required: List[str]) -> List[str]:
    missing = [c for c in required if c not in headers]
    if not missing:
        return headers
    ws.update([headers + missing], "A1")
    log(f"🛠️ Added missing columns: {missing}")
    return headers + missing


# ============================================================
# Variety guard
# ============================================================

POSTED_STATUSES = {
    "POSTED_INSTAGRAM",
    "POSTED_TELEGRAM_VIP",
    "POSTED_TELEGRAM_FREE",
    "POSTED_ALL",
}

def recently_posted_destinations(
    headers: List[str],
    rows: List[List[str]],
    lookback_hours: int,
) -> set:
    """
    Collect destinations posted within lookback window (by best available timestamp).
    """
    h = {name: i for i, name in enumerate(headers)}
    now = now_utc()
    cutoff = now - dt.timedelta(hours=lookback_hours)
    recent = set()

    ts_cols = []
    for c in ["posted_all_at", "posted_instagram_at", "posted_telegram_free_at", "posted_telegram_vip_at"]:
        if c in h:
            ts_cols.append(c)

    dest_key = "destination_city" if "destination_city" in h else ("destination_iata" if "destination_iata" in h else None)
    if not dest_key:
        return recent

    for r in rows:
        status = safe_get(r, h.get("status", -1)).upper()
        if status not in POSTED_STATUSES:
            continue

        dt_found: Optional[dt.datetime] = None
        for c in ts_cols:
            t = parse_iso_z(safe_get(r, h[c]))
            if t:
                if dt_found is None or t > dt_found:
                    dt_found = t

        if dt_found and dt_found >= cutoff:
            recent.add(safe_get(r, h[dest_key]).strip().upper())

    return recent


# ============================================================
# Main
# ============================================================

def main() -> int:
    spreadsheet_id = env_any(["SPREADSHEET_ID", "SHEET_ID"])
    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID (or SHEET_ID)")

    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    winners = env_int("WINNERS_PER_RUN", 1)
    lookback_hours = env_int("VARIETY_LOOKBACK_HOURS", 120)

    gc = get_client()
    sh = open_sheet_with_backoff(gc, spreadsheet_id)
    ws = sh.worksheet(tab)

    values = ws.get_all_values()
    if not values or len(values) < 2:
        log("No rows to score.")
        return 0

    headers = [h.strip() for h in values[0]]

    required = [
        "status",
        "deal_id",
        "worthiness_verdict",
        "worthiness_score",
        "destination_city",
        "destination_iata",
        "scored_timestamp",
    ]
    headers = ensure_columns(ws, headers, required)

    values = ws.get_all_values()
    headers = [h.strip() for h in values[0]]
    rows = values[1:]
    h = {name: i for i, name in enumerate(headers)}

    # Compute recent destinations to avoid repeats
    recent_dests = recently_posted_destinations(headers, rows, lookback_hours)
    if recent_dests:
        log(f"🧠 Variety guard: {len(recent_dests)} recent destinations in last {lookback_hours}h")

    # Collect candidates
    candidates: List[Tuple[float, int, List[str]]] = []
    for idx0, r in enumerate(rows):
        rownum = idx0 + 2

        status = safe_get(r, h["status"]).upper()
        if status != "NEW":
            continue

        verdict = safe_get(r, h["worthiness_verdict"]).upper()
        if verdict != "PUBLISH":
            continue

        score = safe_float(safe_get(r, h["worthiness_score"]))

        dest_city = safe_get(r, h.get("destination_city", -1)).strip()
        dest_iata = safe_get(r, h.get("destination_iata", -1)).strip()
        dest_norm = (dest_city or dest_iata or "").strip().upper()

        # Variety guard: skip if this destination was posted recently
        if dest_norm and dest_norm in recent_dests:
            continue

        candidates.append((score, rownum, r))

    if not candidates:
        log("Done. Promoted 0 (no NEW rows with worthiness_verdict=PUBLISH passing variety guard).")
        return 0

    # Highest score first
    candidates.sort(key=lambda t: t[0], reverse=True)

    promote = candidates[:max(1, winners)]
    updates = []
    for score, rownum, r in promote:
        deal_id = safe_get(r, h["deal_id"])
        log(f"✅ Promote row {rownum} deal_id={deal_id} worthiness_score={score:.2f}")

        updates.append({"range": a1(rownum, h["status"]), "values": [["READY_TO_POST"]]})
        updates.append({"range": a1(rownum, h["scored_timestamp"]), "values": [[now_utc_iso()]]})

    ws.batch_update(updates, value_input_option="USER_ENTERED")
    log(f"Done. Promoted {len(promote)} row(s) to READY_TO_POST.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
