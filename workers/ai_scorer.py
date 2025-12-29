#!/usr/bin/env python3
"""
Traveltxter V3_beta_b_final — AI Scorer (CLEAN + PIPELINE-CORRECT)

Pipeline alignment (LOCKED):
- Sheet column used: raw_status
- Scorer: NEW -> SCORED
- Render worker: SCORED -> READY_TO_POST
- Publishers: READY_TO_POST -> POSTED_*

Reads:    RAW_DEALS where raw_status == NEW
Writes:   ai_score, ai_verdict, ai_notes, scored_timestamp
Promotes: raw_status -> SCORED

Key fix:
- gspread get_all_values() returns ragged rows; we pad each row to header length
"""

import os
import sys
import json
import datetime as dt
from typing import Dict, Any, List, Tuple

import gspread
from google.oauth2.service_account import Credentials


# =========================
# Logging / Helpers
# =========================

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
    print(f"{ts} | {msg}", flush=True)


def get_env(name: str, required: bool = True, default: str = "") -> str:
    v = os.getenv(name)
    if not v and required:
        print(f"ERROR: Missing {name}")
        sys.exit(1)
    return str(v or default).strip()


def col_to_a1(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def normalize_status(s: Any) -> str:
    """Normalize status values robustly (handles invisible chars)."""
    if s is None:
        return ""
    s = str(s)
    s = s.replace("\u00A0", " ")   # NBSP
    s = s.replace("\u200B", "")   # zero-width space
    s = s.replace("\uFEFF", "")   # BOM / zero-width no-break
    return s.strip().upper()


# =========================
# Scoring Logic
# =========================

def score_deal(rec: Dict[str, Any]) -> Dict[str, Any]:
    """Simple heuristic scoring engine."""
    def safe_val(key: str, default: float = 0.0) -> float:
        try:
            val = str(rec.get(key, "")).replace("£", "").replace(",", "").strip()
            return float(val) if val else default
        except Exception:
            return default

    price = safe_val("price_gbp", 9999)
    stops = safe_val("stops", 0)
    days = safe_val("trip_length_days", 0)

    score = 60
    notes: List[str] = []

    if price <= 60:
        score += 25
        notes.append("Bargain")
    elif price <= 150:
        score += 10
        notes.append("Fair Price")

    if stops == 0:
        score += 15
        notes.append("Direct")
    elif stops > 1:
        score -= 5

    if 3 <= days <= 10:
        score += 5
        notes.append("Good Length")

    score = max(0, min(100, int(score)))

    if score >= 80:
        verdict = "GOOD"
    elif score >= 60:
        verdict = "AVERAGE"
    else:
        verdict = "POOR"

    return {
        "ai_score": score,
        "ai_verdict": verdict,
        "ai_notes": ", ".join(notes) if notes else "Standard",
    }


# =========================
# Main
# =========================

def main() -> None:
    log("🚀 AI SCORER STARTING (V3_beta_b_final CLEAN)")

    sheet_id = get_env("SHEET_ID")
    ws_name = get_env("WORKSHEET_NAME", required=False, default="RAW_DEALS")
    max_rows = int(get_env("MAX_ROWS_PER_RUN", required=False, default="10"))

    # Auth (GitHub Actions uses GCP_SA_JSON secret)
    sa_json = json.loads(get_env("GCP_SA_JSON"))
    creds = Credentials.from_service_account_info(
        sa_json,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    client = gspread.authorize(creds)
    ws = client.open_by_key(sheet_id).worksheet(ws_name)

    log(f"Connected to worksheet: {ws.title}")

    # Read all values (ragged rows possible)
    all_values = ws.get_all_values()
    if len(all_values) < 2:
        log("No data found in sheet.")
        return

    headers = [h.strip() for h in all_values[0]]
    header_len = len(headers)
    hmap = {h: i + 1 for i, h in enumerate(headers) if h}

    log(f"Found {header_len} columns, {len(all_values) - 1} data rows")

    # LOCK: use raw_status only (matches your sheet + render worker)
    status_col_name = "raw_status"
    if status_col_name not in hmap:
        log("❌ Critical Error: 'raw_status' column not found.")
        log(f"Headers (first 30): {headers[:30]}")
        return

    status_col_idx0 = hmap[status_col_name] - 1
    log(f"Using status column: '{status_col_name}' at position {hmap[status_col_name]} ({col_to_a1(hmap[status_col_name])})")

    rows_to_process: List[Tuple[int, Dict[str, Any]]] = []

    # Find NEW rows (pad each row to header length)
    for row_num, row in enumerate(all_values[1:], start=2):
        padded = (row + [""] * header_len)[:header_len]
        status_raw = padded[status_col_idx0]
        status_val = normalize_status(status_raw)

        deal_id = padded[hmap["deal_id"] - 1] if "deal_id" in hmap else ""
        log(f"Row {row_num} deal_id={deal_id} raw_status='{status_raw}' -> '{status_val}'")

        if status_val == "NEW":
            rec = dict(zip(headers, padded))
            rows_to_process.append((row_num, rec))
            log(f"  ✅ MATCH: row {row_num} (NEW) queued")

        if len(rows_to_process) >= max_rows:
            log(f"Reached MAX_ROWS_PER_RUN={max_rows}")
            break

    if not rows_to_process:
        log("No NEW rows found. Exiting.")
        return

    # Build updates (A1 ranges)
    updates: List[Dict[str, Any]] = []
    now = dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    def add(row_num: int, col: str, val: Any) -> None:
        if col in hmap:
            col_idx = hmap[col]
            updates.append(
                {"range": f"{col_to_a1(col_idx)}{row_num}", "values": [[val]]}
            )

    for row_num, rec in rows_to_process:
        res = score_deal(rec)
        log(f"Scoring row {row_num}: score={res['ai_score']} verdict={res['ai_verdict']}")

        add(row_num, "ai_score", res["ai_score"])
        add(row_num, "ai_verdict", res["ai_verdict"])
        add(row_num, "ai_notes", res["ai_notes"])
        add(row_num, "scored_timestamp", now)

        # Pipeline-correct promotion: NEW -> SCORED (render worker consumes SCORED)
        add(row_num, status_col_name, "SCORED")

    log(f"Writing {len(updates)} cell updates...")
    ws.batch_update(updates)
    log(f"✅ Done: processed {len(rows_to_process)} rows. NEW → SCORED")


if __name__ == "__main__":
    main()
