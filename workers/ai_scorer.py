#!/usr/bin/env python3
"""
Traveltxter V3_beta_b - AI Scorer (Production Grade with Debug)
- Processes multiple rows in one run
- Enhanced debugging to show what's in each row
- Standardizes raw_status lifecycle
"""

import os
import sys
import json
import datetime as dt
from typing import Dict, Any, List, Optional

import gspread
from google.oauth2.service_account import Credentials

# ============================================================
# Core Helpers
# ============================================================

def log(msg: str):
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

# ============================================================
# Scoring Logic
# ============================================================

def score_deal(rec: Dict[str, Any]) -> Dict[str, Any]:
    """Heuristic scoring engine"""
    def safe_val(key, default=0):
        try:
            val = str(rec.get(key, "")).replace("£", "").replace(",", 
"").strip()
            return float(val) if val else default
        except:
            return default

    price = safe_val("price_gbp", 9999)
    stops = safe_val("stops", 0)
    days = safe_val("trip_length_days", 0)
    
    score = 60
    notes = []

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
        "ai_notes": ", ".join(notes) if notes else "Standard"
    }

# ============================================================
# Main Batch Processor
# ============================================================

def main():
    log("🚀 AI SCORER STARTING (Batch Mode with Debug)")
    
    # 1. Setup Connection
    sheet_id = get_env("SHEET_ID")
    ws_name = get_env("WORKSHEET_NAME", required=False, 
default="RAW_DEALS")
    sa_json = json.loads(get_env("GCP_SA_JSON"))
    max_rows = int(get_env("MAX_ROWS_PER_RUN", required=False, 
default="10"))

    creds = Credentials.from_service_account_info(
        sa_json, 
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    client = gspread.authorize(creds)
    ws = client.open_by_key(sheet_id).worksheet(ws_name)
    
    log(f"Connected to worksheet: {ws.title}")

    # 2. Fetch Data
    all_values = ws.get_all_values()
    if len(all_values) < 2:
        log("No data found in sheet.")
        return

    headers = [h.strip() for h in all_values[0]]
    hmap = {h: i+1 for i, h in enumerate(headers) if h}
    
    log(f"Found {len(headers)} columns, {len(all_values)-1} data rows")
    log(f"DEBUG: First 10 headers: {headers[:10]}")
    
    # Identify status column
    status_col = None
    for col_name in ["raw_status", "RAW_STATUS", "status"]:
        if col_name in hmap:
            status_col = col_name
            log(f"Using status column: '{status_col}' at position 
{hmap[status_col]}")
            break
    
    if not status_col:
        log("❌ Critical Error: No status column found.")
        log(f"Available columns: {list(hmap.keys())[:20]}")
        return

    # 3. Find target rows
    rows_to_process = []
    status_idx = hmap[status_col] - 1  # Convert to 0-based
    
    log(f"Status column is at index {status_idx} (0-based), which is 
column {col_to_a1(hmap[status_col])}")
    log(f"Searching for rows with status = NEW or READY...")
    log(f"DEBUG: Will check rows 2 through {len(all_values)}")
    
    # Debug: Show all rows
    for idx, row in enumerate(all_values[1:], start=2):
        row_len = len(row)
        
        # Show basic info for every row
        deal_id = row[0] if len(row) > 0 else "NO_ID"
        log(f"DEBUG: Row {idx} - length={row_len}, deal_id={deal_id}")
        
        # Handle short rows
        if row_len <= status_idx:
            log(f"  -> Row {idx} is too short ({row_len} cells, need 
>{status_idx})")
            continue
        
        # Get status value
        status_cell = row[status_idx] if status_idx < row_len else ""
        status_val = status_cell.strip().upper()
        
        log(f"  -> Row {idx} raw_status (column 
{col_to_a1(hmap[status_col])}): '{status_cell}' -> '{status_val}'")
        
        # V3_beta_b Fix: Look for 'NEW' and 'READY' to clear backlog
        if status_val in ["NEW", "READY"]:
            rows_to_process.append((idx, dict(zip(headers, row))))
            log(f"  ✅ Row {idx}: MATCHED (status={status_val}) -> will 
process")
        else:
            log(f"  ⏭️  Row {idx}: SKIPPED (status='{status_val}' not in 
[NEW, READY])")
        
        if len(rows_to_process) >= max_rows:
            log(f"Reached max_rows limit ({max_rows}), stopping search")
            break

    if not rows_to_process:
        log("❌ No rows found with status = NEW or READY")
        log("💡 Checked all rows above - none matched")
        return
    
    log(f"✅ Found {len(rows_to_process)} rows to process.")

    # 4. Process Batch
    all_updates = []
    
    for row_num, rec in rows_to_process:
        deal_id = rec.get('deal_id', 'N/A')
        log(f"Processing Row {row_num} (deal_id: {deal_id})")
        
        # Calculate Score
        res = score_deal(rec)
        log(f"  Score: {res['ai_score']}, Verdict: {res['ai_verdict']}")
        
        # Prep Updates
        timestamp = dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        
        row_update = [
            (hmap.get("ai_score"), res["ai_score"]),
            (hmap.get("ai_verdict"), res["ai_verdict"]),
            (hmap.get("ai_notes"), res["ai_notes"]),
            (hmap.get("scored_timestamp"), timestamp),
            (hmap.get(status_col), "SCORED")  # Promote to SCORED
        ]

        for col_idx, val in row_update:
            if col_idx:  # Only if column exists
                all_updates.append({
                    "range": f"{col_to_a1(col_idx)}{row_num}",
                    "values": [[val]]
                })

    # 5. Write Back
    if all_updates:
        log(f"Writing {len(all_updates)} updates to sheet...")
        ws.batch_update(all_updates)
        log(f"✅ Successfully processed {len(rows_to_process)} deals.")
        log(f"🔄 Status: NEW/READY → SCORED")
    else:
        log("Nothing to update (missing columns?).")

if __name__ == "__main__":
    main()
