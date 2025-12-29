#!/usr/bin/env python3
"""
ONE-TIME FIX: Sync Status Values and Clear Backlog
Run this ONCE to fix all 13 stuck deals
"""

import os
import json
import gspread
from google.oauth2.service_account import Credentials

# Configuration
SHEET_ID = os.getenv("SHEET_ID", "YOUR_SHEET_ID_HERE")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def main():
    print("🔧 ONE-TIME FIX: Syncing status values...")
    
    # Connect to sheet
    sa_json = os.getenv("GCP_SA_JSON")
    if not sa_json:
        print("❌ Set GCP_SA_JSON environment variable")
        return
    
    sa_info = json.loads(sa_json)
    creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SHEET_ID)
    ws = sheet.worksheet("RAW_DEALS")
    
    print(f"✅ Connected to: {sheet.title}")
    
    # Get all data
    all_data = ws.get_all_values()
    headers = all_data[0]
    rows = all_data[1:]
    
    # Find column indices
    try:
        raw_status_idx = headers.index("raw_status")
        deal_id_idx = headers.index("deal_id")
    except ValueError as e:
        print(f"❌ Missing column: {e}")
        return
    
    print(f"📊 Found {len(rows)} rows to check")
    
    # Track changes
    fixes = {
        "READY → NEW": 0,
        "READY_TO_POST → NEW": 0,
        "Empty → NEW": 0,
        "Already correct": 0
    }
    
    # Process each row
    for row_num, row in enumerate(rows, start=2):  # Row 2+ (1 is headers)
        if len(row) <= max(raw_status_idx, deal_id_idx):
            continue
        
        deal_id = row[deal_id_idx].strip() if deal_id_idx < len(row) else 
""
        if not deal_id:
            continue
        
        current_status = row[raw_status_idx].strip() if raw_status_idx < 
len(row) else ""
        
        # Determine what to do
        if current_status in ("READY", "Ready"):
            # Fix: READY → NEW (for reprocessing)
            ws.update_cell(row_num, raw_status_idx + 1, "NEW")
            fixes["READY → NEW"] += 1
            print(f"  Row {row_num} ({deal_id}): READY → NEW")
        
        elif current_status == "READY_TO_POST":
            # Option A: Keep as-is (ready to post)
            # Option B: Reset to NEW (reprocess through scorer)
            # Using Option B for safety
            ws.update_cell(row_num, raw_status_idx + 1, "NEW")
            fixes["READY_TO_POST → NEW"] += 1
            print(f"  Row {row_num} ({deal_id}): READY_TO_POST → NEW 
(reprocessing)")
        
        elif current_status == "":
            # Empty status → NEW
            ws.update_cell(row_num, raw_status_idx + 1, "NEW")
            fixes["Empty → NEW"] += 1
            print(f"  Row {row_num} ({deal_id}): Empty → NEW")
        
        elif current_status in ("NEW", "SCORED", "POSTED_TELEGRAM", 
"POSTED_INSTAGRAM"):
            # Already in correct format
            fixes["Already correct"] += 1
        
        else:
            print(f"  ⚠️  Row {row_num} ({deal_id}): Unknown status 
'{current_status}' - leaving as-is")
    
    # Summary
    print("\n" + "="*60)
    print("📊 FIX SUMMARY")
    print("="*60)
    for action, count in fixes.items():
        if count > 0:
            print(f"  {action}: {count}")
    print("="*60)
    
    total_fixed = sum(v for k, v in fixes.items() if k != "Already 
correct")
    
    if total_fixed > 0:
        print(f"\n✅ Fixed {total_fixed} rows!")
        print("\n🎯 NEXT STEPS:")
        print("1. Run AI Scorer workflow (will process all NEW rows)")
        print("2. Run Render Worker workflow (will process SCORED rows)")
        print("3. Run Telegram Publisher (will post READY_TO_POST rows)")
    else:
        print("\n✅ All rows already in correct format!")
    
    print("\n💡 TIP: After this runs successfully:")
    print("   - All deals will have raw_status = NEW")
    print("   - AI Scorer will process them (NEW → SCORED)")
    print("   - Render Worker will add graphics (SCORED → 
READY_TO_POST)")
    print("   - Telegram will post them (READY_TO_POST → 
POSTED_TELEGRAM)")


if __name__ == "__main__":
    main()

