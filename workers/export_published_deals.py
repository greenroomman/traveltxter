#!/usr/bin/env python3
"""
Export Published Deals Worker
==============================

Purpose:
    Export deals that have been published (Instagram/Telegram) to a static JSON file
    for consumption by the TravelTxter landing page.

Contract:
    - Reads: RAW_DEALS (read-only)
    - Writes: public/deals.json (static file)
    - Never modifies RAW_DEALS
    - Only exports deals with published status

Status Filter:
    POSTED_INSTAGRAM | VIP_DONE | POSTED_ALL

Output Schema:
    {
      "deals": [...],
      "updated_at": "ISO timestamp",
      "next_run": "HH:MM UTC",
      "count": N
    }
"""

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import os
from datetime import datetime, timezone
import pytz

# ============================================================================
# CONFIGURATION
# ============================================================================

SCOPES = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]

SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

# Published statuses (from V5 lifecycle)
PUBLISHED_STATUSES = [
    "POSTED_INSTAGRAM",
    "VIP_DONE", 
    "POSTED_ALL"
]

# Next run times (UTC)
NEXT_RUNS = {
    "AM": "21:00 UTC",  # Next PM run
    "PM": "09:00 UTC (next day)"  # Next AM run
}

OUTPUT_FILE = "public/deals.json"

# ============================================================================
# GOOGLE SHEETS CONNECTION
# ============================================================================

def get_sheet():
    """Authenticate and return Google Sheet client"""
    try:
        if not SHEET_ID:
            raise ValueError("GOOGLE_SHEET_ID environment variable not set")
        
        if not SERVICE_ACCOUNT_JSON:
            raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON environment variable not set")
        
        # Parse service account credentials
        creds_dict = json.loads(SERVICE_ACCOUNT_JSON)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPES)
        
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SHEET_ID)
        
        print(f"✅ Connected to Google Sheet: {SHEET_ID}")
        return sheet
        
    except Exception as e:
        print(f"❌ Failed to connect to Google Sheets: {e}")
        raise

# ============================================================================
# DEAL EXPORT LOGIC
# ============================================================================

def calculate_next_run():
    """Calculate next pipeline run time based on current UTC hour"""
    now_utc = datetime.now(timezone.utc)
    current_hour = now_utc.hour
    
    # AM run = 9:00 UTC, PM run = 21:00 UTC
    if current_hour < 9:
        return NEXT_RUNS["AM"].replace("09:00", "09:00")
    elif current_hour < 21:
        return NEXT_RUNS["AM"]  # Next is PM at 21:00
    else:
        return NEXT_RUNS["PM"]  # Next is AM tomorrow

def transform_deal(row):
    """
    Transform RAW_DEALS row to landing page schema
    
    Input (from RAW_DEALS):
        - deal_id
        - origin_iata, destination_iata
        - origin_city, destination_city  
        - price_gbp
        - score
        - theme (from publish_window or derived)
        - status
    
    Output (for landing page):
        - id, origin, destination, name
        - price, currency
        - theme, vi_score, signal_strength
    """
    try:
        # Calculate signal strength from score (4-5 bars for publishable deals)
        score = float(row.get('score', 0))
        if score >= 9.0:
            signal_strength = 5
        elif score >= 8.5:
            signal_strength = 4
        else:
            signal_strength = 3
        
        return {
            "id": row.get('deal_id'),
            "origin": row.get('origin_iata', ''),
            "destination": row.get('destination_iata', ''),
            "name": f"{row.get('origin_city', row.get('origin_iata'))} → {row.get('destination_city', row.get('destination_iata'))}",
            "price": float(row.get('price_gbp', 0)),
            "currency": "GBP",
            "theme": row.get('publish_window', 'Adventure').replace('_', ' ').title(),
            "vi_score": round(score, 1),
            "signal_strength": signal_strength
        }
    except Exception as e:
        print(f"⚠️  Failed to transform deal {row.get('deal_id')}: {e}")
        return None

def export_published_deals():
    """Main export function - reads RAW_DEALS and writes public/deals.json"""
    
    print("\n" + "="*60)
    print("EXPORT PUBLISHED DEALS WORKER")
    print("="*60 + "\n")
    
    try:
        # Connect to Google Sheets
        sheet = get_sheet()
        raw_deals = sheet.worksheet("RAW_DEALS")
        
        print(f"📖 Reading RAW_DEALS...")
        records = raw_deals.get_all_records()
        print(f"   Total rows: {len(records)}")
        
        # Filter for published deals only
        published_records = [
            r for r in records 
            if r.get('status') in PUBLISHED_STATUSES
        ]
        print(f"   Published deals: {len(published_records)}")
        
        # Transform to landing page schema
        deals = []
        for record in published_records:
            transformed = transform_deal(record)
            if transformed:
                deals.append(transformed)
        
        # Sort by score descending (best deals first)
        deals.sort(key=lambda d: d['vi_score'], reverse=True)
        
        # Limit to top 10 (or all if fewer)
        deals = deals[:10]
        
        # Build output JSON
        output = {
            "deals": deals,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "next_run": calculate_next_run(),
            "count": len(deals)
        }
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        
        # Write to static file
        with open(OUTPUT_FILE, 'w') as f:
            json.dump(output, f, indent=2)
        
        print(f"\n✅ Export complete:")
        print(f"   Deals exported: {len(deals)}")
        print(f"   Output file: {OUTPUT_FILE}")
        print(f"   Next run: {output['next_run']}")
        
        # Log deal summary
        if deals:
            print(f"\n📊 Deal Summary:")
            for deal in deals:
                print(f"   • {deal['name']} - £{deal['price']} (Vi {deal['vi_score']})")
        else:
            print(f"\n🔇 No published deals found (restraint is a feature)")
        
        print("\n" + "="*60 + "\n")
        
    except Exception as e:
        print(f"\n❌ Export failed: {e}")
        
        # Write empty state on error (so landing page doesn't break)
        error_output = {
            "deals": [],
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "next_run": calculate_next_run(),
            "count": 0,
            "error": str(e)
        }
        
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        with open(OUTPUT_FILE, 'w') as f:
            json.dump(error_output, f, indent=2)
        
        raise

# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    export_published_deals()
