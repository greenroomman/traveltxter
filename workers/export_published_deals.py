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

Status Filter (robust):
    - Any status starting with "POSTED_" (covers IG + Telegram variants)
    - Plus legacy "VIP_DONE"

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

# ============================================================================
# CONFIGURATION
# ============================================================================

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

# Legacy compatibility (kept explicit)
LEGACY_PUBLISHED_STATUSES = {"VIP_DONE"}

# Next run times (UTC)
NEXT_RUNS = {
    "AM": "21:00 UTC",            # Next PM run
    "PM": "09:00 UTC (next day)", # Next AM run
}

OUTPUT_FILE = "public/deals.json"

# ============================================================================
# GOOGLE SHEETS CONNECTION
# ============================================================================

def get_sheet():
    """Authenticate and return Google Sheet client"""
    if not SHEET_ID:
        raise ValueError("GOOGLE_SHEET_ID environment variable not set")
    if not SERVICE_ACCOUNT_JSON:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON environment variable not set")

    creds_dict = json.loads(SERVICE_ACCOUNT_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPES)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SHEET_ID)

    print(f"✅ Connected to Google Sheet: {SHEET_ID}")
    return sheet

# ============================================================================
# DEAL EXPORT LOGIC
# ============================================================================

def calculate_next_run():
    """Calculate next pipeline run time based on current UTC hour"""
    now_utc = datetime.now(timezone.utc)
    h = now_utc.hour

    # AM run ~09:00 UTC, PM run ~21:00 UTC
    if h < 9:
        return "09:00 UTC"
    if h < 21:
        return NEXT_RUNS["AM"]  # Next is PM at 21:00
    return NEXT_RUNS["PM"]      # Next is AM tomorrow


def is_published_status(status: str | None) -> bool:
    """
    Published = anything already posted to any channel.
    Robust rule: POSTED_* plus legacy VIP_DONE.
    """
    if not status:
        return False
    s = str(status).strip()
    return s.startswith("POSTED_") or s in LEGACY_PUBLISHED_STATUSES


def transform_deal(row: dict):
    """
    Transform RAW_DEALS row to landing page schema.
    """
    try:
        score = float(row.get("score", 0) or 0)
        if score >= 9.0:
            signal_strength = 5
        elif score >= 8.5:
            signal_strength = 4
        else:
            signal_strength = 3

        return {
            "id": row.get("deal_id"),
            "origin": row.get("origin_iata", "") or "",
            "destination": row.get("destination_iata", "") or "",
            "name": f"{row.get('origin_city', row.get('origin_iata'))} → {row.get('destination_city', row.get('destination_iata'))}",
            "price": float(row.get("price_gbp", 0) or 0),
            "currency": "GBP",
            # Keeping your existing mapping (publish_window used as theme label)
            "theme": str(row.get("publish_window", "Adventure")).replace("_", " ").title(),
            "vi_score": round(score, 1),
            "signal_strength": signal_strength,
        }
    except Exception as e:
        print(f"⚠️  Failed to transform deal {row.get('deal_id')}: {e}")
        return None


def export_published_deals():
    """Main export function - reads RAW_DEALS and writes public/deals.json"""

    print("\n" + "=" * 60)
    print("EXPORT PUBLISHED DEALS WORKER")
    print("=" * 60 + "\n")

    try:
        sheet = get_sheet()
        raw_deals = sheet.worksheet("RAW_DEALS")

        print("📖 Reading RAW_DEALS.")
        records = raw_deals.get_all_records()
        print(f"   Total rows: {len(records)}")

        # Filter for published deals only
        published_records = [r for r in records if is_published_status(r.get("status"))]
        print(f"   Published deals (status POSTED_* or VIP_DONE): {len(published_records)}")

        deals = []
        for record in published_records:
            transformed = transform_deal(record)
            if transformed:
                deals.append(transformed)

        # Sort best first
        deals.sort(key=lambda d: d.get("vi_score", 0), reverse=True)

        # Top 10 only
        deals = deals[:10]

        output = {
            "deals": deals,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "next_run": calculate_next_run(),
            "count": len(deals),
        }

        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        with open(OUTPUT_FILE, "w") as f:
            json.dump(output, f, indent=2)

        print("\n✅ Export complete:")
        print(f"   Deals exported: {len(deals)}")
        print(f"   Output file: {OUTPUT_FILE}")
        print(f"   Next run: {output['next_run']}")

        if deals:
            print("\n📊 Deal Summary:")
            for d in deals:
                print(f"   • {d['name']} - £{d['price']} (Vi {d['vi_score']})")
        else:
            print("\n🔇 No published deals found (restraint is a feature)")

        print("\n" + "=" * 60 + "\n")

    except Exception as e:
        print(f"\n❌ Export failed: {e}")

        error_output = {
            "deals": [],
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "next_run": calculate_next_run(),
            "count": 0,
            "error": str(e),
        }

        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        with open(OUTPUT_FILE, "w") as f:
            json.dump(error_output, f, indent=2)

        raise


if __name__ == "__main__":
    export_published_deals()
