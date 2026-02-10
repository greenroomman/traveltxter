#!/usr/bin/env python3
"""
Export Published Deals Worker
=============================

Purpose:
  Export deals for the landing page into a static JSON file consumed by the React app.

Canonical rule (LOCKED):
  Landing page visibility is driven by publish_window (AM/PM), NOT channel status.
  - Eligible iff publish_window ∈ {"AM","PM"}
  - Status is ignored (decouples homepage from publisher internals)

Reads:
  - Google Sheet: RAW_DEALS (read-only)

Writes:
  - public/deals.json (static file for frontend)
"""

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import gspread
from oauth2client.service_account import ServiceAccountCredentials

# =============================================================================
# CONFIG
# =============================================================================

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

# The file the frontend ultimately serves as /deals.json
OUTPUT_FILE = "public/deals.json"

# =============================================================================
# GOOGLE SHEETS
# =============================================================================


def get_sheet() -> gspread.Spreadsheet:
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


# =============================================================================
# EXPORT LOGIC (CANONICAL)
# =============================================================================


def calculate_next_run() -> str:
    """
    Conservative display helper for UI.
    (You can replace with your real scheduler times if you want; not critical.)
    """
    now_utc = datetime.now(timezone.utc)
    h = now_utc.hour
    # Approx slots: AM ~09:00 UTC, PM ~21:00 UTC
    if h < 9:
        return "09:00 UTC"
    if h < 21:
        return "21:00 UTC"
    return "09:00 UTC (next day)"


def is_exportable_by_window(row: Dict[str, Any]) -> bool:
    """
    LOCKED: Landing page eligibility is based on publish_window.
    """
    window = str(row.get("publish_window", "")).strip().upper()
    return window in {"AM", "PM"}


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        if not s:
            return default
        return float(s)
    except Exception:
        return default


def _signal_strength(score: float) -> int:
    # Keep your original vibe: 3–5 bars
    if score >= 9.0:
        return 5
    if score >= 8.5:
        return 4
    return 3


def transform_deal(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Transform a RAW_DEALS row into the landing page schema.
    This stays compatible with your current UI pattern.

    Expected-ish columns (best effort; blanks tolerated):
      deal_id, origin_city, destination_city, origin_iata, destination_iata,
      price_gbp, score, publish_window
    """
    try:
        score = _safe_float(row.get("score"), 0.0)
        price = _safe_float(row.get("price_gbp"), 0.0)

        origin_city = (row.get("origin_city") or row.get("origin_iata") or "").strip()
        dest_city = (row.get("destination_city") or row.get("destination_iata") or "").strip()
        origin_iata = (row.get("origin_iata") or "").strip()
        dest_iata = (row.get("destination_iata") or "").strip()

        window = str(row.get("publish_window", "")).strip().upper() or "AM"

        # Minimum sanity: must have a route label
        if not origin_city and not origin_iata:
            return None
        if not dest_city and not dest_iata:
            return None

        name = f"{origin_city or origin_iata} → {dest_city or dest_iata}"

        return {
            "id": row.get("deal_id"),
            "origin": origin_iata,
            "destination": dest_iata,
            "name": name,
            "price": price,
            "currency": "GBP",
            "theme": window,  # <-- canonical: the trigger is AM/PM
            "vi_score": round(score, 1),
            "signal_strength": _signal_strength(score),
        }
    except Exception as e:
        print(f"⚠️  Failed to transform row deal_id={row.get('deal_id')}: {e}")
        return None


def export_published_deals() -> None:
    print("\n" + "=" * 72)
    print("EXPORT PUBLISHED DEALS — LANDING PAGE FEED (publish_window driven)")
    print("=" * 72 + "\n")

    sheet = get_sheet()
    ws = sheet.worksheet("RAW_DEALS")

    print("📖 Reading RAW_DEALS …")
    records: List[Dict[str, Any]] = ws.get_all_records()
    print(f"   Total rows: {len(records)}")

    eligible = [r for r in records if is_exportable_by_window(r)]
    print(f"   Eligible (publish_window in AM/PM): {len(eligible)}")

    deals: List[Dict[str, Any]] = []
    for r in eligible:
        d = transform_deal(r)
        if d:
            deals.append(d)

    # Sort best-first for UI
    deals.sort(key=lambda d: d.get("vi_score", 0), reverse=True)

    # Keep the homepage lightweight
    deals = deals[:10]

    output = {
        "deals": deals,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "next_run": calculate_next_run(),
        "count": len(deals),
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    print("\n✅ Export complete")
    print(f"   Output: {OUTPUT_FILE}")
    print(f"   Deals exported: {len(deals)}")
    print(f"   Next run: {output['next_run']}")

    if deals:
        print("\n📊 Top deals:")
        for d in deals[:5]:
            print(f"   • {d['name']} — £{d['price']:.0f} (Vi {d['vi_score']}) [{d['theme']}]")
    else:
        print("\n🔇 No eligible deals (publish_window not AM/PM or missing route fields)")

    print("\n" + "=" * 72 + "\n")


def _write_error_stub(err: Exception) -> None:
    stub = {
        "deals": [],
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "next_run": calculate_next_run(),
        "count": 0,
        "error": str(err),
    }
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(stub, f, indent=2)


if __name__ == "__main__":
    try:
        export_published_deals()
    except Exception as e:
        print(f"\n❌ Export failed: {e}")
        _write_error_stub(e)
        raise
