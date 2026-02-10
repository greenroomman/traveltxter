#!/usr/bin/env python3
"""
Export Published Deals Worker
=============================

LOCKED:
- Landing page visibility is driven by publish_window (AM/PM), NOT channel status.
- Export a small, deterministic set of deals for the landing page.

Reads:
- Google Sheet: RAW_DEALS (read-only)

Writes:
- public/deals.json (committed by GitHub Actions for the site)
"""

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ============================================================================
# CONFIG
# ============================================================================

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

SHEET_ID = os.getenv("GOOGLE_SHEET_ID") or os.getenv("GOOGLE_SHEET_ID".upper())
SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

OUTPUT_FILE = "public/deals.json"

# ✅ Hard cap for landing page
MAX_DEALS = 3

# ============================================================================
# GOOGLE SHEETS
# ============================================================================


def get_sheet() -> gspread.Spreadsheet:
    if not SHEET_ID:
        raise ValueError("GOOGLE_SHEET_ID environment variable not set")
    if not SERVICE_ACCOUNT_JSON:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON environment variable not set")

    creds_dict = json.loads(SERVICE_ACCOUNT_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPES)
    client = gspread.authorize(creds)
    return client.open_by_key(SHEET_ID)


# ============================================================================
# EXPORT LOGIC
# ============================================================================


def calculate_next_run() -> str:
    now_utc = datetime.now(timezone.utc)
    h = now_utc.hour
    # Approx slots: AM ~09:00 UTC, PM ~21:00 UTC
    if h < 9:
        return "09:00 UTC"
    if h < 21:
        return "21:00 UTC"
    return "09:00 UTC (next day)"


def is_exportable_by_window(row: Dict[str, Any]) -> bool:
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
    if score >= 9.0:
        return 5
    if score >= 8.5:
        return 4
    return 3


def transform_deal(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    score = _safe_float(row.get("score"), 0.0)
    price = _safe_float(row.get("price_gbp"), 0.0)

    origin_city = (row.get("origin_city") or row.get("origin_iata") or "").strip()
    dest_city = (row.get("destination_city") or row.get("destination_iata") or "").strip()
    origin_iata = (row.get("origin_iata") or "").strip()
    dest_iata = (row.get("destination_iata") or "").strip()

    window = str(row.get("publish_window", "")).strip().upper() or "AM"

    # Minimum route sanity
    if not (origin_city or origin_iata):
        return None
    if not (dest_city or dest_iata):
        return None

    deal_id = row.get("deal_id")
    if not deal_id:
        return None

    name = f"{origin_city or origin_iata} → {dest_city or dest_iata}"

    return {
        "id": str(deal_id),
        "origin": origin_iata,
        "destination": dest_iata,
        "name": name,
        "price": price,
        "currency": "GBP",
        "theme": window,  # AM/PM is the trigger
        "vi_score": round(score, 1),
        "signal_strength": _signal_strength(score),
    }


def export_published_deals() -> None:
    print("\n" + "=" * 72)
    print("EXPORT PUBLISHED DEALS — LANDING PAGE FEED (publish_window driven)")
    print("=" * 72 + "\n")

    sheet = get_sheet()
    ws = sheet.worksheet("RAW_DEALS")

    records: List[Dict[str, Any]] = ws.get_all_records()
    print(f"📖 RAW_DEALS rows: {len(records)}")

    eligible = [r for r in records if is_exportable_by_window(r)]
    print(f"✅ Eligible (publish_window AM/PM): {len(eligible)}")

    deals: List[Dict[str, Any]] = []
    for r in eligible:
        d = transform_deal(r)
        if d:
            deals.append(d)

    # Dedupe by id (prevents React key collisions and repeats)
    deduped: Dict[str, Dict[str, Any]] = {}
    for d in deals:
        deduped[d["id"]] = d
    deals = list(deduped.values())

    # Sort best-first and cap to MAX_DEALS
    deals.sort(key=lambda d: d.get("vi_score", 0), reverse=True)
    deals = deals[:MAX_DEALS]

    output = {
        "deals": deals,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "next_run": calculate_next_run(),
        "count": len(deals),
    }

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    print(f"\n✅ Wrote {OUTPUT_FILE}")
    print(f"📦 Deals exported: {len(deals)} (max {MAX_DEALS})")
    if deals:
        for d in deals:
            print(f"  • {d['name']} — £{d['price']:.0f} (Vi {d['vi_score']}) [{d['theme']}]")
    else:
        print("🔇 No deals exported")


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
