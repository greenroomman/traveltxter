"""
travelr_sync.py
═══════════════════════════════════════════════════════════
V5 Pipeline Step 7 — Travelr Sync Worker

Position in pipeline:
  1. Feeder
  2. AI Scorer
  3. Enrichment Router
  4. Renderer
  5. Link Router
  6. Publishers (IG → TG VIP → TG FREE)
  7. Travelr Sync  ← THIS FILE

Purpose:
  Read deals that have completed the full V5 pipeline
  (status = POSTED_ALL) and write them to Travelr's
  deals_cache table in Supabase.

Reads:
  RAW_DEALS (Google Sheets) — status = POSTED_ALL

Writes:
  Supabase deals_cache table (via REST API)

Does NOT:
  - Modify RAW_DEALS status
  - Score deals
  - Enrich deals
  - Publish anywhere
  - Remember state between runs (stateless)

Idempotency:
  Uses Supabase upsert (Prefer: resolution=merge-duplicates)
  Safe to rerun — will not create duplicate rows.

Required environment variables:
  TRAVELR_SUPABASE_URL          https://yourref.supabase.co
  TRAVELR_SERVICE_ROLE_KEY      eyJ... (service_role key)
  GOOGLE_SHEETS_SPREADSHEET_ID  your sheet ID
  GOOGLE_CREDENTIALS_JSON       path to service account JSON
═══════════════════════════════════════════════════════════
"""

import os
import json
import logging
import requests
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timezone

# ── Logging ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("travelr_sync")

# ── Config ───────────────────────────────────────────────
SUPABASE_URL     = os.environ["TRAVELR_SUPABASE_URL"].rstrip("/")
SERVICE_ROLE_KEY = os.environ["TRAVELR_SERVICE_ROLE_KEY"]
SPREADSHEET_ID   = os.environ["GOOGLE_SHEETS_SPREADSHEET_ID"]
CREDENTIALS_FILE = os.environ.get("GOOGLE_CREDENTIALS_JSON", "credentials.json")

DEALS_CACHE_ENDPOINT = f"{SUPABASE_URL}/rest/v1/deals_cache"

SUPABASE_HEADERS = {
    "Authorization": f"Bearer {SERVICE_ROLE_KEY}",
    "apikey":        SERVICE_ROLE_KEY,
    "Content-Type":  "application/json",
    "Prefer":        "resolution=merge-duplicates",
}

# Status to read from RAW_DEALS
# Change to POSTED_INSTAGRAM if you want to sync before Telegram publishing
TARGET_STATUS = "POSTED_ALL"


# ── Sheets connection ─────────────────────────────────────
def get_sheet():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(SPREADSHEET_ID)


def read_ready_deals(sheet) -> list[dict]:
    """
    Read all rows from RAW_DEALS where status = TARGET_STATUS.
    Returns list of dicts keyed by column header.
    """
    ws = sheet.worksheet("RAW_DEALS")
    rows = ws.get_all_records()
    ready = [r for r in rows if r.get("status", "").strip() == TARGET_STATUS]
    log.info(f"RAW_DEALS: {len(rows)} total rows, {len(ready)} with status={TARGET_STATUS}")
    return ready


# ── Payload builder ───────────────────────────────────────
def build_payload(deal: dict) -> dict | None:
    """
    Map RAW_DEALS columns to deals_cache schema.
    Returns None if required fields are missing.
    """
    deal_id = str(deal.get("deal_id", "")).strip()
    if not deal_id:
        log.warning("Skipping row with no deal_id")
        return None

    price = deal.get("price_gbp") or deal.get("price") or 0
    try:
        price = float(str(price).replace("£", "").replace(",", "").strip())
    except (ValueError, TypeError):
        log.warning(f"Skipping {deal_id}: unparseable price '{price}'")
        return None

    score = deal.get("score") or 0
    try:
        score = int(score)
    except (ValueError, TypeError):
        score = 0

    # Dates display — use pre-formatted if available, else compose
    dates_display = (
        deal.get("dates_display")
        or deal.get("date_range")
        or f"{deal.get('outbound_date', '')} – {deal.get('return_date', '')}"
    ).strip()

    return {
        "id":                str(deal_id),
        "origin_iata":       str(deal.get("origin_iata", "")).strip().upper(),
        "destination_iata":  str(deal.get("destination_iata", "")).strip().upper(),
        "city":              str(deal.get("destination_city", deal.get("city", ""))).strip(),
        "country":           str(deal.get("destination_country", deal.get("country", ""))).strip(),
        "emoji":             str(deal.get("emoji", "✈️")).strip() or "✈️",
        "hue":               str(deal.get("hue", "214,40%,14%")).strip(),
        "price_gbp":         price,
        "currency":          "GBP",
        "stops":             int(deal.get("stops", 0) or 0),
        "cabin_class":       str(deal.get("cabin_class", "economy")).strip(),
        "carrier":           str(deal.get("carriers", deal.get("carrier", ""))).strip(),
        "bags_included":     bool(deal.get("bags_incl", False)),
        "outbound_date":     str(deal.get("outbound_date", "")).strip() or None,
        "return_date":       str(deal.get("return_date", "")).strip() or None,
        "dates_display":     dates_display,
        "score":             score,
        "window_type":       str(deal.get("publish_window", "AM")).strip().upper(),
        "theme":             str(deal.get("theme", "")).strip(),
        "tagline":           str(deal.get("phrase_used", deal.get("tagline", ""))).strip(),
        "detail":            str(deal.get("deal_detail", deal.get("detail", ""))).strip(),
        "duffel_offer_id":   str(deal.get("duffel_offer_id", "")).strip() or None,
        "booking_url":       str(deal.get("booking_link_vip", deal.get("booking_url", ""))).strip() or None,
        "status":            "READY_TO_PUBLISH",
        "is_active":         True,
        "ingested_at_utc":   str(deal.get("ingested_at_utc", "")).strip() or None,
        "scored_at":         str(deal.get("scored_timestamp", "")).strip() or None,
    }


# ── Supabase writer ───────────────────────────────────────
def upsert_deal(payload: dict) -> bool:
    """
    Upsert a single deal into Supabase deals_cache.
    Idempotent — safe to call multiple times for the same deal_id.
    Returns True on success.
    """
    try:
        r = requests.post(
            DEALS_CACHE_ENDPOINT,
            headers=SUPABASE_HEADERS,
            json=payload,
            timeout=10,
        )
        if r.status_code in (200, 201):
            return True
        log.error(f"Supabase error for {payload['id']}: {r.status_code} {r.text[:200]}")
        return False
    except requests.RequestException as e:
        log.error(f"Network error for {payload['id']}: {e}")
        return False


# ── Main ──────────────────────────────────────────────────
def main():
    log.info("── Travelr Sync starting ──────────────────────────")

    # 1. Connect to Sheets
    try:
        sheet = get_sheet()
        log.info(f"Connected to spreadsheet: {sheet.title}")
    except Exception as e:
        log.error(f"Failed to connect to Google Sheets: {e}")
        raise SystemExit(1)

    # 2. Read ready deals
    try:
        deals = read_ready_deals(sheet)
    except Exception as e:
        log.error(f"Failed to read RAW_DEALS: {e}")
        raise SystemExit(1)

    if not deals:
        log.info(f"No deals with status={TARGET_STATUS}. Nothing to sync. Exiting cleanly.")
        return

    # 3. Sync each deal
    synced  = 0
    skipped = 0
    failed  = 0

    for deal in deals:
        payload = build_payload(deal)

        if payload is None:
            skipped += 1
            continue

        success = upsert_deal(payload)

        if success:
            log.info(f"  ✓ synced  {payload['id']:30s}  {payload['city']:18s}  £{payload['price_gbp']:.0f}  score={payload['score']}")
            synced += 1
        else:
            log.error(f"  ✗ failed  {payload['id']}")
            failed += 1

    # 4. Summary
    log.info("──────────────────────────────────────────────────")
    log.info(f"Travelr Sync complete:  synced={synced}  skipped={skipped}  failed={failed}")

    if failed > 0:
        log.warning(f"{failed} deal(s) failed to sync. Check Supabase credentials and network.")
        raise SystemExit(1)

    log.info("── Green ✓ ────────────────────────────────────────")


if __name__ == "__main__":
    main()
