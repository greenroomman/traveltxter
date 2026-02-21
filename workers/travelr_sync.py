"""
travelr_sync.py
═══════════════════════════════════════════════════════════
V5 Pipeline Step 7 — Travelr Sync Worker

Required environment variables (matches existing V5 workers):
  TRAVELR_SUPABASE_URL          https://yourref.supabase.co
  TRAVELR_SERVICE_ROLE_KEY      eyJ... (service_role key)
  SPREADSHEET_ID or SHEET_ID    your sheet ID
  GCP_SA_JSON_ONE_LINE          service account JSON as one line
  GCP_SA_JSON                   service account JSON (fallback)
═══════════════════════════════════════════════════════════
"""

import os
import json
import logging
import requests
import gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("travelr_sync")

SUPABASE_URL     = os.environ["TRAVELR_SUPABASE_URL"].rstrip("/")
SERVICE_ROLE_KEY = os.environ["TRAVELR_SERVICE_ROLE_KEY"]
SPREADSHEET_ID   = (
    os.environ.get("GOOGLE_SHEETS_SPREADSHEET_ID")
    or os.environ.get("SPREADSHEET_ID")
    or os.environ.get("SHEET_ID")
)
if not SPREADSHEET_ID:
    raise RuntimeError("Missing GOOGLE_SHEETS_SPREADSHEET_ID / SPREADSHEET_ID / SHEET_ID")

DEALS_CACHE_ENDPOINT = f"{SUPABASE_URL}/rest/v1/deals_cache"

SUPABASE_HEADERS = {
    "Authorization": f"Bearer {SERVICE_ROLE_KEY}",
    "apikey":        SERVICE_ROLE_KEY,
    "Content-Type":  "application/json",
    "Prefer":        "resolution=merge-duplicates",
}

TARGET_STATUSES = {
    "POSTED_ALL",
    "POSTED_INSTAGRAM",
    "VIP_DONE",
    "READY_FREE",
    "PUBLISHED",
    "READY_FOR_FREE",
    "READY_FOR_BOTH",
}

GOOGLE_SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]


def _normalise_private_key(pk: str) -> str:
    if "\\n" in pk and "\n" not in pk:
        pk = pk.replace("\\n", "\n")
    return pk.strip()


def load_sa_info() -> dict:
    raw = os.environ.get("GCP_SA_JSON_ONE_LINE") or os.environ.get("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON / GCP_SA_JSON_ONE_LINE")
    try:
        info = json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"GCP_SA_JSON is not valid JSON: {e}") from e
    info["private_key"] = _normalise_private_key(info.get("private_key", ""))
    return info


def get_sheet():
    info = load_sa_info()
    creds = Credentials.from_service_account_info(info, scopes=GOOGLE_SCOPE)
    client = gspread.authorize(creds)
    return client.open_by_key(SPREADSHEET_ID)


def read_ready_deals(sheet) -> list[dict]:
    ws = sheet.worksheet("RAW_DEALS")
    rows = ws.get_all_records()
    ready = [r for r in rows if r.get("status", "").strip() in TARGET_STATUSES]
    log.info(f"RAW_DEALS: {len(rows)} total rows, {len(ready)} with publishable status")
    return ready


def build_payload(deal: dict) -> dict | None:
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


def upsert_deal(payload: dict) -> bool:
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


def main():
    log.info("── Travelr Sync starting ──────────────────────────")
    log.info(f"Target statuses: {sorted(TARGET_STATUSES)}")

    try:
        sheet = get_sheet()
        log.info(f"Connected to spreadsheet: {sheet.title}")
    except Exception as e:
        log.error(f"Failed to connect to Google Sheets: {e}")
        raise SystemExit(1)

    try:
        deals = read_ready_deals(sheet)
    except Exception as e:
        log.error(f"Failed to read RAW_DEALS: {e}")
        raise SystemExit(1)

    if not deals:
        log.info("No publishable deals found. Nothing to sync. Exiting cleanly.")
        return

    synced = skipped = failed = 0

    for deal in deals:
        payload = build_payload(deal)
        if payload is None:
            skipped += 1
            continue
        if upsert_deal(payload):
            log.info(f"  ✓ synced  {payload['id']:30s}  {payload['city']:18s}  £{payload['price_gbp']:.0f}  score={payload['score']}")
            synced += 1
        else:
            log.error(f"  ✗ failed  {payload['id']}")
            failed += 1

    log.info("──────────────────────────────────────────────────")
    log.info(f"Travelr Sync complete:  synced={synced}  skipped={skipped}  failed={failed}")

    if failed > 0:
        log.warning(f"{failed} deal(s) failed to sync.")
        raise SystemExit(1)

    log.info("── Green ✓ ────────────────────────────────────────")


if __name__ == "__main__":
    main()
