"""
travelr_sync.py
═══════════════════════════════════════════════════════════
V5 Pipeline Step 7 — Travelr Sync Worker

Reads publishable deals from RAW_DEALS (Google Sheets) and
upserts them into deals_cache (Supabase).

Required environment variables:
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
from datetime import datetime, timedelta, timezone
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

# Tab name is configurable — matches RAW_DEALS_TAB secret in GitHub Actions
RAW_DEALS_TAB = os.environ.get("RAW_DEALS_TAB", "RAW_DEALS")

SUPABASE_HEADERS = {
    "Authorization": f"Bearer {SERVICE_ROLE_KEY}",
    "apikey":        SERVICE_ROLE_KEY,
    "Content-Type":  "application/json",
    "Prefer":        "resolution=merge-duplicates",
}

# All statuses that represent a deal ready for Travelr consumption
TARGET_STATUSES = {
    "POSTED_ALL",
    "POSTED_INSTAGRAM",
    "VIP_DONE",
    "READY_FREE",
    "PUBLISHED",
    "READY_FOR_FREE",
    "READY_FOR_BOTH",
}

# Deals expire from cache this many days after outbound_date
# If outbound_date is missing, expire CACHE_TTL_DAYS from ingested_at
CACHE_TTL_DAYS = 30

GOOGLE_SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]


# ── Helpers ────────────────────────────────────────────────────────────────


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


def safe_timestamp(val) -> str | None:
    """Handle both ISO timestamps and Excel serial numbers."""
    if not val:
        return None
    val = str(val).strip()
    try:
        serial = float(val)
        if 40000 < serial < 60000:
            return (datetime(1899, 12, 30) + timedelta(days=serial)).isoformat()
    except ValueError:
        pass
    return val or None


def compute_expires_at(deal: dict) -> str:
    """
    Expire the deal CACHE_TTL_DAYS after its outbound_date.
    Falls back to CACHE_TTL_DAYS after ingested_at.
    Falls back to CACHE_TTL_DAYS from now.
    """
    # Try outbound_date first — most meaningful expiry anchor
    for field in ("outbound_date", "ingested_at_utc"):
        raw = str(deal.get(field, "")).strip()
        if not raw:
            continue
        try:
            # Handle Excel serial
            serial = float(raw)
            if 40000 < serial < 60000:
                base = datetime(1899, 12, 30) + timedelta(days=serial)
                return (base + timedelta(days=CACHE_TTL_DAYS)).isoformat()
        except ValueError:
            pass
        try:
            # Parse ISO date or datetime
            base = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return (base + timedelta(days=CACHE_TTL_DAYS)).isoformat()
        except ValueError:
            continue

    # Final fallback: now + TTL
    return (datetime.now(timezone.utc) + timedelta(days=CACHE_TTL_DAYS)).isoformat()


# ── Sheets ─────────────────────────────────────────────────────────────────


def get_sheet():
    info = load_sa_info()
    creds = Credentials.from_service_account_info(info, scopes=GOOGLE_SCOPE)
    client = gspread.authorize(creds)
    return client.open_by_key(SPREADSHEET_ID)


def read_ready_deals(sheet) -> list[dict]:
    ws = sheet.worksheet(RAW_DEALS_TAB)
    rows = ws.get_all_records()
    ready = [r for r in rows if r.get("status", "").strip() in TARGET_STATUSES]
    log.info(f"Tab '{RAW_DEALS_TAB}': {len(rows)} total rows, {len(ready)} with publishable status")
    return ready


# ── Payload builder ────────────────────────────────────────────────────────


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

    # carriers: prefer dedicated carriers column, fall back to carrier
    carriers_raw = str(
        deal.get("carriers") or deal.get("carrier") or ""
    ).strip()
    # deals_cache.carriers is PostgreSQL text[] — must be a list
    carriers_list = [c.strip() for c in carriers_raw.split(",") if c.strip()]

    return {
        "id":               deal_id,
        "origin_iata":      str(deal.get("origin_iata", "")).strip().upper(),
        "destination_iata": str(deal.get("destination_iata", "")).strip().upper(),
        "city":             str(deal.get("destination_city", deal.get("city", ""))).strip(),
        "country":          str(deal.get("destination_country", deal.get("country", ""))).strip(),
        "emoji":            str(deal.get("emoji", "✈️")).strip() or "✈️",
        "hue":              str(deal.get("hue", "214,40%,14%")).strip(),
        "price_gbp":        price,
        "currency":         "GBP",
        "stops":            int(deal.get("stops", 0) or 0),
        "cabin_class":      str(deal.get("cabin_class", "economy")).strip(),
        "carrier":          carriers_raw,   # singular text — primary display
        "carriers":         carriers_list,  # text[]  — PostgreSQL array
        "bags_included":    bool(deal.get("bags_incl", False)),
        "outbound_date":    str(deal.get("outbound_date", "")).strip() or None,
        "return_date":      str(deal.get("return_date", "")).strip() or None,
        "dates_display":    dates_display,
        "score":            score,
        "window_type":      str(deal.get("publish_window", "AM")).strip().upper(),
        "theme":            str(deal.get("theme", "")).strip(),
        "tagline":          str(deal.get("phrase_used", deal.get("tagline", ""))).strip(),
        "detail":           str(deal.get("deal_detail", deal.get("detail", ""))).strip(),
        "duffel_offer_id":  str(deal.get("duffel_offer_id", "")).strip() or None,
        "booking_url":      str(deal.get("booking_link_vip", deal.get("booking_url", ""))).strip() or None,
        "status":           "READY_TO_PUBLISH",
        "is_active":        True,
        "ingested_at_utc":  safe_timestamp(deal.get("ingested_at_utc", "")),
        "scored_at":        safe_timestamp(deal.get("scored_timestamp", "")),
        "expires_at":       compute_expires_at(deal),
    }


# ── Supabase upsert ────────────────────────────────────────────────────────


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


# ── Main ───────────────────────────────────────────────────────────────────


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
            log.info(
                f"  ✓  {payload['id']:30s}  "
                f"{payload['city']:18s}  "
                f"£{payload['price_gbp']:.0f}  "
                f"score={payload['score']}  "
                f"expires={payload['expires_at'][:10]}"
            )
            synced += 1
        else:
            log.error(f"  ✗  {payload['id']}")
            failed += 1

    log.info("──────────────────────────────────────────────────")
    log.info(f"Travelr Sync complete:  synced={synced}  skipped={skipped}  failed={failed}")

    if failed > 0:
        log.warning(f"{failed} deal(s) failed to sync.")
        raise SystemExit(1)

    log.info("── Green ✓ ────────────────────────────────────────")


if __name__ == "__main__":
    main()
