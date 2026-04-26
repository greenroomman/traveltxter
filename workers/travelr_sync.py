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

SUPABASE_URL = os.environ["TRAVELR_SUPABASE_URL"].rstrip("/")
SERVICE_ROLE_KEY = os.environ["TRAVELR_SERVICE_ROLE_KEY"]

SPREADSHEET_ID = (
    os.environ.get("GOOGLE_SHEETS_SPREADSHEET_ID")
    or os.environ.get("SPREADSHEET_ID")
    or os.environ.get("SHEET_ID")
)

if not SPREADSHEET_ID:
    raise RuntimeError("Missing GOOGLE_SHEETS_SPREADSHEET_ID / SPREADSHEET_ID / SHEET_ID")

RAW_DEALS_TAB = os.environ.get("RAW_DEALS_TAB", "RAW_DEALS")
DEALS_CACHE_ENDPOINT = f"{SUPABASE_URL}/rest/v1/deals_cache?on_conflict=id"

SUPABASE_HEADERS = {
    "Authorization": f"Bearer {SERVICE_ROLE_KEY}",
    "apikey": SERVICE_ROLE_KEY,
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates,return=minimal",
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

CACHE_TTL_DAYS = 30
_UNIX_TS_MIN = 978_307_200

GOOGLE_SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]


def _normalise_private_key(pk: str) -> str:
    if "\\\\n" in pk and "\\n" not in pk:
        pk = pk.replace("\\\\n", "\\n")
    return pk.strip()


def load_sa_info() -> dict:
    raw = os.environ.get("GCP_SA_JSON_ONE_LINE") or os.environ.get("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON / GCP_SA_JSON_ONE_LINE")

    info = json.loads(raw)
    info["private_key"] = _normalise_private_key(info.get("private_key", ""))
    return info


def safe_timestamp(val):
    if not val:
        return None

    raw = str(val).strip()
    if not raw:
        return None

    try:
        serial = float(raw)
        if 40000 < serial < 60000:
            return (datetime(1899, 12, 30) + timedelta(days=serial)).isoformat()
        if serial > _UNIX_TS_MIN:
            return datetime.fromtimestamp(serial, tz=timezone.utc).isoformat()
    except:
        pass

    return raw


def safe_date(val):
    if not val:
        return None

    raw = str(val).strip()
    if not raw:
        return None

    try:
        serial = float(raw)
        if 40000 < serial < 60000:
            return (datetime(1899, 12, 30) + timedelta(days=serial)).strftime("%Y-%m-%d")
        if serial > _UNIX_TS_MIN:
            return datetime.fromtimestamp(serial, tz=timezone.utc).strftime("%Y-%m-%d")
    except:
        pass

    return raw[:10]


def truthy(val):
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in {"true", "1", "yes", "y"}


def compute_expires_at(deal):
    for field in ("outbound_date", "ingested_at_utc"):
        raw = str(deal.get(field, "")).strip()
        if not raw:
            continue

        try:
            serial = float(raw)
            if 40000 < serial < 60000:
                base = datetime(1899, 12, 30) + timedelta(days=serial)
                return (base + timedelta(days=CACHE_TTL_DAYS)).isoformat()
            if serial > _UNIX_TS_MIN:
                base = datetime.fromtimestamp(serial, tz=timezone.utc)
                return (base + timedelta(days=CACHE_TTL_DAYS)).isoformat()
        except:
            pass

        try:
            base = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return (base + timedelta(days=CACHE_TTL_DAYS)).isoformat()
        except:
            continue

    return (datetime.now(timezone.utc) + timedelta(days=CACHE_TTL_DAYS)).isoformat()


def get_sheet():
    info = load_sa_info()
    creds = Credentials.from_service_account_info(info, scopes=GOOGLE_SCOPE)
    client = gspread.authorize(creds)
    return client.open_by_key(SPREADSHEET_ID)


def read_ready_deals(sheet):
    ws = sheet.worksheet(RAW_DEALS_TAB)
    rows = ws.get_all_records()
    ready = [r for r in rows if str(r.get("status", "")).strip() in TARGET_STATUSES]
    log.info("Rows: %s | Ready: %s", len(rows), len(ready))
    return ready


def build_payload(deal):
    deal_id = str(deal.get("deal_id", "")).strip()
    if not deal_id:
        return None

    try:
        price = float(str(deal.get("price_gbp") or 0).replace("£", "").replace(",", ""))
    except:
        return None

    try:
        score = int(float(deal.get("score") or 0))
    except:
        score = 0

    carriers_raw = str(deal.get("carriers") or "").strip()
    carriers_list = [c.strip() for c in carriers_raw.split(",") if c.strip()]

    return {
        "id": deal_id,
        "origin_iata": str(deal.get("origin_iata", "")).upper(),
        "destination_iata": str(deal.get("destination_iata", "")).upper(),
        "city": str(deal.get("destination_city") or "").strip(),
        "country": str(deal.get("destination_country") or "").strip(),
        "price_gbp": price,
        "currency": "GBP",
        "stops": int(deal.get("stops", 0) or 0),
        "cabin_class": str(deal.get("cabin_class") or "economy"),
        "carrier": carriers_raw,
        "carriers": carriers_list,
        "bags_included": truthy(deal.get("bags_incl")),
        "outbound_date": safe_date(deal.get("outbound_date")),
        "return_date": safe_date(deal.get("return_date")),
        "dates_display": str(deal.get("dates_display") or ""),
        "score": score,
        "theme": str(deal.get("theme") or ""),
        "tagline": str(deal.get("phrase_used") or ""),
        "detail": str(deal.get("deal_detail") or ""),
        "booking_url": str(deal.get("booking_link_vip") or ""),
        "status": "READY_TO_PUBLISH",
        "is_active": True,
        "ingested_at_utc": safe_timestamp(deal.get("ingested_at_utc")),
        "scored_at": safe_timestamp(deal.get("scored_timestamp")),
        "expires_at": compute_expires_at(deal),
    }


def upsert_deal(payload):
    try:
        r = requests.post(
            DEALS_CACHE_ENDPOINT,
            headers=SUPABASE_HEADERS,
            json=payload,
            timeout=15,
        )
        return r.status_code in (200, 201, 204)
    except:
        return False


def main():
    sheet = get_sheet()
    deals = read_ready_deals(sheet)

    synced = 0
    skipped = 0
    failed = 0

    for deal in deals:
        payload = build_payload(deal)

        if not payload:
            skipped += 1
            continue

        if upsert_deal(payload):
            synced += 1
        else:
            failed += 1

    log.info("Done | synced=%s skipped=%s failed=%s", synced, skipped, failed)

    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()