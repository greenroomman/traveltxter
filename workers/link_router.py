#!/usr/bin/env python3
# workers/link_router.py
"""
TravelTxter Link Router – V4.7 (Fixed Duffel Links + Skyscanner Fallback)

Purpose:
- Populate booking_link_vip for monetisable rows
- Primary: Duffel Links (if account has access)
- Fallback: Skyscanner deep links (actual searchable flights)
- DO NOT use homepage links (not useful for booking)

Changes in V4.7:
- Fixed: Duffel Links 403 error handling
- Fixed: Fallback now uses Skyscanner deep links (users can actually book)
- Added: Better error logging
- Added: Option to save offer_id for future custom booking page

Eligibility:
- status in {"READY_TO_POST", "READY_TO_PUBLISH"}
- booking_link_vip is blank
- origin_iata, destination_iata, outbound_date, return_date present
"""

from __future__ import annotations

import os
import json
from datetime import datetime
from typing import Any, Dict, List, Tuple, Optional
from urllib.parse import urlencode

import requests
import gspread
from google.oauth2.service_account import Credentials


# -------------------- ENV --------------------

RAW_DEALS_TAB = (os.environ.get("RAW_DEALS_TAB", "RAW_DEALS") or "RAW_DEALS").strip() or "RAW_DEALS"
SPREADSHEET_ID = (os.environ.get("SPREADSHEET_ID") or os.environ.get("SHEET_ID") or "").strip()
GCP_SA_JSON_ONE_LINE = (os.environ.get("GCP_SA_JSON_ONE_LINE") or os.environ.get("GCP_SA_JSON") or "").strip()

DUFFEL_API_KEY = (os.environ.get("DUFFEL_API_KEY") or "").strip()
DUFFEL_API_BASE = (os.environ.get("DUFFEL_API_BASE") or "https://api.duffel.com").strip().rstrip("/")
DUFFEL_VERSION = (os.environ.get("DUFFEL_VERSION") or "v2").strip() or "v2"

# Set to false if you don't have Duffel Links access (403 errors)
DUFFEL_LINKS_ENABLED = (os.environ.get("DUFFEL_LINKS_ENABLED") or "false").strip().lower() == "true"

REDIRECT_BASE_URL = (os.environ.get("REDIRECT_BASE_URL") or "").strip()
DEFAULT_HOME_BASE = "http://www.traveltxter.com/"

# Fallback strategy: "skyscanner" or "homepage"
FALLBACK_STRATEGY = (os.environ.get("FALLBACK_STRATEGY") or "skyscanner").strip().lower()

MAX_ROWS_PER_RUN = int((os.environ.get("LINK_ROUTER_MAX_ROWS_PER_RUN", "20") or "20").strip() or "20")


# -------------------- LOGGING --------------------

def _log(msg: str) -> None:
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"{ts} | {msg}", flush=True)


# -------------------- SCHEMA --------------------

REQUIRED_COLUMNS = [
    "status",
    "deal_id",
    "origin_iata",
    "destination_iata",
    "outbound_date",
    "return_date",
    "price_gbp",
    "booking_link_vip",
]

ELIGIBLE_STATUSES = {"READY_TO_POST", "READY_TO_PUBLISH"}


def _validate_headers(headers: List[str]) -> None:
    hset = {h.strip() for h in headers if str(h).strip()}
    missing = [c for c in REQUIRED_COLUMNS if c not in hset]
    if missing:
        raise RuntimeError(f"RAW_DEALS schema missing required columns: {missing}")


# -------------------- GOOGLE SHEETS --------------------

def _parse_sa_json(raw: str) -> dict:
    raw = (raw or "").strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return json.loads(raw.replace("\\n", "\n"))


def _gs_client() -> gspread.Client:
    if not GCP_SA_JSON_ONE_LINE:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
    info = _parse_sa_json(GCP_SA_JSON_ONE_LINE)
    creds = Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


def _headers(ws) -> List[str]:
    return [str(h).strip() for h in ws.row_values(1)]


def _colmap(headers: List[str]) -> Dict[str, int]:
    return {h: i + 1 for i, h in enumerate(headers) if h}


def _s(v: Any) -> str:
    return str(v or "").strip()


def _parse_date_to_iso(d: str) -> str:
    """
    Accepts: "YYYY-MM-DD" or "DD/MM/YYYY"
    Returns: "YYYY-MM-DD" or "" if cannot parse.
    """
    d = _s(d)
    if not d:
        return ""
    if len(d) == 10 and d[4] == "-" and d[7] == "-":
        return d
    try:
        dt = datetime.strptime(d, "%d/%m/%Y")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""


def _batch_write(ws, updates: List[Tuple[int, Dict[str, Any]]], cm: Dict[str, int]) -> int:
    cells: List[gspread.cell.Cell] = []
    for row_num, payload in updates:
        for header, value in payload.items():
            if header not in cm:
                continue
            cells.append(gspread.cell.Cell(row=row_num, col=cm[header], value=value))
    if not cells:
        return 0
    ws.update_cells(cells, value_input_option="RAW")
    return len(cells)


# -------------------- DUFFEL LINKS (SESSIONS) --------------------

def _duffel_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {DUFFEL_API_KEY}",
        "Content-Type": "application/json",
        "Duffel-Version": DUFFEL_VERSION,
        "Accept": "application/json",
    }


def _ensure_base_url() -> str:
    return (REDIRECT_BASE_URL or DEFAULT_HOME_BASE).strip() or DEFAULT_HOME_BASE


def _make_outcome_urls(base: str, deal_id: str) -> Dict[str, str]:
    qp = urlencode({"deal_id": deal_id, "src": "duffel_links"})
    sep = "&" if "?" in base else "?"
    success = f"{base}{sep}{qp}&outcome=success"
    failure = f"{base}{sep}{qp}&outcome=failure"
    abandon = f"{base}{sep}{qp}&outcome=abandon"
    return {"success_url": success, "failure_url": failure, "abandonment_url": abandon}


def _create_duffel_links_session(origin_iata: str, dest_iata: str, out_iso: str, in_iso: str, deal_id: str) -> Optional[str]:
    """
    Creates a Duffel Links session and returns the session URL.
    Returns None if Duffel Links not available (403) or any error.
    """
    if not (DUFFEL_LINKS_ENABLED and DUFFEL_API_KEY):
        return None
    if not (origin_iata and dest_iata and out_iso and in_iso and deal_id):
        return None

    base = _ensure_base_url()
    urls = _make_outcome_urls(base, deal_id)

    payload = {
        "data": {
            "reference": deal_id,
            "success_url": urls["success_url"],
            "failure_url": urls["failure_url"],
            "abandonment_url": urls["abandonment_url"],
            "flights": {
                "slices": [
                    {"origin": origin_iata, "destination": dest_iata, "departure_date": out_iso},
                    {"origin": dest_iata, "destination": origin_iata, "departure_date": in_iso},
                ],
                "passengers": [{"type": "adult"}],
                "cabin_class": "economy",
            },
        }
    }

    url = f"{DUFFEL_API_BASE}/links/sessions"

    try:
        r = requests.post(url, headers=_duffel_headers(), json=payload, timeout=25)
    except Exception as e:
        _log(f"⚠️ Duffel Links session exception: {e}")
        return None

    if r.status_code == 403:
        # Feature not available on this account
        _log("⚠️ Duffel Links not available (403). Account may not have access to this feature.")
        _log("   Consider: (1) Contact help@duffel.com to enable, or (2) Set DUFFEL_LINKS_ENABLED=false")
        return None

    if not (200 <= r.status_code < 300):
        snippet = _s(r.text)[:250]
        _log(f"❌ Duffel Links session error {r.status_code}: {snippet}")
        return None

    try:
        data = r.json() or {}
        session_url = (data.get("data") or {}).get("url")
        if session_url:
            return session_url
    except Exception:
        pass

    _log("⚠️ Duffel Links session response missing data.url")
    return None


# -------------------- FALLBACK LINKS --------------------

def _create_skyscanner_link(origin_iata: str, dest_iata: str, out_iso: str, in_iso: str) -> str:
    """
    Create deep link to Skyscanner search results.
    Users land on actual flight search and can book immediately.
    
    Format: https://www.skyscanner.net/transport/flights/ORIGIN/DEST/YYMMDD/YYMMDD/
    Example: https://www.skyscanner.net/transport/flights/LHR/BKK/20260218/20260228/
    """
    # Convert ISO dates (2026-02-18) to compact format (20260218)
    out_compact = out_iso.replace("-", "")
    in_compact = in_iso.replace("-", "")
    
    origin = origin_iata.upper()
    dest = dest_iata.upper()
    
    url = f"https://www.skyscanner.net/transport/flights/{origin}/{dest}/{out_compact}/{in_compact}/"
    
    return url


def _create_google_flights_link(origin_iata: str, dest_iata: str, out_iso: str, in_iso: str) -> str:
    """
    Alternative: Create deep link to Google Flights search.
    """
    origin = origin_iata.upper()
    dest = dest_iata.upper()
    
    # Google Flights uses a query parameter format
    query = f"flights from {origin} to {dest} on {out_iso} return {in_iso}"
    encoded_query = urlencode({"q": query})
    
    url = f"https://www.google.com/travel/flights?{encoded_query}"
    
    return url


def _create_homepage_link(deal_id: str, origin_iata: str, dest_iata: str, out_iso: str, in_iso: str, price_gbp: str) -> str:
    """
    Last resort: Link to homepage with query params.
    Not ideal - users can't actually book from this.
    """
    base = (REDIRECT_BASE_URL or DEFAULT_HOME_BASE).strip().rstrip()
    
    params = {
        "deal_id": _s(deal_id),
        "from": _s(origin_iata).upper(),
        "to": _s(dest_iata).upper(),
        "out": _s(out_iso),
        "in": _s(in_iso),
        "price": _s(price_gbp).replace("£", "").strip(),
        "src": "vip_fallback",
    }
    
    qs = urlencode({k: v for k, v in params.items() if v})
    
    if "?" in base:
        return f"{base}&{qs}"
    return f"{base}?{qs}"


def _create_fallback_link(deal_id: str, origin_iata: str, dest_iata: str, out_iso: str, in_iso: str, price_gbp: str) -> str:
    """
    Create fallback booking link based on FALLBACK_STRATEGY.
    """
    if FALLBACK_STRATEGY == "skyscanner":
        return _create_skyscanner_link(origin_iata, dest_iata, out_iso, in_iso)
    elif FALLBACK_STRATEGY == "google":
        return _create_google_flights_link(origin_iata, dest_iata, out_iso, in_iso)
    else:
        # Default to homepage
        return _create_homepage_link(deal_id, origin_iata, dest_iata, out_iso, in_iso, price_gbp)


# -------------------- MAIN --------------------

def main() -> int:
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")

    _log("============================================================")
    _log("🚀 TravelTxter Link Router V4.7 (Duffel Links + Smart Fallback)")
    _log("============================================================")
    _log(f"RAW_DEALS_TAB={RAW_DEALS_TAB}")
    _log(f"DUFFEL_API_BASE={DUFFEL_API_BASE} DUFFEL_VERSION={DUFFEL_VERSION}")
    _log(f"DUFFEL_API_KEY={'set' if DUFFEL_API_KEY else 'missing'}")
    _log(f"DUFFEL_LINKS_ENABLED={DUFFEL_LINKS_ENABLED}")
    _log(f"FALLBACK_STRATEGY={FALLBACK_STRATEGY}")
    _log(f"MAX_ROWS_PER_RUN={MAX_ROWS_PER_RUN}")

    gc = _gs_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(RAW_DEALS_TAB)

    headers = _headers(ws)
    _validate_headers(headers)
    cm = _colmap(headers)

    records = ws.get_all_records()
    updates: List[Tuple[int, Dict[str, Any]]] = []

    attempted = 0
    duffel_ok = 0
    used_fallback = 0

    for i, r in enumerate(records, start=2):
        if len(updates) >= MAX_ROWS_PER_RUN:
            break

        status = _s(r.get("status")).upper()
        if status not in ELIGIBLE_STATUSES:
            continue

        current_link = _s(r.get("booking_link_vip"))
        if current_link:
            continue

        deal_id = _s(r.get("deal_id"))
        o = _s(r.get("origin_iata")).upper()
        d = _s(r.get("destination_iata")).upper()
        out_iso = _parse_date_to_iso(r.get("outbound_date"))
        in_iso = _parse_date_to_iso(r.get("return_date"))
        price = _s(r.get("price_gbp"))

        if not (deal_id and o and d and out_iso and in_iso):
            continue

        attempted += 1

        # Try Duffel Links first (if enabled and available)
        link = _create_duffel_links_session(o, d, out_iso, in_iso, deal_id)
        
        if link:
            duffel_ok += 1
            _log(f"✅ Duffel Links: {deal_id} | {o}→{d}")
        else:
            # Use smart fallback (Skyscanner/Google/Homepage)
            link = _create_fallback_link(deal_id, o, d, out_iso, in_iso, price)
            used_fallback += 1
            _log(f"🔗 Fallback ({FALLBACK_STRATEGY}): {deal_id} | {o}→{d}")

        updates.append((i, {"booking_link_vip": link}))

    written_cells = _batch_write(ws, updates, cm)

    _log(f"Attempted link generations: {attempted}")
    _log(f"booking_link_vip populated for: {len(updates)} rows")
    _log(f"Duffel Links created: {duffel_ok}")
    _log(f"Fallback links used: {used_fallback} (strategy: {FALLBACK_STRATEGY})")
    _log(f"Cells written (batch): {written_cells}")

    if duffel_ok == 0 and attempted > 0 and DUFFEL_LINKS_ENABLED:
        _log("")
        _log("⚠️  NOTICE: Duffel Links is enabled but no links were created.")
        _log("   This likely means your Duffel account doesn't have access to Links.")
        _log("   Options:")
        _log("   1. Contact help@duffel.com to enable Duffel Links on your account")
        _log("   2. Set DUFFEL_LINKS_ENABLED=false to skip trying")
        _log(f"   3. Current fallback ({FALLBACK_STRATEGY}) is working and users can book")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
