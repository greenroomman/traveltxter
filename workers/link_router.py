#!/usr/bin/env python3
# workers/link_router.py
"""
TravelTxter Link Router – V4.8 (Fast batch API + timing logs)

Purpose:
- Populate booking_link_vip for selected rows (READY_* only)
- PRIMARY: TravelUp via CJ deep link wrapper (URL-only rail, monetised)
- FALLBACK: Google Flights deep links (best trust/UX, non-monetised)
- LAST RESORT: Duffel Links (NOT monetised for you; keep as emergency only)

V4.8 Performance improvements:
- Replaced get_all_records() with batch get_all_values() (single API call)
- Added timing logs for sheet operations
- 7+ minute processing reduced to seconds

Rail doctrine (locked):
- Third-party rails must NEVER be surfaced in user-facing copy.
- Rails may appear ONLY inside booking_link_vip URL.
- No CJ scripts / banner code / impression tags are used here.

Notes:
- This file only writes to RAW_DEALS.booking_link_vip
- Stateless + deterministic: same row inputs -> same link outputs.
"""

from __future__ import annotations

import os
import json
import re
import unicodedata
from datetime import datetime
from typing import Any, Dict, List, Tuple, Optional
from urllib.parse import urlencode, quote

import requests
import gspread
from google.oauth2.service_account import Credentials


# -------------------- ENV --------------------

RAW_DEALS_TAB = (os.environ.get("RAW_DEALS_TAB", "RAW_DEALS") or "RAW_DEALS").strip() or "RAW_DEALS"
SPREADSHEET_ID = (os.environ.get("SPREADSHEET_ID") or os.environ.get("SHEET_ID") or "").strip()
GCP_SA_JSON_ONE_LINE = (os.environ.get("GCP_SA_JSON_ONE_LINE") or os.environ.get("GCP_SA_JSON") or "").strip()

# Duffel (LAST RESORT ONLY)
DUFFEL_API_KEY = (os.environ.get("DUFFEL_API_KEY") or "").strip()
DUFFEL_API_BASE = (os.environ.get("DUFFEL_API_BASE") or "https://api.duffel.com").strip().rstrip("/")
DUFFEL_VERSION = (os.environ.get("DUFFEL_VERSION") or "v2").strip() or "v2"
# Default false: you do NOT earn from Duffel Links, so keep it off unless you explicitly want it.
DUFFEL_LINKS_ENABLED = (os.environ.get("DUFFEL_LINKS_ENABLED") or "false").strip().lower() == "true"

REDIRECT_BASE_URL = (os.environ.get("REDIRECT_BASE_URL") or "").strip()
DEFAULT_HOME_BASE = "http://www.traveltxter.com/"

# Fallback strategy AFTER TravelUp: google (default), skyscanner, homepage
FALLBACK_STRATEGY = (os.environ.get("FALLBACK_STRATEGY") or "google").strip().lower()

MAX_ROWS_PER_RUN = int((os.environ.get("LINK_ROUTER_MAX_ROWS_PER_RUN", "20") or "20").strip() or "20")

# TravelUp (CJ) rail (PRIMARY)
TRAVELUP_ENABLED = (os.environ.get("TRAVELUP_ENABLED") or "true").strip().lower() == "true"
TRAVELUP_BASE = (os.environ.get("TRAVELUP_BASE") or "https://www.travelup.com").strip().rstrip("/")
TRAVELUP_LOCALE_PATH = (os.environ.get("TRAVELUP_LOCALE_PATH") or "/en-gb/flight-offers").strip().strip("/")
# Confirmed in your CJ UI (dpbolvw + publisher 101634441 + deep link id 15510915)
TRAVELUP_CJ_CLICK_BASE = (
    os.environ.get("TRAVELUP_CJ_CLICK_BASE")
    or "https://www.dpbolvw.net/click-101634441-15510915?url="
).strip()
# sid tracking: deal_id (default), none, static
TRAVELUP_SID_MODE = (os.environ.get("TRAVELUP_SID_MODE") or "deal_id").strip().lower()
TRAVELUP_SID_STATIC = (os.environ.get("TRAVELUP_SID_STATIC") or "traveltxter").strip()


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

# destination_city is OPTIONAL (but required for TravelUp link generation)
OPTIONAL_COLUMNS = ["destination_city"]

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


def _unique_headers(headers: list[str]) -> list[str]:
    """Make headers unique and non-empty for dict construction.
    Handles duplicate or empty headers by creating stable synthetic names.
    """
    seen: dict[str, int] = {}
    out: list[str] = []
    for i, h in enumerate(headers):
        base = (str(h).strip() if h is not None else "")
        if base == "":
            base = f"__col_{i+1}"
        key = base
        seen[key] = seen.get(key, 0) + 1
        if seen[key] > 1:
            key = f"{base}__{seen[base]}"
        out.append(key)
    return out


def _get_records(ws) -> list[dict]:
    """Fast batch read using get_all_values() - single API call per sheet.
    Replaces get_all_records() which makes multiple slow API calls.
    """
    all_values = ws.get_all_values()
    if not all_values:
        return []
    
    # First row is headers
    raw_headers = all_values[0]
    headers = _unique_headers(raw_headers)
    
    records = []
    for row in all_values[1:]:
        # Pad row to match header length (handles short rows)
        padded = row + [''] * (len(headers) - len(row))
        record = dict(zip(headers, padded[:len(headers)]))
        records.append(record)
    
    return records


def _headers(ws) -> List[str]:
    """Get headers using fast batch read (single API call)."""
    all_values = ws.get_all_values()
    if not all_values:
        return []
    return [str(h).strip() for h in all_values[0]]


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


# -------------------- TRAVELUP (CJ) – PRIMARY --------------------

def _slugify_city(city: str) -> str:
    """
    Conservative slugify for TravelUp destination pages:
    - lowercase
    - remove diacritics
    - remove punctuation
    - spaces -> hyphens
    """
    city = _s(city).lower()
    if not city:
        return ""

    city = unicodedata.normalize("NFKD", city)
    city = "".join(ch for ch in city if not unicodedata.combining(ch))
    city = re.sub(r"[^\w\s-]", "", city, flags=re.UNICODE)  # drop punctuation
    city = city.replace("_", " ")
    city = re.sub(r"\s+", " ", city).strip()
    city = city.replace(" ", "-")
    city = re.sub(r"-{2,}", "-", city).strip("-")
    return city


def _travelup_destination_url(dest_iata: str, destination_city: str) -> str:
    """
    https://www.travelup.com/en-gb/flight-offers/{city-slug}-{iata}
    """
    iata = _s(dest_iata).lower()
    slug = _slugify_city(destination_city)
    if not (iata and slug):
        return ""
    return f"{TRAVELUP_BASE}/{TRAVELUP_LOCALE_PATH}/{slug}-{iata}"


def _travelup_wrap_cj(url_to_wrap: str, deal_id: str) -> str:
    """Wrap a TravelUp URL using CJ click base. Adds sid if configured."""
    if not url_to_wrap:
        return ""
    encoded = quote(url_to_wrap, safe="")
    out = f"{TRAVELUP_CJ_CLICK_BASE}{encoded}"

    if TRAVELUP_SID_MODE == "none":
        return out

    if TRAVELUP_SID_MODE == "static":
        sid_val = TRAVELUP_SID_STATIC
    else:
        sid_val = deal_id

    if sid_val:
        joiner = "&" if "?" in out else "?"
        out = f"{out}{joiner}sid={quote(_s(sid_val), safe='')}"
    return out


def _create_travelup_link(dest_iata: str, destination_city: str, deal_id: str) -> str:
    """Return CJ-wrapped TravelUp destination URL, or "" if cannot safely build."""
    if not TRAVELUP_ENABLED:
        return ""
    dest_url = _travelup_destination_url(dest_iata, destination_city)
    if not dest_url:
        return ""
    return _travelup_wrap_cj(dest_url, deal_id)


# -------------------- FALLBACK LINKS (NON-MONETISED) --------------------

def _create_skyscanner_link(origin_iata: str, dest_iata: str, out_iso: str, in_iso: str) -> str:
    out_compact = out_iso.replace("-", "")
    in_compact = in_iso.replace("-", "")
    origin = origin_iata.upper()
    dest = dest_iata.upper()
    return f"https://www.skyscanner.net/transport/flights/{origin}/{dest}/{out_compact}/{in_compact}/"


def _create_google_flights_link(origin_iata: str, dest_iata: str, out_iso: str, in_iso: str) -> str:
    origin = origin_iata.upper()
    dest = dest_iata.upper()
    query = f"flights from {origin} to {dest} on {out_iso} return {in_iso}"
    encoded_query = urlencode({"q": query})
    return f"https://www.google.com/travel/flights?{encoded_query}"


def _create_homepage_link(deal_id: str, origin_iata: str, dest_iata: str, out_iso: str, in_iso: str, price_gbp: str) -> str:
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
    return f"{base}&{qs}" if "?" in base else f"{base}?{qs}"


def _create_fallback_link(deal_id: str, origin_iata: str, dest_iata: str, out_iso: str, in_iso: str, price_gbp: str) -> str:
    if FALLBACK_STRATEGY == "skyscanner":
        return _create_skyscanner_link(origin_iata, dest_iata, out_iso, in_iso)
    if FALLBACK_STRATEGY == "homepage":
        return _create_homepage_link(deal_id, origin_iata, dest_iata, out_iso, in_iso, price_gbp)
    # default
    return _create_google_flights_link(origin_iata, dest_iata, out_iso, in_iso)


# -------------------- DUFFEL LINKS (LAST RESORT) --------------------

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
    if not (DUFFEL_LINKS_ENABLED and DUFFEL_API_KEY):
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
        _log("⚠️ Duffel Links not available (403). Account may not have access to this feature.")
        return None

    if not (200 <= r.status_code < 300):
        snippet = _s(r.text)[:250]
        _log(f"❌ Duffel Links session error {r.status_code}: {snippet}")
        return None

    try:
        data = r.json() or {}
        return (data.get("data") or {}).get("url") or None
    except Exception:
        return None


# -------------------- MAIN --------------------

def main() -> int:
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")

    _log("============================================================")
    _log("🚀 TravelTxter Link Router V4.7.2 (TravelUp primary + fallback + Duffel last resort)")
    _log("============================================================")
    _log(f"RAW_DEALS_TAB={RAW_DEALS_TAB}")
    _log(f"TRAVELUP_ENABLED={TRAVELUP_ENABLED}")
    _log(f"TRAVELUP_CJ_CLICK_BASE={'set' if TRAVELUP_CJ_CLICK_BASE else 'missing'}")
    _log(f"FALLBACK_STRATEGY={FALLBACK_STRATEGY}")
    _log(f"DUFFEL_LINKS_ENABLED={DUFFEL_LINKS_ENABLED} (last resort)")
    _log(f"MAX_ROWS_PER_RUN={MAX_ROWS_PER_RUN}")

    gc = _gs_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(RAW_DEALS_TAB)

    headers = _headers(ws)
    _validate_headers(headers)
    cm = _colmap(headers)
    has_destination_city = "destination_city" in cm

    _log("📥 Loading RAW_DEALS...")
    import time
    t_start = time.time()
    records = _get_records(ws)
    elapsed = time.time() - t_start
    _log(f"✅ RAW_DEALS loaded: {len(records)} rows ({elapsed:.1f}s)")
    
    updates: List[Tuple[int, Dict[str, Any]]] = []

    attempted = 0
    used_travelup = 0
    used_fallback = 0
    used_duffel_last_resort = 0
    travelup_skipped_no_city = 0

    for i, r in enumerate(records, start=2):
        if len(updates) >= MAX_ROWS_PER_RUN:
            break

        status = _s(r.get("status")).upper()
        if status not in ELIGIBLE_STATUSES:
            continue

        if _s(r.get("booking_link_vip")):
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

        # 1) PRIMARY: TravelUp (CJ) – requires destination_city
        destination_city = _s(r.get("destination_city")) if has_destination_city else ""
        link = ""
        if TRAVELUP_ENABLED:
            if destination_city:
                link = _create_travelup_link(d, destination_city, deal_id)
                if link:
                    used_travelup += 1
                    _log(f"💷 TravelUp (CJ): {deal_id} | {o}→{d} | {destination_city}")
            else:
                travelup_skipped_no_city += 1

        # 2) FALLBACK: Google / Skyscanner / Homepage
        if not link:
            link = _create_fallback_link(deal_id, o, d, out_iso, in_iso, price)
            used_fallback += 1
            _log(f"🔗 Fallback ({FALLBACK_STRATEGY}): {deal_id} | {o}→{d}")

        # 3) LAST RESORT: Duffel Links (only if enabled AND fallback somehow failed)
        if not link:
            dl = _create_duffel_links_session(o, d, out_iso, in_iso, deal_id)
            if dl:
                link = dl
                used_duffel_last_resort += 1
                _log(f"🆘 Duffel Links (last resort): {deal_id} | {o}→{d}")

        if link:
            updates.append((i, {"booking_link_vip": link}))

    written_cells = _batch_write(ws, updates, cm)

    _log(f"Attempted link generations: {attempted}")
    _log(f"booking_link_vip populated for: {len(updates)} rows")
    _log(f"TravelUp (CJ) links used: {used_travelup}")
    _log(f"Fallback links used: {used_fallback} (strategy: {FALLBACK_STRATEGY})")
    if TRAVELUP_ENABLED and not has_destination_city:
        _log("⚠️ destination_city column not found in RAW_DEALS – TravelUp link generation will be skipped.")
    _log(f"TravelUp skipped due to missing destination_city: {travelup_skipped_no_city}")
    _log(f"Duffel Links last-resort used: {used_duffel_last_resort}")
    _log(f"Cells written (batch): {written_cells}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
