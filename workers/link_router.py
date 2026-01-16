#!/usr/bin/env python3
# workers/link_router.py
"""
TravelTxter Link Router (V4.6 compatible) — Duffel Links + Demo fallback

Purpose (LOCKED):
- Populate booking_link_vip for monetisable rows.
- Primary: try Duffel Links (if available).
- Fallback: deterministic DEMO link so VIP posts never have dead links.
- DO NOT touch rendering or publishing.
- Stateless: Sheets is the only memory.

Eligibility:
- status in {"READY_TO_POST", "READY_TO_PUBLISH"}
- booking_link_vip is blank
- origin_iata, destination_iata, outbound_date, return_date present

Demo base:
- DEMO_BASE_URL if set, else REDIRECT_BASE_URL, else http://www.traveltxter.com/
- Landing page is homepage with query params: http://www.traveltxter.com/?deal_id=...

Duffel:
- DUFFEL_API_KEY optional; if missing we go straight to demo links
- DUFFEL_API_BASE default https://api.duffel.com
- DUFFEL_VERSION default v2 (must be v2)
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

REDIRECT_BASE_URL = (os.environ.get("REDIRECT_BASE_URL") or "").strip()

# Canonical homepage base (your instruction)
DEFAULT_HOME_BASE = "http://www.traveltxter.com/"

DEMO_BASE_URL = (os.environ.get("DEMO_BASE_URL") or "").strip()
if not DEMO_BASE_URL:
    DEMO_BASE_URL = (REDIRECT_BASE_URL or "").strip() or DEFAULT_HOME_BASE

MAX_ROWS_PER_RUN = int(os.environ.get("LINK_ROUTER_MAX_ROWS_PER_RUN", "20") or "20")


# -------------------- LOGGING --------------------

def _log(msg: str) -> None:
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"{ts} | {msg}", flush=True)


# -------------------- SCHEMA VALIDATION (self-contained) --------------------

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
        # common case: one-line env var with escaped newlines
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
    """
    updates = [(row_num, {"booking_link_vip": "..."})]
    Writes as few API calls as possible (single update_cells call).
    """
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


# -------------------- DUFFEL LINKS --------------------

def _duffel_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {DUFFEL_API_KEY}",
        "Content-Type": "application/json",
        "Duffel-Version": DUFFEL_VERSION,
        "Accept": "application/json",
    }


def _create_duffel_link(origin_iata: str, dest_iata: str, out_iso: str, in_iso: str) -> Optional[str]:
    """
    Try to create Duffel Links URL.
    If Duffel returns non-2xx (including 404), return None and let fallback handle it.
    """
    if not DUFFEL_API_KEY:
        return None
    if not (origin_iata and dest_iata and out_iso and in_iso):
        return None

    payload = {
        "data": {
            "type": "air_links",
            "slices": [
                {"origin": origin_iata, "destination": dest_iata, "departure_date": out_iso},
                {"origin": dest_iata, "destination": origin_iata, "departure_date": in_iso},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": "economy",
            "metadata": {"source": "traveltxter_vip_demo"},
        }
    }

    if REDIRECT_BASE_URL:
        payload["data"]["redirect_url"] = REDIRECT_BASE_URL

    url = f"{DUFFEL_API_BASE}/air/links"
    try:
        r = requests.post(url, headers=_duffel_headers(), json=payload, timeout=25)
    except Exception as e:
        _log(f"❌ Duffel Links request exception: {e}")
        return None

    if not (200 <= r.status_code < 300):
        snippet = _s(r.text)[:250]
        _log(f"❌ Duffel Links error {r.status_code}: {snippet}")
        return None

    try:
        data = r.json()
        link = data.get("data", {}).get("url") or data.get("data", {}).get("href")
        if link:
            return link
    except Exception:
        pass

    _log("❌ Duffel Links response missing URL; falling back to demo link")
    return None


# -------------------- DEMO FALLBACK --------------------

def _normalize_base(base: str) -> str:
    # Ensure we don't end up with "http://.../??a=b"
    b = (base or "").strip()
    if not b:
        b = DEFAULT_HOME_BASE
    return b.rstrip()  # do not strip trailing slash; homepage is OK


def _create_demo_link(deal_id: str, origin_iata: str, dest_iata: str, out_iso: str, in_iso: str, price_gbp: str) -> str:
    params = {
        "deal_id": _s(deal_id),
        "from": _s(origin_iata).upper(),
        "to": _s(dest_iata).upper(),
        "out": _s(out_iso),
        "in": _s(in_iso),
        "price": _s(price_gbp).replace("£", "").strip(),
        "src": "vip_demo",
    }
    qs = urlencode({k: v for k, v in params.items() if v})
    base = _normalize_base(DEMO_BASE_URL)
    if "?" in base:
        return f"{base}&{qs}"
    return f"{base}?{qs}"


# -------------------- MAIN --------------------

def main() -> int:
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")

    _log("============================================================")
    _log("🚀 TravelTxter Link Router starting (Duffel Links + Demo fallback)")
    _log("============================================================")
    _log(f"RAW_DEALS_TAB={RAW_DEALS_TAB}")
    _log(f"DUFFEL_API_BASE={DUFFEL_API_BASE} DUFFEL_VERSION={DUFFEL_VERSION} DUFFEL_API_KEY={'set' if DUFFEL_API_KEY else 'missing'}")
    _log(f"DEMO_BASE_URL={DEMO_BASE_URL}")
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
    used_fallback = 0

    # get_all_records() indexes data rows starting at row 2
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

        link = _create_duffel_link(o, d, out_iso, in_iso)
        if not link:
            link = _create_demo_link(deal_id, o, d, out_iso, in_iso, price)
            used_fallback += 1

        updates.append((i, {"booking_link_vip": link}))

    written_cells = _batch_write(ws, updates, cm)

    _log(f"Attempted link generations: {attempted}")
    _log(f"booking_link_vip populated for: {len(updates)} rows")
    _log(f"Fallback demo links used: {used_fallback}")
    _log(f"Cells written (batch): {written_cells}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
