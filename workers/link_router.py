# workers/link_router.py
"""
TravelTxter Link Router (V4.6 compatible) — Duffel Links DEMO + SAFE FALLBACK

Purpose (DO NOT CHANGE PIPELINE):
- Populate booking_link_vip for monetisable rows.
- Primary: try Duffel Links (demo booking links).
- Fallback: if Duffel Links fails (incl 404), write a deterministic DEMO link so VIP posts never have dead/no links.
- DO NOT touch rendering or publishing.
- DO NOT depend on any local utils/ package.
- Batch-write updates to avoid Google Sheets 429 write quota errors.

Eligibility:
- status in {"READY_TO_POST", "READY_TO_PUBLISH"}
- booking_link_vip is blank (or missing)
- origin_iata, destination_iata, outbound_date, return_date present

Environment:
- SPREADSHEET_ID or SHEET_ID (required)
- GCP_SA_JSON_ONE_LINE or GCP_SA_JSON (required)
- RAW_DEALS_TAB (default "RAW_DEALS")

Duffel:
- DUFFEL_API_KEY (optional but recommended; if missing we go straight to demo links)
- DUFFEL_API_BASE (default "https://api.duffel.com")
- DUFFEL_VERSION (default "v2")  # must be v2
- REDIRECT_BASE_URL (optional; used as Duffel link redirect_url if set)

Demo fallback:
- DEMO_BASE_URL (optional). If not set, uses REDIRECT_BASE_URL if present, else https://traveltxter.co.uk/deal
- The demo link is honest and deterministic (query params).
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

# ✅ Phase 2+ hardening: shared schema validation
# (SheetContract must live in workers/sheet_contract.py)
from sheet_contract import SheetContract


# -------------------- ENV --------------------

RAW_DEALS_TAB = os.environ.get("RAW_DEALS_TAB", "RAW_DEALS").strip() or "RAW_DEALS"
SPREADSHEET_ID = (os.environ.get("SPREADSHEET_ID") or os.environ.get("SHEET_ID") or "").strip()
GCP_SA_JSON_ONE_LINE = (os.environ.get("GCP_SA_JSON_ONE_LINE") or os.environ.get("GCP_SA_JSON") or "").strip()

DUFFEL_API_KEY = (os.environ.get("DUFFEL_API_KEY") or "").strip()
DUFFEL_API_BASE = (os.environ.get("DUFFEL_API_BASE") or "https://api.duffel.com").strip().rstrip("/")
DUFFEL_VERSION = (os.environ.get("DUFFEL_VERSION") or "v2").strip()  # must be v2
REDIRECT_BASE_URL = (os.environ.get("REDIRECT_BASE_URL") or "").strip()

# ✅ Phase 2+ hardening: never allow empty base URL (prevents "https://?deal_id=...")
DEMO_BASE_URL = (os.environ.get("DEMO_BASE_URL") or "").strip()
if not DEMO_BASE_URL:
    DEMO_BASE_URL = (REDIRECT_BASE_URL or "").strip() or "https://traveltxter.co.uk/deal"

MAX_ROWS_PER_RUN = int(os.environ.get("LINK_ROUTER_MAX_ROWS_PER_RUN", "20") or "20")


# -------------------- LOGGING --------------------

def _log(msg: str) -> None:
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"{ts} | {msg}", flush=True)


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
    return [h.strip() for h in ws.row_values(1)]


def _colmap(headers: List[str]) -> Dict[str, int]:
    return {h: i + 1 for i, h in enumerate(headers) if h}


def _s(v: Any) -> str:
    return str(v or "").strip()


def _parse_date_to_iso(d: str) -> str:
    """
    Accepts: "YYYY-MM-DD" or "DD/MM/YYYY" (common in your sheet)
    Returns: "YYYY-MM-DD" or "" if cannot parse.
    """
    d = _s(d)
    if not d:
        return ""
    # Already ISO
    if len(d) == 10 and d[4] == "-" and d[7] == "-":
        return d
    # DD/MM/YYYY
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
    cells = []
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
        "Duffel-Version": DUFFEL_VERSION,  # must be v2
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

    # Optional redirect target (must be a real URL if set)
    if REDIRECT_BASE_URL:
        payload["data"]["redirect_url"] = REDIRECT_BASE_URL

    url = f"{DUFFEL_API_BASE}/air/links"

    try:
        r = requests.post(url, headers=_duffel_headers(), json=payload, timeout=25)
    except Exception as e:
        _log(f"❌ Duffel Links request exception: {e}")
        return None

    if not (200 <= r.status_code < 300):
        # Key behaviour: do NOT block pipeline; fall back to demo link
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

def _create_demo_link(deal_id: str, origin_iata: str, dest_iata: str, out_iso: str, in_iso: str, price_gbp: str) -> str:
    """
    Deterministic, honest demo link that always exists.
    This prevents 'no-link' VIP posts while Duffel Links is in demo mode.
    """
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
    base = (DEMO_BASE_URL or "").strip().rstrip("?") or "https://traveltxter.co.uk/deal"
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

    # ✅ Phase 2+ hardening: shared schema validation (fail loud)
    SheetContract.validate_schema(headers, required=[
        "status",
        "deal_id",
        "origin_iata",
        "destination_iata",
        "outbound_date",
        "return_date",
        "booking_link_vip",
    ])

    cm = _colmap(headers)

    # price column is optional, but used for demo query param
    price_col = "price_gbp" if "price_gbp" in cm else None

    rows = ws.get_all_records()
    if not rows:
        _log("No rows in RAW_DEALS. Exiting.")
        return 0

    eligible_status = {"READY_TO_POST", "READY_TO_PUBLISH"}
    attempted = 0
    created = 0
    used_demo = 0
    updates: List[Tuple[int, Dict[str, Any]]] = []

    for idx, r in enumerate(rows, start=2):  # sheet row numbers (header row is 1)
        if created >= MAX_ROWS_PER_RUN:
            break

        status = _s(r.get("status")).upper()
        if status not in eligible_status:
            continue

        if _s(r.get("booking_link_vip")):
            continue  # already has a link

        deal_id = _s(r.get("deal_id"))
        origin = _s(r.get("origin_iata")).upper()
        dest = _s(r.get("destination_iata")).upper()
        out_iso = _parse_date_to_iso(r.get("outbound_date"))
        in_iso = _parse_date_to_iso(r.get("return_date"))

        if not (origin and dest and out_iso and in_iso):
            continue

        attempted += 1

        link = _create_duffel_link(origin, dest, out_iso, in_iso)
        if not link:
            # Always fall back to demo link to prevent dead VIP posts
            price_val = _s(r.get(price_col)) if price_col else ""
            link = _create_demo_link(deal_id, origin, dest, out_iso, in_iso, price_val)
            used_demo += 1

        updates.append((idx, {"booking_link_vip": link}))
        created += 1

    cells_written = _batch_write(ws, updates, cm)

    _log(f"Attempted link generations: {attempted}")
    _log(f"booking_link_vip populated for: {created} rows")
    _log(f"Fallback demo links used: {used_demo}")
    _log(f"Cells written (batch): {cells_written}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
