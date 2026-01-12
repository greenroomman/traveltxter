# workers/link_router.py
"""
TravelTxter Link Router (V4.6 compatible) — Duffel Links DEMO

Purpose:
- Populate booking_link_vip for monetisable rows using Duffel Links (demo booking links).
- DO NOT touch rendering or publishing.
- DO NOT depend on any local utils/ package.
- Batch-write updates to avoid Google Sheets 429 write quota errors.

Assumptions (from your current RAW_DEALS schema):
- origin_iata
- destination_iata
- outbound_date   (DD/MM/YYYY)
- return_date     (DD/MM/YYYY)
- status
- booking_link_vip

Eligible statuses:
- READY_TO_POST
- READY_TO_PUBLISH
"""

import os
import json
import requests
from datetime import datetime, timezone

import gspread
from gspread.cell import Cell
from google.oauth2.service_account import Credentials


RAW_DEALS_TAB = os.environ.get("RAW_DEALS_TAB", "RAW_DEALS").strip() or "RAW_DEALS"
SPREADSHEET_ID = (os.environ.get("SPREADSHEET_ID") or os.environ.get("SHEET_ID") or "").strip()
GCP_SA_JSON_ONE_LINE = (os.environ.get("GCP_SA_JSON_ONE_LINE") or os.environ.get("GCP_SA_JSON") or "").strip()

DUFFEL_API_KEY = (os.environ.get("DUFFEL_API_KEY") or "").strip()
DUFFEL_API_BASE = (os.environ.get("DUFFEL_API_BASE") or "https://api.duffel.com").strip()
DUFFEL_VERSION = (os.environ.get("DUFFEL_VERSION") or "v2").strip() or "v2"

# Optional but supported in your stack; if present, Duffel will redirect after search.
REDIRECT_BASE_URL = (os.environ.get("REDIRECT_BASE_URL") or "").strip()

NOW = datetime.now(timezone.utc)

ELIGIBLE_STATUSES = {"READY_TO_POST", "READY_TO_PUBLISH"}


def _log(msg: str) -> None:
    ts = NOW.strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"{ts} | {msg}", flush=True)


def _parse_sa_json(raw: str) -> dict:
    raw = (raw or "").strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # tolerate escaped newlines
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


def _headers(ws) -> list:
    return [h.strip() for h in ws.row_values(1)]


def _colmap(headers: list) -> dict:
    return {h: i + 1 for i, h in enumerate(headers) if h}


def _s(v) -> str:
    return (str(v or "").strip())


def _iata(v) -> str:
    return _s(v).upper()[:3]


def _ddmmyyyy_to_iso(v) -> str:
    """
    Accepts:
      - DD/MM/YYYY
      - YYYY-MM-DD (passes through)
    Returns:
      - YYYY-MM-DD or ""
    """
    s = _s(v)
    if not s:
        return ""
    # ISO passthrough
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s
    # DD/MM/YYYY
    if len(s) == 10 and s[2] == "/" and s[5] == "/":
        dd = s[0:2]
        mm = s[3:5]
        yyyy = s[6:10]
        if dd.isdigit() and mm.isdigit() and yyyy.isdigit():
            return f"{yyyy}-{mm}-{dd}"
    return ""


def _duffel_headers() -> dict:
    if not DUFFEL_API_KEY:
        raise RuntimeError("Missing DUFFEL_API_KEY")
    return {
        "Authorization": f"Bearer {DUFFEL_API_KEY}",
        "Duffel-Version": DUFFEL_VERSION,
        "Content-Type": "application/json",
    }


def _create_duffel_link(origin_iata: str, dest_iata: str, out_iso: str, in_iso: str) -> str | None:
    """
    Creates a Duffel Links URL (demo booking link).
    """
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

    if r.status_code != 201:
        _log(f"❌ Duffel Links error {r.status_code}: {r.text[:300]}")
        return None

    try:
        return r.json()["data"]["url"]
    except Exception:
        return None


def _batch_write(ws, updates: list[tuple[int, dict]], cm: dict) -> int:
    """
    updates: [(row_number, {"booking_link_vip": "..."}), ...]
    Performs a single update_cells call to avoid 429 quota.
    """
    cells: list[Cell] = []
    for row_num, data in updates:
        for header, value in data.items():
            col = cm.get(header)
            if not col:
                continue
            cells.append(Cell(row=row_num, col=col, value=value))

    if not cells:
        return 0

    ws.update_cells(cells, value_input_option="RAW")
    return len(cells)


def main() -> int:
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")

    gc = _gs_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(RAW_DEALS_TAB)

    headers = _headers(ws)
    cm = _colmap(headers)

    required = ["status", "booking_link_vip", "origin_iata", "destination_iata", "outbound_date", "return_date"]
    missing = [c for c in required if c not in cm]
    if missing:
        raise RuntimeError(f"RAW_DEALS missing required columns: {missing}")

    records = ws.get_all_records()

    _log(f"Link router scanning rows: {len(records)}")
    updates: list[tuple[int, dict]] = []
    attempted = 0
    created = 0

    for idx, r in enumerate(records, start=2):  # sheet row number
        status = _s(r.get("status"))
        if status not in ELIGIBLE_STATUSES:
            continue

        existing = _s(r.get("booking_link_vip"))
        if existing:
            continue

        origin = _iata(r.get("origin_iata"))
        dest = _iata(r.get("destination_iata"))
        out_iso = _ddmmyyyy_to_iso(r.get("outbound_date"))
        in_iso = _ddmmyyyy_to_iso(r.get("return_date"))

        if not (origin and dest and out_iso and in_iso):
            continue

        attempted += 1
        link = _create_duffel_link(origin, dest, out_iso, in_iso)
        if not link:
            continue

        updates.append((idx, {"booking_link_vip": link}))
        created += 1

    cells_written = _batch_write(ws, updates, cm)

    _log(f"Attempted Duffel Links: {attempted}")
    _log(f"booking_link_vip populated for: {created} rows")
    _log(f"Cells written (batch): {cells_written}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
