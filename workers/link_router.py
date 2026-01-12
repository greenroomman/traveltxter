# workers/link_router.py
import os
import json
import requests
from datetime import datetime, timezone

import gspread
from gspread.cell import Cell
from google.oauth2.service_account import Credentials


RAW_DEALS_TAB = os.environ.get("RAW_DEALS_TAB", "RAW_DEALS")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
GCP_SA_JSON_ONE_LINE = os.environ.get("GCP_SA_JSON_ONE_LINE")
DUFFEL_API_KEY = os.environ.get("DUFFEL_API_KEY")
REDIRECT_BASE_URL = os.environ.get("REDIRECT_BASE_URL")  # required in your env already :contentReference[oaicite:1]{index=1}

NOW = datetime.now(timezone.utc)

DUFFEL_LINKS_ENDPOINT = "https://api.duffel.com/air/links"

HEADERS = {
    "Authorization": f"Bearer {DUFFEL_API_KEY}",
    "Duffel-Version": "v2",
    "Content-Type": "application/json",
}


def _log(msg: str) -> None:
    print(f"{NOW.strftime('%Y-%m-%dT%H:%M:%SZ')} | {msg}")


def _get_gspread_client():
    if not GCP_SA_JSON_ONE_LINE:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE")
    sa = json.loads(GCP_SA_JSON_ONE_LINE)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(sa, scopes=scopes)
    return gspread.authorize(creds)


def _open_ws(gc, tab_name: str):
    sh = gc.open_by_key(SPREADSHEET_ID)
    return sh.worksheet(tab_name)


def _headers(ws):
    return [h.strip() for h in ws.row_values(1)]


def _col_idx_map(headers):
    return {h: i + 1 for i, h in enumerate(headers) if h}


def _stringify(v):
    if v is None:
        return ""
    return str(v).strip()


def _date_only(v):
    """
    Duffel needs YYYY-MM-DD. Accepts:
      - YYYY-MM-DD
      - ddmmyy / ddmmyyyy not accepted (we do not invent)
    """
    s = _stringify(v)
    # Already ISO date
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s
    return ""


def _create_duffel_link(origin_iata, dest_iata, out_date, in_date):
    """
    Creates a Duffel Links URL (demo booking link).
    Adds a redirect URL for tracking / future routing (required by your setup).
    """
    if not (origin_iata and dest_iata and out_date and in_date):
        return None

    payload = {
        "data": {
            "type": "air_links",
            "slices": [
                {
                    "origin": origin_iata,
                    "destination": dest_iata,
                    "departure_date": out_date,
                },
                {
                    "origin": dest_iata,
                    "destination": origin_iata,
                    "departure_date": in_date,
                },
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": "economy",
            "metadata": {
                "source": "traveltxter_vip_demo",
            },
        }
    }

    # If you have a redirect base, attach a meaningful redirect URL
    if REDIRECT_BASE_URL:
        # Keep it simple and deterministic (no schema invention)
        payload["data"]["redirect_url"] = REDIRECT_BASE_URL

    try:
        r = requests.post(
            DUFFEL_LINKS_ENDPOINT,
            headers=HEADERS,
            json=payload,
            timeout=20,
        )
    except Exception as e:
        _log(f"Duffel Links request failed: {e}")
        return None

    if r.status_code != 201:
        try:
            _log(f"Duffel Links error {r.status_code}: {r.text[:300]}")
        except Exception:
            _log(f"Duffel Links error {r.status_code}")
        return None

    try:
        return r.json()["data"]["url"]
    except Exception:
        return None


def _batch_write_cells(ws, row_updates, colmap):
    """
    row_updates: list of (row_number, {header:value})
    Performs ONE batchUpdate call to avoid 429 quota.
    """
    cells = []
    for row_num, data in row_updates:
        for header, value in data.items():
            col = colmap.get(header)
            if not col:
                continue
            cells.append(Cell(row=row_num, col=col, value=value))

    if not cells:
        return 0

    ws.update_cells(cells, value_input_option="RAW")
    return len(cells)


def main():
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID")
    if not DUFFEL_API_KEY:
        raise RuntimeError("Missing DUFFEL_API_KEY")

    gc = _get_gspread_client()
    ws = _open_ws(gc, RAW_DEALS_TAB)

    headers = _headers(ws)
    colmap = _col_idx_map(headers)

    required_cols = ["status", "booking_link_vip"]
    for c in required_cols:
        if c not in colmap:
            raise RuntimeError(f"RAW_DEALS missing required column: {c}")

    # Try common column names for route/date data WITHOUT inventing schema:
    origin_key = None
    dest_key = None
    out_key = None
    in_key = None

    for c in ["origin_iata", "from_iata", "origin", "from_airport"]:
        if c in colmap:
            origin_key = c
            break
    for c in ["dest_iata", "to_iata", "destination", "to_airport"]:
        if c in colmap:
            dest_key = c
            break
    for c in ["out_date", "departure_date", "date_out", "depart_date"]:
        if c in colmap:
            out_key = c
            break
    for c in ["in_date", "return_date", "date_in", "returning_date"]:
        if c in colmap:
            in_key = c
            break

    if not all([origin_key, dest_key, out_key, in_key]):
        raise RuntimeError(
            "RAW_DEALS missing route/date columns needed for Duffel Links. "
            f"Found origin={origin_key} dest={dest_key} out={out_key} in={in_key}"
        )

    # Pull all records (sheet rows 2..n)
    records = ws.get_all_records()
    updates = []

    eligible_statuses = {"READY_TO_POST", "READY_TO_PUBLISH"}

    for idx, r in enumerate(records, start=2):
        status = _stringify(r.get("status"))
        if status not in eligible_statuses:
            continue

        existing_link = _stringify(r.get("booking_link_vip"))
        if existing_link:
            continue

        origin_iata = _stringify(r.get(origin_key)).upper()
        dest_iata = _stringify(r.get(dest_key)).upper()
        out_date = _date_only(r.get(out_key))
        in_date = _date_only(r.get(in_key))

        link = _create_duffel_link(origin_iata, dest_iata, out_date, in_date)
        if not link:
            continue

        updates.append((idx, {"booking_link_vip": link}))

    n = _batch_write_cells(ws, updates, colmap)
    _log(f"Done. booking_link_vip populated for {len(updates)} rows. cells written: {n}")


if __name__ == "__main__":
    main()
