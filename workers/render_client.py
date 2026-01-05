#!/usr/bin/env python3
"""
TravelTxter V4.5x — render_client.py (CLEAN REPLACEMENT)

Runs in GitHub Actions (NO PIL / Pillow). Calls PythonAnywhere render API.

Finds rows where:
  status == READY_TO_POST

Sends LOCKED payload (DO NOT CHANGE):
  TO:   <City>
  FROM: <City>
  OUT:  ddmmyy
  IN:   ddmmyy
  PRICE: £xxx   (rounded up)

Writes back to RAW_DEALS:
  graphic_url
  rendered_timestamp
  status -> READY_TO_PUBLISH
  render_error
  render_response_snippet

Column fallbacks (because your sheet currently has mixed schemas):
  FROM city: origin_city OR origin OR origin_iata
  TO city:   destination_city OR dest OR destination_iata
  OUT date:  outbound_date OR out_date
  IN date:   return_date OR ret_date
  PRICE:     price_gbp OR price

Also includes a small IATA->City map for common airports; unknown codes pass through.
"""

import os
import json
import time
import math
import re
import datetime as dt
from typing import Any, Dict, List

import requests
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError


# -----------------------------
# Logging
# -----------------------------
def now_utc() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def log(msg: str) -> None:
    print(f"{now_utc()} | {msg}", flush=True)


# -----------------------------
# Env
# -----------------------------
def env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def env_int(name: str, default: int) -> int:
    v = env(name, "")
    try:
        return int(v) if v else default
    except Exception:
        return default


# -----------------------------
# Sanitizers
# -----------------------------
def clean_text(v: Any) -> str:
    """
    Removes common "list-ish" artifacts like ['Málaga'] and trims whitespace.
    Keeps unicode letters (Málaga) intact.
    """
    s = "" if v is None else str(v).strip()
    # strip wrappers: ["X"], ['X'], [X]
    s = re.sub(r"^\[\s*['\"]?", "", s)
    s = re.sub(r"['\"]?\s*\]$", "", s)
    # collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


# Minimal IATA -> City mapping (extend anytime; unknown falls back to the code)
IATA_TO_CITY = {
    # London
    "LHR": "London",
    "LGW": "London",
    "STN": "London",
    "LTN": "London",
    "LCY": "London",
    "SEN": "London",
    # UK (common)
    "BRS": "Bristol",
    "MAN": "Manchester",
    "BHX": "Birmingham",
    "EDI": "Edinburgh",
    "GLA": "Glasgow",
    "NQY": "Newquay",
    "EXT": "Exeter",
    "SOU": "Southampton",
    "CWL": "Cardiff",
    # Popular destinations
    "BCN": "Barcelona",
    "AGP": "Málaga",
    "FAO": "Faro",
    "ALC": "Alicante",
    "PMI": "Palma",
    "TFS": "Tenerife",
    "LIS": "Lisbon",
    "OPO": "Porto",
    "AMS": "Amsterdam",
    "CDG": "Paris",
    "FCO": "Rome",
    "MXP": "Milan",
    "ATH": "Athens",
    "DUB": "Dublin",
    "KEF": "Reykjavík",
}


def iata_to_city(code_or_city: str) -> str:
    v = clean_text(code_or_city)
    if not v:
        return ""
    if len(v) == 3 and v.upper() == v:
        return IATA_TO_CITY.get(v.upper(), v)
    return v


# -----------------------------
# Formatting
# -----------------------------
def to_ddmmyy(date_iso: str) -> str:
    try:
        d = dt.date.fromisoformat(clean_text(date_iso))
        return d.strftime("%d%m%y")
    except Exception:
        return ""


def price_rounded_up_gbp(price_any: str) -> str:
    try:
        p = float(clean_text(price_any))
        return f"£{int(math.ceil(p))}"
    except Exception:
        return ""


# -----------------------------
# Sheets helpers
# -----------------------------
def get_client() -> gspread.Client:
    sa = env("GCP_SA_JSON_ONE_LINE")
    if not sa:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE")
    info = json.loads(sa)
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)


def ensure_cols(ws: gspread.Worksheet, cols: List[str]) -> Dict[str, int]:
    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("RAW_DEALS missing header row")
    changed = False
    for c in cols:
        if c not in headers:
            headers.append(c)
            changed = True
    if changed:
        ws.update([headers], "A1")
    return {h: i for i, h in enumerate(headers)}


def col_letter(n1: int) -> str:
    s = ""
    n = n1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def a1(row: int, col0: int) -> str:
    return f"{col_letter(col0 + 1)}{row}"


def batch_update(ws: gspread.Worksheet, data: List[Dict[str, Any]], tries: int = 6) -> None:
    delay = 1.0
    for attempt in range(1, tries + 1):
        try:
            ws.batch_update(data)
            return
        except APIError as e:
            msg = str(e)
            if "429" in msg or "Quota exceeded" in msg:
                log(f"⚠️ Sheets quota 429. Backoff {delay:.1f}s (attempt {attempt}/{tries})")
                time.sleep(delay)
                delay = min(delay * 2, 20.0)
                continue
            raise


def get_first(row: List[str], hm: Dict[str, int], *cols: str) -> str:
    for c in cols:
        idx = hm.get(c, -1)
        if idx >= 0 and idx < len(row):
            v = clean_text(row[idx])
            if v and v.lower() != "nan":
                return v
    return ""


# -----------------------------
# Main
# -----------------------------
def main() -> int:
    spreadsheet_id = env("SPREADSHEET_ID")
    tab = env("RAW_DEALS_TAB", "RAW_DEALS")
    render_url = env("RENDER_URL")
    max_rows = env_int("RENDER_MAX_ROWS", 1)

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")
    if not render_url:
        raise RuntimeError("Missing RENDER_URL")

    log("============================================================")
    log("🎨 Render client starting (GitHub Actions HTTP client)")
    log(f"RENDER_URL={render_url}")
    log(f"MAX_ROWS={max_rows}")
    log("============================================================")

    gc = get_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(tab)

    # Ensure columns exist (safe to append if missing)
    hm = ensure_cols(ws, [
        "status",
        "origin_city", "destination_city",
        "origin_iata", "destination_iata",
        "origin", "dest",
        "outbound_date", "return_date",
        "out_date", "ret_date",
        "price_gbp", "price",
        "graphic_url",
        "rendered_timestamp",
        "render_error",
        "render_response_snippet",
    ])

    values = ws.get_all_values()
    if len(values) <= 1:
        log("No rows in sheet.")
        return 0

    rows = values[1:]
    rendered = 0

    for sheet_row, row in enumerate(rows, start=2):
        if rendered >= max_rows:
            break

        status = get_first(row, hm, "status").upper()
        if status != "READY_TO_POST":
            continue

        # Pull best available fields (your sheet currently often uses origin/dest/out_date/ret_date/price)
        from_raw = get_first(row, hm, "origin_city", "origin", "origin_iata")
        to_raw = get_first(row, hm, "destination_city", "dest", "destination_iata")
        out_iso = get_first(row, hm, "outbound_date", "out_date")
        in_iso = get_first(row, hm, "return_date", "ret_date")
        price_any = get_first(row, hm, "price_gbp", "price")

        from_city = iata_to_city(from_raw)
        to_city = iata_to_city(to_raw)
        out_ddmmyy = to_ddmmyy(out_iso)
        in_ddmmyy = to_ddmmyy(in_iso)
        price_disp = price_rounded_up_gbp(price_any)

        missing = []
        if not to_city: missing.append("TO")
        if not from_city: missing.append("FROM")
        if not out_ddmmyy: missing.append("OUT")
        if not in_ddmmyy: missing.append("IN")
        if not price_disp: missing.append("PRICE")

        if missing:
            msg = f"Missing fields for render payload: {missing}"
            log(f"❌ Row {sheet_row}: {msg}")
            batch_update(ws, [
                {"range": a1(sheet_row, hm["render_error"]), "values": [[msg]]},
                {"range": a1(sheet_row, hm["render_response_snippet"]), "values": [[""]]},
            ])
            continue

        payload = {
            "TO": clean_text(to_city),
            "FROM": clean_text(from_city),
            "OUT": out_ddmmyy,
            "IN": in_ddmmyy,
            "PRICE": price_disp,
        }

        log(f"🎨 Rendering row {sheet_row} → {payload}")

        try:
            r = requests.post(render_url, json=payload, timeout=45)
        except Exception as e:
            msg = f"Render request failed: {e}"
            log(f"❌ {msg}")
            batch_update(ws, [
                {"range": a1(sheet_row, hm["render_error"]), "values": [[msg]]},
                {"range": a1(sheet_row, hm["render_response_snippet"]), "values": [[""]]},
            ])
            continue

        snippet = clean_text((r.text or "")[:220]).replace("\n", " ")
        if r.status_code != 200:
            msg = f"Render HTTP {r.status_code}"
            log(f"❌ {msg} :: {snippet}")
            batch_update(ws, [
                {"range": a1(sheet_row, hm["render_error"]), "values": [[msg]]},
                {"range": a1(sheet_row, hm["render_response_snippet"]), "values": [[snippet]]},
            ])
            continue

        try:
            j = r.json()
        except Exception:
            msg = "Render returned non-JSON"
            log(f"❌ {msg} :: {snippet}")
            batch_update(ws, [
                {"range": a1(sheet_row, hm["render_error"]), "values": [[msg]]},
                {"range": a1(sheet_row, hm["render_response_snippet"]), "values": [[snippet]]},
            ])
            continue

        image_url = clean_text(j.get("image_url") or j.get("graphic_url") or "")
        if not image_url:
            msg = "Render returned no image_url"
            log(f"❌ {msg} :: {snippet}")
            batch_update(ws, [
                {"range": a1(sheet_row, hm["render_error"]), "values": [[msg]]},
                {"range": a1(sheet_row, hm["render_response_snippet"]), "values": [[snippet]]},
            ])
            continue

        # Success: write URL + promote
        batch_update(ws, [
            {"range": a1(sheet_row, hm["graphic_url"]), "values": [[image_url]]},
            {"range": a1(sheet_row, hm["rendered_timestamp"]), "values": [[now_utc()]]},
            {"range": a1(sheet_row, hm["render_error"]), "values": [[""]]},
            {"range": a1(sheet_row, hm["render_response_snippet"]), "values": [[snippet]]},
            {"range": a1(sheet_row, hm["status"]), "values": [["READY_TO_PUBLISH"]]},
        ])

        rendered += 1
        log(f"✅ Rendered row {sheet_row} → {image_url} (READY_TO_PUBLISH)")

    log(f"Done. Rendered {rendered}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
