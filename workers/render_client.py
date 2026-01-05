#!/usr/bin/env python3
"""
TravelTxter V4.5x — Render Client Worker (GitHub Actions)

Purpose:
- Convert READY_TO_POST -> READY_TO_PUBLISH by calling PythonAnywhere render API
- Writes graphic_url + rendered_timestamp

IMPORTANT:
- This is an HTTP client only. No PIL / Pillow. No local image rendering.

Env required:
- SPREADSHEET_ID
- RAW_DEALS_TAB (default: RAW_DEALS)
- GCP_SA_JSON_ONE_LINE
- RENDER_URL  (e.g. https://<user>.pythonanywhere.com/api/render)

Locked payload contract (DO NOT CHANGE):
TO: <City>
FROM: <City>
OUT: ddmmyy
IN: ddmmyy
PRICE: £xxx  (rounded up)

Sheet columns used/written (created if missing):
- status
- origin_city
- destination_city
- outbound_date
- return_date
- price_gbp
- graphic_url
- rendered_timestamp
- render_error
- render_response_snippet
"""

import os
import json
import time
import math
import datetime as dt
from typing import Any, Dict, List

import requests
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError


# -------------------------
# Logging
# -------------------------
def now_utc() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def log(msg: str) -> None:
    print(f"{now_utc()} | {msg}", flush=True)


# -------------------------
# Env helpers
# -------------------------
def env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def env_int(name: str, default: int) -> int:
    v = env(name, "")
    try:
        return int(v) if v else default
    except Exception:
        return default


# -------------------------
# Sheets helpers
# -------------------------
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


# -------------------------
# Data formatting
# -------------------------
def to_ddmmyy(date_iso: str) -> str:
    # date_iso = YYYY-MM-DD
    try:
        d = dt.date.fromisoformat(date_iso)
        return d.strftime("%d%m%y")
    except Exception:
        return ""


def money_round_up(price_gbp: str) -> str:
    # PRICE must be "£xxx" rounded up
    try:
        p = float(str(price_gbp).strip())
        return f"£{int(math.ceil(p))}"
    except Exception:
        return ""


def main() -> int:
    spreadsheet_id = env("SPREADSHEET_ID")
    tab = env("RAW_DEALS_TAB", "RAW_DEALS")
    render_url = env("RENDER_URL")
    max_rows = env_int("RENDER_MAX_ROWS", 3)

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")
    if not render_url:
        raise RuntimeError("Missing RENDER_URL")

    log("============================================================")
    log("🎨 Render client starting")
    log(f"RENDER_URL={render_url}")
    log(f"MAX_ROWS={max_rows}")
    log("============================================================")

    gc = get_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(tab)

    hm = ensure_cols(ws, [
        "status",
        "origin_city",
        "destination_city",
        "outbound_date",
        "return_date",
        "price_gbp",
        "graphic_url",
        "rendered_timestamp",
        "render_error",
        "render_response_snippet",
    ])

    values = ws.get_all_values()
    if len(values) <= 1:
        log("No rows.")
        return 0

    data = values[1:]

    def get(row: List[str], col: str) -> str:
        i = hm.get(col, -1)
        return row[i].strip() if i >= 0 and i < len(row) else ""

    rendered = 0

    for sheet_row, row in enumerate(data, start=2):
        if rendered >= max_rows:
            break

        status = get(row, "status").upper()
        if status != "READY_TO_POST":
            continue

        to_city = get(row, "destination_city")
        from_city = get(row, "origin_city")
        out_iso = get(row, "outbound_date")
        in_iso = get(row, "return_date")
        price = money_round_up(get(row, "price_gbp"))

        # Must have all required fields for locked payload
        missing = []
        if not to_city: missing.append("destination_city")
        if not from_city: missing.append("origin_city")
        if not out_iso: missing.append("outbound_date")
        if not in_iso: missing.append("return_date")
        if not price: missing.append("price_gbp")

        if missing:
            msg = f"Missing fields for render: {missing}"
            log(f"❌ Row {sheet_row}: {msg}")
            batch_update(ws, [
                {"range": a1(sheet_row, hm["render_error"]), "values": [[msg]]},
                {"range": a1(sheet_row, hm["render_response_snippet"]), "values": [[""]]},
            ])
            continue

        payload = {
            "TO": to_city,
            "FROM": from_city,
            "OUT": to_ddmmyy(out_iso),
            "IN": to_ddmmyy(in_iso),
            "PRICE": price,
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

        snippet = (r.text or "")[:180].replace("\n", " ")
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

        image_url = j.get("image_url") or j.get("graphic_url") or ""
        if not image_url:
            msg = "Render returned no image_url"
            log(f"❌ {msg} :: {snippet}")
            batch_update(ws, [
                {"range": a1(sheet_row, hm["render_error"]), "values": [[msg]]},
                {"range": a1(sheet_row, hm["render_response_snippet"]), "values": [[snippet]]},
            ])
            continue

        # Success: write graphic_url + promote status
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
