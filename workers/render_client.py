#!/usr/bin/env python3
"""
TravelTxter V4.5x — render_client.py (LOCKED)

ROLE:
- Finds ONE row with status == READY_TO_POST
- Sends strict render payload to PythonAnywhere (HTTP only)
- Writes graphic_url + rendered_timestamp
- Promotes status -> READY_TO_PUBLISH

Rendering contract (NON-NEGOTIABLE):
{
  "TO": "Barcelona",
  "FROM": "London",
  "OUT": "030226",
  "IN": "080226",
  "PRICE": "£89",
  "DEAL_ID": "<deal_id>"
}

No IATA codes, no emojis, no themes.
Hardened Google SA JSON parsing + backoff for Sheets 429.
"""

from __future__ import annotations

import os
import json
import time
import datetime as dt
from typing import Dict, Any, List, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError


# ============================================================
# Logging
# ============================================================

def ts() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def log(msg: str) -> None:
    print(f"{ts()} | {msg}", flush=True)


# ============================================================
# Env helpers
# ============================================================

def env_str(k: str, default: str = "") -> str:
    return os.environ.get(k, default).strip()

def env_int(k: str, default: int) -> int:
    try:
        return int(env_str(k, str(default)))
    except Exception:
        return default


# ============================================================
# Robust SA JSON parsing
# ============================================================

def _extract_json_object(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()

    try:
        return json.loads(raw)
    except Exception:
        pass

    try:
        return json.loads(raw.replace("\\n", "\n"))
    except Exception:
        pass

    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise RuntimeError("Invalid GCP_SA_JSON_ONE_LINE: no JSON object found")

    candidate = raw[start:end + 1]

    try:
        return json.loads(candidate)
    except Exception:
        pass

    try:
        return json.loads(candidate.replace("\\n", "\n"))
    except Exception as e:
        raise RuntimeError("Invalid GCP_SA_JSON_ONE_LINE: JSON parse failed") from e


def get_client() -> gspread.Client:
    sa = env_str("GCP_SA_JSON_ONE_LINE") or env_str("GCP_SA_JSON")
    if not sa:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")

    info = _extract_json_object(sa)

    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds)


def open_sheet_with_backoff(gc: gspread.Client, spreadsheet_id: str, attempts: int = 8) -> gspread.Spreadsheet:
    delay = 4.0
    for i in range(1, attempts + 1):
        try:
            return gc.open_by_key(spreadsheet_id)
        except APIError as e:
            msg = str(e)
            if "429" in msg or "Quota exceeded" in msg:
                log(f"⏳ Sheets quota (429). Retry {i}/{attempts} in {int(delay)}s...")
                time.sleep(delay)
                delay = min(delay * 1.6, 45.0)
                continue
            raise
    raise RuntimeError("Sheets quota still exceeded after retries (429). Try again shortly.")


# ============================================================
# A1 helpers
# ============================================================

def col_letter(n1: int) -> str:
    s = ""
    n = n1
    while n:
        n, rr = divmod(n - 1, 26)
        s = chr(65 + rr) + s
    return s

def a1(rownum: int, col0: int) -> str:
    return f"{col_letter(col0 + 1)}{rownum}"


# ============================================================
# Sheet helpers
# ============================================================

def ensure_columns(ws: gspread.Worksheet, headers: List[str], required: List[str]) -> List[str]:
    missing = [c for c in required if c not in headers]
    if not missing:
        return headers
    ws.update([headers + missing], "A1")
    log(f"🛠️  Added missing columns: {missing}")
    return headers + missing

def safe_get(row: List[str], idx: int) -> str:
    return row[idx].strip() if 0 <= idx < len(row) else ""


# ============================================================
# Formatting helpers
# ============================================================

def ddmmyy(date_str: str) -> str:
    """
    Accepts:
      - 2026-03-01
      - 01/03/2026
      - 2026/03/01
    Returns:
      - 010326
    """
    s = (date_str or "").strip()
    if not s:
        return ""

    # ISO YYYY-MM-DD
    try:
        d = dt.datetime.strptime(s, "%Y-%m-%d").date()
        return d.strftime("%d%m%y")
    except Exception:
        pass

    # DD/MM/YYYY
    try:
        d = dt.datetime.strptime(s, "%d/%m/%Y").date()
        return d.strftime("%d%m%y")
    except Exception:
        pass

    # YYYY/MM/DD
    try:
        d = dt.datetime.strptime(s, "%Y/%m/%d").date()
        return d.strftime("%d%m%y")
    except Exception:
        pass

    return s.replace("-", "").replace("/", "")[:6]


def money_gbp(price_gbp: str) -> str:
    s = (price_gbp or "").strip().replace("£", "")
    try:
        v = float(s)
        return f"£{int(v + 0.999)}"  # round up
    except Exception:
        return f"£{s}" if s else "£0"


# ============================================================
# Main
# ============================================================

def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    render_url = env_str("RENDER_URL")
    max_rows = env_int("RENDER_MAX_ROWS", 1)

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")
    if not render_url:
        raise RuntimeError("Missing RENDER_URL")

    gc = get_client()
    sh = open_sheet_with_backoff(gc, spreadsheet_id)
    ws = sh.worksheet(tab)

    values = ws.get_all_values()
    if not values or len(values) < 2:
        log("No rows.")
        return 0

    headers = [h.strip() for h in values[0]]

    required_cols = [
        "status",
        "deal_id",
        "origin_city",
        "destination_city",
        "outbound_date",
        "return_date",
        "price_gbp",
        "graphic_url",
        "rendered_timestamp",
        "render_error",
        "render_response_snippet",
    ]
    headers = ensure_columns(ws, headers, required_cols)

    # Re-read once after header mutation
    values = ws.get_all_values()
    headers = [h.strip() for h in values[0]]
    rows = values[1:]
    h = {name: i for i, name in enumerate(headers)}

    rendered = 0

    for rownum, r in enumerate(rows, start=2):
        if rendered >= max_rows:
            break

        status = safe_get(r, h["status"]).upper()
        if status != "READY_TO_POST":
            continue

        deal_id = safe_get(r, h["deal_id"]) or f"row_{rownum}"
        to_city = safe_get(r, h["destination_city"])
        from_city = safe_get(r, h["origin_city"])
        out_date = ddmmyy(safe_get(r, h["outbound_date"]))
        in_date = ddmmyy(safe_get(r, h["return_date"]))
        price = money_gbp(safe_get(r, h["price_gbp"]))

        payload = {
            "TO": to_city,
            "FROM": from_city,
            "OUT": out_date,
            "IN": in_date,
            "PRICE": price,
            "DEAL_ID": deal_id,
        }

        log(f"🎨 Rendering row {rownum} payload={payload}")

        try:
            resp = requests.post(render_url, json=payload, timeout=60)
            snippet = (resp.text or "")[:180].replace("\n", " ")
            if resp.status_code != 200:
                raise RuntimeError(f"Render HTTP {resp.status_code} :: {snippet}")

            j = resp.json()
            image_url = j.get("image_url") or j.get("graphic_url") or ""
            if not image_url:
                raise RuntimeError(f"No image_url in render response :: {snippet}")

            batch = [
                {"range": a1(rownum, h["graphic_url"]), "values": [[image_url]]},
                {"range": a1(rownum, h["rendered_timestamp"]), "values": [[ts()]]},
                {"range": a1(rownum, h["render_error"]), "values": [[""]]},
                {"range": a1(rownum, h["render_response_snippet"]), "values": [[snippet]]},
                {"range": a1(rownum, h["status"]), "values": [["READY_TO_PUBLISH"]]},
            ]
            ws.batch_update(batch, value_input_option="USER_ENTERED")

            rendered += 1
            log(f"✅ Rendered row {rownum} -> {image_url}")

        except Exception as e:
            err = str(e)[:220]
            batch = [
                {"range": a1(rownum, h["render_error"]), "values": [[err]]},
                {"range": a1(rownum, h["render_response_snippet"]), "values": [[err]]},
            ]
            ws.batch_update(batch, value_input_option="USER_ENTERED")
            log(f"❌ Render failed row {rownum}: {err}")

    log(f"Done. Rendered {rendered}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
