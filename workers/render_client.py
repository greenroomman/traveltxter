#!/usr/bin/env python3
"""
TravelTxter V4.5x — Render Client Worker (GitHub Actions)

Reads:
- RAW_DEALS rows with status == READY_TO_POST

Calls:
- PythonAnywhere RENDER_URL with locked payload:
    TO: <City>
    FROM: <City>
    OUT: ddmmyy
    IN: ddmmyy
    PRICE: £xxx  (rounded up)

Also includes OPTIONAL DEAL_ID (does not break contract):
    DEAL_ID: <deal_id>

Writes:
- graphic_url
- rendered_timestamp
- render_error
- render_response_snippet
- status -> READY_TO_PUBLISH

Important:
- GitHub never renders images locally.
- This file consults CONFIG_SIGNALS for proper city names.

Env required:
- SPREADSHEET_ID
- GCP_SA_JSON_ONE_LINE (or GCP_SA_JSON)
- RAW_DEALS_TAB (default RAW_DEALS)
- RENDER_URL

Env optional:
- RENDER_MAX_ROWS (default 1)
"""

from __future__ import annotations

import os
import json
import math
import datetime as dt
from typing import Dict, Any, List, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials

from lib.sheet_config import load_config_bundle, iata_signal_maps


# ============================================================
# Logging
# ============================================================

def utcnow() -> dt.datetime:
    return dt.datetime.utcnow()

def ts() -> str:
    return utcnow().replace(microsecond=0).isoformat() + "Z"

def log(msg: str) -> None:
    print(f"{ts()} | {msg}", flush=True)


# ============================================================
# Env
# ============================================================

def env_str(k: str, default: str = "") -> str:
    return os.environ.get(k, default).strip()

def env_int(k: str, default: int) -> int:
    try:
        return int(env_str(k, str(default)))
    except Exception:
        return default


# ============================================================
# Sheets auth + helpers
# ============================================================

def get_client() -> gspread.Client:
    sa = env_str("GCP_SA_JSON_ONE_LINE") or env_str("GCP_SA_JSON")
    if not sa:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE (recommended) or GCP_SA_JSON")
    try:
        info = json.loads(sa)
    except json.JSONDecodeError:
        info = json.loads(sa.replace("\\n", "\n"))

    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds)

def col_letter(n1: int) -> str:
    s = ""
    n = n1
    while n:
        n, rr = divmod(n - 1, 26)
        s = chr(65 + rr) + s
    return s

def a1(rownum: int, col0: int) -> str:
    return f"{col_letter(col0 + 1)}{rownum}"

def ensure_columns(ws: gspread.Worksheet, headers: List[str], required: List[str]) -> List[str]:
    missing = [c for c in required if c not in headers]
    if not missing:
        return headers
    new_headers = headers + missing
    ws.update([new_headers], "A1")
    log(f"🛠️  Added missing columns to header: {missing}")
    return new_headers

def batch_update(ws: gspread.Worksheet, updates: List[Dict[str, Any]]) -> None:
    if not updates:
        return
    ws.batch_update(updates)


# ============================================================
# Formatting
# ============================================================

def to_ddmmyy(iso_date: str) -> str:
    """
    Input: '2026-02-03' -> '030226'
    """
    s = (iso_date or "").strip()
    try:
        d = dt.date.fromisoformat(s[:10])
        return d.strftime("%d%m%y")
    except Exception:
        return ""

def price_to_pounds(price_any: Any) -> str:
    """
    Input: 89.12 or '89.12' -> '£90'
    """
    try:
        p = float(str(price_any).replace("£", "").strip())
        return f"£{int(math.ceil(p))}"
    except Exception:
        return ""

def looks_like_iata(s: str) -> bool:
    s = (s or "").strip().upper()
    return len(s) == 3 and s.isalpha()

def best_city(value_city: str, value_iata: str, iata_to_city: Dict[str, str]) -> str:
    """
    Prefer a real city if present.
    If city looks like IATA, replace from CONFIG_SIGNALS map.
    """
    c = (value_city or "").strip()
    i = (value_iata or "").strip().upper()

    if c and not looks_like_iata(c):
        return c
    if i and looks_like_iata(i):
        return iata_to_city.get(i, i)
    # last resort
    return c or i


def get_cell(row: List[str], hm: Dict[str, int], col: str) -> str:
    idx = hm.get(col)
    if idx is None:
        return ""
    return (row[idx] if idx < len(row) else "").strip()


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
    sh = gc.open_by_key(spreadsheet_id)

    # Load CONFIG_SIGNALS map for city names
    cfg = load_config_bundle(sh)
    sig_city, _sig_country = iata_signal_maps(cfg.signals)

    ws = sh.worksheet(tab)

    values = ws.get_all_values()
    if not values or len(values) < 2:
        log("No rows to render.")
        return 0

    headers = [h.strip() for h in values[0]]

    # Ensure output columns exist
    required_out = ["graphic_url", "rendered_timestamp", "render_error", "render_response_snippet", "status"]
    headers = ensure_columns(ws, headers, required_out)

    # Re-read after header update
    values = ws.get_all_values()
    headers = [h.strip() for h in values[0]]
    hm = {name: i for i, name in enumerate(headers)}
    rows = values[1:]

    # Required inputs (we'll read whatever exists)
    for c in ["status", "origin_city", "destination_city", "origin_iata", "destination_iata", "outbound_date", "return_date", "price_gbp"]:
        if c not in hm:
            raise RuntimeError(f"Missing required column in RAW_DEALS: {c}")

    rendered = 0

    for idx, row in enumerate(rows, start=2):
        if rendered >= max_rows:
            break

        status = get_cell(row, hm, "status").upper()
        if status != "READY_TO_POST":
            continue

        deal_id = get_cell(row, hm, "deal_id")  # may be blank on legacy rows

        origin_city = best_city(get_cell(row, hm, "origin_city"), get_cell(row, hm, "origin_iata"), sig_city)
        dest_city = best_city(get_cell(row, hm, "destination_city"), get_cell(row, hm, "destination_iata"), sig_city)

        out_iso = get_cell(row, hm, "outbound_date")
        in_iso = get_cell(row, hm, "return_date")
        price = price_to_pounds(get_cell(row, hm, "price_gbp"))

        payload = {
            # locked contract keys:
            "TO": dest_city,
            "FROM": origin_city,
            "OUT": to_ddmmyy(out_iso),
            "IN": to_ddmmyy(in_iso),
            "PRICE": price,
        }

        # Optional (helps prevent PythonAnywhere returning no_id.png)
        if deal_id:
            payload["DEAL_ID"] = deal_id

        log(f"🎨 Rendering row {idx} payload={payload}")

        try:
            r = requests.post(render_url, json=payload, timeout=45)
        except Exception as e:
            msg = f"Render request failed: {e}"
            log(f"❌ {msg}")
            batch_update(ws, [
                {"range": a1(idx, hm["render_error"]), "values": [[msg]]},
                {"range": a1(idx, hm["render_response_snippet"]), "values": [[""]]},
            ])
            continue

        snippet = (r.text or "")[:220].replace("\n", " ")

        if r.status_code != 200:
            msg = f"Render HTTP {r.status_code}"
            log(f"❌ {msg} :: {snippet}")
            batch_update(ws, [
                {"range": a1(idx, hm["render_error"]), "values": [[msg]]},
                {"range": a1(idx, hm["render_response_snippet"]), "values": [[snippet]]},
            ])
            continue

        try:
            j = r.json()
        except Exception:
            j = {}

        # Accept multiple possible response keys (we do NOT assume your PA code is perfect)
        image_url = (
            (j.get("image_url") if isinstance(j, dict) else None)
            or (j.get("graphic_url") if isinstance(j, dict) else None)
            or (j.get("url") if isinstance(j, dict) else None)
            or ""
        )

        if not image_url:
            msg = "Render response missing image_url"
            log(f"❌ {msg} :: {snippet}")
            batch_update(ws, [
                {"range": a1(idx, hm["render_error"]), "values": [[msg]]},
                {"range": a1(idx, hm["render_response_snippet"]), "values": [[snippet]]},
            ])
            continue

        batch_update(ws, [
            {"range": a1(idx, hm["graphic_url"]), "values": [[image_url]]},
            {"range": a1(idx, hm["rendered_timestamp"]), "values": [[ts()]]} if "rendered_timestamp" in hm else {"range": a1(idx, hm["rendered_timestamp"]), "values": [[ts()]]},
            {"range": a1(idx, hm["render_error"]), "values": [[""]]},
            {"range": a1(idx, hm["render_response_snippet"]), "values": [[snippet]]},
            {"range": a1(idx, hm["status"]), "values": [["READY_TO_PUBLISH"]]},
        ])

        rendered += 1
        log(f"✅ Rendered row {idx} → {image_url} (READY_TO_PUBLISH)")

    log(f"Done. Rendered {rendered}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
