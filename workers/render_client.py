#!/usr/bin/env python3
"""
TravelTxter — Render Client

Consumes: status == READY_TO_POST
Writes:   graphic_url
Promotes: READY_TO_POST -> READY_TO_PUBLISH

NOTE (LOCKED FIX):
- Renderer template already prints the £ symbol.
- Therefore this worker must send PRICE as numeric only (e.g. "660"), not "£660",
  otherwise the graphic shows "££660".
"""

import os
import sys
import json
import time
import math
import datetime as dt
from typing import Dict, Any, List, Tuple, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# Logging
# ============================================================

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


# ============================================================
# Env
# ============================================================

def env_str(name: str, default: str = "") -> str:
    return (os.environ.get(name, default) or "").strip()


def env_any(names: List[str], default: str = "") -> str:
    for n in names:
        v = env_str(n, "")
        if v:
            return v
    return default


# ============================================================
# Sheets auth
# ============================================================

def _parse_sa_json(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return json.loads(raw.replace("\\n", "\n"))


def gs_client() -> gspread.Client:
    raw = env_any(["GCP_SA_JSON_ONE_LINE", "GCP_SA_JSON"])
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")

    info = _parse_sa_json(raw)
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds)


# ============================================================
# Helpers
# ============================================================

def col(header_map: Dict[str, int], name: str) -> int:
    if name not in header_map:
        return -1
    return header_map[name]


def safe_get(row: List[str], idx: int) -> str:
    return row[idx].strip() if 0 <= idx < len(row) else ""


def ddmmyy(iso_date: str) -> str:
    s = (iso_date or "").strip()
    if not s:
        return ""
    try:
        d = dt.date.fromisoformat(s)
        return d.strftime("%d%m%y")
    except Exception:
        return s


def ensure_columns(ws: gspread.Worksheet, headers: List[str], required: List[str]) -> Tuple[List[str], Dict[str, int]]:
    missing = [c for c in required if c not in headers]
    if missing:
        headers = headers + missing
        ws.update([headers], "A1")
        log(f"🛠️ Added missing columns: {missing}")
    header_map = {h.strip(): i for i, h in enumerate(headers)}
    return headers, header_map


def render_endpoint(render_url: str) -> str:
    """
    Keep your existing behaviour: if RENDER_URL already includes /render or /api/render, use it.
    If it points at base domain, append /render.
    """
    base = (render_url or "").strip().rstrip("/")
    if not base:
        raise RuntimeError("Missing RENDER_URL")

    if base.endswith("/render") or base.endswith("/api/render"):
        return base

    # Default to /render for PythonAnywhere
    return base + "/render"


# ============================================================
# Main
# ============================================================

def main() -> int:
    spreadsheet_id = env_any(["SPREADSHEET_ID", "SHEET_ID"])
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    render_url = env_str("RENDER_URL")

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID (or SHEET_ID)")
    if not render_url:
        raise RuntimeError("Missing RENDER_URL")

    render_ep = render_endpoint(render_url)
    log(f"Render endpoint: {render_ep}")

    gc = gs_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(tab)

    values = ws.get_all_values()
    if len(values) < 2:
        log("No rows found.")
        return 0

    headers = [h.strip() for h in values[0]]
    required = [
        "status",
        "deal_id",
        "origin_city",
        "destination_city",
        "outbound_date",
        "return_date",
        "price_gbp",
        "graphic_url",
        "rendered_at",
    ]
    headers, hmap = ensure_columns(ws, headers, required)

    c_status = col(hmap, "status")
    c_deal = col(hmap, "deal_id")
    c_from = col(hmap, "origin_city")
    c_to = col(hmap, "destination_city")
    c_out = col(hmap, "outbound_date")
    c_ret = col(hmap, "return_date")
    c_price = col(hmap, "price_gbp")
    c_gurl = col(hmap, "graphic_url")
    c_rend = col(hmap, "rendered_at")

    # Process first READY_TO_POST row
    for i, row in enumerate(values[1:], start=2):
        status = safe_get(row, c_status).upper()
        if status != "READY_TO_POST":
            continue

        deal_id = safe_get(row, c_deal)
        origin_city = safe_get(row, c_from)
        dest_city = safe_get(row, c_to)

        out_iso = safe_get(row, c_out)
        ret_iso = safe_get(row, c_ret)
        out_fmt = ddmmyy(out_iso)
        ret_fmt = ddmmyy(ret_iso)

        # Price formatting: renderer already prints the £ symbol, so we send numeric only (e.g. 660)
        raw_price = safe_get(row, c_price)
        raw_price = raw_price.replace("£", "").replace(",", "").strip()
        try:
            price_val = float(raw_price or "0")
        except Exception:
            price_val = 0.0
        price_fmt = str(int(math.ceil(price_val)))

        payload = {
            "deal_id": deal_id,
            "TO": dest_city,
            "FROM": origin_city,
            "OUT": out_fmt,
            "IN": ret_fmt,
            "PRICE": price_fmt,
        }

        log(f"🖼️  Rendering row {i} deal_id={deal_id} ({origin_city} -> {dest_city})")

        resp = requests.post(render_ep, json=payload, timeout=60)
        if resp.status_code >= 400:
            raise RuntimeError(f"Renderer error {resp.status_code}: {resp.text[:400]}")

        j = resp.json() if "application/json" in (resp.headers.get("content-type", "") or "") else {}
        graphic_url = (j.get("graphic_url") or j.get("url") or "").strip()
        if not graphic_url:
            raise RuntimeError(f"Renderer response missing graphic_url: {str(j)[:400]}")

        ws.update([[graphic_url]], gspread.utils.rowcol_to_a1(i, c_gurl + 1))
        ws.update([[dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"]], gspread.utils.rowcol_to_a1(i, c_rend + 1))
        ws.update([["READY_TO_PUBLISH"]], gspread.utils.rowcol_to_a1(i, c_status + 1))

        log(f"✅ Rendered graphic_url set for row {i}")
        return 0

    log("Done. Rendered 0.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
