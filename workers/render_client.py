#!/usr/bin/env python3
"""
workers/render_client.py

LOCKED ROLE:
- Consumes: status == READY_TO_POST
- Calls:    RENDER_URL exactly as provided (NO guessing paths)
- Writes:   graphic_url
- Promotes: READY_TO_POST -> READY_TO_PUBLISH

CRITICAL RULE (LOCKED):
- Renderer template ALREADY prints the £ symbol
- Therefore this worker MUST send PRICE as numeric only (e.g. "660")
  otherwise the graphic will show "££660"
"""

from __future__ import annotations

import os
import json
import math
import datetime as dt
from typing import Dict, Any, List

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
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


# ============================================================
# Helpers
# ============================================================

def col_letter(n1: int) -> str:
    s = ""
    n = n1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def a1(rownum: int, col0: int) -> str:
    return f"{col_letter(col0 + 1)}{rownum}"


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


def ensure_columns(ws: gspread.Worksheet, headers: List[str], required: List[str]) -> Dict[str, int]:
    missing = [c for c in required if c not in headers]
    if missing:
        headers = headers + missing
        ws.update([headers], "A1")
        log(f"🛠️ Added missing columns: {missing}")
    return {h: i for i, h in enumerate(headers)}


# ============================================================
# Renderer endpoint (LOCKED)
# ============================================================

def render_endpoint(render_url: str) -> str:
    """
    LOCKED:
    - Use RENDER_URL EXACTLY as provided in GitHub Secrets
    - Do NOT append /render or /api/render
    """
    url = (render_url or "").strip()
    if not url:
        raise RuntimeError("Missing RENDER_URL")
    return url.rstrip("/")


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
        log("No rows.")
        return 0

    headers = [h.strip() for h in values[0]]
    hmap = ensure_columns(
        ws,
        headers,
        [
            "status",
            "deal_id",
            "origin_city",
            "destination_city",
            "outbound_date",
            "return_date",
            "price_gbp",
            "graphic_url",
            "rendered_at",
        ],
    )

    c_status = hmap["status"]
    c_deal = hmap["deal_id"]
    c_from = hmap["origin_city"]
    c_to = hmap["destination_city"]
    c_out = hmap["outbound_date"]
    c_ret = hmap["return_date"]
    c_price = hmap["price_gbp"]
    c_gurl = hmap["graphic_url"]
    c_rend = hmap["rendered_at"]

    # Process first READY_TO_POST row only (deterministic)
    for rownum, row in enumerate(values[1:], start=2):
        if safe_get(row, c_status).upper() != "READY_TO_POST":
            continue

        deal_id = safe_get(row, c_deal)
        origin_city = safe_get(row, c_from)
        dest_city = safe_get(row, c_to)

        out_fmt = ddmmyy(safe_get(row, c_out))
        ret_fmt = ddmmyy(safe_get(row, c_ret))

        # 🔒 PRICE FIX: renderer prints £, so we send numeric only
        raw_price = safe_get(row, c_price).replace("£", "").replace(",", "").strip()
        try:
            price_val = float(raw_price)
        except Exception:
            price_val = 0.0

        price_numeric = str(int(math.ceil(price_val)))

        payload = {
            "TO": dest_city,
            "FROM": origin_city,
            "OUT": out_fmt,
            "IN": ret_fmt,
            "PRICE": price_numeric,
        }

        log(f"🖼️ Rendering row {rownum} deal_id={deal_id}")

        resp = requests.post(render_ep, json=payload, timeout=90)
        if resp.status_code >= 400:
            raise RuntimeError(f"Renderer error {resp.status_code}: {resp.text[:400]}")

        data = resp.json()
        graphic_url = (data.get("graphic_url") or data.get("url") or "").strip()
        if not graphic_url:
            raise RuntimeError(f"Renderer response missing graphic_url: {data}")

        ws.update([[graphic_url]], a1(rownum, c_gurl))
        ws.update([[dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"]], a1(rownum, c_rend))
        ws.update([["READY_TO_PUBLISH"]], a1(rownum, c_status))

        log(f"✅ Rendered graphic_url set for row {rownum}")
        return 0

    log("Done. Rendered 0.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
