#!/usr/bin/env python3
"""
workers/render_client.py

LOCKED PURPOSE:
- Finds status == READY_TO_POST
- Calls PythonAnywhere renderer (POST /api/render)
- Writes graphic_url back to RAW_DEALS
- Promotes status -> READY_TO_PUBLISH

CRITICAL FIX (NO REINVENT):
- If RAW_DEALS does not have 'graphic_url' column, create it.
- Same for 'render_error' (optional but useful).
- This stops the "PNG exists but no graphic_url appears" loop.

Does NOT change creative rendering.
"""

from __future__ import annotations

import os
import json
import math
import time
import datetime as dt
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse, urlunparse

import requests
import gspread
from google.oauth2.service_account import Credentials


# -------------------------
# Logging
# -------------------------

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


def die(msg: str, code: int = 1) -> None:
    log(f"FATAL: {msg}")
    raise SystemExit(code)


# -------------------------
# Env
# -------------------------

def env(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()


def must_env(name: str) -> str:
    v = env(name)
    if not v:
        die(f"Missing env var: {name}")
    return v


# -------------------------
# Sheets auth
# -------------------------

def _parse_sa_json(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()
    try:
        return json.loads(raw)
    except Exception:
        return json.loads(raw.replace("\\n", "\n"))


def gs_client() -> gspread.Client:
    raw = env("GCP_SA_JSON_ONE_LINE") or env("GCP_SA_JSON")
    if not raw:
        die("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
    info = _parse_sa_json(raw)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


# -------------------------
# A1 helpers
# -------------------------

def a1_col(idx0: int) -> str:
    idx = idx0 + 1
    s = ""
    while idx:
        idx, r = divmod(idx - 1, 26)
        s = chr(65 + r) + s
    return s


def a1_cell(col0: int, row1: int) -> str:
    return f"{a1_col(col0)}{row1}"


# -------------------------
# Date + price formatting
# -------------------------

def ddmmyy(iso_date: str) -> str:
    s = (iso_date or "").strip()
    if not s:
        return ""
    try:
        d = dt.date.fromisoformat(s[:10])
        return d.strftime("%d%m%y")
    except Exception:
        digits = "".join(ch for ch in s if ch.isdigit())
        return digits[-6:] if len(digits) >= 6 else digits


def ceil_price_digits_only(raw: str) -> str:
    s = (raw or "").strip().replace("£", "").replace(",", "")
    try:
        v = float(s) if s else 0.0
    except Exception:
        v = 0.0
    return str(int(math.ceil(v)))


# -------------------------
# Renderer URL normalisation
# -------------------------

def normalize_renderer_urls(render_url_raw: str) -> Tuple[str, str, str]:
    """
    Returns (render_ep, health_ep, base_url)

    render_ep is always POST {base}/api/render
    health_ep is GET  {base}/api/health
    """
    u = (render_url_raw or "").strip().rstrip("/")
    if not u:
        die("Missing RENDER_URL")

    p = urlparse(u)
    if not p.scheme:
        u = "https://" + u
        p = urlparse(u)

    base = urlunparse((p.scheme, p.netloc, "", "", "", "")).rstrip("/")

    # If someone gave /api/render already, keep base anyway
    render_ep = base + "/api/render"
    health_ep = base + "/api/health"
    return render_ep, health_ep, base


def absolutise_image_url(image_url: str, base_url: str) -> str:
    """
    Renderer may return:
      - absolute https://...
      - or /static/renders/...
    We store absolute URLs in the sheet (IG-safe).
    """
    u = (image_url or "").strip()
    if not u:
        return ""
    if u.startswith("/"):
        return base_url + u
    pu = urlparse(u)
    if not pu.scheme:
        return "https://" + u.lstrip("/")
    return u


# -------------------------
# Sheet header enforcement (THE FIX)
# -------------------------

def ensure_columns(ws: gspread.Worksheet, headers: List[str], required: List[str]) -> List[str]:
    missing = [c for c in required if c not in headers]
    if not missing:
        return headers

    new_headers = headers + missing
    ws.update([new_headers], "A1")
    log(f"🛠️ Added missing columns: {missing}")
    return new_headers


# -------------------------
# Main
# -------------------------

def main() -> None:
    spreadsheet_id = must_env("SPREADSHEET_ID")
    raw_tab = env("RAW_DEALS_TAB", "RAW_DEALS")
    render_url_raw = must_env("RENDER_URL")
    max_rows = int(env("RENDER_MAX_ROWS", "1"))

    render_ep, health_ep, base_url = normalize_renderer_urls(render_url_raw)

    gc = gs_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(raw_tab)

    values: List[List[str]] = ws.get_all_values()
    if not values or len(values) < 2:
        die("RAW_DEALS is empty (no header + rows).", 0)

    # ✅ Ensure we have columns to write into (this stops the loop)
    headers = ensure_columns(ws, values[0], ["graphic_url", "render_error"])
    # Refresh values after header change
    values = ws.get_all_values()
    headers = values[0]

    h = {name.strip(): i for i, name in enumerate(headers)}

    def col(name: str) -> int:
        if name not in h:
            die(f"Missing required column in RAW_DEALS: {name}")
        return h[name]

    c_status = col("status")
    c_deal_id = col("deal_id")
    c_price = col("price_gbp")
    c_origin_city = col("origin_city")
    c_dest_city = col("destination_city")
    c_out = col("outbound_date")
    c_ret = col("return_date")
    c_graphic = col("graphic_url")
    c_render_err = col("render_error")

    # Healthcheck (best effort)
    try:
        r = requests.get(health_ep, timeout=10)
        log(f"Renderer healthcheck: {r.status_code}")
    except Exception as e:
        log(f"Renderer healthcheck failed (non-fatal): {e}")

    rendered = 0

    for i in range(1, len(values)):
        row = values[i]
        status = (row[c_status] if c_status < len(row) else "").strip()
        if status != "READY_TO_POST":
            continue

        sheet_row_num = i + 1

        deal_id = (row[c_deal_id] if c_deal_id < len(row) else "").strip()
        origin_city = (row[c_origin_city] if c_origin_city < len(row) else "").strip()
        dest_city = (row[c_dest_city] if c_dest_city < len(row) else "").strip()
        out_fmt = ddmmyy((row[c_out] if c_out < len(row) else "").strip())
        ret_fmt = ddmmyy((row[c_ret] if c_ret < len(row) else "").strip())
        price_fmt = ceil_price_digits_only(row[c_price] if c_price < len(row) else "")

        payload = {
            "deal_id": deal_id,
            "TO": dest_city,
            "FROM": origin_city,
            "OUT": out_fmt,
            "IN": ret_fmt,
            "PRICE": price_fmt,
        }

        log(f"🖼️  Rendering row {sheet_row_num} deal_id={deal_id} ({origin_city} -> {dest_city})")

        try:
            resp = requests.post(render_ep, json=payload, timeout=60)
        except Exception as e:
            ws.update([[f"Render request failed: {e}"]], a1_cell(c_render_err, sheet_row_num))
            continue

        raw = (resp.text or "")[:400]
        if resp.status_code >= 400:
            ws.update([[f"Render HTTP {resp.status_code}: {raw}"]], a1_cell(c_render_err, sheet_row_num))
            continue

        # Parse JSON
        ctype = (resp.headers.get("content-type") or "")
        data = resp.json() if "application/json" in ctype else {}
        image_url = (data.get("graphic_url") or data.get("image_url") or "").strip()

        if not image_url:
            ws.update([[f"Render OK but missing graphic_url: {str(data)[:300]}"]], a1_cell(c_render_err, sheet_row_num))
            continue

        # ✅ Store an IG-safe absolute URL
        image_url_abs = absolutise_image_url(image_url, base_url)

        ws.update([[image_url_abs]], a1_cell(c_graphic, sheet_row_num))
        ws.update([[""]], a1_cell(c_render_err, sheet_row_num))
        ws.update([["READY_TO_PUBLISH"]], a1_cell(c_status, sheet_row_num))

        log(f"✅ Rendered row {sheet_row_num} -> READY_TO_PUBLISH ({image_url_abs})")
        rendered += 1

        if rendered >= max_rows:
            break

    log(f"Done. Rendered {rendered}.")


if __name__ == "__main__":
    main()
