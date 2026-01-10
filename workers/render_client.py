#!/usr/bin/env python3
"""
workers/render_client.py

LOCKED PURPOSE:
- Finds status == READY_TO_POST
- Calls PythonAnywhere renderer (POST /api/render)
- Writes graphic_url back to RAW_DEALS
- Promotes status -> READY_TO_PUBLISH

Non-negotiable render payload contract:
{
  "TO": "City",
  "FROM": "City",
  "OUT": "DDMMYY",
  "IN": "DDMMYY",
  "PRICE": "123"   # NUMERIC ONLY (renderer adds £)
}

Does NOT change creative rendering.
"""

from __future__ import annotations

import os
import json
import math
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
        # some users store JSON with escaped newlines
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
    """
    Convert YYYY-MM-DD -> DDMMYY.
    If it's already digits, keep last 6 digits.
    """
    s = (iso_date or "").strip()
    if not s:
        return ""
    try:
        d = dt.date.fromisoformat(s[:10])
        return d.strftime("%d%m%y")
    except Exception:
        digits = "".join(ch for ch in s if ch.isdigit())
        return digits[-6:] if len(digits) >= 6 else digits


def clean_numeric_price(v: str) -> str:
    """
    Returns numeric-only string (ceil to int).
    Renderer adds the £ itself.
    """
    s = (v or "").replace("Â£", "").replace("£", "").replace(",", "").strip()
    try:
        n = float(s) if s else 0.0
    except Exception:
        n = 0.0
    return str(int(math.ceil(n)))


# -------------------------
# Renderer URL normalisation
# -------------------------

def normalize_renderer_urls(render_url_raw: str) -> Tuple[str, str, str]:
    """
    Returns (render_ep, health_ep, base_url)

    Accepts:
    - https://domain
    - https://domain/
    - https://domain/api/render
    - https://domain/api/render/
    """
    u = (render_url_raw or "").strip()
    if not u:
        die("Missing RENDER_URL")

    if not u.startswith("http://") and not u.startswith("https://"):
        u = "https://" + u

    u = u.rstrip("/")
    p = urlparse(u)
    base = urlunparse((p.scheme, p.netloc, "", "", "", "")).rstrip("/")

    if p.path.endswith("/api/render"):
        render_ep = u
    else:
        render_ep = base + "/api/render"

    health_ep = base + "/api/health"
    return render_ep, health_ep, base


def absolutise_image_url(image_url: str, base_url: str) -> str:
    u = (image_url or "").strip()
    if not u:
        return ""
    if u.startswith("/"):
        return base_url + u
    pu = urlparse(u)
    if not pu.scheme:
        return "https://" + u.lstrip("/")
    return u


def preflight_public_image(url: str) -> None:
    if not url:
        raise RuntimeError("graphic_url is blank")

    headers = {"User-Agent": "traveltxter-render-preflight/1.0"}
    r = requests.get(url, stream=True, timeout=30, headers=headers, allow_redirects=True)

    if r.status_code != 200:
        snippet = ""
        try:
            snippet = (r.text or "")[:180].replace("\n", " ")
        except Exception:
            snippet = ""
        raise RuntimeError(f"graphic_url not fetchable (HTTP {r.status_code}) :: {snippet}")

    ctype = (r.headers.get("Content-Type") or "").lower().strip()
    if not ctype.startswith("image/"):
        raise RuntimeError(f"graphic_url not an image (Content-Type={ctype})")


# -------------------------
# Sheet header enforcement
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

    values = ws.get_all_values()
    if len(values) < 2:
        log("No rows.")
        return

    headers = [h.strip() for h in values[0]]
    headers = ensure_columns(
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
            "render_error",
            "rendered_timestamp",
        ],
    )

    h = {name: idx for idx, name in enumerate(headers)}

    c_status = h["status"]
    c_deal = h["deal_id"]
    c_from = h["origin_city"]
    c_to = h["destination_city"]
    c_out = h["outbound_date"]
    c_ret = h["return_date"]
    c_price = h["price_gbp"]
    c_gurl = h["graphic_url"]
    c_err = h["render_error"]
    c_rts = h["rendered_timestamp"]

    # best-effort healthcheck (do not fail the pipeline if missing)
    try:
        r = requests.get(health_ep, timeout=10)
        log(f"Renderer healthcheck: {r.status_code}")
    except Exception:
        log("Renderer healthcheck: (skipped)")

    rendered = 0

    for sheet_row_num, row in enumerate(values[1:], start=2):
        if rendered >= max_rows:
            break

        status = (row[c_status] if c_status < len(row) else "").strip().upper()
        if status != "READY_TO_POST":
            continue

        existing_url = (row[c_gurl] if c_gurl < len(row) else "").strip()
        if existing_url:
            continue

        deal_id = (row[c_deal] if c_deal < len(row) else "").strip()
        origin_city = (row[c_from] if c_from < len(row) else "").strip()
        dest_city = (row[c_to] if c_to < len(row) else "").strip()

        out_fmt = ddmmyy((row[c_out] if c_out < len(row) else "").strip())
        ret_fmt = ddmmyy((row[c_ret] if c_ret < len(row) else "").strip())

        price_numeric = clean_numeric_price((row[c_price] if c_price < len(row) else "").strip())

        payload = {
            "deal_id": deal_id,
            "TO": dest_city,
            "FROM": origin_city,
            "OUT": out_fmt,
            "IN": ret_fmt,
            "PRICE": price_numeric,  # NUMERIC ONLY (renderer adds £)
        }

        log(f"🖼️  Rendering row {sheet_row_num} deal_id={deal_id} ({origin_city} -> {dest_city})")

        try:
            resp = requests.post(render_ep, json=payload, timeout=90)
        except Exception as e:
            msg = f"Render request failed: {e}"
            ws.update([[msg]], a1_cell(c_err, sheet_row_num))
            die(msg)

        if resp.status_code >= 400:
            raw = resp.text[:500]
            msg = f"Render HTTP {resp.status_code}: {raw}"
            log(f"❌ {msg}")
            ws.update([[msg]], a1_cell(c_err, sheet_row_num))
            continue

        data = resp.json() if "application/json" in (resp.headers.get("content-type") or "") else {}
        image_url = (data.get("graphic_url") or data.get("image_url") or "").strip()
        image_url = absolutise_image_url(image_url, base_url)

        if not image_url:
            msg = f"Render OK but missing graphic_url. Response: {str(data)[:300]}"
            log(f"❌ {msg}")
            ws.update([[msg]], a1_cell(c_err, sheet_row_num))
            continue

        # Must be public + image/*
        try:
            preflight_public_image(image_url)
        except Exception as e:
            msg = f"graphic_url preflight failed: {e}"
            log(f"❌ {msg}")
            ws.update([[msg]], a1_cell(c_err, sheet_row_num))
            continue

        # Write URL + promote status
        ws.update([[image_url]], a1_cell(c_gurl, sheet_row_num))
        ws.update([["READY_TO_PUBLISH"]], a1_cell(c_status, sheet_row_num))
        ws.update([[dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"]], a1_cell(c_rts, sheet_row_num))
        ws.update([[""]], a1_cell(c_err, sheet_row_num))

        log(f"✅ Rendered row {sheet_row_num} -> READY_TO_PUBLISH ({image_url})")
        rendered += 1

    log(f"Done. Rendered {rendered}.")


if __name__ == "__main__":
    main()
