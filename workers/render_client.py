#!/usr/bin/env python3
"""
workers/render_client.py

Renders the BEST eligible row by calling the PythonAnywhere renderer.

Key behavior (LOCKED):
- RENDER_URL may be either:
  (a) base domain: https://greenroomman.pythonanywhere.com
  (b) full endpoint: https://greenroomman.pythonanywhere.com/api/render
- We normalize to a POST endpoint at /api/render
- Healthcheck uses /api/health (best effort)

IMPORTANT FIX (THIS SESSION):
- Instagram error 9004 happens when graphic_url is not a PUBLIC, DIRECT image URL.
- The renderer sometimes returns a relative path like /static/renders/xxx.png
  which Instagram cannot fetch unless it is made absolute (https://domain/static/...)
- Therefore: we absolutise and preflight the returned image URL BEFORE writing it to the sheet.

PRICE RULE:
- Renderer now guarantees a single £ inside the image.
- We send numeric-only PRICE to keep things clean (e.g. "660").
"""

from __future__ import annotations

import os
import sys
import json
import math
import time
import datetime as dt
from typing import Any, Dict, List, Tuple, Optional
from urllib.parse import urlparse, urlunparse

import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# Logging
# ============================================================

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


def die(msg: str, code: int = 1) -> None:
    log(f"FATAL: {msg}")
    raise SystemExit(code)


# ============================================================
# Env
# ============================================================

def get_env(name: str) -> str:
    v = (os.getenv(name) or "").strip()
    if not v:
        die(f"Missing env var: {name}")
    return v


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
    raw = (os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON") or "").strip()
    if not raw:
        die("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")

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

def a1_col(idx0: int) -> str:
    """0-based col index -> A1 column letters"""
    idx = idx0 + 1
    s = ""
    while idx:
        idx, r = divmod(idx - 1, 26)
        s = chr(65 + r) + s
    return s


def ddmmyy(iso_date: str) -> str:
    s = (iso_date or "").strip()
    if not s:
        return ""
    try:
        d = dt.date.fromisoformat(s[:10])
        return d.strftime("%d%m%y")
    except Exception:
        # best-effort fallback
        digits = "".join(ch for ch in s if ch.isdigit())
        return digits[-6:] if len(digits) >= 6 else digits


def normalize_renderer_urls(render_url_raw: str) -> Tuple[str, str]:
    """
    Returns (render_ep, health_ep)
    - render_ep MUST be POST /api/render
    - health_ep best-effort GET /api/health
    """
    u = (render_url_raw or "").strip().rstrip("/")
    if not u:
        die("Missing RENDER_URL")

    p = urlparse(u)
    if not p.scheme:
        # If someone pasted "domain.com/..." assume https
        u = "https://" + u
        p = urlparse(u)

    base = urlunparse((p.scheme, p.netloc, "", "", "", ""))

    # If the provided URL already ends with /api/render, keep it.
    if p.path.endswith("/api/render"):
        render_ep = u
    else:
        render_ep = base.rstrip("/") + "/api/render"

    health_ep = base.rstrip("/") + "/api/health"
    return render_ep, health_ep


def absolutise_image_url(image_url: str, render_ep: str) -> str:
    """
    Make renderer-returned URLs safe for Instagram:
    - If '/static/..' -> 'https://domain/static/..'
    - If missing scheme -> assume https
    """
    u = (image_url or "").strip()
    if not u:
        return ""

    # If it is relative (/static/...), prefix with scheme+host from render_ep
    if u.startswith("/"):
        p = urlparse(render_ep)
        base = urlunparse((p.scheme, p.netloc, "", "", "", "")).rstrip("/")
        return base + u

    pu = urlparse(u)
    if not pu.scheme:
        return "https://" + u.lstrip("/")

    return u


def preflight_public_image(url: str) -> None:
    """
    Ensure the URL is a direct, public image:
    - HTTP 200
    - Content-Type: image/*
    """
    if not url:
        raise RuntimeError("graphic_url is blank after normalisation")

    headers = {"User-Agent": "traveltxter-render-preflight/1.0"}
    r = requests.get(url, stream=True, timeout=30, headers=headers, allow_redirects=True)

    if r.status_code != 200:
        snippet = (r.text or "")[:200].replace("\n", " ")
        raise RuntimeError(f"Rendered image not fetchable (HTTP {r.status_code}) :: {snippet}")

    ctype = (r.headers.get("Content-Type") or "").lower().strip()
    if not ctype.startswith("image/"):
        # small peek to catch HTML/JSON
        try:
            chunk = next(r.iter_content(chunk_size=512))
            peek = chunk[:120]
        except Exception:
            peek = b""
        raise RuntimeError(f"Rendered URL not an image (Content-Type={ctype}) :: peek={peek!r}")


# ============================================================
# Main
# ============================================================

def main() -> None:
    spreadsheet_id = get_env("SPREADSHEET_ID")
    raw_tab = (os.getenv("RAW_DEALS_TAB", "RAW_DEALS") or "RAW_DEALS").strip()
    render_url_raw = get_env("RENDER_URL")
    render_max_rows = int(os.getenv("RENDER_MAX_ROWS", "1"))

    render_ep, health_ep = normalize_renderer_urls(render_url_raw)

    gc = gs_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(raw_tab)

    rows: List[List[Any]] = ws.get_all_values()
    if not rows or len(rows) < 2:
        die("RAW_DEALS is empty (no header + rows).", 0)

    headers = rows[0]
    h = {name.strip(): i for i, name in enumerate(headers)}

    def col(name: str) -> int:
        if name not in h:
            die(f"Missing required column in RAW_DEALS: {name}")
        return h[name]

    # Required cols for render payload
    c_status = col("status")
    c_deal_id = col("deal_id")
    c_price = col("price_gbp")
    c_origin_city = col("origin_city")
    c_dest_city = col("destination_city")
    c_out = col("outbound_date")
    c_ret = col("return_date")

    # Optional cols
    c_render_url = h.get("graphic_url")
    c_render_err = h.get("render_error")

    # Healthcheck (best effort)
    try:
        r = requests.get(health_ep, timeout=10)
        log(f"Renderer healthcheck: {r.status_code}")
    except Exception as e:
        log(f"Renderer healthcheck failed (non-fatal): {e}")

    # Pick BEST eligible row (keep existing behavior)
    candidates: List[Tuple[int, List[Any]]] = []
    for i in range(1, len(rows)):
        row = rows[i]
        status = (row[c_status] if c_status < len(row) else "").strip()
        if status != "READY_TO_POST":
            continue
        candidates.append((i, row))

    if not candidates:
        log("No READY_TO_POST rows to render.")
        return

    # Deterministic: first candidate (existing behavior in this file’s style)
    to_process = candidates[: max(1, render_max_rows)]

    for idx, row in to_process:
        sheet_row_num = idx + 1  # rows list is 0-based, sheets are 1-based

        deal_id = (row[c_deal_id] if c_deal_id < len(row) else "").strip()
        origin_city = (row[c_origin_city] if c_origin_city < len(row) else "").strip()
        dest_city = (row[c_dest_city] if c_dest_city < len(row) else "").strip()
        out_fmt = ddmmyy((row[c_out] if c_out < len(row) else "").strip())
        in_fmt = ddmmyy((row[c_ret] if c_ret < len(row) else "").strip())

        # PRICE: numeric only (renderer prints £)
        try:
            raw_price = (row[c_price] if c_price < len(row) else "0")
            raw_price = (raw_price or "").strip().replace("£", "").replace(",", "")
            price_val = float(raw_price or "0")
        except Exception:
            price_val = 0.0
        price_fmt = str(int(math.ceil(price_val)))

        payload = {
            "deal_id": deal_id,
            "TO": dest_city,
            "FROM": origin_city,
            "OUT": out_fmt,
            "IN": in_fmt,
            "PRICE": price_fmt,
        }

        log(f"🖼️ Rendering row {sheet_row_num} deal_id={deal_id}")

        try:
            resp = requests.post(render_ep, json=payload, timeout=90)
        except Exception as e:
            msg = f"Render request failed: {e}"
            log(f"❌ {msg}")
            if c_render_err is not None:
                ws.update([[msg]], f"{a1_col(c_render_err)}{sheet_row_num}")
            continue

        raw = (resp.text or "")[:400]
        if resp.status_code >= 400:
            msg = f"Render HTTP {resp.status_code}: {raw}"
            log(f"❌ {msg}")
            if c_render_err is not None:
                ws.update([[msg]], f"{a1_col(c_render_err)}{sheet_row_num}")
            continue

        data = resp.json() if "application/json" in (resp.headers.get("content-type", "") or "") else {}
        image_url = (data.get("image_url") or data.get("graphic_url") or "").strip()

        if not image_url:
            msg = f"Render OK but missing image_url. Response: {str(data)[:300]}"
            log(f"❌ {msg}")
            if c_render_err is not None:
                ws.update([[msg]], f"{a1_col(c_render_err)}{sheet_row_num}")
            continue

        # ✅ FIX: absolutise + preflight so IG can fetch it
        image_url_abs = absolutise_image_url(image_url, render_ep)

        try:
            preflight_public_image(image_url_abs)
        except Exception as e:
            msg = f"Rendered URL not IG-safe: {str(e)[:260]}"
            log(f"❌ {msg}")
            if c_render_err is not None:
                ws.update([[msg]], f"{a1_col(c_render_err)}{sheet_row_num}")
            continue

        # Write graphic_url
        if c_render_url is None:
            die("RAW_DEALS missing graphic_url column (required for downstream).")

        ws.update([[image_url_abs]], f"{a1_col(c_render_url)}{sheet_row_num}")

        # Clear render_error if present
        if c_render_err is not None:
            ws.update([[""]], f"{a1_col(c_render_err)}{sheet_row_num}")

        # Promote status
        ws.update([["READY_TO_PUBLISH"]], f"{a1_col(c_status)}{sheet_row_num}")

        log(f"✅ Rendered and promoted row {sheet_row_num} -> READY_TO_PUBLISH")


if __name__ == "__main__":
    main()
