#!/usr/bin/env python3
"""
workers/render_client.py (CLEAN)

LOCKED PURPOSE:
- Find RAW_DEALS rows where status == READY_TO_POST
- Call PythonAnywhere renderer (POST /api/render)
- Write graphic_url back to RAW_DEALS
- Promote status -> READY_TO_PUBLISH

IMPORTANT:
- DOES NOT require any Instagram env vars.
- PRICE sent as numeric-only (renderer adds £).
- Dates sent as DDMMYY (no separators).
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
# Formatting helpers
# -------------------------

def ddmmyy(iso_date: str) -> str:
    """
    Convert YYYY-MM-DD -> DDMMYY (no separators).
    Example: 2026-01-26 -> 260126
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


def numeric_price_only(raw: str) -> str:
    """
    Renderer adds £ itself, so we send numeric-only string.
    Rounds up to int.
    """
    s = (raw or "").strip().replace("Â£", "").replace("£", "").replace(",", "")
    try:
        v = float(s) if s else 0.0
    except Exception:
        v = 0.0
    return str(int(math.ceil(v)))


# -------------------------
# Renderer endpoint logic
# -------------------------

def _base_from_any_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    if not u.startswith("http://") and not u.startswith("https://"):
        u = "https://" + u
    u = u.rstrip("/")
    p = urlparse(u)
    return urlunparse((p.scheme, p.netloc, "", "", "", "")).rstrip("/")


def normalize_renderer(render_url_raw: str) -> Tuple[str, str, str]:
    """
    Returns: (render_ep, health_ep, base_url)
    Accepts:
      - https://domain
      - https://domain/api/render
    Always normalises to POST /api/render.
    """
    base = _base_from_any_url(render_url_raw)
    if not base:
        die("Missing RENDER_URL")
    render_ep = base + "/api/render"
    health_ep = base + "/api/health"
    return render_ep, health_ep, base


def absolutise_url(maybe_url: str, base_url: str) -> str:
    u = (maybe_url or "").strip()
    if not u:
        return ""
    if u.startswith("/"):
        return base_url + u
    pu = urlparse(u)
    if not pu.scheme:
        return "https://" + u.lstrip("/")
    return u


def preflight_public_image(url: str) -> None:
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

def main() -> int:
    spreadsheet_id = must_env("SPREADSHEET_ID")
    raw_tab = env("RAW_DEALS_TAB", "RAW_DEALS")
    render_url_raw = must_env("RENDER_URL")
    max_rows = int(env("RENDER_MAX_ROWS", "1"))

    render_ep, health_ep, base_url = normalize_renderer(render_url_raw)

    log(f"Render endpoint: {render_ep}")

    # Best-effort healthcheck (non-fatal)
    try:
        r = requests.get(health_ep, timeout=10)
        log(f"Renderer healthcheck: {r.status_code}")
    except Exception as e:
        log(f"Renderer healthcheck failed (non-fatal): {e}")

    gc = gs_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(raw_tab)

    values = ws.get_all_values()
    if not values or len(values) < 2:
        log("RAW_DEALS empty. Nothing to do.")
        return 0

    headers = [h.strip() for h in values[0]]
    headers = ensure_columns(ws, headers, ["graphic_url", "render_error", "rendered_timestamp"])
    values = ws.get_all_values()
    headers = [h.strip() for h in values[0]]

    h = {name: i for i, name in enumerate(headers)}

    # Required columns (do not guess)
    required = [
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
    ]
    for c in required:
        if c not in h:
            die(f"Missing required column in RAW_DEALS: {c}")

    c_status = h["status"]
    c_deal_id = h["deal_id"]
    c_origin_city = h["origin_city"]
    c_dest_city = h["destination_city"]
    c_outbound = h["outbound_date"]
    c_return = h["return_date"]
    c_price = h["price_gbp"]
    c_graphic = h["graphic_url"]
    c_err = h["render_error"]
    c_rts = h["rendered_timestamp"]

    rendered = 0

    for i in range(1, len(values)):
        if rendered >= max_rows:
            break

        row = values[i]
        sheet_row = i + 1

        status = (row[c_status] if c_status < len(row) else "").strip()
        if status != "READY_TO_POST":
            continue

        if (row[c_graphic] if c_graphic < len(row) else "").strip():
            continue

        deal_id = (row[c_deal_id] if c_deal_id < len(row) else "").strip()
        origin_city = (row[c_origin_city] if c_origin_city < len(row) else "").strip()
        dest_city = (row[c_dest_city] if c_dest_city < len(row) else "").strip()
        out_iso = (row[c_outbound] if c_outbound < len(row) else "").strip()
        ret_iso = (row[c_return] if c_return < len(row) else "").strip()
        price_raw = (row[c_price] if c_price < len(row) else "").strip()

        payload = {
            "deal_id": deal_id,
            "TO": dest_city,
            "FROM": origin_city,
            "OUT": ddmmyy(out_iso),
            "IN": ddmmyy(ret_iso),
            "PRICE": numeric_price_only(price_raw),  # numeric only
        }

        log(f"🖼️  Rendering row {sheet_row} deal_id={deal_id} ({origin_city} -> {dest_city})")

        try:
            resp = requests.post(render_ep, json=payload, timeout=90)
        except Exception as e:
            msg = f"Render request failed: {str(e)[:280]}"
            ws.update([[msg]], a1_cell(c_err, sheet_row))
            continue

        if resp.status_code >= 400:
            raw = (resp.text or "")[:400]
            msg = f"Render HTTP {resp.status_code}: {raw}"
            ws.update([[msg]], a1_cell(c_err, sheet_row))
            continue

        try:
            data = resp.json()
        except Exception:
            msg = f"Render returned non-JSON: {(resp.text or '')[:280]}"
            ws.update([[msg]], a1_cell(c_err, sheet_row))
            continue

        img_url = (data.get("graphic_url") or data.get("image_url") or "").strip()
        img_url = absolutise_url(img_url, base_url)

        if not img_url:
            msg = f"Render OK but missing graphic_url: {str(data)[:280]}"
            ws.update([[msg]], a1_cell(c_err, sheet_row))
            continue

        try:
            preflight_public_image(img_url)
        except Exception as e:
            msg = f"graphic_url preflight failed: {str(e)[:280]}"
            ws.update([[msg]], a1_cell(c_err, sheet_row))
            continue

        ws.update([[img_url]], a1_cell(c_graphic, sheet_row))
        ws.update([["READY_TO_PUBLISH"]], a1_cell(c_status, sheet_row))
        ws.update([[dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"]], a1_cell(c_rts, sheet_row))
        ws.update([[""]], a1_cell(c_err, sheet_row))

        log(f"✅ Rendered row {sheet_row} -> READY_TO_PUBLISH ({img_url})")
        rendered += 1

    log(f"Done. Rendered {rendered}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
