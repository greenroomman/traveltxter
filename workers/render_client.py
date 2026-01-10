#!/usr/bin/env python3
"""
workers/render_client.py (PASTE-SAFE, PRODUCTION-HARDENED)

Purpose:
- Find RAW_DEALS rows where status == READY_TO_POST
- Call PythonAnywhere renderer (POST)
- Write graphic_url back to RAW_DEALS
- Promote status -> READY_TO_PUBLISH

Hard guarantees:
- Normalises RENDER_URL to /api/render if needed (prevents 405 regressions)
- Uses header-map only (no hardcoded column numbers)
- Converts ISO dates -> DDMMYY (renderer expects no separators)
- Writes absolute https graphic_url (IG-safe)
- Preflights graphic_url is fetchable (HTTP 200 + image/*) before promoting
"""

from __future__ import annotations

import os
import json
import math
import datetime as dt
from typing import Any, Dict, List, Optional
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
# Google Sheets auth
# -------------------------

def _parse_sa_json(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()
    try:
        return json.loads(raw)
    except Exception:
        return json.loads(raw.replace("\\n", "\n"))


def get_ws() -> gspread.Worksheet:
    spreadsheet_id = must_env("SPREADSHEET_ID")
    tab = env("RAW_DEALS_TAB", "RAW_DEALS")

    raw = env("GCP_SA_JSON_ONE_LINE") or env("GCP_SA_JSON")
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
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(spreadsheet_id)
    return sh.worksheet(tab)


# -------------------------
# Helpers
# -------------------------

def iso_to_ddmmyy(iso_date: str) -> str:
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
        # If someone passed DDMMYY already, keep last 6 digits
        return digits[-6:] if len(digits) >= 6 else digits


def price_digits_only(raw: str) -> str:
    """
    Ensure numeric string (no £, commas). Rounded up.
    """
    s = (raw or "").strip().replace("£", "").replace("Â£", "").replace(",", "")
    try:
        v = float(s) if s else 0.0
    except Exception:
        v = 0.0
    return str(int(math.ceil(v)))


def normalize_render_url(render_url_raw: str) -> Dict[str, str]:
    """
    Accept:
      - https://domain
      - https://domain/
      - https://domain/api/render
      - https://domain/api/render/
    Returns:
      { "base": "https://domain", "render": "https://domain/api/render", "health": "https://domain/api/health" }
    """
    u = (render_url_raw or "").strip()
    if not u:
        die("Missing RENDER_URL")

    if not u.startswith("http://") and not u.startswith("https://"):
        u = "https://" + u

    u = u.rstrip("/")
    p = urlparse(u)

    base = urlunparse((p.scheme, p.netloc, "", "", "", "")).rstrip("/")

    # If user accidentally set base domain, force /api/render (prevents 405)
    if p.path.endswith("/api/render"):
        render_ep = u
    else:
        render_ep = base + "/api/render"

    health_ep = base + "/api/health"
    return {"base": base, "render": render_ep, "health": health_ep}


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
    """
    Must be publicly fetchable and be an image.
    """
    headers = {"User-Agent": "traveltxter-render-preflight/1.0"}
    r = requests.get(url, stream=True, timeout=30, headers=headers, allow_redirects=True)
    if r.status_code != 200:
        snippet = ""
        try:
            snippet = (r.text or "")[:200].replace("\n", " ")
        except Exception:
            snippet = ""
        raise RuntimeError(f"graphic_url not fetchable (HTTP {r.status_code}) :: {snippet}")

    ctype = (r.headers.get("Content-Type") or "").lower().strip()
    if not ctype.startswith("image/"):
        raise RuntimeError(f"graphic_url not an image (Content-Type={ctype})")


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
    ws = get_ws()
    max_rows = int(env("RENDER_MAX_ROWS", "1"))

    render_url_raw = must_env("RENDER_URL")
    eps = normalize_render_url(render_url_raw)
    base_url = eps["base"]
    render_ep = eps["render"]
    health_ep = eps["health"]

    log(f"Render base: {base_url}")
    log(f"Render endpoint: {render_ep}")

    # Best-effort healthcheck (non-fatal)
    try:
        r = requests.get(health_ep, timeout=10)
        log(f"Renderer healthcheck: {r.status_code}")
    except Exception as e:
        log(f"Renderer healthcheck failed (non-fatal): {e}")

    values = ws.get_all_values()
    if not values or len(values) < 2:
        log("RAW_DEALS empty. Nothing to do.")
        return 0

    headers = ensure_columns(ws, values[0], ["graphic_url", "render_error", "rendered_at"])
    values = ws.get_all_values()
    headers = values[0]

    h = {name.strip(): i for i, name in enumerate(headers)}

    def has_col(name: str) -> bool:
        return name in h

    def col(name: str) -> int:
        if name not in h:
            die(f"Missing required column in RAW_DEALS: {name}")
        return h[name]

    # Required input columns
    c_status = col("status")
    c_deal_id = col("deal_id")
    c_origin_city = col("origin_city")
    c_dest_city = col("destination_city")
    c_outbound = col("outbound_date")
    c_return = col("return_date")
    c_price = col("price_gbp")

    # Output columns (now ensured)
    c_graphic = col("graphic_url")
    c_render_error = col("render_error")
    c_rendered_at = col("rendered_at")

    rendered = 0

    for i in range(1, len(values)):
        if rendered >= max_rows:
            break

        row = values[i]
        sheet_row = i + 1

        status = (row[c_status] if c_status < len(row) else "").strip()
        if status != "READY_TO_POST":
            continue

        existing_url = (row[c_graphic] if c_graphic < len(row) else "").strip()
        if existing_url:
            # Already rendered; skip
            continue

        deal_id = (row[c_deal_id] if c_deal_id < len(row) else "").strip()
        origin_city = (row[c_origin_city] if c_origin_city < len(row) else "").strip()
        dest_city = (row[c_dest_city] if c_dest_city < len(row) else "").strip()
        out_iso = (row[c_outbound] if c_outbound < len(row) else "").strip()
        ret_iso = (row[c_return] if c_return < len(row) else "").strip()
        price_raw = (row[c_price] if c_price < len(row) else "").strip()

        payload = {
            # Canonical render payload (LOCKED)
            "TO": dest_city,
            "FROM": origin_city,
            "OUT": iso_to_ddmmyy(out_iso),
            "IN": iso_to_ddmmyy(ret_iso),
            "PRICE": price_digits_only(price_raw),

            # Optional identifier (harmless)
            "deal_id": deal_id,
        }

        log(f"🖼️ Rendering row {sheet_row} deal_id={deal_id} ({origin_city} -> {dest_city})")

        try:
            resp = requests.post(render_ep, json=payload, timeout=60)
        except Exception as e:
            ws.update([[f"Render request failed: {str(e)[:280]}"]], f"{chr(65+c_render_error)}{sheet_row}")
            continue

        if resp.status_code != 200:
            raw = (resp.text or "")[:400]
            ws.update([[f"Render HTTP {resp.status_code}: {raw}"]], f"{chr(65+c_render_error)}{sheet_row}")
            continue

        # JSON expected
        try:
            data = resp.json()
        except Exception:
            ws.update([[f"Render returned non-JSON: {(resp.text or '')[:280]}"]], f"{chr(65+c_render_error)}{sheet_row}")
            continue

        img = (data.get("graphic_url") or data.get("image_url") or "").strip()
        if not img:
            ws.update([[f"Render OK but missing graphic_url: {str(data)[:280]}"]], f"{chr(65+c_render_error)}{sheet_row}")
            continue

        img_abs = absolutise_url(img, base_url)

        # Preflight before writing/promoting
        try:
            preflight_public_image(img_abs)
        except Exception as e:
            ws.update([[f"Rendered URL failed preflight: {str(e)[:280]}"]], f"{chr(65+c_render_error)}{sheet_row}")
            continue

        # Write + promote
        ws.update([[img_abs]], f"{chr(65+c_graphic)}{sheet_row}")
        ws.update([[""]], f"{chr(65+c_render_error)}{sheet_row}")
        ws.update([[dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"]], f"{chr(65+c_rendered_at)}{sheet_row}")
        ws.update([["READY_TO_PUBLISH"]], f"{chr(65+c_status)}{sheet_row}")

        log(f"✅ graphic_url written + promoted READY_TO_PUBLISH ({img_abs})")
        rendered += 1

    log(f"Done. Rendered {rendered}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
