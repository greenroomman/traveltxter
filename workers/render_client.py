#!/usr/bin/env python3
"""
workers/render_client.py  (PASTE-SAFE, HARDENED)

LOCKED PURPOSE (NO REINVENTION):
- Find RAW_DEALS rows where status == READY_TO_POST
- Call PythonAnywhere renderer
- Write graphic_url back to RAW_DEALS
- Promote status -> READY_TO_PUBLISH

HARDENING (so it doesn't break again):
1) Accepts RENDER_URL as either:
   - https://domain
   - https://domain/api/render
   - https://domain/render  (legacy)
   We auto-try sensible endpoints in order until one works.
2) Ensures columns exist:
   - graphic_url
   - render_error
3) Never promotes unless the returned image URL is publicly fetchable (HTTP 200 + Content-Type image/*)
4) Always stores an absolute https URL in graphic_url

Does NOT change your creative renderer in any way.
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


def candidate_render_endpoints(render_url_raw: str) -> Tuple[List[str], str]:
    """
    Returns (candidates, base_url)
    Candidates are tried in order until one works.
    """
    u = (render_url_raw or "").strip()
    if not u:
        die("Missing RENDER_URL")

    if not u.startswith("http://") and not u.startswith("https://"):
        u = "https://" + u
    u = u.rstrip("/")

    base = _base_from_any_url(u)
    p = urlparse(u)

    candidates: List[str] = []

    # If user already provided a specific endpoint path, try it first
    if p.path and p.path != "/":
        candidates.append(u)

    # Then try the two common ones we support
    candidates.append(base + "/api/render")
    candidates.append(base + "/render")  # legacy

    # De-dupe while preserving order
    seen = set()
    out = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            out.append(c)

    return out, base


def candidate_health_endpoints(base_url: str) -> List[str]:
    return [base_url + "/api/health", base_url + "/health"]


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


def try_render(endpoints: List[str], payload: Dict[str, Any]) -> Tuple[str, Dict[str, Any], int, str]:
    """
    Try POSTing to endpoints until we get a 2xx with a JSON graphic_url.
    Returns: (used_endpoint, json_data, status_code, response_text_snippet)
    Raises: RuntimeError if all attempts fail.
    """
    last_err = None
    last_status = 0
    last_snip = ""

    for ep in endpoints:
        try:
            log(f"➡️  Render endpoint try: {ep}")
            resp = requests.post(ep, json=payload, timeout=60)
            last_status = resp.status_code
            last_snip = (resp.text or "")[:300]
            if resp.status_code >= 400:
                last_err = RuntimeError(f"HTTP {resp.status_code}: {last_snip}")
                continue

            ctype = (resp.headers.get("content-type") or "")
            if "application/json" not in ctype.lower():
                last_err = RuntimeError(f"Expected JSON, got Content-Type={ctype} :: {last_snip}")
                continue

            data = resp.json()
            return ep, data, resp.status_code, last_snip

        except Exception as e:
            last_err = e
            continue

    raise RuntimeError(f"All render endpoints failed. Last status={last_status} :: {last_snip} :: {last_err}")


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

    endpoints, base_url = candidate_render_endpoints(render_url_raw)
    health_eps = candidate_health_endpoints(base_url)

    log(f"Renderer base_url: {base_url}")
    log(f"Renderer endpoints (in order): {endpoints}")

    # Best-effort healthcheck
    for hep in health_eps:
        try:
            r = requests.get(hep, timeout=10)
            log(f"Renderer healthcheck {hep}: {r.status_code}")
            break
        except Exception:
            continue

    gc = gs_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(raw_tab)

    values: List[List[str]] = ws.get_all_values()
    if not values or len(values) < 2:
        die("RAW_DEALS is empty (no header + rows).", 0)

    # Ensure required write columns exist
    headers = ensure_columns(ws, values[0], ["graphic_url", "render_error"])
    values = ws.get_all_values()
    headers = values[0]

    h = {name.strip(): i for i, name in enumerate(headers)}

    def col(name: str) -> int:
        if name not in h:
            die(f"Missing required column in RAW_DEALS: {name}")
        return h[name]

    # Required columns (contract)
    c_status = col("status")
    c_deal_id = col("deal_id")
    c_price = col("price_gbp")
    c_origin_city = col("origin_city")
    c_dest_city = col("destination_city")
    c_out = col("outbound_date")
    c_ret = col("return_date")

    # Write columns
    c_graphic = col("graphic_url")
    c_render_err = col("render_error")

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

        # Payload: include BOTH key styles to avoid future contract drift
        payload = {
            # Identifiers
            "deal_id": deal_id,

            # Uppercase contract (used in your current render_api support)
            "TO": dest_city,
            "FROM": origin_city,
            "OUT": out_fmt,
            "IN": ret_fmt,
            "PRICE": price_fmt,

            # Lowercase contract (legacy-safe)
            "to_city": dest_city,
            "from_city": origin_city,
            "out_date": out_fmt,
            "in_date": ret_fmt,
            "price": price_fmt,
        }

        log(f"🖼️  Rendering row {sheet_row_num} deal_id={deal_id} ({origin_city} -> {dest_city})")

        try:
            used_ep, data, _, _ = try_render(endpoints, payload)
        except Exception as e:
            ws.update([[f"Render failed: {str(e)[:280]}"]], a1_cell(c_render_err, sheet_row_num))
            log(f"❌ Render failed row {sheet_row_num}: {e}")
            continue

        # Accept either key name
        img_url = (data.get("graphic_url") or data.get("image_url") or "").strip()
        if not img_url:
            ws.update([[f"Render OK but missing graphic_url in JSON: {str(data)[:260]}"]], a1_cell(c_render_err, sheet_row_num))
            log(f"❌ Missing graphic_url row {sheet_row_num}")
            continue

        img_url_abs = absolutise_url(img_url, base_url)

        # Preflight must pass before we promote
        try:
            preflight_public_image(img_url_abs)
        except Exception as e:
            ws.update([[f"Rendered URL failed preflight: {str(e)[:260]}"]], a1_cell(c_render_err, sheet_row_num))
            log(f"❌ Preflight failed row {sheet_row_num}: {e}")
            continue

        # Write + promote
        ws.update([[img_url_abs]], a1_cell(c_graphic, sheet_row_num))
        ws.update([[""]], a1_cell(c_render_err, sheet_row_num))
        ws.update([["READY_TO_PUBLISH"]], a1_cell(c_status, sheet_row_num))

        log(f"✅ Rendered row {sheet_row_num} -> READY_TO_PUBLISH via {used_ep}")
        rendered += 1

        if rendered >= max_rows:
            break

    log(f"Done. Rendered {rendered}.")


if __name__ == "__main__":
    main()
