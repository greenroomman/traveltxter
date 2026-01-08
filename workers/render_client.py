#!/usr/bin/env python3
"""
workers/render_client.py

Renders the BEST eligible row by calling the PythonAnywhere renderer.

Key behavior:
- RENDER_URL may be either:
  (a) base domain: https://greenroomman.pythonanywhere.com
  (b) full endpoint: https://greenroomman.pythonanywhere.com/api/render
- We normalize to a POST endpoint at /api/render
- Healthcheck uses /api/health (best effort)
- Payload MUST be:
    TO: <City>
    FROM: <City>
    OUT: ddmmyy
    IN: ddmmyy
    PRICE: £xxx   (rounded up)
"""

import os
import math
import json
import datetime as dt
from typing import Dict, Any, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials


# ----------------------------
# Logging helpers
# ----------------------------
def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


def die(msg: str, code: int = 1) -> None:
    log(f"❌ {msg}")
    raise SystemExit(code)


# ----------------------------
# Env + Sheets auth
# ----------------------------
def get_env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    if v is None or str(v).strip() == "":
        die(f"Missing required env var: {name}")
    return str(v).strip()


def gs_client() -> gspread.Client:
    sa_json = get_env("GCP_SA_JSON_ONE_LINE")
    info = json.loads(sa_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def normalize_renderer_urls(render_url_raw: str) -> Tuple[str, str]:
    """
    Returns:
      (render_endpoint, health_endpoint)

    If render_url_raw already contains a path, we respect it but still enforce
    that render endpoint is /api/render.
    """
    base = render_url_raw.rstrip("/")

    # If user provided full endpoint, keep base domain from it.
    # e.g. https://x.com/api/render -> base_domain=https://x.com
    if "://" in base:
        # split off scheme://host[:port]
        scheme, rest = base.split("://", 1)
        host = rest.split("/", 1)[0]
        base_domain = f"{scheme}://{host}"
    else:
        base_domain = base  # unusual, but keep it

    # If they already provided /api/render explicitly, use it.
    if base.endswith("/api/render"):
        render_ep = base
    else:
        # If they provided some other path (e.g. /render), ignore and enforce /api/render
        render_ep = f"{base_domain}/api/render"

    health_ep = f"{base_domain}/api/health"
    return render_ep, health_ep


def a1_col(idx0: int) -> str:
    """0-based column index to A1 letters."""
    idx = idx0 + 1
    s = ""
    while idx:
        idx, r = divmod(idx - 1, 26)
        s = chr(65 + r) + s
    return s


def looks_like_iata(s: str) -> bool:
    t = (s or "").strip()
    return len(t) == 3 and t.isalpha() and t.upper() == t


def ddmmyy(date_str: str) -> str:
    """
    Accepts YYYY-MM-DD or ddmmyy already.
    """
    t = (date_str or "").strip()
    if len(t) == 6 and t.isdigit():
        return t
    # Expect ISO yyyy-mm-dd
    try:
        y, m, d = t.split("-")
        return f"{d}{m}{y[-2:]}"
    except Exception:
        return t  # fall back; renderer may reject, but we log clearly


# ----------------------------
# Main logic
# ----------------------------
def main() -> None:
    spreadsheet_id = get_env("SPREADSHEET_ID")
    raw_tab = os.getenv("RAW_DEALS_TAB", "RAW_DEALS").strip() or "RAW_DEALS"
    render_url_raw = get_env("RENDER_URL")
    render_max_rows = int(os.getenv("RENDER_MAX_ROWS", "1"))

    render_ep, health_ep = normalize_renderer_urls(render_url_raw)

    gc = gs_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(raw_tab)

    rows: List[List[Any]] = ws.get_all_values()
    if not rows or len(rows) < 2:
        die("RAW_DEALS is empty (no header + rows).")

    header = rows[0]
    h = {name.strip(): i for i, name in enumerate(header) if name.strip()}

    def col(name: str) -> int:
        if name not in h:
            die(f"Missing required column in {raw_tab}: {name}")
        return h[name]

    # Required fields for render payload
    c_status = col("status")
    c_deal_id = col("deal_id")
    c_price = col("price_gbp")
    c_origin_city = col("origin_city")
    c_dest_city = col("destination_city")
    c_out = col("outbound_date")
    c_ret = col("return_date")

    # Optional
    c_render_url = h.get("graphic_url")
    c_render_err = h.get("render_error")
    c_dest_country = h.get("destination_country")

    # Healthcheck (best effort)
    try:
        r = requests.get(health_ep, timeout=10)
        log(f"Renderer healthcheck: {r.status_code}")
    except Exception as e:
        log(f"Renderer healthcheck failed (non-fatal): {e}")

    # Pick BEST eligible row: READY_TO_POST first, else READY_TO_PUBLISH? (we only render READY_TO_POST)
    candidates: List[Tuple[int, List[Any]]] = []
    for i in range(1, len(rows)):
        row = rows[i]
        status = (row[c_status] if c_status < len(row) else "").strip()
        if status == "READY_TO_POST":
            candidates.append((i + 1, row))  # sheet row number (1-based)
    if not candidates:
        log("Done. Rendered 0. (No rows eligible: status=READY_TO_POST)")
        return

    # Render up to N rows (usually 1)
    rendered = 0

    for sheet_row_num, row in candidates[:render_max_rows]:
        deal_id = (row[c_deal_id] if c_deal_id < len(row) else "").strip()
        origin_city = (row[c_origin_city] if c_origin_city < len(row) else "").strip()
        dest_city = (row[c_dest_city] if c_dest_city < len(row) else "").strip()
        dest_country = ""
        if c_dest_country is not None and c_dest_country < len(row):
            dest_country = (row[c_dest_country] or "").strip()

        # Guard: never render IATA-as-city rows; better to fail fast than post garbage.
        if not origin_city or not dest_city or looks_like_iata(origin_city) or looks_like_iata(dest_city):
            msg = f"Blocked render: city fields invalid (origin_city='{origin_city}', destination_city='{dest_city}')"
            log(f"⛔ {msg} row {sheet_row_num} deal_id={deal_id}")

            # Mark error in-sheet if columns exist
            updates = []
            if c_render_err is not None:
                updates.append((c_render_err, msg))
            # Also move to BANKED to keep it out of publish path
            updates.append((c_status, "BANKED"))

            if updates:
                for col_idx, value in updates:
                    a1 = f"{a1_col(col_idx)}{sheet_row_num}"
                    ws.update([[value]], a1)
            continue

        out_iso = (row[c_out] if c_out < len(row) else "").strip()
        ret_iso = (row[c_ret] if c_ret < len(row) else "").strip()
        out_fmt = ddmmyy(out_iso)
        ret_fmt = ddmmyy(ret_iso)

        # Price formatting: £xxx rounded up
        try:
            price_val = float((row[c_price] if c_price < len(row) else "0").strip() or "0")
        except Exception:
            price_val = 0.0
        price_fmt = f"£{int(math.ceil(price_val))}"

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
            die(f"Render request failed: {e}")

        if resp.status_code == 404:
            # This is the exact failure you reported: wrong URL path
            die(
                "Render HTTP 404 (endpoint not found). "
                f"Tried POST to: {render_ep}. "
                "Fix your RENDER_URL to either:\n"
                "  https://greenroomman.pythonanywhere.com\n"
                "or\n"
                "  https://greenroomman.pythonanywhere.com/api/render"
            )

        if resp.status_code >= 400:
            # Save error message if possible
            raw = resp.text[:500]
            msg = f"Render HTTP {resp.status_code}: {raw}"
            log(f"❌ {msg}")
            if c_render_err is not None:
                ws.update([[msg]], f"{a1_col(c_render_err)}{sheet_row_num}")
            # keep status as READY_TO_POST so it can retry later (unless you prefer ERROR_HARD)
            continue

        data = resp.json() if "application/json" in resp.headers.get("content-type", "") else {}
        image_url = data.get("image_url") or data.get("graphic_url") or ""

        if not image_url:
            msg = f"Render OK but missing image_url. Response: {str(data)[:300]}"
            log(f"❌ {msg}")
            if c_render_err is not None:
                ws.update([[msg]], f"{a1_col(c_render_err)}{sheet_row_num}")
            continue

        # Write graphic_url + promote to READY_TO_PUBLISH
        if c_render_url is not None:
            ws.update([[image_url]], f"{a1_col(c_render_url)}{sheet_row_num}")
        ws.update([["READY_TO_PUBLISH"]], f"{a1_col(c_status)}{sheet_row_num}")

        log(f"✅ Rendered row {sheet_row_num} -> READY_TO_PUBLISH ({image_url})")
        rendered += 1

    log(f"Done. Rendered {rendered}.")


if __name__ == "__main__":
    main()
