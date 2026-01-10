#!/usr/bin/env python3
"""
workers/render_client.py

LOCKED ROLE:
- Consumes: status == READY_TO_POST
- Calls:    RENDER_URL endpoint (PythonAnywhere renderer)
- Writes:   graphic_url
- Promotes: READY_TO_POST -> READY_TO_PUBLISH

CRITICAL RENDER PAYLOAD CONTRACT (LOCKED):
TO: <City>
FROM: <City>
OUT: ddmmyy
IN: ddmmyy
PRICE: £xxx

This patch fixes:
1) TO city showing as "MEXICO (CANCUN)" -> sends "CANCUN"
2) PRICE showing as "££660" -> sends "£660" (exactly one £)
"""

from __future__ import annotations

import os
import json
import time
import math
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials


# -----------------------------
# Logging
# -----------------------------

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


# -----------------------------
# Env
# -----------------------------

def env_str(k: str, default: str = "") -> str:
    return (os.environ.get(k, default) or "").strip()

def env_any(keys: List[str], default: str = "") -> str:
    for k in keys:
        v = env_str(k, "")
        if v:
            return v
    return default


# -----------------------------
# Sheets auth
# -----------------------------

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


# -----------------------------
# A1 helpers
# -----------------------------

def col_letter(n1: int) -> str:
    s = ""
    n = n1
    while n:
        n, rr = divmod(n - 1, 26)
        s = chr(65 + rr) + s
    return s

def a1(rownum: int, col0: int) -> str:
    return f"{col_letter(col0 + 1)}{rownum}"

def safe_get(row: List[str], idx: int) -> str:
    return row[idx].strip() if 0 <= idx < len(row) else ""


# -----------------------------
# Normalisers (LOCKED FIXES)
# -----------------------------

def normalise_to_city(raw: str) -> str:
    """
    Fixes cases like:
      - 'MEXICO (CANCUN)' -> 'CANCUN'
      - 'CANCUN, MEXICO'  -> 'CANCUN'
      - 'MEXICO - CANCUN' -> 'CANCUN' (best guess)
    """
    s = (raw or "").strip()
    if not s:
        return ""

    # If "(CITY)" exists, prefer inside brackets
    if "(" in s and ")" in s:
        inside = s.split("(", 1)[1].split(")", 1)[0].strip()
        if inside:
            return inside.upper()

    # If "CITY, COUNTRY", take left side
    if "," in s:
        left = s.split(",", 1)[0].strip()
        if left:
            return left.upper()

    # If "COUNTRY - CITY", take right side
    if " - " in s:
        right = s.split(" - ", 1)[1].strip()
        if right:
            return right.upper()

    return s.upper()

def normalise_from_city(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    # Keep origin as nice title-case (London, Edinburgh etc)
    return " ".join([w[:1].upper() + w[1:].lower() for w in s.split()])

def ddmmyy(iso_date: str) -> str:
    """
    Input: '2026-03-01' -> '010326'
    """
    s = (iso_date or "").strip()
    if not s:
        return ""
    try:
        d = dt.date.fromisoformat(s)
        return d.strftime("%d%m%y")
    except Exception:
        return s  # don’t crash if format is odd

def normalise_price_for_renderer(raw: str) -> str:
    """
    Output must be EXACTLY '£660' (one £ only), integer, rounded up.
    Accepts: '£660', '££660', '660', '660.12', '£660.12'
    """
    s = (raw or "").strip()
    if not s:
        return "£?"

    # strip all pound signs and commas
    s2 = s.replace("£", "").replace(",", "").strip()

    try:
        v = float(s2)
        v_int = int(math.ceil(v))
        return f"£{v_int}"
    except Exception:
        # If it’s not numeric, at least collapse double £
        s = s.replace("££", "£")
        if not s.startswith("£") and "£" in raw:
            s = "£" + s2
        return s


# -----------------------------
# Renderer call
# -----------------------------

def render_image(render_url: str, payload_text: str) -> str:
    """
    Expects renderer returns JSON with 'graphic_url' or 'url'.
    """
    url = render_url.rstrip("/")
    r = requests.post(url, json={"text": payload_text}, timeout=90)
    if r.status_code >= 400:
        raise RuntimeError(f"Renderer error {r.status_code}: {r.text[:400]}")
    j = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
    graphic_url = (j.get("graphic_url") or j.get("url") or "").strip()
    if not graphic_url:
        raise RuntimeError(f"Renderer response missing graphic_url: {str(j)[:400]}")
    return graphic_url


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    spreadsheet_id = env_any(["SPREADSHEET_ID", "SHEET_ID"])
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    render_url = env_str("RENDER_URL")

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID (or SHEET_ID)")
    if not render_url:
        raise RuntimeError("Missing RENDER_URL")

    gc = gs_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(tab)

    values = ws.get_all_values()
    if len(values) < 2:
        log("No rows.")
        return 0

    headers = [h.strip() for h in values[0]]
    h = {name: i for i, name in enumerate(headers)}

    required = [
        "status", "deal_id",
        "origin_city", "destination_city",
        "outbound_date", "return_date",
        "price_gbp",
        "graphic_url", "rendered_at",
    ]
    missing = [c for c in required if c not in headers]
    if missing:
        headers = headers + missing
        ws.update([headers], "A1")
        h = {name: i for i, name in enumerate(headers)}
        log(f"🛠️ Added missing columns: {missing}")

    # Pick first READY_TO_POST row (keep behaviour simple & deterministic)
    rows = values[1:]
    for rownum, r in enumerate(rows, start=2):
        status = safe_get(r, h["status"]).strip().upper()
        if status != "READY_TO_POST":
            continue

        deal_id = safe_get(r, h["deal_id"])
        origin = safe_get(r, h["origin_city"])
        dest = safe_get(r, h["destination_city"])
        out_d = safe_get(r, h["outbound_date"])
        ret_d = safe_get(r, h["return_date"])
        price_raw = safe_get(r, h["price_gbp"])

        to_city = normalise_to_city(dest)
        from_city = normalise_from_city(origin)
        out_fmt = ddmmyy(out_d)
        in_fmt = ddmmyy(ret_d)
        price_fmt = normalise_price_for_renderer(price_raw)

        payload = "\n".join([
            f"TO: {to_city}",
            f"FROM: {from_city}",
            f"OUT: {out_fmt}",
            f"IN: {in_fmt}",
            f"PRICE: {price_fmt}",
        ])

        log(f"🖼️  Rendering row {rownum} deal_id={deal_id} ({from_city} -> {to_city})")
        graphic_url = render_image(render_url, payload)

        ws.update([[graphic_url]], a1(rownum, h["graphic_url"]))
        ws.update([[dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"]], a1(rownum, h["rendered_at"]))
        ws.update([["READY_TO_PUBLISH"]], a1(rownum, h["status"]))

        log(f"✅ Rendered graphic_url set for row {rownum}")
        return 0

    log("Done. Rendered 0.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
