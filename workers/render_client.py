#!/usr/bin/env python3
"""
workers/render_client.py (FULL REPLACEMENT — V4.6)

LOCKED PURPOSE:
- Select RAW_DEALS rows where status == READY_TO_POST
- PRIORITISE NEWEST deals (ingested_at_utc DESC, then row number DESC)
- Call PythonAnywhere renderer (POST /api/render)
- Write graphic_url back to RAW_DEALS
- Promote status -> READY_TO_PUBLISH

IMPORTANT BEHAVIOUR CHANGE (INTENTIONAL & LOCKED):
- Selection is NO LONGER sheet-order / top-down
- Selection is ALWAYS freshness-first
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
# Env helpers
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


def numeric_price_only(raw: str) -> str:
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
    if not u.startswith("http://") and not u.startswith("https://"):
        u = "https://" + u
    u = u.rstrip("/")
    p = urlparse(u)
    return urlunparse((p.scheme, p.netloc, "", "", "", "")).rstrip("/")


def normalize_renderer(render_url_raw: str) -> Tuple[str, str, str]:
    base = _base_from_any_url(render_url_raw)
    if not base:
        die("Missing RENDER_URL")
    return base + "/api/render", base + "/api/health", base


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
    r = requests.get(url, stream=True, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"graphic_url not fetchable (HTTP {r.status_code})")
    ctype = (r.headers.get("Content-Type") or "").lower().strip()
    if not ctype.startswith("image/"):
        raise RuntimeError(f"graphic_url not an image (Content-Type={ctype})")


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

    try:
        r = requests.get(health_ep, timeout=10)
        log(f"Renderer healthcheck: {r.status_code}")
    except Exception:
        log("Renderer healthcheck failed (non-fatal)")

    gc = gs_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(raw_tab)

    values = ws.get_all_values()
    if len(values) < 2:
        log("RAW_DEALS empty.")
        return 0

    headers = [h.strip() for h in values[0]]
    h = {name: i for i, name in enumerate(headers)}

    required = [
        "status",
        "deal_id",
        "origin_city",
        "destination_city",
        "outbound_date",
        "return_date",
        "price_gbp",
        "graphic_url",
        "ingested_at_utc",
    ]
    for c in required:
        if c not in h:
            die(f"Missing required column: {c}")

    candidates: List[Tuple[str, int]] = []

    for i in range(1, len(values)):
        row = values[i]
        if row[h["status"]] != "READY_TO_POST":
            continue
        if row[h["graphic_url"]].strip():
            continue
        ts = row[h["ingested_at_utc"]].strip()
        candidates.append((ts, i + 1))

    if not candidates:
        log("No eligible rows to render.")
        return 0

    candidates.sort(
        key=lambda x: (
            dt.datetime.fromisoformat(x[0].replace("Z", "")) if x[0] else dt.datetime.min,
            x[1],
        ),
        reverse=True,
    )

    rendered = 0

    for ts, sheet_row in candidates:
        if rendered >= max_rows:
            break

        row = values[sheet_row - 1]

        payload = {
            "deal_id": row[h["deal_id"]],
            "TO": row[h["destination_city"]],
            "FROM": row[h["origin_city"]],
            "OUT": ddmmyy(row[h["outbound_date"]]),
            "IN": ddmmyy(row[h["return_date"]]),
            "PRICE": numeric_price_only(row[h["price_gbp"]]),
        }

        log(f"🖼️ Rendering row {sheet_row} (ingested_at_utc={ts})")

        resp = requests.post(render_ep, json=payload, timeout=90)
        if resp.status_code >= 400:
            ws.update([[f"Render HTTP {resp.status_code}"]], a1_cell(h["graphic_url"], sheet_row))
            continue

        data = resp.json()
        img_url = absolutise_url(data.get("graphic_url", ""), base_url)
        preflight_public_image(img_url)

        ws.update([[img_url]], a1_cell(h["graphic_url"], sheet_row))
        ws.update([["READY_TO_PUBLISH"]], a1_cell(h["status"], sheet_row))

        rendered += 1
        log(f"✅ Rendered row {sheet_row}")

    log(f"Done. Rendered {rendered}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
