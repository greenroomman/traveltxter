#!/usr/bin/env python3
"""
TravelTxter V4.5x — render_client.py (HTTP render only)

Purpose:
- Read RAW_DEALS rows where status == READY_TO_POST
- Call PythonAnywhere render API via HTTP (RENDER_URL)
- Payload contract (LOCKED):
    {"TO": "<City>", "FROM": "<City>", "OUT": "ddmmyy", "IN": "ddmmyy", "PRICE": "£xxx"}
- Write:
    graphic_url
    rendered_timestamp
    render_error
    render_response_snippet
- Promote:
    READY_TO_POST -> READY_TO_PUBLISH   (only if graphic_url is valid)

Hard rules:
- Header-mapped writes only
- Never write placeholder URLs (no_id.png)
- Never promote if render fails

Env required:
- SPREADSHEET_ID
- GCP_SA_JSON_ONE_LINE
- RAW_DEALS_TAB (default RAW_DEALS)
- RENDER_URL (e.g. https://greenroomman.pythonanywhere.com/api/render)

Env optional:
- RENDER_MAX_ROWS (default 1)
- RENDER_TIMEOUT_SECONDS (default 45)
"""

from __future__ import annotations

import os
import json
import math
import datetime as dt
from typing import Dict, Any, List, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# Logging
# ============================================================

def utcnow() -> dt.datetime:
    return dt.datetime.utcnow()

def ts() -> str:
    return utcnow().replace(microsecond=0).isoformat() + "Z"

def log(msg: str) -> None:
    print(f"{ts()} | {msg}", flush=True)


# ============================================================
# Env
# ============================================================

def env_str(k: str, default: str = "") -> str:
    return os.environ.get(k, default).strip()

def env_int(k: str, default: int) -> int:
    try:
        return int(env_str(k, str(default)))
    except Exception:
        return default


# ============================================================
# Sheets
# ============================================================

def get_client() -> gspread.Client:
    sa = env_str("GCP_SA_JSON_ONE_LINE") or env_str("GCP_SA_JSON")
    if not sa:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE (recommended) or GCP_SA_JSON")

    try:
        info = json.loads(sa)
    except json.JSONDecodeError:
        info = json.loads(sa.replace("\\n", "\n"))

    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)

def col_letter(n1: int) -> str:
    s = ""
    n = n1
    while n:
        n, rr = divmod(n - 1, 26)
        s = chr(65 + rr) + s
    return s

def a1(rownum: int, col0: int) -> str:
    return f"{col_letter(col0 + 1)}{rownum}"


# ============================================================
# Contract formatting
# ============================================================

def ddmmyy(iso_date: str) -> str:
    """
    Input: YYYY-MM-DD
    Output: ddmmyy
    """
    s = (iso_date or "").strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        yyyy = s[0:4]
        mm = s[5:7]
        dd = s[8:10]
        return f"{dd}{mm}{yyyy[2:4]}"
    # last resort: strip non-digits and take last 6
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits[-6:] if len(digits) >= 6 else digits

def price_rounded_gbp(value: Any) -> str:
    """
    Uses price_gbp float (e.g. 65.48) and rounds up to whole pounds as required.
    """
    try:
        v = float(str(value).replace("£", "").strip())
        return f"£{int(math.ceil(v))}"
    except Exception:
        s = str(value or "").strip()
        if s.startswith("£"):
            return s
        return ""

def is_placeholder(url: str) -> bool:
    u = (url or "").strip().lower()
    return (not u) or ("no_id.png" in u)


# ============================================================
# Main
# ============================================================

def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    render_url = env_str("RENDER_URL")

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")
    if not render_url:
        raise RuntimeError("Missing RENDER_URL (e.g. https://<user>.pythonanywhere.com/api/render)")

    max_rows = env_int("RENDER_MAX_ROWS", 1)
    timeout_s = env_int("RENDER_TIMEOUT_SECONDS", 45)

    gc = get_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(tab)

    values = ws.get_all_values()
    if not values or len(values) < 2:
        log("Sheet empty. Nothing to render.")
        return 0

    headers = [h.strip() for h in values[0]]
    rows = values[1:]
    h = {name: i for i, name in enumerate(headers)}

    required = [
        "status",
        "origin_city", "destination_city",
        "outbound_date", "return_date",
        "price_gbp",
        "graphic_url", "rendered_timestamp", "render_error", "render_response_snippet",
    ]
    for c in required:
        if c not in h:
            raise RuntimeError(f"Missing required column in RAW_DEALS: {c}")

    # Find READY_TO_POST rows
    targets: List[int] = []
    for rownum, r in enumerate(rows, start=2):
        status = (r[h["status"]] if h["status"] < len(r) else "").strip().upper()
        if status != "READY_TO_POST":
            continue

        # If already rendered with a non-placeholder, skip
        existing = (r[h["graphic_url"]] if h["graphic_url"] < len(r) else "").strip()
        if existing and not is_placeholder(existing):
            continue

        targets.append(rownum)
        if len(targets) >= max_rows:
            break

    if not targets:
        log("No READY_TO_POST rows needing render.")
        return 0

    for rownum in targets:
        r = rows[rownum - 2]

        to_city = (r[h["destination_city"]] if h["destination_city"] < len(r) else "").strip()
        from_city = (r[h["origin_city"]] if h["origin_city"] < len(r) else "").strip()
        out_date = (r[h["outbound_date"]] if h["outbound_date"] < len(r) else "").strip()
        ret_date = (r[h["return_date"]] if h["return_date"] < len(r) else "").strip()
        price = (r[h["price_gbp"]] if h["price_gbp"] < len(r) else "").strip()

        payload = {
            "TO": to_city,
            "FROM": from_city,
            "OUT": ddmmyy(out_date),
            "IN": ddmmyy(ret_date),
            "PRICE": price_rounded_gbp(price),
        }

        # Clear prior render errors
        ws.batch_update([
            {"range": a1(rownum, h["render_error"]), "values": [[""]]},
            {"range": a1(rownum, h["render_response_snippet"]), "values": [[""]]},
        ])

        log(f"🖼️  Render row {rownum} -> {render_url}")
        log(f"➡️  Payload: {payload}")

        try:
            resp = requests.post(render_url, json=payload, timeout=timeout_s)
            txt = (resp.text or "")[:800]

            try:
                j = resp.json()
            except Exception:
                j = {"_raw": txt}

            image_url = (j.get("image_url") or j.get("graphic_url") or "").strip()
            success = bool(j.get("success", True))  # allow older services that omit 'success'

            if (not success) or is_placeholder(image_url):
                err = f"Render invalid: image_url={image_url or '(missing)'} status={resp.status_code}"
                ws.batch_update([
                    {"range": a1(rownum, h["graphic_url"]), "values": [[""]]},
                    {"range": a1(rownum, h["render_error"]), "values": [[err]]},
                    {"range": a1(rownum, h["render_response_snippet"]), "values": [[json.dumps(j)[:800]]]},
                ])
                log(f"❌ {err}")
                continue

            # Write results and promote
            updates = [
                {"range": a1(rownum, h["graphic_url"]), "values": [[image_url]]},
                {"range": a1(rownum, h["rendered_timestamp"]), "values": [[ts()]]},
                {"range": a1(rownum, h["render_response_snippet"]), "values": [[json.dumps(j)[:800]]]},
                {"range": a1(rownum, h["status"]), "values": [["READY_TO_PUBLISH"]]},
            ]
            ws.batch_update(updates)
            log(f"✅ Rendered row {rownum}: {image_url}")

        except Exception as e:
            ws.batch_update([
                {"range": a1(rownum, h["graphic_url"]), "values": [[""]]},
                {"range": a1(rownum, h["render_error"]), "values": [[str(e)[:300]]]},
            ])
            log(f"❌ Render exception row {rownum}: {e}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
