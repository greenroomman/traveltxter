#!/usr/bin/env python3
import os
import json
import math
import datetime as dt
from typing import Any, Dict, List, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError


def now_utc_str() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def log(msg: str) -> None:
    print(f"{now_utc_str()} | {msg}", flush=True)


def env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def col_letter(n1: int) -> str:
    s = ""
    n = n1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def a1(row: int, col0: int) -> str:
    return f"{col_letter(col0 + 1)}{row}"


def batch_update_with_backoff(ws: gspread.Worksheet, data: List[Dict[str, Any]], tries: int = 6) -> None:
    delay = 1.0
    for i in range(tries):
        try:
            ws.batch_update(data)
            return
        except APIError as e:
            if "429" in str(e) or "Quota exceeded" in str(e):
                log(f"⚠️ Sheets quota 429. Backoff {delay:.1f}s")
                import time
                time.sleep(delay)
                delay = min(delay * 2, 20.0)
                continue
            raise


def get_client() -> gspread.Client:
    sa = env("GCP_SA_JSON_ONE_LINE")
    if not sa:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE")
    info = json.loads(sa)
    creds = Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return gspread.authorize(creds)


def ensure_cols(ws: gspread.Worksheet, needed: List[str]) -> Dict[str, int]:
    headers = ws.row_values(1)
    changed = False
    for c in needed:
        if c not in headers:
            headers.append(c)
            changed = True
    if changed:
        ws.update([headers], "A1")
    return {h: i for i, h in enumerate(headers)}


def ddmmyy(date_iso: str) -> str:
    # input: YYYY-MM-DD -> ddmmyy
    y, m, d = date_iso.split("-")
    return f"{d}{m}{y[-2:]}"


def main() -> int:
    log("============================================================")
    log("🖼️ Render worker starting (locked payload contract)")
    log("============================================================")

    sid = env("SPREADSHEET_ID")
    tab = env("RAW_DEALS_TAB", "RAW_DEALS")
    render_url = env("RENDER_URL")
    if not sid or not render_url:
        raise RuntimeError("Missing SPREADSHEET_ID or RENDER_URL")

    gc = get_client()
    sh = gc.open_by_key(sid)
    ws = sh.worksheet(tab)

    hm = ensure_cols(ws, [
        "status",
        "origin_city", "destination_city",
        "outbound_date", "return_date",
        "price_gbp",
        "graphic_url",
        "rendered_timestamp",
        "render_error",
        "render_response_snippet",
    ])

    rows = ws.get_all_values()
    if len(rows) <= 1:
        log("No rows.")
        return 0

    # Pick the ONE row that is READY_TO_PUBLISH and missing graphic_url
    target_row = None
    for i, r in enumerate(rows[1:], start=2):
        status = (r[hm["status"]] if hm["status"] < len(r) else "").strip().upper()
        graphic = (r[hm["graphic_url"]] if hm["graphic_url"] < len(r) else "").strip()
        if status == "READY_TO_PUBLISH" and not graphic:
            target_row = (i, r)
            break

    if not target_row:
        log("No READY_TO_PUBLISH row found needing render. (Nothing to do)")
        return 0

    sheet_row, r = target_row
    to_city = (r[hm["destination_city"]] if hm["destination_city"] < len(r) else "").strip()
    from_city = (r[hm["origin_city"]] if hm["origin_city"] < len(r) else "").strip()
    out_date = (r[hm["outbound_date"]] if hm["outbound_date"] < len(r) else "").strip()
    in_date = (r[hm["return_date"]] if hm["return_date"] < len(r) else "").strip()
    price_raw = (r[hm["price_gbp"]] if hm["price_gbp"] < len(r) else "").strip()

    if not (to_city and from_city and out_date and in_date and price_raw):
        msg = "Missing required fields for render (need destination_city, origin_city, outbound_date, return_date, price_gbp)."
        log(f"❌ Row {sheet_row}: {msg}")
        batch_update_with_backoff(ws, [{
            "range": a1(sheet_row, hm["render_error"]),
            "values": [[msg]]
        }])
        return 0

    try:
        price_gbp = float(price_raw)
    except Exception:
        price_gbp = None

    if price_gbp is None:
        msg = f"Invalid price_gbp: {price_raw}"
        log(f"❌ Row {sheet_row}: {msg}")
        batch_update_with_backoff(ws, [{
            "range": a1(sheet_row, hm["render_error"]),
            "values": [[msg]]
        }])
        return 0

    payload = {
        "TO": to_city,
        "FROM": from_city,
        "OUT": ddmmyy(out_date),
        "IN": ddmmyy(in_date),
        "PRICE": f"£{int(math.ceil(price_gbp))}",
    }

    log(f"Rendering row {sheet_row} payload={payload}")

    try:
        resp = requests.post(render_url, json=payload, timeout=60)
        snippet = (resp.text or "")[:240]
        resp.raise_for_status()
        data = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
        graphic_url = data.get("graphic_url") or data.get("url") or ""
        if not graphic_url:
            raise RuntimeError(f"Renderer returned no graphic_url. snippet={snippet}")

        batch_update_with_backoff(ws, [
            {"range": a1(sheet_row, hm["graphic_url"]), "values": [[graphic_url]]},
            {"range": a1(sheet_row, hm["rendered_timestamp"]), "values": [[now_utc_str()]]},
            {"range": a1(sheet_row, hm["render_error"]), "values": [[""]]},
            {"range": a1(sheet_row, hm["render_response_snippet"]), "values": [[snippet]]},
        ])
        log(f"✅ Rendered row {sheet_row} -> graphic_url set")
        return 0

    except Exception as e:
        err = str(e)[:400]
        log(f"❌ Render failed row {sheet_row}: {err}")
        batch_update_with_backoff(ws, [
            {"range": a1(sheet_row, hm["render_error"]), "values": [[err]]},
            {"range": a1(sheet_row, hm["render_response_snippet"]), "values": [[(locals().get("snippet") or "")[:240]]]},
        ])
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
