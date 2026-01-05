#!/usr/bin/env python3
import os, json, math
import datetime as dt
from typing import Any, Dict, List, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials

def now_utc_str() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def log(msg: str) -> None:
    print(f"{now_utc_str()} | {msg}", flush=True)

def env_str(name: str, default: str="") -> str:
    return (os.getenv(name) or default).strip()

def a1_update(ws: gspread.Worksheet, a1: str, value: Any) -> None:
    ws.update([[value]], a1)

def col_letter(n: int) -> str:
    s=""
    while n:
        n,r = divmod(n-1,26)
        s=chr(65+r)+s
    return s

def a1_for(row: int, col0: int) -> str:
    return f"{col_letter(col0+1)}{row}"

def get_client() -> gspread.Client:
    sa = env_str("GCP_SA_JSON_ONE_LINE")
    info = json.loads(sa)
    creds = Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return gspread.authorize(creds)

def ensure_columns(ws: gspread.Worksheet, required: List[str]) -> Dict[str,int]:
    headers = ws.row_values(1)
    changed=False
    for c in required:
        if c not in headers:
            headers.append(c); changed=True
    if changed:
        ws.update([headers], "A1")
    return {h:i for i,h in enumerate(headers)}

def to_ddmmyy(date_iso: str) -> str:
    d = dt.date.fromisoformat(date_iso)
    return d.strftime("%d%m%y")

def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    raw_tab = env_str("RAW_DEALS_TAB","RAW_DEALS")
    render_url = env_str("RENDER_URL")
    if not (spreadsheet_id and render_url):
        raise RuntimeError("Missing SPREADSHEET_ID or RENDER_URL")

    log("============================================================")
    log("🖼️ Render worker starting (locked payload contract)")
    log("============================================================")

    gc = get_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(raw_tab)

    cols = [
        "status","destination_city","destination_iata","origin_city","origin_iata",
        "outbound_date","return_date","price_gbp",
        "graphic_url","rendered_timestamp","render_error","render_response_snippet"
    ]
    hm = ensure_columns(ws, cols)

    rows = ws.get_all_values()
    if len(rows) <= 1:
        log("No rows.")
        return 0

    def get(r: List[str], col: str) -> str:
        idx = hm[col]
        return r[idx].strip() if idx < len(r) else ""

    updated = 0
    for i, row in enumerate(rows[1:], start=2):
        status = get(row,"status").upper()
        if status != "READY_TO_PUBLISH":
            continue
        if get(row,"graphic_url"):
            continue

        to_city = get(row,"destination_city") or get(row,"destination_iata")
        from_city = get(row,"origin_city") or get(row,"origin_iata")
        out_iso = get(row,"outbound_date")
        in_iso = get(row,"return_date")
        price = float(get(row,"price_gbp") or "0")

        if not (to_city and from_city and out_iso and in_iso and price > 0):
            continue

        out_dd = to_ddmmyy(out_iso)
        in_dd  = to_ddmmyy(in_iso)
        price_rounded = int(math.ceil(price))

        payload_text = "\n".join([
            f"TO: {to_city}",
            f"FROM: {from_city}",
            f"OUT: {out_dd}",
            f"IN: {in_dd}",
            f"PRICE: £{price_rounded}",
        ])

        try:
            r = requests.post(render_url, json={"text": payload_text}, timeout=45)
            if r.status_code >= 300:
                a1_update(ws, a1_for(i, hm["render_error"]), f"HTTP {r.status_code}")
                a1_update(ws, a1_for(i, hm["render_response_snippet"]), r.text[:200])
                continue
            data = r.json()
            graphic_url = data.get("graphic_url") or data.get("url") or ""
            if not graphic_url:
                a1_update(ws, a1_for(i, hm["render_error"]), "No graphic_url in response")
                a1_update(ws, a1_for(i, hm["render_response_snippet"]), json.dumps(data)[:200])
                continue

            a1_update(ws, a1_for(i, hm["graphic_url"]), graphic_url)
            a1_update(ws, a1_for(i, hm["rendered_timestamp"]), now_utc_str())
            updated += 1
            log(f"✅ Rendered row {i} -> graphic_url set")
            break  # max 1 per run (keeps output stable)
        except Exception as e:
            a1_update(ws, a1_for(i, hm["render_error"]), str(e)[:200])

    log(f"Done. Rendered {updated} row(s).")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
