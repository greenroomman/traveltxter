#!/usr/bin/env python3
"""
workers/render_client.py

Render Client — V4.6.3 STATUS PROMOTION HOTFIX
FULL REPLACEMENT (per protocol)

What this fixes:
- Keeps the renderer payload contract EXACTLY the same (FROM/TO/OUT/IN/PRICE)
- Keeps PythonAnywhere renderer untouched
- After successful render + graphic_url write:
  - Promote status READY_TO_POST -> READY_TO_PUBLISH
  - (If already READY_TO_PUBLISH, leave as-is)

LOCKED BEHAVIOUR:
- Google Sheets is the single source of truth
- RAW_DEALS_VIEW is never written to
- Renderer is stateless
- ALWAYS prioritise newest eligible deals (fresh-first)
- No schema changes, no workflow changes
"""

import os
import json
import time
import math
import re
import datetime as dt
import requests
import gspread
from google.oauth2.service_account import Credentials

try:
    from dateutil import parser as date_parser
except Exception:
    date_parser = None


# ==================== ENV ====================

SPREADSHEET_ID = (os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID") or "").strip()
RAW_DEALS_TAB = (os.getenv("RAW_DEALS_TAB", "RAW_DEALS") or "RAW_DEALS").strip()

RENDER_URL = (os.getenv("RENDER_URL") or "").strip()

def _get_int_env(name: str, default: int) -> int:
    v = (os.getenv(name) or "").strip()
    if not v:
        return int(default)
    try:
        return int(float(v))
    except Exception:
        return int(default)

RENDER_MAX_ROWS = _get_int_env("RENDER_MAX_ROWS", 1)
RUN_SLOT = (os.getenv("RUN_SLOT", "UNKNOWN") or "UNKNOWN").strip()

# Rows eligible for rendering (we keep READY_TO_POST included for compatibility)
ELIGIBLE_STATUSES = {
    "READY_TO_PUBLISH",
    "READY_TO_POST",
}

# Promotion rule (the missing hop in your cascade)
PROMOTE_FROM = "READY_TO_POST"
PROMOTE_TO = "READY_TO_PUBLISH"


# ==================== LOGGING ====================

def log(msg: str):
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


# ==================== GOOGLE SHEETS ====================

def parse_sa_json(raw: str) -> dict:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return json.loads(raw.replace("\\n", "\n"))


def gs_client():
    raw = (os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON") or "").strip()
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON")
    info = parse_sa_json(raw)
    creds = Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


# ==================== HELPERS ====================

def parse_utc(ts: str):
    try:
        return dt.datetime.fromisoformat(ts.replace("Z", ""))
    except Exception:
        return None


def norm_header(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").strip().lower())


def get_first_value(row: list, idx_norm: dict, *candidates: str) -> str:
    for c in candidates:
        key = norm_header(c)
        if key in idx_norm:
            val = (row[idx_norm[key]] or "").strip()
            if val:
                return val
    return ""


def normalise_date_to_ddmmyy(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""

    if re.fullmatch(r"\d{6}", s):
        return s

    m = re.fullmatch(r"(\d{1,2})[\/\-\.\s](\d{1,2})[\/\-\.\s](\d{2,4})", s)
    if m:
        d = int(m.group(1))
        mo = int(m.group(2))
        y = int(m.group(3))
        if y < 100:
            y += 2000
        try:
            dt_obj = dt.date(y, mo, d)
            return dt_obj.strftime("%d%m%y")
        except Exception:
            return ""

    m = re.fullmatch(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if m:
        y = int(m.group(1))
        mo = int(m.group(2))
        d = int(m.group(3))
        try:
            dt_obj = dt.date(y, mo, d)
            return dt_obj.strftime("%d%m%y")
        except Exception:
            return ""

    if date_parser:
        try:
            dt_obj = date_parser.parse(s).date()
            return dt_obj.strftime("%d%m%y")
        except Exception:
            return ""

    return ""


def normalise_price_to_pounds_rounded(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""

    m = re.search(r"(\d+(?:\.\d+)?)", s.replace(",", ""))
    if not m:
        return ""

    try:
        val = float(m.group(1))
        pounds = int(math.ceil(val))
        return f"£{pounds}"
    except Exception:
        return ""


def build_render_payload(row: list, idx_norm: dict) -> dict:
    from_city = get_first_value(
        row, idx_norm,
        "from_city", "origin_city", "origin_city_name", "origin", "from", "departure_city"
    )
    to_city = get_first_value(
        row, idx_norm,
        "to_city", "destination_city", "destination_city_name", "destination", "to", "arrival_city"
    )

    out_raw = get_first_value(
        row, idx_norm,
        "out_date", "depart_date", "departure_date", "outbound_date", "depart", "out"
    )
    in_raw = get_first_value(
        row, idx_norm,
        "in_date", "return_date", "inbound_date", "return", "in"
    )

    price_raw = get_first_value(
        row, idx_norm,
        "price", "price_gbp", "total_price_gbp", "gbp_price", "price_total", "price_total_gbp"
    )

    out_ddmmyy = normalise_date_to_ddmmyy(out_raw)
    in_ddmmyy = normalise_date_to_ddmmyy(in_raw)
    price_fmt = normalise_price_to_pounds_rounded(price_raw)

    return {
        "FROM": from_city,
        "TO": to_city,
        "OUT": out_ddmmyy,
        "IN": in_ddmmyy,
        "PRICE": price_fmt,
    }


def payload_is_complete(p: dict) -> bool:
    return all((p.get("FROM"), p.get("TO"), p.get("OUT"), p.get("IN"), p.get("PRICE")))


# ==================== MAIN ====================

def main():
    log("=" * 60)
    log(f"🖼️ Render Client starting | RUN_SLOT={RUN_SLOT}")
    log("=" * 60)

    if not SPREADSHEET_ID or not RENDER_URL:
        raise RuntimeError("Missing SPREADSHEET_ID/SHEET_ID or RENDER_URL")

    gc = gs_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(RAW_DEALS_TAB)

    headers = ws.row_values(1)
    rows = ws.get_all_values()[1:]

    idx_norm = {norm_header(h): i for i, h in enumerate(headers)}

    # Required baseline columns
    for col in ("status", "ingested_at_utc", "graphic_url"):
        if norm_header(col) not in idx_norm:
            raise RuntimeError(f"Missing required column: {col}")

    status_col = idx_norm[norm_header("status")] + 1
    ingested_col = idx_norm[norm_header("ingested_at_utc")] + 1
    graphic_col = idx_norm[norm_header("graphic_url")] + 1

    eligible = []
    for i, row in enumerate(rows, start=2):
        status = (row[status_col - 1] or "").strip()
        graphic_url = (row[graphic_col - 1] or "").strip()
        ingested = (row[ingested_col - 1] or "").strip()

        if status not in ELIGIBLE_STATUSES:
            continue
        if graphic_url:
            continue

        ts = parse_utc(ingested)
        eligible.append({"row_num": i, "ingested_at": ts})

    if not eligible:
        log("No eligible rows to render.")
        return 0

    eligible.sort(
        key=lambda r: (r["ingested_at"] or dt.datetime.min, r["row_num"]),
        reverse=True,
    )

    to_render = eligible[:RENDER_MAX_ROWS]
    log(f"Eligible rows: {len(eligible)} | Rendering: {len(to_render)}")

    for item in to_render:
        row_num = item["row_num"]
        log(f"🖼️ Rendering row {row_num}")

        row = rows[row_num - 2]
        payload = build_render_payload(row, idx_norm)

        log(
            "Render payload: "
            f"FROM='{payload.get('FROM')}' "
            f"TO='{payload.get('TO')}' "
            f"OUT='{payload.get('OUT')}' "
            f"IN='{payload.get('IN')}' "
            f"PRICE='{payload.get('PRICE')}'"
        )

        if not payload_is_complete(payload):
            log(f"❌ Skipping row {row_num}: payload incomplete (missing city/date/price fields)")
            continue

        r = requests.post(RENDER_URL, json=payload, timeout=60)

        if r.status_code != 200:
            log(f"❌ Render failed row {row_num}: HTTP {r.status_code}")
            continue

        try:
            graphic_url = (r.json() or {}).get("graphic_url")
        except Exception:
            graphic_url = None

        if not graphic_url:
            log(f"❌ No graphic_url returned for row {row_num}")
            continue

        # 1) Write graphic_url (existing behavior)
        graphic_cell = gspread.utils.rowcol_to_a1(row_num, graphic_col)
        ws.update([[graphic_url]], graphic_cell)
        log(f"✅ Wrote graphic_url for row {row_num}")

        # 2) Promote status READY_TO_POST -> READY_TO_PUBLISH (missing hop)
        current_status = (row[status_col - 1] or "").strip()
        if current_status == PROMOTE_FROM:
            status_cell = gspread.utils.rowcol_to_a1(row_num, status_col)
            ws.update([[PROMOTE_TO]], status_cell)
            log(f"✅ Promoted status {PROMOTE_FROM} -> {PROMOTE_TO} for row {row_num}")
        else:
            log(f"ℹ️ Status not promoted (current_status='{current_status}') for row {row_num}")

        time.sleep(1)

    log("Render cycle complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
