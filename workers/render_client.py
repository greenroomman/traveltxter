#!/usr/bin/env python3
"""
workers/render_client.py

Render Client — V4.6.2 HOTFIX
FULL REPLACEMENT (per protocol)

FIX:
- Send the LOCKED renderer payload (FROM/TO/OUT/IN/PRICE) instead of only row_number
- Robust field extraction from RAW_DEALS using tolerant header matching
- Date normalisation to ddmmyy
- Price normalisation to £xxx (rounded up)

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

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS")
RENDER_URL = os.getenv("RENDER_URL")
RENDER_MAX_ROWS = int(os.getenv("RENDER_MAX_ROWS", "1") or "1")
RUN_SLOT = os.getenv("RUN_SLOT", "UNKNOWN")

ELIGIBLE_STATUSES = {
    "READY_TO_PUBLISH",
    "READY_TO_POST",
}


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
    raw = os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON")
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
    """
    Return the first non-empty cell value for any header candidate.
    candidates are raw header names; we normalise for matching.
    """
    for c in candidates:
        key = norm_header(c)
        if key in idx_norm:
            val = (row[idx_norm[key]] or "").strip()
            if val:
                return val
    return ""


def normalise_date_to_ddmmyy(raw: str) -> str:
    """
    Accepts:
    - ddmmyy
    - dd/mm/yyyy or dd-mm-yyyy
    - yyyy-mm-dd
    - ISO timestamps
    Outputs: ddmmyy (e.g., 260226)
    """
    s = (raw or "").strip()
    if not s:
        return ""

    # already ddmmyy
    if re.fullmatch(r"\d{6}", s):
        return s

    # dd/mm/yyyy or dd-mm-yyyy or dd.mm.yyyy
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

    # yyyy-mm-dd
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

    # dateutil fallback if available
    if date_parser:
        try:
            dt_obj = date_parser.parse(s).date()
            return dt_obj.strftime("%d%m%y")
        except Exception:
            return ""

    return ""


def normalise_price_to_pounds_rounded(raw: str) -> str:
    """
    Accepts: '123', '123.45', '£123', 'GBP 123.45', etc.
    Outputs: '£123' with rounding UP to whole pounds.
    """
    s = (raw or "").strip()
    if not s:
        return ""

    # extract first number-like token
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
    """
    Build payload exactly as locked contract:
    FROM, TO, OUT, IN, PRICE
    Values must be:
    - FROM/TO = city name strings (preferred)
    - OUT/IN = ddmmyy
    - PRICE = £xxx (rounded up)
    """
    # Cities: tolerate multiple header variants.
    from_city = get_first_value(
        row, idx_norm,
        "from_city", "origin_city", "origin_city_name", "origin", "from", "departure_city"
    )
    to_city = get_first_value(
        row, idx_norm,
        "to_city", "destination_city", "destination_city_name", "destination", "to", "arrival_city"
    )

    # Dates: tolerate multiple header variants.
    out_raw = get_first_value(
        row, idx_norm,
        "out_date", "depart_date", "departure_date", "outbound_date", "depart", "out"
    )
    in_raw = get_first_value(
        row, idx_norm,
        "in_date", "return_date", "inbound_date", "return", "in"
    )

    # Price: tolerate multiple header variants.
    price_raw = get_first_value(
        row, idx_norm,
        "price", "price_gbp", "total_price_gbp", "gbp_price", "price_total", "price_total_gbp"
    )

    out_ddmmyy = normalise_date_to_ddmmyy(out_raw)
    in_ddmmyy = normalise_date_to_ddmmyy(in_raw)
    price_fmt = normalise_price_to_pounds_rounded(price_raw)

    payload = {
        "FROM": from_city,
        "TO": to_city,
        "OUT": out_ddmmyy,
        "IN": in_ddmmyy,
        "PRICE": price_fmt,
    }
    return payload


def payload_is_complete(p: dict) -> bool:
    return all((p.get("FROM"), p.get("TO"), p.get("OUT"), p.get("IN"), p.get("PRICE")))


# ==================== MAIN ====================

def main():
    log("=" * 60)
    log(f"🖼️ Render Client starting | RUN_SLOT={RUN_SLOT}")
    log("=" * 60)

    if not SPREADSHEET_ID or not RENDER_URL:
        raise RuntimeError("Missing SPREADSHEET_ID or RENDER_URL")

    gc = gs_client()
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(RAW_DEALS_TAB)

    headers = ws.row_values(1)
    rows = ws.get_all_values()[1:]

    # Normalised header index for tolerant matching
    idx_norm = {norm_header(h): i for i, h in enumerate(headers)}

    # Required baseline columns for locked behaviour
    for col in ("status", "ingested_at_utc", "graphic_url"):
        if norm_header(col) not in idx_norm:
            raise RuntimeError(f"Missing required column: {col}")

    eligible = []

    for i, row in enumerate(rows, start=2):
        status = (row[idx_norm[norm_header("status")]] or "").strip()
        graphic_url = (row[idx_norm[norm_header("graphic_url")]] or "").strip()
        ingested = (row[idx_norm[norm_header("ingested_at_utc")]] or "").strip()

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

        # Pull the row (already loaded), build payload from RAW_DEALS
        row = rows[row_num - 2]  # because rows excludes header and is 0-based
        payload = build_render_payload(row, idx_norm)

        # Forensic-safe payload logging (no secrets)
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

        # LOCKED CONTRACT: send exactly these keys
        r = requests.post(
            RENDER_URL,
            json=payload,
            timeout=60,
        )

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

        cell = gspread.utils.rowcol_to_a1(row_num, idx_norm[norm_header("graphic_url")] + 1)
        ws.update([[graphic_url]], cell)

        log(f"✅ Rendered row {row_num}")
        time.sleep(1)

    log("Render cycle complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
