from __future__ import annotations

import os
import json
import re
import math
import base64
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# CONFIG (ENV ONLY — NO FALLBACK MAGIC)
# ============================================================

SPREADSHEET_ID = (os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID") or "").strip()
RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS")
OPS_MASTER_TAB = os.getenv("OPS_MASTER_TAB", "OPS_MASTER")

RENDER_URL = (os.getenv("RENDER_URL") or "").strip()  # MUST be full endpoint
RUN_SLOT = (os.getenv("RUN_SLOT") or "PM").strip().upper()

SERVICE_ACCOUNT_JSON = (
    os.getenv("GCP_SA_JSON_ONE_LINE")
    or os.getenv("GCP_SA_JSON")
    or ""
).strip()

RENDER_MAX_PER_RUN = int(os.getenv("RENDER_MAX_PER_RUN", "1"))

STATUS_READY_TO_POST = "READY_TO_POST"

OPS_THEME_CELL = "B2"

GOOGLE_SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


# ============================================================
# UTIL
# ============================================================

def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat() + "Z"


def normalize_price(val: str) -> str:
    m = re.search(r"(\d+(?:\.\d+)?)", str(val))
    if not m:
        return ""
    return f"£{int(math.ceil(float(m.group(1))))}"


def normalize_date(val: str) -> str:
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(val, fmt).strftime("%d%m%y")
        except Exception:
            pass
    if re.fullmatch(r"\d{6}", val):
        return val
    return ""


def normalize_theme(val: str) -> str:
    if not val:
        return "adventure"
    return re.sub(r"[^a-z0-9_]", "_", val.lower()).strip("_")


# ============================================================
# GOOGLE AUTH
# ============================================================

def load_sa_info() -> dict:
    raw = SERVICE_ACCOUNT_JSON
    if raw.startswith("base64:"):
        raw = base64.b64decode(raw.split("base64:", 1)[1]).decode("utf-8")

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        raw = raw.replace("\\n", "\n")
        return json.loads(raw)


def gspread_client() -> gspread.Client:
    creds = Credentials.from_service_account_info(
        load_sa_info(),
        scopes=GOOGLE_SCOPE,
    )
    return gspread.authorize(creds)


# ============================================================
# CORE
# ============================================================

def main() -> None:
    print("============================================================")
    print("🖼️  TRAVELTXTTER V5 — RENDER CLIENT START")
    print(f"🎯 Renderer endpoint: {RENDER_URL}")
    print("============================================================")

    if not RENDER_URL:
        raise RuntimeError("RENDER_URL is not set")

    gc = gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws = sh.worksheet(RAW_DEALS_TAB)
    ws_ops = sh.worksheet(OPS_MASTER_TAB)

    ops_theme = normalize_theme(ws_ops.acell(OPS_THEME_CELL).value or "")
    layout = "AM" if RUN_SLOT == "AM" else "PM"

    rows = ws.get_all_values()
    if len(rows) < 2:
        print("⚠️ RAW_DEALS empty")
        return

    headers = rows[0]
    data = rows[1:]

    idx = {h: i for i, h in enumerate(headers)}

    def col(row, name):
        i = idx.get(name)
        return row[i].strip() if i is not None and i < len(row) else ""

    candidates: List[int] = []

    for i, r in enumerate(data):
        if col(r, "status") != STATUS_READY_TO_POST:
            continue
        if col(r, "graphic_url"):
            continue
        candidates.append(i + 2)

    candidates = candidates[:RENDER_MAX_PER_RUN]

    if not candidates:
        print("⚠️ No render candidates")
        return

    for row_num in candidates:
        r = ws.row_values(row_num)

        payload = {
            "TO": col(r, "destination_city"),
            "FROM": col(r, "origin_city"),
            "OUT": normalize_date(col(r, "outbound_date")),
            "IN": normalize_date(col(r, "return_date")),
            "PRICE": normalize_price(col(r, "price_gbp")),
            "layout": layout,
            "theme": normalize_theme(col(r, "theme") or ops_theme),
        }

        print(f"🎯 Render row {row_num}: {payload}")

        resp = requests.post(RENDER_URL, json=payload, timeout=60)
        if resp.status_code != 200:
            raise RuntimeError(f"Render failed ({resp.status_code}): {resp.text}")

        graphic_url = resp.json().get("graphic_url")
        if not graphic_url:
            raise RuntimeError(f"No graphic_url in response: {resp.text}")

        ws.batch_update([
            {
                "range": f"{chr(65 + idx['graphic_url'])}{row_num}",
                "values": [[graphic_url]],
            },
            {
                "range": f"{chr(65 + idx['rendered_timestamp'])}{row_num}",
                "values": [[utc_now()]],
            },
        ])

        print(f"✅ Rendered → {graphic_url}")

    print("✅ Render client complete")


if __name__ == "__main__":
    main()
