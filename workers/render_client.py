from __future__ import annotations

import os
import json
import re
import math
import base64
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# ENV
# ============================================================

SPREADSHEET_ID = (os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID") or "").strip()
RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS")
OPS_MASTER_TAB = os.getenv("OPS_MASTER_TAB", "OPS_MASTER")

# MUST be the full endpoint, e.g. https://greenroomman.pythonanywhere.com/api/render
RENDER_URL = (os.getenv("RENDER_URL") or "").strip()

RUN_SLOT = (os.getenv("RUN_SLOT") or "PM").strip().upper()  # AM or PM
RENDER_MAX_PER_RUN = int(os.getenv("RENDER_MAX_PER_RUN", "2"))

SERVICE_ACCOUNT_JSON = (
    os.getenv("GCP_SA_JSON_ONE_LINE")
    or os.getenv("GCP_SA_JSON")
    or ""
).strip()

OPS_THEME_CELL = os.getenv("OPS_THEME_CELL", "B2")  # theme of day

STATUS_READY_TO_POST = "READY_TO_POST"

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
    m = re.search(r"(\d+(?:\.\d+)?)", str(val or ""))
    if not m:
        return ""
    return f"£{int(math.ceil(float(m.group(1))))}"


def normalize_date_ddmmyy(val: str) -> str:
    val = (val or "").strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(val, fmt).strftime("%d%m%y")
        except Exception:
            pass
    if re.fullmatch(r"\d{6}", val):
        return val
    return ""


def normalize_theme(val: str) -> str:
    v = (val or "").strip().lower()
    if not v:
        return "adventure"
    v = re.sub(r"[^a-z0-9_]", "_", v)
    v = re.sub(r"_+", "_", v).strip("_")
    return v or "adventure"


def a1_col(col_idx_0: int) -> str:
    """0-based column index -> A1 column letters (supports AA, AB, ...)."""
    n = col_idx_0 + 1
    letters = []
    while n:
        n, rem = divmod(n - 1, 26)
        letters.append(chr(65 + rem))
    return "".join(reversed(letters))


def require_headers(headers: List[str], required: List[str], tab_name: str) -> Dict[str, int]:
    idx = {h.strip(): i for i, h in enumerate(headers) if h.strip()}
    missing = [h for h in required if h not in idx]
    if missing:
        raise RuntimeError(f"{tab_name} missing required headers: {missing}")
    return idx


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
        # common GH secret format: escaped newlines
        raw = raw.replace("\\n", "\n")
        return json.loads(raw)


def gspread_client() -> gspread.Client:
    creds = Credentials.from_service_account_info(load_sa_info(), scopes=GOOGLE_SCOPE)
    return gspread.authorize(creds)


# ============================================================
# RENDER POST
# ============================================================

def post_render(payload: Dict[str, Any]) -> str:
    # RENDER_URL is authoritative; do not append paths.
    resp = requests.post(RENDER_URL, json=payload, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"Render failed ({resp.status_code}): {resp.text}")

    try:
        data = resp.json()
    except Exception:
        raise RuntimeError(f"Render returned non-JSON: {resp.text}")

    graphic_url = (data.get("graphic_url") or "").strip()
    if not graphic_url:
        raise RuntimeError(f"Render response missing graphic_url: {data}")
    return graphic_url


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    print("============================================================")
    print("🖼️  TRAVELTXTTER V5 — RENDER CLIENT START")
    print(f"🎯 Renderer endpoint: {RENDER_URL}")
    print(f"🎛️  RUN_SLOT={RUN_SLOT}  RENDER_MAX_PER_RUN={RENDER_MAX_PER_RUN}")
    print("============================================================")

    if not SPREADSHEET_ID:
        raise RuntimeError("SPREADSHEET_ID (or SHEET_ID) is not set")
    if not RENDER_URL:
        raise RuntimeError("RENDER_URL is not set (must be full endpoint /api/render)")
    if "/api/api/" in RENDER_URL:
        raise RuntimeError(f"RENDER_URL looks wrong (double /api/): {RENDER_URL}")

    layout = "AM" if RUN_SLOT == "AM" else "PM"

    gc = gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_raw = sh.worksheet(RAW_DEALS_TAB)
    ws_ops = sh.worksheet(OPS_MASTER_TAB)

    ops_theme = normalize_theme(ws_ops.acell(OPS_THEME_CELL).value or "")

    values = ws_raw.get_all_values()
    if len(values) < 2:
        print("⚠️ RAW_DEALS has no data rows.")
        return

    headers = values[0]
    rows = values[1:]

    # Required for payload
    required = [
        "status",
        "origin_city",
        "destination_city",
        "outbound_date",
        "return_date",
        "price_gbp",
        "theme",
        "graphic_url",
    ]
    idx = require_headers(headers, required, RAW_DEALS_TAB)

    # Optional write-backs (only if present)
    has_rendered_ts = "rendered_timestamp" in [h.strip() for h in headers]
    has_render_error = "render_error" in [h.strip() for h in headers]

    idx_rendered_ts = idx.get("rendered_timestamp") if has_rendered_ts else None
    idx_render_error = idx.get("render_error") if has_render_error else None

    def get(row: List[str], name: str) -> str:
        i = idx.get(name)
        return (row[i] if i is not None and i < len(row) else "").strip()

    # Candidates: READY_TO_POST and missing graphic_url
    candidates: List[int] = []
    for i0, row in enumerate(rows):
        if get(row, "status") != STATUS_READY_TO_POST:
            continue
        if get(row, "graphic_url"):
            continue
        candidates.append(i0 + 2)  # sheet row number

    candidates = candidates[: max(0, RENDER_MAX_PER_RUN)]

    if not candidates:
        print("⚠️ No render candidates (READY_TO_POST without graphic_url).")
        return

    for row_num in candidates:
        row = ws_raw.row_values(row_num)

        theme_used = normalize_theme(get(row, "theme") or ops_theme)

        payload = {
            "TO": get(row, "destination_city"),
            "FROM": get(row, "origin_city"),
            "OUT": normalize_date_ddmmyy(get(row, "outbound_date")),
            "IN": normalize_date_ddmmyy(get(row, "return_date")),
            "PRICE": normalize_price(get(row, "price_gbp")),
            "layout": layout,
            "theme": theme_used,
        }

        print(
            f"🎯 Render row {row_num}: layout={layout} theme_used='{theme_used}' "
            f"FROM='{payload['FROM']}' TO='{payload['TO']}' PRICE='{payload['PRICE']}'"
        )

        graphic_url = post_render(payload)

        # Write-backs
        updates: List[Tuple[str, str]] = []

        # graphic_url (required)
        col_letter = a1_col(idx["graphic_url"])
        updates.append((f"{col_letter}{row_num}", graphic_url))

        # rendered_timestamp (optional)
        if idx_rendered_ts is not None:
            col_letter = a1_col(idx_rendered_ts)
            updates.append((f"{col_letter}{row_num}", utc_now()))

        # clear render_error (optional)
        if idx_render_error is not None:
            col_letter = a1_col(idx_render_error)
            updates.append((f"{col_letter}{row_num}", ""))

        for a1, val in updates:
            ws_raw.update(a1, [[val]])

        print(f"✅ Rendered → {graphic_url}")

    print("✅ Render client complete")


if __name__ == "__main__":
    main()
