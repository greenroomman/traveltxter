from __future__ import annotations

import os
import re
import json
import math
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# CONFIG (V5 — LOCKED)
# ============================================================
# Render rules:
# - Layout is derived ONLY from ingested_at_utc (row-level truth)
# - Theme is taken from RAW_DEALS_VIEW.dynamic_theme
# - OPS_MASTER!B5 is used ONLY if RDV dynamic_theme is blank/unusable
# ============================================================

SPREADSHEET_ID = (os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID") or "").strip()

RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS")
RAW_DEALS_VIEW_TAB = os.getenv("RAW_DEALS_VIEW_TAB", "RAW_DEALS_VIEW")
OPS_MASTER_TAB = os.getenv("OPS_MASTER_TAB", "OPS_MASTER")

RENDER_URL = (os.getenv("RENDER_URL") or "").strip()

SERVICE_ACCOUNT_JSON = (
    os.getenv("GCP_SA_JSON_ONE_LINE")
    or os.getenv("GCP_SA_JSON")
    or ""
).strip()

STATUS_READY_TO_POST = os.getenv("STATUS_READY_TO_POST", "READY_TO_POST")
STATUS_READY_TO_PUBLISH = os.getenv("STATUS_READY_TO_PUBLISH", "READY_TO_PUBLISH")

RENDER_MAX_PER_RUN = int(float(os.getenv("RENDER_MAX_PER_RUN", "1") or "1"))

# RDV dynamic_theme column (AT = 46, 1-based)
RDV_DYNAMIC_THEME_COL = int(float(os.getenv("RDV_DYNAMIC_THEME_COL", "46") or "46"))

OPS_THEME_CELL = os.getenv("OPS_THEME_CELL", "B5")

GOOGLE_SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


# ============================================================
# THEME NORMALISATION (LOCKED)
# ============================================================

AUTHORITATIVE_THEMES = {
    "winter_sun",
    "summer_sun",
    "beach_break",
    "snow",
    "northern_lights",
    "surf",
    "adventure",
    "city_breaks",
    "culture_history",
    "long_haul",
    "luxury_value",
    "unexpected_value",
}

THEME_ALIASES = {
    "winter sun": "winter_sun",
    "summer sun": "summer_sun",
    "beach break": "beach_break",
    "northern lights": "northern_lights",
    "city breaks": "city_breaks",
    "culture / history": "culture_history",
    "culture & history": "culture_history",
    "culture": "culture_history",
    "history": "culture_history",
    "long haul": "long_haul",
    "luxury": "luxury_value",
    "unexpected": "unexpected_value",
}


def normalize_theme(raw: str | None) -> str:
    if not raw:
        return ""
    parts = re.split(r"[,\|;]+", str(raw).lower())
    for p in parts:
        t = p.strip()
        if not t:
            continue
        t = THEME_ALIASES.get(t, t)
        t = re.sub(r"[^a-z0-9_]+", "_", t).strip("_")
        if t in AUTHORITATIVE_THEMES:
            return t
    return ""


# ============================================================
# GOOGLE SHEETS HELPERS
# ============================================================

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat() + "Z"


def _sa_creds() -> Credentials:
    if not SERVICE_ACCOUNT_JSON:
        raise RuntimeError("Missing GCP_SA_JSON / GCP_SA_JSON_ONE_LINE")
    try:
        info = json.loads(SERVICE_ACCOUNT_JSON)
    except json.JSONDecodeError:
        info = json.loads(SERVICE_ACCOUNT_JSON.replace("\\n", "\n"))
    return Credentials.from_service_account_info(info, scopes=GOOGLE_SCOPE)


def _col_index(headers: list[str], name: str) -> Optional[int]:
    target = name.strip().lower()
    for i, h in enumerate(headers):
        if (h or "").strip().lower() == target:
            return i
    return None


def _a1(col_1: int, row_1: int) -> str:
    n, s = col_1, ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return f"{s}{row_1}"


def _batch_update(ws: gspread.Worksheet, headers: list[str], row_1: int, updates: Dict[str, str]) -> None:
    idx = {h: i for i, h in enumerate(headers)}
    payload = []
    for k, v in updates.items():
        if k in idx:
            payload.append(
                {"range": _a1(idx[k] + 1, row_1), "values": [[v]]}
            )
    if payload:
        ws.batch_update(payload)


# ============================================================
# SLOT INFERENCE (CANONICAL)
# ============================================================

def _parse_utc(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def infer_layout(row: Dict[str, str]) -> str:
    for k in ("ingested_at_utc", "created_utc", "timestamp", "created_at"):
        dt = _parse_utc(row.get(k, ""))
        if dt:
            return "AM" if dt.hour < 12 else "PM"
    raise RuntimeError("Cannot infer AM/PM — missing valid timestamp")


# ============================================================
# PAYLOAD NORMALISATION (RENDER API CONTRACT)
# ============================================================

def normalize_price(raw: str | None) -> str:
    if not raw:
        return ""
    m = re.search(r"(\d+(?:\.\d+)?)", raw.replace(",", ""))
    return f"£{int(math.ceil(float(m.group(1))))}" if m else ""


def normalize_date(raw: str | None) -> str:
    if not raw:
        return ""
    raw = raw.strip()
    if re.fullmatch(r"\d{6}", raw):
        return raw
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        y, m, d = raw.split("-")
        return f"{int(d):02d}{int(m):02d}{int(y)%100:02d}"
    raise ValueError(f"Unsupported date format: {raw}")


# ============================================================
# OPS MASTER (FALLBACK ONLY)
# ============================================================

def read_ops_theme(sh: gspread.Spreadsheet) -> str:
    ws = sh.worksheet(OPS_MASTER_TAB)
    return normalize_theme((ws.acell(OPS_THEME_CELL).value or "").strip())


# ============================================================
# RENDER ONE ROW
# ============================================================

def render_row(
    sh: gspread.Spreadsheet,
    row_num: int,
    raw_headers: list[str],
    raw_values: list[str],
    rdv_theme_raw: str,
    ops_theme: str,
) -> bool:
    ws_raw = sh.worksheet(RAW_DEALS_TAB)
    row = dict(zip(raw_headers, raw_values))

    if row.get("status") != STATUS_READY_TO_POST:
        return False

    theme = normalize_theme(rdv_theme_raw) or ops_theme
    if not theme:
        raise RuntimeError("No valid theme available for render")

    layout = infer_layout(row)

    payload = {
        "FROM": row.get("origin_city", ""),
        "TO": row.get("destination_city", ""),
        "OUT": normalize_date(row.get("outbound_date")),
        "IN": normalize_date(row.get("return_date")),
        "PRICE": normalize_price(row.get("price_gbp")),
        "theme": theme,
        "layout": layout,
        "run_slot": layout,
    }

    resp = requests.post(RENDER_URL, json=payload, timeout=60)
    if not resp.ok:
        raise RuntimeError(f"Render failed {resp.status_code}: {resp.text}")

    graphic_url = resp.json().get("graphic_url")
    if not graphic_url:
        raise RuntimeError("Render response missing graphic_url")

    ts = _utc_now_iso()
    _batch_update(
        ws_raw,
        raw_headers,
        row_num,
        {
            "graphic_url": graphic_url,
            "rendered_timestamp": ts,
            "rendered_at": ts,
            "render_error": "",
            "status": STATUS_READY_TO_PUBLISH,
        },
    )
    return True


# ============================================================
# MAIN
# ============================================================

def main() -> int:
    if not SPREADSHEET_ID or not RENDER_URL:
        raise RuntimeError("Missing SPREADSHEET_ID or RENDER_URL")

    gc = gspread.authorize(_sa_creds())
    sh = gc.open_by_key(SPREADSHEET_ID)

    ops_theme = read_ops_theme(sh)

    ws_raw = sh.worksheet(RAW_DEALS_TAB)
    ws_rdv = sh.worksheet(RAW_DEALS_VIEW_TAB)

    raw_all = ws_raw.get_all_values()
    rdv_all = ws_rdv.get_all_values()

    headers = raw_all[0]
    status_i = _col_index(headers, "status")
    graphic_i = _col_index(headers, "graphic_url")

    candidates = []
    for i in range(1, len(raw_all)):
        if raw_all[i][status_i] == STATUS_READY_TO_POST and not raw_all[i][graphic_i]:
            candidates.append(i + 1)
        if len(candidates) >= RENDER_MAX_PER_RUN:
            break

    for row_num in candidates:
        idx = row_num - 1
        rdv_theme_raw = (
            rdv_all[idx][RDV_DYNAMIC_THEME_COL - 1]
            if idx < len(rdv_all) and RDV_DYNAMIC_THEME_COL - 1 < len(rdv_all[idx])
            else ""
        )
        render_row(
            sh,
            row_num,
            headers,
            raw_all[idx],
            rdv_theme_raw,
            ops_theme,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
