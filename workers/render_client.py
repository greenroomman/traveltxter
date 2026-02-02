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
# CONFIG
# ============================================================
# Version: V5.1 (Performance optimized)
# - Batch load RAW_DEALS and RAW_DEALS_VIEW once (no per-row API calls)
# - Eliminated 3 API calls per row (was 5min for 1 row, now ~10-20s)
# - Added timing logs for diagnostics
# ============================================================

SPREADSHEET_ID = (os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID") or "").strip()

RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS")
RAW_DEALS_VIEW_TAB = os.getenv("RAW_DEALS_VIEW_TAB", "RAW_DEALS_VIEW")
OPS_MASTER_TAB = os.getenv("OPS_MASTER_TAB", "OPS_MASTER")

RENDER_URL = (os.getenv("RENDER_URL") or "").strip()

SERVICE_ACCOUNT_JSON = (os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON") or "").strip()

# If provided, can still force. But we now self-heal from ingested_at_utc.
RUN_SLOT_ENV = (os.getenv("RUN_SLOT") or "").strip().upper()  # "AM" or "PM" or ""

STATUS_READY_TO_POST = os.getenv("STATUS_READY_TO_POST", "READY_TO_POST")
STATUS_READY_TO_PUBLISH = os.getenv("STATUS_READY_TO_PUBLISH", "READY_TO_PUBLISH")

RENDER_MAX_PER_RUN = int(float(os.getenv("RENDER_MAX_PER_RUN", "1") or "1"))

# RDV dynamic_theme locked column AT = 46 (1-based)
RDV_DYNAMIC_THEME_COL = int(float(os.getenv("RDV_DYNAMIC_THEME_COL", "46") or "46"))

# OPS_MASTER theme-of-day locked cell
OPS_THEME_CELL = os.getenv("OPS_THEME_CELL", "B5")

GOOGLE_SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


# ============================================================
# THEME GOVERNANCE (LOCKED)
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

THEME_PRIORITY = [
    "luxury_value",
    "unexpected_value",
    "long_haul",
    "snow",
    "northern_lights",
    "winter_sun",
    "summer_sun",
    "beach_break",
    "surf",
    "culture_history",
    "city_breaks",
    "adventure",
]


def normalize_theme(raw_theme: str | None) -> str:
    if not raw_theme:
        return "adventure"

    parts = re.split(r"[,\|;]+", str(raw_theme).lower())
    tokens: list[str] = []

    for p in parts:
        t = p.strip()
        if not t:
            continue
        if t in THEME_ALIASES:
            t = THEME_ALIASES[t]
        t = re.sub(r"[^a-z0-9_]+", "_", t).strip("_")
        if t in THEME_ALIASES:
            t = THEME_ALIASES[t]
        if t in AUTHORITATIVE_THEMES:
            tokens.append(t)

    if not tokens:
        return "adventure"

    for pr in THEME_PRIORITY:
        if pr in tokens:
            return pr

    return tokens[0]


# ============================================================
# GOOGLE SHEETS HELPERS
# ============================================================

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat() + "Z"


def _sa_creds() -> Credentials:
    if not SERVICE_ACCOUNT_JSON:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
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


def _a1(col_1_based: int, row_1_based: int) -> str:
    n = col_1_based
    letters = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        letters = chr(65 + r) + letters
    return f"{letters}{row_1_based}"


def _batch_update(ws: gspread.Worksheet, headers: list[str], row_1_based: int, updates: Dict[str, str]) -> None:
    idx = {h: i for i, h in enumerate(headers)}
    data = []
    for k, v in updates.items():
        if k not in idx:
            continue
        data.append({"range": _a1(idx[k] + 1, row_1_based), "values": [[v]]})
    if data:
        ws.batch_update(data)


def _row_get(row: List[str], i: Optional[int]) -> str:
    if i is None or i < 0 or i >= len(row):
        return ""
    return (row[i] or "").strip()


# ============================================================
# SLOT INFERENCE (NEW, V5-SAFE)
# - Prefer ingested_at_utc; fallback to created_utc/timestamp/created_at
# - Deterministic: hour < 12 UTC => AM else PM
# ============================================================

def _parse_utc_dt(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    # Accept: 2026-02-01T16:55:49Z, 2026-02-01T16:55:49+00:00, with/without micros
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dtv = datetime.fromisoformat(s)
        if dtv.tzinfo is None:
            dtv = dtv.replace(tzinfo=timezone.utc)
        return dtv.astimezone(timezone.utc)
    except Exception:
        return None


def infer_run_slot(row: Dict[str, str]) -> str:
    # If env explicitly forces it, keep that as highest authority.
    if RUN_SLOT_ENV in ("AM", "PM"):
        return RUN_SLOT_ENV

    # Otherwise infer from row timestamps (priority order).
    for k in ("ingested_at_utc", "created_utc", "timestamp", "created_at"):
        dtv = _parse_utc_dt(row.get(k, ""))
        if dtv:
            return "AM" if dtv.hour < 12 else "PM"

    # Last resort: current UTC time
    now = datetime.now(timezone.utc)
    return "AM" if now.hour < 12 else "PM"


# ============================================================
# NORMALISERS (LOCKED PA PAYLOAD CONTRACT)
# OUT/IN: ddmmyy
# PRICE: £ integer (rounded up)
# ============================================================

def normalize_price_gbp(raw_price: str | None) -> str:
    if not raw_price:
        return ""
    s = str(raw_price).strip()
    if not s:
        return ""
    m = re.search(r"(\d+(?:\.\d+)?)", s.replace(",", ""))
    if not m:
        return ""
    val = float(m.group(1))
    return f"£{int(math.ceil(val))}"


def normalize_date_ddmmyy(raw_date: str | None) -> str:
    if not raw_date:
        return ""
    s = str(raw_date).strip()
    if not s:
        return ""

    if re.fullmatch(r"\d{6}", s):
        return s

    if re.fullmatch(r"\d{8}", s):
        dd = s[0:2]
        mm = s[2:4]
        yy = s[6:8]
        return f"{dd}{mm}{yy}"

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        y, mo, d = s.split("-")
        return f"{int(d):02d}{int(mo):02d}{int(y) % 100:02d}"

    m = re.fullmatch(r"(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})", s)
    if m:
        d = int(m.group(1))
        mo = int(m.group(2))
        y = int(m.group(3))
        return f"{d:02d}{mo:02d}{y % 100:02d}"

    raise ValueError(f"Unsupported date format: {raw_date!r}")


# ============================================================
# OPS MASTER THEME (LOCKED)
# ============================================================

def read_ops_theme(sh: gspread.Spreadsheet) -> str:
    ws_ops = sh.worksheet(OPS_MASTER_TAB)
    raw = (ws_ops.acell(OPS_THEME_CELL).value or "").strip()
    if not raw:
        return ""
    return normalize_theme(raw)


# ============================================================
# PAYLOAD BUILDING
# ============================================================

def build_payload(row: Dict[str, str], theme_for_palette: str, run_slot_effective: str) -> Dict[str, str]:
    out_raw = row.get("outbound_date", "") or row.get("out_date", "")
    in_raw = row.get("return_date", "") or row.get("in_date", "")
    price_raw = row.get("price_gbp", "") or row.get("price", "")
    from_city = row.get("origin_city", "") or row.get("from_city", "")
    to_city = row.get("destination_city", "") or row.get("to_city", "")

    payload: Dict[str, str] = {
        "FROM": from_city,
        "TO": to_city,
        "OUT": normalize_date_ddmmyy(out_raw) if out_raw else "",
        "IN": normalize_date_ddmmyy(in_raw) if in_raw else "",
        "PRICE": normalize_price_gbp(price_raw) if price_raw else "",
        "theme": normalize_theme(theme_for_palette),
    }

    # ✅ CRITICAL: renderer API uses "layout" (defaults to PM if missing)
    if run_slot_effective in ("AM", "PM"):
        payload["layout"] = run_slot_effective
        payload["run_slot"] = run_slot_effective

    return payload


def extract_graphic_url(resp_json: Dict[str, Any]) -> str:
    for k in ("graphic_url", "png_url", "image_url", "url"):
        v = resp_json.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    if isinstance(resp_json.get("data"), dict):
        return extract_graphic_url(resp_json["data"])
    return ""


# ============================================================
# CANDIDATE SELECTION (ROBUST)
# ============================================================

def find_candidates_from_sheet_values(
    sheet_values: List[List[str]],
    status_i: int,
    graphic_i: int,
    max_n: int,
) -> List[int]:
    candidates: List[int] = []
    for idx0 in range(1, len(sheet_values)):
        row = sheet_values[idx0]
        st = _row_get(row, status_i)
        if st != STATUS_READY_TO_POST:
            continue
        gu = _row_get(row, graphic_i)
        if gu:
            continue
        candidates.append(idx0 + 1)  # sheet row number
        if len(candidates) >= max_n:
            break
    return candidates


# ============================================================
# RENDER ONE SHEET ROW
# ============================================================

def render_sheet_row(
    sh: gspread.Spreadsheet,
    sheet_row: int,
    ops_theme: str,
    raw_headers: List[str],
    raw_values: List[str],
    rdv_dynamic_theme_raw: str,
) -> bool:
    """Render a single row using pre-loaded data (no API calls except for write-back)."""
    ws_raw = sh.worksheet(RAW_DEALS_TAB)
    
    row = dict(zip(raw_headers, raw_values))

    status = (row.get("status") or "").strip()
    if status != STATUS_READY_TO_POST:
        print(f"⚠️ Skip row {sheet_row}: status={status!r}")
        return False

    theme_for_palette = ops_theme.strip() if ops_theme.strip() else normalize_theme(rdv_dynamic_theme_raw)

    run_slot_effective = infer_run_slot(row)
    payload = build_payload(row, theme_for_palette, run_slot_effective)

    print(
        f"Target row {sheet_row} | RUN_SLOT_ENV={RUN_SLOT_ENV!r} | "
        f"run_slot_effective={run_slot_effective!r} | "
        f"layout={payload.get('layout')!r} | OPS_THEME={ops_theme!r} | "
        f"RDV_AT={rdv_dynamic_theme_raw!r} | theme_used={payload.get('theme')!r} | "
        f"ingested_at_utc={row.get('ingested_at_utc','')!r} | payload={payload}"
    )

    if not payload["FROM"] or not payload["TO"]:
        raise ValueError("FROM/TO missing (origin_city/destination_city)")
    if not re.fullmatch(r"\d{6}", payload["OUT"]):
        raise ValueError(f"OUT must be ddmmyy (6 digits). Got {payload['OUT']!r}")
    if not re.fullmatch(r"\d{6}", payload["IN"]):
        raise ValueError(f"IN must be ddmmyy (6 digits). Got {payload['IN']!r}")
    if not payload["PRICE"].startswith("£"):
        raise ValueError(f"PRICE must be £<int>. Got {payload['PRICE']!r}")

    try:
        t_render_start = time.time()
        resp = requests.post(RENDER_URL, json=payload, timeout=60)
        render_elapsed = time.time() - t_render_start
        
        if not resp.ok:
            raise RuntimeError(f"Render failed ({resp.status_code}): {resp.text}")

        try:
            resp_json = resp.json()
        except Exception:
            raise RuntimeError(f"Render returned non-JSON: {resp.text[:200]}")

        graphic_url = extract_graphic_url(resp_json)
        if not graphic_url:
            raise RuntimeError("Render response missing graphic_url")

        ts = _utc_now_iso()
        t_write_start = time.time()
        _batch_update(
            ws_raw,
            raw_headers,
            sheet_row,
            {
                "graphic_url": graphic_url,
                "rendered_timestamp": ts,
                "rendered_at": ts,
                "render_error": "",
                "status": STATUS_READY_TO_PUBLISH,
            },
        )
        write_elapsed = time.time() - t_write_start

        print(
            f"✅ Render OK row {sheet_row}: status {STATUS_READY_TO_POST}→{STATUS_READY_TO_PUBLISH} "
            f"(render={render_elapsed:.1f}s, write={write_elapsed:.1f}s)"
        )
        return True

    except Exception as e:
        ts = _utc_now_iso()
        _batch_update(
            ws_raw,
            raw_headers,
            sheet_row,
            {
                "render_error": f"{type(e).__name__}: {e}",
                "rendered_timestamp": ts,
                "rendered_at": ts,
            },
        )
        raise


# ============================================================
# MAIN
# ============================================================

def main() -> int:
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")
    if not RENDER_URL:
        raise RuntimeError("Missing RENDER_URL")

    gc = gspread.authorize(_sa_creds())
    sh = gc.open_by_key(SPREADSHEET_ID)

    ops_theme = read_ops_theme(sh)
    if ops_theme:
        print(f"🎯 OPS_MASTER theme of the day ({OPS_THEME_CELL}): {ops_theme}")
    else:
        print(f"⚠️ OPS_MASTER theme of the day ({OPS_THEME_CELL}) is blank -> will fallback to RDV AT per row")

    # ✅ BATCH LOAD: RAW_DEALS (all rows, once)
    print("📥 Loading RAW_DEALS...")
    t_start = time.time()
    ws_raw = sh.worksheet(RAW_DEALS_TAB)
    sheet_values_raw = ws_raw.get_all_values()
    elapsed = time.time() - t_start
    print(f"✅ RAW_DEALS loaded: {len(sheet_values_raw)-1} rows ({elapsed:.1f}s)")
    
    if len(sheet_values_raw) < 2:
        print("ℹ️ RAW_DEALS has no data rows")
        return 0

    raw_headers = sheet_values_raw[0]
    status_i = _col_index(raw_headers, "status")
    graphic_i = _col_index(raw_headers, "graphic_url")

    if status_i is None:
        raise RuntimeError("RAW_DEALS missing required header: status")
    if graphic_i is None:
        raise RuntimeError("RAW_DEALS missing required header: graphic_url")

    # ✅ BATCH LOAD: RAW_DEALS_VIEW (all rows, once)
    print("📥 Loading RAW_DEALS_VIEW...")
    t_start = time.time()
    ws_rdv = sh.worksheet(RAW_DEALS_VIEW_TAB)
    sheet_values_rdv = ws_rdv.get_all_values()
    elapsed = time.time() - t_start
    print(f"✅ RAW_DEALS_VIEW loaded: {len(sheet_values_rdv)-1} rows ({elapsed:.1f}s)")

    candidates = find_candidates_from_sheet_values(sheet_values_raw, status_i, graphic_i, RENDER_MAX_PER_RUN)
    if not candidates:
        print(f"ℹ️ No render candidates (status={STATUS_READY_TO_POST}, graphic_url blank)")
        return 0

    print(f"🎯 Render candidates (sheet rows): {candidates} | RUN_SLOT_ENV={RUN_SLOT_ENV!r}")

    ok = 0
    for sheet_row in candidates:
        # Extract pre-loaded data (0-indexed array, but sheet_row is 1-indexed)
        row_idx = sheet_row - 1
        
        if row_idx >= len(sheet_values_raw):
            print(f"⚠️ Row {sheet_row} out of bounds")
            continue
            
        raw_values = sheet_values_raw[row_idx]
        
        # Get RDV dynamic_theme from pre-loaded data (column AT = 46, 0-indexed = 45)
        rdv_dynamic_theme_raw = ""
        if row_idx < len(sheet_values_rdv) and (RDV_DYNAMIC_THEME_COL - 1) < len(sheet_values_rdv[row_idx]):
            rdv_dynamic_theme_raw = sheet_values_rdv[row_idx][RDV_DYNAMIC_THEME_COL - 1]
        
        if render_sheet_row(sh, sheet_row, ops_theme, raw_headers, raw_values, rdv_dynamic_theme_raw):
            ok += 1

    print(f"✅ Render complete: {ok}/{len(candidates)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
