from __future__ import annotations

import os
import re
import json
import math
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials

# ============================================================
# TRAVELTXTER — RENDER CLIENT (V5 CANONICAL, HARDENED)
#
# AUTHORITY / GOVERNANCE
# - RAW_DEALS is the only writable source of truth.
# - RAW_DEALS_VIEW is read-only.
# - Theme MUST be read from RDV.dynamic_theme (fallback OPS_MASTER!B5 only if blank).
# - Layout is derived ONLY from ingested_at_utc timestamp (UTC):
#       hour < 12  => AM
#       else       => PM
# - This worker MUST NOT change status. It only writes:
#       graphic_url, rendered_timestamp, rendered_at, render_error
#
# RENDER API CONTRACT (PythonAnywhere)
# - Payload MUST include exactly:
#     TO: <City>, FROM: <City>, OUT: ddmmyy, IN: ddmmyy, PRICE: £xxx (rounded up)
# - We also include:
#     theme (palette) and layout ("AM"/"PM") for renderer compliance.
# ============================================================

# ----------------------------
# Environment
# ----------------------------
SPREADSHEET_ID = (os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID") or "").strip()

RAW_DEALS_TAB = (os.getenv("RAW_DEALS_TAB") or "RAW_DEALS").strip() or "RAW_DEALS"
RAW_DEALS_VIEW_TAB = (os.getenv("RAW_DEALS_VIEW_TAB") or "RAW_DEALS_VIEW").strip() or "RAW_DEALS_VIEW"
OPS_MASTER_TAB = (os.getenv("OPS_MASTER_TAB") or "OPS_MASTER").strip() or "OPS_MASTER"

RENDER_URL = (os.getenv("RENDER_URL") or "").strip()

SERVICE_ACCOUNT_JSON = (
    (os.getenv("GCP_SA_JSON_ONE_LINE") or "").strip()
    or (os.getenv("GCP_SA_JSON") or "").strip()
)

RENDER_MAX_PER_RUN = int(float(os.getenv("RENDER_MAX_PER_RUN", "1") or "1"))

# Preferred: resolve RDV dynamic_theme by header name.
RDV_DYNAMIC_THEME_COL = int(float(os.getenv("RDV_DYNAMIC_THEME_COL", "46") or "46"))
RDV_DYNAMIC_THEME_HEADER = (os.getenv("RDV_DYNAMIC_THEME_HEADER") or "dynamic_theme").strip()

# OPS_MASTER theme-of-day locked cell
OPS_THEME_CELL = (os.getenv("OPS_THEME_CELL") or "B5").strip() or "B5"

GOOGLE_SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Statuses to render (V5 canonical + legacy tolerance)
RENDER_ELIGIBLE_STATUSES = {
    "PUBLISH_AM",
    "PUBLISH_PM",
    "PUBLISH_BOTH",
    "READY_TO_PUBLISH",
    "READY_TO_POST",
}

# ----------------------------
# Theme governance (locked set)
# ----------------------------
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
    """Accepts single or multi-theme strings (e.g. 'snow|long_haul') and returns 1 authoritative theme."""
    if not raw_theme:
        return ""
    parts = re.split(r"[,\|;]+", str(raw_theme).lower())
    tokens: List[str] = []
    for p in parts:
        t = p.strip()
        if not t:
            continue
        t = THEME_ALIASES.get(t, t)
        t = re.sub(r"[^a-z0-9_]+", "_", t).strip("_")
        t = THEME_ALIASES.get(t, t)
        if t in AUTHORITATIVE_THEMES:
            tokens.append(t)
    if not tokens:
        return ""
    for pr in THEME_PRIORITY:
        if pr in tokens:
            return pr
    return tokens[0]


# ----------------------------
# Logging
# ----------------------------
def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def log(msg: str) -> None:
    print(f"{_utc_now_iso()} | {msg}", flush=True)


# ----------------------------
# Robust SA JSON parsing
# ----------------------------
def _repair_private_key_newlines(raw: str) -> str:
    """
    Repairs invalid JSON where private_key contains literal newlines.
    Converts literal newlines inside the private_key field to \\n.
    """
    pat = re.compile(r'("private_key"\s*:\s*")(.+?)(")', re.DOTALL)
    m = pat.search(raw)
    if not m:
        return raw
    prefix, pk, suffix = m.group(1), m.group(2), m.group(3)
    pk_fixed = pk.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "\\n").replace("\t", "\\t")
    return raw[: m.start()] + prefix + pk_fixed + suffix + raw[m.end():]


def _sa_creds() -> Credentials:
    if not SERVICE_ACCOUNT_JSON:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")

    raw = SERVICE_ACCOUNT_JSON

    # Attempt 1: parse as-is
    try:
        info = json.loads(raw)
        return Credentials.from_service_account_info(info, scopes=GOOGLE_SCOPE)
    except Exception:
        pass

    # Attempt 2: common escaped-newlines variant
    try:
        info = json.loads(raw.replace("\\n", "\n"))
        return Credentials.from_service_account_info(info, scopes=GOOGLE_SCOPE)
    except Exception:
        pass

    # Attempt 3: repair literal newlines inside private_key field
    repaired = _repair_private_key_newlines(raw)
    try:
        info = json.loads(repaired)
        return Credentials.from_service_account_info(info, scopes=GOOGLE_SCOPE)
    except Exception:
        pass

    # Attempt 4: repair + unescape
    repaired2 = _repair_private_key_newlines(raw).replace("\\n", "\n")
    info = json.loads(repaired2)
    return Credentials.from_service_account_info(info, scopes=GOOGLE_SCOPE)


# ----------------------------
# Sheets helpers
# ----------------------------
def _col_index(headers: List[str], name: str) -> Optional[int]:
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


def _batch_update(ws: gspread.Worksheet, headers: List[str], row_1_based: int, updates: Dict[str, str]) -> None:
    # exact header match contract
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


# ----------------------------
# Timestamp parsing (ISO OR Sheets serial number)
# ----------------------------
_SHEETS_EPOCH = datetime(1899, 12, 30, tzinfo=timezone.utc)  # Google Sheets date serial epoch


def _parse_utc_dt(val: str) -> Optional[datetime]:
    """
    Accepts:
      - ISO 8601 strings (with Z)
      - Google Sheets serial numbers (e.g. 45500.5123)
    Returns datetime in UTC.
    """
    s = (val or "").strip()
    if not s:
        return None

    # numeric serial?
    try:
        # Guard against things like "2026-02-05" which float() will fail anyway
        f = float(s)
        # Reasonable serial range check (>= 30000 ~ year 1982)
        if f >= 30000:
            days = int(f)
            frac = f - days
            dtv = _SHEETS_EPOCH + timedelta(days=days) + timedelta(seconds=round(frac * 86400))
            return dtv.astimezone(timezone.utc)
    except Exception:
        pass

    # ISO parsing
    try:
        iso = s
        if iso.endswith("Z"):
            iso = iso[:-1] + "+00:00"
        dtv = datetime.fromisoformat(iso)
        if dtv.tzinfo is None:
            dtv = dtv.replace(tzinfo=timezone.utc)
        return dtv.astimezone(timezone.utc)
    except Exception:
        return None


def infer_layout_from_ingest(ingested_at_utc: str) -> str:
    ts = _parse_utc_dt(ingested_at_utc)
    if not ts:
        raise ValueError("❌ Cannot infer layout: missing/invalid ingested_at_utc (need ISO or Sheets serial)")
    return "AM" if ts.hour < 12 else "PM"


# ----------------------------
# Normalisers (PA payload contract)
# ----------------------------
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


# ----------------------------
# OPS master theme (fallback only)
# ----------------------------
def read_ops_theme(sh: gspread.Spreadsheet) -> str:
    ws_ops = sh.worksheet(OPS_MASTER_TAB)
    raw = (ws_ops.acell(OPS_THEME_CELL).value or "").strip()
    return normalize_theme(raw)


# ----------------------------
# Payload building
# ----------------------------
def build_payload(
    *,
    origin_city: str,
    destination_city: str,
    outbound_date: str,
    return_date: str,
    price_gbp: str,
    theme_for_palette: str,
    layout: str,
) -> Dict[str, str]:
    payload: Dict[str, str] = {
        "FROM": origin_city.strip(),
        "TO": destination_city.strip(),
        "OUT": normalize_date_ddmmyy(outbound_date),
        "IN": normalize_date_ddmmyy(return_date),
        "PRICE": normalize_price_gbp(price_gbp),
        "theme": normalize_theme(theme_for_palette) or "adventure",
        "layout": layout,
        "run_slot": layout,
    }
    return payload


def extract_graphic_url(resp_json: Dict[str, Any]) -> str:
    for k in ("graphic_url", "png_url", "image_url", "url"):
        v = resp_json.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    if isinstance(resp_json.get("data"), dict):
        return extract_graphic_url(resp_json["data"])
    return ""


# ----------------------------
# Candidate selection (latest-first)
# ----------------------------
def find_candidates_latest_first(
    raw_values: List[List[str]],
    headers: List[str],
    max_n: int,
) -> List[Tuple[int, datetime]]:
    status_i = _col_index(headers, "status")
    graphic_i = _col_index(headers, "graphic_url")
    ingest_i = _col_index(headers, "ingested_at_utc")

    if status_i is None:
        raise RuntimeError("RAW_DEALS missing required header: status")
    if graphic_i is None:
        raise RuntimeError("RAW_DEALS missing required header: graphic_url")
    if ingest_i is None:
        raise RuntimeError("RAW_DEALS missing required header: ingested_at_utc")

    candidates: List[Tuple[int, datetime]] = []
    for idx0 in range(1, len(raw_values)):
        row = raw_values[idx0]
        st = _row_get(row, status_i)
        if st not in RENDER_ELIGIBLE_STATUSES:
            continue
        if _row_get(row, graphic_i):
            continue
        ing = _parse_utc_dt(_row_get(row, ingest_i))
        if not ing:
            continue
        candidates.append((idx0 + 1, ing))

    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates[:max_n]


def resolve_rdv_dynamic_theme_index(rdv_headers: List[str]) -> int:
    """
    Prefer header lookup; fallback to env numeric column.
    Returns 0-based index.
    """
    by_name = _col_index(rdv_headers, RDV_DYNAMIC_THEME_HEADER)
    if by_name is not None:
        return by_name
    # fallback to configured numeric column (1-based)
    return max(0, RDV_DYNAMIC_THEME_COL - 1)


# ----------------------------
# Render one row (no status mutation)
# ----------------------------
def render_sheet_row(
    *,
    ws_raw: gspread.Worksheet,
    sheet_row: int,
    raw_headers: List[str],
    raw_row_values: List[str],
    rdv_dynamic_theme_raw: str,
    ops_theme_fallback: str,
) -> bool:
    row = dict(zip(raw_headers, raw_row_values))

    st = (row.get("status") or "").strip()
    if st not in RENDER_ELIGIBLE_STATUSES:
        log(f"⚠️ Skip row {sheet_row}: status={st!r} not eligible")
        return False

    theme_rdv = normalize_theme(rdv_dynamic_theme_raw)
    theme_ops = normalize_theme(ops_theme_fallback)
    theme_for_palette = theme_rdv or theme_ops or "adventure"

    ingested_at = row.get("ingested_at_utc", "") or ""
    layout = infer_layout_from_ingest(ingested_at)

    # City fallbacks (never block render)
    origin_city = (row.get("origin_city") or "").strip() or (row.get("origin_iata") or "").strip()
    dest_city = (row.get("destination_city") or "").strip() or (row.get("destination_iata") or "").strip()

    payload = build_payload(
        origin_city=origin_city,
        destination_city=dest_city,
        outbound_date=(row.get("outbound_date") or ""),
        return_date=(row.get("return_date") or ""),
        price_gbp=(row.get("price_gbp") or ""),
        theme_for_palette=theme_for_palette,
        layout=layout,
    )

    # Hard validation for PA contract
    if not payload["FROM"] or not payload["TO"]:
        raise ValueError("FROM/TO missing (origin_city/destination_city AND origin_iata/destination_iata empty)")
    if not re.fullmatch(r"\d{6}", payload["OUT"]):
        raise ValueError(f"OUT must be ddmmyy (6 digits). Got {payload['OUT']!r}")
    if not re.fullmatch(r"\d{6}", payload["IN"]):
        raise ValueError(f"IN must be ddmmyy (6 digits). Got {payload['IN']!r}")
    if not payload["PRICE"].startswith("£"):
        raise ValueError(f"PRICE must be £<int>. Got {payload['PRICE']!r}")

    log(
        f"🎯 Render row {sheet_row}: status={st} layout={layout} "
        f"theme_rdv={theme_rdv!r} ops_fallback={theme_ops!r} theme_used={payload.get('theme')!r} "
        f"FROM={payload.get('FROM')!r} TO={payload.get('TO')!r} PRICE={payload.get('PRICE')!r}"
    )

    try:
        t0 = time.time()
        resp = requests.post(RENDER_URL, json=payload, timeout=90)
        render_s = time.time() - t0

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
        _batch_update(
            ws_raw,
            raw_headers,
            sheet_row,
            {
                "graphic_url": graphic_url,
                "rendered_timestamp": ts,
                "rendered_at": ts,
                "render_error": "",
            },
        )

        log(f"✅ Render OK row {sheet_row} (render={render_s:.1f}s)")
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


# ----------------------------
# Main
# ----------------------------
def main() -> int:
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")
    if not RENDER_URL:
        raise RuntimeError("Missing RENDER_URL")

    gc = gspread.authorize(_sa_creds())
    sh = gc.open_by_key(SPREADSHEET_ID)

    ops_theme = read_ops_theme(sh)
    if ops_theme:
        log(f"🎯 OPS_MASTER theme of the day ({OPS_THEME_CELL}): {ops_theme}")
    else:
        log(f"⚠️ OPS_MASTER theme of the day ({OPS_THEME_CELL}) is blank")

    # Batch load sheets once
    log("📥 Loading RAW_DEALS...")
    t0 = time.time()
    ws_raw = sh.worksheet(RAW_DEALS_TAB)
    sheet_values_raw = ws_raw.get_all_values()
    log(f"✅ RAW_DEALS loaded: {max(0, len(sheet_values_raw)-1)} rows ({time.time()-t0:.1f}s)")
    if len(sheet_values_raw) < 2:
        log("ℹ️ RAW_DEALS has no data rows")
        return 0

    raw_headers = sheet_values_raw[0]

    log("📥 Loading RAW_DEALS_VIEW (read-only)...")
    t0 = time.time()
    ws_rdv = sh.worksheet(RAW_DEALS_VIEW_TAB)
    sheet_values_rdv = ws_rdv.get_all_values()
    log(f"✅ RAW_DEALS_VIEW loaded: {max(0, len(sheet_values_rdv)-1)} rows ({time.time()-t0:.1f}s)")

    rdv_headers = sheet_values_rdv[0] if sheet_values_rdv else []
    dyn_theme_idx = resolve_rdv_dynamic_theme_index(rdv_headers) if rdv_headers else (RDV_DYNAMIC_THEME_COL - 1)

    candidates = find_candidates_latest_first(sheet_values_raw, raw_headers, RENDER_MAX_PER_RUN)
    if not candidates:
        log("ℹ️ No render candidates (eligible status + graphic_url blank + valid ingested_at_utc)")
        return 0

    log(f"🎯 Render candidates (latest-first): {[r for (r, _) in candidates]}")

    ok = 0
    for sheet_row, _ing in candidates:
        row_idx0 = sheet_row - 1
        if row_idx0 >= len(sheet_values_raw):
            log(f"⚠️ Row {sheet_row} out of bounds (RAW_DEALS)")
            continue

        raw_row_values = sheet_values_raw[row_idx0]

        # RDV row alignment: RDV mirrors RD row-for-row (including header row)
        rdv_dynamic_theme_raw = ""
        if row_idx0 < len(sheet_values_rdv):
            rdv_row = sheet_values_rdv[row_idx0]
            if 0 <= dyn_theme_idx < len(rdv_row):
                rdv_dynamic_theme_raw = (rdv_row[dyn_theme_idx] or "").strip()

        if render_sheet_row(
            ws_raw=ws_raw,
            sheet_row=sheet_row,
            raw_headers=raw_headers,
            raw_row_values=raw_row_values,
            rdv_dynamic_theme_raw=rdv_dynamic_theme_raw,
            ops_theme_fallback=ops_theme,
        ):
            ok += 1

    log(f"✅ Render complete: {ok}/{len(candidates)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
