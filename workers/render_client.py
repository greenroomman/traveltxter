from __future__ import annotations

import os
import re
import json
import math
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# CONFIG
# ============================================================

SPREADSHEET_ID = (os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID") or "").strip()
RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS")
RAW_DEALS_VIEW_TAB = os.getenv("RAW_DEALS_VIEW_TAB", "RAW_DEALS_VIEW")
RENDER_URL = (os.getenv("RENDER_URL") or "").strip()

SERVICE_ACCOUNT_JSON = (os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON") or "").strip()

STATUS_READY_TO_POST = os.getenv("STATUS_READY_TO_POST", "READY_TO_POST")
STATUS_READY_TO_PUBLISH = os.getenv("STATUS_READY_TO_PUBLISH", "READY_TO_PUBLISH")

RENDER_MAX_PER_RUN = int(float(os.getenv("RENDER_MAX_PER_RUN", "1") or "1"))

# RDV dynamic_theme locked column AT = 46 (1-based)
RDV_DYNAMIC_THEME_COL = int(float(os.getenv("RDV_DYNAMIC_THEME_COL", "46") or "46"))

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
        # ddmmyyyy or yyyymmdd ambiguity exists; we only handle ddmmyyyy by picking yy from end
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

    m = re.fullmatch(r"(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})", s)
    if m:
        y = int(m.group(1))
        mo = int(m.group(2))
        d = int(m.group(3))
        return f"{d:02d}{mo:02d}{y % 100:02d}"

    raise ValueError(f"Cannot normalize date to ddmmyy: {raw_date!r}")


def build_payload(row: Dict[str, str], dynamic_theme: str | None) -> Dict[str, str]:
    out_raw = row.get("outbound_date", "") or row.get("out_date", "")
    in_raw = row.get("return_date", "") or row.get("in_date", "")
    price_raw = row.get("price_gbp", "") or row.get("price", "")
    from_city = row.get("origin_city", "") or row.get("from_city", "")
    to_city = row.get("destination_city", "") or row.get("to_city", "")

    out_ddmmyy = normalize_date_ddmmyy(out_raw) if out_raw else ""
    in_ddmmyy = normalize_date_ddmmyy(in_raw) if in_raw else ""
    price_gbp = normalize_price_gbp(price_raw) if price_raw else ""

    return {
        "FROM": from_city,
        "TO": to_city,
        "OUT": out_ddmmyy,
        "IN": in_ddmmyy,
        "PRICE": price_gbp,
        "theme": normalize_theme(dynamic_theme),
    }


def extract_graphic_url(resp_json: Dict[str, Any]) -> str:
    for k in ("graphic_url", "png_url", "image_url", "url"):
        v = resp_json.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    if isinstance(resp_json.get("data"), dict):
        return extract_graphic_url(resp_json["data"])
    return ""


# ============================================================
# CORE: FIND CANDIDATES AND RENDER
# ============================================================

def find_candidates(ws_raw: gspread.Worksheet, headers: list[str], max_n: int) -> List[int]:
    status_i = _col_index(headers, "status")
    graphic_i = _col_index(headers, "graphic_url")

    if status_i is None:
        raise RuntimeError("RAW_DEALS missing required header: status")
    if graphic_i is None:
        raise RuntimeError("RAW_DEALS missing required header: graphic_url")

    status_vals = ws_raw.col_values(status_i + 1)  # includes header
    graphic_vals = ws_raw.col_values(graphic_i + 1)  # includes header

    candidates: List[int] = []
    for sheet_row in range(2, len(status_vals) + 1):
        st = (status_vals[sheet_row - 1] or "").strip()
        if st != STATUS_READY_TO_POST:
            continue
        gu = (graphic_vals[sheet_row - 1] or "").strip()
        if gu:
            continue
        candidates.append(sheet_row)
        if len(candidates) >= max_n:
            break

    return candidates


def render_sheet_row(sh: gspread.Spreadsheet, sheet_row: int) -> bool:
    ws_raw = sh.worksheet(RAW_DEALS_TAB)
    ws_rdv = sh.worksheet(RAW_DEALS_VIEW_TAB)

    headers = ws_raw.row_values(1)
    values = ws_raw.row_values(sheet_row)
    row = dict(zip(headers, values))

    status = (row.get("status") or "").strip()
    if status != STATUS_READY_TO_POST:
        print(f"⚠️ Skip row {sheet_row}: status={status!r}")
        return False

    dynamic_theme = ws_rdv.cell(sheet_row, RDV_DYNAMIC_THEME_COL).value
    payload = build_payload(row, dynamic_theme)

    print(f"Target row {sheet_row} | dynamic_theme={dynamic_theme!r} | payload={payload}")

    if not payload["FROM"] or not payload["TO"]:
        raise ValueError("FROM/TO missing (origin_city/destination_city)")
    if not re.fullmatch(r"\d{6}", payload["OUT"]):
        raise ValueError(f"OUT must be ddmmyy (6 digits). Got {payload['OUT']!r}")
    if not re.fullmatch(r"\d{6}", payload["IN"]):
        raise ValueError(f"IN must be ddmmyy (6 digits). Got {payload['IN']!r}")
    if not payload["PRICE"].startswith("£"):
        raise ValueError(f"PRICE must be £<int>. Got {payload['PRICE']!r}")

    try:
        resp = requests.post(RENDER_URL, json=payload, timeout=60)
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
            headers,
            sheet_row,
            {
                "graphic_url": graphic_url,
                "rendered_timestamp": ts,
                "rendered_at": ts,
                "render_error": "",
                "status": STATUS_READY_TO_PUBLISH,
            },
        )

        print(f"✅ Render OK row {sheet_row}: status {STATUS_READY_TO_POST}→{STATUS_READY_TO_PUBLISH}")
        return True

    except Exception as e:
        ts = _utc_now_iso()
        _batch_update(
            ws_raw,
            headers,
            sheet_row,
            {
                "render_error": f"{type(e).__name__}: {e}",
                "rendered_timestamp": ts,
                "rendered_at": ts,
            },
        )
        raise


def main() -> int:
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")
    if not RENDER_URL:
        raise RuntimeError("Missing RENDER_URL")

    gc = gspread.authorize(_sa_creds())
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_raw = sh.worksheet(RAW_DEALS_TAB)
    headers = ws_raw.row_values(1)

    candidates = find_candidates(ws_raw, headers, max_n=RENDER_MAX_PER_RUN)
    if not candidates:
        print(f"ℹ️ No render candidates (status={STATUS_READY_TO_POST}, graphic_url blank)")
        return 0

    print(f"🎯 Render candidates (sheet rows): {candidates}")

    ok = 0
    for sheet_row in candidates:
        if render_sheet_row(sh, sheet_row):
            ok += 1

    print(f"✅ Render complete: {ok}/{len(candidates)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
