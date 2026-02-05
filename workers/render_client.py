from __future__ import annotations

import os
import re
import json
import math
import base64
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
OPS_MASTER_TAB = os.getenv("OPS_MASTER_TAB", "OPS_MASTER")

RENDER_URL = (os.getenv("RENDER_URL") or "").strip()

# Prefer ONE_LINE; fall back to JSON
SERVICE_ACCOUNT_JSON = (os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON") or "").strip()

RUN_SLOT = (os.getenv("RUN_SLOT") or "").strip().upper()  # "AM" or "PM"

STATUS_READY_TO_POST = os.getenv("STATUS_READY_TO_POST", "READY_TO_POST")
STATUS_READY_TO_PUBLISH = os.getenv("STATUS_READY_TO_PUBLISH", "READY_TO_PUBLISH")

RENDER_MAX_PER_RUN = int(float(os.getenv("RENDER_MAX_PER_RUN", "1") or "1"))

# RDV dynamic_theme column (1-based) if you still use it
RDV_DYNAMIC_THEME_COL = int(float(os.getenv("RDV_DYNAMIC_THEME_COL", "46") or "46"))

# OPS_MASTER theme-of-day cell (V5: B2)
OPS_THEME_CELL = os.getenv("OPS_THEME_CELL", "B2")

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


def _normalize_render_url(url: str) -> str:
    """
    RENDER_URL must already point to the renderer endpoint.
    Example:
      https://greenroomman.pythonanywhere.com/api/render
    Do NOT mutate it.
    """
    u = (url or "").strip()
    if not u:
        raise RuntimeError("RENDER_URL is empty")
    return u.rstrip("/")



def _repair_private_key_json(raw: str) -> str:
    """
    If the JSON string contains actual newlines inside the private_key value,
    json.loads will throw "Invalid control character". This rewrites only the
    private_key value to use escaped \\n.
    """
    # Match "private_key": "...."
    m = re.search(r'"private_key"\s*:\s*"(.*?)"\s*(,|\})', raw, flags=re.DOTALL)
    if not m:
        return raw

    pk_val = m.group(1)
    # Replace actual line breaks with \n escapes (and preserve existing escapes)
    pk_fixed = pk_val.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "\\n")
    start, end = m.span(1)
    return raw[:start] + pk_fixed + raw[end:]


def _load_service_account_info() -> dict:
    raw = SERVICE_ACCOUNT_JSON
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")

    # Optional: allow base64: prefix (handy if someone stores it that way)
    if raw.startswith("base64:"):
        raw = base64.b64decode(raw.split("base64:", 1)[1].strip()).decode("utf-8", "replace")

    # Try clean JSON
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    # Try converting literal "\n" sequences to real newlines (some setups do this)
    try:
        return json.loads(raw.replace("\\n", "\n"))
    except json.JSONDecodeError:
        pass

    # Try repairing private_key field (actual newlines inside JSON string)
    repaired = _repair_private_key_json(raw)
    try:
        return json.loads(repaired)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Service account JSON still invalid after repair: {e}") from e


def _sa_creds() -> Credentials:
    info = _load_service_account_info()
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
    # Accept YYYY-MM-DD or DD/MM/YYYY or DD-MM-YYYY
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%d%m%y")
        except ValueError:
            pass
    # Already ddmmyy?
    if re.fullmatch(r"\d{6}", s):
        return s
    return ""


# ============================================================
# OPS + SELECTION
# ============================================================

def _get_ops_theme(ws_ops: gspread.Worksheet) -> str:
    try:
        raw = (ws_ops.acell(OPS_THEME_CELL).value or "").strip()
    except Exception:
        raw = ""
    return normalize_theme(raw)


def _infer_layout() -> str:
    # Renderer only needs "AM" or "PM"
    if RUN_SLOT in ("AM", "PM"):
        return RUN_SLOT
    return "PM"


def _pick_candidates(ws_raw: gspread.Worksheet, ws_rdv: gspread.Worksheet | None, ops_theme: str) -> List[int]:
    """
    Latest-first render candidates:
    - status in READY_TO_POST / READY_TO_PUBLISH
    - graphic_url blank
    - if RDV exists and has dynamic_theme, prefer that theme, otherwise allow ops theme fallback
    """
    rows = ws_raw.get_all_values()
    if len(rows) < 2:
        return []
    headers = rows[0]
    data = rows[1:]

    i_status = _col_index(headers, "status")
    i_graphic = _col_index(headers, "graphic_url")
    i_ingest = _col_index(headers, "ingested_at_utc")
    i_theme = _col_index(headers, "theme")

    def status_ok(s: str) -> bool:
        s = (s or "").strip().upper()
        return s in (STATUS_READY_TO_POST, STATUS_READY_TO_PUBLISH)

    candidates: List[Tuple[int, str]] = []  # (row_1_based, ingest_val)

    for idx0, r in enumerate(data):
        row_1_based = idx0 + 2
        st = _row_get(r, i_status)
        if not status_ok(st):
            continue
        if _row_get(r, i_graphic):
            continue
        ingest = _row_get(r, i_ingest)
        # If ingest missing, still allow but it will sort later
        candidates.append((row_1_based, ingest))

    # Latest-first: ISO timestamps sort lexicographically
    candidates.sort(key=lambda t: t[1], reverse=True)
    return [r for r, _ in candidates][:RENDER_MAX_PER_RUN]


# ============================================================
# RENDER CALL
# ============================================================

def _post_render(payload: Dict[str, Any]) -> str:
    url = _normalize_render_url(RENDER_URL)
    if not url:
        raise RuntimeError("Missing RENDER_URL")
    resp = requests.post(url, json=payload, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"Render failed ({resp.status_code}): {resp.text}")
    j = resp.json()
    graphic_url = (j.get("graphic_url") or "").strip()
    if not graphic_url:
        raise RuntimeError(f"Render OK but missing graphic_url in response: {j}")
    return graphic_url


def main() -> None:
    print("============================================================")
    print("🖼️  TRAVELTXTTER V5 — RENDER CLIENT START")
    print(f"🎯 Renderer endpoint: {_normalize_render_url(RENDER_URL) or '(missing)'}")
    print("============================================================")

    creds = _sa_creds()
    gc = gspread.authorize(creds)

    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_raw = sh.worksheet(RAW_DEALS_TAB)
    ws_ops = sh.worksheet(OPS_MASTER_TAB)

    # RDV is optional (don’t hard fail if tab removed)
    ws_rdv = None
    try:
        ws_rdv = sh.worksheet(RAW_DEALS_VIEW_TAB)
    except Exception:
        ws_rdv = None

    ops_theme = _get_ops_theme(ws_ops)
    layout = _infer_layout()

    rows = ws_raw.get_all_values()
    if len(rows) < 2:
        print("⚠️ RAW_DEALS empty. Exiting 0.")
        return

    headers = rows[0]
    i_to_city = _col_index(headers, "destination_city")
    i_from_city = _col_index(headers, "origin_city")
    i_out = _col_index(headers, "outbound_date")
    i_in = _col_index(headers, "return_date")
    i_price = _col_index(headers, "price_gbp")
    i_theme = _col_index(headers, "theme")

    candidates = _pick_candidates(ws_raw, ws_rdv, ops_theme)
    if not candidates:
        print("⚠️ No render candidates found. Exiting 0.")
        return

    for row_1_based in candidates:
        row = ws_raw.row_values(row_1_based)
        to_city = _row_get(row, i_to_city) or ""
        from_city = _row_get(row, i_from_city) or ""
        out_date = normalize_date_ddmmyy(_row_get(row, i_out))
        in_date = normalize_date_ddmmyy(_row_get(row, i_in))
        price = normalize_price_gbp(_row_get(row, i_price))

        # Theme: prefer RAW_DEALS.theme if present; else ops theme
        theme_used = normalize_theme(_row_get(row, i_theme) or ops_theme)

        payload = {
            "TO": to_city,
            "FROM": from_city,
            "OUT": out_date,
            "IN": in_date,
            "PRICE": price,
            "layout": layout,
            "theme": theme_used,
        }

        print(f"🎯 Render row {row_1_based}: layout={layout} theme_used='{theme_used}' FROM='{from_city}' TO='{to_city}' PRICE='{price}'")

        graphic_url = _post_render(payload)

        _batch_update(
            ws_raw,
            headers,
            row_1_based,
            {
                "graphic_url": graphic_url,
                "rendered_timestamp": _utc_now_iso(),
                "render_error": "",
            },
        )
        print(f"✅ Rendered row {row_1_based} -> {graphic_url}")

    print("✅ Render client complete.")


if __name__ == "__main__":
    main()
