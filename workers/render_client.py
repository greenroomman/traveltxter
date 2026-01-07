#!/usr/bin/env python3
"""
TravelTxter V4.5x — render_client.py (BEST-ROW PICK + RESPONSE KEY FIX)

Fixes:
- Renderer returns 'image_url' (not 'graphic_url') in your live response.
- Normalize returned URL to ensure it includes https://
- Healthcheck can be 405 depending on PA config; do not treat as fatal.

Consumes:
- status == READY_TO_POST
- graphic_url blank

Produces:
- graphic_url (sheet column remains graphic_url as your contract)
- rendered_timestamp
- render_error
- render_response_snippet

Promotes:
- READY_TO_POST -> READY_TO_PUBLISH (only after successful render)

Selection:
- Picks BEST eligible row (score/recency) rather than first-in-sheet.
"""

from __future__ import annotations

import os
import json
import time
import datetime as dt
from typing import Dict, Any, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials


# -----------------------------
# Logging
# -----------------------------

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


# -----------------------------
# Env helpers
# -----------------------------

def env_str(k: str, default: str = "") -> str:
    return (os.environ.get(k, default) or "").strip()

def env_int(k: str, default: int) -> int:
    try:
        return int(env_str(k, str(default)))
    except Exception:
        return default


# -----------------------------
# Time + parse helpers
# -----------------------------

def now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def parse_iso_utc(s: str) -> Optional[dt.datetime]:
    t = (s or "").strip()
    if not t:
        return None
    try:
        t = t.replace("Z", "")
        return dt.datetime.fromisoformat(t)
    except Exception:
        return None

def parse_float(s: str) -> Optional[float]:
    t = (s or "").strip().replace("£", "").replace(",", "")
    if not t:
        return None
    try:
        return float(t)
    except Exception:
        return None


# -----------------------------
# Sheets auth
# -----------------------------

def _parse_sa_json(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return json.loads(raw.replace("\\n", "\n"))

def gs_client() -> gspread.Client:
    raw = env_str("GCP_SA_JSON_ONE_LINE")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE")
    info = _parse_sa_json(raw)
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds)

def ensure_columns(ws: gspread.Worksheet, required_cols: List[str]) -> Dict[str, int]:
    headers = ws.row_values(1)
    if not headers:
        ws.update([required_cols], "A1")
        headers = required_cols[:]
        log(f"🛠️  Initialised headers for {ws.title}")

    headers = [h.strip() for h in headers]
    missing = [c for c in required_cols if c not in headers]
    if missing:
        headers = headers + missing
        ws.update([headers], "A1")
        log(f"🛠️  Added missing columns: {missing}")

    return {h: i for i, h in enumerate(headers)}


# -----------------------------
# Renderer call
# -----------------------------

def warm_up(render_url: str) -> None:
    """
    PythonAnywhere may reject GET on /api/render or /api/health depending on config.
    Treat any response as non-fatal; this is just a connectivity hint.
    """
    health = render_url.replace("/api/render", "/api/health")
    try:
        r = requests.get(health, timeout=10)
        log(f"Renderer healthcheck: {r.status_code}")
    except Exception as e:
        log(f"Renderer healthcheck failed (continuing): {e}")

def post_render(render_url: str, payload: Dict[str, str]) -> Dict[str, Any]:
    r = requests.post(render_url, json=payload, timeout=60)
    try:
        j = r.json()
    except Exception:
        j = {"raw": r.text[:300]}
    if r.status_code >= 400:
        raise RuntimeError(f"Render HTTP {r.status_code}: {j}")
    return j


def normalize_image_url(u: str) -> str:
    """
    Ensures returned URL is absolute.
    Accepts:
      - https://greenroomman.pythonanywhere.com/static/...
      - greenroomman.pythonanywhere.com/static/...
      - /static/renders/abc.png  (will be joined with base)
    """
    u = (u or "").strip()
    if not u:
        return ""

    if u.startswith("http://") or u.startswith("https://"):
        return u

    # relative path
    if u.startswith("/"):
        base = env_str("RENDER_URL").split("/api/")[0].rstrip("/")
        return base + u

    # bare host/path
    return "https://" + u.lstrip("/")


def extract_returned_url(j: Dict[str, Any]) -> str:
    """
    Your live renderer returns image_url. Accept multiple keys safely.
    """
    for k in ("graphic_url", "image_url", "url", "image", "png_url"):
        v = j.get(k)
        if isinstance(v, str) and v.strip():
            return normalize_image_url(v)
    return ""


# -----------------------------
# Formatting
# -----------------------------

def to_ddmmyy(date_str: str) -> str:
    """
    Accepts ISO yyyy-mm-dd or already ddmmyy.
    """
    s = (date_str or "").strip()
    if not s:
        return ""
    if len(s) == 6 and s.isdigit():
        return s
    try:
        d = dt.date.fromisoformat(s[:10])
        return d.strftime("%d%m%y")
    except Exception:
        return s

def fmt_price(price_gbp: str) -> str:
    v = parse_float(price_gbp)
    if v is None:
        return (price_gbp or "").strip()
    # renderer contract: £xxx rounded up
    return f"£{int(v + 0.9999)}"


# -----------------------------
# Best-row selection
# -----------------------------

def row_get(vals: List[str], idx: int) -> str:
    return vals[idx].strip() if 0 <= idx < len(vals) else ""

def pick_best_candidate(rows: List[List[str]], h: Dict[str, int]) -> Optional[Tuple[int, List[str]]]:
    candidates: List[Tuple[int, List[str]]] = []

    for sheet_rownum, vals in enumerate(rows, start=2):
        status = row_get(vals, h["status"])
        graphic_url = row_get(vals, h["graphic_url"])
        if status != "READY_TO_POST":
            continue
        if graphic_url:
            continue
        candidates.append((sheet_rownum, vals))

    if not candidates:
        return None

    def sort_key(item: Tuple[int, List[str]]):
        rownum, vals = item
        deal_score = parse_float(row_get(vals, h.get("deal_score", -1))) if "deal_score" in h else None
        scored_ts = parse_iso_utc(row_get(vals, h.get("scored_timestamp", -1))) if "scored_timestamp" in h else None
        created_ts = parse_iso_utc(row_get(vals, h.get("timestamp", -1))) if "timestamp" in h else None
        if created_ts is None and "created_at" in h:
            created_ts = parse_iso_utc(row_get(vals, h.get("created_at", -1)))

        # desc sort by negation
        return (
            -(deal_score if deal_score is not None else -1e18),
            -(scored_ts.timestamp() if scored_ts else -1e18),
            -(created_ts.timestamp() if created_ts else -1e18),
            -rownum,
        )

    candidates.sort(key=sort_key)
    return candidates[0]


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    render_url = env_str("RENDER_URL")
    max_rows = env_int("RENDER_MAX_ROWS", 1)

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")
    if not render_url:
        raise RuntimeError("Missing RENDER_URL")

    gc = gs_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(tab)

    need_cols = [
        "status",
        "deal_id",
        "origin_iata",
        "destination_iata",
        "origin_city",
        "destination_city",
        "outbound_date",
        "return_date",
        "price_gbp",
        "graphic_url",
        "rendered_timestamp",
        "render_error",
        "render_response_snippet",
        # optional rank/recency
        "deal_score",
        "scored_timestamp",
        "timestamp",
        "created_at",
    ]
    h = ensure_columns(ws, need_cols)

    warm_up(render_url)

    values = ws.get_all_values()
    if len(values) < 2:
        log("No rows in RAW_DEALS.")
        return 0

    rows = values[1:]
    rendered = 0

    while rendered < max_rows:
        best = pick_best_candidate(rows, h)
        if not best:
            break

        rownum, vals = best

        def get(col: str) -> str:
            return row_get(vals, h[col]) if col in h else ""

        deal_id = (get("deal_id") or "").strip() or f"row{rownum}"
        to_city = (get("destination_city") or "").strip() or (get("destination_iata") or "").strip()
        from_city = (get("origin_city") or "").strip() or (get("origin_iata") or "").strip()
        out_date = to_ddmmyy(get("outbound_date"))
        in_date = to_ddmmyy(get("return_date"))
        price = fmt_price(get("price_gbp"))

        payload = {
            "TO": to_city,
            "FROM": from_city,
            "OUT": out_date,
            "IN": in_date,
            "PRICE": price,
            "DEAL_ID": deal_id,
        }

        log(f"🖼️  Rendering BEST row {rownum} deal_id={deal_id} ({from_city} -> {to_city})")

        try:
            j = post_render(render_url, payload)
            graphic_url = extract_returned_url(j)
            if not graphic_url:
                raise RuntimeError(f"Renderer returned no usable URL keys: {j}")

            ws.update_cell(rownum, h["graphic_url"] + 1, graphic_url)
            ws.update_cell(rownum, h["rendered_timestamp"] + 1, now_iso())
            ws.update_cell(rownum, h["render_error"] + 1, "")
            ws.update_cell(rownum, h["render_response_snippet"] + 1, str(j)[:250])
            ws.update_cell(rownum, h["status"] + 1, "READY_TO_PUBLISH")

            log(f"✅ Rendered row {rownum} -> READY_TO_PUBLISH ({graphic_url})")
            rendered += 1

            # refresh snapshot for next pick
            values = ws.get_all_values()
            rows = values[1:]

        except Exception as e:
            err = str(e)[:250]
            ws.update_cell(rownum, h["render_error"] + 1, err)
            ws.update_cell(rownum, h["rendered_timestamp"] + 1, now_iso())
            ws.update_cell(rownum, h["render_response_snippet"] + 1, err)
            log(f"❌ Render failed row {rownum}: {err}")
            rendered += 1  # prevent infinite loop

        time.sleep(1)

    log(f"Done. Rendered {rendered}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
