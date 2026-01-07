#!/usr/bin/env python3
"""
TravelTxter V4.5x — render_client.py (LOCKED)

Consumes:
- status == READY_TO_POST
- graphic_url blank

Produces:
- graphic_url
- rendered_timestamp
- render_error
- render_response_snippet

Promotes:
- READY_TO_POST -> READY_TO_PUBLISH (only after successful render)

Non-negotiable render payload:
{
  "TO": "City",
  "FROM": "City",
  "OUT": "DDMMYY",
  "IN": "DDMMYY",
  "PRICE": "£123",
}
"""

from __future__ import annotations

import os
import json
import time
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse

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
# Env
# -----------------------------

def env_str(k: str, default: str = "") -> str:
    return (os.environ.get(k, default) or "").strip()

def env_int(k: str, default: int) -> int:
    try:
        return int(env_str(k, str(default)))
    except Exception:
        return default


# -----------------------------
# Sheets auth
# -----------------------------

def _parse_sa_json(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return json.loads(raw.replace("\\n", "\n"))

def gs_client():
    raw = env_str("GCP_SA_JSON_ONE_LINE")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE")
    info = _parse_sa_json(raw)
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds)


def ensure_columns(ws, required_cols: List[str]) -> Dict[str, int]:
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
# URL normalisation + warmup
# -----------------------------

def normalise_render_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return u
    parsed = urlparse(u)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc or parsed.path  # handle accidental "domain.com/api/render"
    path = parsed.path if parsed.netloc else ""
    if not path.endswith("/api/render"):
        path = "/api/render"
    return urlunparse((scheme, netloc, path, "", "", ""))

def health_url_from_render_url(render_url: str) -> str:
    """
    PythonAnywhere app exposes GET /health (NOT /api/health)
    """
    parsed = urlparse(render_url)
    return urlunparse((parsed.scheme, parsed.netloc, "/health", "", "", ""))

def warm_up(render_url: str) -> None:
    hu = health_url_from_render_url(render_url)
    try:
        r = requests.get(hu, timeout=15, headers={"User-Agent": "traveltxter-render-client/1.0"})
        log(f"🫖 Render health: {r.status_code}")
    except Exception as e:
        log(f"🫖 Warmup failed (will still try render): {str(e)[:160]}")


# -----------------------------
# Formatting helpers
# -----------------------------

def ddmmyy(iso_yyyy_mm_dd: str) -> str:
    s = (iso_yyyy_mm_dd or "").strip()
    try:
        d = dt.datetime.strptime(s[:10], "%Y-%m-%d").date()
        return d.strftime("%d%m%y")
    except Exception:
        # allow already ddmmyy
        s2 = "".join(ch for ch in s if ch.isdigit())
        return s2[-6:] if len(s2) >= 6 else s2

def money_gbp(price_gbp: Any) -> str:
    s = str(price_gbp or "").strip().replace("£", "").replace(",", "")
    try:
        v = float(s)
        # ceil-ish
        return f"£{int(v + 0.999)}"
    except Exception:
        return f"£{s}" if s else "£0"


def is_iata3(x: str) -> bool:
    s = (x or "").strip().upper()
    return len(s) == 3 and s.isalpha()


UK_AIRPORT_CITY_FALLBACK = {
    "LHR": "London",
    "LGW": "London",
    "STN": "London",
    "LTN": "London",
    "LCY": "London",
    "SEN": "London",
    "MAN": "Manchester",
    "BRS": "Bristol",
    "BHX": "Birmingham",
    "EDI": "Edinburgh",
    "GLA": "Glasgow",
    "NCL": "Newcastle",
    "LPL": "Liverpool",
    "NQY": "Newquay",
    "SOU": "Southampton",
    "CWL": "Cardiff",
    "EXT": "Exeter",
}


# -----------------------------
# Render with retries
# -----------------------------

def post_render(render_url: str, payload: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], str, int]:
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "traveltxter-render-client/1.0",
    }
    r = requests.post(render_url, json=payload, timeout=(15, 90), headers=headers)
    snippet = (r.text or "")[:320].replace("\n", " ")
    if r.status_code >= 400:
        return False, {}, snippet, r.status_code
    try:
        return True, r.json(), snippet, r.status_code
    except Exception:
        return False, {}, snippet, r.status_code


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    render_url_raw = env_str("RENDER_URL")
    render_max = env_int("RENDER_MAX_ROWS", 1)

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")
    if not render_url_raw:
        raise RuntimeError("Missing RENDER_URL")

    render_url = normalise_render_url(render_url_raw)
    if render_url != render_url_raw:
        log(f"🔧 Normalised RENDER_URL: {render_url_raw} -> {render_url}")

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
    ]
    h = ensure_columns(ws, need_cols)

    warm_up(render_url)

    rows = ws.get_all_values()
    if len(rows) < 2:
        log("No rows in RAW_DEALS.")
        return 0

    rendered = 0

    for i in range(2, len(rows) + 1):  # 1-indexed in Sheets, skip header
        if rendered >= render_max:
            break

        row = rows[i - 1]
        def get(col: str) -> str:
            idx = h[col]
            return row[idx] if idx < len(row) else ""

        status = (get("status") or "").strip()
        graphic_url = (get("graphic_url") or "").strip()

        if status != "READY_TO_POST":
            continue
        if graphic_url:
            continue

        deal_id = (get("deal_id") or "").strip() or f"row{i}"
        to_city = (get("destination_city") or "").strip()
        from_city = (get("origin_city") or "").strip()

        # Hard fallback: if sheet only has IATA
        if not to_city:
            to_city = (get("destination_iata") or "").strip()
        if not from_city:
            from_city = (get("origin_iata") or "").strip()

        # If still IATA, translate UK origins so images look human
        if is_iata3(from_city):
            from_city = UK_AIRPORT_CITY_FALLBACK.get(from_city.upper(), from_city)

        out_date = ddmmyy(get("outbound_date"))
        in_date = ddmmyy(get("return_date"))
        price = money_gbp(get("price_gbp"))

        payload = {"TO": to_city, "FROM": from_city, "OUT": out_date, "IN": in_date, "PRICE": price, "DEAL_ID": deal_id}
        log(f"🎨 Rendering row {i} payload={payload}")

        ok = False
        last_snip = ""
        last_code = 0
        last_json: Dict[str, Any] = {}

        for attempt in range(1, 6):
            ok, data, snip, code = post_render(render_url, payload)
            last_snip, last_code, last_json = snip, code, data

            if ok and isinstance(data, dict) and (data.get("image_url") or data.get("graphic_url")):
                break

            sleep_s = [0, 4, 6, 10, 16, 24][attempt]
            log(f"⏳ Render retry {attempt}/5 in {sleep_s}s... (Render HTTP {code} :: {snip[:160]})")
            time.sleep(sleep_s)

        # Prepare updates
        now = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        updates = {}
        if ok:
            image_url = (last_json.get("image_url") or last_json.get("graphic_url") or "").strip()
            if image_url and image_url.startswith("/"):
                image_url = f"{urlparse(render_url).scheme}://{urlparse(render_url).netloc}{image_url}"

            if image_url:
                updates["graphic_url"] = image_url
                updates["rendered_timestamp"] = now
                updates["render_error"] = ""
                updates["render_response_snippet"] = ""
                updates["status"] = "READY_TO_PUBLISH"
                rendered += 1
            else:
                updates["render_error"] = f"Render OK but no image_url/graphic_url in JSON (HTTP {last_code})"
                updates["render_response_snippet"] = (json.dumps(last_json)[:300] if last_json else last_snip[:300])
        else:
            updates["render_error"] = f"Render HTTP {last_code}"
            updates["render_response_snippet"] = last_snip[:300]

        # Write updates (single row, multiple cells)
        for col, val in updates.items():
            cidx = h[col] + 1
            ws.update_cell(i, cidx, val)

        if not ok or "graphic_url" not in updates:
            log(f"❌ Render failed row {i}: {updates.get('render_error','unknown')} :: {updates.get('render_response_snippet','')[:140]}")
        else:
            log(f"✅ Rendered row {i} -> READY_TO_PUBLISH")

    log(f"Done. Rendered {rendered}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
