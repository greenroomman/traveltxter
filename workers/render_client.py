#!/usr/bin/env python3
"""
Render Client worker for TravelTxter V6-safe single-image pipeline

Purpose:
- Generate Instagram graphics for deals ready to post.
- Preserve the existing PythonAnywhere /api/render contract.
- Promote successfully rendered deals to READY_TO_PUBLISH so instagram_publisher.py can pick them up.

Reads:
- RAW_DEALS rows with status READY_TO_POST or READY_TO_PUBLISH and blank graphic_url.

Writes RAW_DEALS only:
- graphic_url
- status -> READY_TO_PUBLISH after a successful render

Contract:
- Idempotent: skips rows where graphic_url already exists.
- Does not alter score, timestamps, phrase fields, enrichment fields, or deal content.
- Compatible with either:
    RENDER_URL=https://greenroomman.pythonanywhere.com/api/render
  or:
    RENDER_BASE_URL=https://greenroomman.pythonanywhere.com
"""

from __future__ import annotations

import datetime as dt
import json
import os
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import gspread
import requests
from google.oauth2.service_account import Credentials


# ------------------ env helpers ------------------

def env(k: str, d: str = "") -> str:
    return (os.getenv(k, d) or "").strip()


# ------------------ robust SA JSON parsing ------------------

def _repair_private_key_newlines(raw: str) -> str:
    pat = re.compile(r'("private_key"\s*:\s*")(.+?)(")', re.DOTALL)
    m = pat.search(raw)
    if not m:
        return raw
    prefix, pk, suffix = m.group(1), m.group(2), m.group(3)
    pk_fixed = pk.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "\\n")
    pk_fixed = pk_fixed.replace("\t", "\\t")
    return raw[: m.start()] + prefix + pk_fixed + suffix + raw[m.end():]


def load_sa_info() -> Dict[str, Any]:
    raw = env("GCP_SA_JSON_ONE_LINE") or env("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")

    attempts = [
        raw,
        raw.replace("\\n", "\n"),
        _repair_private_key_newlines(raw),
        _repair_private_key_newlines(raw).replace("\\n", "\n"),
    ]
    last_error: Optional[Exception] = None
    for candidate in attempts:
        try:
            return json.loads(candidate)
        except Exception as e:
            last_error = e

    raise RuntimeError(f"Could not parse service account JSON: {last_error}")


def gspread_client():
    info = load_sa_info()
    creds = Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


# ------------------ sheet helpers ------------------

def idx_map(headers: List[str]) -> Dict[str, int]:
    return {h: i for i, h in enumerate(headers)}


def row_dict(headers: List[str], row: List[str]) -> Dict[str, str]:
    return {headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))}


def get_cell(row: Dict[str, str], key: str, default: str = "") -> str:
    return (row.get(key) or default or "").strip()


def first_present(row: Dict[str, str], keys: List[str], default: str = "") -> str:
    for key in keys:
        value = get_cell(row, key)
        if value:
            return value
    return default


# ------------------ date + price helpers ------------------

def parse_date(s: str) -> Optional[dt.date]:
    """Parse YYYY-MM-DD or ISO-ish date string."""
    s = (s or "").strip()
    if not s:
        return None

    # Most RAW_DEALS dates should be YYYY-MM-DD.
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except Exception:
        pass

    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y", "%d-%m-%Y", "%d-%m-%y"):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except Exception:
            continue

    return None


def format_ddmmyy(d: dt.date) -> str:
    """Format as ddmmyy, e.g. 120326."""
    return d.strftime("%d%m%y")


def clean_price(raw: str) -> str:
    """Return a clean Â£xxx string and avoid double-Â£ / ÃÂ£ failures."""
    s = (raw or "").strip()
    s = s.replace("ÃÂ£", "").replace("Â£", "").replace(",", "").strip()
    if not s:
        raise ValueError("missing price")
    try:
        value = int(round(float(s)))
    except Exception as e:
        raise ValueError(f"invalid price: {raw!r}") from e
    if value <= 0:
        raise ValueError(f"invalid non-positive price: {raw!r}")
    return f"Â£{value}"


# ------------------ render API ------------------

def resolve_render_endpoint() -> str:
    """
    Supports both the locked guide variable RENDER_URL and the older RENDER_BASE_URL.
    """
    render_url = env("RENDER_URL")
    if render_url:
        render_url = render_url.rstrip("/")
        if render_url.endswith("/api/render"):
            return render_url
        return f"{render_url}/api/render"

    base = env("RENDER_BASE_URL", "https://greenroomman.pythonanywhere.com").rstrip("/")
    return f"{base}/api/render"


def call_render_api(
    endpoint: str,
    *,
    to_city: str,
    from_city: str,
    out_date: str,
    in_date: str,
    price: str,
    layout: str,
    theme: str,
    deal_id: str,
    signal: str = "",
    benefit: str = "",
    phrase_bank: str = "",
    booking_link: str = "",
) -> str:
    """Call PythonAnywhere render API and return graphic_url."""
    payload = {
        # Locked render_api.py contract.
        "TO": to_city,
        "FROM": from_city,
        "OUT": out_date,
        "IN": in_date,
        "PRICE": price,
        "LAYOUT": layout,
        "THEME": theme,
        "deal_id": deal_id,
        # Forward-compatible fields. Current render_api.py may ignore these until upgraded.
        "signal": signal,
        "benefit": benefit,
        "phrase_bank": phrase_bank,
        "booking_link": booking_link,
    }

    response = requests.post(endpoint, json=payload, timeout=45)
    response.raise_for_status()

    try:
        result = response.json()
    except Exception as e:
        raise RuntimeError(f"Render API returned non-JSON response: {response.text[:300]!r}") from e

    if not result.get("ok"):
        raise RuntimeError(f"Render failed: {result}")

    graphic_url = str(result.get("graphic_url") or "").strip()
    if not graphic_url:
        raise RuntimeError(f"No graphic_url in response: {result}")

    parsed = urlparse(graphic_url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise RuntimeError(f"graphic_url is not a public URL: {graphic_url!r}")

    return graphic_url


# ------------------ row extraction ------------------

def require_headers(headers: List[str], required: List[str]) -> None:
    present = set(headers)
    missing = [k for k in required if k not in present]
    if missing:
        raise RuntimeError(f"RAW_DEALS missing required header(s): {', '.join(missing)}")


def determine_layout(rd: Dict[str, str]) -> str:
    publish_window = get_cell(rd, "publish_window").upper()
    if "AM" in publish_window:
        return "AM"
    if "PM" in publish_window:
        return "PM"
    return "PM"


def determine_signal(rd: Dict[str, str]) -> str:
    explicit = first_present(rd, ["signal", "signal_state", "worthiness_verdict", "score_label"])
    if explicit:
        return explicit.upper()

    score_raw = first_present(rd, ["score", "worthiness_score", "price_value_score"])
    try:
        score = float(score_raw)
    except Exception:
        return ""

    # Works whether scores are 0-1 or 0-100.
    if 0 <= score <= 1:
        score *= 100

    if score >= 92:
        return "ELITE"
    if score >= 85:
        return "RARE"
    if score >= 75:
        return "STRONG"
    return "STANDARD"


def build_render_input(rd: Dict[str, str], row_i: int) -> Dict[str, str]:
    deal_id = get_cell(rd, "deal_id") or f"row_{row_i}"

    to_city = first_present(rd, ["destination_city", "to_city", "destination"])
    from_city = first_present(rd, ["origin_city", "from_city", "origin"])
    if not to_city or not from_city:
        raise ValueError("missing origin_city or destination_city")

    out_date_obj = parse_date(first_present(rd, ["outbound_date", "out_date"]))
    in_date_obj = parse_date(first_present(rd, ["return_date", "in_date"]))
    if not out_date_obj or not in_date_obj:
        raise ValueError("invalid outbound_date or return_date")

    theme = first_present(rd, ["theme", "dynamic_theme"], "adventure")
    phrase = first_present(rd, ["phrase_used", "phrase_bank", "benefit", "promo_hint"], "")
    booking_link = first_present(rd, ["booking_link_vip", "affiliate_url", "booking_link"], "")

    return {
        "deal_id": deal_id,
        "to_city": to_city,
        "from_city": from_city,
        "out_date": format_ddmmyy(out_date_obj),
        "in_date": format_ddmmyy(in_date_obj),
        "price": clean_price(first_present(rd, ["price_gbp", "price", "PRICE"])),
        "layout": determine_layout(rd),
        "theme": theme,
        "signal": determine_signal(rd),
        "benefit": phrase,
        "phrase_bank": phrase,
        "booking_link": booking_link,
    }


# ------------------ main ------------------

def main() -> int:
    raw_tab = env("RAW_DEALS_TAB", "RAW_DEALS")
    spreadsheet_id = env("SPREADSHEET_ID") or env("SHEET_ID")
    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")

    endpoint = resolve_render_endpoint()

    gc = gspread_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(raw_tab)

    values = ws.get_all_values()
    if not values:
        raise RuntimeError(f"{raw_tab} is empty")

    headers = values[0]
    h = idx_map(headers)

    required = [
        "status",
        "deal_id",
        "origin_city",
        "destination_city",
        "outbound_date",
        "return_date",
        "price_gbp",
        "graphic_url",
    ]
    require_headers(headers, required)

    to_render: List[Tuple[int, Dict[str, str]]] = []
    for row_i, row in enumerate(values[1:], start=2):
        rd = row_dict(headers, row)
        status = get_cell(rd, "status")
        graphic_url = get_cell(rd, "graphic_url")

        if status not in ("READY_TO_POST", "READY_TO_PUBLISH"):
            continue
        if graphic_url:
            continue

        to_render.append((row_i, rd))

    if not to_render:
        print("â No deals need rendering. All READY_TO_POST/READY_TO_PUBLISH deals already have graphics.")
        return 0

    print(f"ð¸ Render Client V6-safe â Found {len(to_render)} deal(s) needing graphics")
    print(f"ð Render endpoint: {endpoint}")
    print("=" * 72)

    rendered_count = 0
    error_count = 0

    for row_i, rd in to_render:
        deal_id = get_cell(rd, "deal_id") or f"row_{row_i}"

        try:
            render_input = build_render_input(rd, row_i)

            graphic_url = call_render_api(endpoint, **render_input)

            # Write back. gspread uses 1-based columns.
            ws.update_cell(row_i, h["graphic_url"] + 1, graphic_url)
            ws.update_cell(row_i, h["status"] + 1, "READY_TO_PUBLISH")

            print(
                "â "
                f"row={row_i} deal_id={deal_id} "
                f"{render_input['from_city']}â{render_input['to_city']} "
                f"layout={render_input['layout']} theme={render_input['theme']} "
                f"signal={render_input['signal'] or 'n/a'} "
                f"â {graphic_url[:100]}..."
            )
            rendered_count += 1

        except Exception as e:
            print(f"â row={row_i} deal_id={deal_id} â Render failed: {e}")
            error_count += 1
            continue

    print("=" * 72)
    print(f"ð Rendered: {rendered_count} | Errors: {error_count}")

    if error_count > 0 and rendered_count == 0:
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
