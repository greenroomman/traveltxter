#!/usr/bin/env python3
"""
Render Client worker for TravelTxter V6-safe single-image pipeline + MIZAR.

Preserves the existing pipeline contract:
- Reads RAW_DEALS rows with status READY_TO_POST or READY_TO_PUBLISH and blank graphic_url.
- Calls the existing PythonAnywhere /api/render endpoint.
- Writes graphic_url and promotes successful renders to READY_TO_PUBLISH.
- Never blocks rendering if MIZAR is unavailable.

MIZAR integration:
- Uses the real route, outbound/return dates and real price_gbp from RAW_DEALS.
- Calls /v1/signal with a 5 second timeout.
- mizar_signal becomes true only when regret_risk_score >= 0.65 and, when the API
  returns gated_recommendation, that recommendation is book_now.
- Passes mizar_score and mizar_signal through to the renderer.
- Writes mizar_score / mizar_signal only when those columns exist.
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

MIZAR_THRESHOLD = 0.65


def env(k: str, d: str = "") -> str:
    return (os.getenv(k, d) or "").strip()


def env_int(k: str, d: int) -> int:
    try:
        return int(env(k, str(d)))
    except Exception:
        return d


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
        except Exception as exc:
            last_error = exc
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


def parse_date(s: str) -> Optional[dt.date]:
    s = (s or "").strip()
    if not s:
        return None
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
    return d.strftime("%d%m%y")


def price_float(raw: str) -> float:
    s = (raw or "").strip().replace("£", "").replace("Â£", "").replace("ÃÂ£", "").replace(",", "")
    value = float(s)
    if value <= 0:
        raise ValueError(f"invalid non-positive price: {raw!r}")
    return value


def clean_price(raw: str) -> str:
    return f"£{int(round(price_float(raw)))}"


def resolve_render_endpoint() -> str:
    render_url = env("RENDER_URL")
    if render_url:
        render_url = render_url.rstrip("/")
        if render_url.endswith("/api/render"):
            return render_url
        return f"{render_url}/api/render"
    base = env("RENDER_BASE_URL", "https://greenroomman.pythonanywhere.com").rstrip("/")
    return f"{base}/api/render"


def call_mizar(rd: Dict[str, str]) -> Tuple[Optional[float], bool]:
    """Return (score, signal). Failure never blocks rendering."""
    api_key = env("MIZAR_API_KEY")
    if not api_key:
        print("  ⚠️ MIZAR_API_KEY missing; continuing without MIZAR")
        return None, False

    origin = first_present(rd, ["origin_iata", "origin"]).upper()
    destination = first_present(rd, ["destination_iata", "destination"]).upper()
    outbound = first_present(rd, ["outbound_date", "out_date"])
    return_date = first_present(rd, ["return_date", "in_date"])
    raw_price = first_present(rd, ["price_gbp", "price", "PRICE"])

    if not origin or not destination or not outbound or not raw_price:
        print("  ⚠️ MIZAR skipped: missing route/date/price")
        return None, False

    try:
        payload: Dict[str, Any] = {
            "origin": origin,
            "destination": destination,
            "outbound_date": outbound[:10],
            "price_gbp": price_float(raw_price),
            "session_id": "traveltxter_render",
            "client_platform": "social",
            "decision_source_type": "traveltxter_social",
            "cabin_class": first_present(rd, ["cabin_class"], "economy"),
        }
        if return_date:
            payload["return_date"] = return_date[:10]
            payload["trip_type"] = "return"
        else:
            payload["return_date"] = None
            payload["trip_type"] = "oneway"

        base = env("MIZAR_API_BASE", "https://mizar-api.vercel.app").rstrip("/")
        response = requests.post(
            f"{base}/v1/signal",
            json=payload,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            timeout=5,
        )
        response.raise_for_status()
        data = response.json()

        if data.get("regret_risk_score") is None:
            raise RuntimeError(f"MIZAR response missing regret_risk_score: {data}")

        score = float(data["regret_risk_score"])
        gated = str(data.get("gated_recommendation") or "").strip().lower()
        signal = score >= MIZAR_THRESHOLD and (not gated or gated == "book_now")

        gate_note = gated or "not_returned"
        print(f"  ✓ MIZAR {origin}→{destination}: score={score:.2f} gate={gate_note} signal={signal}")
        return score, signal

    except requests.exceptions.Timeout:
        print("  ⚠️ MIZAR timeout after 5s; continuing without MIZAR")
        return None, False
    except Exception as exc:
        print(f"  ⚠️ MIZAR unavailable: {str(exc)[:180]}; continuing without MIZAR")
        return None, False


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
    mizar_score: Optional[float] = None,
    mizar_signal: bool = False,
) -> str:
    payload = {
        "TO": to_city,
        "FROM": from_city,
        "OUT": out_date,
        "IN": in_date,
        "PRICE": price,
        "LAYOUT": layout,
        "THEME": theme,
        "deal_id": deal_id,
        "signal": signal,
        "benefit": benefit,
        "phrase_bank": phrase_bank,
        "booking_link": booking_link,
        "mizar_score": mizar_score if mizar_score is not None else 0.0,
        "mizar_signal": bool(mizar_signal),
    }

    response = requests.post(endpoint, json=payload, timeout=45)
    response.raise_for_status()
    try:
        result = response.json()
    except Exception as exc:
        raise RuntimeError(f"Render API returned non-JSON response: {response.text[:300]!r}") from exc

    if not result.get("ok"):
        raise RuntimeError(f"Render failed: {result}")

    graphic_url = str(result.get("graphic_url") or "").strip()
    if not graphic_url:
        raise RuntimeError(f"No graphic_url in response: {result}")

    parsed = urlparse(graphic_url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise RuntimeError(f"graphic_url is not a public URL: {graphic_url!r}")
    return graphic_url


def require_headers(headers: List[str], required: List[str]) -> None:
    present = set(headers)
    missing = [key for key in required if key not in present]
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
    if 0 <= score <= 1:
        score *= 100
    if score >= 92:
        return "ELITE"
    if score >= 85:
        return "RARE"
    if score >= 75:
        return "STRONG"
    return "STANDARD"


def build_render_input(rd: Dict[str, str], row_i: int) -> Dict[str, Any]:
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
    mizar_score, mizar_signal = call_mizar(rd)

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
        "mizar_score": mizar_score,
        "mizar_signal": mizar_signal,
    }


def main() -> int:
    raw_tab = env("RAW_DEALS_TAB", "RAW_DEALS")
    spreadsheet_id = env("SPREADSHEET_ID") or env("SHEET_ID")
    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")

    endpoint = resolve_render_endpoint()
    render_max = env_int("RENDER_MAX_ROWS", 2)

    gc = gspread_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(raw_tab)
    values = ws.get_all_values()
    if not values:
        raise RuntimeError(f"{raw_tab} is empty")

    headers = values[0]
    h = idx_map(headers)
    require_headers(
        headers,
        [
            "status",
            "deal_id",
            "origin_city",
            "destination_city",
            "outbound_date",
            "return_date",
            "price_gbp",
            "graphic_url",
        ],
    )

    to_render: List[Tuple[int, Dict[str, str]]] = []
    for row_i, row in enumerate(values[1:], start=2):
        rd = row_dict(headers, row)
        if get_cell(rd, "status") not in ("READY_TO_POST", "READY_TO_PUBLISH"):
            continue
        if get_cell(rd, "graphic_url"):
            continue
        to_render.append((row_i, rd))

    if not to_render:
        print("✓ No deals need rendering. All READY_TO_POST/READY_TO_PUBLISH deals already have graphics.")
        return 0

    print(f"📸 Render Client V6-safe + MIZAR — Found {len(to_render)} deal(s) needing graphics")
    print(f"🔗 Render endpoint: {endpoint}")
    print("=" * 72)

    rendered_count = 0
    error_count = 0

    for row_i, rd in to_render[:render_max]:
        deal_id = get_cell(rd, "deal_id") or f"row_{row_i}"
        try:
            render_input = build_render_input(rd, row_i)
            graphic_url = call_render_api(endpoint, **render_input)

            ws.update_cell(row_i, h["graphic_url"] + 1, graphic_url)
            ws.update_cell(row_i, h["status"] + 1, "READY_TO_PUBLISH")

            if "mizar_score" in h and render_input["mizar_score"] is not None:
                ws.update_cell(row_i, h["mizar_score"] + 1, f"{render_input['mizar_score']:.4f}")
            if "mizar_signal" in h:
                ws.update_cell(row_i, h["mizar_signal"] + 1, "TRUE" if render_input["mizar_signal"] else "FALSE")

            print(
                f"✓ row={row_i} deal_id={deal_id} "
                f"{render_input['from_city']}→{render_input['to_city']} "
                f"layout={render_input['layout']} theme={render_input['theme']} "
                f"mizar={render_input['mizar_score'] if render_input['mizar_score'] is not None else 'n/a'} "
                f"signal={render_input['mizar_signal']} → {graphic_url[:100]}..."
            )
            rendered_count += 1
        except Exception as exc:
            print(f"❌ row={row_i} deal_id={deal_id} — Render failed: {exc}")
            error_count += 1

    print("=" * 72)
    print(f"📊 Rendered: {rendered_count} | Errors: {error_count}")
    if error_count > 0 and rendered_count == 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
