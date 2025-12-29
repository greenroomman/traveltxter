#!/usr/bin/env python3
"""
Traveltxter V3_beta_b_final — Render Worker (PIPELINE-CORRECT)

Pipeline:
- Reads:  raw_status == SCORED
- Renders: calls RENDER_URL (POST JSON)
- Writes: graphic_url, rendered_timestamp
- Promotes: raw_status -> READY_TO_POST
- On failure: raw_status -> RENDER_AGAIN + error columns

Notes:
- Uses header mapping (no column letters)
- Pads ragged rows from get_all_values()
- NEVER uppercases URLs
"""

import os
import sys
import json
import ssl
import datetime as dt
from typing import Dict, Any, List, Optional, Tuple
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

import gspread
from google.oauth2.service_account import Credentials


SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


def die(msg: str, code: int = 1) -> None:
    log(msg)
    sys.exit(code)


def get_env(name: str, required: bool = True, default: str = "") -> str:
    v = os.getenv(name)
    if not v:
        if required:
            die(f"ERROR: Missing environment variable: {name}")
        return default
    return str(v).strip()


def utc_now() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def norm_status(v: Any) -> str:
    """Normalize status (upper/trim + remove invisible chars)."""
    if v is None:
        return ""
    s = str(v)
    s = s.replace("\u00A0", " ")
    s = s.replace("\u200B", "")
    s = s.replace("\uFEFF", "")
    return s.strip().upper()


def build_hmap(headers: List[str]) -> Dict[str, int]:
    return {h.strip(): i + 1 for i, h in enumerate(headers) if h.strip()}


def col_to_a1(n: int) -> str:
    out = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        out = chr(65 + r) + out
    return out


def a1(row: int, col: int) -> str:
    return f"{col_to_a1(col)}{row}"


def get_worksheet():
    sheet_id = get_env("SHEET_ID", required=True)
    worksheet_name = get_env("WORKSHEET_NAME", required=False, default="RAW_DEALS")
    sa_json = get_env("GCP_SA_JSON", required=True)

    try:
        sa_info = json.loads(sa_json)
    except Exception:
        die("ERROR: GCP_SA_JSON must be valid JSON on a single line.")

    creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open_by_key(sheet_id).worksheet(worksheet_name)


def _ssl_context() -> ssl.SSLContext:
    return ssl.create_default_context()


def call_render(render_url: str, payload: Dict[str, Any], timeout_seconds: int) -> Dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    req = Request(
        render_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    ctx = _ssl_context()
    with urlopen(req, timeout=timeout_seconds, context=ctx) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
        try:
            return json.loads(raw)
        except Exception:
            raise RuntimeError("Render response was not JSON. First 300 chars: " + raw[:300])


def get_all(ws) -> Tuple[List[str], List[List[str]]]:
    values = ws.get_all_values()
    if len(values) < 2:
        return [], []
    return values[0], values[1:]


def pad_row(row: List[str], header_len: int) -> List[str]:
    return (row + [""] * header_len)[:header_len]


def record_from_row(headers: List[str], row: List[str]) -> Dict[str, str]:
    return {headers[i]: row[i] for i in range(len(headers))}


def batch_update_row(ws, row: int, hmap: Dict[str, int], updates: Dict[str, Any]) -> None:
    data = []
    for k, v in updates.items():
        if k in hmap:
            data.append({"range": a1(row, hmap[k]), "values": [[v]]})
    if data:
        ws.batch_update(data)


def pick(rec: Dict[str, Any], *keys: str) -> str:
    for k in keys:
        v = rec.get(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return ""


def main() -> None:
    log("🎨 RENDER WORKER STARTING")

    render_url = get_env("RENDER_URL", required=True)
    timeout_s = int(get_env("RENDER_TIMEOUT", required=False, default="30"))
    max_renders = int(get_env("MAX_RENDERS_PER_RUN", required=False, default="5"))

    status_col = get_env("RENDER_STATUS_COLUMN", required=False, default="raw_status")
    input_status = get_env("RENDER_INPUT_STATUS", required=False, default="SCORED").strip().upper()
    output_status = get_env("RENDER_OUTPUT_STATUS", required=False, default="READY_TO_POST").strip().upper()

    ws = get_worksheet()
    log(f"Connected to worksheet: {ws.title}")

    headers, rows = get_all(ws)
    if not headers:
        log("No data found.")
        return

    headers = [h.strip() for h in headers]
    hmap = build_hmap(headers)
    header_len = len(headers)

    required = [
        status_col,
        "graphic_url",
        "rendered_timestamp",
        "render_error",
        "render_http_status",
        "render_response_snippet",
    ]
    missing = [c for c in required if c not in hmap]
    if missing:
        die("ERROR: Missing required columns: " + ", ".join(missing))

    status_idx0 = hmap[status_col] - 1

    renders_done = 0

    for row_num, row in enumerate(rows, start=2):
        if renders_done >= max_renders:
            break

        padded = pad_row(row, header_len)
        raw_status = norm_status(padded[status_idx0])

        if raw_status != input_status:
            continue

        rec = record_from_row(headers, padded)

        deal_id = pick(rec, "deal_id", "DEAL_ID")
        origin = pick(rec, "origin_city", "ORIGIN_CITY")
        dest = pick(rec, "destination_city", "DESTINATION_CITY")
        price = pick(rec, "price_gbp", "PRICE_GBP")
        out_date = pick(rec, "outbound_date", "OUTBOUND_DATE")

        log(f"Candidate row {row_num}: deal_id={deal_id} {origin}->{dest} £{price} outbound={out_date}")

        # Build payload (keep predictable)
        payload = {
            "deal_id": deal_id,
            "origin_city": origin,
            "destination_city": dest,
            "destination_country": pick(rec, "destination_country", "DESTINATION_COUNTRY"),
            "price_gbp": price,
            "outbound_date": out_date,
            "return_date": pick(rec, "return_date", "RETURN_DATE"),
            "trip_length_days": pick(rec, "trip_length_days", "TRIP_LENGTH_DAYS"),
            "stops": pick(rec, "stops", "STOPS"),
            "baggage_included": pick(rec, "baggage_included", "BAGGAGE_INCLUDED"),
            "airline": pick(rec, "airline", "AIRLINE"),
            "ai_score": pick(rec, "ai_score", "AI_SCORE"),
            "ai_grading": pick(rec, "ai_grading", "AI_GRADING"),
            "ai_verdict": pick(rec, "ai_verdict", "AI_VERDICT"),
            "ai_caption": pick(rec, "ai_caption", "AI_CAPTION"),
        }

        try:
            result = call_render(render_url, payload, timeout_seconds=timeout_s)

            graphic_url = (result.get("graphic_url") or result.get("image_url") or "").strip()
            if not graphic_url:
                raise RuntimeError("Render returned no graphic_url/image_url.")

            batch_update_row(
                ws,
                row_num,
                hmap,
                {
                    "graphic_url": graphic_url,
                    "rendered_timestamp": utc_now(),
                    "render_error": "",
                    "render_http_status": "200",
                    "render_response_snippet": "",
                    status_col: output_status,
                },
            )
            log(f"✅ Render OK row {row_num}: {status_col}={output_status}")
            renders_done += 1

        except HTTPError as e:
            try:
                body = e.read().decode("utf-8", errors="replace")
            except Exception:
                body = ""
            snippet = (body or "")[:240]
            status = str(getattr(e, "code", "HTTPError"))
            batch_update_row(
                ws,
                row_num,
                hmap,
                {
                    "rendered_timestamp": utc_now(),
                    "render_error": "HTTPError " + status,
                    "render_http_status": status,
                    "render_response_snippet": snippet,
                    status_col: "RENDER_AGAIN",
                },
            )
            log(f"❌ Render FAILED row {row_num}: HTTPError {status}")
            renders_done += 1

        except URLError as e:
            msg = str(e)
            batch_update_row(
                ws,
                row_num,
                hmap,
                {
                    "rendered_timestamp": utc_now(),
                    "render_error": "URLError: " + msg[:200],
                    "render_http_status": "",
                    "render_response_snippet": "",
                    status_col: "RENDER_AGAIN",
                },
            )
            log(f"❌ Render FAILED row {row_num}: URLError")
            renders_done += 1

        except Exception as e:
            msg = str(e)
            batch_update_row(
                ws,
                row_num,
                hmap,
                {
                    "rendered_timestamp": utc_now(),
                    "render_error": "ERROR: " + msg[:200],
                    "render_http_status": "",
                    "render_response_snippet": "",
                    status_col: "RENDER_AGAIN",
                },
            )
            log(f"❌ Render FAILED row {row_num}: {msg[:120]}")
            renders_done += 1

    log(f"Done. Render attempts this run: {renders_done}")


if __name__ == "__main__":
    main()
