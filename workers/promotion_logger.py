#!/usr/bin/env python3
"""
TravelTxter V4.5x — promotion_logger.py (LOCKED)

Purpose:
- Append a "promotion event" row into PROMOTION_QUEUE each time something is posted.
- Designed to run immediately AFTER a publisher step in GitHub Actions.

Idempotent:
- Will NOT duplicate a (deal_id, posted_channel) event if it already exists.

Reads:
- RAW_DEALS

Writes:
- PROMOTION_QUEUE

Env required:
- SPREADSHEET_ID
- GCP_SA_JSON_ONE_LINE (or GCP_SA_JSON)
- POSTED_CHANNEL  (INSTAGRAM | TELEGRAM_VIP | TELEGRAM_FREE)

Env optional:
- RAW_DEALS_TAB (default RAW_DEALS)
- PROMOTION_QUEUE_TAB (default PROMOTION_QUEUE)
- PROMO_LOGGER_MAX_ROWS (default 50)
"""

from __future__ import annotations

import os
import json
import datetime as dt
from typing import Any, Dict, List, Tuple, Set, Optional

import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# Logging / time
# ============================================================

def utcnow() -> dt.datetime:
    return dt.datetime.utcnow()

def ts() -> str:
    return utcnow().replace(microsecond=0).isoformat() + "Z"

def log(msg: str) -> None:
    print(f"{ts()} | {msg}", flush=True)


# ============================================================
# Env
# ============================================================

def env_str(k: str, default: str = "") -> str:
    return (os.environ.get(k, default) or "").strip()

def env_int(k: str, default: int) -> int:
    try:
        return int(env_str(k, str(default)))
    except Exception:
        return default


# ============================================================
# Sheets auth
# ============================================================

def _parse_sa_json(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return json.loads(raw.replace("\\n", "\n"))

def get_client() -> gspread.Client:
    sa = env_str("GCP_SA_JSON_ONE_LINE") or env_str("GCP_SA_JSON")
    if not sa:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE (recommended) or GCP_SA_JSON")
    info = _parse_sa_json(sa)
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds)


# ============================================================
# Sheet helpers
# ============================================================

def ensure_columns(ws: gspread.Worksheet, required_cols: List[str]) -> Dict[str, int]:
    headers = ws.row_values(1)
    if not headers:
        ws.update([required_cols], "A1")
        headers = required_cols[:]
        log(f"🛠️  Initialised headers for {ws.title}")

    headers = [h.strip() for h in headers]
    missing = [c for c in required_cols if c not in headers]
    if missing:
        ws.update([headers + missing], "A1")
        headers = headers + missing
        log(f"🛠️  Added missing columns to {ws.title}: {missing}")

    return {h: i for i, h in enumerate(headers)}

def safe_get(row: List[str], idx: int) -> str:
    return row[idx].strip() if 0 <= idx < len(row) else ""


# ============================================================
# Channel mapping
# ============================================================

def normalise_channel(ch: str) -> str:
    c = (ch or "").strip().upper()
    # Accept a few common aliases
    if c in ("IG", "INSTAGRAM"):
        return "INSTAGRAM"
    if c in ("VIP", "TELEGRAM_VIP"):
        return "TELEGRAM_VIP"
    if c in ("FREE", "TELEGRAM_FREE"):
        return "TELEGRAM_FREE"
    return c

def raw_timestamp_column_for_channel(ch: str) -> str:
    # These columns are written by your publishers. 
    if ch == "INSTAGRAM":
        return "posted_instagram_at"
    if ch == "TELEGRAM_VIP":
        return "posted_telegram_vip_at"
    if ch == "TELEGRAM_FREE":
        return "posted_telegram_free_at"
    raise RuntimeError(f"Unsupported POSTED_CHANNEL: {ch}")


# ============================================================
# Main
# ============================================================

def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    raw_tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    pq_tab = env_str("PROMOTION_QUEUE_TAB", "PROMOTION_QUEUE")
    posted_channel = normalise_channel(env_str("POSTED_CHANNEL"))

    max_rows = env_int("PROMO_LOGGER_MAX_ROWS", 50)

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")
    if not posted_channel:
        raise RuntimeError("Missing POSTED_CHANNEL (INSTAGRAM | TELEGRAM_VIP | TELEGRAM_FREE)")

    raw_ts_col = raw_timestamp_column_for_channel(posted_channel)

    gc = get_client()
    sh = gc.open_by_key(spreadsheet_id)

    raw_ws = sh.worksheet(raw_tab)
    pq_ws = sh.worksheet(pq_tab)

    # RAW_DEALS columns we need
    raw_required = [
        "deal_id",
        "deal_theme",
        "origin_iata",
        "destination_iata",
        "outbound_date",
        "return_date",
        "price_gbp",
        "stops",
        raw_ts_col,
    ]
    raw_h = ensure_columns(raw_ws, raw_required)

    # PROMOTION_QUEUE columns we write into (leave the rest blank)
    # Your exported PQ header includes this exact label. 
    pq_required = [
        "deal_id",
        "theme",
        "origin_iata",
        "destination_iata",
        "outbound_date",
        "return_date",
        "price_gbp",
        "stops",
        "posted_channel (IG / VIP / FREE)",
        "posted_at",
        "clicks (Telegram link clicks if you can track; or proxy)",
        "ctr (if available)",
        "saves / shares / comments (IG)",
        "vip_signups (optional proxy)",
        "performance_score (formula)",
        "hook_pass (formula TRUE/FALSE)",
    ]
    pq_h = ensure_columns(pq_ws, pq_required)

    # Build a dedupe set from existing PROMOTION_QUEUE rows
    pq_vals = pq_ws.get_all_values()
    if len(pq_vals) >= 2:
        pq_headers = [h.strip() for h in pq_vals[0]]
        pq_map = {h: i for i, h in enumerate(pq_headers)}
        didx = pq_map.get("deal_id")
        cidx = pq_map.get("posted_channel (IG / VIP / FREE)")
        existing: Set[Tuple[str, str]] = set()

        if didx is not None and cidx is not None:
            for r in pq_vals[1:]:
                d = safe_get(r, didx)
                c = safe_get(r, cidx).upper()
                if d and c:
                    existing.add((d, c))
    else:
        existing = set()

    # Read RAW_DEALS and append promotion events for any rows that have the channel timestamp set.
    raw_vals = raw_ws.get_all_values()
    if len(raw_vals) < 2:
        log("RAW_DEALS empty. Nothing to log.")
        return 0

    raw_headers = [h.strip() for h in raw_vals[0]]
    raw_map = {h: i for i, h in enumerate(raw_headers)}
    missing_raw = [c for c in raw_required if c not in raw_map]
    if missing_raw:
        raise RuntimeError(f"RAW_DEALS missing required columns (after ensure): {missing_raw}")

    appended = 0

    # Stable scan: top-to-bottom (oldest first). Append up to max_rows.
    for row in raw_vals[1:]:
        if appended >= max_rows:
            break

        deal_id = safe_get(row, raw_map["deal_id"])
        if not deal_id:
            continue

        posted_at = safe_get(row, raw_map[raw_ts_col])
        if not posted_at:
            continue

        key = (deal_id, posted_channel)
        if key in existing:
            continue

        theme = safe_get(row, raw_map["deal_theme"])
        origin = safe_get(row, raw_map["origin_iata"])
        dest = safe_get(row, raw_map["destination_iata"])
        out_d = safe_get(row, raw_map["outbound_date"])
        ret_d = safe_get(row, raw_map["return_date"])
        price = safe_get(row, raw_map["price_gbp"])
        stops = safe_get(row, raw_map["stops"])

        # Construct a row matching PQ headers (only for columns we ensured)
        out_row = [""] * len(pq_ws.row_values(1))
        def put(col: str, val: str) -> None:
            idx = pq_h.get(col)
            if idx is None:
                return
            # Expand if sheet has grown
            if idx >= len(out_row):
                out_row.extend([""] * (idx - len(out_row) + 1))
            out_row[idx] = val

        put("deal_id", deal_id)
        put("theme", theme)
        put("origin_iata", origin)
        put("destination_iata", dest)
        put("outbound_date", out_d)
        put("return_date", ret_d)
        put("price_gbp", price)
        put("stops", stops)
        put("posted_channel (IG / VIP / FREE)", posted_channel)
        put("posted_at", posted_at)

        # Leave metrics blank (formulas will compute later if present)
        pq_ws.append_row(out_row, value_input_option="USER_ENTERED")
        existing.add(key)
        appended += 1
        log(f"🧾 Logged promotion event: deal_id={deal_id} channel={posted_channel} posted_at={posted_at}")

    log(f"Done. Appended {appended} promotion event(s) for channel={posted_channel}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
