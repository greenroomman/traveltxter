#!/usr/bin/env python3
"""
TravelTxter V4.5x — telegram_publisher.py (LOCKED + 24h FREE DELAY)

AM (RUN_SLOT=AM):
  consumes: status == POSTED_INSTAGRAM
  writes:   posted_telegram_vip_at
  promotes: POSTED_INSTAGRAM -> POSTED_TELEGRAM_VIP

PM (RUN_SLOT=PM):
  consumes: status == POSTED_TELEGRAM_VIP AND vip_age_hours >= FREE_DELAY_HOURS (default 24)
  writes:   posted_telegram_free_at
  promotes: POSTED_TELEGRAM_VIP -> POSTED_ALL
"""

from __future__ import annotations

import os
import json
import datetime as dt
from typing import Any, Dict, List

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

def clean_url(u: str) -> str:
    return (u or "").strip().replace(" ", "")


# -----------------------------
# Time helpers
# -----------------------------

def parse_iso_utc(s: str) -> dt.datetime | None:
    """
    Accepts: 2026-01-07T07:53:14Z  OR 2026-01-07T07:53:14.123Z
    Returns naive UTC datetime (for simple subtraction).
    """
    t = (s or "").strip()
    if not t:
        return None
    try:
        # strip trailing Z and any timezone info; we store UTC anyway
        t = t.replace("Z", "")
        # fromisoformat handles fractional seconds if present
        return dt.datetime.fromisoformat(t)
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
# Telegram
# -----------------------------

def tg_send(bot_token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    r = requests.post(
        url,
        json={"chat_id": chat_id, "text": text, "disable_web_page_preview": False},
        timeout=30,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"Telegram send failed HTTP {r.status_code}: {r.text[:200]}")


def build_message(row: Dict[str, str], is_vip: bool) -> str:
    price = row.get("price_gbp", "").strip()
    dest = row.get("destination_city", "").strip() or row.get("destination_iata", "").strip()
    origin = row.get("origin_city", "").strip() or row.get("origin_iata", "").strip()
    out_d = row.get("outbound_date", "").strip()
    ret_d = row.get("return_date", "").strip()

    link = row.get("booking_link_vip", "").strip() if is_vip else row.get("affiliate_url", "").strip()
    if not link:
        link = row.get("affiliate_url", "").strip() or row.get("booking_link_vip", "").strip()

    bits = []
    bits.append(f"£{price.replace('£','').strip()} to {dest}".strip())
    bits.append(f"From: {origin}")
    bits.append(f"Dates: {out_d} → {ret_d}")
    if link:
        bits.append(f"Book: {clean_url(link)}")
    return "\n".join(bits).strip()


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    run_slot = env_str("RUN_SLOT", "AM").upper()
    spreadsheet_id = env_str("SPREADSHEET_ID")
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")

    # NEW: delay gate
    free_delay_hours = env_int("FREE_DELAY_HOURS", 24)

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")

    # Tokens/channels
    bot_vip = env_str("TELEGRAM_BOT_TOKEN_VIP")
    chan_vip = env_str("TELEGRAM_CHANNEL_VIP")
    bot_free = env_str("TELEGRAM_BOT_TOKEN")
    chan_free = env_str("TELEGRAM_CHANNEL")

    if run_slot == "AM" and (not bot_vip or not chan_vip):
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN_VIP or TELEGRAM_CHANNEL_VIP")
    if run_slot == "PM" and (not bot_free or not chan_free):
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHANNEL")

    gc = gs_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(tab)

    required = [
        "status",
        "price_gbp",
        "origin_iata",
        "destination_iata",
        "origin_city",
        "destination_city",
        "outbound_date",
        "return_date",
        "affiliate_url",
        "booking_link_vip",
        "posted_telegram_vip_at",
        "posted_telegram_free_at",
    ]
    h = ensure_columns(ws, required)

    rows = ws.get_all_values()
    if len(rows) < 2:
        log("No rows.")
        return 0

    consume_status = "POSTED_INSTAGRAM" if run_slot == "AM" else "POSTED_TELEGRAM_VIP"
    promote_status = "POSTED_TELEGRAM_VIP" if run_slot == "AM" else "POSTED_ALL"
    ts_col = "posted_telegram_vip_at" if run_slot == "AM" else "posted_telegram_free_at"

    now_dt = dt.datetime.utcnow().replace(microsecond=0)

    # Find first eligible row (deterministic)
    target_row_idx = None
    target_row_vals = None

    for i in range(2, len(rows) + 1):
        vals = rows[i - 1]
        status = (vals[h["status"]] if h["status"] < len(vals) else "").strip()
        ts_val = (vals[h[ts_col]] if h[ts_col] < len(vals) else "").strip()

        if status != consume_status or ts_val:
            continue

        # NEW: PM run must be >= 24h after VIP
        if run_slot == "PM":
            vip_ts = (vals[h["posted_telegram_vip_at"]] if h["posted_telegram_vip_at"] < len(vals) else "").strip()
            vip_dt = parse_iso_utc(vip_ts)
            if not vip_dt:
                log(f"⏭️  Skip row {i}: missing/invalid posted_telegram_vip_at (cannot enforce 24h delay)")
                continue

            age_hours = (now_dt - vip_dt).total_seconds() / 3600.0
            if age_hours < float(free_delay_hours):
                log(f"⏭️  Skip row {i}: VIP age {age_hours:.1f}h < {free_delay_hours}h (FREE not due yet)")
                continue

        target_row_idx = i
        target_row_vals = vals
        break

    if not target_row_idx:
        log(f"Done. Telegram posted 0. (No eligible rows for slot={run_slot})")
        return 0

    # Build row dict
    row = {}
    for col, idx in h.items():
        row[col] = target_row_vals[idx] if idx < len(target_row_vals) else ""

    now = now_dt.isoformat() + "Z"

    if run_slot == "AM":
        msg = build_message(row, is_vip=True)
        tg_send(bot_vip, chan_vip, msg)
    else:
        msg = build_message(row, is_vip=False)
        tg_send(bot_free, chan_free, msg)

    # Write back
    ws.update_cell(target_row_idx, h[ts_col] + 1, now)
    ws.update_cell(target_row_idx, h["status"] + 1, promote_status)

    log(f"✅ Telegram posted 1. Row {target_row_idx} -> {promote_status}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
