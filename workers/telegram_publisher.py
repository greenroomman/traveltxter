#!/usr/bin/env python3
"""
TravelTxter V4.5x — telegram_publisher.py (LOCKED + 24h FREE gate)

AM (RUN_SLOT=AM):
  consumes: status == POSTED_INSTAGRAM
  writes:   posted_telegram_vip_at
  promotes: POSTED_INSTAGRAM -> POSTED_TELEGRAM_VIP

PM (RUN_SLOT=PM):
  consumes: status == POSTED_TELEGRAM_VIP
  gate:     posted_telegram_vip_at must be >= FREE_DELAY_HOURS ago (default 24)
  writes:   posted_telegram_free_at
  promotes: POSTED_TELEGRAM_VIP -> POSTED_ALL

Notes:
- Uses Telegram HTML parse_mode for proper hyperlinks (no messy raw URLs).
- Message formatting matches your definitive templates (no underscore divider lines).
- Headline uses Title Case for cities; TO: line uses UPPERCASE.
"""

from __future__ import annotations

import os
import json
import datetime as dt
from typing import Any, Dict, List, Tuple, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# Logging
# ============================================================

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


# ============================================================
# Env helpers
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

def _parse_sa(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()
    try:
        return json.loads(raw)
    except Exception:
        return json.loads(raw.replace("\\n", "\n"))

def get_client() -> gspread.Client:
    sa_raw = env_str("GCP_SA_JSON_ONE_LINE") or env_str("GCP_SA_JSON")
    if not sa_raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
    info = _parse_sa(sa_raw)
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds)


# ============================================================
# Sheet helpers
# ============================================================

def ensure_columns(ws: gspread.Worksheet, required: List[str]) -> Dict[str, int]:
    headers = ws.row_values(1)
    if not headers:
        ws.update([required], "A1")
        headers = required[:]
        log(f"🛠️  Created headers for {ws.title}")

    headers = [h.strip() for h in headers]
    missing = [c for c in required if c not in headers]
    if missing:
        ws.update([headers + missing], "A1")
        headers = headers + missing
        log(f"🛠️  Added missing columns: {missing}")

    return {h: i for i, h in enumerate(headers)}

def safe_get(vals: List[str], idx: int) -> str:
    return vals[idx].strip() if 0 <= idx < len(vals) else ""


# ============================================================
# Telegram helpers
# ============================================================

def html_escape(s: str) -> str:
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )

def tg_send(bot_token: str, chat_id: str, html_text: str, preview: bool = False) -> None:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": html_text,
        "parse_mode": "HTML",
        "disable_web_page_preview": (not preview),
    }
    r = requests.post(url, json=payload, timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"Telegram send failed HTTP {r.status_code}: {r.text[:200]}")


# ============================================================
# Time parsing / 24h gate
# ============================================================

def parse_isoish(ts: str) -> Optional[dt.datetime]:
    s = (ts or "").strip()
    if not s:
        return None
    # Accept "2026-01-07T12:21:40Z" or without Z
    try:
        if s.endswith("Z"):
            s = s[:-1]
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None

def is_older_than_hours(ts_str: str, hours: int) -> bool:
    t = parse_isoish(ts_str)
    if not t:
        return False
    cutoff = dt.datetime.utcnow() - dt.timedelta(hours=hours)
    return t <= cutoff


# ============================================================
# Formatting helpers
# ============================================================

def title_case_city(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    # Keep common short codes as-is (e.g., "USA", "UAE")
    if s.isupper() and len(s) <= 4:
        return s
    return " ".join([w.capitalize() for w in s.split()])

def upper_city(s: str) -> str:
    return (s or "").strip().upper()

def clean_price(price_gbp: str) -> str:
    p = (price_gbp or "").strip().replace("£", "")
    if not p:
        return ""
    return f"£{p}"

def pick_phrase(row: Dict[str, str]) -> str:
    """
    Optional: if you later add a column like 'phrase_bank' or 'benefit_line',
    this will use it automatically.
    """
    for key in ["phrase_bank", "benefit_line", "benefit_text", "why_good"]:
        v = (row.get(key) or "").strip()
        if v:
            return v
    return ""


# ============================================================
# Message templates (matches your definitive examples)
# ============================================================

def build_vip_message(row: Dict[str, str]) -> str:
    price = clean_price(row.get("price_gbp", ""))
    dest_city = title_case_city(row.get("destination_city", "")) or row.get("destination_iata", "").strip().upper()
    dest_to_line = upper_city(row.get("destination_city", "")) or row.get("destination_iata", "").strip().upper()
    origin_city = title_case_city(row.get("origin_city", "")) or row.get("origin_iata", "").strip().upper()

    out_d = (row.get("outbound_date") or "").strip()
    ret_d = (row.get("return_date") or "").strip()

    # Prefer VIP booking link, fallback to affiliate_url
    link = (row.get("booking_link_vip") or "").strip() or (row.get("affiliate_url") or "").strip()

    phrase = pick_phrase(row)

    lines = []
    # Headline
    lines.append(f"{html_escape(price)} to {html_escape(dest_city)}")
    # Body
    lines.append(f"TO: {html_escape(dest_to_line)}")
    lines.append(f"FROM: {html_escape(origin_city)}")
    lines.append(f"OUT: {html_escape(out_d)}")
    lines.append(f"BACK: {html_escape(ret_d)}")

    if phrase:
        lines.append("")
        lines.append(html_escape(phrase))

    if link:
        lines.append("")
        lines.append(f"<a href=\"{html_escape(link)}\">BOOKING LINK</a>")

    return "\n".join(lines).strip()


def build_free_message(row: Dict[str, str], monthly_link: str, yearly_link: str) -> str:
    price = clean_price(row.get("price_gbp", ""))
    dest_city = title_case_city(row.get("destination_city", "")) or row.get("destination_iata", "").strip().upper()
    dest_to_line = upper_city(row.get("destination_city", "")) or row.get("destination_iata", "").strip().upper()
    origin_city = title_case_city(row.get("origin_city", "")) or row.get("origin_iata", "").strip().upper()

    out_d = (row.get("outbound_date") or "").strip()
    ret_d = (row.get("return_date") or "").strip()

    phrase = pick_phrase(row)

    lines = []
    lines.append(f"{html_escape(price)} to {html_escape(dest_city)}")
    lines.append(f"TO: {html_escape(dest_to_line)}")
    lines.append(f"FROM: {html_escape(origin_city)}")
    lines.append(f"OUT: {html_escape(out_d)}")
    lines.append(f"BACK: {html_escape(ret_d)}")

    if phrase:
        lines.append("")
        lines.append(html_escape(phrase))

    # Upsell block (no divider line)
    lines.append("")
    lines.append("Want instant access?")
    lines.append("Join TravelTxter for early access")
    lines.append("")
    lines.append("• VIP members saw this 24 hours ago")
    lines.append("• Deals 24 hours early")
    lines.append("• Direct booking links")
    lines.append("• Exclusive mistake fares")
    lines.append("• £3 p/m or £30 p/a")
    lines.append("• Cancel anytime")

    links = []
    if monthly_link:
        links.append(f"<a href=\"{html_escape(monthly_link)}\">Upgrade now (Monthly)</a>")
    if yearly_link:
        links.append(f"<a href=\"{html_escape(yearly_link)}\">Upgrade now (Annual)</a>")
    if links:
        lines.append("")
        lines.extend(links)

    return "\n".join(lines).strip()


# ============================================================
# Selection logic (best row)
# ============================================================

def row_to_dict(headers: List[str], vals: List[str]) -> Dict[str, str]:
    d = {}
    for i, h in enumerate(headers):
        d[h] = vals[i].strip() if i < len(vals) else ""
    return d

def pick_best_candidate(
    headers: List[str],
    rows: List[List[str]],
    hmap: Dict[str, int],
    consume_status: str,
    ts_col: str,
    run_slot: str,
    free_delay_hours: int,
) -> Optional[Tuple[int, List[str]]]:
    """
    Returns (sheet_row_index, row_values) for the best eligible row.
    If deal_score exists: pick highest deal_score, tie-break newest-ish by row order.
    Otherwise: pick first eligible (deterministic).
    """
    candidates: List[Tuple[float, int, List[str]]] = []
    has_score = "deal_score" in hmap

    for sheet_row_idx in range(2, len(rows) + 1):
        vals = rows[sheet_row_idx - 1]
        status = safe_get(vals, hmap["status"])
        if status != consume_status:
            continue

        already = safe_get(vals, hmap[ts_col])
        if already:
            continue

        # 24h gate for PM free
        if run_slot == "PM":
            vip_ts = safe_get(vals, hmap.get("posted_telegram_vip_at", -1)) if "posted_telegram_vip_at" in hmap else ""
            if not is_older_than_hours(vip_ts, free_delay_hours):
                continue

        if has_score:
            s_raw = safe_get(vals, hmap["deal_score"])
            try:
                score = float(s_raw)
            except Exception:
                score = 0.0
            candidates.append((score, sheet_row_idx, vals))
        else:
            # first eligible
            return (sheet_row_idx, vals)

    if not candidates:
        return None

    # Highest score wins; if tie, earliest row (older) is fine (stable). You can flip if you want.
    candidates.sort(key=lambda t: t[0], reverse=True)
    return (candidates[0][1], candidates[0][2])


# ============================================================
# Main
# ============================================================

def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    run_slot = env_str("RUN_SLOT", "AM").strip().upper()

    if run_slot not in ("AM", "PM"):
        run_slot = "AM"

    free_delay_hours = env_int("FREE_DELAY_HOURS", 24)

    bot_vip = env_str("TELEGRAM_BOT_TOKEN_VIP")
    chan_vip = env_str("TELEGRAM_CHANNEL_VIP")
    bot_free = env_str("TELEGRAM_BOT_TOKEN")
    chan_free = env_str("TELEGRAM_CHANNEL")

    stripe_monthly = env_str("STRIPE_MONTHLY_LINK")
    stripe_yearly = env_str("STRIPE_YEARLY_LINK")

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")

    if run_slot == "AM":
        if not bot_vip or not chan_vip:
            raise RuntimeError("Missing TELEGRAM_BOT_TOKEN_VIP / TELEGRAM_CHANNEL_VIP for AM VIP publish")
    else:
        if not bot_free or not chan_free:
            raise RuntimeError("Missing TELEGRAM_BOT_TOKEN / TELEGRAM_CHANNEL for PM FREE publish")

    gc = get_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(tab)

    required = [
        "status",
        "deal_id",
        "deal_theme",
        "price_gbp",
        "origin_iata",
        "destination_iata",
        "origin_city",
        "destination_city",
        "outbound_date",
        "return_date",
        "affiliate_url",
        "booking_link_vip",
        "deal_score",
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

    picked = pick_best_candidate(
        headers=[c for c, _ in sorted(h.items(), key=lambda kv: kv[1])],
        rows=rows[1:],
        hmap=h,
        consume_status=consume_status,
        ts_col=ts_col,
        run_slot=run_slot,
        free_delay_hours=free_delay_hours,
    )

    if not picked:
        if run_slot == "PM":
            log(f"Done. Telegram posted 0. (No rows eligible for FREE: status={consume_status} and VIP>= {free_delay_hours}h ago)")
        else:
            log(f"Done. Telegram posted 0. (No rows with status={consume_status})")
        return 0

    target_row_idx, target_vals = picked
    headers = ws.row_values(1)
    row = row_to_dict(headers, target_vals)

    # Build + send message
    now = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    if run_slot == "AM":
        msg = build_vip_message(row)
        tg_send(bot_vip, chan_vip, msg, preview=False)
    else:
        msg = build_free_message(row, stripe_monthly, stripe_yearly)
        tg_send(bot_free, chan_free, msg, preview=False)

    # Write back timestamp + status promotion
    ws.update_cell(target_row_idx, h[ts_col] + 1, now)
    ws.update_cell(target_row_idx, h["status"] + 1, promote_status)

    log(f"✅ Telegram posted 1. Row {target_row_idx} -> {promote_status} ({run_slot})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
