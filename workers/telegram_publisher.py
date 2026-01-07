#!/usr/bin/env python3
"""
TravelTxter V4.5x — telegram_publisher.py (LOCKED)

AM (RUN_SLOT=AM):
  consumes: status == POSTED_INSTAGRAM
  writes:   posted_telegram_vip_at
  promotes: POSTED_INSTAGRAM -> POSTED_TELEGRAM_VIP

PM (RUN_SLOT=PM):
  consumes: status == POSTED_TELEGRAM_VIP
  posts FREE only if posted_telegram_vip_at <= now - 24h
  writes:   posted_telegram_free_at
  promotes: POSTED_TELEGRAM_VIP -> POSTED_ALL

Key fixes:
- Template matches your definitive formats (VIP + FREE)
- No trailing separator line
- TO is UPPERCASE; FROM is Title Case
- Uses HTML hyperlinks (parse_mode=HTML)
- Picks "latest & greatest" (by deal_score then recency), not top row
"""

from __future__ import annotations

import os
import json
import re
import time
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

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

def utcnow() -> dt.datetime:
    return dt.datetime.utcnow()

def iso_now() -> str:
    return utcnow().replace(microsecond=0).isoformat() + "Z"


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
    raw = env_str("GCP_SA_JSON_ONE_LINE") or env_str("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
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

def tg_send(bot_token: str, chat_id: str, html_text: str) -> None:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    r = requests.post(
        url,
        json={
            "chat_id": chat_id,
            "text": html_text,
            "parse_mode": "HTML",
            "disable_web_page_preview": False,
        },
        timeout=30,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"Telegram send failed HTTP {r.status_code}: {r.text[:400]}")

def html_escape(s: str) -> str:
    s = s or ""
    return (s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
             .replace('"', "&quot;"))

def title_case_city(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    # Keep common airport-city words sane
    return " ".join([w[:1].upper() + w[1:].lower() for w in s.split()])

def upper_city(s: str) -> str:
    return (s or "").strip().upper()

def pick_flag(country: str) -> str:
    # If you already have a flag column, use it; otherwise use the country name and a tiny mapping.
    # Safe default: no emoji other than flags (you asked for that rule elsewhere).
    c = (country or "").strip().lower()
    flags = {
        "iceland": "🇮🇸",
        "thailand": "🇹🇭",
        "hungary": "🇭🇺",
        "spain": "🇪🇸",
        "portugal": "🇵🇹",
        "france": "🇫🇷",
        "italy": "🇮🇹",
        "greece": "🇬🇷",
        "morocco": "🇲🇦",
        "turkey": "🇹🇷",
        "canada": "🇨🇦",
        "united states": "🇺🇸",
        "usa": "🇺🇸",
        "mexico": "🇲🇽",
        "japan": "🇯🇵",
    }
    return flags.get(c, "")

def safe_float(s: str) -> Optional[float]:
    try:
        x = (s or "").strip().replace("£", "").replace(",", "")
        return float(x) if x else None
    except Exception:
        return None

def fmt_price_gbp(price_gbp: str) -> str:
    # Your template shows £103.35 etc. Keep 2dp if present; otherwise format.
    p = (price_gbp or "").strip()
    if not p:
        return ""
    if p.startswith("£"):
        return p
    f = safe_float(p)
    if f is None:
        return p
    return f"£{f:.2f}"

def parse_iso(ts: str) -> Optional[dt.datetime]:
    ts = (ts or "").strip()
    if not ts:
        return None
    try:
        return dt.datetime.fromisoformat(ts.replace("Z", ""))
    except Exception:
        return None


def build_vip_message(row: Dict[str, str]) -> str:
    price = fmt_price_gbp(row.get("price_gbp", ""))
    country = row.get("destination_country", "").strip()
    flag = pick_flag(country)
    dest = upper_city(row.get("destination_city") or row.get("destination_iata") or "")
    origin = title_case_city(row.get("origin_city") or row.get("origin_iata") or "")
    out_d = (row.get("outbound_date") or "").strip()
    ret_d = (row.get("return_date") or "").strip()

    phrase = (row.get("phrase") or row.get("why_good") or row.get("ai_notes") or "").strip()

    booking = clean_url(row.get("booking_link_vip") or row.get("affiliate_url") or "")
    booking_html = f'<a href="{html_escape(booking)}">Booking link</a>' if booking else ""

    lines = []
    head = f"{html_escape(price)} to {html_escape(country)} {flag}".strip()
    lines.append(head)
    lines.append(f"TO: {html_escape(dest)}")
    lines.append(f"FROM: {html_escape(origin)}")
    lines.append(f"OUT: {html_escape(out_d)}")
    lines.append(f"BACK: {html_escape(ret_d)}")

    if phrase:
        lines.append("")
        lines.append(html_escape(phrase))

    if booking_html:
        lines.append("")
        lines.append(booking_html)

    return "\n".join([l for l in lines if l is not None])


def build_free_message(row: Dict[str, str], stripe_monthly: str, stripe_yearly: str) -> str:
    price = fmt_price_gbp(row.get("price_gbp", ""))
    country = row.get("destination_country", "").strip()
    flag = pick_flag(country)
    dest = upper_city(row.get("destination_city") or row.get("destination_iata") or "")
    origin = title_case_city(row.get("origin_city") or row.get("origin_iata") or "")
    out_d = (row.get("outbound_date") or "").strip()
    ret_d = (row.get("return_date") or "").strip()

    phrase = (row.get("phrase") or row.get("why_good") or row.get("ai_notes") or "").strip()

    m = clean_url(stripe_monthly)
    y = clean_url(stripe_yearly)
    monthly_link = f'<a href="{html_escape(m)}">Upgrade now (Monthly)</a>' if m else ""
    yearly_link = f'<a href="{html_escape(y)}">Upgrade now (Annual)</a>' if y else ""

    lines = []
    head = f"{html_escape(price)} to {html_escape(country)} {flag}".strip()
    lines.append(head)
    lines.append(f"TO: {html_escape(dest)}")
    lines.append(f"FROM: {html_escape(origin)}")
    lines.append(f"OUT: {html_escape(out_d)}")
    lines.append(f"BACK: {html_escape(ret_d)}")

    if phrase:
        lines.append("")
        lines.append(html_escape(phrase))

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

    if monthly_link or yearly_link:
        lines.append("")
        if monthly_link:
            lines.append(monthly_link)
        if yearly_link:
            lines.append(yearly_link)

    return "\n".join(lines)


def row_to_dict(headers: List[str], r: List[str]) -> Dict[str, str]:
    d: Dict[str, str] = {}
    for i, h in enumerate(headers):
        d[h] = (r[i] if i < len(r) else "") or ""
    return d


def choose_best_candidate(
    candidates: List[Tuple[int, Dict[str, str]]],
    run_slot: str,
) -> Optional[Tuple[int, Dict[str, str]]]:
    """
    Pick "latest & greatest":
    - Primary: deal_score desc
    - Secondary: scored_timestamp desc
    - Fallback: created_at desc
    """
    def score_key(item):
        rownum, row = item
        ds = safe_float(row.get("deal_score", "")) or 0.0
        scored = parse_iso(row.get("scored_timestamp", "")) or dt.datetime(1970, 1, 1)
        created = parse_iso(row.get("created_at", "")) or parse_iso(row.get("timestamp", "")) or dt.datetime(1970, 1, 1)
        return (ds, scored, created, -rownum)

    candidates_sorted = sorted(candidates, key=score_key, reverse=True)
    return candidates_sorted[0] if candidates_sorted else None


def main() -> int:
    run_slot = env_str("RUN_SLOT", "AM").upper()
    spreadsheet_id = env_str("SPREADSHEET_ID")
    raw_tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")

    # Routing based on slot
    if run_slot == "AM":
        bot_token = env_str("TELEGRAM_BOT_TOKEN_VIP")
        chat_id = env_str("TELEGRAM_CHANNEL_VIP")
        consume_status = "POSTED_INSTAGRAM"
        promote_status = "POSTED_TELEGRAM_VIP"
        is_vip = True
    else:
        bot_token = env_str("TELEGRAM_BOT_TOKEN")
        chat_id = env_str("TELEGRAM_CHANNEL")
        consume_status = "POSTED_TELEGRAM_VIP"
        promote_status = "POSTED_ALL"
        is_vip = False

    if not bot_token or not chat_id:
        raise RuntimeError(f"Missing Telegram creds for RUN_SLOT={run_slot}")

    stripe_monthly = env_str("STRIPE_MONTHLY_LINK")
    stripe_yearly = env_str("STRIPE_YEARLY_LINK")

    gc = gs_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(raw_tab)

    required_cols = [
        "status",
        "deal_id",
        "price_gbp",
        "destination_country",
        "destination_city",
        "destination_iata",
        "origin_city",
        "origin_iata",
        "outbound_date",
        "return_date",
        "deal_score",
        "scored_timestamp",
        "created_at",
        "timestamp",
        "affiliate_url",
        "booking_link_vip",
        "posted_instagram_at",
        "posted_telegram_vip_at",
        "posted_telegram_free_at",
        "ai_notes",
        "phrase",
        "why_good",
    ]
    ensure_columns(ws, required_cols)

    values = ws.get_all_values()
    headers = [h.strip() for h in values[0]]
    rows = values[1:]
    h = {name: i for i, name in enumerate(headers)}

    candidates: List[Tuple[int, Dict[str, str]]] = []

    now = utcnow()
    vip_age_hours_required = 24.0

    for rownum, r in enumerate(rows, start=2):
        status = (r[h["status"]] if h["status"] < len(r) else "").strip().upper()
        if status != consume_status:
            continue

        row = row_to_dict(headers, r)

        # PM rule: only release FREE if VIP is at least 24h old
        if run_slot != "AM":
            vip_ts = parse_iso(row.get("posted_telegram_vip_at", ""))
            if not vip_ts:
                continue
            age_hours = (now - vip_ts).total_seconds() / 3600.0
            if age_hours < vip_age_hours_required:
                continue

        candidates.append((rownum, row))

    if not candidates:
        log(f"⏭️  No eligible rows for RUN_SLOT={run_slot} (consume_status={consume_status}).")
        return 0

    chosen = choose_best_candidate(candidates, run_slot)
    if not chosen:
        log("⏭️  No eligible candidate after ranking.")
        return 0

    rownum, row = chosen
    deal_id = (row.get("deal_id") or "").strip()

    if is_vip:
        msg = build_vip_message(row)
    else:
        msg = build_free_message(row, stripe_monthly=stripe_monthly, stripe_yearly=stripe_yearly)

    log(f"📨 Telegram posting {('VIP' if is_vip else 'FREE')} row {rownum} deal_id={deal_id}")
    tg_send(bot_token, chat_id, msg)

    # Promote status + timestamps
    batch = []
    if is_vip:
        if "posted_telegram_vip_at" in h:
            batch.append({"range": a1(rownum, h["posted_telegram_vip_at"]), "values": [[iso_now()]]})
    else:
        if "posted_telegram_free_at" in h:
            batch.append({"range": a1(rownum, h["posted_telegram_free_at"]), "values": [[iso_now()]]})

    batch.append({"range": a1(rownum, h["status"]), "values": [[promote_status]]})

    ws.batch_update(batch, value_input_option="USER_ENTERED")
    log(f"✅ Telegram posted 1. Row {rownum} -> {promote_status}")
    return 0


def col_letter(n1: int) -> str:
    s = ""
    n = n1
    while n:
        n, rr = divmod(n - 1, 26)
        s = chr(65 + rr) + s
    return s

def a1(rownum: int, col0: int) -> str:
    return f"{col_letter(col0 + 1)}{rownum}"


if __name__ == "__main__":
    raise SystemExit(main())
