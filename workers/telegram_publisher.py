#!/usr/bin/env python3
"""
TravelTxter V4.5x — telegram_publisher.py

Purpose:
- Deterministic Telegram publishing with status gating.
- VIP-first (AM slot), Free-later (PM slot).

Status rules (LOCKED):
AM run (RUN_SLOT=AM):
  consumes: status == POSTED_INSTAGRAM
  writes:   posted_telegram_vip_at
  promotes: POSTED_INSTAGRAM -> POSTED_TELEGRAM_VIP

PM run (RUN_SLOT=PM):
  consumes: status == POSTED_TELEGRAM_VIP
  writes:   posted_telegram_free_at
  promotes: POSTED_TELEGRAM_VIP -> POSTED_ALL

Templates:
- VIP message: deal block + booking link, NO upsell banner
- FREE message: same deal block + upsell block + Stripe links

Env required:
- SPREADSHEET_ID
- GCP_SA_JSON_ONE_LINE
- RAW_DEALS_TAB (default RAW_DEALS)

Telegram env required:
- TELEGRAM_BOT_TOKEN_VIP
- TELEGRAM_CHANNEL_VIP
- TELEGRAM_BOT_TOKEN
- TELEGRAM_CHANNEL

Monetisation env required (for FREE upsell):
- STRIPE_MONTHLY_LINK
- STRIPE_YEARLY_LINK

Control env:
- RUN_SLOT (AM or PM)  [set by workflow]
- MAX_POSTS_PER_RUN (default 1)
"""

from __future__ import annotations

import os
import json
import datetime as dt
from typing import Dict, Any, List, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# Logging
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
    return os.environ.get(k, default).strip()

def env_int(k: str, default: int) -> int:
    try:
        return int(env_str(k, str(default)))
    except Exception:
        return default

def clean_url(u: str) -> str:
    return (u or "").strip().replace(" ", "")


# ============================================================
# Sheets
# ============================================================

def get_client() -> gspread.Client:
    sa = env_str("GCP_SA_JSON_ONE_LINE") or env_str("GCP_SA_JSON")
    if not sa:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE (recommended) or GCP_SA_JSON")
    try:
        info = json.loads(sa)
    except json.JSONDecodeError:
        info = json.loads(sa.replace("\\n", "\n"))

    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)

def col_letter(n1: int) -> str:
    s = ""
    n = n1
    while n:
        n, rr = divmod(n - 1, 26)
        s = chr(65 + rr) + s
    return s

def a1(rownum: int, col0: int) -> str:
    return f"{col_letter(col0 + 1)}{rownum}"


# ============================================================
# Message builders (no reinvention)
# ============================================================

def money_2dp(x: Any) -> str:
    try:
        return f"£{float(x):.2f}"
    except Exception:
        s = str(x or "").strip()
        if s.startswith("£"):
            return s
        return ""

def build_deal_block(row: Dict[str, str]) -> str:
    country = (row.get("destination_country") or "").strip()
    to_city = (row.get("destination_city") or row.get("destination_iata") or "").strip()
    from_city = (row.get("origin_city") or row.get("origin_iata") or "").strip()
    out_iso = (row.get("outbound_date") or "").strip()
    back_iso = (row.get("return_date") or "").strip()
    price = money_2dp(row.get("price_gbp") or "")

    # Telegram: keep clean, no weird formatting
    lines = [
        f"{price} to {country.title()}" if country else f"{price} to {to_city}",
        f"TO: {(to_city or '').upper()}",
        f"FROM: {from_city}",
        f"OUT: {out_iso}",
        f"BACK: {back_iso}",
        "",
    ]
    return "\n".join(lines)

def build_vip_message(row: Dict[str, str], link: str) -> str:
    # VIP: no upsell
    return (build_deal_block(row) + f"Book: {link}").strip()

def build_free_message(row: Dict[str, str], link: str, monthly: str, yearly: str) -> str:
    lines = [
        build_deal_block(row).strip(),
        f"Book: {link}",
        "",
        "Heads up:",
        "• VIP members saw this 24 hours ago",
        "• Availability is running low",
        "• Best deals go to VIPs first",
        "",
        "Want instant access?",
        "Join TravelTxter Nomad for £7.99 / month:",
        "",
        "• Deals 24 hours early",
        "• Direct booking links",
        "• Exclusive mistake fares",
        "• Cancel anytime",
        "",
    ]
    if monthly:
        lines.append(f"Upgrade now (Monthly): {monthly}")
    if yearly:
        lines.append(f"Upgrade now (Yearly): {yearly}")
    return "\n".join(lines).strip()


# ============================================================
# Telegram API
# ============================================================

def tg_send(bot_token: str, chat_id: str, text: str) -> Dict[str, Any]:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": False,
    }
    r = requests.post(url, json=payload, timeout=45)
    try:
        return r.json()
    except Exception:
        return {"ok": False, "_raw": r.text, "_status": r.status_code}


# ============================================================
# Main
# ============================================================

def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    run_slot = env_str("RUN_SLOT", "AM").upper()
    max_posts = env_int("MAX_POSTS_PER_RUN", 1)

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")

    vip_token = env_str("TELEGRAM_BOT_TOKEN_VIP")
    vip_chat = env_str("TELEGRAM_CHANNEL_VIP")
    free_token = env_str("TELEGRAM_BOT_TOKEN")
    free_chat = env_str("TELEGRAM_CHANNEL")

    if not (vip_token and vip_chat and free_token and free_chat):
        raise RuntimeError("Missing Telegram env: TELEGRAM_BOT_TOKEN_VIP, TELEGRAM_CHANNEL_VIP, TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL")

    monthly = clean_url(env_str("STRIPE_MONTHLY_LINK"))
    yearly = clean_url(env_str("STRIPE_YEARLY_LINK"))

    gc = get_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(tab)

    values = ws.get_all_values()
    if not values or len(values) < 2:
        log("Sheet empty. Nothing to post.")
        return 0

    headers = [h.strip() for h in values[0]]
    rows = values[1:]
    h = {name: i for i, name in enumerate(headers)}

    # Required columns
    need = [
        "status", "price_gbp", "origin_city", "destination_city", "destination_country",
        "outbound_date", "return_date",
        "affiliate_url", "booking_link_vip",
        "posted_telegram_vip_at", "posted_telegram_free_at",
    ]
    for c in need:
        if c not in h:
            raise RuntimeError(f"Missing required column in RAW_DEALS: {c}")

    # Determine mode
    if run_slot == "AM":
        consume_status = "POSTED_INSTAGRAM"
        promote_status = "POSTED_TELEGRAM_VIP"
        ts_col = "posted_telegram_vip_at"
        token = vip_token
        chat = vip_chat
        mode = "VIP"
    else:
        consume_status = "POSTED_TELEGRAM_VIP"
        promote_status = "POSTED_ALL"
        ts_col = "posted_telegram_free_at"
        token = free_token
        chat = free_chat
        mode = "FREE"

    posted = 0

    for rownum, r in enumerate(rows, start=2):
        status = (r[h["status"]] if h["status"] < len(r) else "").strip().upper()
        if status != consume_status:
            continue

        rowdict: Dict[str, str] = {name: (r[idx] if idx < len(r) else "") for name, idx in h.items()}

        link = (rowdict.get("booking_link_vip") or "").strip()
        if not link:
            link = (rowdict.get("affiliate_url") or "").strip()

        if not link:
            # Don’t post a deal without a link; leave status for router to fix
            log(f"⏭️  Skip row {rownum}: no booking_link_vip or affiliate_url")
            continue

        if mode == "VIP":
            text = build_vip_message(rowdict, link=link)
        else:
            text = build_free_message(rowdict, link=link, monthly=monthly, yearly=yearly)

        log(f"✈️  Telegram {mode} post row {rownum}")
        resp = tg_send(token, chat, text)

        if not resp.get("ok"):
            raise RuntimeError(f"Telegram send failed: {resp}")

        # Update sheet: timestamp + status
        updates: List[Dict[str, Any]] = [
            {"range": a1(rownum, h[ts_col]), "values": [[ts()]]},
            {"range": a1(rownum, h["status"]), "values": [[promote_status]]},
        ]
        ws.batch_update(updates)

        posted += 1
        log(f"✅ Telegram {mode} posted row {rownum} -> {promote_status}")

        if posted >= max_posts:
            break

    if posted == 0:
        log(f"No rows in status {consume_status} to post for RUN_SLOT={run_slot}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
