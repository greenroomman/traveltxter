#!/usr/bin/env python3
"""
TravelTxter V4.5x — telegram_publisher.py (LOCKED)

AM run (RUN_SLOT=AM):
- Finds rows with status == POSTED_INSTAGRAM and posted_telegram_vip_at empty
- Posts to VIP channel (no upsell block)
- Writes posted_telegram_vip_at
- status -> POSTED_TELEGRAM_VIP

PM run (RUN_SLOT=PM):
- Finds rows with status == POSTED_TELEGRAM_VIP and posted_telegram_free_at empty
- Enforces VIP_DELAY_HOURS since posted_telegram_vip_at
- Posts to FREE channel (includes upsell block)
- Writes posted_telegram_free_at
- status -> POSTED_ALL

Hard rules:
- Sheets are the state machine
- No AI creativity
- Only flag emojis allowed
- Deterministic template

Env required:
- SPREADSHEET_ID
- GCP_SA_JSON_ONE_LINE (or GCP_SA_JSON)
- RAW_DEALS_TAB (default RAW_DEALS)
- RUN_SLOT = AM or PM
- TELEGRAM_BOT_TOKEN_VIP + TELEGRAM_CHANNEL_VIP
- TELEGRAM_BOT_TOKEN + TELEGRAM_CHANNEL
Optional:
- VIP_DELAY_HOURS (default 24)
- STRIPE_MONTHLY_LINK / STRIPE_YEARLY_LINK (used in FREE upsell)
"""

from __future__ import annotations

import os
import json
import time
import datetime as dt
from typing import Dict, Any, List, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError


# ============================================================
# Logging
# ============================================================

def ts() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def log(msg: str) -> None:
    print(f"{ts()} | {msg}", flush=True)


# ============================================================
# Env helpers
# ============================================================

def env_str(k: str, default: str = "") -> str:
    return os.environ.get(k, default).strip()

def env_int(k: str, default: int) -> int:
    try:
        return int(env_str(k, str(default)))
    except Exception:
        return default


# ============================================================
# Robust JSON extraction (for GCP_SA_JSON_ONE_LINE)
# ============================================================

def _extract_json_object(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()

    # Fast path
    try:
        return json.loads(raw)
    except Exception:
        pass

    # Replace escaped newlines
    try:
        return json.loads(raw.replace("\\n", "\n"))
    except Exception:
        pass

    # Extract first {...}
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise RuntimeError("Invalid GCP_SA_JSON_ONE_LINE: no JSON object found")

    candidate = raw[start:end + 1]

    try:
        return json.loads(candidate)
    except Exception:
        pass

    try:
        return json.loads(candidate.replace("\\n", "\n"))
    except Exception as e:
        raise RuntimeError("Invalid GCP_SA_JSON_ONE_LINE: JSON parse failed") from e


def get_client() -> gspread.Client:
    sa = env_str("GCP_SA_JSON_ONE_LINE") or env_str("GCP_SA_JSON")
    if not sa:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
    info = _extract_json_object(sa)
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds)


def open_sheet_with_backoff(gc: gspread.Client, spreadsheet_id: str, attempts: int = 8) -> gspread.Spreadsheet:
    delay = 4.0
    for i in range(1, attempts + 1):
        try:
            return gc.open_by_key(spreadsheet_id)
        except APIError as e:
            msg = str(e)
            if "429" in msg or "Quota exceeded" in msg:
                log(f"⏳ Sheets quota (429). Retry {i}/{attempts} in {int(delay)}s...")
                time.sleep(delay)
                delay = min(delay * 1.6, 45.0)
                continue
            raise
    raise RuntimeError("Sheets quota still exceeded after retries (429).")


# ============================================================
# A1 helpers
# ============================================================

def col_letter(n1: int) -> str:
    s = ""
    n = n1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def a1(rownum: int, col0: int) -> str:
    return f"{col_letter(col0 + 1)}{rownum}"


# ============================================================
# Sheet helpers
# ============================================================

def ensure_columns(ws: gspread.Worksheet, headers: List[str], required: List[str]) -> List[str]:
    missing = [c for c in required if c not in headers]
    if not missing:
        return headers
    ws.update([headers + missing], "A1")
    log(f"🛠️  Added missing columns: {missing}")
    return headers + missing

def safe_get(row: List[str], idx: int) -> str:
    return row[idx].strip() if 0 <= idx < len(row) else ""

def parse_iso(s: str) -> Optional[dt.datetime]:
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(s.replace("Z", ""))
    except Exception:
        return None


# ============================================================
# Locked caption template helpers
# ============================================================

FLAG_MAP = {
    "ICELAND": "🇮🇸",
    "SPAIN": "🇪🇸",
    "PORTUGAL": "🇵🇹",
    "FRANCE": "🇫🇷",
    "ITALY": "🇮🇹",
    "GREECE": "🇬🇷",
    "MOROCCO": "🇲🇦",
    "TURKEY": "🇹🇷",
    "THAILAND": "🇹🇭",
    "JAPAN": "🇯🇵",
    "USA": "🇺🇸",
    "UNITED STATES": "🇺🇸",
}

def country_flag(country: str) -> str:
    c = (country or "").strip().upper()
    return FLAG_MAP.get(c, "")

def build_core_lines(price_gbp: str, destination_country: str, destination_city: str, origin_city: str,
                     outbound_date: str, return_date: str) -> List[str]:
    flag = country_flag(destination_country)
    dest_upper = (destination_city or "").strip().upper()
    header = f"£{price_gbp} to {destination_country}{(' ' + flag) if flag else ''}".strip()
    return [
        header,
        f"TO: {dest_upper}",
        f"FROM: {origin_city}",
        f"OUT: {outbound_date}",
        f"BACK: {return_date}",
        "",
        "Heads up:",
        "• VIP members saw this 24 hours ago",
        "• Availability is running low",
        "• Best deals go to VIPs first",
        "",
    ]

def build_vip_message(core: List[str], booking_link: str) -> str:
    lines = core + [
        booking_link.strip(),
    ]
    return "\n".join([l for l in lines if l is not None]).strip() + "\n"

def build_free_message(core: List[str], booking_link: str, stripe_monthly: str, stripe_yearly: str) -> str:
    lines = core + [
        "Want instant access?",
        "Join TravelTxter Nomad for £7.99 / month:",
        "",
        "• Deals 24 hours early",
        "• Direct booking links",
        "• Exclusive mistake fares",
        "• Cancel anytime",
        "",
        f"Upgrade now (Monthly): {stripe_monthly}".strip(),
        f"Upgrade now (Yearly): {stripe_yearly}".strip(),
        "",
        booking_link.strip(),
    ]
    # remove empty upgrade lines if links missing
    out = []
    for ln in lines:
        if "Upgrade now" in ln and ln.endswith(":"):
            continue
        out.append(ln)
    return "\n".join(out).strip() + "\n"


# ============================================================
# Telegram API
# ============================================================

def tg_send(bot_token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    r = requests.post(url, data={
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": False,
    }, timeout=45)
    j = r.json()
    if not r.ok or not j.get("ok"):
        raise RuntimeError(f"Telegram send failed: {j}")


# ============================================================
# Main
# ============================================================

def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    run_slot = env_str("RUN_SLOT", "AM").upper()

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")
    if run_slot not in ("AM", "PM"):
        raise RuntimeError("RUN_SLOT must be AM or PM")

    bot_vip = env_str("TELEGRAM_BOT_TOKEN_VIP")
    chan_vip = env_str("TELEGRAM_CHANNEL_VIP")
    bot_free = env_str("TELEGRAM_BOT_TOKEN")
    chan_free = env_str("TELEGRAM_CHANNEL")

    # Require all 4 always (simplifies workflow wiring)
    if not bot_vip or not chan_vip or not bot_free or not chan_free:
        raise RuntimeError("Missing Telegram env: TELEGRAM_BOT_TOKEN_VIP, TELEGRAM_CHANNEL_VIP, TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL")

    vip_delay_hours = env_int("VIP_DELAY_HOURS", 24)
    stripe_monthly = env_str("STRIPE_MONTHLY_LINK")
    stripe_yearly = env_str("STRIPE_YEARLY_LINK")

    gc = get_client()
    sh = open_sheet_with_backoff(gc, spreadsheet_id)
    ws = sh.worksheet(tab)

    values = ws.get_all_values()
    if not values or len(values) < 2:
        log("No rows.")
        return 0

    headers = [h.strip() for h in values[0]]

    required_cols = [
        "status",
        "price_gbp",
        "destination_country",
        "destination_city",
        "origin_city",
        "outbound_date",
        "return_date",
        "booking_link_vip",
        "affiliate_url",
        "posted_instagram_at",
        "posted_telegram_vip_at",
        "posted_telegram_free_at",
    ]
    headers = ensure_columns(ws, headers, required_cols)

    values = ws.get_all_values()
    headers = [h.strip() for h in values[0]]
    rows = values[1:]
    h = {name: i for i, name in enumerate(headers)}

    posted = 0

    for rownum, r in enumerate(rows, start=2):
        status = safe_get(r, h["status"]).upper()

        price_gbp = safe_get(r, h["price_gbp"])
        destination_country = safe_get(r, h["destination_country"])
        destination_city = safe_get(r, h["destination_city"])
        origin_city = safe_get(r, h["origin_city"])
        outbound_date = safe_get(r, h["outbound_date"])
        return_date = safe_get(r, h["return_date"])

        booking_link = safe_get(r, h["booking_link_vip"]) or safe_get(r, h["affiliate_url"])

        posted_ig = safe_get(r, h["posted_instagram_at"])
        posted_vip = safe_get(r, h["posted_telegram_vip_at"])
        posted_free = safe_get(r, h["posted_telegram_free_at"])

        # ---------------- AM: VIP ----------------
        if run_slot == "AM":
            if status != "POSTED_INSTAGRAM":
                continue
            if posted_vip:
                continue
            if not booking_link:
                log(f"⏭️  Skip row {rownum}: no booking_link_vip/affiliate_url")
                continue

            core = build_core_lines(price_gbp, destination_country, destination_city, origin_city, outbound_date, return_date)
            msg = build_vip_message(core, booking_link)

            log(f"📣 Telegram VIP posting row {rownum}")
            tg_send(bot_vip, chan_vip, msg)

            updates = [
                {"range": a1(rownum, h["posted_telegram_vip_at"]), "values": [[ts()]]},
                {"range": a1(rownum, h["status"]), "values": [["POSTED_TELEGRAM_VIP"]]},
            ]
            ws.batch_update(updates, value_input_option="USER_ENTERED")
            posted += 1
            log(f"✅ Telegram VIP posted row {rownum}")
            break  # one per run

        # ---------------- PM: FREE ----------------
        if run_slot == "PM":
            if status != "POSTED_TELEGRAM_VIP":
                continue
            if posted_free:
                continue
            if not booking_link:
                log(f"⏭️  Skip row {rownum}: no booking_link_vip/affiliate_url")
                continue

            vip_time = parse_iso(posted_vip)
            if not vip_time:
                # If missing, enforce by skipping (don’t cheat delay)
                log(f"⏭️  Skip row {rownum}: missing posted_telegram_vip_at (delay enforcement)")
                continue

            if (dt.datetime.utcnow() - vip_time) < dt.timedelta(hours=vip_delay_hours):
                log(f"⏭️  Skip row {rownum}: VIP delay not met")
                continue

            core = build_core_lines(price_gbp, destination_country, destination_city, origin_city, outbound_date, return_date)
            msg = build_free_message(core, booking_link, stripe_monthly, stripe_yearly)

            log(f"📣 Telegram FREE posting row {rownum}")
            tg_send(bot_free, chan_free, msg)

            updates = [
                {"range": a1(rownum, h["posted_telegram_free_at"]), "values": [[ts()]]},
                {"range": a1(rownum, h["status"]), "values": [["POSTED_ALL"]]},
            ]
            ws.batch_update(updates, value_input_option="USER_ENTERED")
            posted += 1
            log(f"✅ Telegram FREE posted row {rownum}")
            break  # one per run

    log(f"Done. Telegram posted {posted}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
