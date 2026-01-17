#!/usr/bin/env python3
"""
workers/telegram_publisher.py
FULL REPLACEMENT — V4.6 (LOCKED)

PURPOSE (LOCKED):
- Publish Telegram VIP first, then Telegram FREE after delay
- ALWAYS select NEWEST eligible deal (fresh-first, never sheet order)
- Enforce THEME-OF-THE-DAY gate
- Enforce LOCKED MEDIA OUTPUT FORMATS (as specified)

SELECTION RULE (CRITICAL, LOCKED):
1) status eligible for this mode
2) theme matches theme-of-the-day
3) NEWEST ingested_at_utc DESC
4) tie-breaker: highest row number

MEDIA OUTPUT (LOCKED):
- TELEGRAM VIP format (price-led, why-is-it-good, booking link)
- TELEGRAM FREE format (price-led, upsell block, hyperlinks)
"""

from __future__ import annotations

import os
import json
import datetime as dt
from typing import Any, Dict, List, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# THEME OF DAY (MUST MATCH PIPELINE WORKER)
# ============================================================

MASTER_THEMES = [
    "winter_sun",
    "summer_sun",
    "beach_break",
    "snow",
    "northern_lights",
    "surf",
    "adventure",
    "city_breaks",
    "culture_history",
    "long_haul",
    "luxury_value",
    "unexpected_value",
]


def theme_of_day_utc() -> str:
    today = dt.datetime.utcnow().date()
    doy = int(today.strftime("%j"))
    return MASTER_THEMES[doy % len(MASTER_THEMES)]


def norm_theme(s: str) -> str:
    return (s or "").strip().lower().replace(" ", "_")


def resolve_theme_of_day() -> str:
    override = norm_theme(os.getenv("THEME_OF_DAY", ""))
    return override if override else norm_theme(theme_of_day_utc())


# ============================================================
# LOGGING / ENV
# ============================================================

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


def env(k: str, default: str = "") -> str:
    return (os.environ.get(k, default) or "").strip()


def env_int(k: str, default: int) -> int:
    v = env(k, "")
    return int(v) if v else default


def iso_now() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def parse_iso(ts: str) -> Optional[dt.datetime]:
    try:
        return dt.datetime.fromisoformat(ts.replace("Z", ""))
    except Exception:
        return None


# ============================================================
# GOOGLE SHEETS
# ============================================================

def parse_sa_json(raw: str) -> Dict[str, Any]:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return json.loads(raw.replace("\\n", "\n"))


def gs_client() -> gspread.Client:
    raw = env("GCP_SA_JSON_ONE_LINE") or env("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON")
    info = parse_sa_json(raw)
    creds = Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


def a1(row: int, col0: int) -> str:
    return gspread.utils.rowcol_to_a1(row, col0 + 1)


# ============================================================
# TELEGRAM
# ============================================================

def tg_send(bot_token: str, chat_id: str, text: str) -> None:
    r = requests.post(
        f"https://api.telegram.org/bot{bot_token}/sendMessage",
        data={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": "true",
        },
        timeout=60,
    )
    j = r.json()
    if not j.get("ok"):
        raise RuntimeError(f"Telegram send failed: {j}")


def esc(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ============================================================
# MESSAGE BUILDERS (LOCKED FORMATS)
# ============================================================

def build_vip_message(row: Dict[str, str]) -> str:
    price = esc(row.get("price_gbp"))
    dest = esc(row.get("destination_city") or row.get("destination_iata"))
    country = esc(row.get("destination_country"))
    origin = esc(row.get("origin_city") or row.get("origin_iata"))
    out_date = esc(row.get("outbound_date"))
    back_date = esc(row.get("return_date"))
    phrase = esc(row.get("phrase_bank"))
    booking = esc(row.get("booking_link_vip") or row.get("affiliate_url") or row.get("deeplink"))

    lines: List[str] = []
    lines.append(f"£{price} to {dest}")
    if country:
        lines[-1] += f" {country}"
    lines.append(f"TO: {dest.upper()}")
    lines.append(f"FROM: {origin}")
    lines.append(f"OUT: {out_date}")
    lines.append(f"BACK: {back_date}")
    lines.append("")
    if phrase:
        lines.append(f"{phrase} why is it good?")
        lines.append("")
    if booking:
        lines.append(f"<a href=\"{booking}\">BOOKING LINK</a>")
    return "\n".join(lines).strip()


def build_free_message(row: Dict[str, str], upgrade_monthly: str, upgrade_yearly: str) -> str:
    price = esc(row.get("price_gbp"))
    dest = esc(row.get("destination_city") or row.get("destination_iata"))
    country = esc(row.get("destination_country"))
    origin = esc(row.get("origin_city") or row.get("origin_iata"))
    out_date = esc(row.get("outbound_date"))
    back_date = esc(row.get("return_date"))
    phrase = esc(row.get("phrase_bank"))

    lines: List[str] = []
    lines.append(f"£{price} to {dest}")
    if country:
        lines[-1] += f" {country}"
    lines.append(f"TO: {dest.upper()}")
    lines.append(f"FROM: {origin}")
    lines.append(f"OUT: {out_date}")
    lines.append(f"BACK: {back_date}")
    lines.append("")
    if phrase:
        lines.append(phrase)
        lines.append("")
    lines.append("Want instant access?")
    lines.append("Join TravelTxter for early access")
    lines.append("")
    lines.append("• VIP members saw this 24 hours ago")
    lines.append("• Direct booking links")
    lines.append("• We find exclusive mistake fares")
    lines.append("• Subscription: £3 p/m or £30 p/a")
    lines.append("")
    if upgrade_monthly:
        lines.append(f"<a href=\"{upgrade_monthly}\">Upgrade monthly</a>")
    if upgrade_yearly:
        lines.append(f"<a href=\"{upgrade_yearly}\">Upgrade yearly</a>")
    return "\n".join(lines).strip()


# ============================================================
# MAIN
# ============================================================

def main() -> int:
    spreadsheet_id = env("SPREADSHEET_ID") or env("SHEET_ID")
    raw_tab = env("RAW_DEALS_TAB", "RAW_DEALS")
    run_slot = env("RUN_SLOT", "VIP").upper()

    free_delay_hours = env_int("FREE_DELAY_HOURS", 24)

    theme_today = resolve_theme_of_day()
    log(f"🎯 Theme of the day: {theme_today} | RUN_SLOT={run_slot}")

    if run_slot in ("PM", "FREE"):
        mode = "FREE"
        bot_token = env("TELEGRAM_BOT_TOKEN")
        chat_id = env("TELEGRAM_CHANNEL")
        consume_status = "POSTED_TELEGRAM_VIP"
        promote_status = "POSTED_ALL"
        ts_col = "posted_telegram_free_at"
    else:
        mode = "VIP"
        bot_token = env("TELEGRAM_BOT_TOKEN_VIP")
        chat_id = env("TELEGRAM_CHANNEL_VIP")
        consume_status = "POSTED_INSTAGRAM"
        promote_status = "POSTED_TELEGRAM_VIP"
        ts_col = "posted_telegram_vip_at"

    if not spreadsheet_id or not bot_token or not chat_id:
        raise RuntimeError("Missing required Telegram or Sheet env vars")

    gc = gs_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(raw_tab)

    values = ws.get_all_values()
    if len(values) < 2:
        log("No rows.")
        return 0

    headers = [h.strip() for h in values[0]]
    h = {k: i for i, k in enumerate(headers)}

    required = [
        "status", "deal_theme", "theme", "ingested_at_utc",
        "price_gbp", "origin_city", "origin_iata",
        "destination_city", "destination_iata", "destination_country",
        "outbound_date", "return_date", "phrase_bank",
        "booking_link_vip", "affiliate_url", "deeplink",
        "posted_telegram_vip_at", "posted_telegram_free_at",
        ts_col,
    ]
    for c in required:
        if c not in h:
            raise RuntimeError(f"Missing column: {c}")

    now = dt.datetime.utcnow()

    # --------------------
    # COLLECT ELIGIBLE ROWS (FRESH-FIRST)
    # --------------------
    eligible = []

    for rownum, r in enumerate(values[1:], start=2):
        if r[h["status"]].strip() != consume_status:
            continue

        row = {headers[i]: (r[i] if i < len(r) else "") for i in range(len(headers))}
        row_theme = norm_theme(row.get("deal_theme")) or norm_theme(row.get("theme"))
        if row_theme != theme_today:
            continue

        if mode == "FREE":
            vip_ts = parse_iso(row.get("posted_telegram_vip_at"))
            if not vip_ts:
                continue
            age_hrs = (now - vip_ts).total_seconds() / 3600.0
            if age_hrs < free_delay_hours:
                continue

        ts = parse_iso(row.get("ingested_at_utc")) or dt.datetime.min
        eligible.append({"rownum": rownum, "ts": ts, "row": row})

    if not eligible:
        log("No eligible Telegram rows.")
        return 0

    eligible.sort(key=lambda x: (x["ts"], x["rownum"]), reverse=True)
    target = eligible[0]

    rownum = target["rownum"]
    row = target["row"]

    # --------------------
    # BUILD + SEND MESSAGE
    # --------------------
    if mode == "VIP":
        msg = build_vip_message(row)
    else:
        msg = build_free_message(
            row,
            env("STRIPE_LINK_MONTHLY"),
            env("STRIPE_LINK_YEARLY"),
        )

    tg_send(bot_token, chat_id, msg)

    ws.batch_update(
        [
            {"range": a1(rownum, h[ts_col]), "values": [[iso_now()]]},
            {"range": a1(rownum, h["status"]), "values": [[promote_status]]},
        ],
        value_input_option="USER_ENTERED",
    )

    log(f"✅ Telegram {mode} posted row {rownum} -> {promote_status}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
