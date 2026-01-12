# workers/telegram_publisher.py
#!/usr/bin/env python3
"""
TravelTxter V4.5x — telegram_publisher.py (LOCKED)

ROLE:
- VIP mode: consumes POSTED_INSTAGRAM → posts to VIP channel → status POSTED_TELEGRAM_VIP
- FREE mode: consumes POSTED_TELEGRAM_VIP that are >= 24h old → posts to FREE channel → status POSTED_ALL
- Fills phrase_bank from PHRASE_BANK if missing
- Includes booking link if any of booking_link_vip/deeplink/affiliate_url exists

POLISH (LOCKED, NON-BREAKING):
- Enforces Theme-of-the-Day gate (must match feeder rotation)
- Stripe link env var compatibility: supports BOTH naming schemes:
  - STRIPE_LINK_MONTHLY / STRIPE_LINK_YEARLY (historical)
  - STRIPE_MONTHLY_LINK / STRIPE_YEARLY_LINK (current workflow vars)
"""

from __future__ import annotations

import os
import json
import datetime as dt
import hashlib
from typing import Any, Dict, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials


# =========================
# Theme-of-day (must match feeder)
# =========================

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
    day_of_year = int(today.strftime("%j"))
    return MASTER_THEMES[day_of_year % len(MASTER_THEMES)]


def norm_theme(s: str) -> str:
    return (s or "").strip().lower().replace(" ", "_")


# -----------------------------
# Logging
# -----------------------------

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


# -----------------------------
# Env helpers
# -----------------------------

def env_str(k: str, default: str = "") -> str:
    return (os.environ.get(k, default) or "").strip()


def env_int(k: str, default: int) -> int:
    v = env_str(k, "")
    return int(v) if v else int(default)


def env_any(keys: List[str], default: str = "") -> str:
    for k in keys:
        v = env_str(k, "")
        if v:
            return v
    return default


def utcnow() -> dt.datetime:
    return dt.datetime.utcnow().replace(microsecond=0)


def iso_now() -> str:
    return utcnow().isoformat() + "Z"


# -----------------------------
# Sheets auth
# -----------------------------

def _parse_sa_json(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return json.loads(raw.replace("\\n", "\n"))


def gs_client() -> gspread.Client:
    raw = env_any(["GCP_SA_JSON_ONE_LINE", "GCP_SA_JSON"])
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
    info = _parse_sa_json(raw)
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds)


# -----------------------------
# Utilities
# -----------------------------

def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        s = str(x).strip().replace("£", "").replace(",", "")
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def parse_iso_z(s: str) -> Optional[dt.datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1]
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None


def pick_first_present(row: Dict[str, str], keys: List[str]) -> str:
    for k in keys:
        v = (row.get(k, "") or "").strip()
        if v:
            return v
    return ""


# -----------------------------
# Telegram send
# -----------------------------

def tg_send(bot_token: str, chat_id: str, message_html: str) -> None:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    r = requests.post(
        url,
        data={
            "chat_id": chat_id,
            "text": message_html,
            "parse_mode": "HTML",
            "disable_web_page_preview": "true",
        },
        timeout=60,
    )
    j = r.json()
    if not j.get("ok"):
        raise RuntimeError(f"Telegram send failed: {j}")


# -----------------------------
# Phrase bank
# -----------------------------

def _truthy(x: Any) -> bool:
    return str(x).strip().lower() in ("true", "yes", "1", "y", "approved")


def load_phrase_bank(sh: gspread.Spreadsheet) -> List[Dict[str, str]]:
    try:
        ws = sh.worksheet("PHRASE_BANK")
    except Exception:
        return []
    vals = ws.get_all_values()
    if len(vals) < 2:
        return []
    headers = [h.strip() for h in vals[0]]
    idx = {h: i for i, h in enumerate(headers)}
    out: List[Dict[str, str]] = []
    for r in vals[1:]:
        d = {h: (r[idx[h]] if idx[h] < len(r) else "").strip() for h in headers}
        if any(d.values()):
            out.append(d)
    return out


def _pick_from_pool(pool: List[Dict[str, str]], deal_id: str) -> str:
    if not pool:
        return ""
    h = hashlib.md5((deal_id or "x").encode()).hexdigest()
    return (pool[int(h[:8], 16) % len(pool)].get("phrase", "") or "").strip()


def pick_phrase(bank: List[Dict[str, str]], theme: str, deal_id: str) -> str:
    theme_u = (theme or "").strip().upper()
    themed = [
        r for r in bank
        if (r.get("phrase") or "").strip()
        and _truthy(r.get("approved", ""))
        and (r.get("theme") or "").strip().upper() == theme_u
    ]
    chosen = _pick_from_pool(themed, deal_id)
    if chosen:
        return chosen

    any_ok = [
        r for r in bank
        if (r.get("phrase") or "").strip()
        and _truthy(r.get("approved", ""))
    ]
    return _pick_from_pool(any_ok, deal_id)


# -----------------------------
# Ranking
# -----------------------------

def rank_key(rownum: int, row: Dict[str, str]) -> Tuple[float, dt.datetime, dt.datetime, int]:
    ds = safe_float(row.get("deal_score", "")) or 0.0
    scored = parse_iso_z(row.get("scored_timestamp", "")) or dt.datetime(1970, 1, 1)
    created = (
        parse_iso_z(row.get("created_at", "")) or
        parse_iso_z(row.get("timestamp", "")) or
        dt.datetime(1970, 1, 1)
    )
    return (ds, scored, created, rownum)


def pick_best_eligible(
    headers: List[str],
    rows: List[List[str]],
    h: Dict[str, int],
    mode: str,
    consume_status: str,
    theme_today: str,
    free_delay_hours: int = 24,
) -> Optional[Tuple[int, Dict[str, str]]]:
    now = utcnow()
    eligible: List[Tuple[int, Dict[str, str]]] = []

    for rownum, vals in enumerate(rows, start=2):
        status = (vals[h["status"]] if h["status"] < len(vals) else "").strip()
        if status != consume_status:
            continue

        row = {headers[i]: (vals[i] if i < len(vals) else "") for i in range(len(headers))}

        # Theme-of-day gate (brand integrity)
        row_theme = norm_theme(row.get("deal_theme") or row.get("theme") or "")
        if row_theme and row_theme != theme_today:
            continue

        if mode == "FREE":
            vip_ts = parse_iso_z(row.get("posted_telegram_vip_at", ""))
            if not vip_ts:
                continue
            age_hrs = (now - vip_ts).total_seconds() / 3600.0
            if age_hrs < float(free_delay_hours):
                continue

        eligible.append((rownum, row))

    if not eligible:
        return None

    eligible.sort(key=lambda it: rank_key(it[0], it[1]), reverse=True)
    return eligible[0]


# -----------------------------
# Message builder
# -----------------------------

def html_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def get_upgrade_link() -> str:
    # Supports both naming schemes + a single STRIPE_LINK fallback
    monthly = env_any(["STRIPE_LINK_MONTHLY", "STRIPE_MONTHLY_LINK"], "")
    yearly = env_any(["STRIPE_LINK_YEARLY", "STRIPE_YEARLY_LINK"], "")
    single = env_str("STRIPE_LINK", "")
    return monthly or yearly or single


def build_message(row: Dict[str, str], mode: str) -> str:
    origin = html_escape(pick_first_present(row, ["origin_city", "origin_iata"]))
    dest_city = html_escape(pick_first_present(row, ["destination_city", "destination_iata"]))
    dest_country = html_escape(row.get("destination_country", ""))
    out_date = html_escape(row.get("outbound_date", ""))
    in_date = html_escape(row.get("return_date", ""))
    price = html_escape(row.get("price_gbp", ""))

    phrase = html_escape(row.get("phrase_bank", ""))
    if phrase:
        phrase = f"{phrase}\n\n"

    booking = (row.get("booking_link_vip") or row.get("deeplink") or row.get("affiliate_url") or "").strip()

    if mode == "VIP":
        msg = (
            f"{phrase}"
            f"🔥 £{price} to {dest_city}, {dest_country}\n\n"
            f"📍 From {origin}\n"
            f"📅 {out_date} → {in_date}\n\n"
        )
        if booking:
            msg += f"🔗 BOOKING LINK:\n{html_escape(booking)}\n"
        return msg

    upgrade = get_upgrade_link()
    msg = (
        f"{phrase}"
        f"🔥 £{price} to {dest_city}, {dest_country}\n\n"
        f"📍 From {origin}\n"
        f"📅 {out_date} → {in_date}\n\n"
        f"💎 Want instant access?\n"
        f"Join TravelTxter VIP:\n"
    )
    if upgrade:
        msg += f"👉 Upgrade now: {html_escape(upgrade)}\n"
    return msg


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    run_slot = env_str("RUN_SLOT", "VIP").upper()
    mode = "FREE" if run_slot in ("FREE", "PM") else "VIP"

    spreadsheet_id = env_any(["SPREADSHEET_ID", "SHEET_ID"])
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    free_delay_hours = env_int("FREE_DELAY_HOURS", 24)

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")

    if mode == "VIP":
        bot_token = env_any(["TELEGRAM_BOT_TOKEN_VIP"])
        chat_id = env_any(["TELEGRAM_CHANNEL_VIP"])
        if not bot_token or not chat_id:
            raise RuntimeError("Missing TELEGRAM_BOT_TOKEN_VIP or TELEGRAM_CHANNEL_VIP")
        consume_status = "POSTED_INSTAGRAM"
        promote_status = "POSTED_TELEGRAM_VIP"
        ts_col = "posted_telegram_vip_at"
    else:
        bot_free = env_any(["TELEGRAM_BOT_TOKEN"])
        chan_free = env_any(["TELEGRAM_CHANNEL"])
        if not bot_free or not chan_free:
            raise RuntimeError("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHANNEL")
        bot_token, chat_id = bot_free, chan_free
        consume_status = "POSTED_TELEGRAM_VIP"
        promote_status = "POSTED_ALL"
        ts_col = "posted_telegram_free_at"

    gc = gs_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(tab)

    values = ws.get_all_values()
    if len(values) < 2:
        log("No rows.")
        return 0

    headers = [h.strip() for h in values[0]]
    h = {k: i for i, k in enumerate(headers)}

    required = [
        "status", "deal_id", "deal_theme", "theme", "price_gbp",
        "origin_city", "destination_city", "origin_iata", "destination_iata",
        "destination_country", "outbound_date", "return_date",
        "booking_link_vip", "deeplink", "affiliate_url",
        "phrase_bank", "deal_score", "scored_timestamp",
        "posted_telegram_vip_at", "posted_telegram_free_at",
        ts_col
    ]
    missing = [c for c in required if c not in h]
    if missing:
        raise RuntimeError(f"RAW_DEALS missing required columns: {missing}")

    bank = load_phrase_bank(sh)

    theme_today = norm_theme(theme_of_day_utc())
    log(f"🎯 Theme of the day (UTC): {theme_today} | MODE={mode} | RUN_SLOT={run_slot}")

    best = pick_best_eligible(
        headers=headers,
        rows=values[1:],
        h=h,
        mode=mode,
        consume_status=consume_status,
        theme_today=theme_today,
        free_delay_hours=free_delay_hours,
    )
    if not best:
        log("Done. Telegram posted 0 (no eligible rows match status+theme gate).")
        return 0

    rownum, row = best

    # Ensure phrase_bank present
    if not (row.get("phrase_bank") or "").strip():
        chosen = pick_phrase(bank, (row.get("deal_theme") or row.get("theme") or ""), row.get("deal_id", ""))
        if chosen:
            ws.update([[chosen]], gspread.utils.rowcol_to_a1(rownum, h["phrase_bank"] + 1))
            row["phrase_bank"] = chosen

    msg = build_message(row, mode)
    tg_send(bot_token, chat_id, msg)

    ws.batch_update(
        [
            {"range": gspread.utils.rowcol_to_a1(rownum, h[ts_col] + 1), "values": [[iso_now()]]},
            {"range": gspread.utils.rowcol_to_a1(rownum, h["status"] + 1), "values": [[promote_status]]},
        ],
        value_input_option="USER_ENTERED",
    )

    log(f"✅ Telegram posted row {rownum} -> {promote_status} (theme={theme_today})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
