#!/usr/bin/env python3
"""
TravelTxter V4.5x — telegram_publisher.py (LOCKED OUTPUT TEMPLATES)

AM (RUN_SLOT=AM):
  consumes: status == POSTED_INSTAGRAM
  sends:    VIP template
  writes:   posted_telegram_vip_at
  promotes: POSTED_INSTAGRAM -> POSTED_TELEGRAM_VIP

PM (RUN_SLOT=PM):
  consumes: status == POSTED_TELEGRAM_VIP
  sends:    FREE template
  writes:   posted_telegram_free_at
  promotes: POSTED_TELEGRAM_VIP -> POSTED_ALL
"""

from __future__ import annotations

import os
import json
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

def clean_url(u: str) -> str:
    return (u or "").strip().replace(" ", "")


# -----------------------------
# HTML helpers (Telegram)
# -----------------------------

def html_escape(s: str) -> str:
    s = s or ""
    return (
        s.replace("&", "&amp;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
         .replace('"', "&quot;")
    )

def html_link(url: str, label: str) -> str:
    u = clean_url(url)
    if not u:
        return ""
    return f'<a href="{html_escape(u)}">{html_escape(label)}</a>'


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

def tg_send(bot_token: str, chat_id: str, text_html: str) -> None:
    """
    Uses HTML parse mode so hyperlinks work.
    """
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    r = requests.post(
        url,
        json={
            "chat_id": chat_id,
            "text": text_html,
            "parse_mode": "HTML",
            "disable_web_page_preview": False,
        },
        timeout=30,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"Telegram send failed HTTP {r.status_code}: {r.text[:200]}")


# -----------------------------
# Formatting helpers
# -----------------------------

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

def fmt_price_gbp(x: str) -> str:
    """
    Keep £103.35 style when possible.
    """
    s = (x or "").strip().replace(",", "")
    if not s:
        return ""
    s = s.replace("£", "")
    try:
        v = float(s)
        # preserve two decimals when not an integer
        if v.is_integer():
            return f"£{int(v)}"
        return f"£{v:.2f}"
    except Exception:
        return f"£{s}"

def normalize_origin_city(x: str) -> str:
    """
    Telegram wants: 'London' not 'LHR' if it slips through.
    Keep it conservative: only map common UK airport codes.
    """
    s = (x or "").strip()
    u = s.upper()
    UK_AIRPORT_CITY_FALLBACK = {
        "LHR": "London", "LGW": "London", "STN": "London", "LTN": "London",
        "LCY": "London", "SEN": "London",
        "MAN": "Manchester",
        "BRS": "Bristol",
        "BHX": "Birmingham",
        "EDI": "Edinburgh",
        "GLA": "Glasgow",
        "NCL": "Newcastle",
        "LPL": "Liverpool",
        "NQY": "Newquay",
        "SOU": "Southampton",
        "CWL": "Cardiff",
        "EXT": "Exeter",
    }
    return UK_AIRPORT_CITY_FALLBACK.get(u, s)


# -----------------------------
# Phrase bank loader (safe/optional)
# -----------------------------

def _pick_header(headers: List[str], candidates: List[str]) -> Optional[int]:
    h = [x.strip() for x in headers]
    for c in candidates:
        if c in h:
            return h.index(c)
    # try case-insensitive match
    upper = {x.strip().upper(): i for i, x in enumerate(headers)}
    for c in candidates:
        i = upper.get(c.upper())
        if i is not None:
            return i
    return None

def load_phrase_bank(sh: gspread.Spreadsheet) -> List[Dict[str, str]]:
    """
    Reads PHRASE_BANK tab if present.
    Supports flexible headers:
      phrase/text, channel, theme, enabled, priority
    """
    try:
        ws = sh.worksheet("PHRASE_BANK")
    except Exception:
        return []

    values = ws.get_all_values()
    if not values or len(values) < 2:
        return []

    headers = values[0]
    i_phrase = _pick_header(headers, ["phrase", "text", "copy"])
    if i_phrase is None:
        return []

    i_channel = _pick_header(headers, ["channel"])
    i_theme = _pick_header(headers, ["theme"])
    i_enabled = _pick_header(headers, ["enabled", "active"])
    i_priority = _pick_header(headers, ["priority", "rank"])

    out: List[Dict[str, str]] = []
    for r in values[1:]:
        phrase = (r[i_phrase] if i_phrase < len(r) else "").strip()
        if not phrase:
            continue

        channel = (r[i_channel] if (i_channel is not None and i_channel < len(r)) else "").strip()
        theme = (r[i_theme] if (i_theme is not None and i_theme < len(r)) else "").strip()
        enabled = (r[i_enabled] if (i_enabled is not None and i_enabled < len(r)) else "").strip()
        priority = (r[i_priority] if (i_priority is not None and i_priority < len(r)) else "").strip()

        out.append({
            "phrase": phrase,
            "channel": channel,
            "theme": theme,
            "enabled": enabled,
            "priority": priority,
        })

    return out

def pick_phrase(rows: List[Dict[str, str]], channel: str, theme: str) -> str:
    """
    Deterministic:
    - filter enabled
    - filter channel match (channel or all/blank)
    - filter theme match (theme or all/blank)
    - sort by priority (desc), then phrase (asc)
    - return first
    """
    ch = (channel or "").strip().lower()
    th = (theme or "").strip().lower()

    def enabled_ok(v: str) -> bool:
        vv = (v or "").strip().lower()
        return vv in ("", "true", "yes", "1", "on", "enabled")

    pool = []
    for r in rows:
        if not enabled_ok(r.get("enabled", "")):
            continue
        rch = (r.get("channel", "") or "").strip().lower()
        rth = (r.get("theme", "") or "").strip().lower()

        ch_ok = (rch in ("", "all", ch))
        th_ok = (rth in ("", "all", th))
        if ch_ok and th_ok:
            pool.append(r)

    if not pool:
        return ""

    def prio(x: str) -> int:
        try:
            return int(float((x or "").strip()))
        except Exception:
            return 0

    pool.sort(key=lambda r: (-prio(r.get("priority", "")), r.get("phrase", "")))
    return pool[0].get("phrase", "").strip()


# -----------------------------
# Templates (definitive)
# -----------------------------

DIVIDER = "_______________________________________________"

def build_telegram_free(row: Dict[str, str], phrase: str, monthly_link: str, yearly_link: str) -> str:
    price = fmt_price_gbp(row.get("price_gbp", ""))
    country = (row.get("destination_country", "") or "").strip()
    flag = country_flag(country)
    dest_city = (row.get("destination_city", "") or "").strip() or (row.get("destination_iata", "") or "").strip()
    origin_city = normalize_origin_city((row.get("origin_city", "") or "").strip() or (row.get("origin_iata", "") or "").strip())
    out_d = (row.get("outbound_date", "") or "").strip()
    back_d = (row.get("return_date", "") or "").strip()

    headline = f"{price} to {country}{(' ' + flag) if flag else ''}".strip()

    lines: List[str] = [
        html_escape(headline),
        f"TO: {html_escape(dest_city.upper())}",
        f"FROM: {html_escape(origin_city)}",
        f"OUT:  {html_escape(out_d)}",
        f"BACK: {html_escape(back_d)}",
        "",
    ]

    if phrase:
        lines.append(html_escape(phrase.strip()))
        lines.append("")

    lines += [
        "Want instant access?",
        "Join TravelTxter for early access",
        "",
        "* VIP members saw this 24 hours ago",
        "* Deals 24 hours early",
        "* Direct booking links",
        "* Exclusive mistake fares",
        "* £3 p/m or £30 p/a",
        "* Cancel anytime",
        "",
    ]

    upsell_bits = []
    if monthly_link:
        upsell_bits.append(html_link(monthly_link, "Upgrade now (Monthly)"))
    if yearly_link:
        upsell_bits.append(html_link(yearly_link, "Upgrade now (Yearly)"))
    if upsell_bits:
        lines.append(" | ".join(upsell_bits))

    lines += ["", DIVIDER]
    return "\n".join(lines).strip()

def build_telegram_vip(row: Dict[str, str], phrase: str, booking_url: str) -> str:
    price = fmt_price_gbp(row.get("price_gbp", ""))
    country = (row.get("destination_country", "") or "").strip()
    flag = country_flag(country)
    dest_city = (row.get("destination_city", "") or "").strip() or (row.get("destination_iata", "") or "").strip()
    origin_city = normalize_origin_city((row.get("origin_city", "") or "").strip() or (row.get("origin_iata", "") or "").strip())
    out_d = (row.get("outbound_date", "") or "").strip()
    back_d = (row.get("return_date", "") or "").strip()

    headline = f"{price} to {country}{(' ' + flag) if flag else ''}".strip()

    lines: List[str] = [
        html_escape(headline),
        f"TO: {html_escape(dest_city.upper())}",
        f"FROM: {html_escape(origin_city)}",
        f"OUT:  {html_escape(out_d)}",
        f"BACK: {html_escape(back_d)}",
        "",
    ]

    if phrase:
        lines.append(html_escape(phrase.strip()))
        lines.append("")

    if booking_url:
        lines.append(html_link(booking_url, "BOOKING LINK"))
    else:
        # Still show label even if missing so it’s obvious in logs
        lines.append("BOOKING LINK: (missing)")

    lines += ["", DIVIDER]
    return "\n".join(lines).strip()

def pick_booking_link(row: Dict[str, str]) -> str:
    vip = (row.get("booking_link_vip", "") or "").strip()
    aff = (row.get("affiliate_url", "") or "").strip()
    return clean_url(vip or aff)


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    run_slot = env_str("RUN_SLOT", "AM").upper()
    spreadsheet_id = env_str("SPREADSHEET_ID")
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")

    # Tokens/channels
    bot_vip = env_str("TELEGRAM_BOT_TOKEN_VIP")
    chan_vip = env_str("TELEGRAM_CHANNEL_VIP")
    bot_free = env_str("TELEGRAM_BOT_TOKEN")
    chan_free = env_str("TELEGRAM_CHANNEL")

    # Upsell links (used in FREE template)
    stripe_monthly = env_str("STRIPE_MONTHLY_LINK")  # Adventurer £3/mo
    stripe_yearly = env_str("STRIPE_YEARLY_LINK")    # Nomad £30/yr

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
        "destination_country",
        "outbound_date",
        "return_date",
        "affiliate_url",
        "booking_link_vip",
        "deal_theme",
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

    # Find first eligible row (deterministic)
    target_row_idx = None
    target_row_vals = None

    for i in range(2, len(rows) + 1):
        vals = rows[i - 1]
        status = (vals[h["status"]] if h["status"] < len(vals) else "").strip()
        ts_val = (vals[h[ts_col]] if h[ts_col] < len(vals) else "").strip()
        if status == consume_status and not ts_val:
            target_row_idx = i
            target_row_vals = vals
            break

    if not target_row_idx:
        log(f"Done. Telegram posted 0. (No rows with status={consume_status})")
        return 0

    # Build row dict
    row: Dict[str, str] = {}
    for col, idx in h.items():
        row[col] = target_row_vals[idx] if idx < len(target_row_vals) else ""

    # Load phrase bank once per run
    pb_rows = load_phrase_bank(sh)
    theme = (row.get("deal_theme", "") or "").strip()
    phrase_channel = "telegram_vip" if run_slot == "AM" else "telegram_free"
    phrase = pick_phrase(pb_rows, phrase_channel, theme)

    now = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    if run_slot == "AM":
        booking = pick_booking_link(row)
        msg = build_telegram_vip(row, phrase=phrase, booking_url=booking)
        tg_send(bot_vip, chan_vip, msg)
    else:
        msg = build_telegram_free(row, phrase=phrase, monthly_link=stripe_monthly, yearly_link=stripe_yearly)
        tg_send(bot_free, chan_free, msg)

    # Write back
    ws.update_cell(target_row_idx, h[ts_col] + 1, now)
    ws.update_cell(target_row_idx, h["status"] + 1, promote_status)

    log(f"✅ Telegram posted 1. Row {target_row_idx} -> {promote_status}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
