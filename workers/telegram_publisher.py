# workers/telegram_publisher.py
#!/usr/bin/env python3
"""
TravelTxter — Telegram Publisher (PROD SAFE, LOCKED) + VIP BUNDLE ADDON

Modes (unchanged contract):
- VIP: consumes POSTED_INSTAGRAM -> posts VIP -> status POSTED_TELEGRAM_VIP
- FREE: consumes POSTED_TELEGRAM_VIP older than FREE_DELAY_HOURS -> posts FREE -> status POSTED_ALL

ADDON (VIP bundle):
- VIP can also include up to (VIP_BUNDLE_SIZE-1) runner-ups with status READY_TO_VIP_BUNDLE
- Runner-ups are marked POSTED_TELEGRAM_VIP_BUNDLE (so FREE will NOT repost them)

Theme gate (unchanged):
- Theme-of-the-day resolved from THEME_OF_DAY env override else deterministic UTC rotation.
- Row theme must match (deal_theme OR theme). If mismatch -> skip.

Notes:
- Keeps your current status spine and timestamp columns.
- No changes to rendering/IG pipeline. Telegram remains status-driven.
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
# Theme of day (must match pipeline_worker rotation logic)
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
# Logging + env
# ============================================================

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


def env_str(k: str, default: str = "") -> str:
    return (os.environ.get(k, default) or "").strip()


def env_any(keys: List[str], default: str = "") -> str:
    for k in keys:
        v = env_str(k, "")
        if v:
            return v
    return default


def env_int(k: str, default: int) -> int:
    v = env_str(k, "")
    return int(v) if v else int(default)


def iso_now() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


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


# ============================================================
# Sheets auth
# ============================================================

def parse_sa_json(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return json.loads(raw.replace("\\n", "\n"))


def gs_client() -> gspread.Client:
    raw = env_any(["GCP_SA_JSON_ONE_LINE", "GCP_SA_JSON"])
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
    info = parse_sa_json(raw)
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds)


def a1(row: int, col0: int) -> str:
    return gspread.utils.rowcol_to_a1(row, col0 + 1)


def safe_get(r: List[str], idx: int) -> str:
    if idx < 0 or idx >= len(r):
        return ""
    return (r[idx] or "").strip()


# ============================================================
# Telegram
# ============================================================

def tg_send(bot_token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    r = requests.post(
        url,
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


def html_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def get_upgrade_link() -> str:
    monthly = env_any(["STRIPE_LINK_MONTHLY", "STRIPE_MONTHLY_LINK"], "")
    yearly = env_any(["STRIPE_LINK_YEARLY", "STRIPE_YEARLY_LINK"], "")
    single = env_str("STRIPE_LINK", "")
    return monthly or yearly or single


def build_message(row: Dict[str, str], mode: str) -> str:
    origin = html_escape((row.get("origin_city") or row.get("origin_iata") or "").strip())
    dest = html_escape((row.get("destination_city") or row.get("destination_iata") or "").strip())
    country = html_escape((row.get("destination_country") or "").strip())
    out_date = html_escape((row.get("outbound_date") or "").strip())
    in_date = html_escape((row.get("return_date") or "").strip())
    price = html_escape((row.get("price_gbp") or "").strip())
    phrase = html_escape((row.get("phrase_bank") or "").strip())

    booking = (row.get("booking_link_vip") or row.get("deeplink") or row.get("affiliate_url") or "").strip()
    booking = html_escape(booking)

    lines: List[str] = []
    if phrase:
        lines.append(phrase)
        lines.append("")

    lines.append(f"🔥 £{price} to {dest}, {country}".strip())
    if origin:
        lines.append(f"📍 From {origin}")
    if out_date and in_date:
        lines.append(f"📅 {out_date} → {in_date}")
    lines.append("")

    if mode == "VIP":
        if booking:
            lines.append("🔗 BOOKING LINK:")
            lines.append(booking)
        return "\n".join(lines).strip()

    upgrade = get_upgrade_link()
    lines.append("💎 Want instant access?")
    lines.append("Join TravelTxter VIP:")
    if upgrade:
        lines.append(f"👉 Upgrade now: {upgrade}")
    return "\n".join(lines).strip()


def build_vip_bundle_message(rows: List[Dict[str, str]]) -> str:
    # First row is the Instagram winner, subsequent rows are runner-ups.
    blocks: List[str] = []
    for i, row in enumerate(rows):
        if i == 0:
            blocks.append(build_message(row, "VIP"))
        else:
            blocks.append(build_message(row, "VIP"))
    return "\n\n<hr/>\n\n".join(blocks).replace("<hr/>", "────────")


# ============================================================
# Main
# ============================================================

def main() -> int:
    spreadsheet_id = env_any(["SPREADSHEET_ID", "SHEET_ID"])
    raw_tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    run_slot = env_str("RUN_SLOT", "VIP").upper()
    free_delay_hours = env_int("FREE_DELAY_HOURS", 24)

    # VIP bundle size (total deals in VIP message, incl winner)
    vip_bundle_size = env_int("VIP_BUNDLE_SIZE", 1)
    vip_bundle_size = max(1, min(5, vip_bundle_size))  # safety cap (1–5)

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID/SHEET_ID")

    mode = "FREE" if run_slot in ("PM", "FREE") else "VIP"
    theme_today = resolve_theme_of_day()
    log(f"🎯 Theme of the day (resolved): {theme_today} | MODE={mode} | RUN_SLOT={run_slot} | VIP_BUNDLE_SIZE={vip_bundle_size}")

    if mode == "VIP":
        bot_token = env_str("TELEGRAM_BOT_TOKEN_VIP", "")
        chat_id = env_str("TELEGRAM_CHANNEL_VIP", "")
        consume_status = "POSTED_INSTAGRAM"
        promote_status = "POSTED_TELEGRAM_VIP"
        ts_col = "posted_telegram_vip_at"
        runner_status = "READY_TO_VIP_BUNDLE"
        runner_promote_status = "POSTED_TELEGRAM_VIP_BUNDLE"
    else:
        bot_token = env_str("TELEGRAM_BOT_TOKEN", "")
        chat_id = env_str("TELEGRAM_CHANNEL", "")
        consume_status = "POSTED_TELEGRAM_VIP"
        promote_status = "POSTED_ALL"
        ts_col = "posted_telegram_free_at"

    if not bot_token or not chat_id:
        raise RuntimeError("Missing Telegram token/channel for this mode")

    gc = gs_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(raw_tab)

    values = ws.get_all_values()
    if len(values) < 2:
        log("No rows.")
        return 0

    headers = values[0]
    h = {k.strip(): i for i, k in enumerate(headers) if isinstance(k, str) and k.strip()}

    required = [
        "status", "deal_theme", "theme", "price_gbp",
        "origin_city", "origin_iata",
        "destination_city", "destination_iata",
        "destination_country", "outbound_date", "return_date",
        "phrase_bank", "booking_link_vip", "deeplink", "affiliate_url",
        "posted_telegram_vip_at", "posted_telegram_free_at",
        ts_col,
    ]
    missing = [c for c in required if c not in h]
    if missing:
        raise RuntimeError(f"RAW_DEALS missing required columns: {missing}")

    now = dt.datetime.utcnow()

    # Helper to convert a sheet row into dict
    def row_dict(r: List[str]) -> Dict[str, str]:
        return {headers[i]: (r[i] if i < len(r) else "") for i in range(len(headers)) if isinstance(headers[i], str)}

    # ----------------------------
    # VIP MODE: winner + runner-ups
    # ----------------------------
    if mode == "VIP":
        winner_rownum: Optional[int] = None
        winner_row: Optional[Dict[str, str]] = None

        # 1) Find first eligible winner (POSTED_INSTAGRAM + theme match)
        for rownum, r in enumerate(values[1:], start=2):
            if safe_get(r, h["status"]) != consume_status:
                continue

            row = row_dict(r)
            row_theme = norm_theme((row.get("deal_theme") or "").strip()) or norm_theme((row.get("theme") or "").strip())
            if not row_theme or row_theme != theme_today:
                continue

            winner_rownum = rownum
            winner_row = row
            break

        if not winner_rownum or not winner_row:
            log("Done. VIP posted 0 (no eligible rows match status+theme gate).")
            return 0

        # 2) Collect runner-ups (READY_TO_VIP_BUNDLE + theme match)
        runner_rows: List[Dict[str, str]] = []
        runner_rownums: List[int] = []
        if vip_bundle_size > 1:
            for rownum, r in enumerate(values[1:], start=2):
                if len(runner_rows) >= (vip_bundle_size - 1):
                    break
                if safe_get(r, h["status"]) != runner_status:
                    continue

                row = row_dict(r)
                row_theme = norm_theme((row.get("deal_theme") or "").strip()) or norm_theme((row.get("theme") or "").strip())
                if not row_theme or row_theme != theme_today:
                    continue

                runner_rows.append(row)
                runner_rownums.append(rownum)

        bundle_rows = [winner_row] + runner_rows
        msg = build_vip_bundle_message(bundle_rows)
        tg_send(bot_token, chat_id, msg)

        # 3) Update statuses + timestamps (batch_update)
        updates = []

        # Winner: POSTED_TELEGRAM_VIP
        updates.append({"range": a1(winner_rownum, h[ts_col]), "values": [[iso_now()]]})
        updates.append({"range": a1(winner_rownum, h["status"]), "values": [[promote_status]]})

        # Runner-ups: POSTED_TELEGRAM_VIP_BUNDLE (so FREE won't repost)
        for rn in runner_rownums:
            updates.append({"range": a1(rn, h[ts_col]), "values": [[iso_now()]]})
            updates.append({"range": a1(rn, h["status"]), "values": [[runner_promote_status]]})

        ws.batch_update(updates, value_input_option="USER_ENTERED")

        log(f"✅ VIP Telegram posted winner row {winner_rownum} -> {promote_status}")
        if runner_rownums:
            log(f"✅ VIP Telegram included runner-ups: {len(runner_rownums)} rows -> {runner_promote_status}")
        return 0

    # ----------------------------
    # FREE MODE: unchanged
    # ----------------------------

    # Pick first eligible row (simple and safe)
    for rownum, r in enumerate(values[1:], start=2):
        if safe_get(r, h["status"]) != consume_status:
            continue

        row = row_dict(r)

        row_theme = norm_theme((row.get("deal_theme") or "").strip()) or norm_theme((row.get("theme") or "").strip())
        if not row_theme or row_theme != theme_today:
            continue

        vip_ts = parse_iso_z((row.get("posted_telegram_vip_at") or "").strip())
        if not vip_ts:
            continue
        age_hrs = (now - vip_ts).total_seconds() / 3600.0
        if age_hrs < float(free_delay_hours):
            continue

        msg = build_message(row, "FREE")
        tg_send(bot_token, chat_id, msg)

        ws.batch_update(
            [
                {"range": a1(rownum, h[ts_col]), "values": [[iso_now()]]},
                {"range": a1(rownum, h["status"]), "values": [[promote_status]]},
            ],
            value_input_option="USER_ENTERED",
        )

        log(f"✅ Telegram posted row {rownum} -> {promote_status}")
        return 0

    log("Done. Telegram posted 0 (no eligible rows match status+theme gate).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
