#!/usr/bin/env python3
"""
TravelTxter — Telegram Publisher (V4, safe + backwards-compatible)

- Reads from Google Sheets RAW_DEALS
- Filters by status (default: raw_status == POSTED_INSTAGRAM)
- Posts to Telegram channel
- Promotes status on success (default: POSTED_TELEGRAM)
- Optional V4 templates (free/vip) controlled by env vars
- Backwards compatible with existing workflow env names (TELEGRAM_*)

This file is designed to be drop-in and NOT break existing working 
pipelines.
"""

import os
import re
import json
import html
import time
import logging
import datetime as dt
from typing import Dict, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials


# =========================
# Logging
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("telegram_publisher")


def utc_now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v)


def env_first(names: List[str], default: str = "") -> str:
    for n in names:
        v = os.getenv(n)
        if v is not None and str(v).strip() != "":
            return str(v)
    return default


def truthy(s: str) -> bool:
    return str(s).strip().lower() in ("1", "true", "yes", "y", "on")


def to_float(s: str, default: float = 0.0) -> float:
    try:
        return float(str(s).strip())
    except Exception:
        return default


# =========================
# Telegram helpers
# =========================

def telegram_send_message(
    bot_token: str,
    chat_id: str,
    text: str,
    disable_preview: bool = True,
) -> Tuple[bool, str]:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": disable_preview,
    }
    try:
        r = requests.post(url, json=payload, timeout=20)
        if r.status_code != 200:
            return False, f"HTTP {r.status_code}: {r.text}"
        data = r.json()
        if not data.get("ok"):
            return False, f"Telegram API error: {data}"
        return True, "ok"
    except Exception as e:
        return False, f"Exception: {e}"


def safe(s: str) -> str:
    # Telegram HTML parse_mode safe escape
    return html.escape(str(s or "").strip())


def clean_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "").strip())


# =========================
# Templates
# =========================

def format_message_legacy(row: Dict[str, str]) -> str:
    # A simple safe legacy message (keeps system functional even if no V4 
# fields exist
    origin = safe(row.get("origin_city", row.get("origin", "")))
    dest = safe(row.get("destination_city", row.get("destination", "")))
    price = safe(row.get("price_gbp", row.get("price", "")))
    out_date = safe(row.get("outbound_date", ""))
    ret_date = safe(row.get("return_date", ""))
    airline = safe(row.get("airline", ""))
    link = row.get("affiliate_url", row.get("deal_url", 
row.get("booking_url", ""))).strip()

    bits = []
    bits.append("✈️ <b>Flight deal</b>")
    if origin and dest:
        bits.append(f"🇬🇧 {origin} → {dest}")
    if price:
        bits.append(f"💰 £{price}")
    if out_date or ret_date:
        if out_date and ret_date:
            bits.append(f"📅 {out_date} → {ret_date}")
        else:
            bits.append(f"📅 {out_date or ret_date}")
    if airline:
        bits.append(f"🛫 {airline}")

    if link:
        bits.append(f"\n👉 <b>Book now:</b> {safe(link)}")

    return "\n".join(bits).strip()


def format_message_v4_vip(row: Dict[str, str]) -> str:
    origin = safe(row.get("origin_city", row.get("origin", "")))
    dest = safe(row.get("destination_city", row.get("destination", "")))
    price = safe(row.get("price_gbp", row.get("price", "")))
    out_date = safe(row.get("outbound_date", ""))
    ret_date = safe(row.get("return_date", ""))
    cabin = safe(row.get("cabin_class", "")).lower()
    cabin_show = "Business" if cabin == "business" else ("First" if cabin 
== "first" else ("Economy" if cabin else ""))

    ai_grade = safe(row.get("ai_grading", row.get("ai_grade", 
""))).upper()
reason = clean_whitespace(
    row.get("ai_notes") or row.get("ai_reason") or row.get("notes") or ""
)
    
affiliate = (row.get("affiliate_url") or 
row.get("deal_url") or 
row.get("booking_url") or "").strip()

    header = "✈️ <b>A-GRADE DEAL</b>" if ai_grade == "A" else 
"✈️ <b>DEAL</b>"
<b>DEAL</b>"

    lines = [header, ""]
    if origin and dest:
        lines.append(f"🇬🇧 {origin} → {dest}")
    if cabin_show:
        lines.append(f"💼 {safe(cabin_show)}")
    if out_date or ret_date:
        if out_date and ret_date:
            lines.append(f"📅 {out_date} → {ret_date}")
        else:
            lines.append(f"📅 {out_date or ret_date}")
    if price:
        lines.append(f"💰 £{price}")

    if reason:
        lines.append("")
        lines.append("<b>Why this is special:</b>")
        # keep it short; first ~3 bullet-worthy clauses
        # split on ; or . then take up to 3
        parts = [p.strip() for p in re.split(r"[.;]\s+", reason) if 
p.strip()]
        for p in parts[:3]:
            lines.append(f"• {safe(p)}")

    lines.append("")
    lines.append("⏳ Likely to disappear fast.")

    if affiliate:
        lines.append(f"\n👉 <b>Book now:</b> {safe(affiliate)}")
    else:
        lines.append("\n⚠️ <i>Missing affiliate_url</i>")

    return "\n".join(lines).strip()


def format_message_v4_free(row: Dict[str, str], stripe_link: str) -> str:
    dest = safe(row.get("destination_city", row.get("destination", "")))
    origin = safe(row.get("origin_city", row.get("origin", "")))
    price = safe(row.get("price_gbp", row.get("price", "")))
    out_date = safe(row.get("outbound_date", ""))
    ret_date = safe(row.get("return_date", ""))
    cabin = safe(row.get("cabin_class", "")).lower()
    cabin_show = "Business" if cabin == "business" else ("First" if cabin 
== "first" else "Economy")

    lines = []
    if price and dest:
        lines.append(f"🔥 <b>£{price} flights to {dest}</b>")
    else:
        lines.append("🔥 <b>Deal spotted</b>")

    lines.append("")
    lines.append("Details:")
    if origin and dest:
        lines.append(f"• 🇬🇧 From {origin}")
    if out_date or ret_date:
        if out_date and ret_date:
            lines.append(f"• 📅 {out_date} → {ret_date}")
        else:
            lines.append(f"• 📅 {out_date or ret_date}")
    lines.append(f"• 💼 {safe(cabin_show)}")

    lines.append("")
    lines.append("⚠️ Heads up:")
    lines.append("This deal was sent to our VIP channel first.")

    if stripe_link:
        lines.append("")
        lines.append("👉 Want deals like this <b>as soon as we find 
them</b>?")
        lines.append(f"Join Traveltxter VIP 👇")
        lines.append(safe(stripe_link))
    else:
        lines.append("")
        lines.append("👉 Want deals like this <b>as soon as we find 
them</b>?")
        lines.append("<i>(VIP link not configured)</i>")

    return "\n".join(lines).strip()


def build_message(row: Dict[str, str], template_version: str, mode: str, 
stripe_link: str) -> str:
    tv = (template_version or "legacy").strip().lower()
    md = (mode or "free").strip().lower()

    if tv == "v4":
        if md == "vip":
            return format_message_v4_vip(row)
        return format_message_v4_free(row, stripe_link)

    # legacy / fallback
    return format_message_legacy(row)


# =========================
# Google Sheets helpers
# =========================

def get_gspread_client() -> gspread.Client:
    sa_json = env_first(["GCP_SA_JSON"], "")
    if not sa_json:
        raise RuntimeError("Missing GCP_SA_JSON env var (service account 
json).")

    try:
        info = json.loads(sa_json)
    except Exception as e:
        raise RuntimeError(f"GCP_SA_JSON is not valid JSON: {e}")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def open_worksheet(gc: gspread.Client, spreadsheet_id: str, tab: str) -> 
gspread.Worksheet:
    sh = gc.open_by_key(spreadsheet_id)
    return sh.worksheet(tab)


def header_index_map(headers: List[str]) -> Dict[str, int]:
    m = {}
    for i, h in enumerate(headers):
        m[str(h).strip()] = i
    return m


def get_cell(row: List[str], idx_map: Dict[str, int], key: str) -> str:
    if key not in idx_map:
        return ""
    i = idx_map[key]
    if i < 0 or i >= len(row):
        return ""
    return str(row[i]).strip()


def set_cell(ws: gspread.Worksheet, row_num: int, col_num: int, value: 
str) -> None:
    ws.update_cell(row_num, col_num, value)


# =========================
# Main
# =========================

def main() -> int:
    # ---- identity / run meta
    run_id = env_first(["GITHUB_RUN_ID", "RUN_ID"], "local")
    attempt = env_first(["GITHUB_RUN_ATTEMPT", "RUN_ATTEMPT"], "1")
    worker_id = env_first(["WORKER_ID"], "telegram_publisher")
    dry_run = truthy(env_first(["DRY_RUN", "TELEGRAM_DRY_RUN"], "false"))

    # ---- spreadsheet config (back-compat)
    spreadsheet_id = env_first(["SPREADSHEET_ID", "SHEET_ID"], "").strip()
    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID/SHEET_ID env var.")

    tab = env_first(["RAW_DEALS_TAB", "DEALS_SHEET_NAME"], 
"RAW_DEALS").strip()

    status_col = env_first(["TG_STATUS_COLUMN", "TELEGRAM_STATUS_COLUMN", 
"RAW_STATUS_COLUMN"], "raw_status").strip()
    required_status = env_first(["TG_REQUIRED_STATUS", 
"TELEGRAM_REQUIRED_STATUS"], "POSTED_INSTAGRAM").strip().upper()
    posted_status = env_first(["TG_POSTED_STATUS", 
"TELEGRAM_POSTED_STATUS"], "POSTED_TELEGRAM").strip().upper()

    allow_verdicts_raw = env_first(["TELEGRAM_ALLOW_VERDICTS", 
"TG_ALLOW_VERDICTS"], "").strip()
    allow_verdicts = [v.strip().upper() for v in 
allow_verdicts_raw.split(",") if v.strip()] if allow_verdicts_raw else []
    min_ai_score = to_float(env_first(["TELEGRAM_MIN_AI_SCORE", 
"TG_MIN_AI_SCORE"], "0"), 0.0)

    max_posts = int(env_first(["MAX_POSTS_PER_RUN", 
"TELEGRAM_MAX_POSTS_PER_RUN"], "1").strip() or "1")

    # ---- telegram config
    bot_token = env_first(["TELEGRAM_BOT_TOKEN"], "").strip()
    chat_id = env_first(["TELEGRAM_CHANNEL"], "").strip()
    if not bot_token or not chat_id:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHANNEL 
env var.")

    # ---- v4 template controls
    tg_template_version = env_first(["TG_TEMPLATE_VERSION", 
"TELEGRAM_TEMPLATE_VERSION"], "legacy").strip().lower()
    tg_mode = env_first(["TG_MODE", "TELEGRAM_MODE"], 
"free").strip().lower()
    stripe_link = env_first(["STRIPE_LINK"], "").strip()
    premium_filter = env_first(["DEAL_PREMIUM_FILTER"], 
"all").strip().lower()

    posted_ts_col = env_first(["TELEGRAM_POSTED_TIMESTAMP_COLUMN"], 
"telegram_published_timestamp").strip()

    # ---- log header
    
log.info("\n============================================================")
    log.info("🚀 TravelTxter Telegram Publisher Starting")
    
log.info("============================================================")
    log.info(f"⏰ Timestamp: {utc_now_iso()}")
    log.info(f"🆔 Worker ID: {worker_id}")
    log.info(f"📋 Run: #{attempt} (ID: {run_id})")
    log.info(f"🧪 Dry Run: {dry_run}")
    log.info(f"📄 Tab: {tab}")
    log.info(f"🔎 Filter: {status_col} == {required_status}")
    log.info(f"✅ Promote on success: {status_col} -> {posted_status}")
    log.info(f"📊 Max posts per run: {max_posts}")
    
log.info("============================================================\n")

    # ---- open sheet
    gc = get_gspread_client()
    ws = open_worksheet(gc, spreadsheet_id, tab)
    log.info(f"✅ Using worksheet: '{ws.title}' ({ws.row_count} rows)")

    values = ws.get_all_values()
    if not values or len(values) < 2:
        log.info("No data rows found.")
        return 0

    headers = values[0]
    idx = header_index_map(headers)

    def has_col(name: str) -> bool:
        return name in idx

    considered = 0
    published = 0
    failed = 0

    for r_i in range(1, len(values)):  # 0 is header
        if published >= max_posts:
            break

        row = values[r_i]
        row_num = r_i + 1  # 1-indexed in Sheets

        current_status = get_cell(row, idx, status_col).strip().upper()
        if current_status != required_status:
            continue

        considered += 1

        # verdict gate (optional)
        verdict = get_cell(row, idx, "ai_verdict").strip().upper()
        if allow_verdicts and verdict and verdict not in allow_verdicts:
            continue

        # min ai score gate
        ai_score = to_float(get_cell(row, idx, "ai_score"), 0.0)
        if ai_score < min_ai_score:
            continue

        # premium filter gate (optional)
        is_premium_val = get_cell(row, idx, "is_premium")
        is_premium = truthy(is_premium_val)
        if premium_filter == "premium_only" and not is_premium:
            continue
        if premium_filter == "free_only" and is_premium:
            continue

        # build row dict
        row_dict: Dict[str, str] = {}
        for h in headers:
            key = str(h).strip()
            if not key:
                continue
            row_dict[key] = get_cell(row, idx, key)

        # message
        msg = build_message(row_dict, tg_template_version, tg_mode, 
stripe_link)

        if dry_run:
            log.info(f"🧪 Dry-run: would post row {row_num} 
(deal_id={row_dict.get('deal_id','')})")
            published += 1
            continue

        ok, info = telegram_send_message(bot_token, chat_id, msg, 
disable_preview=True)
        if not ok:
            failed += 1
            log.error(f"❌ Telegram post failed for row {row_num}: 
{info}")
            continue

        # promote status + timestamp
        try:
            # update status cell
            if has_col(status_col):
                col_num = idx[status_col] + 1
                set_cell(ws, row_num, col_num, posted_status)

            # timestamp (only if column exists)
            if posted_ts_col and has_col(posted_ts_col):
                col_num = idx[posted_ts_col] + 1
                set_cell(ws, row_num, col_num, utc_now_iso())

            published += 1
            log.info(f"✅ Posted row {row_num} and promoted status to 
{posted_status}")
            time.sleep(0.6)  # small throttle
        except Exception as e:
            failed += 1
            log.error(f"❌ Posted but failed to update sheet for row 
{row_num}: {e}")

    # summary
    
log.info("\n============================================================")
    log.info("📊 PUBLISH SUMMARY")
    
log.info("============================================================")
    log.info(f"🔎 Considered: {considered}")
    log.info(f"✅ Published:  {published}")
    log.info(f"❌ Failed:     {failed}")
    
log.info("============================================================\n")

    # optional stats write
    try:
        os.makedirs("logs", exist_ok=True)
        stats_path = os.path.join("logs", "telegram_stats.json")
        payload = {
            "timestamp": utc_now_iso(),
            "considered": considered,
            "published": published,
            "failed": failed,
            "required_status": required_status,
            "posted_status": posted_status,
            "template_version": tg_template_version,
            "mode": tg_mode,
            "premium_filter": premium_filter,
        }
        with open(stats_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        log.info(f"📊 Stats saved to {stats_path}")
    except Exception:
        # don't fail the run for stats logging
        pass

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        log.error(f"❌ Worker failed with error: {e}")
        raise

