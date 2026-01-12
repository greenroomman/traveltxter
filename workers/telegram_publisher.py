#!/usr/bin/env python3
"""
TravelTxter V4.5x — telegram_publisher.py (LOCKED + THEME-GATED)

ADDITIONAL LOCKED RULE (RESTORED):
- Telegram may ONLY publish rows whose theme matches THEME OF THE DAY.

Theme source priority:
1) CONFIG tab (key/value)
2) Env var THEME_OF_DAY (fallback)

If no theme-of-day is set -> HARD STOP (do not publish).
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

def env_any(keys: List[str], default: str = "") -> str:
    for k in keys:
        v = env_str(k, "")
        if v:
            return v
    return default

def iso_now() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


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
# CONFIG → Theme of the day
# -----------------------------

def _norm_theme(s: str) -> str:
    return (s or "").strip().lower().replace(" ", "_")

def load_theme_of_day(sh: gspread.Spreadsheet) -> str:
    try:
        ws = sh.worksheet("CONFIG")
        values = ws.get_all_values()
        if values and len(values) >= 2:
            headers = [h.strip().lower() for h in values[0]]
            rows = values[1:]

            # key/value headers
            if "key" in headers and "value" in headers:
                k_i = headers.index("key")
                v_i = headers.index("value")
                for r in rows:
                    k = (r[k_i] if k_i < len(r) else "").strip().lower()
                    v = (r[v_i] if v_i < len(r) else "").strip()
                    if k in ("theme_of_day", "active_theme", "todays_theme", "today_theme", "theme"):
                        if v:
                            return _norm_theme(v)

            # fallback: first two columns
            for r in rows:
                if len(r) >= 2:
                    k = (r[0] or "").strip().lower()
                    v = (r[1] or "").strip()
                    if k in ("theme_of_day", "active_theme", "todays_theme", "today_theme", "theme"):
                        if v:
                            return _norm_theme(v)
    except Exception:
        pass

    env_theme = env_str("THEME_OF_DAY")
    if env_theme:
        return _norm_theme(env_theme)

    return ""


# -----------------------------
# Utilities
# -----------------------------

def safe_float(x: str) -> Optional[float]:
    try:
        return float(str(x).strip())
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
# Ranking (unchanged)
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


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    run_slot = env_str("RUN_SLOT", "VIP").upper()
    mode = "FREE" if run_slot in ("FREE", "PM") else "VIP"

    spreadsheet_id = env_any(["SPREADSHEET_ID", "SHEET_ID"])
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")

    gc = gs_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(tab)

    theme_of_day = load_theme_of_day(sh)
    if not theme_of_day:
        raise RuntimeError("Theme-of-day is not set (CONFIG.theme_of_day or THEME_OF_DAY env var required)")

    log(f"🎯 Theme of the day (LOCKED): {theme_of_day}")

    values = ws.get_all_values()
    if len(values) < 2:
        log("No rows.")
        return 0

    headers = [h.strip() for h in values[0]]
    rows = values[1:]
    h = {name: i for i, name in enumerate(headers)}

    consume_status = "POSTED_INSTAGRAM" if mode == "VIP" else "POSTED_TELEGRAM_VIP"
    ts_col = "posted_telegram_vip_at" if mode == "VIP" else "posted_telegram_free_at"
    promote_status = "POSTED_TELEGRAM_VIP" if mode == "VIP" else "POSTED_ALL"

    eligible: List[Tuple[int, Dict[str, str]]] = []

    for rownum, vals in enumerate(rows, start=2):
        status = (vals[h["status"]] if h["status"] < len(vals) else "").strip()
        if status != consume_status:
            continue

        row = {headers[i]: (vals[i] if i < len(vals) else "") for i in range(len(headers))}
        row_theme = _norm_theme(pick_first_present(row, ["deal_theme", "theme"]))

        if row_theme != theme_of_day:
            continue

        eligible.append((rownum, row))

    if not eligible:
        log("Done. Telegram posted 0 (no rows match theme-of-day gate).")
        return 0

    eligible.sort(key=lambda it: rank_key(it[0], it[1]), reverse=True)
    rownum, row = eligible[0]

    deal_id = (row.get("deal_id") or "").strip()
    phrase = (row.get("phrase_bank") or "").strip()
    if not phrase:
        bank = load_phrase_bank(sh)
        chosen = pick_phrase(bank, pick_first_present(row, ["deal_theme", "theme"]), deal_id)
        if chosen:
            ws.update([[chosen]], gspread.utils.rowcol_to_a1(rownum, h["phrase_bank"] + 1))
            row["phrase_bank"] = chosen

    # Build message (unchanged formatting)
    if mode == "VIP":
        msg = row.get("message_vip") or ""
    else:
        msg = row.get("message_free") or ""

    if not msg:
        raise RuntimeError("Message builder returned empty message")

    bot = env_any(["TELEGRAM_BOT_TOKEN_VIP"]) if mode == "VIP" else env_any(["TELEGRAM_BOT_TOKEN"])
    chat = env_any(["TELEGRAM_CHANNEL_VIP"]) if mode == "VIP" else env_any(["TELEGRAM_CHANNEL"])

    tg_send(bot, chat, msg)

    ws.batch_update(
        [
            {"range": gspread.utils.rowcol_to_a1(rownum, h[ts_col] + 1), "values": [[iso_now()]]},
            {"range": gspread.utils.rowcol_to_a1(rownum, h["status"] + 1), "values": [[promote_status]]},
        ],
        value_input_option="USER_ENTERED",
    )

    log(f"✅ Telegram posted row {rownum} -> {promote_status} (theme={theme_of_day})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
