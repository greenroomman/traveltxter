#!/usr/bin/env python3
import os
import json
import datetime as dt
from typing import Any, Dict, List, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError


def now_utc_str() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def log(msg: str) -> None:
    print(f"{now_utc_str()} | {msg}", flush=True)


def env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def env_int(name: str, default: int) -> int:
    v = env(name, "")
    try:
        return int(v) if v else default
    except Exception:
        return default


def col_letter(n1: int) -> str:
    s = ""
    n = n1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def a1(row: int, col0: int) -> str:
    return f"{col_letter(col0 + 1)}{row}"


def batch_update_with_backoff(ws: gspread.Worksheet, data: List[Dict[str, Any]], tries: int = 6) -> None:
    delay = 1.0
    for _ in range(tries):
        try:
            ws.batch_update(data)
            return
        except APIError as e:
            if "429" in str(e) or "Quota exceeded" in str(e):
                log(f"⚠️ Sheets quota 429. Backoff {delay:.1f}s")
                import time
                time.sleep(delay)
                delay = min(delay * 2, 20.0)
                continue
            raise


def get_client() -> gspread.Client:
    sa = env("GCP_SA_JSON_ONE_LINE")
    if not sa:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE")
    info = json.loads(sa)
    creds = Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return gspread.authorize(creds)


def ensure_cols(ws: gspread.Worksheet, needed: List[str]) -> Dict[str, int]:
    headers = ws.row_values(1)
    changed = False
    for c in needed:
        if c not in headers:
            headers.append(c)
            changed = True
    if changed:
        ws.update([headers], "A1")
    return {h: i for i, h in enumerate(headers)}


def tg_send(bot_token: str, chat_id: str, text: str) -> str:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    r = requests.post(url, json={"chat_id": chat_id, "text": text, "disable_web_page_preview": False}, timeout=60)
    r.raise_for_status()
    data = r.json()
    return str(data.get("result", {}).get("message_id", ""))


def parse_utc(ts: str) -> Optional[dt.datetime]:
    try:
        if ts.endswith("Z"):
            ts = ts[:-1]
        return dt.datetime.fromisoformat(ts).replace(tzinfo=dt.timezone.utc)
    except Exception:
        return None


def main() -> int:
    run_slot = env("RUN_SLOT", "AM").upper()
    vip_delay_hours = env_int("VIP_DELAY_HOURS", 24)

    log("============================================================")
    log(f"✉️ Telegram publisher starting RUN_SLOT={run_slot}")
    log("============================================================")

    sid = env("SPREADSHEET_ID")
    tab = env("RAW_DEALS_TAB", "RAW_DEALS")

    bot_free = env("TELEGRAM_BOT_TOKEN")
    chan_free = env("TELEGRAM_CHANNEL")
    bot_vip = env("TELEGRAM_BOT_TOKEN_VIP")
    chan_vip = env("TELEGRAM_CHANNEL_VIP")

    if not sid:
        raise RuntimeError("Missing SPREADSHEET_ID")

    gc = get_client()
    sh = gc.open_by_key(sid)
    ws = sh.worksheet(tab)

    hm = ensure_cols(ws, [
        "status",
        "caption_final",
        "tg_free_message_id", "tg_free_timestamp",
        "tg_monthly_message_id", "tg_monthly_timestamp",  # VIP
        "ig_published_timestamp",
        "last_error", "fail_count",
    ])

    rows = ws.get_all_values()
    if len(rows) <= 1:
        log("No rows.")
        return 0

    if run_slot == "AM":
        # VIP post: only after Instagram posted
        if not (bot_vip and chan_vip):
            log("VIP bot/channel missing; skipping.")
            return 0

        target = None
        for i, r in enumerate(rows[1:], start=2):
            status = (r[hm["status"]] if hm["status"] < len(r) else "").strip().upper()
            ig_ts = (r[hm["ig_published_timestamp"]] if hm["ig_published_timestamp"] < len(r) else "").strip()
            vip_id = (r[hm["tg_monthly_message_id"]] if hm["tg_monthly_message_id"] < len(r) else "").strip()
            if status == "POSTED_INSTAGRAM" and ig_ts and not vip_id:
                target = (i, r)
                break

        if not target:
            log("No eligible VIP row (need status=POSTED_INSTAGRAM and not already TG VIP).")
            return 0

        sheet_row, r = target
        text = (r[hm["caption_final"]] if hm["caption_final"] < len(r) else "").strip() or "New deal"

        try:
            mid = tg_send(bot_vip, chan_vip, text)
            batch_update_with_backoff(ws, [
                {"range": a1(sheet_row, hm["tg_monthly_message_id"]), "values": [[mid]]},
                {"range": a1(sheet_row, hm["tg_monthly_timestamp"]), "values": [[now_utc_str()]]},
                {"range": a1(sheet_row, hm["status"]), "values": [["POSTED_TELEGRAM_VIP"]]},
                {"range": a1(sheet_row, hm["last_error"]), "values": [[""]]},
            ])
            log(f"✅ TG VIP posted row {sheet_row} -> message_id={mid}")
            return 0
        except Exception as e:
            err = str(e)[:400]
            log(f"❌ TG VIP failed row {sheet_row}: {err}")
            return 0

    # PM slot: FREE post only after VIP delay window
    if not (bot_free and chan_free):
        log("FREE bot/channel missing; skipping.")
        return 0

    now_dt = dt.datetime.now(dt.timezone.utc)

    target = None
    for i, r in enumerate(rows[1:], start=2):
        status = (r[hm["status"]] if hm["status"] < len(r) else "").strip().upper()
        vip_ts = (r[hm["tg_monthly_timestamp"]] if hm["tg_monthly_timestamp"] < len(r) else "").strip()
        free_id = (r[hm["tg_free_message_id"]] if hm["tg_free_message_id"] < len(r) else "").strip()

        if status != "POSTED_TELEGRAM_VIP":
            continue
        if free_id:
            continue
        vip_dt = parse_utc(vip_ts)
        if not vip_dt:
            continue
        if (now_dt - vip_dt).total_seconds() < vip_delay_hours * 3600:
            continue

        target = (i, r)
        break

    if not target:
        log("No eligible FREE row (need status=POSTED_TELEGRAM_VIP and VIP delay satisfied).")
        return 0

    sheet_row, r = target
    text = (r[hm["caption_final"]] if hm["caption_final"] < len(r) else "").strip() or "New deal"

    try:
        mid = tg_send(bot_free, chan_free, text)
        batch_update_with_backoff(ws, [
            {"range": a1(sheet_row, hm["tg_free_message_id"]), "values": [[mid]]},
            {"range": a1(sheet_row, hm["tg_free_timestamp"]), "values": [[now_utc_str()]]},
            {"range": a1(sheet_row, hm["status"]), "values": [["POSTED_ALL"]]},
            {"range": a1(sheet_row, hm["last_error"]), "values": [[""]]},
        ])
        log(f"✅ TG FREE posted row {sheet_row} -> message_id={mid}")
        return 0
    except Exception as e:
        err = str(e)[:400]
        log(f"❌ TG FREE failed row {sheet_row}: {err}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
