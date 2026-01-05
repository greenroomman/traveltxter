#!/usr/bin/env python3
import os, json
import datetime as dt
from typing import Any, Dict, List, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials

def now_utc_dt() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def now_utc_str() -> str:
    return now_utc_dt().replace(microsecond=0).isoformat().replace("+00:00","Z")

def log(msg: str) -> None:
    print(f"{now_utc_str()} | {msg}", flush=True)

def env_str(name: str, default: str="") -> str:
    return (os.getenv(name) or default).strip()

def env_int(name: str, default: int) -> int:
    v = env_str(name,"")
    try:
        return int(v) if v else default
    except Exception:
        return default

def a1_update(ws: gspread.Worksheet, a1: str, value: Any) -> None:
    ws.update([[value]], a1)

def col_letter(n: int) -> str:
    s=""
    while n:
        n,r=divmod(n-1,26)
        s=chr(65+r)+s
    return s

def a1_for(row: int, col0: int) -> str:
    return f"{col_letter(col0+1)}{row}"

def get_client() -> gspread.Client:
    sa = env_str("GCP_SA_JSON_ONE_LINE")
    info = json.loads(sa)
    creds = Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return gspread.authorize(creds)

def ensure_columns(ws: gspread.Worksheet, required: List[str]) -> Dict[str,int]:
    headers = ws.row_values(1)
    changed=False
    for c in required:
        if c not in headers:
            headers.append(c); changed=True
    if changed:
        ws.update([headers], "A1")
    return {h:i for i,h in enumerate(headers)}

def send_telegram(bot_token: str, chat_id: str, text: str, buttons: Optional[List[List[Dict[str,str]]]]=None) -> str:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    if buttons:
        payload["reply_markup"] = json.dumps({"inline_keyboard": buttons})
    r = requests.post(url, data=payload, timeout=45)
    if r.status_code >= 300:
        raise RuntimeError(f"Telegram send failed: {r.status_code} {r.text[:300]}")
    return str(r.json().get("result", {}).get("message_id",""))

def strip_emojis(text: str) -> str:
    # Telegram: no emojis at all (simple conservative filter).
    return "".join(ch for ch in text if ord(ch) < 0x1F000)

def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    raw_tab = env_str("RAW_DEALS_TAB","RAW_DEALS")
    run_slot = env_str("RUN_SLOT","AM").upper()
    vip_delay_hours = env_int("VIP_DELAY_HOURS", 24)

    bot_vip = env_str("TELEGRAM_BOT_TOKEN_VIP")
    chat_vip = env_str("TELEGRAM_CHANNEL_VIP")
    bot_free = env_str("TELEGRAM_BOT_TOKEN")
    chat_free = env_str("TELEGRAM_CHANNEL")

    monthly = env_str("STRIPE_MONTHLY_LINK")
    yearly = env_str("STRIPE_YEARLY_LINK")

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")
    if not (bot_vip and chat_vip and bot_free and chat_free):
        raise RuntimeError("Missing Telegram creds")

    log("============================================================")
    log(f"✉️ Telegram publisher starting RUN_SLOT={run_slot}")
    log("============================================================")

    gc = get_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(raw_tab)

    cols = [
        "status","caption_final","booking_link_vip","affiliate_url",
        "tg_monthly_message_id","tg_monthly_timestamp",
        "tg_free_message_id","tg_free_timestamp",
        "is_telegram_eligible"
    ]
    hm = ensure_columns(ws, cols)

    rows = ws.get_all_values()
    def get(r: List[str], col: str) -> str:
        idx = hm[col]
        return r[idx].strip() if idx < len(r) else ""

    # VIP in AM after IG posted
    if run_slot == "AM":
        for i, row in enumerate(rows[1:], start=2):
            if get(row,"status").upper() != "POSTED_INSTAGRAM":
                continue
            if get(row,"is_telegram_eligible").upper() != "TRUE":
                continue
            if get(row,"tg_monthly_message_id"):
                continue

            caption = strip_emojis(get(row,"caption_final") or "")
            if not caption:
                continue

            link = get(row,"booking_link_vip") or get(row,"affiliate_url")
            # VIP message: caption + booking link line (no emojis)
            text = f"{caption}\n\nBook: {link}"

            try:
                mid = send_telegram(bot_vip, chat_vip, text)
                a1_update(ws, a1_for(i, hm["tg_monthly_message_id"]), mid)
                a1_update(ws, a1_for(i, hm["tg_monthly_timestamp"]), now_utc_str())
                a1_update(ws, a1_for(i, hm["status"]), "POSTED_TELEGRAM_VIP")
                log(f"✅ VIP TG posted row {i} message_id={mid}")
                break
            except Exception as e:
                log(f"❌ VIP TG error row {i}: {e}")

    # FREE in PM after delay
    if run_slot == "PM":
        for i, row in enumerate(rows[1:], start=2):
            if get(row,"status").upper() != "POSTED_TELEGRAM_VIP":
                continue
            if get(row,"tg_free_message_id"):
                continue

            vip_ts = get(row,"tg_monthly_timestamp")
            if not vip_ts:
                continue
            try:
                vip_time = dt.datetime.fromisoformat(vip_ts.replace("Z","+00:00"))
            except Exception:
                continue
            if now_utc_dt() < vip_time + dt.timedelta(hours=vip_delay_hours):
                continue

            caption = strip_emojis(get(row,"caption_final") or "")
            if not caption:
                continue

            link = get(row,"affiliate_url") or get(row,"booking_link_vip")
            text = f"{caption}\n\nBook: {link}"

            buttons = [[
                {"text":"Monthly", "url": monthly},
                {"text":"Yearly", "url": yearly},
            ]]

            try:
                mid = send_telegram(bot_free, chat_free, text, buttons=buttons)
                a1_update(ws, a1_for(i, hm["tg_free_message_id"]), mid)
                a1_update(ws, a1_for(i, hm["tg_free_timestamp"]), now_utc_str())
                a1_update(ws, a1_for(i, hm["status"]), "POSTED_ALL")
                log(f"✅ FREE TG posted row {i} message_id={mid}")
                break
            except Exception as e:
                log(f"❌ FREE TG error row {i}: {e}")

    log("Done.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
