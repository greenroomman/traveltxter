# workers/telegram_publisher.py
# V4.7 — phrase_used first, phrase_bank fallback
# Pure rendering. No language logic.

import os
import json
import datetime as dt
import requests
import gspread
from google.oauth2.service_account import Credentials


def env(k, d=""):
    return (os.getenv(k, d) or "").strip()


def _sa_creds():
    raw = env("GCP_SA_JSON_ONE_LINE") or env("GCP_SA_JSON")
    try:
        info = json.loads(raw)
    except json.JSONDecodeError:
        info = json.loads(raw.replace("\\n", "\n"))
    return Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )


def phrase_from_row(row):
    return (row.get("phrase_used") or row.get("phrase_bank") or "").strip()


def tg_send(token, chat_id, text):
    r = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": "true",
        },
    )
    if not r.json().get("ok"):
        raise RuntimeError(r.text)


def main():
    gc = gspread.authorize(_sa_creds())
    sh = gc.open_by_key(env("SPREADSHEET_ID") or env("SHEET_ID"))
    ws = sh.worksheet(env("RAW_DEALS_TAB", "RAW_DEALS"))

    values = ws.get_all_values()
    headers = values[0]
    h = {k: i for i, k in enumerate(headers)}

    for i, r in enumerate(values[1:], start=2):
        if r[h["status"]] != "POSTED_INSTAGRAM":
            continue

        row = {headers[j]: r[j] for j in range(len(headers))}
        phrase = phrase_from_row(row)

        msg = "\n".join(
            [
                f"£{row['price_gbp']} to {row['destination_city']}",
                f"FROM: {row['origin_city']}",
                f"OUT: {row['outbound_date']}",
                f"BACK: {row['return_date']}",
                "",
                phrase,
                "",
                "VIP members saw this first",
            ]
        ).strip()

        tg_send(env("TELEGRAM_BOT_TOKEN_VIP"), env("TELEGRAM_CHANNEL_VIP"), msg)

        ws.update_cell(i, h["status"] + 1, "POSTED_TELEGRAM_VIP")
        ws.update_cell(i, h["posted_telegram_vip_at"] + 1, dt.datetime.utcnow().isoformat() + "Z")
        return 0

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
