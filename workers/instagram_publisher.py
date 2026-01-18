# workers/instagram_publisher.py
# V4.7 — phrase_used first, phrase_bank fallback
# No phrase selection. No creativity.

import os
import json
import time
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


def main():
    gc = gspread.authorize(_sa_creds())
    sh = gc.open_by_key(env("SPREADSHEET_ID") or env("SHEET_ID"))
    ws = sh.worksheet(env("RAW_DEALS_TAB", "RAW_DEALS"))

    values = ws.get_all_values()
    headers = values[0]
    h = {k: i for i, k in enumerate(headers)}

    for i, r in enumerate(values[1:], start=2):
        if r[h["status"]] != "READY_TO_PUBLISH":
            continue

        phrase = phrase_from_row({headers[j]: r[j] for j in range(len(headers))})
        caption = "\n".join(
            [
                r[h["destination_country"]],
                f"To: {r[h['destination_city']]}",
                f"From: {r[h['origin_city']]}",
                f"Price: £{r[h['price_gbp']]}",
                "",
                phrase,
                "Link in bio…",
            ]
        ).strip()

        image_url = r[h["graphic_url"]]

        create = requests.post(
            f"https://graph.facebook.com/v20.0/{env('IG_USER_ID')}/media",
            data={
                "image_url": image_url,
                "caption": caption,
                "access_token": env("IG_ACCESS_TOKEN"),
            },
        ).json()

        cid = create.get("id")
        if not cid:
            raise RuntimeError(create)

        time.sleep(2)

        pub = requests.post(
            f"https://graph.facebook.com/v20.0/{env('IG_USER_ID')}/media_publish",
            data={
                "creation_id": cid,
                "access_token": env("IG_ACCESS_TOKEN"),
            },
        ).json()

        if "id" not in pub:
            raise RuntimeError(pub)

        ws.update_cell(i, h["status"] + 1, "POSTED_INSTAGRAM")
        ws.update_cell(i, h["posted_instagram_at"] + 1, dt.datetime.utcnow().isoformat() + "Z")
        return 0

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
