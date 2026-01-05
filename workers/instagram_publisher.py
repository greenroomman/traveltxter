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


def ig_create_container(ig_user_id: str, token: str, image_url: str, caption: str) -> str:
    url = f"https://graph.facebook.com/v19.0/{ig_user_id}/media"
    r = requests.post(url, data={"image_url": image_url, "caption": caption, "access_token": token}, timeout=60)
    r.raise_for_status()
    return r.json().get("id", "")


def ig_publish_container(ig_user_id: str, token: str, creation_id: str) -> str:
    url = f"https://graph.facebook.com/v19.0/{ig_user_id}/media_publish"
    r = requests.post(url, data={"creation_id": creation_id, "access_token": token}, timeout=60)
    r.raise_for_status()
    return r.json().get("id", "")


def main() -> int:
    log("============================================================")
    log("📸 Instagram publisher starting")
    log("============================================================")

    sid = env("SPREADSHEET_ID")
    tab = env("RAW_DEALS_TAB", "RAW_DEALS")
    ig_token = env("IG_ACCESS_TOKEN")
    ig_user_id = env("IG_USER_ID")

    if not (sid and ig_token and ig_user_id):
        raise RuntimeError("Missing SPREADSHEET_ID / IG_ACCESS_TOKEN / IG_USER_ID")

    gc = get_client()
    sh = gc.open_by_key(sid)
    ws = sh.worksheet(tab)

    hm = ensure_cols(ws, [
        "status",
        "graphic_url",
        "caption_final",
        "ig_creation_id",
        "ig_media_id",
        "ig_published_timestamp",
        "last_error",
        "fail_count",
    ])

    rows = ws.get_all_values()
    if len(rows) <= 1:
        log("No rows.")
        return 0

    target = None
    for i, r in enumerate(rows[1:], start=2):
        status = (r[hm["status"]] if hm["status"] < len(r) else "").strip().upper()
        graphic = (r[hm["graphic_url"]] if hm["graphic_url"] < len(r) else "").strip()
        ig_media = (r[hm["ig_media_id"]] if hm["ig_media_id"] < len(r) else "").strip()
        if status == "READY_TO_PUBLISH" and graphic and not ig_media:
            target = (i, r)
            break

    if not target:
        log("No eligible IG row (need status=READY_TO_PUBLISH + graphic_url, and not already posted).")
        return 0

    sheet_row, r = target
    graphic = (r[hm["graphic_url"]] if hm["graphic_url"] < len(r) else "").strip()
    caption = (r[hm["caption_final"]] if hm["caption_final"] < len(r) else "").strip()

    if not caption:
        caption = ""  # allow blank but better than crash

    try:
        creation_id = ig_create_container(ig_user_id, ig_token, graphic, caption)
        media_id = ig_publish_container(ig_user_id, ig_token, creation_id)

        batch_update_with_backoff(ws, [
            {"range": a1(sheet_row, hm["ig_creation_id"]), "values": [[creation_id]]},
            {"range": a1(sheet_row, hm["ig_media_id"]), "values": [[media_id]]},
            {"range": a1(sheet_row, hm["ig_published_timestamp"]), "values": [[now_utc_str()]]},
            {"range": a1(sheet_row, hm["status"]), "values": [["POSTED_INSTAGRAM"]]},
            {"range": a1(sheet_row, hm["last_error"]), "values": [[""]]},
        ])
        log(f"✅ IG posted row {sheet_row} -> media_id={media_id}")
        return 0

    except Exception as e:
        err = str(e)[:400]
        log(f"❌ IG failed row {sheet_row}: {err}")
        # increment fail_count
        try:
            fc = int((r[hm["fail_count"]] if hm["fail_count"] < len(r) else "0") or "0")
        except Exception:
            fc = 0
        fc += 1
        batch_update_with_backoff(ws, [
            {"range": a1(sheet_row, hm["fail_count"]), "values": [[str(fc)]]},
            {"range": a1(sheet_row, hm["last_error"]), "values": [[err]]},
        ])
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
