#!/usr/bin/env python3
import os, json
import datetime as dt
from typing import Any, Dict, List

import requests
import gspread
from google.oauth2.service_account import Credentials

def now_utc_str() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def log(msg: str) -> None:
    print(f"{now_utc_str()} | {msg}", flush=True)

def env_str(name: str, default: str="") -> str:
    return (os.getenv(name) or default).strip()

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

def contains_non_flag_emoji(text: str) -> bool:
    # Hard rule: IG allows ONLY flag emojis.
    # Implementation: detect common emoji ranges excluding flags is hard without libs.
    # Practical enforcement: block if any emoji char outside regional indicator range.
    # Regional indicator symbols: U+1F1E6–U+1F1FF (flags are pairs).
    for ch in text:
        o = ord(ch)
        if 0x1F1E6 <= o <= 0x1F1FF:
            continue
        if o >= 0x1F300:  # broad emoji blocks
            return True
    return False

def ig_create_container(ig_user_id: str, access_token: str, image_url: str, caption: str) -> str:
    url = f"https://graph.facebook.com/v19.0/{ig_user_id}/media"
    r = requests.post(url, data={"image_url": image_url, "caption": caption, "access_token": access_token}, timeout=45)
    if r.status_code >= 300:
        raise RuntimeError(f"IG media create failed: {r.status_code} {r.text[:300]}")
    return r.json()["id"]

def ig_publish_container(ig_user_id: str, access_token: str, creation_id: str) -> str:
    url = f"https://graph.facebook.com/v19.0/{ig_user_id}/media_publish"
    r = requests.post(url, data={"creation_id": creation_id, "access_token": access_token}, timeout=45)
    if r.status_code >= 300:
        raise RuntimeError(f"IG publish failed: {r.status_code} {r.text[:300]}")
    return r.json().get("id","")

def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    raw_tab = env_str("RAW_DEALS_TAB","RAW_DEALS")
    token = env_str("IG_ACCESS_TOKEN")
    ig_user_id = env_str("IG_USER_ID")

    if not (spreadsheet_id and token and ig_user_id):
        raise RuntimeError("Missing IG creds or SPREADSHEET_ID")

    log("============================================================")
    log("📸 Instagram publisher starting")
    log("============================================================")

    gc = get_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(raw_tab)

    cols = ["status","graphic_url","caption_final","is_instagram_eligible","ig_creation_id","ig_media_id","ig_published_timestamp","ig_processing_lock","ig_locked_by"]
    hm = ensure_columns(ws, cols)

    rows = ws.get_all_values()
    def get(r: List[str], col: str) -> str:
        idx = hm[col]
        return r[idx].strip() if idx < len(r) else ""

    for i, row in enumerate(rows[1:], start=2):
        if get(row,"status").upper() != "READY_TO_PUBLISH":
            continue
        if get(row,"is_instagram_eligible").upper() != "TRUE":
            continue
        if not get(row,"graphic_url"):
            continue

        caption = get(row,"caption_final")
        if not caption:
            continue

        # Enforce emoji rule
        if contains_non_flag_emoji(caption):
            log(f"🛑 Blocked row {i}: caption contains non-flag emoji")
            continue

        try:
            creation_id = ig_create_container(ig_user_id, token, get(row,"graphic_url"), caption)
            media_id = ig_publish_container(ig_user_id, token, creation_id)

            a1_update(ws, a1_for(i, hm["ig_creation_id"]), creation_id)
            a1_update(ws, a1_for(i, hm["ig_media_id"]), media_id)
            a1_update(ws, a1_for(i, hm["ig_published_timestamp"]), now_utc_str())
            a1_update(ws, a1_for(i, hm["status"]), "POSTED_INSTAGRAM")

            log(f"✅ IG posted row {i} media_id={media_id}")
            break  # 1 per run
        except Exception as e:
            log(f"❌ IG error row {i}: {e}")

    log("Done.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
