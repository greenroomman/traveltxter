#!/usr/bin/env python3
"""
workers/instagram_publisher.py

LOCKED SYSTEM RULES:
- Consume: status == READY_TO_PUBLISH
- Require: graphic_url present
- Produce: status -> POSTED_INSTAGRAM
- Non-technical stability fixes only (no redesign).

This patch fixes:
1) SyntaxError in caption quoting (illegal backslash inside f-string expression).
2) Instagram "media not ready" publish timing by adding retries.
"""

from __future__ import annotations

import os
import json
import time
import datetime as dt
import hashlib
from typing import Any, Dict, List

import requests
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError


# =========================
# Logging
# =========================

def now_utc_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def log(msg: str) -> None:
    print(f"{now_utc_iso()} | {msg}", flush=True)


# =========================
# Env
# =========================

def env_str(k: str, default: str = "") -> str:
    return (os.environ.get(k, default) or "").strip()


# =========================
# Sheets auth
# =========================

def _extract_sa(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()
    try:
        return json.loads(raw)
    except Exception:
        return json.loads(raw.replace("\\n", "\n"))


def get_client() -> gspread.Client:
    sa_raw = env_str("GCP_SA_JSON_ONE_LINE") or env_str("GCP_SA_JSON")
    if not sa_raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
    info = _extract_sa(sa_raw)
    creds = Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


def open_sheet_with_backoff(gc: gspread.Client, spreadsheet_id: str, attempts: int = 8) -> gspread.Spreadsheet:
    delay = 4.0
    for i in range(1, attempts + 1):
        try:
            return gc.open_by_key(spreadsheet_id)
        except APIError as e:
            if "429" in str(e):
                log(f"⏳ Sheets quota hit. Retry {i}/{attempts} in {int(delay)}s")
                time.sleep(delay)
                delay = min(delay * 1.6, 45.0)
                continue
            raise
    raise RuntimeError("Sheets quota still exceeded after retries")


# =========================
# A1 helpers
# =========================

def col_letter(n1: int) -> str:
    s = ""
    n = n1
    while n:
        n, rr = divmod(n - 1, 26)
        s = chr(65 + rr) + s
    return s


def a1(rownum: int, col0: int) -> str:
    return f"{col_letter(col0 + 1)}{rownum}"


def safe_get(row: List[str], idx: int) -> str:
    return row[idx].strip() if 0 <= idx < len(row) else ""


def ensure_columns(ws: gspread.Worksheet, headers: List[str], required: List[str]) -> List[str]:
    missing = [c for c in required if c not in headers]
    if missing:
        ws.update([headers + missing], "A1")
        log(f"🛠️ Added missing columns: {missing}")
        return headers + missing
    return headers


# =========================
# Phrase Bank
# =========================

def _truthy(x: str) -> bool:
    return (x or "").strip().lower() in ("true", "yes", "1", "y", "approved")


def load_phrase_bank(sh: gspread.Spreadsheet) -> List[Dict[str, str]]:
    try:
        ws = sh.worksheet("PHRASE_BANK")
    except Exception:
        return []
    values = ws.get_all_values()
    if len(values) < 2:
        return []
    headers = values[0]
    idx = {h: i for i, h in enumerate(headers)}
    out: List[Dict[str, str]] = []
    for r in values[1:]:
        d = {h: (r[idx[h]] if idx[h] < len(r) else "").strip() for h in headers}
        if any(d.values()):
            out.append(d)
    return out


def _channel_ok(hint: str) -> bool:
    return not hint or hint.upper() in ("ALL", "IG", "INSTAGRAM")


def pick_phrase_from_bank(bank: List[Dict[str, str]], theme: str, deal_id: str) -> str:
    pool = [
        r for r in bank
        if r.get("phrase")
        and _truthy(r.get("approved", ""))
        and _channel_ok(r.get("channel_hint", ""))
        and (not r.get("theme") or r.get("theme", "").upper() == (theme or "").upper())
    ]
    if not pool:
        return ""
    h = hashlib.md5((deal_id or "x").encode()).hexdigest()
    return (pool[int(h[:8], 16) % len(pool)].get("phrase", "") or "").strip()


# =========================
# Caption (LOCKED)
# =========================

def build_caption_ig(country: str, to_city: str, from_city: str, price: str, out_d: str, back_d: str, phrase: str) -> str:
    lines = [
        country,
        f"To: {to_city}",
        f"From: {from_city}",
        f"Price: {price}",
        f"Out: {out_d}",
        f"Return: {back_d}",
        "",
    ]

    # ✅ FIX: no backslashes inside f-string expressions
    if phrase:
        clean_phrase = (phrase or "").replace('"', "").strip()
        if clean_phrase:
            lines.append(f"“{clean_phrase}”")
            lines.append("")

    lines.append("Link in bio…")
    return "\n".join(lines).strip()


# =========================
# Instagram API (retry publish)
# =========================

def ig_create_and_publish(access_token: str, ig_user_id: str, image_url: str, caption: str) -> None:
    create_url = f"https://graph.facebook.com/v20.0/{ig_user_id}/media"
    r1 = requests.post(
        create_url,
        data={"image_url": image_url, "caption": caption, "access_token": access_token},
        timeout=60,
    )
    j1 = r1.json()
    if "id" not in j1:
        raise RuntimeError(f"IG /media failed: {j1}")

    creation_id = j1["id"]

    publish_url = f"https://graph.facebook.com/v20.0/{ig_user_id}/media_publish"

    # IG sometimes needs time to process the media container before it can be published.
    last = None
    for attempt in range(1, 11):  # up to ~60 seconds total
        time.sleep(6)
        r2 = requests.post(
            publish_url,
            data={"creation_id": creation_id, "access_token": access_token},
            timeout=60,
        )
        last = r2.json()
        if "id" in last:
            return
        log(f"⏳ IG not ready yet (attempt {attempt}/10)")

    raise RuntimeError(f"IG /media_publish failed after retries: {last}")


# =========================
# Main
# =========================

def main() -> int:
    sh = open_sheet_with_backoff(get_client(), env_str("SPREADSHEET_ID"))
    ws = sh.worksheet(env_str("RAW_DEALS_TAB", "RAW_DEALS"))

    values = ws.get_all_values()
    if not values:
        log("RAW_DEALS is empty.")
        return 0

    headers = ensure_columns(
        ws,
        values[0],
        [
            "status", "deal_id", "graphic_url",
            "price_gbp", "origin_city", "destination_city", "destination_country",
            "outbound_date", "return_date", "deal_theme", "phrase_bank",
            "posted_instagram_at", "publish_error", "publish_error_at"
        ],
    )
    h = {k: i for i, k in enumerate(headers)}

    bank = load_phrase_bank(sh)

    for rownum, r in enumerate(values[1:], start=2):
        if safe_get(r, h["status"]) != "READY_TO_PUBLISH":
            continue

        image_url = safe_get(r, h["graphic_url"])
        if not image_url:
            continue

        deal_id = safe_get(r, h["deal_id"])
        theme = safe_get(r, h["deal_theme"])

        phrase = safe_get(r, h["phrase_bank"])
        if not phrase:
            phrase = pick_phrase_from_bank(bank, theme, deal_id)
            if phrase:
                ws.update([[phrase]], a1(rownum, h["phrase_bank"]))

        caption = build_caption_ig(
            safe_get(r, h["destination_country"]),
            safe_get(r, h["destination_city"]),
            safe_get(r, h["origin_city"]),
            safe_get(r, h["price_gbp"]),
            safe_get(r, h["outbound_date"]),
            safe_get(r, h["return_date"]),
            phrase,
        )

        try:
            ig_create_and_publish(
                env_str("IG_ACCESS_TOKEN"),
                env_str("IG_USER_ID"),
                image_url,
                caption,
            )
        except Exception as e:
            ws.update([[str(e)[:300]]], a1(rownum, h["publish_error"]))
            ws.update([[now_utc_iso()]], a1(rownum, h["publish_error_at"]))
            raise

        ws.update([[now_utc_iso()]], a1(rownum, h["posted_instagram_at"]))
        ws.update([["POSTED_INSTAGRAM"]], a1(rownum, h["status"]))
        log(f"✅ IG posted row {rownum}")
        return 0

    log("No eligible rows.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
