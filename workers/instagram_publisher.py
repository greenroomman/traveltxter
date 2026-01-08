#!/usr/bin/env python3
"""
workers/instagram_publisher.py

LOCKED IG CAPTION DESIGN:
- Do NOT change caption layout/lines.
- Only fix: phrase selection must use RAW_DEALS.phrase_bank if present,
  else pick from PHRASE_BANK and write it back.

Posting contract:
- Consumes: status == READY_TO_PUBLISH
- Requires: graphic_url present
- Produces: status -> POSTED_INSTAGRAM
"""

from __future__ import annotations

import os
import json
import time
import datetime as dt
import hashlib
from typing import Any, Dict, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError


# ============================================================
# Logging
# ============================================================

def now_utc_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def log(msg: str) -> None:
    print(f"{now_utc_iso()} | {msg}", flush=True)


# ============================================================
# Env helpers
# ============================================================

def env_str(k: str, default: str = "") -> str:
    return (os.environ.get(k, default) or "").strip()

def env_int(k: str, default: int) -> int:
    try:
        return int(env_str(k, str(default)))
    except Exception:
        return default


# ============================================================
# Sheets auth
# ============================================================

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
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds)

def open_sheet_with_backoff(gc: gspread.Client, spreadsheet_id: str, attempts: int = 8) -> gspread.Spreadsheet:
    delay = 4.0
    for i in range(1, attempts + 1):
        try:
            return gc.open_by_key(spreadsheet_id)
        except APIError as e:
            msg = str(e)
            if "429" in msg or "Quota exceeded" in msg:
                log(f"⏳ Sheets quota (429). Retry {i}/{attempts} in {int(delay)}s...")
                time.sleep(delay)
                delay = min(delay * 1.6, 45.0)
                continue
            raise
    raise RuntimeError("Sheets quota still exceeded after retries (429).")


# ============================================================
# A1 helpers
# ============================================================

def col_letter(n1: int) -> str:
    s = ""
    n = n1
    while n:
        n, rr = divmod(n - 1, 26)
        s = chr(65 + rr) + s
    return s

def a1(rownum: int, col0: int) -> str:
    return f"{col_letter(col0 + 1)}{rownum}"

def ensure_columns(ws: gspread.Worksheet, headers: List[str], required: List[str]) -> List[str]:
    missing = [c for c in required if c not in headers]
    if not missing:
        return headers
    ws.update([headers + missing], "A1")
    log(f"🛠️ Added missing columns: {missing}")
    return headers + missing

def safe_get(row: List[str], idx: int) -> str:
    return row[idx].strip() if 0 <= idx < len(row) else ""


# ============================================================
# Phrase Bank (LOCKED)
# Tab: PHRASE_BANK
# Headers: theme / category / phrase / approved / channel_hint / max_per_month / notes
# ============================================================

def _truthy(x: str) -> bool:
    v = (x or "").strip().lower()
    return v in ("true", "yes", "1", "y", "on", "approved")

def load_phrase_bank(sh: gspread.Spreadsheet) -> List[Dict[str, str]]:
    try:
        ws = sh.worksheet("PHRASE_BANK")
    except Exception:
        return []

    values = ws.get_all_values()
    if not values or len(values) < 2:
        return []

    headers = [h.strip() for h in values[0]]
    idx = {h: i for i, h in enumerate(headers)}

    out: List[Dict[str, str]] = []
    for r in values[1:]:
        d: Dict[str, str] = {}
        for h in headers:
            j = idx[h]
            d[h] = (r[j] if j < len(r) else "").strip()
        if any(d.values()):
            out.append(d)
    return out

def channel_match(hint: str, target: str) -> bool:
    """
    target: IG
    allow hints: blank, ALL, INSTAGRAM, IG
    """
    h = (hint or "").strip().upper()
    if not h or h == "ALL":
        return True
    if h in ("IG", "INSTAGRAM"):
        return True
    return False

def pick_phrase_from_bank(bank: List[Dict[str, str]], theme: str, deal_id: str) -> str:
    th = (theme or "").strip().upper()

    pool = []
    for r in bank:
        phrase = (r.get("phrase", "") or "").strip()
        if not phrase:
            continue
        if not _truthy(r.get("approved", "")):
            continue
        if not channel_match(r.get("channel_hint", ""), "IG"):
            continue

        rt = (r.get("theme", "") or "").strip().upper()
        if th and rt and rt != th:
            continue

        pool.append(r)

    if not pool:
        return ""

    h = hashlib.md5((deal_id or "noid").encode("utf-8")).hexdigest()
    idx = int(h[:8], 16) % len(pool)
    return (pool[idx].get("phrase", "") or "").strip()


# ============================================================
# LOCKED IG caption design (do not change)
# ============================================================

FLAG_MAP = {
    "ICELAND": "🇮🇸",
    "SPAIN": "🇪🇸",
    "PORTUGAL": "🇵🇹",
    "FRANCE": "🇫🇷",
    "ITALY": "🇮🇹",
    "GREECE": "🇬🇷",
    "MOROCCO": "🇲🇦",
    "TURKEY": "🇹🇷",
    "THAILAND": "🇹🇭",
    "JAPAN": "🇯🇵",
    "USA": "🇺🇸",
    "UNITED STATES": "🇺🇸",
}

def country_flag(country: str) -> str:
    c = (country or "").strip().upper()
    return FLAG_MAP.get(c, "")

def quote_phrase(s: str) -> str:
    t = (s or "").strip()
    if not t:
        return ""
    t = t.strip('"\''"“”‘’")
    return f"“{t}”"

def build_caption_ig(country: str, to_city: str, from_city: str, price_gbp: str, out_d: str, back_d: str, phrase: str) -> str:
    flag = country_flag(country)
    first_line = f"{country}{(' ' + flag) if flag else ''}".strip() if country else (to_city or "TravelTxter deal")

    lines: List[str] = [
        first_line,
        f"To: {to_city}",
        f"From: {from_city}",
        f"Price: {price_gbp}",
        f"Out: {out_d}",
        f"Return: {back_d}",
        "",
    ]
    if phrase:
        lines.append(quote_phrase(phrase))
        lines.append("")
    lines.append("Link in bio…")
    return "\n".join(lines).strip()


# ============================================================
# Instagram API
# ============================================================

def ig_create_and_publish(access_token: str, ig_user_id: str, image_url: str, caption: str) -> None:
    """
    2-step publish:
      1) /media
      2) /media_publish
    """
    # Create media container
    create_url = f"https://graph.facebook.com/v20.0/{ig_user_id}/media"
    r1 = requests.post(create_url, data={
        "image_url": image_url,
        "caption": caption,
        "access_token": access_token,
    }, timeout=60)
    j1 = r1.json()
    if "id" not in j1:
        raise RuntimeError(f"IG /media failed: {j1}")

    creation_id = j1["id"]

    # Publish
    publish_url = f"https://graph.facebook.com/v20.0/{ig_user_id}/media_publish"
    r2 = requests.post(publish_url, data={
        "creation_id": creation_id,
        "access_token": access_token,
    }, timeout=60)
    j2 = r2.json()
    if "id" not in j2:
        raise RuntimeError(f"IG /media_publish failed: {j2}")


# ============================================================
# Main
# ============================================================

def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    ig_access_token = env_str("IG_ACCESS_TOKEN")
    ig_user_id = env_str("IG_USER_ID")

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")
    if not ig_access_token or not ig_user_id:
        raise RuntimeError("Missing IG_ACCESS_TOKEN / IG_USER_ID")

    gc = get_client()
    sh = open_sheet_with_backoff(gc, spreadsheet_id)
    ws = sh.worksheet(tab)

    values = ws.get_all_values()
    if not values or len(values) < 2:
        log("No rows to publish.")
        return 0

    headers = [h.strip() for h in values[0]]
    required_cols = [
        "status",
        "deal_id",
        "graphic_url",
        "price_gbp",
        "origin_city",
        "destination_city",
        "destination_country",
        "outbound_date",
        "return_date",
        "deal_theme",
        "phrase_bank",
        "posted_instagram_at",
        "publish_error",
        "publish_error_at",
    ]
    headers = ensure_columns(ws, headers, required_cols)

    values = ws.get_all_values()
    headers = [h.strip() for h in values[0]]
    rows = values[1:]
    h = {name: i for i, name in enumerate(headers)}

    phrase_rows = load_phrase_bank(sh)

    # Pick first eligible (status READY_TO_PUBLISH, has graphic_url)
    target: Optional[Tuple[int, List[str]]] = None
    for rownum, r in enumerate(rows, start=2):
        if safe_get(r, h["status"]).strip() != "READY_TO_PUBLISH":
            continue
        if not safe_get(r, h["graphic_url"]):
            continue
        target = (rownum, r)
        break

    if not target:
        log("No eligible rows (READY_TO_PUBLISH with graphic_url).")
        return 0

    rownum, r = target
    deal_id = safe_get(r, h["deal_id"])
    theme = safe_get(r, h["deal_theme"])

    # Phrase: use RAW_DEALS.phrase_bank first (LOCKED), else pick+persist.
    phrase = safe_get(r, h["phrase_bank"])
    if not phrase:
        picked = pick_phrase_from_bank(phrase_rows, theme, deal_id)
        if picked:
            ws.update([[picked]], a1(rownum, h["phrase_bank"]))
            phrase = picked

    # Build caption (LOCKED FORMAT)
    caption = build_caption_ig(
        country=safe_get(r, h["destination_country"]),
        to_city=safe_get(r, h["destination_city"]),
        from_city=safe_get(r, h["origin_city"]),
        price_gbp=safe_get(r, h["price_gbp"]),
        out_d=safe_get(r, h["outbound_date"]),
        back_d=safe_get(r, h["return_date"]),
        phrase=phrase,
    )

    image_url = safe_get(r, h["graphic_url"])

    try:
        ig_create_and_publish(ig_access_token, ig_user_id, image_url, caption)
    except Exception as e:
        ws.batch_update(
            [
                {"range": a1(rownum, h["publish_error"]), "values": [[str(e)[:300]]]},
                {"range": a1(rownum, h["publish_error_at"]), "values": [[now_utc_iso()]]},
            ],
            value_input_option="USER_ENTERED",
        )
        raise

    ws.batch_update(
        [
            {"range": a1(rownum, h["posted_instagram_at"]), "values": [[now_utc_iso()]]},
            {"range": a1(rownum, h["status"]), "values": [["POSTED_INSTAGRAM"]]},
        ],
        value_input_option="USER_ENTERED",
    )

    log(f"✅ IG posted row {rownum} deal_id={deal_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
