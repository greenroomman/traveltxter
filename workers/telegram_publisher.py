#!/usr/bin/env python3
"""
workers/instagram_publisher.py

LOCKED IG CAPTION DESIGN:
- Do NOT change caption layout/lines.
- Phrase selection rule:
    1) Use RAW_DEALS.phrase_bank if present
    2) Else pick from PHRASE_BANK (approved + channel hint) and WRITE BACK to RAW_DEALS.phrase_bank

Posting contract:
- Consumes: status == READY_TO_PUBLISH
- Requires: graphic_url present
- Produces: status -> POSTED_INSTAGRAM + posted_instagram_at timestamp
- On failure: writes publish_error + publish_error_at (does NOT mutate to POSTED_INSTAGRAM)

Critical pipeline fix:
- Instagram is async. After /media returns a creation_id, the media may NOT be ready immediately.
- We MUST poll the container status_code until FINISHED (or timeout) before calling /media_publish.
"""

from __future__ import annotations

import os
import json
import time
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials


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
    v = os.environ.get(k)
    return v.strip() if v and v.strip() else default

def env_int(k: str, default: int) -> int:
    v = os.environ.get(k)
    try:
        return int(v) if v is not None else default
    except Exception:
        return default


# ============================================================
# Google Sheets
# ============================================================

RAW_DEALS_TAB = env_str("RAW_DEALS_TAB", "RAW_DEALS")
PHRASE_BANK_TAB = env_str("PHRASE_BANK_TAB", "PHRASE_BANK")

REQUIRED_RAW_COLS = [
    "status",
    "deal_id",
    "destination_country",
    "destination_city",
    "origin_city",
    "price_gbp",
    "outbound_date",
    "return_date",
    "graphic_url",
    "phrase_bank",
    "posted_instagram_at",
    "publish_error",
    "publish_error_at",
    "deal_theme",
    "theme",
]

PHRASE_BANK_REQUIRED_COLS = [
    "theme",
    "category",
    "phrase",
    "approved",
    "channel_hint",
    "max_per_month",
    "notes",
]

def _load_sa_creds() -> Credentials:
    """
    Supports either:
    - GCP_SA_JSON_ONE_LINE (recommended)
    - GCP_SA_JSON
    """
    raw = env_str("GCP_SA_JSON_ONE_LINE") or env_str("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing service account JSON: set GCP_SA_JSON_ONE_LINE or GCP_SA_JSON")

    try:
        info = json.loads(raw)
    except Exception:
        # Some users paste multi-line JSON into a single-line secret without escaping;
        # try to repair obvious newline issues (minimal, safe).
        info = json.loads(raw.replace("\n", "").strip())

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    return Credentials.from_service_account_info(info, scopes=scopes)

def _open_sheet() -> gspread.Spreadsheet:
    sheet_id = env_str("SPREADSHEET_ID") or env_str("SHEET_ID")
    if not sheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")
    gc = gspread.authorize(_load_sa_creds())
    return gc.open_by_key(sheet_id)

def _ensure_headers(ws: gspread.Worksheet, required: List[str]) -> List[str]:
    headers = ws.row_values(1)
    changed = False
    for col in required:
        if col not in headers:
            headers.append(col)
            changed = True
    if changed:
        ws.update([headers], "A1")
    return headers

def _header_map(headers: List[str]) -> Dict[str, int]:
    return {h: i + 1 for i, h in enumerate(headers)}

def _cell(ws: gspread.Worksheet, row: int, col: int, value: Any) -> None:
    # gspread v6 safe: 2D update
    a1 = gspread.utils.rowcol_to_a1(row, col)
    ws.update([[value]], a1)

def _row_dict_from_values(headers: List[str], values: List[str]) -> Dict[str, str]:
    d: Dict[str, str] = {}
    for i, h in enumerate(headers):
        d[h] = values[i] if i < len(values) else ""
    return d


# ============================================================
# Phrase helpers (LOCKED)
# ============================================================

def _truthy(v: str) -> bool:
    s = (v or "").strip().lower()
    return s in ("1", "true", "yes", "y", "approved", "ok")

def _channel_ok(hint: str) -> bool:
    s = (hint or "").strip().lower()
    return (s == "" or s in ("ig", "instagram", "all", "any", "both"))

def pick_phrase(ws_phrase: gspread.Worksheet, theme: str) -> str:
    """
    Returns a phrase string or "".
    Does NOT attempt fancy rotation here — keep it deterministic and simple.
    """
    rows = ws_phrase.get_all_records()
    theme_norm = (theme or "").strip().lower()

    candidates: List[str] = []
    for r in rows:
        phrase = (r.get("phrase") or "").strip()
        if not phrase:
            continue
        if not _truthy(str(r.get("approved", ""))):
            continue
        if not _channel_ok(str(r.get("channel_hint", ""))):
            continue

        r_theme = (str(r.get("theme", "")) or "").strip().lower()
        # Theme match if provided, otherwise allow generic phrases
        if theme_norm and r_theme and r_theme != theme_norm:
            continue

        candidates.append(phrase)

    if not candidates:
        return ""
    # deterministic-ish: pick first; avoids random churn and helps auditing
    return candidates[0]

def quote_phrase(p: str) -> str:
    p = (p or "").strip()
    if not p:
        return ""
    # simple human quote styling
    if p.startswith("“") or p.startswith('"'):
        return p
    return f"“{p}”"

def country_flag(country: str) -> str:
    """
    Conservative: only a few common ones; avoid wrong flags.
    """
    c = (country or "").strip().lower()
    m = {
        "spain": "🇪🇸",
        "portugal": "🇵🇹",
        "france": "🇫🇷",
        "italy": "🇮🇹",
        "greece": "🇬🇷",
        "iceland": "🇮🇸",
        "hungary": "🇭🇺",
        "czech republic": "🇨🇿",
        "czechia": "🇨🇿",
        "turkey": "🇹🇷",
        "morocco": "🇲🇦",
        "mexico": "🇲🇽",
        "japan": "🇯🇵",
        "thailand": "🇹🇭",
        "united states": "🇺🇸",
        "usa": "🇺🇸",
        "australia": "🇦🇺",
    }
    return m.get(c, "")

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
# Instagram API (async-safe)
# ============================================================

GRAPH_BASE = "https://graph.facebook.com/v19.0"

def _ig_post(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.post(url, data=params, timeout=45)
    try:
        j = r.json()
    except Exception:
        j = {"_raw": r.text}
    if r.status_code >= 400:
        raise RuntimeError(f"IG HTTP {r.status_code}: {j}")
    return j

def _ig_get(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.get(url, params=params, timeout=45)
    try:
        j = r.json()
    except Exception:
        j = {"_raw": r.text}
    if r.status_code >= 400:
        raise RuntimeError(f"IG HTTP {r.status_code}: {j}")
    return j

def ig_create_container(ig_user_id: str, access_token: str, image_url: str, caption: str) -> str:
    url = f"{GRAPH_BASE}/{ig_user_id}/media"
    j = _ig_post(url, {
        "image_url": image_url,
        "caption": caption,
        "access_token": access_token,
    })
    creation_id = str(j.get("id") or "").strip()
    if not creation_id:
        raise RuntimeError(f"IG /media did not return id: {j}")
    return creation_id

def ig_wait_until_ready(creation_id: str, access_token: str, max_wait_s: int, poll_s: int) -> str:
    """
    Poll the container status until FINISHED.
    Returns final status_code.
    """
    url = f"{GRAPH_BASE}/{creation_id}"
    deadline = time.time() + max_wait_s
    last_status = "UNKNOWN"
    while time.time() < deadline:
        j = _ig_get(url, {"fields": "status_code", "access_token": access_token})
        last_status = str(j.get("status_code") or "UNKNOWN")
        if last_status == "FINISHED":
            return last_status
        if last_status in ("ERROR", "EXPIRED"):
            return last_status
        time.sleep(poll_s)
    return last_status  # timeout

def ig_publish(ig_user_id: str, access_token: str, creation_id: str) -> str:
    url = f"{GRAPH_BASE}/{ig_user_id}/media_publish"
    j = _ig_post(url, {"creation_id": creation_id, "access_token": access_token})
    media_id = str(j.get("id") or "").strip()
    if not media_id:
        raise RuntimeError(f"IG /media_publish did not return id: {j}")
    return media_id

def ig_create_and_publish(
    ig_user_id: str,
    access_token: str,
    image_url: str,
    caption: str,
    max_wait_s: int = 45,
    poll_s: int = 3,
    publish_retries: int = 2,
) -> str:
    """
    Full async-safe publish:
    - create container
    - wait until status FINISHED
    - publish
    - if publish returns "not ready" edge-case, wait+retry a couple times
    """
    creation_id = ig_create_container(ig_user_id, access_token, image_url, caption)
    log(f"IG: created container {creation_id}")

    status = ig_wait_until_ready(creation_id, access_token, max_wait_s=max_wait_s, poll_s=poll_s)
    log(f"IG: container status_code={status}")

    if status != "FINISHED":
        raise RuntimeError(f"IG container not ready (status_code={status})")

    # publish with small retry in case Graph lags even after FINISHED
    last_err: Optional[Exception] = None
    for attempt in range(publish_retries + 1):
        try:
            media_id = ig_publish(ig_user_id, access_token, creation_id)
            log(f"IG: published media {media_id}")
            return media_id
        except Exception as e:
            last_err = e
            # Retry only if looks like readiness delay
            msg = str(e).lower()
            if ("not ready" in msg) or ("not available" in msg) or ("9007" in msg):
                wait = 4 + attempt * 4
                log(f"IG: publish not ready yet; retrying in {wait}s (attempt {attempt+1}/{publish_retries+1})")
                time.sleep(wait)
                continue
            raise
    raise RuntimeError(f"IG publish failed after retries: {last_err}")


# ============================================================
# Main worker
# ============================================================

def main() -> int:
    ig_token = env_str("IG_ACCESS_TOKEN")
    ig_user_id = env_str("IG_USER_ID")
    if not ig_token or not ig_user_id:
        raise RuntimeError("Missing IG_ACCESS_TOKEN or IG_USER_ID")

    max_wait_s = env_int("IG_MAX_WAIT_S", 45)
    poll_s = env_int("IG_POLL_S", 3)

    ss = _open_sheet()
    ws_raw = ss.worksheet(RAW_DEALS_TAB)
    ws_phrase = ss.worksheet(PHRASE_BANK_TAB)

    raw_headers = _ensure_headers(ws_raw, REQUIRED_RAW_COLS)
    phrase_headers = _ensure_headers(ws_phrase, PHRASE_BANK_REQUIRED_COLS)
    hmap = _header_map(raw_headers)

    # Read all rows (values) so we can keep exact row indexes
    all_values = ws_raw.get_all_values()
    if len(all_values) < 2:
        log("No rows to publish.")
        return 0

    posted = 0

    # Find first eligible row:
    # status == READY_TO_PUBLISH, graphic_url present, not posted yet
    eligible_row_index: Optional[int] = None
    row_data: Optional[Dict[str, str]] = None

    for r in range(2, len(all_values) + 1):
        d = _row_dict_from_values(raw_headers, all_values[r - 1])
        status = (d.get("status") or "").strip()
        graphic_url = (d.get("graphic_url") or "").strip()
        already = (d.get("posted_instagram_at") or "").strip()

        if status == "READY_TO_PUBLISH" and graphic_url and not already:
            eligible_row_index = r
            row_data = d
            break

    if not eligible_row_index or not row_data:
        log("No eligible rows (READY_TO_PUBLISH with graphic_url).")
        return 0

    r = eligible_row_index
    d = row_data

    deal_id = (d.get("deal_id") or "").strip()
    to_city = (d.get("destination_city") or "").strip()
    from_city = (d.get("origin_city") or "").strip()
    country = (d.get("destination_country") or "").strip()
    price_gbp = (d.get("price_gbp") or "").strip()
    out_d = (d.get("outbound_date") or "").strip()
    back_d = (d.get("return_date") or "").strip()
    graphic_url = (d.get("graphic_url") or "").strip()

    # Phrase: use RAW_DEALS.phrase_bank if present; else pick and write-back
    phrase = (d.get("phrase_bank") or "").strip()
    theme = (d.get("deal_theme") or d.get("theme") or "").strip()

    if not phrase:
        phrase = pick_phrase(ws_phrase, theme=theme)
        if phrase:
            _cell(ws_raw, r, hmap["phrase_bank"], phrase)
            log(f"Phrase selected and written back to RAW_DEALS.phrase_bank (deal_id={deal_id})")
        else:
            log("No approved phrase found; continuing without phrase.")

    caption = build_caption_ig(country, to_city, from_city, price_gbp, out_d, back_d, phrase)

    try:
        log(f"📸 IG publishing row {r} deal_id={deal_id} ({from_city} -> {to_city})")
        _ = ig_create_and_publish(
            ig_user_id=ig_user_id,
            access_token=ig_token,
            image_url=graphic_url,
            caption=caption,
            max_wait_s=max_wait_s,
            poll_s=poll_s,
            publish_retries=2,
        )

        # Success -> update sheet
        _cell(ws_raw, r, hmap["posted_instagram_at"], now_utc_iso())
        _cell(ws_raw, r, hmap["status"], "POSTED_INSTAGRAM")
        _cell(ws_raw, r, hmap["publish_error"], "")
        _cell(ws_raw, r, hmap["publish_error_at"], "")
        posted += 1

        log(f"✅ IG posted 1 (deal_id={deal_id})")

    except Exception as e:
        err = str(e)
        log(f"❌ IG publish failed: {err}")

        # Do not mutate status on failure; log error fields
        if "publish_error" in hmap:
            _cell(ws_raw, r, hmap["publish_error"], err[:480])
        if "publish_error_at" in hmap:
            _cell(ws_raw, r, hmap["publish_error_at"], now_utc_iso())

        return 1

    log(f"Done. IG posted {posted}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
