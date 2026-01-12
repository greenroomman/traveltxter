#!/usr/bin/env python3
"""
workers/instagram_publisher.py

LOCKED SYSTEM RULES:
- Consume: status == READY_TO_PUBLISH
- Require: graphic_url present AND fetchable
- Require: row theme matches THEME OF THE DAY (from CONFIG or env)
- Produce: status -> POSTED_INSTAGRAM

THIS PATCH FIXES:
1) Theme gating: publish ONLY deals matching today's theme (CONFIG-driven).
2) Phrase missing: PHRASE_BANK.channel_hint is descriptive, so we DO NOT filter on it.
3) IG failures from bad graphic_url: preflight + AUTO-REPAIR common URL variants,
   including legacy PythonAnywhere /static/renders -> /renders.

IMPORTANT:
- Does NOT change rendering logic (render_engine).
- Does NOT change scorer/feeder selection logic.
- Only gates Instagram publishing to theme-of-day and improves URL resilience.
"""

from __future__ import annotations

import os
import json
import time
import datetime as dt
import hashlib
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse

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
# CONFIG: Theme of the day (LOCKED GATE)
# =========================

def _norm_theme(s: str) -> str:
    return (s or "").strip().lower().replace(" ", "_")


def load_theme_of_day(sh: gspread.Spreadsheet) -> str:
    """
    Reads today's active theme from CONFIG (preferred) or env var THEME_OF_DAY (fallback).

    We do NOT assume CONFIG schema beyond this common pattern:
      - A key/value table where first column is key and second column is value
      - OR header row contains 'key' and 'value' columns

    Keys we will accept (case-insensitive):
      - theme_of_day
      - active_theme
      - todays_theme
      - today_theme
      - theme
    """
    # 1) Try CONFIG tab
    try:
        ws = sh.worksheet("CONFIG")
        values = ws.get_all_values()
        if values and len(values) >= 2:
            headers = [h.strip().lower() for h in values[0]]
            rows = values[1:]

            # Option A: explicit key/value headers
            if "key" in headers and "value" in headers:
                k_i = headers.index("key")
                v_i = headers.index("value")
                for r in rows:
                    k = (r[k_i] if k_i < len(r) else "").strip().lower()
                    v = (r[v_i] if v_i < len(r) else "").strip()
                    if k in ("theme_of_day", "active_theme", "todays_theme", "today_theme", "theme"):
                        if v:
                            return _norm_theme(v)

            # Option B: first two columns are key/value
            for r in rows:
                if not r:
                    continue
                k = (r[0] if 0 < len(r) else "").strip().lower()
                v = (r[1] if 1 < len(r) else "").strip()
                if k in ("theme_of_day", "active_theme", "todays_theme", "today_theme", "theme"):
                    if v:
                        return _norm_theme(v)
    except Exception:
        pass

    # 2) Fallback: env var
    env_theme = env_str("THEME_OF_DAY")
    if env_theme:
        return _norm_theme(env_theme)

    return ""


# =========================
# Price normaliser (caption only)
# =========================

def normalise_gbp_price(raw: str) -> str:
    """
    Ensure caption display is exactly '£123.45' (one £ only), if numeric exists.
    """
    s = (raw or "").strip()
    if not s:
        return ""

    s2 = s.replace("£", "").strip()
    try:
        v = float(s2.replace(",", ""))
        return f"£{v:,.2f}"
    except Exception:
        s = s.replace("££", "£")
        if s.startswith("£"):
            return s
        if "£" in raw:
            return "£" + s2
        return s


# =========================
# Phrase Bank
# =========================

def _truthy(x: Any) -> bool:
    if x is True:
        return True
    if x is False or x is None:
        return False
    return str(x).strip().lower() in ("true", "yes", "1", "y", "approved")


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


def _pick_from_pool(pool: List[Dict[str, str]], deal_id: str) -> str:
    if not pool:
        return ""
    h = hashlib.md5((deal_id or "x").encode()).hexdigest()
    return (pool[int(h[:8], 16) % len(pool)].get("phrase", "") or "").strip()


def pick_phrase_from_bank(bank: List[Dict[str, str]], theme: str, deal_id: str) -> str:
    """
    IMPORTANT:
    - PHRASE_BANK.channel_hint is descriptive text, so we DO NOT filter on it.
    - 1) Try approved + theme match
    - 2) Fallback to any approved phrase
    """
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


# =========================
# Caption (LOCKED)
# =========================

def build_caption_ig(country: str, to_city: str, from_city: str, price: str, out_d: str, back_d: str, phrase: str) -> str:
    lines = [
        (country or "").strip(),
        f"To: {(to_city or '').strip()}",
        f"From: {(from_city or '').strip()}",
        f"Price: {(price or '').strip()}",
        f"Out: {(out_d or '').strip()}",
        f"Return: {(back_d or '').strip()}",
        "",
    ]

    if phrase:
        clean_phrase = (phrase or "").replace('"', "").strip()
        if clean_phrase:
            lines.append(f"“{clean_phrase}”")
            lines.append("")

    lines.append("Link in bio…")
    return "\n".join([x for x in lines if x is not None]).strip()


# =========================
# URL helpers (AUTO-REPAIR 404s)
# =========================

def _base_from_render_url(render_url: str) -> Optional[str]:
    ru = (render_url or "").strip()
    if not ru:
        return None
    p = urlparse(ru)
    if not p.scheme or not p.netloc:
        return None
    return urlunparse((p.scheme, p.netloc, "", "", "", "")).rstrip("/")


def absolutise_image_url(image_url: str) -> str:
    """
    IG must fetch this URL publicly.
    - If url starts with /... -> prefix with scheme+host from RENDER_URL
    - If url has no scheme -> prefix https://
    """
    u = (image_url or "").strip()
    if not u:
        return ""

    if u.startswith("/"):
        base = _base_from_render_url(env_str("RENDER_URL"))
        if base:
            return base + u
        return u

    p = urlparse(u)
    if not p.scheme:
        return "https://" + u.lstrip("/")

    return u


def _candidate_url_variants(url: str) -> List[str]:
    """
    If the URL 404s, try common variants.

    Safe swaps only:
    - https <-> http
    - /static/ <-> /staticfiles/
    - /static/renders/ <-> /renders/  (PythonAnywhere renderer canonical path)
    """
    u = (url or "").strip()
    if not u:
        return []

    out = [u]

    # scheme swap
    if u.startswith("https://"):
        out.append("http://" + u[len("https://"):])
    elif u.startswith("http://"):
        out.append("https://" + u[len("http://"):])

    # path swaps on each scheme variant
    more = []
    for x in out:
        if "/static/renders/" in x:
            more.append(x.replace("/static/renders/", "/renders/"))
        if "/renders/" in x:
            more.append(x.replace("/renders/", "/static/renders/"))

        if "/static/" in x:
            more.append(x.replace("/static/", "/staticfiles/"))
        if "/staticfiles/" in x:
            more.append(x.replace("/staticfiles/", "/static/"))
    out.extend(more)

    # de-dupe preserving order
    seen = set()
    uniq = []
    for x in out:
        if x not in seen:
            uniq.append(x)
            seen.add(x)
    return uniq


def preflight_and_repair_image_url(url: str) -> str:
    """
    Returns a working, public, direct image URL or raises with a clear error.
    """
    candidates = _candidate_url_variants(url)
    if not candidates:
        raise RuntimeError("graphic_url is blank after normalisation")

    last_status = None
    last_snippet = ""
    for cand in candidates:
        headers = {"User-Agent": "traveltxter-ig-preflight/1.0"}
        r = requests.get(cand, stream=True, timeout=30, headers=headers, allow_redirects=True)
        last_status = r.status_code
        if r.status_code != 200:
            try:
                last_snippet = (r.text or "")[:180].replace("\n", " ")
            except Exception:
                last_snippet = ""
            continue

        ctype = (r.headers.get("Content-Type") or "").lower().strip()
        if ctype.startswith("image/"):
            return cand

        # 200 but not image
        try:
            chunk = next(r.iter_content(chunk_size=512))
            peek = chunk[:120]
        except Exception:
            peek = b""
        raise RuntimeError(f"graphic_url not an image (Content-Type={ctype}) :: peek={peek!r}")

    raise RuntimeError(f"graphic_url not fetchable (HTTP {last_status}) :: {last_snippet}")


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

    last = None
    for attempt in range(1, 11):
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

    theme_of_day = load_theme_of_day(sh)
    if not theme_of_day:
        # Hard stop: you explicitly said published content MUST conform to theme of the day.
        raise RuntimeError("Theme-of-day is not set. Provide CONFIG theme_of_day (preferred) or env THEME_OF_DAY.")

    log(f"🎯 Theme of the day (LOCKED): {theme_of_day}")

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
            "outbound_date", "return_date", "deal_theme", "theme", "phrase_bank",
            "posted_instagram_at", "publish_error", "publish_error_at"
        ],
    )
    h = {k: i for i, k in enumerate(headers)}

    bank = load_phrase_bank(sh)

    posted_any = False
    skipped_wrong_theme = 0

    for rownum, r in enumerate(values[1:], start=2):
        if safe_get(r, h["status"]) != "READY_TO_PUBLISH":
            continue

        graphic_url_raw = safe_get(r, h["graphic_url"])
        if not graphic_url_raw:
            continue

        deal_id = safe_get(r, h["deal_id"])
        row_theme_raw = safe_get(r, h.get("deal_theme", -1)) or safe_get(r, h.get("theme", -1))
        row_theme = _norm_theme(row_theme_raw)

        # THEME GATE (LOCKED)
        if row_theme != theme_of_day:
            skipped_wrong_theme += 1
            continue

        # Phrase
        phrase = safe_get(r, h["phrase_bank"])
        if not phrase:
            phrase = pick_phrase_from_bank(bank, row_theme_raw, deal_id)
            if phrase:
                ws.update([[phrase]], a1(rownum, h["phrase_bank"]))

        # Caption price
        price_clean = normalise_gbp_price(safe_get(r, h["price_gbp"]))

        caption = build_caption_ig(
            safe_get(r, h["destination_country"]),
            safe_get(r, h["destination_city"]),
            safe_get(r, h["origin_city"]),
            price_clean,
            safe_get(r, h["outbound_date"]),
            safe_get(r, h["return_date"]),
            phrase,
        )

        try:
            # 1) absolutise if /... etc
            image_url = absolutise_image_url(graphic_url_raw)

            # 2) repair if 404 by trying safe variants
            image_url_working = preflight_and_repair_image_url(image_url)

            # 3) if repaired, write back to sheet so pipeline stays clean
            if image_url_working != graphic_url_raw:
                ws.update([[image_url_working]], a1(rownum, h["graphic_url"]))

            # 4) publish
            ig_create_and_publish(
                env_str("IG_ACCESS_TOKEN"),
                env_str("IG_USER_ID"),
                image_url_working,
                caption,
            )

        except Exception as e:
            ws.update([[str(e)[:300]]], a1(rownum, h["publish_error"]))
            ws.update([[now_utc_iso()]], a1(rownum, h["publish_error_at"]))
            raise

        ws.update([[now_utc_iso()]], a1(rownum, h["posted_instagram_at"]))
        ws.update([["POSTED_INSTAGRAM"]], a1(rownum, h["status"]))
        log(f"✅ IG posted row {rownum} (theme={row_theme})")
        posted_any = True
        return 0

    if skipped_wrong_theme:
        log(f"ℹ️ Skipped {skipped_wrong_theme} READY_TO_PUBLISH rows due to wrong theme (need theme={theme_of_day}).")

    log("No eligible rows (theme-of-day gate enforced).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
