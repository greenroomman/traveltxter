#!/usr/bin/env python3
"""
TravelTxter V4.5x — instagram_publisher.py (DEFINITIVE IG CAPTION TEMPLATE)

CAPTION MUST BE EXACTLY:

Thailand [country flag]
To: Phuket
From: London
Price: £685
Out: 2026-02-18
Return: 2026-02-28

“Quieter dates, usually easier on your wallet.”
Link in bio…

Rules:
- Flags only (no other emojis)
- Phrase is quoted in curly quotes “...”
- Pulls PHRASE_BANK by your CSV schema (theme/phrase/approved/etc)
- Phrase selection deterministic by deal_id
"""

from __future__ import annotations

import os
import json
import time
import datetime as dt
import hashlib
from typing import Dict, Any, List, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError


# ============================================================
# Logging
# ============================================================

def ts() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def log(msg: str) -> None:
    print(f"{ts()} | {msg}", flush=True)


# ============================================================
# Env helpers
# ============================================================

def env_str(k: str, default: str = "") -> str:
    return os.environ.get(k, default).strip()

def env_int(k: str, default: int) -> int:
    try:
        return int(env_str(k, str(default)))
    except Exception:
        return default


# ============================================================
# Robust SA JSON parsing
# ============================================================

def _extract_json_object(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()

    try:
        return json.loads(raw)
    except Exception:
        pass

    try:
        return json.loads(raw.replace("\\n", "\n"))
    except Exception:
        pass

    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise RuntimeError("Invalid GCP_SA_JSON_ONE_LINE: no JSON object found")

    candidate = raw[start:end + 1]

    try:
        return json.loads(candidate)
    except Exception:
        pass

    try:
        return json.loads(candidate.replace("\\n", "\n"))
    except Exception as e:
        raise RuntimeError("Invalid GCP_SA_JSON_ONE_LINE: JSON parse failed") from e


def get_client() -> gspread.Client:
    sa = env_str("GCP_SA_JSON_ONE_LINE") or env_str("GCP_SA_JSON")
    if not sa:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")

    info = _extract_json_object(sa)

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
    raise RuntimeError("Sheets quota still exceeded after retries (429). Try again shortly.")


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


# ============================================================
# Sheet helpers
# ============================================================

def ensure_columns(ws: gspread.Worksheet, headers: List[str], required: List[str]) -> List[str]:
    missing = [c for c in required if c not in headers]
    if not missing:
        return headers
    ws.update([headers + missing], "A1")
    log(f"🛠️  Added missing columns: {missing}")
    return headers + missing

def safe_get(row: List[str], idx: int) -> str:
    return row[idx].strip() if 0 <= idx < len(row) else ""


# ============================================================
# Flags + formatting
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
    return FLAG_MAP.get((country or "").strip().upper(), "")

def fmt_price_gbp(x: str) -> str:
    s = (x or "").strip().replace(",", "").replace("£", "")
    if not s:
        return ""
    try:
        v = float(s)
        if v.is_integer():
            return f"£{int(v)}"
        return f"£{v:.2f}"
    except Exception:
        return f"£{s}"

def quote_phrase(s: str) -> str:
    """
    Ensures phrase prints as: “...”
    Strips any existing straight/curly quotes.
    """
    t = (s or "").strip()
    if not t:
        return ""
    t = t.strip('"\''"“”‘’")
    return f"“{t}”"


# ============================================================
# PHRASE_BANK loader (matches your CSV schema)
# Columns expected:
# theme, category, phrase, approved, channel_hint, max_per_month, notes
# ============================================================

def _truthy(x: str) -> bool:
    v = (x or "").strip().lower()
    return v in ("true", "yes", "1", "y", "on", "enabled")

def load_phrase_bank(sh: gspread.Spreadsheet) -> List[Dict[str, str]]:
    try:
        ws = sh.worksheet("PHRASE_BANK")
    except Exception:
        return []

    values = ws.get_all_values()
    if not values or len(values) < 2:
        return []

    headers = [h.strip() for h in values[0]]
    out: List[Dict[str, str]] = []
    for r in values[1:]:
        d: Dict[str, str] = {}
        for i, h in enumerate(headers):
            d[h] = (r[i] if i < len(r) else "").strip()
        if any(d.values()):
            out.append(d)
    return out

def pick_theme_phrase(phrase_rows: List[Dict[str, str]], deal_theme: str, deal_id: str) -> str:
    th = (deal_theme or "").strip().upper()

    approved = [
        r for r in phrase_rows
        if _truthy(r.get("approved", "")) and (r.get("phrase", "").strip() != "")
    ]
    if not approved:
        return ""

    themed = [r for r in approved if (r.get("theme", "").strip().upper() == th)] if th else []
    pool = themed if themed else approved

    key = (deal_id or "no_deal_id").encode("utf-8")
    h = hashlib.md5(key).hexdigest()
    idx = int(h[:8], 16) % len(pool)
    return (pool[idx].get("phrase", "") or "").strip()


# ============================================================
# Instagram Graph API
# ============================================================

def ig_create_container(graph_version: str, ig_user_id: str, token: str, image_url: str, caption: str) -> str:
    url = f"https://graph.facebook.com/{graph_version}/{ig_user_id}/media"
    r = requests.post(
        url,
        data={"image_url": image_url, "caption": caption, "access_token": token},
        timeout=60,
    )
    j = r.json()
    if "id" not in j:
        raise RuntimeError(f"IG container create failed: {j}")
    return j["id"]

def ig_publish(graph_version: str, ig_user_id: str, token: str, creation_id: str) -> str:
    url = f"https://graph.facebook.com/{graph_version}/{ig_user_id}/media_publish"
    r = requests.post(
        url,
        data={"creation_id": creation_id, "access_token": token},
        timeout=60,
    )
    j = r.json()
    if "id" not in j:
        raise RuntimeError(f"IG publish failed: {j}")
    return j["id"]

def ig_publish_with_retries(graph_version: str, ig_user_id: str, token: str, creation_id: str, attempts: int = 10) -> str:
    delay = 4.0
    last_err: Optional[str] = None
    for i in range(1, attempts + 1):
        try:
            return ig_publish(graph_version, ig_user_id, token, creation_id)
        except Exception as e:
            msg = str(e)
            last_err = msg
            if "2207027" in msg or "Media ID is not available" in msg or "not ready" in msg.lower():
                log(f"⏳ IG media not ready. Retry {i}/{attempts} in {int(delay)}s...")
                time.sleep(delay)
                delay = min(delay * 1.6, 45.0)
                continue
            raise
    raise RuntimeError(f"IG publish failed after retries: {last_err}")


# ============================================================
# Caption template (definitive IG)
# ============================================================

def build_caption_ig(
    destination_country: str,
    destination_city: str,
    origin_city: str,
    price_gbp: str,
    outbound_date: str,
    return_date: str,
    phrase: str,
) -> str:
    country = (destination_country or "").strip()
    city = (destination_city or "").strip()
    origin = (origin_city or "").strip()

    flag = country_flag(country)
    first_line = f"{country}{(' ' + flag) if flag else ''}".strip() if country else (city or "TravelTxter deal")

    lines: List[str] = [
        first_line,
        f"To: {city or country}",
        f"From: {origin}",
        f"Price: {fmt_price_gbp(price_gbp)}",
        f"Out: {outbound_date}",
        f"Return: {return_date}",
        "",
    ]

    if phrase:
        lines.append(quote_phrase(phrase))
        lines.append("")

    lines.append("Link in bio…")
    return "\n".join(lines).strip()


# ============================================================
# Main
# ============================================================

def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")

    ig_token = env_str("IG_ACCESS_TOKEN")
    ig_user_id = env_str("IG_USER_ID")
    graph_version = env_str("GRAPH_API_VERSION", "v20.0")
    max_rows = env_int("IG_MAX_ROWS", 1)

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")
    if not ig_token or not ig_user_id:
        raise RuntimeError("Missing IG_ACCESS_TOKEN / IG_USER_ID")

    gc = get_client()
    sh = open_sheet_with_backoff(gc, spreadsheet_id)
    ws = sh.worksheet(tab)

    values = ws.get_all_values()
    if not values or len(values) < 2:
        log("No rows.")
        return 0

    headers = [h.strip() for h in values[0]]
    required_cols = [
        "status",
        "graphic_url",
        "deal_id",
        "price_gbp",
        "destination_country",
        "destination_city",
        "origin_city",
        "outbound_date",
        "return_date",
        "deal_theme",
        "posted_instagram_at",
    ]
    headers = ensure_columns(ws, headers, required_cols)

    # Re-read after header mutation
    values = ws.get_all_values()
    headers = [h.strip() for h in values[0]]
    rows = values[1:]
    h = {name: i for i, name in enumerate(headers)}

    # Phrase bank once per run
    pb = load_phrase_bank(sh)

    posted = 0
    for rownum, r in enumerate(rows, start=2):
        if posted >= max_rows:
            break

        status = safe_get(r, h["status"]).upper()
        if status != "READY_TO_PUBLISH":
            continue

        image_url = safe_get(r, h["graphic_url"])
        if not image_url:
            log(f"⏭️  Skip row {rownum}: missing graphic_url")
            continue

        deal_id = safe_get(r, h["deal_id"])
        deal_theme = safe_get(r, h["deal_theme"])
        phrase = pick_theme_phrase(pb, deal_theme, deal_id)

        caption = build_caption_ig(
            destination_country=safe_get(r, h["destination_country"]),
            destination_city=safe_get(r, h["destination_city"]),
            origin_city=safe_get(r, h["origin_city"]),
            price_gbp=safe_get(r, h["price_gbp"]),
            outbound_date=safe_get(r, h["outbound_date"]),
            return_date=safe_get(r, h["return_date"]),
            phrase=phrase,
        )

        log(f"📸 Posting IG for row {rownum}")
        creation_id = ig_create_container(graph_version, ig_user_id, ig_token, image_url, caption)
        media_id = ig_publish_with_retries(graph_version, ig_user_id, ig_token, creation_id)

        batch = [
            {"range": a1(rownum, h["posted_instagram_at"]), "values": [[ts()]]},
            {"range": a1(rownum, h["status"]), "values": [["POSTED_INSTAGRAM"]]},
        ]
        ws.batch_update(batch, value_input_option="USER_ENTERED")

        posted += 1
        log(f"✅ IG posted row {rownum} media_id={media_id}")
        time.sleep(2)

    log(f"Done. IG posted {posted}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
