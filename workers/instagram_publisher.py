#!/usr/bin/env python3
"""
TravelTxter V4.5x — instagram_publisher.py (LOCKED)

ROLE:
- Consumes: status == READY_TO_PUBLISH
- Requires: graphic_url (publicly accessible URL)
- Creates IG media container + publishes
- Writes: posted_instagram_at
- Promotes status -> POSTED_INSTAGRAM

Rules:
- Caption template is LOCKED (flags only, no other emojis)
- No markdown lists
- Robust retries for "Media not ready" (error code 9007/subcode 2207027)
- Hardened Google SA JSON parsing + Sheets 429 backoff
"""

from __future__ import annotations

import os
import json
import time
import datetime as dt
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
# Caption template (LOCKED)
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

def build_caption(
    price_gbp: str,
    destination_country: str,
    destination_city: str,
    origin_city: str,
    outbound_date: str,
    return_date: str,
    stripe_monthly: str,
    stripe_yearly: str,
) -> str:
    flag = country_flag(destination_country)
    dest_upper = (destination_city or "").strip().upper()

    lines = [
        f"£{price_gbp} to {destination_country}{(' ' + flag) if flag else ''}".strip(),
        f"TO: {dest_upper}",
        f"FROM: {origin_city}",
        f"OUT: {outbound_date}",
        f"BACK: {return_date}",
        "",
        "Heads up:",
        "• VIP members saw this 24 hours ago",
        "• Availability is running low",
        "• Best deals go to VIPs first",
        "",
        "Want instant access?",
        "Join TravelTxter Nomad for £7.99 / month:",
        "",
        "• Deals 24 hours early",
        "• Direct booking links",
        "• Exclusive mistake fares",
        "• Cancel anytime",
        "",
        f"Upgrade now (Monthly): {stripe_monthly}".strip(),
        f"Upgrade now (Yearly): {stripe_yearly}".strip(),
    ]

    # remove upgrade lines if links missing
    out: List[str] = []
    for ln in lines:
        if ln.startswith("Upgrade now (Monthly):") and ln.endswith(":"):
            continue
        if ln.startswith("Upgrade now (Yearly):") and ln.endswith(":"):
            continue
        out.append(ln)

    return "\n".join(out).strip()


# ============================================================
# Instagram Graph API
# ============================================================

def ig_create_container(graph_version: str, ig_user_id: str, token: str, image_url: str, caption: str) -> str:
    url = f"https://graph.facebook.com/{graph_version}/{ig_user_id}/media"
    r = requests.post(
        url,
        data={
            "image_url": image_url,
            "caption": caption,
            "access_token": token,
        },
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
        data={
            "creation_id": creation_id,
            "access_token": token,
        },
        timeout=60,
    )
    j = r.json()
    if "id" not in j:
        raise RuntimeError(f"IG publish failed: {j}")
    return j["id"]


def ig_publish_with_retries(
    graph_version: str,
    ig_user_id: str,
    token: str,
    creation_id: str,
    attempts: int = 10,
) -> str:
    """
    Handles "Media not ready" (9007 / 2207027).
    """
    delay = 4.0
    last_err: Optional[str] = None

    for i in range(1, attempts + 1):
        try:
            return ig_publish(graph_version, ig_user_id, token, creation_id)
        except Exception as e:
            msg = str(e)
            last_err = msg

            # Meta "not ready" signature
            if "2207027" in msg or "Media ID is not available" in msg or "not ready" in msg.lower():
                log(f"⏳ IG media not ready. Retry {i}/{attempts} in {int(delay)}s...")
                time.sleep(delay)
                delay = min(delay * 1.6, 45.0)
                continue

            raise

    raise RuntimeError(f"IG publish failed after retries: {last_err}")


# ============================================================
# Main
# ============================================================

def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")

    ig_token = env_str("IG_ACCESS_TOKEN")
    ig_user_id = env_str("IG_USER_ID")
    graph_version = env_str("GRAPH_API_VERSION", "v20.0")

    stripe_monthly = env_str("STRIPE_MONTHLY_LINK")
    stripe_yearly = env_str("STRIPE_YEARLY_LINK")

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
        "price_gbp",
        "destination_country",
        "destination_city",
        "origin_city",
        "outbound_date",
        "return_date",
        "posted_instagram_at",
    ]
    headers = ensure_columns(ws, headers, required_cols)

    # Re-read once after header mutation
    values = ws.get_all_values()
    headers = [h.strip() for h in values[0]]
    rows = values[1:]
    h = {name: i for i, name in enumerate(headers)}

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

        caption = build_caption(
            price_gbp=safe_get(r, h["price_gbp"]),
            destination_country=safe_get(r, h["destination_country"]),
            destination_city=safe_get(r, h["destination_city"]),
            origin_city=safe_get(r, h["origin_city"]),
            outbound_date=safe_get(r, h["outbound_date"]),
            return_date=safe_get(r, h["return_date"]),
            stripe_monthly=stripe_monthly,
            stripe_yearly=stripe_yearly,
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
