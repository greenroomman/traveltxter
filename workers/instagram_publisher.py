#!/usr/bin/env python3
"""
TravelTxter V4.5x — instagram_publisher.py

Purpose:
- Post ONE deal to Instagram from RAW_DEALS
- Only consumes status == READY_TO_PUBLISH
- Requires graphic_url
- Caption is LOCKED template (flags only)
- Robust: polls container status and retries publish on "not ready"

Writes (if columns exist):
- posted_instagram_at
- status -> POSTED_INSTAGRAM
(optional) ig_media_id

Env required:
- SPREADSHEET_ID
- GCP_SA_JSON_ONE_LINE
- RAW_DEALS_TAB (default RAW_DEALS)
- IG_ACCESS_TOKEN
- IG_USER_ID
- STRIPE_MONTHLY_LINK
- STRIPE_YEARLY_LINK

Env optional:
- IG_CONTAINER_MAX_WAIT_SECONDS (default 120)
- IG_CONTAINER_POLL_SECONDS (default 4)
- IG_PUBLISH_RETRIES (default 6)
"""

from __future__ import annotations

import os
import json
import time
import datetime as dt
from typing import Any, Dict, List, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# Logging
# ============================================================

def utcnow() -> dt.datetime:
    return dt.datetime.utcnow()

def ts() -> str:
    return utcnow().replace(microsecond=0).isoformat() + "Z"

def log(msg: str) -> None:
    print(f"{ts()} | {msg}", flush=True)


# ============================================================
# Env
# ============================================================

def env_str(k: str, default: str = "") -> str:
    return os.environ.get(k, default).strip()

def env_int(k: str, default: int) -> int:
    try:
        return int(env_str(k, str(default)))
    except Exception:
        return default

def clean_url(u: str) -> str:
    return (u or "").strip().replace(" ", "")


# ============================================================
# Sheets
# ============================================================

def get_client() -> gspread.Client:
    sa = env_str("GCP_SA_JSON_ONE_LINE") or env_str("GCP_SA_JSON")
    if not sa:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE (recommended) or GCP_SA_JSON")
    try:
        info = json.loads(sa)
    except json.JSONDecodeError:
        info = json.loads(sa.replace("\\n", "\n"))

    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)

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
# Locked caption template
# ============================================================

COUNTRY_FLAG = {
    "SPAIN": "🇪🇸",
    "ICELAND": "🇮🇸",
    "PORTUGAL": "🇵🇹",
    "FRANCE": "🇫🇷",
    "ITALY": "🇮🇹",
    "GREECE": "🇬🇷",
    "THAILAND": "🇹🇭",
    "JAPAN": "🇯🇵",
    "MEXICO": "🇲🇽",
    "MONTENEGRO": "🇲🇪",
}

def money_2dp(x: Any) -> str:
    try:
        return f"£{float(x):.2f}"
    except Exception:
        s = str(x or "").strip()
        if s.startswith("£"):
            return s
        return ""

def build_locked_caption(row: Dict[str, str], monthly: str, yearly: str) -> str:
    country = (row.get("destination_country") or "").strip()
    to_city = (row.get("destination_city") or row.get("destination_iata") or "").strip()
    from_city = (row.get("origin_city") or row.get("origin_iata") or "").strip()
    out_iso = (row.get("outbound_date") or "").strip()
    back_iso = (row.get("return_date") or "").strip()
    price = money_2dp(row.get("price_gbp") or "")

    flag = COUNTRY_FLAG.get(country.upper(), "")
    headline = f"{price} to {country.title()}" if country else f"{price} to {to_city}"
    if flag:
        headline = f"{headline} {flag}"  # flags only

    lines = [
        headline,
        f"TO: {(to_city or '').upper()}",
        f"FROM: {from_city}",
        f"OUT: {out_iso}",
        f"BACK: {back_iso}",
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
    ]

    if monthly:
        lines.append(f"Upgrade now (Monthly): {monthly}")
    if yearly:
        lines.append(f"Upgrade now (Yearly): {yearly}")

    return "\n".join(lines).strip()


# ============================================================
# Instagram Graph API helpers
# ============================================================

def ig_get(url: str, params: Dict[str, str], timeout: int = 60) -> Dict[str, Any]:
    r = requests.get(url, params=params, timeout=timeout)
    try:
        return r.json()
    except Exception:
        return {"_raw": r.text, "_status": r.status_code}

def ig_post(url: str, data: Dict[str, str], timeout: int = 60) -> Dict[str, Any]:
    r = requests.post(url, data=data, timeout=timeout)
    try:
        return r.json()
    except Exception:
        return {"_raw": r.text, "_status": r.status_code}

def wait_for_container_ready(creation_id: str, access_token: str, max_wait: int, poll: int) -> None:
    url = f"https://graph.facebook.com/v20.0/{creation_id}"
    deadline = time.time() + max_wait
    last = None

    while time.time() < deadline:
        j = ig_get(url, params={"fields": "status_code", "access_token": access_token})
        last = j
        status = str(j.get("status_code", "")).upper()
        if status == "FINISHED":
            return
        if status == "ERROR":
            raise RuntimeError(f"IG container ERROR: {j}")
        time.sleep(poll)

    raise RuntimeError(f"IG container not ready after {max_wait}s: {last}")

def publish_with_retry(ig_user: str, creation_id: str, access_token: str, retries: int) -> str:
    url = f"https://graph.facebook.com/v20.0/{ig_user}/media_publish"
    last = None

    for attempt in range(1, retries + 1):
        j = ig_post(url, data={"creation_id": creation_id, "access_token": access_token})
        last = j
        if "id" in j:
            return str(j["id"])

        err = j.get("error") or {}
        code = err.get("code")
        sub = err.get("error_subcode")
        msg = (err.get("message") or "").lower()

        not_ready = (
            code == 9007 or sub == 2207027 or
            "not ready" in msg or "media id is not available" in msg
        )

        if not_ready and attempt < retries:
            sleep_s = min(15, 2 * attempt)
            log(f"⏳ IG not ready (attempt {attempt}/{retries}) — sleep {sleep_s}s")
            time.sleep(sleep_s)
            continue

        raise RuntimeError(f"IG publish failed: {j}")

    raise RuntimeError(f"IG publish failed after retries: {last}")


# ============================================================
# Main
# ============================================================

def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    ig_token = env_str("IG_ACCESS_TOKEN")
    ig_user = env_str("IG_USER_ID")
    monthly = clean_url(env_str("STRIPE_MONTHLY_LINK"))
    yearly = clean_url(env_str("STRIPE_YEARLY_LINK"))

    if not (spreadsheet_id and ig_token and ig_user):
        raise RuntimeError("Missing one of: SPREADSHEET_ID, IG_ACCESS_TOKEN, IG_USER_ID")

    max_wait = env_int("IG_CONTAINER_MAX_WAIT_SECONDS", 120)
    poll = env_int("IG_CONTAINER_POLL_SECONDS", 4)
    retries = env_int("IG_PUBLISH_RETRIES", 6)

    gc = get_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(tab)

    values = ws.get_all_values()
    if not values or len(values) < 2:
        log("Sheet empty. Nothing to post.")
        return 0

    headers = [h.strip() for h in values[0]]
    rows = values[1:]
    h = {name: i for i, name in enumerate(headers)}

    required = ["status", "graphic_url", "origin_city", "destination_city", "destination_country", "outbound_date", "return_date", "price_gbp"]
    for c in required:
        if c not in h:
            raise RuntimeError(f"Missing required column in RAW_DEALS: {c}")

    # Optional columns
    posted_col = h.get("posted_instagram_at")
    igid_col = h.get("ig_media_id")

    # Find first READY_TO_PUBLISH with graphic_url
    target_rownum: Optional[int] = None
    for rownum, r in enumerate(rows, start=2):
        status = (r[h["status"]] if h["status"] < len(r) else "").strip().upper()
        if status != "READY_TO_PUBLISH":
            continue
        g = (r[h["graphic_url"]] if h["graphic_url"] < len(r) else "").strip()
        if g and "no_id.png" not in g.lower():
            target_rownum = rownum
            break

    if not target_rownum:
        log("No READY_TO_PUBLISH rows with valid graphic_url found.")
        return 0

    r = rows[target_rownum - 2]
    rowdict: Dict[str, str] = {name: (r[idx] if idx < len(r) else "") for name, idx in h.items()}

    image_url = rowdict.get("graphic_url", "").strip()
    caption = build_locked_caption(rowdict, monthly=monthly, yearly=yearly)

    log(f"📸 Posting IG for row {target_rownum}")

    # 1) Create media container
    create_url = f"https://graph.facebook.com/v20.0/{ig_user}/media"
    j1 = ig_post(create_url, data={
        "image_url": image_url,
        "caption": caption,
        "access_token": ig_token,
    })

    if "id" not in j1:
        raise RuntimeError(f"IG container create failed: {j1}")

    creation_id = str(j1["id"])

    # 2) Wait until container finished
    wait_for_container_ready(creation_id, ig_token, max_wait=max_wait, poll=poll)

    # 3) Publish (retry)
    media_id = publish_with_retry(ig_user, creation_id, ig_token, retries=retries)

    # 4) Update sheet: POSTED_INSTAGRAM + timestamp (+ media id if column exists)
    updates: List[Dict[str, Any]] = [{"range": a1(target_rownum, h["status"]), "values": [["POSTED_INSTAGRAM"]]}]
    if posted_col is not None:
        updates.append({"range": a1(target_rownum, posted_col), "values": [[ts()]]})
    if igid_col is not None:
        updates.append({"range": a1(target_rownum, igid_col), "values": [[media_id]]})

    ws.batch_update(updates)
    log(f"✅ IG posted row {target_rownum} -> media_id={media_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
