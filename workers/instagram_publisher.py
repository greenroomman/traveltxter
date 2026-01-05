#!/usr/bin/env python3
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

def now_utc() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def log(msg: str) -> None:
    print(f"{now_utc()} | {msg}", flush=True)


# ============================================================
# Env helpers
# ============================================================

def env(key: str, default: str = "") -> str:
    return os.environ.get(key, default).strip()

def clean_url(u: str) -> str:
    # Defensively remove accidental spaces that break IG/Telegram URLs
    u = (u or "").strip()
    return u.replace(" ", "")


# ============================================================
# Caption builder (LOCKED TEMPLATE)
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
}

def money_2dp(x: Any) -> str:
    try:
        return f"£{float(x):.2f}"
    except Exception:
        return ""

def build_locked_caption(row: Dict[str, str]) -> str:
    country = (row.get("destination_country") or "").strip()
    to_city = (row.get("destination_city") or row.get("dest") or row.get("destination_iata") or "").strip()
    from_city = (row.get("origin_city") or row.get("origin") or row.get("origin_iata") or "").strip()

    out_iso = (row.get("outbound_date") or row.get("out_date") or "").strip()
    back_iso = (row.get("return_date") or row.get("ret_date") or "").strip()

    price = money_2dp(row.get("price_gbp") or row.get("price") or "")

    flag = COUNTRY_FLAG.get(country.upper(), "")
    headline = f"{price} to {country.title()}" if country else f"{price} to {to_city}"
    if flag:
        headline = f"{headline} {flag}"  # flags only

    monthly = clean_url(env("STRIPE_MONTHLY_LINK", ""))
    yearly = clean_url(env("STRIPE_YEARLY_LINK", ""))

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

    # Keep URLs on their own lines so IG never breaks them
    if monthly:
        lines.append(f"Upgrade now (Monthly): {monthly}")
    if yearly:
        lines.append(f"Upgrade now (Yearly): {yearly}")

    return "\n".join(lines).strip()


# ============================================================
# Google Sheets
# ============================================================

def get_client() -> gspread.Client:
    sa = env("GCP_SA_JSON_ONE_LINE")
    info = json.loads(sa)
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)


# ============================================================
# Instagram helpers
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

def wait_for_container_ready(creation_id: str, access_token: str) -> None:
    """
    Critical fix: IG containers are often NOT immediately publishable.
    If you publish too fast you get:
      code 9007 / subcode 2207027: "Media is not ready"
    We poll status_code until FINISHED (or ERROR / timeout).
    """
    max_wait = int(env("IG_CONTAINER_MAX_WAIT_SECONDS", "120") or "120")
    poll = float(env("IG_CONTAINER_POLL_SECONDS", "4") or "4")

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

def publish_with_retry(ig_user: str, creation_id: str, access_token: str) -> str:
    """Retries publish on 'media not ready' errors."""
    publish_url = f"https://graph.facebook.com/v20.0/{ig_user}/media_publish"
    retries = int(env("IG_PUBLISH_RETRIES", "6") or "6")

    last = None
    for attempt in range(1, retries + 1):
        j2 = ig_post(
            publish_url,
            data={"creation_id": creation_id, "access_token": access_token},
            timeout=60
        )
        last = j2
        if "id" in j2:
            return str(j2["id"])

        err = j2.get("error") or {}
        code = err.get("code")
        sub = err.get("error_subcode")
        msg = (err.get("message") or "").lower()

        not_ready = (
            code == 9007
            or sub == 2207027
            or "not ready" in msg
            or "media id is not available" in msg
        )

        if not_ready and attempt < retries:
            sleep_s = min(15, 2 * attempt)  # 2s,4s,6s... capped
            log(f"⏳ IG not ready (attempt {attempt}/{retries}). Sleeping {sleep_s}s then retry...")
            time.sleep(sleep_s)
            continue

        raise RuntimeError(f"IG publish failed: {j2}")

    raise RuntimeError(f"IG publish failed after retries: {last}")


# ============================================================
# Main
# ============================================================

def main() -> int:
    spreadsheet_id = env("SPREADSHEET_ID")
    tab = env("RAW_DEALS_TAB", "RAW_DEALS")
    ig_token = env("IG_ACCESS_TOKEN")
    ig_user = env("IG_USER_ID")

    if not (spreadsheet_id and ig_token and ig_user):
        raise RuntimeError("Missing one of: SPREADSHEET_ID, IG_ACCESS_TOKEN, IG_USER_ID")

    gc = get_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(tab)

    values = ws.get_all_values()
    if not values or len(values) < 2:
        log("Sheet empty. Nothing to post.")
        return 0

    headers = values[0]
    rows = values[1:]
    hmap = {h: i for i, h in enumerate(headers)}

    def cell(row: List[str], key: str) -> str:
        i = hmap.get(key, -1)
        return row[i].strip() if i >= 0 and i < len(row) else ""

    # Find one READY_TO_PUBLISH row with graphic_url
    target_idx: Optional[int] = None
    for idx, r in enumerate(rows, start=2):
        status = cell(r, "status").upper()
        if status != "READY_TO_PUBLISH":
            continue
        graphic_url = cell(r, "graphic_url")
        if graphic_url:
            target_idx = idx
            break

    if not target_idx:
        log("No READY_TO_PUBLISH rows with graphic_url found. Nothing to post.")
        return 0

    r = rows[target_idx - 2]
    rowdict: Dict[str, str] = {h: (r[i] if i < len(r) else "") for h, i in hmap.items()}

    image_url = rowdict.get("graphic_url", "").strip()
    caption = build_locked_caption(rowdict)

    log(f"📸 Posting IG for row {target_idx}")

    # 1) Create media container
    create_url = f"https://graph.facebook.com/v20.0/{ig_user}/media"
    j1 = ig_post(create_url, data={
        "image_url": image_url,
        "caption": caption,
        "access_token": ig_token,
    }, timeout=60)

    if "id" not in j1:
        raise RuntimeError(f"IG container create failed: {j1}")

    creation_id = str(j1["id"])

    # 2) Wait until container is FINISHED (fixes 9007)
    wait_for_container_ready(creation_id, ig_token)

    # 3) Publish (with retry)
    media_id = publish_with_retry(ig_user, creation_id, ig_token)

    # 4) Update sheet (status + timestamps)
    def col_letter(n1: int) -> str:
        s = ""
        n = n1
        while n:
            n, rr = divmod(n - 1, 26)
            s = chr(65 + rr) + s
        return s

    def a1(rownum: int, col0: int) -> str:
        return f"{col_letter(col0 + 1)}{rownum}"

    updates = []
    if "status" in hmap:
        updates.append({"range": a1(target_idx, hmap["status"]), "values": [["POSTED_INSTAGRAM"]]})
    if "ig_media_id" in hmap:
        updates.append({"range": a1(target_idx, hmap["ig_media_id"]), "values": [[media_id]]})
    if "posted_instagram_at" in hmap:
        updates.append({"range": a1(target_idx, hmap["posted_instagram_at"]), "values": [[now_utc()]]})
    elif "ig_published_timestamp" in hmap:
        # Backward-compat for older sheet columns
        updates.append({"range": a1(target_idx, hmap["ig_published_timestamp"]), "values": [[now_utc()]]})

    if updates:
        ws.batch_update(updates)

    log(f"✅ IG posted row {target_idx} -> media_id={media_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
