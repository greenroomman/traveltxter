#!/usr/bin/env python3
import os
import json
import math
import datetime as dt
from typing import Any, Dict, List, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials


def now_utc() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def log(msg: str) -> None:
    print(f"{now_utc()} | {msg}", flush=True)

def env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()

def clean_url(u: str) -> str:
    # remove ALL whitespace that can break URLs in captions
    return "".join((u or "").split())

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

def to_ddmmyy(date_iso: str) -> str:
    try:
        d = dt.date.fromisoformat(str(date_iso).strip())
        return d.strftime("%d%m%y")
    except Exception:
        return str(date_iso).strip()

def money_2dp(x: Any) -> str:
    try:
        return f"£{float(x):.2f}"
    except Exception:
        return ""

def build_locked_caption(row: Dict[str, str]) -> str:
    # Inputs (fallbacks to your “real” columns)
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
        f"TO: {to_city.upper()}",
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
        "Join TravelTxter Nomad",
        "for £7.99 / month:",
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


def get_client() -> gspread.Client:
    sa = env("GCP_SA_JSON_ONE_LINE")
    info = json.loads(sa)
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)


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
    headers = values[0]
    rows = values[1:]
    hmap = {h: i for i, h in enumerate(headers)}

    def cell(row: List[str], key: str) -> str:
        i = hmap.get(key, -1)
        return row[i].strip() if i >= 0 and i < len(row) else ""

    # Find one READY_TO_PUBLISH row with graphic_url
    target_idx = None
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

    # Build row dict for template
    rowdict: Dict[str, str] = {h: (r[i] if i < len(r) else "") for h, i in hmap.items()}

    image_url = rowdict.get("graphic_url", "").strip()
    caption = build_locked_caption(rowdict)

    log(f"📸 Posting IG for row {target_idx}")

    # Create media container
    create_url = f"https://graph.facebook.com/v20.0/{ig_user}/media"
    resp = requests.post(create_url, data={
        "image_url": image_url,
        "caption": caption,
        "access_token": ig_token,
    }, timeout=60)
    j = resp.json()
    if "id" not in j:
        raise RuntimeError(f"IG create failed: {j}")

    creation_id = j["id"]

    # Publish media container
    publish_url = f"https://graph.facebook.com/v20.0/{ig_user}/media_publish"
    resp2 = requests.post(publish_url, data={
        "creation_id": creation_id,
        "access_token": ig_token,
    }, timeout=60)
    j2 = resp2.json()
    if "id" not in j2:
        raise RuntimeError(f"IG publish failed: {j2}")

    media_id = j2["id"]

    # Update sheet (status + timestamps)
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
    if "ig_published_timestamp" in hmap:
        updates.append({"range": a1(target_idx, hmap["ig_published_timestamp"]), "values": [[now_utc()]]})

    if updates:
        ws.batch_update(updates)

    log(f"✅ IG posted row {target_idx} -> media_id={media_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
