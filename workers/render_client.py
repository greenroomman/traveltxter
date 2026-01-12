#!/usr/bin/env python3
"""
workers/instagram_publisher.py

LOCKED SYSTEM RULES:
- Consume: status == READY_TO_PUBLISH
- Require: graphic_url present AND publicly fetchable (HTTP 200 + image/*)
- Produce: status -> POSTED_INSTAGRAM
- If graphic_url is broken (404/500/etc): mark publish_error + set status -> READY_TO_POST and clear graphic_url (so render can regenerate)

This prevents the pipeline getting stuck on one bad row forever.
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


# =========================
# Logging
# =========================

def now_utc_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def log(msg: str) -> None:
    print(f"{now_utc_iso()} | {msg}", flush=True)


def env_str(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()


# =========================
# Sheets auth
# =========================

def _parse_sa_json(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()
    try:
        return json.loads(raw)
    except Exception:
        return json.loads(raw.replace("\\n", "\n"))


def get_client() -> gspread.Client:
    raw = env_str("GCP_SA_JSON_ONE_LINE") or env_str("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")

    info = _parse_sa_json(raw)
    creds = Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


def ensure_columns(ws: gspread.Worksheet, headers: List[str], required: List[str]) -> List[str]:
    missing = [c for c in required if c not in headers]
    if not missing:
        return headers
    new_headers = headers + missing
    ws.update([new_headers], "A1")
    log(f"🛠️ Added missing columns: {missing}")
    return new_headers


def safe_get(row: List[str], idx: int) -> str:
    if idx < 0:
        return ""
    if idx >= len(row):
        return ""
    return (row[idx] or "").strip()


def a1(col0: int, row1: int) -> str:
    # 0-based col -> A1
    col = col0 + 1
    s = ""
    while col:
        col, r = divmod(col - 1, 26)
        s = chr(65 + r) + s
    return f"{s}{row1}"


# =========================
# URL preflight + repair
# =========================

def _normalize_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    # Add scheme if missing
    if u.startswith("greenroomman.pythonanywhere.com/"):
        u = "https://" + u
    if u.startswith("www.") or u.startswith("pythonanywhere.com/"):
        u = "https://" + u
    if not u.startswith("http://") and not u.startswith("https://"):
        # safest default
        u = "https://" + u.lstrip("/")
    # Remove accidental double slashes after domain
    u = u.replace("://", "§§§").replace("//", "/").replace("§§§", "://")
    return u


def preflight_public_image(url: str) -> None:
    url = _normalize_url(url)
    headers = {"User-Agent": "traveltxter-ig-preflight/1.0"}
    r = requests.get(url, stream=True, timeout=30, headers=headers, allow_redirects=True)

    if r.status_code != 200:
        snippet = ""
        try:
            snippet = (r.text or "")[:220].replace("\n", " ")
        except Exception:
            snippet = ""
        raise RuntimeError(f"graphic_url not fetchable (HTTP {r.status_code}) :: {snippet}")

    ctype = (r.headers.get("Content-Type") or "").lower().strip()
    if not ctype.startswith("image/"):
        raise RuntimeError(f"graphic_url not an image (Content-Type={ctype})")


def preflight_and_repair_image_url(url: str) -> str:
    """
    Try a couple of harmless repairs before failing.
    Returns a working URL or raises RuntimeError.
    """
    candidates = []
    u = (url or "").strip()
    if u:
        candidates.append(u)
        candidates.append(_normalize_url(u))

    # Common repair: sometimes sheets store without scheme or with extra slashes
    # (candidates already cover this)

    last_err: Optional[Exception] = None
    for c in candidates:
        try:
            preflight_public_image(c)
            return _normalize_url(c)
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(str(last_err) if last_err else "Unknown preflight failure")


# =========================
# Caption builder (simple, locked style)
# =========================

def build_caption_ig(
    country: str,
    to_city: str,
    from_city: str,
    price_gbp: str,
    out_d: str,
    back_d: str,
    phrase: str,
) -> str:
    # Keep this plain and deterministic (no “AI tone”)
    country = (country or "").strip()
    to_city = (to_city or "").strip()
    from_city = (from_city or "").strip()

    price = (price_gbp or "").strip()
    if not price.startswith("£"):
        price = "£" + price.replace("Â£", "").replace("£", "").strip()

    # Dates shown as ISO if present (already stored that way in sheet)
    out_d = (out_d or "").strip()
    back_d = (back_d or "").strip()

    # Phrase bank is optional
    phrase = (phrase or "").strip()

    lines = [
        f"{price} to {to_city}" + (f", {country}" if country else ""),
        "",
        f"TO: {to_city.upper()}",
        f"FROM: {from_city}",
        f"OUT: {out_d}",
        f"BACK: {back_d}",
    ]
    if phrase:
        lines += ["", phrase]

    return "\n".join(lines).strip()


# =========================
# Instagram Graph API
# =========================

def ig_create_and_publish(ig_user_id: str, access_token: str, image_url: str, caption: str) -> str:
    """
    Creates media + publishes it. Returns creation_id.
    """
    api_base = "https://graph.facebook.com/v19.0"

    # 1) Create media container
    create_url = f"{api_base}/{ig_user_id}/media"
    payload = {
        "image_url": image_url,
        "caption": caption,
        "access_token": access_token,
    }
    r1 = requests.post(create_url, data=payload, timeout=60)
    j1 = r1.json() if r1.content else {}
    if r1.status_code >= 400 or "id" not in j1:
        raise RuntimeError(f"IG /media failed: {j1}")

    creation_id = j1["id"]

    # 2) Publish (with retries for “not ready yet”)
    publish_url = f"{api_base}/{ig_user_id}/media_publish"
    pub_payload = {"creation_id": creation_id, "access_token": access_token}

    last_err = None
    for attempt in range(1, 8):
        r2 = requests.post(publish_url, data=pub_payload, timeout=60)
        j2 = r2.json() if r2.content else {}

        if r2.status_code < 400 and "id" in j2:
            return creation_id

        last_err = j2
        time.sleep(3 * attempt)

    raise RuntimeError(f"IG /media_publish failed after retries: {last_err}")


# =========================
# Main
# =========================

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
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(tab)

    values = ws.get_all_values()
    if len(values) < 2:
        log("No rows.")
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

    # ✅ NEW BEHAVIOUR:
    # Scan READY_TO_PUBLISH rows and pick the first one with a WORKING image URL.
    # If image URL is broken, mark row to be rerendered and continue scanning.
    target: Optional[Tuple[int, List[str], str]] = None  # (rownum, row_values, working_url)

    for rownum, r in enumerate(rows, start=2):
        if safe_get(r, h["status"]).strip() != "READY_TO_PUBLISH":
            continue

        raw_url = safe_get(r, h["graphic_url"])
        if not raw_url:
            continue

        try:
            working_url = preflight_and_repair_image_url(raw_url)

            # If repaired, write back the working URL
            if working_url != raw_url:
                ws.update([[working_url]], a1(h["graphic_url"], rownum))

            target = (rownum, r, working_url)
            break

        except Exception as e:
            # Mark this row so render_client can regenerate a fresh URL
            msg = f"IG preflight failed (rerender): {str(e)[:220]}"
            ws.batch_update(
                [
                    {"range": a1(h["publish_error"], rownum), "values": [[msg]]},
                    {"range": a1(h["publish_error_at"], rownum), "values": [[now_utc_iso()]]},
                    {"range": a1(h["graphic_url"], rownum), "values": [[""]]},
                    {"range": a1(h["status"], rownum), "values": [["READY_TO_POST"]]},
                ],
                value_input_option="USER_ENTERED",
            )
            log(f"⚠️ Skipped row {rownum} (bad graphic_url) -> set READY_TO_POST")
            continue

    if not target:
        log("No eligible rows with a working graphic_url.")
        return 0

    rownum, r, image_url_working = target
    deal_id = safe_get(r, h["deal_id"])

    # Phrase (optional; already stored by scorer/render steps normally)
    phrase = safe_get(r, h["phrase_bank"])

    caption = build_caption_ig(
        country=safe_get(r, h["destination_country"]),
        to_city=safe_get(r, h["destination_city"]),
        from_city=safe_get(r, h["origin_city"]),
        price_gbp=safe_get(r, h["price_gbp"]),
        out_d=safe_get(r, h["outbound_date"]),
        back_d=safe_get(r, h["return_date"]),
        phrase=phrase,
    )

    log(f"📸 IG posting row {rownum} deal_id={deal_id}")

    try:
        ig_create_and_publish(
            ig_user_id=ig_user_id,
            access_token=ig_access_token,
            image_url=image_url_working,
            caption=caption,
        )
    except Exception as e:
        msg = f"IG publish failed: {str(e)[:240]}"
        ws.batch_update(
            [
                {"range": a1(h["publish_error"], rownum), "values": [[msg]]},
                {"range": a1(h["publish_error_at"], rownum), "values": [[now_utc_iso()]]},
            ],
            value_input_option="USER_ENTERED",
        )
        raise

    # Success -> mark posted
    ws.batch_update(
        [
            {"range": a1(h["posted_instagram_at"], rownum), "values": [[now_utc_iso()]]},
            {"range": a1(h["status"], rownum), "values": [["POSTED_INSTAGRAM"]]},
            {"range": a1(h["publish_error"], rownum), "values": [[""]]},
        ],
        value_input_option="USER_ENTERED",
    )

    log(f"✅ IG posted row {rownum} deal_id={deal_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
