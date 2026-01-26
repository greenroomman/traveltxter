# workers/instagram_publisher.py
# FULL REPLACEMENT — RDV Gate Enforced, truthful logging via ig_gate_reason (optional), quiet mode supported
#
# LOCKED:
# - Caption wording/structure unchanged
# - Uses graphic_url
# - Writes posted_instagram_at in RAW_DEALS only after successful publish
# - Idempotent (won't repost if posted_instagram_at already set)
# - Posts max 1 per run
#
# IMPROVEMENTS:
# - If RDV has ig_gate_reason, logs that (not block_reason) to avoid "PASS but blocked" confusion
# - Supports quiet mode via env IG_PUBLISHER_QUIET=true (logs only publish + already-posted + summary)
# - instagram_ok supports TRUE/FALSE and 1/0

import os
import json
import time
import datetime as dt
import requests
import gspread
from google.oauth2.service_account import Credentials


def env(k, d=""):
    return (os.getenv(k, d) or "").strip()


def _sa_creds():
    raw = env("GCP_SA_JSON_ONE_LINE") or env("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
    try:
        info = json.loads(raw)
    except json.JSONDecodeError:
        info = json.loads(raw.replace("\\n", "\n"))
    return Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )


def phrase_from_row(row):
    return (row.get("phrase_used") or row.get("phrase_bank") or "").strip()


def get_country_flag(country):
    if not country:
        return "🌍"
    return {
        "Iceland": "🇮🇸",
        "Spain": "🇪🇸",
        "Portugal": "🇵🇹",
        "Greece": "🇬🇷",
        "Turkey": "🇹🇷",
        "Morocco": "🇲🇦",
        "Jordan": "🇯🇴",
        "Canada": "🇨🇦",
        "USA": "🇺🇸",
        "Indonesia": "🇮🇩",
        "Thailand": "🇹🇭",
        "Japan": "🇯🇵",
        "Australia": "🇦🇺",
        "France": "🇫🇷",
        "Italy": "🇮🇹",
        "Germany": "🇩🇪",
    }.get(country, "🌍")


def _truthy_cell(v) -> bool:
    if v is True:
        return True
    if v is False or v is None:
        return False
    s = str(v).strip().upper()
    return s in {"TRUE", "1", "YES", "Y"}


def _quiet_mode() -> bool:
    return env("IG_PUBLISHER_QUIET", "").lower() in {"1", "true", "yes", "y"}


def main():
    quiet = _quiet_mode()

    gc = gspread.authorize(_sa_creds())

    sheet_id = env("SPREADSHEET_ID") or env("SHEET_ID")
    if not sheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")

    sh = gc.open_by_key(sheet_id)

    ws_view = sh.worksheet("RAW_DEALS_VIEW")
    ws_raw = sh.worksheet("RAW_DEALS")

    values = ws_view.get_all_values()
    if len(values) < 2:
        print("No rows in RAW_DEALS_VIEW")
        return 0

    headers = values[0]
    h = {k: i for i, k in enumerate(headers)}

    # ---- REQUIRED RDV HEADERS (HARD FAIL) ----
    required = [
        "deal_id",
        "status",
        "graphic_url",
        "destination_country",
        "destination_city",
        "origin_city",
        "price_gbp",
        "outbound_date",
        "return_date",
        "instagram_ok",
    ]
    for col in required:
        if col not in h:
            raise RuntimeError(f"RAW_DEALS_VIEW missing required header: {col}")

    # ---- OPTIONAL RDV HEADERS (SOFT) ----
    has_ig_gate_reason = "ig_gate_reason" in h
    has_block_reason = "block_reason" in h

    ig_user_id = env("IG_USER_ID")
    ig_access_token = env("IG_ACCESS_TOKEN")
    api_ver = env("GRAPH_API_VERSION", "v20.0")

    if not ig_user_id or not ig_access_token:
        raise RuntimeError("Missing Instagram credentials")

    raw_vals = ws_raw.get_all_values()
    if len(raw_vals) < 2:
        print("No rows in RAW_DEALS")
        return 0

    raw_headers = raw_vals[0]
    raw_h = {k: i for i, k in enumerate(raw_headers)}

    if "deal_id" not in raw_h:
        raise RuntimeError("RAW_DEALS missing required column: deal_id")
    if "posted_instagram_at" not in raw_h:
        raise RuntimeError("RAW_DEALS missing required column: posted_instagram_at")

    if not quiet:
        print("=" * 70)
        print("📣 Instagram Publisher — RDV Gate Enforced")
        print("SOURCE: RAW_DEALS_VIEW")
        print("=" * 70)

    published = 0
    blocked = 0
    ready = 0
    skipped_posted = 0

    for idx, r in enumerate(values[1:], start=2):
        if r[h["status"]] != "READY_TO_PUBLISH":
            continue

        ready += 1

        instagram_ok_raw = r[h["instagram_ok"]]
        if not _truthy_cell(instagram_ok_raw):
            blocked += 1
            if quiet:
                continue

            # Prefer IG-specific gate reason; fallback to block_reason; else show instagram_ok raw
            reason = None
            if has_ig_gate_reason:
                reason = (r[h["ig_gate_reason"]] or "").strip()
            if not reason and has_block_reason:
                reason = (r[h["block_reason"]] or "").strip()
            if not reason:
                reason = f"instagram_ok={instagram_ok_raw}"

            print(
                f"⛔ BLOCKED {r[h['deal_id']]} — "
                f"{r[h['destination_city']]} £{r[h['price_gbp']]} — {reason}"
            )
            continue

        deal_id = r[h["deal_id"]]

        # Idempotency check (RAW_DEALS)
        for raw_i, raw_r in enumerate(raw_vals[1:], start=2):
            if raw_r[raw_h["deal_id"]] != deal_id:
                continue

            if raw_r[raw_h["posted_instagram_at"]]:
                skipped_posted += 1
                if not quiet:
                    print(f"↩️  Skipping already posted: {deal_id}")
                break

            # Publish
            country = r[h["destination_country"]]
            city = r[h["destination_city"]]
            price = r[h["price_gbp"]]
            outbound = r[h["outbound_date"]]
            ret = r[h["return_date"]]
            phrase = phrase_from_row(dict(zip(headers, r)))
            image_url = r[h["graphic_url"]]

            flag = get_country_flag(country)

            # DO NOT CHANGE CAPTION STRUCTURE (LOCKED)
            caption = "\n".join([
                f"{country} {flag}",
                "",
                f"London to {city} from £{price}",
                f"Out: {outbound}",
                f"Return: {ret}",
                "",
                phrase,
                "",
                "VIP members saw this first. We post here later, and the free channel gets it after that.",
                "",
                "Link in bio.",
            ]).strip()

            create = requests.post(
                f"https://graph.facebook.com/{api_ver}/{ig_user_id}/media",
                data={
                    "image_url": image_url,
                    "caption": caption,
                    "access_token": ig_access_token,
                },
                timeout=30,
            ).json()

            cid = create.get("id")
            if not cid:
                raise RuntimeError(f"IG create failed: {create}")

            time.sleep(2)

            pub = requests.post(
                f"https://graph.facebook.com/{api_ver}/{ig_user_id}/media_publish",
                data={
                    "creation_id": cid,
                    "access_token": ig_access_token,
                },
                timeout=30,
            ).json()

            if "id" not in pub:
                raise RuntimeError(f"IG publish failed: {pub}")

            ws_raw.update_cell(
                raw_i,
                raw_h["posted_instagram_at"] + 1,
                dt.datetime.utcnow().isoformat() + "Z",
            )

            print(f"✅ Published {deal_id} — {city} £{price}")
            published += 1
            return 0  # one post per run

    # Summary (always)
    print("=" * 70)
    print(f"READY_TO_PUBLISH: {ready}")
    if not quiet:
        print(f"BLOCKED BY GATE: {blocked}")
    print(f"ALREADY_POSTED_SKIPS: {skipped_posted}")
    print(f"PUBLISHED THIS RUN: {published}")
    print("=" * 70)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
