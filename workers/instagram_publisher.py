# workers/instagram_publisher.py
# FINAL HARDENED VERSION — RDV SAFE / SCHEMA TOLERANT (FULL REPLACEMENT)
#
# LOCKED CONSTRAINTS (DO NOT VIOLATE):
# - DO NOT change caption wording or structure
# - DO NOT change graphic usage (graphic_url)
# - Instagram is marketing-only
# - MUST timestamp posted_instagram_at in RAW_DEALS
# - MUST be idempotent
# - MUST enforce instagram_ok gate (when available)
#
# This version:
# - Prefers reading from RAW_DEALS_VIEW (RDV)
# - Falls back safely to RAW_DEALS if RDV is missing required columns
# - Tolerates missing optional columns (block_reason, posted_instagram_at in RDV)
# - Never hard-fails purely due to RDV schema mismatch (marketing-only must be fail-soft)
# - Writes timestamps to RAW_DEALS only after successful IG publish
# - Posts at most one per run

import os
import json
import time
import datetime as dt
import requests
import gspread
from google.oauth2.service_account import Credentials


# ------------------------
# Utilities
# ------------------------

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


def _build_header_map(headers):
    return {str(k).strip(): i for i, k in enumerate(headers)}


def _get_cell(row, h, key, default=""):
    i = h.get(key)
    if i is None:
        return default
    if i >= len(row):
        return default
    return (row[i] or "").strip()


def _truthy(v: str) -> bool:
    return (v or "").strip().upper() == "TRUE"


# ------------------------
# Main
# ------------------------

def main():
    gc = gspread.authorize(_sa_creds())

    sheet_id = env("SPREADSHEET_ID") or env("SHEET_ID")
    if not sheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")

    sh = gc.open_by_key(sheet_id)

    # Always require RAW_DEALS (sole writable source)
    ws_raw = sh.worksheet("RAW_DEALS")

    # Try RDV first (read-only view)
    ws_view = None
    try:
        ws_view = sh.worksheet("RAW_DEALS_VIEW")
    except Exception:
        ws_view = None

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
    raw_h = _build_header_map(raw_headers)

    # Must exist in RAW_DEALS
    if "deal_id" not in raw_h:
        raise RuntimeError("RAW_DEALS missing required column: deal_id")
    if "posted_instagram_at" not in raw_h:
        raise RuntimeError("RAW_DEALS missing required column: posted_instagram_at")

    # Fast index: deal_id -> (row_number, posted_instagram_at)
    raw_index = {}
    for row_num, raw_r in enumerate(raw_vals[1:], start=2):
        did = _get_cell(raw_r, raw_h, "deal_id", "")
        if did:
            raw_index[did] = (row_num, _get_cell(raw_r, raw_h, "posted_instagram_at", ""))

    # Pull RDV values if available; otherwise fall back to RAW_DEALS data
    values = None
    headers = None
    h = {}

    source = "RAW_DEALS_VIEW"
    if ws_view is not None:
        try:
            values = ws_view.get_all_values()
        except Exception:
            values = None

    if not values or len(values) < 2:
        # RDV not available or empty -> fall back
        source = "RAW_DEALS"
        values = raw_vals
        headers = raw_headers
        h = raw_h
    else:
        headers = values[0]
        h = _build_header_map(headers)

    # Required fields to build the IG post (prefer RDV; fall back handled by source swap)
    required_keys = [
        "deal_id",
        "status",
        "graphic_url",
        "destination_country",
        "destination_city",
        "origin_city",
        "price_gbp",
        "outbound_date",
        "return_date",
    ]

    missing_required = [k for k in required_keys if k not in h]
    if missing_required:
        # Marketing-only: fail-soft. Attempt fallback to RAW_DEALS if we were on RDV.
        if source == "RAW_DEALS_VIEW":
            print(f"⚠️ RDV schema mismatch (missing {missing_required}). Falling back to RAW_DEALS (fail-soft).")
            source = "RAW_DEALS"
            values = raw_vals
            headers = raw_headers
            h = raw_h
            missing_required = [k for k in required_keys if k not in h]

        # If still missing in RAW_DEALS, we cannot construct a compliant IG post
        if missing_required:
            print(f"⛔ Cannot publish to Instagram — missing required columns in RAW_DEALS: {missing_required}")
            return 0

    # Gate fields (must enforce when available)
    has_instagram_ok = "instagram_ok" in h
    has_block_reason = "block_reason" in h

    print("=" * 70)
    print("📣 Instagram Publisher — RDV Gate Enforced")
    print(f"SOURCE: {source}")
    print("=" * 70)

    published = 0
    blocked = 0
    ready = 0
    skipped_missing_gate = 0

    # Iterate candidates from chosen source
    for row_idx, r in enumerate(values[1:], start=2):
        if _get_cell(r, h, "status", "") != "READY_TO_PUBLISH":
            continue

        ready += 1

        # Enforce instagram_ok if present; if absent, fail-soft (do not post), but do not crash.
        if has_instagram_ok:
            if not _truthy(_get_cell(r, h, "instagram_ok", "")):
                blocked += 1
                if has_block_reason:
                    print(
                        f"⛔ BLOCKED {_get_cell(r,h,'deal_id','')} — "
                        f"{_get_cell(r,h,'destination_city','')} £{_get_cell(r,h,'price_gbp','')} — "
                        f"{_get_cell(r,h,'block_reason','')}"
                    )
                else:
                    print(
                        f"⛔ BLOCKED {_get_cell(r,h,'deal_id','')} — "
                        f"{_get_cell(r,h,'destination_city','')} £{_get_cell(r,h,'price_gbp','')} — "
                        f"instagram_ok != TRUE"
                    )
                continue
        else:
            skipped_missing_gate += 1
            print(
                f"⚠️ SKIP (fail-soft) {_get_cell(r,h,'deal_id','')} — "
                f"RDV missing instagram_ok gate column"
            )
            continue

        deal_id = _get_cell(r, h, "deal_id", "")
        if not deal_id:
            continue

        # Idempotency check (RAW_DEALS index)
        raw_hit = raw_index.get(deal_id)
        if not raw_hit:
            # Deal not found in RAW_DEALS — should not happen, but fail-soft
            print(f"⚠️ SKIP (fail-soft) — deal_id not found in RAW_DEALS: {deal_id}")
            continue

        raw_row_num, posted_ts = raw_hit
        if posted_ts:
            print(f"↩️  Skipping already posted: {deal_id}")
            continue

        # Construct IG content
        country = _get_cell(r, h, "destination_country", "")
        city = _get_cell(r, h, "destination_city", "")
        price = _get_cell(r, h, "price_gbp", "")
        outbound = _get_cell(r, h, "outbound_date", "")
        ret = _get_cell(r, h, "return_date", "")
        phrase = phrase_from_row(dict(zip(headers, r)))
        image_url = _get_cell(r, h, "graphic_url", "")

        flag = get_country_flag(country)

        # DO NOT change caption wording or structure (locked)
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

        # Publish to Instagram (fail-hard only on credential/config; per-row publish failures are fail-soft)
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
            print(f"⛔ IG create failed (fail-soft) for {deal_id}: {create}")
            continue

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
            print(f"⛔ IG publish failed (fail-soft) for {deal_id}: {pub}")
            continue

        # Stamp RAW_DEALS only after successful publish (locked requirement)
        ws_raw.update_cell(
            raw_row_num,
            raw_h["posted_instagram_at"] + 1,
            dt.datetime.utcnow().isoformat() + "Z",
        )

        print(f"✅ Published {deal_id} — {city} £{price}")
        published += 1
        return 0  # one post per run (locked behavior)

    print("=" * 70)
    print(f"READY_TO_PUBLISH: {ready}")
    print(f"BLOCKED BY GATE: {blocked}")
    if skipped_missing_gate:
        print(f"SKIPPED (missing instagram_ok column): {skipped_missing_gate}")
    print(f"PUBLISHED THIS RUN: {published}")
    print("=" * 70)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
