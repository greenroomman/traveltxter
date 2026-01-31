# workers/instagram_publisher.py
# FULL REPLACEMENT — Theme-of-the-day (OPS_MASTER!B5) + Latest-first + Variety guard
#
# LOCKED:
# - Caption wording/structure unchanged
# - Uses graphic_url
# - Writes posted_instagram_at in RAW_DEALS only after successful publish
# - Idempotent (won't repost if posted_instagram_at already set)
# - Posts max 1 per run
# - RDV is read-only (never written)
#
# NEW (deterministic, spreadsheet-led):
# - TODAY_THEME source of truth: OPS_MASTER!B5
# - Candidate pool: READY_TO_PUBLISH + instagram_ok TRUE + graphic_url present + not already posted
# - Selection order:
#   1) Theme-match (dynamic_theme contains TODAY_THEME) + variety guard + newest-first (ingested_at_utc)
#   2) Any-theme + variety guard + newest-first
#   3) Any-theme + newest-first (last resort)
#
# Multi-theme support:
# - dynamic_theme may contain multiple themes: "long_haul|snow" or "long_haul, snow"
# - Theme-match checks membership in token set
#
# Variety guard:
# - Avoid repeating destination_city seen in posted_instagram_at within lookback window
# - Lookback hours: env VARIETY_LOOKBACK_HOURS (default 120)
#
# Quiet mode:
# - IG_PUBLISHER_QUIET=true logs only publish + already-posted + summary

from __future__ import annotations

import os
import json
import time
import re
import datetime as dt
from typing import Dict, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials


def env(k: str, d: str = "") -> str:
    return (os.getenv(k, d) or "").strip()


def _get_int_env(name: str, default: int) -> int:
    v = env(name)
    if not v:
        return default
    try:
        return int(float(v))
    except Exception:
        return default


def _sa_creds() -> Credentials:
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


def phrase_from_row(row: Dict[str, str]) -> str:
    return (row.get("phrase_used") or row.get("phrase_bank") or "").strip()


def get_country_flag(country: str) -> str:
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


def _parse_iso_utc(ts: str) -> Optional[dt.datetime]:
    s = (ts or "").strip()
    if not s:
        return None
    try:
        # accept "...Z" or without Z
        return dt.datetime.fromisoformat(s.replace("Z", ""))
    except Exception:
        return None


# =========================
# THEME NORMALISATION (LOCKED)
# =========================

AUTHORITATIVE_THEMES = {
    "winter_sun",
    "summer_sun",
    "beach_break",
    "snow",
    "northern_lights",
    "surf",
    "adventure",
    "city_breaks",
    "culture_history",
    "long_haul",
    "luxury_value",
    "unexpected_value",
}

THEME_ALIASES = {
    "winter sun": "winter_sun",
    "summer sun": "summer_sun",
    "beach break": "beach_break",
    "northern lights": "northern_lights",
    "city breaks": "city_breaks",
    "culture / history": "culture_history",
    "culture & history": "culture_history",
    "culture": "culture_history",
    "history": "culture_history",
    "long haul": "long_haul",
    "luxury": "luxury_value",
    "unexpected": "unexpected_value",
    # If anything "pro" leaks in, hard-map to a real palette theme
    "pro": "luxury_value",
    "pro+": "luxury_value",
    "vip pro": "luxury_value",
}


def normalize_theme(raw_theme: str | None) -> str:
    raw = (raw_theme or "").strip()
    if not raw:
        return "adventure"

    t = raw.lower().strip()
    if t in THEME_ALIASES:
        t = THEME_ALIASES[t]

    t = re.sub(r"[^a-z0-9_]+", "_", t).strip("_")
    if t in THEME_ALIASES:
        t = THEME_ALIASES[t]

    if t in AUTHORITATIVE_THEMES:
        return t

    return "adventure"


def theme_tokens(raw_theme: str | None) -> List[str]:
    """
    Return list of authoritative theme tokens from a (possibly multi) theme string.
    Supports separators: comma, pipe, semicolon.
    """
    if not raw_theme:
        return []
    parts = re.split(r"[,\|;]+", str(raw_theme))
    out: List[str] = []
    for p in parts:
        t = normalize_theme(p)
        if t in AUTHORITATIVE_THEMES:
            out.append(t)
    # de-dup while preserving order
    seen = set()
    uniq = []
    for t in out:
        if t not in seen:
            uniq.append(t)
            seen.add(t)
    return uniq


# =========================
# SELECTION LOGIC
# =========================

def _best_candidate(
    candidates: List[dict],
    posted_recent_cities: set,
    enforce_variety: bool,
) -> Optional[dict]:
    """
    candidates should already be sorted newest-first.
    """
    if not candidates:
        return None
    if not enforce_variety:
        return candidates[0]

    for c in candidates:
        city = (c.get("destination_city") or "").strip()
        if not city:
            return c
        if city not in posted_recent_cities:
            return c
    return None


def main() -> int:
    quiet = _quiet_mode()
    variety_lookback_hours = _get_int_env("VARIETY_LOOKBACK_HOURS", 120)

    gc = gspread.authorize(_sa_creds())

    sheet_id = env("SPREADSHEET_ID") or env("SHEET_ID")
    if not sheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")

    sh = gc.open_by_key(sheet_id)

    ws_view = sh.worksheet("RAW_DEALS_VIEW")
    ws_raw = sh.worksheet("RAW_DEALS")
    ws_ops = sh.worksheet("OPS_MASTER")

    # TODAY_THEME from OPS_MASTER!B5 (authoritative)
    today_theme_raw = ws_ops.acell("B5").value
    today_theme = normalize_theme(today_theme_raw)

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
    has_dynamic_theme = "dynamic_theme" in h
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

    # Required RAW columns
    for col in ("deal_id", "posted_instagram_at", "ingested_at_utc"):
        if col not in raw_h:
            raise RuntimeError(f"RAW_DEALS missing required column: {col}")

    # Build RAW maps for fast lookup
    deal_to_raw_row: Dict[str, int] = {}
    deal_to_posted_at: Dict[str, str] = {}
    deal_to_ingested_at: Dict[str, Optional[dt.datetime]] = {}

    for raw_i, raw_r in enumerate(raw_vals[1:], start=2):
        did = (raw_r[raw_h["deal_id"]] or "").strip()
        if not did:
            continue
        deal_to_raw_row[did] = raw_i
        deal_to_posted_at[did] = (raw_r[raw_h["posted_instagram_at"]] or "").strip()
        deal_to_ingested_at[did] = _parse_iso_utc(raw_r[raw_h["ingested_at_utc"]] or "")

    # Map deal_id -> destination_city (from RDV) for variety history
    deal_to_city: Dict[str, str] = {}
    for r in values[1:]:
        did = (r[h["deal_id"]] or "").strip()
        if did:
            deal_to_city[did] = (r[h["destination_city"]] or "").strip()

    # Determine posted_recent_cities (within lookback)
    now = dt.datetime.utcnow()
    cutoff = now - dt.timedelta(hours=variety_lookback_hours)
    posted_recent_cities: set = set()
    for did, posted_ts in deal_to_posted_at.items():
        if not posted_ts:
            continue
        posted_dt = _parse_iso_utc(posted_ts)
        if not posted_dt:
            continue
        if posted_dt >= cutoff:
            c = (deal_to_city.get(did) or "").strip()
            if c:
                posted_recent_cities.add(c)

    if not quiet:
        print("=" * 70)
        print("📣 Instagram Publisher — Theme-first + Latest-first + Variety Guard")
        print("SOURCE: RAW_DEALS_VIEW (read-only) + RAW_DEALS (posted/ingested)")
        print(f"TODAY_THEME (OPS_MASTER!B5): {today_theme_raw!r} -> {today_theme}")
        print(f"VARIETY_LOOKBACK_HOURS: {variety_lookback_hours}")
        print("=" * 70)

    # Build candidate list (RDV gate + not already posted + graphic_url present)
    ready = 0
    blocked = 0
    skipped_posted = 0
    missing_graphic = 0
    missing_raw = 0

    candidates: List[dict] = []
    for idx, r in enumerate(values[1:], start=2):
        status = (r[h["status"]] or "").strip()
        if status != "READY_TO_PUBLISH":
            continue

        ready += 1

        instagram_ok_raw = r[h["instagram_ok"]]
        if not _truthy_cell(instagram_ok_raw):
            blocked += 1
            if quiet:
                continue

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

        image_url = (r[h["graphic_url"]] or "").strip()
        if not image_url:
            missing_graphic += 1
            continue

        deal_id = (r[h["deal_id"]] or "").strip()
        if not deal_id:
            continue

        raw_row = deal_to_raw_row.get(deal_id)
        if not raw_row:
            missing_raw += 1
            continue

        if deal_to_posted_at.get(deal_id):
            skipped_posted += 1
            if not quiet:
                print(f"↩️  Skipping already posted: {deal_id}")
            continue

        ingested_dt = deal_to_ingested_at.get(deal_id) or dt.datetime.min

        dyn_raw = (r[h["dynamic_theme"]] or "").strip() if has_dynamic_theme else ""
        dyn_tokens = theme_tokens(dyn_raw)

        candidates.append(
            {
                "rdv_row": idx,
                "raw_row": raw_row,
                "deal_id": deal_id,
                "destination_country": (r[h["destination_country"]] or "").strip(),
                "destination_city": (r[h["destination_city"]] or "").strip(),
                "origin_city": (r[h["origin_city"]] or "").strip(),
                "price_gbp": (r[h["price_gbp"]] or "").strip(),
                "outbound_date": (r[h["outbound_date"]] or "").strip(),
                "return_date": (r[h["return_date"]] or "").strip(),
                "graphic_url": image_url,
                "dyn_raw": dyn_raw,
                "dyn_tokens": dyn_tokens,
                "ingested_dt": ingested_dt,
                "phrase": phrase_from_row(dict(zip(headers, r))),
            }
        )

    if not candidates:
        print("=" * 70)
        print(f"READY_TO_PUBLISH: {ready}")
        if not quiet:
            print(f"BLOCKED BY GATE: {blocked}")
        print(f"MISSING_GRAPHIC_URL: {missing_graphic}")
        print(f"MISSING_RAW_MATCH: {missing_raw}")
        print(f"ALREADY_POSTED_SKIPS: {skipped_posted}")
        print("PUBLISHED THIS RUN: 0")
        print("=" * 70)
        return 0

    # Newest-first sort by ingested_at_utc (fallback: rdv_row for stability)
    candidates.sort(key=lambda c: (c["ingested_dt"], -c["rdv_row"]), reverse=True)

    # Pool 1: theme-match (candidate dyn_tokens contains today_theme) if dynamic_theme exists
    if has_dynamic_theme:
        themed = [c for c in candidates if today_theme in (c["dyn_tokens"] or [])]
    else:
        themed = []

    # Select candidate deterministically with fallback ladder
    chosen = None
    chosen = _best_candidate(themed, posted_recent_cities, enforce_variety=True) if themed else None
    if chosen is None:
        chosen = _best_candidate(candidates, posted_recent_cities, enforce_variety=True)
    if chosen is None:
        chosen = _best_candidate(candidates, posted_recent_cities, enforce_variety=False)

    if not chosen:
        # Should never happen, but keep safe
        print("No candidate could be selected.")
        return 0

    # Publish chosen
    deal_id = chosen["deal_id"]
    country = chosen["destination_country"]
    city = chosen["destination_city"]
    price = chosen["price_gbp"]
    outbound = chosen["outbound_date"]
    ret = chosen["return_date"]
    phrase = chosen["phrase"]
    image_url = chosen["graphic_url"]

    flag = get_country_flag(country)

    # DO NOT CHANGE CAPTION STRUCTURE (LOCKED)
    caption = "\n".join(
        [
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
        ]
    ).strip()

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
        chosen["raw_row"],
        raw_h["posted_instagram_at"] + 1,
        dt.datetime.utcnow().isoformat() + "Z",
    )

    print(f"✅ Published {deal_id} — {city} £{price}")
    if not quiet:
        if has_dynamic_theme:
            print(f"   dynamic_theme: {chosen['dyn_raw']!r} -> tokens={chosen['dyn_tokens']} | today_theme={today_theme}")
        print(f"   picked_by: {'theme+variety+newest' if (has_dynamic_theme and chosen in themed) else 'variety+newest/fallback'}")
        if posted_recent_cities:
            print(f"   variety_blocked_recent_cities_count: {len(posted_recent_cities)}")

    # Summary (always)
    print("=" * 70)
    print(f"READY_TO_PUBLISH: {ready}")
    if not quiet:
        print(f"BLOCKED BY GATE: {blocked}")
    print(f"MISSING_GRAPHIC_URL: {missing_graphic}")
    print(f"MISSING_RAW_MATCH: {missing_raw}")
    print(f"ALREADY_POSTED_SKIPS: {skipped_posted}")
    print("PUBLISHED THIS RUN: 1")
    print("=" * 70)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
