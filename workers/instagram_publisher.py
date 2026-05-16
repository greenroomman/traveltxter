# workers/instagram_publisher.py
# V6 Safe — Instagram Publisher (single-image compatible)
#
# Purpose:
# - Publish rendered TravelTxter images to Instagram
#
# Reads:
# - OPS_MASTER!B2 for theme of the day
# - RAW_DEALS as source of truth
#
# Publishes:
# - Single image posts using RAW_DEALS.graphic_url
#
# Writes to RAW_DEALS only:
# - posted_instagram_at
# - instagram_media_id if column exists
# - status -> POSTED_INSTAGRAM when publish succeeds
# - publish_error / publish_error_at if columns exist
#
# Contract:
# - Only publishes rows with status = READY_TO_PUBLISH
# - Requires graphic_url to be populated and publicly reachable
# - Does not post broken image URLs
# - Keeps the current single-image Graph API flow
# - Carousel support should be added later as a separate contract change

from __future__ import annotations

import os
import re
import json
import time
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials


# ------------------ env helpers ------------------

def env(k: str, d: str = "") -> str:
    return (os.getenv(k, d) or "").strip()

def env_int(k: str, d: int) -> int:
    try:
        return int(env(k, str(d)))
    except Exception:
        return d

def env_bool(k: str, d: bool = False) -> bool:
    v = env(k, "")
    if not v:
        return d
    return v.lower() in ("1", "true", "yes", "y", "on")


# ------------------ time helpers ------------------

def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def iso_z(t: dt.datetime) -> str:
    return t.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def parse_iso_utc(s: str) -> Optional[dt.datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        return dt.datetime.fromisoformat(s).astimezone(dt.timezone.utc)
    except Exception:
        return None

def hours_since(ts: Optional[dt.datetime], ref: dt.datetime) -> Optional[float]:
    if not ts:
        return None
    return (ref - ts).total_seconds() / 3600.0


# ------------------ robust SA JSON parsing ------------------

def _repair_private_key_newlines(raw: str) -> str:
    pat = re.compile(r'("private_key"\s*:\s*")(.+?)(")', re.DOTALL)
    m = pat.search(raw)
    if not m:
        return raw

    prefix, pk, suffix = m.group(1), m.group(2), m.group(3)
    pk_fixed = pk.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "\\n")
    pk_fixed = pk_fixed.replace("\t", "\\t")

    return raw[: m.start()] + prefix + pk_fixed + suffix + raw[m.end():]

def load_sa_info() -> Dict[str, Any]:
    raw = env("GCP_SA_JSON_ONE_LINE") or env("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")

    try:
        return json.loads(raw)
    except Exception:
        pass

    try:
        return json.loads(raw.replace("\\n", "\n"))
    except Exception:
        pass

    repaired = _repair_private_key_newlines(raw)
    try:
        return json.loads(repaired)
    except Exception:
        pass

    repaired2 = _repair_private_key_newlines(raw).replace("\\n", "\n")
    return json.loads(repaired2)

def gspread_client():
    info = load_sa_info()
    creds = Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


# ------------------ sheet helpers ------------------

def idx_map(headers: List[str]) -> Dict[str, int]:
    return {h: i for i, h in enumerate(headers)}

def row_dict(headers: List[str], row: List[str]) -> Dict[str, str]:
    return {headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))}

def get_cell(row: Dict[str, str], key: str) -> str:
    return (row.get(key) or "").strip()

def truthy(v: str) -> bool:
    return str(v or "").strip().lower() in ("true", "1", "yes", "y")

def normalize_theme(t: str) -> str:
    t = (t or "").strip().lower()
    t = t.replace(" ", "_")
    return t

def update_cell_if_present(ws, row_i: int, h: Dict[str, int], col: str, value: str) -> None:
    if col in h:
        ws.update_cell(row_i, h[col] + 1, value)


# ------------------ IG API helpers ------------------

def graph_url(version: str, path: str) -> str:
    return f"https://graph.facebook.com/{version}/{path.lstrip('/')}"

def preflight_image_url(image_url: str) -> None:
    if not image_url:
        raise RuntimeError("Missing graphic_url")

    try:
        r = requests.get(image_url, timeout=20, allow_redirects=True)
    except Exception as e:
        raise RuntimeError(f"Graphic URL preflight failed: {e}") from e

    if r.status_code != 200:
        raise RuntimeError(f"Graphic URL preflight returned HTTP {r.status_code}: {image_url}")

    ctype = (r.headers.get("content-type") or "").lower()
    if ctype and not ctype.startswith("image/"):
        raise RuntimeError(f"Graphic URL is not an image. content-type={ctype}")

def ig_create_container(version: str, ig_user_id: str, access_token: str, image_url: str, caption: str) -> str:
    r = requests.post(
        graph_url(version, f"{ig_user_id}/media"),
        data={
            "image_url": image_url,
            "caption": caption,
            "access_token": access_token,
        },
        timeout=45,
    )

    try:
        j = r.json()
    except Exception:
        raise RuntimeError(f"IG media create failed: HTTP {r.status_code} {r.text[:300]}")

    if "id" not in j:
        raise RuntimeError(f"IG media create failed: {j}")

    return j["id"]

def ig_publish_container(version: str, ig_user_id: str, access_token: str, creation_id: str) -> str:
    r = requests.post(
        graph_url(version, f"{ig_user_id}/media_publish"),
        data={
            "creation_id": creation_id,
            "access_token": access_token,
        },
        timeout=45,
    )

    try:
        j = r.json()
    except Exception:
        raise RuntimeError(f"IG publish failed: HTTP {r.status_code} {r.text[:300]}")

    if "id" not in j:
        raise RuntimeError(f"IG publish failed: {j}")

    return j["id"]


# ------------------ selection logic ------------------

def run_slot() -> str:
    s = env("RUN_SLOT", "").upper()
    return s if s in ("AM", "PM") else ""

def is_fresh_enough(row: Dict[str, str], ref: dt.datetime, max_age_hours: float) -> bool:
    if max_age_hours <= 0:
        return True

    if "is_fresh_24h" in row:
        v = get_cell(row, "is_fresh_24h")
        if v:
            return truthy(v)

    ts = parse_iso_utc(get_cell(row, "ingested_at_utc"))
    h = hours_since(ts, ref)
    return h is not None and h <= max_age_hours

def pick_candidate(
    rows: List[Tuple[int, Dict[str, str]]],
    theme_today: str,
    slot: str,
    ref: dt.datetime,
    max_age_hours: float,
) -> Optional[Tuple[int, Dict[str, str], str]]:
    """
    Select one Instagram candidate.

    Required:
    - status = READY_TO_PUBLISH
    - graphic_url populated
    - posted_instagram_at blank
    - fresh enough unless INSTAGRAM_MAX_AGE_HOURS <= 0

    Preference:
    - current slot
    - theme match
    - newest ingested row
    """

    current_slot = slot
    opposite_slot = "PM" if slot == "AM" else "AM"

    eligible = []

    for i, r in rows:
        if get_cell(r, "status") != "READY_TO_PUBLISH":
            continue
        if not get_cell(r, "graphic_url"):
            continue
        if get_cell(r, "posted_instagram_at"):
            continue
        if not is_fresh_enough(r, ref, max_age_hours):
            continue

        rt = normalize_theme(get_cell(r, "theme") or get_cell(r, "deal_theme"))
        theme_match = (rt == theme_today) if rt else True

        eligible.append((i, r, theme_match))

    if not eligible:
        return None

    def sort_key(item):
        i, r, theme_match = item
        ing = parse_iso_utc(get_cell(r, "ingested_at_utc")) or dt.datetime.min.replace(tzinfo=dt.timezone.utc)
        score_raw = get_cell(r, "score") or get_cell(r, "worthiness_score") or "0"
        try:
            score = float(str(score_raw).replace("%", "").strip() or 0)
        except Exception:
            score = 0.0

        return (
            1 if theme_match else 0,
            score,
            ing,
        )

    def publish_window_matches(r: Dict[str, str], wanted: str) -> bool:
        pw = get_cell(r, "publish_window").upper()
        return pw == wanted or pw == "BOTH" or not pw

    if current_slot:
        primary = [(i, r, m) for (i, r, m) in eligible if publish_window_matches(r, current_slot)]
        if primary:
            primary.sort(key=sort_key, reverse=True)
            i, r, m = primary[0]
            return i, r, f"primary_slot={current_slot} theme_match={m}"

        fallback = [(i, r, m) for (i, r, m) in eligible if publish_window_matches(r, opposite_slot)]
        if fallback:
            fallback.sort(key=sort_key, reverse=True)
            i, r, m = fallback[0]
            return i, r, f"rotation_fallback={opposite_slot} theme_match={m}"

    theme_hits = [(i, r, m) for (i, r, m) in eligible if m]
    if theme_hits:
        theme_hits.sort(key=sort_key, reverse=True)
        i, r, m = theme_hits[0]
        return i, r, f"fallback_theme_any theme_match={m}"

    eligible.sort(key=sort_key, reverse=True)
    i, r, m = eligible[0]
    return i, r, f"fallback_any theme_match={m}"


# ------------------ caption ------------------

def clean_price(row: Dict[str, str]) -> str:
    raw = get_cell(row, "price_gbp") or get_cell(row, "price") or ""
    raw = raw.replace("Â£", "").replace("£", "").replace(",", "").strip()
    if not raw:
        return ""
    try:
        return f"£{int(float(raw))}"
    except Exception:
        return f"£{raw}"

def build_caption(row: Dict[str, str], theme_today: str, current_slot: str) -> str:
    to_city = get_cell(row, "destination_city") or get_cell(row, "destination_iata")
    from_city = get_cell(row, "origin_city") or get_cell(row, "origin_iata")
    out_d = get_cell(row, "outbound_date")
    in_d = get_cell(row, "return_date")
    country = get_cell(row, "destination_country")
    phrase = get_cell(row, "phrase_used") or get_cell(row, "phrase_bank")
    price = clean_price(row)

    if not phrase or phrase.lower() in ("empty", "none", "null", "nan"):
        phrase = "One worth checking before the fare moves."

    headline = country.strip() if country else to_city

    lines = []
    if headline:
        lines.append(headline)

    if to_city:
        lines.append(f"To: {to_city}")
    if from_city:
        lines.append(f"From: {from_city}")
    if price:
        lines.append(f"Price: {price}")
    if out_d:
        lines.append(f"Out: {out_d}")
    if in_d:
        lines.append(f"Return: {in_d}")

    lines.append("")
    lines.append(phrase.strip())
    lines.append("")

    if current_slot == "AM":
        lines.append("AM radar. Link in bio for the live feed.")
    elif current_slot == "PM":
        lines.append("PM shortlist. Link in bio for the live feed.")
    else:
        lines.append("Link in bio for the live feed.")

    return "\n".join([x for x in lines if x is not None])


# ------------------ main ------------------

def main() -> int:
    ref = now_utc()
    slot = run_slot()

    RAW_TAB = env("RAW_DEALS_TAB", "RAW_DEALS")
    OPS_TAB = env("OPS_MASTER_TAB", "OPS_MASTER")

    ig_token = env("IG_ACCESS_TOKEN")
    ig_user_id = env("IG_USER_ID")
    version = env("GRAPH_API_VERSION", "v20.0")
    max_age_hours = float(env("INSTAGRAM_MAX_AGE_HOURS", "24") or "24")

    if not ig_token or not ig_user_id:
        raise RuntimeError("Missing IG_ACCESS_TOKEN / IG_USER_ID")

    gc = gspread_client()
    sh = gc.open_by_key(env("SPREADSHEET_ID") or env("SHEET_ID"))

    try:
        ws_ops = sh.worksheet(OPS_TAB)
        theme_today = normalize_theme(ws_ops.acell("B2").value or "")
    except Exception:
        theme_today = ""

    if not theme_today:
        theme_today = normalize_theme(env("THEME_OF_DAY", "")) or "adventure"

    ws = sh.worksheet(RAW_TAB)
    values = ws.get_all_values()

    if not values:
        raise RuntimeError(f"{RAW_TAB} is empty")

    headers = values[0]
    h = idx_map(headers)

    required = ["status", "deal_id", "graphic_url"]
    for k in required:
        if k not in h:
            raise RuntimeError(f"RAW_DEALS missing required header: {k}")

    rows: List[Tuple[int, Dict[str, str]]] = []
    for i, r in enumerate(values[1:], start=2):
        rows.append((i, row_dict(headers, r)))

    pick = pick_candidate(rows, theme_today, slot, ref, max_age_hours)

    if not pick:
        print("⚠️ No Instagram candidate found: no READY_TO_PUBLISH row with a usable graphic_url.")
        return 0

    row_i, row, reason = pick
    deal_id = get_cell(row, "deal_id")
    image_url = get_cell(row, "graphic_url")
    caption = build_caption(row, theme_today, slot)

    print("======================================================================")
    print("📣 Instagram Publisher — V6 Safe Single Image")
    print(f"TODAY_THEME: '{theme_today}' | CURRENT_SLOT: '{slot or '(none)'}'")
    print(f"SELECTED row={row_i} | deal_id={deal_id}")
    print(f"SELECTION: {reason}")
    print(f"IMAGE: {image_url}")
    print("======================================================================")

    try:
        preflight_image_url(image_url)

        dry_run = env_bool("INSTAGRAM_DRY_RUN", False)
        if dry_run:
            print("🧪 INSTAGRAM_DRY_RUN=true. Not publishing.")
            print("Caption:")
            print(caption)
            return 0

        creation_id = ig_create_container(version, ig_user_id, ig_token, image_url, caption)
        time.sleep(2)
        media_id = ig_publish_container(version, ig_user_id, ig_token, creation_id)

        print(f"✅ IG published media_id={media_id}")

        update_cell_if_present(ws, row_i, h, "posted_instagram_at", iso_z(ref))
        update_cell_if_present(ws, row_i, h, "instagram_media_id", media_id)
        update_cell_if_present(ws, row_i, h, "status", "POSTED_INSTAGRAM")
        update_cell_if_present(ws, row_i, h, "publish_error", "")
        update_cell_if_present(ws, row_i, h, "publish_error_at", "")

        return 0

    except Exception as e:
        msg = str(e)[:450]
        print(f"⛔ IG publish failed: {msg}")

        update_cell_if_present(ws, row_i, h, "publish_error", msg)
        update_cell_if_present(ws, row_i, h, "publish_error_at", iso_z(ref))

        raise


if __name__ == "__main__":
    raise SystemExit(main())
