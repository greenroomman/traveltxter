# workers/instagram_publisher.py
# V6 Safe — Instagram Publisher (single-image compatible) + MIZAR caption line

from __future__ import annotations

import datetime as dt
import json
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import gspread
import requests
from google.oauth2.service_account import Credentials

MIZAR_THRESHOLD = 0.65


def env(k: str, d: str = "") -> str:
    return (os.getenv(k, d) or "").strip()


def env_bool(k: str, d: bool = False) -> bool:
    v = env(k, "")
    if not v:
        return d
    return v.lower() in ("1", "true", "yes", "y", "on")


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
    for candidate in (
        raw,
        raw.replace("\\n", "\n"),
        _repair_private_key_newlines(raw),
        _repair_private_key_newlines(raw).replace("\\n", "\n"),
    ):
        try:
            return json.loads(candidate)
        except Exception:
            pass
    raise RuntimeError("Could not parse service account JSON")


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


def idx_map(headers: List[str]) -> Dict[str, int]:
    return {h: i for i, h in enumerate(headers)}


def row_dict(headers: List[str], row: List[str]) -> Dict[str, str]:
    return {headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))}


def get_cell(row: Dict[str, str], key: str) -> str:
    return (row.get(key) or "").strip()


def truthy(v: str) -> bool:
    return str(v or "").strip().lower() in ("true", "1", "yes", "y")


def normalize_theme(t: str) -> str:
    return (t or "").strip().lower().replace(" ", "_")


def update_cell_if_present(ws, row_i: int, h: Dict[str, int], col: str, value: str) -> None:
    if col in h:
        ws.update_cell(row_i, h[col] + 1, value)


def graph_url(version: str, path: str) -> str:
    return f"https://graph.facebook.com/{version}/{path.lstrip('/')}"


def preflight_image_url(image_url: str) -> None:
    if not image_url:
        raise RuntimeError("Missing graphic_url")
    try:
        response = requests.get(image_url, timeout=20, allow_redirects=True)
    except Exception as exc:
        raise RuntimeError(f"Graphic URL preflight failed: {exc}") from exc
    if response.status_code != 200:
        raise RuntimeError(f"Graphic URL preflight returned HTTP {response.status_code}: {image_url}")
    ctype = (response.headers.get("content-type") or "").lower()
    if ctype and not ctype.startswith("image/"):
        raise RuntimeError(f"Graphic URL is not an image. content-type={ctype}")


def ig_create_container(version: str, ig_user_id: str, access_token: str, image_url: str, caption: str) -> str:
    response = requests.post(
        graph_url(version, f"{ig_user_id}/media"),
        data={"image_url": image_url, "caption": caption, "access_token": access_token},
        timeout=45,
    )
    try:
        data = response.json()
    except Exception:
        raise RuntimeError(f"IG media create failed: HTTP {response.status_code} {response.text[:300]}")
    if "id" not in data:
        raise RuntimeError(f"IG media create failed: {data}")
    return data["id"]


def ig_publish_container(version: str, ig_user_id: str, access_token: str, creation_id: str) -> str:
    response = requests.post(
        graph_url(version, f"{ig_user_id}/media_publish"),
        data={"creation_id": creation_id, "access_token": access_token},
        timeout=45,
    )
    try:
        data = response.json()
    except Exception:
        raise RuntimeError(f"IG publish failed: HTTP {response.status_code} {response.text[:300]}")
    if "id" not in data:
        raise RuntimeError(f"IG publish failed: {data}")
    return data["id"]


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
    age = hours_since(ts, ref)
    return age is not None and age <= max_age_hours


def pick_candidate(
    rows: List[Tuple[int, Dict[str, str]]],
    theme_today: str,
    slot: str,
    ref: dt.datetime,
    max_age_hours: float,
) -> Optional[Tuple[int, Dict[str, str], str]]:
    current_slot = slot
    opposite_slot = "PM" if slot == "AM" else "AM"
    eligible = []

    for i, row in rows:
        if get_cell(row, "status") != "READY_TO_PUBLISH":
            continue
        if not get_cell(row, "graphic_url"):
            continue
        if get_cell(row, "posted_instagram_at"):
            continue
        if not is_fresh_enough(row, ref, max_age_hours):
            continue

        row_theme = normalize_theme(get_cell(row, "theme") or get_cell(row, "deal_theme"))
        theme_match = row_theme == theme_today if row_theme else True
        eligible.append((i, row, theme_match))

    if not eligible:
        return None

    def sort_key(item):
        _, row, theme_match = item
        ingested = parse_iso_utc(get_cell(row, "ingested_at_utc")) or dt.datetime.min.replace(tzinfo=dt.timezone.utc)
        score_raw = get_cell(row, "score") or get_cell(row, "worthiness_score") or "0"
        try:
            score = float(str(score_raw).replace("%", "").strip() or 0)
        except Exception:
            score = 0.0
        return (1 if theme_match else 0, score, ingested)

    def publish_window_matches(row: Dict[str, str], wanted: str) -> bool:
        pw = get_cell(row, "publish_window").upper()
        return pw == wanted or pw == "BOTH" or not pw

    if current_slot:
        primary = [(i, row, match) for i, row, match in eligible if publish_window_matches(row, current_slot)]
        if primary:
            primary.sort(key=sort_key, reverse=True)
            i, row, match = primary[0]
            return i, row, f"primary_slot={current_slot} theme_match={match}"

        fallback = [(i, row, match) for i, row, match in eligible if publish_window_matches(row, opposite_slot)]
        if fallback:
            fallback.sort(key=sort_key, reverse=True)
            i, row, match = fallback[0]
            return i, row, f"rotation_fallback={opposite_slot} theme_match={match}"

    theme_hits = [(i, row, match) for i, row, match in eligible if match]
    if theme_hits:
        theme_hits.sort(key=sort_key, reverse=True)
        i, row, match = theme_hits[0]
        return i, row, f"fallback_theme_any theme_match={match}"

    eligible.sort(key=sort_key, reverse=True)
    i, row, match = eligible[0]
    return i, row, f"fallback_any theme_match={match}"


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
    """Canonical caption with optional, gate-aware MIZAR line."""
    to_city = get_cell(row, "destination_city") or get_cell(row, "destination_iata")
    from_city = get_cell(row, "origin_city") or get_cell(row, "origin_iata")
    out_d = get_cell(row, "outbound_date")
    in_d = get_cell(row, "return_date")
    country = get_cell(row, "destination_country")
    phrase = get_cell(row, "phrase_used") or get_cell(row, "phrase_bank")
    price = clean_price(row)

    if not phrase or phrase.lower() in ("empty", "none", "null", "nan"):
        phrase = "One worth checking before the fare moves."

    mizar_line = ""
    if truthy(get_cell(row, "mizar_signal")):
        try:
            score = float(get_cell(row, "mizar_score"))
            if score >= MIZAR_THRESHOLD:
                mizar_line = f"MIZAR signal: {int(score * 100)}% probability of price rise within 7 days"
        except Exception:
            pass

    headline = country.strip() if country else to_city
    lines: List[str] = []
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

    if mizar_line:
        lines.append("")
        lines.append(mizar_line)

    lines.append("")
    if current_slot == "AM":
        lines.append("AM radar. Link in bio for the live feed.")
    elif current_slot == "PM":
        lines.append("PM shortlist. Link in bio for the live feed.")
    else:
        lines.append("Link in bio for the live feed.")

    return "\n".join(lines)


def main() -> int:
    ref = now_utc()
    slot = run_slot()
    raw_tab = env("RAW_DEALS_TAB", "RAW_DEALS")
    ops_tab = env("OPS_MASTER_TAB", "OPS_MASTER")

    ig_token = env("IG_ACCESS_TOKEN")
    ig_user_id = env("IG_USER_ID")
    version = env("GRAPH_API_VERSION", "v20.0")
    max_age_hours = float(env("INSTAGRAM_MAX_AGE_HOURS", "24") or "24")

    if not ig_token or not ig_user_id:
        raise RuntimeError("Missing IG_ACCESS_TOKEN / IG_USER_ID")

    spreadsheet_id = env("SPREADSHEET_ID") or env("SHEET_ID")
    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")

    gc = gspread_client()
    sh = gc.open_by_key(spreadsheet_id)

    try:
        ws_ops = sh.worksheet(ops_tab)
        theme_today = normalize_theme(ws_ops.acell("B2").value or "")
    except Exception:
        theme_today = ""
    if not theme_today:
        theme_today = normalize_theme(env("THEME_OF_DAY", "")) or "adventure"

    ws = sh.worksheet(raw_tab)
    values = ws.get_all_values()
    if not values:
        raise RuntimeError(f"{raw_tab} is empty")

    headers = values[0]
    h = idx_map(headers)
    for required in ("status", "deal_id", "graphic_url"):
        if required not in h:
            raise RuntimeError(f"RAW_DEALS missing required header: {required}")

    rows = [(i, row_dict(headers, row)) for i, row in enumerate(values[1:], start=2)]
    pick = pick_candidate(rows, theme_today, slot, ref, max_age_hours)
    if not pick:
        print("⚠️ No Instagram candidate found: no READY_TO_PUBLISH row with a usable graphic_url.")
        return 0

    row_i, row, reason = pick
    deal_id = get_cell(row, "deal_id")
    image_url = get_cell(row, "graphic_url")
    caption = build_caption(row, theme_today, slot)

    print("=" * 70)
    print("📣 Instagram Publisher — V6 Safe + MIZAR")
    print(f"TODAY_THEME: '{theme_today}' | CURRENT_SLOT: '{slot or '(none)'}'")
    print(f"SELECTED row={row_i} | deal_id={deal_id}")
    print(f"SELECTION: {reason}")
    print(f"IMAGE: {image_url}")
    print("=" * 70)

    try:
        preflight_image_url(image_url)

        if env_bool("INSTAGRAM_DRY_RUN", False):
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

    except Exception as exc:
        msg = str(exc)[:450]
        print(f"⛔ IG publish failed: {msg}")
        update_cell_if_present(ws, row_i, h, "publish_error", msg)
        update_cell_if_present(ws, row_i, h, "publish_error_at", iso_z(ref))
        raise


if __name__ == "__main__":
    raise SystemExit(main())
