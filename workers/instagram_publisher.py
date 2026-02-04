# workers/instagram_publisher.py
# V5 — Instagram Publisher
#
# Reads:
# - OPS_MASTER!B5 (theme of the day)
# - RAW_DEALS_VIEW (read-only) OPTIONAL (not required for posting)
# - RAW_DEALS (source of truth; writable)
#
# Writes (RAW_DEALS only):
# - posted_instagram_at (timestamp)
# - status: PUBLISH_AM / PUBLISH_PM / PUBLISH_BOTH  -> READY_TO_POST   (ONLY these)
# - publish_error / publish_error_at (if columns exist)
#
# Posting rule:
# - IG must always advertise: if no publish_window candidate exists, fallback to latest fresh rendered deal
#   for today theme (graphic_url present, not already posted) even if status isn't PUBLISH_*.
#   In that case: DO NOT change status.

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
    """
    Repairs a common bad-secret format where the JSON is mostly valid
    but private_key contains literal newlines (invalid in JSON strings).
    Converts those literal newlines inside the private_key field to \\n.
    """
    # Locate "private_key": "...."
    # DOTALL is required because the private key may contain literal newlines.
    pat = re.compile(r'("private_key"\s*:\s*")(.+?)(")', re.DOTALL)
    m = pat.search(raw)
    if not m:
        return raw

    prefix, pk, suffix = m.group(1), m.group(2), m.group(3)

    # Convert real newlines to \n escape sequences
    pk_fixed = pk.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "\\n")

    # Also ensure any stray unescaped tabs are escaped
    pk_fixed = pk_fixed.replace("\t", "\\t")

    return raw[: m.start()] + prefix + pk_fixed + suffix + raw[m.end():]

def load_sa_info() -> Dict[str, Any]:
    """
    Load SA JSON from:
      1) GCP_SA_JSON_ONE_LINE (preferred)
      2) GCP_SA_JSON (fallback)
    And repair if secret contains invalid control chars.
    """
    raw = env("GCP_SA_JSON_ONE_LINE") or env("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")

    # Attempt 1: direct JSON
    try:
        return json.loads(raw)
    except Exception:
        pass

    # Attempt 2: common one-line secret contains literal "\\n"
    try:
        return json.loads(raw.replace("\\n", "\n"))
    except Exception:
        pass

    # Attempt 3: repair literal newlines inside private_key value
    repaired = _repair_private_key_newlines(raw)
    try:
        return json.loads(repaired)
    except Exception:
        pass

    # Attempt 4: repair + then unescape \\n
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

def safe_float(v: str, default: float = 0.0) -> float:
    try:
        s = str(v or "").strip()
        if not s:
            return default
        s = s.replace("£", "").replace(",", "")
        return float(s)
    except Exception:
        return default

def normalize_theme(t: str) -> str:
    t = (t or "").strip().lower()
    t = t.replace(" ", "_")
    return t


# ------------------ IG API helpers ------------------

def graph_url(version: str, path: str) -> str:
    return f"https://graph.facebook.com/{version}/{path.lstrip('/')}"

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
    j = r.json()
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
    j = r.json()
    if "id" not in j:
        raise RuntimeError(f"IG publish failed: {j}")
    return j["id"]


# ------------------ selection logic ------------------

def run_slot() -> str:
    s = env("RUN_SLOT", "").upper()
    return s if s in ("AM", "PM") else ""

def is_fresh_24h(row: Dict[str, str], ref: dt.datetime) -> bool:
    # Prefer explicit column if present
    if "is_fresh_24h" in row:
        v = get_cell(row, "is_fresh_24h")
        if v:
            return truthy(v)

    # Else compute from ingested_at_utc if available
    ts = parse_iso_utc(get_cell(row, "ingested_at_utc"))
    h = hours_since(ts, ref)
    return (h is not None and h <= 24.0)

def pick_candidate(rows: List[Tuple[int, Dict[str, str]]],
                   theme_today: str,
                   slot: str,
                   ref: dt.datetime) -> Optional[Tuple[int, Dict[str, str], str]]:
    """
    Returns (row_index_1based, row, reason) or None
    """
    # Primary: publish_window matches slot
    want_windows = []
    if slot == "AM":
        want_windows = ["PUBLISH_AM", "PUBLISH_BOTH"]
    elif slot == "PM":
        want_windows = ["PUBLISH_PM", "PUBLISH_BOTH"]
    else:
        want_windows = ["PUBLISH_BOTH", "PUBLISH_AM", "PUBLISH_PM"]

    # Filter: must have graphic_url and not already IG posted and be fresh
    eligible = []
    for i, r in rows:
        if not get_cell(r, "graphic_url"):
            continue
        if get_cell(r, "posted_instagram_at"):
            continue
        if not is_fresh_24h(r, ref):
            continue

        # theme match (prefer column theme, then deal_theme)
        rt = normalize_theme(get_cell(r, "theme") or get_cell(r, "deal_theme"))
        match = (rt == theme_today) if rt else True  # if blank, don't block
        eligible.append((i, r, match))

    if not eligible:
        return None

    # Window-matching candidates first
    window_hits = []
    for i, r, match in eligible:
        pw = get_cell(r, "publish_window").upper()
        if pw in want_windows:
            window_hits.append((i, r, match))

    def sort_key(item):
        i, r, match = item
        ing = parse_iso_utc(get_cell(r, "ingested_at_utc")) or dt.datetime.min.replace(tzinfo=dt.timezone.utc)
        # Prefer theme match, newest ingest
        return (1 if match else 0, ing)

    if window_hits:
        window_hits.sort(key=sort_key, reverse=True)
        i, r, match = window_hits[0]
        return i, r, f"publish_window={get_cell(r,'publish_window')} theme_match={match}"

    # Fallback: any fresh rendered for theme_today
    theme_hits = [(i, r, m) for (i, r, m) in eligible if m]
    if theme_hits:
        theme_hits.sort(key=sort_key, reverse=True)
        i, r, match = theme_hits[0]
        return i, r, f"fallback_theme_latest theme_match={match}"

    # Last fallback: latest fresh rendered regardless of theme (still advertising)
    eligible.sort(key=sort_key, reverse=True)
    i, r, match = eligible[0]
    return i, r, f"fallback_any_latest theme_match={match}"


# ------------------ caption ------------------

def build_caption(row: Dict[str, str], theme_today: str, slot: str) -> str:
    """
    IG is marketing: keep it light, not price-led.
    Use what we have without failing if blanks.
    """
    to_city = get_cell(row, "destination_city") or get_cell(row, "destination_iata")
    from_city = get_cell(row, "origin_city") or get_cell(row, "origin_iata")
    out_d = get_cell(row, "outbound_date")
    in_d = get_cell(row, "return_date")

    phrase = get_cell(row, "phrase_used") or get_cell(row, "phrase_bank")
    if phrase:
        phrase = phrase.strip()

    # Keep it consistent with your IG rules: no hype, light CTA
    lines = [
        f"Theme today: {theme_today.replace('_',' ')}",
        f"TO: {to_city}",
        f"FROM: {from_city}",
        f"OUT: {out_d}",
        f"IN:  {in_d}",
    ]

    if phrase:
        lines.append(phrase)

    # Slot hint (AM = “what’s hot”, PM = “deal worth a look”)
    if slot == "AM":
        lines.append("AM radar: what’s looking interesting right now. Link in bio for the full feed.")
    elif slot == "PM":
        lines.append("PM shortlist: one worth a look if you’re in the mood for it. Link in bio for the full feed.")
    else:
        lines.append("Link in bio for the full feed.")

    return "\n".join([x for x in lines if x.strip()])


# ------------------ main ------------------

def main() -> int:
    ref = now_utc()
    slot = run_slot()

    RAW_TAB = env("RAW_DEALS_TAB", "RAW_DEALS")
    OPS_TAB = env("OPS_MASTER_TAB", "OPS_MASTER")

    ig_token = env("IG_ACCESS_TOKEN")
    ig_user_id = env("IG_USER_ID")
    version = env("GRAPH_API_VERSION", "v20.0")

    if not ig_token or not ig_user_id:
        raise RuntimeError("Missing IG_ACCESS_TOKEN / IG_USER_ID")

    gc = gspread_client()
    sh = gc.open_by_key(env("SPREADSHEET_ID") or env("SHEET_ID"))

    ws_ops = sh.worksheet(OPS_TAB)
    theme_today = normalize_theme(ws_ops.acell("B5").value or "")
    if not theme_today:
        theme_today = "adventure"

    ws = sh.worksheet(RAW_TAB)
    values = ws.get_all_values()
    headers = values[0]
    h = idx_map(headers)

    required = ["status", "deal_id", "graphic_url"]
    for k in required:
        if k not in h:
            raise RuntimeError(f"RAW_DEALS missing required header: {k}")

    # Optional write columns
    has_posted = ("posted_instagram_at" in h)
    has_pub_err = ("publish_error" in h)
    has_pub_err_at = ("publish_error_at" in h)

    # Pull rows (index starts at 2 for first data row)
    rows: List[Tuple[int, Dict[str, str]]] = []
    for i, r in enumerate(values[1:], start=2):
        rd = row_dict(headers, r)
        rows.append((i, rd))

    pick = pick_candidate(rows, theme_today, slot, ref)
    if not pick:
        print("⚠️ No IG candidate found (no fresh rendered deals). Exiting 0.")
        return 0

    row_i, row, reason = pick
    print("======================================================================")
    print("📣 Instagram Publisher — V5 (publish_window + fallback)")
    print(f"TODAY_THEME: '{theme_today}' | RUN_SLOT: '{slot or '(auto)'}'")
    print(f"SELECTED row={row_i} | deal_id={get_cell(row,'deal_id')} | reason={reason}")
    print("======================================================================")

    caption = build_caption(row, theme_today, slot)
    image_url = get_cell(row, "graphic_url")

    try:
        creation_id = ig_create_container(version, ig_user_id, ig_token, image_url, caption)
        # small wait improves publish reliability
        time.sleep(2)
        media_id = ig_publish_container(version, ig_user_id, ig_token, creation_id)
        print(f"✅ IG published media_id={media_id}")

        # Write posted_instagram_at
        if has_posted:
            ws.update_cell(row_i, h["posted_instagram_at"] + 1, iso_z(ref))

        # Status transition ONLY when currently PUBLISH_*
        status = get_cell(row, "status")
        if status in ("PUBLISH_AM", "PUBLISH_PM", "PUBLISH_BOTH"):
            ws.update_cell(row_i, h["status"] + 1, "READY_TO_POST")

        # Clear publish_error on success
        if has_pub_err:
            ws.update_cell(row_i, h["publish_error"] + 1, "")
        if has_pub_err_at:
            ws.update_cell(row_i, h["publish_error_at"] + 1, "")

        return 0

    except Exception as e:
        msg = str(e)[:450]
        print(f"⛔ IG publish failed: {msg}")

        if has_pub_err:
            ws.update_cell(row_i, h["publish_error"] + 1, msg)
        if has_pub_err_at:
            ws.update_cell(row_i, h["publish_error_at"] + 1, iso_z(ref))

        raise


if __name__ == "__main__":
    raise SystemExit(main())
