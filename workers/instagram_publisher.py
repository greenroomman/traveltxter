# workers/instagram_publisher.py
#!/usr/bin/env python3
"""
TravelTxter — Instagram Publisher (PROD SAFE, LOCKED)

Consumes:
- RAW_DEALS where status == READY_TO_PUBLISH

Enforces:
- Theme-of-the-day gate:
    theme_of_day = THEME_OF_DAY env override (if set) else deterministic UTC rotation.
  Row theme must match (deal_theme OR theme). If mismatch -> skip row.

Publishes:
- Posts image to Instagram via Graph API
- Writes posted_instagram_at, status=POSTED_INSTAGRAM
- On failure, writes publish_error/publish_error_at and re-queues to READY_TO_POST
  if the failure is image URL fetchability.

Notes:
- Does NOT depend on CONFIG sheet having theme_of_day (it doesn't in your export).
- Repairs common PythonAnywhere URL variants (/static/renders vs /renders).
"""

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


# ============================================================
# Theme of day (must match pipeline_worker rotation logic)
# ============================================================

MASTER_THEMES = [
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
]


def theme_of_day_utc() -> str:
    today = dt.datetime.utcnow().date()
    doy = int(today.strftime("%j"))
    return MASTER_THEMES[doy % len(MASTER_THEMES)]


def norm_theme(s: str) -> str:
    return (s or "").strip().lower().replace(" ", "_")


def resolve_theme_of_day() -> str:
    # AM/PM reinstate: set THEME_OF_DAY in workflow for full operator control.
    override = norm_theme(os.getenv("THEME_OF_DAY", ""))
    return override if override else norm_theme(theme_of_day_utc())


# ============================================================
# Logging
# ============================================================

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


# ============================================================
# Env helpers
# ============================================================

def env_str(k: str, default: str = "") -> str:
    return (os.environ.get(k, default) or "").strip()


def env_any(keys: List[str], default: str = "") -> str:
    for k in keys:
        v = env_str(k, "")
        if v:
            return v
    return default


def iso_now() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


# ============================================================
# Google Sheets auth
# ============================================================

def parse_sa_json(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return json.loads(raw.replace("\\n", "\n"))


def gs_client() -> gspread.Client:
    raw = env_any(["GCP_SA_JSON_ONE_LINE", "GCP_SA_JSON"])
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
    info = parse_sa_json(raw)
    creds = Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


def a1(row: int, col0: int) -> str:
    return gspread.utils.rowcol_to_a1(row, col0 + 1)


def safe_get(r: List[str], idx: int) -> str:
    if idx < 0 or idx >= len(r):
        return ""
    return (r[idx] or "").strip()


# ============================================================
# Image URL preflight + repair (PythonAnywhere)
# ============================================================

def preflight(url: str) -> Tuple[int, str]:
    try:
        r = requests.get(url, timeout=25, allow_redirects=True)
        ct = r.headers.get("Content-Type", "")
        if r.status_code == 200 and ct.startswith("image/"):
            return 200, ""
        snippet = ""
        if "text" in ct:
            snippet = (r.text or "")[:180].replace("\n", " ").strip()
        return int(r.status_code), snippet
    except Exception as e:
        return 0, f"{type(e).__name__}: {e}"[:180]


def candidate_url_variants(raw_url: str) -> List[str]:
    u = (raw_url or "").strip()
    if not u:
        return []
    variants = [u]

    base = env_str("PUBLIC_BASE_URL", "https://greenroomman.pythonanywhere.com").rstrip("/")

    # If relative -> absolutise
    if not u.startswith("http://") and not u.startswith("https://"):
        if u.startswith("/"):
            variants.append(base + u)
        else:
            variants.append(base + "/" + u)
    else:
        variants.append(u)

    abs_u = variants[-1]

    swaps = [
        ("/static/renders/", "/renders/"),
        ("/renders/", "/static/renders/"),
    ]
    for a, b in swaps:
        if a in abs_u:
            variants.append(abs_u.replace(a, b))

    # Try reconstruct from filename
    m = re.search(r"([^/]+\.png)$", abs_u)
    if m:
        fname = m.group(1)
        variants.append(f"{base}/renders/{fname}")
        variants.append(f"{base}/static/renders/{fname}")

    # De-dupe
    out, seen = [], set()
    for v in variants:
        if v and v not in seen:
            out.append(v)
            seen.add(v)
    return out


def preflight_and_repair_image_url(raw_url: str) -> str:
    last_status, last_snip = 0, ""
    for cand in candidate_url_variants(raw_url):
        status, snip = preflight(cand)
        last_status, last_snip = status, snip
        if status == 200:
            return cand
    raise RuntimeError(f"graphic_url not fetchable (HTTP {last_status}) :: {last_snip}")


# ============================================================
# Instagram Graph API
# ============================================================

GRAPH_BASE = "https://graph.facebook.com/v20.0"


def ig_create_container(ig_user_id: str, token: str, image_url: str, caption: str) -> str:
    url = f"{GRAPH_BASE}/{ig_user_id}/media"
    r = requests.post(
        url,
        data={"image_url": image_url, "caption": caption, "access_token": token},
        timeout=60,
    )
    j = r.json()
    if "id" not in j:
        raise RuntimeError(f"IG create container failed: {j}")
    return str(j["id"])


def ig_publish_container(ig_user_id: str, token: str, creation_id: str) -> str:
    url = f"{GRAPH_BASE}/{ig_user_id}/media_publish"
    r = requests.post(
        url,
        data={"creation_id": creation_id, "access_token": token},
        timeout=60,
    )
    j = r.json()
    if "id" not in j:
        raise RuntimeError(f"IG publish failed: {j}")
    return str(j["id"])


# ============================================================
# Caption (minimal facts + phrase)
# ============================================================

def build_caption(row: Dict[str, str], theme_today: str) -> str:
    phrase = (row.get("phrase_bank") or "").strip()
    origin = (row.get("origin_city") or row.get("origin_iata") or "").strip()
    dest = (row.get("destination_city") or row.get("destination_iata") or "").strip()
    country = (row.get("destination_country") or "").strip()
    out_date = (row.get("outbound_date") or "").strip()
    in_date = (row.get("return_date") or "").strip()
    price = (row.get("price_gbp") or "").strip()

    lines: List[str] = []
    if phrase:
        lines.append(phrase)
        lines.append("")

    lines.append(f"£{price} to {dest}{', ' + country if country else ''}".strip())
    if origin:
        lines.append(f"From {origin}")
    if out_date and in_date:
        lines.append(f"{out_date} → {in_date}")
    lines.append("")
    lines.append(f"Theme today: {theme_today.replace('_', ' ')}")
    lines.append("#traveltxter #traveldeals #cheapflights")
    return "\n".join(lines).strip()


# ============================================================
# Main
# ============================================================

def main() -> int:
    spreadsheet_id = env_any(["SPREADSHEET_ID", "SHEET_ID"])
    raw_tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    token = env_str("IG_ACCESS_TOKEN", "")
    ig_user_id = env_str("IG_USER_ID", "")

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID/SHEET_ID")
    if not token or not ig_user_id:
        raise RuntimeError("Missing IG_ACCESS_TOKEN or IG_USER_ID")

    theme_today = resolve_theme_of_day()
    log(f"🎯 Theme of the day (resolved): {theme_today}")

    gc = gs_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(raw_tab)

    values = ws.get_all_values()
    if len(values) < 2:
        log("No rows.")
        return 0

    headers = [h.strip() for h in values[0] if h is not None]
    h = {k: i for i, k in enumerate(values[0]) if isinstance(k, str) and k.strip()}

    required = ["status", "graphic_url", "deal_theme", "theme", "posted_instagram_at", "publish_error", "publish_error_at"]
    missing = [c for c in required if c not in h]
    if missing:
        raise RuntimeError(f"RAW_DEALS missing required columns: {missing}")

    # Find first eligible row
    for rownum, r in enumerate(values[1:], start=2):
        status = safe_get(r, h["status"])
        if status != "READY_TO_PUBLISH":
            continue

        row = {values[0][i]: (r[i] if i < len(r) else "") for i in range(len(values[0])) if isinstance(values[0][i], str)}
        row_theme = norm_theme((row.get("deal_theme") or "").strip()) or norm_theme((row.get("theme") or "").strip())
        if not row_theme or row_theme != theme_today:
            continue

        graphic_url = (row.get("graphic_url") or "").strip()
        if not graphic_url:
            continue

        try:
            image_url = preflight_and_repair_image_url(graphic_url)

            # Write back repaired URL if it changed (keeps pipeline clean)
            if image_url != graphic_url:
                ws.update([[image_url]], a1(rownum, h["graphic_url"]))

            caption = build_caption(row, theme_today)

            log(f"📸 Publishing row {rownum} deal_id={row.get('deal_id','')}")
            creation_id = ig_create_container(ig_user_id, token, image_url, caption)
            time.sleep(3)
            media_id = ig_publish_container(ig_user_id, token, creation_id)

            ws.batch_update(
                [
                    {"range": a1(rownum, h["posted_instagram_at"]), "values": [[iso_now()]]},
                    {"range": a1(rownum, h["status"]), "values": [["POSTED_INSTAGRAM"]]},
                    {"range": a1(rownum, h["publish_error"]), "values": [[""]]},
                    {"range": a1(rownum, h["publish_error_at"]), "values": [[""]]},
                ],
                value_input_option="USER_ENTERED",
            )

            log(f"✅ Instagram published media_id={media_id} row={rownum}")
            return 0

        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            ws.update([[err[:300]]], a1(rownum, h["publish_error"]))
            ws.update([[iso_now()]], a1(rownum, h["publish_error_at"]))

            # If the issue is image URL not fetchable, re-queue for rerender
            if "graphic_url not fetchable" in err:
                ws.update([["READY_TO_POST"]], a1(rownum, h["status"]))
                log(f"⚠️ Image fetch failed; re-queued row {rownum} -> READY_TO_POST")
                return 0

            raise

    log("Done. Instagram posted 0 (no eligible rows match theme gate).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
