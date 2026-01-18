# workers/instagram_publisher.py
# FULL REPLACEMENT (V4.7) — phrase_used preferred, phrase_bank fallback

from __future__ import annotations

import os
import re
import json
import time
import datetime as dt
from typing import Any, Dict, List, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials


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
    override = norm_theme(os.getenv("THEME_OF_DAY", ""))
    return override if override else norm_theme(theme_of_day_utc())


def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


def iso_now() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def env(k: str, default: str = "") -> str:
    return (os.environ.get(k, default) or "").strip()


def parse_sa_json(raw: str) -> Dict[str, Any]:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return json.loads(raw.replace("\\n", "\n"))


def gs_client() -> gspread.Client:
    raw = env("GCP_SA_JSON_ONE_LINE") or env("GCP_SA_JSON")
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


def preflight(url: str) -> Tuple[int, str]:
    try:
        r = requests.get(url, timeout=25, allow_redirects=True)
        ct = (r.headers.get("Content-Type", "") or "").strip().lower()
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
    base = env("PUBLIC_BASE_URL", "https://greenroomman.pythonanywhere.com").rstrip("/")

    if not u.startswith("http://") and not u.startswith("https://"):
        variants.append(base + (u if u.startswith("/") else "/" + u))
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

    m = re.search(r"([^/]+\.png)$", abs_u)
    if m:
        fname = m.group(1)
        variants.append(f"{base}/renders/{fname}")
        variants.append(f"{base}/static/renders/{fname}")

    out: List[str] = []
    seen = set()
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


GRAPH_BASE = "https://graph.facebook.com/v20.0"


def ig_create_container(ig_user_id: str, token: str, image_url: str, caption: str) -> str:
    r = requests.post(
        f"{GRAPH_BASE}/{ig_user_id}/media",
        data={"image_url": image_url, "caption": caption, "access_token": token},
        timeout=60,
    )
    j = r.json()
    if "id" not in j:
        raise RuntimeError(f"IG create failed: {j}")
    return str(j["id"])


def ig_publish_container(ig_user_id: str, token: str, creation_id: str) -> str:
    r = requests.post(
        f"{GRAPH_BASE}/{ig_user_id}/media_publish",
        data={"creation_id": creation_id, "access_token": token},
        timeout=60,
    )
    j = r.json()
    if "id" not in j:
        raise RuntimeError(f"IG publish failed: {j}")
    return str(j["id"])


def phrase_from_row(row: Dict[str, str]) -> str:
    p = (row.get("phrase_used") or "").strip()
    if p:
        return p
    return (row.get("phrase_bank") or "").strip()


def build_caption(row: Dict[str, str]) -> str:
    country = (row.get("destination_country") or "").strip()
    flag = (row.get("country_flag") or "").strip()
    dest = (row.get("destination_city") or row.get("destination_iata") or "").strip()
    origin = (row.get("origin_city") or row.get("origin_iata") or "").strip()
    price = (row.get("price_gbp") or "").strip().replace("£", "").strip()
    out_date = (row.get("outbound_date") or "").strip()
    ret_date = (row.get("return_date") or "").strip()
    phrase = phrase_from_row(row)

    first_line = country if country else (row.get("destination_iata") or "").strip()
    if flag:
        first_line = f"{first_line} {flag}".strip()

    lines: List[str] = [
        first_line,
        f"To: {dest}",
        f"From: {origin}",
        f"Price: £{price}",
        f"Out: {out_date}",
        f"Return: {ret_date}",
        "",
    ]
    if phrase:
        lines.append(phrase)
    lines.append("Link in bio…")
    return "\n".join([l for l in lines if l is not None]).strip()


def main() -> int:
    spreadsheet_id = env("SPREADSHEET_ID") or env("SHEET_ID")
    raw_tab = env("RAW_DEALS_TAB", "RAW_DEALS")
    token = env("IG_ACCESS_TOKEN")
    ig_user_id = env("IG_USER_ID")

    if not spreadsheet_id or not token or not ig_user_id:
        raise RuntimeError("Missing required env vars: SPREADSHEET_ID, IG_ACCESS_TOKEN, IG_USER_ID")

    theme_today = resolve_theme_of_day()
    log(f"🎯 Theme of the day: {theme_today}")

    gc = gs_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(raw_tab)

    values = ws.get_all_values()
    if len(values) < 2:
        log("No rows.")
        return 0

    headers = [h.strip() for h in values[0]]
    h = {k: i for i, k in enumerate(headers)}

    required = [
        "status",
        "graphic_url",
        "deal_theme",
        "theme",
        "ingested_at_utc",
        "phrase_used",
        "phrase_bank",
        "posted_instagram_at",
        "publish_error",
        "publish_error_at",
    ]
    for c in required:
        if c not in h:
            raise RuntimeError(f"Missing column: {c}")

    eligible: List[Dict[str, Any]] = []

    for i, r in enumerate(values[1:], start=2):
        if (r[h["status"]] or "").strip() != "READY_TO_PUBLISH":
            continue

        row = {headers[j]: (r[j] if j < len(r) else "") for j in range(len(headers))}
        row_theme = norm_theme(row.get("deal_theme")) or norm_theme(row.get("theme"))
        if row_theme != theme_today:
            continue

        if not (row.get("graphic_url") or "").strip():
            continue

        ts_raw = (row.get("ingested_at_utc") or "").strip()
        try:
            ts = dt.datetime.fromisoformat(ts_raw.replace("Z", ""))
        except Exception:
            ts = dt.datetime.min

        eligible.append({"row_num": i, "ts": ts, "row": row})

    if not eligible:
        log("No eligible rows match status + theme gate.")
        return 0

    eligible.sort(key=lambda x: (x["ts"], x["row_num"]), reverse=True)
    target = eligible[0]
    rownum = target["row_num"]
    row = target["row"]

    try:
        image_url = preflight_and_repair_image_url(row["graphic_url"])
        if image_url != row["graphic_url"]:
            ws.update([[image_url]], a1(rownum, h["graphic_url"]))

        caption = build_caption(row)

        log(f"📸 Publishing row {rownum} deal_id={(row.get('deal_id') or '').strip()}")
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

        if "graphic_url not fetchable" in err:
            ws.update([["READY_TO_POST"]], a1(rownum, h["status"]))
            log(f"⚠️ Image fetch failed; re-queued row {rownum} -> READY_TO_POST")
            return 0

        raise


if __name__ == "__main__":
    raise SystemExit(main())
