# workers/instagram_publisher.py
# ============================================================
# TravelTxter Instagram Publisher — V5 (AM/PM + publish_window)
# ============================================================
#
# INTENT (LOCKED):
# - Instagram is ADVERTISING, not product.
# - AM = theme-led editorial ("what's hot?")
# - PM = deal consideration ("worth a look")
# - Must post ONCE per run, always.
#
# AUTHORITY:
# - Theme of day: OPS_MASTER!B5
# - Timing: RUN_SLOT=AM|PM (fallback = UTC hour)
# - Eligibility: RAW_DEALS.publish_window (column AL)
#
# publish_window semantics:
#   AM    -> eligible only for AM
#   PM    -> eligible only for PM
#   BOTH  -> eligible for either (failsafe buffer, ≤12h old)
#
# HARD RULES:
# - RDV is READ-ONLY
# - Requires graphic_url
# - Writes posted_instagram_at ONLY after successful publish
# - Never blocks on price / cheap lane / VIP logic
# - Variety guard applies
#
# ============================================================

from __future__ import annotations

import os
import json
import time
import datetime as dt
import re
import requests
import gspread
from google.oauth2.service_account import Credentials

# ------------------------
# Helpers
# ------------------------

def env(k: str, d: str = "") -> str:
    return (os.getenv(k, d) or "").strip()

def get_run_slot() -> str:
    slot = env("RUN_SLOT").upper()
    if slot in {"AM", "PM"}:
        return slot
    return "AM" if dt.datetime.utcnow().hour < 12 else "PM"

def parse_utc(ts: str):
    if not ts:
        return None
    try:
        return dt.datetime.fromisoformat(ts.replace("Z", ""))
    except Exception:
        return None

def sa_creds():
    raw = env("GCP_SA_JSON_ONE_LINE") or env("GCP_SA_JSON")
    info = json.loads(raw.replace("\\n", "\n"))
    return Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )

# ------------------------
# Theme normalisation
# ------------------------

AUTHORITATIVE_THEMES = {
    "winter_sun","summer_sun","beach_break","snow","northern_lights",
    "surf","adventure","city_breaks","culture_history",
    "long_haul","luxury_value","unexpected_value"
}

def normalize_theme(t: str) -> str:
    if not t:
        return "adventure"
    t = t.lower().strip()
    t = re.sub(r"[^a-z0-9_]+", "_", t)
    return t if t in AUTHORITATIVE_THEMES else "adventure"

def theme_tokens(raw: str):
    if not raw:
        return []
    parts = re.split(r"[,\|;]+", raw)
    out = []
    for p in parts:
        t = normalize_theme(p)
        if t in AUTHORITATIVE_THEMES:
            out.append(t)
    return list(dict.fromkeys(out))

# ------------------------
# Main
# ------------------------

def main():
    run_slot = get_run_slot()            # AM / PM
    now = dt.datetime.utcnow()
    freshness_cutoff = now - dt.timedelta(hours=12)
    variety_hours = int(env("VARIETY_LOOKBACK_HOURS", "120"))

    gc = gspread.authorize(sa_creds())
    sh = gc.open_by_key(env("SPREADSHEET_ID") or env("SHEET_ID"))

    ws_raw = sh.worksheet("RAW_DEALS")
    ws_rdv = sh.worksheet("RAW_DEALS_VIEW")
    ws_ops = sh.worksheet("OPS_MASTER")

    today_theme = normalize_theme(ws_ops.acell("B5").value)

    raw_vals = ws_raw.get_all_values()
    rdv_vals = ws_rdv.get_all_values()

    raw_h = {h:i for i,h in enumerate(raw_vals[0])}
    rdv_h = {h:i for i,h in enumerate(rdv_vals[0])}

    REQUIRED_RAW = ["deal_id","graphic_url","ingested_at_utc","posted_instagram_at","publish_window"]
    REQUIRED_RDV = ["deal_id","destination_city","destination_country","dynamic_theme"]

    for c in REQUIRED_RAW:
        if c not in raw_h:
            raise RuntimeError(f"RAW_DEALS missing {c}")
    for c in REQUIRED_RDV:
        if c not in rdv_h:
            raise RuntimeError(f"RAW_DEALS_VIEW missing {c}")

    # Map RDV by deal_id
    rdv_map = {}
    for r in rdv_vals[1:]:
        did = r[rdv_h["deal_id"]]
        if did:
            rdv_map[did] = r

    # Build recent-city variety block
    recent_cities = set()
    cutoff = now - dt.timedelta(hours=variety_hours)
    for r in raw_vals[1:]:
        ts = parse_utc(r[raw_h["posted_instagram_at"]])
        if ts and ts >= cutoff:
            did = r[raw_h["deal_id"]]
            if did in rdv_map:
                city = rdv_map[did][rdv_h["destination_city"]]
                if city:
                    recent_cities.add(city)

    candidates = []

    for idx, r in enumerate(raw_vals[1:], start=2):
        did = r[raw_h["deal_id"]]
        if not did or did not in rdv_map:
            continue

        if r[raw_h["posted_instagram_at"]]:
            continue

        img = r[raw_h["graphic_url"]]
        if not img:
            continue

        pub_window = (r[raw_h["publish_window"]] or "").upper()
        if pub_window not in {"AM","PM","BOTH"}:
            continue

        ingested = parse_utc(r[raw_h["ingested_at_utc"]])
        if not ingested or ingested < freshness_cutoff:
            continue

        # Window eligibility
        if pub_window != "BOTH" and pub_window != run_slot:
            continue

        rdv = rdv_map[did]
        dyn_tokens = theme_tokens(rdv[rdv_h["dynamic_theme"]])

        candidates.append({
            "raw_row": idx,
            "deal_id": did,
            "image": img,
            "city": rdv[rdv_h["destination_city"]],
            "country": rdv[rdv_h["destination_country"]],
            "themes": dyn_tokens,
            "ingested": ingested,
        })

    if not candidates:
        print("⚠️ No eligible Instagram candidates (this should be rare).")
        return 0

    # Sort newest first
    candidates.sort(key=lambda x: x["ingested"], reverse=True)

    # Selection ladder
    themed = [c for c in candidates if today_theme in c["themes"]]
    pool = themed or candidates

    chosen = None
    for c in pool:
        if c["city"] not in recent_cities:
            chosen = c
            break
    if not chosen:
        chosen = pool[0]

    # Publish
    ig_user = env("IG_USER_ID")
    token = env("IG_ACCESS_TOKEN")
    api = env("GRAPH_API_VERSION","v20.0")

    caption = f"""{chosen['country']}
London → {chosen['city']}

What’s hot for {today_theme.replace('_',' ')} right now.

VIP sees deals first.
Link in bio.
"""

    create = requests.post(
        f"https://graph.facebook.com/{api}/{ig_user}/media",
        data={
            "image_url": chosen["image"],
            "caption": caption,
            "access_token": token,
        },
        timeout=30,
    ).json()

    cid = create.get("id")
    if not cid:
        raise RuntimeError(f"IG create failed: {create}")

    time.sleep(2)

    pub = requests.post(
        f"https://graph.facebook.com/{api}/{ig_user}/media_publish",
        data={
            "creation_id": cid,
            "access_token": token,
        },
        timeout=30,
    ).json()

    if "id" not in pub:
        raise RuntimeError(f"IG publish failed: {pub}")

    ws_raw.update_cell(
        chosen["raw_row"],
        raw_h["posted_instagram_at"] + 1,
        now.isoformat() + "Z"
    )

    print(f"✅ Instagram posted ({run_slot}) — {chosen['city']} [{chosen['deal_id']}]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
