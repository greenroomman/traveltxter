# ============================================================
# TravelTxter Instagram Publisher — V5 (Slot-Rotating, Non-Blocking)
# ============================================================
#
# PURPOSE
# - Instagram is MARKETING / ADVERTISING only
# - AM / PM are rotating slots, not permissions
#
# ARCHITECTURE (LOCKED)
# - Feeder decides long-haul vs short-haul
# - Scorer decides worthiness + publish_window (preference only)
# - Publisher decides WHAT TO SHOW NOW
#
# HARD RULES
# - Must publish once per run
# - Must never re-score, re-price, or re-evaluate haul
# - publish_window is a preference hint, NOT a gate
# - PUBLISH_BOTH is always eligible
#
# WRITE BACK
# - RAW_DEALS.posted_instagram_at ONLY after success
#
# ============================================================

from __future__ import annotations

import os, json, time, datetime as dt, re, requests
import gspread
from google.oauth2.service_account import Credentials

# ------------------------------------------------------------
# Utilities
# ------------------------------------------------------------

def env(k: str, d: str = "") -> str:
    return (os.getenv(k, d) or "").strip()

def utc_now():
    return dt.datetime.now(dt.timezone.utc)

def run_slot() -> str:
    slot = env("RUN_SLOT").upper()
    if slot in {"AM", "PM"}:
        return slot
    return "AM" if utc_now().hour < 12 else "PM"

def parse_ts(v):
    try:
        return dt.datetime.fromisoformat(str(v).replace("Z","+00:00"))
    except Exception:
        return None

def norm_theme(v: str) -> str:
    if not v:
        return ""
    v = v.lower().strip()
    v = re.sub(r"[^a-z0-9_]+", "_", v)
    return v

def theme_tokens(raw: str):
    if not raw:
        return []
    parts = re.split(r"[,\|;]+", raw)
    return list(dict.fromkeys(norm_theme(p) for p in parts if p))

# ------------------------------------------------------------
# Auth
# ------------------------------------------------------------

def gspread_client():
    raw = env("GCP_SA_JSON_ONE_LINE") or env("GCP_SA_JSON")
    creds = Credentials.from_service_account_info(
        json.loads(raw.replace("\\n","\n")),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)

# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def main():
    SLOT = run_slot()
    NOW = utc_now()
    VARIETY_HOURS = int(env("VARIETY_LOOKBACK_HOURS","120"))
    FRESH_HOURS = 24

    gc = gspread_client()
    sh = gc.open_by_key(env("SPREADSHEET_ID") or env("SHEET_ID"))

    ws_raw = sh.worksheet("RAW_DEALS")
    ws_rdv = sh.worksheet("RAW_DEALS_VIEW")
    ws_ops = sh.worksheet("OPS_MASTER")

    TODAY_THEME = norm_theme(ws_ops.acell("B5").value)

    raw_vals = ws_raw.get_all_values()
    rdv_vals = ws_rdv.get_all_values()

    raw_h = {h:i for i,h in enumerate(raw_vals[0])}
    rdv_h = {h:i for i,h in enumerate(rdv_vals[0])}

    REQUIRED_RAW = ["deal_id","status","graphic_url","ingested_at_utc","posted_instagram_at","publish_window"]
    REQUIRED_RDV = ["deal_id","destination_city","destination_country","dynamic_theme"]

    for c in REQUIRED_RAW:
        if c not in raw_h:
            raise RuntimeError(f"RAW_DEALS missing {c}")
    for c in REQUIRED_RDV:
        if c not in rdv_h:
            raise RuntimeError(f"RAW_DEALS_VIEW missing {c}")

    rdv_by_id = {r[rdv_h["deal_id"]]: r for r in rdv_vals[1:] if r[rdv_h["deal_id"]]}

    # --------------------------------------------------------
    # Variety guard (recent cities)
    # --------------------------------------------------------

    recent_cities = set()
    cutoff = NOW - dt.timedelta(hours=VARIETY_HOURS)

    for r in raw_vals[1:]:
        ts = parse_ts(r[raw_h["posted_instagram_at"]])
        if ts and ts >= cutoff:
            did = r[raw_h["deal_id"]]
            if did in rdv_by_id:
                city = rdv_by_id[did][rdv_h["destination_city"]]
                if city:
                    recent_cities.add(city)

    # --------------------------------------------------------
    # Candidate pool
    # --------------------------------------------------------

    candidates = []

    for row_i, r in enumerate(raw_vals[1:], start=2):
        if r[raw_h["status"]] != "READY_TO_POST":
            continue
        if r[raw_h["posted_instagram_at"]]:
            continue
        if not r[raw_h["graphic_url"]]:
            continue

        ts = parse_ts(r[raw_h["ingested_at_utc"]])
        if not ts or (NOW - ts).total_seconds() > FRESH_HOURS * 3600:
            continue

        did = r[raw_h["deal_id"]]
        rdv = rdv_by_id.get(did)
        if not rdv:
            continue

        pref = (r[raw_h["publish_window"]] or "").upper()
        themes = theme_tokens(rdv[rdv_h["dynamic_theme"]])

        candidates.append({
            "row": row_i,
            "deal_id": did,
            "image": r[raw_h["graphic_url"]],
            "city": rdv[rdv_h["destination_city"]],
            "country": rdv[rdv_h["destination_country"]],
            "themes": themes,
            "pref": pref,
            "ingested": ts,
        })

    if not candidates:
        print("⚠️ No Instagram candidates — this should be rare.")
        return 0

    # --------------------------------------------------------
    # Selection ladder (NON-BLOCKING)
    # --------------------------------------------------------

    def score(c):
        s = 0
        if TODAY_THEME in c["themes"]:
            s += 10
        if c["pref"] == f"PUBLISH_{SLOT}":
            s += 3
        if c["pref"] == "PUBLISH_BOTH":
            s += 2
        if c["city"] not in recent_cities:
            s += 1
        return s

    candidates.sort(key=lambda c: (score(c), c["ingested"]), reverse=True)
    chosen = candidates[0]

    # --------------------------------------------------------
    # Publish
    # --------------------------------------------------------

    caption = f"""{chosen['country']}
London → {chosen['city']}

What’s hot right now for {TODAY_THEME.replace("_"," ")}.

VIP sees deals first.
Link in bio.
"""

    ig_user = env("IG_USER_ID")
    token = env("IG_ACCESS_TOKEN")
    api = env("GRAPH_API_VERSION","v20.0")

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
        chosen["row"],
        raw_h["posted_instagram_at"] + 1,
        NOW.isoformat().replace("+00:00","Z")
    )

    print(f"✅ Instagram posted [{SLOT}] — {chosen['city']} ({chosen['deal_id']})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
