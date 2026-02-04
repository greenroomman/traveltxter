from __future__ import annotations

import os
import re
import json
import time
import hashlib
import datetime as dt
from typing import Dict, List, Any, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials

# ============================================================
# TRAVELTXTER — FEEDER v5.5 FAST
#
# GOAL
# - Make runtime dominated by Duffel, not Google Sheets.
# - CONFIG is the authority for destination + intent theme + Fi/Vi.
# - Origins are sourced from CONFIG_ORIGIN_POOLS (preferred) or ORIGIN_POOL_CSV fallback.
#
# NOTE
# - City/country fields intentionally left blank (enrich_router fills from IATA_MASTER).
# - Keeps de-dupe + caps + rotation + 90/10 + diminishing returns.
# ============================================================


# ------------------------
# Logging / env
# ------------------------
def log(msg: str) -> None:
    ts = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    print(f"{ts} | {msg}", flush=True)


def env(k: str, d: str = "") -> str:
    return (os.getenv(k, d) or "").strip()


def env_int(k: str, d: int) -> int:
    try:
        return int(env(k))
    except Exception:
        return d


def fnum(v: Any, d: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return d


def tab(env_name: str, default: str) -> str:
    v = os.getenv(env_name)
    return default if not v or not v.strip() else v.strip()


def safe_get(d: Dict[str, str], *keys: str) -> str:
    for k in keys:
        if k in d and (d.get(k) or "").strip():
            return (d.get(k) or "").strip()
    return ""


# ------------------------
# Robust SA JSON parsing
# ------------------------
def _repair_private_key_newlines(raw: str) -> str:
    pat = re.compile(r'("private_key"\s*:\s*")(.+?)(")', re.DOTALL)
    m = pat.search(raw)
    if not m:
        return raw
    prefix, pk, suffix = m.group(1), m.group(2), m.group(3)
    pk_fixed = pk.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "\\n").replace("\t", "\\t")
    return raw[: m.start()] + prefix + pk_fixed + suffix + raw[m.end():]


def _load_sa_info() -> Dict[str, Any]:
    raw = env("GCP_SA_JSON_ONE_LINE") or env("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")

    for attempt in (
        lambda s: json.loads(s),
        lambda s: json.loads(s.replace("\\n", "\n")),
        lambda s: json.loads(_repair_private_key_newlines(s)),
        lambda s: json.loads(_repair_private_key_newlines(s).replace("\\n", "\n")),
    ):
        try:
            return attempt(raw)
        except Exception:
            continue

    return json.loads(_repair_private_key_newlines(raw))


def gspread_client():
    info = _load_sa_info()
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds)


# ------------------------
# Sheets helpers
# ------------------------
def open_ws(sh, name: str):
    try:
        return sh.worksheet(name)
    except Exception as e:
        raise RuntimeError(f"WorksheetNotFound: '{name}'") from e


def header(ws) -> List[str]:
    return ws.row_values(1)


def records(ws) -> List[Dict[str, str]]:
    rows = ws.get_all_values()
    if not rows:
        return []
    hdr = rows[0]
    out: List[Dict[str, str]] = []
    for r in rows[1:]:
        rr = r + [""] * (len(hdr) - len(r))
        out.append(dict(zip(hdr, rr)))
    return out


def a1(ws, cell: str) -> str:
    v = ws.acell(cell).value
    return (v or "").strip()


def col_to_a1(col_idx_1based: int) -> str:
    # 1 -> A, 26 -> Z, 27 -> AA ...
    n = col_idx_1based
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


# ------------------------
# Duffel
# ------------------------
def duffel_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {env('DUFFEL_API_KEY')}",
        "Duffel-Version": "v2",
        "Content-Type": "application/json",
    }


def normalise_airlines_csv(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    if s.upper() in ("ANY", "ALL", "*", "NONE", "NULL"):
        return ""
    parts = [p.strip().upper() for p in s.split(",") if p.strip()]
    parts = [p for p in parts if 1 <= len(p) <= 3 and p.isalnum()]
    return ",".join(parts)


def duffel_offer_request(
    origin: str,
    dest: str,
    out_date: str,
    ret_date: str,
    max_connections: int,
    cabin_class: str,
    included_airlines_csv: str = "",
) -> Optional[Dict[str, Any]]:
    body: Dict[str, Any] = {
        "data": {
            "slices": [
                {"origin": origin, "destination": dest, "departure_date": out_date},
                {"origin": dest, "destination": origin, "departure_date": ret_date},
            ],
            "passengers": [{"type": "adult"}],
            "cabin_class": cabin_class,
            "max_connections": int(max_connections),
        }
    }

    air = normalise_airlines_csv(included_airlines_csv)
    if air:
        body["data"]["included_airlines"] = [x for x in air.split(",") if x]

    r = requests.post(
        "https://api.duffel.com/air/offer_requests",
        headers=duffel_headers(),
        json=body,
        timeout=35,
    )
    if not r.ok:
        return None
    return r.json()


def best_offer(resp: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    data = (resp or {}).get("data") or {}
    offers = data.get("offers") or []
    return offers[0] if offers else None


def offer_stops(offer: Dict[str, Any]) -> int:
    try:
        slices = offer.get("slices") or []
        if not slices:
            return 0
        segs = slices[0].get("segments") or []
        return max(0, len(segs) - 1)
    except Exception:
        return 0


# ------------------------
# Theme match from CONFIG intent columns
# ------------------------
THEME_COLS = ("primary_theme", "short_stay_theme", "long_stay_winter_theme", "long_stay_summer_theme")


def theme_matches_config_row(r: Dict[str, str], theme_today: str) -> bool:
    t = (theme_today or "").strip().lower()
    for c in THEME_COLS:
        if (r.get(c) or "").strip().lower() == t:
            return True
    return False


# ------------------------
# Date candidates (rotating, deterministic per day)
# ------------------------
def date_candidates(today: dt.date, k: int, base_days_ahead: int, step_days: int) -> List[dt.date]:
    out: List[dt.date] = []
    k = max(1, int(k))
    for i in range(k):
        out.append(today + dt.timedelta(days=base_days_ahead + i * step_days))
    return out


def day_rotation_offset() -> int:
    ymd = dt.datetime.utcnow().strftime("%Y%m%d")
    return int(hashlib.md5(ymd.encode("utf-8")).hexdigest()[:6], 16)


# ------------------------
# Insert / dedupe
# ------------------------
def make_deal_id(origin: str, dest: str, out_date: str, ret_date: str, cabin: str) -> str:
    key = f"{origin}|{dest}|{out_date}|{ret_date}|{cabin}"
    return hashlib.md5(key.encode("utf-8")).hexdigest()[:12]


# ------------------------
# Planning (Pi + diminishing returns)
# ------------------------
def diminishing_return_score(Pi: float, q_used: int) -> float:
    Pi = max(0.0, min(1.0, Pi))
    return Pi * ((1.0 - Pi) ** max(0, q_used))


def build_query_plan(
    candidates: List[Dict[str, Any]],
    max_searches: int,
    primary_budget: int,
    secondary_budget: int,
    max_queries_per_dest: int,
) -> List[Tuple[str, Dict[str, Any]]]:
    plan: List[Tuple[str, Dict[str, Any]]] = []
    qcount: Dict[str, int] = {}

    def pick_one() -> Optional[Dict[str, Any]]:
        best = None
        best_score = -1.0
        for r in candidates:
            dest = (r.get("destination_iata") or "").strip()
            if not dest:
                continue
            used = qcount.get(dest, 0)
            if used >= max_queries_per_dest:
                continue
            sc = diminishing_return_score(float(r["_Pi"]), used)
            if used >= 2:
                sc *= 0.85
            if sc > best_score:
                best_score = sc
                best = r
        return best

    lane_seq = (["PRIMARY"] * primary_budget) + (["SECONDARY"] * secondary_budget)
    lane_seq = lane_seq[:max_searches]

    for lane in lane_seq:
        r = pick_one()
        if not r:
            break
        dest = (r.get("destination_iata") or "").strip()
        qcount[dest] = qcount.get(dest, 0) + 1
        plan.append((lane, r))

    # ensure at least one secondary if budgeted and we still have capacity
    if secondary_budget > 0 and all(l != "SECONDARY" for l, _ in plan) and len(plan) < max_searches:
        r = pick_one()
        if r:
            plan.append(("SECONDARY", r))

    return plan


# ------------------------
# Origin pools
# ------------------------
def parse_iata_csv(s: str) -> List[str]:
    parts = [p.strip().upper() for p in (s or "").split(",") if p.strip()]
    parts = [p for p in parts if 3 <= len(p) <= 4 and p.isalnum()]
    # keep order, de-dupe
    out: List[str] = []
    seen = set()
    for p in parts:
        if p not in seen:
            out.append(p)
            seen.add(p)
    return out


def load_origin_pools(sh, pool_tab: str) -> Dict[str, List[str]]:
    """
    Preferred: CONFIG_ORIGIN_POOLS tab with:
      destination_iata | origins_csv
    Returns dest -> [origins...]
    """
    try:
        ws = sh.worksheet(pool_tab)
    except Exception:
        return {}

    rows = ws.get_all_values()
    if not rows or len(rows) < 2:
        return {}

    hdr = [h.strip() for h in rows[0]]
    try:
        c_dest = hdr.index("destination_iata")
        c_csv = hdr.index("origins_csv")
    except Exception:
        return {}

    out: Dict[str, List[str]] = {}
    for r in rows[1:]:
        if c_dest >= len(r):
            continue
        dest = (r[c_dest] or "").strip().upper()
        if not dest:
            continue
        csv = r[c_csv] if c_csv < len(r) else ""
        origins = parse_iata_csv(csv)
        if origins:
            out[dest] = origins
    return out


# ------------------------
# Main
# ------------------------
def main() -> int:
    sheet_id = env("SPREADSHEET_ID") or env("SHEET_ID")
    if not sheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")

    RAW_TAB = tab("RAW_DEALS_TAB", "RAW_DEALS")
    CFG_TAB = tab("FEEDER_CONFIG_TAB", "CONFIG")
    OPS_TAB = tab("OPS_MASTER_TAB", "OPS_MASTER")

    # Optional (fast) pool map; avoid ROUTE_CAPABILITY_MAP entirely
    ORIGIN_POOLS_TAB = tab("CONFIG_ORIGIN_POOLS_TAB", "CONFIG_ORIGIN_POOLS")
    GLOBAL_ORIGIN_POOL = parse_iata_csv(env("ORIGIN_POOL_CSV", "LHR,LGW,MAN"))

    MAX_SEARCHES = env_int("DUFFEL_MAX_SEARCHES_PER_RUN", 12)
    MAX_INSERTS = env_int("DUFFEL_MAX_INSERTS", 50)
    MAX_PER_ORIGIN = env_int("DUFFEL_MAX_INSERTS_PER_ORIGIN", 15)
    MAX_PER_ROUTE = env_int("DUFFEL_MAX_INSERTS_PER_ROUTE", 5)

    DESTS_PER_RUN = env_int("DUFFEL_ROUTES_PER_RUN", 4)
    ORIGINS_PER_DEST = env_int("ORIGINS_PER_DEST", 3)
    K_DATES_PER_DEST = env_int("K_DATES_PER_DEST", 3)

    PRIMARY_PCT = env_int("SLOT_PRIMARY_PCT", 90)
    BASE_DAYS_AHEAD = env_int("BASE_DAYS_AHEAD", 30)
    DATE_STEP_DAYS = env_int("DATE_STEP_DAYS", 14)

    MAX_QUERIES_PER_DEST = env_int("MAX_QUERIES_PER_DEST", 3)

    # Cabin: economy-only for now (RDV premium not ready)
    CABIN_CLASS = env("FEEDER_CABIN_CLASS", "economy").lower() or "economy"

    # Connection tolerance: simple by lane (keeps feeder independent of ZTB)
    # If you want ZTB back later, do it in a tiny lookup, not a full table scan.
    MAX_CONN_PRIMARY = env_int("MAX_CONN_PRIMARY", 1)
    MAX_CONN_SECONDARY = env_int("MAX_CONN_SECONDARY", 2)

    primary_budget = int(round(MAX_SEARCHES * (PRIMARY_PCT / 100.0)))
    primary_budget = max(0, min(MAX_SEARCHES, primary_budget))
    secondary_budget = max(0, MAX_SEARCHES - primary_budget)

    gc = gspread_client()
    sh = gc.open_by_key(sheet_id)

    RAW = open_ws(sh, RAW_TAB)
    CFG = open_ws(sh, CFG_TAB)
    OPS = open_ws(sh, OPS_TAB)

    theme = a1(OPS, "B5")
    log(f"🎯 Theme of day: {theme}")
    log(
        f"CAPS: MAX_SEARCHES={MAX_SEARCHES} | MAX_INSERTS={MAX_INSERTS} | PER_ORIGIN={MAX_PER_ORIGIN} | "
        f"PER_ROUTE={MAX_PER_ROUTE} | DESTS_PER_RUN={DESTS_PER_RUN} | ORIGINS_PER_DEST={ORIGINS_PER_DEST} | "
        f"K_DATES_PER_DEST={K_DATES_PER_DEST} | PRIMARY/SECONDARY={primary_budget}/{secondary_budget} | cabin={CABIN_CLASS}"
    )
    log(f"LANES: max_conn_primary={MAX_CONN_PRIMARY} | max_conn_secondary={MAX_CONN_SECONDARY}")

    # --- Load origin pools (fast). No RCM scan.
    t0 = time.time()
    origin_pools = load_origin_pools(sh, ORIGIN_POOLS_TAB)
    log(f"✅ Origin pools loaded: {len(origin_pools)} destinations ({time.time()-t0:.1f}s)")

    # --- Load CONFIG rows (small: ~200)
    cfg_all = records(CFG)
    cfg_rows = [
        r for r in cfg_all
        if (r.get("enabled") or "").strip().lower() in ("true", "1", "yes")
        and (r.get("destination_iata") or "").strip()
        and theme_matches_config_row(r, theme)
    ]
    if not cfg_rows:
        log("⚠️ No CONFIG rows for theme (intent theme columns).")
        return 0

    # --- Best per destination by Pi (Fi×Vi)
    best_by_dest: Dict[str, Dict[str, Any]] = {}
    for r in cfg_rows:
        dest = (r.get("destination_iata") or "").strip().upper()
        Fi = fnum(safe_get(r, "search_weight", "feasibility_score", "Fi"), 0.0)
        Vi = fnum(safe_get(r, "value_score", "Vi"), 0.5)
        Pi = Fi * Vi
        rr: Dict[str, Any] = dict(r)
        rr["_Fi"], rr["_Vi"], rr["_Pi"] = Fi, Vi, Pi
        if dest not in best_by_dest or Pi > float(best_by_dest[dest].get("_Pi", 0.0)):
            best_by_dest[dest] = rr

    candidates = list(best_by_dest.values())
    candidates.sort(key=lambda x: float(x.get("_Pi", 0.0)), reverse=True)
    candidates = candidates[:max(1, DESTS_PER_RUN)]

    # --- Read RAW header once
    raw_hdr = header(RAW)
    if not raw_hdr:
        raise RuntimeError("RAW_DEALS header row is empty")

    # --- Existing deal_ids: single column fetch (fast)
    existing_deal_ids = set()
    if "deal_id" in raw_hdr:
        c = raw_hdr.index("deal_id") + 1
        col_letter = col_to_a1(c)
        # This is much faster than col_values on large sheets in many cases
        vals = RAW.get(f"{col_letter}2:{col_letter}")
        for row in vals:
            if row and row[0]:
                existing_deal_ids.add(row[0].strip())

    # --- Date set + deterministic rotation
    today = dt.date.today()
    outs = date_candidates(today, K_DATES_PER_DEST, BASE_DAYS_AHEAD, DATE_STEP_DAYS)
    rot = day_rotation_offset()

    # --- Insert caps bookkeeping
    inserted = 0
    inserts_by_origin: Dict[str, int] = {}
    inserts_by_route: Dict[Tuple[str, str], int] = {}
    used_triplets: set[Tuple[str, str, str]] = set()

    def can_insert(origin_iata: str, dest_iata: str, deal_id: str) -> bool:
        if inserted >= MAX_INSERTS:
            return False
        if deal_id in existing_deal_ids:
            return False
        if inserts_by_origin.get(origin_iata, 0) >= MAX_PER_ORIGIN:
            return False
        if inserts_by_route.get((origin_iata, dest_iata), 0) >= MAX_PER_ROUTE:
            return False
        return True

    # --- Build plan (PRIMARY/SECONDARY with diminishing returns)
    plan = build_query_plan(
        candidates=candidates,
        max_searches=MAX_SEARCHES,
        primary_budget=primary_budget,
        secondary_budget=secondary_budget,
        max_queries_per_dest=MAX_QUERIES_PER_DEST,
    )
    if not plan:
        log("⚠️ No query plan could be built.")
        return 0

    log(
        f"🧭 Query plan: {len(plan)} planned | primary={sum(1 for x in plan if x[0]=='PRIMARY')} | "
        f"secondary={sum(1 for x in plan if x[0]=='SECONDARY')}"
    )

    searches = 0
    primary_used = 0
    secondary_used = 0

    # Skips
    skipped_no_origins = 0
    skipped_dupe_triplet = 0
    skipped_cannot_insert = 0
    skipped_no_offer = 0
    skipped_non_gbp = 0
    skipped_bad_price = 0
    skipped_all_combos_blocked = 0

    per_dest_q: Dict[str, int] = {}
    out_rows: List[Dict[str, str]] = []

    for lane, r in plan:
        if searches >= MAX_SEARCHES or inserted >= MAX_INSERTS:
            break

        dest = (r.get("destination_iata") or "").strip().upper()
        if not dest:
            continue

        max_conn = MAX_CONN_PRIMARY if lane == "PRIMARY" else MAX_CONN_SECONDARY

        # Origins: prefer per-destination pool, else fallback to global
        origins = origin_pools.get(dest) or GLOBAL_ORIGIN_POOL
        origins = origins[:max(1, ORIGINS_PER_DEST)]
        if not origins:
            skipped_no_origins += 1
            continue

        q = per_dest_q.get(dest, 0)
        per_dest_q[dest] = q + 1

        # Trip length: prefer trip_length_days (RAW schema), fallback to 4
        trip_len = int(float(safe_get(r, "trip_length_days", "trip_days") or "4"))
        trip_len = max(2, min(21, trip_len))

        included_airlines = safe_get(r, "included_airlines", "included_airlines_csv", "carriers")

        combos_tried = 0
        max_combo_tries = max(1, len(origins) * len(outs))
        picked: Optional[Tuple[str, dt.date, dt.date, str]] = None

        while combos_tried < max_combo_tries:
            idx = (q + rot + combos_tried)
            origin = origins[idx % len(origins)]
            out_date = outs[(idx // len(origins)) % len(outs)]
            ret_date = out_date + dt.timedelta(days=trip_len)

            triplet = (origin, dest, out_date.isoformat())
            if triplet in used_triplets:
                combos_tried += 1
                skipped_dupe_triplet += 1
                continue

            deal_id = make_deal_id(origin, dest, out_date.isoformat(), ret_date.isoformat(), CABIN_CLASS)
            if not can_insert(origin, dest, deal_id):
                combos_tried += 1
                skipped_cannot_insert += 1
                continue

            picked = (origin, out_date, ret_date, deal_id)
            break

        if not picked:
            skipped_all_combos_blocked += 1
            log(f"⚠️ All origin/date combos blocked for dest={dest} (duplicates/caps).")
            continue

        origin, out_date, ret_date, deal_id = picked
        used_triplets.add((origin, dest, out_date.isoformat()))

        log(
            f"🔎 Search {searches+1}/{MAX_SEARCHES} [{lane}] {origin} → {dest} "
            f"| Pi={float(r['_Pi']):.2f} | max_conn={max_conn} | cabin={CABIN_CLASS}"
        )

        resp = duffel_offer_request(
            origin=origin,
            dest=dest,
            out_date=out_date.isoformat(),
            ret_date=ret_date.isoformat(),
            max_connections=max_conn,
            cabin_class=CABIN_CLASS,
            included_airlines_csv=included_airlines,
        )

        searches += 1
        if lane == "PRIMARY":
            primary_used += 1
        else:
            secondary_used += 1

        offer = best_offer(resp or {})
        if not offer:
            skipped_no_offer += 1
            continue

        total_currency = (offer.get("total_currency") or "").strip().upper()
        total_amount = (offer.get("total_amount") or "").strip()
        if total_currency and total_currency != "GBP":
            skipped_non_gbp += 1
            continue

        try:
            price = float(total_amount)
        except Exception:
            skipped_bad_price += 1
            continue

        stops = offer_stops(offer)
        connection_type = "direct" if stops == 0 else "via"

        now_utc = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

        row: Dict[str, str] = {
            "status": "NEW",
            "deal_id": deal_id,
            "price_gbp": f"{price:.2f}",
            "origin_city": "",                 # enrich_router fills
            "origin_iata": origin,
            "destination_country": "",         # enrich_router fills
            "destination_city": "",            # enrich_router fills
            "destination_iata": dest,
            "outbound_date": out_date.isoformat(),
            "return_date": ret_date.isoformat(),
            "stops": str(stops),
            "deal_theme": theme,
            "ingested_at_utc": now_utc,
            "bags_incl": "",
            "cabin_class": CABIN_CLASS,
            "connection_type": connection_type,
            "scored_timestamp": "",
            "via_hub": "",
            "inbound_duration_minutes": "",
            "outbound_duration_minutes": "",
            "graphic_url": "",
            "booking_link_vip": "",
            "rendered_timestamp": "",
            "render_error": "",
            "created_utc": now_utc,
            "total_duration_hours": "",
            "posted_instagram_at": "",
            "phrase_used": "",
            "posted_telegram_vip_at": "",
            "posted_telegram_free_at": "",
            "timestamp": now_utc,
            "created_at": now_utc,
            "phrase_bank": "",
            "trip_length_days": str(trip_len),
            "origin_country": "",              # enrich_router fills
            "currency": "GBP",
            "carriers": "",
            "publish_window": "",
            "theme": "",                       # leave to RDV/dynamic_theme logic
            "age_hours": "",
            "is_fresh_24h": "",
            "publish_error": "",
            "publish_error_at": "",
            "rendered_at": "",
        }

        # Bookkeeping
        inserted += 1
        existing_deal_ids.add(deal_id)
        inserts_by_origin[origin] = inserts_by_origin.get(origin, 0) + 1
        inserts_by_route[(origin, dest)] = inserts_by_route.get((origin, dest), 0) + 1
        out_rows.append(row)

    if not out_rows:
        log(
            "⚠️ No rows inserted.\n"
            f"SKIPS: no_origins={skipped_no_origins} dupe_triplet={skipped_dupe_triplet} "
            f"cannot_insert={skipped_cannot_insert} all_combos_blocked={skipped_all_combos_blocked} "
            f"no_offer={skipped_no_offer} non_gbp={skipped_non_gbp} bad_price={skipped_bad_price}"
        )
        return 0

    # --- BULK APPEND (single Sheets write)
    values: List[List[str]] = []
    for r in out_rows:
        values.append([r.get(h, "") for h in raw_hdr])

    RAW.append_rows(values, value_input_option="RAW")

    log(f"✅ Inserted {len(out_rows)} row(s) into {RAW_TAB}.")
    log(
        f"SUMMARY: searches={searches} | primary={primary_used} | secondary={secondary_used} | inserted={len(out_rows)}\n"
        f"SKIPS: no_origins={skipped_no_origins} dupe_triplet={skipped_dupe_triplet} "
        f"cannot_insert={skipped_cannot_insert} all_combos_blocked={skipped_all_combos_blocked} "
        f"no_offer={skipped_no_offer} non_gbp={skipped_non_gbp} bad_price={skipped_bad_price}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
