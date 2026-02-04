from __future__ import annotations

import os
import re
import json
import time
import hashlib
import random
import datetime as dt
from typing import Dict, List, Any, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials

# ============================================================
# TRAVELTXTER — FEEDER v5.7 (CONFIG-ROUTE DRIVEN)
#
# Uses CONFIG headers (authoritative):
# enabled, active_in_feeder, origin_iata, destination_iata,
# days_ahead_min, days_ahead_max, trip_length_days, max_connections,
# included_airlines, cabin_class, search_weight, value_score,
# primary_theme, short_stay_theme, long_stay_winter_theme, long_stay_summer_theme
#
# DOES NOT load IATA_MASTER / ROUTE_CAPABILITY_MAP (enrich handles).
# Bulk append to RAW_DEALS.
# 90/10: PRIMARY uses row max_connections, SECONDARY relaxes +1 (capped).
# Search de-dupe: FEEDER_LOG (optional) + in-run.
# ============================================================


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


def env_float(k: str, d: float) -> float:
    try:
        return float(env(k))
    except Exception:
        return d


def first_int(*keys: str, default: int) -> int:
    for k in keys:
        v = env(k, "")
        if v:
            try:
                return int(v)
            except Exception:
                pass
    return default


# ------------------------
# Robust SA JSON parsing
# ------------------------
def _repair_private_key_newlines(raw: str) -> str:
    pat = re.compile(r'("private_key"\s*:\s*")(.+?)(")', re.DOTALL)
    m = pat.search(raw)
    if not m:
        return raw
    prefix, pk, suffix = m.group(1), m.group(2), m.group(3)
    pk_fixed = pk.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "\\n")
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
    n = col_idx_1based
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


# ------------------------
# Value parsers
# ------------------------
def truthy(v: str) -> bool:
    return (v or "").strip().lower() in ("true", "1", "yes", "y")


def fnum(v: Any, d: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return d


def inum(v: Any, d: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return d


def parse_airlines_field(s: str) -> List[str]:
    """
    CONFIG.included_airlines can be:
    - empty / ANY / ALL / *  => no filter
    - CSV: "BA,U2,FR"
    """
    s = (s or "").strip()
    if not s:
        return []
    if s.upper() in ("ANY", "ALL", "*"):
        return []
    parts = [p.strip().upper() for p in s.split(",") if p.strip()]
    parts = [p for p in parts if 1 <= len(p) <= 3 and p.isalnum()]
    # preserve order, unique
    return list(dict.fromkeys(parts))


# ------------------------
# Theme match (CONFIG)
# ------------------------
THEME_COLS = ("primary_theme", "short_stay_theme", "long_stay_winter_theme", "long_stay_summer_theme")


def theme_matches_row(r: Dict[str, str], theme_today: str) -> bool:
    t = (theme_today or "").strip().lower()
    for c in THEME_COLS:
        if (r.get(c) or "").strip().lower() == t:
            return True
    return False


# ------------------------
# Duffel
# ------------------------
def duffel_headers() -> Dict[str, str]:
    key = env("DUFFEL_API_KEY")
    if not key:
        raise RuntimeError("Missing DUFFEL_API_KEY")
    return {
        "Authorization": f"Bearer {key}",
        "Duffel-Version": "v2",
        "Content-Type": "application/json",
    }


def duffel_offer_request(
    origin: str,
    dest: str,
    out_date: str,
    ret_date: str,
    max_connections: int,
    cabin_class: str,
    included_airlines: List[str],
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
    if included_airlines:
        body["data"]["included_airlines"] = included_airlines

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
        segs = slices[0].get("segments") or []
        return max(0, len(segs) - 1)
    except Exception:
        return 0


# ------------------------
# Search de-dupe
# ------------------------
def search_sig(theme: str, origin: str, dest: str, out_date: str) -> str:
    key = f"{theme}|{origin}|{dest}|{out_date}"
    return hashlib.md5(key.encode("utf-8")).hexdigest()[:16]


def build_recent_sig_set(ws_log, hours: int) -> set[str]:
    if ws_log is None:
        return set()
    try:
        rows = ws_log.get_all_values()
        if not rows or len(rows) < 2:
            return set()
        hdr = rows[0]
        if "ts_utc" not in hdr or "sig" not in hdr:
            return set()
        c_ts = hdr.index("ts_utc")
        c_sig = hdr.index("sig")

        now = dt.datetime.utcnow()
        cutoff = now - dt.timedelta(hours=hours)

        out = set()
        for r in rows[1:]:
            if c_ts >= len(r) or c_sig >= len(r):
                continue
            ts = (r[c_ts] or "").strip()
            sig = (r[c_sig] or "").strip()
            if not ts or not sig:
                continue
            try:
                tsv = dt.datetime.fromisoformat(ts.replace("Z", "+00:00")).replace(tzinfo=None)
            except Exception:
                continue
            if tsv >= cutoff:
                out.add(sig)
        return out
    except Exception:
        return set()


def append_log_rows(ws_log, sigs: List[str]) -> None:
    if ws_log is None or not sigs:
        return
    now = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    values = [[now, s] for s in sigs]
    try:
        ws_log.append_rows(values, value_input_option="RAW")
    except Exception:
        pass


# ------------------------
# Deal ID
# ------------------------
def make_deal_id(origin: str, dest: str, out_date: str, ret_date: str, cabin: str) -> str:
    key = f"{origin}|{dest}|{out_date}|{ret_date}|{cabin}"
    return hashlib.md5(key.encode("utf-8")).hexdigest()[:12]


# ------------------------
# Main
# ------------------------
def main() -> int:
    sheet_id = env("SPREADSHEET_ID") or env("SHEET_ID")
    if not sheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")

    RAW_TAB = env("RAW_DEALS_TAB", "RAW_DEALS")
    CFG_TAB = env("FEEDER_CONFIG_TAB", "CONFIG")
    OPS_TAB = env("OPS_MASTER_TAB", "OPS_MASTER")
    FEEDER_LOG_TAB = env("FEEDER_LOG_TAB", "FEEDER_LOG")

    # Governor precedence
    MAX_SEARCHES = first_int("DUFFEL_MAX_SEARCHES_PER_RUN", "FEEDER_MAX_SEARCHES", default=12)
    MAX_INSERTS = first_int("DUFFEL_MAX_INSERTS", "FEEDER_MAX_INSERTS", default=50)
    MAX_PER_ORIGIN = env_int("DUFFEL_MAX_INSERTS_PER_ORIGIN", 15)
    MAX_PER_ROUTE = env_int("DUFFEL_MAX_INSERTS_PER_ROUTE", 5)

    SLEEP = env_float("FEEDER_SLEEP_SECONDS", 0.1)
    DEDUPE_HOURS = env_int("DUFFEL_SEARCH_DEDUPE_HOURS", 4)

    # 90/10
    primary_pct = env_int("SLOT_PRIMARY_PCT", 90)
    primary_budget = max(0, min(MAX_SEARCHES, int(round(MAX_SEARCHES * (primary_pct / 100.0)))))
    secondary_budget = max(0, MAX_SEARCHES - primary_budget)
    lane_seq = (["PRIMARY"] * primary_budget) + (["SECONDARY"] * secondary_budget)
    lane_seq = lane_seq[:MAX_SEARCHES]

    # Sheets
    gc = gspread_client()
    sh = gc.open_by_key(sheet_id)

    RAW = open_ws(sh, RAW_TAB)
    CFG = open_ws(sh, CFG_TAB)
    OPS = open_ws(sh, OPS_TAB)

    ws_log = None
    try:
        ws_log = sh.worksheet(FEEDER_LOG_TAB)
    except Exception:
        ws_log = None

    theme_today = a1(OPS, "B5") or env("THEME", "DEFAULT")
    log(f"🎯 Theme of day: {theme_today}")

    cfg_all = records(CFG)

    # Filter: enabled AND active_in_feeder AND theme match AND has origin/dest
    cfg_rows: List[Dict[str, str]] = []
    for r in cfg_all:
        if not truthy(r.get("enabled", "")):
            continue
        if not truthy(r.get("active_in_feeder", "")):
            continue
        if not (r.get("origin_iata") or "").strip():
            continue
        if not (r.get("destination_iata") or "").strip():
            continue
        if not theme_matches_row(r, theme_today):
            continue
        cfg_rows.append(r)

    if not cfg_rows:
        log("⚠️ No CONFIG routes eligible for theme (enabled+active_in_feeder+theme match).")
        return 0

    # Build route candidates with Pi = Fi*Vi (search_weight * value_score)
    routes: List[Dict[str, Any]] = []
    for r in cfg_rows:
        origin = (r.get("origin_iata") or "").strip().upper()
        dest = (r.get("destination_iata") or "").strip().upper()

        Fi = fnum(r.get("search_weight"), 0.0)
        Vi = fnum(r.get("value_score"), 0.5)
        Pi = Fi * Vi

        # per-route rules
        dmin = inum(r.get("days_ahead_min"), 21)
        dmax = inum(r.get("days_ahead_max"), 84)
        if dmax < dmin:
            dmin, dmax = dmax, dmin

        trip_len = inum(r.get("trip_length_days"), 7)
        max_conn = inum(r.get("max_connections"), 1)

        cabin = (r.get("cabin_class") or "economy").strip().lower() or "economy"
        included_airlines = parse_airlines_field(r.get("included_airlines", ""))

        routes.append(
            {
                "_origin": origin,
                "_dest": dest,
                "_Fi": Fi,
                "_Vi": Vi,
                "_Pi": Pi,
                "_dmin": dmin,
                "_dmax": dmax,
                "_trip": trip_len,
                "_max_conn": max_conn,
                "_cabin": cabin,
                "_airlines": included_airlines,
            }
        )

    # Rank by Pi (publishable probability proxy)
    routes.sort(key=lambda x: float(x["_Pi"]), reverse=True)

    # Soft diversity: don’t let one dest dominate
    dest_used: Dict[str, int] = {}
    def pick_next_route() -> Dict[str, Any]:
        best = None
        best_score = -1.0
        for rr in routes:
            dest = rr["_dest"]
            used = dest_used.get(dest, 0)
            Pi = float(rr["_Pi"])
            # diminishing returns for repeated destination
            score = Pi * ((1.0 - min(0.95, Pi)) ** used)
            if used >= 2:
                score *= 0.85
            if score > best_score:
                best_score = score
                best = rr
        assert best is not None
        return best

    raw_hdr = header(RAW)
    if not raw_hdr:
        raise RuntimeError("RAW_DEALS header row is empty")

    # Existing deal ids
    existing_deal_ids = set()
    if "deal_id" in raw_hdr:
        c = raw_hdr.index("deal_id") + 1
        col = col_to_a1(c)
        vals = RAW.get(f"{col}2:{col}")
        for row in vals:
            if row and row[0]:
                existing_deal_ids.add(row[0].strip())

    inserted = 0
    searches = 0
    by_origin: Dict[str, int] = {}
    by_route: Dict[Tuple[str, str], int] = {}

    recent_sigs = build_recent_sig_set(ws_log, DEDUPE_HOURS)
    inrun_sigs = set()
    appended_sigs: List[str] = []

    out_rows: List[Dict[str, str]] = []

    # skips
    sk_dedupe = 0
    sk_no_offer = 0
    sk_non_gbp = 0
    sk_bad_price = 0
    sk_caps = 0

    # deterministic-ish seed
    seed = int(hashlib.md5(dt.datetime.utcnow().strftime("%Y%m%d").encode()).hexdigest()[:8], 16)
    rng = random.Random(seed)

    def can_insert(origin: str, dest: str, deal_id: str) -> bool:
        if inserted >= MAX_INSERTS:
            return False
        if deal_id in existing_deal_ids:
            return False
        if by_origin.get(origin, 0) >= MAX_PER_ORIGIN:
            return False
        if by_route.get((origin, dest), 0) >= MAX_PER_ROUTE:
            return False
        return True

    for lane in lane_seq:
        if searches >= MAX_SEARCHES or inserted >= MAX_INSERTS:
            break

        rr = pick_next_route()
        origin = rr["_origin"]
        dest = rr["_dest"]

        dest_used[dest] = dest_used.get(dest, 0) + 1

        dmin = int(rr["_dmin"])
        dmax = int(rr["_dmax"])
        trip_len = int(rr["_trip"])
        cabin = rr["_cabin"]
        base_max_conn = int(rr["_max_conn"])

        # 90/10 relaxation: allow +1 connection on secondary lane (capped at 2)
        max_conn = base_max_conn if lane == "PRIMARY" else min(2, base_max_conn + 1)

        # choose outbound date within per-route window
        days_ahead = rng.randint(dmin, dmax)
        out_date = dt.date.today() + dt.timedelta(days=days_ahead)
        ret_date = out_date + dt.timedelta(days=trip_len)

        sig = search_sig(theme_today, origin, dest, out_date.isoformat())
        if sig in recent_sigs or sig in inrun_sigs:
            sk_dedupe += 1
            continue
        inrun_sigs.add(sig)

        deal_id = make_deal_id(origin, dest, out_date.isoformat(), ret_date.isoformat(), cabin)
        if not can_insert(origin, dest, deal_id):
            sk_caps += 1
            continue

        log(
            f"🔎 Search {searches+1}/{MAX_SEARCHES} [{lane}] {origin}→{dest} "
            f"| Pi={float(rr['_Pi']):.2f} | max_conn={max_conn} | cabin={cabin} | trip={trip_len}d | window={dmin}-{dmax}"
        )

        resp = duffel_offer_request(
            origin=origin,
            dest=dest,
            out_date=out_date.isoformat(),
            ret_date=ret_date.isoformat(),
            max_connections=max_conn,
            cabin_class=cabin,
            included_airlines=rr["_airlines"],
        )
        searches += 1

        offer = best_offer(resp or {})
        if not offer:
            sk_no_offer += 1
            time.sleep(SLEEP)
            continue

        cur = (offer.get("total_currency") or "").strip().upper()
        amt = (offer.get("total_amount") or "").strip()
        if cur and cur != "GBP":
            sk_non_gbp += 1
            time.sleep(SLEEP)
            continue

        try:
            price = float(amt)
        except Exception:
            sk_bad_price += 1
            time.sleep(SLEEP)
            continue

        stops = offer_stops(offer)
        connection_type = "direct" if stops == 0 else "via"

        now_utc = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

        row: Dict[str, str] = {
            "status": "NEW",
            "deal_id": deal_id,
            "price_gbp": f"{price:.2f}",
            "origin_city": "",
            "origin_iata": origin,
            "destination_country": "",
            "destination_city": "",
            "destination_iata": dest,
            "outbound_date": out_date.isoformat(),
            "return_date": ret_date.isoformat(),
            "stops": str(stops),
            "deal_theme": theme_today,
            "ingested_at_utc": now_utc,
            "bags_incl": "",
            "cabin_class": cabin,
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
            "origin_country": "",
            "currency": "GBP",
            "carriers": "",
            "publish_window": "",
            "theme": "",
            "age_hours": "",
            "is_fresh_24h": "",
            "publish_error": "",
            "publish_error_at": "",
            "rendered_at": "",
        }

        out_rows.append(row)

        inserted += 1
        existing_deal_ids.add(deal_id)
        by_origin[origin] = by_origin.get(origin, 0) + 1
        by_route[(origin, dest)] = by_route.get((origin, dest), 0) + 1

        appended_sigs.append(sig)
        time.sleep(SLEEP)

    if not out_rows:
        log(
            "⚠️ No rows inserted.\n"
            f"SKIPS: dedupe={sk_dedupe} caps={sk_caps} no_offer={sk_no_offer} non_gbp={sk_non_gbp} bad_price={sk_bad_price}"
        )
        return 0

    # BULK append
    values: List[List[str]] = []
    for r in out_rows:
        values.append([r.get(h, "") for h in raw_hdr])

    RAW.append_rows(values, value_input_option="RAW")
    append_log_rows(ws_log, appended_sigs)

    log(f"✅ Inserted {len(out_rows)} row(s) into {RAW_TAB}.")
    log(
        f"SUMMARY: searches={searches} inserted={len(out_rows)}\n"
        f"SKIPS: dedupe={sk_dedupe} caps={sk_caps} no_offer={sk_no_offer} non_gbp={sk_non_gbp} bad_price={sk_bad_price}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
