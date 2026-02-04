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
# TRAVELTXTER — FEEDER v5.6 (FAST + ENV-RULED)
#
# PRINCIPLES
# - CONFIG is authority for destinations + intent themes + Fi/Vi.
# - ENV is authority for origins + stops + trip/window bounds.
# - No IATA_MASTER, no ROUTE_CAPABILITY_MAP in feeder (enrich/router handles enrichment).
# - Bulk append to RAW_DEALS in one call.
# - De-dupe recent searches using DUFFEL_SEARCH_DEDUPE_HOURS (lightweight, in-memory per run
#   + optional FEEDER_LOG tab if present).
# ============================================================


# ------------------------
# Logging
# ------------------------
def log(msg: str) -> None:
    ts = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    print(f"{ts} | {msg}", flush=True)


# ------------------------
# Env helpers
# ------------------------
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


def first_float(*keys: str, default: float) -> float:
    for k in keys:
        v = env(k, "")
        if v:
            try:
                return float(v)
            except Exception:
                pass
    return default


def parse_csv_iata(s: str) -> List[str]:
    parts = [p.strip().upper() for p in (s or "").split(",") if p.strip()]
    out: List[str] = []
    seen = set()
    for p in parts:
        if p and p not in seen and p.isalnum() and 3 <= len(p) <= 4:
            out.append(p)
            seen.add(p)
    return out


def theme_key(theme: str) -> str:
    t = (theme or "").strip().upper()
    t = re.sub(r"[^A-Z0-9_]+", "_", t)
    return t


def get_theme_origins(theme: str) -> List[str]:
    t = theme_key(theme)
    s = env(f"ORIGINS_{t}", "")
    if s:
        return parse_csv_iata(s)
    return parse_csv_iata(env("ORIGINS_DEFAULT", "LHR,MAN,BRS,LGW,STN"))


def get_theme_max_stops(theme: str) -> int:
    t = theme_key(theme)
    return first_int(f"MAX_STOPS_{t}", "MAX_STOPS_DEFAULT", default=1)


def parse_minmax(spec: str, default_min: int, default_max: int) -> Tuple[int, int]:
    # Accept forms like "21 / 84" or "21/84" or "21 - 84"
    if not spec:
        return default_min, default_max
    m = re.findall(r"(\d+)", spec)
    if len(m) >= 2:
        a, b = int(m[0]), int(m[1])
        return min(a, b), max(a, b)
    return default_min, default_max


def get_theme_window_days(theme: str) -> Tuple[int, int]:
    t = theme_key(theme)
    spec = env(f"WINDOW_{t}_MIN/MAX", "")
    if spec:
        return parse_minmax(spec, 21, 84)
    spec = env("WINDOW_DEFAULT_MIN/MAX", "21 / 84")
    return parse_minmax(spec, 21, 84)


def get_theme_trip_days(theme: str) -> Tuple[int, int]:
    t = theme_key(theme)
    spec = env(f"TRIP_{t}_MIN/MAX", "")
    if spec:
        return parse_minmax(spec, 4, 10)
    spec = env("TRIP_DEFAULT_MIN/MAX", "4 / 10")
    return parse_minmax(spec, 4, 10)


# ------------------------
# Robust SA JSON parsing (fixes the “invalid control character” issues)
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

    # last resort (let json raise)
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


def normalise_airlines_csv(s: str) -> List[str]:
    s = (s or "").strip()
    if not s:
        return []
    if s.upper() in ("ANY", "ALL", "*"):
        return []
    parts = [p.strip().upper() for p in s.split(",") if p.strip()]
    parts = [p for p in parts if 1 <= len(p) <= 3 and p.isalnum()]
    return list(dict.fromkeys(parts))


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
# CONFIG theme match
# ------------------------
THEME_COLS = ("primary_theme", "short_stay_theme", "long_stay_winter_theme", "long_stay_summer_theme")


def theme_matches_config_row(r: Dict[str, str], theme_today: str) -> bool:
    t = (theme_today or "").strip().lower()
    for c in THEME_COLS:
        if (r.get(c) or "").strip().lower() == t:
            return True
    return False


def fnum(v: Any, d: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return d


# ------------------------
# Search de-dupe (in-memory per run + optional FEEDER_LOG tab)
# ------------------------
def search_sig(theme: str, origin: str, dest: str, out_date: str) -> str:
    # we de-dupe per theme + origin + dest + outbound date
    key = f"{theme}|{origin}|{dest}|{out_date}"
    return hashlib.md5(key.encode("utf-8")).hexdigest()[:16]


def build_recent_sig_set(ws_log, hours: int) -> set[str]:
    """
    Optional: read FEEDER_LOG with headers:
      ts_utc | sig
    Keep only last N hours.
    If tab missing or schema unknown, return empty set (still de-dupes in-memory).
    """
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
        # non-fatal
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

    # Governor precedence: DUFFEL_* first, fallback to FEEDER_*
    MAX_SEARCHES = first_int("DUFFEL_MAX_SEARCHES_PER_RUN", "FEEDER_MAX_SEARCHES", default=12)
    MAX_INSERTS = first_int("DUFFEL_MAX_INSERTS", "FEEDER_MAX_INSERTS", default=50)
    MAX_PER_ORIGIN = env_int("DUFFEL_MAX_INSERTS_PER_ORIGIN", 15)
    MAX_PER_ROUTE = env_int("DUFFEL_MAX_INSERTS_PER_ROUTE", 5)

    DESTS_PER_RUN = env_int("DUFFEL_ROUTES_PER_RUN", 6)
    WINNERS_PER_RUN = env_int("WINNERS_PER_RUN", 2)  # used as soft target; we still append all valid offers found
    SLEEP = env_float("FEEDER_SLEEP_SECONDS", 0.1)

    DEDUPE_HOURS = env_int("DUFFEL_SEARCH_DEDUPE_HOURS", 4)

    # 90/10
    primary_pct = env_int("SLOT_PRIMARY_PCT", 90)
    primary_budget = max(0, min(MAX_SEARCHES, int(round(MAX_SEARCHES * (primary_pct / 100.0)))))
    secondary_budget = max(0, MAX_SEARCHES - primary_budget)

    # cabin
    cabin_class = env("FEEDER_CABIN_CLASS", "economy").lower() or "economy"

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

    theme_today = a1(OPS, "B5")
    if not theme_today:
        theme_today = env("THEME", "DEFAULT")
    log(f"🎯 Theme of day: {theme_today}")

    origins = get_theme_origins(theme_today)
    if not origins:
        raise RuntimeError("No origins available (ORIGINS_* / ORIGINS_DEFAULT empty)")

    max_stops = get_theme_max_stops(theme_today)
    wmin, wmax = get_theme_window_days(theme_today)
    tmin, tmax = get_theme_trip_days(theme_today)

    log(
        f"CAPS: searches={MAX_SEARCHES} inserts={MAX_INSERTS} per_origin={MAX_PER_ORIGIN} per_route={MAX_PER_ROUTE} "
        f"dests_per_run={DESTS_PER_RUN} winners_soft={WINNERS_PER_RUN} primary/secondary={primary_budget}/{secondary_budget}"
    )
    log(f"RULES: origins={','.join(origins)} | max_stops={max_stops} | window={wmin}-{wmax}d | trip={tmin}-{tmax}d | cabin={cabin_class}")
    log(f"DEDUPE: last {DEDUPE_HOURS}h")

    # Load CONFIG rows (small)
    cfg_all = records(CFG)
    cfg_rows = [
        r for r in cfg_all
        if (r.get("enabled") or "").strip().lower() in ("true", "1", "yes")
        and (r.get("destination_iata") or "").strip()
        and theme_matches_config_row(r, theme_today)
    ]
    if not cfg_rows:
        log("⚠️ No CONFIG destinations eligible for theme.")
        return 0

    # Compute Pi = Fi * Vi (Fi from search_weight, Vi from value_score)
    candidates: List[Dict[str, Any]] = []
    for r in cfg_rows:
        dest = (r.get("destination_iata") or "").strip().upper()
        Fi = fnum(r.get("search_weight") or r.get("feasibility_score") or "0", 0.0)
        Vi = fnum(r.get("value_score") or "0.5", 0.5)
        Pi = Fi * Vi
        rr = dict(r)
        rr["_dest"] = dest
        rr["_Fi"] = Fi
        rr["_Vi"] = Vi
        rr["_Pi"] = Pi
        candidates.append(rr)

    # Rank and keep top N destinations
    candidates.sort(key=lambda x: float(x.get("_Pi", 0.0)), reverse=True)
    candidates = candidates[: max(1, DESTS_PER_RUN)]

    # Read RAW header + deal_id col once
    raw_hdr = header(RAW)
    if not raw_hdr:
        raise RuntimeError("RAW_DEALS header row is empty")

    existing_deal_ids = set()
    if "deal_id" in raw_hdr:
        c = raw_hdr.index("deal_id") + 1
        col = col_to_a1(c)
        vals = RAW.get(f"{col}2:{col}")
        for row in vals:
            if row and row[0]:
                existing_deal_ids.add(row[0].strip())

    # Optional de-dupe set from FEEDER_LOG
    recent_sigs = build_recent_sig_set(ws_log, DEDUPE_HOURS)
    inrun_sigs = set()

    # Insert caps bookkeeping
    inserted = 0
    by_origin: Dict[str, int] = {}
    by_route: Dict[Tuple[str, str], int] = {}

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

    # Pre-generate outbound dates across window, deterministic with day seed
    seed = int(hashlib.md5(dt.datetime.utcnow().strftime("%Y%m%d").encode()).hexdigest()[:8], 16)
    rng = random.Random(seed)

    def pick_outbound_date() -> dt.date:
        d = rng.randint(wmin, wmax)
        return (dt.date.today() + dt.timedelta(days=d))

    def pick_trip_len() -> int:
        return rng.randint(tmin, tmax)

    # Lane planner (90/10)
    lane_seq = (["PRIMARY"] * primary_budget) + (["SECONDARY"] * secondary_budget)
    lane_seq = lane_seq[:MAX_SEARCHES]

    # Build query sequence across candidates with light rotation + diminishing returns
    dest_use: Dict[str, int] = {}

    def pick_candidate() -> Dict[str, Any]:
        best = None
        best_score = -1.0
        for r in candidates:
            dest = r["_dest"]
            used = dest_use.get(dest, 0)
            # diminishing returns
            Pi = float(r["_Pi"])
            score = Pi * ((1.0 - min(0.95, Pi)) ** used)
            if used >= 2:
                score *= 0.85
            if score > best_score:
                best_score = score
                best = r
        assert best is not None
        return best

    searches = 0
    appended_sigs: List[str] = []
    out_rows: List[Dict[str, str]] = []

    # Skips
    sk_dedupe = 0
    sk_no_offer = 0
    sk_non_gbp = 0
    sk_bad_price = 0
    sk_caps = 0

    for lane in lane_seq:
        if searches >= MAX_SEARCHES or inserted >= MAX_INSERTS:
            break

        r = pick_candidate()
        dest = r["_dest"]
        dest_use[dest] = dest_use.get(dest, 0) + 1

        # pick origin (rotate)
        origin = origins[(searches + seed) % len(origins)]

        out_date = pick_outbound_date()
        trip_len = pick_trip_len()
        ret_date = out_date + dt.timedelta(days=trip_len)

        sig = search_sig(theme_today, origin, dest, out_date.isoformat())
        if sig in recent_sigs or sig in inrun_sigs:
            sk_dedupe += 1
            continue
        inrun_sigs.add(sig)

        # lane connections (secondary relaxes stops by +1)
        lane_max_stops = max_stops if lane == "PRIMARY" else min(2, max_stops + 1)

        # Duffel uses max_connections not max_stops: max_connections == max_stops
        max_connections = lane_max_stops

        deal_id = make_deal_id(origin, dest, out_date.isoformat(), ret_date.isoformat(), cabin_class)
        if not can_insert(origin, dest, deal_id):
            sk_caps += 1
            continue

        included_airlines = normalise_airlines_csv(env("AIRLINES", ""))
        # You also had "included_airlines_csv" concept; if you later add per-theme/per-dest airline rules,
        # wire it here. For now: global airline allowlist is optional.
        # If AIRLINES is empty -> no filter -> Duffel ANY.

        log(
            f"🔎 Search {searches+1}/{MAX_SEARCHES} [{lane}] {origin}→{dest} "
            f"| Pi={float(r['_Pi']):.2f} | max_conn={max_connections} | trip={trip_len}d | window={wmin}-{wmax}"
        )

        resp = duffel_offer_request(
            origin=origin,
            dest=dest,
            out_date=out_date.isoformat(),
            ret_date=ret_date.isoformat(),
            max_connections=max_connections,
            cabin_class=cabin_class,
            included_airlines=included_airlines,
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
            "cabin_class": cabin_class,
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

        # stop early if we hit your “soft winners” goal and inserts are healthy
        if inserted >= WINNERS_PER_RUN and inserted >= 2:
            # keep going only if you want to fill up to MAX_INSERTS; otherwise end early
            # uncomment next line to end early:
            # break
            pass

        time.sleep(SLEEP)

    if not out_rows:
        log(
            "⚠️ No rows inserted.\n"
            f"SKIPS: dedupe={sk_dedupe} caps={sk_caps} no_offer={sk_no_offer} non_gbp={sk_non_gbp} bad_price={sk_bad_price}"
        )
        return 0

    # BULK append in one write
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
