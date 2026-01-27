# workers/ai_scorer.py
# FULL FILE REPLACEMENT — V4.8 scorer (RDV-authoritative, channel quotas, ZTB theme-of-day)
#
# Core contract (unchanged):
# - RAW_DEALS is canonical (writes happen here only)
# - RAW_DEALS_VIEW is read-only (intelligence / formulas)
# - Phrase selection happens ONCE at promotion time
# - Publishers never select language
#
# New contract additions:
# - RDV drives eligibility via worthiness_verdict / priority_score when available.
# - Channel quotas per run (defaults): PRO=1, VIP=2, FREE=1
# - Theme-of-day is computed from ZTB (enabled + in-season + deterministic rotation), with THEME_OVERRIDE.
# - Editorial tie-breaker: on SNOW days, if any non-commodity candidate is eligible, commodity gateways cannot win.
#
# What this scorer does NOT do:
# - It does not compute scores (price/theme/novelty/timing/fatigue) — RDV does.
# - It does not block searches — feeder explores; scorer promotes.
#
# Expected RDV columns (best effort):
# - status, deal_id (required)
# - destination_iata, dynamic_theme, hard_reject (recommended)
# - worthiness_verdict (recommended: FREE/VIP/PRO verdicts)
# - priority_score (recommended; fallback to worthiness_score)
#
# RAW must contain:
# - status, deal_id, ingested_at_utc, phrase_used, phrase_bank
#
# Optional (used if present):
# - destination_iata, gateway_type, worthiness_verdict, promo_hint

import os
import json
import hashlib
from datetime import datetime, timezone, timedelta, date

import gspread
from gspread.cell import Cell
from google.oauth2.service_account import Credentials


# -------------------------
# Sheet tabs / env
# -------------------------

RAW_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS")
VIEW_TAB = os.getenv("RAW_DEALS_VIEW_TAB", "RAW_DEALS_VIEW")
PHRASE_TAB = os.getenv("PHRASE_BANK_TAB", "PHRASE_BANK")

ZTB_TAB = os.getenv("ZTB_TAB", "ZTB")  # fallback to ZONE_THEME_BENCHMARKS if missing
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID")

SA_JSON = os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON")

# Timing controls
MIN_INGEST_AGE_SECONDS = int(os.getenv("MIN_INGEST_AGE_SECONDS", "90"))
ELIGIBLE_WINDOW_HOURS = int(os.getenv("ELIGIBLE_WINDOW_HOURS", "72"))  # 0 disables

# Channel quotas (per run)
PRO_WINNERS_PER_RUN = int(os.getenv("PRO_WINNERS_PER_RUN", "1"))
VIP_WINNERS_PER_RUN = int(os.getenv("VIP_WINNERS_PER_RUN", "2"))
FREE_WINNERS_PER_RUN = int(os.getenv("FREE_WINNERS_PER_RUN", "1"))

# Destination repeat block (applies to VIP/FREE by default; PRO can be allowed to bypass via env)
DEST_REPEAT_HOURS = int(os.getenv("DEST_REPEAT_HOURS", os.getenv("VARIETY_LOOKBACK_HOURS", "120")) or "120")
PRO_BYPASS_REPEAT_BLOCK = os.getenv("PRO_BYPASS_REPEAT_BLOCK", "TRUE").strip().upper() in ("TRUE", "1", "YES", "Y", "ON")

# Snow commodity tie-breaker
SNOW_THEME_KEY = os.getenv("SNOW_THEME_KEY", "snow")  # if your theme is "snow"
COMMODITY_GATEWAY_KEY = os.getenv("COMMODITY_GATEWAY_KEY", "commodity")

# Phrase columns in RAW
PHRASE_USED_COL = "phrase_used"
PHRASE_BANK_COL = "phrase_bank"


# -------------------------
# Minimal RDV columns (read-only)
# -------------------------

VIEW_REQUIRED_COLS = (
    "status",
    "deal_id",
    "destination_iata",
    "dynamic_theme",
    "hard_reject",
    "worthiness_verdict",
    "priority_score",
    "worthiness_score",     # fallback ranker if priority_score absent
)

RAW_OPTIONAL_DEST_COLS = ("destination_iata", "dest_iata")


# -------------------------
# Helpers
# -------------------------

def _log(msg):
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"{ts} | {msg}", flush=True)


def _sa_creds():
    if not SA_JSON:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
    raw = SA_JSON.strip()
    try:
        info = json.loads(raw)
    except json.JSONDecodeError:
        info = json.loads(raw.replace("\\n", "\n"))
    return Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )


def _norm(v):
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()
    return str(v).strip()


def _norm_theme(s):
    return _norm(s).lower().replace(" ", "_")


def _norm_iata(s):
    return _norm(s).upper()[:3]


def _truthy(v):
    return _norm(v).upper() in ("TRUE", "YES", "Y", "1", "APPROVED")


def _float_or_none(v):
    s = _norm(v)
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _parse_iso_utc(ts_raw):
    s = _norm(ts_raw)
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1]
        dtv = datetime.fromisoformat(s)
        if dtv.tzinfo is None:
            dtv = dtv.replace(tzinfo=timezone.utc)
        return dtv.astimezone(timezone.utc)
    except Exception:
        return None


def _ws_headers(ws):
    return [h.strip() for h in ws.row_values(1)]


def _stable_pick(key, items):
    if not items:
        return ""
    h = hashlib.md5(key.encode("utf-8")).hexdigest()
    idx = int(h[:8], 16) % len(items)
    return items[idx]


def _safe_view_rows(ws_view):
    """
    Read RAW_DEALS_VIEW without get_all_records(), to avoid duplicate-header crash.
    Keep FIRST occurrence of a header if duplicates exist.
    """
    values = ws_view.get_all_values()
    if not values or len(values) < 2:
        return []

    headers = [h.strip() for h in values[0]]
    first_idx = {}
    dupes = set()

    for i, h in enumerate(headers):
        if not h:
            continue
        if h in first_idx:
            dupes.add(h)
            continue
        first_idx[h] = i

    if dupes:
        _log(
            "⚠️ RAW_DEALS_VIEW has duplicate headers; scorer uses first occurrence for: "
            + ", ".join(sorted(dupes)[:30])
            + (" ..." if len(dupes) > 30 else "")
        )

    for must in ("status", "deal_id"):
        if must not in first_idx:
            _log(f"⚠️ RAW_DEALS_VIEW missing required '{must}'. Treating view as empty.")
            return []

    cols_to_map = [c for c in VIEW_REQUIRED_COLS if c in first_idx]

    rows = []
    for row in values[1:]:
        if len(row) < len(headers):
            row = row + [""] * (len(headers) - len(row))

        r = {}
        for c in cols_to_map:
            r[c] = row[first_idx[c]] if first_idx.get(c) is not None else ""
        rows.append(r)

    return rows


# -------------------------
# ZTB theme-of-day (deterministic)
# -------------------------

def _mmdd(d: date) -> int:
    return int(d.strftime("%m%d"))


def _in_window(today_mmdd: int, start_mmdd: int, end_mmdd: int) -> bool:
    if start_mmdd <= end_mmdd:
        return start_mmdd <= today_mmdd <= end_mmdd
    return (today_mmdd >= start_mmdd) or (today_mmdd <= end_mmdd)


def _eligible_themes_from_ztb(ztb_rows):
    t = _mmdd(datetime.now(timezone.utc).date())
    eligible = []
    for r in ztb_rows:
        theme = _norm(r.get("theme"))
        if not theme:
            continue
        if not _truthy(r.get("enabled")):
            continue
        try:
            start = int(float(_norm(r.get("start_mmdd") or "101")))
            end = int(float(_norm(r.get("end_mmdd") or "1231")))
        except Exception:
            start, end = 101, 1231
        if _in_window(t, start, end):
            eligible.append(theme)
    # stable unique
    out = []
    seen = set()
    for x in sorted(eligible):
        k = x.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(x)
    return out


def _theme_of_day(eligible):
    if not eligible:
        return "unexpected_value"
    anchor = date(2026, 1, 1)
    idx = (datetime.now(timezone.utc).date() - anchor).days % len(eligible)
    return eligible[idx]


def _theme_override(eligible):
    o = _norm(os.getenv("THEME_OVERRIDE", ""))
    if not o:
        return None
    for t in eligible:
        if t.lower() == o.lower():
            return t
    _log(f"⚠️ THEME_OVERRIDE '{o}' not in eligible pool. Ignoring.")
    return None


# -------------------------
# Destination repeat memory
# -------------------------

def _posted_destinations_recent(raw_rows, hours):
    if hours <= 0:
        return set()

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=hours)

    recent = set()
    for r in raw_rows:
        s = _norm(r.get("status"))
        if not (s.startswith("POSTED_") or s == "POSTED_ALL"):
            continue

        ts = _parse_iso_utc(r.get("ingested_at_utc"))
        if not ts or ts < cutoff:
            continue

        dest = ""
        for k in RAW_OPTIONAL_DEST_COLS:
            if _norm(r.get(k)):
                dest = _norm_iata(r.get(k))
                break
        if dest:
            recent.add(dest)
    return recent


# -------------------------
# Verdict normalization (RDV -> channels)
# -------------------------

def _classify_channel(verdict_raw: str) -> str:
    """
    Return one of: PRO, VIP, FREE, NONE
    We intentionally accept multiple naming styles so RDV can evolve without breaking scorer.
    """
    v = _norm(verdict_raw).upper()

    if v.startswith("PRO_WORTHY_") or v.startswith("PRO_") or v == "PRO":
        return "PRO"

    # VIP variants
    if v in ("VIP", "POSTABLE_VIP", "VIP_POSTABLE", "WORTHY_VIP") or v.startswith("VIP_"):
        return "VIP"

    # FREE variants
    if v in ("FREE", "POSTABLE_FREE", "FREE_POSTABLE", "WORTHY_FREE") or v.startswith("FREE_"):
        return "FREE"

    return "NONE"


def _rank_value(r):
    """
    Prefer priority_score, else worthiness_score, else -inf.
    Higher is better.
    """
    p = _float_or_none(r.get("priority_score"))
    if p is not None:
        return p
    w = _float_or_none(r.get("worthiness_score"))
    if w is not None:
        return w
    return -1e12


# -------------------------
# Main
# -------------------------

def main():
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")

    gc = gspread.authorize(_sa_creds())
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_raw = sh.worksheet(RAW_TAB)
    ws_view = sh.worksheet(VIEW_TAB)

    # ZTB (theme-of-day)
    try:
        ws_ztb = sh.worksheet(ZTB_TAB)
    except Exception:
        ws_ztb = sh.worksheet("ZONE_THEME_BENCHMARKS")

    ztb_rows = ws_ztb.get_all_records()
    eligible_themes = _eligible_themes_from_ztb(ztb_rows)
    theme_today = _theme_of_day(eligible_themes)
    ovr = _theme_override(eligible_themes)
    if ovr:
        theme_today = ovr

    _log(f"✅ ZTB: eligible_today={len(eligible_themes)} | theme_today={theme_today} | pool={eligible_themes}")

    # Phrase bank (optional)
    try:
        ws_phrase = sh.worksheet(PHRASE_TAB)
        phrase_rows = ws_phrase.get_all_records()
    except Exception as e:
        _log(f"PHRASE_BANK not readable: {e}")
        phrase_rows = []

    phrase_index = []
    for r in phrase_rows:
        theme = _norm_theme(r.get("theme"))
        phrase = _norm(r.get("phrase"))
        approved = _truthy(r.get("approved"))

        dest = ""
        cat = _norm(r.get("category")).lower()
        if cat.startswith("dest:"):
            dest = _norm_iata(cat.split("dest:", 1)[1])

        if dest and theme and phrase and approved:
            phrase_index.append({"dest": dest, "theme": theme, "phrase": phrase})

    # RAW rows (canonical)
    raw_rows = ws_raw.get_all_records()
    headers = _ws_headers(ws_raw)
    col = {h: i + 1 for i, h in enumerate(headers)}

    for required in ("status", "deal_id", "ingested_at_utc", PHRASE_USED_COL, PHRASE_BANK_COL):
        if required not in col:
            raise RuntimeError(f"Missing required RAW_DEALS column: {required}")

    # deal_id -> RAW row number and raw record
    deal_rownum = {}
    deal_raw = {}
    for idx, r in enumerate(raw_rows, start=2):
        did = _norm(r.get("deal_id"))
        if did:
            deal_rownum[did] = idx
            deal_raw[did] = r

    # View rows (safe)
    view_rows = _safe_view_rows(ws_view)

    # Candidate filtering (NEW only, aged, in-window)
    now = datetime.now(timezone.utc)
    min_allowed_ts = None
    if ELIGIBLE_WINDOW_HOURS and ELIGIBLE_WINDOW_HOURS > 0:
        min_allowed_ts = now - timedelta(hours=ELIGIBLE_WINDOW_HOURS)

    skipped = {"too_fresh": 0, "no_ingest_ts": 0, "too_old": 0, "missing_raw_row": 0}
    candidates = []

    for r in view_rows:
        if _norm(r.get("status")) != "NEW":
            continue

        did = _norm(r.get("deal_id"))
        if not did:
            continue
        if did not in deal_rownum:
            skipped["missing_raw_row"] += 1
            continue

        rawrec = deal_raw.get(did, {})
        ts = _parse_iso_utc(rawrec.get("ingested_at_utc"))
        if not ts:
            skipped["no_ingest_ts"] += 1
            continue

        if (now - ts).total_seconds() < MIN_INGEST_AGE_SECONDS:
            skipped["too_fresh"] += 1
            continue

        if min_allowed_ts and ts < min_allowed_ts:
            skipped["too_old"] += 1
            continue

        dest = _norm_iata(r.get("destination_iata")) or _norm_iata(rawrec.get("destination_iata"))
        verdict_raw = _norm(r.get("worthiness_verdict")) or _norm(rawrec.get("worthiness_verdict"))
        channel = _classify_channel(verdict_raw)

        hard_reject = _truthy(r.get("hard_reject")) or _truthy(rawrec.get("hard_reject"))

        # gateway_type is sourced from RAW (feeder passed through CONFIG). RDV may not include it.
        gateway_type = _norm(rawrec.get("gateway_type")).lower()

        # dynamic_theme for phrase pick; fall back to deal_theme / theme
        theme_candidate = _norm(r.get("dynamic_theme"))
        if not theme_candidate:
            theme_candidate = _norm(rawrec.get("deal_theme")) or _norm(rawrec.get("theme"))
        theme_norm = _norm_theme(theme_candidate)

        candidates.append({
            "did": did,
            "rownum": deal_rownum[did],
            "dest": dest,
            "hard_reject": hard_reject,
            "verdict_raw": verdict_raw,
            "channel": channel,
            "rank": _rank_value(r),
            "theme": theme_norm,
            "gateway_type": gateway_type,
        })

    _log(
        f"Eligible NEW candidates: {len(candidates)} | skipped_too_fresh={skipped['too_fresh']} "
        f"skipped_no_ingest_ts={skipped['no_ingest_ts']} skipped_too_old={skipped['too_old']} "
        f"skipped_missing_raw_row={skipped['missing_raw_row']}"
    )

    if not candidates:
        _log("No eligible NEW rows")
        return 0

    # Repeat-block memory (VIP/FREE; PRO optionally bypass)
    recent_posted_dests = _posted_destinations_recent(raw_rows, DEST_REPEAT_HOURS)
    _log(f"Repeat block window: {DEST_REPEAT_HOURS}h | recent_posted_dests={len(recent_posted_dests)}")

    def repeat_blocked(c):
        return bool(c["dest"]) and (c["dest"] in recent_posted_dests)

    # Split pools by channel
    pro_pool = []
    vip_pool = []
    free_pool = []
    none_pool = []

    for c in candidates:
        if c["channel"] == "PRO":
            pro_pool.append(c)
        elif c["channel"] == "VIP":
            vip_pool.append(c)
        elif c["channel"] == "FREE":
            free_pool.append(c)
        else:
            none_pool.append(c)

    # Apply hard_reject + repeat blocks where appropriate
    vip_pool = [c for c in vip_pool if not c["hard_reject"]]
    free_pool = [c for c in free_pool if not c["hard_reject"]]

    vip_pool = [c for c in vip_pool if not repeat_blocked(c)]
    free_pool = [c for c in free_pool if not repeat_blocked(c)]

    if not PRO_BYPASS_REPEAT_BLOCK:
        pro_pool = [c for c in pro_pool if not repeat_blocked(c)]

    # SNOW non-commodity tie-breaker:
    # If theme_today is snow AND there exists ANY eligible non-commodity VIP/FREE/PRO candidate,
    # then commodity gateways are disallowed as winners for this run.
    snow_day = (_norm_theme(theme_today) == _norm_theme(SNOW_THEME_KEY))

    non_commodity_exists = any(
        (c.get("gateway_type") and c["gateway_type"] != COMMODITY_GATEWAY_KEY)
        for c in (pro_pool + vip_pool + free_pool)
    )
    if snow_day and non_commodity_exists:
        def _is_commodity(c):
            return (c.get("gateway_type") == COMMODITY_GATEWAY_KEY)

        pro_pool = [c for c in pro_pool if not _is_commodity(c)]
        vip_pool = [c for c in vip_pool if not _is_commodity(c)]
        free_pool = [c for c in free_pool if not _is_commodity(c)]
        _log("SNOW rule: non-commodity candidates exist → commodity gateways disallowed for winner selection this run.")

    # Rank pools (descending)
    pro_pool.sort(key=lambda x: (-x["rank"], x["did"]))
    vip_pool.sort(key=lambda x: (-x["rank"], x["did"]))
    free_pool.sort(key=lambda x: (-x["rank"], x["did"]))

    _log(f"Pools after gates: PRO={len(pro_pool)} | VIP={len(vip_pool)} | FREE={len(free_pool)} | NONE={len(none_pool)}")

    winners = []

    # Select winners by quotas (PRO first, then VIP, then FREE)
    if PRO_WINNERS_PER_RUN > 0:
        winners.extend(pro_pool[:PRO_WINNERS_PER_RUN])

    if VIP_WINNERS_PER_RUN > 0:
        winners.extend(vip_pool[:VIP_WINNERS_PER_RUN])

    if FREE_WINNERS_PER_RUN > 0:
        winners.extend(free_pool[:FREE_WINNERS_PER_RUN])

    # Deduplicate winners by deal_id (in case verdict labeling overlaps)
    dedup = []
    seen = set()
    for w in winners:
        if w["did"] in seen:
            continue
        seen.add(w["did"])
        dedup.append(w)
    winners = dedup

    if not winners:
        _log("No winners selected after quotas + gates. Marking eligible NEW as SCORED.")
    else:
        _log("Winners selected: " + ", ".join([f"{w['channel']}:{w['did']}" for w in winners]))

    winner_ids = {w["did"] for w in winners}

    # Updates:
    # 1) Mark evaluated candidates as SCORED (except winners)
    # 2) Promote winners to READY_TO_POST + lock phrase
    updates = []
    scored_count = 0

    for c in candidates:
        if c["did"] in winner_ids:
            continue
        updates.append(Cell(c["rownum"], col["status"], "SCORED"))
        scored_count += 1

    promoted = 0
    for w in winners:
        did = w["did"]
        rownum = w["rownum"]
        dest = w["dest"]
        theme = w["theme"]  # RDV-derived theme label for phrase selection

        phrases = sorted([p["phrase"] for p in phrase_index if p["dest"] == dest and p["theme"] == theme])
        phrase = _stable_pick(f"{did}|{dest}|{theme}", phrases)

        updates.append(Cell(rownum, col["status"], "READY_TO_POST"))
        updates.append(Cell(rownum, col[PHRASE_USED_COL], phrase))
        updates.append(Cell(rownum, col[PHRASE_BANK_COL], phrase))

        # Optional: write worthiness_verdict into RAW if column exists (helps ops visibility)
        if "worthiness_verdict" in col and w["verdict_raw"]:
            updates.append(Cell(rownum, col["worthiness_verdict"], w["verdict_raw"]))

        # Optional: promo_hint for PRO
        if w["channel"] == "PRO":
            promo_col = col.get("promo_hint") or col.get("PROMO_HINT")
            if promo_col:
                updates.append(Cell(rownum, promo_col, "PRO_EDITORIAL"))

        promoted += 1
        _log(
            f"Promoted {did} → READY_TO_POST | channel={w['channel']} dest={dest} theme={theme} "
            f"rank={w['rank']:.2f} phrase={'YES' if phrase else 'NO'}"
        )

    _log(f"Marked SCORED (evaluated non-winners): {scored_count} | winners_promoted: {promoted}")

    if updates:
        ws_raw.update_cells(updates, value_input_option="RAW")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
