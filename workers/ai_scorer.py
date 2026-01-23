# workers/ai_scorer.py
# V4.7.3+PRO — scorer: NEW -> (READY_TO_POST for winners) else -> SCORED
# Contract:
# - RAW_DEALS is canonical (writes happen here only)
# - RAW_DEALS_VIEW is read-only (intelligence / formulas)
# - Phrase selection happens ONCE at promotion time
# - Publishers never select language
# - Full-file replacement only
#
# PRO additions (minimal, deterministic):
# - Compute PRO_score from RDV identity signals (no price_value dependency)
# - Weekly rarity gate (sheet-state only)
# - Allow one PRO “rescue” promotion when gate is open (can override hard_reject)
# - Optional: write worthiness_verdict/PROMO_HINT/PRO_score to RAW if columns exist

import os
import json
import hashlib
from datetime import datetime, timezone, timedelta

import gspread
from gspread.cell import Cell
from google.oauth2.service_account import Credentials


RAW_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS")
VIEW_TAB = os.getenv("RAW_DEALS_VIEW_TAB", "RAW_DEALS_VIEW")
PHRASE_TAB = os.getenv("PHRASE_BANK_TAB", "PHRASE_BANK")

CAPABILITY_TAB = os.getenv("CAPABILITY_TAB", "ROUTE_CAPABILITY_MAP")
SIGNALS_TAB = os.getenv("SIGNALS_TAB", os.getenv("CONFIG_SIGNALS_TAB", "CONFIG_SIGNALS"))

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID")
SA_JSON = os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON")

MIN_INGEST_AGE_SECONDS = int(os.getenv("MIN_INGEST_AGE_SECONDS", "90"))
WINNERS_PER_RUN = int(os.getenv("WINNERS_PER_RUN", "1"))

# Optional freshness window: only consider NEW rows ingested within this many hours.
# Set 0 or blank to disable.
ELIGIBLE_WINDOW_HOURS = int(os.getenv("ELIGIBLE_WINDOW_HOURS", "72"))

# Optional winner threshold (only applied if worthiness_score exists)
MIN_WORTHINESS_SCORE = float(os.getenv("MIN_WORTHINESS_SCORE", "0"))

PHRASE_USED_COL = "phrase_used"
PHRASE_BANK_COL = "phrase_bank"

# -------------------------
# PRO controls (safe defaults)
# -------------------------
PRO_ENABLED = os.getenv("PRO_ENABLED", "TRUE").strip().upper() in ("TRUE", "1", "YES", "Y", "ON")
PRO_RARITY_DAYS = int(os.getenv("PRO_RARITY_DAYS", "7"))

# Calibrated thresholds (Claude-style defaults)
THETA_PRO_REVIEW = float(os.getenv("THETA_PRO_REVIEW", "50.0"))
THETA_PRO_PRIORITY = float(os.getenv("THETA_PRO_PRIORITY", "56.7"))

# “Reasonableness” gate to prevent nonsense (only applied if price is present)
# If blank/0 → disabled
PRO_MAX_PRICE_GBP = float(os.getenv("PRO_MAX_PRICE_GBP", "0") or "0")

# If you want PRO to “steal” a run only when VIP winner is weak, set e.g. 66.
# If 0 → PRO can promote whenever gate open + threshold met.
PRO_ONLY_IF_VIP_BELOW = float(os.getenv("PRO_ONLY_IF_VIP_BELOW", "0") or "0")


# View columns we actually need (minimizes coupling to RDV header chaos)
# NOTE: Expanded to support PRO_score.
VIEW_REQUIRED_COLS = (
    "status",
    "deal_id",
    "destination_iata",
    "dynamic_theme",
    "hard_reject",
    "worthiness_score",
    # PRO signals (best-effort; missing columns will safely disable PRO path)
    "theme_fit_score",
    "novelty_score",
    "timing_score",
    "fatigue_penalty",
    "stops",
    "price_gbp",
)


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


def _norm(s):
    """Return a trimmed string for any input type (int-safe)."""
    if s is None:
        return ""
    if isinstance(s, str):
        return s.strip()
    return str(s).strip()


def _norm_theme(s):
    return _norm(s).lower().replace(" ", "_")


def _norm_iata(s):
    return _norm(s).upper()


def _truthy(v):
    return _norm(v).upper() in ("TRUE", "YES", "Y", "1", "APPROVED")


def _stable_pick(key, items):
    if not items:
        return ""
    h = hashlib.md5(key.encode("utf-8")).hexdigest()
    idx = int(h[:8], 16) % len(items)
    return items[idx]


def _parse_iso_utc(ts_raw):
    """
    Accepts '2026-01-01T12:34:56Z' or without 'Z'.
    Returns aware UTC datetime or None.
    """
    s = _norm(ts_raw)
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1]
        dt = datetime.fromisoformat(s)
        # assume UTC if naive
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _float_or_none(v):
    s = _norm(v)
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _int_or_none(v):
    s = _norm(v)
    if not s:
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def _clean_iata(x):
    return _norm(x).upper()[:3]


def _ws_headers(ws):
    return [h.strip() for h in ws.row_values(1)]


def _safe_view_rows(ws_view):
    """
    Read RAW_DEALS_VIEW without get_all_records(), to avoid:
      gspread.exceptions.GSpreadException: the header row in the worksheet is not unique

    We only map the required columns. If duplicate headers exist, we keep the FIRST occurrence.
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
            "⚠️ RAW_DEALS_VIEW has duplicate headers (scorer will ignore duplicates, using first occurrence): "
            + ", ".join(sorted(dupes)[:30])
            + (" ..." if len(dupes) > 30 else "")
        )

    # Hard requirement for scoring pipeline to run
    must_have = ("status", "deal_id")
    missing_must = [c for c in must_have if c not in first_idx]
    if missing_must:
        _log(f"⚠️ RAW_DEALS_VIEW missing required columns for scoring: {missing_must}. Treating as empty.")
        return []

    # Soft requirements for VIP/PRO intelligence
    # We'll map whatever exists; missing PRO cols will just disable PRO scoring
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
# PRO scoring (calibrated, identity-first)
# Uses only RDV columns; ignores price_value_score entirely.
# -------------------------
def _clip(x, lo=0.0, hi=100.0):
    try:
        if x is None:
            return lo
        return max(lo, min(hi, float(x)))
    except Exception:
        return lo


def _theme_rarity_points(theme_norm):
    # Points, not multipliers (keeps calibration stable)
    t = _norm_theme(theme_norm)
    if t == "long_haul":
        return 25.0
    if t == "city_breaks":
        return 25.0
    if t == "winter_sun":
        return 20.0
    if t == "luxury_value":
        return 15.0
    # volume themes get no boost
    return 0.0


def calculate_pro_score(theme_fit, novelty, stops, fatigue, theme_norm):
    """
    Claude-calibrated, percentile-friendly score.
    Outputs ~[0..~60] on your observed distributions; thresholds live in env.
    """
    tf = _float_or_none(theme_fit)
    nv = _float_or_none(novelty)
    st = _int_or_none(stops)
    ft = _float_or_none(fatigue)

    # Normalise theme_fit [70..100] → [0..40]
    if tf is None:
        tf_part = 0.0
    else:
        tf_part = _clip((tf - 70.0) / 30.0 * 40.0, 0.0, 40.0)

    # Normalise novelty [60..100] → [0..25]
    if nv is None:
        nv_part = 0.0
    else:
        nv_part = _clip((nv - 60.0) / 40.0 * 25.0, 0.0, 25.0)

    # Nonstop bonus / connection kill-switch
    if st is None:
        conn_part = 0.0
    elif st == 0:
        conn_part = 10.0
    elif st == 1:
        conn_part = 0.0
    else:
        conn_part = -50.0

    rarity_part = _theme_rarity_points(theme_norm)

    # Fatigue penalty scaled down
    if ft is None:
        fat_part = 0.0
    else:
        fat_part = float(ft) * 0.5

    score = tf_part + nv_part + conn_part + rarity_part - fat_part
    return _clip(score, 0.0, 100.0)


def _pro_verdict_for(score):
    if score is None:
        return ""
    if score >= THETA_PRO_PRIORITY:
        return "PRO_WORTHY_PRIORITY"
    if score >= THETA_PRO_REVIEW:
        return "PRO_WORTHY_REVIEW"
    return ""


def _pro_rarity_gate_open(raw_rows, raw_headers_map):
    """
    Sheet-state only: if any PRO_WORTHY_* has been posted in last PRO_RARITY_DAYS → gate closed.
    Uses whatever columns exist; if required columns missing, default OPEN (fail-soft).
    """
    if PRO_RARITY_DAYS <= 0:
        return True

    col_verdict = raw_headers_map.get("worthiness_verdict")
    col_status = raw_headers_map.get("status")
    col_ts = raw_headers_map.get("ingested_at_utc")  # fallback timestamp

    if not col_verdict or not col_status or not col_ts:
        _log("⚠️ PRO rarity gate: missing RAW columns (worthiness_verdict/status/ingested_at_utc). Gate OPEN (fail-soft).")
        return True

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=PRO_RARITY_DAYS)

    # raw_rows here are dicts (from get_all_records)
    for r in raw_rows:
        v = _norm(r.get("worthiness_verdict"))
        s = _norm(r.get("status"))
        if not v.startswith("PRO_WORTHY_"):
            continue
        if not (s.startswith("POSTED_") or s == "POSTED_ALL"):
            continue
        ts = _parse_iso_utc(r.get("ingested_at_utc"))
        if ts and ts >= cutoff:
            return False
    return True


def main():
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")

    gc = gspread.authorize(_sa_creds())
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_raw = sh.worksheet(RAW_TAB)
    ws_view = sh.worksheet(VIEW_TAB)

    # Load phrase bank (optional; system must not break if unavailable)
    try:
        ws_phrase = sh.worksheet(PHRASE_TAB)
        phrase_rows = ws_phrase.get_all_records()
    except Exception as e:
        _log(f"PHRASE_BANK not readable: {e}")
        phrase_rows = []

    # Build phrase index: (dest_iata, theme) -> [phrases...]
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

    # SAFE read VIEW rows (prevents duplicate-header crash)
    view_rows = _safe_view_rows(ws_view)

    # RAW still uses get_all_records() (RAW headers must be unique for canonical writes)
    raw_rows = ws_raw.get_all_records()

    headers = _ws_headers(ws_raw)
    col = {h: i + 1 for i, h in enumerate(headers)}

    # Hard requirements (fail fast with a clear message)
    for required in ("status", "deal_id", "ingested_at_utc", PHRASE_USED_COL, PHRASE_BANK_COL):
        if required not in col:
            raise RuntimeError(f"Missing required RAW_DEALS column: {required}")

    now = datetime.now(timezone.utc)
    min_allowed_ts = None
    if ELIGIBLE_WINDOW_HOURS and ELIGIBLE_WINDOW_HOURS > 0:
        min_allowed_ts = now - timedelta(hours=ELIGIBLE_WINDOW_HOURS)

    # Map deal_id -> RAW_DEALS row number and ingested_at
    deal_row = {}
    deal_ingested = {}
    deal_raw_record = {}
    for idx, r in enumerate(raw_rows, start=2):
        did = _norm(r.get("deal_id"))
        if did:
            deal_row[did] = idx
            deal_ingested[did] = r.get("ingested_at_utc")
            deal_raw_record[did] = r

    # Build candidate list from VIEW rows with status=NEW
    candidates = []
    skipped = {
        "too_fresh": 0,
        "no_ingest_ts": 0,
        "too_old": 0,
        "missing_raw_row": 0,
    }

    for r in view_rows:
        if _norm(r.get("status")) != "NEW":
            continue

        did = _norm(r.get("deal_id"))
        if not did:
            continue
        if did not in deal_row:
            skipped["missing_raw_row"] += 1
            continue

        ts = _parse_iso_utc(deal_ingested.get(did))
        if not ts:
            skipped["no_ingest_ts"] += 1
            continue

        if (now - ts).total_seconds() < MIN_INGEST_AGE_SECONDS:
            skipped["too_fresh"] += 1
            continue

        if min_allowed_ts and ts < min_allowed_ts:
            skipped["too_old"] += 1
            continue

        # Intelligence fields (best-effort; don't hard fail if missing)
        hard_reject = _truthy(r.get("hard_reject"))
        worth = _float_or_none(r.get("worthiness_score"))

        # Theme fallback: VIEW.dynamic_theme -> RAW.deal_theme -> RAW.theme
        rawrec = deal_raw_record.get(did, {})
        theme_candidate = _norm(r.get("dynamic_theme"))
        if not theme_candidate:
            theme_candidate = _norm(rawrec.get("deal_theme")) or _norm(rawrec.get("theme"))

        candidates.append(
            {
                "did": did,
                "rownum": deal_row[did],
                "dest": _norm_iata(r.get("destination_iata")),
                "theme": _norm_theme(theme_candidate),
                "hard_reject": hard_reject,
                "worthiness": worth,
                # PRO signals (may be blank)
                "theme_fit_score": r.get("theme_fit_score", ""),
                "novelty_score": r.get("novelty_score", ""),
                "timing_score": r.get("timing_score", ""),
                "fatigue_penalty": r.get("fatigue_penalty", ""),
                "stops": r.get("stops", ""),
                "price_gbp": r.get("price_gbp", ""),
            }
        )

    _log(
        "Eligible NEW candidates: "
        f"{len(candidates)} | skipped_too_fresh={skipped['too_fresh']} "
        f"skipped_no_ingest_ts={skipped['no_ingest_ts']} skipped_too_old={skipped['too_old']} "
        f"skipped_missing_raw_row={skipped['missing_raw_row']}"
    )

    if not candidates:
        _log("No eligible NEW rows")
        return 0

    # -------------------------
    # Winner selection
    # -------------------------

    # Existing VIP pool: never pick hard_reject
    vip_pool = [c for c in candidates if not c["hard_reject"]]

    # PRO pool: can include hard_reject (that's the whole point),
    # but still needs enough identity signals to score.
    pro_pool = list(candidates) if PRO_ENABLED else []

    # Log intelligence availability
    has_worthiness = sum(1 for c in vip_pool if c["worthiness"] is not None)
    _log(
        f"Intelligence check: {has_worthiness}/{len(vip_pool)} VIP-eligible candidates have worthiness_score. "
        f"{'✅ Using scores for ranking' if has_worthiness > 0 else '⚠️ Random ranking (no scores)'}"
    )

    # VIP ranking (unchanged)
    def _vip_sort_key(c):
        w = c["worthiness"]
        if w is None:
            w = -1e9
        return (-w, c["did"])

    vip_pool.sort(key=_vip_sort_key)
    vip_best = vip_pool[0] if vip_pool else None

    # PRO scoring + ranking
    pro_best = None
    pro_best_score = None
    pro_best_verdict = ""

    if PRO_ENABLED and pro_pool:
        # Gate open?
        gate_open = _pro_rarity_gate_open(raw_rows, col)
        _log(f"PRO gate: {'OPEN' if gate_open else 'CLOSED'} | rarity_days={PRO_RARITY_DAYS}")

        if gate_open:
            scored = []
            for c in pro_pool:
                # Optional “reasonableness” price cap
                price = _float_or_none(c.get("price_gbp"))
                if PRO_MAX_PRICE_GBP and PRO_MAX_PRICE_GBP > 0 and price is not None and price > PRO_MAX_PRICE_GBP:
                    continue

                ps = calculate_pro_score(
                    c.get("theme_fit_score"),
                    c.get("novelty_score"),
                    c.get("stops"),
                    c.get("fatigue_penalty"),
                    c.get("theme"),
                )
                c["pro_score"] = ps
                scored.append(c)

            scored.sort(key=lambda x: (-x.get("pro_score", 0.0), x["did"]))
            if scored:
                pro_best = scored[0]
                pro_best_score = pro_best.get("pro_score")
                pro_best_verdict = _pro_verdict_for(pro_best_score or 0.0)

                _log(
                    f"Top PRO candidate: did={pro_best['did']} score={pro_best_score:.2f} "
                    f"theme={pro_best['theme']} hard_reject={pro_best['hard_reject']}"
                )

    # Decide final winners list (still respects WINNERS_PER_RUN)
    winners = []

    # Policy:
    # - If PRO candidate clears threshold AND (optional) VIP best is weak → pick PRO
    # - Else pick VIP as before
    if WINNERS_PER_RUN != 1:
        _log("⚠️ PRO logic is designed for WINNERS_PER_RUN=1. Continuing, but behavior may be surprising.")

    choose_pro = False
    if pro_best and pro_best_verdict:
        if PRO_ONLY_IF_VIP_BELOW and PRO_ONLY_IF_VIP_BELOW > 0:
            vip_w = vip_best["worthiness"] if vip_best and vip_best["worthiness"] is not None else -1e9
            if vip_w < PRO_ONLY_IF_VIP_BELOW:
                choose_pro = True
        else:
            choose_pro = True

    if choose_pro and len(winners) < WINNERS_PER_RUN:
        winners.append(pro_best)
        _log(f"Winner selection: PRO ({pro_best_verdict}) promoted this run.")
    else:
        if vip_best:
            # Apply existing MIN_WORTHINESS_SCORE filter
            if vip_best["worthiness"] is None or vip_best["worthiness"] >= MIN_WORTHINESS_SCORE:
                winners.append(vip_best)
                _log("Winner selection: VIP promoted this run.")
            else:
                _log("Winner selection: VIP best below MIN_WORTHINESS_SCORE; no promotion.")
        else:
            _log("Winner selection: No VIP-eligible candidates (all hard_reject or empty). No promotion.")

    winner_ids = {w["did"] for w in winners}

    # Updates:
    # 1) Mark all evaluated candidates as SCORED (except winners)
    # 2) Promote winners to READY_TO_POST + lock phrase
    # 3) Best-effort: write PRO fields if columns exist
    updates = []

    scored_count = 0
    for c in candidates:
        if c["did"] in winner_ids:
            continue
        updates.append(Cell(c["rownum"], col["status"], "SCORED"))
        scored_count += 1

    promoted = 0
    for c in winners:
        did = c["did"]
        rownum = c["rownum"]
        dest = c["dest"]
        theme = c["theme"]

        phrases = sorted([p["phrase"] for p in phrase_index if p["dest"] == dest and p["theme"] == theme])
        phrase = _stable_pick(f"{did}|{dest}|{theme}", phrases)

        updates.append(Cell(rownum, col["status"], "READY_TO_POST"))
        updates.append(Cell(rownum, col[PHRASE_USED_COL], phrase))
        updates.append(Cell(rownum, col[PHRASE_BANK_COL], phrase))

        # If this was a PRO winner, best-effort mark fields on RAW (only if columns exist)
        if c is pro_best and pro_best_verdict:
            pro_score_col = col.get("PRO_score") or col.get("pro_score")
            verdict_col = col.get("worthiness_verdict")
            promo_col = col.get("PROMO_HINT") or col.get("promo_hint")

            if pro_score_col:
                updates.append(Cell(rownum, pro_score_col, f"{pro_best_score:.2f}"))
            if verdict_col:
                updates.append(Cell(rownum, verdict_col, pro_best_verdict))
            if promo_col:
                updates.append(Cell(rownum, promo_col, "PRO_EDITORIAL"))

        promoted += 1
        _log(
            f"Promoted {did} → READY_TO_POST | dest={dest} theme={theme} "
            f"worthiness={c['worthiness'] if c['worthiness'] is not None else 'NA'} "
            f"phrase={'YES' if phrase else 'NO'}"
        )

    _log(f"Marked SCORED (evaluated non-winners): {scored_count} | winners_promoted: {promoted}")

    if updates:
        ws_raw.update_cells(updates, value_input_option="RAW")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
