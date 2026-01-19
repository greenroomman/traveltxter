# V4.7.2 — scorer: NEW -> (READY_TO_POST for winners) else -> SCORED
# Contract:
# - RAW_DEALS is canonical (writes happen here only)
# - RAW_DEALS_VIEW is read-only (intelligence / formulas)
# - Phrase selection happens ONCE at promotion time
# - Publishers never select language
# - Full-file replacement only
#
# Phrase rotation (IMPLEMENTED):
# - PHRASE_BANK is the source of truth (approved-only)
# - Key = (destination_iata, dynamic_theme)
# - Enforce max_per_month using RAW_DEALS history (last 30 days, ingested_at_utc)
# - Deterministic pick: prefer under-limit phrases, then least-used, stable tie-break (no randomness)

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

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID")
SA_JSON = os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON")

MIN_INGEST_AGE_SECONDS = int(os.getenv("MIN_INGEST_AGE_SECONDS", "90"))
WINNERS_PER_RUN = int(os.getenv("WINNERS_PER_RUN", "1"))

# Optional freshness window: only consider NEW rows ingested within this many hours.
# Set 0 or blank to disable.
ELIGIBLE_WINDOW_HOURS = int(os.getenv("ELIGIBLE_WINDOW_HOURS", "72"))

# Optional winner threshold (only applied if worthiness_score exists)
MIN_WORTHINESS_SCORE = float(os.getenv("MIN_WORTHINESS_SCORE", "0"))

# Phrase rotation window (days) for max_per_month enforcement
PHRASE_WINDOW_DAYS = int(os.getenv("PHRASE_WINDOW_DAYS", "30"))

PHRASE_USED_COL = "phrase_used"
PHRASE_BANK_COL = "phrase_bank"


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


def _md5_int(s: str) -> int:
    h = hashlib.md5(s.encode("utf-8")).hexdigest()
    return int(h[:12], 16)


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


def _int_or_default(v, default: int) -> int:
    s = _norm(v)
    if not s:
        return default
    try:
        return int(float(s))
    except Exception:
        return default


def _get_row_theme(raw_row: dict) -> str:
    """
    For phrase usage counting we need a theme dimension.
    RAW_DEALS may not contain dynamic_theme; prefer it if present, else fall back to deal_theme.
    """
    if "dynamic_theme" in raw_row:
        t = _norm_theme(raw_row.get("dynamic_theme"))
        if t:
            return t
    return _norm_theme(raw_row.get("deal_theme"))


def _build_phrase_candidates(phrase_rows):
    """
    Build phrase candidates indexed by (dest, theme).
    PHRASE_BANK headers expected (as per your sheet):
      destination_iata, theme, category, phrase, approved, channel_hint, max_per_month, notes

    Back-compat:
      If destination_iata blank, allow category='dest:XXX' convention.
    """
    idx = {}  # (dest, theme) -> list[{phrase, max_per_month}]
    dropped = {"not_approved": 0, "missing_fields": 0}

    for r in phrase_rows:
        phrase = _norm(r.get("phrase"))
        approved = _truthy(r.get("approved"))
        if not approved:
            dropped["not_approved"] += 1
            continue

        theme = _norm_theme(r.get("theme"))

        # Primary: destination_iata column
        dest = _norm_iata(r.get("destination_iata"))

        # Fallback: category like "dest:LIS"
        if not dest:
            cat = _norm(r.get("category")).lower()
            if cat.startswith("dest:"):
                dest = _norm_iata(cat.split("dest:", 1)[1])

        if not (dest and theme and phrase):
            dropped["missing_fields"] += 1
            continue

        max_pm = _int_or_default(r.get("max_per_month"), 999)

        key = (dest, theme)
        idx.setdefault(key, []).append({"phrase": phrase, "max_per_month": max_pm})

    # Deterministic ordering: sort by phrase text
    for k in idx:
        idx[k].sort(key=lambda x: x["phrase"])

    _log(
        f"PHRASE_BANK candidates loaded: keys={len(idx)} "
        f"dropped_not_approved={dropped['not_approved']} dropped_missing_fields={dropped['missing_fields']}"
    )
    return idx


def _build_phrase_usage(raw_rows, now_utc: datetime, window_days: int):
    """
    Count phrase usage in RAW_DEALS within a time window.
    Uses ingested_at_utc as the timestamp anchor.

    Returns:
      usage[(dest, theme, phrase)] = count
    """
    usage = {}
    cutoff = now_utc - timedelta(days=window_days)

    for r in raw_rows:
        ts = _parse_iso_utc(r.get("ingested_at_utc"))
        if not ts or ts < cutoff:
            continue

        dest = _norm_iata(r.get("destination_iata"))
        if not dest:
            continue

        theme = _get_row_theme(r)
        if not theme:
            continue

        phrase = _norm(r.get(PHRASE_BANK_COL) or r.get(PHRASE_USED_COL))
        if not phrase:
            continue

        key = (dest, theme, phrase)
        usage[key] = usage.get(key, 0) + 1

    return usage


def _select_phrase(dest: str, theme: str, deal_id: str, candidates_for_key, usage_map):
    """
    Deterministic phrase selection.

    Rules:
    1) Prefer phrases under max_per_month (based on last PHRASE_WINDOW_DAYS in RAW_DEALS).
    2) Among eligible, choose least-used.
    3) Tie-break deterministically by md5(deal_id|phrase) then phrase text.
    4) If none under limit, choose least-used anyway (transparent reset behaviour).
    """
    if not candidates_for_key:
        return ""

    scored = []
    for c in candidates_for_key:
        phrase = c["phrase"]
        max_pm = c["max_per_month"]
        used = usage_map.get((dest, theme, phrase), 0)
        under = used < max_pm
        tie = _md5_int(f"{deal_id}|{dest}|{theme}|{phrase}")
        scored.append((0 if under else 1, used, tie, phrase))

    # Sort: under-limit first (0), then least used, then stable tie, then phrase
    scored.sort(key=lambda x: (x[0], x[1], x[2], x[3]))
    return scored[0][3]


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

    phrase_candidates = _build_phrase_candidates(phrase_rows)

    view_rows = ws_view.get_all_records()
    raw_rows = ws_raw.get_all_records()

    headers = [h.strip() for h in ws_raw.row_values(1)]
    col = {h: i + 1 for i, h in enumerate(headers)}

    # Hard requirements (fail fast with a clear message)
    for required in ("status", "deal_id", "ingested_at_utc", "destination_iata", PHRASE_USED_COL, PHRASE_BANK_COL):
        if required not in col:
            raise RuntimeError(f"Missing required RAW_DEALS column: {required}")

    now = datetime.now(timezone.utc)
    min_allowed_ts = None
    if ELIGIBLE_WINDOW_HOURS and ELIGIBLE_WINDOW_HOURS > 0:
        min_allowed_ts = now - timedelta(hours=ELIGIBLE_WINDOW_HOURS)

    # Build phrase usage map from RAW_DEALS (last N days)
    usage_map = _build_phrase_usage(raw_rows, now_utc=now, window_days=PHRASE_WINDOW_DAYS)

    # Map deal_id -> RAW_DEALS row number and ingested_at
    deal_row = {}
    deal_ingested = {}
    for idx, r in enumerate(raw_rows, start=2):
        did = _norm(r.get("deal_id"))
        if did:
            deal_row[did] = idx
            deal_ingested[did] = r.get("ingested_at_utc")

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

        hard_reject = _truthy(r.get("hard_reject"))
        worth = _float_or_none(r.get("worthiness_score"))

        candidates.append(
            {
                "did": did,
                "rownum": deal_row[did],
                "dest": _norm_iata(r.get("destination_iata")),
                "theme": _norm_theme(r.get("dynamic_theme")),
                "hard_reject": hard_reject,
                "worthiness": worth,
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

    # Decide winners:
    eligible_for_winner = [c for c in candidates if not c["hard_reject"]]

    if not eligible_for_winner:
        _log("All eligible candidates were hard_reject=TRUE; marking evaluated rows as SCORED only.")
        eligible_for_winner = []

    def _winner_sort_key(c):
        w = c["worthiness"]
        if w is None:
            w = -1e9
        return (-w, c["did"])

    eligible_for_winner.sort(key=_winner_sort_key)

    winners = []
    for c in eligible_for_winner:
        if len(winners) >= WINNERS_PER_RUN:
            break
        if c["worthiness"] is not None and c["worthiness"] < MIN_WORTHINESS_SCORE:
            continue
        winners.append(c)

    winner_ids = {w["did"] for w in winners}

    updates = []

    # 1) Mark evaluated non-winners as SCORED
    scored_count = 0
    for c in candidates:
        if c["did"] in winner_ids:
            continue
        updates.append(Cell(c["rownum"], col["status"], "SCORED"))
        scored_count += 1

    # 2) Promote winners + lock phrase
    promoted = 0
    for c in winners:
        did = c["did"]
        rownum = c["rownum"]
        dest = c["dest"]
        theme = c["theme"]

        key = (dest, theme)
        cand = phrase_candidates.get(key, [])
        phrase = _select_phrase(dest, theme, did, cand, usage_map)

        updates.append(Cell(rownum, col["status"], "READY_TO_POST"))
        updates.append(Cell(rownum, col[PHRASE_USED_COL], phrase))
        updates.append(Cell(rownum, col[PHRASE_BANK_COL], phrase))

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
