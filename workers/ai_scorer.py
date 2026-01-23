# workers/ai_scorer.py
# V4.7.3 — scorer: NEW -> (READY_TO_POST for winners) else -> SCORED
# Contract:
# - RAW_DEALS is canonical (writes happen here only)
# - RAW_DEALS_VIEW is read-only (intelligence / formulas)
# - Phrase selection happens ONCE at promotion time
# - Publishers never select language
# - Full-file replacement only
#
# Hardening changes in this version:
# 1) RAW_DEALS_VIEW read no longer uses get_all_records() (which hard-crashes on duplicate headers).
#    Instead, we read values + map only required columns, ignoring duplicate header collisions safely.
# 2) Phrase theme selection falls back: dynamic_theme (VIEW) -> deal_theme (RAW) -> theme (RAW)
#    so a broken RDV dynamic_theme doesn't break phrase selection.

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


# View columns we actually need (minimizes coupling to RDV header chaos)
VIEW_REQUIRED_COLS = (
    "status",
    "deal_id",
    "destination_iata",
    "dynamic_theme",
    "hard_reject",
    "worthiness_score",
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


def _clean_iata(x):
    return _norm(x).upper()[:3]


def _load_route_maps(sh):
    """Best-effort: returns (dest_city_map, dest_country_map)."""
    try:
        ws = sh.worksheet(CAPABILITY_TAB)
        rows = ws.get_all_records()
    except Exception as e:
        _log(f"ROUTE_CAPABILITY_MAP not readable: {e}")
        return {}, {}

    dest_city_map = {}
    dest_country_map = {}
    for r in rows:
        d = _clean_iata(r.get("destination_iata"))
        if not d:
            continue
        dc = _norm(r.get("destination_city"))
        dcty = _norm(r.get("destination_country"))
        if dc and d not in dest_city_map:
            dest_city_map[d] = dc
        if dcty and d not in dest_country_map:
            dest_country_map[d] = dcty
    return dest_city_map, dest_country_map


def _load_signals(sh):
    """
    Best-effort: returns dict dest_iata -> row.

    RESILIENT: Tries CONFIG_SIGNALS first, then CONFIG (for post-merge compatibility).
    Never crashes if tabs are missing.
    """
    # Try CONFIG_SIGNALS (legacy)
    try:
        ws = sh.worksheet(SIGNALS_TAB)
        rows = ws.get_all_records()
        _log(f"✅ Loaded enrichment from {SIGNALS_TAB}")
    except Exception:
        # Fall back to CONFIG (after merge)
        try:
            config_tab = os.getenv("CONFIG_TAB", "CONFIG")
            ws = sh.worksheet(config_tab)
            rows = ws.get_all_records()
            _log(f"✅ Loaded enrichment from {config_tab} (CONFIG_SIGNALS not found)")
        except Exception as e:
            _log(f"⚠️ No enrichment tabs available ({SIGNALS_TAB} or CONFIG): {e}")
            return {}

    out = {}
    for r in rows:
        key = _clean_iata(r.get("destination_iata") or r.get("iata_hint") or r.get("iata"))
        if key:
            out[key] = r
    return out


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

    missing = [c for c in VIEW_REQUIRED_COLS if c not in first_idx]
    if missing:
        _log(f"⚠️ RAW_DEALS_VIEW missing required columns for scoring: {missing}. Treating as empty.")
        return []

    rows = []
    for row in values[1:]:
        # pad to header length
        if len(row) < len(headers):
            row = row + [""] * (len(headers) - len(row))

        r = {}
        for c in VIEW_REQUIRED_COLS:
            r[c] = row[first_idx[c]] if first_idx.get(c) is not None else ""
        rows.append(r)

    return rows


def main():
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")

    gc = gspread.authorize(_sa_creds())
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_raw = sh.worksheet(RAW_TAB)
    ws_view = sh.worksheet(VIEW_TAB)

    # Route enrichment sources (best-effort; scorer must never crash if tabs are missing)
    dest_city_map, dest_country_map = _load_route_maps(sh)
    signals = _load_signals(sh)

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
    # - never pick hard_reject
    # - if worthiness exists, sort desc and apply optional threshold
    eligible_for_winner = [c for c in candidates if not c["hard_reject"]]

    if not eligible_for_winner:
        _log("All eligible candidates were hard_reject=TRUE; marking evaluated rows as SCORED only.")
        eligible_for_winner = []

    # Log intelligence availability
    has_worthiness = sum(1 for c in eligible_for_winner if c["worthiness"] is not None)
    _log(
        f"Intelligence check: {has_worthiness}/{len(eligible_for_winner)} candidates have worthiness_score. "
        f"{'✅ Using scores for ranking' if has_worthiness > 0 else '⚠️ Random ranking (no scores)'}"
    )

    # Sort winners by worthiness if available; else stable by deal_id hash
    def _winner_sort_key(c):
        # worthiness: higher first; None treated as very low
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

    # Updates:
    # 1) Mark all evaluated candidates as SCORED (except winners)
    # 2) Promote winners to READY_TO_POST + lock phrase
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

        # SKIP destination backfill: feeder already populates these from ROUTE_CAPABILITY_MAP
        # (Keeping this code commented for reference if needed in future)
        #
        # rawrec = deal_raw_record.get(did, {})
        # dest_city_col = col.get("destination_city")
        # dest_country_col = col.get("destination_country")
        #
        # if dest_city_col and not _norm(rawrec.get("destination_city")):
        #     city = dest_city_map.get(dest) or _norm(signals.get(dest, {}).get("destination_city"))
        #     if city:
        #         updates.append(Cell(rownum, dest_city_col, city))
        #
        # if dest_country_col and not _norm(rawrec.get("destination_country")):
        #     country = dest_country_map.get(dest) or _norm(signals.get(dest, {}).get("destination_country"))
        #     if country:
        #         updates.append(Cell(rownum, dest_country_col, country))

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
