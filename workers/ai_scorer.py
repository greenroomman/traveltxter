# workers/ai_scorer.py
"""
TravelTxter ai_scorer.py (V4.6.2) — VIEW JUDGE + PHRASE ASSIGNER (Spreadsheet Brain Contract)

Locked constraints respected:
- No redesign
- No renames
- No schema/workflow changes
- RAW_DEALS is canonical; RAW_DEALS_VIEW is read-only

Change in this version:
- When promoting NEW -> READY_TO_POST, scorer assigns an approved phrase and writes:
  - RAW_DEALS.phrase_used (primary)
  - RAW_DEALS.phrase_bank (optional mirror for backward compatibility if column exists)

Phrase source:
- PHRASE_BANK tab (read-only)

Deterministic selection (no randomness):
- Uses stable hash of (deal_id, dest_iata, theme) to pick a phrase from candidates.
"""

from __future__ import annotations

import os
import json
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Set, Optional

import gspread
from gspread.cell import Cell
from google.oauth2.service_account import Credentials

from sheet_contract import SheetContract


RAW_DEALS_TAB = os.environ.get("RAW_DEALS_TAB", "RAW_DEALS").strip() or "RAW_DEALS"
RAW_DEALS_VIEW_TAB = os.environ.get("RAW_DEALS_VIEW_TAB", "RAW_DEALS_VIEW").strip() or "RAW_DEALS_VIEW"
PHRASE_BANK_TAB = os.environ.get("PHRASE_BANK_TAB", "PHRASE_BANK").strip() or "PHRASE_BANK"

SPREADSHEET_ID = (os.environ.get("SPREADSHEET_ID") or os.environ.get("SHEET_ID") or "").strip()
GCP_SA_JSON_ONE_LINE = (os.environ.get("GCP_SA_JSON_ONE_LINE") or os.environ.get("GCP_SA_JSON") or "").strip()

MAX_ROWS_PER_RUN = int(os.environ.get("MAX_ROWS_PER_RUN", "50") or "50")
WINNERS_PER_RUN = int(os.environ.get("WINNERS_PER_RUN", "1") or "1")

VIP_BUNDLE_SIZE = int(os.environ.get("VIP_BUNDLE_SIZE", "3") or "3")
VIP_RUNNERS_UP = max(0, VIP_BUNDLE_SIZE - 1)

THEME_OF_DAY = (os.environ.get("THEME_OF_DAY") or "").strip().lower()
THEME_BONUS = float(os.environ.get("THEME_BONUS", "20") or "20")

GEM_SCORE_THRESHOLD = float(os.environ.get("GEM_SCORE_THRESHOLD", "65") or "65")
STANDARD_OFF_THEME_THRESHOLD = float(os.environ.get("STANDARD_OFF_THEME_THRESHOLD", "999") or "999")

HARD_BLOCK_BAD_DEALS = (os.environ.get("HARD_BLOCK_BAD_DEALS", "true") or "true").strip().lower() in ("1", "true", "yes", "y")
MIN_PRICE_VALUE_SCORE = float(os.environ.get("MIN_PRICE_VALUE_SCORE", "5") or "5")

FATIGUE_LOOKBACK_DAYS = int(os.environ.get("FATIGUE_LOOKBACK_DAYS", "7") or "7")
FATIGUE_MAX_POSTS_PER_DEST = int(os.environ.get("FATIGUE_MAX_POSTS_PER_DEST", "2") or "2")
FATIGUE_PRICE_DROP_ALLOW = float(os.environ.get("FATIGUE_PRICE_DROP_ALLOW", "0.10") or "0.10")

ENABLE_BACKLOG_RETARGET = (os.environ.get("ENABLE_BACKLOG_RETARGET", "true") or "true").strip().lower() in ("1", "true", "yes", "y")
BACKLOG_LOOKBACK_ROWS = int(os.environ.get("BACKLOG_LOOKBACK_ROWS", "300") or "300")
BACKLOG_MIN_SCORE = float(os.environ.get("BACKLOG_MIN_SCORE", "35") or "35")
BACKLOG_THEME_FIRST = (os.environ.get("BACKLOG_THEME_FIRST", "true") or "true").strip().lower() in ("1", "true", "yes", "y")

ENFORCE_DISTINCT_DESTS = (os.environ.get("ENFORCE_DISTINCT_DESTS", "true") or "true").strip().lower() in ("1", "true", "yes", "y")

# Phase 2+ handshake controls
INGESTED_AT_COL = os.environ.get("INGESTED_AT_COL", "ingested_at_utc").strip() or "ingested_at_utc"
MIN_INGEST_AGE_SECONDS = int(os.environ.get("MIN_INGEST_AGE_SECONDS", "90") or "90")

# RAW_DEALS phrase columns
PHRASE_USED_COL = os.environ.get("PHRASE_USED_COL", "phrase_used").strip() or "phrase_used"
PHRASE_BANK_COL = os.environ.get("PHRASE_BANK_COL", "phrase_bank").strip() or "phrase_bank"  # optional mirror


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

VERDICT_ELITE = "💎 VIP + INSTA (Elite)"
VERDICT_STANDARD = "✅ POST (Standard)"
VERDICT_BACKLOG = "⚠️ BACKLOG (Low Priority)"


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"{ts} | {msg}", flush=True)


def _parse_sa_json(raw: str) -> dict:
    raw = (raw or "").strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return json.loads(raw.replace("\\n", "\n"))


def _gs_client() -> gspread.Client:
    if not GCP_SA_JSON_ONE_LINE:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
    info = _parse_sa_json(GCP_SA_JSON_ONE_LINE)
    creds = Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


def _headers(ws) -> List[str]:
    return [h.strip() for h in ws.row_values(1)]


def _colmap(headers: List[str]) -> Dict[str, int]:
    # 1-indexed for gspread Cell
    return {h: i + 1 for i, h in enumerate(headers) if h}


def _s(v: Any) -> str:
    return str(v or "").strip()


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        s = _s(v)
        if not s:
            return float(default)
        s = s.replace("£", "").replace(",", "").strip()
        return float(s)
    except Exception:
        return float(default)


def _truthy(v: Any) -> bool:
    x = _s(v).strip().upper()
    return x in ("TRUE", "YES", "Y", "1", "ON", "APPROVED")


def _norm_theme(s: Any) -> str:
    return _s(s).strip().lower().replace(" ", "_")


def _norm_iata(s: Any) -> str:
    return _s(s).strip().upper()


def _theme_of_day_utc() -> str:
    today = datetime.now(timezone.utc).date()
    doy = int(today.strftime("%j"))
    return MASTER_THEMES[doy % len(MASTER_THEMES)]


def _theme_today() -> str:
    if THEME_OF_DAY:
        return THEME_OF_DAY
    return _theme_of_day_utc().lower()


def _is_new(row: Dict[str, Any]) -> bool:
    return _s(row.get("status")) == "NEW"


def _theme_match(row: Dict[str, Any], theme_today: str) -> bool:
    return _norm_theme(row.get("dynamic_theme")) == _norm_theme(theme_today)


def _verdict(row: Dict[str, Any]) -> str:
    return _s(row.get("worthiness_verdict"))


def _destination_key(row: Dict[str, Any]) -> str:
    return _norm_iata(row.get("destination_iata")) or _s(row.get("destination_city")).lower()


def _is_posted_row(row: Dict[str, Any]) -> bool:
    status = _s(row.get("status")).upper()
    return status.startswith("POSTED") or "POSTED" in status


def _fatigue_allows_candidate(candidate_view_row: Dict[str, Any], raw_records: List[Dict[str, Any]]) -> bool:
    dest_key = _destination_key(candidate_view_row)
    if not dest_key:
        return True

    cutoff = datetime.now(timezone.utc) - timedelta(days=FATIGUE_LOOKBACK_DAYS)

    posted = []
    for r in raw_records:
        if not _is_posted_row(r):
            continue
        ts = (
            SheetContract.parse_iso_utc(r.get("posted_instagram_at"))
            or SheetContract.parse_iso_utc(r.get("posted_all_at"))
            or SheetContract.parse_iso_utc(r.get("posted_telegram_at"))
            or SheetContract.parse_iso_utc(r.get("posted_telegram_vip_at"))
        )
        if ts and ts < cutoff:
            continue
        if _destination_key(r) == dest_key:
            posted.append(r)

    if len(posted) < FATIGUE_MAX_POSTS_PER_DEST:
        return True

    cand_price = _safe_float(candidate_view_row.get("price_gbp"), 0.0)
    if cand_price <= 0:
        return False

    posted.sort(
        key=lambda r: SheetContract.parse_iso_utc(r.get("posted_instagram_at")) or datetime(1970, 1, 1, tzinfo=timezone.utc),
        reverse=True,
    )
    last_price = _safe_float(posted[0].get("price_gbp"), 0.0)
    if last_price <= 0:
        return False

    return cand_price <= (last_price * (1.0 - FATIGUE_PRICE_DROP_ALLOW))


def _passes_value_gate(row: Dict[str, Any]) -> bool:
    if not HARD_BLOCK_BAD_DEALS:
        return True
    pvs = _safe_float(row.get("price_value_score"), 0.0)
    return pvs >= MIN_PRICE_VALUE_SCORE


def _priority_score(worthiness: float, theme_match: bool) -> float:
    return worthiness + (THEME_BONUS if theme_match else 0.0)


def _eligible_primary(row: Dict[str, Any], theme_today: str) -> bool:
    if not _passes_value_gate(row):
        return False

    v = _verdict(row)
    w = _safe_float(row.get("worthiness_score"), 0.0)
    tm = _theme_match(row, theme_today)

    if v == VERDICT_ELITE:
        return tm or (w >= GEM_SCORE_THRESHOLD)

    if v == VERDICT_STANDARD:
        return tm or (w >= STANDARD_OFF_THEME_THRESHOLD)

    return False


def _stable_index(key: str, n: int) -> int:
    if n <= 0:
        return 0
    h = hashlib.md5(key.encode("utf-8")).hexdigest()
    return int(h[:8], 16) % n


def _select_phrase(
    phrase_rows: List[Dict[str, Any]],
    destination_iata: str,
    theme: str,
    deal_id: str,
) -> str:
    """
    Deterministic phrase selection based on:
    1) Exact match: (dest + theme)
    2) Same dest, any theme
    3) Theme-only phrase
    4) Blank
    """
    dest = _norm_iata(destination_iata)
    th = _norm_theme(theme)

    if not phrase_rows:
        return ""

    # Normalise a few possible PHRASE_BANK shapes without assuming schema changes:
    # We try to support both:
    # - destination_iata/theme/phrase/approved
    # - theme/category/phrase/approved where category includes "dest:XXX"
    def row_phrase(r: Dict[str, Any]) -> str:
        return _s(r.get("phrase"))

    def row_theme(r: Dict[str, Any]) -> str:
        return _norm_theme(r.get("theme"))

    def row_dest(r: Dict[str, Any]) -> str:
        # Supports either explicit destination_iata or encoded in category
        d = _norm_iata(r.get("destination_iata"))
        if d:
            return d
        cat = _s(r.get("category")).strip().lower()
        if cat.startswith("dest:"):
            return _norm_iata(cat.split("dest:", 1)[1])
        return ""

    def is_theme_only(r: Dict[str, Any]) -> bool:
        cat = _s(r.get("category")).strip().lower()
        # Accept empty category or explicit theme-only markers
        if not cat:
            return True
        return cat in ("theme", "theme_only", "theme_phrase", "generic")

    def approved(r: Dict[str, Any]) -> bool:
        return _truthy(r.get("approved"))

    # Candidate pools
    exact = [r for r in phrase_rows if approved(r) and row_dest(r) == dest and row_theme(r) == th and row_phrase(r)]
    if exact:
        i = _stable_index(f"{deal_id}|{dest}|{th}|exact", len(exact))
        return row_phrase(sorted(exact, key=lambda x: row_phrase(x)))[i]  # sorted for stability

    same_dest = [r for r in phrase_rows if approved(r) and row_dest(r) == dest and row_phrase(r)]
    if same_dest:
        same_dest_sorted = sorted(same_dest, key=lambda x: (row_theme(x), row_phrase(x)))
        i = _stable_index(f"{deal_id}|{dest}|any", len(same_dest_sorted))
        return row_phrase(same_dest_sorted[i])

    theme_only = [r for r in phrase_rows if approved(r) and row_theme(r) == th and is_theme_only(r) and row_phrase(r)]
    if theme_only:
        theme_only_sorted = sorted(theme_only, key=lambda x: row_phrase(x))
        i = _stable_index(f"{deal_id}|{th}|theme_only", len(theme_only_sorted))
        return row_phrase(theme_only_sorted[i])

    return ""


def _batch_update_cells(ws_raw, cells: List[Cell]) -> int:
    if not cells:
        return 0
    ws_raw.update_cells(cells, value_input_option="RAW")
    return len(cells)


def main() -> int:
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")

    theme_today = _theme_today()

    _log(f"MIN_INGEST_AGE_SECONDS={MIN_INGEST_AGE_SECONDS} INGESTED_AT_COL={INGESTED_AT_COL}")
    _log(f"MAX_ROWS_PER_RUN={MAX_ROWS_PER_RUN} WINNERS_PER_RUN={WINNERS_PER_RUN} VIP_BUNDLE_SIZE={VIP_BUNDLE_SIZE}")
    _log(f"Theme preference: THEME_OF_DAY={theme_today} THEME_BONUS={THEME_BONUS}")

    gc = _gs_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_view = sh.worksheet(RAW_DEALS_VIEW_TAB)
    ws_raw = sh.worksheet(RAW_DEALS_TAB)

    # Phrase bank read (read-only). If missing, proceed with blank phrase.
    phrase_rows: List[Dict[str, Any]] = []
    try:
        ws_phrase = sh.worksheet(PHRASE_BANK_TAB)
        phrase_rows = ws_phrase.get_all_records()
        _log(f"Loaded PHRASE_BANK rows: {len(phrase_rows)}")
    except Exception as e:
        _log(f"PHRASE_BANK not readable ({type(e).__name__}: {e}). Proceeding without phrases.")

    view_rows = ws_view.get_all_records()
    if not view_rows:
        _log("RAW_DEALS_VIEW empty. Exiting.")
        return 0

    raw_headers = _headers(ws_raw)

    # Require phrase_used because publishers will read it.
    SheetContract.assert_columns_present(
        raw_headers,
        required=["status", "deal_id", INGESTED_AT_COL, PHRASE_USED_COL],
        tab_name=RAW_DEALS_TAB,
    )
    raw_cm = _colmap(raw_headers)

    raw_records = ws_raw.get_all_records()

    deal_id_to_rownum: Dict[str, int] = {}
    deal_id_to_ingested: Dict[str, Any] = {}

    for idx, r in enumerate(raw_records, start=2):
        did = _s(r.get("deal_id"))
        if did:
            deal_id_to_rownum[did] = idx
            deal_id_to_ingested[did] = r.get(INGESTED_AT_COL)

    scan_n = max(50, MAX_ROWS_PER_RUN * 10)
    scan_rows = view_rows[-scan_n:]
    _log(f"Scanning RAW_DEALS_VIEW tail window: {len(scan_rows)} rows (of total {len(view_rows)})")

    candidates: List[Dict[str, Any]] = []
    too_fresh = 0

    for r in scan_rows:
        if not _is_new(r):
            continue

        did = _s(r.get("deal_id"))
        if not did:
            continue

        raw_row = deal_id_to_rownum.get(did)
        if not raw_row:
            continue

        ing = deal_id_to_ingested.get(did)
        if not SheetContract.is_older_than_seconds(ing, MIN_INGEST_AGE_SECONDS):
            too_fresh += 1
            continue

        if not _eligible_primary(r, theme_today):
            continue
        if not _fatigue_allows_candidate(r, raw_records):
            continue

        w = _safe_float(r.get("worthiness_score"), 0.0)
        tm = _theme_match(r, theme_today)
        ps = _priority_score(w, tm)

        candidates.append(
            {
                "deal_id": did,
                "raw_row": raw_row,
                "worthiness_score": w,
                "priority_score": ps,
                "verdict": _verdict(r),
                "theme_match": tm,
                "dynamic_theme": _norm_theme(r.get("dynamic_theme")),
                "dest_key": _destination_key(r),
                "destination_iata": _norm_iata(r.get("destination_iata")),
            }
        )

    _log(f"Primary eligible NEW candidates: {len(candidates)} | skipped_too_fresh={too_fresh}")

    if not candidates:
        _log("No eligible candidates (view gating / thresholds / or formula outputs not populated yet). Exiting cleanly.")
        return 0

    candidates.sort(key=lambda x: (x["priority_score"], x["worthiness_score"]), reverse=True)

    picked: List[Dict[str, Any]] = []
    seen_dests: Set[str] = set()

    for c in candidates:
        if ENFORCE_DISTINCT_DESTS and c["dest_key"]:
            if c["dest_key"] in seen_dests:
                continue
        picked.append(c)
        if c["dest_key"]:
            seen_dests.add(c["dest_key"])
        if len(picked) >= 1:  # workflow uses WINNERS_PER_RUN=1; keep deterministic
            break

    if not picked:
        _log("No candidates after diversity filter. Exiting.")
        return 0

    winner = picked[0]
    rownum = int(winner["raw_row"])
    deal_id = _s(winner["deal_id"])
    dest_iata = _norm_iata(winner.get("destination_iata"))
    phrase_theme = _norm_theme(winner.get("dynamic_theme")) or _norm_theme(theme_today)

    phrase = _select_phrase(
        phrase_rows=phrase_rows,
        destination_iata=dest_iata,
        theme=phrase_theme,
        deal_id=deal_id,
    )

    # Build cell updates (single update_cells call)
    cells: List[Cell] = []
    status_col = raw_cm["status"]
    cells.append(Cell(row=rownum, col=status_col, value="READY_TO_POST"))

    # phrase_used is required by contract now
    phrase_used_col = raw_cm.get(PHRASE_USED_COL)
    if phrase_used_col:
        cells.append(Cell(row=rownum, col=phrase_used_col, value=phrase))

    # Optional mirror to phrase_bank for backward compatibility if column exists
    phrase_bank_col = raw_cm.get(PHRASE_BANK_COL)
    if phrase_bank_col:
        cells.append(Cell(row=rownum, col=phrase_bank_col, value=phrase))

    n = _batch_update_cells(ws_raw, cells)

    preview = (phrase or "").strip()
    if len(preview) > 80:
        preview = preview[:77] + "..."
    _log(f"Updated row {rownum}: status=READY_TO_POST | phrase_used={(preview or '[blank]')} | cells={n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
