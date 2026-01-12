# workers/ai_scorer.py
"""
TravelTxter ai_scorer.py (V4.6) — PURE VIEW JUDGE (Spreadsheet Brain Contract)

CONTRACT:
- READ intelligence ONLY from RAW_DEALS_VIEW (dynamic_theme, price_value_score, worthiness_score, worthiness_verdict)
- WRITE outcome ONLY to RAW_DEALS.status (READY_TO_POST / READY_TO_VIP_BUNDLE)
- JOIN VIEW -> RAW_DEALS using deal_id (column B, within A–Z mirror)
- Batch writes only (avoid 429)
- DO NOT modify rendering/publishing architecture
"""

from __future__ import annotations

import os
import json
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Set

import gspread
from gspread.cell import Cell
from google.oauth2.service_account import Credentials


RAW_DEALS_TAB = os.environ.get("RAW_DEALS_TAB", "RAW_DEALS").strip() or "RAW_DEALS"
RAW_DEALS_VIEW_TAB = os.environ.get("RAW_DEALS_VIEW_TAB", "RAW_DEALS_VIEW").strip() or "RAW_DEALS_VIEW"

SPREADSHEET_ID = (os.environ.get("SPREADSHEET_ID") or os.environ.get("SHEET_ID") or "").strip()
GCP_SA_JSON_ONE_LINE = (os.environ.get("GCP_SA_JSON_ONE_LINE") or os.environ.get("GCP_SA_JSON") or "").strip()

# How many rows we will consider from the view
MAX_ROWS_PER_RUN = int(os.environ.get("MAX_ROWS_PER_RUN", "50") or "50")

# Winner for Instagram
WINNERS_PER_RUN = int(os.environ.get("WINNERS_PER_RUN", "1") or "1")

# VIP bundle extras (runner-ups)
VIP_BUNDLE_SIZE = int(os.environ.get("VIP_BUNDLE_SIZE", "3") or "3")  # total incl winner
VIP_RUNNERS_UP = max(0, VIP_BUNDLE_SIZE - 1)

THEME_OF_DAY = (os.environ.get("THEME_OF_DAY") or "").strip().lower()
THEME_BONUS = float(os.environ.get("THEME_BONUS", "20") or "20")

# “Gem Hunter” thresholds
GEM_SCORE_THRESHOLD = float(os.environ.get("GEM_SCORE_THRESHOLD", "65") or "65")
STANDARD_OFF_THEME_THRESHOLD = float(os.environ.get("STANDARD_OFF_THEME_THRESHOLD", "999") or "999")

# Hard-block junk deals
HARD_BLOCK_BAD_DEALS = (os.environ.get("HARD_BLOCK_BAD_DEALS", "true") or "true").strip().lower() in ("1", "true", "yes", "y")
MIN_PRICE_VALUE_SCORE = float(os.environ.get("MIN_PRICE_VALUE_SCORE", "5") or "5")

# Fatigue controls
FATIGUE_LOOKBACK_DAYS = int(os.environ.get("FATIGUE_LOOKBACK_DAYS", "7") or "7")
FATIGUE_MAX_POSTS_PER_DEST = int(os.environ.get("FATIGUE_MAX_POSTS_PER_DEST", "2") or "2")
FATIGUE_PRICE_DROP_ALLOW = float(os.environ.get("FATIGUE_PRICE_DROP_ALLOW", "0.10") or "0.10")

# Optional backlog retarget
ENABLE_BACKLOG_RETARGET = (os.environ.get("ENABLE_BACKLOG_RETARGET", "true") or "true").strip().lower() in ("1", "true", "yes", "y")
BACKLOG_LOOKBACK_ROWS = int(os.environ.get("BACKLOG_LOOKBACK_ROWS", "300") or "300")
BACKLOG_MIN_SCORE = float(os.environ.get("BACKLOG_MIN_SCORE", "35") or "35")
BACKLOG_THEME_FIRST = (os.environ.get("BACKLOG_THEME_FIRST", "true") or "true").strip().lower() in ("1", "true", "yes", "y")

# Enforced diversity
ENFORCE_DISTINCT_DESTS = (os.environ.get("ENFORCE_DISTINCT_DESTS", "true") or "true").strip().lower() in ("1", "true", "yes", "y")


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


def _parse_iso_dt(v: Any) -> Optional[datetime]:
    s = _s(v)
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        dtv = datetime.fromisoformat(s)
        if dtv.tzinfo is None:
            dtv = dtv.replace(tzinfo=timezone.utc)
        return dtv.astimezone(timezone.utc)
    except Exception:
        return None


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
    return _s(row.get("dynamic_theme")).strip().lower() == theme_today


def _verdict(row: Dict[str, Any]) -> str:
    return _s(row.get("worthiness_verdict"))


def _destination_key(row: Dict[str, Any]) -> str:
    return _s(row.get("destination_iata")).upper() or _s(row.get("destination_city")).lower()


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
            _parse_iso_dt(r.get("posted_instagram_at"))
            or _parse_iso_dt(r.get("posted_all_at"))
            or _parse_iso_dt(r.get("posted_telegram_at"))
            or _parse_iso_dt(r.get("posted_telegram_vip_at"))
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

    posted.sort(key=lambda r: _parse_iso_dt(r.get("posted_instagram_at")) or datetime(1970, 1, 1, tzinfo=timezone.utc), reverse=True)
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


def _eligible_backlog(row: Dict[str, Any], theme_today: str, theme_first: bool) -> bool:
    if not _passes_value_gate(row):
        return False
    if _verdict(row) != VERDICT_BACKLOG:
        return False
    w = _safe_float(row.get("worthiness_score"), 0.0)
    if w < BACKLOG_MIN_SCORE:
        return False
    if theme_first:
        return _theme_match(row, theme_today)
    return True


def _batch_update_status(ws_raw, status_col: int, updates: List[Dict[str, Any]]) -> int:
    """
    updates: [{"row": int, "value": str}]
    """
    if not updates:
        return 0
    cells = [Cell(row=u["row"], col=status_col, value=u["value"]) for u in updates]
    ws_raw.update_cells(cells, value_input_option="RAW")
    return len(cells)


def main() -> int:
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")

    theme_today = _theme_today()

    _log(f"MAX_ROWS_PER_RUN={MAX_ROWS_PER_RUN} WINNERS_PER_RUN={WINNERS_PER_RUN} VIP_BUNDLE_SIZE={VIP_BUNDLE_SIZE}")
    _log(f"Theme preference: THEME_OF_DAY={theme_today} THEME_BONUS={THEME_BONUS}")
    _log(f"GEM_SCORE_THRESHOLD={GEM_SCORE_THRESHOLD}")
    _log(f"HARD_BLOCK_BAD_DEALS={HARD_BLOCK_BAD_DEALS} MIN_PRICE_VALUE_SCORE={MIN_PRICE_VALUE_SCORE}")
    _log(f"ENFORCE_DISTINCT_DESTS={ENFORCE_DISTINCT_DESTS}")

    gc = _gs_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_view = sh.worksheet(RAW_DEALS_VIEW_TAB)
    ws_raw = sh.worksheet(RAW_DEALS_TAB)

    view_rows = ws_view.get_all_records()
    if not view_rows:
        _log("RAW_DEALS_VIEW empty. Exiting.")
        return 0

    raw_headers = _headers(ws_raw)
    raw_cm = _colmap(raw_headers)
    if "status" not in raw_cm:
        raise RuntimeError("RAW_DEALS missing required column: status")
    if "deal_id" not in raw_cm:
        raise RuntimeError("RAW_DEALS missing required column: deal_id")

    raw_records = ws_raw.get_all_records()
    deal_id_to_rownum: Dict[str, int] = {}
    for idx, r in enumerate(raw_records, start=2):
        did = _s(r.get("deal_id"))
        if did:
            deal_id_to_rownum[did] = idx

    # Candidate scan window
    scan_rows = view_rows[: MAX_ROWS_PER_RUN * 10]

    candidates: List[Dict[str, Any]] = []
    for r in scan_rows:
        if not _is_new(r):
            continue
        did = _s(r.get("deal_id"))
        if not did:
            continue
        raw_row = deal_id_to_rownum.get(did)
        if not raw_row:
            continue

        if not _eligible_primary(r, theme_today):
            continue
        if not _fatigue_allows_candidate(r, raw_records):
            continue

        w = _safe_float(r.get("worthiness_score"), 0.0)
        tm = _theme_match(r, theme_today)
        ps = _priority_score(w, tm)
        dest = _destination_key(r)

        candidates.append(
            {
                "deal_id": did,
                "raw_row": raw_row,
                "worthiness_score": w,
                "priority_score": ps,
                "verdict": _verdict(r),
                "theme_match": tm,
                "dynamic_theme": _s(r.get("dynamic_theme")).lower(),
                "dest_key": dest,
            }
        )

    _log(f"Primary eligible NEW candidates: {len(candidates)}")

    # Backlog fallback if truly nothing eligible
    if not candidates and ENABLE_BACKLOG_RETARGET:
        recent_slice = view_rows[-BACKLOG_LOOKBACK_ROWS:] if len(view_rows) > BACKLOG_LOOKBACK_ROWS else view_rows[:]
        backlog_candidates: List[Dict[str, Any]] = []

        for r in recent_slice:
            if not _is_new(r):
                continue
            did = _s(r.get("deal_id"))
            if not did:
                continue
            raw_row = deal_id_to_rownum.get(did)
            if not raw_row:
                continue

            if not _eligible_backlog(r, theme_today, theme_first=BACKLOG_THEME_FIRST):
                continue
            if not _fatigue_allows_candidate(r, raw_records):
                continue

            w = _safe_float(r.get("worthiness_score"), 0.0)
            tm = _theme_match(r, theme_today)
            ps = _priority_score(w, tm)
            dest = _destination_key(r)

            backlog_candidates.append(
                {
                    "deal_id": did,
                    "raw_row": raw_row,
                    "worthiness_score": w,
                    "priority_score": ps,
                    "verdict": _verdict(r),
                    "theme_match": tm,
                    "dynamic_theme": _s(r.get("dynamic_theme")).lower(),
                    "dest_key": dest,
                }
            )

        if not backlog_candidates and BACKLOG_THEME_FIRST:
            for r in recent_slice:
                if not _is_new(r):
                    continue
                did = _s(r.get("deal_id"))
                if not did:
                    continue
                raw_row = deal_id_to_rownum.get(did)
                if not raw_row:
                    continue

                if not _eligible_backlog(r, theme_today, theme_first=False):
                    continue
                if not _fatigue_allows_candidate(r, raw_records):
                    continue

                w = _safe_float(r.get("worthiness_score"), 0.0)
                tm = _theme_match(r, theme_today)
                ps = _priority_score(w, tm)
                dest = _destination_key(r)

                backlog_candidates.append(
                    {
                        "deal_id": did,
                        "raw_row": raw_row,
                        "worthiness_score": w,
                        "priority_score": ps,
                        "verdict": _verdict(r),
                        "theme_match": tm,
                        "dynamic_theme": _s(r.get("dynamic_theme")).lower(),
                        "dest_key": dest,
                    }
                )

        _log(f"Backlog retarget candidates: {len(backlog_candidates)}")
        candidates = backlog_candidates

    if not candidates:
        _log("No eligible candidates after gates. Exiting.")
        return 0

    # Rank candidates
    candidates.sort(key=lambda x: (x["priority_score"], x["worthiness_score"]), reverse=True)

    # ✅ Select winner + runner-ups with destination diversity (optional)
    selected: List[Dict[str, Any]] = []
    used_dests: Set[str] = set()

    for c in candidates:
        if ENFORCE_DISTINCT_DESTS and c["dest_key"]:
            if c["dest_key"] in used_dests:
                continue
        selected.append(c)
        if c["dest_key"]:
            used_dests.add(c["dest_key"])
        if len(selected) >= (WINNERS_PER_RUN + VIP_RUNNERS_UP):
            break

    if not selected:
        _log("No candidates after diversity selection. Exiting.")
        return 0

    winner = selected[0]
    runner_ups = selected[1: 1 + VIP_RUNNERS_UP]

    updates = []
    # Winner for Instagram pipeline
    updates.append({"row": winner["raw_row"], "value": "READY_TO_POST"})

    # Runner-ups for VIP only (won't be touched by IG publisher)
    for r in runner_ups:
        updates.append({"row": r["raw_row"], "value": "READY_TO_VIP_BUNDLE"})

    written = _batch_update_status(ws_raw, raw_cm["status"], updates)

    _log(f"✅ Winner promoted -> READY_TO_POST (row={winner['raw_row']} deal_id={winner['deal_id']})")
    if runner_ups:
        _log(f"✅ VIP runners-up promoted -> READY_TO_VIP_BUNDLE: {len(runner_ups)} rows")
        for r in runner_ups:
            _log(f"Runner-up: deal_id={r['deal_id']} score={r['worthiness_score']} raw_row={r['raw_row']}")
    _log(f"Cells written (batch): {written}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
