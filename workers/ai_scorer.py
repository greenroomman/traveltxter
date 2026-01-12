# workers/ai_scorer.py
"""
TravelTxter ai_scorer.py (V4.6+) — GEM HUNTER (Theme Preference, Not Kill-Switch)

NON-NEGOTIABLE CONTRACT (unchanged):
- READ intelligence ONLY from RAW_DEALS_VIEW:
    status, deal_id, dynamic_theme, price_value_score, worthiness_score, worthiness_verdict
- WRITE outcome ONLY to RAW_DEALS.status:
    READY_TO_POST (batch write)
- JOIN VIEW -> RAW_DEALS via deal_id
- Deterministic + stateless (Google Sheets is the single stateful brain)

Dragon upgrades implemented:
1) Theme-flex:
   - Theme match gives a bonus (preference), not a hard gate
   - Elite "💎 VIP + INSTA (Elite)" can bypass theme
   - Standard "✅ POST (Standard)" stays on-theme (unless you raise threshold)
2) Zero-waste / Backlog retarget:
   - If no Elite/Standard candidates exist, allow BACKLOG deals (theme-first)
   - Backlog scope is limited to recent rows (lookback rows) to avoid old junk resurfacing

Hard safety gates (kept):
- price_value_score must be > 0 (ghosted deals never promoted)
- fatigue guard: avoid posting same destination >2x in 7 days unless price dropped >10%

Outputs:
- Promotes WINNERS_PER_RUN rows to READY_TO_POST
"""

from __future__ import annotations

import os
import json
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import gspread
from gspread.cell import Cell
from google.oauth2.service_account import Credentials


# -------------------- ENV --------------------

RAW_DEALS_TAB = os.environ.get("RAW_DEALS_TAB", "RAW_DEALS").strip() or "RAW_DEALS"
RAW_DEALS_VIEW_TAB = os.environ.get("RAW_DEALS_VIEW_TAB", "RAW_DEALS_VIEW").strip() or "RAW_DEALS_VIEW"

SPREADSHEET_ID = (os.environ.get("SPREADSHEET_ID") or os.environ.get("SHEET_ID") or "").strip()
GCP_SA_JSON_ONE_LINE = (os.environ.get("GCP_SA_JSON_ONE_LINE") or os.environ.get("GCP_SA_JSON") or "").strip()

WINNERS_PER_RUN = int(os.environ.get("WINNERS_PER_RUN", "1") or "1")
MAX_ROWS_PER_RUN = int(os.environ.get("MAX_ROWS_PER_RUN", "50") or "50")

# Theme handling
THEME_OF_DAY = (os.environ.get("THEME_OF_DAY") or "").strip().lower()
THEME_BONUS = float(os.environ.get("THEME_BONUS", "20") or "20")  # preference points, not a gate
RUN_SLOT = (os.environ.get("RUN_SLOT") or "").strip().upper()

# "Gem" threshold for off-theme allowance
GEM_SCORE_THRESHOLD = float(os.environ.get("GEM_SCORE_THRESHOLD", "65") or "65")  # Elite threshold
STANDARD_OFF_THEME_THRESHOLD = float(os.environ.get("STANDARD_OFF_THEME_THRESHOLD", "999") or "999")
# (default effectively disables off-theme standard; set to e.g. 80 if you want)

# Backlog retargeting controls
ENABLE_BACKLOG_RETARGET = (os.environ.get("ENABLE_BACKLOG_RETARGET", "true") or "true").strip().lower() in ("1", "true", "yes", "y")
BACKLOG_LOOKBACK_ROWS = int(os.environ.get("BACKLOG_LOOKBACK_ROWS", "300") or "300")
BACKLOG_MIN_SCORE = float(os.environ.get("BACKLOG_MIN_SCORE", "35") or "35")  # aligns with your verdict ladder
BACKLOG_THEME_FIRST = (os.environ.get("BACKLOG_THEME_FIRST", "true") or "true").strip().lower() in ("1", "true", "yes", "y")

# Fatigue behaviour
FATIGUE_LOOKBACK_DAYS = int(os.environ.get("FATIGUE_LOOKBACK_DAYS", "7") or "7")
FATIGUE_MAX_POSTS_PER_DEST = int(os.environ.get("FATIGUE_MAX_POSTS_PER_DEST", "2") or "2")
FATIGUE_PRICE_DROP_ALLOW = float(os.environ.get("FATIGUE_PRICE_DROP_ALLOW", "0.10") or "0.10")

NOW = datetime.now(timezone.utc)

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


# -------------------- LOGGING --------------------

def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"{ts} | {msg}", flush=True)


# -------------------- HELPERS --------------------

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


def _batch_write_status(ws_raw, status_col: int, row_numbers: List[int], new_status: str) -> int:
    if not row_numbers:
        return 0
    cells = [Cell(row=r, col=status_col, value=new_status) for r in row_numbers]
    ws_raw.update_cells(cells, value_input_option="RAW")
    return len(cells)


# -------------------- FATIGUE --------------------

def _destination_key(row: Dict[str, Any]) -> str:
    d = _s(row.get("destination_iata")).upper()
    if d:
        return d
    return _s(row.get("destination_city")).strip().lower()


def _is_posted_row(row: Dict[str, Any]) -> bool:
    status = _s(row.get("status")).upper()
    if status.startswith("POSTED"):
        return True
    if "POSTED" in status:
        return True
    if _s(row.get("posted_instagram_at")):
        return True
    if _s(row.get("posted_all_at")):
        return True
    if _s(row.get("posted_telegram_at")):
        return True
    return False


def _fatigue_allows_candidate(
    candidate_view_row: Dict[str, Any],
    raw_records: List[Dict[str, Any]],
    lookback_days: int,
    max_posts_per_dest: int,
    allow_drop: float,
) -> bool:
    """
    If destination posted >= max_posts_per_dest in lookback window,
    allow only if price dropped > allow_drop vs most recent posted price.
    """
    dest_key = _destination_key(candidate_view_row)
    if not dest_key:
        return True

    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)

    posted = []
    for r in raw_records:
        if not _is_posted_row(r):
            continue
        ts = (
            _parse_iso_dt(r.get("posted_instagram_at"))
            or _parse_iso_dt(r.get("posted_all_at"))
            or _parse_iso_dt(r.get("posted_telegram_at"))
        )
        if ts and ts < cutoff:
            continue
        if _destination_key(r) == dest_key:
            posted.append(r)

    if len(posted) < max_posts_per_dest:
        return True

    cand_price = _safe_float(candidate_view_row.get("price_gbp"), 0.0)
    if cand_price <= 0:
        return False

    def _posted_time(r):
        return _parse_iso_dt(r.get("posted_instagram_at")) or datetime(1970, 1, 1, tzinfo=timezone.utc)

    posted.sort(key=_posted_time, reverse=True)
    last_price = _safe_float(posted[0].get("price_gbp"), 0.0)
    if last_price <= 0:
        return False

    return cand_price <= (last_price * (1.0 - allow_drop))


# -------------------- SCORING / SELECTION --------------------

def _priority_score(worthiness: float, theme_match: bool) -> float:
    return worthiness + (THEME_BONUS if theme_match else 0.0)


def _is_new(row: Dict[str, Any]) -> bool:
    return _s(row.get("status")) == "NEW"


def _verdict(row: Dict[str, Any]) -> str:
    return _s(row.get("worthiness_verdict"))


def _theme_match(row: Dict[str, Any], theme_today: str) -> bool:
    return _s(row.get("dynamic_theme")).strip().lower() == theme_today


def _eligible_primary(row: Dict[str, Any], theme_today: str) -> bool:
    """
    Primary hunt:
    - Elite: allow any theme if worthiness >= GEM threshold, but prefer theme.
    - Standard: theme match required (unless STANDARD_OFF_THEME_THRESHOLD is lowered).
    """
    if _safe_float(row.get("price_value_score"), 0.0) <= 0.0:
        return False

    v = _verdict(row)
    w = _safe_float(row.get("worthiness_score"), 0.0)
    tm = _theme_match(row, theme_today)

    if v == VERDICT_ELITE:
        if tm:
            return True
        return w >= GEM_SCORE_THRESHOLD

    if v == VERDICT_STANDARD:
        if tm:
            return True
        return w >= STANDARD_OFF_THEME_THRESHOLD

    return False


def _eligible_backlog(row: Dict[str, Any], theme_today: str, theme_first: bool) -> bool:
    """
    Backlog retarget:
    - Allow BACKLOG if worthiness >= BACKLOG_MIN_SCORE and price_value_score > 0
    - If theme_first is True, require theme match for backlog selection pass
    """
    if _safe_float(row.get("price_value_score"), 0.0) <= 0.0:
        return False

    if _verdict(row) != VERDICT_BACKLOG:
        return False

    w = _safe_float(row.get("worthiness_score"), 0.0)
    if w < BACKLOG_MIN_SCORE:
        return False

    if theme_first:
        return _theme_match(row, theme_today)

    return True


# -------------------- MAIN --------------------

def main() -> int:
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")

    theme_today = _theme_today()
    _log(f"MAX_ROWS_PER_RUN={MAX_ROWS_PER_RUN} WINNERS_PER_RUN={WINNERS_PER_RUN}")
    _log(f"Theme preference: THEME_OF_DAY={theme_today} THEME_BONUS={THEME_BONUS} RUN_SLOT={RUN_SLOT or 'n/a'}")
    _log(f"Gem thresholds: GEM_SCORE_THRESHOLD={GEM_SCORE_THRESHOLD} STANDARD_OFF_THEME_THRESHOLD={STANDARD_OFF_THEME_THRESHOLD}")
    _log(f"Backlog: enabled={ENABLE_BACKLOG_RETARGET} lookback_rows={BACKLOG_LOOKBACK_ROWS} min_score={BACKLOG_MIN_SCORE} theme_first={BACKLOG_THEME_FIRST}")
    _log(f"Fatigue: lookback_days={FATIGUE_LOOKBACK_DAYS} max_posts_per_dest={FATIGUE_MAX_POSTS_PER_DEST} allow_drop={FATIGUE_PRICE_DROP_ALLOW}")

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

    # RAW_DEALS records for mapping and fatigue checks
    raw_records = ws_raw.get_all_records()
    deal_id_to_rownum: Dict[str, int] = {}
    for idx, r in enumerate(raw_records, start=2):
        did = _s(r.get("deal_id"))
        if did:
            deal_id_to_rownum[did] = idx

    required_view = ["status", "deal_id", "dynamic_theme", "price_value_score", "worthiness_score", "worthiness_verdict"]
    missing_view = [c for c in required_view if c not in view_rows[0]]
    if missing_view:
        raise RuntimeError(f"RAW_DEALS_VIEW missing required columns: {missing_view}")

    # Work on a bounded slice for determinism + performance
    # For primary: scan a wider window but bounded
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

        # Fatigue guard
        if not _fatigue_allows_candidate(
            candidate_view_row=r,
            raw_records=raw_records,
            lookback_days=FATIGUE_LOOKBACK_DAYS,
            max_posts_per_dest=FATIGUE_MAX_POSTS_PER_DEST,
            allow_drop=FATIGUE_PRICE_DROP_ALLOW,
        ):
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
                "dynamic_theme": _s(r.get("dynamic_theme")).lower(),
            }
        )

    _log(f"Loaded RAW_DEALS_VIEW rows: {len(view_rows)}")
    _log(f"Primary eligible NEW candidates: {len(candidates)}")

    # Backlog retarget if no primary candidates
    if not candidates and ENABLE_BACKLOG_RETARGET:
        # Use most recent N rows (end of view) as a recency proxy.
        recent_slice = view_rows[-BACKLOG_LOOKBACK_ROWS:] if len(view_rows) > BACKLOG_LOOKBACK_ROWS else view_rows[:]
        backlog_candidates: List[Dict[str, Any]] = []

        # First pass: theme-first backlog if configured
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

            if not _fatigue_allows_candidate(
                candidate_view_row=r,
                raw_records=raw_records,
                lookback_days=FATIGUE_LOOKBACK_DAYS,
                max_posts_per_dest=FATIGUE_MAX_POSTS_PER_DEST,
                allow_drop=FATIGUE_PRICE_DROP_ALLOW,
            ):
                continue

            w = _safe_float(r.get("worthiness_score"), 0.0)
            tm = _theme_match(r, theme_today)
            ps = _priority_score(w, tm)

            backlog_candidates.append(
                {
                    "deal_id": did,
                    "raw_row": raw_row,
                    "worthiness_score": w,
                    "priority_score": ps,
                    "verdict": _verdict(r),
                    "theme_match": tm,
                    "dynamic_theme": _s(r.get("dynamic_theme")).lower(),
                }
            )

        # Optional second pass: if theme-first produced nothing, allow any-theme backlog
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

                if not _fatigue_allows_candidate(
                    candidate_view_row=r,
                    raw_records=raw_records,
                    lookback_days=FATIGUE_LOOKBACK_DAYS,
                    max_posts_per_dest=FATIGUE_MAX_POSTS_PER_DEST,
                    allow_drop=FATIGUE_PRICE_DROP_ALLOW,
                ):
                    continue

                w = _safe_float(r.get("worthiness_score"), 0.0)
                tm = _theme_match(r, theme_today)
                ps = _priority_score(w, tm)

                backlog_candidates.append(
                    {
                        "deal_id": did,
                        "raw_row": raw_row,
                        "worthiness_score": w,
                        "priority_score": ps,
                        "verdict": _verdict(r),
                        "theme_match": tm,
                        "dynamic_theme": _s(r.get("dynamic_theme")).lower(),
                    }
                )

        _log(f"Backlog retarget candidates: {len(backlog_candidates)}")
        candidates = backlog_candidates

    if not candidates:
        _log("No eligible candidates after primary + backlog. Exiting.")
        return 0

    # Sort by priority_score (worthiness + theme bonus), then worthiness
    candidates.sort(key=lambda x: (x["priority_score"], x["worthiness_score"]), reverse=True)
    winners = candidates[: max(1, WINNERS_PER_RUN)]
    winner_rows = [w["raw_row"] for w in winners]

    written = _batch_write_status(ws_raw, raw_cm["status"], winner_rows, "READY_TO_POST")
    _log(f"✅ Winners promoted to READY_TO_POST: {len(winner_rows)} (cells written={written})")

    for w in winners:
        _log(
            "Winner: "
            f"deal_id={w['deal_id']} "
            f"priority={w['priority_score']:.2f} score={w['worthiness_score']:.2f} "
            f"verdict={w['verdict']} "
            f"theme_match={w['theme_match']} dynamic_theme={w['dynamic_theme']} "
            f"raw_row={w['raw_row']}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
