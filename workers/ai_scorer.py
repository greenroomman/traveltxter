# workers/ai_scorer.py
"""
TravelTxter ai_scorer.py (V4.6) — PURE VIEW JUDGE (Spreadsheet Brain Contract)

CONTRACT:
- READ intelligence ONLY from RAW_DEALS_VIEW (dynamic_theme, price_value_score, worthiness_score, worthiness_verdict)
- WRITE outcome ONLY to RAW_DEALS.status (READY_TO_POST)
- JOIN VIEW -> RAW_DEALS using deal_id (confirmed column B, within A–Z mirror)
- Batch writes only (avoid 429)

Verdict logic is canonical in RAW_DEALS_VIEW using:
AB=0 -> "❌ IGNORE (Bad Price)"
AG>=65 -> "💎 VIP + INSTA (Elite)"
AG>=45 -> "✅ POST (Standard)"
AG>=35 -> "⚠️ BACKLOG (Low Priority)"
else -> "❌ IGNORE (Weak Score)"

We promote:
- "💎 VIP + INSTA (Elite)"
- "✅ POST (Standard)"
Optionally also promote BACKLOG if you later decide (not in this file).

Extra hard gates:
- dynamic_theme must match THEME_OF_DAY
- price_value_score > 0 (ghosted deals excluded)

Fatigue guard (7 days):
- If destination posted >= 2 times in last 7 days, skip unless price dropped >10% vs most recent posted price.
"""

from __future__ import annotations

import os
import json
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import gspread
from gspread.cell import Cell
from google.oauth2.service_account import Credentials


RAW_DEALS_TAB = os.environ.get("RAW_DEALS_TAB", "RAW_DEALS").strip() or "RAW_DEALS"
RAW_DEALS_VIEW_TAB = os.environ.get("RAW_DEALS_VIEW_TAB", "RAW_DEALS_VIEW").strip() or "RAW_DEALS_VIEW"

SPREADSHEET_ID = (os.environ.get("SPREADSHEET_ID") or os.environ.get("SHEET_ID") or "").strip()
GCP_SA_JSON_ONE_LINE = (os.environ.get("GCP_SA_JSON_ONE_LINE") or os.environ.get("GCP_SA_JSON") or "").strip()

WINNERS_PER_RUN = int(os.environ.get("WINNERS_PER_RUN", "1") or "1")
MAX_ROWS_PER_RUN = int(os.environ.get("MAX_ROWS_PER_RUN", "50") or "50")

FATIGUE_LOOKBACK_DAYS = int(os.environ.get("FATIGUE_LOOKBACK_DAYS", "7") or "7")
FATIGUE_MAX_POSTS_PER_DEST = int(os.environ.get("FATIGUE_MAX_POSTS_PER_DEST", "2") or "2")
FATIGUE_PRICE_DROP_ALLOW = float(os.environ.get("FATIGUE_PRICE_DROP_ALLOW", "0.10") or "0.10")

THEME_OF_DAY = (os.environ.get("THEME_OF_DAY") or "").strip().lower()
RUN_SLOT = (os.environ.get("RUN_SLOT") or "").strip().upper()

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

PROMOTE_VERDICTS = {
    "💎 VIP + INSTA (Elite)",
    "✅ POST (Standard)",
}


def _log(msg: str) -> None:
    ts = NOW.strftime("%Y-%m-%dT%H:%M:%SZ")
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


def _batch_write_status(ws_raw, status_col: int, row_numbers: List[int], new_status: str) -> int:
    cells: List[Cell] = [Cell(row=r, col=status_col, value=new_status) for r in row_numbers]
    if not cells:
        return 0
    ws_raw.update_cells(cells, value_input_option="RAW")
    return len(cells)


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
    candidate: Dict[str, Any],
    raw_records: List[Dict[str, Any]],
    lookback_days: int,
    max_posts_per_dest: int,
    allow_drop: float,
) -> bool:
    dest_key = _destination_key(candidate)
    if not dest_key:
        return True

    cutoff = NOW - timedelta(days=lookback_days)

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

    cand_price = _safe_float(candidate.get("price_gbp"), 0.0)
    if cand_price <= 0:
        return False

    def _posted_time(r):
        return _parse_iso_dt(r.get("posted_instagram_at")) or datetime(1970, 1, 1, tzinfo=timezone.utc)

    posted.sort(key=_posted_time, reverse=True)
    last_price = _safe_float(posted[0].get("price_gbp"), 0.0)
    if last_price <= 0:
        return False

    return cand_price <= (last_price * (1.0 - allow_drop))


def main() -> int:
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")

    theme_today = _theme_today()
    _log(f"MAX_ROWS_PER_RUN={MAX_ROWS_PER_RUN} WINNERS_PER_RUN={WINNERS_PER_RUN}")
    _log(f"Theme gate: THEME_OF_DAY={theme_today} RUN_SLOT={RUN_SLOT or 'n/a'}")
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

    # Load RAW_DEALS records to map deal_id -> row number and enforce fatigue
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

    # Filter candidates using view intelligence only
    candidates = []
    for r in view_rows[:MAX_ROWS_PER_RUN * 10]:  # scan enough rows; still bounded
        if _s(r.get("status")) != "NEW":
            continue
        if _s(r.get("dynamic_theme")).lower() != theme_today:
            continue
        if _safe_float(r.get("price_value_score"), 0.0) <= 0.0:
            continue

        verdict = _s(r.get("worthiness_verdict"))
        if verdict not in PROMOTE_VERDICTS:
            continue

        did = _s(r.get("deal_id"))
        if not did:
            continue
        raw_row = deal_id_to_rownum.get(did)
        if not raw_row:
            continue

        # Fatigue gate uses RAW_DEALS history (allowed by architecture)
        if not _fatigue_allows_candidate(
            candidate=r,
            raw_records=raw_records,
            lookback_days=FATIGUE_LOOKBACK_DAYS,
            max_posts_per_dest=FATIGUE_MAX_POSTS_PER_DEST,
            allow_drop=FATIGUE_PRICE_DROP_ALLOW,
        ):
            continue

        worthiness = _safe_float(r.get("worthiness_score"), 0.0)

        candidates.append({"deal_id": did, "raw_row": raw_row, "worthiness_score": worthiness, "verdict": verdict})

    _log(f"Loaded RAW_DEALS_VIEW rows: {len(view_rows)}")
    _log(f"Eligible NEW candidates after gates: {len(candidates)}")

    if not candidates:
        _log("No eligible candidates. Exiting.")
        return 0

    # Sort by worthiness_score desc; promote top N
    candidates.sort(key=lambda x: x["worthiness_score"], reverse=True)
    winners = candidates[:max(1, WINNERS_PER_RUN)]
    winner_rows = [w["raw_row"] for w in winners]

    written = _batch_write_status(ws_raw, raw_cm["status"], winner_rows, "READY_TO_POST")
    _log(f"✅ Winners promoted to READY_TO_POST: {len(winner_rows)} (cells written={written})")

    # Helpful visibility (no writes)
    for w in winners:
        _log(f"Winner: deal_id={w['deal_id']} score={w['worthiness_score']} verdict={w['verdict']} raw_row={w['raw_row']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
