# workers/ai_scorer.py
# TRAVELTXTTER — AI SCORER (V5, MINIMAL SHEET CONTRACT)
#
# PURPOSE
# - Deterministically evaluate NEW rows in RAW_DEALS using RAW_DEALS_VIEW (RDV) formulas.
# - Write publish-intent triggers into RAW_DEALS:
#     PUBLISH_AM / PUBLISH_PM / PUBLISH_BOTH
# - Mark everything else as SCORED (or HARD_REJECT when RDV hard_reject=TRUE)
#
# GOVERNANCE (LOCKED)
# - RAW_DEALS is the ONLY writable source of truth.
# - RAW_DEALS_VIEW is read-only (formulas only).
# - This worker writes to RAW_DEALS only:
#     - status
#     - publish_window (AM/PM/BOTH)
#     - score (numeric, from RDV worthiness/priority)
#     - scored_timestamp (if column exists)
#
# SLOT RULE (LOCKED)
# - Slot defaults from ingested_at_utc (UTC hour):
#     hour < 12 => AM
#     else      => PM
#
# BOTH RULE (LOCKED)
# - Mark as BOTH when:
#     - worthiness_verdict starts with "PRO_" OR
#     - worthiness_score >= SCORER_BOTH_SCORE (default 80)
#
# PERFORMANCE
# - Single batch load for RAW_DEALS + RDV via get_all_values()
# - Single batch write via update_cells()

from __future__ import annotations

import os
import json
import re
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials


# -----------------------------
# Logging / env helpers
# -----------------------------

def log(msg: str) -> None:
    ts = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    print(f"{ts} | {msg}", flush=True)


def _env(k: str, default: str = "") -> str:
    return str(os.getenv(k, default) or "").strip()


def _env_int(k: str, default: int) -> int:
    v = _env(k, "")
    try:
        return int(v)
    except Exception:
        return default


def _norm_header(h: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (h or "").strip().lower()).strip("_")


def _build_first_index(headers: List[str]) -> Dict[str, int]:
    idx: Dict[str, int] = {}
    for i, h in enumerate(headers):
        hh = str(h or "").strip()
        if hh and hh not in idx:
            idx[hh] = i
    return idx


def _build_norm_first_index(headers: List[str]) -> Dict[str, int]:
    idx: Dict[str, int] = {}
    for i, h in enumerate(headers):
        nh = _norm_header(h)
        if nh and nh not in idx:
            idx[nh] = i
    return idx


def _strip_control_chars(s: str) -> str:
    # Remove ASCII control chars except \n \r \t to avoid JSONDecodeError invalid control character
    return re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", s)


def _get_gspread_client() -> gspread.Client:
    raw = _env("GCP_SA_JSON_ONE_LINE") or _env("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON or GCP_SA_JSON_ONE_LINE")

    raw = _strip_control_chars(raw)

    # Robust parsing for:
    # - one-line JSON
    # - JSON with escaped newlines
    # - JSON with literal newlines
    try:
        creds_info = json.loads(raw)
    except Exception:
        try:
            creds_info = json.loads(raw.replace("\\n", "\n"))
        except Exception:
            # last resort: collapse literal newlines to escaped newlines
            creds_info = json.loads(raw.replace("\n", "\\n"))

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)


# -----------------------------
# Timestamp parsing
# -----------------------------

_GOOGLE_SHEETS_EPOCH = dt.datetime(1899, 12, 30, tzinfo=dt.timezone.utc)

def _parse_dt(val: Any) -> Optional[dt.datetime]:
    """
    Accepts:
    - ISO8601 strings (with or without Z)
    - Google Sheets serial numbers (days since 1899-12-30)
    - Unix epoch seconds (>= 1e9)
    - Unix epoch milliseconds (>= 1e12)
    """
    if val is None:
        return None

    if isinstance(val, dt.datetime):
        return val if val.tzinfo else val.replace(tzinfo=dt.timezone.utc)

    s = str(val).strip()
    if not s:
        return None

    # numeric?
    try:
        f = float(s)
        # epoch ms
        if f >= 1_000_000_000_000:
            return dt.datetime.fromtimestamp(f / 1000.0, tz=dt.timezone.utc)
        # epoch sec
        if f >= 1_000_000_000:
            return dt.datetime.fromtimestamp(f, tz=dt.timezone.utc)
        # google sheets serial days
        # typical modern serials are > 40000
        if f >= 20000:
            return _GOOGLE_SHEETS_EPOCH + dt.timedelta(days=f)
    except Exception:
        pass

    # ISO
    try:
        if s.endswith("Z"):
            return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        d = dt.datetime.fromisoformat(s)
        return d if d.tzinfo else d.replace(tzinfo=dt.timezone.utc)
    except Exception:
        return None


def _find_header(raw_headers: List[str], *candidates: str) -> Optional[str]:
    header_set = set(raw_headers)
    for c in candidates:
        if c in header_set:
            return c
    norm_map = {_norm_header(h): h for h in raw_headers}
    for c in candidates:
        nc = _norm_header(c)
        if nc in norm_map:
            return norm_map[nc]
    return None


def _slot_from_ingest(ts: dt.datetime) -> str:
    return "AM" if ts.hour < 12 else "PM"


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    SPREADSHEET_ID = (_env("SPREADSHEET_ID") or _env("SHEET_ID")).strip()
    if not SPREADSHEET_ID:
        log("❌ Missing SPREADSHEET_ID / SHEET_ID")
        return 1

    RAW_DEALS_TAB = _env("RAW_DEALS_TAB", "RAW_DEALS")
    RDV_TAB = _env("RAW_DEALS_VIEW_TAB", "RAW_DEALS_VIEW")  # IMPORTANT: set this env in workflow

    MIN_INGEST_AGE_SECONDS = _env_int("MIN_INGEST_AGE_SECONDS", 90)
    MAX_AGE_HOURS = _env_int("SCORER_MAX_AGE_HOURS", 24)  # keep tight unless you explicitly widen it

    QUOTA_PRO = _env_int("SCORER_QUOTA_PRO", 1)
    QUOTA_VIP = _env_int("SCORER_QUOTA_VIP", 2)
    QUOTA_FREE = _env_int("SCORER_QUOTA_FREE", 1)

    BOTH_SCORE = float(_env_int("SCORER_BOTH_SCORE", 80))

    gc = _get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_raw = sh.worksheet(RAW_DEALS_TAB)
    ws_rdv = sh.worksheet(RDV_TAB)

    # Load RAW_DEALS
    log("📥 Loading RAW_DEALS...")
    raw_values = ws_raw.get_all_values()
    if not raw_values or len(raw_values) < 2:
        log("❌ RAW_DEALS appears empty (no data rows).")
        return 1

    raw_headers = raw_values[0]
    raw_idx = _build_first_index(raw_headers)

    # Required columns
    for col in ("status", "deal_id"):
        if col not in raw_idx:
            log(f"❌ RAW_DEALS missing required header: {col}")
            return 1

    # Ingest timestamp header (new sheet should be ingested_at_utc)
    ingest_header = _find_header(
        raw_headers,
        "ingested_at_utc",
        "ingested_at",
        "ingest_ts",
        "ingested_dt",
        "created_utc",
        "created_at",
        "timestamp",
    )
    if not ingest_header:
        log("❌ Could not find ingest timestamp header in RAW_DEALS (expected ingested_at_utc).")
        return 1
    log(f"🕒 Using ingest timestamp header: {ingest_header}")

    # Optional write-back columns (new contract: publish_window, score)
    publish_window_header = _find_header(raw_headers, "publish_window")
    score_header = _find_header(raw_headers, "score")
    scored_ts_header = _find_header(raw_headers, "scored_timestamp", "scored_at", "scored_at_utc")

    if not publish_window_header:
        log("⚠️ RAW_DEALS missing 'publish_window' column. Status triggers still written.")
    if not score_header:
        log("⚠️ RAW_DEALS missing 'score' column. Score write-back will be skipped.")
    if not scored_ts_header:
        log("ℹ️ RAW_DEALS missing 'scored_timestamp' column. Timestamp write-back will be skipped.")

    # Load RDV
    log("📥 Loading RAW_DEALS_VIEW...")
    rdv_values = ws_rdv.get_all_values()
    if not rdv_values or len(rdv_values) < 2:
        log("❌ RAW_DEALS_VIEW appears empty (no rows).")
        return 1

    rdv_headers = rdv_values[0]
    rdv_norm_idx = _build_norm_first_index(rdv_headers)

    def rdv_cell(row: List[str], *candidate_headers: str) -> str:
        for h in candidate_headers:
            j = rdv_norm_idx.get(_norm_header(h))
            if j is not None and j < len(row):
                return str(row[j] or "").strip()
        return ""

    # Build RDV lookup by deal_id (first occurrence wins)
    rdv_by_id: Dict[str, Dict[str, str]] = {}
    for r in rdv_values[1:]:
        did = rdv_cell(r, "deal_id")
        if not did or did in rdv_by_id:
            continue
        rdv_by_id[did] = {
            "deal_id": did,
            "worthiness_verdict": rdv_cell(r, "worthiness_verdict"),
            "hard_reject": rdv_cell(r, "hard_reject"),
            "priority_score": rdv_cell(r, "priority_score"),
            "worthiness_score": rdv_cell(r, "worthiness_score"),
        }

    def raw_cell(row: List[str], col: str) -> str:
        j = raw_idx.get(col)
        if j is None:
            return ""
        return row[j] if j < len(row) else ""

    def score_float(rdv: Dict[str, str]) -> float:
        for k in ("priority_score", "worthiness_score"):
            v = (rdv.get(k) or "").strip()
            try:
                return float(v)
            except Exception:
                continue
        return 0.0

    now = dt.datetime.now(dt.timezone.utc)

    eligible: List[Tuple[int, str, dt.datetime, Dict[str, str]]] = []
    skipped_too_fresh = 0
    skipped_too_old = 0
    skipped_no_ingest = 0
    skipped_missing_rdv = 0

    # Scan NEW rows
    for rownum in range(2, len(raw_values) + 1):
        row = raw_values[rownum - 1]
        if not row:
            continue

        status = raw_cell(row, "status").strip()
        if status != "NEW":
            continue

        did = raw_cell(row, "deal_id").strip()
        if not did:
            continue

        ts = _parse_dt(raw_cell(row, ingest_header))
        if not ts:
            skipped_no_ingest += 1
            continue

        age_s = (now - ts).total_seconds()
        if age_s < MIN_INGEST_AGE_SECONDS:
            skipped_too_fresh += 1
            continue
        if age_s > MAX_AGE_HOURS * 3600:
            skipped_too_old += 1
            continue

        rdv = rdv_by_id.get(did)
        if not rdv:
            skipped_missing_rdv += 1
            continue

        eligible.append((rownum, did, ts, rdv))

    log(
        f"Eligible NEW candidates: {len(eligible)} | "
        f"skipped_too_fresh={skipped_too_fresh} skipped_no_ingest_ts={skipped_no_ingest} "
        f"skipped_too_old={skipped_too_old} skipped_missing_rdv={skipped_missing_rdv}"
    )

    if not eligible:
        log("No eligible NEW rows")
        return 0

    # Sort highest score first
    eligible.sort(key=lambda t: score_float(t[3]), reverse=True)

    winners_pro: List[Tuple[int, str, dt.datetime, Dict[str, str]]] = []
    winners_vip: List[Tuple[int, str, dt.datetime, Dict[str, str]]] = []
    winners_free: List[Tuple[int, str, dt.datetime, Dict[str, str]]] = []
    others: List[Tuple[int, str, dt.datetime, Dict[str, str]]] = []
    hard_rejects: List[Tuple[int, str, dt.datetime, Dict[str, str]]] = []

    for rownum, did, ts, rdv in eligible:
        verdict = (rdv.get("worthiness_verdict") or "").upper().strip()
        hard_reject = (rdv.get("hard_reject") or "").upper().strip() == "TRUE"

        if hard_reject:
            hard_rejects.append((rownum, did, ts, rdv))
            continue

        if verdict.startswith("PRO_") and len(winners_pro) < QUOTA_PRO:
            winners_pro.append((rownum, did, ts, rdv))
            continue

        # Treat POSTABLE/VIP as VIP bucket
        if (("VIP" in verdict) or verdict.startswith("POSTABLE")) and len(winners_vip) < QUOTA_VIP:
            winners_vip.append((rownum, did, ts, rdv))
            continue

        if len(winners_free) < QUOTA_FREE:
            winners_free.append((rownum, did, ts, rdv))
            continue

        others.append((rownum, did, ts, rdv))

    # Prepare batch updates (RAW_DEALS only)
    updates: List[gspread.Cell] = []

    status_col = raw_idx["status"] + 1
    pubwin_col = (raw_idx[publish_window_header] + 1) if publish_window_header and publish_window_header in raw_idx else None
    score_col = (raw_idx[score_header] + 1) if score_header and score_header in raw_idx else None
    scored_col = (raw_idx[scored_ts_header] + 1) if scored_ts_header and scored_ts_header in raw_idx else None

    now_iso = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

    def set_cell(r: int, c: int, v: str) -> None:
        updates.append(gspread.Cell(r, c, v))

    def set_status(r: int, v: str) -> None:
        set_cell(r, status_col, v)

    def set_publish_window(r: int, v: str) -> None:
        if pubwin_col:
            set_cell(r, pubwin_col, v)

    def set_score(r: int, v: float) -> None:
        if score_col:
            # keep it numeric for RDV
            set_cell(r, score_col, f"{v:.2f}".rstrip("0").rstrip("."))

    def set_scored_ts(r: int) -> None:
        if scored_col:
            set_cell(r, scored_col, now_iso)

    def decide_publish_window(ts: dt.datetime, rdv: Dict[str, str]) -> str:
        slot = _slot_from_ingest(ts)  # AM/PM
        verdict = (rdv.get("worthiness_verdict") or "").upper().strip()
        s = score_float(rdv)
        if verdict.startswith("PRO_") or s >= BOTH_SCORE:
            return "BOTH"
        return slot

    def promote(rows: List[Tuple[int, str, dt.datetime, Dict[str, str]]]) -> None:
        for rownum, _did, ts, rdv in rows:
            pw = decide_publish_window(ts, rdv)
            s = score_float(rdv)
            set_publish_window(rownum, pw)
            set_status(rownum, f"PUBLISH_{pw}")
            set_score(rownum, s)
            set_scored_ts(rownum)

    # Hard rejects
    for rownum, _did, _ts, rdv in hard_rejects:
        set_status(rownum, "HARD_REJECT")
        set_score(rownum, score_float(rdv))
        set_scored_ts(rownum)

    # Winners
    promote(winners_pro)
    promote(winners_vip)
    promote(winners_free)

    # Others -> SCORED
    for rownum, _did, _ts, rdv in others:
        set_status(rownum, "SCORED")
        set_score(rownum, score_float(rdv))
        set_scored_ts(rownum)

    if updates:
        ws_raw.update_cells(updates, value_input_option="USER_ENTERED")

    log(
        f"✅ Status writes: PRO={len(winners_pro)} VIP={len(winners_vip)} FREE={len(winners_free)} "
        f"HARD_REJECT={len(hard_rejects)} SCORED={len(others)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
