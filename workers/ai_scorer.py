# workers/ai_scorer.py
# FULL FILE REPLACEMENT — AI SCORER v4.8j (PHRASE_BANK RESTORED + RDV DUPLICATE-SAFE)
#
# Fixes restored behavior:
# - When a row is promoted NEW -> READY_TO_POST, populate:
#     * RAW_DEALS.phrase_bank (required)
#     * RAW_DEALS.phrase_used (if column exists)
#
# Maintains fixes:
# - Uses ingested_at_utc (canonical) for ingest timestamp
# - Reads RAW_DEALS_VIEW (RDV) via get_all_values() to avoid duplicate header crash
#
# Governance:
# - Writes ONLY to RAW_DEALS
# - Reads RDV and PHRASE_BANK (read-only)
# - Does not write to RDV

from __future__ import annotations

import os
import json
import re
import hashlib
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials


# -----------------------------
# Logging / env helpers
# -----------------------------

def log(msg: str) -> None:
    ts = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    print(f"{ts} | {msg}", flush=True)


def _env(k: str, default: str = "") -> str:
    return str(os.getenv(k, default) or "").strip()


def _env_int(k: str, default: int) -> int:
    v = _env(k, "")
    try:
        return int(v)
    except Exception:
        return default


def _is_true(v: Any) -> bool:
    s = str(v or "").strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def _norm_header(h: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (h or "").strip().lower()).strip("_")


def _build_first_index(headers: List[str]) -> Dict[str, int]:
    """Exact header -> first index (0-based). Duplicate-safe."""
    idx: Dict[str, int] = {}
    for i, h in enumerate(headers):
        hh = str(h or "").strip()
        if hh and hh not in idx:
            idx[hh] = i
    return idx


def _build_norm_first_index(headers: List[str]) -> Dict[str, int]:
    """Normalized header -> first index (0-based). Duplicate-safe."""
    idx: Dict[str, int] = {}
    for i, h in enumerate(headers):
        nh = _norm_header(h)
        if nh and nh not in idx:
            idx[nh] = i
    return idx


def _get_sa_json() -> Dict[str, Any]:
    raw = _env("GCP_SA_JSON_ONE_LINE") or _env("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON or GCP_SA_JSON_ONE_LINE")
    try:
        return json.loads(raw)
    except Exception:
        return json.loads(raw.replace("\\n", "\n"))


def _get_gspread_client() -> gspread.Client:
    creds_info = _get_sa_json()
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)


# -----------------------------
# Date parsing / theme-of-day
# -----------------------------

def _parse_dt(val: Any) -> Optional[dt.datetime]:
    if val is None:
        return None
    if isinstance(val, dt.datetime):
        return val if val.tzinfo else val.replace(tzinfo=dt.timezone.utc)

    s = str(val).strip()
    if not s:
        return None

    try:
        if s.endswith("Z"):
            return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        d = dt.datetime.fromisoformat(s)
        return d if d.tzinfo else d.replace(tzinfo=dt.timezone.utc)
    except Exception:
        pass

    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", s)
    if m:
        try:
            y, mo, d = map(int, m.groups())
            return dt.datetime(y, mo, d, tzinfo=dt.timezone.utc)
        except Exception:
            return None

    return None


def _find_ingest_header(raw_headers: List[str]) -> Optional[str]:
    # Canonical first
    candidates = [
        "ingested_at_utc",
        "ingested_at",
        "ingest_ts",
        "ingest_timestamp",
        "created_utc",
        "created_at",
        "timestamp",
    ]
    header_set = set(raw_headers)
    for c in candidates:
        if c in header_set:
            return c

    norm_map = {_norm_header(h): h for h in raw_headers}
    for c in candidates:
        nc = _norm_header(c)
        if nc in norm_map:
            return norm_map[nc]

    for h in raw_headers:
        nh = _norm_header(h)
        if "ingest" in nh and ("utc" in nh or "at" in nh or "ts" in nh or "time" in nh):
            return h

    return None


def _mmdd(d: dt.date) -> int:
    return int(d.strftime("%m%d"))


def _in_window(mmdd: int, start_mmdd: int, end_mmdd: int) -> bool:
    if start_mmdd <= end_mmdd:
        return start_mmdd <= mmdd <= end_mmdd
    return (mmdd >= start_mmdd) or (mmdd <= end_mmdd)


def _eligible_themes_from_ztb(ztb_rows: List[Dict[str, Any]]) -> List[str]:
    today_mmdd = _mmdd(dt.datetime.now(dt.timezone.utc).date())
    themes: List[str] = []
    for r in ztb_rows:
        theme = str(r.get("theme") or "").strip()
        if not theme:
            continue
        if not _is_true(r.get("enabled")):
            continue
        start_mmdd = int(r.get("start_mmdd") or 101)
        end_mmdd = int(r.get("end_mmdd") or 1231)
        if _in_window(today_mmdd, start_mmdd, end_mmdd):
            themes.append(theme)
    return list(dict.fromkeys(themes))


def _theme_of_day(eligible: List[str]) -> str:
    if not eligible:
        return "unexpected_value"
    base = dt.date(2026, 1, 1)
    idx = (dt.datetime.now(dt.timezone.utc).date() - base).days % len(eligible)
    return sorted(eligible)[idx]


# -----------------------------
# Phrase selection
# -----------------------------

def _sha_int(s: str) -> int:
    return int(hashlib.sha1(s.encode("utf-8")).hexdigest(), 16)


def _pick_phrase(
    deal_id: str,
    theme: str,
    channel: str,
    phrases: List[Dict[str, Any]],
) -> str:
    """
    Deterministic selection.
    PHRASE_BANK expected headers:
      theme, category, phrase, approved, channel_hint, max_per_month, notes
    """
    th = (theme or "").strip().lower()
    ch = (channel or "").strip().upper()

    # 1) strict match: theme + channel_hint
    strict: List[str] = []
    for r in phrases:
        if not _is_true(r.get("approved")):
            continue
        p = str(r.get("phrase") or "").strip()
        if not p:
            continue
        rt = str(r.get("theme") or "").strip().lower()
        rh = str(r.get("channel_hint") or "").strip().upper()
        if rt == th and rh == ch:
            strict.append(p)

    if strict:
        idx = _sha_int(f"{deal_id}|{th}|{ch}|strict") % len(strict)
        return strict[idx]

    # 2) theme-only approved
    theme_only: List[str] = []
    for r in phrases:
        if not _is_true(r.get("approved")):
            continue
        p = str(r.get("phrase") or "").strip()
        if not p:
            continue
        rt = str(r.get("theme") or "").strip().lower()
        if rt == th:
            theme_only.append(p)

    if theme_only:
        idx = _sha_int(f"{deal_id}|{th}|theme") % len(theme_only)
        return theme_only[idx]

    # 3) any approved fallback
    any_ok: List[str] = []
    for r in phrases:
        if not _is_true(r.get("approved")):
            continue
        p = str(r.get("phrase") or "").strip()
        if p:
            any_ok.append(p)

    if any_ok:
        idx = _sha_int(f"{deal_id}|any") % len(any_ok)
        return any_ok[idx]

    return ""


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    SPREADSHEET_ID = (_env("SPREADSHEET_ID") or _env("SHEET_ID")).strip()
    if not SPREADSHEET_ID:
        log("❌ Missing SPREADSHEET_ID / SHEET_ID")
        return 1

    RAW_DEALS_TAB = _env("RAW_DEALS_TAB", "RAW_DEALS")
    RDV_TAB = _env("RAW_DEALS_VIEW_TAB", "RAW_DEALS_VIEW")
    ZTB_TAB = _env("ZTB_TAB", "ZTB")
    PHRASE_BANK = _env("PHRASE_BANK", "PHRASE_BANK")

    MIN_INGEST_AGE_SECONDS = _env_int("MIN_INGEST_AGE_SECONDS", 30)
    MAX_AGE_HOURS = _env_int("SCORER_MAX_AGE_HOURS", 72)

    QUOTA_PRO = _env_int("SCORER_QUOTA_PRO", 1)
    QUOTA_VIP = _env_int("SCORER_QUOTA_VIP", 2)
    QUOTA_FREE = _env_int("SCORER_QUOTA_FREE", 1)

    gc = _get_gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_raw = sh.worksheet(RAW_DEALS_TAB)
    ws_rdv = sh.worksheet(RDV_TAB)

    try:
        ws_ztb = sh.worksheet(ZTB_TAB)
    except Exception:
        ws_ztb = sh.worksheet("ZONE_THEME_BENCHMARKS")

    ws_phr = sh.worksheet(PHRASE_BANK)

    # Theme-of-day
    ztb_rows = ws_ztb.get_all_records()
    eligible = _eligible_themes_from_ztb(ztb_rows)
    theme_today = _theme_of_day(eligible)
    log(f"✅ ZTB: eligible_today={len(eligible)} | theme_today={theme_today} | pool={sorted(eligible)}")

    # Load phrase bank (this sheet should have unique headers; if it doesn't, we can harden it too)
    phrase_rows = ws_phr.get_all_records()
    approved_count = sum(1 for r in phrase_rows if _is_true(r.get("approved")) and str(r.get("phrase") or "").strip())
    log(f"✅ PHRASE_BANK loaded: {len(phrase_rows)} rows | approved_with_phrase={approved_count}")

    # RAW headers
    raw_headers = ws_raw.row_values(1)
    raw_idx = _build_first_index(raw_headers)

    required_raw = ["status", "deal_id", "ingested_at_utc"]
    for col in ("status", "deal_id"):
        if col not in raw_idx:
            log(f"❌ RAW_DEALS missing required header: {col}")
            return 1

    ingest_header = _find_ingest_header(raw_headers)
    if not ingest_header:
        log("❌ Could not find an ingest timestamp header in RAW_DEALS.")
        return 1
    log(f"🕒 Using ingest timestamp header: {ingest_header}")

    # Optional write columns
    phrase_bank_col = "phrase_bank" if "phrase_bank" in raw_idx else None
    phrase_used_col = "phrase_used" if "phrase_used" in raw_idx else None

    if not phrase_bank_col:
        log("❌ RAW_DEALS missing required header: phrase_bank (needed to lock phrase).")
        return 1

    # RDV duplicate-safe load
    rdv_values = ws_rdv.get_all_values()
    if not rdv_values or len(rdv_values) < 2:
        log("❌ RDV appears empty (no rows).")
        return 1

    rdv_headers = rdv_values[0]
    rdv_norm_idx = _build_norm_first_index(rdv_headers)

    def rdv_cell(row: List[str], *candidate_headers: str) -> str:
        for h in candidate_headers:
            j = rdv_norm_idx.get(_norm_header(h))
            if j is not None and j < len(row):
                return str(row[j] or "").strip()
        return ""

    # Build RDV lookup by deal_id
    rdv_by_id: Dict[str, Dict[str, str]] = {}
    rdv_missing_did = 0
    for r in rdv_values[1:]:
        did = rdv_cell(r, "deal_id")
        if not did:
            rdv_missing_did += 1
            continue
        if did in rdv_by_id:
            continue
        rdv_by_id[did] = {
            "deal_id": did,
            "worthiness_verdict": rdv_cell(r, "worthiness_verdict"),
            "hard_reject": rdv_cell(r, "hard_reject"),
            "priority_score": rdv_cell(r, "priority_score"),
            "worthiness_score": rdv_cell(r, "worthiness_score"),
            "dynamic_theme": rdv_cell(r, "dynamic_theme"),
        }

    if rdv_missing_did:
        log(f"⚠️ RDV rows missing deal_id: {rdv_missing_did}")

    # Load RAW values (with row numbers)
    raw_values = ws_raw.get_all_values()
    now = dt.datetime.now(dt.timezone.utc)

    # Candidate selection
    eligible_rows: List[Tuple[int, str, Dict[str, str]]] = []
    skipped_no_ingest_ts = 0
    skipped_too_fresh = 0
    skipped_too_old = 0
    skipped_missing_rdv = 0
    missing_ingest_ids: List[str] = []

    def raw_cell(row: List[str], col: str) -> str:
        j = raw_idx.get(col)
        if j is None:
            return ""
        return row[j] if j < len(row) else ""

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

        ingest_val = raw_cell(row, ingest_header).strip()
        ts = _parse_dt(ingest_val)
        if not ts:
            skipped_no_ingest_ts += 1
            if len(missing_ingest_ids) < 10:
                missing_ingest_ids.append(did)
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

        eligible_rows.append((rownum, did, rdv))

    log(
        f"Eligible NEW candidates: {len(eligible_rows)} | "
        f"skipped_too_fresh={skipped_too_fresh} skipped_no_ingest_ts={skipped_no_ingest_ts} "
        f"skipped_too_old={skipped_too_old} skipped_missing_raw_row={skipped_missing_rdv}"
    )
    if skipped_no_ingest_ts:
        log(f"⚠️ Missing ingest ts for first {len(missing_ingest_ids)} NEW rows: {missing_ingest_ids}")

    if not eligible_rows:
        log("No eligible NEW rows")
        return 0

    def score_of(rdv: Dict[str, str]) -> float:
        for k in ("priority_score", "worthiness_score"):
            v = (rdv.get(k) or "").strip()
            try:
                return float(v)
            except Exception:
                continue
        return 0.0

    eligible_rows.sort(key=lambda t: score_of(t[2]), reverse=True)

    # Allocate winners
    winners_pro: List[Tuple[int, str]] = []
    winners_vip: List[Tuple[int, str]] = []
    winners_free: List[Tuple[int, str]] = []
    to_scored: List[int] = []

    for rownum, did, rdv in eligible_rows:
        verdict = (rdv.get("worthiness_verdict") or "").upper().strip()
        hard_reject = (rdv.get("hard_reject") or "").upper().strip() == "TRUE"

        if hard_reject:
            to_scored.append(rownum)
            continue

        if verdict.startswith("PRO_") and len(winners_pro) < QUOTA_PRO:
            winners_pro.append((rownum, did))
            continue

        if ("VIP" in verdict or verdict.startswith("POSTABLE")) and len(winners_vip) < QUOTA_VIP:
            winners_vip.append((rownum, did))
            continue

        if len(winners_free) < QUOTA_FREE:
            winners_free.append((rownum, did))
            continue

        to_scored.append(rownum)

    # Prepare batch updates
    updates: List[gspread.Cell] = []
    status_col = raw_idx["status"] + 1
    phrase_bank_col_idx = raw_idx[phrase_bank_col] + 1
    phrase_used_col_idx = (raw_idx[phrase_used_col] + 1) if phrase_used_col else None

    def set_status(rownum: int, v: str) -> None:
        updates.append(gspread.Cell(rownum, status_col, v))

    def set_phrase(rownum: int, deal_id: str, theme: str, channel: str) -> None:
        phrase = _pick_phrase(deal_id=deal_id, theme=theme, channel=channel, phrases=phrase_rows)
        if phrase:
            updates.append(gspread.Cell(rownum, phrase_bank_col_idx, phrase))
            if phrase_used_col_idx is not None:
                updates.append(gspread.Cell(rownum, phrase_used_col_idx, phrase))

    # SCORED
    for r in to_scored:
        set_status(r, "SCORED")

    # READY_TO_POST + phrase lock
    for rownum, did in winners_pro:
        set_status(rownum, "READY_TO_POST")
        set_phrase(rownum, did, theme_today, "PRO")

    for rownum, did in winners_vip:
        set_status(rownum, "READY_TO_POST")
        set_phrase(rownum, did, theme_today, "VIP")

    for rownum, did in winners_free:
        set_status(rownum, "READY_TO_POST")
        set_phrase(rownum, did, theme_today, "FREE")

    if updates:
        ws_raw.update_cells(updates, value_input_option="USER_ENTERED")

    log(
        f"✅ Promoted: PRO={len(winners_pro)} VIP={len(winners_vip)} FREE={len(winners_free)} "
        f"| Marked SCORED={len(to_scored)}"
    )

    # Extra visibility
    phrase_written = sum(1 for c in updates if c.col == phrase_bank_col_idx)
    log(f"📝 phrase_bank written: {phrase_written}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
