# workers/ai_scorer.py
# FULL FILE REPLACEMENT — AI SCORER v4.8h (INGESTED_AT_UTC CANONICAL)
#
# Fixes: Eligible NEW candidates: 0 + skipped_no_ingest_ts=65
# Root cause: RAW_DEALS uses ingested_at_utc but scorer was not resolving it.
#
# Governance:
# - RAW_DEALS is the only writable state
# - RAW_DEALS_VIEW is read-only intelligence (RDV)
# - Scorer reads RDV for worthiness, writes status updates to RAW_DEALS only

from __future__ import annotations

import os
import json
import re
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials


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


def _build_header_index(headers: List[str]) -> Dict[str, int]:
    idx: Dict[str, int] = {}
    for i, h in enumerate(headers):
        hh = str(h or "").strip()
        if hh:
            idx[hh] = i
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


def _parse_dt(val: Any) -> Optional[dt.datetime]:
    if val is None:
        return None
    if isinstance(val, dt.datetime):
        return val if val.tzinfo else val.replace(tzinfo=dt.timezone.utc)

    s = str(val).strip()
    if not s:
        return None

    # ISO 8601, with optional Z
    try:
        if s.endswith("Z"):
            return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        d = dt.datetime.fromisoformat(s)
        return d if d.tzinfo else d.replace(tzinfo=dt.timezone.utc)
    except Exception:
        pass

    # Date-only: YYYY-MM-DD
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", s)
    if m:
        try:
            y, mo, d = map(int, m.groups())
            return dt.datetime(y, mo, d, tzinfo=dt.timezone.utc)
        except Exception:
            return None

    return None


def _find_ingest_header(headers: List[str]) -> Optional[str]:
    """
    Canonical for your sheet: ingested_at_utc
    Fallbacks kept for backward compatibility.
    """
    candidates = [
        "ingested_at_utc",  # CANONICAL (your header row)
        "ingested_at",
        "ingest_ts",
        "ingest_timestamp",
        "ingestedAt",
        "ingested",
        "created_utc",
        "created_at",
        "timestamp",
    ]

    header_set = set(headers)
    for c in candidates:
        if c in header_set:
            return c

    # normalized matching
    norm_map = {_norm_header(h): h for h in headers}
    for c in candidates:
        nc = _norm_header(c)
        if nc in norm_map:
            return norm_map[nc]

    # heuristic: any header containing ingest + (utc/at/ts/time)
    for h in headers:
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


def main() -> int:
    SPREADSHEET_ID = (_env("SPREADSHEET_ID") or _env("SHEET_ID")).strip()
    if not SPREADSHEET_ID:
        log("❌ Missing SPREADSHEET_ID / SHEET_ID")
        return 1

    RAW_DEALS_TAB = _env("RAW_DEALS_TAB", "RAW_DEALS")
    RDV_TAB = _env("RAW_DEALS_VIEW_TAB", "RAW_DEALS_VIEW")
    ZTB_TAB = _env("ZTB_TAB", "ZTB")

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

    ztb_rows = ws_ztb.get_all_records()
    eligible = _eligible_themes_from_ztb(ztb_rows)
    theme_today = _theme_of_day(eligible)
    log(f"✅ ZTB: eligible_today={len(eligible)} | theme_today={theme_today} | pool={sorted(eligible)}")

    raw_headers = ws_raw.row_values(1)
    rdv_headers = ws_rdv.row_values(1)

    raw_idx = _build_header_index(raw_headers)

    ingest_header = _find_ingest_header(raw_headers)
    if not ingest_header:
        log("❌ Could not find an ingest timestamp header in RAW_DEALS.")
        return 1
    log(f"🕒 Using ingest timestamp header: {ingest_header}")

    # Required headers (exact names exist in your sheet)
    if "status" not in raw_idx or "deal_id" not in raw_idx:
        log("❌ RAW_DEALS missing required headers: status and/or deal_id")
        return 1

    # RDV lookup by deal_id (read-only)
    rdv_rows = ws_rdv.get_all_records()
    rdv_by_id: Dict[str, Dict[str, Any]] = {}
    for r in rdv_rows:
        did = str(r.get("deal_id") or "").strip()
        if did:
            rdv_by_id[did] = r

    raw_values = ws_raw.get_all_values()
    now = dt.datetime.now(dt.timezone.utc)

    eligible_rows: List[Tuple[int, str, Dict[str, Any]]] = []
    skipped_no_ingest_ts = 0
    skipped_too_fresh = 0
    skipped_too_old = 0
    skipped_missing_rdv = 0
    missing_ingest_ids: List[str] = []

    def get_cell(row: List[str], col_name: str) -> str:
        j = raw_idx.get(col_name)
        if j is None:
            return ""
        return row[j] if j < len(row) else ""

    for rownum in range(2, len(raw_values) + 1):
        row = raw_values[rownum - 1]
        if not row:
            continue

        status = get_cell(row, "status").strip()
        if status != "NEW":
            continue

        did = get_cell(row, "deal_id").strip()
        if not did:
            continue

        ingest_val = get_cell(row, ingest_header).strip()
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

    def rdv_get(r: Dict[str, Any], key: str) -> str:
        if key in r:
            return str(r.get(key) or "").strip()
        nk = _norm_header(key)
        for kk in r.keys():
            if _norm_header(kk) == nk:
                return str(r.get(kk) or "").strip()
        return ""

    def score_of(rdv: Dict[str, Any]) -> float:
        for k in ("priority_score", "worthiness_score"):
            v = rdv_get(rdv, k)
            try:
                return float(v)
            except Exception:
                continue
        return 0.0

    # Sort candidates by RDV priority_score (desc)
    eligible_rows.sort(key=lambda t: score_of(t[2]), reverse=True)

    winners_pro: List[int] = []
    winners_vip: List[int] = []
    winners_free: List[int] = []
    to_scored: List[int] = []

    for rownum, did, rdv in eligible_rows:
        verdict = rdv_get(rdv, "worthiness_verdict").upper()
        hard_reject = rdv_get(rdv, "hard_reject").upper() == "TRUE"

        if hard_reject:
            to_scored.append(rownum)
            continue

        if verdict.startswith("PRO_") and len(winners_pro) < QUOTA_PRO:
            winners_pro.append(rownum)
            continue

        if ("VIP" in verdict or verdict.startswith("POSTABLE")) and len(winners_vip) < QUOTA_VIP:
            winners_vip.append(rownum)
            continue

        if len(winners_free) < QUOTA_FREE:
            winners_free.append(rownum)
            continue

        to_scored.append(rownum)

    # Batch update statuses in RAW_DEALS
    status_col = raw_idx["status"] + 1  # 1-based for gspread
    updates: List[gspread.Cell] = []

    for r in to_scored:
        updates.append(gspread.Cell(r, status_col, "SCORED"))
    for r in winners_pro + winners_vip + winners_free:
        updates.append(gspread.Cell(r, status_col, "READY_TO_POST"))

    if updates:
        ws_raw.update_cells(updates, value_input_option="USER_ENTERED")

    log(
        f"✅ Promoted: PRO={len(winners_pro)} VIP={len(winners_vip)} FREE={len(winners_free)} "
        f"| Marked SCORED={len(to_scored)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
