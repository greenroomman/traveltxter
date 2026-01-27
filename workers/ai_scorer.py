# workers/ai_scorer.py
# FULL FILE REPLACEMENT — AI SCORER v4.8g (INGEST_TS COMPAT)
#
# Fixes: "skipped_no_ingest_ts" blocking all NEW candidates
# by accepting multiple ingest timestamp header variants and parsing them robustly.
#
# Governance:
# - Writes ONLY to RAW_DEALS
# - Reads RAW_DEALS_VIEW for verdict/scores
# - Does not touch RDV
# - No schema renames required

from __future__ import annotations

import os
import sys
import json
import re
import hashlib
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
if THIS_DIR not in sys.path:
    sys.path.insert(0, THIS_DIR)


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

    # Accept: ISO with Z, ISO with offset, "YYYY-MM-DD", "YYYY-MM-DDTHH:MM:SS"
    try:
        if s.endswith("Z"):
            return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.datetime.fromisoformat(s)
    except Exception:
        pass

    # Date-only
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", s)
    if m:
        try:
            y, mo, d = map(int, m.groups())
            return dt.datetime(y, mo, d, tzinfo=dt.timezone.utc)
        except Exception:
            return None

    return None


def _find_ingest_header(headers: List[str]) -> Optional[str]:
    # Prefer exact canonical header names
    canon = [
        "ingested_at",
        "ingested_at_utc",
        "ingest_ts",
        "ingest_timestamp",
        "ingestedAt",
        "ingested",
    ]

    header_set = set(headers)
    for c in canon:
        if c in header_set:
            return c

    # Try normalized matching (handles "Ingested At", "INGESTED_AT", etc.)
    norm_map = {_norm_header(h): h for h in headers}
    for c in canon:
        nc = _norm_header(c)
        if nc in norm_map:
            return norm_map[nc]

    # Last resort: any header containing "ingest" and "at"/"ts"/"time"
    for h in headers:
        nh = _norm_header(h)
        if "ingest" in nh and ("at" in nh or "ts" in nh or "time" in nh):
            return h

    return None


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

    # Per-run quotas
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

    # Theme of day (same deterministic ZTB logic as feeder)
    ztb_rows = ws_ztb.get_all_records()
    today = dt.datetime.now(dt.timezone.utc).date()

    def mmdd(d: dt.date) -> int:
        return int(d.strftime("%m%d"))

    def in_window(v: int, a: int, b: int) -> bool:
        if a <= b:
            return a <= v <= b
        return (v >= a) or (v <= b)

    eligible = []
    for r in ztb_rows:
        if not _is_true(r.get("enabled")):
            continue
        theme = str(r.get("theme") or "").strip()
        if not theme:
            continue
        a = int(r.get("start_mmdd") or 101)
        b = int(r.get("end_mmdd") or 1231)
        if in_window(mmdd(today), a, b):
            eligible.append(theme)

    eligible = sorted(list(dict.fromkeys(eligible)))
    if not eligible:
        theme_today = "unexpected_value"
    else:
        base = dt.date(2026, 1, 1)
        idx = (today - base).days % len(eligible)
        theme_today = eligible[idx]

    log(f"✅ ZTB: eligible_today={len(eligible)} | theme_today={theme_today} | pool={eligible}")

    raw_headers = ws_raw.row_values(1)
    rdv_headers = ws_rdv.row_values(1)

    raw_idx = _build_header_index(raw_headers)
    rdv_idx = _build_header_index(rdv_headers)

    # Locate ingest header in RAW_DEALS
    ingest_header = _find_ingest_header(raw_headers)
    if not ingest_header:
        log("❌ Could not find any ingest timestamp header in RAW_DEALS.")
        log("   Expected one of: ingested_at / ingested_at_utc / ingest_ts / ingest_timestamp ...")
        return 1
    log(f"🕒 Using ingest timestamp header: {ingest_header}")

    # Required columns
    col_status = next((h for h in raw_headers if _norm_header(h) == "status"), None)
    col_deal_id = next((h for h in raw_headers if _norm_header(h) == "deal_id"), None)

    if not col_status or not col_deal_id:
        log("❌ RAW_DEALS missing required headers: status and/or deal_id")
        return 1

    # Pull RDV records (read-only truth layer)
    rdv_rows = ws_rdv.get_all_records()

    # Build quick lookup: deal_id -> rdv_row
    rdv_by_id: Dict[str, Dict[str, Any]] = {}
    for r in rdv_rows:
        did = str(r.get("deal_id") or "").strip()
        if did:
            rdv_by_id[did] = r

    # Pull RAW rows (we need row numbers)
    raw_values = ws_raw.get_all_values()
    now = dt.datetime.now(dt.timezone.utc)

    eligible_rows: List[Tuple[int, Dict[str, Any], Dict[str, Any]]] = []
    skipped_no_ingest_ts = 0
    skipped_too_fresh = 0
    skipped_too_old = 0
    skipped_missing_raw_row = 0

    # For debug: capture first N deal_ids missing ingest ts
    missing_ingest_ids: List[str] = []

    for i in range(2, len(raw_values) + 1):
        row = raw_values[i - 1]
        if not row:
            continue

        def get(col_name: str) -> str:
            j = raw_idx.get(col_name)
            if j is None:
                return ""
            return row[j] if j < len(row) else ""

        status = get(col_status).strip()
        if status != "NEW":
            continue

        did = get(col_deal_id).strip()
        if not did:
            continue

        ingest_val = get(ingest_header).strip()
        ts = _parse_dt(ingest_val)
        if not ts:
            skipped_no_ingest_ts += 1
            if len(missing_ingest_ids) < 10:
                missing_ingest_ids.append(did)
            continue

        age = (now - ts).total_seconds()
        if age < MIN_INGEST_AGE_SECONDS:
            skipped_too_fresh += 1
            continue

        if age > MAX_AGE_HOURS * 3600:
            skipped_too_old += 1
            continue

        rdv = rdv_by_id.get(did)
        if not rdv:
            skipped_missing_raw_row += 1
            continue

        eligible_rows.append((i, {"deal_id": did}, rdv))

    log(
        f"Eligible NEW candidates: {len(eligible_rows)} | "
        f"skipped_too_fresh={skipped_too_fresh} skipped_no_ingest_ts={skipped_no_ingest_ts} "
        f"skipped_too_old={skipped_too_old} skipped_missing_raw_row={skipped_missing_raw_row}"
    )
    if skipped_no_ingest_ts > 0:
        log(f"⚠️ Missing ingest_ts for first {len(missing_ingest_ids)} NEW rows: {missing_ingest_ids}")

    if not eligible_rows:
        log("No eligible NEW rows")
        return 0

    # RDV fields used (best-effort names)
    def rdv_get(r: Dict[str, Any], *keys: str) -> str:
        for k in keys:
            if k in r:
                return str(r.get(k) or "").strip()
            # normalize lookup
            nk = _norm_header(k)
            for kk in r.keys():
                if _norm_header(kk) == nk:
                    return str(r.get(kk) or "").strip()
        return ""

    # Prepare winners
    winners_pro: List[Tuple[int, str]] = []
    winners_vip: List[Tuple[int, str]] = []
    winners_free: List[Tuple[int, str]] = []

    scored_rows: List[int] = []

    # Simple ranking: use worthiness_score if present else priority_score else 0
    def score_of(rdv: Dict[str, Any]) -> float:
        for k in ("priority_score", "worthiness_score", "score"):
            v = rdv_get(rdv, k)
            try:
                return float(v)
            except Exception:
                continue
        return 0.0

    # Filter by today's theme if RDV has dynamic_theme; else allow all NEW
    filtered = []
    for raw_rownum, raw_meta, rdv in eligible_rows:
        dyn = rdv_get(rdv, "dynamic_theme", "theme", "primary_theme").lower()
        if dyn and theme_today.lower() not in dyn:
            # allow if missing dynamic theme
            pass
        filtered.append((raw_rownum, raw_meta["deal_id"], rdv))

    filtered.sort(key=lambda t: score_of(t[2]), reverse=True)

    # Promotion buckets by worthiness_verdict
    for raw_rownum, did, rdv in filtered:
        verdict = rdv_get(rdv, "worthiness_verdict", "worthiness_verdict_text", "worthiness").upper()
        hard_reject = rdv_get(rdv, "hard_reject").upper() == "TRUE"

        if hard_reject:
            scored_rows.append(raw_rownum)
            continue

        # PRO candidate
        if verdict.startswith("PRO_") and len(winners_pro) < QUOTA_PRO:
            winners_pro.append((raw_rownum, did))
            continue

        # VIP candidate
        if ("VIP" in verdict or verdict.startswith("POSTABLE")) and len(winners_vip) < QUOTA_VIP:
            winners_vip.append((raw_rownum, did))
            continue

        # FREE candidate
        if len(winners_free) < QUOTA_FREE:
            winners_free.append((raw_rownum, did))
            continue

        scored_rows.append(raw_rownum)

    # Write status updates (batch by cell updates)
    # Determine status column index
    status_col_idx = raw_idx[col_status] + 1  # 1-based
    updates = []

    def upd(rownum: int, status: str) -> None:
        updates.append(gspread.Cell(rownum, status_col_idx, status))

    for r in scored_rows:
        upd(r, "SCORED")
    for r, _ in winners_pro:
        upd(r, "READY_TO_POST")
    for r, _ in winners_vip:
        upd(r, "READY_TO_POST")
    for r, _ in winners_free:
        upd(r, "READY_TO_POST")

    if updates:
        ws_raw.update_cells(updates, value_input_option="USER_ENTERED")

    log(f"✅ Promoted: PRO={len(winners_pro)} VIP={len(winners_vip)} FREE={len(winners_free)} | Marked SCORED={len(scored_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
