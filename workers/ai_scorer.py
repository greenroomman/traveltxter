# workers/ai_scorer.py
from __future__ import annotations

import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials

# ============================================================
# TRAVELTXTTER V5 — SCORER (MINIMAL, PIPELINE-COMPLIANT)
#
# PURPOSE
# - Promote newly-ingested deals into the publish pipeline.
#
# CONTRACT (V5)
# - Input tab:  RAW_DEALS (writable, source of truth)
# - Optional:   RAW_DEALS_VIEW (read-only signals; non-fatal if absent)
# - Control:    OPS_MASTER theme cell is B2 (authoritative)
#
# STATUS MODEL (V5)
# - Feeder writes: status = NEW (or blank, treated as NEW)
# - Scorer writes:
#     NEW -> READY_TO_POST (publishable)
#     NEW -> SCORED        (not publishable)
#     NEW -> HARD_REJECT   (invalid / bad row)
#
# NOTE
# - Render/enrich read status but do not change it.
# - This file is idempotent and safe to re-run.
# ============================================================


# ----------------------------- Env -----------------------------
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID") or ""
RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS")
RAW_DEALS_VIEW_TAB = os.getenv("RAW_DEALS_VIEW_TAB", "RAW_DEALS_VIEW")
OPS_MASTER_TAB = os.getenv("OPS_MASTER_TAB", "OPS_MASTER")

# Theme of day is OPS_MASTER!B2 (per your latest rule)
OPS_THEME_CELL = os.getenv("OPS_THEME_CELL", "B2")

MIN_INGEST_AGE_SECONDS = int(os.getenv("MIN_INGEST_AGE_SECONDS", "90"))

# How far back we score NEW deals (avoid touching ancient test rows)
MAX_AGE_HOURS = float(os.getenv("SCORER_MAX_AGE_HOURS", "24"))

# Thresholds (simple + deterministic)
VIP_SCORE_THRESHOLD = float(os.getenv("VIP_SCORE_THRESHOLD", "65"))
HARD_REJECT_SCORE_THRESHOLD = float(os.getenv("HARD_REJECT_SCORE_THRESHOLD", "10"))

# ----------------------------- Columns -----------------------------
REQUIRED_HEADERS = [
    "deal_id",
    "origin_iata",
    "destination_iata",
    "outbound_date",
    "return_date",
    "price_gbp",
    "theme",
    "status",
    "ingested_at_utc",
]

WRITE_HEADERS = [
    "status",
    "score",
    "publish_window",
    "scored_timestamp",
]


@dataclass(frozen=True)
class RDVSignals:
    age_hours: Optional[float] = None
    is_fresh_24h: Optional[bool] = None
    dynamic_theme: Optional[str] = None
    fallback_rank: Optional[float] = None


# ----------------------------- Logging -----------------------------
def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"{ts} | {msg}", flush=True)


# ----------------------------- SA JSON robust loader -----------------------------
_PRIVATE_KEY_RE = re.compile(r'("private_key"\s*:\s*")(.+?)(")', re.DOTALL)


def _normalize_sa_json(raw: str) -> str:
    raw = (raw or "").strip()

    # If it looks like it was pasted with literal newlines inside the private_key string,
    # normal json.loads will throw "Invalid control character".
    if '"private_key"' in raw and "\n" in raw:
        def _fix(m: re.Match) -> str:
            key_body = m.group(2)
            key_body = key_body.replace("\r\n", "\n").replace("\r", "\n")
            key_body = key_body.replace("\n", "\\n")
            return f'{m.group(1)}{key_body}{m.group(3)}'

        raw = _PRIVATE_KEY_RE.sub(_fix, raw, count=1)

    # Common GitHub secret pattern: newlines are stored as \\n
    if "\\n" in raw and '"private_key"' in raw:
        raw = raw.replace("\\n", "\\n")  # keep escaped

    return raw


def gspread_client() -> gspread.Client:
    raw = os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON") or ""
    if not raw.strip():
        raise RuntimeError("Missing service account JSON in GCP_SA_JSON or GCP_SA_JSON_ONE_LINE")

    normalized = _normalize_sa_json(raw)
    try:
        info = json.loads(normalized)
    except json.JSONDecodeError:
        # One more pass: some users store literal \\n but also wrap the entire json in quotes
        normalized = normalized.strip().strip("'").strip('"')
        info = json.loads(_normalize_sa_json(normalized))

    creds = Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


# ----------------------------- Helpers -----------------------------
def normalize_header(h: str) -> str:
    return re.sub(r"\s+", "_", (h or "").strip().lower())


def header_map(headers: List[str]) -> Dict[str, int]:
    """
    Normalizes headers and returns first occurrence index.
    This avoids failures when Sheets has accidental whitespace.
    """
    hmap: Dict[str, int] = {}
    for i, h in enumerate(headers):
        k = normalize_header(h)
        if k and k not in hmap:
            hmap[k] = i
    return hmap


def col(row: List[str], hmap: Dict[str, int], name: str) -> str:
    idx = hmap.get(normalize_header(name))
    if idx is None or idx >= len(row):
        return ""
    return (row[idx] or "").strip()


def parse_float(x: str) -> Optional[float]:
    try:
        return float(str(x).strip())
    except Exception:
        return None


def parse_iso_utc(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    # Accept:
    # - 2026-02-05T16:29:08Z
    # - 2026-02-05T16:29:08+00:00
    # - 2026-02-05 16:29:08
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def a1_col(n: int) -> str:
    """0-indexed -> A1 column letters"""
    s = ""
    n += 1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def read_theme_of_day(sh: gspread.Spreadsheet) -> str:
    ws_ops = sh.worksheet(OPS_MASTER_TAB)
    theme = (ws_ops.acell(OPS_THEME_CELL).value or "").strip()
    return theme or "DEFAULT"


def load_rdv_signals(sh: gspread.Spreadsheet) -> Dict[str, RDVSignals]:
    """
    Optional. Non-fatal.
    Expected headers (minimal): deal_id, age_hours, is_fresh_24h, dynamic_theme, fallback_rank
    """
    try:
        ws = sh.worksheet(RAW_DEALS_VIEW_TAB)
    except Exception:
        return {}

    values = ws.get_all_values()
    if len(values) < 2:
        return {}

    headers = values[0]
    hmap = header_map(headers)
    out: Dict[str, RDVSignals] = {}

    for r in values[1:]:
        deal_id = col(r, hmap, "deal_id")
        if not deal_id:
            continue

        age = parse_float(col(r, hmap, "age_hours"))
        fresh_raw = col(r, hmap, "is_fresh_24h").lower()
        is_fresh = None
        if fresh_raw in ("true", "1", "yes"):
            is_fresh = True
        elif fresh_raw in ("false", "0", "no"):
            is_fresh = False

        dyn_theme = col(r, hmap, "dynamic_theme")
        fb_rank = parse_float(col(r, hmap, "fallback_rank"))

        out[deal_id] = RDVSignals(
            age_hours=age,
            is_fresh_24h=is_fresh,
            dynamic_theme=(dyn_theme.strip() if dyn_theme else None),
            fallback_rank=fb_rank,
        )

    return out


def compute_publish_window(ingested: datetime) -> str:
    """
    AM/PM are slot labels; scorer chooses the window purely from ingest timestamp:
    - 00:00–11:59 UTC => AM
    - 12:00–23:59 UTC => PM
    """
    return "AM" if ingested.hour < 12 else "PM"


def simple_score(price_gbp: Optional[float], rdv_fallback_rank: Optional[float]) -> float:
    """
    Deterministic scoring:
    - Prefer RDV fallback_rank if present (you said formulas are the brain now).
    - Else price-only heuristic to keep pipeline moving.
    """
    if rdv_fallback_rank is not None:
        # Clamp to 0..100 if formula gives weird values
        return max(0.0, min(100.0, float(rdv_fallback_rank)))

    if price_gbp is None:
        return 50.0

    # Basic: cheaper is better. Clamp to sensible range.
    if price_gbp <= 80:
        return 85.0
    if price_gbp <= 140:
        return 75.0
    if price_gbp <= 220:
        return 68.0
    if price_gbp <= 320:
        return 60.0
    return 45.0


def main() -> int:
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID (or SHEET_ID) env var")

    gc = gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    theme_today = read_theme_of_day(sh)
    log(f"🎯 Theme of day (OPS_MASTER!{OPS_THEME_CELL}): {theme_today}")

    # Load RDV signals (optional)
    rdv_map = load_rdv_signals(sh)
    if rdv_map:
        log(f"✅ RDV loaded for scoring signals: {len(rdv_map)} deal_ids indexed")
    else:
        log("⚠️ RDV signals not loaded (non-fatal). Using RAW_DEALS only.")

    ws_raw = sh.worksheet(RAW_DEALS_TAB)
    values = ws_raw.get_all_values()
    if not values or len(values) < 2:
        log("⚠️ RAW_DEALS has no rows to score.")
        return 0

    headers = values[0]
    hmap = header_map(headers)

    missing_required = [h for h in REQUIRED_HEADERS if normalize_header(h) not in hmap]
    if missing_required:
        raise RuntimeError(f"{RAW_DEALS_TAB} missing required headers: {missing_required}")

    # Identify write column indexes (some may be absent; we’ll skip)
    write_cols: Dict[str, int] = {}
    for h in WRITE_HEADERS:
        idx = hmap.get(normalize_header(h))
        if idx is not None:
            write_cols[h] = idx

    if "status" not in write_cols:
        # status is required by REQUIRED_HEADERS; this is defensive
        raise RuntimeError("RAW_DEALS missing 'status' column (required)")

    now = datetime.now(timezone.utc)
    updates: List[Tuple[str, List[List[object]]]] = []
    stats = {
        "eligible": 0,
        "too_fresh": 0,
        "too_old": 0,
        "no_ingest_ts": 0,
        "hard_reject": 0,
        "ready": 0,
        "scored": 0,
    }

    # Rows start at 2 in Sheets (row 1 is header)
    for i, row in enumerate(values[1:], start=2):
        deal_id = col(row, hmap, "deal_id")
        if not deal_id:
            continue

        status = col(row, hmap, "status").upper()
        # Treat blank as NEW to avoid “Eligible NEW candidates: 0” when feeder forgets status.
        if status not in ("", "NEW"):
            continue

        ingested_s = col(row, hmap, "ingested_at_utc")
        ingested = parse_iso_utc(ingested_s)
        if not ingested:
            stats["no_ingest_ts"] += 1
            continue

        age_sec = (now - ingested).total_seconds()
        age_hours = age_sec / 3600.0

        if age_sec < MIN_INGEST_AGE_SECONDS:
            stats["too_fresh"] += 1
            continue

        if age_hours > MAX_AGE_HOURS:
            stats["too_old"] += 1
            continue

        stats["eligible"] += 1

        # Theme: prefer RDV dynamic_theme if present, else RAW_DEALS theme
        sig = rdv_map.get(deal_id)
        dyn_theme = (sig.dynamic_theme if sig else None) or col(row, hmap, "theme")
        dyn_theme = (dyn_theme or "").strip() or theme_today

        # If you want hard theme-locking, enforce here.
        # For V5 reliability, we DO NOT hard-block; Telegram can still theme-filter or fallback via RDV.
        price = parse_float(col(row, hmap, "price_gbp"))
        score = simple_score(price, sig.fallback_rank if sig else None)

        publish_window = col(row, hmap, "publish_window").upper()
        if publish_window not in ("AM", "PM", "BOTH"):
            publish_window = compute_publish_window(ingested)

        # Decide status outcome
        if score < HARD_REJECT_SCORE_THRESHOLD:
            new_status = "HARD_REJECT"
            stats["hard_reject"] += 1
        elif score >= VIP_SCORE_THRESHOLD:
            new_status = "READY_TO_POST"
            stats["ready"] += 1
        else:
            new_status = "SCORED"
            stats["scored"] += 1

        # Prepare A1 updates (row-specific)
        row_updates: List[Tuple[int, object]] = []

        # status
        row_updates.append((write_cols["status"], new_status))

        # score
        if "score" in write_cols:
            row_updates.append((write_cols["score"], round(float(score), 2)))

        # publish_window
        if "publish_window" in write_cols:
            row_updates.append((write_cols["publish_window"], publish_window))

        # scored_timestamp (numeric epoch seconds — avoids RDV parse issues)
        if "scored_timestamp" in write_cols:
            row_updates.append((write_cols["scored_timestamp"], int(now.timestamp())))

        # Batch by contiguous ranges (simple: one cell per range is fine at this scale)
        for cidx, val in row_updates:
            a1 = f"{a1_col(cidx)}{i}"
            updates.append((a1, [[val]]))

    # Apply updates (batch_update is faster than per-cell)
    if updates:
        # Convert to batch_update payload
        data = [{"range": rng, "values": vals} for (rng, vals) in updates]
        ws_raw.batch_update(data, value_input_option="USER_ENTERED")
        log(
            f"✅ Status writes: READY_TO_POST={stats['ready']} SCORED={stats['scored']} HARD_REJECT={stats['hard_reject']}"
        )
    else:
        log("✅ No status changes needed (idempotent).")

    log(
        "SUMMARY: eligible={eligible} too_fresh={too_fresh} too_old={too_old} no_ingest_ts={no_ingest_ts}".format(
            **stats
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        # Fail loudly in CI
        print(f"ERROR: {e}", file=sys.stderr)
        raise
