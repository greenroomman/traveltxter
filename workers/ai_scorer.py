# workers/ai_scorer.py
# TRAVELTXTTER — SCORER (V5, MINIMAL SHEET CONTRACT)
#
# PURPOSE
# - Evaluate NEW rows in RAW_DEALS (deterministic, cheap).
# - Write publish-intent triggers into RAW_DEALS:
#     status = PUBLISH_AM / PUBLISH_PM / PUBLISH_BOTH
#     publish_window = AM / PM / BOTH
#     score = numeric (best-effort from RDV if present, else simple heuristic)
#     scored_timestamp = Google Sheets serial number (NOT ISO string) if column exists
#
# GOVERNANCE
# - RAW_DEALS is the ONLY writable DB.
# - RAW_DEALS_VIEW (RDV) is OPTIONAL read-only signal layer.
# - This worker writes ONLY these columns (if present):
#     status, publish_window, score, scored_timestamp
#
# SLOT RULE (LOCKED)
# - publish_window defaults from ingested_at_utc hour (UTC):
#     hour < 12 => AM
#     else      => PM
#
# BOTH RULE (LOCKED)
# - Mark BOTH when score >= SCORER_BOTH_SCORE (default 80)
#
# PERFORMANCE
# - One batch load for RAW_DEALS (+ optional RDV)
# - One batch update write

from __future__ import annotations

import os
import re
import json
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials


GOOGLE_SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]


# -----------------------------
# Logging / env
# -----------------------------
def log(msg: str) -> None:
    ts = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    print(f"{ts} | {msg}", flush=True)


def _env(k: str, default: str = "") -> str:
    return str(os.getenv(k, default) or "").strip()


def _env_int(k: str, default: int) -> int:
    try:
        return int(_env(k, str(default)))
    except Exception:
        return default


def _env_float(k: str, default: float) -> float:
    try:
        return float(_env(k, str(default)))
    except Exception:
        return default


def _norm_header(h: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (h or "").strip().lower()).strip("_")


def _first_index(headers: List[str]) -> Dict[str, int]:
    """First occurrence wins (immune to duplicate headers)."""
    idx: Dict[str, int] = {}
    for i, h in enumerate(headers):
        hh = (h or "").strip()
        if hh and hh not in idx:
            idx[hh] = i
    return idx


def _first_norm_index(headers: List[str]) -> Dict[str, int]:
    idx: Dict[str, int] = {}
    for i, h in enumerate(headers):
        nh = _norm_header(h)
        if nh and nh not in idx:
            idx[nh] = i
    return idx


def _strip_control_chars(s: str) -> str:
    # Remove ASCII control chars except \n \r \t (prevents JSONDecodeError invalid control character)
    return re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", s)


# -----------------------------
# Sheets helpers
# -----------------------------
def _repair_private_key_newlines(raw: str) -> str:
    """
    Repairs secrets where private_key contains literal newlines (invalid JSON string).
    Converts literal newlines in private_key to \\n.
    """
    pat = re.compile(r'("private_key"\s*:\s*")(.+?)(")', re.DOTALL)
    m = pat.search(raw)
    if not m:
        return raw
    prefix, pk, suffix = m.group(1), m.group(2), m.group(3)
    pk_fixed = pk.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "\\n").replace("\t", "\\t")
    return raw[: m.start()] + prefix + pk_fixed + suffix + raw[m.end() :]


def gspread_client() -> gspread.Client:
    raw = _env("GCP_SA_JSON_ONE_LINE") or _env("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON / GCP_SA_JSON_ONE_LINE")

    raw = _strip_control_chars(raw)
    raw = _repair_private_key_newlines(raw)

    try:
        info = json.loads(raw)
    except json.JSONDecodeError:
        info = json.loads(raw.replace("\\n", "\n"))

    creds = Credentials.from_service_account_info(info, scopes=GOOGLE_SCOPE)
    return gspread.authorize(creds)


def open_sheet(gc: gspread.Client) -> gspread.Spreadsheet:
    sid = _env("SPREADSHEET_ID") or _env("SHEET_ID")
    if not sid:
        raise RuntimeError("Missing SPREADSHEET_ID (or SHEET_ID)")
    return gc.open_by_key(sid)


def get_all(ws: gspread.Worksheet) -> List[List[str]]:
    return ws.get_all_values() or []


def col_i(headers: List[str], name: str) -> Optional[int]:
    want = _norm_header(name)
    nidx = _first_norm_index(headers)
    return nidx.get(want, None)


# -----------------------------
# Time helpers
# -----------------------------
def parse_iso_utc(s: str) -> Optional[dt.datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        d = dt.datetime.fromisoformat(s)
        return d.astimezone(dt.timezone.utc) if d.tzinfo else d.replace(tzinfo=dt.timezone.utc)
    except Exception:
        return None


def sheets_serial(now_utc: Optional[dt.datetime] = None) -> float:
    """
    Google Sheets serial date (days since 1899-12-30).
    Works reliably with numeric math in formulas.
    """
    d = now_utc or dt.datetime.now(dt.timezone.utc)
    # Unix epoch -> days -> + 25569 (Excel/Sheets offset)
    return d.timestamp() / 86400.0 + 25569.0


def infer_slot_from_ingest(ingested_at_utc: str) -> str:
    ts = parse_iso_utc(ingested_at_utc)
    if not ts:
        return "PM"  # safe default
    return "AM" if ts.hour < 12 else "PM"


# -----------------------------
# Optional RDV mapping
# -----------------------------
def build_rdv_map(rdv_values: List[List[str]]) -> Dict[str, Dict[str, str]]:
    """
    Map deal_id -> signals from RDV if present.
    We only consume:
      - score (or worthiness_score / priority_score)
      - worthiness_verdict
      - hard_reject (bool-ish)
    """
    if not rdv_values or len(rdv_values) < 2:
        return {}

    headers = rdv_values[0]
    i_deal = col_i(headers, "deal_id")
    if i_deal is None:
        return {}

    i_score = col_i(headers, "score")
    if i_score is None:
        i_score = col_i(headers, "worthiness_score")
    if i_score is None:
        i_score = col_i(headers, "priority_score")

    i_verdict = col_i(headers, "worthiness_verdict")
    i_reject = col_i(headers, "hard_reject")

    out: Dict[str, Dict[str, str]] = {}
    for row in rdv_values[1:]:
        did = (row[i_deal] if i_deal < len(row) else "").strip()
        if not did:
            continue
        out[did] = {
            "score": (row[i_score] if i_score is not None and i_score < len(row) else "").strip(),
            "verdict": (row[i_verdict] if i_verdict is not None and i_verdict < len(row) else "").strip(),
            "hard_reject": (row[i_reject] if i_reject is not None and i_reject < len(row) else "").strip(),
        }
    return out


def truthy(v: str) -> bool:
    x = (v or "").strip().lower()
    return x in ("true", "1", "yes", "y", "hard_reject", "reject")


def safe_float(v: str) -> Optional[float]:
    v = (v or "").strip()
    if not v:
        return None
    try:
        return float(v)
    except Exception:
        return None


# -----------------------------
# Scoring fallback (cheap)
# -----------------------------
def fallback_score(price_gbp: str, stops: str) -> float:
    """
    A deliberately simple, deterministic fallback if RDV isn't available.
    Higher is better. Range ~0-100.
    """
    p = safe_float(price_gbp) or 9999.0
    s = safe_float(stops) or 0.0

    # price component (very rough)
    if p <= 120:
        price_part = 85
    elif p <= 180:
        price_part = 75
    elif p <= 260:
        price_part = 65
    elif p <= 350:
        price_part = 55
    else:
        price_part = 40

    # friction penalty
    friction = 10 * min(2.0, s)  # stops 0..2 => 0..20
    return max(0.0, min(100.0, price_part - friction))


# -----------------------------
# Main
# -----------------------------
def main() -> int:
    RAW_TAB = _env("RAW_DEALS_TAB", "RAW_DEALS")
    RDV_TAB = _env("RAW_DEALS_VIEW_TAB", _env("RAW_DEALS_VIEW", "RAW_DEALS_VIEW"))

    BOTH_SCORE = _env_float("SCORER_BOTH_SCORE", 80.0)
    MIN_AGE = _env_int("MIN_INGEST_AGE_SECONDS", 90)

    gc = gspread_client()
    sh = open_sheet(gc)

    ws_raw = sh.worksheet(RAW_TAB)
    raw_values = get_all(ws_raw)
    if not raw_values or len(raw_values) < 2:
        log("RAW_DEALS empty. Nothing to score.")
        return 0

    headers = raw_values[0]
    idx_status = col_i(headers, "status")
    idx_deal = col_i(headers, "deal_id")
    idx_ingest = col_i(headers, "ingested_at_utc")
    idx_price = col_i(headers, "price_gbp")
    idx_stops = col_i(headers, "stops")

    idx_publish_window = col_i(headers, "publish_window")
    idx_score = col_i(headers, "score")
    idx_scored_ts = col_i(headers, "scored_timestamp")

    missing_core = [n for n, i in [
        ("deal_id", idx_deal),
        ("status", idx_status),
        ("ingested_at_utc", idx_ingest),
    ] if i is None]
    if missing_core:
        raise RuntimeError(f"RAW_DEALS missing required headers: {missing_core}")

    # Optional RDV read
    rdv_map: Dict[str, Dict[str, str]] = {}
    try:
        ws_rdv = sh.worksheet(RDV_TAB)
        rdv_values = get_all(ws_rdv)
        rdv_map = build_rdv_map(rdv_values)
        if rdv_map:
            log(f"✅ RDV loaded for scoring signals: {len(rdv_map)} deal_ids indexed")
        else:
            log("ℹ️ RDV present but no usable mapping found (missing deal_id/score columns). Using fallback scoring.")
    except Exception:
        log("⚠️ RDV not available (non-fatal). Using fallback scoring.")

    now = dt.datetime.now(dt.timezone.utc)
    serial_now = sheets_serial(now)

    # Build updates
    cells: List[gspread.cell.Cell] = []
    scanned = 0
    eligible = 0
    wrote_publish = 0
    wrote_scored = 0
    wrote_reject = 0

    for r_i, row in enumerate(raw_values[1:], start=2):  # sheet row number
        scanned += 1

        status = (row[idx_status] if idx_status < len(row) else "").strip().upper()
        if status != "NEW":
            continue

        deal_id = (row[idx_deal] if idx_deal < len(row) else "").strip()
        if not deal_id:
            continue

        ing = (row[idx_ingest] if idx_ingest < len(row) else "").strip()
        ts = parse_iso_utc(ing)
        if not ts:
            continue

        # too fresh guard
        if (now - ts).total_seconds() < MIN_AGE:
            continue

        eligible += 1

        # resolve score/verdict
        rdv = rdv_map.get(deal_id, {})
        rdv_score = safe_float(rdv.get("score", ""))
        rdv_verdict = (rdv.get("verdict", "") or "").strip().upper()
        hard_reject = truthy(rdv.get("hard_reject", "")) or rdv_verdict.startswith("HARD REJECT")

        price = (row[idx_price] if idx_price is not None and idx_price < len(row) else "").strip()
        stops = (row[idx_stops] if idx_stops is not None and idx_stops < len(row) else "").strip()

        score = rdv_score if rdv_score is not None else fallback_score(price, stops)

        # window
        slot = infer_slot_from_ingest(ing)  # AM/PM
        publish_window = slot

        # status decision
        if hard_reject:
            new_status = "HARD_REJECT"
            wrote_reject += 1
        else:
            if score >= BOTH_SCORE:
                new_status = "PUBLISH_BOTH"
                publish_window = "BOTH"
                wrote_publish += 1
            elif score >= 65:
                new_status = "PUBLISH_" + slot
                wrote_publish += 1
            else:
                new_status = "SCORED"
                wrote_scored += 1

        # write-back (only if columns exist)
        def set_cell(idx: Optional[int], value: Any) -> None:
            if idx is None:
                return
            cells.append(gspread.cell.Cell(row=r_i, col=idx + 1, value=value))

        set_cell(idx_status, new_status)
        set_cell(idx_publish_window, publish_window)
        set_cell(idx_score, f"{score:.1f}")
        if idx_scored_ts is not None:
            # numeric serial for formula math
            set_cell(idx_scored_ts, f"{serial_now:.8f}")

    log(
        f"Eligible NEW candidates: {eligible} | "
        f"PUBLISH={wrote_publish} SCORED={wrote_scored} HARD_REJECT={wrote_reject}"
    )

    if not cells:
        log("✅ No status changes needed (idempotent).")
        return 0

    # Batch write
    ws_raw.update_cells(cells, value_input_option="USER_ENTERED")
    log(f"✅ Wrote {len(cells)} cell updates to RAW_DEALS.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
