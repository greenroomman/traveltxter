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

    out: Dict[str]()
