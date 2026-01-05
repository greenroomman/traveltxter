#!/usr/bin/env python3
"""
TravelTxter V4.5x — ai_scorer.py (SCORING + WINNER SELECTION, SELF-HEALING HEADERS)

Fixes:
- If scoring columns are missing in RAW_DEALS, this script APPENDS them to the header row automatically:
    deal_score, dest_variety_score, theme_variety_score, scored_timestamp
- Header-mapped only. No column numbers.
- No creative captions.

Purpose:
- Score NEW -> promote to SCORED
- Promote exactly ONE winner SCORED -> READY_TO_POST

Env required:
- SPREADSHEET_ID
- GCP_SA_JSON_ONE_LINE (or GCP_SA_JSON)
- RAW_DEALS_TAB (default RAW_DEALS)

Env optional:
- MAX_ROWS_PER_RUN (default 25)
- WINNERS_PER_RUN (default 1)
- VARIETY_LOOKBACK_HOURS (default 120)
- DEST_REPEAT_PENALTY (default 80)
"""

from __future__ import annotations

import os
import json
import math
import datetime as dt
from typing import Dict, List, Any, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# Logging
# ============================================================

def utcnow() -> dt.datetime:
    return dt.datetime.utcnow()

def ts() -> str:
    return utcnow().replace(microsecond=0).isoformat() + "Z"

def log(msg: str) -> None:
    print(f"{ts()} | {msg}", flush=True)


# ============================================================
# Env
# ============================================================

def env_str(k: str, default: str = "") -> str:
    return os.environ.get(k, default).strip()

def env_int(k: str, default: int) -> int:
    try:
        return int(env_str(k, str(default)))
    except Exception:
        return default


# ============================================================
# Sheets
# ============================================================

def get_client() -> gspread.Client:
    sa = env_str("GCP_SA_JSON_ONE_LINE") or env_str("GCP_SA_JSON")
    if not sa:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE (recommended) or GCP_SA_JSON")
    try:
        info = json.loads(sa)
    except json.JSONDecodeError:
        info = json.loads(sa.replace("\\n", "\n"))

    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)

def col_letter(n1: int) -> str:
    s = ""
    n = n1
    while n:
        n, rr = divmod(n - 1, 26)
        s = chr(65 + rr) + s
    return s

def a1(rownum: int, col0: int) -> str:
    return f"{col_letter(col0 + 1)}{rownum}"


def ensure_columns(ws: gspread.Worksheet, headers: List[str], required: List[str]) -> List[str]:
    """
    Appends any missing required columns to the header row and writes back to the sheet.
    Returns the updated headers list.
    """
    missing = [c for c in required if c not in headers]
    if not missing:
        return headers

    new_headers = headers + missing
    # gspread v6 safe: ws.update(values, range_name)
    ws.update([new_headers], "A1")
    log(f"🛠️  Added missing columns to header: {missing}")
    return new_headers


# ============================================================
# Scoring helpers (deterministic + explainable)
# ============================================================

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def parse_float(s: Any) -> Optional[float]:
    try:
        if s is None:
            return None
        return float(str(s).replace("£", "").strip())
    except Exception:
        return None

def parse_int(s: Any) -> Optional[int]:
    try:
        if s is None:
            return None
        return int(float(str(s).strip()))
    except Exception:
        return None

def parse_iso_date(s: str) -> Optional[dt.date]:
    s = (s or "").strip()
    try:
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            return dt.date.fromisoformat(s[:10])
        return None
    except Exception:
        return None

def days_until(date_str: str) -> Optional[int]:
    d = parse_iso_date(date_str)
    if not d:
        return None
    return (d - utcnow().date()).days


def score_price(price_gbp: float) -> float:
    if price_gbp <= 40:
        return 100.0
    if price_gbp <= 120:
        return 100.0 - ((price_gbp - 40.0) * (60.0 / 80.0))
    if price_gbp <= 250:
        return 40.0 - ((price_gbp - 120.0) * (30.0 / 130.0))
    return 10.0

def score_timing(days_out: int) -> float:
    if days_out < 0:
        return 0.0
    if 20 <= days_out <= 60:
        return 100.0
    if days_out < 20:
        return clamp(40.0 + (days_out * 3.0), 40.0, 95.0)
    return clamp(100.0 - ((days_out - 60) * (60.0 / 120.0)), 40.0, 100.0)

def score_stops(stops: int) -> float:
    if stops <= 0:
        return 100.0
    if stops == 1:
        return 70.0
    if stops == 2:
        return 40.0
    return 20.0

def compute_deal_score(price_gbp: float, days_out: int, stops: int) -> float:
    p = score_price(price_gbp)
    t = score_timing(days_out)
    s = score_stops(stops)
    return (0.55 * p) + (0.30 * t) + (0.15 * s)


# ============================================================
# Variety scoring
# ============================================================

def parse_iso_ts(s: str) -> Optional[dt.datetime]:
    s = (s or "").strip()
    try:
        if s.endswith("Z"):
            return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None

def recent_counts(rows: List[Dict[str, str]], lookback_hours: int) -> Tuple[Dict[str, int], Dict[str, int]]:
    cutoff = utcnow() - dt.timedelta(hours=lookback_hours)
    dest_counts: Dict[str, int] = {}
    theme_counts: Dict[str, int] = {}

    def bump(d: Dict[str, int], k: str) -> None:
        if not k:
            return
        d[k] = d.get(k, 0) + 1

    for r in rows:
        dest_key = (r.get("destination_city") or r.get("destination_iata") or "").strip().upper()
        theme_key = (r.get("deal_theme") or "").strip().upper()

        ts_s = (
            r.get("posted_instagram_at")
            or r.get("rendered_timestamp")
            or r.get("scored_timestamp")
            or r.get("created_at")
            or r.get("scanned_at")
            or ""
        )
        t = parse_iso_ts(ts_s)
        if t is None:
            bump(dest_counts, dest_key)
            bump(theme_counts, theme_key)
            continue
        if t >= cutoff:
            bump(dest_counts, dest_key)
            bump(theme_counts, theme_key)

    return dest_counts, theme_counts

def variety_score(count: int) -> float:
    if count <= 0:
        return 100.0
    if count == 1:
        return 70.0
    if count == 2:
        return 45.0
    return 25.0


# ============================================================
# Main
# ============================================================

def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")

    max_rows = env_int("MAX_ROWS_PER_RUN", 25)
    winners_per_run = env_int("WINNERS_PER_RUN", 1)
    lookback_hours = env_int("VARIETY_LOOKBACK_HOURS", 120)
    dest_repeat_penalty = env_int("DEST_REPEAT_PENALTY", 80)

    gc = get_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(tab)

    values = ws.get_all_values()
    if not values or len(values) < 2:
        log("Sheet empty. Nothing to score.")
        return 0

    headers = [h.strip() for h in values[0]]
    # Self-heal scoring columns if missing
    scoring_cols = ["deal_score", "dest_variety_score", "theme_variety_score", "scored_timestamp"]
    headers = ensure_columns(ws, headers, scoring_cols)

    # Re-read after header update to ensure column alignment
    values = ws.get_all_values()
    headers = [h.strip() for h in values[0]]
    rows = values[1:]
    h = {name: i for i, name in enumerate(headers)}

    required_inputs = ["status", "price_gbp", "outbound_date", "stops", "destination_city", "destination_iata", "deal_theme"]
    for c in required_inputs:
        if c not in h:
            raise RuntimeError(f"Missing required column in RAW_DEALS: {c}")

    # Build dict rows for variety lookback
    all_row_dicts: List[Dict[str, str]] = []
    for r in rows:
        d: Dict[str, str] = {}
        for name, idx in h.items():
            d[name] = (r[idx] if idx < len(r) else "")
        all_row_dicts.append(d)

    dest_counts, theme_counts = recent_counts(all_row_dicts, lookback_hours)

    # Find NEW rows
    new_rownums: List[int] = []
    for idx, r in enumerate(rows, start=2):
        status = (r[h["status"]] if h["status"] < len(r) else "").strip().upper()
        if status == "NEW":
            new_rownums.append(idx)
            if len(new_rownums) >= max_rows:
                break

    if not new_rownums:
        log("No NEW rows found. Nothing to score.")
        return 0

    log(f"Scoring {len(new_rownums)} NEW row(s)")

    batch_updates: List[Dict[str, Any]] = []
    scored_candidates: List[Tuple[int, float]] = []

    for rownum in new_rownums:
        r = rows[rownum - 2]

        price = parse_float(r[h["price_gbp"]] if h["price_gbp"] < len(r) else "")
        if price is None:
            continue

        stops = parse_int(r[h["stops"]] if h["stops"] < len(r) else "") or 0
        days_out = days_until(r[h["outbound_date"]] if h["outbound_date"] < len(r) else "") or 0

        base = compute_deal_score(price, days_out, stops)

        dest_key = (r[h["destination_city"]] if h["destination_city"] < len(r) else "").strip().upper()
        if not dest_key:
            dest_key = (r[h["destination_iata"]] if h["destination_iata"] < len(r) else "").strip().upper()
        theme_key = (r[h["deal_theme"]] if h["deal_theme"] < len(r) else "").strip().upper()

        dv = variety_score(dest_counts.get(dest_key, 0))
        tv = variety_score(theme_counts.get(theme_key, 0))

        final = clamp(base * 0.85 + dv * 0.10 + tv * 0.05, 0.0, 100.0)

        batch_updates.append({"range": a1(rownum, h["deal_score"]), "values": [[f"{final:.1f}"]]})
        batch_updates.append({"range": a1(rownum, h["dest_variety_score"]), "values": [[f"{dv:.1f}"]]})
        batch_updates.append({"range": a1(rownum, h["theme_variety_score"]), "values": [[f"{tv:.1f}"]]})
        batch_updates.append({"range": a1(rownum, h["scored_timestamp"]), "values": [[ts()]]})
        batch_updates.append({"range": a1(rownum, h["status"]), "values": [["SCORED"]]})

        scored_candidates.append((rownum, final))

    if batch_updates:
        ws.batch_update(batch_updates)

    log(f"✅ Promoted {len(scored_candidates)} row(s) NEW -> SCORED")

    if not scored_candidates:
        log("No scorable NEW rows found.")
        return 0

    def selection_score(rownum: int, score: float) -> float:
        r = rows[rownum - 2]
        dest_key = (r[h["destination_city"]] if h["destination_city"] < len(r) else "").strip().upper()
        if not dest_key:
            dest_key = (r[h["destination_iata"]] if h["destination_iata"] < len(r) else "").strip().upper()
        repeats = dest_counts.get(dest_key, 0)
        penalty = dest_repeat_penalty if repeats >= 1 else 0
        return score - penalty

    ranked = sorted(scored_candidates, key=lambda x: selection_score(x[0], x[1]), reverse=True)
    winners = ranked[:max(1, winners_per_run)]

    win_updates: List[Dict[str, Any]] = []
    for (rownum, _) in winners:
        win_updates.append({"range": a1(rownum, h["status"]), "values": [["READY_TO_POST"]]})

    ws.batch_update(win_updates)
    log(f"🏁 Winner(s) promoted SCORED -> READY_TO_POST: {[w[0] for w in winners]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
