#!/usr/bin/env python3
"""
Traveltxter — Minimal AI Scorer (Schema-aligned, deterministic)

Purpose:
- Consume RAW_DEALS rows where status == NEW and ai_score is blank
- Validate canonical columns exist and required fields are present
- Produce:
  ai_score, ai_verdict, ai_grading, ai_caption, scored_timestamp
  is_telegram_eligible, is_instagram_eligible
- Promote status -> READY_TO_PUBLISH

No redesign. No external APIs. Deterministic scoring.
"""

import os
import json
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials


# -------------------------
# Logging / time
# -------------------------

def now_utc_dt() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def now_utc_str() -> str:
    return now_utc_dt().replace(microsecond=0).isoformat().replace("+00:00", "Z")

def log(msg: str) -> None:
    print(f"{now_utc_str()} | {msg}", flush=True)


# -------------------------
# Env helpers
# -------------------------

def env_str(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()

def env_int(name: str, default: int) -> int:
    v = env_str(name, "")
    try:
        return int(v) if v else default
    except Exception:
        return default


# -------------------------
# gspread v6 safe update
# -------------------------

def a1_update(ws: gspread.Worksheet, a1: str, value: Any) -> None:
    ws.update([[value]], a1)


# -------------------------
# Auth
# -------------------------

def get_gspread_client() -> gspread.Client:
    sa_json = env_str("GCP_SA_JSON_ONE_LINE")
    if not sa_json:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE")
    info = json.loads(sa_json)
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)


# -------------------------
# Sheet utils
# -------------------------

def ensure_columns(ws: gspread.Worksheet, required: List[str]) -> Dict[str, int]:
    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("RAW_DEALS_TAB has no header row")
    changed = False
    for c in required:
        if c not in headers:
            headers.append(c)
            changed = True
    if changed:
        ws.update([headers], "A1")
    return {h: i for i, h in enumerate(headers)}

def col_letter(n: int) -> str:
    # 1-indexed -> Excel letters
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def a1_for(row: int, col_index_0: int) -> str:
    # row is 1-indexed, col_index_0 is 0-indexed
    return f"{col_letter(col_index_0 + 1)}{row}"


# -------------------------
# Scoring logic (deterministic)
# -------------------------

def parse_float(x: str) -> Optional[float]:
    try:
        return float(str(x).strip())
    except Exception:
        return None

def parse_int(x: str) -> Optional[int]:
    try:
        return int(float(str(x).strip()))
    except Exception:
        return None

def days_until(date_iso: str) -> Optional[int]:
    try:
        d = dt.date.fromisoformat(date_iso)
        today = now_utc_dt().date()
        return (d - today).days
    except Exception:
        return None

def clamp(n: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, n))

def score_row(price_gbp: float, stops: int, depart_in_days: int) -> Tuple[int, str, str, List[str]]:
    """
    Returns: (ai_score 0-100, verdict, grading, reasons[])
    Very simple:
      - Value: cheaper = better
      - Friction: fewer stops = better
      - Timing: 14-90 days out is sweet spot
    """
    reasons: List[str] = []

    # Value score (0-60)
    # 40 = decent, 80+ = very good, 120+ = weaker (for short-haul style deals)
    value = 60 * (1.0 - clamp((price_gbp - 40.0) / 120.0, 0.0, 1.0))
    reasons.append(f"Price £{price_gbp:.0f}")

    # Stops penalty (0-20)
    # 0 stops full points, 1 stop mild penalty, 2+ bigger penalty
    if stops <= 0:
        friction = 20
        reasons.append("Direct / low friction")
    elif stops == 1:
        friction = 12
        reasons.append("1 stop")
    else:
        friction = 5
        reasons.append(f"{stops} stops")

    # Timing (0-20)
    # Prefer 14-90 days; too close or too far reduces score
    if depart_in_days is None:
        timing = 10
        reasons.append("Timing unknown")
    else:
        if 14 <= depart_in_days <= 90:
            timing = 20
            reasons.append(f"Good timing ({depart_in_days}d out)")
        elif 7 <= depart_in_days < 14:
            timing = 14
            reasons.append(f"Fair timing ({depart_in_days}d out)")
        elif depart_in_days < 7:
            timing = 8
            reasons.append(f"Late notice ({depart_in_days}d out)")
        else:
            timing = 12
            reasons.append(f"Further out ({depart_in_days}d out)")

    total = int(round(clamp(value + friction + timing, 0, 100)))

    if total >= 80:
        verdict = "POST"
        grading = "A"
    elif total >= 65:
        verdict = "MAYBE"
        grading = "B"
    else:
        verdict = "SKIP"
        grading = "C"

    return total, verdict, grading, reasons


def build_caption(origin_city: str, origin_iata: str, dest_city: str, dest_iata: str,
                  out_date: str, ret_date: str, price_gbp: float, airline: str, stops: int,
                  affiliate_url: str) -> str:
    """
    Simple caption. Keeps you moving. You can swap this later for your phrase-bank system.
    """
    o = origin_city or origin_iata
    d = dest_city or dest_iata
    stop_txt = "Direct" if stops <= 0 else f"{stops} stop" + ("s" if stops != 1 else "")
    air_txt = f" with {airline}" if airline else ""
    return (
        f"🔥 £{price_gbp:.0f} return to {d}\n\n"
        f"📍 From {o} ({origin_iata})\n"
        f"📅 {out_date} → {ret_date}\n"
        f"✈️ {stop_txt}{air_txt}\n\n"
        f"Book: {affiliate_url}"
    )


# -------------------------
# Main
# -------------------------

def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    raw_tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")
    max_rows = env_int("SCORER_MAX_ROWS", 6)

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")

    log("============================================================")
    log("🧠 AI Scorer starting (deterministic)")
    log("============================================================")

    gc = get_gspread_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(raw_tab)

    required_cols = [
        # canonical input fields
        "deal_id",
        "origin_city", "origin_iata",
        "destination_city", "destination_iata",
        "outbound_date", "return_date",
        "price_gbp",
        "stops", "airline",
        "affiliate_url",
        "status",
        # canonical outputs
        "ai_score", "ai_verdict", "ai_grading", "ai_caption", "scored_timestamp",
        "is_telegram_eligible", "is_instagram_eligible",
    ]
    hm = ensure_columns(ws, required_cols)

    rows = ws.get_all_values()
    if len(rows) <= 1:
        log("No data rows found.")
        return 0

    headers = rows[0]
    data = rows[1:]

    def get(row: List[str], col: str) -> str:
        idx = hm.get(col)
        if idx is None or idx >= len(row):
            return ""
        return row[idx].strip()

    processed = 0
    updated = 0

    for i, row in enumerate(data, start=2):  # sheet row index
        if updated >= max_rows:
            break

        status = get(row, "status").upper()
        ai_score_existing = get(row, "ai_score")

        if status != "NEW":
            continue
        if ai_score_existing:
            continue

        # Required fields
        origin_iata = get(row, "origin_iata")
        dest_iata = get(row, "destination_iata")
        out_date = get(row, "outbound_date")
        ret_date = get(row, "return_date")
        price_s = get(row, "price_gbp")
        aff = get(row, "affiliate_url")

        price = parse_float(price_s)
        if not (origin_iata and dest_iata and out_date and ret_date and aff and price is not None):
            # Mark as skipped (optional) — for now just ignore quietly
            processed += 1
            continue

        stops = parse_int(get(row, "stops")) or 0
        airline = get(row, "airline")
        origin_city = get(row, "origin_city")
        dest_city = get(row, "destination_city")

        depart_in = days_until(out_date)
        score, verdict, grading, reasons = score_row(price, stops, depart_in if depart_in is not None else 999)

        caption = build_caption(origin_city, origin_iata, dest_city, dest_iata, out_date, ret_date, price, airline, stops, aff)

        # Eligibility rules (simple)
        is_tg = "TRUE" if verdict in ("POST", "MAYBE") else "FALSE"
        is_ig = "TRUE" if verdict == "POST" else "FALSE"

        # Promote status for the rest of pipeline
        next_status = "READY_TO_PUBLISH" if verdict == "POST" else ("READY_TO_POST" if verdict == "MAYBE" else "SKIPPED")

        # Write outputs (cell-by-cell, safe)
        a1_update(ws, a1_for(i, hm["ai_score"]), str(score))
        a1_update(ws, a1_for(i, hm["ai_verdict"]), verdict)
        a1_update(ws, a1_for(i, hm["ai_grading"]), grading)
        a1_update(ws, a1_for(i, hm["ai_caption"]), caption)
        a1_update(ws, a1_for(i, hm["scored_timestamp"]), now_utc_str())
        a1_update(ws, a1_for(i, hm["is_telegram_eligible"]), is_tg)
        a1_update(ws, a1_for(i, hm["is_instagram_eligible"]), is_ig)
        a1_update(ws, a1_for(i, hm["status"]), next_status)

        updated += 1
        processed += 1
        log(f"✅ Scored row {i}: {origin_iata}->{dest_iata} £{price:.0f} score={score} verdict={verdict} -> {next_status} | {'; '.join(reasons)}")

    log(f"Done. Scored {updated} row(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
