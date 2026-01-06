#!/usr/bin/env python3
"""
TravelTxter V4.5x — ai_scorer.py (LOCKED SCORE-ONLY)

ROLE:
- Reads RAW_DEALS
- Scores NEW rows
- Promotes winners to READY_TO_POST
- Marks others as SCORED

NEVER:
- calls Duffel
- inserts rows
- publishes

STATUS FLOW:
NEW -> SCORED
NEW (winner) -> READY_TO_POST
"""

from __future__ import annotations

import os
import json
import time
import datetime as dt
from typing import Dict, Any, List, Tuple, Optional

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError


# ============================================================
# Logging
# ============================================================

def ts() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def log(msg: str) -> None:
    print(f"{ts()} | {msg}", flush=True)


# ============================================================
# Env helpers
# ============================================================

def env_str(k: str, default: str = "") -> str:
    return os.environ.get(k, default).strip()

def env_int(k: str, default: int) -> int:
    try:
        return int(env_str(k, str(default)))
    except Exception:
        return default

def env_float(k: str, default: float) -> float:
    try:
        return float(env_str(k, str(default)))
    except Exception:
        return default


# ============================================================
# Robust JSON extraction (fixes secret formatting issues)
# ============================================================

def _extract_json_object(raw: str) -> Dict[str, Any]:
    """
    Safely extract the FIRST valid JSON object from a string.
    Handles:
    - extra characters / trailing prompt symbols
    - pasted twice
    - unescaped newlines in private_key
    - escaped \\n sequences
    """
    raw = (raw or "").strip()

    # Fast path
    try:
        return json.loads(raw)
    except Exception:
        pass

    # Replace escaped newlines
    try:
        return json.loads(raw.replace("\\n", "\n"))
    except Exception:
        pass

    # Extract first {...} block
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise RuntimeError("Invalid GCP_SA_JSON_ONE_LINE: no JSON object found")

    candidate = raw[start:end + 1]

    # Try candidate raw
    try:
        return json.loads(candidate)
    except Exception:
        pass

    # Try candidate with newline repair
    try:
        return json.loads(candidate.replace("\\n", "\n"))
    except Exception as e:
        raise RuntimeError("Invalid GCP_SA_JSON_ONE_LINE: JSON parse failed") from e


def get_client() -> gspread.Client:
    sa = env_str("GCP_SA_JSON_ONE_LINE") or env_str("GCP_SA_JSON")
    if not sa:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")

    info = _extract_json_object(sa)

    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)


def open_sheet_with_backoff(gc: gspread.Client, spreadsheet_id: str, attempts: int = 6) -> gspread.Spreadsheet:
    delay = 3.0
    for i in range(1, attempts + 1):
        try:
            return gc.open_by_key(spreadsheet_id)
        except APIError as e:
            msg = str(e)
            if "429" in msg or "Quota exceeded" in msg:
                log(f"⏳ Sheets quota (429). Retry {i}/{attempts} in {int(delay)}s...")
                time.sleep(delay)
                delay = min(delay * 1.7, 30.0)
                continue
            raise
    raise RuntimeError("Sheets quota still exceeded after retries (429).")


# ============================================================
# A1 helpers
# ============================================================

def col_letter(n1: int) -> str:
    s = ""
    n = n1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def a1(rownum: int, col0: int) -> str:
    return f"{col_letter(col0 + 1)}{rownum}"


# ============================================================
# Sheet helpers
# ============================================================

def ensure_columns(ws: gspread.Worksheet, headers: List[str], required: List[str]) -> List[str]:
    missing = [c for c in required if c not in headers]
    if not missing:
        return headers
    ws.update([headers + missing], "A1")
    log(f"🛠️  Added missing columns: {missing}")
    return headers + missing


def safe_get(row: List[str], idx: int) -> str:
    return row[idx].strip() if 0 <= idx < len(row) else ""


def parse_iso(s: str) -> Optional[dt.datetime]:
    if not s:
        return None
    try:
        # handles "2026-01-05T..." and "2026-01-05 ..."
        return dt.datetime.fromisoformat(s.replace("Z", ""))
    except Exception:
        return None


# ============================================================
# Scoring logic (simple, deterministic)
# ============================================================

def compute_base_score(price_gbp: float) -> int:
    # Simple value-first score: lower price -> higher score
    # Clamp to sensible bounds.
    score = int(1000 - (price_gbp * 2.0))
    return max(0, min(score, 1000))


def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")

    WINNERS_PER_RUN = env_int("WINNERS_PER_RUN", 1)
    DEST_REPEAT_PENALTY = env_int("DEST_REPEAT_PENALTY", 80)
    VARIETY_LOOKBACK_HOURS = env_int("VARIETY_LOOKBACK_HOURS", 120)

    min_price = env_float("MVP_MIN_PRICE_GBP", 10.0)
    max_price = env_float("MVP_MAX_PRICE_GBP", 800.0)

    gc = get_client()
    sh = open_sheet_with_backoff(gc, spreadsheet_id)
    ws = sh.worksheet(tab)

    values = ws.get_all_values()
    if not values or len(values) < 2:
        log("No data to score.")
        return 0

    headers = [h.strip() for h in values[0]]

    required_cols = [
        "status",
        "price_gbp",
        "destination_iata",
        "deal_score",
        "dest_variety_score",
        "theme_variety_score",
        "scored_timestamp",
    ]
    headers = ensure_columns(ws, headers, required_cols)

    # Re-read once after header mutation
    values = ws.get_all_values()
    headers = [h.strip() for h in values[0]]
    rows = values[1:]
    h = {name: i for i, name in enumerate(headers)}

    # Optional columns
    idx_posted_ig = h.get("posted_instagram_at", None)
    idx_theme = h.get("deal_theme", None)

    # --------------------------------------------------------
    # Recent destination counts for variety penalty
    # --------------------------------------------------------
    cutoff = dt.datetime.utcnow() - dt.timedelta(hours=VARIETY_LOOKBACK_HOURS)
    recent_dest_counts: Dict[str, int] = {}

    if idx_posted_ig is not None:
        for r in rows:
            posted = parse_iso(safe_get(r, idx_posted_ig))
            if not posted or posted < cutoff:
                continue
            dest = safe_get(r, h["destination_iata"]).upper()
            if dest:
                recent_dest_counts[dest] = recent_dest_counts.get(dest, 0) + 1

    # --------------------------------------------------------
    # Gather NEW candidates
    # --------------------------------------------------------
    candidates: List[Dict[str, Any]] = []

    for rownum, r in enumerate(rows, start=2):
        status = safe_get(r, h["status"]).upper()
        if status != "NEW":
            continue

        try:
            price = float(safe_get(r, h["price_gbp"]))
        except Exception:
            continue

        if price < min_price or price > max_price:
            continue

        dest = safe_get(r, h["destination_iata"]).upper()
        theme = safe_get(r, idx_theme).strip() if idx_theme is not None else ""

        base = compute_base_score(price)

        # Destination variety penalty
        repeat_n = recent_dest_counts.get(dest, 0)
        dest_variety = max(0, 100 - (repeat_n * 25))
        penalty = DEST_REPEAT_PENALTY * repeat_n

        final = base - penalty

        candidates.append({
            "rownum": rownum,
            "price": price,
            "dest": dest,
            "theme": theme,
            "base": base,
            "final": final,
            "dest_variety": dest_variety,
            "theme_variety": 100,  # placeholder for now (deterministic)
        })

    if not candidates:
        log("No eligible NEW deals to score.")
        return 0

    # --------------------------------------------------------
    # Rank + select winners
    # --------------------------------------------------------
    candidates.sort(key=lambda x: x["final"], reverse=True)
    winners = candidates[:max(1, WINNERS_PER_RUN)]

    winner_rows = {w["rownum"] for w in winners}

    # --------------------------------------------------------
    # Batch update
    # --------------------------------------------------------
    batch = []

    for c in candidates:
        r = c["rownum"]

        batch.append({"range": a1(r, h["deal_score"]), "values": [[str(c["final"])]]})
        batch.append({"range": a1(r, h["dest_variety_score"]), "values": [[str(c["dest_variety"])]]})
        batch.append({"range": a1(r, h["theme_variety_score"]), "values": [[str(c["theme_variety"])]]})
        batch.append({"range": a1(r, h["scored_timestamp"]), "values": [[ts()]]})

        new_status = "READY_TO_POST" if r in winner_rows else "SCORED"
        batch.append({"range": a1(r, h["status"]), "values": [[new_status]]})

    ws.batch_update(batch, value_input_option="USER_ENTERED")

    log(f"✅ Scored {len(candidates)} NEW deals, promoted {len(winners)} winner(s) -> READY_TO_POST")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
