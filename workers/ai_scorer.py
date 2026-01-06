#!/usr/bin/env python3
"""
TravelTxter V4.5x — AI Scorer (Deterministic + Discovery Back-prop)

ROLE:
- Reads RAW_DEALS where status == NEW
- Scores deterministically
- Promotes exactly WINNERS_PER_RUN to READY_TO_POST
- Marks non-winners as SCORED
- Optionally uses DISCOVERY_WEEKLY_REPORT as a *prior* (small boost), never as a rule engine.

NON-NEGOTIABLES:
- No AI copy
- No publishing
- No rendering
- No edits to CONFIG/THEMES/SIGNALS
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

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


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

def env_float(k: str, default: float) -> float:
    try:
        return float(env_str(k, str(default)))
    except Exception:
        return default


# ============================================================
# Sheets auth / open with backoff
# ============================================================

def _extract_sa(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()
    try:
        return json.loads(raw)
    except Exception:
        return json.loads(raw.replace("\\n", "\n"))

def get_client() -> gspread.Client:
    sa_raw = env_str("GCP_SA_JSON_ONE_LINE") or env_str("GCP_SA_JSON")
    if not sa_raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
    info = _extract_sa(sa_raw)
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
    )
    return gspread.authorize(creds)

def open_sheet(gc: gspread.Client, spreadsheet_id: str, attempts: int = 8) -> gspread.Spreadsheet:
    delay = 3.0
    for i in range(1, attempts + 1):
        try:
            return gc.open_by_key(spreadsheet_id)
        except APIError as e:
            msg = str(e)
            if "429" in msg or "Quota" in msg:
                log(f"⏳ Sheets quota 429. Retry {i}/{attempts} in {int(delay)}s...")
                time.sleep(delay)
                delay = min(delay * 1.6, 45.0)
                continue
            raise
    raise RuntimeError("Sheets quota still exceeded after retries.")


# ============================================================
# A1 / header mapping
# ============================================================

def col_letter(n: int) -> str:
    s = ""
    x = n
    while x:
        x, r = divmod(x - 1, 26)
        s = chr(65 + r) + s
    return s

def a1(row: int, col0: int) -> str:
    return f"{col_letter(col0 + 1)}{row}"

def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        s = str(x).strip().replace("£", "").replace(",", "")
        if not s:
            return None
        return float(s)
    except Exception:
        return None

def safe_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if not s:
            return None
        return int(float(s))
    except Exception:
        return None


# ============================================================
# Discovery back-prop: load weekly priors
# ============================================================

def load_discovery_priors(sh: gspread.Spreadsheet) -> Dict[str, float]:
    """
    Returns per-destination boost based on DISCOVERY_WEEKLY_REPORT.

    Conservative, explainable:
      - HIGH confidence, repeated outside_config -> +6
      - HIGH confidence consistent low price -> +5
      - MEDIUM -> smaller boosts
      - currency signals -> small +2 (means: don't ignore this destination)
    """
    priors: Dict[str, float] = {}

    try:
        ws = sh.worksheet("DISCOVERY_WEEKLY_REPORT")
    except Exception:
        log("ℹ️  DISCOVERY_WEEKLY_REPORT not found. Continuing without priors.")
        return priors

    rows = ws.get_all_values()
    if len(rows) < 2:
        log("ℹ️  DISCOVERY_WEEKLY_REPORT empty. Continuing without priors.")
        return priors

    headers = [h.strip() for h in rows[0]]
    idx = {h: i for i, h in enumerate(headers)}

    def g(r: List[str], name: str) -> str:
        j = idx.get(name)
        return r[j].strip() if j is not None and j < len(r) else ""

    for r in rows[1:]:
        entity_type = g(r, "entity_type")
        entity_value = g(r, "entity_value").upper()
        insight_type = g(r, "insight_type")
        confidence = g(r, "confidence").upper()
        ev = safe_int(g(r, "evidence_count")) or 0

        if entity_type != "destination" or not entity_value:
            continue

        boost = 0.0

        if confidence == "HIGH":
            if insight_type == "REPEATED_DESTINATION_OUTSIDE_CONFIG":
                boost = 6.0
            elif insight_type == "CONSISTENT_LOW_PRICE_DESTINATION":
                boost = 5.0
            elif insight_type == "CURRENCY_FILTER_SIGNAL":
                boost = 2.0
            else:
                boost = 2.5
        elif confidence == "MEDIUM":
            if insight_type == "REPEATED_DESTINATION_OUTSIDE_CONFIG":
                boost = 3.0
            elif insight_type == "CONSISTENT_LOW_PRICE_DESTINATION":
                boost = 2.5
            elif insight_type == "CURRENCY_FILTER_SIGNAL":
                boost = 1.0
            else:
                boost = 1.5
        else:
            # LOW confidence: ignore (keeps scorer stable)
            boost = 0.0

        # tiny scaling with evidence (capped)
        boost += min(2.0, ev / 10.0)

        if boost > 0:
            priors[entity_value] = max(priors.get(entity_value, 0.0), boost)

    log(f"🧠 Discovery priors loaded: {len(priors)} destinations")
    return priors


# ============================================================
# Scoring
# ============================================================

def compute_base_score(price_gbp: Optional[float], stops: Optional[int], days_ahead: Optional[int]) -> float:
    """
    Deterministic base score:
    - Lower price is better (dominant factor)
    - Fewer stops is better
    - Medium-term travel windows slightly favoured
    """
    score = 0.0

    # Price (dominant). If missing, penalise heavily.
    if price_gbp is None:
        score -= 40.0
    else:
        # Map price ranges to points (simple & robust)
        if price_gbp <= 50:
            score += 55
        elif price_gbp <= 80:
            score += 45
        elif price_gbp <= 110:
            score += 35
        elif price_gbp <= 150:
            score += 25
        elif price_gbp <= 200:
            score += 15
        else:
            score += 5

    # Stops
    if stops is None:
        score -= 5
    else:
        if stops == 0:
            score += 12
        elif stops == 1:
            score += 6
        else:
            score -= 4 * (stops - 1)

    # Timing (optional)
    if days_ahead is not None:
        if 20 <= days_ahead <= 70:
            score += 8
        elif days_ahead < 10:
            score -= 6
        elif days_ahead > 120:
            score -= 4

    return score


def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    raw_tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")

    WINNERS_PER_RUN = env_int("WINNERS_PER_RUN", 1)
    VARIETY_LOOKBACK_HOURS = env_int("VARIETY_LOOKBACK_HOURS", 120)
    DEST_REPEAT_PENALTY = env_float("DEST_REPEAT_PENALTY", 80.0)

    # Back-prop knobs (conservative defaults)
    DISCOVERY_PRIOR_MAX = env_float("DISCOVERY_PRIOR_MAX", 8.0)   # cap boost
    DISCOVERY_PRIOR_WEIGHT = env_float("DISCOVERY_PRIOR_WEIGHT", 1.0)  # multiplier

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")

    gc = get_client()
    sh = open_sheet(gc, spreadsheet_id)
    ws = sh.worksheet(raw_tab)

    values = ws.get_all_values()
    if len(values) < 2:
        log("Sheet empty. Nothing to score.")
        return 0

    headers = [h.strip() for h in values[0]]
    h = {name: i for i, name in enumerate(headers)}

    required = ["status", "deal_id", "price_gbp", "origin_iata", "destination_iata", "outbound_date", "return_date", "stops", "deal_theme"]
    missing = [c for c in required if c not in h]
    if missing:
        raise RuntimeError(f"RAW_DEALS missing required columns: {missing}")

    # Columns we write
    out_cols = ["deal_score", "dest_variety_score", "theme_variety_score", "scored_timestamp"]
    for c in out_cols:
        if c not in h:
            # Add column at end
            headers.append(c)
            h[c] = len(headers) - 1
    ws.update([headers], "A1")

    # Load priors once per run
    priors = load_discovery_priors(sh)

    # Identify NEW rows
    new_rows: List[Tuple[int, List[str]]] = []
    for i, row in enumerate(values[1:], start=2):  # sheet row numbers
        status = row[h["status"]] if h["status"] < len(row) else ""
        if status.strip() == "NEW":
            new_rows.append((i, row))

    if not new_rows:
        log("No NEW rows to score.")
        return 0

    log(f"Scoring NEW rows: {len(new_rows)}")

    # Build recent destinations list for variety penalty
    lookback_cutoff = dt.datetime.utcnow() - dt.timedelta(hours=VARIETY_LOOKBACK_HOURS)
    recent_dests = set()
    if "scored_timestamp" in h:
        ts_idx = h["scored_timestamp"]
        dest_idx = h["destination_iata"]
        for row in values[1:]:
            ts_val = row[ts_idx] if ts_idx < len(row) else ""
            dest_val = row[dest_idx] if dest_idx < len(row) else ""
            if not ts_val or not dest_val:
                continue
            try:
                t = dt.datetime.fromisoformat(ts_val.replace("Z", ""))
                if t >= lookback_cutoff:
                    recent_dests.add(dest_val.strip().upper())
            except Exception:
                continue

    scored: List[Tuple[float, int]] = []  # (final_score, sheet_row)

    for sheet_row, row in new_rows:
        dest = (row[h["destination_iata"]] if h["destination_iata"] < len(row) else "").strip().upper()
        theme = (row[h["deal_theme"]] if h["deal_theme"] < len(row) else "").strip()

        price = safe_float(row[h["price_gbp"]] if h["price_gbp"] < len(row) else "")
        stops = safe_int(row[h["stops"]] if h["stops"] < len(row) else "")

        # days ahead
        days_ahead = None
        od = row[h["outbound_date"]] if h["outbound_date"] < len(row) else ""
        if od:
            try:
                d = dt.datetime.fromisoformat(od[:10]).date()
                days_ahead = (d - dt.datetime.utcnow().date()).days
            except Exception:
                days_ahead = None

        base = compute_base_score(price, stops, days_ahead)

        # Variety penalty (simple, strong)
        variety_pen = 0.0
        if dest and dest in recent_dests:
            variety_pen = -abs(DEST_REPEAT_PENALTY)

        # Theme variety (lightweight placeholders; keep deterministic)
        theme_variety = 0.0
        if theme:
            theme_variety = 2.0  # minimal nudge; avoid over-engineering

        # Discovery prior (small boost, capped)
        prior = priors.get(dest, 0.0)
        prior = min(DISCOVERY_PRIOR_MAX, prior) * DISCOVERY_PRIOR_WEIGHT

        final_score = base + theme_variety + prior + variety_pen

        # Write per-row scoring fields
        updates = []
        updates.append((a1(sheet_row, h["deal_score"]), str(round(final_score, 2))))
        updates.append((a1(sheet_row, h["dest_variety_score"]), str(round(variety_pen, 2))))
        updates.append((a1(sheet_row, h["theme_variety_score"]), str(round(theme_variety, 2))))
        updates.append((a1(sheet_row, h["scored_timestamp"]), dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"))

        # Batch update row (fast enough, and avoids per-cell API spam)
        for cell, val in updates:
            ws.update([[val]], cell)

        scored.append((final_score, sheet_row))

    # Select winners
    scored.sort(key=lambda x: x[0], reverse=True)
    winners = scored[:max(1, WINNERS_PER_RUN)]
    winner_rows = {r for _s, r in winners}

    for _s, sheet_row in scored:
        status_cell = a1(sheet_row, h["status"])
        if sheet_row in winner_rows:
            ws.update([["READY_TO_POST"]], status_cell)
        else:
            ws.update([["SCORED"]], status_cell)

    log(f"✅ Winners promoted to READY_TO_POST: {len(winner_rows)} (WINNERS_PER_RUN={WINNERS_PER_RUN})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
