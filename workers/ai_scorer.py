# workers/ai_scorer.py
#!/usr/bin/env python3
"""
TravelTxter — AI Scorer (Deterministic) with ZONE × THEME Benchmarks
✅ FIXED: Google Sheets writes are BATCHED (no per-cell loop) to avoid 429 quota errors.

Reads:
- RAW_DEALS where status == NEW

Writes (header-mapped):
- deal_score
- dest_variety_score
- theme_variety_score
- scored_timestamp
- why_good
- ai_notes (optional if column exists)
- zone (optional if column exists)
- price_band (optional if column exists)
- worthiness_score (optional if column exists; sourced from RAW_DEALS_VIEW)
- worthiness_verdict (optional if column exists; sourced from RAW_DEALS_VIEW)
- status (promotes winners to READY_TO_POST, others to SCORED)

Handshake (LOCKED):
- If RAW_DEALS_VIEW exists, scorer will READ its intelligence (dynamic_theme + worthiness)
  and write worthiness_* back into RAW_DEALS for auditability.
- Winner selection prefers worthiness_score if available; otherwise uses internal deal_score.
"""

from __future__ import annotations

import os
import sys
import json
import time
import datetime as dt
from typing import Dict, Any, List, Tuple, Optional, Set

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError


# =======================
# RAW_DEALS_VIEW helpers (read-only intelligence layer)
# =======================

def try_load_raw_deals_view_map(sh: gspread.Spreadsheet) -> Dict[str, Dict[str, Any]]:
    """
    Builds a deal_id -> intelligence dict from RAW_DEALS_VIEW.
    This is READ-ONLY. We never write to the view.

    Expected columns (from exported sheet):
      - deal_id
      - dynamic_theme
      - price_value_score
      - timing_score
      - worthiness_score
      - worthiness_verdict

    If the tab is missing or empty, returns {} and scorer falls back to its internal scoring only.
    """
    try:
        ws = sh.worksheet("RAW_DEALS_VIEW")
    except Exception:
        return {}

    values = ws.get_all_values()
    if len(values) < 2:
        return {}

    headers = [h.strip() for h in values[0]]
    idx = {h: i for i, h in enumerate(headers)}

    required = ["deal_id", "dynamic_theme", "price_value_score", "timing_score", "worthiness_score", "worthiness_verdict"]
    if not all(k in idx for k in required):
        return {}

    m: Dict[str, Dict[str, Any]] = {}
    for r in values[1:]:
        deal_id = (r[idx["deal_id"]] if idx["deal_id"] < len(r) else "").strip()
        if not deal_id:
            continue

        def _get(name: str) -> str:
            return (r[idx[name]] if idx[name] < len(r) else "").strip()

        m[deal_id] = {
            "dynamic_theme": _get("dynamic_theme"),
            "price_value_score": _get("price_value_score"),
            "timing_score": _get("timing_score"),
            "worthiness_score": _get("worthiness_score"),
            "worthiness_verdict": _get("worthiness_verdict"),
        }
    return m


# ============================================================
# Logging
# ============================================================

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


# =======================
# Env helpers
# =======================

def env_int(key: str, default: int) -> int:
    v = os.getenv(key)
    if v is None or str(v).strip() == "":
        return int(default)
    return int(v)


def env_float(key: str, default: float) -> float:
    v = os.getenv(key)
    if v is None or str(v).strip() == "":
        return float(default)
    return float(v)


def env_str(key: str, default: str = "") -> str:
    v = os.getenv(key)
    if v is None or str(v).strip() == "":
        return default
    return str(v)


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
        return int(float(str(x).strip()))
    except Exception:
        return None


def low(s: str) -> str:
    return (s or "").strip().lower().replace(" ", "_")


def up(s: str) -> str:
    return (s or "").strip().upper()


def now_utc_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


# ============================================================
# Google Sheets auth
# ============================================================

def parse_sa_json(raw: str) -> Dict[str, Any]:
    raw = (raw or "").strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return json.loads(raw.replace("\\n", "\n"))


def gs_client() -> gspread.Client:
    raw = env_str("GCP_SA_JSON_ONE_LINE") or env_str("GCP_SA_JSON")
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
    info = parse_sa_json(raw)
    creds = Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


# ============================================================
# Benchmarks + scoring
# ============================================================

def load_benchmarks(sh: gspread.Spreadsheet) -> Dict[Tuple[str, str], Dict[str, float]]:
    ws = sh.worksheet("ZONE_THEME_BENCHMARKS")
    values = ws.get_all_values()
    if len(values) < 2:
        return {}

    headers = [h.strip() for h in values[0]]
    h = {k: i for i, k in enumerate(headers)}

    required = ["zone", "theme", "low_price", "normal_price", "high_price"]
    for k in required:
        if k not in h:
            raise RuntimeError(f"ZONE_THEME_BENCHMARKS missing column: {k}")

    out: Dict[Tuple[str, str], Dict[str, float]] = {}
    for r in values[1:]:
        zone = low(r[h["zone"]] if h["zone"] < len(r) else "")
        theme = low(r[h["theme"]] if h["theme"] < len(r) else "")
        if not zone or not theme:
            continue
        low_p = safe_float(r[h["low_price"]] if h["low_price"] < len(r) else None)
        norm_p = safe_float(r[h["normal_price"]] if h["normal_price"] < len(r) else None)
        high_p = safe_float(r[h["high_price"]] if h["high_price"] < len(r) else None)
        if low_p is None or norm_p is None or high_p is None:
            continue
        out[(zone, theme)] = {"low_price": low_p, "normal_price": norm_p, "high_price": high_p}
    return out


def infer_zone(dest_iata: str, dest_country: str) -> str:
    # Minimal deterministic heuristic (locked)
    c = low(dest_country)
    if c in ("united kingdom", "uk", "england", "scotland", "wales", "northern ireland"):
        return "uk"
    if c in ("france", "spain", "portugal", "italy", "greece", "germany", "netherlands", "belgium", "austria", "switzerland", "poland", "czechia", "czech republic", "hungary", "ireland", "norway", "sweden", "denmark", "finland"):
        return "europe"
    if c in ("united states", "usa", "canada", "mexico"):
        return "americas"
    if c in ("thailand", "japan", "china", "vietnam", "indonesia", "malaysia", "singapore", "philippines", "india", "nepal"):
        return "asia"
    if c in ("australia", "new zealand", "fiji"):
        return "australasia"
    if c in ("south africa", "morocco", "egypt", "kenya", "tanzania"):
        return "africa"
    return "world"


def compute_price_band_score(price: float, bench: Optional[Dict[str, float]]) -> Tuple[float, str]:
    if not bench:
        return (0.0, "UNKNOWN")

    low_p = bench["low_price"]
    norm_p = bench["normal_price"]
    high_p = bench["high_price"]

    if price <= low_p:
        return (30.0, "ELITE")
    if price <= norm_p:
        return (15.0, "GOOD")
    if price <= high_p:
        return (5.0, "OK")
    return (-25.0, "BAD")


def compute_base_score(price: float, stops: int, days_ahead: int) -> float:
    # Simple deterministic heuristics (locked)
    s = 0.0
    # cheaper is better
    if price <= 100:
        s += 20
    elif price <= 200:
        s += 10
    elif price <= 350:
        s += 5

    # fewer stops better
    if stops == 0:
        s += 10
    elif stops == 1:
        s += 3
    else:
        s -= 10

    # timing preference (rough)
    if 20 <= days_ahead <= 70:
        s += 8
    elif 10 <= days_ahead <= 120:
        s += 4

    return s


def why_good_text(band: str, zone: str, theme: str, price: float, bench: Optional[Dict[str, float]]) -> str:
    if bench and band in ("ELITE", "GOOD"):
        return f"{band} vs {zone}/{theme} benchmark (≈£{bench['normal_price']:.0f}); price £{price:.0f}"
    if band == "OK":
        return f"Decent vs {zone}/{theme} benchmark; price £{price:.0f}"
    if band == "BAD":
        return f"Over benchmark for {zone}/{theme}; price £{price:.0f}"
    return f"Scored without benchmark; price £{price:.0f}"


# ============================================================
# Efficient sheet writing
# ============================================================

def update_col_blocks(ws: gspread.Worksheet, col0: int, updates: Dict[int, Any]) -> int:
    """
    Updates a single column using contiguous blocks.
    col0 is 0-based index into the sheet.
    updates maps sheet_row (1-based) -> value.
    Returns number of ws.update() calls.
    """
    if not updates:
        return 0

    items = sorted(updates.items(), key=lambda x: x[0])
    calls = 0

    start = items[0][0]
    prev = start
    block_vals = [items[0][1]]

    for row_idx, val in items[1:]:
        if row_idx == prev + 1:
            block_vals.append(val)
        else:
            a1 = gspread.utils.rowcol_to_a1(start, col0 + 1)
            b1 = gspread.utils.rowcol_to_a1(prev, col0 + 1)
            ws.update([[v] for v in block_vals], f"{a1}:{b1}")
            calls += 1
            start = row_idx
            block_vals = [val]
        prev = row_idx

    a1 = gspread.utils.rowcol_to_a1(start, col0 + 1)
    b1 = gspread.utils.rowcol_to_a1(prev, col0 + 1)
    ws.update([[v] for v in block_vals], f"{a1}:{b1}")
    calls += 1
    return calls


# ============================================================
# Main
# ============================================================

def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID") or env_str("SHEET_ID")
    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID/SHEET_ID")
    raw_tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")

    MAX_ROWS_PER_RUN = env_int("MAX_ROWS_PER_RUN", 50)
    WINNERS_PER_RUN = env_int("WINNERS_PER_RUN", 1)
    VARIETY_LOOKBACK_HOURS = env_int("VARIETY_LOOKBACK_HOURS", 120)
    DEST_REPEAT_PENALTY = env_int("DEST_REPEAT_PENALTY", 80)
    THEME_REPEAT_PENALTY = env_int("THEME_REPEAT_PENALTY", 30)
    HARD_BLOCK_BAD_DEALS = (os.getenv("HARD_BLOCK_BAD_DEALS", "true").strip().lower() == "true")

    log(f"MAX_ROWS_PER_RUN={MAX_ROWS_PER_RUN} WINNERS_PER_RUN={WINNERS_PER_RUN}")
    log(f"VARIETY_LOOKBACK_HOURS={VARIETY_LOOKBACK_HOURS} DEST_REPEAT_PENALTY={DEST_REPEAT_PENALTY} THEME_REPEAT_PENALTY={THEME_REPEAT_PENALTY}")
    log(f"HARD_BLOCK_BAD_DEALS={HARD_BLOCK_BAD_DEALS}")

    gc = gs_client()
    sh = gc.open_by_key(spreadsheet_id)

    view_map = try_load_raw_deals_view_map(sh)
    if view_map:
        log(f"Loaded RAW_DEALS_VIEW intelligence rows: {len(view_map)}")
    else:
        log("RAW_DEALS_VIEW intelligence not available; using internal scoring only.")

    ws = sh.worksheet(raw_tab)
    values = ws.get_all_values()
    if len(values) < 2:
        log("No rows.")
        return 0

    headers = [h.strip() for h in values[0]]
    h = {k: i for i, k in enumerate(headers)}

    required_cols = [
        "status", "deal_id", "price_gbp",
        "destination_iata", "destination_country",
        "outbound_date", "return_date",
        "stops", "deal_theme",
        "scored_timestamp", "deal_score",
        "dest_variety_score", "theme_variety_score",
        "why_good"
    ]
    missing = [c for c in required_cols if c not in h]
    if missing:
        raise RuntimeError(f"RAW_DEALS missing required columns: {missing}")

    has_ai_notes = ("ai_notes" in h)
    has_zone = ("zone" in h)
    has_price_band = ("price_band" in h)
    has_worthiness_score = ("worthiness_score" in h)
    has_worthiness_verdict = ("worthiness_verdict" in h)

    # Collect NEW rows
    new_rows: List[Tuple[int, List[str]]] = []
    for idx, row in enumerate(values[1:], start=2):
        if (row[h["status"]] if h["status"] < len(row) else "").strip() == "NEW":
            new_rows.append((idx, row))
            if len(new_rows) >= MAX_ROWS_PER_RUN:
                break

    log(f"Found NEW rows: {len(new_rows)}")
    if not new_rows:
        return 0

    # Benchmarks
    benchmarks = load_benchmarks(sh)
    log(f"Loaded ZONE_THEME_BENCHMARKS rows: {len(benchmarks)}")

    # Variety lookback sets
    lookback_cutoff = dt.datetime.utcnow() - dt.timedelta(hours=int(VARIETY_LOOKBACK_HOURS))
    recent_dests: Set[str] = set()
    recent_themes: Set[str] = set()

    ts_idx = h["scored_timestamp"]
    dest_idx = h["destination_iata"]
    theme_idx = h["deal_theme"]

    for row in values[1:]:
        ts_val = row[ts_idx] if ts_idx < len(row) else ""
        if not ts_val:
            continue
        try:
            t = dt.datetime.fromisoformat(ts_val.replace("Z", ""))
        except Exception:
            continue
        if t < lookback_cutoff:
            continue

        dv = row[dest_idx] if dest_idx < len(row) else ""
        tv = row[theme_idx] if theme_idx < len(row) else ""
        if dv:
            recent_dests.add(up(dv))
        if tv:
            recent_themes.add(low(tv))

    # Prepare per-row computed outputs
    now_iso = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    col_updates: Dict[str, Dict[int, Any]] = {
        "deal_score": {},
        "dest_variety_score": {},
        "theme_variety_score": {},
        "scored_timestamp": {},
        "why_good": {},
        "status": {},
    }
    if has_ai_notes:
        col_updates["ai_notes"] = {}
    if has_zone:
        col_updates["zone"] = {}
    if has_price_band:
        col_updates["price_band"] = {}
    if has_worthiness_score:
        col_updates["worthiness_score"] = {}
    if has_worthiness_verdict:
        col_updates["worthiness_verdict"] = {}

    scored_rows: List[Tuple[float, int, str]] = []

    for sheet_row, row in new_rows:
        dest_iata = up(row[h["destination_iata"]] if h["destination_iata"] < len(row) else "")
        dest_country = (row[h["destination_country"]] if h["destination_country"] < len(row) else "").strip()
        theme = low(row[h["deal_theme"]] if h["deal_theme"] < len(row) else "")
        deal_id = (row[h["deal_id"]] if h["deal_id"] < len(row) else "").strip()

        price = safe_float(row[h["price_gbp"]] if h["price_gbp"] < len(row) else None) or 0.0
        stops = safe_int(row[h["stops"]] if h["stops"] < len(row) else None) or 0

        # days ahead from outbound_date
        days_ahead = 0
        try:
            od = (row[h["outbound_date"]] if h["outbound_date"] < len(row) else "").strip()
            if od:
                d = dt.datetime.strptime(od, "%Y-%m-%d").date()
                days_ahead = (d - dt.datetime.utcnow().date()).days
        except Exception:
            days_ahead = 0

        zone = infer_zone(dest_iata, dest_country)
        bench = benchmarks.get((zone, theme))
        band_score, band = compute_price_band_score(price, bench)
        base_score = compute_base_score(price, stops, days_ahead)

        dest_variety_score = -float(DEST_REPEAT_PENALTY) if (dest_iata and dest_iata in recent_dests) else 0.0
        theme_variety_score = -float(THEME_REPEAT_PENALTY) if (theme and theme in recent_themes) else 0.0

        final_score = base_score + band_score + dest_variety_score + theme_variety_score
        why = why_good_text(band, zone, theme, price, bench)

        note_bits = [f"zone={zone}", f"theme={theme}", f"band={band}"]
        if bench:
            note_bits.append(f"bench=({bench['low_price']},{bench['normal_price']},{bench['high_price']})")
        if dest_variety_score < 0:
            note_bits.append("dest_repeat_penalty")
        if theme_variety_score < 0:
            note_bits.append("theme_repeat_penalty")
        notes = "; ".join(note_bits)

        # -------------------------------------------------------
        # RAW_DEALS_VIEW handshake (read-only intelligence)
        # -------------------------------------------------------
        view = view_map.get(deal_id) if deal_id else None

        pv_score = None
        worth_score = None
        worth_verdict = None
        if view:
            pv_score = safe_float(view.get("price_value_score"))
            worth_score = safe_float(view.get("worthiness_score"))
            worth_verdict = (view.get("worthiness_verdict") or "").strip()

            # Write back worthiness fields for auditability (these columns exist in RAW_DEALS export)
            if has_worthiness_score and worth_score is not None:
                col_updates["worthiness_score"][sheet_row] = round(float(worth_score), 2)
            if has_worthiness_verdict and worth_verdict:
                col_updates["worthiness_verdict"][sheet_row] = worth_verdict

            # Forensic guardrail: optionally hard-block if price_value_score is extremely low
            if pv_score is not None and pv_score < 5:
                band = "BAD"

        # Winner selection score: prefer worthiness_score if present, else fall back to internal final_score
        winner_score = float(worth_score) if (worth_score is not None) else float(final_score)

        # Stash column outputs (status set later after winner selection)
        col_updates["deal_score"][sheet_row] = round(final_score, 2)
        col_updates["dest_variety_score"][sheet_row] = round(dest_variety_score, 2)
        col_updates["theme_variety_score"][sheet_row] = round(theme_variety_score, 2)
        col_updates["scored_timestamp"][sheet_row] = now_iso
        col_updates["why_good"][sheet_row] = why
        if has_ai_notes:
            col_updates["ai_notes"][sheet_row] = notes
        if has_zone:
            col_updates["zone"][sheet_row] = zone
        if has_price_band:
            col_updates["price_band"][sheet_row] = band

        scored_rows.append((winner_score, sheet_row, band))

    # Winner selection
    eligible = []
    for score, sheet_row, band in scored_rows:
        if HARD_BLOCK_BAD_DEALS and band == "BAD":
            continue
        eligible.append((score, sheet_row, band))

    eligible.sort(key=lambda x: x[0], reverse=True)
    winners = eligible[:WINNERS_PER_RUN] if eligible else []
    winner_rows: Set[int] = {r for _s, r, _b in winners} if winners else set()

    if not winner_rows:
        log("⚠️ No eligible winners (all NEW deals were BAD vs benchmarks). Marking them SCORED; no publishing.")

    for _s, sheet_row, _band in scored_rows:
        col_updates["status"][sheet_row] = "READY_TO_POST" if sheet_row in winner_rows else "SCORED"

    # Batch write per column using contiguous blocks
    total_calls = 0
    write_order = ["deal_score", "dest_variety_score", "theme_variety_score", "scored_timestamp", "why_good"]
    if has_ai_notes:
        write_order.append("ai_notes")
    if has_zone:
        write_order.append("zone")
    if has_price_band:
        write_order.append("price_band")
    if has_worthiness_score:
        write_order.append("worthiness_score")
    if has_worthiness_verdict:
        write_order.append("worthiness_verdict")
    write_order.append("status")  # last

    log(f"Batch writing columns: {', '.join(write_order)}")
    for col_name in write_order:
        col0 = h[col_name]
        calls = update_col_blocks(ws, col0, col_updates[col_name])
        total_calls += calls

    log(f"✅ Batch updates complete. update() calls used: {total_calls}")
    log(f"✅ Winners promoted to READY_TO_POST: {len(winner_rows)} (WINNERS_PER_RUN={WINNERS_PER_RUN})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
