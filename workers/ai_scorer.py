#!/usr/bin/env python3
"""
TravelTxter — AI Scorer (Deterministic) with ZONE × THEME Benchmarks

Reads:
- RAW_DEALS where status == NEW

Writes:
- deal_score
- dest_variety_score
- theme_variety_score
- scored_timestamp
- why_good
- ai_notes (optional)
- status -> READY_TO_POST for winners, SCORED for non-winners

Brain:
- ZONE_THEME_BENCHMARKS tab:
  zone,theme,low_price,normal_price,high_price,notes

Principles:
- Deterministic (no AI text generation)
- Explainable ("why_good" is a short deterministic reason)
- Safe (won't publish obvious non-deals)
"""

import os
import json
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple, Set

import gspread
from google.oauth2.service_account import Credentials


# =======================
# Logging
# =======================

def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


# =======================
# Env helpers
# =======================

def env_str(key: str, default: Optional[str] = None) -> str:
    v = os.getenv(key, default)
    if v is None or str(v).strip() == "":
        raise RuntimeError(f"Missing {key}")
    return str(v)

def env_int(key: str, default: int) -> int:
    v = os.getenv(key)
    if v is None or str(v).strip() == "":
        return int(default)
    return int(str(v).strip())

def env_float(key: str, default: float) -> float:
    v = os.getenv(key)
    if v is None or str(v).strip() == "":
        return float(default)
    return float(str(v).strip())


# =======================
# A1 helpers
# =======================

def col_letter(col1: int) -> str:
    s = ""
    x = col1
    while x:
        x, r = divmod(x - 1, 26)
        s = chr(65 + r) + s
    return s

def a1(row: int, col0: int) -> str:
    return f"{col_letter(col0 + 1)}{row}"


# =======================
# Parsing helpers
# =======================

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

def up(x: Any) -> str:
    return str(x or "").strip().upper()

def low(x: Any) -> str:
    return str(x or "").strip().lower()


# =======================
# Sheets init
# =======================

def open_sheet() -> gspread.Spreadsheet:
    spreadsheet_id = os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID")
    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID (or SHEET_ID)")

    gcp = os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON")
    if not gcp:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE (or GCP_SA_JSON)")

    sa = json.loads(gcp)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(sa, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(spreadsheet_id)


# =======================
# Load ZONE_THEME_BENCHMARKS
# =======================

def load_zone_theme_benchmarks(sh: gspread.Spreadsheet) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """
    Returns dict keyed by (zone, theme):
      { low_price, normal_price, high_price, notes }
    """
    tab = "ZONE_THEME_BENCHMARKS"
    try:
        ws = sh.worksheet(tab)
    except Exception:
        log("⚠️ ZONE_THEME_BENCHMARKS not found. Scoring will run WITHOUT zone/theme price intelligence.")
        return {}

    values = ws.get_all_values()
    if len(values) < 2:
        log("⚠️ ZONE_THEME_BENCHMARKS empty. Scoring will run WITHOUT zone/theme price intelligence.")
        return {}

    headers = [h.strip() for h in values[0]]
    idx = {h: i for i, h in enumerate(headers)}
    required = ["zone", "theme", "low_price", "normal_price", "high_price"]
    missing = [c for c in required if c not in idx]
    if missing:
        log(f"⚠️ ZONE_THEME_BENCHMARKS missing columns {missing}. Scoring will run WITHOUT zone/theme price intelligence.")
        return {}

    bench: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for r in values[1:]:
        z = low(r[idx["zone"]] if idx["zone"] < len(r) else "")
        t = low(r[idx["theme"]] if idx["theme"] < len(r) else "")
        if not z or not t:
            continue
        lp = safe_float(r[idx["low_price"]] if idx["low_price"] < len(r) else None)
        np = safe_float(r[idx["normal_price"]] if idx["normal_price"] < len(r) else None)
        hp = safe_float(r[idx["high_price"]] if idx["high_price"] < len(r) else None)
        if lp is None or np is None or hp is None:
            continue
        bench[(z, t)] = {
            "low_price": lp,
            "normal_price": np,
            "high_price": hp,
            "notes": (r[idx["notes"]] if "notes" in idx and idx["notes"] < len(r) else "").strip()
        }

    log(f"✓ Loaded {len(bench)} ZONE×THEME benchmark rows")
    return bench


# =======================
# Zone inference (v1 deterministic)
# =======================

CANARIES_MADEIRA_IATA = {
    "TFS", "TFN", "ACE", "FUE", "LPA", "SPC", "FNC", "PXO", "PDL"
}
ICELAND_ARCTIC_IATA = {"KEF"}

NORTH_AMERICA_WEST_IATA = {"LAX", "SFO", "SAN", "SEA", "PDX", "LAS", "PHX", "DEN", "SLC", "YVR", "YYC"}
NORTH_AMERICA_EAST_IATA = {"JFK", "EWR", "BOS", "IAD", "DCA", "MIA", "ORD", "ATL", "DFW", "IAH", "YYZ", "YUL", "YOW"}

EUROPE_COUNTRIES = {
    "UNITED KINGDOM","IRELAND","FRANCE","SPAIN","PORTUGAL","ITALY","GERMANY","NETHERLANDS","BELGIUM","LUXEMBOURG",
    "SWITZERLAND","AUSTRIA","CZECHIA","CZECH REPUBLIC","POLAND","HUNGARY","SLOVAKIA","SLOVENIA","CROATIA","BOSNIA",
    "BOSNIA AND HERZEGOVINA","SERBIA","MONTENEGRO","ALBANIA","NORTH MACEDONIA","GREECE","BULGARIA","ROMANIA",
    "ESTONIA","LATVIA","LITHUANIA","FINLAND","SWEDEN","NORWAY","DENMARK","ICELAND","MALTA","CYPRUS","TURKEY"
}

NORTH_AFRICA_COUNTRIES = {"MOROCCO","TUNISIA","ALGERIA","EGYPT"}
MIDDLE_EAST_COUNTRIES = {"UNITED ARAB EMIRATES","UAE","QATAR","BAHRAIN","SAUDI ARABIA","OMAN","JORDAN","KUWAIT","LEBANON","ISRAEL","IRAQ"}

AFRICA_SOUTH_COUNTRIES = {"SOUTH AFRICA","NAMIBIA","BOTSWANA","ZIMBABWE","MOZAMBIQUE","ZAMBIA"}
AFRICA_EAST_COUNTRIES = {"KENYA","TANZANIA","UGANDA","RWANDA","ETHIOPIA","SEYCHELLES","MAURITIUS"}

ASIA_EAST_COUNTRIES = {"JAPAN","SOUTH KOREA","KOREA","CHINA","HONG KONG","TAIWAN","MONGOLIA"}
ASIA_SOUTHEAST_COUNTRIES = {"THAILAND","VIETNAM","INDONESIA","MALAYSIA","SINGAPORE","PHILIPPINES","CAMBODIA","LAOS","MYANMAR","BRUNEI"}
ASIA_SOUTH_COUNTRIES = {"INDIA","SRI LANKA","NEPAL","PAKISTAN","BANGLADESH","MALDIVES","BHUTAN"}

AUSTRALASIA_COUNTRIES = {"AUSTRALIA","NEW ZEALAND"}

SOUTH_AMERICA_NORTH_COUNTRIES = {"COSTA RICA","PANAMA","COLOMBIA","ECUADOR","VENEZUELA"}
SOUTH_AMERICA_SOUTH_COUNTRIES = {"PERU","CHILE","ARGENTINA","BRAZIL","BOLIVIA","URUGUAY","PARAGUAY"}

NORTH_AMERICA_COUNTRIES = {"UNITED STATES","USA","CANADA","MEXICO"}


def infer_zone(destination_iata: str, destination_country: str) -> str:
    di = up(destination_iata)
    dc = up(destination_country)

    if di in ICELAND_ARCTIC_IATA or dc == "ICELAND":
        return "iceland_arctic"

    if di in CANARIES_MADEIRA_IATA:
        return "canaries_madeira"

    if dc in NORTH_AFRICA_COUNTRIES:
        return "north_africa"

    if dc in MIDDLE_EAST_COUNTRIES:
        return "middle_east"

    if dc in AFRICA_SOUTH_COUNTRIES:
        return "africa_south"

    if dc in AFRICA_EAST_COUNTRIES:
        return "africa_east"

    if dc in AUSTRALASIA_COUNTRIES:
        return "australasia"

    if dc in ASIA_EAST_COUNTRIES:
        return "asia_east"

    if dc in ASIA_SOUTHEAST_COUNTRIES:
        return "asia_southeast"

    if dc in ASIA_SOUTH_COUNTRIES:
        return "asia_south"

    if dc in SOUTH_AMERICA_NORTH_COUNTRIES:
        return "south_america_north"

    if dc in SOUTH_AMERICA_SOUTH_COUNTRIES:
        return "south_america_south"

    if dc in NORTH_AMERICA_COUNTRIES:
        # split east/west by IATA hint
        if di in NORTH_AMERICA_WEST_IATA:
            return "north_america_west"
        if di in NORTH_AMERICA_EAST_IATA:
            return "north_america_east"
        # default east (more common for UK)
        return "north_america_east"

    # Europe fallback
    if dc in EUROPE_COUNTRIES or dc == "":
        # If we can't tell, treat as shorthaul to be conservative
        return "europe_shorthaul"

    # Unknown fallback
    return "europe_shorthaul"


# =======================
# Core scoring
# =======================

def compute_base_score(price: Optional[float], stops: Optional[int], days_ahead: Optional[int]) -> float:
    """
    Low-tech baseline: timing + stops + slight price presence.
    This is NOT the price intelligence layer.
    """
    score = 0.0

    # Price present?
    if price is not None:
        score += 5.0

    # Stops
    if stops is not None:
        if stops == 0:
            score += 12.0
        elif stops == 1:
            score += 6.0
        else:
            score -= 4.0 * max(0, stops - 1)

    # Timing
    if days_ahead is not None:
        if 20 <= days_ahead <= 70:
            score += 8.0
        elif days_ahead < 10:
            score -= 6.0
        elif days_ahead > 120:
            score -= 4.0

    return score


def compute_price_band_score(price: Optional[float], bench: Optional[Dict[str, Any]]) -> Tuple[float, str]:
    """
    Returns (score, band_label)
      band_label in {PRIZE, GOOD, OK, BAD, UNKNOWN}
    """
    if price is None or not bench:
        return 0.0, "UNKNOWN"

    low_p = float(bench["low_price"])
    norm_p = float(bench["normal_price"])
    high_p = float(bench["high_price"])

    if price <= low_p:
        return 40.0, "PRIZE"
    if price <= norm_p:
        return 20.0, "GOOD"
    if price <= high_p:
        return 5.0, "OK"
    return -30.0, "BAD"


def why_good_text(band: str, zone: str, theme: str, price: Optional[float], bench: Optional[Dict[str, Any]]) -> str:
    if price is None or not bench or band == "UNKNOWN":
        return "Scored using timing/stops only (no benchmark match)."
    lp = int(round(float(bench["low_price"])))
    np = int(round(float(bench["normal_price"])))
    hp = int(round(float(bench["high_price"])))
    p = int(round(float(price)))
    if band == "PRIZE":
        return f"Prize fish for {theme} in {zone}: £{p} (<= low £{lp})."
    if band == "GOOD":
        return f"Strong value for {theme} in {zone}: £{p} (<= normal £{np})."
    if band == "OK":
        return f"Acceptable for {theme} in {zone}: £{p} (<= high £{hp})."
    return f"Not a deal for {theme} in {zone}: £{p} (> high £{hp})."


# =======================
# Main
# =======================

def main() -> int:
    raw_tab = os.getenv("RAW_DEALS_TAB", "RAW_DEALS")

    WINNERS_PER_RUN = env_int("WINNERS_PER_RUN", 1)
    VARIETY_LOOKBACK_HOURS = env_int("VARIETY_LOOKBACK_HOURS", 120)
    DEST_REPEAT_PENALTY = env_float("DEST_REPEAT_PENALTY", 80.0)
    THEME_REPEAT_PENALTY = env_float("THEME_REPEAT_PENALTY", 25.0)

    # If TRUE, we will never promote "BAD" band deals even if nothing else exists.
    HARD_BLOCK_BAD_DEALS = (os.getenv("HARD_BLOCK_BAD_DEALS", "true").strip().lower() == "true")

    log("=" * 80)
    log("TRAVELTXTER AI SCORER — ZONE×THEME BENCHMARKS")
    log("=" * 80)
    log(f"RAW_DEALS_TAB={raw_tab}")
    log(f"WINNERS_PER_RUN={WINNERS_PER_RUN}")
    log(f"VARIETY_LOOKBACK_HOURS={VARIETY_LOOKBACK_HOURS}, DEST_REPEAT_PENALTY={DEST_REPEAT_PENALTY}, THEME_REPEAT_PENALTY={THEME_REPEAT_PENALTY}")
    log(f"HARD_BLOCK_BAD_DEALS={HARD_BLOCK_BAD_DEALS}")

    sh = open_sheet()
    ws = sh.worksheet(raw_tab)

    # Load benchmark brain
    benchmarks = load_zone_theme_benchmarks(sh)

    values = ws.get_all_values()
    if len(values) < 2:
        log("Sheet empty. Nothing to score.")
        return 0

    headers = [h.strip() for h in values[0]]
    h = {name: i for i, name in enumerate(headers)}

    # Required columns (LOCKED)
    required = [
        "status", "deal_id", "price_gbp",
        "origin_city", "origin_iata",
        "destination_city", "destination_iata", "destination_country",
        "outbound_date", "return_date", "stops",
        "deal_theme"
    ]
    missing = [c for c in required if c not in h]
    if missing:
        raise RuntimeError(f"RAW_DEALS missing required columns: {missing}")

    # Columns we write (create if missing)
    out_cols = ["deal_score", "dest_variety_score", "theme_variety_score", "scored_timestamp", "why_good"]
    for c in out_cols:
        if c not in h:
            raise RuntimeError(f"RAW_DEALS missing required output column: {c}")

    # Optional write columns (only if present)
    has_ai_notes = ("ai_notes" in h)
    has_zone = ("zone" in h)  # optional if you add later
    has_price_band = ("price_band" in h)  # optional if you add later

    # Gather NEW rows
    new_rows: List[Tuple[int, List[str]]] = []
    for i, row in enumerate(values[1:], start=2):  # sheet row number
        status = (row[h["status"]] if h["status"] < len(row) else "").strip()
        if status == "NEW":
            new_rows.append((i, row))

    if not new_rows:
        log("No NEW rows to score.")
        return 0

    log(f"✓ Found NEW rows: {len(new_rows)}")

    # Recent destination + theme sets for variety penalties
    lookback_cutoff = dt.datetime.utcnow() - dt.timedelta(hours=VARIETY_LOOKBACK_HOURS)
    recent_dests: Set[str] = set()
    recent_themes: Set[str] = set()

    if "scored_timestamp" in h and "destination_iata" in h and "deal_theme" in h:
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

            dest_val = row[dest_idx] if dest_idx < len(row) else ""
            theme_val = row[theme_idx] if theme_idx < len(row) else ""
            if dest_val:
                recent_dests.add(up(dest_val))
            if theme_val:
                recent_themes.add(str(theme_val).strip().lower())

    scored: List[Tuple[float, int, str]] = []  # (final_score, sheet_row, band)

    updates: List[Tuple[str, Any]] = []

    for sheet_row, row in new_rows:
        dest_iata = up(row[h["destination_iata"]] if h["destination_iata"] < len(row) else "")
        dest_country = (row[h["destination_country"]] if h["destination_country"] < len(row) else "").strip()
        theme = low(row[h["deal_theme"]] if h["deal_theme"] < len(row) else "")

        price = safe_float(row[h["price_gbp"]] if h["price_gbp"] < len(row) else None)
        stops = safe_int(row[h["stops"]] if h["stops"] < len(row) else None)

        # days ahead from outbound_date
        days_ahead = None
        od = row[h["outbound_date"]] if h["outbound_date"] < len(row) else ""
        if od:
            try:
                d = dt.datetime.fromisoformat(str(od)[:10]).date()
                days_ahead = (d - dt.datetime.utcnow().date()).days
            except Exception:
                days_ahead = None

        # Zone & benchmark lookup
        zone = infer_zone(dest_iata, dest_country)
        bench = benchmarks.get((zone, theme))

        # Price band score
        band_score, band = compute_price_band_score(price, bench)

        # Base score (timing/stops)
        base_score = compute_base_score(price, stops, days_ahead)

        # Variety penalties
        dest_variety_score = 0.0
        if dest_iata and dest_iata in recent_dests:
            dest_variety_score = -float(DEST_REPEAT_PENALTY)

        theme_variety_score = 0.0
        if theme and theme in recent_themes:
            theme_variety_score = -float(THEME_REPEAT_PENALTY)

        final_score = base_score + band_score + dest_variety_score + theme_variety_score

        # Deterministic rationale
        why = why_good_text(band, zone, theme, price, bench)

        note_bits = []
        note_bits.append(f"zone={zone}")
        note_bits.append(f"theme={theme}")
        note_bits.append(f"band={band}")
        if bench:
            note_bits.append(f"bench=({bench['low_price']},{bench['normal_price']},{bench['high_price']})")
        if dest_variety_score < 0:
            note_bits.append("dest_repeat_penalty")
        if theme_variety_score < 0:
            note_bits.append("theme_repeat_penalty")
        notes = "; ".join(note_bits)

        # Write outputs
        updates.append((a1(sheet_row, h["deal_score"]), round(final_score, 2)))
        updates.append((a1(sheet_row, h["dest_variety_score"]), round(dest_variety_score, 2)))
        updates.append((a1(sheet_row, h["theme_variety_score"]), round(theme_variety_score, 2)))
        updates.append((a1(sheet_row, h["scored_timestamp"]), dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"))
        updates.append((a1(sheet_row, h["why_good"]), why))
        if has_ai_notes:
            updates.append((a1(sheet_row, h["ai_notes"]), notes))
        if has_zone:
            updates.append((a1(sheet_row, h["zone"]), zone))
        if has_price_band:
            updates.append((a1(sheet_row, h["price_band"]), band))

        scored.append((final_score, sheet_row, band))

    # Batch write (single-cell updates but grouped)
    log(f"Writing {len(updates)} cell updates...")
    for cell, val in updates:
        ws.update([[val]], cell)

    # Rank: highest score first
    scored.sort(key=lambda x: x[0], reverse=True)

    # Winner selection with safety: optionally block BAD deals
    winners: List[Tuple[float, int, str]] = []
    for s, r, band in scored:
        if HARD_BLOCK_BAD_DEALS and band == "BAD":
            continue
        winners.append((s, r, band))
        if len(winners) >= max(1, WINNERS_PER_RUN):
            break

    if not winners:
        log("⚠️ No eligible winners (all NEW deals were BAD vs benchmarks). Marking them SCORED; no publishing.")
        winner_rows: Set[int] = set()
    else:
        winner_rows = {r for _s, r, _b in winners}

    # Update statuses
    for _s, sheet_row, _band in scored:
        status_cell = a1(sheet_row, h["status"])
        if sheet_row in winner_rows:
            ws.update([["READY_TO_POST"]], status_cell)
        else:
            ws.update([["SCORED"]], status_cell)

    log(f"✅ Winners promoted to READY_TO_POST: {len(winner_rows)} (WINNERS_PER_RUN={WINNERS_PER_RUN})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
