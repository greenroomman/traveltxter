#!/usr/bin/env python3
from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials

# ============================================================
# TravelTxter Enrichment Router — V5
# - IATA backfill (origin/destination city/country)
# - Phrase Bank selection
# - Writes ONLY enrichment fields (no status changes)
# ============================================================

RAW_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS")
PHRASE_BANK_TAB = os.getenv("PHRASE_BANK_TAB") or os.getenv("PHRASES_TAB") or "PHRASE_BANK"
IATA_MASTER_TAB = os.getenv("IATA_MASTER_TAB", "IATA_MASTER")

PHRASE_CHANNEL = (os.getenv("PHRASE_CHANNEL", "vip") or "vip").strip().lower()
MAX_ROWS_PER_RUN = int(os.getenv("ENRICH_MAX_ROWS_PER_RUN", "60") or "60")
ENFORCE_MAX_PER_MONTH = str(os.getenv("ENFORCE_MAX_PER_MONTH", "true")).strip().lower() in ("1", "true", "yes", "y")
REQUIRE_PHRASE = str(os.getenv("REQUIRE_PHRASE", "false")).strip().lower() in ("1", "true", "yes", "y")

ELIGIBLE_STATUSES = ["READY_TO_POST", "READY_TO_PUBLISH", "SCORED", "PUBLISH_AM", "PUBLISH_PM", "PUBLISH_BOTH", "READY_FREE", "VIP_DONE"]

# -------------------- utilities --------------------

def log(msg: str) -> None:
    print(f"{datetime.now(timezone.utc).isoformat(timespec='seconds')}Z | {msg}")

def _norm(s: str) -> str:
    return re.sub(r"[\s\-]+", "_", (s or "").strip().lower())

def _first_norm_index(headers: List[str]) -> Dict[str, int]:
    idx: Dict[str, int] = {}
    for i, h in enumerate(headers):
        n = _norm(h)
        if n and n not in idx:
            idx[n] = i
    return idx

def _norm_index_with_dupes(headers: List[str]) -> Tuple[Dict[str, int], Dict[str, List[int]]]:
    """Return normalised header -> first index, plus a dupes map normalised header -> all indexes."""
    norm_to_idxs: Dict[str, List[int]] = {}
    for i, h in enumerate(headers):
        n = _norm(h)
        if not n:
            continue
        norm_to_idxs.setdefault(n, []).append(i)
    first: Dict[str, int] = {k: v[0] for k, v in norm_to_idxs.items()}
    dupes: Dict[str, List[int]] = {k: v for k, v in norm_to_idxs.items() if len(v) > 1}
    return first, dupes

def get_env_sa_json() -> str:
    raw = os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON") or ""
    raw = raw.strip()
    if not raw:
        raise RuntimeError("Missing service account JSON in env (GCP_SA_JSON_ONE_LINE or GCP_SA_JSON).")
    # Handle common GitHub Secrets escaping
    raw = raw.replace("\\n", "\n")
    return raw

def gspread_client() -> gspread.Client:
    import json
    sa = json.loads(get_env_sa_json())
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(sa, scopes=scopes)
    return gspread.authorize(creds)

def open_sheet(gc: gspread.Client) -> gspread.Spreadsheet:
    sid = os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID")
    if not sid:
        raise RuntimeError("Missing SPREADSHEET_ID (or SHEET_ID).")
    return gc.open_by_key(sid)

def getv(row: List[str], idx0: Optional[int]) -> str:
    if idx0 is None:
        return ""
    if idx0 < 0 or idx0 >= len(row):
        return ""
    return (row[idx0] or "").strip()

def safe_upper(s: str) -> str:
    return (s or "").strip().upper()

# -------------------- phrase bank --------------------

@dataclass
class Phrase:
    destination_iata: str
    theme: str
    category: str
    phrase: str
    channel_hint: str
    max_per_month: int

def month_key(dt: datetime) -> str:
    return dt.strftime("%Y-%m")

def load_iata_master(ws: gspread.Worksheet) -> Dict[str, Tuple[str, str]]:
    """
    IATA_MASTER headers:
      iata_code | city | country
    """
    t0 = time.time()
    rows = ws.get_all_values()
    if not rows or len(rows) < 2:
        return {}
    headers = rows[0]
    idx = _first_norm_index(headers)

    c_iata = idx.get("iata_code")
    c_city = idx.get("city")
    c_country = idx.get("country")
    if c_iata is None or c_city is None or c_country is None:
        raise RuntimeError("IATA_MASTER missing required headers: iata_code, city, country")

    m: Dict[str, Tuple[str, str]] = {}
    for r in rows[1:]:
        code = safe_upper(getv(r, c_iata))
        if not code:
            continue
        city = getv(r, c_city)
        country = getv(r, c_country)
        m[code] = (city, country)

    log(f"📥 IATA_MASTER: {len(rows)-1} rows read, {len(m)} entries indexed ({time.time()-t0:.1f}s)")
    return m

def load_phrase_bank(ws: gspread.Worksheet) -> List[Phrase]:
    """
    PHRASE_BANK headers + sample:
      destination_iata theme category phrase approved channel_hint max_per_month notes context_hint
    """
    t0 = time.time()
    rows = ws.get_all_values()
    if not rows or len(rows) < 2:
        return []
    headers = rows[0]
    idx = _first_norm_index(headers)

    need = ["destination_iata", "theme", "category", "phrase", "approved", "channel_hint", "max_per_month"]
    missing = [h for h in need if idx.get(h) is None]
    if missing:
        raise RuntimeError(f"PHRASE_BANK missing required headers: {missing}")

    phrases: List[Phrase] = []
    for r in rows[1:]:
        approved = getv(r, idx["approved"]).strip().lower() in ("1", "true", "yes", "y")
        if not approved:
            continue

        dest = safe_upper(getv(r, idx["destination_iata"]))
        theme = _norm(getv(r, idx["theme"]))
        category = getv(r, idx["category"])
        phrase_txt = getv(r, idx["phrase"])
        channel_hint = (getv(r, idx["channel_hint"]) or "").strip()
        try:
            maxpm = int(getv(r, idx["max_per_month"]) or "0")
        except ValueError:
            maxpm = 0

        if not dest or not theme or not phrase_txt:
            continue

        phrases.append(Phrase(dest, theme, category, phrase_txt, channel_hint, maxpm))

    log(f"📥 PHRASE_BANK: {len(rows)-1} rows read, {len(phrases)} approved phrases ({time.time()-t0:.1f}s)")
    return phrases

def build_phrase_usage_index(raw_rows: List[List[str]], idx_raw: Dict[str, int]) -> Dict[Tuple[str, str, str], int]:
    """
    Count phrase usage per (dest, theme, month) based on RAW_DEALS.phrase_used and ingested_at_utc.
    """
    c_phrase = idx_raw.get("phrase_used")
    c_dest = idx_raw.get("destination_iata")
    c_theme = idx_raw.get("theme")
    c_ing = idx_raw.get("ingested_at_utc")
    if c_phrase is None or c_dest is None or c_theme is None or c_ing is None:
        return {}

    counts: Dict[Tuple[str, str, str], int] = {}
    for r in raw_rows:
        phrase = getv(r, c_phrase)
        if not phrase:
            continue
        dest = safe_upper(getv(r, c_dest))
        theme = _norm(getv(r, c_theme))
        ing = getv(r, c_ing)
        if not dest or not theme or not ing:
            continue
        # ingested_at_utc is stored in sheet; allow either ISO string or number; month key only needs parse best-effort
        mk = ""
        try:
            # ISO parse
            dt = datetime.fromisoformat(ing.replace("Z", "+00:00"))
            mk = month_key(dt.astimezone(timezone.utc))
        except Exception:
            # numeric serial (Sheets): ignore if we can't parse
            continue
        k = (dest, theme, mk)
        counts[k] = counts.get(k, 0) + 1
    return counts

def choose_phrase(
    dest: str,
    theme: str,
    phrases: List[Phrase],
    usage: Dict[Tuple[str, str, str], int],
    now_utc: datetime
) -> Optional[Phrase]:
    mk = month_key(now_utc)
    candidates = [p for p in phrases if p.destination_iata == dest and p.theme == theme]
    if not candidates:
        return None

    # Channel filtering (light-touch)
    if PHRASE_CHANNEL and PHRASE_CHANNEL != "any":
        filtered = []
        for p in candidates:
            hint = (p.channel_hint or "").lower()
            if not hint:
                filtered.append(p)
            elif PHRASE_CHANNEL in hint.lower():
                filtered.append(p)
        if filtered:
            candidates = filtered

    if not ENFORCE_MAX_PER_MONTH:
        return candidates[0]

    # enforce max_per_month where > 0
    for p in candidates:
        if p.max_per_month <= 0:
            return p
        used = usage.get((p.destination_iata, p.theme, mk), 0)
        if used < p.max_per_month:
            return p

    return None

# -------------------- main --------------------

def main() -> None:
    log("============================================================")
    log("🧩 TravelTxter Enrichment Router — V5 (IATA backfill + Phrase Bank selection)")
    log("============================================================")
    log(f"{RAW_TAB=} | {PHRASE_BANK_TAB=} | {IATA_MASTER_TAB=}")
    log(f"{PHRASE_CHANNEL=} | {MAX_ROWS_PER_RUN=}")
    log(f"{ENFORCE_MAX_PER_MONTH=} | {REQUIRE_PHRASE=}")
    log(f"ELIGIBLE_STATUSES={ELIGIBLE_STATUSES}")

    gc = gspread_client()
    sh = open_sheet(gc)
    ws_raw = sh.worksheet(RAW_TAB)
    ws_iata = sh.worksheet(IATA_MASTER_TAB)
    ws_ph = sh.worksheet(PHRASE_BANK_TAB)

    iata_map = load_iata_master(ws_iata)
    log(f"✅ IATA_MASTER loaded: {len(iata_map)} entries")

    phrase_list = load_phrase_bank(ws_ph)
    log(f"✅ PHRASE_BANK loaded (approved): {len(phrase_list)} phrases")

    t0 = time.time()
    raw = ws_raw.get_all_values()
    log(f"📥 Loading {RAW_TAB}...")
    if not raw or len(raw) < 2:
        log("✅ No rows to enrich.")
        return

    log(f"✅ {RAW_TAB} loaded: {len(raw)-1} rows ({time.time()-t0:.1f}s)")

    headers = raw[0]
    idx, dupes = _norm_index_with_dupes(headers)
    if dupes:
        log(f"⚠️ Duplicate headers detected in RAW_DEALS (first occurrence used): {sorted(list(dupes.keys()))}")

    # columns (optional if missing)
    c_status = idx.get("status")
    c_origin_iata = idx.get("origin_iata")
    c_dest_iata = idx.get("destination_iata")
    c_origin_city = idx.get("origin_city")
    c_origin_country = idx.get("origin_country")
    c_dest_city = idx.get("destination_city")
    c_dest_country = idx.get("destination_country")
    c_theme = idx.get("theme")
    c_phrase_used = idx.get("phrase_used")
    c_phrase_cat = idx.get("phrase_category")
    c_ing = idx.get("ingested_at_utc")

    # Hard requirement to do anything meaningful
    need_any = [c_status, c_origin_iata, c_dest_iata, c_theme, c_ing]
    if any(v is None for v in need_any):
        missing = []
        if c_status is None: missing.append("status")
        if c_origin_iata is None: missing.append("origin_iata")
        if c_dest_iata is None: missing.append("destination_iata")
        if c_theme is None: missing.append("theme")
        if c_ing is None: missing.append("ingested_at_utc")
        raise RuntimeError(f"RAW_DEALS missing required headers for enrich: {missing}")

    # Build phrase usage index (month-aware)
    usage = build_phrase_usage_index(raw[1:], idx)
    log(f"✅ Phrase usage index built (month-aware): {len(usage)} keys")

    # Queue updates
    updates: List[Tuple[int, int, str]] = []  # (row_num_1based, col_num_1based, value)

    def queue(row_num: int, col_num: int, value: str) -> None:
        updates.append((row_num, col_num, value))

    def get_row_status(r: List[str]) -> str:
        return _norm(getv(r, c_status))

    scanned = 0
    eligible = 0
    enriched = 0
    cityfills = 0
    phrasefills = 0
    phrase_misses = 0

    now_utc = datetime.now(timezone.utc)

    # Start from row 2 in sheet, which is raw[1] here
    for i, r in enumerate(raw[1:], start=2):
        scanned += 1
        status = get_row_status(r)
        if status.upper() not in [s.upper() for s in ELIGIBLE_STATUSES]:
            continue
        eligible += 1
        if enriched >= MAX_ROWS_PER_RUN:
            break

        row_changed = False

        origin = safe_upper(getv(r, c_origin_iata))
        dest = safe_upper(getv(r, c_dest_iata))
        theme = _norm(getv(r, c_theme))

        # IATA backfill: origin city (+ optional origin country)
        if c_origin_city is not None and not getv(r, c_origin_city) and origin in iata_map:
            city, _country = iata_map[origin]
            if city:
                queue(i, c_origin_city + 1, city)
                cityfills += 1
                row_changed = True

        if c_origin_country is not None and not getv(r, c_origin_country) and origin in iata_map:
            _city, country = iata_map[origin]
            if country:
                queue(i, c_origin_country + 1, country)
                cityfills += 1
                row_changed = True

        # IATA backfill: destination city/country
        if c_dest_city is not None and not getv(r, c_dest_city) and dest in iata_map:
            city, _country = iata_map[dest]
            if city:
                queue(i, c_dest_city + 1, city)
                cityfills += 1
                row_changed = True

        if c_dest_country is not None and not getv(r, c_dest_country) and dest in iata_map:
            _city, country = iata_map[dest]
            if country:
                queue(i, c_dest_country + 1, country)
                cityfills += 1
                row_changed = True

        # Phrase selection
        if c_phrase_used is not None and not getv(r, c_phrase_used):
            chosen = choose_phrase(dest, theme, phrase_list, usage, now_utc)
            if chosen is None:
                phrase_misses += 1
                if REQUIRE_PHRASE:
                    # Do not enrich row if phrase is required but unavailable
                    continue
            else:
                queue(i, c_phrase_used + 1, chosen.phrase)
                phrasefills += 1
                row_changed = True

                if c_phrase_cat is not None and not getv(r, c_phrase_cat) and chosen.category:
                    queue(i, c_phrase_cat + 1, chosen.category)
                    row_changed = True

        if row_changed:
            enriched += 1

    log("------------------------------------------------------------")
    log(f"Scanned rows: {scanned}")
    log(f"Eligible rows (SCORED/READY_*/PUBLISH_*): {eligible}")
    log(f"Rows enriched: {enriched} (cap {MAX_ROWS_PER_RUN})")
    log(f"City/Country fills: {cityfills}")
    log(f"Phrase fills: {phrasefills} (channel={PHRASE_CHANNEL})")
    log(f"Phrase misses (no candidate after governance): {phrase_misses}")
    log(f"Cells queued: {len(updates)}")
    log("------------------------------------------------------------")

    if not updates:
        log("✅ Enrichment Router complete (idempotent; safe to re-run)")
        return

    # Batch write (minimise API calls)
    # We write per cell but in a single update_cells call.
    cell_list = ws_raw.range(
        min(r for r, c, v in updates),
        min(c for r, c, v in updates),
        max(r for r, c, v in updates),
        max(c for r, c, v in updates),
    )
    # Build a map for quick assignment
    cell_map = {(cell.row, cell.col): cell for cell in cell_list}
    for r, c, v in updates:
        cell = cell_map.get((r, c))
        if cell:
            cell.value = v

    ws_raw.update_cells(cell_list, value_input_option="USER_ENTERED")
    log(f"✅ Cells written (batch): {len(updates)}")
    log("✅ Enrichment Router complete (idempotent; safe to re-run)")

if __name__ == "__main__":
    main()
