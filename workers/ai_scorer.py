#!/usr/bin/env python3
"""
TRAVELTXTTER V5 — AI_SCORER (Deterministic, Spreadsheet-led)

PURPOSE
- Read RAW_DEALS rows with status=NEW.
- Assign a numeric score.
- Promote a small number of winners to status=READY_TO_POST (publishable).
- Mark remaining NEW rows as SCORED (seen, but not publish-worthy right now).
- Optionally HARD_REJECT clearly invalid rows (non-GBP, missing dates, etc).

CONTRACT (V5 MINIMAL)
- RAW_DEALS is the single writable truth.
- RAW_DEALS_VIEW is read-only; scorer may *read* it but must not write to it.
- Downstream workers (enrich/render/publishers) READ status; they do not change it
  except publishers changing status after posting.
"""

from __future__ import annotations

import base64
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials


# ----------------------------- Logging -----------------------------

def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    print(f"{ts} | {msg}", flush=True)


# -------------------------- GCP auth helpers -----------------------

_CONTROL_CHARS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def _clean_json_string(raw: str) -> str:
    raw = (raw or "").strip()
    raw = _CONTROL_CHARS.sub("", raw)
    return raw


def _try_json_loads(raw: str) -> Optional[dict]:
    try:
        return json.loads(raw)
    except Exception:
        return None


def load_service_account_info() -> dict:
    """
    Robustly parse service account json from:
    - GCP_SA_JSON_ONE_LINE (preferred)
    - GCP_SA_JSON

    Handles:
    - raw JSON
    - JSON with escaped newlines
    - base64-encoded JSON (common in CI)
    """
    raw = os.environ.get("GCP_SA_JSON_ONE_LINE") or os.environ.get("GCP_SA_JSON") or ""
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON env var")

    raw = _clean_json_string(raw)

    # 1) direct json
    obj = _try_json_loads(raw)
    if obj:
        return obj

    # 2) escaped newline variant
    obj = _try_json_loads(raw.replace("\\n", "\n"))
    if obj:
        return obj

    # 3) base64
    try:
        decoded = base64.b64decode(raw).decode("utf-8", errors="replace")
        decoded = _clean_json_string(decoded)
        obj = _try_json_loads(decoded) or _try_json_loads(decoded.replace("\\n", "\n"))
        if obj:
            return obj
    except Exception:
        pass

    # 4) last resort: remove literal newlines and retry (some secrets get wrapped)
    compact = raw.replace("\n", "")
    obj = _try_json_loads(compact)
    if obj:
        return obj

    raise RuntimeError("Could not parse service account JSON (check secret formatting)")


def gspread_client() -> gspread.Client:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    info = load_service_account_info()
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


# ----------------------------- Sheet utils -------------------------

def open_spreadsheet(gc: gspread.Client) -> gspread.Spreadsheet:
    sheet_id = os.environ.get("SPREADSHEET_ID") or os.environ.get("SHEET_ID")
    if not sheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID env var")
    return gc.open_by_key(sheet_id)


def open_ws(sh: gspread.Spreadsheet, name: str) -> gspread.Worksheet:
    try:
        return sh.worksheet(name)
    except Exception as e:
        raise RuntimeError(f"WorksheetNotFound: '{name}'") from e


def headers_map(ws: gspread.Worksheet) -> Dict[str, int]:
    header_row = ws.row_values(1)
    return {h.strip(): i for i, h in enumerate(header_row) if h.strip()}


def get_cell(ws: gspread.Worksheet, a1: str) -> str:
    try:
        return str(ws.acell(a1).value or "").strip()
    except Exception:
        return ""


def _parse_iso_utc(ts: str) -> Optional[datetime]:
    if not ts:
        return None
    s = ts.strip()
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _safe_float(x: str) -> Optional[float]:
    s = str(x or "").strip().replace("£", "").replace(",", "")
    if s == "":
        return None
    try:
        return float(s)
    except Exception:
        return None


def _safe_int(x: str) -> Optional[int]:
    s = str(x or "").strip()
    if s == "":
        return None
    try:
        return int(float(s))
    except Exception:
        return None


# ------------------------------ Scoring ----------------------------

@dataclass
class DealRow:
    row_idx: int
    deal_id: str
    theme: str
    status: str
    price_gbp: Optional[float]
    currency: str
    stops: Optional[int]
    ingested_at_utc: str


def _env_int(name: str, default: int) -> int:
    v = (os.environ.get(name) or "").strip()
    if not v:
        return default
    try:
        return int(v)
    except Exception:
        return default


def _max_stops_for_theme(theme: str) -> int:
    key = f"MAX_STOPS_{theme.upper()}"
    v = (os.environ.get(key) or "").strip()
    if v:
        try:
            return int(v)
        except Exception:
            pass
    return 1


def _compute_relative_scores(prices: List[float]) -> List[float]:
    if not prices:
        return []
    sp = sorted(prices)
    n = len(sp)
    q1 = sp[max(0, int(0.25 * (n - 1)))]
    q3 = sp[max(0, int(0.75 * (n - 1)))]
    span = max(1.0, (q3 - q1))
    out: List[float] = []
    for p in prices:
        z = (p - q1) / span
        score = 85.0 - (z * 30.0)
        score = max(1.0, min(99.0, score))
        out.append(score)
    return out


# ------------------------------ Main --------------------------------

RAW_TAB = os.environ.get("RAW_DEALS_TAB", "RAW_DEALS")
OPS_TAB = os.environ.get("OPS_MASTER_TAB", "OPS_MASTER")

OPS_THEME_CELL = os.environ.get("OPS_THEME_CELL", "B2")  # V5: theme of day
OPS_SLOT_CELL = os.environ.get("OPS_SLOT_CELL", "A2")    # V5: AM/PM label


def main() -> int:
    log("📥 TravelTxter V5 — AI_SCORER start")

    min_age_seconds = _env_int("MIN_INGEST_AGE_SECONDS", 90)
    winners_per_run = _env_int("WINNERS_PER_RUN", 2)

    gc = gspread_client()
    sh = open_spreadsheet(gc)

    ws_raw = open_ws(sh, RAW_TAB)
    ws_ops = open_ws(sh, OPS_TAB)

    theme_today = get_cell(ws_ops, OPS_THEME_CELL) or "DEFAULT"
    slot = (os.environ.get("RUN_SLOT") or get_cell(ws_ops, OPS_SLOT_CELL) or "PM").upper()
    if slot not in ("AM", "PM"):
        slot = "PM"

    log(f"🎯 Theme of day: {theme_today}")
    log(f"🕒 Slot: {slot} | MIN_INGEST_AGE_SECONDS={min_age_seconds} | WINNERS_PER_RUN={winners_per_run}")

    hmap = headers_map(ws_raw)
    required = ["deal_id", "theme", "status", "price_gbp", "currency", "stops", "ingested_at_utc", "publish_window", "score"]
    missing = [h for h in required if h not in hmap]
    if missing:
        raise RuntimeError(f"{RAW_TAB} schema missing required columns: {missing}")

    has_scored_ts = "scored_timestamp" in hmap
    if not has_scored_ts:
        log(f"ℹ️ {RAW_TAB} missing 'scored_timestamp' column. Timestamp write-back will be skipped.")

    values = ws_raw.get_all_values()
    if len(values) < 2:
        log("No rows to score.")
        return 0

    now = datetime.now(timezone.utc)

    rows: List[DealRow] = []
    skipped_too_fresh = 0
    skipped_no_ts = 0

    for sheet_i, r in enumerate(values[1:], start=2):
        def col(name: str) -> str:
            j = hmap.get(name)
            return (r[j] if (j is not None and j < len(r)) else "").strip()

        status = col("status").upper()
        if status != "NEW":
            continue

        ing = col("ingested_at_utc")
        dt = _parse_iso_utc(ing)
        if not dt:
            skipped_no_ts += 1
            continue

        age_s = (now - dt).total_seconds()
        if age_s < float(min_age_seconds):
            skipped_too_fresh += 1
            continue

        deal_id = col("deal_id")
        if not deal_id:
            continue

        rows.append(
            DealRow(
                row_idx=sheet_i,
                deal_id=deal_id,
                theme=(col("theme") or theme_today).strip() or theme_today,
                status=status,
                price_gbp=_safe_float(col("price_gbp")),
                currency=col("currency").upper(),
                stops=_safe_int(col("stops")),
                ingested_at_utc=ing,
            )
        )

    log(f"Eligible NEW candidates: {len(rows)} | skipped_too_fresh={skipped_too_fresh} skipped_no_ingest_ts={skipped_no_ts}")

    if not rows:
        log("✅ No status changes needed (idempotent).")
        return 0

    max_stops = _max_stops_for_theme(theme_today)

    hard_reject: List[DealRow] = []
    scored_pool: List[DealRow] = []
    promotable: List[DealRow] = []

    for d in rows:
        if d.currency and d.currency != "GBP":
            hard_reject.append(d)
            continue
        if d.price_gbp is None or d.price_gbp <= 0:
            hard_reject.append(d)
            continue

        scored_pool.append(d)

        if d.theme == theme_today and (d.stops is None or d.stops <= max_stops):
            promotable.append(d)

    prices = [d.price_gbp for d in promotable if d.price_gbp is not None]
    rel = _compute_relative_scores(prices)

    price_to_best: Dict[float, float] = {}
    for p, s in zip(prices, rel):
        if (p not in price_to_best) or (s > price_to_best[p]):
            price_to_best[p] = s

    scored_items: List[Tuple[DealRow, float]] = []
    for d in promotable:
        scored_items.append((d, float(price_to_best.get(d.price_gbp or 0.0, 50.0))))

    scored_items.sort(key=lambda t: t[1], reverse=True)
    winners = scored_items[: max(0, winners_per_run)]
    winner_ids = {w[0].deal_id for w in winners}

    updates: List[gspread.Cell] = []

    def set_cell(row_idx: int, header: str, value: Any) -> None:
        col_idx = hmap[header] + 1
        updates.append(gspread.Cell(row_idx, col_idx, value))

    now_iso = now.isoformat(timespec="seconds").replace("+00:00", "Z")

    publish_ct = 0
    scored_ct = 0
    hard_ct = 0

    for d in rows:
        if d in hard_reject:
            set_cell(d.row_idx, "status", "HARD_REJECT")
            set_cell(d.row_idx, "publish_window", "")
            set_cell(d.row_idx, "score", 0)
            if has_scored_ts:
                set_cell(d.row_idx, "scored_timestamp", now_iso)
            hard_ct += 1
            continue

        base_score = 50.0
        if d.theme == theme_today and d.price_gbp is not None:
            base_score = float(price_to_best.get(d.price_gbp, 50.0))

        if d.deal_id in winner_ids:
            set_cell(d.row_idx, "status", "READY_TO_POST")
            set_cell(d.row_idx, "publish_window", slot)
            set_cell(d.row_idx, "score", round(base_score, 2))
            if has_scored_ts:
                set_cell(d.row_idx, "scored_timestamp", now_iso)
            publish_ct += 1
        else:
            set_cell(d.row_idx, "status", "SCORED")
            set_cell(d.row_idx, "publish_window", "")
            set_cell(d.row_idx, "score", round(base_score, 2))
            if has_scored_ts:
                set_cell(d.row_idx, "scored_timestamp", now_iso)
            scored_ct += 1

    if updates:
        ws_raw.update_cells(updates, value_input_option="USER_ENTERED")

    log(f"✅ Status writes: READY_TO_POST={publish_ct} SCORED={scored_ct} HARD_REJECT={hard_ct}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        log(f"❌ ERROR: {e}")
        raise
