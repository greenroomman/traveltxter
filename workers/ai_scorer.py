#!/usr/bin/env python3
"""
TRAVELTXTTER V5 — AI_SCORER (Deterministic, Spreadsheet-led)

PURPOSE
- Read RAW_DEALS rows with status=NEW.
- Assign a numeric score (0–99).
- Promote up to WINNERS_PER_RUN to status=READY_TO_POST.
- Mark remaining NEW rows as SCORED.
- Optionally HARD_REJECT invalid rows (non-GBP, missing price/dates/timestamps).

CONTRACT (V5 MINIMAL)
- RAW_DEALS is the single writable truth.
- RAW_DEALS_VIEW is read-only; scorer may read it (optional) but must never write to it.
- Downstream workers READ status.
- Publishers change status after posting (not scorer).
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
    - secrets wrapped with literal newlines
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

    # 4) last resort: remove literal newlines and retry
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


def get_cell(ws: gspread.Wor
ksheet, a1: str) -> str:
    try:
        return str(ws.acell(a1).value or "").strip()
    except Exception:
        return ""


def _parse_iso_utc(ts: str) -> Optional[datetime]:
    if not ts:
        return None
    s = str(ts).strip()
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


# ------------------------------ RDV optional ------------------------

def _load_rdv_dynamic_theme_index(
    sh: gspread.Spreadsheet,
    rdv_tab: str,
) -> Dict[str, str]:
    """
    Optional. If RDV exists and has columns:
      deal_id, dynamic_theme
    build dict for scoring/theme match.

    If not available, return {}.
    """
    try:
        ws = sh.worksheet(rdv_tab)
    except Exception:
        return {}

    try:
        vals = ws.get_all_values()
        if len(vals) < 2:
            return {}
        hdr = [h.strip() for h in vals[0]]
        hmap = {h: i for i, h in enumerate(hdr) if h}
        if "deal_id" not in hmap or "dynamic_theme" not in hmap:
            return {}
        out: Dict[str, str] = {}
        for r in vals[1:]:
            did = (r[hmap["deal_id"]] if hmap["deal_id"] < len(r) else "").strip()
            th = (r[hmap["dynamic_theme"]] if hmap["dynamic_theme"] < len(r) else "").strip()
            if did and th:
                out[did] = th
        return out
    except Exception:
        return {}


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


def _compute_scores_by_price(prices: List[float]) -> Dict[float, float]:
    """
    Cheapness relative score: cheapest ~ high score.
    Uses robust quartile span to avoid outliers.
    """
    if not prices:
        return {}

    sp = sorted(prices)
    n = len(sp)
    q1 = sp[max(0, int(0.25 * (n - 1)))]
    q3 = sp[max(0, int(0.75 * (n - 1)))]
    span = max(1.0, (q3 - q1))

    out: Dict[float, float] = {}
    for p in prices:
        z = (p - q1) / span
        score = 85.0 - (z * 30.0)
        score = max(1.0, min(99.0, score))
        # keep best score for same price
        out[p] = max(out.get(p, 0.0), score)
    return out


# ------------------------------ Main --------------------------------

RAW_TAB = os.environ.get("RAW_DEALS_TAB", "RAW_DEALS")
OPS_TAB = os.environ.get("OPS_MASTER_TAB", "OPS_MASTER")
RDV_TAB = os.environ.get("RAW_DEALS_VIEW_TAB", "RAW_DEALS_VIEW")

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

    required = [
        "deal_id",
        "status",
        "price_gbp",
        "currency",
        "stops",
        "ingested_at_utc",
        "publish_window",
        "score",
    ]
    missing = [h for h in required if h not in hmap]
    if missing:
        raise RuntimeError(f"{RAW_TAB} schema missing required columns: {missing}")

    has_theme_col = "theme" in hmap
    has_scored_ts = "scored_timestamp" in hmap
    if not has_scored_ts:
        log(f"ℹ️ {RAW_TAB} missing 'scored_timestamp' column. Timestamp write-back will be skipped.")

    # Optional RDV theme signal
    rdv_theme_by_id = _load_rdv_dynamic_theme_index(sh, RDV_TAB)
    if rdv_theme_by_id:
        log(f"✅ RDV loaded for scoring signals: {len(rdv_theme_by_id)} deal_ids indexed")
    else:
        log("ℹ️ RDV not used for scoring signals (missing tab or columns).")

    values = ws_raw.get_all_values()
    if len(values) < 2:
        log("No rows to score.")
        return 0

    now = datetime.now(timezone.utc)
    now_iso = now.isoformat(timespec="seconds").replace("+00:00", "Z")

    # Forensic counters
    seen_rows = len(values) - 1
    seen_new = 0
    skipped_status_not_new = 0
    skipped_missing_deal_id = 0
    skipped_missing_ingest = 0
    skipped_unparseable_ingest = 0
    skipped_too_fresh = 0

    new_rows: List[DealRow] = []

    for sheet_i, r in enumerate(values[1:], start=2):

        def col(name: str) -> str:
            j = hmap.get(name)
            return (r[j] if (j is not None and j < len(r)) else "").strip()

        status = col("status").upper()
        if status != "NEW":
            skipped_status_not_new += 1
            continue

        seen_new += 1

        deal_id = col("deal_id")
        if not deal_id:
            skipped_missing_deal_id += 1
            continue

        ing = col("ingested_at_utc")
        if not ing:
            skipped_missing_ingest += 1
            continue

        dt = _parse_iso_utc(ing)
        if not dt:
            skipped_unparseable_ingest += 1
            continue

        age_s = (now - dt).total_seconds()
        if age_s < float(min_age_seconds):
            skipped_too_fresh += 1
            continue

        theme_row = (col("theme") if has_theme_col else "").strip()
        theme_signal = (rdv_theme_by_id.get(deal_id) or "").strip()
        resolved_theme = (theme_row or theme_signal or theme_today).strip() or theme_today

        new_rows.append(
            DealRow(
                row_idx=sheet_i,
                deal_id=deal_id,
                theme=resolved_theme,
                status=status,
                price_gbp=_safe_float(col("price_gbp")),
                currency=col("currency").upper(),
                stops=_safe_int(col("stops")),
                ingested_at_utc=ing,
            )
        )

    log(
        "Forensics: "
        f"rows={seen_rows} NEW={seen_new} "
        f"eligible={len(new_rows)} "
        f"skipped_status_not_new={skipped_status_not_new} "
        f"skipped_missing_deal_id={skipped_missing_deal_id} "
        f"skipped_missing_ingest={skipped_missing_ingest} "
        f"skipped_unparseable_ingest={skipped_unparseable_ingest} "
        f"skipped_too_fresh={skipped_too_fresh}"
    )

    if not new_rows:
        log("✅ No status changes needed (idempotent).")
        return 0

    max_stops = _max_stops_for_theme(theme_today)

    hard_reject: List[DealRow] = []
    promotable: List[DealRow] = []

    # Basic validation + promotable filtering
    for d in new_rows:
        if d.currency and d.currency != "GBP":
            hard_reject.append(d)
            continue
        if d.price_gbp is None or d.price_gbp <= 0:
            hard_reject.append(d)
            continue

        # promotable = matches theme_today AND stops within tolerance
        if (d.theme == theme_today) and (d.stops is None or d.stops <= max_stops):
            promotable.append(d)

    # Price-based scoring within promotable only (so “cheap within theme” wins)
    price_scores = _compute_scores_by_price([d.price_gbp for d in promotable if d.price_gbp is not None])

    scored_items: List[Tuple[DealRow, float]] = []
    for d in promotable:
        base = float(price_scores.get(d.price_gbp or 0.0, 50.0))
        # small friction penalty for stops
        if d.stops is not None:
            base -= float(d.stops) * 5.0
        base = max(1.0, min(99.0, base))
        scored_items.append((d, base))

    scored_items.sort(key=lambda t: t[1], reverse=True)
    winners = scored_items[: max(0, winners_per_run)]
    winner_ids = {w[0].deal_id for w in winners}

    updates: List[gspread.Cell] = []

    def set_cell(row_idx: int, header: str, value: Any) -> None:
        col_idx = hmap[header] + 1
        updates.append(gspread.Cell(row_idx, col_idx, value))

    publish_ct = 0
    scored_ct = 0
    hard_ct = 0
    off_theme_ct = 0

    for d in new_rows:
        if d in hard_reject:
            set_cell(d.row_idx, "status", "HARD_REJECT")
            set_cell(d.row_idx, "publish_window", "")
            set_cell(d.row_idx, "score", 0)
            if has_scored_ts:
                set_cell(d.row_idx, "scored_timestamp", now_iso)
            hard_ct += 1
            continue

        # default score if not in promotable set
        score_val = 50.0
        if d.deal_id in winner_ids:
            # winner score
            # look up computed score from scored_items
            for row_obj, sc in winners:
                if row_obj.deal_id == d.deal_id:
                    score_val = sc
                    break
            set_cell(d.row_idx, "status", "READY_TO_POST")
            set_cell(d.row_idx, "publish_window", slot)
            set_cell(d.row_idx, "score", round(score_val, 2))
            if has_scored_ts:
                set_cell(d.row_idx, "scored_timestamp", now_iso)
            publish_ct += 1
        else:
            # non-winners become SCORED (even if off-theme); score is informative only
            if d.theme != theme_today:
                off_theme_ct += 1
            if d.price_gbp is not None:
                # cheapness relative to nothing => keep stable mid-score with small price influence
                score_val = max(1.0, min(99.0, 60.0 - (d.price_gbp / 50.0)))
            if d.stops is not None:
                score_val -= float(d.stops) * 3.0

            set_cell(d.row_idx, "status", "SCORED")
            set_cell(d.row_idx, "publish_window", "")
            set_cell(d.row_idx, "score", round(score_val, 2))
            if has_scored_ts:
                set_cell(d.row_idx, "scored_timestamp", now_iso)
            scored_ct += 1

    if updates:
        ws_raw.update_cells(updates, value_input_option="USER_ENTERED")

    log(
        f"✅ Status writes: READY_TO_POST={publish_ct} SCORED={scored_ct} "
        f"HARD_REJECT={hard_ct} off_theme_scored={off_theme_ct}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        log(f"❌ ERROR: {e}")
        raise
