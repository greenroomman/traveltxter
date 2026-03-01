#!/usr/bin/env python3
"""
TRAVELTXTTER V5 — AI_SCORER (Bucket-Aware, Origin-Adjusted)

PURPOSE
- Read RAW_DEALS rows with status=NEW.
- Assign a numeric score (0–99).
- Promote up to WINNERS_PER_RUN to status=READY_TO_POST.
- Mark remaining NEW rows as SCORED.
- Optionally HARD_REJECT invalid rows.

SCORING ARCHITECTURE (v2 — bucket-aware)
- Destinations are scored within their BUCKET COHORT, not theme cohort.
- Theme is a label on the row; it does NOT gate promotable candidates.
- Bucket is inferred from CONFIG_BUCKETS by destination_iata.
- Origin tier adjustment: Tier 2/3 airports get a price allowance so
  regional fares are not penalised vs London fares for same destination.
- Winners selected per-bucket then ranked globally for coverage diversity.

ORIGIN TIER PRICE ADJUSTMENT
- Tier 1 (LHR/LGW/MAN/BHX): 1.00 — no adjustment
- Tier 2 (BRS/EDI/GLA etc.): 1.15 — 15% allowance
- Tier 3 (SOU/BOH/CWL/EXT): 1.25 — 25% allowance
Adjusted price = actual_price / tier_multiplier
Only used for scoring comparison — stored price_gbp never modified.

CONTRACT (V5)
- RAW_DEALS is the single writable truth.
- RAW_DEALS_VIEW is read-only.
- Downstream workers READ status.
- Publishers change status after posting (not scorer).
"""

from __future__ import annotations

import base64
import json
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials


# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    print(f"{ts} | {msg}", flush=True)


# ─────────────────────────────────────────────
# GCP AUTH
# ─────────────────────────────────────────────

_CONTROL_CHARS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def _clean_json_string(raw: str) -> str:
    return _CONTROL_CHARS.sub("", (raw or "").strip())


def _try_json_loads(raw: str) -> Optional[dict]:
    try:
        return json.loads(raw)
    except Exception:
        return None


def load_service_account_info() -> dict:
    raw = os.environ.get("GCP_SA_JSON_ONE_LINE") or os.environ.get("GCP_SA_JSON") or ""
    if not raw:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON env var")
    raw = _clean_json_string(raw)
    obj = _try_json_loads(raw)
    if obj:
        return obj
    obj = _try_json_loads(raw.replace("\\n", "\n"))
    if obj:
        return obj
    try:
        decoded = base64.b64decode(raw).decode("utf-8", errors="replace")
        decoded = _clean_json_string(decoded)
        obj = _try_json_loads(decoded) or _try_json_loads(decoded.replace("\\n", "\n"))
        if obj:
            return obj
    except Exception:
        pass
    obj = _try_json_loads(raw.replace("\n", ""))
    if obj:
        return obj
    raise RuntimeError("Could not parse service account JSON.")


def gspread_client() -> gspread.Client:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(load_service_account_info(), scopes=scopes)
    return gspread.authorize(creds)


# ─────────────────────────────────────────────
# SHEET UTILS
# ─────────────────────────────────────────────

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
    return {h.strip(): i for i, h in enumerate(ws.row_values(1)) if h.strip()}


def get_cell(ws: gspread.Worksheet, a1: str) -> str:
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


# ─────────────────────────────────────────────
# CONFIG_BUCKETS — destination → bucket_id
# ─────────────────────────────────────────────

def load_dest_to_bucket(sh: gspread.Spreadsheet, buckets_tab: str) -> Dict[str, int]:
    """
    Returns: destination_iata → bucket_id
    Gracefully falls back to empty dict if tab missing.
    """
    try:
        ws = sh.worksheet(buckets_tab)
    except Exception:
        log(f"WARNING: CONFIG_BUCKETS tab '{buckets_tab}' not found. Single-pool scoring active.")
        return {}

    rows = ws.get_all_records()
    mapping: Dict[str, int] = {}
    for r in rows:
        enabled = str(r.get("enabled", "")).strip().upper() in ("TRUE", "1", "YES", "Y")
        if not enabled:
            continue
        iata = str(r.get("destination_iata", "")).strip().upper()
        try:
            bid = int(r.get("bucket_id", 0) or 0)
        except Exception:
            bid = 0
        if iata and bid > 0:
            mapping[iata] = bid
    log(f"CONFIG_BUCKETS: {len(mapping)} destinations mapped.")
    return mapping


# ─────────────────────────────────────────────
# CONFIG_ORIGINS — origin → tier
# ─────────────────────────────────────────────

def load_origin_tiers(sh: gspread.Spreadsheet, origins_tab: str) -> Dict[str, int]:
    """
    Returns: airport_iata → tier (1/2/3)
    Gracefully falls back to empty dict (all treated as Tier 1).
    """
    try:
        ws = sh.worksheet(origins_tab)
    except Exception:
        log(f"WARNING: CONFIG_ORIGINS tab '{origins_tab}' not found. All origins Tier 1.")
        return {}

    rows = ws.get_all_records()
    mapping: Dict[str, int] = {}
    for r in rows:
        enabled = str(r.get("enabled", "")).strip().upper() in ("TRUE", "1", "YES", "Y")
        if not enabled:
            continue
        iata = str(r.get("airport_iata", "")).strip().upper()
        try:
            tier = int(r.get("tier", 1) or 1)
        except Exception:
            tier = 1
        if iata:
            mapping[iata] = tier
    log(f"CONFIG_ORIGINS: {len(mapping)} origins mapped to tiers.")
    return mapping


# ─────────────────────────────────────────────
# ORIGIN TIER PRICE ADJUSTMENT
# ─────────────────────────────────────────────

ORIGIN_TIER_MULTIPLIER: Dict[int, float] = {
    1: 1.00,
    2: 1.15,
    3: 1.25,
}


def adjusted_price(price_gbp: float, origin_iata: str, origin_tiers: Dict[str, int]) -> float:
    """
    Normalise price for scoring comparison only.
    Stored price_gbp is never modified.
    """
    tier = origin_tiers.get(origin_iata, 1)
    multiplier = ORIGIN_TIER_MULTIPLIER.get(tier, 1.00)
    return price_gbp / multiplier


# ─────────────────────────────────────────────
# SCORING — quartile within bucket cohort
# ─────────────────────────────────────────────

def _compute_scores_by_adjusted_price(adj_prices: List[float]) -> Dict[float, float]:
    """
    Within-cohort quartile scoring.
    Lowest adjusted price → highest score.
    Single-item cohorts receive a neutral score (60.0).
    """
    if not adj_prices:
        return {}
    if len(adj_prices) == 1:
        return {adj_prices[0]: 60.0}

    sp = sorted(adj_prices)
    n = len(sp)
    q1 = sp[max(0, int(0.25 * (n - 1)))]
    q3 = sp[max(0, int(0.75 * (n - 1)))]
    span = max(1.0, q3 - q1)

    out: Dict[float, float] = {}
    for p in adj_prices:
        z = (p - q1) / span
        score = 85.0 - (z * 30.0)
        score = max(1.0, min(99.0, score))
        out[p] = max(out.get(p, 0.0), score)
    return out


# ─────────────────────────────────────────────
# DEAL ROW
# ─────────────────────────────────────────────

@dataclass
class DealRow:
    row_idx: int
    deal_id: str
    theme: str
    status: str
    price_gbp: Optional[float]
    currency: str
    stops: Optional[int]
    origin_iata: str
    destination_iata: str
    ingested_at_utc: str
    bucket_id: int      # 0 = unmapped (fallback pool)
    adj_price: float    # origin-adjusted price for scoring only


# ─────────────────────────────────────────────
# RDV optional theme signal (preserved)
# ─────────────────────────────────────────────

def _load_rdv_dynamic_theme_index(sh: gspread.Spreadsheet, rdv_tab: str) -> Dict[str, str]:
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


# ─────────────────────────────────────────────
# ENV
# ─────────────────────────────────────────────

RAW_TAB = os.environ.get("RAW_DEALS_TAB", "RAW_DEALS")
OPS_TAB = os.environ.get("OPS_MASTER_TAB", "OPS_MASTER")
RDV_TAB = os.environ.get("RAW_DEALS_VIEW_TAB", "RAW_DEALS_VIEW")
BUCKETS_TAB = os.environ.get("FEEDER_BUCKETS_TAB", "CONFIG_BUCKETS")
ORIGINS_TAB = os.environ.get("FEEDER_ORIGINS_TAB", "CONFIG_ORIGINS")
OPS_THEME_CELL = os.environ.get("OPS_THEME_CELL", "B2")
OPS_SLOT_CELL = os.environ.get("OPS_SLOT_CELL", "A2")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main() -> int:
    log("AI_SCORER (bucket-aware) start")

    min_age_seconds = _env_int("MIN_INGEST_AGE_SECONDS", 0)
    winners_per_run = _env_int("WINNERS_PER_RUN", 2)
    winners_per_bucket = _env_int("WINNERS_PER_BUCKET", 1)

    gc = gspread_client()
    sh = open_spreadsheet(gc)

    ws_raw = open_ws(sh, RAW_TAB)
    ws_ops = open_ws(sh, OPS_TAB)

    theme_today = get_cell(ws_ops, OPS_THEME_CELL) or "DEFAULT"
    slot = (os.environ.get("RUN_SLOT") or get_cell(ws_ops, OPS_SLOT_CELL) or "PM").upper()
    if slot not in ("AM", "PM"):
        slot = "PM"

    log(f"Theme (label): {theme_today} | Slot: {slot} | winners_per_run={winners_per_run}")

    dest_to_bucket = load_dest_to_bucket(sh, BUCKETS_TAB)
    origin_tiers = load_origin_tiers(sh, ORIGINS_TAB)

    hmap = headers_map(ws_raw)
    required = ["deal_id", "status", "price_gbp", "currency", "stops", "publish_window", "score"]
    missing_cols = [h for h in required if h not in hmap]
    if missing_cols:
        raise RuntimeError(f"{RAW_TAB} missing required columns: {missing_cols}")

    has_theme_col = "theme" in hmap
    has_ingest_col = "ingested_at_utc" in hmap
    has_origin_col = "origin_iata" in hmap
    has_dest_col = "destination_iata" in hmap
    has_scored_ts = "scored_timestamp" in hmap

    rdv_theme_by_id = _load_rdv_dynamic_theme_index(sh, RDV_TAB)

    values = ws_raw.get_all_values()
    if len(values) < 2:
        log("No rows to score.")
        return 0

    now = datetime.now(timezone.utc)
    now_iso = now.isoformat(timespec="seconds").replace("+00:00", "Z")

    # ── Ingest NEW rows ──
    seen_rows = len(values) - 1
    seen_new = 0
    skipped_status = 0
    skipped_no_id = 0
    skipped_too_fresh = 0
    ingest_blank = 0
    ingest_unparseable = 0
    new_rows: List[DealRow] = []

    for sheet_i, r in enumerate(values[1:], start=2):

        def col(name: str) -> str:
            j = hmap.get(name)
            return (r[j] if (j is not None and j < len(r)) else "").strip()

        if col("status").upper() != "NEW":
            skipped_status += 1
            continue

        seen_new += 1
        deal_id = col("deal_id")
        if not deal_id:
            skipped_no_id += 1
            continue

        ing = col("ingested_at_utc") if has_ingest_col else ""
        if has_ingest_col and not ing:
            ingest_blank += 1

        if min_age_seconds > 0 and ing:
            dt_ing = _parse_iso_utc(ing)
            if not dt_ing:
                ingest_unparseable += 1
            elif (now - dt_ing).total_seconds() < float(min_age_seconds):
                skipped_too_fresh += 1
                continue
        elif has_ingest_col and ing and not _parse_iso_utc(ing):
            ingest_unparseable += 1

        theme_row = (col("theme") if has_theme_col else "").strip()
        theme_signal = (rdv_theme_by_id.get(deal_id) or "").strip()
        resolved_theme = (theme_row or theme_signal or theme_today).strip() or theme_today

        origin_iata = (col("origin_iata") if has_origin_col else "").strip().upper()
        dest_iata = (col("destination_iata") if has_dest_col else "").strip().upper()
        price = _safe_float(col("price_gbp"))
        bucket_id = dest_to_bucket.get(dest_iata, 0)
        adj = adjusted_price(price, origin_iata, origin_tiers) if price is not None else 0.0

        new_rows.append(DealRow(
            row_idx=sheet_i,
            deal_id=deal_id,
            theme=resolved_theme,
            status="NEW",
            price_gbp=price,
            currency=col("currency").upper(),
            stops=_safe_int(col("stops")),
            origin_iata=origin_iata,
            destination_iata=dest_iata,
            ingested_at_utc=ing,
            bucket_id=bucket_id,
            adj_price=adj,
        ))

    log(
        f"rows={seen_rows} NEW={seen_new} eligible={len(new_rows)} "
        f"skipped_status={skipped_status} skipped_no_id={skipped_no_id} "
        f"too_fresh={skipped_too_fresh} ingest_blank={ingest_blank}"
    )

    if not new_rows:
        log("No status changes needed.")
        return 0

    # ── Hard reject ──
    hard_reject_ids: set = set()
    candidates: List[DealRow] = []
    for d in new_rows:
        if (d.currency and d.currency != "GBP") or (d.price_gbp is None or d.price_gbp <= 0):
            hard_reject_ids.add(d.deal_id)
        else:
            candidates.append(d)

    # ── Score within each bucket ──
    by_bucket: Dict[int, List[DealRow]] = defaultdict(list)
    for d in candidates:
        by_bucket[d.bucket_id].append(d)

    log(f"Bucket distribution: { {k: len(v) for k, v in sorted(by_bucket.items())} }")

    scored_map: Dict[str, float] = {}  # deal_id → score

    for bucket_id, rows in by_bucket.items():
        adj_prices = [d.adj_price for d in rows if d.adj_price > 0]
        price_scores = _compute_scores_by_adjusted_price(adj_prices)

        for d in rows:
            base = float(price_scores.get(d.adj_price, 50.0))
            if d.stops is not None:
                base -= float(d.stops) * 5.0
            scored_map[d.deal_id] = max(1.0, min(99.0, base))

        bucket_label = f"B{bucket_id}" if bucket_id > 0 else "Unmapped"
        best = max(rows, key=lambda x: scored_map[x.deal_id])
        log(f"  {bucket_label} ({len(rows)} rows): "
            f"best={best.destination_iata}({best.origin_iata}) "
            f"adj=£{round(best.adj_price)} "
            f"score={round(scored_map[best.deal_id], 1)}")

    # ── Winner selection: best per bucket → rank globally ──
    per_bucket_top: List[Tuple[DealRow, float]] = []
    for bucket_id, rows in by_bucket.items():
        valid = sorted(rows, key=lambda x: scored_map.get(x.deal_id, 0), reverse=True)
        per_bucket_top.extend((d, scored_map[d.deal_id]) for d in valid[:winners_per_bucket])

    per_bucket_top.sort(key=lambda t: t[1], reverse=True)
    winners = per_bucket_top[:max(0, winners_per_run)]
    winner_ids = {w[0].deal_id for w in winners}

    log(f"Winners: {[w[0].destination_iata+'('+w[0].origin_iata+') £'+str(w[0].price_gbp)+' s='+str(round(w[1],1)) for w in winners]}")

    # ── Write ──
    updates: List[gspread.Cell] = []

    def set_cell(row_idx: int, header: str, value: Any) -> None:
        updates.append(gspread.Cell(row_idx, hmap[header] + 1, value))

    publish_ct = scored_ct = hard_ct = 0

    for d in new_rows:
        if d.deal_id in hard_reject_ids:
            set_cell(d.row_idx, "status", "HARD_REJECT")
            set_cell(d.row_idx, "publish_window", "")
            set_cell(d.row_idx, "score", 0)
            if has_scored_ts:
                set_cell(d.row_idx, "scored_timestamp", now_iso)
            hard_ct += 1
        elif d.deal_id in winner_ids:
            sc = scored_map.get(d.deal_id, 75.0)
            set_cell(d.row_idx, "status", "READY_TO_POST")
            set_cell(d.row_idx, "publish_window", slot)
            set_cell(d.row_idx, "score", round(sc, 2))
            if has_scored_ts:
                set_cell(d.row_idx, "scored_timestamp", now_iso)
            publish_ct += 1
        else:
            sc = scored_map.get(d.deal_id, 50.0)
            set_cell(d.row_idx, "status", "SCORED")
            set_cell(d.row_idx, "publish_window", "")
            set_cell(d.row_idx, "score", round(sc, 2))
            if has_scored_ts:
                set_cell(d.row_idx, "scored_timestamp", now_iso)
            scored_ct += 1

    if updates:
        ws_raw.update_cells(updates, value_input_option="USER_ENTERED")

    log(f"READY_TO_POST={publish_ct} SCORED={scored_ct} HARD_REJECT={hard_ct}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        log(f"ERROR: {e}")
        raise
