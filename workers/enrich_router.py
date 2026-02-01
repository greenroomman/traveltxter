#!/usr/bin/env python3
# workers/enrich_router.py
"""
TravelTxter Enrichment Router — V5 (Deterministic backfill for cities/countries + phrase selection)

Purpose (V5-compliant):
- Fill missing origin/destination city + country in RAW_DEALS from IATA_MASTER
- Fill missing RAW_DEALS.phrase_bank from PHRASE_BANK (approved phrases only)
- Operate on SCORED + READY_* rows (and safe to re-run)
- Stateless + deterministic: same row inputs -> same outputs
- Writes ONLY enrichment columns and ONLY if blank (never overwrites)

Hard rules:
- Never writes to RAW_DEALS_VIEW
- Never changes status
- Never scores
- Never generates free-form copy (phrases are human-approved only)
"""

from __future__ import annotations

import os
import json
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Optional

import gspread
from google.oauth2.service_account import Credentials


# -------------------- ENV --------------------

SPREADSHEET_ID = (os.environ.get("SPREADSHEET_ID") or os.environ.get("SHEET_ID") or "").strip()
GCP_SA_JSON_ONE_LINE = (os.environ.get("GCP_SA_JSON_ONE_LINE") or os.environ.get("GCP_SA_JSON") or "").strip()

RAW_DEALS_TAB = (os.environ.get("RAW_DEALS_TAB", "RAW_DEALS") or "RAW_DEALS").strip() or "RAW_DEALS"
PHRASE_BANK_TAB = (os.environ.get("PHRASE_BANK_TAB", "PHRASE_BANK") or "PHRASE_BANK").strip() or "PHRASE_BANK"
IATA_MASTER_TAB = (os.environ.get("IATA_MASTER_TAB", "IATA_MASTER") or "IATA_MASTER").strip() or "IATA_MASTER"

# Which channel are we selecting phrases for? (used against PHRASE_BANK.channel_hint)
# Typical values: "vip", "ig", "free", "pro"
PHRASE_CHANNEL = (os.environ.get("PHRASE_CHANNEL", "vip") or "vip").strip().lower()

# How many RAW_DEALS rows to enrich per run (caps runtime / API)
MAX_ROWS_PER_RUN = int((os.environ.get("ENRICH_MAX_ROWS_PER_RUN", "60") or "60").strip() or "60")

# If true, enforce per-phrase monthly caps using ingested_at_utc (if available).
ENFORCE_MAX_PER_MONTH = (os.environ.get("ENFORCE_MAX_PER_MONTH", "true") or "true").strip().lower() == "true"

# If true, require phrase selection for eligible rows; otherwise only fill if blank and candidates exist.
REQUIRE_PHRASE = (os.environ.get("REQUIRE_PHRASE", "false") or "false").strip().lower() == "true"


# -------------------- LOGGING --------------------

def _log(msg: str) -> None:
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"{ts} | {msg}", flush=True)


# -------------------- GOOGLE SHEETS --------------------

def _parse_sa_json(raw: str) -> dict:
    raw = (raw or "").strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return json.loads(raw.replace("\\n", "\n"))


def _gs_client() -> gspread.Client:
    if not GCP_SA_JSON_ONE_LINE:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE / GCP_SA_JSON")
    info = _parse_sa_json(GCP_SA_JSON_ONE_LINE)
    creds = Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


def _headers(ws) -> List[str]:
    return [str(h).strip() for h in ws.row_values(1)]


def _colmap(headers: List[str]) -> Dict[str, int]:
    return {h: i + 1 for i, h in enumerate(headers) if h}


def _s(v: Any) -> str:
    return str(v or "").strip()


def _upper(v: Any) -> str:
    return _s(v).upper()


def _lower(v: Any) -> str:
    return _s(v).lower()


def _truthy(v: Any) -> bool:
    x = _lower(v)
    return x in {"true", "1", "yes", "y", "approved"}


def _safe_int(v: Any) -> Optional[int]:
    try:
        vv = _s(v)
        if vv == "":
            return None
        return int(float(vv))
    except Exception:
        return None


def _batch_write(ws, updates: List[Tuple[int, Dict[str, Any]]], cm: Dict[str, int]) -> int:
    cells: List[gspread.cell.Cell] = []
    for row_num, payload in updates:
        for header, value in payload.items():
            if header not in cm:
                continue
            cells.append(gspread.cell.Cell(row=row_num, col=cm[header], value=value))
    if not cells:
        return 0
    ws.update_cells(cells, value_input_option="RAW")
    return len(cells)


# -------------------- SCHEMA VALIDATION --------------------

RAW_REQUIRED = [
    "status",
    "deal_id",
    "origin_iata",
    "destination_iata",
    "theme",
    "phrase_bank",
]

# These are the enrichment targets. We only write them if the columns exist.
RAW_ENRICH_COLS = [
    "origin_city",
    "origin_country",
    "destination_city",
    "destination_country",
    "phrase_bank",
]

# Eligibility: should do all incl SCORED
ELIGIBLE_STATUSES = {"SCORED", "READY_TO_POST", "READY_TO_PUBLISH"}


def _require_headers(cm: Dict[str, int], required: List[str], tabname: str) -> None:
    missing = [h for h in required if h not in cm]
    if missing:
        raise RuntimeError(f"{tabname} missing required columns: {missing}")


# -------------------- IATA MASTER LOOKUP --------------------

def _load_iata_map(iata_ws) -> Dict[str, Tuple[str, str]]:
    """
    IATA_MASTER headers provided:
      iata_code, city, country
    """
    headers = _headers(iata_ws)
    cm = _colmap(headers)
    _require_headers(cm, ["iata_code", "city", "country"], "IATA_MASTER")

    rows = iata_ws.get_all_records()
    out: Dict[str, Tuple[str, str]] = {}

    for r in rows:
        code = _upper(r.get("iata_code"))
        city = _s(r.get("city"))
        country = _s(r.get("country"))
        if code and (city or country):
            out[code] = (city, country)
    return out


# -------------------- PHRASE BANK --------------------

def _load_phrase_bank(phrase_ws) -> List[Dict[str, Any]]:
    """
    PHRASE_BANK headers provided:
      destination_iata, theme, category, phrase, approved, channel_hint, max_per_month, notes, context_hint
    """
    headers = _headers(phrase_ws)
    cm = _colmap(headers)
    _require_headers(
        cm,
        ["destination_iata", "theme", "category", "phrase", "approved", "channel_hint", "max_per_month"],
        "PHRASE_BANK",
    )

    rows = phrase_ws.get_all_records()
    clean: List[Dict[str, Any]] = []

    for r in rows:
        phrase = _s(r.get("phrase"))
        if not phrase:
            continue
        if not _truthy(r.get("approved")):
            continue

        clean.append(
            {
                "destination_iata": _upper(r.get("destination_iata")),
                "theme": _lower(r.get("theme")),
                "category": _lower(r.get("category")),
                "phrase": phrase,
                "channel_hint": _lower(r.get("channel_hint")),
                "max_per_month": _safe_int(r.get("max_per_month")),
                "context_hint": _s(r.get("context_hint")),
            }
        )

    # stable sort for deterministic selection (no randomness)
    clean.sort(key=lambda x: (x["destination_iata"], x["theme"], x["category"], x["phrase"]))
    return clean


def _channel_allows(channel_hint: str, target_channel: str) -> bool:
    """
    channel_hint is a free text field; we treat it as "contains token".
    Empty channel_hint means "allowed everywhere".
    """
    ch = (channel_hint or "").strip().lower()
    if not ch:
        return True
    return target_channel in ch


def _hash_mod(key: str, n: int) -> int:
    h = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return int(h[:12], 16) % n


def _month_key(dt: datetime) -> str:
    return dt.strftime("%Y-%m")


def _parse_ingested_month(raw: str) -> Optional[str]:
    """
    Tries to parse ingested_at_utc-like values:
    - ISO strings e.g. 2026-02-01T16:55:53Z
    - 'YYYY-MM-DD HH:MM:SS'
    Returns YYYY-MM or None if unknown.
    """
    s = _s(raw)
    if not s:
        return None

    # common ISO forms
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            return _month_key(dt)
        except Exception:
            continue

    # best-effort: take YYYY-MM if present
    if len(s) >= 7 and s[4] == "-" and s[7 - 1] == "-":
        return s[:7]
    return None


def _build_phrase_usage_index(raw_records: List[Dict[str, Any]], has_ingested: bool) -> Dict[Tuple[str, str], int]:
    """
    Returns usage count by (phrase, YYYY-MM) using RAW_DEALS.phrase_bank.
    If ingested timestamp not available, returns empty (caps cannot be enforced reliably).
    """
    if not has_ingested:
        return {}

    idx: Dict[Tuple[str, str], int] = {}
    for r in raw_records:
        phrase = _s(r.get("phrase_bank"))
        if not phrase:
            continue
        mk = _parse_ingested_month(r.get("ingested_at_utc"))
        if not mk:
            continue
        key = (phrase, mk)
        idx[key] = idx.get(key, 0) + 1
    return idx


def _select_phrase(
    candidates: List[Dict[str, Any]],
    deal_id: str,
    dest_iata: str,
    theme: str,
    usage_idx: Dict[Tuple[str, str], int],
    this_month: str,
    target_channel: str,
) -> str:
    """
    Deterministic phrase selection:
    1) filter approved candidates for channel
    2) prioritize destination_iata match, then theme match
    3) enforce max_per_month if possible
    4) pick stable by hash(deal_id)
    """
    dest = _upper(dest_iata)
    th = _lower(theme)

    # Step 1: channel filter
    pool = [p for p in candidates if _channel_allows(p.get("channel_hint", ""), target_channel)]

    # Step 2: destination_iata preference
    exact_dest = [p for p in pool if p["destination_iata"] == dest]
    any_dest = [p for p in pool if p["destination_iata"] == ""]
    pool2 = exact_dest + any_dest
    if not pool2:
        pool2 = pool  # last fallback: ignore destination constraint

    # Step 3: theme preference
    exact_theme = [p for p in pool2 if p["theme"] == th]
    any_theme = [p for p in pool2 if p["theme"] in {"", "any", "all"}]
    pool3 = exact_theme + any_theme
    if not pool3:
        pool3 = pool2  # last fallback: ignore theme constraint

    # Step 4: enforce max_per_month if we can
    usable: List[Dict[str, Any]] = []
    for p in pool3:
        mx = p.get("max_per_month")
        if not ENFORCE_MAX_PER_MONTH or mx is None:
            usable.append(p)
            continue
        used = usage_idx.get((p["phrase"], this_month), 0)
        if used < mx:
            usable.append(p)

    if not usable:
        return ""

    idx = _hash_mod(deal_id or f"{dest}:{th}", len(usable))
    return _s(usable[idx].get("phrase"))


# -------------------- MAIN --------------------

def main() -> int:
    if not SPREADSHEET_ID:
        raise RuntimeError("Missing SPREADSHEET_ID / SHEET_ID")

    _log("============================================================")
    _log("🧩 TravelTxter Enrichment Router — V5 (IATA backfill + Phrase Bank selection)")
    _log("============================================================")
    _log(f"RAW_DEALS_TAB={RAW_DEALS_TAB} | PHRASE_BANK_TAB={PHRASE_BANK_TAB} | IATA_MASTER_TAB={IATA_MASTER_TAB}")
    _log(f"PHRASE_CHANNEL={PHRASE_CHANNEL} | MAX_ROWS_PER_RUN={MAX_ROWS_PER_RUN}")
    _log(f"ENFORCE_MAX_PER_MONTH={ENFORCE_MAX_PER_MONTH} | REQUIRE_PHRASE={REQUIRE_PHRASE}")
    _log(f"ELIGIBLE_STATUSES={sorted(list(ELIGIBLE_STATUSES))}")

    gc = _gs_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    raw_ws = sh.worksheet(RAW_DEALS_TAB)
    phrase_ws = sh.worksheet(PHRASE_BANK_TAB)
    iata_ws = sh.worksheet(IATA_MASTER_TAB)

    raw_headers = _headers(raw_ws)
    raw_cm = _colmap(raw_headers)
    _require_headers(raw_cm, RAW_REQUIRED, "RAW_DEALS")

    # Optional columns
    has_ingested = "ingested_at_utc" in raw_cm
    has_origin_city = "origin_city" in raw_cm
    has_origin_country = "origin_country" in raw_cm
    has_dest_city = "destination_city" in raw_cm
    has_dest_country = "destination_country" in raw_cm

    # Load external maps
    iata_map = _load_iata_map(iata_ws)
    _log(f"✅ IATA_MASTER loaded: {len(iata_map)} entries")

    phrase_candidates = _load_phrase_bank(phrase_ws)
    _log(f"✅ PHRASE_BANK loaded (approved): {len(phrase_candidates)} phrases")

    # Load RAW_DEALS records
    raw_records = raw_ws.get_all_records()
    _log(f"✅ RAW_DEALS loaded: {len(raw_records)} rows")

    # Build phrase usage index (optional)
    usage_idx: Dict[Tuple[str, str], int] = {}
    if ENFORCE_MAX_PER_MONTH:
        if has_ingested:
            usage_idx = _build_phrase_usage_index(raw_records, has_ingested=True)
            _log(f"✅ Phrase usage index built (month-aware): {len(usage_idx)} keys")
        else:
            _log("⚠️ ingested_at_utc not found; cannot enforce max_per_month reliably. Caps will be best-effort only.")
            usage_idx = {}

    this_month = datetime.utcnow().strftime("%Y-%m")

    updates: List[Tuple[int, Dict[str, Any]]] = []

    scanned = 0
    eligible = 0
    enriched_rows = 0
    city_fills = 0
    phrase_fills = 0
    missing_iata_lookups = 0
    phrase_misses = 0

    for i, r in enumerate(raw_records, start=2):
        if len(updates) >= MAX_ROWS_PER_RUN:
            break

        scanned += 1

        status = _upper(r.get("status"))
        if status not in ELIGIBLE_STATUSES:
            continue

        eligible += 1

        deal_id = _s(r.get("deal_id"))
        o = _upper(r.get("origin_iata"))
        d = _upper(r.get("destination_iata"))
        theme = _lower(r.get("theme"))

        if not (deal_id and o and d and theme):
            continue

        payload: Dict[str, Any] = {}

        # ---- City/Country enrichment ----
        if (has_origin_city or has_origin_country) and o:
            ocity, ocountry = iata_map.get(o, ("", ""))
            if not ocity and not ocountry:
                missing_iata_lookups += 1
            if has_origin_city and not _s(r.get("origin_city")) and ocity:
                payload["origin_city"] = ocity
                city_fills += 1
            if has_origin_country and not _s(r.get("origin_country")) and ocountry:
                payload["origin_country"] = ocountry
                city_fills += 1

        if (has_dest_city or has_dest_country) and d:
            dcity, dcountry = iata_map.get(d, ("", ""))
            if not dcity and not dcountry:
                missing_iata_lookups += 1
            if has_dest_city and not _s(r.get("destination_city")) and dcity:
                payload["destination_city"] = dcity
                city_fills += 1
            if has_dest_country and not _s(r.get("destination_country")) and dcountry:
                payload["destination_country"] = dcountry
                city_fills += 1

        # ---- Phrase enrichment ----
        current_phrase = _s(r.get("phrase_bank"))
        if not current_phrase:
            phrase = _select_phrase(
                candidates=phrase_candidates,
                deal_id=deal_id,
                dest_iata=d,
                theme=theme,
                usage_idx=usage_idx,
                this_month=this_month,
                target_channel=PHRASE_CHANNEL,
            )

            if phrase:
                payload["phrase_bank"] = phrase
                phrase_fills += 1
            else:
                phrase_misses += 1
                if REQUIRE_PHRASE:
                    # Don't write anything; leave blank. This worker does not hard-fail unless you choose to add that policy.
                    pass

        if payload:
            updates.append((i, payload))
            enriched_rows += 1

    written_cells = _batch_write(raw_ws, updates, raw_cm)

    _log("------------------------------------------------------------")
    _log(f"Scanned rows: {scanned}")
    _log(f"Eligible rows (SCORED/READY_*): {eligible}")
    _log(f"Rows enriched: {enriched_rows} (cap {MAX_ROWS_PER_RUN})")
    _log(f"City/Country fills: {city_fills}")
    _log(f"Phrase fills: {phrase_fills} (channel={PHRASE_CHANNEL})")
    _log(f"Phrase misses (no candidate after governance): {phrase_misses}")
    _log(f"Missing IATA lookups encountered: {missing_iata_lookups}")
    _log(f"Cells written (batch): {written_cells}")
    _log("------------------------------------------------------------")
    _log("✅ Enrichment Router complete (idempotent; safe to re-run)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
