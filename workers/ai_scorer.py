#!/usr/bin/env python3
"""
TravelTxter V4.5x — ai_scorer.py (Deterministic + Discovery Back-prop) — METADATA SAFETY NET

Fix:
- If a row is NEW but destination_city is IATA-ish or destination_country is blank,
  bank it immediately (status=BANKED) so it cannot reach publish.

Scoring logic remains deterministic and unchanged for valid rows.
"""

from __future__ import annotations

import os
import json
import time
import datetime as dt
import re
from typing import Dict, Any, List, Tuple, Optional

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError


IATA_RE = re.compile(r"^[A-Z]{3}$")


def log(msg: str) -> None:
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


def env_str(k: str, default: str = "") -> str:
    return (os.environ.get(k, default) or "").strip()

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


def is_iata3(s: str) -> bool:
    return bool(IATA_RE.match((s or "").strip().upper()))


def compute_base_score(price_gbp: Optional[float], stops: Optional[int], days_ahead: Optional[int]) -> float:
    score = 0.0

    if price_gbp is None:
        score -= 40.0
    else:
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

    if stops is None:
        score -= 5
    else:
        if stops == 0:
            score += 12
        elif stops == 1:
            score += 6
        else:
            score -= 4 * (stops - 1)

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

    # Ensure columns we might write
    for c in ["deal_score", "dest_variety_score", "theme_variety_score", "scored_timestamp", "banked_utc", "reason_banked"]:
        if c not in h:
            headers.append(c)
            h[c] = len(headers) - 1
    ws.update([headers], "A1")

    # Reload after header update
    values = ws.get_all_values()
    rows = values[1:]

    required = ["status", "deal_id", "price_gbp", "destination_iata", "destination_city", "destination_country", "outbound_date", "return_date", "stops", "deal_theme"]
    hh = {name: i for i, name in enumerate([x.strip() for x in values[0]])}
    missing = [c for c in required if c not in hh]
    if missing:
        raise RuntimeError(f"RAW_DEALS missing required columns: {missing}")

    # Build recent destination set for variety penalty
    lookback_cutoff = dt.datetime.utcnow() - dt.timedelta(hours=VARIETY_LOOKBACK_HOURS)
    recent_dests = set()
    for r in rows:
        ts_val = r[hh["scored_timestamp"]] if hh["scored_timestamp"] < len(r) else ""
        dest_val = r[hh["destination_iata"]] if hh["destination_iata"] < len(r) else ""
        if not ts_val or not dest_val:
            continue
        try:
            t = dt.datetime.fromisoformat(ts_val.replace("Z", ""))
            if t >= lookback_cutoff:
                recent_dests.add(dest_val.strip().upper())
        except Exception:
            continue

    # Collect NEW rows
    new_rows: List[Tuple[int, List[str]]] = []
    for idx, r in enumerate(rows, start=2):
        status = (r[hh["status"]] if hh["status"] < len(r) else "").strip()
        if status == "NEW":
            new_rows.append((idx, r))

    if not new_rows:
        log("No NEW rows to score.")
        return 0

    log(f"Scoring NEW rows: {len(new_rows)}")

    scored: List[Tuple[float, int]] = []

    for sheet_row, r in new_rows:
        dest_city = (r[hh["destination_city"]] if hh["destination_city"] < len(r) else "").strip()
        dest_country = (r[hh["destination_country"]] if hh["destination_country"] < len(r) else "").strip()
        dest_iata = (r[hh["destination_iata"]] if hh["destination_iata"] < len(r) else "").strip().upper()

        # METADATA SAFETY NET: bank if missing / IATA masquerading as city
        if (not dest_country) or (not dest_city) or is_iata3(dest_city):
            ws.batch_update(
                [
                    {"range": a1(sheet_row, hh["status"]), "values": [["BANKED"]]},
                    {"range": a1(sheet_row, hh["banked_utc"]), "values": [[dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"]]},
                    {"range": a1(sheet_row, hh["reason_banked"]), "values": [["missing_destination_city_or_country"]]},
                ],
                value_input_option="USER_ENTERED",
            )
            log(f"🧺 Banked row {sheet_row} (dest={dest_iata}) — missing metadata")
            continue

        price = safe_float(r[hh["price_gbp"]] if hh["price_gbp"] < len(r) else "")
        stops = safe_int(r[hh["stops"]] if hh["stops"] < len(r) else "")

        days_ahead = None
        od = r[hh["outbound_date"]] if hh["outbound_date"] < len(r) else ""
        if od:
            try:
                d = dt.datetime.fromisoformat(od[:10]).date()
                days_ahead = (d - dt.datetime.utcnow().date()).days
            except Exception:
                days_ahead = None

        base = compute_base_score(price, stops, days_ahead)

        variety_pen = 0.0
        if dest_iata and dest_iata in recent_dests:
            variety_pen = -abs(DEST_REPEAT_PENALTY)

        theme_variety = 2.0 if (r[hh["deal_theme"]] if hh["deal_theme"] < len(r) else "").strip() else 0.0
        final_score = base + theme_variety + variety_pen

        ws.batch_update(
            [
                {"range": a1(sheet_row, hh["deal_score"]), "values": [[str(round(final_score, 2))]]},
                {"range": a1(sheet_row, hh["dest_variety_score"]), "values": [[str(round(variety_pen, 2))]]},
                {"range": a1(sheet_row, hh["theme_variety_score"]), "values": [[str(round(theme_variety, 2))]]},
                {"range": a1(sheet_row, hh["scored_timestamp"]), "values": [[dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"]]},
            ],
            value_input_option="USER_ENTERED",
        )

        scored.append((final_score, sheet_row))

    if not scored:
        log("No valid NEW rows scored (all banked).")
        return 0

    scored.sort(key=lambda x: x[0], reverse=True)
    winners = scored[:max(1, WINNERS_PER_RUN)]
    winner_rows = {r for _s, r in winners}

    for _s, sheet_row in scored:
        if sheet_row in winner_rows:
            ws.update([["READY_TO_POST"]], a1(sheet_row, hh["status"]))
        else:
            ws.update([["SCORED"]], a1(sheet_row, hh["status"]))

    log(f"✅ Winners promoted to READY_TO_POST: {len(winner_rows)} (WINNERS_PER_RUN={WINNERS_PER_RUN})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
