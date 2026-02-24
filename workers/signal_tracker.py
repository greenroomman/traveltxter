#!/usr/bin/env python3
"""
workers/signal_tracker.py
TRAVELTXTTER V5 — SIGNAL TRACKER

Oilpan contract:
  Reads:  RAW_DEALS (destination_iata, score, price_gbp, status, theme, window_type)
  Writes: Supabase signal_history ONLY
  Never:  modifies RAW_DEALS, RDV, or any sheet
  Is:     stateless, idempotent, stop-on-failure

Runs after: ai_scorer.py
Runs before: enrich_router.py

Purpose:
  For each scored destination, compare current score against the last
  recorded score in signal_history. If the delta is meaningful (>= threshold)
  or no prior record exists, write a new signal_history row to Supabase.
  This powers the Travelr Signals page with real movement data over time.
"""

from __future__ import annotations

import os
import json
import datetime as dt
from typing import Any, Dict, List, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials


# ─────────────────────────────────────────
# Config
# ─────────────────────────────────────────

SCORE_DELTA_THRESHOLD = int(os.getenv("SIGNAL_DELTA_THRESHOLD", "10"))
SCORED_STATUSES = {"SCORED", "READY_TO_POST", "READY_TO_PUBLISH", "POSTED_INSTAGRAM", "VIP_DONE", "POSTED_ALL"}


# ─────────────────────────────────────────
# Env helpers
# ─────────────────────────────────────────

def env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


# ─────────────────────────────────────────
# GSpread auth
# ─────────────────────────────────────────

def _sanitize_sa_json(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        raise RuntimeError("Missing GCP service account JSON.")
    try:
        json.loads(raw)
        return raw
    except Exception:
        pass
    try:
        fixed = raw.replace("\\n", "\n")
        json.loads(fixed)
        return fixed
    except Exception:
        pass
    raw2 = raw.replace("\r", "")
    json.loads(raw2)
    return raw2


def gspread_client() -> gspread.Client:
    raw = os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON") or ""
    raw = _sanitize_sa_json(raw)
    info = json.loads(raw)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


# ─────────────────────────────────────────
# Supabase client
# ─────────────────────────────────────────

class SupabaseClient:
    def __init__(self, url: str, service_key: str):
        self.url = url.rstrip("/")
        self.headers = {
            "apikey": service_key,
            "Authorization": f"Bearer {service_key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        }

    def select(self, table: str, query: str = "") -> List[Dict]:
        url = f"{self.url}/rest/v1/{table}?{query}"
        resp = requests.get(url, headers=self.headers, timeout=15)
        resp.raise_for_status()
        return resp.json()

    def insert(self, table: str, rows: List[Dict]) -> None:
        if not rows:
            return
        url = f"{self.url}/rest/v1/{table}"
        resp = requests.post(url, headers=self.headers, json=rows, timeout=15)
        resp.raise_for_status()


# ─────────────────────────────────────────
# Read RAW_DEALS
# ─────────────────────────────────────────

def load_scored_deals(ws_raw: gspread.Worksheet) -> List[Dict]:
    """
    Returns one row per destination_iata — the highest-scored deal per destination.
    Only includes rows with a valid score and a scored status.
    """
    all_rows = ws_raw.get_all_records()
    best: Dict[str, Dict] = {}

    for row in all_rows:
        status = str(row.get("status", "")).strip().upper()
        if status not in SCORED_STATUSES:
            continue

        dest = str(row.get("destination_iata", "")).strip().upper()
        if not dest:
            continue

        try:
            score = int(float(str(row.get("score", "") or "0")))
        except (ValueError, TypeError):
            continue

        if score <= 0:
            continue

        if dest not in best or score > best[dest]["score"]:
            best[dest] = {
                "destination_iata": dest,
                "city":             str(row.get("destination_city", "") or row.get("destination_iata", "")).strip(),
                "country":          str(row.get("destination_country", "")).strip(),
                "theme":            str(row.get("theme", "")).strip(),
                "window_type":      str(row.get("publish_window", "")).strip().upper() or "PM",
                "score":            score,
                "price_gbp":        _parse_price(row.get("price_gbp")),
                "deal_id":          str(row.get("deal_id", "")).strip(),
                "tagline":          str(row.get("phrase_used", "")).strip(),
            }

    return list(best.values())


def _parse_price(val: Any) -> Optional[float]:
    try:
        f = float(str(val or "").replace("£", "").strip())
        return f if f > 0 else None
    except (ValueError, TypeError):
        return None


# ─────────────────────────────────────────
# Read signal_history from Supabase
# ─────────────────────────────────────────

def load_last_signals(supabase: SupabaseClient, iata_codes: List[str]) -> Dict[str, Dict]:
    """
    Returns {iata: latest_signal_history_row} for the given IATA codes.
    Uses one query per IATA to get the most recent record.
    """
    last: Dict[str, Dict] = {}
    for iata in iata_codes:
        try:
            rows = supabase.select(
                "signal_history",
                f"destination_iata=eq.{iata}&order=recorded_at.desc&limit=1"
            )
            if rows:
                last[iata] = rows[0]
        except Exception as e:
            print(f"   ⚠️  Could not fetch history for {iata}: {e}")
    return last


# ─────────────────────────────────────────
# Build signal rows
# ─────────────────────────────────────────

def build_signal_rows(
    deals: List[Dict],
    history: Dict[str, Dict],
    threshold: int,
) -> List[Dict]:
    """
    For each deal, determine if a new signal_history row should be written.
    Rules:
      - No prior record → always write (baseline)
      - score delta >= threshold → write (meaningful movement)
      - price delta >= £20 with any score change → write
      - Otherwise skip (noise)
    """
    to_insert: List[Dict] = []
    now_iso = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    for deal in deals:
        iata = deal["destination_iata"]
        score_now = deal["score"]
        price_now = deal["price_gbp"]
        prior = history.get(iata)

        if prior is None:
            # Baseline — first time we've seen this destination
            score_prev = None
            price_prev = None
            score_delta = 0
            price_delta = None
            direction = "stable"
            reason = "baseline"
        else:
            score_prev = prior.get("score_current")
            price_prev = prior.get("price_current")

            try:
                score_delta = score_now - int(score_prev or 0)
            except (TypeError, ValueError):
                score_delta = 0

            try:
                price_delta = round(float(price_now or 0) - float(price_prev or 0), 2) if (price_now and price_prev) else None
            except (TypeError, ValueError):
                price_delta = None

            # Direction based on score
            if score_delta >= threshold:
                direction = "up"
            elif score_delta <= -threshold:
                direction = "down"
            else:
                direction = "stable"

            # Decide whether to write
            meaningful_score = abs(score_delta) >= threshold
            meaningful_price = price_delta is not None and abs(price_delta) >= 20

            if not meaningful_score and not meaningful_price:
                print(f"   ⏭️  {iata}: delta={score_delta:+d} pts — below threshold, skip")
                continue

            reason = f"delta={score_delta:+d}pts"

        row = {
            "destination_iata": iata,
            "city":             deal.get("city") or iata,
            "country":          deal.get("country", ""),
            "theme":            deal.get("theme", ""),
            "score_current":    score_now,
            "score_previous":   score_prev,
            "score_delta":      score_delta,
            "price_current":    price_now,
            "price_previous":   price_prev,
            "price_delta":      price_delta,
            "direction":        direction,
            "window_type":      deal.get("window_type", "PM"),
            "deal_id":          deal.get("deal_id", ""),
            "tagline":          deal.get("tagline", ""),
            "recorded_at":      now_iso,
        }
        to_insert.append(row)
        print(f"   ✅ {iata}: score={score_now} ({reason}) direction={direction}")

    return to_insert


# ─────────────────────────────────────────
# Main
# ─────────────────────────────────────────

def main() -> int:
    print("=" * 70)
    print("TRAVELTXTTER V5 — SIGNAL TRACKER")
    print("=" * 70)

    supabase_url = env_str("SUPABASE_URL")
    supabase_key = env_str("SUPABASE_SERVICE_ROLE_KEY")
    spreadsheet_id = env_str("SPREADSHEET_ID") or env_str("SHEET_ID")
    raw_tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")

    if not supabase_url or not supabase_key:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.")
    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID.")

    # Connect
    supabase = SupabaseClient(supabase_url, supabase_key)
    gc = gspread_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws_raw = sh.worksheet(raw_tab)

    # Step 1: Load scored deals from RAW_DEALS
    print(f"\n📋 Loading scored deals from {raw_tab}...")
    deals = load_scored_deals(ws_raw)
    print(f"   Found {len(deals)} scored destinations")

    if not deals:
        print("⚠️  No scored deals found. Nothing to track.")
        return 0

    # Step 2: Load prior signal history from Supabase
    iata_codes = [d["destination_iata"] for d in deals]
    print(f"\n📡 Loading signal history from Supabase for {len(iata_codes)} destinations...")
    history = load_last_signals(supabase, iata_codes)
    print(f"   Found prior records for {len(history)}/{len(iata_codes)} destinations")

    # Step 3: Determine what to write
    print(f"\n🔍 Evaluating signals (threshold: {SCORE_DELTA_THRESHOLD} pts)...")
    to_insert = build_signal_rows(deals, history, SCORE_DELTA_THRESHOLD)

    # Step 4: Write to Supabase
    print(f"\n💾 Writing {len(to_insert)} signal row(s) to Supabase...")
    if to_insert:
        supabase.insert("signal_history", to_insert)
        print(f"✅ Inserted {len(to_insert)} signal row(s).")
    else:
        print("⚠️  No meaningful changes detected. Nothing written.")

    print("\n" + "=" * 70)
    print(f"SIGNAL TRACKER COMPLETE — {len(to_insert)} rows written")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
