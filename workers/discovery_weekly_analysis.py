#!/usr/bin/env python3
"""
TravelTxter V4.5x — Weekly Discovery Analysis

ROLE:
- Read DISCOVERY_BANK
- Aggregate weekly patterns
- Write DISCOVERY_WEEKLY_REPORT
- NEVER mutates CONFIG / THEMES / RAW_DEALS

This is analysis-only, human-in-the-loop intelligence.
"""

from __future__ import annotations

import os
import json
import statistics
import datetime as dt
from collections import defaultdict
from typing import Dict, List

import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# Logging
# ============================================================

def log(msg: str):
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    print(f"{ts} | {msg}", flush=True)


# ============================================================
# Auth / Sheets
# ============================================================

def get_client():
    raw = os.environ["GCP_SA_JSON_ONE_LINE"]
    info = json.loads(raw.replace("\\n", "\n"))
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return gspread.authorize(creds)


def get_ws(title: str):
    gc = get_client()
    sh = gc.open_by_key(os.environ["SPREADSHEET_ID"])
    return sh.worksheet(title)


# ============================================================
# Helpers
# ============================================================

def monday_of_week(d: dt.date) -> dt.date:
    return d - dt.timedelta(days=d.weekday())


def parse_date(s: str) -> dt.date:
    return dt.datetime.fromisoformat(s[:10]).date()


def confidence_from_score(score: int) -> str:
    if score > 10:
        return "HIGH"
    if score >= 6:
        return "MEDIUM"
    return "LOW"


# ============================================================
# Analysis
# ============================================================

def main():
    disc_ws = get_ws("DISCOVERY_BANK")

    try:
        report_ws = get_ws("DISCOVERY_WEEKLY_REPORT")
        report_ws.clear()
    except Exception:
        report_ws = disc_ws.spreadsheet.add_worksheet(
            title="DISCOVERY_WEEKLY_REPORT",
            rows=500,
            cols=20
        )

    DISC_HEADERS = disc_ws.row_values(1)
    rows = disc_ws.get_all_records()

    if not rows:
        log("No discovery data found.")
        return

    # -------- filter to last full week --------
    today = dt.datetime.utcnow().date()
    last_monday = monday_of_week(today) - dt.timedelta(days=7)
    week_start = last_monday.isoformat()

    weekly = [
        r for r in rows
        if parse_date(r["found_at_utc"]) >= last_monday
    ]

    if not weekly:
        log("No discovery rows for last week.")
        return

    # -------- aggregate by destination --------
    by_dest: Dict[str, List[dict]] = defaultdict(list)
    for r in weekly:
        by_dest[r["destination_iata"]].append(r)

    insights = []

    for dest, items in by_dest.items():
        prices = [float(r["price"]) for r in items if r["price"]]
        origins = {r["origin_iata"] for r in items}
        themes = {r["raw_theme_guess"] for r in items}
        flags = {r["reason_flag"] for r in items}
        country = items[0]["destination_country"]

        count = len(items)
        score = count + len(origins)

        if count < 3:
            continue

        median_price = round(statistics.median(prices), 2)
        price_range = f"{min(prices)}–{max(prices)}"

        # ---------- insight types ----------
        if "outside_config" in flags:
            insight_type = "REPEATED_DESTINATION_OUTSIDE_CONFIG"
            recommendation = f"Consider adding {dest} ({country}) to a theme."
        elif "non_gbp" in flags:
            insight_type = "CURRENCY_FILTER_SIGNAL"
            recommendation = f"{dest} often cheap but priced in non-GBP currency."
        else:
            insight_type = "CONSISTENT_LOW_PRICE_DESTINATION"
            recommendation = f"{dest} shows consistent low pricing."

        insights.append([
            week_start,
            insight_type,
            "destination",
            dest,
            count,
            median_price,
            price_range,
            len(origins),
            ", ".join(sorted(themes)),
            ", ".join(sorted(flags)),
            recommendation,
            confidence_from_score(score),
            "",
        ])

    # -------- write report --------
    HEADERS = [
        "week_start_utc",
        "insight_type",
        "entity_type",
        "entity_value",
        "evidence_count",
        "median_price",
        "price_range",
        "origin_coverage",
        "themes_seen",
        "reason_flags",
        "recommendation",
        "confidence",
        "notes",
    ]

    report_ws.update([HEADERS], "A1")
    if insights:
        report_ws.update(insights, "A2")

    log(f"Discovery weekly report written ({len(insights)} insights).")


if __name__ == "__main__":
    main()
