from __future__ import annotations

import os
import json
import time
import math
import hashlib
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
import gspread
from google.oauth2.service_account import Credentials


# ============================================================
# TRAVELTXTTER V5 — FEEDER (MINIMAL, CONFIG-DRIVEN)
# PURPOSE:
#   Insert NEW raw inventory rows into RAW_DEALS using Duffel.
#
# READS:
#   - OPS_MASTER!B2 (theme_of_the_day)
#   - CONFIG (route constraints + Pi inputs)
#   - RAW_DEALS (dedupe + caps)
#
# WRITES:
#   - RAW_DEALS: append NEW rows (status="NEW", ingested_at_utc ISO)
#
# DOES NOT:
#   - score
#   - enrich
#   - render
#   - publish
# ============================================================

DUFFEL_API = "https://api.duffel.com"


# ------------------------- ENV -------------------------
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID") or os.getenv("SHEET_ID") or ""
RAW_DEALS_TAB = os.getenv("RAW_DEALS_TAB", "RAW_DEALS")
CONFIG_TAB = os.getenv("FEEDER_CONFIG_TAB", "CONFIG")
OPS_TAB = os.getenv("OPS_MASTER_TAB", "OPS_MASTER")

DUFFEL_API_KEY = os.getenv("DUFFEL_API_KEY", "")

MAX_SEARCHES = int(os.getenv("DUFFEL_MAX_SEARCHES_PER_RUN", "12"))
MAX_INSERTS = int(os.getenv("DUFFEL_MAX_INSERTS", "50"))
DESTS_PER_RUN = int(os.getenv("DUFFEL_ROUTES_PER_RUN", "6"))

# When CONFIG has multiple eligible rows, we sample some variety
RANDOM_SEED = os.getenv("FEEDER_RANDOM_SEED", "")
SLEEP_SECONDS = float(os.getenv("FEEDER_SLEEP_SECONDS", "0.05"))

# Cabin default if CONFIG blank
DEFAULT_CABIN = os.getenv("DEFAULT_CABIN_CLASS", "economy")

# ------------------------- RAW_DEALS CONTRACT -------------------------
# Must exist as headers in row 1 of RAW_DEALS.
RAW_DEALS_HEADERS = [
    "deal_id",
    "origin_iata",
    "destination_iata",
    "origin_city",
    "destination_city",
    "destination_country",
    "outbound_date",
    "return_date",
    "price_gbp",
    "currency",
    "stops",
    "cabin_class",
    "carriers",
    "theme",
    "status",
    "publish_window",
    "score",
    "phrase_used",
    "graphic_url",
    "posted_vip_at",
    "posted_free_at",
    "posted_instagram_at",
    "ingested_at_utc",
    # NOTE: do NOT include duplicate headers like phrasse_used, etc.
]

# ------------------------- CONFIG CONTRACT -------------------------
# Your CONFIG headers (you pasted these) are supported:
# enabled,priority,origin_iata,destination_iata,days_ahead_min,days_ahead_max,
# trip_length_days,max_connections,included_airlines,cabin_class,search_weight,
# audience_type,content_priority,seasonality_boost,active_in_feeder,gateway_type,
# is_long_haul,primary_theme,slot_hint,reachability,short_stay_theme,
# long_stay_winter_theme,long_stay_summer_theme,value_score


def log(msg: str) -> None:
    print(f"{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')} | {msg}", flush=True)


def _normalize_sa_json(raw: str) -> Dict[str, Any]:
    """
    Robustly parse service account JSON from GitHub Secrets.
    Handles either:
      - one-line JSON
      - JSON with escaped newlines \\n in private_key
      - JSON with literal newlines in private_key
    """
    raw = (raw or "").strip()
    if not raw:
        raise RuntimeError("Missing service account JSON (GCP_SA_JSON or GCP_SA_JSON_ONE_LINE).")

    # Try as-is first
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    # Try replacing escaped newlines
    try:
        return json.loads(raw.replace("\\n", "\n"))
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Service account JSON decode failed: {e}") from e


def gspread_client() -> gspread.Client:
    raw = os.getenv("GCP_SA_JSON_ONE_LINE") or os.getenv("GCP_SA_JSON") or ""
    info = _normalize_sa_json(raw)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/drive.file",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def open_sheet(gc: gspread.Client) -> gspread.Spreadsheet:
    if not SPREADSHEET_ID:
        raise RuntimeError("SPREADSHEET_ID (or SHEET_ID) env var is missing.")
    return gc.open_by_key(SPREADSHEET_ID)


def get_ops_theme(sh: gspread.Spreadsheet) -> str:
    ws = sh.worksheet(OPS_TAB)
    theme = (ws.acell("B2").value or "").strip()
    if not theme:
        raise RuntimeError("OPS_MASTER!B2 (theme_of_the_day) is blank.")
    return theme


def _header_map(headers: List[str]) -> Dict[str, int]:
    # maps header -> 0-based index
    return {h.strip(): i for i, h in enumerate(headers) if h and h.strip()}


def read_table(ws: gspread.Worksheet) -> Tuple[List[str], List[List[Any]]]:
    values = ws.get_all_values()
    if not values or not values[0]:
        return [],
