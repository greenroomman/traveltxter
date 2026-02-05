#!/usr/bin/env python3
# ============================================================
# TRAVELTXTTER V5 — FEEDER (CREDENTIAL-SAFE)
# ============================================================

import os
import sys
import time
import json
import re
import uuid
import base64
from datetime import datetime, timezone
from typing import List, Dict

import gspread
from google.oauth2.service_account import Credentials

# ------------------------------------------------------------
# ENV
# ------------------------------------------------------------
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
RAW_DEALS_TAB = os.environ.get("RAW_DEALS_TAB", "RAW_DEALS")
FEEDER_CONFIG_TAB = os.environ.get("FEEDER_CONFIG_TAB", "CONFIG")
THEME = os.environ.get("THEME", "DEFAULT")
MAX_SEARCHES = int(os.environ.get("DUFFEL_MAX_SEARCHES_PER_RUN", "12"))
DESTS_PER_RUN = int(os.environ.get("DUFFEL_ROUTES_PER_RUN", "4"))

# ------------------------------------------------------------
# LOGGING
# ------------------------------------------------------------
def log(msg: str):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"{ts} | {msg}", flush=True)

# ------------------------------------------------------------
# HEADER NORMALISATION
# ------------------------------------------------------------
def _norm(h: str) -> str:
    if not h:
        return ""
    h = h.replace("\u00a0", " ").strip()
    h = re.sub(r"\s+", "_", h)
    return h.lower()

def header_map(headers: List[str]) -> Dict[str, int]:
    return {_norm(h): i for i, h in enumerate(headers) if _norm(h)}

def ensure_headers(headers: List[str], required: List[str], tab_name: str):
    hm = header_map(headers)
    missing = [_norm(h) for h in required if _norm(h) not in hm]
    if missing:
        raise RuntimeError(
            f"{tab_name} missing required headers: {missing}\n"
            f"Detected headers: {list(hm.keys())}"
        )
    return hm

# ------------------------------------------------------------
# SAFE GOOGLE AUTH (FIX)
# ------------------------------------------------------------
def load_service_account():
    raw = (
        os.environ.get("GCP_SA_JSON_ONE_LINE")
        or os.environ.get("GCP_SA_JSON")
    )
    if not raw:
        raise RuntimeError("Missing GCP service account secret")

    # 1) Try raw JSON
    try:
        return json.loads(raw)
    except Exception:
        pass

    # 2) Try newline repair
    try:
        return json.loads(raw.replace("\\n", "\n"))
    except Exception:
        pass

    # 3) Try base64
    try:
        decoded = base64.b64decode(raw).decode("utf-8")
        return json.loads(decoded)
    except Exception:
        pass

    raise RuntimeError(
        "Failed to parse GCP service account JSON. "
        "Secret is malformed or incorrectly escaped."
    )

def gspread_client():
    info = load_service_account()
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)

# ------------------------------------------------------------
# LOAD CONFIG
# ------------------------------------------------------------
def load_config(ws, theme: str):
    rows = ws.get_all_values()
    headers = rows[0]

    hm = ensure_headers(
        headers,
        ["enabled", "destination_iata", "theme", "weight"],
        FEEDER_CONFIG_TAB,
    )

    cfg = []
    for r in rows[1:]:
        if not r or len(r) < len(headers):
            continue
        if r[hm["enabled"]].strip().upper() != "TRUE":
            continue
        if r[hm["theme"]] not in (theme, "DEFAULT"):
            continue
        try:
            weight = float(r[hm["weight"]])
        except Exception:
            weight = 1.0

        cfg.append({"dest": r[hm["destination_iata"]], "weight": weight})

    cfg.sort(key=lambda x: x["weight"], reverse=True)
    return cfg[:DESTS_PER_RUN]

# ------------------------------------------------------------
# INSERT DEAL
# ------------------------------------------------------------
def insert_deal(ws, hm, dest_iata: str, theme: str):
    ts = datetime.now(timezone.utc).timestamp()
    row = [""] * len(hm)

    def setv(col, val):
        if col in hm:
            row[hm[col]] = val

    setv("deal_id", uuid.uuid4().hex[:12])
    setv("destination_iata", dest_iata)
    setv("theme", theme)
    setv("status", "NEW")
    setv("ingested_at_utc", ts)

    ws.append_row(row, value_input_option="USER_ENTERED")

# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------
def main():
    log("=" * 70)
    log("TRAVELTXTTER V5 — FEEDER START (MIN CONFIG)")
    log("=" * 70)
    log(f"🎯 Theme of day: {THEME}")

    gc = gspread_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    ws_raw = sh.worksheet(RAW_DEALS_TAB)
    raw_vals = ws_raw.get_all_values()
    raw_headers = raw_vals[0]

    raw_hm = ensure_headers(
        raw_headers,
        ["deal_id", "destination_iata", "theme", "status", "ingested_at_utc"],
        RAW_DEALS_TAB,
    )

    ws_cfg = sh.worksheet(FEEDER_CONFIG_TAB)
    cfg = load_config(ws_cfg, THEME)

    if not cfg:
        log("⚠️ No CONFIG routes eligible for theme.")
        return

    inserted = 0
    for item in cfg:
        if inserted >= MAX_SEARCHES:
            break
        log(f"🔎 Ingesting {item['dest']} (weight={item['weight']})")
        insert_deal(ws_raw, raw_hm, item["dest"], THEME)
        inserted += 1
        time.sleep(0.05)

    log(f"✅ Inserted {inserted} row(s)")

# ------------------------------------------------------------
if __name__ == "__main__":
    raise SystemExit(main())
