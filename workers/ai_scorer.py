#!/usr/bin/env python3
"""
TravelTxter V4.5x — ai_scorer.py (LOCKED)

ROLE:
- SCORE ONLY
- SELECT WINNERS
- PROMOTE STATUS

NEVER:
- call Duffel
- insert rows
- mutate anything except scoring columns + status

STATUS FLOW:
NEW -> SCORED
NEW (winner) -> READY_TO_POST
"""

from __future__ import annotations

import os
import json
import math
import datetime as dt
from typing import Dict, List, Any

import gspread
from google.oauth2.service_account import Credentials

from lib.sheet_config import (
    load_config_bundle,
    mvp_hard_limits,
)


# ============================================================
# Helpers
# ============================================================

def ts() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def log(msg: str) -> None:
    print(f"{ts()} | {msg}", flush=True)

def env_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, default))
    except Exception:
        return default

def env_str(key: str, default: str = "") -> str:
    return os.environ.get(key, default).strip()


# ============================================================
# Sheets auth
# ============================================================

def get_client() -> gspread.Client:
    sa = env_str("GCP_SA_JSON_ONE_LINE") or env_str("GCP_SA_JSON")
    if not sa:
        raise RuntimeError("Missing GCP_SA_JSON_ONE_LINE")
    info = json.loads(sa.replace("\\n", "\n"))
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)


# ============================================================
# Main
# ============================================================

def main() -> int:
    spreadsheet_id = env_str("SPREADSHEET_ID")
    tab = env_str("RAW_DEALS_TAB", "RAW_DEALS")

    if not spreadsheet_id:
        raise RuntimeError("Missing SPREADSHEET_ID")

    WINNERS_PER_RUN = env_int("WINNERS_PER_RUN", 1)
    DEST_REPEAT_PENALTY = env_int("DEST_REPEAT_PENALTY", 80)
    VARIETY_LOOKBACK_HOURS = env_int("VARIETY_LOOKBACK_HOURS", 120)

    gc = get_client()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(tab)

    values = ws.get_all_values()
    if len(values) < 2:
        log("No data to score.")
        return 0

    headers = [h.strip() for h in values[0]]
    rows = values[1:]
    h = {name: i for i, name in enumerate(headers)}

    required = [
        "status",
        "price_gbp",
        "destination_iata",
        "outbound_date",
        "deal_score",
    ]
    for c in required:
        if c not in h:
            raise RuntimeError(f"Missing required column: {c}")

    cfg = load_config_bundle(sh)
    limits = mvp_hard_limits(cfg.mvp_rules)

    now = dt.datetime.utcnow()

    # --------------------------------------------------------
    # Collect NEW rows only
    # --------------------------------------------------------

    candidates: List[Dict[str, Any]] = []

    for idx, row in enumerate(rows, start=2):
        if row[h["status"]].strip().upper() != "NEW":
            continue

        try:
            price = float(row[h["price_gbp"]])
        except Exception:
            continue

        # MVP hard limits
        if price < limits["min_price_gbp"] or price > limits["max_price_gbp"]:
            continue

        score = max(0, int(1000 - price * 2))
        candidates.append({
            "row": idx,
            "score": score,
            "dest": row[h["destination_iata"]],
        })

    if not candidates:
        log("No eligible NEW deals to score.")
        return 0

    # --------------------------------------------------------
    # Variety penalty (recent destinations)
    # --------------------------------------------------------

    recent_dests: Dict[str, int] = {}
    cutoff = now - dt.timedelta(hours=VARIETY_LOOKBACK_HOURS)

    for r in rows:
        try:
            t = dt.datetime.fromisoformat(r[h.get("posted_instagram_at", -1)])
            if t < cutoff:
                continue
            dest = r[h["destination_iata"]]
            recent_dests[dest] = recent_dests.get(dest, 0) + 1
        except Exception:
            continue

    for c in candidates:
        if c["dest"] in recent_dests:
            c["score"] -= DEST_REPEAT_PENALTY * recent_dests[c["dest"]]

    # --------------------------------------------------------
    # Rank + select winners
    # --------------------------------------------------------

    candidates.sort(key=lambda x: x["score"], reverse=True)
    winners = candidates[:WINNERS_PER_RUN]

    updates = []

    for c in candidates:
        r = c["row"]
        updates.append({
            "range": f"{chr(65+h['deal_score'])}{r}",
            "values": [[c["score"]]],
        })

        if c in winners:
            updates.append({
                "range": f"{chr(65+h['status'])}{r}",
                "values": [["READY_TO_POST"]],
            })
        else:
            updates.append({
                "range": f"{chr(65+h['status'])}{r}",
                "values": [["SCORED"]],
            })

    ws.batch_update(updates)
    log(f"Scored {len(candidates)} deals, promoted {len(winners)} winner(s).")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
