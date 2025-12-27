#!/usr/bin/env python3
"""
AI Scorer v2 – CI-safe, minimal, deterministic
"""

import os
import json
from typing import Dict

from openai import OpenAI

from lib.sheets import (
    get_worksheet,
    ensure_headers,
    find_and_lock_row,
    update_row_by_headers,
)

# -----------------------------
# Helpers
# -----------------------------

def get_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required env var: {name}")
    return val


def score_deal(deal: Dict) -> Dict:
    """
    Minimal deterministic scoring stub.
    Replace later with full LLM logic.
    """
    price = float(deal.get("price_gbp", 9999) or 9999)

    if price < 50:
        verdict = "GOOD"
        score = 90
    elif price < 150:
        verdict = "AVERAGE"
        score = 60
    else:
        verdict = "POOR"
        score = 30

    return {
        "ai_score": score,
        "ai_verdict": verdict,
        "ai_notes": f"Auto-scored at £{price}",
        "status": "READY_TO_POST",
    }


# -----------------------------
# Main worker
# -----------------------------

def main() -> None:
    # --- Env ---
    sheet_id = get_env("SHEET_ID")
    tab_name = get_env("RAW_DEALS_TAB")
    worker_id = get_env("WORKER_ID")

    # OPENAI_API_KEY is validated here even if not used yet
    _ = get_env("OPENAI_API_KEY")

    # --- Sheets ---
    ws = get_worksheet(sheet_id, tab_name)

    required_headers = [
        "deal_id",
        "price_gbp",
        "status",
        "processing_lock",
        "locked_by",
        "ai_score",
        "ai_verdict",
        "ai_notes",
    ]

    header_map = ensure_headers(ws, required_headers)

    # --- Lock one row ---
    locked = find_and_lock_row(
        ws=ws,
        header_map=header_map,
        status_col="status",
        pick_status="NEW",
        set_status="SCORING",
        worker_id=worker_id,
    )

    if not locked:
        print("No NEW rows to score.")
        return

    row_num, row_dict = locked

    # --- Score ---
    result = score_deal(row_dict)

    # --- Write back ---
    update_row_by_headers(
        ws,
        header_map,
        row_num,
        result,
    )

    print(f"Scored row {row_num}: {result['ai_verdict']} ({result['ai_score']})")


# -----------------------------
# Entrypoint
# -----------------------------

if __name__ == "__main__":
    main()
