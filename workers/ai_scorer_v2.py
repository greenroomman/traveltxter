#!/usr/bin/env python3
"""
V3.2(A) AI Scorer Worker
Clean, minimal, CI-safe version
"""

import os
import json
import time
from typing import Dict, Any

import httpx
from openai import OpenAI

from lib.sheets import (
    get_worksheet,
    ensure_headers,
    fetch_next_row_for_processing,
    update_row_by_headers,
)

REQUIRED_HEADERS = [
    "deal_id",
    "origin_city",
    "destination_city",
    "price_gbp",
    "outbound_date",
    "return_date",
    "ai_score",
    "ai_verdict",
    "ai_notes",
    "status",
]


def get_env(name: str, default: str | None = None) -> str:
    val = os.getenv(name, default)
    if val is None or val == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return val


def score_with_ai(client: OpenAI, row: Dict[str, Any]) -> Dict[str, Any]:
    prompt = f"""
You are scoring a flight deal.

Return STRICT JSON with keys:
- ai_score (0-100 integer)
- ai_verdict ("GOOD", "AVERAGE", or "POOR")
- ai_notes (short sentence)

Deal:
Origin: {row.get('origin_city')}
Destination: {row.get('destination_city')}
Price GBP: {row.get('price_gbp')}
Outbound: {row.get('outbound_date')}
Return: {row.get('return_date')}
"""

    resp = client.chat.completions.create(
        model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
    )

    text = resp.choices[0].message.content.strip()
    return json.loads(text)


def main() -> None:
    sheet_id = get_env("SHEET_ID")
    raw_tab = get_env("RAW_DEALS_TAB")
    worker_id = os.getenv("WORKER_ID", "ai_scorer_v2")

    ws = get_worksheet(sheet_id, raw_tab)
    ensure_headers(ws, REQUIRED_HEADERS)

    client = OpenAI(
        api_key=get_env("OPENAI_API_KEY"),
        http_client=httpx.Client(proxies=None),
    )

    row = fetch_next_row_for_processing(
        ws,
        status_col="status",
        set_status="SCORING",
        worker_id=worker_id,
    )

    if not row:
        print("No rows to score")
        return

    result = score_with_ai(client, row)

    update_row_by_headers(
        ws,
        row["_row"],
        {
            "ai_score": result["ai_score"],
            "ai_verdict": result["ai_verdict"],
            "ai_notes": result["ai_notes"],
            "status": "SCORED",
        },
    )

    print(f"Scored deal {row.get('deal_id')}")


if __name__ == "__main__":
    main()

