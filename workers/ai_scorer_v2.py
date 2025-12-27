import os
import json
from datetime import timedelta
from typing import Dict, Any

from openai import OpenAI

from lib.sheets import (
    get_env,
    get_gspread_client,
    validate_sheet_schema,
    ensure_headers,
    claim_first_available,
    update_row_by_headers,
)

RAW_REQ = [
    "deal_id",
    "origin_city",
    "destination_city",
    "destination_country",
    "price_gbp",
    "outbound_date",
    "return_date",
    "trip_length_days",
    "stops",
    "baggage_included",
    "airline",
    "deal_source",
    "notes",
    "date_added",
    "raw_status",
    "ai_score",
    "ai_caption",
    "ai_verdict",
    "ai_notes",
    "processing_lock",
    "locked_by",
    "price_score",
    "date_score",
    "friction_score",
    "backpacker_score",
    "confidence",
    "final_score",
    "theme",
    "reasons",
    "scoring_version",
]

SYSTEM_PROMPT = (
    "You score flight deals for UK backpackers. "
    "Return ONLY valid JSON. No markdown. No extra text."
)

THEMES = {"CITY_BREAK", "BEACH", "ADVENTURE", "CULTURE", "PARTY", "NATURE", "SKI"}


def clamp_int(x: Any) -> int:
    try:
        v = int(float(str(x).strip()))
        return max(0, min(100, v))
    except Exception:
        return 0


def parse_weights() -> Dict[str, float]:
    raw = os.getenv("SCORE_WEIGHTS", "").strip()
    if not raw:
        return {"price": 0.35, "date": 0.20, "friction": 0.20, "backpacker": 0.25}
    try:
        w = json.loads(raw)
        return {
            "price": float(w.get("price", 0.35)),
            "date": float(w.get("date", 0.20)),
            "friction": float(w.get("friction", 0.20)),
            "backpacker": float(w.get("backpacker", 0.25)),
        }
    except Exception:
        return {"price": 0.35, "date": 0.20, "friction": 0.20, "backpacker": 0.25}


def compute_final(scores: Dict[str, int], w: Dict[str, float]) -> int:
    return int(
        round(
            scores["price_score"] * w["price"]
            + scores["date_score"] * w["date"]
            + scores["friction_score"] * w["friction"]
            + scores["backpacker_score"] * w["backpacker"]
        )
    )


def verdict(final_score: int, confidence: int) -> str:
    if final_score >= 75 and confidence >= 70:
        return "GOOD"
    if final_score >= 55:
        return "AVERAGE"
    return "POOR"


def build_prompt(row: Dict[str, Any]) -> str:
    # Triple-quoted string = no risk of unterminated line breaks
    return f"""Score this flight deal for UK backpackers.

Return JSON with EXACT keys:
price_score, date_score, friction_score, backpacker_score, confidence, theme, reasons, ai_caption

Rules:
- All scores/confidence must be integers 0-100
- theme must be one of: CITY_BREAK, BEACH, ADVENTURE, CULTURE, PARTY, NATURE, SKI
- reasons must be ONE string with 3 short bullets separated by ' ; '
- ai_caption must be a short Instagram caption including route + price + dates

Origin: {row.get('origin_city')}
Destination: {row.get('destination_city')}, {row.get('destination_country')}
Price GBP: {row.get('price_gbp')}
Outbound: {row.get('outbound_date')}
Return: {row.get('return_date')}
Trip length (days): {row.get('trip_length_days')}
Stops: {row.get('stops')}
Baggage included: {row.get('baggage_included')}
Airline: {row.get('airline')}
Notes: {row.get('notes')}
"""


def main():
    client = OpenAI(api_key=get_env("OPENAI_API_KEY"))
    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()

    sheet_id = get_env("SHEET_ID")
    tab = os.getenv("RAW_DEALS_TAB", "RAW_DEALS").strip()
    worker_id = os.getenv("WORKER_ID", "ai_scorer_v2").strip()
    weights = parse_weights()

    sh = get_gspread_client().open_by_key(sheet_id)
    ws = sh.worksheet(tab)

    validate_sheet_schema(ws, RAW_REQ)

    row = claim_first_available(
        ws,
        RAW_REQ,
        status_col="raw_status",
        wanted_status="NEW",
        set_status="PROCESSING",
        worker_id=worker_id,
        max_lock_age=timedelta(minutes=15),
    )

    if not row:
        print("No rows to score.")
        return

    row_num = int(row["_row_number"])
    header_map = ensure_headers(ws, RAW_REQ)
    deal_id = row.get("deal_id", "")

    try:
        resp = client.chat.completions.create(
            model=model,
            temperature=0.1,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": build_prompt(row)},
            ],
        )

        data = json.loads(resp.choices[0].message.content)

        required = {
            "price_score",
            "date_score",
            "friction_score",
            "backpacker_score",
            "confidence",
            "theme",
            "reasons",
            "ai_caption",
        }
        missing = required - set(data.keys())
        if missing:
            raise ValueError(f"Missing keys in AI response: {sorted(missing)}")

        scores = {
            "price_score": clamp_int(data["price_score"]),
            "date_score": clamp_int(data["date_score"]),
            "friction_score": clamp_int(data["friction_score"]),
            "backpacker_score": clamp_int(data["backpacker_score"]),
        }

        confidence = clamp_int(data["confidence"])
        theme = str(data["theme"]).strip().upper()
        if theme not in THEMES:
            theme = "CITY_BREAK"

        final_score = compute_final(scores, weights)
        ai_verdict = verdict(final_score, confidence)

        update_row_by_headers(
            ws,
            header_map,
            row_num,
            {
                **scores,
                "confidence": confidence,
                "final_score": final_score,
                "ai_score": final_score,
                "ai_verdict": ai_verdict,
                "theme": theme,
                "reasons": str(data["reasons"]).strip(),
                "ai_caption": str(data["ai_caption"]).strip(),
                "ai_notes": str(data["reasons"]).strip(),
                "scoring_version": "v3.2a",
                "raw_status": "SCORED",
                "processing_lock": "",
                "locked_by": "",
            },
        )

        print(f"OK scored {deal_id} → {final_score} {ai_verdict}")

    except Exception as e:
        update_row_by_headers(
            ws,
            header_map,
            row_num,
            {
                "raw_status": "ERROR",
                "ai_notes": f"SCORER_FAIL {type(e).__name__}: {e}",
                "processing_lock": "",
                "locked_by": "",
            },
        )
        print(f"FAIL scoring {deal_id}: {e}")


if __name__ == "__main__":
    main()
