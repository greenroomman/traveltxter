# workers/ai_scorer.py
import os
import sys
from datetime import datetime, timedelta, timezone

from utils.sheets import (
    get_worksheet,
    batch_update_rows,
    read_view_rows,
)
from utils.scoring import (
    compute_worthiness_score,
    compute_variety_penalties,
)

RAW_DEALS_TAB = os.environ.get("RAW_DEALS_TAB", "RAW_DEALS")
WINNERS_PER_RUN = int(os.environ.get("WINNERS_PER_RUN", 3))  # 🔥 WAS 1
VARIETY_LOOKBACK_HOURS = int(os.environ.get("VARIETY_LOOKBACK_HOURS", 120))
DEST_REPEAT_PENALTY = int(os.environ.get("DEST_REPEAT_PENALTY", 80))
THEME_REPEAT_PENALTY = int(os.environ.get("THEME_REPEAT_PENALTY", 30))
HARD_BLOCK_BAD_DEALS = True

NOW = datetime.now(timezone.utc)

def main():
    print(f"{NOW.isoformat()} | AI SCORER START")
    print(f"WINNERS_PER_RUN={WINNERS_PER_RUN}")

    ws = get_worksheet(RAW_DEALS_TAB)

    view_rows = read_view_rows()
    print(f"Loaded RAW_DEALS_VIEW rows: {len(view_rows)}")

    new_rows = [r for r in view_rows if r.get("status") == "NEW"]
    print(f"Found NEW rows: {len(new_rows)}")

    if not new_rows:
        print("No NEW rows. Exiting.")
        return

    scored_updates = []

    for row in new_rows:
        worthiness_score, verdict, why = compute_worthiness_score(row)

        if HARD_BLOCK_BAD_DEALS and worthiness_score < 5:
            verdict = "❌ IGNORE"

        dest_penalty, theme_penalty = compute_variety_penalties(
            row,
            lookback_hours=VARIETY_LOOKBACK_HOURS,
            dest_penalty=DEST_REPEAT_PENALTY,
            theme_penalty=THEME_REPEAT_PENALTY,
        )

        final_score = worthiness_score - dest_penalty - theme_penalty

        row_update = {
            "deal_score": final_score,
            "dest_variety_score": dest_penalty,
            "theme_variety_score": theme_penalty,
            "worthiness_score": worthiness_score,
            "worthiness_verdict": verdict,
            "why_good": why,
            "ai_notes": (
                "High intent winter-sun route"
                if verdict in ("🔥 ELITE", "✅ POST")
                else "Below publish threshold"
            ),
            "scored_timestamp": NOW.isoformat(),
        }

        scored_updates.append((row["row_number"], row_update))

    batch_update_rows(ws, scored_updates)

    publishable = [
        r for r in new_rows
        if r.get("worthiness_verdict") in ("🔥 ELITE", "✅ POST")
    ]

    publishable.sort(
        key=lambda r: r.get("worthiness_score", 0),
        reverse=True
    )

    winners = publishable[:WINNERS_PER_RUN]

    status_updates = []
    for r in winners:
        status_updates.append((
            r["row_number"],
            {"status": "READY_TO_POST"}
        ))

    if status_updates:
        batch_update_rows(ws, status_updates)

    print(f"Promoted to READY_TO_POST: {len(status_updates)}")
    print("AI SCORER COMPLETE")

if __name__ == "__main__":
    main()
