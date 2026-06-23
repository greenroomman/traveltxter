#!/usr/bin/env python3
"""
atlas_daily_slack_signal.py — MIZAR daily route intelligence feed

Posts the top 10 same-day MIZAR route signals to Slack.

Required env vars:
MIZAR_SUPABASE_URL
MIZAR_SUPABASE_SERVICE_ROLE_KEY
SLACK_WEBHOOK_URL
"""

import datetime
import os
import sys

import requests
from supabase import create_client


EXCLUDED_DESTINATIONS = {"DXB", "AUH", "DOH", "AMM", "BEY", "TLV"}
MODEL_VERSION = "v3_1_0"


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def utc_day_bounds(day: datetime.date) -> tuple[str, str]:
    start = datetime.datetime.combine(
        day,
        datetime.time.min,
        tzinfo=datetime.timezone.utc,
    )
    end = start + datetime.timedelta(days=1)
    return start.isoformat(), end.isoformat()


def signal_label(row: dict) -> str | None:
    gated = row.get("gated_recommendation")
    raw = row.get("raw_recommendation")

    if gated == "book_now":
        return "Book Now"

    if gated == "monitor" and raw == "book_now":
        return "Held Back"

    return None


def format_score(value) -> str:
    try:
        return f"{float(value):.2f}"
    except Exception:
        return "n/a"


def route_text(row: dict) -> str:
    origin = row.get("origin_iata") or row.get("origin") or "?"
    destination = row.get("destination_iata") or row.get("destination") or "?"
    return f"{origin} → {destination}"


def post_slack(webhook_url: str, message: str) -> None:
    response = requests.post(
        webhook_url,
        json={"text": message},
        timeout=10,
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"Slack post failed: {response.status_code} {response.text}"
        )

    print("Slack posted OK")


def build_message(rows: list[dict], today: datetime.date) -> str:
    date_label = today.isoformat()

    lines = [
        f"*MIZAR Route Signal — {date_label}*",
        "Top routes scoring for fare rise probability today",
        "",
        "```",
        "Route          | Score | Signal",
        "---------------|-------|----------",
    ]

    if not rows:
        lines.append("No qualifying route signals found today.")
    else:
        for row in rows:
            route = route_text(row)
            score = format_score(row.get("regret_risk_score"))
            signal = row.get("_signal_label", "n/a")
            lines.append(f"{route:<14} | {score:<5} | {signal}")

    lines.extend(
        [
            "```",
            "",
            "Score = probability of a 10%+ fare rise within 7 days.",
            "Held Back = high score, signal deliberately suppressed. Route history did not support confident action.",
        ]
    )

    return "\n".join(lines)


def main() -> int:
    supabase_url = require_env("MIZAR_SUPABASE_URL")
    supabase_key = require_env("MIZAR_SUPABASE_SERVICE_ROLE_KEY")
    slack_webhook_url = require_env("SLACK_WEBHOOK_URL")

    supabase = create_client(supabase_url, supabase_key)

    today = datetime.datetime.now(datetime.timezone.utc).date()
    start_iso, end_iso = utc_day_bounds(today)

    result = (
        supabase.table("user_decisions")
        .select(
            "created_at,origin_iata,destination_iata,regret_risk_score,"
            "gated_recommendation,raw_recommendation,model_version"
        )
        .gte("created_at", start_iso)
        .lt("created_at", end_iso)
        .eq("model_version", MODEL_VERSION)
        .order("regret_risk_score", desc=True)
        .limit(100)
        .execute()
    )

    candidates = []
    for row in result.data or []:
        destination = (row.get("destination_iata") or "").upper()
        if destination in EXCLUDED_DESTINATIONS:
            continue

        label = signal_label(row)
        if not label:
            continue

        row["_signal_label"] = label
        candidates.append(row)

        if len(candidates) >= 10:
            break

    message = build_message(candidates, today)
    print(message)
    post_slack(slack_webhook_url, message)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
