#!/usr/bin/env python3

from __future__ import annotations

import json
import os
from typing import Any, Dict, List

from supabase import create_client


BASELINE_BRIER = 0.1451
ALERT_THRESHOLD = 0.25

BANDS = [
    ("0.00-0.30", 0.00, 0.30),
    ("0.30-0.50", 0.30, 0.50),
    ("0.50-0.70", 0.50, 0.70),
    ("0.70-1.00", 0.70, 1.0000001),
]


def emit(payload: Dict[str, Any]) -> None:
    print(json.dumps(payload, sort_keys=True))


def get_required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def fetch_verified_rows() -> List[Dict[str, Any]]:
    supabase_url = get_required_env("MIZAR_SUPABASE_URL")
    supabase_key = get_required_env("MIZAR_SUPABASE_SERVICE_ROLE_KEY")

    supabase = create_client(supabase_url, supabase_key)

    rows: List[Dict[str, Any]] = []
    offset = 0
    batch_size = 1000

    while True:
        result = (
            supabase.table("outcome_verification")
            .select(
                "prediction_outcome, ground_truth_rose, "
                "user_decisions!inner(regret_risk_score, model_version, validation_eligible)"
            )
            .eq("user_decisions.model_version", "v3_1_0")
            .eq("user_decisions.validation_eligible", True)
            .not_.is_("prediction_outcome", "null")
            .range(offset, offset + batch_size - 1)
            .execute()
        )

        batch = result.data or []
        rows.extend(batch)

        if len(batch) < batch_size:
            break

        offset += batch_size

    return rows


def normalise_truth(row: Dict[str, Any]) -> float | None:
    value = row.get("ground_truth_rose")

    if isinstance(value, bool):
        return 1.0 if value else 0.0

    if isinstance(value, (int, float)):
        return 1.0 if float(value) >= 0.5 else 0.0

    outcome = str(row.get("prediction_outcome", "")).strip().upper()

    if outcome in {"TP", "FN", "ROSE", "FARE_ROSE", "TRUE_POSITIVE", "FALSE_NEGATIVE"}:
        return 1.0

    if outcome in {"TN", "FP", "NO_RISE", "FARE_DID_NOT_RISE", "TRUE_NEGATIVE", "FALSE_POSITIVE"}:
        return 0.0

    return None


def extract_score(row: Dict[str, Any]) -> float | None:
    decision = row.get("user_decisions") or {}

    try:
        score = float(decision.get("regret_risk_score"))
    except (TypeError, ValueError):
        return None

    if 0.0 <= score <= 1.0:
        return score

    return None


def build_band_report(points: List[Dict[str, float]]) -> List[Dict[str, Any]]:
    report: List[Dict[str, Any]] = []

    for label, lower, upper in BANDS:
        band_points = [
            p for p in points
            if lower <= p["score"] < upper
        ]

        n = len(band_points)

        if n == 0:
            report.append(
                {
                    "band": label,
                    "n": 0,
                    "mean_predicted": None,
                    "observed_rise_rate": None,
                }
            )
            continue

        mean_predicted = sum(p["score"] for p in band_points) / n
        observed_rise_rate = sum(p["truth"] for p in band_points) / n

        report.append(
            {
                "band": label,
                "n": n,
                "mean_predicted": round(mean_predicted, 4),
                "observed_rise_rate": round(observed_rise_rate, 4),
            }
        )

    return report


def main() -> None:
    rows = fetch_verified_rows()

    points: List[Dict[str, float]] = []

    for row in rows:
        score = extract_score(row)
        truth = normalise_truth(row)

        if score is None or truth is None:
            continue

        points.append({"score": score, "truth": truth})

    n = len(points)

    if n < 100:
        emit(
            {
                "event": "MIZAR_CALIBRATION_CHECK",
                "status": "skipped",
                "reason": "insufficient_n",
                "n": n,
            }
        )
        return

    brier_score = sum((p["score"] - p["truth"]) ** 2 for p in points) / n
    bands = build_band_report(points)

    emit(
        {
            "event": "MIZAR_CALIBRATION_CHECK",
            "brier_score": round(brier_score, 4),
            "baseline_brier": BASELINE_BRIER,
            "brier_delta": round(brier_score - BASELINE_BRIER, 4),
            "n_verified": n,
            "bands": bands,
        }
    )

    if brier_score > ALERT_THRESHOLD:
        emit(
            {
                "event": "MIZAR_CALIBRATION_ALERT",
                "brier_score": round(brier_score, 4),
                "threshold": ALERT_THRESHOLD,
            }
        )


if __name__ == "__main__":
    main()
