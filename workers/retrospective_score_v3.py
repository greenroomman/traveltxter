from __future__ import annotations

import os
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from supabase import create_client

from main import (
    SignalRequest,
    score_signal,
    recommendation_from_score,
)

MODEL_VERSION_TO_SCORE = "v3_0_0_retrospective"
SOURCE_MODEL_VERSION = "v2_0_0"
BATCH_SIZE = 1000
UPSERT_SIZE = 100


def load_env_file(path: str = ".env.local") -> None:
    if not os.path.exists(path):
        return

    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def get_supabase_client():
    load_env_file()

    url = (
        os.environ.get("SUPABASE_URL")
        or os.environ.get("MIZAR_SUPABASE_URL")
    )
    key = (
        os.environ.get("SUPABASE_SERVICE_KEY")
        or os.environ.get("MIZAR_SUPABASE_SERVICE_ROLE_KEY")
    )

    if not url or not key:
        raise RuntimeError("Missing Supabase env vars")

    return create_client(url, key)


def score_band(score: float) -> str:
    if score >= 0.45:
        return "high_risk"
    if score >= 0.30:
        return "mid_risk"
    return "low_risk"


def confidence_band(confidence_score: float) -> str:
    if confidence_score >= 0.70:
        return "high"
    if confidence_score >= 0.40:
        return "medium"
    return "low"


def fetch_v2_decisions(sb, offset: int) -> List[Dict[str, Any]]:
    result = (
        sb.table("user_decisions")
        .select(
            "decision_id,origin_iata,destination_iata,outbound_date,return_date,"
            "price_shown_gbp,trip_type,cabin_class,model_version"
        )
        .eq("model_version", SOURCE_MODEL_VERSION)
        .not_.is_("regret_risk_score", "null")
        .order("created_at", desc=False)
        .range(offset, offset + BATCH_SIZE - 1)
        .execute()
    )
    return result.data or []


def build_signal_request(row: Dict[str, Any]) -> Optional[SignalRequest]:
    origin = row.get("origin_iata")
    destination = row.get("destination_iata")
    outbound_date = row.get("outbound_date")
    return_date = row.get("return_date")
    price = row.get("price_shown_gbp")

    if not origin or not destination or not outbound_date or not price:
        return None

    trip_type = row.get("trip_type") or ("return" if return_date else "oneway")
    cabin_class = row.get("cabin_class") or "economy"

    try:
        return SignalRequest(
            origin=origin,
            destination=destination,
            outbound_date=outbound_date,
            return_date=return_date,
            price_gbp=float(price),
            client_platform="api",
            trip_type=trip_type,
            cabin_class=cabin_class,
        )
    except Exception:
        if return_date:
            try:
                return SignalRequest(
                    origin=origin,
                    destination=destination,
                    outbound_date=outbound_date,
                    return_date=return_date,
                    price_gbp=float(price),
                    client_platform="api",
                    trip_type="return",
                    cabin_class=cabin_class,
                )
            except Exception:
                return None

        try:
            return SignalRequest(
                origin=origin,
                destination=destination,
                outbound_date=outbound_date,
                price_gbp=float(price),
                client_platform="api",
                trip_type="oneway",
                cabin_class=cabin_class,
            )
        except Exception:
            return None


def upsert_scores(sb, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return

    for i in range(0, len(rows), UPSERT_SIZE):
        chunk = rows[i : i + UPSERT_SIZE]
        sb.table("model_decision_scores").upsert(
            chunk,
            on_conflict="decision_id,scored_model_version",
        ).execute()


def main() -> None:
    sb = get_supabase_client()

    offset = 0
    total_read = 0
    total_scored = 0
    total_skipped = 0
    total_failed = 0

    print("Starting retrospective v3 scoring")
    print(f"Source model: {SOURCE_MODEL_VERSION}")
    print(f"Scored model: {MODEL_VERSION_TO_SCORE}")

    while True:
        rows = fetch_v2_decisions(sb, offset)
        if not rows:
            break

        output_rows: List[Dict[str, Any]] = []

        for row in rows:
            total_read += 1
            req = build_signal_request(row)

            if req is None:
                total_skipped += 1
                continue

            try:
                score, actual_model_version, context = score_signal(req)
                conf_score = float(context.get("confidence_score", 0.0))

                output_rows.append(
                    {
                        "decision_id": row["decision_id"],
                        "scored_model_version": MODEL_VERSION_TO_SCORE,
                        "score": score,
                        "recommendation": recommendation_from_score(score),
                        "score_band": score_band(score),
                        "confidence_band": confidence_band(conf_score),
                        "model_artifact": "atlas_regret_risk_v3.joblib",
                        "feature_artifact": "atlas_v3_features.txt",
                        "scorer_name": "retrospective_score_v3.py",
                        "scorer_version": "1.0",
                        "notes": "Retrospective v3 score generated from original v2 decision inputs. Does not alter user_decisions.",
                        "metadata": {
                            "source_model_version": row.get("model_version"),
                            "actual_loaded_model_version": actual_model_version,
                            "snapshot_age_hours": context.get("snapshot_age_hours"),
                            "snapshot_count_30d": context.get("snapshot_count_30d"),
                            "volatility_7d": context.get("volatility_7d"),
                            "confidence_score": conf_score,
                            "rescored_at": datetime.now(timezone.utc).isoformat(),
                        },
                    }
                )
                total_scored += 1

            except Exception as exc:
                total_failed += 1
                print(f"FAILED {row.get('decision_id')}: {type(exc).__name__}: {exc}")

        upsert_scores(sb, output_rows)

        print(
            f"Progress: read={total_read} scored={total_scored} "
            f"skipped={total_skipped} failed={total_failed}"
        )

        offset += BATCH_SIZE

    print("Done")
    print(
        json.dumps(
            {
                "read": total_read,
                "scored": total_scored,
                "skipped": total_skipped,
                "failed": total_failed,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
