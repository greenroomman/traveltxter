#!/usr/bin/env python3
"""
MIZAR nightly commercial invariant assertions.

Purpose:
Run defensibility checks against the live MIZAR Supabase project.

Rules:
- Use MIZAR_SUPABASE_URL and MIZAR_SUPABASE_SERVICE_ROLE_KEY only.
- Emit JSON lines for every invariant.
- Emit one breach event if any invariant fails.
- Always exit 0. Breaches alert; they do not fail the workflow.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from supabase import create_client


MODEL_VERSION = "v3_1_0"
EXPECTED_MODEL_HASH = "7a6df46f"
FIX_2_CUTOFF = "2026-05-17T12:00:00+00:00"


def emit(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, sort_keys=True, default=str), flush=True)


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


SUPABASE_URL = require_env("MIZAR_SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = require_env("MIZAR_SUPABASE_SERVICE_ROLE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


def count_response(response: Any) -> int:
    if getattr(response, "count", None) is not None:
        return int(response.count)
    data = getattr(response, "data", None) or []
    return len(data)


def check_ci_1() -> dict[str, Any]:
    since = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()

    response = (
        supabase.table("user_decisions")
        .select("decision_id", count="exact")
        .eq("model_version", MODEL_VERSION)
        .eq("validation_eligible", True)
        .gte("regret_risk_score", 0.70)
        .gte("created_at", since)
        .limit(1)
        .execute()
    )

    value = count_response(response)
    passed = value > 0

    return {
        "event": "MIZAR_INVARIANT_CHECK",
        "invariant": "CI-1",
        "status": "pass" if passed else "breach",
        "value": value,
        "threshold": ">0",
        "meaning": "eligible high-risk decisions in last 48 hours",
    }


def check_ci_2() -> dict[str, Any]:
    since = (datetime.now(timezone.utc) - timedelta(days=7)).date().isoformat()

    response = (
        supabase.table("user_decisions")
        .select("decision_id", count="exact")
        .eq("route_class", "suppress")
        .eq("gated_recommendation", "book_now")
        .gte("created_at", since)
        .limit(1)
        .execute()
    )

    value = count_response(response)
    passed = value == 0

    return {
        "event": "MIZAR_INVARIANT_CHECK",
        "invariant": "CI-2",
        "status": "pass" if passed else "breach",
        "value": value,
        "threshold": "=0",
        "meaning": "suppressed routes must not surface book_now",
    }


def check_ci_3() -> dict[str, Any]:
    since = (datetime.now(timezone.utc) - timedelta(days=7)).date().isoformat()

    response = (
        supabase.table("user_decisions")
        .select("regret_risk_score")
        .eq("model_version", MODEL_VERSION)
        .gte("created_at", since)
        .order("regret_risk_score", desc=True)
        .limit(1)
        .execute()
    )

    data = response.data or []
    value = None
    if data:
        value = data[0].get("regret_risk_score")

    passed = value is not None and float(value) > 0.50

    return {
        "event": "MIZAR_INVARIANT_CHECK",
        "invariant": "CI-3",
        "status": "pass" if passed else "breach",
        "value": value,
        "threshold": ">0.50",
        "meaning": "recent max score must not show compression",
    }


def check_ci_4() -> dict[str, Any]:
    response = (
        supabase.table("user_decisions")
        .select("decision_id", count="exact")
        .eq("model_version", MODEL_VERSION)
        .gte("created_at", FIX_2_CUTOFF)
        .or_("raw_recommendation.is.null,gated_recommendation.is.null")
        .limit(1)
        .execute()
    )

    value = count_response(response)
    passed = value == 0

    return {
        "event": "MIZAR_INVARIANT_CHECK",
        "invariant": "CI-4",
        "status": "pass" if passed else "breach",
        "value": value,
        "threshold": "=0",
        "meaning": "raw and gated recommendations must be present after Fix 2",
    }


def check_ci_5() -> dict[str, Any]:
    response = (
        supabase.table("model_registry")
        .select("joblib_hash")
        .eq("model_version", MODEL_VERSION)
        .order("deployed_at", desc=True)
        .limit(1)
        .execute()
    )

    data = response.data or []
    value = data[0].get("joblib_hash") if data else None
    passed = value == EXPECTED_MODEL_HASH

    return {
        "event": "MIZAR_INVARIANT_CHECK",
        "invariant": "CI-5",
        "status": "pass" if passed else "breach",
        "value": value,
        "threshold": EXPECTED_MODEL_HASH,
        "meaning": "latest registered model hash must match expected production hash",
    }


def main() -> int:
    checks = [
        check_ci_1,
        check_ci_2,
        check_ci_3,
        check_ci_4,
        check_ci_5,
    ]

    results: list[dict[str, Any]] = []

    for check in checks:
        try:
            result = check()
        except Exception as exc:
            result = {
                "event": "MIZAR_INVARIANT_CHECK",
                "invariant": check.__name__.replace("check_", "").upper().replace("_", "-"),
                "status": "breach",
                "value": None,
                "threshold": "query_success",
                "meaning": "invariant query failed",
                "error": str(exc),
            }

        results.append(result)
        emit(result)

    failed = [result for result in results if result.get("status") != "pass"]

    if failed:
        emit(
            {
                "event": "MIZAR_INVARIANT_BREACH",
                "failed_invariants": [result["invariant"] for result in failed],
                "details": failed,
            }
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
