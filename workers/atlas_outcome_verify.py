from __future__ import annotations

"""
atlas_outcome_verify.py

MIZAR Atlas — Outcome Verification Worker

Runs daily at 10:00 UTC via atlas_outcome_verify.yml.
"""

import json
import logging
import os
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timezone
from typing import Any

from supabase import create_client


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)

log = logging.getLogger(__name__)


# ------------------------------------------------------------
# Config
# ------------------------------------------------------------

SUPABASE_URL = os.environ["MIZAR_SUPABASE_URL"]
SUPABASE_KEY = os.environ["MIZAR_SUPABASE_SERVICE_ROLE_KEY"]
DUFFEL_TOKEN = os.environ.get("DUFFEL_API_KEY") or os.environ["DUFFEL_ACCESS_TOKEN"]

DUFFEL_BASE = "https://api.duffel.com"
DUFFEL_HEADERS = {
    "Authorization": f"Bearer {DUFFEL_TOKEN}",
    "Duffel-Version": "v2",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

RISE_THRESHOLD_PCT = 10.0
HIGH_RISK_THRESHOLD = 0.70
REQUEST_DELAY_S = 1.2

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# ------------------------------------------------------------
# Duffel helpers
# ------------------------------------------------------------

def _duffel_post(path: str, payload: dict[str, Any], retries: int = 3) -> dict[str, Any] | None:
    """POST to Duffel with retry on 429."""
    url = f"{DUFFEL_BASE}{path}"
    data = json.dumps(payload).encode("utf-8")
    delays = [2, 5, 10]

    for attempt in range(retries):
        request = urllib.request.Request(
            url,
            data=data,
            headers=DUFFEL_HEADERS,
            method="POST",
        )

        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.loads(response.read().decode("utf-8"))

        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")

            if exc.code == 429:
                wait = delays[min(attempt, len(delays) - 1)]
                log.warning(
                    "Duffel 429. Waiting %ss. Attempt %d/%d.",
                    wait,
                    attempt + 1,
                    retries,
                )
                time.sleep(wait)
                continue

            log.warning("Duffel HTTP %d: %s", exc.code, body[:500])
            return None, duffel_search_id

        except Exception as exc:
            log.warning("Duffel request error: %s", exc)
            return None, duffel_search_id

    log.error("Duffel exhausted retries for %s", path)
    return None


def cheapest_gbp_price(
    origin: str,
    destination: str,
    outbound_date: date,
    cabin_class: str = "economy",
    trip_type: str = "return",
    return_date: date | None = None,
) -> tuple[float | None, str | None]:
    """Search Duffel for the cheapest GBP offer on the given route/date."""

    cabin_map = {
        "economy": "economy",
        "premium_economy": "premium_economy",
        "business": "business",
        "first": "first",
    }

    duffel_cabin = cabin_map.get((cabin_class or "economy").lower(), "economy")

    slices = [
        {
            "origin": origin,
            "destination": destination,
            "departure_date": outbound_date.isoformat(),
        }
    ]

    if trip_type == "return" and return_date is not None:
        slices.append(
            {
                "origin": destination,
                "destination": origin,
                "departure_date": return_date.isoformat(),
            }
        )

    payload = {
        "data": {
            "slices": slices,
            "passengers": [{"type": "adult"}],
            "cabin_class": duffel_cabin,
        }
    }

    response = _duffel_post("/air/offer_requests?return_offers=true", payload)

    duffel_search_id = None

    if response:
        duffel_search_id = response.get("data", {}).get("id")

    if response is None:
        return None, duffel_search_id

    offers = response.get("data", {}).get("offers", [])

    if not offers:
        return None, duffel_search_id

    gbp_prices: list[float] = []

    for offer in offers:
        if offer.get("total_currency") != "GBP":
            continue

        try:
            gbp_prices.append(float(offer["total_amount"]))
        except (KeyError, TypeError, ValueError):
            continue

    if not gbp_prices:
        return None, duffel_search_id

    return min(gbp_prices), duffel_search_id


# ------------------------------------------------------------
# Classification
# ------------------------------------------------------------

def classify_outcome(regret_risk_score: float, ground_truth_rose: bool) -> str:
    predicted_high = regret_risk_score >= HIGH_RISK_THRESHOLD

    if predicted_high and ground_truth_rose:
        return "TP"

    if predicted_high and not ground_truth_rose:
        return "FP"

    if not predicted_high and not ground_truth_rose:
        return "TN"

    return "FN"


# ------------------------------------------------------------
# Supabase helpers
# ------------------------------------------------------------

def parse_datetime_utc(value: str) -> datetime:
    """Parse Supabase timestamptz safely."""
    cleaned = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(cleaned)

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def fetch_pending_decisions() -> list[dict[str, Any]]:
    """Return validation-eligible pending decisions ready for t+7 verification.

    Supabase Python client returns 1,000 rows by default. This worker must
    paginate explicitly so older pending rows cannot hide newer v3 decisions.
    """

    batch_size = 1000
    start = 0
    rows: list[dict[str, Any]] = []

    while True:
        end = start + batch_size - 1

        response = (
            supabase.table("user_decisions")
            .select(
                "decision_id, decision_timestamp, origin_iata, destination_iata, "
                "outbound_date, return_date, price_shown_gbp, regret_risk_score, "
                "trip_type, cabin_class"
            )
            .eq("verification_status", "pending")
            .eq("validation_eligible", True)
            .order("decision_timestamp", desc=False)
            .range(start, end)
            .execute()
        )

        batch = response.data or []
        rows.extend(batch)

        if len(batch) < batch_size:
            break

        start += batch_size

    eligible: list[dict[str, Any]] = []
    now_dt = datetime.now(timezone.utc)

    for row in rows:
        decision_timestamp = row.get("decision_timestamp")

        if not decision_timestamp:
            continue

        try:
            decision_dt = parse_datetime_utc(decision_timestamp)
        except Exception:
            log.warning("Skipping decision with invalid decision_timestamp: %s", row)
            continue

        if (now_dt - decision_dt).days >= 7:
            eligible.append(row)

    log.info(
        "Found %d validation-eligible pending decisions, %d eligible for t+7.",
        len(rows),
        len(eligible),
    )
    return eligible


def already_verified(decision_id: str) -> bool:
    """Guard against duplicate processing on outcome_verification.decision_id."""

    response = (
        supabase.table("outcome_verification")
        .select("verification_id")
        .eq("decision_id", decision_id)
        .limit(1)
        .execute()
    )

    return len(response.data or []) > 0


def mark_decision_status(decision_id: str, status: str) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()

    (
        supabase.table("user_decisions")
        .update(
            {
                "verification_status": status,
                "updated_at": now_iso,
            }
        )
        .eq("decision_id", decision_id)
        .execute()
    )


def write_verification(
    decision_id: str,
    price_t7: float | None,
    price_shown: float | None,
    regret_risk_score: float | None,
    failure_reason: str | None,
    duffel_search_id: str | None = None,
) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()
    status = "failed"

    if price_shown is None or price_shown <= 0:
        row = {
            "decision_id": decision_id,
            "verification_timestamp": now_iso,
            "price_t7_gbp": price_t7,
            "price_change_pct": None,
            "ground_truth_rose": None,
            "prediction_outcome": None,
            "verification_method": "duffel_api",
            "failure_reason": failure_reason or "invalid_original_price",
            "duffel_search_id": duffel_search_id,
        }

    elif price_t7 is None:
        row = {
            "decision_id": decision_id,
            "verification_timestamp": now_iso,
            "price_t7_gbp": None,
            "price_change_pct": None,
            "ground_truth_rose": None,
            "prediction_outcome": None,
            "verification_method": "duffel_api",
            "failure_reason": failure_reason or "no_price_returned",
            "duffel_search_id": duffel_search_id,
        }

    else:
        change_pct = ((float(price_t7) - float(price_shown)) / float(price_shown)) * 100
        ground_truth_rose = change_pct >= RISE_THRESHOLD_PCT
        outcome = classify_outcome(float(regret_risk_score or 0.0), ground_truth_rose)

        row = {
            "decision_id": decision_id,
            "verification_timestamp": now_iso,
            "price_t7_gbp": round(float(price_t7), 2),
            "price_change_pct": round(change_pct, 2),
            "ground_truth_rose": ground_truth_rose,
            "prediction_outcome": outcome,
            "verification_method": "duffel_api",
            "failure_reason": None,
            "duffel_search_id": duffel_search_id,
        }

        status = "verified"

    try:
        (
            supabase.table("outcome_verification")
            .upsert(row, on_conflict="decision_id")
            .execute()
        )

        mark_decision_status(decision_id, status)

    except Exception as exc:
        log.error("Failed to write verification for %s: %s", decision_id, exc)
        mark_decision_status(decision_id, "failed")


# ------------------------------------------------------------
# Main loop
# ------------------------------------------------------------

def run() -> None:
    decisions = fetch_pending_decisions()

    if not decisions:
        log.info("No eligible decisions. Exiting.")
        return

    success = 0
    failed = 0
    skipped = 0

    for decision in decisions:
        decision_id = decision["decision_id"]

        if already_verified(decision_id):
            log.info("SKIP %s. Already exists in outcome_verification.", decision_id)
            mark_decision_status(decision_id, "verified")
            skipped += 1
            continue

        origin = decision.get("origin_iata")
        destination = decision.get("destination_iata")
        outbound_date_str = decision.get("outbound_date")
        return_date_str = decision.get("return_date")
        trip_type = decision.get("trip_type") or "return"
        cabin_class = decision.get("cabin_class") or "economy"

        try:
            price_shown = float(decision["price_shown_gbp"])
        except Exception:
            price_shown = None

        try:
            score = float(decision["regret_risk_score"])
        except Exception:
            score = 0.0

        if not origin or not destination:
            log.warning(
                "Invalid route for %s: origin=%s destination=%s",
                decision_id,
                origin,
                destination,
            )
            write_verification(decision_id, None, price_shown, score, "invalid_route")
            failed += 1
            continue

        try:
            outbound_dt = date.fromisoformat(outbound_date_str)
        except Exception:
            log.warning("Invalid outbound_date for %s: %s", decision_id, outbound_date_str)
            write_verification(decision_id, None, price_shown, score, "invalid_outbound_date")
            failed += 1
            continue

        if outbound_dt < datetime.now(timezone.utc).date():
            log.info("SKIP %s. outbound_date %s has passed.", decision_id, outbound_dt)
            write_verification(decision_id, None, price_shown, score, "flight_already_departed")
            failed += 1
            time.sleep(REQUEST_DELAY_S)
            continue

        return_dt = None

        if return_date_str:
            try:
                return_dt = date.fromisoformat(return_date_str)
            except Exception:
                log.warning("Invalid return_date for %s: %s", decision_id, return_date_str)

        if trip_type == "return" and return_dt is None:
            log.warning("Return trip missing return_date for %s.", decision_id)
            write_verification(decision_id, None, price_shown, score, "missing_return_date")
            failed += 1
            continue

        log.info(
            "Verifying %s | %s→%s | outbound=%s | return=%s | trip_type=%s | cabin=%s | score=%.3f | shown=%s",
            decision_id,
            origin,
            destination,
            outbound_dt,
            return_dt,
            trip_type,
            cabin_class,
            score,
            f"£{price_shown:.2f}" if price_shown is not None else "NULL",
        )

        price_t7, duffel_search_id = cheapest_gbp_price(
            origin=origin,
            destination=destination,
            outbound_date=outbound_dt,
            cabin_class=cabin_class,
            trip_type=trip_type,
            return_date=return_dt,
        )

        if price_t7 is not None and price_shown is not None and price_shown > 0:
            change_pct = ((price_t7 - price_shown) / price_shown) * 100
            outcome = classify_outcome(score, change_pct >= RISE_THRESHOLD_PCT)

            log.info(
                "Result %s | t+7=£%.2f | change=%.1f%% | outcome=%s",
                decision_id,
                price_t7,
                change_pct,
                outcome,
            )

            success += 1
            write_verification(decision_id, price_t7, price_shown, score, None, duffel_search_id)

        else:
            log.warning("No valid GBP price returned for %s.", decision_id)
            failed += 1
            write_verification(decision_id, None, price_shown, score, "duffel_no_gbp_offer", duffel_search_id)

        time.sleep(REQUEST_DELAY_S)

    log.info(
        "Done. Success=%d Failed=%d Skipped=%d Total=%d",
        success,
        failed,
        skipped,
        len(decisions),
    )


if __name__ == "__main__":
    run()