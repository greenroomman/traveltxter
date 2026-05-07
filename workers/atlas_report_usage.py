#!/usr/bin/env python3
"""
atlas_report_usage.py

MIZAR PAYG usage reporter.

Runs nightly via GitHub Actions.

For each active PAYG subscriber:
1. Count API usage in the current billing period.
2. Report billable usage to Stripe using action=set.
3. Write the result to billing_periods.

Required env vars:
MIZAR_SUPABASE_URL
MIZAR_SUPABASE_SERVICE_ROLE_KEY
STRIPE_SECRET_KEY
"""

import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import requests
from supabase import create_client


SUPABASE_URL = os.environ.get("MIZAR_SUPABASE_URL")
SUPABASE_KEY = os.environ.get("MIZAR_SUPABASE_SERVICE_ROLE_KEY")
STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY")

STRIPE_API_BASE = "https://api.stripe.com/v1"


def require_env() -> None:
    missing = [
        name for name, value in {
            "MIZAR_SUPABASE_URL": SUPABASE_URL,
            "MIZAR_SUPABASE_SERVICE_ROLE_KEY": SUPABASE_KEY,
            "STRIPE_SECRET_KEY": STRIPE_SECRET_KEY,
        }.items()
        if not value
    ]

    if missing:
        print(f"ERROR: Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def unix_now() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def stripe_headers(idempotency_key: Optional[str] = None) -> Dict[str, str]:
    headers = {
        "Authorization": f"Bearer {STRIPE_SECRET_KEY}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    if idempotency_key:
        headers["Idempotency-Key"] = idempotency_key
    return headers


def get_metered_subscription_item(subscription_id: str) -> str:
    url = f"{STRIPE_API_BASE}/subscriptions/{subscription_id}"

    response = requests.get(
        url,
        headers=stripe_headers(),
        params={"expand[]": "items.data.price.recurring"},
        timeout=30,
    )

    if response.status_code >= 400:
        raise RuntimeError(f"Stripe subscription fetch failed: {response.status_code} {response.text}")

    subscription = response.json()
    items = subscription.get("items", {}).get("data", [])

    metered_items = []
    for item in items:
        recurring = item.get("price", {}).get("recurring") or {}
        if recurring.get("usage_type") == "metered":
            metered_items.append(item)

    if not metered_items:
        raise RuntimeError(
            f"No metered subscription item found for subscription {subscription_id}"
        )

    if len(metered_items) > 1:
        raise RuntimeError(
            f"Multiple metered subscription items found for subscription {subscription_id}; refusing to guess"
        )

    return metered_items[0]["id"]


def report_usage_to_stripe(
    subscription_item_id: str,
    quantity: int,
    idempotency_key: str,
) -> str:
    url = f"{STRIPE_API_BASE}/subscription_items/{subscription_item_id}/usage_records"

    response = requests.post(
        url,
        headers=stripe_headers(idempotency_key=idempotency_key),
        data={
            "quantity": str(quantity),
            "timestamp": str(unix_now()),
            "action": "set",
        },
        timeout=30,
    )

    if response.status_code >= 400:
        raise RuntimeError(f"Stripe usage report failed: {response.status_code} {response.text}")

    usage_record = response.json()
    return usage_record.get("id", "")


def count_actual_usage(supabase: Any, user_id: str, period_start: str, period_end: str) -> int:
    result = (
        supabase.table("api_usage")
        .select("user_id", count="exact")
        .eq("user_id", user_id)
        .gte("created_at", period_start)
        .lt("created_at", period_end)
        .execute()
    )

    return int(result.count or 0)


def write_billing_period(
    supabase: Any,
    user_id: str,
    tier: str,
    period_start: str,
    period_end: str,
    actual_usage: int,
    billable_usage: int,
    reported_to_stripe: bool,
    stripe_usage_record_id: Optional[str],
) -> None:
    payload = {
        "user_id": user_id,
        "tier": tier,
        "period_start": period_start,
        "period_end": period_end,
        "actual_usage": actual_usage,
        "billable_usage": billable_usage,
        "reported_to_stripe": reported_to_stripe,
        "reported_at": utc_now_iso(),
        "stripe_usage_record_id": stripe_usage_record_id,
    }

    (
        supabase.table("billing_periods")
        .upsert(payload, on_conflict="user_id,period_start")
        .execute()
    )


def process_user(supabase: Any, user: Dict[str, Any]) -> Tuple[bool, str]:
    user_id = user.get("user_id")
    tier = user.get("tier")
    stripe_subscription_id = user.get("stripe_subscription_id")
    period_start = user.get("period_start")
    period_end = user.get("period_end")

    if not user_id:
        return False, "missing user_id"

    if not tier:
        return False, f"user {user_id}: missing tier"

    if tier != "payg":
        return False, f"user {user_id}: unsupported tier {tier}; PAYG only"

    if not stripe_subscription_id:
        return False, f"user {user_id}: missing stripe_subscription_id"

    if not period_start or not period_end:
        return False, f"user {user_id}: missing period_start or period_end"

    actual_usage = count_actual_usage(
        supabase=supabase,
        user_id=user_id,
        period_start=period_start,
        period_end=period_end,
    )

    billable_usage = actual_usage

    print(
        f"user={user_id} tier={tier} actual_usage={actual_usage} "
        f"billable_usage={billable_usage} period={period_start} to {period_end}"
    )

    idempotency_key = f"{user_id}_{period_start}"

    try:
        subscription_item_id = get_metered_subscription_item(stripe_subscription_id)
        stripe_usage_record_id = report_usage_to_stripe(
            subscription_item_id=subscription_item_id,
            quantity=billable_usage,
            idempotency_key=idempotency_key,
        )

        write_billing_period(
            supabase=supabase,
            user_id=user_id,
            tier=tier,
            period_start=period_start,
            period_end=period_end,
            actual_usage=actual_usage,
            billable_usage=billable_usage,
            reported_to_stripe=True,
            stripe_usage_record_id=stripe_usage_record_id,
        )

        return True, f"user {user_id}: reported usage_record={stripe_usage_record_id}"

    except Exception as exc:
        print(f"ERROR: user {user_id}: {exc}")

        write_billing_period(
            supabase=supabase,
            user_id=user_id,
            tier=tier,
            period_start=period_start,
            period_end=period_end,
            actual_usage=actual_usage,
            billable_usage=billable_usage,
            reported_to_stripe=False,
            stripe_usage_record_id=None,
        )

        return False, f"user {user_id}: failed but continued"


def main() -> None:
    require_env()

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    print("MIZAR PAYG usage reporter starting")

    result = (
        supabase.table("user_tiers")
        .select(
            "user_id,tier,subscription_status,stripe_subscription_id,"
            "stripe_customer_id,monthly_limit,period_start,period_end"
        )
        .eq("tier", "payg")
        .eq("subscription_status", "active")
        .execute()
    )

    users = result.data or []

    print(f"Found {len(users)} active PAYG subscriber(s)")

    successes = 0
    failures = 0

    for user in users:
        ok, message = process_user(supabase, user)
        print(message)

        if ok:
            successes += 1
        else:
            failures += 1

    print("MIZAR PAYG usage reporter complete")
    print(f"Successes: {successes}")
    print(f"Failures: {failures}")

    sys.exit(0)


if __name__ == "__main__":
    main()