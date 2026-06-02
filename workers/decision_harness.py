#!/usr/bin/env python3
"""
decision_harness.py
MIZAR Atlas - Snapshot-matched Decision Harness

Purpose:
Creates real /v1/signal decisions from actual snapshot rows.

Core rules:
- Every harness decision must be created from a real snapshot row.
- No hardcoded outbound dates.
- No invented return dates.
- No synthetic prices.
- Cohort must be diversified across DTD buckets.

Run:
python3 decision_harness.py
"""

import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from collections import Counter

import httpx
import requests
from dotenv import load_dotenv
from supabase import create_client


load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
API_KEY = os.environ["MIZAR_API_KEY"]
API_BASE = os.environ.get("MIZAR_API_BASE", "https://mizar-api.vercel.app")

SUPABASE = create_client(SUPABASE_URL, SUPABASE_KEY)

REQUEST_SLEEP_SECONDS = float(os.environ.get("HARNESS_SLEEP_SECONDS", "0.3"))
MAX_ROWS = int(os.environ.get("HARNESS_MAX_ROWS", "500"))
HARNESS_LOOKBACK_DAYS = int(os.environ.get("HARNESS_LOOKBACK_DAYS", "10"))
BUCKET_TARGET = int(os.environ.get("HARNESS_BUCKET_TARGET", "125"))


def utc_now():
    return datetime.now(timezone.utc)


def clean_date(value):
    if value is None:
        return None
    return str(value)[:10]


def parse_date(value):
    value = clean_date(value)
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except Exception:
        return None


def dtd_bucket(outbound_date):
    outbound = parse_date(outbound_date)
    if not outbound:
        return None

    dtd = (outbound - utc_now().date()).days

    if 7 <= dtd <= 14:
        return "dtd_07_14"
    if 15 <= dtd <= 30:
        return "dtd_15_30"
    if 31 <= dtd <= 60:
        return "dtd_31_60"
    if 61 <= dtd <= 84:
        return "dtd_61_84"

    return None


def get_latest_snapshot_date():
    result = (
        SUPABASE.table("snapshots")
        .select("snapshot_date")
        .not_.is_("price_gbp", "null")
        .order("snapshot_date", desc=True)
        .limit(1)
        .execute()
    )

    rows = result.data or []
    if not rows:
        return None

    return clean_date(rows[0].get("snapshot_date"))


def fetch_snapshot_rows(snapshot_date):
    latest = parse_date(snapshot_date)
    if not latest:
        return []

    since = latest - timedelta(days=HARNESS_LOOKBACK_DAYS)

    query = (
        SUPABASE.table("snapshots")
        .select(
            "snapshot_id,"
            "snapshot_date,"
            "origin_iata,"
            "destination_iata,"
            "outbound_date,"
            "return_date,"
            "price_gbp,"
            "cabin_class,"
            "direct,"
            "stops,"
            "carrier_count,"
            "lcc_present"
        )
        .gte("snapshot_date", since.isoformat())
        .lte("snapshot_date", latest.isoformat())
        .not_.is_("price_gbp", "null")
        .not_.is_("outbound_date", "null")
        .order("snapshot_date", desc=True)
        .limit(max(MAX_ROWS * 4, 1000))
    )

    _max_attempts = 3
    _delays = [2, 4]
    result = None
    for _attempt in range(1, _max_attempts + 1):
        try:
            result = query.execute()
            break
        except (httpx.RemoteProtocolError, httpx.ConnectError) as e:
            if _attempt < _max_attempts:
                _delay = _delays[_attempt - 1]
                print(f"[WARN] fetch_snapshot_rows attempt {_attempt} failed: {e}. Retrying in {_delay}s...")
                time.sleep(_delay)
            else:
                print(f"[ERROR] fetch_snapshot_rows failed after {_max_attempts} attempts: {e}")
                raise

    rows = result.data or []

    buckets = {
        "dtd_07_14": [],
        "dtd_15_30": [],
        "dtd_31_60": [],
        "dtd_61_84": [],
    }

    route_seen = {bucket: Counter() for bucket in buckets}

    for row in rows:
        bucket = dtd_bucket(row.get("outbound_date"))
        if not bucket:
            continue

        route = f"{row.get('origin_iata')}->{row.get('destination_iata')}"
        if route_seen[bucket][route] >= 2:
            continue

        buckets[bucket].append(row)
        route_seen[bucket][route] += 1

    selected = []
    for bucket_name in ["dtd_07_14", "dtd_15_30", "dtd_31_60", "dtd_61_84"]:
        selected.extend(buckets[bucket_name][:BUCKET_TARGET])

    return selected[:MAX_ROWS]


def is_valid_snapshot_row(row):
    required = [
        "origin_iata",
        "destination_iata",
        "outbound_date",
        "price_gbp",
    ]

    for field in required:
        if row.get(field) in (None, ""):
            return False, f"missing {field}"

    try:
        price = float(row.get("price_gbp"))
    except Exception:
        return False, "invalid price_gbp"

    if price <= 0:
        return False, "non-positive price_gbp"

    if not dtd_bucket(row.get("outbound_date")):
        return False, "outside_dtd_window"

    return True, "ok"


def build_payload(row, session_id):
    return_date = clean_date(row.get("return_date"))

    payload = {
        "origin": row["origin_iata"],
        "destination": row["destination_iata"],
        "outbound_date": clean_date(row["outbound_date"]),
        "price_gbp": float(row["price_gbp"]),
        "session_id": session_id,
        "client_platform": "api",
        "decision_source_type": "harness",
    }

    if return_date:
        payload["return_date"] = return_date
        payload["trip_type"] = "return"
    else:
        payload["return_date"] = None
        payload["trip_type"] = "oneway"

    payload["cabin_class"] = row.get("cabin_class") or "economy"

    return payload


def call_signal(payload):
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
    }

    try:
        response = requests.post(
            f"{API_BASE}/v1/signal",
            json=payload,
            headers=headers,
            timeout=25,
        )
    except requests.RequestException as exc:
        return None, None, f"request_exception: {exc}"

    if response.status_code != 200:
        body = response.text[:500] if response.text else ""
        return None, None, f"http_{response.status_code}: {body}"

    try:
        data = response.json()
    except Exception as exc:
        return None, None, f"json_parse_error: {exc}"

    decision_id = data.get("decision_id")
    score = data.get("regret_risk_score")

    if not decision_id or score is None:
        return None, None, f"missing decision_id_or_score: {data}"

    return decision_id, float(score), None


def main():
    started_at = utc_now()
    print(f"MIZAR Decision Harness starting - {started_at.isoformat()}")
    print(f"API base: {API_BASE}")
    print("Mode: snapshot-matched, diversified DTD buckets")
    print(f"Lookback days: {HARNESS_LOOKBACK_DAYS}")
    print(f"Max rows: {MAX_ROWS}")
    print(f"Bucket target: {BUCKET_TARGET}")

    snapshot_date = get_latest_snapshot_date()

    if not snapshot_date:
        print("No usable snapshots found. Exiting.")
        sys.exit(1)

    rows = fetch_snapshot_rows(snapshot_date)

    if not rows:
        print(f"No usable diversified snapshot rows found up to {snapshot_date}. Exiting.")
        sys.exit(1)

    outbound_counts = Counter(clean_date(r.get("outbound_date")) for r in rows if r.get("outbound_date"))
    bucket_counts = Counter(dtd_bucket(r.get("outbound_date")) for r in rows if r.get("outbound_date"))
    route_counts = Counter(f"{r.get('origin_iata')}->{r.get('destination_iata')}" for r in rows)

    print(f"Latest snapshot date: {snapshot_date}")
    print(f"Snapshot rows selected: {len(rows)}")
    print(f"Distinct routes: {len(route_counts)}")
    print(f"Distinct outbound dates: {len(outbound_counts)}")

    print("DTD bucket distribution:")
    for bucket, count in sorted(bucket_counts.items()):
        print(f"  {bucket}: {count}")

    print("Outbound date distribution:")
    for date_value, count in sorted(outbound_counts.items()):
        print(f"  {date_value}: {count}")

    session_id = "harness_" + started_at.strftime("%Y%m%d_%H%M%S")

    success = 0
    failed = 0
    skipped = 0
    high_risk = 0
    scores = []
    failure_reasons = Counter()

    for i, row in enumerate(rows, start=1):
        valid, reason = is_valid_snapshot_row(row)
        origin = row.get("origin_iata")
        dest = row.get("destination_iata")
        outbound = clean_date(row.get("outbound_date"))
        ret = clean_date(row.get("return_date"))
        price = row.get("price_gbp")
        bucket = dtd_bucket(outbound)

        if not valid:
            skipped += 1
            failure_reasons[f"skipped_{reason}"] += 1
            print(
                f"  [{i}/{len(rows)}] {origin}->{dest} "
                f"out={outbound} bucket={bucket} return={ret} SKIPPED ({reason})"
            )
            continue

        payload = build_payload(row, session_id)
        decision_id, score, error = call_signal(payload)

        if decision_id and score is not None:
            success += 1
            scores.append(score)

            if score >= 0.45:
                high_risk += 1

            print(
                f"  [{i}/{len(rows)}] {origin}->{dest} "
                f"out={outbound} bucket={bucket} return={ret or 'oneway'} £{price} | "
                f"score={score:.3f} | id={str(decision_id)[:8]}..."
            )
        else:
            failed += 1
            failure_reasons[error or "unknown_failure"] += 1
            print(
                f"  [{i}/{len(rows)}] {origin}->{dest} "
                f"out={outbound} bucket={bucket} return={ret or 'oneway'} £{price} FAILED | {error}"
            )

        time.sleep(REQUEST_SLEEP_SECONDS)

    print()
    print("=" * 60)
    print(f"Harness complete - {utc_now().isoformat()}")
    print(f"  Latest snapshot date   : {snapshot_date}")
    print(f"  Snapshot rows selected : {len(rows)}")
    print(f"  Distinct routes        : {len(route_counts)}")
    print(f"  Distinct outbound dates: {len(outbound_counts)}")
    print(f"  Decisions generated    : {success}")
    print(f"  Skipped                : {skipped}")
    print(f"  Failed                 : {failed}")

    print()
    print("DTD bucket summary:")
    for bucket, count in sorted(bucket_counts.items()):
        print(f"  {bucket}: {count}")

    if scores:
        avg_score = sum(scores) / len(scores)
        print()
        print(f"  Avg RegretRisk score   : {avg_score:.3f}")
        print(f"  High-risk signals (v3 ≥0.45)      : {high_risk} ({round(high_risk / success * 100, 1)}%)")
        print(f"  Min score              : {min(scores):.3f}")
        print(f"  Max score              : {max(scores):.3f}")

    if failure_reasons:
        print()
        print("Failure / skip reasons:")
        for reason, count in failure_reasons.most_common():
            print(f"  {reason}: {count}")

    print("=" * 60)
    print()
    print("Verification will run automatically once decisions reach t+7.")
    print("Sanity check expected after run:")
    print("  user_decisions should show diversified outbound_date and DTD buckets.")
    print("  validation_eligible should be TRUE for API rows inside 7-84 DTD.")
    print()

    if success == 0:
        print("WARNING: No decisions were generated. Check API key, endpoint, and snapshot data.")
        sys.exit(1)

    if failed > 0:
        print(f"WARNING: {failed} live API call(s) failed. Check API logs.")
        sys.exit(1)


if __name__ == "__main__":
    main()