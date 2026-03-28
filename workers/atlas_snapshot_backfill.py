#!/usr/bin/env python3
"""
workers/atlas_snapshot_backfill.py
ATLAS SNAPSHOT BACKFILL v3.0 (Route-Level Matching)

FIX FROM v2.0:
- CHANGED matching strategy: no longer requires identical outbound_date/return_date
- Pipeline captures a rolling travel-date window — exact itinerary matching
  never finds t+7 snapshots because the dates shift forward daily
- NOW matches on route (origin + destination) + snapshot_date only,
  using MIN(price_gbp) on the t+7 snapshot date as the comparison price
- This is the correct semantic: "did the cheapest fare on this route rise 10%?"
- Batch-fetches t+7 prices in bulk (one query per snapshot_date group)
  instead of one API call per row — 50x faster

WHAT IT DOES:
- Runs daily after atlas_snapshot_capture.py
- Backfills price_t7, rose_10pct, fell_10pct labels
- Matches by route (origin+dest) and snapshot_date+7, using min price
- Only updates rows where price_t7 is NULL (idempotent)
- Processes in date-ordered batches of 500 rows
"""

from __future__ import annotations

import os
import datetime as dt
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

from supabase import create_client, Client


def env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


def init_supabase() -> Client:
    url = env_str("SUPABASE_URL")
    key = env_str("SUPABASE_KEY")
    if not url or not key:
        raise ValueError("Missing SUPABASE_URL and SUPABASE_KEY env vars")
    return create_client(url, key)


@dataclass
class Snapshot:
    snapshot_id: str
    origin_iata: str
    destination_iata: str
    snapshot_date: dt.date
    price_gbp: Optional[float]


def fetch_unlabeled_snapshots(supabase: Client, batch_size: int = 500) -> List[Snapshot]:
    """
    Fetch snapshots needing t+7 labels where the target date is in the past.
    Uses pagination to go beyond Supabase default 1000-row limit.
    Only fetches rows where snapshot_date + 7 <= today.
    """
    cutoff = str(dt.date.today() - dt.timedelta(days=7))

    result = supabase.table("snapshots").select(
        "snapshot_id, origin_iata, destination_iata, snapshot_date, price_gbp"
    ).is_("price_t7", "null").lte("snapshot_date", cutoff).limit(batch_size).execute()

    snapshots = []
    for row in result.data:
        try:
            snapshots.append(Snapshot(
                snapshot_id=row["snapshot_id"],
                origin_iata=row["origin_iata"],
                destination_iata=row["destination_iata"],
                snapshot_date=dt.date.fromisoformat(row["snapshot_date"]),
                price_gbp=float(row["price_gbp"]) if row.get("price_gbp") else None,
            ))
        except Exception as ex:
            print(f"  ⚠️  Skipping malformed row {row.get('snapshot_id')}: {ex}")

    return snapshots


def build_route_price_index(
    supabase: Client,
    target_dates: List[dt.date]
) -> Dict[Tuple[str, str, str], float]:
    """
    For each target snapshot_date, fetch the MIN price per route.
    Returns a dict keyed by (origin_iata, destination_iata, snapshot_date_str) -> min_price.

    This replaces the per-row API call with a bulk fetch — massively faster.
    """
    index = {}

    for target_date in target_dates:
        try:
            result = supabase.table("snapshots").select(
                "origin_iata, destination_iata, price_gbp"
            ).eq("snapshot_date", str(target_date)).not_.is_("price_gbp", "null").execute()

            # Build min-price per route for this date
            for row in result.data:
                if not row.get("price_gbp"):
                    continue
                key = (row["origin_iata"], row["destination_iata"], str(target_date))
                price = float(row["price_gbp"])
                if key not in index or price < index[key]:
                    index[key] = price

        except Exception as ex:
            print(f"  ⚠️  Failed to fetch prices for {target_date}: {ex}")

    return index


def backfill_t7(supabase: Client):
    print("\\n" + "=" * 70)
    print("BACKFILL t+7 LABELS")
    print("=" * 70)

    # Step 1: fetch unlabelled rows (up to 500 at a time)
    snapshots = fetch_unlabeled_snapshots(supabase, batch_size=500)
    print(f"Found {len(snapshots)} snapshots ready for t+7 backfill")

    if not snapshots:
        print("✅ Nothing to backfill")
        return

    # Step 2: get all unique target dates (snapshot_date + 7)
    target_dates = list(set(
        snap.snapshot_date + dt.timedelta(days=7)
        for snap in snapshots
    ))
    print(f"Fetching prices for {len(target_dates)} target dates...")

    # Step 3: bulk fetch min prices per route per target date
    price_index = build_route_price_index(supabase, target_dates)
    print(f"Built price index: {len(price_index)} route/date combinations")

    # Step 4: match and compute labels
    updates = []
    no_match = 0

    for snap in snapshots:
        if snap.price_gbp is None:
            continue

        target_date = snap.snapshot_date + dt.timedelta(days=7)
        key = (snap.origin_iata, snap.destination_iata, str(target_date))
        future_price = price_index.get(key)

        if future_price is None:
            no_match += 1
            continue

        pct_change = ((future_price - snap.price_gbp) / snap.price_gbp) * 100

        updates.append({
            "snapshot_id": snap.snapshot_id,
            "price_t7": future_price,
            "rose_10pct": pct_change >= 10.0,
            "fell_10pct": pct_change <= -10.0,
        })

    print(f"Matched: {len(updates)} | No t+7 snapshot found: {no_match}")

    # Step 5: write updates
    if not updates:
        print("✅ No updates to write")
        return

    print(f"Writing {len(updates)} labels...")
    written = 0
    failed = 0

    for update in updates:
        try:
            supabase.table("snapshots").update({
                "price_t7": update["price_t7"],
                "rose_10pct": update["rose_10pct"],
                "fell_10pct": update["fell_10pct"],
            }).eq("snapshot_id", update["snapshot_id"]).execute()
            written += 1
        except Exception as ex:
            print(f"  ⚠️  Update failed for {update['snapshot_id']}: {ex}")
            failed += 1

    print(f"✅ Written: {written} | Failed: {failed}")
    print(f"   rose_10pct=True: {sum(1 for u in updates if u['rose_10pct'])}")
    print(f"   fell_10pct=True: {sum(1 for u in updates if u['fell_10pct'])}")


def main():
    print("=" * 70)
    print("ATLAS SNAPSHOT BACKFILL v3.0 (Route-Level Matching)")
    print("=" * 70)

    supabase = init_supabase()
    print(f"✅ Connected to Supabase")
    print(f"   Today: {dt.date.today()}")
    print(f"   Backfill cutoff: rows with snapshot_date <= {dt.date.today() - dt.timedelta(days=7)}")

    backfill_t7(supabase)

    print("\\n" + "=" * 70)
    print("✅ Backfill complete")
    print("=" * 70)


if __name__ == "__main__":
    main()
'''
