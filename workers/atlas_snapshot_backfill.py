#!/usr/bin/env python3
"""
workers/atlas_snapshot_backfill.py
ATLAS SNAPSHOT BACKFILL v3.3 (Route-Level Matching)

FIXES FROM v3.2:
- BUG: .neq("training_action", "exclude") silently drops rows where
  training_action IS NULL in PostgREST — NULL != 'exclude' evaluates
  to NULL, not TRUE, so all 374 genuinely pending rows were filtered out.
  FIX: replaced with .or_("training_action.is.null,training_action.neq.exclude")
  which correctly fetches rows where training_action is NULL or not 'exclude'.

FIXES FROM v3.1:
- BUG: fetch_unlabeled_snapshots pulled permanently unresolvable rows
  (routes dropped from pipeline with no t+7 snapshot ever written).
  These rows blocked every batch — 3,346 rows marked training_action='exclude'
  via direct SQL on 30 March 2026 after diagnosis.
  FIX: filter training_action != 'exclude' at fetch time (see above for
  correct PostgREST syntax using .or_()).
- Changed batch order to DESC (newest-first) so fresh rows with t+7 data
  immediately available are processed first.

FIXES FROM v3.0:
- BUG: build_route_price_index fetched without a row limit, causing silent
  truncation by the Supabase client default page size. Result: 524 index
  entries for 10 dates when 1500+ existed — 0 matches every run.
  FIX: added pagination loop with PAGE_SIZE=1000.
- BUG: fetch_unlabeled_snapshots fetched crisis-contaminated rows, wasting
  batch capacity on rows that should never be labelled.
  FIX: filter crisis_label_contaminated IS NOT TRUE at fetch time.
- BUG: .not_.is_("price_gbp", "null") has inconsistent behaviour across
  supabase-py versions.
  FIX: replaced with .gt("price_gbp", 0) — equivalent and reliable.

WHAT IT DOES:
- Runs daily after atlas_snapshot_capture.py
- Backfills price_t7, rose_10pct, fell_10pct labels
- Matches by route (origin+dest) and snapshot_date+7, using min price
- Only updates rows where price_t7 is NULL (idempotent)
- Processes newest-first in batches of 500 rows
- Skips crisis-contaminated rows and permanently excluded rows

NOTE ON training_action = 'exclude':
- Set via direct SQL for routes where no t+7 snapshot ever existed
  (pipeline route churn — route captured briefly then dropped)
- These rows have rose_10pct IS NULL so they are already excluded from
  training queries — this flag is backfill-only, not a training filter
- Do NOT add training_action filters to model training queries
- Training filter remains: crisis_label_contaminated IS NOT TRUE
  AND rose_10pct IS NOT NULL
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
    Excludes:
    - crisis-contaminated rows (crisis_label_contaminated IS TRUE)
    - permanently unresolvable rows (training_action = 'exclude')
    Ordered newest-first (DESC) so fresh rows with t+7 data available
    are processed before older rows that may still be pending.

    IMPORTANT: .neq("training_action", "exclude") silently drops NULL rows
    in PostgREST. Must use .or_() to correctly express IS NULL OR != 'exclude'.
    """
    cutoff = str(dt.date.today() - dt.timedelta(days=7))

    result = (
        supabase.table("snapshots")
        .select("snapshot_id, origin_iata, destination_iata, snapshot_date, price_gbp, crisis_label_contaminated")
        .is_("price_t7", "null")
        .lte("snapshot_date", cutoff)
        .or_("training_action.is.null,training_action.neq.exclude")
        .order("snapshot_date", desc=True)
        .limit(batch_size)
        .execute()
    )

    snapshots = []
    skipped_crisis = 0

    for row in result.data:
        if row.get("crisis_label_contaminated") is True:
            skipped_crisis += 1
            continue
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

    if skipped_crisis:
        print(f"  ℹ️  Skipped {skipped_crisis} crisis-contaminated rows in this batch")

    return snapshots


def build_route_price_index(
    supabase: Client,
    target_dates: List[dt.date]
) -> Dict[Tuple[str, str, str], float]:
    """
    For each target snapshot_date, fetch the MIN price per route.
    Returns a dict keyed by (origin_iata, destination_iata, snapshot_date_str) -> min_price.

    Paginated to avoid silent truncation by Supabase client defaults.
    Uses .gt("price_gbp", 0) for reliable cross-version behaviour.
    """
    index = {}
    PAGE_SIZE = 1000

    for target_date in target_dates:
        target_str = str(target_date)
        offset = 0
        rows_this_date = 0

        while True:
            try:
                result = (
                    supabase.table("snapshots")
                    .select("origin_iata, destination_iata, price_gbp")
                    .eq("snapshot_date", target_str)
                    .gt("price_gbp", 0)
                    .limit(PAGE_SIZE)
                    .offset(offset)
                    .execute()
                )

                batch = result.data
                if not batch:
                    break

                for row in batch:
                    if not row.get("price_gbp"):
                        continue
                    key = (row["origin_iata"], row["destination_iata"], target_str)
                    price = float(row["price_gbp"])
                    if key not in index or price < index[key]:
                        index[key] = price

                rows_this_date += len(batch)

                if len(batch) < PAGE_SIZE:
                    break

                offset += PAGE_SIZE

            except Exception as ex:
                print(f"  ⚠️  Failed to fetch prices for {target_date} (offset {offset}): {ex}")
                break

        if rows_this_date == 0:
            print(f"  ⚠️  No price data found for target date {target_date}")

    return index


def backfill_t7(supabase: Client):
    print("\n" + "=" * 70)
    print("BACKFILL t+7 LABELS")
    print("=" * 70)

    snapshots = fetch_unlabeled_snapshots(supabase, batch_size=500)
    print(f"Found {len(snapshots)} snapshots ready for t+7 backfill")

    if not snapshots:
        print("✅ Nothing to backfill")
        return

    target_dates = sorted(set(
        snap.snapshot_date + dt.timedelta(days=7)
        for snap in snapshots
    ))
    print(f"Fetching prices for {len(target_dates)} target dates: "
          f"{target_dates[0]} → {target_dates[-1]}")

    price_index = build_route_price_index(supabase, target_dates)
    print(f"Built price index: {len(price_index)} route/date combinations")

    if len(price_index) == 0:
        print("❌ Price index is empty — no t+7 snapshots exist yet for these dates. Try again tomorrow.")
        return

    updates = []
    no_match = 0
    no_price = 0

    for snap in snapshots:
        if snap.price_gbp is None:
            no_price += 1
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

    print(f"Matched: {len(updates)} | No t+7 snapshot found: {no_match} | No source price: {no_price}")

    if no_match > 0 and len(updates) == 0:
        print("❌ 0 matches — t+7 snapshots may not yet exist for these dates.")
        covered = set(k[2] for k in price_index.keys())
        needed = set(str(s.snapshot_date + dt.timedelta(days=7)) for s in snapshots)
        missing = needed - covered
        if missing:
            print(f"   Missing t+7 coverage for dates: {sorted(missing)}")
        return

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
    print(f"   rose_10pct=True:  {sum(1 for u in updates if u['rose_10pct'])}")
    print(f"   fell_10pct=True:  {sum(1 for u in updates if u['fell_10pct'])}")


def main():
    print("=" * 70)
    print("ATLAS SNAPSHOT BACKFILL v3.3 (Route-Level Matching)")
    print("=" * 70)

    supabase = init_supabase()
    print(f"✅ Connected to Supabase")
    print(f"   Today: {dt.date.today()}")
    print(f"   Backfill cutoff: rows with snapshot_date <= {dt.date.today() - dt.timedelta(days=7)}")

    backfill_t7(supabase)

    print("\n" + "=" * 70)
    print("✅ Backfill complete")
    print("=" * 70)


if __name__ == "__main__":
    main()
