#!/usr/bin/env python3
"""
workers/atlas_snapshot_backfill.py
ATLAS SNAPSHOT BACKFILL — v2.0 (Supabase Migration)

Built against Atlas Snapshot Oilpan v1.0.

WHAT CHANGED FROM v1.1:
- **MIGRATED TO SUPABASE** — reads/writes PostgreSQL instead of Google Sheets
- Replaces gspread with supabase-py client
- Same logic, different storage layer
- Batch updates use database transactions (faster)
- Requires SUPABASE_URL and SUPABASE_KEY env vars

WHAT IT DOES:
- Runs daily after atlas_snapshot_capture.py
- Backfills price_t7, price_t14, rose_10pct, fell_10pct labels
- Matches snapshots by origin, dest, outbound_date, return_date
- Only updates rows where labels are NULL (idempotent)

SCHEMA CONTRACT:
- Reads: snapshot_id, origin_iata, destination_iata, outbound_date, return_date, snapshot_date, price_gbp
- Writes: price_t7, price_t14, rose_10pct, fell_10pct

OILPAN CONTRACT:
- Stateless — no memory between runs
- Updates existing rows only (never creates new rows)
- Safe to run multiple times (won't overwrite existing labels)
"""

from __future__ import annotations

import os
import datetime as dt
from typing import Dict, List, Optional
from dataclasses import dataclass

from supabase import create_client, Client


# ─────────────────────────────────────────────
# ENV HELPERS
# ─────────────────────────────────────────────

def env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


# ─────────────────────────────────────────────
# SUPABASE CONNECTION
# ─────────────────────────────────────────────

def init_supabase() -> Client:
    """Initialize Supabase client from env vars."""
    url = env_str("SUPABASE_URL")
    key = env_str("SUPABASE_KEY")
    
    if not url or not key:
        raise ValueError(
            "Missing Supabase credentials. Set SUPABASE_URL and SUPABASE_KEY env vars.\n"
            "Get these from: https://supabase.com/dashboard/project/<project_id>/settings/api"
        )
    
    return create_client(url, key)


# ─────────────────────────────────────────────
# SNAPSHOT MATCHING
# ─────────────────────────────────────────────

@dataclass
class Snapshot:
    snapshot_id: str
    origin_iata: str
    destination_iata: str
    outbound_date: dt.date
    return_date: dt.date
    snapshot_date: dt.date
    price_gbp: Optional[float]


def fetch_unlabeled_snapshots(supabase: Client, label_type: str) -> List[Snapshot]:
    """
    Fetch snapshots that need backfill labels.
    
    Args:
        label_type: 't7' or 't14'
    
    Returns list of Snapshot objects where the label is NULL.
    """
    column = "price_t7" if label_type == "t7" else "price_t14"
    
    result = supabase.table('snapshots').select(
        'snapshot_id, origin_iata, destination_iata, outbound_date, return_date, snapshot_date, price_gbp'
    ).is_(column, 'null').execute()
    
    snapshots = []
    for row in result.data:
        try:
            snapshots.append(Snapshot(
                snapshot_id=row['snapshot_id'],
                origin_iata=row['origin_iata'],
                destination_iata=row['destination_iata'],
                outbound_date=dt.datetime.strptime(row['outbound_date'], '%Y-%m-%d').date(),
                return_date=dt.datetime.strptime(row['return_date'], '%Y-%m-%d').date(),
                snapshot_date=dt.datetime.strptime(row['snapshot_date'], '%Y-%m-%d').date(),
                price_gbp=float(row['price_gbp']) if row.get('price_gbp') else None
            ))
        except Exception as ex:
            print(f"⚠️  Skipping malformed row {row.get('snapshot_id')}: {ex}")
    
    return snapshots


def find_matching_snapshot(
    supabase: Client,
    origin: str,
    dest: str,
    outbound: dt.date,
    return_date: dt.date,
    target_date: dt.date
) -> Optional[float]:
    """
    Find a snapshot matching the route on the target date.
    Returns the price_gbp if found, None otherwise.
    """
    try:
        result = supabase.table('snapshots').select('price_gbp').eq(
            'origin_iata', origin
        ).eq(
            'destination_iata', dest
        ).eq(
            'outbound_date', str(outbound)
        ).eq(
            'return_date', str(return_date)
        ).eq(
            'snapshot_date', str(target_date)
        ).not_.is_('price_gbp', 'null').execute()
        
        if result.data:
            return float(result.data[0]['price_gbp'])
        return None
    
    except Exception as ex:
        print(f"⚠️  Match query failed: {ex}")
        return None


# ─────────────────────────────────────────────
# LABEL CALCULATION
# ─────────────────────────────────────────────

def calculate_labels(original_price: float, future_price: float) -> Dict[str, bool]:
    """
    Calculate rose_10pct and fell_10pct flags.
    
    Returns dict with 'rose_10pct' and 'fell_10pct' booleans.
    """
    if not original_price or not future_price:
        return {"rose_10pct": False, "fell_10pct": False}
    
    pct_change = ((future_price - original_price) / original_price) * 100
    
    return {
        "rose_10pct": pct_change >= 10.0,
        "fell_10pct": pct_change <= -10.0
    }


# ─────────────────────────────────────────────
# MAIN BACKFILL LOGIC
# ─────────────────────────────────────────────

def backfill_t7(supabase: Client):
    """Backfill t+7 labels."""
    print("\n" + "=" * 70)
    print("BACKFILL t+7 LABELS")
    print("=" * 70)
    
    snapshots = fetch_unlabeled_snapshots(supabase, 't7')
    print(f"Found {len(snapshots)} snapshots needing t+7 labels")
    
    if not snapshots:
        print("✅ No snapshots to backfill")
        return
    
    updates = []
    
    for snap in snapshots:
        target_date = snap.snapshot_date + dt.timedelta(days=7)
        
        # Can't backfill if target date is in the future
        if target_date > dt.date.today():
            continue
        
        # Find matching snapshot at t+7
        future_price = find_matching_snapshot(
            supabase,
            snap.origin_iata,
            snap.destination_iata,
            snap.outbound_date,
            snap.return_date,
            target_date
        )
        
        if future_price is None:
            continue
        
        # Calculate labels
        labels = calculate_labels(snap.price_gbp, future_price) if snap.price_gbp else {"rose_10pct": False, "fell_10pct": False}
        
        updates.append({
            "snapshot_id": snap.snapshot_id,
            "price_t7": future_price,
            "rose_10pct": labels["rose_10pct"],
            "fell_10pct": labels["fell_10pct"]
        })
    
    # Write updates
    if updates:
        print(f"📥 Writing {len(updates)} t+7 labels...")
        
        for update in updates:
            try:
                supabase.table('snapshots').update({
                    "price_t7": update["price_t7"],
                    "rose_10pct": update["rose_10pct"],
                    "fell_10pct": update["fell_10pct"]
                }).eq('snapshot_id', update['snapshot_id']).execute()
            except Exception as ex:
                print(f"⚠️  Update failed for {update['snapshot_id']}: {ex}")
        
        print(f"✅ Updated {len(updates)} rows")
    else:
        print("✅ No snapshots ready for t+7 backfill")


def backfill_t14(supabase: Client):
    """Backfill t+14 labels."""
    print("\n" + "=" * 70)
    print("BACKFILL t+14 LABELS")
    print("=" * 70)
    
    snapshots = fetch_unlabeled_snapshots(supabase, 't14')
    print(f"Found {len(snapshots)} snapshots needing t+14 labels")
    
    if not snapshots:
        print("✅ No snapshots to backfill")
        return
    
    updates = []
    
    for snap in snapshots:
        target_date = snap.snapshot_date + dt.timedelta(days=14)
        
        # Can't backfill if target date is in the future
        if target_date > dt.date.today():
            continue
        
        # Find matching snapshot at t+14
        future_price = find_matching_snapshot(
            supabase,
            snap.origin_iata,
            snap.destination_iata,
            snap.outbound_date,
            snap.return_date,
            target_date
        )
        
        if future_price is None:
            continue
        
        updates.append({
            "snapshot_id": snap.snapshot_id,
            "price_t14": future_price
        })
    
    # Write updates
    if updates:
        print(f"📥 Writing {len(updates)} t+14 labels...")
        
        for update in updates:
            try:
                supabase.table('snapshots').update({
                    "price_t14": update["price_t14"]
                }).eq('snapshot_id', update['snapshot_id']).execute()
            except Exception as ex:
                print(f"⚠️  Update failed for {update['snapshot_id']}: {ex}")
        
        print(f"✅ Updated {len(updates)} rows")
    else:
        print("✅ No snapshots ready for t+14 backfill")


def main():
    print("=" * 70)
    print("ATLAS SNAPSHOT BACKFILL v2.0 (Supabase)")
    print("=" * 70)
    
    # Initialize Supabase
    supabase = init_supabase()
    print(f"✅ Connected to Supabase: {env_str('SUPABASE_URL')}")
    
    # Run backfills
    backfill_t7(supabase)
    backfill_t14(supabase)
    
    print("\n" + "=" * 70)
    print("✅ Backfill complete")
    print("=" * 70)


if __name__ == "__main__":
    main()
