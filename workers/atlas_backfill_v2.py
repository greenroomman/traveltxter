import os
import sys
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
from atlas_features_v2 import (
    add_calendar_features,
    compute_baseline,
    compute_momentum,
    compute_fuel_velocity,
)
from supabase import create_client

SUPABASE_URL = os.environ["MIZAR_SUPABASE_URL"]
SUPABASE_KEY = os.environ["MIZAR_SUPABASE_SERVICE_ROLE_KEY"]
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

FEATURE_COLS = [
    "season_bucket",
    "days_to_next_bank_holiday",
    "trip_overlaps_holiday",
    "holiday_intensity_score",
    "price_z_score",
    "price_percentile",
    "price_ratio",
    "baseline_mu",
    "baseline_sigma",
    "trend_3d",
    "trend_7d",
    "volatility_7d",
    "direction_consistency_7d",
    "jet_fuel_7d_change_pct",
]

print("Fetching snapshots from Supabase...")
rows = []
page_size = 1000
offset = 0
while True:
    batch = (
        supabase.table("snapshots")
        .select(
            "snapshot_id,snapshot_date,origin_iata,destination_iata,"
            "outbound_date,return_date,dtd,price_gbp,jet_fuel_usd_gal"
        )
        .range(offset, offset + page_size - 1)
        .execute()
    )
    if not batch.data:
        break
    rows.extend(batch.data)
    print(f"  Fetched {len(rows)} rows so far...")
    if len(batch.data) < page_size:
        break
    offset += page_size

print(f"Total rows loaded: {len(rows)}")
if len(rows) == 0:
    print("ERROR: No rows fetched. Check Supabase credentials.")
    sys.exit(1)

df = pd.DataFrame(rows)
print(f"Columns available: {list(df.columns)}")

print("Computing calendar features...")
df = add_calendar_features(df)
print(f"  season_bucket sample: {df['season_bucket'].value_counts().to_dict()}")

print("Computing price position features...")
df = compute_baseline(df)
print(f"  price_z_score non-null: {df['price_z_score'].notna().sum()}")

print("Computing momentum features...")
df = compute_momentum(df)
print(f"  trend_7d non-null: {df['trend_7d'].notna().sum()}")

print("Computing fuel velocity features...")
df = compute_fuel_velocity(df)
print(f"  jet_fuel_7d_change_pct non-null: {df['jet_fuel_7d_change_pct'].notna().sum()}")

print(f"\nStarting batch upsert — {len(df)} rows in batches of 500...")

def clean_val(val, col):
    if col == "trip_overlaps_holiday":
        if pd.isna(val) if not isinstance(val, bool) else False:
            return None
        return bool(val)
    if col == "season_bucket":
        return None if (pd.isna(val) if not isinstance(val, str) else False) else str(val)
    if col == "days_to_next_bank_holiday":
        return None if pd.isna(val) else int(val)
    try:
        f = float(val)
        return None if np.isnan(f) else round(f, 6)
    except (TypeError, ValueError):
        return None

updated = 0
errors = 0
batch_size = 500

for i in range(0, len(df), batch_size):
    chunk = df.iloc[i : i + batch_size]
    records = []
    for _, row in chunk.iterrows():
        record = {"snapshot_id": row["snapshot_id"]}
        for col in FEATURE_COLS:
            if col in df.columns:
                record[col] = clean_val(row[col], col)
        records.append(record)
    try:
        supabase.table("snapshots").upsert(
            records, on_conflict="snapshot_id"
        ).execute()
        updated += len(records)
        print(f"  Upserted {updated}/{len(df)} rows")
    except Exception as e:
        errors += 1
        print(f"  ERROR on batch {i}-{i+batch_size}: {e}")

print(f"\nDone. Upserted: {updated} | Batch errors: {errors}")

print("\nVerifying coverage in Supabase...")
r1 = supabase.table("snapshots").select("snapshot_id", count="exact").not_.is_("price_z_score", "null").execute()
r2 = supabase.table("snapshots").select("snapshot_id", count="exact").not_.is_("season_bucket", "null").execute()
r3 = supabase.table("snapshots").select("snapshot_id", count="exact").not_.is_("trend_7d", "null").execute()
print(f"  price_z_score populated: {r1.count}")
print(f"  season_bucket populated: {r2.count}")
print(f"  trend_7d populated:      {r3.count}")
print(f"  Total rows:              {len(df)}")
