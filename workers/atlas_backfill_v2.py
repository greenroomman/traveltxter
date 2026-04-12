import os
import sys
import pandas as pd

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

df = pd.DataFrame(rows)
print(f"Total rows loaded: {len(df)}")

print("Computing calendar features...")
df = add_calendar_features(df)

print("Computing price position features...")
df = compute_baseline(df)

print("Computing momentum features...")
df = compute_momentum(df)

print("Computing fuel velocity features...")
df = compute_fuel_velocity(df)

print("Writing features back to Supabase...")
updated = 0
errors = 0
for i in range(0, len(df), 200):
    batch = df.iloc[i : i + 200]
    for _, row in batch.iterrows():
        update_data = {}
        for col in FEATURE_COLS:
            if col not in df.columns:
                continue
            val = row.get(col)
            if pd.isna(val) if not isinstance(val, (str, bool)) else False:
                update_data[col] = None
            elif col == "trip_overlaps_holiday":
                update_data[col] = bool(val)
            elif col == "season_bucket":
                update_data[col] = str(val)
            elif col == "days_to_next_bank_holiday":
                update_data[col] = int(val)
            else:
                try:
                    update_data[col] = round(float(val), 4)
                except (TypeError, ValueError):
                    update_data[col] = None
        try:
            supabase.table("snapshots").update(update_data).eq(
                "snapshot_id", row["snapshot_id"]
            ).execute()
            updated += 1
        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  Error on {row['snapshot_id']}: {e}")
    print(f"  Progress: {min(i + 200, len(df))}/{len(df)} rows processed")

print(f"\nDone. Updated: {updated} | Errors: {errors}")

print("\nVerifying coverage...")
result = supabase.table("snapshots").select(
    "snapshot_id",
    count="exact"
).not_.is_("price_z_score", "null").execute()
print(f"Rows with price_z_score populated: {result.count}")

result2 = supabase.table("snapshots").select(
    "snapshot_id",
    count="exact"
).not_.is_("trend_7d", "null").execute()
print(f"Rows with trend_7d populated: {result2.count}")
