import os
import sys
import pandas as pd
import numpy as np
from datetime import date
from supabase import create_client

SUPABASE_URL = os.environ["MIZAR_SUPABASE_URL"]
SUPABASE_KEY = os.environ["MIZAR_SUPABASE_SERVICE_ROLE_KEY"]
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

UK_BANK_HOLIDAYS = [
    date(2025, 1, 1), date(2025, 4, 18), date(2025, 4, 21),
    date(2025, 5, 5), date(2025, 5, 26), date(2025, 8, 25),
    date(2025, 12, 25), date(2025, 12, 26),
    date(2026, 1, 1), date(2026, 4, 3), date(2026, 4, 6),
    date(2026, 5, 4), date(2026, 5, 25), date(2026, 8, 31),
    date(2026, 12, 25), date(2026, 12, 28),
    date(2027, 1, 1), date(2027, 3, 26), date(2027, 3, 29),
    date(2027, 5, 3), date(2027, 5, 31), date(2027, 8, 30),
    date(2027, 12, 27), date(2027, 12, 28),
]

FEATURE_COLS = [
    "season_bucket", "days_to_next_bank_holiday", "trip_overlaps_holiday",
    "holiday_intensity_score", "price_z_score", "price_percentile",
    "price_ratio", "baseline_mu", "baseline_sigma",
    "trend_3d", "trend_7d", "volatility_7d", "direction_consistency_7d",
    "jet_fuel_7d_change_pct",
]


def assign_season_bucket(d):
    if isinstance(d, str):
        d = date.fromisoformat(str(d)[:10])
    elif hasattr(d, 'date'):
        d = d.date()
    m, day = d.month, d.day
    if (m == 12 and day >= 20) or (m == 1 and day <= 5):
        return "christmas"
    if (m == 4 and 1 <= day <= 15) or (m == 3 and 24 <= day <= 31):
        return "easter"
    if (m == 7 and day >= 15) or m == 8 or (m == 9 and day <= 1):
        return "summer_peak"
    if (m == 1 and day >= 15) or m == 2 or (m == 3 and day <= 15):
        return "ski"
    if m == 2 and 14 <= day <= 21:
        return "half_term"
    if m == 10 and 19 <= day <= 30:
        return "half_term"
    if m in [4, 5, 6, 9, 10]:
        return "shoulder"
    return "off_peak"


def days_to_next_bh(d):
    if isinstance(d, str):
        d = date.fromisoformat(str(d)[:10])
    elif hasattr(d, 'date'):
        d = d.date()
    future = [h for h in UK_BANK_HOLIDAYS if h >= d]
    return (min(future) - d).days if future else 365


def trip_overlaps(outbound, ret):
    if isinstance(outbound, str):
        outbound = date.fromisoformat(str(outbound)[:10])
    elif hasattr(outbound, 'date'):
        outbound = outbound.date()
    if isinstance(ret, str):
        ret = date.fromisoformat(str(ret)[:10])
    elif hasattr(ret, 'date'):
        ret = ret.date()
    return any(outbound <= h <= ret for h in UK_BANK_HOLIDAYS)


def intensity(outbound):
    if isinstance(outbound, str):
        outbound = date.fromisoformat(str(outbound)[:10])
    elif hasattr(outbound, 'date'):
        outbound = outbound.date()
    season = assign_season_bucket(outbound)
    base = {
        "christmas": 0.95, "easter": 0.85, "summer_peak": 0.90,
        "half_term": 0.75, "ski": 0.70, "shoulder": 0.45, "off_peak": 0.20
    }.get(season, 0.30)
    if days_to_next_bh(outbound) <= 3:
        base = min(1.0, base + 0.15)
    return round(base, 3)


def clean_val(val, col):
    if col == "trip_overlaps_holiday":
        if not isinstance(val, bool) and pd.isna(val):
            return None
        return bool(val)
    if col == "season_bucket":
        if not isinstance(val, str) and pd.isna(val):
            return None
        return str(val)
    if col == "days_to_next_bank_holiday":
        if pd.isna(val):
            return None
        return int(val)
    if col == "price_percentile":
        try:
            f = float(val)
            return None if np.isnan(f) else round(min(f, 100.0), 2)
        except (TypeError, ValueError):
            return None
    try:
        f = float(val)
        return None if np.isnan(f) else round(f, 6)
    except (TypeError, ValueError):
        return None


print("Fetching snapshots...")
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
    print(f"  {len(rows)} rows fetched")
    if len(batch.data) < page_size:
        break
    offset += page_size

print(f"Total: {len(rows)}")
if not rows:
    print("ERROR: no rows")
    sys.exit(1)

df = pd.DataFrame(rows)
original_ids = set(df["snapshot_id"].dropna())
print(f"Unique snapshot_ids: {len(original_ids)}")

print("Calendar features...")
df["season_bucket"] = df["outbound_date"].apply(assign_season_bucket)
df["days_to_next_bank_holiday"] = df["outbound_date"].apply(days_to_next_bh)
df["trip_overlaps_holiday"] = df.apply(lambda r: trip_overlaps(r["outbound_date"], r["return_date"]), axis=1)
df["holiday_intensity_score"] = df["outbound_date"].apply(intensity)
print(f"  seasons: {df['season_bucket'].value_counts().to_dict()}")

print("Price position features...")
df["dtd_bucket"] = pd.cut(df["dtd"].astype(float), bins=[-1,7,21,60,120,9999], labels=["0-7","8-21","22-60","61-120","120+"])
df["route"] = df["origin_iata"] + "-" + df["destination_iata"]
baseline = df.groupby(["route","dtd_bucket","season_bucket"])["price_gbp"].agg(baseline_mu="mean", baseline_sigma="std").reset_index()
baseline["baseline_sigma"] = baseline["baseline_sigma"].fillna(10.0).clip(lower=5.0)
df = df.merge(baseline, on=["route","dtd_bucket","season_bucket"], how="left")
df["price_z_score"] = ((df["price_gbp"] - df["baseline_mu"]) / df["baseline_sigma"]).round(4)
df["price_ratio"] = (df["price_gbp"] / df["baseline_mu"]).round(4)

def pct_rank(group):
    group = group.copy()
    group["price_percentile"] = group["price_gbp"].rank(pct=True).mul(100).clip(upper=100.0).round(2)
    return group

df = df.groupby(["route","dtd_bucket","season_bucket"], group_keys=False).apply(pct_rank)

df = df[df["snapshot_id"].isin(original_ids)].copy()
print(f"  Rows after ID safety filter: {len(df)} (expected {len(original_ids)})")
print(f"  price_z_score non-null: {df['price_z_score'].notna().sum()}")

print("Momentum features...")
df = df.sort_values(["origin_iata","destination_iata","outbound_date","snapshot_date"])
key = ["origin_iata","destination_iata","outbound_date"]

df["trend_3d"] = df.groupby(key)["price_gbp"].transform(lambda x: x.pct_change(periods=min(3, max(1, len(x)-1)))).round(4)
df["trend_7d"] = df.groupby(key)["price_gbp"].transform(lambda x: x.pct_change(periods=min(7, max(1, len(x)-1)))).round(4)
df["volatility_7d"] = df.groupby(key)["price_gbp"].transform(lambda x: x.rolling(7, min_periods=2).std()).round(4)

df["_up"] = df.groupby(key)["price_gbp"].transform(lambda x: (x.diff() > 0).astype(float))
df["direction_consistency_7d"] = df.groupby(key)["_up"].transform(lambda x: x.rolling(7, min_periods=2).mean()).round(3)
df.drop(columns=["_up"], inplace=True)
print(f"  trend_7d non-null: {df['trend_7d'].notna().sum()}")

print("Fuel velocity...")
df = df.sort_values("snapshot_date")
fuel = df.groupby("snapshot_date")["jet_fuel_usd_gal"].first().reset_index()
fuel["jet_fuel_7d_change_pct"] = fuel["jet_fuel_usd_gal"].pct_change(periods=7).round(4)
df = df.merge(fuel[["snapshot_date","jet_fuel_7d_change_pct"]], on="snapshot_date", how="left")
df = df[df["snapshot_id"].isin(original_ids)].copy()
print(f"  jet_fuel_7d_change_pct non-null: {df['jet_fuel_7d_change_pct'].notna().sum()}")
print(f"  Final row count: {len(df)}")

print(f"\nUpserting {len(df)} rows in batches of 500...")
updated = 0
errors = 0
for i in range(0, len(df), 500):
    chunk = df.iloc[i:i+500]
    records = []
    for _, row in chunk.iterrows():
        sid = row["snapshot_id"]
        if pd.isna(sid) or sid not in original_ids:
            continue
        record = {"snapshot_id": str(sid)}
        for col in FEATURE_COLS:
            if col in df.columns:
                record[col] = clean_val(row[col], col)
        records.append(record)
    if not records:
        continue
    try:
        supabase.table("snapshots").upsert(records, on_conflict="snapshot_id").execute()
        updated += len(records)
        print(f"  {updated}/{len(df)}")
    except Exception as e:
        errors += 1
        print(f"  ERROR batch {i}: {e}")

print(f"\nDone. Upserted: {updated} | Errors: {errors}")

r1 = supabase.table("snapshots").select("snapshot_id", count="exact").not_.is_("price_z_score","null").execute()
r2 = supabase.table("snapshots").select("snapshot_id", count="exact").not_.is_("season_bucket","null").execute()
r3 = supabase.table("snapshots").select("snapshot_id", count="exact").not_.is_("trend_7d","null").execute()
print(f"price_z_score populated: {r1.count}")
print(f"season_bucket populated: {r2.count}")
print(f"trend_7d populated:      {r3.count}")
print(f"Total rows:              {len(df)}")
