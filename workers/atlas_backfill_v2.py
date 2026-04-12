import pandas as pd
import numpy as np
from datetime import date

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


def days_to_next_bank_holiday(d):
    if isinstance(d, str):
        d = date.fromisoformat(str(d)[:10])
    elif hasattr(d, 'date'):
        d = d.date()
    future = [h for h in UK_BANK_HOLIDAYS if h >= d]
    return (min(future) - d).days if future else 365


def trip_overlaps_holiday(outbound, return_date):
    if isinstance(outbound, str):
        outbound = date.fromisoformat(str(outbound)[:10])
    elif hasattr(outbound, 'date'):
        outbound = outbound.date()
    if isinstance(return_date, str):
        return_date = date.fromisoformat(str(return_date)[:10])
    elif hasattr(return_date, 'date'):
        return_date = return_date.date()
    return any(outbound <= h <= return_date for h in UK_BANK_HOLIDAYS)


def holiday_intensity_score(outbound):
    if isinstance(outbound, str):
        outbound = date.fromisoformat(str(outbound)[:10])
    elif hasattr(outbound, 'date'):
        outbound = outbound.date()
    season = assign_season_bucket(outbound)
    base = {
        "christmas": 0.95, "easter": 0.85, "summer_peak": 0.90,
        "half_term": 0.75, "ski": 0.70, "shoulder": 0.45, "off_peak": 0.20
    }.get(season, 0.30)
    if days_to_next_bank_holiday(outbound) <= 3:
        base = min(1.0, base + 0.15)
    return round(base, 3)


def add_calendar_features(df):
    df = df.copy()
    df["season_bucket"] = df["outbound_date"].apply(assign_season_bucket)
    df["days_to_next_bank_holiday"] = df["outbound_date"].apply(days_to_next_bank_holiday)
    df["trip_overlaps_holiday"] = df.apply(
        lambda r: trip_overlaps_holiday(r["outbound_date"], r["return_date"]), axis=1
    )
    df["holiday_intensity_score"] = df["outbound_date"].apply(holiday_intensity_score)
    return df


def compute_baseline(df):
    df = df.copy()
    df["dtd_bucket"] = pd.cut(
        df["dtd"].astype(float),
        bins=[-1, 7, 21, 60, 120, 9999],
        labels=["0-7", "8-21", "22-60", "61-120", "120+"]
    )
    df["route"] = df["origin_iata"] + "-" + df["destination_iata"]
    baseline = df.groupby(["route", "dtd_bucket", "season_bucket"])["price_gbp"].agg(
        baseline_mu="mean",
        baseline_sigma="std"
    ).reset_index()
    baseline["baseline_sigma"] = baseline["baseline_sigma"].fillna(10.0).clip(lower=5.0)
    df = df.merge(baseline, on=["route", "dtd_bucket", "season_bucket"], how="left")
    df["price_z_score"] = ((df["price_gbp"] - df["baseline_mu"]) / df["baseline_sigma"]).round(3)
    df["price_ratio"] = (df["price_gbp"] / df["baseline_mu"]).round(3)

    def pct_rank(group):
        group = group.copy()
        group["price_percentile"] = group["price_gbp"].rank(pct=True).mul(100).round(1)
        return group

    df = df.groupby(["route", "dtd_bucket", "season_bucket"], group_keys=False).apply(pct_rank)
    return df


def compute_momentum(df):
    df = df.copy().sort_values(
        ["origin_iata", "destination_iata", "outbound_date", "snapshot_date"]
    )
    key = ["origin_iata", "destination_iata", "outbound_date"]

    for n, col in [(3, "trend_3d"), (7, "trend_7d")]:
        df[col] = df.groupby(key)["price_gbp"].transform(
            lambda x: x.pct_change(periods=min(n, max(1, len(x) - 1)))
        ).round(4)

    df["volatility_7d"] = df.groupby(key)["price_gbp"].transform(
        lambda x: x.rolling(7, min_periods=2).std()
    ).round(4)

    def dir_consistency(x):
        if len(x) < 2:
            return np.nan
        diffs = x.diff().dropna()
        if len(diffs) == 0:
            return np.nan
        return round(float((diffs > 0).sum()) / len(diffs), 3)

    df["direction_consistency_7d"] = df.groupby(key)["price_gbp"].transform(
        lambda x: x.rolling(7, min_periods=2).apply(dir_consistency, raw=False)
    )
    return df


def compute_fuel_velocity(df):
    df = df.copy().sort_values("snapshot_date")
    fuel_by_date = (
        df.groupby("snapshot_date")["jet_fuel_usd_gal"]
        .first()
        .reset_index()
    )
    fuel_by_date["jet_fuel_7d_change_pct"] = (
        fuel_by_date["jet_fuel_usd_gal"].pct_change(periods=7).round(4)
    )
    df = df.merge(
        fuel_by_date[["snapshot_date", "jet_fuel_7d_change_pct"]],
        on="snapshot_date",
        how="left"
    )
    return df
