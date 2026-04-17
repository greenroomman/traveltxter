#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
from datetime import date, datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from supabase import Client, create_client

try:
    import joblib
except Exception:
    joblib = None


# ============================================================
# Environment
# ============================================================

SUPABASE_URL = (
    os.environ.get("SUPABASE_URL", "").strip()
    or os.environ.get("MIZAR_SUPABASE_URL", "").strip()
)

SUPABASE_SERVICE_KEY = (
    os.environ.get("SUPABASE_SERVICE_KEY", "").strip()
    or os.environ.get("MIZAR_SUPABASE_SERVICE_ROLE_KEY", "").strip()
)

MODEL_PATH = os.environ.get("MODEL_PATH", "").strip()
DEFAULT_MODEL_VERSION = os.environ.get("MODEL_VERSION", "heuristic_v1")

MAX_SNAPSHOTS = int(os.environ.get("MARKET_PREDICTIONS_MAX_SNAPSHOTS", "750"))


# ============================================================
# Helpers
# ============================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_today() -> date:
    return utc_now().date()


def parse_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except Exception:
        return None


def recommendation_from_score(score: float) -> str:
    if score < 0.40:
        return "monitor"
    if score < 0.70:
        return "consider"
    return "book_now"


def confidence_from_score(score: float) -> Tuple[str, float]:
    confidence_score = min(0.99, max(0.51, abs(score - 0.5) * 1.8 + 0.5))
    if confidence_score >= 0.80:
        return "high", round(confidence_score, 2)
    if confidence_score >= 0.65:
        return "medium", round(confidence_score, 2)
    return "low", round(confidence_score, 2)


@lru_cache(maxsize=1)
def get_supabase() -> Client:
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print("ERROR: Missing SUPABASE_URL or SUPABASE_SERVICE_KEY")
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


@lru_cache(maxsize=1)
def load_model_bundle() -> Optional[Dict[str, Any]]:
    if not MODEL_PATH or not joblib:
        return None
    if not os.path.exists(MODEL_PATH):
        return None

    bundle = joblib.load(MODEL_PATH)
    if not isinstance(bundle, dict):
        return None
    return bundle


# ============================================================
# Calendar features
# ============================================================

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


def assign_season_bucket(d: Any) -> str:
    d = parse_date(d)
    if not d:
        return "off_peak"

    m, day = d.month, d.day

    if (m == 12 and day >= 20) or (m == 1 and day <= 5):
        return "christmas"

    if (m == 3 and day >= 24) or (m == 4 and day <= 15):
        return "easter"

    if (m == 2 and 14 <= day <= 21) or (m == 10 and 19 <= day <= 30):
        return "half_term"

    if (m == 1 and day >= 15) or m == 2 or (m == 3 and day <= 15):
        return "ski"

    if (m == 7 and day >= 15) or m == 8 or (m == 9 and day <= 1):
        return "summer_peak"

    if m in [4, 5, 6, 9, 10]:
        return "shoulder"

    return "off_peak"


def days_to_next_bank_holiday(d: Any) -> int:
    d = parse_date(d)
    if not d:
        return 365
    future = [h for h in UK_BANK_HOLIDAYS if h >= d]
    return (min(future) - d).days if future else 365


def trip_overlaps_holiday(outbound: Any, return_date: Any) -> bool:
    outbound = parse_date(outbound)
    return_date = parse_date(return_date)
    if not outbound or not return_date:
        return False
    return any(outbound <= h <= return_date for h in UK_BANK_HOLIDAYS)


def holiday_intensity_score(outbound: Any) -> float:
    outbound = parse_date(outbound)
    if not outbound:
        return 0.20

    season = assign_season_bucket(outbound)
    base = {
        "christmas": 0.95,
        "easter": 0.85,
        "summer_peak": 0.90,
        "half_term": 0.75,
        "ski": 0.70,
        "shoulder": 0.45,
        "off_peak": 0.20,
    }.get(season, 0.30)

    if days_to_next_bank_holiday(outbound) <= 3:
        base = min(1.0, base + 0.15)

    return round(base, 3)


def add_calendar_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["season_bucket"] = df["outbound_date"].apply(assign_season_bucket)
    df["days_to_next_bank_holiday"] = df["outbound_date"].apply(days_to_next_bank_holiday)
    df["trip_overlaps_holiday"] = df.apply(
        lambda r: trip_overlaps_holiday(r["outbound_date"], r["return_date"]), axis=1
    )
    df["holiday_intensity_score"] = df["outbound_date"].apply(holiday_intensity_score)
    return df


# ============================================================
# Feature engineering
# ============================================================

def compute_baseline(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["dtd_bucket"] = pd.cut(
        df["dtd"].astype(float),
        bins=[-1, 7, 21, 60, 120, 9999],
        labels=["0-7", "8-21", "22-60", "61-120", "120+"]
    )
    df["route"] = df["origin_iata"].astype(str) + "-" + df["destination_iata"].astype(str)

    baseline = df.groupby(["route", "dtd_bucket", "season_bucket"], observed=False)["price_gbp"].agg(
        baseline_mu="mean",
        baseline_sigma="std"
    ).reset_index()

    baseline["baseline_sigma"] = baseline["baseline_sigma"].fillna(10.0).clip(lower=5.0)

    df = df.merge(baseline, on=["route", "dtd_bucket", "season_bucket"], how="left")

    df["price_z_score"] = ((df["price_gbp"] - df["baseline_mu"]) / df["baseline_sigma"]).round(3)
    df["price_ratio"] = (df["price_gbp"] / df["baseline_mu"]).round(3)

    def pct_rank(group: pd.DataFrame) -> pd.DataFrame:
        group = group.copy()
        group["price_percentile"] = group["price_gbp"].rank(pct=True).mul(100).round(1)
        return group

    df = df.groupby(["route", "dtd_bucket", "season_bucket"], group_keys=False, observed=False).apply(pct_rank)
    return df


def compute_momentum(df: pd.DataFrame) -> pd.DataFrame:
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

    def dir_consistency(x: pd.Series) -> float:
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


def compute_fuel_velocity(df: pd.DataFrame) -> pd.DataFrame:
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


# ============================================================
# Snapshot + macro loading
# ============================================================

def get_latest_macro_signals(supabase: Client) -> Dict[str, Any]:
    result = (
        supabase.table("daily_market_signals")
        .select("*")
        .order("signal_date", desc=True)
        .limit(1)
        .execute()
    )
    rows = result.data or []
    return rows[0] if rows else {}


def get_candidate_snapshots(supabase: Client, limit: int = MAX_SNAPSHOTS) -> List[Dict[str, Any]]:
    result = (
        supabase.table("snapshots")
        .select("*")
        .order("snapshot_date", desc=True)
        .limit(limit * 3)
        .execute()
    )

    rows = result.data or []
    clean: List[Dict[str, Any]] = []
    today = utc_today()

    for row in rows:
        outbound = parse_date(row.get("outbound_date"))
        return_date = parse_date(row.get("return_date"))

        if not outbound or outbound < today:
            continue
        if not return_date:
            continue
        if row.get("price_gbp") is None:
            continue
        if not row.get("origin_iata") or not row.get("destination_iata"):
            continue

        clean.append(row)
        if len(clean) >= limit:
            break

    return clean


# ============================================================
# Scoring
# ============================================================

def heuristic_score(feature_row: Dict[str, Any]) -> Tuple[float, str]:
    price_gbp = float(feature_row.get("price_gbp") or 0.0)
    dtd = int(feature_row.get("dtd") or 0)
    origin = str(feature_row.get("origin_iata") or "").upper()
    destination = str(feature_row.get("destination_iata") or "").upper()
    holiday_intensity = float(feature_row.get("holiday_intensity_score") or 0.0)
    trend_7d = float(feature_row.get("trend_7d") or 0.0)
    price_z = float(feature_row.get("price_z_score") or 0.0)
    fuel_change = float(feature_row.get("jet_fuel_7d_change_pct") or 0.0)

    score = 0.35

    if price_gbp >= 500:
        score += 0.15
    elif price_gbp >= 300:
        score += 0.08
    elif price_gbp <= 120:
        score -= 0.04

    if dtd <= 7:
        score += 0.22
    elif dtd <= 14:
        score += 0.14
    elif dtd <= 30:
        score += 0.06
    elif dtd >= 90:
        score -= 0.05

    if holiday_intensity >= 0.80:
        score += 0.08
    elif holiday_intensity >= 0.60:
        score += 0.04

    if trend_7d > 0.05:
        score += 0.05
    elif trend_7d < -0.05:
        score -= 0.04

    if price_z > 1.25:
        score -= 0.03
    elif price_z < -0.75:
        score += 0.03

    if fuel_change > 0.03:
        score += 0.02

    if origin in {"LHR", "LGW", "MAN"}:
        score += 0.02

    if destination in {"JFK", "DXB", "BKK", "SIN", "MIA", "LAX"}:
        score += 0.05

    score = max(0.01, min(0.99, score))
    return round(score, 3), DEFAULT_MODEL_VERSION


def real_model_score(feature_row: Dict[str, Any], bundle: Dict[str, Any]) -> Tuple[float, str]:
    model = bundle.get("model")
    scaler = bundle.get("scaler")
    feature_cols = bundle.get("feature_cols") or []
    version = bundle.get("version") or DEFAULT_MODEL_VERSION

    model_input = {
        "price_gbp": float(feature_row.get("price_gbp") or 0.0),
        "dtd": float(feature_row.get("dtd") or 0.0),
        "holiday_intensity_score": float(feature_row.get("holiday_intensity_score") or 0.0),
        "days_to_next_bank_holiday": float(feature_row.get("days_to_next_bank_holiday") or 365.0),
        "trip_overlaps_holiday": 1.0 if feature_row.get("trip_overlaps_holiday") else 0.0,
        "price_z_score": float(feature_row.get("price_z_score") or 0.0),
        "price_ratio": float(feature_row.get("price_ratio") or 1.0),
        "price_percentile": float(feature_row.get("price_percentile") or 50.0),
        "trend_3d": float(feature_row.get("trend_3d") or 0.0),
        "trend_7d": float(feature_row.get("trend_7d") or 0.0),
        "volatility_7d": float(feature_row.get("volatility_7d") or 0.0),
        "direction_consistency_7d": float(feature_row.get("direction_consistency_7d") or 0.5),
        "carrier_count": float(feature_row.get("carrier_count") or 0.0),
        "lcc_present": 1.0 if feature_row.get("lcc_present") else 0.0,
        "direct": 1.0 if feature_row.get("direct") else 0.0,
        "stops": 0.0 if feature_row.get("direct") else 1.0,
        "jet_fuel_usd_gal": float(feature_row.get("jet_fuel_usd_gal") or 0.0),
        "jet_fuel_7d_change_pct": float(feature_row.get("jet_fuel_7d_change_pct") or 0.0),
        "gbp_usd_rate": float(feature_row.get("gbp_usd_rate") or 0.0),
        "gbp_eur_rate": float(feature_row.get("gbp_eur_rate") or 0.0),
        "day_of_week_departure": float(parse_date(feature_row.get("outbound_date")).weekday()),
        "is_weekend_departure": 1.0 if parse_date(feature_row.get("outbound_date")).weekday() in {4, 5, 6} else 0.0,
    }

    origin = str(feature_row.get("origin_iata") or "").upper()
    for code in ["MAN", "LGW", "LHR", "EDI", "BRS", "LPL", "BHX", "NCL", "GLA"]:
        model_input[f"origin_{code}"] = 1.0 if origin == code else 0.0

    X = np.array([[float(model_input.get(col, 0.0)) for col in feature_cols]], dtype=float)

    if scaler is not None:
        X = scaler.transform(X)

    proba = float(model.predict_proba(X)[0][1])
    proba = max(0.001, min(0.999, proba))
    return round(proba, 3), str(version)


def score_feature_row(feature_row: Dict[str, Any]) -> Tuple[float, str]:
    bundle = load_model_bundle()
    if bundle:
        try:
            return real_model_score(feature_row, bundle)
        except Exception:
            return heuristic_score(feature_row)
    return heuristic_score(feature_row)


# ============================================================
# Build + upsert
# ============================================================

def build_feature_frame(snapshots: List[Dict[str, Any]], macro: Dict[str, Any]) -> pd.DataFrame:
    if not snapshots:
        return pd.DataFrame()

    df = pd.DataFrame(snapshots)

    for col in ["snapshot_date", "outbound_date", "return_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    df["price_gbp"] = pd.to_numeric(df["price_gbp"], errors="coerce")
    df["carrier_count"] = pd.to_numeric(df.get("carrier_count"), errors="coerce").fillna(0).astype(int)
    df["direct"] = df.get("direct", False).fillna(False).astype(bool)
    df["lcc_present"] = df.get("lcc_present", False).fillna(False).astype(bool)

    df["dtd"] = (pd.to_datetime(df["outbound_date"]) - pd.Timestamp(utc_today())).dt.days

    df["jet_fuel_usd_gal"] = float(macro.get("jet_fuel_usd_gal") or 0.0)
    df["gbp_usd_rate"] = float(macro.get("gbp_usd_rate") or 0.0)
    df["gbp_eur_rate"] = float(macro.get("gbp_eur_rate") or 0.0)

    df = add_calendar_features(df)
    df = compute_baseline(df)
    df = compute_momentum(df)
    df = compute_fuel_velocity(df)

    return df


def row_to_prediction_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    score, model_version = score_feature_row(row)
    recommendation = recommendation_from_score(score)
    confidence, confidence_score = confidence_from_score(score)

    def clean_value(v: Any) -> Any:
        if pd.isna(v):
            return None
        if isinstance(v, pd.Timestamp):
            return v.date().isoformat()
        if isinstance(v, date):
            return v.isoformat()
        if isinstance(v, np.integer):
            return int(v)
        if isinstance(v, np.floating):
            return float(v)
        return v

    return {
        "snapshot_id": clean_value(row.get("id")),
        "snapshot_date": clean_value(row.get("snapshot_date")),
        "origin_iata": clean_value(row.get("origin_iata")),
        "destination_iata": clean_value(row.get("destination_iata")),
        "outbound_date": clean_value(row.get("outbound_date")),
        "return_date": clean_value(row.get("return_date")),
        "price_gbp": round(float(row.get("price_gbp") or 0.0), 2),
        "dtd": clean_value(row.get("dtd")),
        "regret_risk_score": score,
        "recommendation": recommendation,
        "confidence": confidence,
        "confidence_score": confidence_score,
        "model_version": model_version,
        "season_bucket": clean_value(row.get("season_bucket")),
        "days_to_next_bank_holiday": clean_value(row.get("days_to_next_bank_holiday")),
        "trip_overlaps_holiday": clean_value(row.get("trip_overlaps_holiday")),
        "holiday_intensity_score": clean_value(row.get("holiday_intensity_score")),
        "price_z_score": clean_value(row.get("price_z_score")),
        "price_ratio": clean_value(row.get("price_ratio")),
        "price_percentile": clean_value(row.get("price_percentile")),
        "trend_3d": clean_value(row.get("trend_3d")),
        "trend_7d": clean_value(row.get("trend_7d")),
        "volatility_7d": clean_value(row.get("volatility_7d")),
        "direction_consistency_7d": clean_value(row.get("direction_consistency_7d")),
        "direct": clean_value(row.get("direct")),
        "lcc_present": clean_value(row.get("lcc_present")),
        "carrier_count": clean_value(row.get("carrier_count")),
        "jet_fuel_usd_gal": clean_value(row.get("jet_fuel_usd_gal")),
        "jet_fuel_7d_change_pct": clean_value(row.get("jet_fuel_7d_change_pct")),
        "gbp_usd_rate": clean_value(row.get("gbp_usd_rate")),
        "gbp_eur_rate": clean_value(row.get("gbp_eur_rate")),
        "updated_at": utc_now().isoformat(),
    }


def main() -> int:
    print("=" * 60)
    print("MIZAR Market Predictions Builder")
    print(f"Started: {utc_now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 60)

    supabase = get_supabase()

    macro = get_latest_macro_signals(supabase)
    print(f"Latest macro signal date: {macro.get('signal_date')}")

    snapshots = get_candidate_snapshots(supabase, MAX_SNAPSHOTS)
    print(f"Candidate snapshots: {len(snapshots)}")

    if not snapshots:
        print("No candidate snapshots found. Exiting.")
        return 0

    df = build_feature_frame(snapshots, macro)

    if df.empty:
        print("Feature frame is empty. Exiting.")
        return 0

    rows = df.to_dict(orient="records")
    payloads: List[Dict[str, Any]] = []
    skipped = 0

    for row in rows:
        try:
            payloads.append(row_to_prediction_payload(row))
        except Exception as ex:
            skipped += 1
            print(f"Skipped row: {ex}")

    if not payloads:
        print("No payloads built. Exiting.")
        return 1

    result = supabase.table("market_predictions").upsert(
        payloads,
        on_conflict="snapshot_date,origin_iata,destination_iata,outbound_date,return_date,price_gbp"
    ).execute()

    print(f"Upsert complete. Rows attempted: {len(payloads)}")
    print(f"Rows skipped: {skipped}")
    print(f"Supabase response rows: {len(result.data or [])}")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())