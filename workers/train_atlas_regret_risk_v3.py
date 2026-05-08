#!/usr/bin/env python3
from __future__ import annotations

"""
train_atlas_regret_risk_v3.py

MIZAR Atlas — Reproducible RegretRisk v3 training pipeline.

Purpose:
- Pull labelled t+7 snapshot pairs from Supabase.
- Build the same feature names used by main.py at inference time.
- Train a calibrated logistic classifier with sigmoid calibration.
- Export atlas_regret_risk_v3.joblib and atlas_v3_features.txt.

Run from repository root:
    python workers/train_atlas_regret_risk_v3.py

Required env vars:
    MIZAR_SUPABASE_URL
    MIZAR_SUPABASE_SERVICE_ROLE_KEY
"""

import os
import math
import joblib
import logging
from datetime import date, datetime, timedelta, timezone
from collections import defaultdict, Counter
from typing import Any

import numpy as np
from supabase import create_client
from sklearn.linear_model import LogisticRegression
from sklearn.calibration import CalibratedClassifierCV
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import brier_score_loss


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)

log = logging.getLogger(__name__)


# ------------------------------------------------------------
# Config
# ------------------------------------------------------------

SUPABASE_URL = os.environ["MIZAR_SUPABASE_URL"]
SUPABASE_KEY = os.environ["MIZAR_SUPABASE_SERVICE_ROLE_KEY"]

OUTPUT_DIR = os.environ.get("ATLAS_MODEL_OUTPUT_DIR", ".")
MODEL_FILENAME = "atlas_regret_risk_v3.joblib"
FEATURES_FILENAME = "atlas_v3_features.txt"
MODEL_VERSION = "v3_0_0"

RISE_THRESHOLD = 0.10
TEST_FRACTION = 0.30
HIGH_RISK_THRESHOLD = 0.70
PAGE_SIZE = int(os.environ.get("ATLAS_TRAINING_PAGE_SIZE", "1000"))

MIDDLE_EAST_AIRPORTS = {"DXB", "AUH", "DOH", "AMM", "BEY", "TLV"}
UK_ORIGINS = ["MAN", "LGW", "LHR", "EDI", "BRS", "LPL", "BHX", "NCL", "GLA"]

# Keep this aligned with main.py.
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
    "price_gbp",
    "price_z_score",
    "price_ratio",
    "price_percentile",
    "dtd",
    "holiday_intensity_score",
    "days_to_next_bank_holiday",
    "trip_overlaps_holiday",
    "trend_7d",
    "volatility_7d",
    "direction_consistency_7d",
    "carrier_count",
    "lcc_present",
    "direct",
    "stops",
    "jet_fuel_usd_gal",
    "jet_fuel_7d_change_pct",
    "gbp_usd_rate",
    "gbp_eur_rate",
    "day_of_week_departure",
    "is_weekend_departure",
    "is_school_holiday_window",
    "is_bank_holiday_adjacent",
    "crisis_flag",
    *[f"origin_{code}" for code in UK_ORIGINS],
]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# ------------------------------------------------------------
# Date / calendar helpers aligned with main.py
# ------------------------------------------------------------

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except Exception:
        return None


def assign_season_bucket(d: date) -> str:
    m, day = d.month, d.day
    if (m == 12 and day >= 20) or (m == 1 and day <= 5):
        return "christmas"
    if (m == 4 and 1 <= day <= 15) or (m == 3 and 24 <= day <= 31):
        return "easter"
    if (m == 7 and day >= 15) or m == 8 or (m == 9 and day <= 1):
        return "summer_peak"
    if (m == 1 and day >= 15) or m == 2 or (m == 3 and day <= 15):
        return "ski"
    if (m == 2 and 14 <= day <= 21) or (m == 10 and 19 <= day <= 30):
        return "half_term"
    if m in {4, 5, 6, 9, 10}:
        return "shoulder"
    return "off_peak"


def days_to_next_bank_holiday(d: date) -> int:
    future = [h for h in UK_BANK_HOLIDAYS if h >= d]
    return (min(future) - d).days if future else 365


def holiday_intensity_score(d: date) -> float:
    base = {
        "christmas": 0.95,
        "easter": 0.85,
        "summer_peak": 0.90,
        "half_term": 0.75,
        "ski": 0.70,
        "shoulder": 0.45,
        "off_peak": 0.20,
    }.get(assign_season_bucket(d), 0.30)
    if days_to_next_bank_holiday(d) <= 3:
        base = min(1.0, base + 0.15)
    return round(base, 3)


# ------------------------------------------------------------
# Supabase loading
# ------------------------------------------------------------

def fetch_all_rows(table: str, select_cols: str, order_col: str | None = None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    start = 0

    while True:
        end = start + PAGE_SIZE - 1
        query = supabase.table(table).select(select_cols).range(start, end)
        if order_col:
            query = query.order(order_col, desc=False)
        result = query.execute()
        batch = result.data or []
        rows.extend(batch)
        log.info("Fetched %d rows from %s so far.", len(rows), table)

        if len(batch) < PAGE_SIZE:
            break
        start += PAGE_SIZE

    return rows


def fetch_snapshots() -> list[dict[str, Any]]:
    cols = (
        "snapshot_id,snapshot_date,origin_iata,destination_iata,"
        "outbound_date,return_date,price_gbp,cabin_class,direct,stops,"
        "carrier_count,lcc_present"
    )
    raw_rows = fetch_all_rows("snapshots", cols, "snapshot_date")
    cleaned: list[dict[str, Any]] = []

    for row in raw_rows:
        origin = (row.get("origin_iata") or "").strip().upper()
        destination = (row.get("destination_iata") or "").strip().upper()
        snapshot_date = parse_date(row.get("snapshot_date"))
        outbound_date = parse_date(row.get("outbound_date"))
        return_date = parse_date(row.get("return_date"))

        if not origin or not destination or not snapshot_date:
            continue
        if origin in MIDDLE_EAST_AIRPORTS or destination in MIDDLE_EAST_AIRPORTS:
            continue

        try:
            price = float(row.get("price_gbp"))
        except Exception:
            continue
        if price <= 0:
            continue

        row["origin_iata"] = origin
        row["destination_iata"] = destination
        row["snapshot_date_obj"] = snapshot_date
        row["outbound_date_obj"] = outbound_date
        row["return_date_obj"] = return_date
        row["price_gbp_float"] = price
        cleaned.append(row)

    log.info("Usable snapshots after cleaning: %d", len(cleaned))
    return cleaned


def fetch_market_signals() -> dict[date, dict[str, float]]:
    defaults = {
        "jet_fuel_usd_gal": 4.183,
        "jet_fuel_7d_change_pct": 0.0,
        "gbp_usd_rate": 1.27,
        "gbp_eur_rate": 1.17,
    }

    try:
        rows = fetch_all_rows(
            "daily_market_signals",
            "signal_date,jet_fuel_usd_gal,jet_fuel_7d_change_pct,gbp_usd_rate,gbp_eur_rate",
            "signal_date",
        )
    except Exception as exc:
        log.warning("Could not fetch daily_market_signals, using defaults: %s", exc)
        return {}

    out: dict[date, dict[str, float]] = {}
    for row in rows:
        signal_date = parse_date(row.get("signal_date"))
        if not signal_date:
            continue
        out[signal_date] = {
            key: float(row.get(key) if row.get(key) is not None else default_value)
            for key, default_value in defaults.items()
        }
    return out


def market_signal_for_date(signals_by_date: dict[date, dict[str, float]], snapshot_date: date) -> dict[str, float]:
    defaults = {
        "jet_fuel_usd_gal": 4.183,
        "jet_fuel_7d_change_pct": 0.0,
        "gbp_usd_rate": 1.27,
        "gbp_eur_rate": 1.17,
    }
    if not signals_by_date:
        return defaults

    available = [d for d in signals_by_date if d <= snapshot_date]
    if not available:
        return defaults
    return signals_by_date[max(available)]


# ------------------------------------------------------------
# Label construction
# ------------------------------------------------------------

def build_route_date_min_price(snapshots: list[dict[str, Any]]) -> dict[tuple[str, str, date], float]:
    route_date_prices: dict[tuple[str, str, date], list[float]] = defaultdict(list)

    for row in snapshots:
        key = (row["origin_iata"], row["destination_iata"], row["snapshot_date_obj"])
        route_date_prices[key].append(row["price_gbp_float"])

    return {key: min(values) for key, values in route_date_prices.items() if values}


def make_labelled_rows(snapshots: list[dict[str, Any]]) -> list[dict[str, Any]]:
    route_date_min_price = build_route_date_min_price(snapshots)
    labelled: list[dict[str, Any]] = []

    for row in snapshots:
        origin = row["origin_iata"]
        destination = row["destination_iata"]
        snapshot_date = row["snapshot_date_obj"]
        price_t0 = row["price_gbp_float"]
        t7_date = snapshot_date + timedelta(days=7)
        price_t7 = route_date_min_price.get((origin, destination, t7_date))

        if price_t7 is None:
            continue

        label = 1 if ((price_t7 - price_t0) / price_t0) >= RISE_THRESHOLD else 0
        enriched = dict(row)
        enriched["price_t7_gbp"] = price_t7
        enriched["label"] = label
        labelled.append(enriched)

    log.info("Labelled t+7 snapshot pairs: %d", len(labelled))
    return labelled


# ------------------------------------------------------------
# Feature engineering aligned with main.py names
# ------------------------------------------------------------

def build_feature_indexes(snapshots: list[dict[str, Any]]) -> dict[str, Any]:
    by_route_season: dict[tuple[str, str, str], list[tuple[date, float]]] = defaultdict(list)
    by_route: dict[tuple[str, str], list[tuple[date, float]]] = defaultdict(list)

    for row in snapshots:
        origin = row["origin_iata"]
        destination = row["destination_iata"]
        snapshot_date = row["snapshot_date_obj"]
        outbound_date = row.get("outbound_date_obj")
        price = row["price_gbp_float"]

        if outbound_date:
            season = assign_season_bucket(outbound_date)
            by_route_season[(origin, destination, season)].append((snapshot_date, price))

        by_route[(origin, destination)].append((snapshot_date, price))

    for values in by_route_season.values():
        values.sort(key=lambda x: x[0])
    for values in by_route.values():
        values.sort(key=lambda x: x[0])

    return {"by_route_season": by_route_season, "by_route": by_route}


def route_relative_features(
    row: dict[str, Any],
    indexes: dict[str, Any],
) -> dict[str, float]:
    neutral = {"price_z_score": 0.0, "price_ratio": 1.0, "price_percentile": 50.0}
    outbound_date = row.get("outbound_date_obj")
    if not outbound_date:
        return neutral

    season = assign_season_bucket(outbound_date)
    key = (row["origin_iata"], row["destination_iata"], season)
    snapshot_date = row["snapshot_date_obj"]
    price = row["price_gbp_float"]

    history = [p for d, p in indexes["by_route_season"].get(key, []) if d <= snapshot_date]
    if len(history) < 3:
        return neutral

    n = len(history)
    mean_price = sum(history) / n
    sigma = max(5.0, (sum((p - mean_price) ** 2 for p in history) / n) ** 0.5)

    return {
        "price_z_score": round((price - mean_price) / sigma, 3),
        "price_ratio": round(price / mean_price, 3) if mean_price > 0 else 1.0,
        "price_percentile": round(sum(1 for p in history if p <= price) / n * 100, 1),
    }


def route_momentum_features(
    row: dict[str, Any],
    indexes: dict[str, Any],
) -> dict[str, float]:
    neutral = {"trend_7d": 0.0, "volatility_7d": 0.0, "direction_consistency_7d": 0.5}
    key = (row["origin_iata"], row["destination_iata"])
    snapshot_date = row["snapshot_date_obj"]

    history = [(d, p) for d, p in indexes["by_route"].get(key, []) if d <= snapshot_date]
    if len(history) < 2:
        return neutral

    latest = list(reversed(history[-14:]))
    prices = [p for _, p in latest]

    if len(prices) < 2 or prices[-1] <= 0:
        return neutral

    trend_7d = (prices[0] - prices[-1]) / prices[-1]

    if len(prices) < 3:
        return {
            "trend_7d": round(trend_7d, 4),
            "volatility_7d": 0.0,
            "direction_consistency_7d": 0.5,
        }

    changes = [
        (prices[i] - prices[i + 1]) / prices[i + 1]
        for i in range(len(prices) - 1)
        if prices[i + 1] > 0
    ]

    if len(changes) >= 2:
        mean_change = sum(changes) / len(changes)
        volatility_7d = (sum((c - mean_change) ** 2 for c in changes) / len(changes)) ** 0.5
    else:
        volatility_7d = 0.0

    if changes and trend_7d != 0:
        dominant_up = trend_7d > 0
        consistent = sum(1 for c in changes if (c > 0) == dominant_up)
        direction_consistency_7d = consistent / len(changes)
    else:
        direction_consistency_7d = 0.5

    return {
        "trend_7d": round(trend_7d, 4),
        "volatility_7d": round(volatility_7d, 4),
        "direction_consistency_7d": round(direction_consistency_7d, 3),
    }


def truthy_float(value: Any, default: float) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except Exception:
        return default


def build_feature_row(
    row: dict[str, Any],
    indexes: dict[str, Any],
    signals_by_date: dict[date, dict[str, float]],
) -> dict[str, float] | None:
    snapshot_date = row["snapshot_date_obj"]
    outbound_date = row.get("outbound_date_obj")
    return_date = row.get("return_date_obj")

    if not outbound_date:
        return None

    dtd = (outbound_date - snapshot_date).days
    if dtd < 0:
        return None

    days_to_bh = days_to_next_bank_holiday(outbound_date)
    rel = route_relative_features(row, indexes)
    momentum = route_momentum_features(row, indexes)
    market = market_signal_for_date(signals_by_date, snapshot_date)

    trip_overlaps_holiday = 0.0
    if return_date is not None:
        trip_overlaps_holiday = 1.0 if any(outbound_date <= h <= return_date for h in UK_BANK_HOLIDAYS) else 0.0

    feature_map = {
        "price_gbp": row["price_gbp_float"],
        "price_z_score": rel["price_z_score"],
        "price_ratio": rel["price_ratio"],
        "price_percentile": rel["price_percentile"],
        "dtd": float(dtd),
        "holiday_intensity_score": holiday_intensity_score(outbound_date),
        "days_to_next_bank_holiday": float(days_to_bh),
        "trip_overlaps_holiday": trip_overlaps_holiday,
        "trend_7d": momentum["trend_7d"],
        "volatility_7d": momentum["volatility_7d"],
        "direction_consistency_7d": momentum["direction_consistency_7d"],
        "carrier_count": truthy_float(row.get("carrier_count"), 3.0),
        "lcc_present": truthy_float(row.get("lcc_present"), 1.0),
        "direct": truthy_float(row.get("direct"), 1.0),
        "stops": truthy_float(row.get("stops"), 0.0),
        "jet_fuel_usd_gal": market["jet_fuel_usd_gal"],
        "jet_fuel_7d_change_pct": market["jet_fuel_7d_change_pct"],
        "gbp_usd_rate": market["gbp_usd_rate"],
        "gbp_eur_rate": market["gbp_eur_rate"],
        "day_of_week_departure": float(outbound_date.weekday()),
        "is_weekend_departure": 1.0 if outbound_date.weekday() in {4, 5, 6} else 0.0,
        "is_school_holiday_window": 0.0,
        "is_bank_holiday_adjacent": 1.0 if days_to_bh <= 1 else 0.0,
        "crisis_flag": 0.0,
    }

    for code in UK_ORIGINS:
        feature_map[f"origin_{code}"] = 1.0 if row["origin_iata"] == code else 0.0

    return feature_map


def build_training_matrix(
    labelled_rows: list[dict[str, Any]],
    indexes: dict[str, Any],
    signals_by_date: dict[date, dict[str, float]],
) -> tuple[np.ndarray, np.ndarray, list[date]]:
    X: list[list[float]] = []
    y: list[int] = []
    dates: list[date] = []
    skipped = 0

    for row in labelled_rows:
        feature_map = build_feature_row(row, indexes, signals_by_date)
        if feature_map is None:
            skipped += 1
            continue

        X.append([float(feature_map.get(col, 0.0)) for col in FEATURE_COLS])
        y.append(int(row["label"]))
        dates.append(row["snapshot_date_obj"])

    if skipped:
        log.warning("Skipped %d labelled rows during feature construction.", skipped)

    return np.array(X, dtype=float), np.array(y, dtype=int), dates


# ------------------------------------------------------------
# Train / evaluate / export
# ------------------------------------------------------------

def temporal_split(
    X: np.ndarray,
    y: np.ndarray,
    dates: list[date],
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, date]:
    unique_dates = sorted(set(dates))
    if len(unique_dates) < 2:
        raise RuntimeError("Need at least two distinct snapshot dates for temporal split.")

    cutoff_index = max(1, int(len(unique_dates) * (1.0 - TEST_FRACTION)))
    cutoff_date = unique_dates[cutoff_index]

    train_idx = [i for i, d in enumerate(dates) if d < cutoff_date]
    test_idx = [i for i, d in enumerate(dates) if d >= cutoff_date]

    if not train_idx or not test_idx:
        raise RuntimeError("Temporal split produced empty train or test set.")

    return X[train_idx], X[test_idx], y[train_idx], y[test_idx], cutoff_date


def positive_rate(y: np.ndarray) -> float:
    return float(np.mean(y)) if len(y) else 0.0


def precision_at_threshold(y_true: np.ndarray, y_score: np.ndarray, threshold: float) -> tuple[float, int]:
    predicted_high = y_score >= threshold
    n_high = int(np.sum(predicted_high))
    if n_high == 0:
        return 0.0, 0
    tp = int(np.sum((y_true == 1) & predicted_high))
    return float(tp / n_high), n_high


def score_distribution(scores: np.ndarray) -> dict[str, int]:
    return {
        "0.00-0.10": int(np.sum((scores >= 0.00) & (scores < 0.10))),
        "0.10-0.20": int(np.sum((scores >= 0.10) & (scores < 0.20))),
        "0.20-0.30": int(np.sum((scores >= 0.20) & (scores < 0.30))),
        "0.30-0.40": int(np.sum((scores >= 0.30) & (scores < 0.40))),
        "0.40-0.50": int(np.sum((scores >= 0.40) & (scores < 0.50))),
        "0.50+": int(np.sum(scores >= 0.50)),
    }


def distribution_is_not_collapsed(distribution: dict[str, int], total: int) -> bool:
    if total <= 0:
        return False
    non_zero_bands = sum(1 for count in distribution.values() if count > 0)
    max_band_share = max(distribution.values()) / total
    return non_zero_bands >= 2 and max_band_share < 0.85


def evaluate_variant(
    name: str,
    fitted_model: Any,
    y_test: np.ndarray,
    scores: np.ndarray,
) -> dict[str, Any]:
    brier = float(brier_score_loss(y_test, scores))
    precision_045, n_045 = precision_at_threshold(y_test, scores, 0.45)
    precision_030, n_030 = precision_at_threshold(y_test, scores, 0.30)
    distribution = score_distribution(scores)

    print(f"Variant: {name}")
    print(f"  Brier score:         {brier:.3f}")
    print(f"  Avg score (test):    {float(np.mean(scores)):.3f}")
    print(f"  Min score (test):    {float(np.min(scores)):.3f}")
    print(f"  Max score (test):    {float(np.max(scores)):.3f}")
    print(f"  % scores >= 0.45:    {100.0 * float(np.mean(scores >= 0.45)):.1f}%")
    print(f"  % scores >= 0.30:    {100.0 * float(np.mean(scores >= 0.30)):.1f}%")
    print(f"  Precision at 0.45:   {100.0 * precision_045:.1f}%  (n={n_045})")
    print(f"  Precision at 0.30:   {100.0 * precision_030:.1f}%  (n={n_030})")
    print("  Score distribution:")
    print(f"    0.00-0.10: {distribution['0.00-0.10']} rows")
    print(f"    0.10-0.20: {distribution['0.10-0.20']} rows")
    print(f"    0.20-0.30: {distribution['0.20-0.30']} rows")
    print(f"    0.30-0.40: {distribution['0.30-0.40']} rows")
    print(f"    0.40-0.50: {distribution['0.40-0.50']} rows")
    print(f"    0.50+:     {distribution['0.50+']} rows")
    print("")

    return {
        "name": name,
        "model": fitted_model,
        "scores": scores,
        "brier_score": brier,
        "precision_at_0.45": precision_045,
        "precision_at_0.30": precision_030,
        "n_high_risk_test": n_045,
        "distribution": distribution,
        "distribution_ok": distribution_is_not_collapsed(distribution, len(scores)),
    }


def write_feature_file(output_dir: str) -> str:
    path = os.path.join(output_dir, FEATURES_FILENAME)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(FEATURE_COLS) + "\n")
    return path


def select_winning_variant(results: list[dict[str, Any]]) -> dict[str, Any] | None:
    eligible = [
        result for result in results
        if result["n_high_risk_test"] >= 30
        and result["distribution_ok"]
        and result["brier_score"] < 0.28
    ]

    if not eligible:
        return None

    eligible.sort(
        key=lambda result: (
            result["precision_at_0.45"],
            -result["brier_score"],
            result["n_high_risk_test"],
        ),
        reverse=True,
    )
    return eligible[0]


def train() -> None:
    log.info("Starting Atlas RegretRisk v3 training run.")
    snapshots = fetch_snapshots()
    if not snapshots:
        raise RuntimeError("No usable snapshots found.")

    labelled_rows = make_labelled_rows(snapshots)
    positives = sum(int(row["label"]) for row in labelled_rows)
    if not labelled_rows:
        raise RuntimeError("No labelled t+7 snapshot pairs found.")

    log.info(
        "Label base: rows=%d positives=%d positive_rate=%.2f%% routes=%d date_range=%s to %s",
        len(labelled_rows),
        positives,
        100.0 * positives / len(labelled_rows),
        len({(r["origin_iata"], r["destination_iata"]) for r in labelled_rows}),
        min(r["snapshot_date_obj"] for r in labelled_rows),
        max(r["snapshot_date_obj"] for r in labelled_rows),
    )

    indexes = build_feature_indexes(snapshots)
    market_signals = fetch_market_signals()
    X, y, dates = build_training_matrix(labelled_rows, indexes, market_signals)

    X_train, X_test, y_train, y_test, cutoff_date = temporal_split(X, y, dates)

    log.info("Temporal cutoff date: %s", cutoff_date.isoformat())
    log.info("Train rows: %d | Test rows: %d", len(y_train), len(y_test))
    log.info("Positive rate train: %.2f%%", 100.0 * positive_rate(y_train))
    log.info("Positive rate test : %.2f%%", 100.0 * positive_rate(y_test))

    if int(np.sum(y_train == 1)) < 5 or int(np.sum(y_train == 0)) < 5:
        raise RuntimeError("Training split has fewer than 5 examples in at least one class; cv=5 cannot run.")

    print("")
    print("=== V3 calibration comparison ===")
    print("Same data pipeline, same feature set, same temporal train/test split.")
    print("")

    variants = {
        "v3_current_scaled": CalibratedClassifierCV(
            Pipeline([
                ("scaler", StandardScaler()),
                ("lr", LogisticRegression(
                    max_iter=5000,
                    class_weight="balanced",
                    C=1.0,
                    solver="lbfgs",
                )),
            ]),
            method="sigmoid",
            cv=5,
        ),
    }

    results: list[dict[str, Any]] = []

    for name, model in variants.items():
        model.fit(X_train, y_train)
        scores = model.predict_proba(X_test)[:, 1]
        results.append(evaluate_variant(name, model, y_test, scores))

    winner = results[0]

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    model_path = os.path.join(OUTPUT_DIR, MODEL_FILENAME)
    feature_path = write_feature_file(OUTPUT_DIR)

    artefact = {
        "model": winner["model"],
        "feature_cols": FEATURE_COLS,
        "version": MODEL_VERSION,
        "trained_at": utc_now().isoformat(),
        "train_rows": int(len(y_train)),
        "test_rows": int(len(y_test)),
        "brier_score": float(winner["brier_score"]),
        "precision_at_0.45": float(winner["precision_at_0.45"]),
        "n_high_risk_test": int(winner["n_high_risk_test"]),
        "variant": "v3_current_scaled_converged",
        "architecture": "Pipeline(StandardScaler + LogisticRegression(C=1.0, max_iter=5000, solver=lbfgs)) + CalibratedClassifierCV(sigmoid, cv=5)",
    }

    joblib.dump(artefact, model_path)

    log.info("Wrote model artefact: %s", model_path)
    log.info("Wrote feature columns: %s", feature_path)
    log.info("Training complete: %s", MODEL_VERSION)


if __name__ == "__main__":
    train()
