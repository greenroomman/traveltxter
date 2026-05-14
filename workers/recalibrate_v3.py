"""
workers/recalibrate_v3.py

Audit the v3 MIZAR model calibrator and produce a recalibrated artefact.

Three-gate audit before any artefact is saved:
  Gate 1 - Raw model AUC >= 0.60 (signal exists)
  Gate 2 - Recalibrated model produces >= 10 decisions above 0.70 on dataset
  Gate 3 - Held-out Brier score improves by >= 0.001 with no AUC collapse

Output artefact (only if all gates pass):
  ~/mizar-api/atlas_regret_risk_v3_0_1_recalibrated.joblib

NEVER overwrites the live model file.
Run from: cd ~/traveltxter && python3 workers/recalibrate_v3.py
"""

import os
import sys
import copy
import logging
import numpy as np
import pandas as pd
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

MIZAR_API_DIR = Path.home() / "mizar-api"
MODEL_IN  = MIZAR_API_DIR / "atlas_regret_risk_v2.joblib"
MODEL_OUT = MIZAR_API_DIR / "atlas_regret_risk_v3_0_1_recalibrated.joblib"

AUC_GATE              = 0.60
MIN_ABOVE_70          = 10
BRIER_IMPROVEMENT_MIN = 0.001

for env_candidate in [
    Path.home() / "traveltxter" / ".env",
    Path.home() / "mizar-api" / ".env",
    Path.home() / ".env",
]:
    if env_candidate.exists():
        from dotenv import load_dotenv
        load_dotenv(env_candidate)
        log.info(f"Loaded env from {env_candidate}")
        break

SUPABASE_URL = (
    os.environ.get("MIZAR_SUPABASE_URL")
    or os.environ.get("SUPABASE_URL")
)
SUPABASE_KEY = (
    os.environ.get("MIZAR_SUPABASE_SERVICE_ROLE_KEY")
    or os.environ.get("SUPABASE_SERVICE_KEY")
)

if not SUPABASE_URL or not SUPABASE_KEY:
    log.error("Missing Supabase credentials.")
    log.error("Set MIZAR_SUPABASE_URL and MIZAR_SUPABASE_SERVICE_ROLE_KEY.")
    sys.exit(1)

import joblib
from supabase import create_client
from sklearn.calibration import CalibratedClassifierCV
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, brier_score_loss, average_precision_score

log.info("=" * 60)
log.info("PHASE 0  Load and inspect model")
log.info("=" * 60)

if not MODEL_IN.exists():
    log.error(f"Model not found: {MODEL_IN}")
    sys.exit(1)

model = joblib.load(MODEL_IN)
log.info(f"Loaded: {MODEL_IN}")
log.info(f"Top-level type: {type(model).__name__}")


def print_structure(obj, indent=0, max_depth=5):
    pad = "  " * indent
    name = type(obj).__name__
    if indent > max_depth:
        return
    log.info(f"{pad}{name}")
    if hasattr(obj, "steps"):
        for sname, sobj in obj.steps:
            log.info(f"{pad}  step: [{sname}]")
            print_structure(sobj, indent + 2, max_depth)
    elif hasattr(obj, "calibrated_classifiers_"):
        n = len(obj.calibrated_classifiers_)
        log.info(f"{pad}  calibrated_classifiers_: {n}")
        cc = obj.calibrated_classifiers_[0]
        log.info(f"{pad}  base estimator: {type(cc.estimator).__name__}")
        if hasattr(cc, "calibrators"):
            for i, cal in enumerate(cc.calibrators):
                log.info(f"{pad}  calibrator[{i}]: {type(cal).__name__}")
    elif hasattr(obj, "estimator"):
        log.info(f"{pad}  .estimator: {type(obj.estimator).__name__}")
        print_structure(obj.estimator, indent + 1, max_depth)


print_structure(model)


def get_feature_names(obj):
    if hasattr(obj, "feature_names_in_"):
        return list(obj.feature_names_in_)
    if hasattr(obj, "steps"):
        for _, step in obj.steps:
            names = get_feature_names(step)
            if names:
                return names
    return None


feature_names = get_feature_names(model)
if feature_names:
    log.info(f"Feature names found in model ({len(feature_names)}): {feature_names}")
else:
    log.warning("Feature names not stored in model. Will use snapshot numeric columns.")

log.info("=" * 60)
log.info("PHASE 1  Load clean labelled snapshots")
log.info("=" * 60)

client = create_client(SUPABASE_URL, SUPABASE_KEY)

SNAPSHOT_COLS = (
    "snapshot_id, snapshot_date, origin_iata, destination_iata, "
    "outbound_date, dtd, day_of_week_departure, day_of_week_snapshot, "
    "is_school_holiday_window, is_bank_holiday_adjacent, "
    "price_gbp, carrier_count, lcc_present, direct, stops, "
    "cabin_class, seats_remaining, price_z_score, price_percentile, "
    "price_ratio, baseline_mu, baseline_sigma, trend_3d, trend_7d, "
    "volatility_7d, direction_consistency_7d, season_bucket, "
    "days_to_next_bank_holiday, trip_overlaps_holiday, "
    "holiday_intensity_score, jet_fuel_7d_change_pct, offer_count, "
    "cheapest_offer_gbp, most_expensive_offer_gbp, shi_score, "
    "route_distance_km, route_type, origin_type, "
    "jet_fuel_usd_gal, carrier_primary_iata, rose_10pct"
)

all_rows = []
offset = 0
batch = 1000

while True:
    resp = (
        client.table("snapshots")
        .select(SNAPSHOT_COLS)
        .eq("crisis_label_contaminated", False)
        .not_.is_("price_t7", "null")
        .order("snapshot_date", desc=False)
        .range(offset, offset + batch - 1)
        .execute()
    )
    rows = resp.data
    if not rows:
        break
    all_rows.extend(rows)
    log.info(f"  Fetched {len(all_rows)} rows...")
    if len(rows) < batch:
        break
    offset += batch

df = pd.DataFrame(all_rows)
df = df[df["rose_10pct"].notna()].copy()
log.info(f"Total clean labelled rows: {len(df)}")
log.info(f"Positive class (rose 10pct): {df['rose_10pct'].sum()} ({df['rose_10pct'].mean():.3f})")

log.info("=" * 60)
log.info("PHASE 2  Prepare features")
log.info("=" * 60)

BOOL_COLS = [
    "is_school_holiday_window", "is_bank_holiday_adjacent",
    "lcc_present", "direct", "trip_overlaps_holiday",
]
for col in BOOL_COLS:
    if col in df.columns:
        df[col] = df[col].astype(float)

y = df["rose_10pct"].astype(int).values

NUMERIC_FALLBACK = [
    "dtd", "price_gbp", "carrier_count", "stops", "seats_remaining",
    "price_z_score", "price_percentile", "price_ratio",
    "baseline_mu", "baseline_sigma", "trend_3d", "trend_7d",
    "volatility_7d", "direction_consistency_7d",
    "days_to_next_bank_holiday", "holiday_intensity_score",
    "jet_fuel_7d_change_pct", "offer_count",
    "cheapest_offer_gbp", "most_expensive_offer_gbp",
    "shi_score", "route_distance_km", "jet_fuel_usd_gal",
    "is_school_holiday_window", "is_bank_holiday_adjacent",
    "lcc_present", "direct", "trip_overlaps_holiday",
]

if feature_names:
    missing = [f for f in feature_names if f not in df.columns]
    if missing:
        log.error(f"Features in model but missing from snapshots: {missing}")
        sys.exit(1)
    X_df = df[feature_names].copy()
    log.info(f"Using {len(feature_names)} model-specified features")
else:
    available = [c for c in NUMERIC_FALLBACK if c in df.columns]
    X_df = df[available].copy()
    log.info(f"Using {len(available)} numeric fallback features: {available}")

X_df = X_df.fillna(X_df.median(numeric_only=True))
X = X_df.values.astype(float)

log.info(f"Feature matrix: {X.shape}")
log.info(f"Label distribution: {np.bincount(y)}  [0=no-rise  1=rose]")

X_train, X_val, y_train, y_val = train_test_split(
    X, y, test_size=0.20, random_state=42, stratify=y
)
log.info(f"Train: {len(X_train)}  Val: {len(X_val)}")

log.info("=" * 60)
log.info("PHASE 3  Raw signal audit  (Gate 1)")
log.info("=" * 60)


def _dig_to_base(obj):
    if isinstance(obj, CalibratedClassifierCV):
        cc = obj.calibrated_classifiers_[0]
        inner = cc.estimator
        if isinstance(inner, Pipeline):
            base = inner.steps[-1][1]
            pre  = Pipeline(inner.steps[:-1]) if len(inner.steps) > 1 else None
            return base, pre
        return inner, None
    if isinstance(obj, Pipeline):
        last = obj.steps[-1][1]
        pre  = Pipeline(obj.steps[:-1]) if len(obj.steps) > 1 else None
        if isinstance(last, CalibratedClassifierCV):
            inner_base, inner_pre = _dig_to_base(last)
            if pre is not None and inner_pre is not None:
                combined_steps = pre.steps + inner_pre.steps
                return inner_base, Pipeline(combined_steps)
            return inner_base, pre or inner_pre
        return last, pre
    return obj, None


def get_raw_logits(model_obj, X_data):
    base, preprocessor = _dig_to_base(model_obj)
    Xp = preprocessor.transform(X_data) if preprocessor is not None else X_data
    if hasattr(base, "decision_function"):
        return base.decision_function(Xp)
    if hasattr(base, "predict_proba"):
        p = np.clip(base.predict_proba(Xp)[:, 1], 1e-9, 1 - 1e-9)
        return np.log(p / (1 - p))
    raise ValueError(
        f"Base estimator {type(base).__name__} has neither "
        "decision_function nor predict_proba."
    )


try:
    raw_all   = get_raw_logits(model, X)
    raw_train = get_raw_logits(model, X_train)
    raw_val   = get_raw_logits(model, X_val)
except Exception as exc:
    log.error(f"Failed to extract raw logits: {exc}")
    log.error("Model structure may not match expected patterns.")
    log.error("Inspect the structure printed in Phase 0 and adjust extraction logic.")
    sys.exit(1)

log.info(f"Raw logit range (all):   [{raw_all.min():.4f}, {raw_all.max():.4f}]")
log.info(f"Raw logit mean (all):    {raw_all.mean():.4f}")
log.info(f"Positives mean logit:    {raw_train[y_train == 1].mean():.4f}")
log.info(f"Negatives mean logit:    {raw_train[y_train == 0].mean():.4f}")

auc_raw = roc_auc_score(y_val, raw_val)
log.info(f"Raw logit AUC (val):     {auc_raw:.4f}  [gate >= {AUC_GATE}]")

if auc_raw < AUC_GATE:
    log.error(
        f"GATE 1 FAILED - AUC {auc_raw:.4f} < {AUC_GATE}. "
        "Raw model has insufficient discriminative power. "
        "Recalibration cannot manufacture signal. Full retrain required."
    )
    sys.exit(1)

log.info(f"GATE 1 PASSED  AUC {auc_raw:.4f}")

log.info("=" * 60)
log.info("PHASE 4  Current calibrated score diagnostics")
log.info("=" * 60)

cur_all = model.predict_proba(X)[:, 1]
cur_val = model.predict_proba(X_val)[:, 1]

brier_cur   = brier_score_loss(y_val, cur_val)
auc_cur     = roc_auc_score(y_val, cur_val)
ap_cur      = average_precision_score(y_val, cur_val)
above70_cur = int((cur_all >= 0.70).sum())

log.info(f"Score range:         [{cur_all.min():.4f}, {cur_all.max():.4f}]")
log.info(f"Brier score (val):   {brier_cur:.4f}")
log.info(f"AUC (val):           {auc_cur:.4f}")
log.info(f"Avg precision (val): {ap_cur:.4f}")
log.info(f"Decisions >= 0.70:   {above70_cur}")
log.info("Score distribution (current):")
for lo, hi in [(0, .1), (.1, .2), (.2, .3), (.3, .5), (.5, .7), (.7, 1.0)]:
    n = int(((cur_all >= lo) & (cur_all < hi)).sum())
    bar = "x" * (n // 20)
    log.info(f"  [{lo:.1f}, {hi:.1f}): {n:>5}  {bar}")

log.info("=" * 60)
log.info("PHASE 5  Fit new Platt calibrator")
log.info("=" * 60)

new_platt = LogisticRegression(C=1.0, solver="lbfgs", max_iter=1000)
new_platt.fit(raw_train.reshape(-1, 1), y_train)

log.info(f"Platt coef:      {new_platt.coef_[0][0]:.4f}")
log.info(f"Platt intercept: {new_platt.intercept_[0]:.4f}")

new_all = new_platt.predict_proba(raw_all.reshape(-1, 1))[:, 1]
new_val = new_platt.predict_proba(raw_val.reshape(-1, 1))[:, 1]

above70_new = int((new_all >= 0.70).sum())
log.info(f"Score range (recalibrated, all):  [{new_all.min():.4f}, {new_all.max():.4f}]")
log.info(f"Decisions >= 0.70 (recalibrated): {above70_new}  [gate >= {MIN_ABOVE_70}]")
log.info("Score distribution (recalibrated):")
for lo, hi in [(0, .1), (.1, .2), (.2, .3), (.3, .5), (.5, .7), (.7, 1.0)]:
    n = int(((new_all >= lo) & (new_all < hi)).sum())
    bar = "x" * (n // 20)
    log.info(f"  [{lo:.1f}, {hi:.1f}): {n:>5}  {bar}")

if above70_new < MIN_ABOVE_70:
    log.error(
        f"GATE 2 FAILED - only {above70_new} decisions >= 0.70 "
        f"(minimum {MIN_ABOVE_70}). Calibrator does not restore usable spread."
    )
    sys.exit(1)

log.info(f"GATE 2 PASSED  {above70_new} decisions >= 0.70")

log.info("=" * 60)
log.info("PHASE 6  Held-out validation  (Gate 3)")
log.info("=" * 60)

brier_new         = brier_score_loss(y_val, new_val)
auc_new           = roc_auc_score(y_val, new_val)
ap_new            = average_precision_score(y_val, new_val)
brier_improvement = brier_cur - brier_new

log.info(f"Brier   current: {brier_cur:.4f}  new: {brier_new:.4f}  improvement: {brier_improvement:+.4f}")
log.info(f"AUC     current: {auc_cur:.4f}  new: {auc_new:.4f}")
log.info(f"Avg P   current: {ap_cur:.4f}  new: {ap_new:.4f}")

log.info("Precision by threshold (recalibrated, val set):")
for t in [0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85]:
    preds  = (new_val >= t).astype(int)
    n_pred = int(preds.sum())
    if n_pred > 0:
        tp   = int(((preds == 1) & (y_val == 1)).sum())
        prec = tp / n_pred
        log.info(f"  t={t:.2f}  predicted={n_pred:>4}  TP={tp:>4}  precision={prec:.3f}")
    else:
        log.info(f"  t={t:.2f}  predicted=   0")

brier_ok = brier_improvement >= BRIER_IMPROVEMENT_MIN
auc_ok   = auc_new >= (auc_raw - 0.05)

if not brier_ok:
    log.error(
        f"GATE 3 FAILED - Brier improvement {brier_improvement:+.4f} "
        f"< {BRIER_IMPROVEMENT_MIN}. New calibrator does not improve held-out validation."
    )
    sys.exit(1)

if not auc_ok:
    log.error(
        f"GATE 3 FAILED - AUC dropped from raw {auc_raw:.4f} "
        f"to recalibrated {auc_new:.4f} (> 5pp collapse). Rejecting."
    )
    sys.exit(1)

log.info(f"GATE 3 PASSED  Brier improved {brier_improvement:+.4f}  AUC holds at {auc_new:.4f}")

log.info("=" * 60)
log.info("PHASE 7  Build recalibrated artefact")
log.info("=" * 60)


class RecalibratedMIZARModel:
    """
    Wraps the original v3 model, replacing its calibration layer with a
    new Platt scaler fitted on clean labelled snapshots.
    Interface: predict_proba(X) -> ndarray shape (n, 2)
    """
    VERSION = "v3_0_1_recalibrated"

    def __init__(self, original_model, platt_calibrator, feature_names=None):
        self._original   = original_model
        self._platt      = platt_calibrator
        self._feat_names = feature_names
        self.classes_    = np.array([0, 1])

    def _raw_logits(self, X):
        return get_raw_logits(self._original, X)

    def predict_proba(self, X):
        logits = self._raw_logits(X)
        p1 = self._platt.predict_proba(logits.reshape(-1, 1))[:, 1]
        return np.column_stack([1.0 - p1, p1])

    def predict(self, X):
        return (self.predict_proba(X)[:, 1] >= 0.5).astype(int)

    @property
    def feature_names_in_(self):
        return self._feat_names


new_model = RecalibratedMIZARModel(
    original_model=model,
    platt_calibrator=new_platt,
    feature_names=feature_names,
)

smoke          = new_model.predict_proba(X_val)
smoke_above_70 = int((smoke[:, 1] >= 0.70).sum())
smoke_range    = (float(smoke[:, 1].min()), float(smoke[:, 1].max()))
log.info(f"Wrapper smoke test - score range (val): [{smoke_range[0]:.4f}, {smoke_range[1]:.4f}]")
log.info(f"Wrapper smoke test - decisions >= 0.70 (val): {smoke_above_70}")

if smoke_above_70 == 0:
    log.error("Wrapper smoke test FAILED - no decisions above 0.70 from wrapper.")
    log.error("Artefact not saved.")
    sys.exit(1)

joblib.dump(new_model, MODEL_OUT)
log.info(f"Artefact saved: {MODEL_OUT}")

log.info("=" * 60)
log.info("AUDIT COMPLETE - ALL GATES PASSED")
log.info("=" * 60)
log.info(f"Gate 1  Raw AUC:               {auc_raw:.4f}  (>= {AUC_GATE})")
log.info(f"Gate 2  Decisions >= 0.70:     {above70_new}    (>= {MIN_ABOVE_70})")
log.info(f"Gate 3  Brier improvement:     {brier_improvement:+.4f}  (>= {BRIER_IMPROVEMENT_MIN})")
log.info(f"Output: {MODEL_OUT}")
log.info("")
log.info("Next steps (do not skip):")
log.info("  1. Review the precision-by-threshold table above.")
log.info("  2. Confirm score distribution looks plausible, not artificially clustered.")
log.info("  3. Only promote to production after human sign-off.")
log.info("  4. Deploy as v3_0_1. Update Brain and Notion before any outreach.")