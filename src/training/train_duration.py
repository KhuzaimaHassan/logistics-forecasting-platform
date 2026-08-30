"""LightGBM training pipeline for NYC taxi corridor duration (ETA) forecasting (M3-4)."""

import logging
import os
import tempfile
from typing import Any, Dict, Optional, Tuple

import lightgbm as lgb
import mlflow
import mlflow.lightgbm
import numpy as np
import pandas as pd

from src.common.mlflow_utils import (
    DURATION_EXPERIMENT_NAME,
    get_or_create_experiment,
    setup_mlflow,
)
from src.training.metrics import (
    compute_per_entity_metrics,
    compute_regression_metrics,
    compute_relative_lift,
)

logger = logging.getLogger(__name__)

# Canonical feature columns for duration modeling
DURATION_FEATURE_COLS = [
    "pickup_zone_id",
    "dropoff_zone_id",
    "avg_duration_last_15m",
    "avg_duration_last_1h",
    "log_avg_duration_last_1h",
    "distance_km",
    "origin_zone_demand_pressure",
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "sin_hour",
    "cos_hour",
    "sin_day_of_week",
    "cos_day_of_week",
]

CATEGORICAL_FEATURES = ["pickup_zone_id", "dropoff_zone_id"]


def prepare_duration_features(
    df: pd.DataFrame,
    target_col: str = "target_avg_duration_next_1h",
) -> Tuple[pd.DataFrame, Optional[pd.Series], Optional[pd.Series]]:
    """Transform Feast corridor dataframe into model-ready feature matrix with log1p target.

    Engineers:
      - Categorical `pickup_zone_id` and `dropoff_zone_id` extracted from `corridor_id`.
      - Cyclical sine/cosine temporal harmonics for hour and day of week.
      - Log-transformed moving average feature `log_avg_duration_last_1h`.
      - Transformed target `log1p(target_avg_duration_next_1h)`.

    Args:
        df: DataFrame containing Feast corridor historical features.
        target_col: Target column name in seconds.

    Returns:
        Tuple of (X: pd.DataFrame, y_log: Optional[pd.Series], y_raw: Optional[pd.Series]).
    """
    if df.empty:
        raise ValueError("Cannot prepare features from an empty DataFrame.")

    X = df.copy()

    # Extract pickup and dropoff zone IDs from corridor_id (e.g., '161_236')
    if "corridor_id" in X.columns:
        corridor_parts = X["corridor_id"].astype(str).str.split("_", expand=True)
        if corridor_parts.shape[1] >= 2:
            X["pickup_zone_id"] = corridor_parts[0].astype("category")
            X["dropoff_zone_id"] = corridor_parts[1].astype("category")
        else:
            X["pickup_zone_id"] = 0
            X["dropoff_zone_id"] = 0
            X["pickup_zone_id"] = X["pickup_zone_id"].astype("category")
            X["dropoff_zone_id"] = X["dropoff_zone_id"].astype("category")

    # Extract temporal components if event_timestamp is present
    if "event_timestamp" in X.columns:
        ts = pd.to_datetime(X["event_timestamp"], utc=True)
        if "hour_of_day" not in X.columns:
            X["hour_of_day"] = ts.dt.hour
        if "day_of_week" not in X.columns:
            X["day_of_week"] = ts.dt.dayofweek
        if "is_weekend" not in X.columns:
            X["is_weekend"] = (ts.dt.dayofweek >= 5).astype(int)

    # Cyclical harmonic encodings
    hours = X["hour_of_day"].astype(float) if "hour_of_day" in X.columns else 0.0
    dows = X["day_of_week"].astype(float) if "day_of_week" in X.columns else 0.0
    X["sin_hour"] = np.sin(2 * np.pi * hours / 24.0)
    X["cos_hour"] = np.cos(2 * np.pi * hours / 24.0)
    X["sin_day_of_week"] = np.sin(2 * np.pi * dows / 7.0)
    X["cos_day_of_week"] = np.cos(2 * np.pi * dows / 7.0)

    # Numeric feature imputation
    if "avg_duration_last_1h" in X.columns:
        X["avg_duration_last_1h"] = (
            X["avg_duration_last_1h"].fillna(700.0).astype(float)
        )
        X["log_avg_duration_last_1h"] = np.log1p(X["avg_duration_last_1h"])
    else:
        X["avg_duration_last_1h"] = 700.0
        X["log_avg_duration_last_1h"] = np.log1p(700.0)

    if "avg_duration_last_15m" in X.columns:
        X["avg_duration_last_15m"] = (
            X["avg_duration_last_15m"].fillna(X["avg_duration_last_1h"]).astype(float)
        )
    if "distance_km" in X.columns:
        X["distance_km"] = X["distance_km"].fillna(3.5).astype(float)
    if "origin_zone_demand_pressure" in X.columns:
        X["origin_zone_demand_pressure"] = (
            X["origin_zone_demand_pressure"].fillna(0.0).astype(float)
        )

    available_cols = [c for c in DURATION_FEATURE_COLS if c in X.columns]
    X_matrix = X[available_cols].copy()

    y_log = None
    y_raw = None
    if target_col in df.columns:
        y_raw = df[target_col].astype(float).copy()
        # Enforce minimum 60.0s before log transformation
        y_clipped = np.maximum(60.0, y_raw.to_numpy())
        y_log = pd.Series(np.log1p(y_clipped), index=df.index)

    return X_matrix, y_log, y_raw


def train_duration_lightgbm(
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    params: Optional[Dict[str, Any]] = None,
    baseline_mae: float = 456.95,
    target_col: str = "target_avg_duration_next_1h",
    min_duration_sec: float = 60.0,
    experiment_name: str = DURATION_EXPERIMENT_NAME,
    run_name: Optional[str] = None,
    log_to_mlflow: bool = True,
) -> Dict[str, Any]:
    """Train and evaluate LightGBM Regressor on log1p targets for corridor trip duration.

    Args:
        train_df: Training partition (Jan 8-24).
        val_df: Validation partition (Jan 25-31).
        params: Optional hyperparameters for LGBMRegressor.
        baseline_mae: Baseline MAE in seconds for relative lift computation.
        target_col: Target column name.
        min_duration_sec: Lower bound floor for predicted durations.
        experiment_name: MLflow experiment name.
        run_name: Optional MLflow run name.
        log_to_mlflow: Whether to log results to MLflow.

    Returns:
        Dictionary containing model, metrics, lift, predictions, feature importances, and run_id.
    """
    logger.info("Preparing feature matrices for corridor duration model...")
    X_train, y_train_log, y_train_raw = prepare_duration_features(
        train_df, target_col=target_col
    )
    X_val, y_val_log, y_val_raw = prepare_duration_features(
        val_df, target_col=target_col
    )

    if y_train_log is None or y_val_log is None or y_val_raw is None:
        raise ValueError("Both train_df and val_df must contain the target column.")

    # Default LightGBM hyperparameters for log-transformed ETA regression
    default_params: Dict[str, Any] = {
        "objective": "regression",
        "n_estimators": 300,
        "learning_rate": 0.05,
        "num_leaves": 63,
        "min_child_samples": 30,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "random_state": 42,
        "n_jobs": -1,
        "verbose": -1,
    }
    if params:
        default_params.update(params)

    # Instantiate and fit LightGBM model on log1p(duration)
    model = lgb.LGBMRegressor(**default_params)
    logger.info(
        "Training LightGBM Duration model on %d train samples (%d features)...",
        len(X_train),
        X_train.shape[1],
    )
    model.fit(
        X_train,
        y_train_log,
        eval_set=[(X_val, y_val_log)],
        callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False)],
    )

    # Predict in log-space and invert to original seconds with 60s floor
    preds_log = model.predict(X_val)
    y_pred_seconds = np.maximum(min_duration_sec, np.expm1(preds_log))

    # Compute validation metrics on original seconds
    metrics = compute_regression_metrics(
        y_val_raw.to_numpy(), y_pred_seconds, prefix="val_"
    )
    lift_pct = compute_relative_lift(
        candidate_mae=metrics["val_mae"], baseline_mae=baseline_mae
    )
    metrics["val_lift_pct_over_baseline"] = lift_pct

    logger.info(
        "Duration LightGBM Validation Metrics: MAE=%.2fs (%.2f min, Lift=%.2f%%), RMSE=%.2fs, WAPE=%.2f%%, MedAE=%.2fs (%.2f min), R2=%.4f (N=%d)",
        metrics["val_mae"],
        metrics["val_mae"] / 60.0,
        lift_pct,
        metrics["val_rmse"],
        metrics["val_wape"],
        metrics["val_medae"],
        metrics["val_medae"] / 60.0,
        metrics["val_r2"],
        int(metrics["val_sample_count"]),
    )

    # Compute feature importances
    importance_df = (
        pd.DataFrame(
            {
                "feature": X_train.columns,
                "importance_split": model.booster_.feature_importance(
                    importance_type="split"
                ),
                "importance_gain": model.booster_.feature_importance(
                    importance_type="gain"
                ),
            }
        )
        .sort_values(by="importance_gain", ascending=False)
        .reset_index(drop=True)
    )

    # Per-corridor breakdown
    val_with_preds = val_df.copy()
    val_with_preds["pred_avg_duration"] = y_pred_seconds
    per_corridor_df = compute_per_entity_metrics(
        val_with_preds,
        entity_col="corridor_id",
        target_col=target_col,
        pred_col="pred_avg_duration",
    )

    run_id: Optional[str] = None
    if log_to_mlflow:
        run_name = run_name or "lightgbm_duration_regressor_log1p"
        setup_mlflow()
        exp_id = get_or_create_experiment(experiment_name)
        with mlflow.start_run(experiment_id=exp_id, run_name=run_name) as run:
            run_id = run.info.run_id

            # Log hyperparameters
            mlflow.log_params(
                {
                    "model_family": "lightgbm",
                    "model_type": "LGBMRegressor",
                    "target_transformation": "log1p",
                    "target_column": target_col,
                    "train_sample_count": len(X_train),
                    "val_sample_count": len(X_val),
                    "baseline_mae": baseline_mae,
                    "min_duration_sec": min_duration_sec,
                    "features_count": X_train.shape[1],
                    **default_params,
                }
            )

            # Log metrics in natural seconds
            mlflow.log_metrics(metrics)

            # Log explanatory metadata
            mlflow.set_tag(
                "notes",
                "Trained on log1p(duration) targets to stabilize right-tail taximeter anomalies; metrics evaluated on natural seconds with 60s floor.",
            )

            # Log artifacts safely
            try:
                with tempfile.TemporaryDirectory() as tmp_dir:
                    # Feature importances CSV
                    feat_csv = os.path.join(tmp_dir, "feature_importances.csv")
                    importance_df.to_csv(feat_csv, index=False)
                    mlflow.log_artifact(feat_csv, artifact_path="feature_importance")

                    # Per-corridor validation breakdown CSV
                    corridor_csv = os.path.join(
                        tmp_dir, "per_corridor_validation_metrics.csv"
                    )
                    per_corridor_df.to_csv(corridor_csv, index=False)
                    mlflow.log_artifact(corridor_csv, artifact_path="evaluation")
            except Exception as art_err:
                logger.warning("Could not log artifacts to MLflow: %s", art_err)

            # Log model artifact
            try:
                mlflow.lightgbm.log_model(
                    lgb_model=model.booster_,
                    artifact_path="model",
                    registered_model_name="corridor_duration_lightgbm_model",
                )
            except Exception as model_err:
                logger.warning("Could not log model binary to MLflow: %s", model_err)

            logger.info("Logged Duration LightGBM run to MLflow Run ID: %s", run_id)

    return {
        "model": model,
        "metrics": metrics,
        "lift_pct": lift_pct,
        "feature_importances": importance_df,
        "per_corridor_metrics": per_corridor_df,
        "predictions": y_pred_seconds,
        "run_id": run_id,
    }
