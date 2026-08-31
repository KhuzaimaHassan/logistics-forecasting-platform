"""LightGBM training pipeline for NYC taxi zone demand forecasting (M3-4)."""

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
    DEMAND_EXPERIMENT_NAME,
    get_or_create_experiment,
    setup_mlflow,
)
from src.training.metrics import (
    compute_per_entity_metrics,
    compute_regression_metrics,
    compute_relative_lift,
)

logger = logging.getLogger(__name__)

# Canonical feature columns for demand modeling
DEMAND_FEATURE_COLS = [
    "zone_id",
    "pickup_count_last_15m",
    "pickup_count_last_1h",
    "pickup_count_last_24h",
    "pickup_count_same_hour_last_week",
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "is_holiday",
    "sin_hour",
    "cos_hour",
    "sin_day_of_week",
    "cos_day_of_week",
]

CATEGORICAL_FEATURES = ["zone_id"]


def prepare_demand_features(
    df: pd.DataFrame,
    target_col: str = "target_pickup_count_next_1h",
) -> Tuple[pd.DataFrame, Optional[pd.Series]]:
    """Transform Feast historical dataframe into model-ready feature matrix.

    Engineers:
      - Categorical encoding on `zone_id`.
      - Cyclical sine/cosine temporal harmonics for hour and day of week.
      - Null imputation for feature gaps.

    Args:
        df: DataFrame containing Feast historical features.
        target_col: Target column name.

    Returns:
        Tuple of (X: pd.DataFrame, y: Optional[pd.Series]).
    """
    if df.empty:
        raise ValueError("Cannot prepare features from an empty DataFrame.")

    X = df.copy()

    # Ensure zone_id is categorical
    if "zone_id" in X.columns:
        X["zone_id"] = X["zone_id"].astype("category")

    # Extract temporal components if event_timestamp is present and columns missing
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

    # Numeric feature null imputation
    for col in [
        "pickup_count_last_15m",
        "pickup_count_last_1h",
        "pickup_count_last_24h",
        "pickup_count_same_hour_last_week",
        "is_weekend",
        "is_holiday",
    ]:
        if col in X.columns:
            X[col] = X[col].fillna(0.0).astype(float)

    # Select feature columns
    available_cols = [c for c in DEMAND_FEATURE_COLS if c in X.columns]
    X_matrix = X[available_cols].copy()

    y = None
    if target_col in df.columns:
        y = df[target_col].astype(float).copy()

    return X_matrix, y


def train_demand_lightgbm(
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    params: Optional[Dict[str, Any]] = None,
    baseline_mae: float = 4.1326,
    target_col: str = "target_pickup_count_next_1h",
    experiment_name: str = DEMAND_EXPERIMENT_NAME,
    run_name: Optional[str] = None,
    log_to_mlflow: bool = True,
) -> Dict[str, Any]:
    """Train and evaluate LightGBM Regressor for NYC taxi zone demand.

    Args:
        train_df: Training partition (Jan 8-24).
        val_df: Validation partition (Jan 25-31).
        params: Optional hyperparameters for LGBMRegressor.
        baseline_mae: Baseline MAE for relative lift computation.
        target_col: Target column name.
        experiment_name: MLflow experiment name.
        run_name: Optional MLflow run name.
        log_to_mlflow: Whether to log results to MLflow.

    Returns:
        Dictionary containing model, metrics, lift, predictions, feature importances, and run_id.
    """
    logger.info("Preparing feature matrices for demand model...")
    X_train, y_train = prepare_demand_features(train_df, target_col=target_col)
    X_val, y_val = prepare_demand_features(val_df, target_col=target_col)

    if y_train is None or y_val is None:
        raise ValueError("Both train_df and val_df must contain the target column.")

    # Default LightGBM hyperparameters
    default_params: Dict[str, Any] = {
        "objective": "regression",
        "n_estimators": 300,
        "learning_rate": 0.05,
        "num_leaves": 63,
        "min_child_samples": 20,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "random_state": 42,
        "n_jobs": -1,
        "verbose": -1,
    }
    if params:
        default_params.update(params)

    # Instantiate and fit LightGBM model
    model = lgb.LGBMRegressor(**default_params)
    logger.info(
        "Training LightGBM Demand model on %d train samples (%d features)...",
        len(X_train),
        X_train.shape[1],
    )
    model.fit(
        X_train,
        y_train,
        eval_set=[(X_val, y_val)],
        callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False)],
    )

    # Predict on validation set (enforce non-negative demand counts)
    raw_preds = model.predict(X_val)
    y_pred = np.maximum(0.0, raw_preds)

    # Compute validation regression metrics
    metrics = compute_regression_metrics(y_val.to_numpy(), y_pred, prefix="val_")
    lift_pct = compute_relative_lift(
        candidate_mae=metrics["val_mae"], baseline_mae=baseline_mae
    )
    metrics["val_lift_pct_over_baseline"] = lift_pct

    logger.info(
        "Demand LightGBM Validation Metrics: MAE=%.4f (Lift=%.2f%%), RMSE=%.4f, WAPE=%.2f%%, MedAE=%.4f, R2=%.4f (N=%d)",
        metrics["val_mae"],
        lift_pct,
        metrics["val_rmse"],
        metrics["val_wape"],
        metrics["val_medae"],
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

    # Per-zone metrics breakdown
    val_with_preds = val_df.copy()
    val_with_preds["pred_pickup_count"] = y_pred
    per_zone_df = compute_per_entity_metrics(
        val_with_preds,
        entity_col="zone_id",
        target_col=target_col,
        pred_col="pred_pickup_count",
    )

    run_id: Optional[str] = None
    if log_to_mlflow:
        run_name = run_name or "lightgbm_demand_regressor"
        setup_mlflow()
        exp_id = get_or_create_experiment(experiment_name)
        with mlflow.start_run(experiment_id=exp_id, run_name=run_name) as run:
            run_id = run.info.run_id

            # Log hyperparameters
            mlflow.log_params(
                {
                    "model_family": "lightgbm",
                    "model_type": "LGBMRegressor",
                    "target_column": target_col,
                    "train_sample_count": len(X_train),
                    "val_sample_count": len(X_val),
                    "baseline_mae": baseline_mae,
                    "features_count": X_train.shape[1],
                    **default_params,
                }
            )

            # Log metrics
            mlflow.log_metrics(metrics)

            # Log artifacts safely
            try:
                with tempfile.TemporaryDirectory() as tmp_dir:
                    # Feature importances CSV
                    feat_csv = os.path.join(tmp_dir, "feature_importances.csv")
                    importance_df.to_csv(feat_csv, index=False)
                    mlflow.log_artifact(feat_csv, artifact_path="feature_importance")

                    # Per-zone validation breakdown CSV
                    zone_csv = os.path.join(tmp_dir, "per_zone_validation_metrics.csv")
                    per_zone_df.to_csv(zone_csv, index=False)
                    mlflow.log_artifact(zone_csv, artifact_path="evaluation")
            except Exception as art_err:
                logger.warning("Could not log artifacts to MLflow: %s", art_err)

            # Log model artifact
            try:
                mlflow.lightgbm.log_model(
                    lgb_model=model.booster_,
                    artifact_path="model",
                    registered_model_name="demand_lightgbm_model",
                )
            except Exception as model_err:
                logger.warning("Could not log model binary to MLflow: %s", model_err)

            logger.info("Logged Demand LightGBM run to MLflow Run ID: %s", run_id)

    return {
        "model": model,
        "metrics": metrics,
        "lift_pct": lift_pct,
        "feature_importances": importance_df,
        "per_zone_metrics": per_zone_df,
        "predictions": y_pred,
        "run_id": run_id,
    }
