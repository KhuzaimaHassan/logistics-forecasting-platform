"""Seasonal-naive baseline models and evaluation pipelines for demand and ETA forecasting."""

import logging
import os
import tempfile
from typing import Any, Dict, Optional

import mlflow
import numpy as np
import pandas as pd

from src.common.mlflow_utils import (
    CORRIDOR_EXPERIMENT_NAME,
    DEMAND_EXPERIMENT_NAME,
    get_or_create_experiment,
    setup_mlflow,
)
from src.training.metrics import compute_per_entity_metrics, compute_regression_metrics

logger = logging.getLogger(__name__)


class DemandSeasonalNaiveBaseline:
    """Seasonal-naive baseline predictor for taxi zone hourly demand.

    Primary Strategy:
      Predicts `pickup_count_same_hour_last_week` (demand at same zone, same day of week, same hour).

    Fallback Hierarchy:
      1. `pickup_count_same_hour_last_week` (exact seasonal lag)
      2. `pickup_count_last_24h` / 24.0 (recent daily rate fallback)
      3. `pickup_count_last_1h` (recent hourly volume fallback)
      4. 0 (inactive/unobserved zone fallback)
    """

    def __init__(self, target_col: str = "target_pickup_count_next_1h"):
        self.target_col = target_col

    def predict(self, df: pd.DataFrame) -> np.ndarray:
        """Generate demand predictions for an observation dataframe.

        Args:
            df: DataFrame containing Feast historical features.

        Returns:
            Numpy array of predicted next-hour pickup counts (non-negative floats).
        """
        if df.empty:
            return np.array([], dtype=np.float64)

        # Primary prediction: same hour last week
        if "pickup_count_same_hour_last_week" in df.columns:
            preds = df["pickup_count_same_hour_last_week"].astype("float64").copy()
        else:
            preds = pd.Series(np.nan, index=df.index, dtype=np.float64)

        # Fallback 1: pickup_count_last_24h / 24
        if "pickup_count_last_24h" in df.columns:
            fallback_24h = (df["pickup_count_last_24h"] / 24.0).astype("float64")
            preds = preds.fillna(fallback_24h)

        # Fallback 2: pickup_count_last_1h
        if "pickup_count_last_1h" in df.columns:
            fallback_1h = df["pickup_count_last_1h"].astype("float64")
            preds = preds.fillna(fallback_1h)

        # Final fallback: 0
        preds = preds.fillna(0.0)

        # Enforce non-negativity constraint
        return np.maximum(0.0, preds.to_numpy())


class CorridorDurationBaseline:
    """Seasonal-naive / moving-average baseline predictor for corridor trip durations (ETA).

    Primary Strategy:
      Predicts `avg_duration_last_1h` (most recent observed hourly moving average for the corridor).

    Fallback Hierarchy:
      1. `avg_duration_last_1h`
      2. Distance velocity heuristic: distance_km * 144.0s (assumes ~25 km/h urban velocity)
      3. Global median NYC taxi trip duration: 700.0s (~11.6 minutes)
    """

    def __init__(
        self,
        target_col: str = "target_avg_duration_next_1h",
        default_velocity_sec_per_km: float = 144.0,  # 25 km/h = 144 s/km
        default_duration_sec: float = 700.0,
        min_duration_sec: float = 60.0,
    ):
        self.target_col = target_col
        self.default_velocity_sec_per_km = default_velocity_sec_per_km
        self.default_duration_sec = default_duration_sec
        self.min_duration_sec = min_duration_sec

    def predict(self, df: pd.DataFrame) -> np.ndarray:
        """Generate trip duration predictions for an observation dataframe.

        Args:
            df: DataFrame containing Feast corridor features.

        Returns:
            Numpy array of predicted next-hour average durations in seconds.
        """
        if df.empty:
            return np.array([], dtype=np.float64)

        # Primary prediction: moving average of last 1 hour
        if "avg_duration_last_1h" in df.columns:
            preds = df["avg_duration_last_1h"].astype("float64").copy()
        else:
            preds = pd.Series(np.nan, index=df.index, dtype=np.float64)

        # Fallback 1: distance velocity heuristic
        if "distance_km" in df.columns:
            fallback_dist = (
                df["distance_km"] * self.default_velocity_sec_per_km
            ).astype("float64")
            preds = preds.fillna(fallback_dist)

        # Final fallback: global default
        preds = preds.fillna(self.default_duration_sec)

        # Enforce minimum duration bound
        return np.maximum(self.min_duration_sec, preds.to_numpy())


def evaluate_demand_baseline(
    val_df: pd.DataFrame,
    experiment_name: str = DEMAND_EXPERIMENT_NAME,
    run_name: Optional[str] = None,
    log_to_mlflow: bool = True,
    target_col: str = "target_pickup_count_next_1h",
) -> Dict[str, Any]:
    """Evaluate Demand Seasonal-Naive Baseline on validation dataset and log to MLflow.

    Args:
        val_df: Validation DataFrame (chronologically isolated split).
        experiment_name: MLflow experiment name.
        run_name: Optional custom run name (defaults to 'baseline_seasonal_naive_demand').
        log_to_mlflow: If True, logs metrics and artifacts to MLflow server.
        target_col: Target column name.

    Returns:
        Dictionary containing predictions, evaluation metrics, entity breakdowns, and run_id.
    """
    if val_df.empty:
        raise ValueError("Cannot evaluate demand baseline on empty validation set.")
    if target_col not in val_df.columns:
        raise ValueError(f"Target column '{target_col}' missing from validation set.")

    baseline = DemandSeasonalNaiveBaseline(target_col=target_col)
    y_pred = baseline.predict(val_df)
    y_true = val_df[target_col].to_numpy()

    metrics = compute_regression_metrics(y_true, y_pred, prefix="val_")
    logger.info(
        "Demand Seasonal-Naive Baseline Validation Metrics: MAE=%.4f, RMSE=%.4f, WAPE=%.2f%%, MedAE=%.4f (N=%d)",
        metrics["val_mae"],
        metrics["val_rmse"],
        metrics["val_wape"],
        metrics["val_medae"],
        int(metrics["val_sample_count"]),
    )

    # Per-zone breakdown
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
        run_name = run_name or "baseline_seasonal_naive_demand"
        setup_mlflow()
        exp_id = get_or_create_experiment(experiment_name)
        with mlflow.start_run(experiment_id=exp_id, run_name=run_name) as run:
            run_id = run.info.run_id
            # Log model parameters
            mlflow.log_params(
                {
                    "model_family": "seasonal_naive",
                    "model_type": "DemandSeasonalNaiveBaseline",
                    "primary_feature": "pickup_count_same_hour_last_week",
                    "target_column": target_col,
                    "val_sample_count": len(val_df),
                }
            )
            # Log metrics
            mlflow.log_metrics(metrics)

            # Log per-zone breakdown artifact
            try:
                with tempfile.TemporaryDirectory() as tmp_dir:
                    breakdown_csv = os.path.join(
                        tmp_dir, "per_zone_validation_metrics.csv"
                    )
                    per_zone_df.to_csv(breakdown_csv, index=False)
                    mlflow.log_artifact(breakdown_csv, artifact_path="evaluation")
            except Exception as art_err:
                logger.warning("Could not log per-zone artifact to MLflow: %s", art_err)

            logger.info("Logged Demand Baseline to MLflow Run ID: %s", run_id)

    return {
        "predictions": y_pred,
        "metrics": metrics,
        "per_entity_metrics": per_zone_df,
        "run_id": run_id,
    }


def evaluate_corridor_duration_baseline(
    val_df: pd.DataFrame,
    experiment_name: str = CORRIDOR_EXPERIMENT_NAME,
    run_name: Optional[str] = None,
    log_to_mlflow: bool = True,
    target_col: str = "target_avg_duration_next_1h",
) -> Dict[str, Any]:
    """Evaluate Corridor Duration Baseline on validation dataset and log to MLflow.

    Args:
        val_df: Validation DataFrame (chronologically isolated split).
        experiment_name: MLflow experiment name.
        run_name: Optional custom run name (defaults to 'baseline_moving_avg_duration').
        log_to_mlflow: If True, logs metrics and artifacts to MLflow server.
        target_col: Target column name.

    Returns:
        Dictionary containing predictions, evaluation metrics, entity breakdowns, and run_id.
    """
    if val_df.empty:
        raise ValueError("Cannot evaluate corridor baseline on empty validation set.")
    if target_col not in val_df.columns:
        raise ValueError(f"Target column '{target_col}' missing from validation set.")

    baseline = CorridorDurationBaseline(target_col=target_col)
    y_pred = baseline.predict(val_df)
    y_true = val_df[target_col].to_numpy()

    metrics = compute_regression_metrics(y_true, y_pred, prefix="val_")
    logger.info(
        "Corridor Duration Baseline Validation Metrics: MAE=%.2fs (%.2f min), RMSE=%.2fs, WAPE=%.2f%%, MedAE=%.2fs (N=%d)",
        metrics["val_mae"],
        metrics["val_mae"] / 60.0,
        metrics["val_rmse"],
        metrics["val_wape"],
        metrics["val_medae"],
        int(metrics["val_sample_count"]),
    )

    # Per-corridor breakdown
    val_with_preds = val_df.copy()
    val_with_preds["pred_avg_duration"] = y_pred
    per_corridor_df = compute_per_entity_metrics(
        val_with_preds,
        entity_col="corridor_id",
        target_col=target_col,
        pred_col="pred_avg_duration",
    )

    run_id: Optional[str] = None
    if log_to_mlflow:
        run_name = run_name or "baseline_moving_avg_duration"
        setup_mlflow()
        exp_id = get_or_create_experiment(experiment_name)
        with mlflow.start_run(experiment_id=exp_id, run_name=run_name) as run:
            run_id = run.info.run_id
            # Log model parameters
            mlflow.log_params(
                {
                    "model_family": "moving_average_baseline",
                    "model_type": "CorridorDurationBaseline",
                    "primary_feature": "avg_duration_last_1h",
                    "fallback_velocity_kmh": 25.0,
                    "target_column": target_col,
                    "val_sample_count": len(val_df),
                }
            )
            # Log metrics
            mlflow.log_metrics(metrics)

            # Log per-corridor breakdown artifact
            try:
                with tempfile.TemporaryDirectory() as tmp_dir:
                    breakdown_csv = os.path.join(
                        tmp_dir, "per_corridor_validation_metrics.csv"
                    )
                    per_corridor_df.to_csv(breakdown_csv, index=False)
                    mlflow.log_artifact(breakdown_csv, artifact_path="evaluation")
            except Exception as art_err:
                logger.warning(
                    "Could not log per-corridor artifact to MLflow: %s", art_err
                )

            logger.info(
                "Logged Corridor Duration Baseline to MLflow Run ID: %s", run_id
            )

    return {
        "predictions": y_pred,
        "metrics": metrics,
        "per_entity_metrics": per_corridor_df,
        "run_id": run_id,
    }
