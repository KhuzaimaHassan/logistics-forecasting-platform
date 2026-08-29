"""Evaluation metrics module for logistics forecasting and ETA predictions."""

import logging
from typing import Dict

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def compute_regression_metrics(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    prefix: str = "val_",
) -> Dict[str, float]:
    """Compute standard regression metrics for forecasting evaluation.

    Calculates:
      - MAE (Mean Absolute Error)
      - RMSE (Root Mean Squared Error)
      - WAPE (Weighted Absolute Percentage Error) = sum(|y - y_hat|) / sum(y) * 100%
      - MedAE (Median Absolute Error)
      - R2 Score (Coefficient of Determination)
      - Mean Bias Error (MBE) = mean(y_hat - y)

    Args:
        y_true: Ground truth target array.
        y_pred: Model prediction array.
        prefix: Prefix to prepend to metric dictionary keys.

    Returns:
        Dictionary mapping metric names to computed float values.

    Raises:
        ValueError: If array lengths do not match or arrays are empty.
    """
    y_t = np.asarray(y_true, dtype=np.float64)
    y_p = np.asarray(y_pred, dtype=np.float64)

    if len(y_t) == 0 or len(y_p) == 0:
        raise ValueError("Cannot compute metrics on empty arrays.")

    if len(y_t) != len(y_p):
        raise ValueError(
            f"Array length mismatch: y_true has {len(y_t)} items, y_pred has {len(y_p)} items."
        )

    # Remove any paired NaNs/infs if present
    valid_mask = np.isfinite(y_t) & np.isfinite(y_p)
    if not np.all(valid_mask):
        valid_count = int(np.sum(valid_mask))
        dropped_count = len(y_t) - valid_count
        logger.warning(
            "Dropped %d non-finite (NaN/Inf) pairs during metric evaluation. Computing on %d valid pairs.",
            dropped_count,
            valid_count,
        )
        if valid_count == 0:
            raise ValueError("No finite values remaining to compute metrics.")
        y_t = y_t[valid_mask]
        y_p = y_p[valid_mask]

    errors = y_p - y_t
    abs_errors = np.abs(errors)

    mae = float(np.mean(abs_errors))
    mse = float(np.mean(errors**2))
    rmse = float(np.sqrt(mse))
    medae = float(np.median(abs_errors))
    mbe = float(np.mean(errors))

    # WAPE: sum(|errors|) / sum(y_true) * 100
    sum_true = float(np.sum(y_t))
    if sum_true > 0:
        wape = float((np.sum(abs_errors) / sum_true) * 100.0)
    else:
        # Fallback when ground truth sum is 0 (e.g. inactive zone)
        wape = 0.0 if np.all(abs_errors == 0) else 100.0

    # R2 Score
    ss_res = float(np.sum(errors**2))
    ss_tot = float(np.sum((y_t - np.mean(y_t)) ** 2))
    if ss_tot > 0:
        r2 = float(1.0 - (ss_res / ss_tot))
    else:
        r2 = 1.0 if ss_res == 0 else 0.0

    return {
        f"{prefix}mae": round(mae, 4),
        f"{prefix}rmse": round(rmse, 4),
        f"{prefix}wape": round(wape, 4),
        f"{prefix}medae": round(medae, 4),
        f"{prefix}mbe": round(mbe, 4),
        f"{prefix}r2": round(r2, 4),
        f"{prefix}sample_count": float(len(y_t)),
    }


def compute_per_entity_metrics(
    df: pd.DataFrame,
    entity_col: str,
    target_col: str,
    pred_col: str,
) -> pd.DataFrame:
    """Compute evaluation metrics broken down by individual entity (zone or corridor).

    Args:
        df: DataFrame containing predictions and ground truth.
        entity_col: Column name identifying the entity (e.g., 'zone_id', 'corridor_id').
        target_col: Column name for ground truth values.
        pred_col: Column name for model predictions.

    Returns:
        DataFrame indexed by entity with per-entity MAE, RMSE, WAPE, and sample counts.
    """
    records = []
    for entity_id, group in df.groupby(entity_col):
        y_true = group[target_col].to_numpy()
        y_pred = group[pred_col].to_numpy()

        try:
            metrics = compute_regression_metrics(y_true, y_pred, prefix="")
            records.append(
                {
                    entity_col: entity_id,
                    "mae": metrics["mae"],
                    "rmse": metrics["rmse"],
                    "wape": metrics["wape"],
                    "medae": metrics["medae"],
                    "sample_count": int(metrics["sample_count"]),
                }
            )
        except Exception as e:
            logger.debug(
                "Skipping entity %s in breakdown due to error: %s", entity_id, e
            )

    result_df = pd.DataFrame(records)
    if not result_df.empty:
        result_df = result_df.sort_values(
            by="sample_count", ascending=False
        ).reset_index(drop=True)
    return result_df


def compute_relative_lift(
    candidate_mae: float,
    baseline_mae: float,
) -> float:
    """Compute percentage error reduction (lift) of a candidate model over baseline.

    Positive lift indicates candidate model improved upon baseline error.

    Args:
        candidate_mae: Candidate model MAE.
        baseline_mae: Baseline model MAE.

    Returns:
        Percentage error reduction (e.g. 15.5 for 15.5% reduction in MAE).
    """
    if baseline_mae <= 0:
        return 0.0
    return round(((baseline_mae - candidate_mae) / baseline_mae) * 100.0, 2)
