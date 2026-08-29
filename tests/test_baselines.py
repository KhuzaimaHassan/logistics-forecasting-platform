"""Unit tests for regression metrics and seasonal-naive baseline evaluators (M3-3)."""

from datetime import datetime, timezone

import mlflow
import numpy as np
import pandas as pd
import pytest

from src.training.baseline import (
    CorridorDurationBaseline,
    DemandSeasonalNaiveBaseline,
    evaluate_corridor_duration_baseline,
    evaluate_demand_baseline,
)
from src.training.metrics import (
    compute_per_entity_metrics,
    compute_regression_metrics,
    compute_relative_lift,
)


@pytest.fixture
def temp_mlflow_env(tmp_path, monkeypatch):
    """Fixture providing a temporary SQLite MLflow tracking environment."""
    sqlite_db = tmp_path / "mlflow_test.db"
    tracking_uri = f"sqlite:///{sqlite_db.as_posix()}"
    old_uri = mlflow.get_tracking_uri()
    monkeypatch.setenv("MLFLOW_TRACKING_URI", tracking_uri)
    mlflow.set_tracking_uri(tracking_uri)
    yield tracking_uri
    mlflow.set_tracking_uri(old_uri)


def test_compute_regression_metrics_exact_values():
    """Test regression metric calculations against analytically known arrays."""
    y_true = np.array([10.0, 20.0, 30.0, 40.0])
    y_pred = np.array([12.0, 18.0, 33.0, 40.0])
    # Errors: [+2, -2, +3, 0]
    # Abs errors: [2, 2, 3, 0] -> MAE = 7/4 = 1.75
    # Sq errors: [4, 4, 9, 0] -> MSE = 17/4 = 4.25 -> RMSE = sqrt(4.25) = 2.06155
    # Sum true: 100.0 -> WAPE = (7 / 100) * 100 = 7.0%
    # MedAE: median([0, 2, 2, 3]) = 2.0

    metrics = compute_regression_metrics(y_true, y_pred, prefix="test_")
    assert metrics["test_mae"] == 1.75
    assert round(metrics["test_rmse"], 4) == 2.0616
    assert metrics["test_wape"] == 7.0
    assert metrics["test_medae"] == 2.0
    assert metrics["test_sample_count"] == 4.0


def test_compute_regression_metrics_edge_cases():
    """Test edge cases: empty arrays, length mismatch, zero sum targets, and perfect predictions."""
    # Empty array
    with pytest.raises(ValueError, match="Cannot compute metrics on empty arrays"):
        compute_regression_metrics(np.array([]), np.array([]))

    # Length mismatch
    with pytest.raises(ValueError, match="Array length mismatch"):
        compute_regression_metrics(np.array([1.0, 2.0]), np.array([1.0]))

    # Perfect predictions
    perfect_metrics = compute_regression_metrics(
        np.array([5.0, 15.0]), np.array([5.0, 15.0])
    )
    assert perfect_metrics["val_mae"] == 0.0
    assert perfect_metrics["val_rmse"] == 0.0
    assert perfect_metrics["val_wape"] == 0.0
    assert perfect_metrics["val_r2"] == 1.0

    # Zero ground truth sum with non-zero predictions
    zero_true_metrics = compute_regression_metrics(
        np.array([0.0, 0.0]), np.array([2.0, 3.0])
    )
    assert zero_true_metrics["val_mae"] == 2.5
    assert zero_true_metrics["val_wape"] == 100.0


def test_compute_per_entity_metrics():
    """Test entity-level metrics grouping and aggregation."""
    df = pd.DataFrame(
        {
            "zone_id": [161, 161, 236, 236],
            "target": [10.0, 20.0, 5.0, 15.0],
            "pred": [12.0, 18.0, 5.0, 10.0],
        }
    )
    result = compute_per_entity_metrics(
        df,
        entity_col="zone_id",
        target_col="target",
        pred_col="pred",
    )
    assert len(result) == 2
    assert "zone_id" in result.columns
    assert "mae" in result.columns
    assert "wape" in result.columns
    # Zone 161: errors [+2, -2] -> MAE = 2.0
    # Zone 236: errors [0, -5] -> MAE = 2.5
    z161_row = result[result["zone_id"] == 161].iloc[0]
    z236_row = result[result["zone_id"] == 236].iloc[0]
    assert z161_row["mae"] == 2.0
    assert z236_row["mae"] == 2.5


def test_compute_relative_lift():
    """Test relative lift calculation."""
    # Baseline MAE 10.0, candidate MAE 8.0 -> 20.0% lift
    assert compute_relative_lift(candidate_mae=8.0, baseline_mae=10.0) == 20.0
    # Candidate worse: baseline 10.0, candidate 12.0 -> -20.0% lift
    assert compute_relative_lift(candidate_mae=12.0, baseline_mae=10.0) == -20.0
    # Zero baseline
    assert compute_relative_lift(candidate_mae=5.0, baseline_mae=0.0) == 0.0


def test_demand_seasonal_naive_baseline_predictions_and_fallbacks():
    """Test DemandSeasonalNaiveBaseline prediction hierarchy and non-negativity."""
    df = pd.DataFrame(
        {
            "pickup_count_same_hour_last_week": [15.0, np.nan, np.nan, np.nan],
            "pickup_count_last_24h": [100.0, 48.0, np.nan, np.nan],
            "pickup_count_last_1h": [10.0, 5.0, 8.0, np.nan],
        }
    )
    baseline = DemandSeasonalNaiveBaseline()
    preds = baseline.predict(df)

    assert len(preds) == 4
    # Row 0: Uses same hour last week -> 15.0
    assert preds[0] == 15.0
    # Row 1: Fallback to last_24h / 24 -> 48 / 24 = 2.0
    assert preds[1] == 2.0
    # Row 2: Fallback to last_1h -> 8.0
    assert preds[2] == 8.0
    # Row 3: All nulls fallback to 0.0
    assert preds[3] == 0.0


def test_corridor_duration_baseline_predictions_and_fallbacks():
    """Test CorridorDurationBaseline prediction hierarchy and minimum clipping."""
    df = pd.DataFrame(
        {
            "avg_duration_last_1h": [900.0, np.nan, np.nan, 30.0],
            "distance_km": [5.0, 2.0, np.nan, 1.0],
        }
    )
    baseline = CorridorDurationBaseline(min_duration_sec=60.0)
    preds = baseline.predict(df)

    assert len(preds) == 4
    # Row 0: Uses avg_duration_last_1h -> 900.0s
    assert preds[0] == 900.0
    # Row 1: Fallback to distance * 144 -> 2.0 * 144 = 288.0s
    assert preds[1] == 288.0
    # Row 2: All nulls fallback to global default 700.0s
    assert preds[2] == 700.0
    # Row 3: 30.0s clipped to minimum 60.0s
    assert preds[3] == 60.0


def test_evaluate_demand_baseline_mlflow(temp_mlflow_env):
    """Test evaluate_demand_baseline runner with MLflow metric and artifact logging."""
    val_df = pd.DataFrame(
        {
            "zone_id": [161, 236, 161, 236],
            "event_timestamp": [
                datetime(2023, 1, 25, 10, 0, tzinfo=timezone.utc),
                datetime(2023, 1, 25, 10, 0, tzinfo=timezone.utc),
                datetime(2023, 1, 25, 11, 0, tzinfo=timezone.utc),
                datetime(2023, 1, 25, 11, 0, tzinfo=timezone.utc),
            ],
            "target_pickup_count_next_1h": [20, 30, 25, 35],
            "pickup_count_same_hour_last_week": [18, 28, 22, 32],
            "pickup_count_last_24h": [200, 300, 250, 350],
            "pickup_count_last_1h": [20, 30, 25, 35],
        }
    )

    result = evaluate_demand_baseline(
        val_df=val_df,
        experiment_name="test-demand-baseline",
        log_to_mlflow=True,
    )

    assert "metrics" in result
    assert "val_mae" in result["metrics"]
    assert result["metrics"]["val_mae"] == 2.5  # Abs errors: [2, 2, 3, 3] -> mean=2.5
    assert result["run_id"] is not None

    # Verify run logged in MLflow
    client = mlflow.tracking.MlflowClient()
    run = client.get_run(result["run_id"])
    assert run.data.metrics["val_mae"] == 2.5
    assert run.data.params["model_family"] == "seasonal_naive"


def test_evaluate_corridor_duration_baseline_mlflow(temp_mlflow_env):
    """Test evaluate_corridor_duration_baseline runner with MLflow metric and artifact logging."""
    val_df = pd.DataFrame(
        {
            "corridor_id": ["161_236", "236_142", "161_236"],
            "event_timestamp": [
                datetime(2023, 1, 25, 10, 0, tzinfo=timezone.utc),
                datetime(2023, 1, 25, 10, 0, tzinfo=timezone.utc),
                datetime(2023, 1, 25, 11, 0, tzinfo=timezone.utc),
            ],
            "target_avg_duration_next_1h": [1000.0, 1200.0, 1050.0],
            "avg_duration_last_1h": [950.0, 1150.0, 1000.0],
            "distance_km": [4.5, 6.0, 4.5],
        }
    )

    result = evaluate_corridor_duration_baseline(
        val_df=val_df,
        experiment_name="test-corridor-baseline",
        log_to_mlflow=True,
    )

    assert "metrics" in result
    assert "val_mae" in result["metrics"]
    assert result["metrics"]["val_mae"] == 50.0  # Abs errors: [50, 50, 50] -> mean=50.0
    assert result["run_id"] is not None

    # Verify run logged in MLflow
    client = mlflow.tracking.MlflowClient()
    run = client.get_run(result["run_id"])
    assert run.data.metrics["val_mae"] == 50.0
    assert run.data.params["model_family"] == "moving_average_baseline"
