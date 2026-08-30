"""Unit tests for LightGBM demand and duration model training pipelines (M3-4)."""

from datetime import datetime, timezone

import mlflow
import numpy as np
import pandas as pd
import pytest

from src.training.train_demand import (
    prepare_demand_features,
    train_demand_lightgbm,
)
from src.training.train_duration import (
    prepare_duration_features,
    train_duration_lightgbm,
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


def test_prepare_demand_features():
    """Test demand feature extraction, categorical casting, and cyclical harmonics."""
    df = pd.DataFrame(
        {
            "zone_id": [161, 236],
            "event_timestamp": [
                datetime(2023, 1, 15, 8, 0, tzinfo=timezone.utc),
                datetime(2023, 1, 15, 20, 0, tzinfo=timezone.utc),
            ],
            "pickup_count_last_15m": [5.0, 10.0],
            "pickup_count_last_1h": [20.0, 40.0],
            "pickup_count_last_24h": [200.0, 400.0],
            "pickup_count_same_hour_last_week": [18.0, 38.0],
            "hour_of_day": [8, 20],
            "day_of_week": [6, 6],
            "is_weekend": [1, 1],
            "is_holiday": [0, 0],
            "target_pickup_count_next_1h": [22.0, 45.0],
        }
    )

    X, y = prepare_demand_features(df)
    assert len(X) == 2
    assert y is not None
    assert len(y) == 2
    assert X["zone_id"].dtype.name == "category"
    assert "sin_hour" in X.columns
    assert "cos_hour" in X.columns
    assert "sin_day_of_week" in X.columns
    assert "cos_day_of_week" in X.columns
    assert np.all(X["sin_hour"] >= -1.0) and np.all(X["sin_hour"] <= 1.0)


def test_prepare_duration_features():
    """Test duration feature extraction, corridor parsing, log transform of moving avg and targets."""
    df = pd.DataFrame(
        {
            "corridor_id": ["161_236", "236_142"],
            "event_timestamp": [
                datetime(2023, 1, 15, 8, 0, tzinfo=timezone.utc),
                datetime(2023, 1, 15, 9, 0, tzinfo=timezone.utc),
            ],
            "avg_duration_last_15m": [850.0, 1100.0],
            "avg_duration_last_1h": [900.0, 1200.0],
            "distance_km": [4.2, 5.8],
            "origin_zone_demand_pressure": [25, 40],
            "target_avg_duration_next_1h": [950.0, 1150.0],
        }
    )

    X, y_log, y_raw = prepare_duration_features(df)
    assert len(X) == 2
    assert y_log is not None
    assert y_raw is not None
    assert "pickup_zone_id" in X.columns
    assert "dropoff_zone_id" in X.columns
    assert X["pickup_zone_id"].dtype.name == "category"
    assert X["dropoff_zone_id"].dtype.name == "category"
    assert "log_avg_duration_last_1h" in X.columns
    # Check log1p target
    assert np.isclose(y_log.iloc[0], np.log1p(950.0))
    assert np.isclose(y_raw.iloc[0], 950.0)


def test_train_demand_lightgbm_synthetic(temp_mlflow_env):
    """Test end-to-end LightGBM demand training, lift calculation, and MLflow logging."""
    np.random.seed(42)
    n_train = 200
    n_val = 50

    def make_synthetic_demand_df(n):
        zones = np.random.choice([161, 236, 142, 132], size=n)
        h = np.random.randint(0, 24, size=n)
        dow = np.random.randint(0, 7, size=n)
        p1h = np.random.poisson(lam=20.0, size=n)
        lag = np.maximum(0, p1h + np.random.normal(0, 2, size=n))
        target = np.maximum(0, p1h + np.random.normal(0, 3, size=n))
        return pd.DataFrame(
            {
                "zone_id": zones,
                "event_timestamp": [
                    datetime(2023, 1, 10, int(hi), 0, tzinfo=timezone.utc) for hi in h
                ],
                "pickup_count_last_15m": p1h / 4.0,
                "pickup_count_last_1h": p1h,
                "pickup_count_last_24h": p1h * 24.0,
                "pickup_count_same_hour_last_week": lag,
                "hour_of_day": h,
                "day_of_week": dow,
                "is_weekend": (dow >= 5).astype(int),
                "is_holiday": 0,
                "target_pickup_count_next_1h": target,
            }
        )

    train_df = make_synthetic_demand_df(n_train)
    val_df = make_synthetic_demand_df(n_val)

    result = train_demand_lightgbm(
        train_df=train_df,
        val_df=val_df,
        params={"n_estimators": 20, "min_child_samples": 5},
        baseline_mae=5.0,
        experiment_name="test-demand-lgbm",
        log_to_mlflow=True,
    )

    assert "model" in result
    assert "metrics" in result
    assert "lift_pct" in result
    assert "feature_importances" in result
    assert result["metrics"]["val_mae"] > 0
    assert np.all(result["predictions"] >= 0.0)  # Non-negative constraint
    assert len(result["feature_importances"]) > 0
    assert result["run_id"] is not None

    # Verify run logged in MLflow
    client = mlflow.tracking.MlflowClient()
    run = client.get_run(result["run_id"])
    assert "val_mae" in run.data.metrics
    assert "val_lift_pct_over_baseline" in run.data.metrics
    assert run.data.params["model_family"] == "lightgbm"


def test_train_duration_lightgbm_synthetic(temp_mlflow_env):
    """Test end-to-end LightGBM duration training on log1p targets and evaluation in seconds."""
    np.random.seed(42)
    n_train = 200
    n_val = 50

    def make_synthetic_corridor_df(n):
        corridors = np.random.choice(
            ["161_236", "236_142", "142_132", "132_161"], size=n
        )
        h = np.random.randint(0, 24, size=n)
        dist = np.random.uniform(1.0, 15.0, size=n)
        avg1h = dist * 150.0 + np.random.normal(0, 50, size=n)
        target = np.maximum(60.0, avg1h + np.random.normal(0, 40, size=n))
        return pd.DataFrame(
            {
                "corridor_id": corridors,
                "event_timestamp": [
                    datetime(2023, 1, 10, int(hi), 0, tzinfo=timezone.utc) for hi in h
                ],
                "avg_duration_last_15m": avg1h,
                "avg_duration_last_1h": avg1h,
                "distance_km": dist,
                "origin_zone_demand_pressure": np.random.randint(5, 50, size=n),
                "target_avg_duration_next_1h": target,
            }
        )

    train_df = make_synthetic_corridor_df(n_train)
    val_df = make_synthetic_corridor_df(n_val)

    result = train_duration_lightgbm(
        train_df=train_df,
        val_df=val_df,
        params={"n_estimators": 20, "min_child_samples": 5},
        baseline_mae=50.0,
        experiment_name="test-duration-lgbm",
        log_to_mlflow=True,
    )

    assert "model" in result
    assert "metrics" in result
    assert "lift_pct" in result
    assert "feature_importances" in result
    assert result["metrics"]["val_mae"] > 0
    assert np.all(result["predictions"] >= 60.0)  # 60s minimum floor
    assert len(result["feature_importances"]) > 0
    assert result["run_id"] is not None

    # Verify run logged in MLflow
    client = mlflow.tracking.MlflowClient()
    run = client.get_run(result["run_id"])
    assert "val_mae" in run.data.metrics
    assert "val_lift_pct_over_baseline" in run.data.metrics
    assert run.data.params["target_transformation"] == "log1p"
