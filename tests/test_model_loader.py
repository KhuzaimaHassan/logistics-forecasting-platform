"""Unit tests for ModelLoaderService and MLflow Production model resolution (M5-1, ADR-020)."""

from typing import Generator

import lightgbm as lgb
import mlflow
import mlflow.lightgbm
import numpy as np
import pandas as pd
import pytest
from mlflow.tracking import MlflowClient

from src.common.mlflow_utils import setup_mlflow
from src.serving.model_loader import LoadedModelInfo, ModelLoaderService
from src.training.baseline import (
    CorridorDurationBaseline,
    DemandSeasonalNaiveBaseline,
)
from src.training.pipeline import (
    DEMAND_MODEL_NAME,
    DURATION_MODEL_NAME,
)

# Preventive suppression of MLflow 3.x stage API deprecation warnings in test suite
pytestmark = pytest.mark.filterwarnings("ignore::FutureWarning:mlflow.*")


@pytest.fixture
def temp_mlflow_env(tmp_path) -> Generator[str, None, None]:
    """Isolated SQLite MLflow tracking environment fixture."""
    sqlite_db = tmp_path / "test_mlflow.db"
    tracking_uri = f"sqlite:///{sqlite_db.as_posix()}"
    old_uri = mlflow.get_tracking_uri()
    setup_mlflow(tracking_uri)
    yield tracking_uri
    mlflow.set_tracking_uri(old_uri)


def _train_and_register_dummy_lgb_model(
    tracking_uri: str,
    model_name: str,
    feature_names: list[str],
    stage: str = "Production",
) -> str:
    """Helper to train, log, register, and stage-transition a lightweight LightGBM model."""
    client = MlflowClient(tracking_uri=tracking_uri)
    exp = client.get_experiment_by_name("test_exp")
    if exp is None:
        exp_id = client.create_experiment("test_exp")
    else:
        exp_id = exp.experiment_id

    # Create dummy training data
    n_samples = 20
    data = {col: np.random.randn(n_samples) for col in feature_names}
    if "zone_id" in data:
        data["zone_id"] = np.random.randint(1, 10, size=n_samples)
    if "pickup_zone_id" in data:
        data["pickup_zone_id"] = np.random.randint(1, 10, size=n_samples)
    if "dropoff_zone_id" in data:
        data["dropoff_zone_id"] = np.random.randint(1, 10, size=n_samples)

    X = pd.DataFrame(data)
    for cat_col in ["zone_id", "pickup_zone_id", "dropoff_zone_id"]:
        if cat_col in X.columns:
            X[cat_col] = X[cat_col].astype("category")

    y = np.random.uniform(5.0, 50.0, size=n_samples)

    model = lgb.LGBMRegressor(n_estimators=5, min_child_samples=1, verbose=-1)
    model.fit(X, y)

    with mlflow.start_run(experiment_id=exp_id) as run:
        run_id = run.info.run_id
        mlflow.lightgbm.log_model(
            lgb_model=model.booster_,
            artifact_path="model",
            registered_model_name=model_name,
            pip_requirements=["lightgbm==4.6.0"],
        )

    # Resolve latest registered version and transition stage
    versions = client.get_latest_versions(model_name)
    v = versions[0]
    if stage:
        client.transition_model_version_stage(
            name=model_name,
            version=v.version,
            stage=stage,
            archive_existing_versions=True,
        )

    return run_id


def test_resolve_production_model_version(temp_mlflow_env):
    """Test resolution of active model versions in Production stage."""
    loader = ModelLoaderService(tracking_uri=temp_mlflow_env)

    # 1. Non-existent model returns None
    assert loader.resolve_production_model_version("non_existent_model") is None

    # 2. Register model in Staging only -> returns None
    _train_and_register_dummy_lgb_model(
        tracking_uri=temp_mlflow_env,
        model_name="staging_only_model",
        feature_names=["f1", "f2"],
        stage="Staging",
    )
    assert loader.resolve_production_model_version("staging_only_model") is None

    # 3. Register model in Production -> resolves active version
    _train_and_register_dummy_lgb_model(
        tracking_uri=temp_mlflow_env,
        model_name="prod_model",
        feature_names=["f1", "f2"],
        stage="Production",
    )
    v_prod = loader.resolve_production_model_version("prod_model")
    assert v_prod is not None
    assert v_prod.current_stage == "Production"
    assert str(v_prod.version) == "1"


def test_load_demand_model_from_registry(temp_mlflow_env):
    """Test loading and inference of a real Production demand model from MLflow."""
    feature_cols = [
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
    run_id = _train_and_register_dummy_lgb_model(
        tracking_uri=temp_mlflow_env,
        model_name=DEMAND_MODEL_NAME,
        feature_names=feature_cols,
        stage="Production",
    )

    loader = ModelLoaderService(tracking_uri=temp_mlflow_env)
    info = loader.load_demand_model()

    assert isinstance(info, LoadedModelInfo)
    assert info.model_name == DEMAND_MODEL_NAME
    assert info.version == "1"
    assert info.stage == "Production"
    assert info.run_id == run_id
    assert info.status == "production"
    assert info.is_fallback is False

    # Test prediction
    test_df = pd.DataFrame(
        [
            {
                "zone_id": 161,
                "pickup_count_last_15m": 12,
                "pickup_count_last_1h": 45,
                "pickup_count_last_24h": 900,
                "pickup_count_same_hour_last_week": 40,
                "hour_of_day": 14,
                "day_of_week": 1,
                "is_weekend": 0,
                "is_holiday": 0,
                "sin_hour": 0.5,
                "cos_hour": 0.86,
                "sin_day_of_week": 0.78,
                "cos_day_of_week": 0.62,
            }
        ]
    )
    test_df["zone_id"] = test_df["zone_id"].astype("category")
    preds = info.predict(test_df)
    assert isinstance(preds, np.ndarray)
    assert len(preds) == 1
    assert not np.isnan(preds[0])


def test_load_duration_model_from_registry(temp_mlflow_env):
    """Test loading and inference of a real Production duration model from MLflow."""
    feature_cols = [
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
    run_id = _train_and_register_dummy_lgb_model(
        tracking_uri=temp_mlflow_env,
        model_name=DURATION_MODEL_NAME,
        feature_names=feature_cols,
        stage="Production",
    )

    loader = ModelLoaderService(tracking_uri=temp_mlflow_env)
    info = loader.load_duration_model()

    assert isinstance(info, LoadedModelInfo)
    assert info.model_name == DURATION_MODEL_NAME
    assert info.version == "1"
    assert info.stage == "Production"
    assert info.run_id == run_id
    assert info.status == "production"
    assert info.is_fallback is False

    test_df = pd.DataFrame(
        [
            {
                "pickup_zone_id": 161,
                "dropoff_zone_id": 236,
                "avg_duration_last_15m": 900.0,
                "avg_duration_last_1h": 920.0,
                "log_avg_duration_last_1h": np.log1p(920.0),
                "distance_km": 4.5,
                "origin_zone_demand_pressure": 45,
                "hour_of_day": 14,
                "day_of_week": 1,
                "is_weekend": 0,
                "sin_hour": 0.5,
                "cos_hour": 0.86,
                "sin_day_of_week": 0.78,
                "cos_day_of_week": 0.62,
            }
        ]
    )
    test_df["pickup_zone_id"] = test_df["pickup_zone_id"].astype("category")
    test_df["dropoff_zone_id"] = test_df["dropoff_zone_id"].astype("category")

    preds = info.predict(test_df)
    assert isinstance(preds, np.ndarray)
    assert len(preds) == 1
    assert not np.isnan(preds[0])


def test_fallback_to_baseline_when_models_missing(temp_mlflow_env):
    """Test graceful fallback to baseline heuristic estimators when registry has no Production models."""
    loader = ModelLoaderService(tracking_uri=temp_mlflow_env)

    demand_info = loader.load_demand_model()
    assert demand_info.is_fallback is True
    assert demand_info.status == "baseline_fallback"
    assert demand_info.version == "baseline-fallback"
    assert isinstance(demand_info.model, DemandSeasonalNaiveBaseline)

    duration_info = loader.load_duration_model()
    assert duration_info.is_fallback is True
    assert duration_info.status == "baseline_fallback"
    assert duration_info.version == "baseline-fallback"
    assert isinstance(duration_info.model, CorridorDurationBaseline)

    # Check fallback prediction works on sample input
    sample_demand_df = pd.DataFrame([{"pickup_count_same_hour_last_week": 33.0}])
    pred_demand = demand_info.predict(sample_demand_df)
    assert pred_demand[0] == 33.0

    sample_dur_df = pd.DataFrame([{"avg_duration_last_1h": 850.0}])
    pred_dur = duration_info.predict(sample_dur_df)
    assert pred_dur[0] == 850.0


def test_fallback_when_mlflow_server_unreachable(monkeypatch):
    """Test that connection failure to an unreachable MLflow server degrades safely without crash."""
    # Speed up connection rejection instead of standard 5-retry backoff
    monkeypatch.setenv("MLFLOW_HTTP_REQUEST_MAX_RETRIES", "0")
    monkeypatch.setenv("MLFLOW_HTTP_REQUEST_TIMEOUT", "1")
    unreachable_uri = "http://127.0.0.1:59999"
    loader = ModelLoaderService(tracking_uri=unreachable_uri)

    demand_info = loader.load_demand_model()
    assert demand_info.is_fallback is True
    assert demand_info.status == "baseline_fallback"

    duration_info = loader.load_duration_model()
    assert duration_info.is_fallback is True
    assert duration_info.status == "baseline_fallback"


def test_load_all_warmup_and_health_metadata(temp_mlflow_env):
    """Test load_all() executes warmup passes and populates /health check metadata."""
    feature_cols = [
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
    _train_and_register_dummy_lgb_model(
        tracking_uri=temp_mlflow_env,
        model_name=DEMAND_MODEL_NAME,
        feature_names=feature_cols,
        stage="Production",
    )

    loader = ModelLoaderService(tracking_uri=temp_mlflow_env)
    models = loader.load_all()

    assert len(models) == 2
    assert DEMAND_MODEL_NAME in models
    assert DURATION_MODEL_NAME in models

    health_meta = loader.get_health_metadata()
    assert health_meta["all_models_loaded"] is True
    assert (
        health_meta["has_fallback_models"] is True
    )  # duration is fallback, demand is prod
    assert health_meta["models"][DEMAND_MODEL_NAME]["status"] == "production"
    assert health_meta["models"][DURATION_MODEL_NAME]["status"] == "baseline_fallback"
