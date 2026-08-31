"""Unit and integration tests for the end-to-end training pipeline (M3-5)."""

from datetime import datetime, timezone

import mlflow
import pandas as pd
import pytest
from feast import FeatureStore, RepoConfig
from sqlalchemy import create_engine, text

from src.features.entities import corridor_entity, zone_entity
from src.features.views import create_file_backed_feature_views
from src.training.pipeline import (
    promote_model_to_production,
    run_training_pipeline,
)


@pytest.fixture
def temp_mlflow_env(tmp_path, monkeypatch):
    """Fixture providing an isolated SQLite MLflow tracking environment."""
    sqlite_db = tmp_path / "mlflow_test.db"
    tracking_uri = f"sqlite:///{sqlite_db.as_posix()}"
    old_uri = mlflow.get_tracking_uri()
    monkeypatch.setenv("MLFLOW_TRACKING_URI", tracking_uri)
    mlflow.set_tracking_uri(tracking_uri)
    yield tracking_uri
    mlflow.set_tracking_uri(old_uri)


def test_promote_model_to_production_logic(temp_mlflow_env):
    """Test model promotion when candidate improves over baseline vs when it fails."""
    client = mlflow.tracking.MlflowClient()
    exp_id = client.create_experiment("test-promotion-exp")

    # Create dummy run and register model
    with mlflow.start_run(experiment_id=exp_id) as run:
        run_id = run.info.run_id
        # Log dummy artifact
        mlflow.log_param("test", "true")

    # Register model
    client.create_registered_model("test_demand_model")
    v1 = client.create_model_version(
        name="test_demand_model",
        source=f"runs:/{run_id}/model",
        run_id=run_id,
    )

    # 1. Candidate outperforms baseline (candidate_mae=2.0 < baseline_mae=4.0)
    outcome_pass = promote_model_to_production(
        client=client,
        model_name="test_demand_model",
        candidate_run_id=run_id,
        candidate_mae=2.0,
        baseline_mae=4.0,
    )
    assert outcome_pass["promoted"] is True
    assert outcome_pass["version"] == str(v1.version)
    assert outcome_pass["stage"] in ("Production", "champion_alias")

    # 2. Candidate underperforms baseline (candidate_mae=5.0 > baseline_mae=4.0)
    outcome_fail = promote_model_to_production(
        client=client,
        model_name="test_demand_model",
        candidate_run_id=run_id,
        candidate_mae=5.0,
        baseline_mae=4.0,
    )
    assert outcome_fail["promoted"] is False
    assert outcome_fail["stage"] == "Staging"


def test_run_training_pipeline_end_to_end(temp_mlflow_env, tmp_path):
    """Test full training pipeline execution on synthetic data with Feast and SQLite."""
    db_path = tmp_path / "warehouse_test.db"
    engine = create_engine(f"sqlite:///{db_path.as_posix()}")

    zone_parquet = tmp_path / "zone_feats.parquet"
    corridor_parquet = tmp_path / "corridor_feats.parquet"
    registry_db = tmp_path / "feast_registry.db"

    # Setup database tables
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE trips (
                pickup_zone_id INTEGER NOT NULL,
                dropoff_zone_id INTEGER NOT NULL,
                pickup_datetime TIMESTAMP NOT NULL,
                dropoff_datetime TIMESTAMP NOT NULL,
                trip_duration_seconds INTEGER NOT NULL,
                trip_distance_km REAL NOT NULL
            );
        """))
        conn.execute(text("""
            CREATE TABLE taxi_zones (
                zone_id INTEGER PRIMARY KEY,
                zone_name TEXT
            );
        """))

    # Populate synthetic trips
    t_start = datetime(2023, 1, 8, 0, 0, 0, tzinfo=timezone.utc)
    t_split = datetime(2023, 1, 10, 0, 0, 0, tzinfo=timezone.utc)
    t_end = datetime(2023, 1, 11, 0, 0, 0, tzinfo=timezone.utc)

    zones = [161, 236]
    zones_df = pd.DataFrame({"zone_id": zones, "zone_name": ["Zone_161", "Zone_236"]})
    zones_df.to_sql("taxi_zones", engine, if_exists="append", index=False)

    # Generate trip records
    trips_data = []
    for h in range(72):
        cur_t = t_start + pd.Timedelta(hours=h)
        for z in zones:
            for _ in range(5):
                trips_data.append(
                    {
                        "pickup_zone_id": z,
                        "dropoff_zone_id": 236 if z == 161 else 161,
                        "pickup_datetime": cur_t + pd.Timedelta(minutes=10),
                        "dropoff_datetime": cur_t + pd.Timedelta(minutes=25),
                        "trip_duration_seconds": 900,
                        "trip_distance_km": 3.5,
                    }
                )
    pd.DataFrame(trips_data).to_sql("trips", engine, if_exists="append", index=False)

    # Generate offline feature store files
    zone_feat_rows = []
    corridor_feat_rows = []
    for h in range(72):
        cur_t = t_start + pd.Timedelta(hours=h)
        for z in zones:
            zone_feat_rows.append(
                {
                    "zone_id": z,
                    "pickup_datetime": cur_t,
                    "created_at": cur_t,
                    "pickup_count_last_15m": 2,
                    "pickup_count_last_1h": 5,
                    "pickup_count_last_24h": 120,
                    "pickup_count_same_hour_last_week": 5,
                    "hour_of_day": cur_t.hour,
                    "day_of_week": cur_t.weekday(),
                    "is_weekend": int(cur_t.weekday() >= 5),
                    "is_holiday": 0,
                }
            )
            corridor_feat_rows.append(
                {
                    "corridor_id": f"{z}_{236 if z == 161 else 161}",
                    "dropoff_datetime": cur_t,
                    "created_at": cur_t,
                    "avg_duration_last_15m": 900.0,
                    "avg_duration_last_1h": 900.0,
                    "distance_km": 3.5,
                    "origin_zone_demand_pressure": 5,
                }
            )

    pd.DataFrame(zone_feat_rows).to_parquet(zone_parquet)
    pd.DataFrame(corridor_feat_rows).to_parquet(corridor_parquet)

    views = create_file_backed_feature_views(
        zone_parquet_path=str(zone_parquet),
        corridor_parquet_path=str(corridor_parquet),
    )
    store = FeatureStore(
        config=RepoConfig(
            registry=str(registry_db),
            project="test_pipeline",
            provider="local",
        )
    )
    store.apply([zone_entity, corridor_entity] + views)

    # Run pipeline
    summary = run_training_pipeline(
        start_time=t_start,
        end_time=t_end,
        split_timestamp=t_split,
        store=store,
        engine=engine,
        zone_ids=zones,
        demand_params={"n_estimators": 10, "min_child_samples": 2},
        duration_params={"n_estimators": 10, "min_child_samples": 2},
        backup_to_r2=False,
        log_to_mlflow=True,
        promote_models=True,
    )

    assert summary["status"] == "success"
    assert summary["datasets"]["demand_train_rows"] > 0
    assert summary["datasets"]["demand_val_rows"] > 0
    assert summary["datasets"]["corridor_train_rows"] > 0
    assert summary["datasets"]["corridor_val_rows"] > 0
    assert "lift_pct" in summary["demand"]
    assert "lift_pct" in summary["duration"]
    assert summary["demand"]["run_id"] is not None
    assert summary["duration"]["run_id"] is not None
