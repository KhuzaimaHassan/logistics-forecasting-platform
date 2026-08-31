"""Run the full end-to-end training pipeline on real 2023-01 NYC taxi data (M3-5)."""

import json
import os
import shutil
import tempfile
import time
from datetime import datetime, timezone

import pandas as pd
from feast import FeatureStore, RepoConfig
from sqlalchemy import create_engine, text

from src.features.entities import corridor_entity, zone_entity
from src.features.offline_extractor import extract_and_load_offline_features
from src.features.views import create_file_backed_feature_views
from src.training.pipeline import run_training_pipeline
from src.transform.batch_transformer import BatchTransformer


def main():
    print(
        "=== M3-5: Running Full End-to-End Training Pipeline on Real 2023-01 Data ==="
    )
    raw_parquet_path = "data/raw/yellow_tripdata_2023-01.parquet"
    if not os.path.exists(raw_parquet_path):
        raise FileNotFoundError(f"Missing raw parquet at {raw_parquet_path}")

    tmp_dir = tempfile.mkdtemp(prefix="m3_5_pipeline_")
    try:
        db_path = os.path.join(tmp_dir, "warehouse.db")
        zone_parquet = os.path.join(tmp_dir, "zone_demand_features.parquet")
        corridor_parquet = os.path.join(tmp_dir, "corridor_duration_features.parquet")
        registry_db = os.path.join(tmp_dir, "feast_registry.db")
        mlflow_db = os.path.join(tmp_dir, "mlflow.db")
        mlruns_dir = os.path.join(tmp_dir, "mlruns")

        os.environ["MLFLOW_TRACKING_URI"] = f"sqlite:///{mlflow_db}"

        engine = create_engine(f"sqlite:///{db_path}")

        print("\n--- 1. Ingesting and Transforming Trips ---")
        t0 = time.perf_counter()
        raw_df = pd.read_parquet(raw_parquet_path)
        clean_trips_df, _ = BatchTransformer().transform_dataframe(raw_df)
        print(
            f"Cleaned {len(clean_trips_df):,} trips in {time.perf_counter() - t0:.2f}s"
        )

        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS trips (
                    pickup_zone_id INTEGER NOT NULL,
                    dropoff_zone_id INTEGER NOT NULL,
                    pickup_datetime TIMESTAMP NOT NULL,
                    dropoff_datetime TIMESTAMP NOT NULL,
                    trip_duration_seconds INTEGER NOT NULL,
                    trip_distance_km REAL NOT NULL
                );
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS zone_demand_features_hourly (
                    zone_id INTEGER NOT NULL,
                    pickup_datetime TIMESTAMP NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    pickup_count_last_15m BIGINT NOT NULL,
                    pickup_count_last_1h BIGINT NOT NULL,
                    pickup_count_last_24h BIGINT NOT NULL,
                    pickup_count_same_hour_last_week BIGINT NOT NULL,
                    hour_of_day INTEGER NOT NULL,
                    day_of_week INTEGER NOT NULL,
                    is_weekend BOOLEAN NOT NULL,
                    is_holiday BOOLEAN NOT NULL,
                    avg_temp_last_1h REAL,
                    is_precipitating BOOLEAN NOT NULL,
                    PRIMARY KEY (zone_id, pickup_datetime)
                );
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS corridor_duration_features_hourly (
                    corridor_id VARCHAR(16) NOT NULL,
                    dropoff_datetime TIMESTAMP NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    avg_duration_last_15m REAL NOT NULL,
                    avg_duration_last_1h REAL NOT NULL,
                    distance_km REAL NOT NULL,
                    origin_zone_demand_pressure BIGINT NOT NULL,
                    avg_traffic_speed_current REAL,
                    PRIMARY KEY (corridor_id, dropoff_datetime)
                );
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS taxi_zones (
                    zone_id INTEGER PRIMARY KEY,
                    zone_name TEXT
                );
            """))

        trips_subset = clean_trips_df[
            [
                "pickup_zone_id",
                "dropoff_zone_id",
                "pickup_datetime",
                "dropoff_datetime",
                "trip_duration_seconds",
                "trip_distance_km",
            ]
        ]
        trips_subset.to_sql(
            "trips", engine, if_exists="append", index=False, chunksize=100000
        )

        unique_zones = list(range(1, 264))
        zones_df = pd.DataFrame(
            {"zone_id": unique_zones, "zone_name": [f"Zone_{z}" for z in unique_zones]}
        )
        zones_df.to_sql("taxi_zones", engine, if_exists="append", index=False)

        print("\n--- 2. Extracting Offline Features ---")
        t1 = time.perf_counter()
        start_month = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        end_month = datetime(2023, 2, 1, 0, 0, 0, tzinfo=timezone.utc)
        z_count, c_count = extract_and_load_offline_features(
            engine=engine,
            start_datetime=start_month,
            end_datetime=end_month,
            lookback_days=7,
        )
        print(
            f"Offline feature extraction complete in {time.perf_counter() - t1:.2f}s: {z_count:,} zone rows, {c_count:,} corridor rows"
        )

        with engine.connect() as conn:
            zone_feats_df = pd.read_sql(
                "SELECT * FROM zone_demand_features_hourly", conn
            )
            corridor_feats_df = pd.read_sql(
                "SELECT * FROM corridor_duration_features_hourly", conn
            )

        zone_feats_df["pickup_datetime"] = pd.to_datetime(
            zone_feats_df["pickup_datetime"], utc=True
        )
        zone_feats_df["created_at"] = pd.to_datetime(
            zone_feats_df["created_at"], utc=True
        )
        zone_feats_df.to_parquet(zone_parquet)

        corridor_feats_df["dropoff_datetime"] = pd.to_datetime(
            corridor_feats_df["dropoff_datetime"], utc=True
        )
        corridor_feats_df["created_at"] = pd.to_datetime(
            corridor_feats_df["created_at"], utc=True
        )
        corridor_feats_df.to_parquet(corridor_parquet)

        views = create_file_backed_feature_views(
            zone_parquet_path=zone_parquet,
            corridor_parquet_path=corridor_parquet,
        )
        store = FeatureStore(
            config=RepoConfig(
                registry=registry_db,
                project="m3_5_pipeline_eval",
                provider="local",
            )
        )
        store.apply([zone_entity, corridor_entity] + views)

        train_start = datetime(2023, 1, 8, 0, 0, 0, tzinfo=timezone.utc)
        val_split = datetime(2023, 1, 25, 0, 0, 0, tzinfo=timezone.utc)
        train_end = datetime(2023, 2, 1, 0, 0, 0, tzinfo=timezone.utc)

        print("\n--- 3. Running Orchestrated Pipeline ---")
        summary = run_training_pipeline(
            start_time=train_start,
            end_time=train_end,
            split_timestamp=val_split,
            store=store,
            engine=engine,
            zone_ids=unique_zones,
            backup_to_r2=True,
            log_to_mlflow=True,
            promote_models=True,
        )

        print("\n" + "=" * 65)
        print("M3-5 END-TO-END TRAINING PIPELINE EXECUTION SUMMARY:")
        print("=" * 65)
        print(f"  Status:               {summary['status'].upper()}")
        print(
            f"  Total Elapsed Time:   {summary['elapsed_seconds']:.2f} seconds ({summary['elapsed_seconds']/60.0:.2f} minutes)"
        )
        print(
            f"  Demand Dataset:       Train={summary['datasets']['demand_train_rows']:,}, Val={summary['datasets']['demand_val_rows']:,} rows"
        )
        print(
            f"  Demand Baseline MAE:  {summary['demand']['baseline_mae']:.4f} pickups/h"
        )
        print(f"  Demand Model MAE:     {summary['demand']['model_mae']:.4f} pickups/h")
        print(f"  Demand Relative Lift: {summary['demand']['lift_pct']:+.2f}%")
        print(f"  Demand MLflow Run:    {summary['demand']['run_id']}")
        print(
            f"  Demand Promotion:     v{summary['demand']['promotion']['version']} -> {summary['demand']['promotion']['stage']} ({summary['demand']['promotion']['reason']})"
        )
        print("-" * 65)
        print(
            f"  Corridor Dataset:     Train={summary['datasets']['corridor_train_rows']:,}, Val={summary['datasets']['corridor_val_rows']:,} rows"
        )
        print(
            f"  Corridor Base MAE:    {summary['duration']['baseline_mae']:.2f}s ({summary['duration']['baseline_mae']/60.0:.2f} min)"
        )
        print(
            f"  Corridor Model MAE:   {summary['duration']['model_mae']:.2f}s ({summary['duration']['model_mae']/60.0:.2f} min)"
        )
        print(f"  Corridor Rel Lift:    {summary['duration']['lift_pct']:+.2f}%")
        print(f"  Corridor MLflow Run:  {summary['duration']['run_id']}")
        print(
            f"  Corridor Promotion:   v{summary['duration']['promotion']['version']} -> {summary['duration']['promotion']['stage']} ({summary['duration']['promotion']['reason']})"
        )
        print("-" * 65)
        print(f"  Cloudflare R2 Backup: {json.dumps(summary['r2_backup'], indent=2)}")
        print("=" * 65)

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
