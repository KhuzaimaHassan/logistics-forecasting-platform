"""Full-scale benchmark training LightGBM demand and duration models on real 2023-01 data (M3-4)."""

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
from src.training.dataset import (
    CORRIDOR_FEATURES,
    DEMAND_FEATURES,
    generate_corridor_training_dataset,
    generate_demand_training_dataset,
    train_val_split_by_time,
)
from src.training.train_demand import train_demand_lightgbm
from src.training.train_duration import train_duration_lightgbm
from src.transform.batch_transformer import BatchTransformer


def main():
    print("=== M3-4: LightGBM Model Training & Lift Benchmark on Real 2023-01 Data ===")
    raw_parquet_path = "data/raw/yellow_tripdata_2023-01.parquet"
    if not os.path.exists(raw_parquet_path):
        raise FileNotFoundError(f"Missing raw parquet at {raw_parquet_path}")

    tmp_dir = tempfile.mkdtemp(prefix="m3_4_lgbm_")
    try:
        db_path = os.path.join(tmp_dir, "warehouse.db")
        zone_parquet = os.path.join(tmp_dir, "zone_demand_features.parquet")
        corridor_parquet = os.path.join(tmp_dir, "corridor_duration_features.parquet")
        registry_db = os.path.join(tmp_dir, "feast_registry.db")
        mlflow_db = os.path.join(tmp_dir, "mlflow.db")

        os.environ["MLFLOW_TRACKING_URI"] = f"sqlite:///{mlflow_db}"

        engine = create_engine(f"sqlite:///{db_path}")

        # 1. Transform raw trips and load into SQLite
        print("\n--- Step 1: Ingesting & Cleaning Trips ---")
        t0 = time.perf_counter()
        raw_df = pd.read_parquet(raw_parquet_path)
        clean_trips_df, report = BatchTransformer().transform_dataframe(raw_df)
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

        # 2. Extract offline features
        print("\n--- Step 2: Extracting Offline Feature Store Aggregations ---")
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
                project="m3_4_eval",
                provider="local",
            )
        )
        store.apply([zone_entity, corridor_entity] + views)

        train_start = datetime(2023, 1, 8, 0, 0, 0, tzinfo=timezone.utc)
        val_split = datetime(2023, 1, 25, 0, 0, 0, tzinfo=timezone.utc)
        train_end = datetime(2023, 2, 1, 0, 0, 0, tzinfo=timezone.utc)

        # 3. Train & Evaluate LightGBM Demand Model
        print("\n--- Step 3: Training & Evaluating LightGBM Demand Model ---")
        demand_df = generate_demand_training_dataset(
            store=store,
            engine=engine,
            start_time=train_start,
            end_time=train_end,
            zone_ids=unique_zones,
            features=DEMAND_FEATURES,
            batch_size=5000,
        )
        demand_train, demand_val = train_val_split_by_time(
            demand_df,
            split_timestamp=val_split,
        )
        print(
            f"Demand dataset ready: Train={len(demand_train):,}, Val={len(demand_val):,} rows"
        )

        demand_lgbm = train_demand_lightgbm(
            train_df=demand_train,
            val_df=demand_val,
            baseline_mae=4.1326,
            experiment_name="nyc-taxi-demand-forecasting",
            run_name="lightgbm_demand_benchmark_jan2023",
            log_to_mlflow=True,
        )
        d_metrics = demand_lgbm["metrics"]
        print("\n=======================================================")
        print("LIGHTGBM DEMAND MODEL BENCHMARK RESULTS (Validation Split):")
        print(f"  Train Samples:      {len(demand_train):,} zone-hours (Jan 8-24)")
        print(f"  Validation Samples: {len(demand_val):,} zone-hours (Jan 25-31)")
        print("  Baseline MAE:       4.1326 pickups/hour")
        print(f"  Model MAE:          {d_metrics['val_mae']:.4f} pickups/hour")
        print(
            f"  Relative Lift:      {demand_lgbm['lift_pct']:+.2f}% over seasonal-naive baseline"
        )
        print(f"  RMSE:               {d_metrics['val_rmse']:.4f} pickups/hour")
        print(f"  WAPE:               {d_metrics['val_wape']:.2f}%")
        print(f"  MedAE:              {d_metrics['val_medae']:.4f} pickups/hour")
        print(f"  Mean Bias (MBE):    {d_metrics['val_mbe']:.4f}")
        print(f"  R2 Score:           {d_metrics['val_r2']:.4f}")
        print(f"  MLflow Run ID:      {demand_lgbm['run_id']}")
        print("\n  Top 5 Important Features (Gain):")
        for _, r in demand_lgbm["feature_importances"].head(5).iterrows():
            print(f"    - {r['feature']:<32} (Gain: {r['importance_gain']:,.1f})")
        print("=======================================================")

        # 4. Train & Evaluate LightGBM Corridor Duration Model
        print(
            "\n--- Step 4: Training & Evaluating LightGBM Corridor Duration Model ---"
        )
        corridor_df = generate_corridor_training_dataset(
            store=store,
            engine=engine,
            start_time=train_start,
            end_time=train_end,
            features=CORRIDOR_FEATURES,
            batch_size=5000,
        )
        corridor_train, corridor_val = train_val_split_by_time(
            corridor_df,
            split_timestamp=val_split,
        )
        print(
            f"Corridor dataset ready: Train={len(corridor_train):,}, Val={len(corridor_val):,} rows"
        )

        corridor_lgbm = train_duration_lightgbm(
            train_df=corridor_train,
            val_df=corridor_val,
            baseline_mae=456.95,
            experiment_name="nyc-taxi-corridor-eta",
            run_name="lightgbm_duration_log1p_benchmark_jan2023",
            log_to_mlflow=True,
        )
        c_metrics = corridor_lgbm["metrics"]
        print("\n=======================================================")
        print("LIGHTGBM CORRIDOR DURATION BENCHMARK RESULTS (Validation Split):")
        print(
            f"  Train Samples:      {len(corridor_train):,} corridor-hours (Jan 8-24)"
        )
        print(f"  Validation Samples: {len(corridor_val):,} corridor-hours (Jan 25-31)")
        print("  Baseline MAE:       456.95 seconds (7.62 minutes)")
        print(
            f"  Model MAE:          {c_metrics['val_mae']:.2f} seconds ({c_metrics['val_mae']/60.0:.2f} minutes)"
        )
        print(
            f"  Relative Lift:      {corridor_lgbm['lift_pct']:+.2f}% over moving-average baseline"
        )
        print(
            f"  RMSE:               {c_metrics['val_rmse']:.2f} seconds ({c_metrics['val_rmse']/60.0:.2f} minutes)"
        )
        print(f"  WAPE:               {c_metrics['val_wape']:.2f}%")
        print(
            f"  MedAE:              {c_metrics['val_medae']:.2f} seconds ({c_metrics['val_medae']/60.0:.2f} minutes)"
        )
        print(f"  Mean Bias (MBE):    {c_metrics['val_mbe']:.2f} seconds")
        print(f"  R2 Score:           {c_metrics['val_r2']:.4f}")
        print(f"  MLflow Run ID:      {corridor_lgbm['run_id']}")
        print("\n  Top 5 Important Features (Gain):")
        for _, r in corridor_lgbm["feature_importances"].head(5).iterrows():
            print(f"    - {r['feature']:<32} (Gain: {r['importance_gain']:,.1f})")
        print("=======================================================")

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
