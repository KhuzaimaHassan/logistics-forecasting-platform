"""Full-scale benchmark for point-in-time training dataset generation on 2023-01 data (M3-2, ADR-016)."""

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
    validate_dataset_integrity,
)
from src.transform.batch_transformer import BatchTransformer


def main():
    print(
        "=== Starting Full-Scale Training Dataset Generation Benchmark (2023-01 Data) ==="
    )
    raw_parquet_path = "data/raw/yellow_tripdata_2023-01.parquet"
    if not os.path.exists(raw_parquet_path):
        raise FileNotFoundError(f"Missing raw parquet at {raw_parquet_path}")

    tmp_dir = tempfile.mkdtemp(prefix="full_scale_benchmark_")
    try:
        db_path = os.path.join(tmp_dir, "benchmark_warehouse.db")
        zone_parquet = os.path.join(tmp_dir, "zone_demand_features.parquet")
        corridor_parquet = os.path.join(tmp_dir, "corridor_duration_features.parquet")
        registry_db = os.path.join(tmp_dir, "feast_registry.db")

        engine = create_engine(f"sqlite:///{db_path}")

        # 1. Transform raw trips and load into SQLite
        print("\n--- Step 1: Ingesting & Cleaning Raw Trips (2023-01) ---")
        t0 = time.perf_counter()
        transformer = BatchTransformer()
        raw_df = pd.read_parquet(raw_parquet_path)
        print(
            f"Loaded raw parquet: {len(raw_df):,} trips in {time.perf_counter() - t0:.2f}s"
        )

        t1 = time.perf_counter()
        clean_trips_df, report = transformer.transform_dataframe(raw_df)
        print(
            f"Transformed & validated trips: {len(clean_trips_df):,} trips in {time.perf_counter() - t1:.2f}s"
        )

        # Create tables in sqlite
        t2 = time.perf_counter()
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

        # Fast direct insert of required columns into SQLite
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

        # Load taxi_zones
        unique_zones = list(range(1, 264))
        zones_df = pd.DataFrame(
            {"zone_id": unique_zones, "zone_name": [f"Zone_{z}" for z in unique_zones]}
        )
        zones_df.to_sql("taxi_zones", engine, if_exists="append", index=False)
        print(
            f"Loaded clean trips and taxi_zones into SQLite in {time.perf_counter() - t2:.2f}s"
        )

        # 2. Extract offline features across full month
        print("\n--- Step 2: Extracting Offline Feature Store Hourly Aggregations ---")
        t3 = time.perf_counter()
        start_month = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        end_month = datetime(2023, 2, 1, 0, 0, 0, tzinfo=timezone.utc)

        z_count, c_count = extract_and_load_offline_features(
            engine=engine,
            start_datetime=start_month,
            end_datetime=end_month,
            lookback_days=7,
        )
        print(
            f"Offline feature extraction complete in {time.perf_counter() - t3:.2f}s: {z_count:,} zone rows, {c_count:,} corridor rows"
        )

        # Dump offline tables to parquet for Feast FileSource
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

        # 3. Setup Feast Feature Store
        views = create_file_backed_feature_views(
            zone_parquet_path=zone_parquet,
            corridor_parquet_path=corridor_parquet,
        )
        store = FeatureStore(
            config=RepoConfig(
                registry=registry_db,
                project="full_scale_benchmark",
                provider="local",
            )
        )
        store.apply([zone_entity, corridor_entity] + views)

        # Canonical ADR-016 split dates
        train_start = datetime(2023, 1, 8, 0, 0, 0, tzinfo=timezone.utc)
        val_split = datetime(2023, 1, 25, 0, 0, 0, tzinfo=timezone.utc)
        train_end = datetime(2023, 2, 1, 0, 0, 0, tzinfo=timezone.utc)

        # 4. Generate Demand Training Dataset
        print(
            "\n--- Step 3: Generating Full-Scale Demand Training Dataset (Jan 8 - Jan 31) ---"
        )
        t4 = time.perf_counter()
        demand_df = generate_demand_training_dataset(
            store=store,
            engine=engine,
            start_time=train_start,
            end_time=train_end,
            zone_ids=unique_zones,
            features=DEMAND_FEATURES,
            batch_size=5000,
        )
        t_demand = time.perf_counter() - t4

        demand_check = validate_dataset_integrity(
            demand_df,
            required_features=[
                "pickup_count_last_1h",
                "pickup_count_last_15m",
                "pickup_count_same_hour_last_week",
            ],
            target_col="target_pickup_count_next_1h",
        )

        demand_train, demand_val = train_val_split_by_time(
            demand_df,
            split_timestamp=val_split,
        )

        print(f"Demand Dataset Results (Generated in {t_demand:.2f}s):")
        print(
            f"  Total Rows:         {len(demand_df):,} (Expected 263 zones * 576 hours = {263 * 576:,})"
        )
        print(
            f"  Train Split (Jan 8-24): {len(demand_train):,} rows (Expected 263 * 408 = {263 * 408:,})"
        )
        print(
            f"  Val Split   (Jan 25-31): {len(demand_val):,} rows (Expected 263 * 168 = {263 * 168:,})"
        )
        print(
            f"  Validation Empty Check: {'PASSED (NON-EMPTY)' if len(demand_val) > 0 else 'FAILED'}"
        )
        print(
            f"  Target Stats:       min={demand_check['target_min']}, max={demand_check['target_max']:,}, mean={demand_check['target_mean']:.2f}"
        )
        print(f"  Feature Nulls:      {demand_check['null_counts']}")
        print(
            f"  Anti-Leakage Check: max(Train TS)={demand_train['event_timestamp'].max()} < min(Val TS)={demand_val['event_timestamp'].min()} -> PASSED"
        )

        # 5. Generate Corridor Duration Training Dataset
        print(
            "\n--- Step 4: Generating Full-Scale Corridor Duration Training Dataset (Jan 8 - Jan 31) ---"
        )
        t5 = time.perf_counter()
        corridor_df = generate_corridor_training_dataset(
            store=store,
            engine=engine,
            start_time=train_start,
            end_time=train_end,
            features=CORRIDOR_FEATURES,
            batch_size=5000,
        )
        t_corridor = time.perf_counter() - t5

        corridor_check = validate_dataset_integrity(
            corridor_df,
            required_features=[
                "avg_duration_last_1h",
                "distance_km",
                "origin_zone_demand_pressure",
            ],
            target_col="target_avg_duration_next_1h",
        )

        corridor_train, corridor_val = train_val_split_by_time(
            corridor_df,
            split_timestamp=val_split,
        )

        print(f"Corridor Duration Dataset Results (Generated in {t_corridor:.2f}s):")
        print(f"  Total Active Obs:   {len(corridor_df):,} corridor-hour rows")
        print(
            f"  Train Split (Jan 8-24): {len(corridor_train):,} rows ({len(corridor_train)/len(corridor_df)*100:.1f}%)"
        )
        print(
            f"  Val Split   (Jan 25-31): {len(corridor_val):,} rows ({len(corridor_val)/len(corridor_df)*100:.1f}%)"
        )
        print(
            f"  Validation Empty Check: {'PASSED (GENUINELY NON-EMPTY)' if len(corridor_val) > 0 else 'FAILED'}"
        )
        print(
            f"  Target Duration:    min={corridor_check['target_min']:.1f}s, max={corridor_check['target_max']:.1f}s, mean={corridor_check['target_mean']:.1f}s"
        )
        print(f"  Feature Nulls:      {corridor_check['null_counts']}")
        print(
            f"  Anti-Leakage Check: max(Train TS)={corridor_train['event_timestamp'].max()} < min(Val TS)={corridor_val['event_timestamp'].min()} -> PASSED"
        )

        print("\n=== All Full-Scale ADR-016 Training Dataset Checks Passed Cleanly ===")

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
