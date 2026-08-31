"""Investigate corridor duration validation residuals by duration range buckets (M3-5)."""

import os
import shutil
import tempfile
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from feast import FeatureStore, RepoConfig
from sqlalchemy import create_engine, text

from src.features.entities import corridor_entity, zone_entity
from src.features.offline_extractor import extract_and_load_offline_features
from src.features.views import create_file_backed_feature_views
from src.training.dataset import (
    CORRIDOR_FEATURES,
    generate_corridor_training_dataset,
    train_val_split_by_time,
)
from src.training.train_duration import train_duration_lightgbm
from src.transform.batch_transformer import BatchTransformer


def main():
    print(
        "=== M3-5 Investigation: Bucketing Corridor Duration Validation Residuals ==="
    )
    raw_parquet_path = "data/raw/yellow_tripdata_2023-01.parquet"
    if not os.path.exists(raw_parquet_path):
        raise FileNotFoundError(f"Missing raw parquet at {raw_parquet_path}")

    tmp_dir = tempfile.mkdtemp(prefix="residual_eval_")
    try:
        db_path = os.path.join(tmp_dir, "warehouse.db")
        zone_parquet = os.path.join(tmp_dir, "zone_demand_features.parquet")
        corridor_parquet = os.path.join(tmp_dir, "corridor_duration_features.parquet")
        registry_db = os.path.join(tmp_dir, "feast_registry.db")
        mlflow_db = os.path.join(tmp_dir, "mlflow.db")

        os.environ["MLFLOW_TRACKING_URI"] = f"sqlite:///{mlflow_db}"

        engine = create_engine(f"sqlite:///{db_path}")

        print("1. Loading and transforming trips...")
        raw_df = pd.read_parquet(raw_parquet_path)
        clean_trips_df, _ = BatchTransformer().transform_dataframe(raw_df)

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

        print("2. Extracting offline features...")
        start_month = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        end_month = datetime(2023, 2, 1, 0, 0, 0, tzinfo=timezone.utc)
        extract_and_load_offline_features(
            engine=engine,
            start_datetime=start_month,
            end_datetime=end_month,
            lookback_days=7,
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
                project="residual_investigation",
                provider="local",
            )
        )
        store.apply([zone_entity, corridor_entity] + views)

        train_start = datetime(2023, 1, 8, 0, 0, 0, tzinfo=timezone.utc)
        val_split = datetime(2023, 1, 25, 0, 0, 0, tzinfo=timezone.utc)
        train_end = datetime(2023, 2, 1, 0, 0, 0, tzinfo=timezone.utc)

        print("3. Generating corridor dataset...")
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

        print("4. Training LightGBM model...")
        result = train_duration_lightgbm(
            train_df=corridor_train,
            val_df=corridor_val,
            baseline_mae=456.95,
            log_to_mlflow=False,
        )

        y_true = corridor_val["target_avg_duration_next_1h"].to_numpy()
        y_pred = result["predictions"]
        residuals = y_pred - y_true  # predicted - actual

        df_res = pd.DataFrame(
            {
                "y_true_sec": y_true,
                "y_pred_sec": y_pred,
                "residual_sec": residuals,
                "abs_error_sec": np.abs(residuals),
            }
        )

        # Define duration buckets
        bins = [0, 300, 900, 1800, 3600, np.inf]
        labels = [
            "< 5 min (<300s)",
            "5-15 min (300-900s)",
            "15-30 min (900-1800s)",
            "30-60 min (1800-3600s)",
            "> 60 min (>3600s)",
        ]
        df_res["bucket"] = pd.cut(
            df_res["y_true_sec"], bins=bins, labels=labels, right=False
        )

        print("\n" + "=" * 95)
        print(
            f"{'Duration Range':<24} | {'Count (N)':<10} | {'% Total':<8} | {'Mean True':<10} | {'Mean Pred':<10} | {'Mean Bias (MBE)':<16} | {'MAE':<10}"
        )
        print("=" * 95)

        total_n = len(df_res)
        for label in labels:
            sub = df_res[df_res["bucket"] == label]
            if len(sub) == 0:
                continue
            cnt = len(sub)
            pct = cnt / total_n * 100
            mean_true = sub["y_true_sec"].mean()
            mean_pred = sub["y_pred_sec"].mean()
            mean_bias = sub["residual_sec"].mean()
            mae = sub["abs_error_sec"].mean()

            print(
                f"{label:<24} | {cnt:<10,} | {pct:<7.2f}% | {mean_true/60.0:>6.2f} min | {mean_pred/60.0:>6.2f} min | {mean_bias:>+10.2f}s ({mean_bias/60.0:>+5.2f}m) | {mae:>6.2f}s ({mae/60.0:>4.2f}m)"
            )

        print("-" * 95)
        overall_bias = df_res["residual_sec"].mean()
        overall_mae = df_res["abs_error_sec"].mean()
        print(
            f"{'Overall (All Validation)':<24} | {total_n:<10,} | {100.0:<7.2f}% | {df_res['y_true_sec'].mean()/60.0:>6.2f} min | {df_res['y_pred_sec'].mean()/60.0:>6.2f} min | {overall_bias:>+10.2f}s ({overall_bias/60.0:>+5.2f}m) | {overall_mae:>6.2f}s ({overall_mae/60.0:>4.2f}m)"
        )
        print("=" * 95)

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
