"""Unit tests for training dataset extraction, target computation, and time-based splitting (M3-2)."""

from datetime import datetime, timezone

import pandas as pd
import pytest
from feast import FeatureStore, RepoConfig
from sqlalchemy import create_engine, text

from src.features.entities import corridor_entity, zone_entity
from src.features.views import create_file_backed_feature_views
from src.training.dataset import (
    build_demand_entity_grid,
    compute_corridor_targets_from_trips,
    compute_demand_targets_from_trips,
    generate_corridor_training_dataset,
    generate_demand_training_dataset,
    train_val_split_by_time,
    validate_dataset_integrity,
)


@pytest.fixture
def sqlite_trips_engine(tmp_path):
    """In-memory SQLite database simulating warehouse.trips and warehouse.taxi_zones."""
    engine = create_engine(f"sqlite:///{tmp_path / 'warehouse.db'}")
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE taxi_zones (
                zone_id INTEGER PRIMARY KEY,
                borough TEXT,
                zone_name TEXT,
                service_zone TEXT,
                centroid_lat REAL,
                centroid_lon REAL
            );
        """))
        conn.execute(text("""
            CREATE TABLE trips (
                trip_id INTEGER PRIMARY KEY,
                pickup_zone_id INTEGER,
                dropoff_zone_id INTEGER,
                pickup_datetime TIMESTAMP,
                dropoff_datetime TIMESTAMP,
                trip_duration_seconds REAL,
                trip_distance_miles REAL,
                fare_amount REAL,
                total_amount REAL,
                created_at TIMESTAMP
            );
        """))
        conn.execute(text("""
            INSERT INTO taxi_zones (zone_id, zone_name) VALUES (161, 'Midtown Center'), (236, 'Upper East Side North');
        """))
        # Seed trips for 2023-01-08
        # Hour 10: 3 trips for 161 (2 to 236, 1 to 161)
        # Hour 11: 2 trips for 161 to 236
        conn.execute(text("""
            INSERT INTO trips (trip_id, pickup_zone_id, dropoff_zone_id, pickup_datetime, dropoff_datetime, trip_duration_seconds, created_at)
            VALUES
                (1, 161, 236, '2023-01-08 10:15:00', '2023-01-08 10:30:00', 900, '2023-01-08 10:30:00'),
                (2, 161, 236, '2023-01-08 10:45:00', '2023-01-08 11:05:00', 1200, '2023-01-08 11:05:00'),
                (3, 161, 161, '2023-01-08 10:50:00', '2023-01-08 11:00:00', 600, '2023-01-08 11:00:00'),
                (4, 161, 236, '2023-01-08 11:10:00', '2023-01-08 11:25:00', 900, '2023-01-08 11:25:00'),
                (5, 161, 236, '2023-01-08 11:30:00', '2023-01-08 11:50:00', 1200, '2023-01-08 11:50:00');
        """))
    return engine


def test_build_demand_entity_grid():
    """Test full Cartesian product geometry for entity grid."""
    zone_ids = [161, 236, 142]
    start = datetime(2023, 1, 8, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2023, 1, 9, 0, 0, 0, tzinfo=timezone.utc)  # 24 hours

    grid = build_demand_entity_grid(zone_ids, start, end)
    assert len(grid) == 3 * 24  # 72 rows
    assert set(grid["zone_id"].unique()) == {161, 236, 142}
    assert grid["event_timestamp"].min() == pd.Timestamp(
        "2023-01-08 00:00:00", tz="UTC"
    )
    assert grid["event_timestamp"].max() == pd.Timestamp(
        "2023-01-08 23:00:00", tz="UTC"
    )


def test_compute_demand_targets_from_trips(sqlite_trips_engine):
    """Test computing ground truth demand targets by zone and hour."""
    start = datetime(2023, 1, 8, 10, 0, 0, tzinfo=timezone.utc)
    end = datetime(2023, 1, 8, 12, 0, 0, tzinfo=timezone.utc)

    targets = compute_demand_targets_from_trips(sqlite_trips_engine, start, end)
    assert len(targets) == 2  # Zone 161 at 10:00 and 11:00

    row_10 = targets[
        targets["event_timestamp"] == pd.Timestamp("2023-01-08 10:00:00", tz="UTC")
    ]
    assert row_10["target_pickup_count_next_1h"].iloc[0] == 3

    row_11 = targets[
        targets["event_timestamp"] == pd.Timestamp("2023-01-08 11:00:00", tz="UTC")
    ]
    assert row_11["target_pickup_count_next_1h"].iloc[0] == 2


def test_compute_corridor_targets_from_trips(sqlite_trips_engine):
    """Test computing ground truth duration targets by corridor and hour."""
    start = datetime(2023, 1, 8, 10, 0, 0, tzinfo=timezone.utc)
    end = datetime(2023, 1, 8, 12, 0, 0, tzinfo=timezone.utc)

    targets = compute_corridor_targets_from_trips(sqlite_trips_engine, start, end)
    # Hour 10: 161_236 (2 trips, avg 1050s) and 161_161 (1 trip, 600s)
    # Hour 11: 161_236 (2 trips, avg 1050s)
    assert len(targets) == 3

    c161_236_10 = targets[
        (targets["corridor_id"] == "161_236")
        & (targets["event_timestamp"] == pd.Timestamp("2023-01-08 10:00:00", tz="UTC"))
    ]
    assert c161_236_10["target_trip_count_next_1h"].iloc[0] == 2
    assert c161_236_10["target_avg_duration_next_1h"].iloc[0] == pytest.approx(1050.0)


def test_generate_demand_training_dataset_pit_join(tmp_path, sqlite_trips_engine):
    """Test end-to-end demand dataset generation with Feast point-in-time feature join."""
    zone_parquet = tmp_path / "zone_demand_features.parquet"
    corridor_parquet = tmp_path / "corridor_duration_features.parquet"
    registry_db = tmp_path / "test_registry.db"

    # Seed offline feature records
    pd.DataFrame(
        [
            {
                "zone_id": 161,
                "pickup_datetime": datetime(2023, 1, 8, 10, 0, 0, tzinfo=timezone.utc),
                "created_at": datetime(2023, 1, 8, 10, 0, 0, tzinfo=timezone.utc),
                "pickup_count_last_15m": 5,
                "pickup_count_last_1h": 20,
                "pickup_count_last_24h": 100,
                "pickup_count_same_hour_last_week": 18,
                "hour_of_day": 10,
                "day_of_week": 6,
                "is_weekend": True,
                "is_holiday": False,
                "avg_temp_last_1h": 8.0,
                "is_precipitating": False,
            },
            {
                "zone_id": 161,
                "pickup_datetime": datetime(2023, 1, 8, 11, 0, 0, tzinfo=timezone.utc),
                "created_at": datetime(2023, 1, 8, 11, 0, 0, tzinfo=timezone.utc),
                "pickup_count_last_15m": 12,
                "pickup_count_last_1h": 35,
                "pickup_count_last_24h": 120,
                "pickup_count_same_hour_last_week": 25,
                "hour_of_day": 11,
                "day_of_week": 6,
                "is_weekend": True,
                "is_holiday": False,
                "avg_temp_last_1h": 9.0,
                "is_precipitating": False,
            },
        ]
    ).to_parquet(zone_parquet)

    pd.DataFrame(
        columns=[
            "corridor_id",
            "dropoff_datetime",
            "created_at",
            "avg_duration_last_15m",
            "avg_duration_last_1h",
            "distance_km",
            "origin_zone_demand_pressure",
            "avg_traffic_speed_current",
        ]
    ).to_parquet(corridor_parquet)

    views = create_file_backed_feature_views(
        zone_parquet_path=str(zone_parquet),
        corridor_parquet_path=str(corridor_parquet),
    )

    store = FeatureStore(
        config=RepoConfig(
            registry=str(registry_db),
            project="test_demand_dataset",
            provider="local",
        )
    )
    store.apply([zone_entity, corridor_entity] + views)

    start = datetime(2023, 1, 8, 10, 0, 0, tzinfo=timezone.utc)
    end = datetime(2023, 1, 8, 12, 0, 0, tzinfo=timezone.utc)

    dataset = generate_demand_training_dataset(
        store=store,
        engine=sqlite_trips_engine,
        start_time=start,
        end_time=end,
        zone_ids=[161, 236],
        features=[
            "zone_demand_features:pickup_count_last_1h",
            "zone_demand_features:pickup_count_last_15m",
            "zone_demand_features:pickup_count_same_hour_last_week",
        ],
    )

    # 2 zones * 2 hours = 4 rows
    assert len(dataset) == 4
    assert "target_pickup_count_next_1h" in dataset.columns
    assert "pickup_count_last_1h" in dataset.columns

    # Verify zone 161 at 10:00: target is 3, feature at T (10:00) is 20
    row_161_10 = dataset[
        (dataset["zone_id"] == 161)
        & (dataset["event_timestamp"] == pd.Timestamp("2023-01-08 10:00:00", tz="UTC"))
    ]
    assert row_161_10["target_pickup_count_next_1h"].iloc[0] == 3
    assert row_161_10["pickup_count_last_1h"].iloc[0] == 20

    # Verify zone 236 (no trips recorded): target is 0
    row_236_10 = dataset[
        (dataset["zone_id"] == 236)
        & (dataset["event_timestamp"] == pd.Timestamp("2023-01-08 10:00:00", tz="UTC"))
    ]
    assert row_236_10["target_pickup_count_next_1h"].iloc[0] == 0


def test_generate_corridor_training_dataset_pit_join(tmp_path, sqlite_trips_engine):
    """Test end-to-end corridor dataset generation with Feast point-in-time feature join."""
    zone_parquet = tmp_path / "zone_demand_corridor.parquet"
    corridor_parquet = tmp_path / "corridor_duration_features.parquet"
    registry_db = tmp_path / "test_corridor_registry.db"

    pd.DataFrame(
        columns=[
            "zone_id",
            "pickup_datetime",
            "created_at",
            "pickup_count_last_15m",
            "pickup_count_last_1h",
            "pickup_count_last_24h",
            "pickup_count_same_hour_last_week",
            "hour_of_day",
            "day_of_week",
            "is_weekend",
            "is_holiday",
            "avg_temp_last_1h",
            "is_precipitating",
        ]
    ).to_parquet(zone_parquet)

    # Seed offline corridor feature records
    pd.DataFrame(
        [
            {
                "corridor_id": "161_236",
                "dropoff_datetime": datetime(2023, 1, 8, 10, 0, 0, tzinfo=timezone.utc),
                "created_at": datetime(2023, 1, 8, 10, 0, 0, tzinfo=timezone.utc),
                "avg_duration_last_15m": 850.0,
                "avg_duration_last_1h": 900.0,
                "distance_km": 4.5,
                "origin_zone_demand_pressure": 25,
                "avg_traffic_speed_current": 18.0,
            }
        ]
    ).to_parquet(corridor_parquet)

    views = create_file_backed_feature_views(
        zone_parquet_path=str(zone_parquet),
        corridor_parquet_path=str(corridor_parquet),
    )

    store = FeatureStore(
        config=RepoConfig(
            registry=str(registry_db),
            project="test_corridor_dataset",
            provider="local",
        )
    )
    store.apply([zone_entity, corridor_entity] + views)

    start = datetime(2023, 1, 8, 10, 0, 0, tzinfo=timezone.utc)
    end = datetime(2023, 1, 8, 12, 0, 0, tzinfo=timezone.utc)

    dataset = generate_corridor_training_dataset(
        store=store,
        engine=sqlite_trips_engine,
        start_time=start,
        end_time=end,
        features=[
            "corridor_duration_features:avg_duration_last_1h",
            "corridor_duration_features:distance_km",
        ],
    )

    assert len(dataset) == 3
    assert "target_avg_duration_next_1h" in dataset.columns
    assert "avg_duration_last_1h" in dataset.columns

    # Verify 161_236 at 10:00: target is 1050s, PIT feature at T (10:00) is 900s
    row = dataset[
        (dataset["corridor_id"] == "161_236")
        & (dataset["event_timestamp"] == pd.Timestamp("2023-01-08 10:00:00", tz="UTC"))
    ]
    assert row["target_avg_duration_next_1h"].iloc[0] == pytest.approx(1050.0)
    assert row["avg_duration_last_1h"].iloc[0] == pytest.approx(900.0)
    assert row["distance_km"].iloc[0] == pytest.approx(4.5)


def test_train_val_split_by_time():
    """Test strict chronological train/validation splitting without data leakage."""
    dates = pd.date_range("2023-01-08", "2023-01-31 23:00", freq="1h", tz="UTC")
    df = pd.DataFrame(
        {
            "event_timestamp": dates,
            "target": range(len(dates)),
            "feature_1": range(len(dates)),
        }
    )

    split_point = datetime(2023, 1, 25, 0, 0, 0, tzinfo=timezone.utc)
    train_df, val_df = train_val_split_by_time(df, split_timestamp=split_point)

    assert not train_df.empty
    assert not val_df.empty
    assert len(train_df) + len(val_df) == len(df)
    assert train_df["event_timestamp"].max() < pd.Timestamp(split_point)
    assert val_df["event_timestamp"].min() >= pd.Timestamp(split_point)


def test_validate_dataset_integrity():
    """Test dataset integrity checks and failure assertions."""
    valid_df = pd.DataFrame(
        {
            "zone_id": [161, 236],
            "event_timestamp": [
                datetime(2023, 1, 8, 10, 0, tzinfo=timezone.utc),
                datetime(2023, 1, 8, 11, 0, tzinfo=timezone.utc),
            ],
            "target_pickup_count_next_1h": [10, 25],
            "pickup_count_last_1h": [15, 30],
        }
    )
    result = validate_dataset_integrity(
        valid_df,
        required_features=["pickup_count_last_1h"],
        target_col="target_pickup_count_next_1h",
    )
    assert result["status"] == "passed"
    assert result["total_rows"] == 2

    # Negative target check
    invalid_target_df = valid_df.copy()
    invalid_target_df.loc[0, "target_pickup_count_next_1h"] = -5
    with pytest.raises(ValueError, match="negative values"):
        validate_dataset_integrity(
            invalid_target_df,
            required_features=["pickup_count_last_1h"],
            target_col="target_pickup_count_next_1h",
        )

    # Missing feature check
    with pytest.raises(ValueError, match="Required feature columns missing"):
        validate_dataset_integrity(
            valid_df,
            required_features=["missing_feature"],
            target_col="target_pickup_count_next_1h",
        )
