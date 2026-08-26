"""Unit and integration tests for offline feature aggregation pipeline."""

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from src.features.offline_extractor import (
    backfill_all_loaded_months,
    compute_corridor_duration_features_hourly,
    compute_zone_demand_features_hourly,
    extract_and_load_offline_features,
    is_us_holiday,
)


def test_is_us_holiday_detection():
    """Verify federal holiday detection in NYC."""
    # New Year's Day
    assert is_us_holiday(datetime(2023, 1, 1, 12, 0, 0)) is True
    # MLK Day (3rd Monday in Jan 2023 = Jan 16)
    assert is_us_holiday(datetime(2023, 1, 16, 10, 0, 0)) is True
    # Presidents' Day (3rd Monday in Feb 2023 = Feb 20)
    assert is_us_holiday(datetime(2023, 2, 20, 10, 0, 0)) is True
    # Independence Day
    assert is_us_holiday(datetime(2023, 7, 4, 15, 0, 0)) is True
    # Christmas Day
    assert is_us_holiday(datetime(2023, 12, 25, 8, 0, 0)) is True

    # Standard non-holidays
    assert is_us_holiday(datetime(2023, 1, 2, 12, 0, 0)) is False
    assert is_us_holiday(datetime(2023, 3, 15, 12, 0, 0)) is False
    assert is_us_holiday(datetime(2023, 6, 1, 12, 0, 0)) is False


def test_compute_zone_demand_features_hourly_empty():
    """Verify empty input handling produces standard zeroed frame."""
    df_empty = pd.DataFrame(columns=["pickup_zone_id", "pickup_datetime"])
    start_t = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end_t = datetime(2023, 1, 1, 2, 0, 0, tzinfo=timezone.utc)

    res = compute_zone_demand_features_hourly(
        df_trips=df_empty,
        start_time=start_t,
        end_time=end_t,
        all_zone_ids=[10, 20],
    )
    # 2 zones x 3 hours = 6 rows
    assert len(res) == 6
    assert (res["pickup_count_last_1h"] == 0).all()
    assert (res["pickup_count_last_15m"] == 0).all()


def test_compute_zone_demand_features_hourly_anti_leakage_and_windows():
    """Verify zone demand rolling window calculations and strict anti-leakage gating."""
    t_obs = datetime(2023, 1, 8, 12, 0, 0, tzinfo=timezone.utc)

    trips = []
    # 1. 10 pickups in last 15 mins of 12:00 snapshot: [11:45, 12:00)
    for _ in range(10):
        trips.append(
            {"pickup_zone_id": 10, "pickup_datetime": t_obs - timedelta(minutes=5)}
        )

    # 2. 20 pickups in last 1 hour (outside 15m): [11:00, 11:45)
    for _ in range(20):
        trips.append(
            {"pickup_zone_id": 10, "pickup_datetime": t_obs - timedelta(minutes=30)}
        )

    # 3. 30 pickups in last 24h (outside 1h): [T-24h, T-1h)
    for _ in range(30):
        trips.append(
            {"pickup_zone_id": 10, "pickup_datetime": t_obs - timedelta(hours=5)}
        )

    # 4. 40 pickups 7 days ago in equivalent 1h window: [T-7d-1h, T-7d)
    for _ in range(40):
        trips.append(
            {
                "pickup_zone_id": 10,
                "pickup_datetime": t_obs - timedelta(days=7, minutes=30),
            }
        )

    # 5. 100 FUTURE pickups occurring after 12:00 snapshot: 12:05 (anti-leakage check)
    for _ in range(100):
        trips.append(
            {"pickup_zone_id": 10, "pickup_datetime": t_obs + timedelta(minutes=5)}
        )

    df_trips = pd.DataFrame(trips)

    res = compute_zone_demand_features_hourly(
        df_trips=df_trips,
        start_time=t_obs,
        end_time=t_obs,
        all_zone_ids=[10],
    )

    assert len(res) == 1
    row = res.iloc[0]

    assert row["zone_id"] == 10
    assert row["pickup_datetime"] == t_obs
    # 15m count should be exactly 10
    assert row["pickup_count_last_15m"] == 10
    # 1h count should be exactly 30 (10 in 15m + 20 outside 15m)
    assert row["pickup_count_last_1h"] == 30
    # 24h count should be exactly 60 (10 + 20 + 30)
    assert row["pickup_count_last_24h"] == 60
    # 7-day seasonal lag should be exactly 40
    assert row["pickup_count_same_hour_last_week"] == 40
    # Calendar features for Sunday 12:00
    assert row["hour_of_day"] == 12
    assert row["day_of_week"] == 6
    assert bool(row["is_weekend"]) is True


def test_compute_corridor_duration_features_hourly_anti_leakage_and_windows():
    """Verify corridor duration rolling averages, distance, and origin pressure joining."""
    t_obs = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    # Zone demand frame providing origin pressure for zone 10 at 12:00
    df_zone_demand = pd.DataFrame(
        [
            {
                "zone_id": 10,
                "pickup_datetime": t_obs,
                "pickup_count_last_1h": 45,
            }
        ]
    )

    trips = [
        # Trip 1: dropped off at 11:50 (within 15m of 12:00)
        {
            "pickup_zone_id": 10,
            "dropoff_zone_id": 20,
            "dropoff_datetime": t_obs - timedelta(minutes=10),
            "trip_duration_seconds": 600.0,
            "trip_distance_km": 5.0,
        },
        # Trip 2: dropped off at 11:20 (within 1h, outside 15m)
        {
            "pickup_zone_id": 10,
            "dropoff_zone_id": 20,
            "dropoff_datetime": t_obs - timedelta(minutes=40),
            "trip_duration_seconds": 1200.0,
            "trip_distance_km": 7.0,
        },
        # Trip 3: in-progress trip dropping off after 12:00: 12:20 (FUTURE anti-leakage test)
        {
            "pickup_zone_id": 10,
            "dropoff_zone_id": 20,
            "dropoff_datetime": t_obs + timedelta(minutes=20),
            "trip_duration_seconds": 5000.0,
            "trip_distance_km": 25.0,
        },
    ]
    df_trips = pd.DataFrame(trips)

    res = compute_corridor_duration_features_hourly(
        df_trips=df_trips,
        df_zone_demand=df_zone_demand,
        start_time=t_obs,
        end_time=t_obs,
    )

    assert len(res) == 1
    row = res.iloc[0]

    assert row["corridor_id"] == "10_20"
    assert row["dropoff_datetime"] == t_obs
    # 15m average duration: Trip 1 only (600.0s)
    assert row["avg_duration_last_15m"] == pytest.approx(600.0, abs=1e-2)
    # 1h average duration: (600 + 1200) / 2 = 900.0s (Trip 3 excluded!)
    assert row["avg_duration_last_1h"] == pytest.approx(900.0, abs=1e-2)
    # 1h average distance: (5.0 + 7.0) / 2 = 6.0km
    assert row["distance_km"] == pytest.approx(6.0, abs=1e-2)
    # Origin pressure joined from zone demand (45)
    assert row["origin_zone_demand_pressure"] == 45


def test_extract_and_load_offline_features_idempotency_sqlite(tmp_path):
    """Test full extraction and database loading with two-run idempotency proof."""
    db_path = tmp_path / "offline_test.db"
    engine = create_engine(f"sqlite:///{db_path.as_posix()}")

    # Create test SQLite tables
    with engine.begin() as conn:
        conn.execute(text("""
                CREATE TABLE trips (
                    trip_id INTEGER PRIMARY KEY,
                    pickup_zone_id INTEGER NOT NULL,
                    dropoff_zone_id INTEGER NOT NULL,
                    pickup_datetime TIMESTAMP NOT NULL,
                    dropoff_datetime TIMESTAMP NOT NULL,
                    trip_duration_seconds INTEGER NOT NULL,
                    trip_distance_km REAL NOT NULL
                );
                """))
        conn.execute(text("""
                CREATE TABLE zone_demand_features_hourly (
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
                CREATE TABLE corridor_duration_features_hourly (
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

        # Insert 10 sample trips
        t_base = datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        for i in range(10):
            conn.execute(
                text("""
                    INSERT INTO trips (trip_id, pickup_zone_id, dropoff_zone_id, pickup_datetime, dropoff_datetime, trip_duration_seconds, trip_distance_km)
                    VALUES (:trip_id, :pu, :do, :pu_dt, :do_dt, :dur, :dist)
                    """),
                {
                    "trip_id": i + 1,
                    "pu": 10,
                    "do": 20,
                    "pu_dt": t_base + timedelta(minutes=i * 5),
                    "do_dt": t_base + timedelta(minutes=i * 5 + 15),
                    "dur": 900,
                    "dist": 4.5,
                },
            )

    start_dt = datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    end_dt = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    # First Run
    z_count1, c_count1 = extract_and_load_offline_features(
        engine=engine,
        start_datetime=start_dt,
        end_datetime=end_dt,
        lookback_days=1,
    )
    assert z_count1 > 0
    assert c_count1 > 0

    with engine.connect() as conn:
        df_z1 = pd.read_sql(
            "SELECT * FROM zone_demand_features_hourly ORDER BY zone_id, pickup_datetime",
            conn,
        )
        df_c1 = pd.read_sql(
            "SELECT * FROM corridor_duration_features_hourly ORDER BY corridor_id, dropoff_datetime",
            conn,
        )

    # Second Run (Idempotency check)
    z_count2, c_count2 = extract_and_load_offline_features(
        engine=engine,
        start_datetime=start_dt,
        end_datetime=end_dt,
        lookback_days=1,
    )
    assert z_count2 == z_count1
    assert c_count2 == c_count1

    with engine.connect() as conn:
        df_z2 = pd.read_sql(
            "SELECT * FROM zone_demand_features_hourly ORDER BY zone_id, pickup_datetime",
            conn,
        )
        df_c2 = pd.read_sql(
            "SELECT * FROM corridor_duration_features_hourly ORDER BY corridor_id, dropoff_datetime",
            conn,
        )

    # Prove row counts and feature values are byte-for-byte identical (ignoring created_at timestamp difference)
    compare_cols_z = [c for c in df_z1.columns if c != "created_at"]
    compare_cols_c = [c for c in df_c1.columns if c != "created_at"]

    pd.testing.assert_frame_equal(df_z1[compare_cols_z], df_z2[compare_cols_z])
    pd.testing.assert_frame_equal(df_c1[compare_cols_c], df_c2[compare_cols_c])


def test_backfill_all_loaded_months_sqlite(tmp_path):
    """Test backfill_all_loaded_months loops through loaded_months."""
    db_path = tmp_path / "backfill_test.db"
    engine = create_engine(f"sqlite:///{db_path.as_posix()}")

    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE loaded_months (month_key VARCHAR(50) PRIMARY KEY, record_count INTEGER, loaded_at TIMESTAMP);"
            )
        )
        conn.execute(
            text(
                "INSERT INTO loaded_months VALUES ('2023-01', 100, CURRENT_TIMESTAMP);"
            )
        )
        conn.execute(text("""
                CREATE TABLE trips (
                    trip_id INTEGER PRIMARY KEY,
                    pickup_zone_id INTEGER NOT NULL,
                    dropoff_zone_id INTEGER NOT NULL,
                    pickup_datetime TIMESTAMP NOT NULL,
                    dropoff_datetime TIMESTAMP NOT NULL,
                    trip_duration_seconds INTEGER NOT NULL,
                    trip_distance_km REAL NOT NULL
                );
                """))
        conn.execute(text("""
                CREATE TABLE zone_demand_features_hourly (
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
                CREATE TABLE corridor_duration_features_hourly (
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

    results = backfill_all_loaded_months(engine=engine, lookback_days=1)
    assert "2023-01" in results
    z_cnt, c_cnt = results["2023-01"]
    assert z_cnt > 0
