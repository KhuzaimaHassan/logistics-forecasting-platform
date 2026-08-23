"""Unit tests for the M1-3 batch transformer and raw loader modules."""

from typing import Generator

import pandas as pd
import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session

from src.common.db import Base
from src.transform.batch_transformer import (
    BatchTransformer,
    batch_insert_warehouse_trips,
    generate_deterministic_trip_id,
)


@pytest.fixture
def db_session() -> Generator[Session, None, None]:
    """Create an in-memory SQLite database session with raw/warehouse schemas attached."""
    engine = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False}
    )

    @event.listens_for(engine, "connect")
    def attach_schemas(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("ATTACH DATABASE ':memory:' AS raw;")
        cursor.execute("ATTACH DATABASE ':memory:' AS warehouse;")
        cursor.close()

    Base.metadata.create_all(engine)
    session = Session(engine)
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def sample_raw_dataframe() -> pd.DataFrame:
    """Create a sample Pandas DataFrame with valid and outlier trip records."""
    return pd.DataFrame(
        [
            # 1. Valid normal trip
            {
                "VendorID": 1,
                "tpep_pickup_datetime": "2023-01-01 10:00:00",
                "tpep_dropoff_datetime": "2023-01-01 10:15:00",
                "passenger_count": 2,
                "trip_distance": 3.5,
                "fare_amount": 15.0,
                "total_amount": 18.5,
                "PULocationID": 132,  # JFK Airport
                "DOLocationID": 230,  # Times Square
            },
            # 2. Outlier duration < 60s (30 seconds)
            {
                "VendorID": 1,
                "tpep_pickup_datetime": "2023-01-01 10:00:00",
                "tpep_dropoff_datetime": "2023-01-01 10:00:30",
                "passenger_count": 1,
                "trip_distance": 0.5,
                "fare_amount": 5.0,
                "total_amount": 5.5,
                "PULocationID": 132,
                "DOLocationID": 230,
            },
            # 3. Outlier distance <= 0.0
            {
                "VendorID": 2,
                "tpep_pickup_datetime": "2023-01-01 11:00:00",
                "tpep_dropoff_datetime": "2023-01-01 11:20:00",
                "passenger_count": 1,
                "trip_distance": 0.0,
                "fare_amount": 10.0,
                "total_amount": 12.0,
                "PULocationID": 138,  # LGA
                "DOLocationID": 132,
            },
            # 4. Outlier speed > 100 mph (100 miles in 10 minutes = 600 mph)
            {
                "VendorID": 1,
                "tpep_pickup_datetime": "2023-01-01 12:00:00",
                "tpep_dropoff_datetime": "2023-01-01 12:10:00",
                "passenger_count": 1,
                "trip_distance": 100.0,
                "fare_amount": 50.0,
                "total_amount": 55.0,
                "PULocationID": 1,
                "DOLocationID": 2,
            },
            # 5. Unmapped Zone ID (Location ID 999 outside 1..265)
            {
                "VendorID": 1,
                "tpep_pickup_datetime": "2023-01-01 13:00:00",
                "tpep_dropoff_datetime": "2023-01-01 13:15:00",
                "passenger_count": 1,
                "trip_distance": 2.0,
                "fare_amount": 10.0,
                "total_amount": 11.0,
                "PULocationID": 999,  # Unmapped
                "DOLocationID": 230,
            },
            # 6. Valid weekend trip
            {
                "VendorID": 2,
                "tpep_pickup_datetime": "2023-01-07 14:07:33",  # Saturday
                "tpep_dropoff_datetime": "2023-01-07 14:27:33",
                "passenger_count": 3,
                "trip_distance": 5.2,
                "fare_amount": 22.0,
                "total_amount": 26.5,
                "PULocationID": 138,
                "DOLocationID": 132,
            },
        ]
    )


def test_batch_transformer_cleaning_and_features(
    sample_raw_dataframe: pd.DataFrame,
) -> None:
    """Test validation thresholds, anomaly filtering, feature derivation, and reporting."""
    transformer = BatchTransformer()
    clean_df, report = transformer.transform_dataframe(
        sample_raw_dataframe, cab_type="yellow"
    )

    # Out of 6 input rows: row 1 and row 6 are valid -> 2 clean rows expected
    assert report.total_input_rows == 6
    assert report.clean_rows == 2
    assert report.rejected_rows == 4
    assert len(clean_df) == 2

    # Check rejection audit counts
    assert report.rejection_reasons["invalid_duration"] == 1
    assert report.rejection_reasons["invalid_distance"] == 1
    assert report.rejection_reasons["speed_anomaly"] == 1
    assert report.rejection_reasons["unmapped_zone_id"] == 1
    assert report.unmapped_zone_ids_seen == {999}

    # Verify features derived on clean rows
    row1 = clean_df.iloc[0]
    assert row1["trip_duration_seconds"] == 900.0  # 15 minutes
    assert row1["time_bin_15m"] == pd.Timestamp("2023-01-01 10:00:00")
    assert row1["hour_of_day"] == 10
    assert row1["day_of_week"] == 6  # Sunday
    assert bool(row1["is_weekend"]) is True
    assert row1["trip_distance_km"] == round(3.5 * 1.60934, 2)

    row6 = clean_df.iloc[1]
    assert row6["time_bin_15m"] == pd.Timestamp("2023-01-07 14:00:00")
    assert row6["day_of_week"] == 5  # Saturday
    assert bool(row6["is_weekend"]) is True


def test_deterministic_trip_id_generation() -> None:
    """Test that deterministic trip_id BigInteger generation is repeatable and stable."""

    class MockRow:

        def __init__(self) -> None:
            self.vendor_id = 1
            self.tpep_pickup_datetime = "2023-01-01 10:00:00"
            self.tpep_dropoff_datetime = "2023-01-01 10:15:00"
            self.PULocationID = 132
            self.DOLocationID = 230
            self.fare_amount = 15.0
            self.trip_distance = 3.5

    row = MockRow()
    id1 = generate_deterministic_trip_id(row)
    id2 = generate_deterministic_trip_id(row)

    assert id1 == id2
    assert isinstance(id1, int)
    assert id1 > 0


def test_batch_insert_warehouse_trips_in_memory(
    db_session: Session, sample_raw_dataframe: pd.DataFrame
) -> None:
    """Test inserting clean transformed records into the database session."""

    from src.common.models import TaxiZone, WarehouseTrip

    # Pre-populate taxi zones for foreign keys
    db_session.add_all(
        [
            TaxiZone(
                zone_id=132,
                borough="Queens",
                zone_name="JFK Airport",
                service_zone="Airports",
                centroid_lat=40.64,
                centroid_lon=-73.78,
            ),
            TaxiZone(
                zone_id=138,
                borough="Queens",
                zone_name="LaGuardia Airport",
                service_zone="Airports",
                centroid_lat=40.77,
                centroid_lon=-73.87,
            ),
            TaxiZone(
                zone_id=230,
                borough="Manhattan",
                zone_name="Times Square",
                service_zone="Yellow Zone",
                centroid_lat=40.75,
                centroid_lon=-73.98,
            ),
        ]
    )
    db_session.commit()

    transformer = BatchTransformer()
    clean_df, _ = transformer.transform_dataframe(sample_raw_dataframe)

    count = batch_insert_warehouse_trips(db_session, clean_df)
    assert count == 2

    # Query DB to verify loaded trips
    db_trips = db_session.query(WarehouseTrip).all()
    assert len(db_trips) == 2
    assert db_trips[0].pickup_zone_id in (132, 138)
    assert db_trips[0].trip_duration_seconds > 0
