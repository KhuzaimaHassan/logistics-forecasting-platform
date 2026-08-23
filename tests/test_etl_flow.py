"""Unit and integration tests for the historical TLC batch ETL Prefect flow."""

from pathlib import Path
from typing import Generator

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session

from src.common.db import Base
from src.common.models import LoadedMonth, RawTrip, TaxiZone, WarehouseTrip
from src.orchestration.flows.historical_etl import (
    check_already_loaded_task,
    ensure_taxi_zones_loaded_task,
    historical_tlc_batch_etl_flow,
    record_loaded_month_task,
)


@pytest.fixture
def sqlite_in_memory_session() -> Generator[Session, None, None]:
    """Provide an in-memory SQLite database session with attached raw and warehouse schemas."""
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
        engine.dispose()


@pytest.fixture
def sample_parquet_fixture(tmp_path: Path) -> Path:
    """Create a valid sample TLC yellow taxi Parquet file fixture."""
    data = {
        "VendorID": [1, 2, 2],
        "tpep_pickup_datetime": [
            "2023-01-01 08:00:00",
            "2023-01-01 08:30:00",
            "2023-01-01 09:00:00",
        ],
        "tpep_dropoff_datetime": [
            "2023-01-01 08:15:00",
            "2023-01-01 08:45:00",
            "2023-01-01 09:20:00",
        ],
        "passenger_count": [1, 2, 1],
        "trip_distance": [2.5, 3.0, 5.2],
        "RatecodeID": [1, 1, 1],
        "store_and_fwd_flag": ["N", "N", "N"],
        "PULocationID": [161, 162, 237],
        "DOLocationID": [141, 142, 238],
        "payment_type": [1, 1, 2],
        "fare_amount": [12.5, 14.0, 22.0],
        "extra": [0.5, 0.5, 0.5],
        "mta_tax": [0.5, 0.5, 0.5],
        "tip_amount": [2.5, 3.0, 0.0],
        "tolls_amount": [0.0, 0.0, 0.0],
        "improvement_surcharge": [1.0, 1.0, 1.0],
        "total_amount": [17.0, 19.0, 24.0],
        "congestion_surcharge": [2.5, 2.5, 2.5],
        "airport_fee": [0.0, 0.0, 0.0],
    }
    df = pd.DataFrame(data)
    parquet_path = tmp_path / "yellow_tripdata_2023-01.parquet"
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_path)
    return parquet_path


def test_ensure_taxi_zones_loaded_task(sqlite_in_memory_session: Session) -> None:
    """Test that ensure_taxi_zones_loaded_task populates taxi zones once."""
    assert sqlite_in_memory_session.query(TaxiZone).count() == 0
    count = ensure_taxi_zones_loaded_task(session=sqlite_in_memory_session)
    assert count > 0
    assert sqlite_in_memory_session.query(TaxiZone).count() == count

    # Subsequent invocation is a no-op
    count2 = ensure_taxi_zones_loaded_task(session=sqlite_in_memory_session)
    assert count2 == count


def test_check_already_loaded_and_record_task(
    sqlite_in_memory_session: Session,
) -> None:
    """Test checking and recording loaded months in warehouse.loaded_months."""
    # Initially not loaded
    is_loaded = check_already_loaded_task(
        cab_type="yellow", year=2023, month=1, session=sqlite_in_memory_session
    )
    assert not is_loaded

    # Record month
    month_key = record_loaded_month_task(
        cab_type="yellow",
        year=2023,
        month=1,
        record_count=1500,
        session=sqlite_in_memory_session,
    )
    assert month_key == "yellow_2023-01"

    # Now should be loaded
    is_loaded_after = check_already_loaded_task(
        cab_type="yellow", year=2023, month=1, session=sqlite_in_memory_session
    )
    assert is_loaded_after

    record = (
        sqlite_in_memory_session.query(LoadedMonth)
        .filter_by(month_key="yellow_2023-01")
        .first()
    )
    assert record is not None
    assert record.record_count == 1500


def test_flow_end_to_end_execution(
    sqlite_in_memory_session: Session, sample_parquet_fixture: Path, monkeypatch
) -> None:
    """Test complete flow execution on sample data."""
    # Mock extract_batch_task to return fixture path
    monkeypatch.setattr(
        "src.orchestration.flows.historical_etl.extract_batch_task.fn",
        lambda cab_type, year, month, download_dir=None: sample_parquet_fixture,
    )

    result = historical_tlc_batch_etl_flow(
        cab_type="yellow",
        year=2023,
        month=1,
        force_reload=False,
        session=sqlite_in_memory_session,
    )

    assert result["status"] == "success"
    assert result["month_key"] == "yellow_2023-01"
    assert result["raw_rows_staged"] == 3
    assert result["warehouse_rows_loaded"] == 3

    # Check database rows
    assert sqlite_in_memory_session.query(RawTrip).count() == 3
    assert sqlite_in_memory_session.query(WarehouseTrip).count() == 3
    assert sqlite_in_memory_session.query(LoadedMonth).count() == 1


def test_flow_idempotency_skip_on_second_run(
    sqlite_in_memory_session: Session, sample_parquet_fixture: Path, monkeypatch
) -> None:
    """Test that a second flow run against the same month cleanly skips without duplicate rows."""
    monkeypatch.setattr(
        "src.orchestration.flows.historical_etl.extract_batch_task.fn",
        lambda cab_type, year, month, download_dir=None: sample_parquet_fixture,
    )

    # First run: loads data
    result1 = historical_tlc_batch_etl_flow(
        cab_type="yellow",
        year=2023,
        month=1,
        force_reload=False,
        session=sqlite_in_memory_session,
    )
    assert result1["status"] == "success"
    raw_count_1 = sqlite_in_memory_session.query(RawTrip).count()
    wh_count_1 = sqlite_in_memory_session.query(WarehouseTrip).count()
    assert raw_count_1 == 3
    assert wh_count_1 == 3

    # Second run: should skip immediately
    result2 = historical_tlc_batch_etl_flow(
        cab_type="yellow",
        year=2023,
        month=1,
        force_reload=False,
        session=sqlite_in_memory_session,
    )
    assert result2["status"] == "skipped"
    assert result2["reason"] == "already_loaded"

    # Row counts in database must remain strictly identical (zero duplicate insertions)
    raw_count_2 = sqlite_in_memory_session.query(RawTrip).count()
    wh_count_2 = sqlite_in_memory_session.query(WarehouseTrip).count()
    assert raw_count_2 == raw_count_1
    assert wh_count_2 == wh_count_1


def test_flow_force_reload_override(
    sqlite_in_memory_session: Session, sample_parquet_fixture: Path, monkeypatch
) -> None:
    """Test that force_reload=True bypasses the idempotency check."""
    monkeypatch.setattr(
        "src.orchestration.flows.historical_etl.extract_batch_task.fn",
        lambda cab_type, year, month, download_dir=None: sample_parquet_fixture,
    )

    # First run
    result1 = historical_tlc_batch_etl_flow(
        cab_type="yellow",
        year=2023,
        month=1,
        force_reload=False,
        session=sqlite_in_memory_session,
    )
    assert result1["status"] == "success"

    # Second run with force_reload=True
    result2 = historical_tlc_batch_etl_flow(
        cab_type="yellow",
        year=2023,
        month=1,
        force_reload=True,
        session=sqlite_in_memory_session,
    )
    assert result2["status"] == "success"
