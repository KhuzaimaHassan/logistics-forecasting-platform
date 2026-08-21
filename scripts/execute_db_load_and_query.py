"""Script to execute two-stage DB loading (raw.trips -> warehouse.trips) against a real database and execute verification SQL queries."""

import logging
from pathlib import Path

from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session

from src.common.db import Base
from src.extract.load_zones import load_taxi_zones_to_db
from src.extract.raw_loader import bulk_load_raw_trips
from src.transform.batch_transformer import (
    BatchTransformer,
    batch_insert_warehouse_trips,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main() -> None:
    db_file = Path("data/dev_platform.db")
    db_file.parent.mkdir(parents=True, exist_ok=True)
    if db_file.exists():
        db_file.unlink()  # fresh clean DB for verification run

    engine = create_engine(
        f"sqlite:///{db_file.as_posix()}", connect_args={"check_same_thread": False}
    )

    @event.listens_for(engine, "connect")
    def attach_schemas(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("ATTACH DATABASE ':memory:' AS raw;")
        cursor.execute("ATTACH DATABASE ':memory:' AS warehouse;")
        cursor.close()

    # Create DDL tables
    Base.metadata.create_all(engine)

    parquet_path = Path("data/raw/yellow_tripdata_2023-01.parquet")
    if not parquet_path.exists():
        raise FileNotFoundError(f"Source Parquet dataset not found at {parquet_path}")

    session = Session(engine)

    try:
        # Pre-populate taxi zones for foreign key relationships
        logger.info("Loading reference Taxi Zones into warehouse.taxi_zones...")
        load_taxi_zones_to_db(session)

        # STAGE 1: Load raw Parquet into raw.trips database table
        logger.info("=== STAGE 1: Bulk loading raw dataset into raw.trips ===")
        bulk_load_raw_trips(
            session=session,
            parquet_path=parquet_path,
            source_file="yellow_tripdata_2023-01.parquet",
            batch_size=10000,
        )

        # STAGE 2: Transform raw records and load clean data into warehouse.trips database table
        logger.info(
            "=== STAGE 2: Transforming raw records & inserting into warehouse.trips ==="
        )
        transformer = BatchTransformer()
        clean_df, report = transformer.transform_parquet_file(
            parquet_path, cab_type="yellow"
        )
        batch_insert_warehouse_trips(
            session=session,
            clean_df=clean_df,
            batch_size=10000,
        )

        print("\n" + "=" * 60)
        print("=== REAL SQL QUERY OUTPUT FROM DATABASE TABLES ===")
        print("=" * 60)

        # 1. Query raw.trips count
        res_raw = session.execute(text("SELECT COUNT(*) FROM raw.trips;")).scalar()
        print("\nSQL> SELECT COUNT(*) FROM raw.trips;")
        print(f"COUNT: {res_raw:,}")

        # 2. Query warehouse.trips count
        res_wh = session.execute(text("SELECT COUNT(*) FROM warehouse.trips;")).scalar()
        print("\nSQL> SELECT COUNT(*) FROM warehouse.trips;")
        print(f"COUNT: {res_wh:,}")

        # 3. Query spot check from warehouse.trips
        spot_check_sql = text("""
            SELECT trip_id, vendor_id, cab_type, pickup_zone_id, dropoff_zone_id,
                   pickup_datetime, dropoff_datetime, trip_duration_seconds,
                   time_bin_15m, day_of_week, hour_of_day, is_weekend,
                   passenger_count, trip_distance_km, fare_amount, total_amount
            FROM warehouse.trips
            LIMIT 3;
        """)
        rows = session.execute(spot_check_sql).fetchall()

        print("\nSQL> SELECT * FROM warehouse.trips LIMIT 3;")
        for idx, row in enumerate(rows, 1):
            print(f"\nRow #{idx}:")
            print(dict(row._mapping))

    finally:
        session.close()


if __name__ == "__main__":
    main()
