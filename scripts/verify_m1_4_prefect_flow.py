"""Verification script to execute the Prefect historical batch ETL flow against real data twice and prove idempotency."""

import logging
from pathlib import Path

from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session

from src.common.db import Base
from src.orchestration.flows.historical_etl import historical_tlc_batch_etl_flow

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


def main() -> None:
    db_file = Path("data/dev_platform.db")
    db_file.parent.mkdir(parents=True, exist_ok=True)
    if db_file.exists():
        db_file.unlink()  # clean run

    engine = create_engine(
        f"sqlite:///{db_file.as_posix()}", connect_args={"check_same_thread": False}
    )

    @event.listens_for(engine, "connect")
    def attach_schemas(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("ATTACH DATABASE ':memory:' AS raw;")
        cursor.execute("ATTACH DATABASE ':memory:' AS warehouse;")
        cursor.close()

    Base.metadata.create_all(engine)
    session = Session(engine)

    print("\n" + "=" * 70)
    print("=== STEP 1: INITIAL FLOW EXECUTION (yellow 2023-01) ===")
    print("=" * 70)

    res1 = historical_tlc_batch_etl_flow(
        cab_type="yellow",
        year=2023,
        month=1,
        force_reload=False,
        download_dir="data/raw",
        engine=engine,
        session=session,
    )
    print("\nFlow Run 1 Result:")
    print(res1)

    raw_cnt_1 = session.execute(text("SELECT COUNT(*) FROM raw.trips;")).scalar()
    wh_cnt_1 = session.execute(text("SELECT COUNT(*) FROM warehouse.trips;")).scalar()
    months_cnt_1 = session.execute(
        text("SELECT COUNT(*) FROM warehouse.loaded_months;")
    ).scalar()
    month_row_1 = session.execute(
        text("SELECT month_key, record_count, loaded_at FROM warehouse.loaded_months;")
    ).fetchall()

    print("\nSQL After Run 1:")
    print(f"  raw.trips COUNT: {raw_cnt_1:,}")
    print(f"  warehouse.trips COUNT: {wh_cnt_1:,}")
    print(f"  warehouse.loaded_months COUNT: {months_cnt_1:,}")
    print(f"  warehouse.loaded_months rows: {[dict(r._mapping) for r in month_row_1]}")

    print("\n" + "=" * 70)
    print("=== STEP 2: SECOND FLOW EXECUTION (IDEMPOTENCY PROOF - yellow 2023-01) ===")
    print("=" * 70)

    res2 = historical_tlc_batch_etl_flow(
        cab_type="yellow",
        year=2023,
        month=1,
        force_reload=False,
        download_dir="data/raw",
        engine=engine,
        session=session,
    )
    print("\nFlow Run 2 Result:")
    print(res2)

    raw_cnt_2 = session.execute(text("SELECT COUNT(*) FROM raw.trips;")).scalar()
    wh_cnt_2 = session.execute(text("SELECT COUNT(*) FROM warehouse.trips;")).scalar()
    months_cnt_2 = session.execute(
        text("SELECT COUNT(*) FROM warehouse.loaded_months;")
    ).scalar()

    print("\nSQL After Run 2 (Idempotency Check):")
    print(f"  raw.trips COUNT: {raw_cnt_2:,} (identical: {raw_cnt_2 == raw_cnt_1})")
    print(f"  warehouse.trips COUNT: {wh_cnt_2:,} (identical: {wh_cnt_2 == wh_cnt_1})")
    print(
        f"  warehouse.loaded_months COUNT: {months_cnt_2:,} (identical: {months_cnt_2 == months_cnt_1})"
    )

    assert (
        res2["status"] == "skipped"
    ), f"Expected status 'skipped', got {res2['status']}"
    assert raw_cnt_2 == raw_cnt_1, "Row count mismatch in raw.trips"
    assert wh_cnt_2 == wh_cnt_1, "Row count mismatch in warehouse.trips"
    print(
        "\nSUCCESS: Idempotency confirmed. Second run skipped and DB row counts are strictly unchanged!"
    )


if __name__ == "__main__":
    main()
