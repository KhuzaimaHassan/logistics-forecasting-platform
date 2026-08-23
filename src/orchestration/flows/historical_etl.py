"""Prefect flow for orchestrating idempotent historical NYC TLC Parquet batch ETL."""

import argparse
import logging
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from prefect import flow, task
from sqlalchemy.orm import Session

from src.common.db import get_engine
from src.common.models import LoadedMonth, TaxiZone
from src.extract.batch_puller import TLCParquetExtractor
from src.extract.load_zones import load_taxi_zones_to_db
from src.extract.raw_loader import bulk_load_raw_trips
from src.transform.batch_transformer import (
    BatchTransformer,
    batch_insert_warehouse_trips,
)

logger = logging.getLogger(__name__)


@task(
    name="ensure_taxi_zones_loaded",
    retries=2,
    retry_delay_seconds=10,
    cache_policy=None,
)
def ensure_taxi_zones_loaded_task(
    engine: Optional[Any] = None, session: Optional[Session] = None
) -> int:
    """One-time prerequisite setup task: ensure reference taxi zones are populated in warehouse.taxi_zones."""
    close_session = False
    if session is None:
        eng = engine or get_engine()
        session = Session(bind=eng)
        close_session = True

    try:
        count = session.query(TaxiZone).count()
        if count == 0:
            logger.info(
                "warehouse.taxi_zones is empty. Initializing reference Taxi Zones..."
            )
            loaded = load_taxi_zones_to_db(session=session)
            logger.info(f"Loaded {loaded} reference Taxi Zones.")
            return loaded
        else:
            logger.info(
                f"warehouse.taxi_zones already initialized ({count} zones present)."
            )
            return count
    finally:
        if close_session:
            session.close()


@task(name="check_already_loaded", retries=2, retry_delay_seconds=5, cache_policy=None)
def check_already_loaded_task(
    cab_type: str,
    year: int,
    month: int,
    engine: Optional[Any] = None,
    session: Optional[Session] = None,
) -> bool:
    """Check whether the target monthly batch is already recorded in warehouse.loaded_months."""
    month_key = f"{cab_type.lower().strip()}_{year:04d}-{month:02d}"
    close_session = False
    if session is None:
        eng = engine or get_engine()
        session = Session(bind=eng)
        close_session = True

    try:
        existing = session.query(LoadedMonth).filter_by(month_key=month_key).first()
        is_loaded = existing is not None
        if is_loaded:
            logger.info(
                f"Month '{month_key}' is already recorded in warehouse.loaded_months "
                f"({existing.record_count:,} records loaded at {existing.loaded_at})."
            )
        else:
            logger.info(
                f"Month '{month_key}' is NOT yet recorded in warehouse.loaded_months."
            )
        return is_loaded
    finally:
        if close_session:
            session.close()


@task(name="extract_tlc_batch", retries=3, retry_delay_seconds=15)
def extract_batch_task(
    cab_type: str,
    year: int,
    month: int,
    download_dir: Optional[Path] = None,
) -> Path:
    """Download monthly TLC Parquet batch from CDN or use local cache."""
    target_dir = download_dir or Path("data/raw")
    target_dir.mkdir(parents=True, exist_ok=True)
    extractor = TLCParquetExtractor(download_dir=target_dir)
    parquet_path = extractor.download_monthly_file(
        cab_type=cab_type, year=year, month=month
    )
    logger.info(
        f"Extracted TLC batch Parquet file: {parquet_path} ({parquet_path.stat().st_size:,} bytes)"
    )
    return parquet_path


@task(name="load_raw_staging", retries=2, retry_delay_seconds=15, cache_policy=None)
def load_raw_staging_task(
    parquet_path: Path,
    source_file: str,
    engine: Optional[Any] = None,
    session: Optional[Session] = None,
    batch_size: int = 50000,
) -> int:
    """Stage 1: Bulk load raw Parquet rows near-verbatim into raw.trips."""
    close_session = False
    if session is None:
        eng = engine or get_engine()
        session = Session(bind=eng)
        close_session = True

    try:
        raw_loaded = bulk_load_raw_trips(
            session=session,
            parquet_path=parquet_path,
            source_file=source_file,
            batch_size=batch_size,
        )
        logger.info(
            f"Stage 1 completed: {raw_loaded:,} raw rows loaded into raw.trips."
        )
        return raw_loaded
    finally:
        if close_session:
            session.close()


@task(
    name="transform_and_load_warehouse",
    retries=2,
    retry_delay_seconds=15,
    cache_policy=None,
)
def transform_and_load_warehouse_task(
    parquet_path: Path,
    cab_type: str,
    engine: Optional[Any] = None,
    session: Optional[Session] = None,
    batch_size: int = 50000,
) -> Tuple[int, Dict[str, Any]]:
    """Stage 2: Validate, clean, engineer features, and bulk insert into warehouse.trips."""
    transformer = BatchTransformer()
    clean_df, report = transformer.transform_parquet_file(
        parquet_path=parquet_path, cab_type=cab_type
    )

    close_session = False
    if session is None:
        eng = engine or get_engine()
        session = Session(bind=eng)
        close_session = True

    try:
        inserted_count = batch_insert_warehouse_trips(
            session=session,
            clean_df=clean_df,
            batch_size=batch_size,
        )
        logger.info(
            f"Stage 2 completed: {inserted_count:,} clean rows inserted into warehouse.trips."
        )
        return inserted_count, {
            "total_input_rows": report.total_input_rows,
            "clean_rows": report.clean_rows,
            "rejected_rows": report.rejected_rows,
            "rejection_reasons": report.rejection_reasons,
        }
    finally:
        if close_session:
            session.close()


@task(name="record_loaded_month", retries=2, retry_delay_seconds=10, cache_policy=None)
def record_loaded_month_task(
    cab_type: str,
    year: int,
    month: int,
    record_count: int,
    engine: Optional[Any] = None,
    session: Optional[Session] = None,
) -> str:
    """Record successfully processed month into warehouse.loaded_months for idempotency."""
    month_key = f"{cab_type.lower().strip()}_{year:04d}-{month:02d}"
    close_session = False
    if session is None:
        eng = engine or get_engine()
        session = Session(bind=eng)
        close_session = True

    try:
        existing = session.query(LoadedMonth).filter_by(month_key=month_key).first()
        if existing:
            existing.record_count = record_count
        else:
            loaded_rec = LoadedMonth(
                month_key=month_key,
                record_count=record_count,
            )
            session.add(loaded_rec)
        session.commit()
        logger.info(
            f"Recorded month '{month_key}' with {record_count:,} records in warehouse.loaded_months."
        )
        return month_key
    finally:
        if close_session:
            session.close()


@flow(name="historical_tlc_batch_etl", retries=1, retry_delay_seconds=30)
def historical_tlc_batch_etl_flow(
    cab_type: str = "yellow",
    year: int = 2023,
    month: int = 1,
    force_reload: bool = False,
    download_dir: Optional[str] = None,
    engine: Optional[Any] = None,
    session: Optional[Session] = None,
) -> Dict[str, Any]:
    """Prefect orchestration flow for historical TLC batch ETL with idempotency checking.

    Flow Pipeline:
      1. One-time Setup: Ensure reference taxi zones exist in warehouse.taxi_zones.
      2. Idempotency Check: Check warehouse.loaded_months for month_key.
         - If already loaded and force_reload=False: Return SKIPPED.
      3. Task 1 (Extract): Download/cache monthly Parquet file.
      4. Task 2 (Raw Stage): Bulk insert raw records into raw.trips.
      5. Task 3 (Warehouse Transform & Load): Validate, clean, and insert into warehouse.trips.
      6. Task 4 (Audit Record): Record completion in warehouse.loaded_months.
    """
    cab_type_clean = cab_type.lower().strip()
    month_key = f"{cab_type_clean}_{year:04d}-{month:02d}"
    dl_path = Path(download_dir) if download_dir else None

    logger.info(
        f"=== Starting historical TLC batch ETL flow for '{month_key}' (force_reload={force_reload}) ==="
    )

    # Step 1: Ensure reference taxi zones are initialized
    ensure_taxi_zones_loaded_task(engine=engine, session=session)

    # Step 2: Idempotency check
    already_loaded = check_already_loaded_task(
        cab_type=cab_type_clean, year=year, month=month, engine=engine, session=session
    )

    if already_loaded and not force_reload:
        logger.info(
            f"SKIPPING: Month '{month_key}' is already loaded. Skipping extraction and DB load."
        )
        return {
            "status": "skipped",
            "reason": "already_loaded",
            "month_key": month_key,
            "message": f"Month '{month_key}' is already present in warehouse.loaded_months.",
        }

    # Step 3: Extract
    parquet_path = extract_batch_task(
        cab_type=cab_type_clean,
        year=year,
        month=month,
        download_dir=dl_path,
    )

    source_file = parquet_path.name

    # Step 4: Stage 1 Raw Load
    raw_loaded = load_raw_staging_task(
        parquet_path=parquet_path,
        source_file=source_file,
        engine=engine,
        session=session,
    )

    # Step 5: Stage 2 Warehouse Transform & Load
    warehouse_loaded, report_metrics = transform_and_load_warehouse_task(
        parquet_path=parquet_path,
        cab_type=cab_type_clean,
        engine=engine,
        session=session,
    )

    # Step 6: Record Month in warehouse.loaded_months
    record_loaded_month_task(
        cab_type=cab_type_clean,
        year=year,
        month=month,
        record_count=warehouse_loaded,
        engine=engine,
        session=session,
    )

    logger.info(
        f"=== Flow completed successfully for '{month_key}': {warehouse_loaded:,} clean warehouse rows landed ==="
    )

    return {
        "status": "success",
        "month_key": month_key,
        "raw_rows_staged": raw_loaded,
        "warehouse_rows_loaded": warehouse_loaded,
        "report_metrics": report_metrics,
    }


def main() -> None:
    """CLI entrypoint to execute the Prefect historical batch ETL flow directly."""
    parser = argparse.ArgumentParser(
        description="Execute historical TLC batch ETL Prefect flow."
    )
    parser.add_argument(
        "--cab-type",
        type=str,
        default="yellow",
        choices=["yellow", "green", "fhv", "fhvhv"],
    )
    parser.add_argument("--year", type=int, default=2023)
    parser.add_argument("--month", type=int, default=1)
    parser.add_argument(
        "--force-reload",
        action="store_true",
        help="Force re-execution even if already recorded.",
    )
    parser.add_argument("--download-dir", type=str, default="data/raw")

    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    result = historical_tlc_batch_etl_flow(
        cab_type=args.cab_type,
        year=args.year,
        month=args.month,
        force_reload=args.force_reload,
        download_dir=args.download_dir,
    )
    print("\nFlow Execution Result:")
    print(result)


if __name__ == "__main__":
    main()
