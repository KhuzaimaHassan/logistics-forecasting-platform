"""Raw loader module for Stage 1 bulk insertion of TLC Parquet datasets into raw.trips."""

import logging
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from src.common.models import RawTrip

logger = logging.getLogger(__name__)


def bulk_load_raw_trips(
    session: Any, parquet_path: Path, source_file: str, batch_size: int = 5000
) -> int:
    """Stage 1: Bulk load raw Parquet records into raw.trips near-verbatim."""
    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet file not found at: {parquet_path}")

    logger.info(
        f"Stage 1 Bulk Loading raw Parquet file '{parquet_path}' into raw.trips..."
    )
    table = pq.read_table(parquet_path)
    df = table.to_pandas()

    if df.empty:
        logger.info("Parquet file contains 0 rows. Skipping raw staging load.")
        return 0

    pickup_col = (
        "tpep_pickup_datetime"
        if "tpep_pickup_datetime" in df.columns
        else "lpep_pickup_datetime"
    )
    dropoff_col = (
        "tpep_dropoff_datetime"
        if "tpep_dropoff_datetime" in df.columns
        else "lpep_dropoff_datetime"
    )

    records = df.to_dict(orient="records")
    total_loaded = 0

    for i in range(0, len(records), batch_size):
        chunk = records[i : i + batch_size]
        objects = []
        for r in chunk:
            objects.append(
                RawTrip(
                    vendor_id=int(r.get("VendorID", 0) or 0),
                    pickup_datetime=r.get(pickup_col),
                    dropoff_datetime=r.get(dropoff_col),
                    passenger_count=int(r.get("passenger_count", 0) or 0),
                    trip_distance=float(r.get("trip_distance", 0.0) or 0.0),
                    ratecode_id=int(r.get("RatecodeID", 0) or 0),
                    store_and_fwd_flag=r.get("store_and_fwd_flag"),
                    pu_location_id=int(r.get("PULocationID", 0) or 0),
                    do_location_id=int(r.get("DOLocationID", 0) or 0),
                    payment_type=int(r.get("payment_type", 0) or 0),
                    fare_amount=float(r.get("fare_amount", 0.0) or 0.0),
                    extra=float(r.get("extra", 0.0) or 0.0),
                    mta_tax=float(r.get("mta_tax", 0.0) or 0.0),
                    tip_amount=float(r.get("tip_amount", 0.0) or 0.0),
                    tolls_amount=float(r.get("tolls_amount", 0.0) or 0.0),
                    improvement_surcharge=float(
                        r.get("improvement_surcharge", 0.0) or 0.0
                    ),
                    total_amount=float(r.get("total_amount", 0.0) or 0.0),
                    congestion_surcharge=float(
                        r.get("congestion_surcharge", 0.0) or 0.0
                    ),
                    airport_fee=float(r.get("airport_fee", 0.0) or 0.0),
                    source_file=source_file,
                )
            )

        session.bulk_save_objects(objects)
        session.commit()
        total_loaded += len(chunk)

    logger.info(
        f"Successfully loaded {total_loaded:,} raw trip records into raw.trips."
    )
    return total_loaded
