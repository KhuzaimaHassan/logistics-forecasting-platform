"""Raw loader module for Stage 1 bulk insertion of TLC Parquet datasets into raw.trips."""

import logging
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from src.common.models import RawTrip

logger = logging.getLogger(__name__)


def bulk_load_raw_trips(
    session: Any, parquet_path: Path, source_file: str, batch_size: int = 50000
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

    # Fast column renaming and dict conversion
    raw_df = pd.DataFrame()
    raw_df["vendor_id"] = df.get("VendorID", 0).fillna(0).astype(int)
    raw_df["pickup_datetime"] = pd.to_datetime(df[pickup_col])
    raw_df["dropoff_datetime"] = pd.to_datetime(df[dropoff_col])
    raw_df["passenger_count"] = df.get("passenger_count", 0.0).fillna(0.0).astype(float)
    raw_df["trip_distance"] = df.get("trip_distance", 0.0).fillna(0.0).astype(float)
    raw_df["rate_code_id"] = df.get("RatecodeID", 0.0).fillna(0.0).astype(float)
    raw_df["store_and_fwd_flag"] = (
        df.get("store_and_fwd_flag", "").fillna("").astype(str)
    )
    raw_df["pickup_location_id"] = df.get("PULocationID", 0).fillna(0).astype(int)
    raw_df["dropoff_location_id"] = df.get("DOLocationID", 0).fillna(0).astype(int)
    raw_df["payment_type"] = df.get("payment_type", 0.0).fillna(0.0).astype(float)
    raw_df["fare_amount"] = df.get("fare_amount", 0.0).fillna(0.0).astype(float)
    raw_df["extra"] = df.get("extra", 0.0).fillna(0.0).astype(float)
    raw_df["mta_tax"] = df.get("mta_tax", 0.0).fillna(0.0).astype(float)
    raw_df["tip_amount"] = df.get("tip_amount", 0.0).fillna(0.0).astype(float)
    raw_df["tolls_amount"] = df.get("tolls_amount", 0.0).fillna(0.0).astype(float)
    raw_df["improvement_surcharge"] = (
        df.get("improvement_surcharge", 0.0).fillna(0.0).astype(float)
    )
    raw_df["total_amount"] = df.get("total_amount", 0.0).fillna(0.0).astype(float)
    raw_df["congestion_surcharge"] = (
        df.get("congestion_surcharge", 0.0).fillna(0.0).astype(float)
    )
    raw_df["airport_fee"] = df.get("airport_fee", 0.0).fillna(0.0).astype(float)
    raw_df["source_file"] = source_file

    mappings = raw_df.to_dict(orient="records")
    total_loaded = 0

    for i in range(0, len(mappings), batch_size):
        chunk = mappings[i : i + batch_size]
        session.bulk_insert_mappings(RawTrip, chunk)
        session.commit()
        total_loaded += len(chunk)

    logger.info(
        f"Successfully loaded {total_loaded:,} raw trip records into raw.trips."
    )
    return total_loaded
