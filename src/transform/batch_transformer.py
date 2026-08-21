"""Batch transformer module for validating, cleaning, and feature-engineering historical TLC trip records."""

import hashlib
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Set, Tuple

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# Standard NYC TLC Taxi Zone Location IDs (1 to 265)
VALID_ZONE_IDS: Set[int] = set(range(1, 266))

# Conversion constant: miles to kilometers
MILES_TO_KM = 1.60934


@dataclass
class TransformationReport:
    """Summary metrics of a batch transformation run for auditing."""

    total_input_rows: int = 0
    clean_rows: int = 0
    rejected_rows: int = 0
    rejection_reasons: Dict[str, int] = field(default_factory=dict)
    unmapped_zone_ids_seen: Set[int] = field(default_factory=set)

    def summary(self) -> str:
        """Format a human-readable report summary."""
        lines = [
            f"Total Input Rows: {self.total_input_rows:,}",
            f"Clean Rows Landed: {self.clean_rows:,}",
            f"Total Rejected Rows: {self.rejected_rows:,}",
            "Rejection Breakdown:",
        ]
        for reason, count in sorted(self.rejection_reasons.items()):
            lines.append(f"  - {reason}: {count:,}")
        if self.unmapped_zone_ids_seen:
            lines.append(
                f"  - Unmapped Zone IDs Seen: {sorted(self.unmapped_zone_ids_seen)}"
            )
        return "\n".join(lines)


def generate_deterministic_trip_id(row: Any) -> int:
    """Generate a deterministic positive 60-bit BigInteger trip_id from a composite trip key."""
    vendor_id = getattr(row, "vendor_id", 0)
    pickup_dt = getattr(
        row, "tpep_pickup_datetime", getattr(row, "pickup_datetime", "")
    )
    dropoff_dt = getattr(
        row, "tpep_dropoff_datetime", getattr(row, "dropoff_datetime", "")
    )
    pu_id = getattr(row, "PULocationID", getattr(row, "pickup_zone_id", 0))
    do_id = getattr(row, "DOLocationID", getattr(row, "dropoff_zone_id", 0))
    fare = float(getattr(row, "fare_amount", 0.0) or 0.0)
    dist = float(
        getattr(row, "trip_distance", getattr(row, "trip_distance_miles", 0.0)) or 0.0
    )

    composite_key = (
        f"{vendor_id}|{pickup_dt}|{dropoff_dt}|{pu_id}|{do_id}|{fare:.2f}|{dist:.2f}"
    )
    # Extract 15 hex characters (60 bits) -> guaranteed positive BigInteger
    return int(hashlib.sha256(composite_key.encode("utf-8")).hexdigest()[:15], 16)


class BatchTransformer:
    """Transformer that applies validation thresholds, cleans anomalies, and derives features."""

    def __init__(
        self,
        min_duration_sec: float = 60.0,
        max_duration_sec: float = 86400.0,
        min_distance_miles: float = 0.01,
        max_distance_miles: float = 300.0,
        max_speed_mph: float = 100.0,
        min_passengers: int = 1,
        max_passengers: int = 9,
    ):
        self.min_duration_sec = min_duration_sec
        self.max_duration_sec = max_duration_sec
        self.min_distance_miles = min_distance_miles
        self.max_distance_miles = max_distance_miles
        self.max_speed_mph = max_speed_mph
        self.min_passengers = min_passengers
        self.max_passengers = max_passengers

    def transform_dataframe(
        self, df: pd.DataFrame, cab_type: str = "yellow"
    ) -> Tuple[pd.DataFrame, TransformationReport]:
        """Validate, clean, and derive features for a Pandas DataFrame of trip records."""
        report = TransformationReport(total_input_rows=len(df))
        if df.empty:
            return df, report

        df = df.copy()

        # Standardize timestamp column names across Yellow / Green TLC schema
        pickup_col = (
            "tpep_pickup_datetime"
            if "tpep_pickup_datetime" in df.columns
            else (
                "lpep_pickup_datetime"
                if "lpep_pickup_datetime" in df.columns
                else "pickup_datetime"
            )
        )
        dropoff_col = (
            "tpep_dropoff_datetime"
            if "tpep_dropoff_datetime" in df.columns
            else (
                "lpep_dropoff_datetime"
                if "lpep_dropoff_datetime" in df.columns
                else "dropoff_datetime"
            )
        )

        if pickup_col not in df.columns or dropoff_col not in df.columns:
            raise ValueError(
                f"Missing required timestamp columns '{pickup_col}' or '{dropoff_col}'."
            )

        df["pickup_datetime"] = pd.to_datetime(df[pickup_col])
        df["dropoff_datetime"] = pd.to_datetime(df[dropoff_col])

        # Standardize vendor_id, location IDs, passenger count
        df["vendor_id"] = (
            df["VendorID"].fillna(0).astype(int)
            if "VendorID" in df.columns
            else df["vendor_id"].fillna(0).astype(int)
        )
        df["pickup_zone_id"] = (
            df["PULocationID"].fillna(0).astype(int)
            if "PULocationID" in df.columns
            else df["pickup_zone_id"].fillna(0).astype(int)
        )
        df["dropoff_zone_id"] = (
            df["DOLocationID"].fillna(0).astype(int)
            if "DOLocationID" in df.columns
            else df["dropoff_zone_id"].fillna(0).astype(int)
        )
        df["passenger_count"] = (
            df["passenger_count"].fillna(1).astype(int)
            if "passenger_count" in df.columns
            else 1
        )
        df["trip_distance_miles"] = (
            df["trip_distance"].fillna(0.0).astype(float)
            if "trip_distance" in df.columns
            else df.get("trip_distance_miles", 0.0)
        )
        df["fare_amount"] = (
            df["fare_amount"].fillna(0.0).astype(float)
            if "fare_amount" in df.columns
            else 0.0
        )
        df["total_amount"] = (
            df["total_amount"].fillna(0.0).astype(float)
            if "total_amount" in df.columns
            else 0.0
        )

        # 1. Compute trip_duration_seconds
        df["trip_duration_seconds"] = (
            df["dropoff_datetime"] - df["pickup_datetime"]
        ).dt.total_seconds()

        # 2. Compute average speed (mph)
        duration_hours = df["trip_duration_seconds"] / 3600.0
        df["average_speed_mph"] = np.where(
            duration_hours > 0, df["trip_distance_miles"] / duration_hours, 0.0
        )

        # Apply filtering masks
        valid_duration = (df["trip_duration_seconds"] >= self.min_duration_sec) & (
            df["trip_duration_seconds"] <= self.max_duration_sec
        )
        valid_distance = (df["trip_distance_miles"] >= self.min_distance_miles) & (
            df["trip_distance_miles"] <= self.max_distance_miles
        )
        valid_passengers = (df["passenger_count"] >= self.min_passengers) & (
            df["passenger_count"] <= self.max_passengers
        )
        valid_speed = df["average_speed_mph"] <= self.max_speed_mph
        valid_fare = (df["fare_amount"] >= 0.0) & (df["total_amount"] >= 0.0)

        # Check Zone ID validity against 1..265
        valid_pu_zone = df["pickup_zone_id"].isin(VALID_ZONE_IDS)
        valid_do_zone = df["dropoff_zone_id"].isin(VALID_ZONE_IDS)
        valid_zones = valid_pu_zone & valid_do_zone

        # Log unmapped zone IDs seen
        unmapped_pu = set(df.loc[~valid_pu_zone, "pickup_zone_id"].unique())
        unmapped_do = set(df.loc[~valid_do_zone, "dropoff_zone_id"].unique())
        report.unmapped_zone_ids_seen = (unmapped_pu | unmapped_do) - {0}

        # Track rejection counts per filter
        report.rejection_reasons = {
            "invalid_duration": int((~valid_duration).sum()),
            "invalid_distance": int((~valid_distance).sum()),
            "invalid_passengers": int((~valid_passengers).sum()),
            "speed_anomaly": int((~valid_speed & valid_duration).sum()),
            "negative_fare": int((~valid_fare).sum()),
            "unmapped_zone_id": int((~valid_zones).sum()),
        }

        # Combined clean mask
        clean_mask = (
            valid_duration
            & valid_distance
            & valid_passengers
            & valid_speed
            & valid_fare
            & valid_zones
        )

        clean_df = df.loc[clean_mask].copy()
        report.clean_rows = len(clean_df)
        report.rejected_rows = report.total_input_rows - report.clean_rows

        if clean_df.empty:
            return clean_df, report

        # Feature Engineering on clean dataset
        clean_df["cab_type"] = cab_type.lower()
        clean_df["time_bin_15m"] = clean_df["pickup_datetime"].dt.floor("15min")
        clean_df["hour_of_day"] = clean_df["pickup_datetime"].dt.hour
        clean_df["day_of_week"] = clean_df["pickup_datetime"].dt.dayofweek
        clean_df["is_weekend"] = clean_df["day_of_week"].isin([5, 6])
        clean_df["trip_distance_km"] = (
            clean_df["trip_distance_miles"] * MILES_TO_KM
        ).round(2)

        # Generate deterministic BigInteger trip_id
        composite_str = (
            clean_df["vendor_id"].astype(str)
            + "|"
            + clean_df["pickup_datetime"].astype(str)
            + "|"
            + clean_df["dropoff_datetime"].astype(str)
            + "|"
            + clean_df["pickup_zone_id"].astype(str)
            + "|"
            + clean_df["dropoff_zone_id"].astype(str)
            + "|"
            + clean_df["fare_amount"].map("{:.2f}".format)
            + "|"
            + clean_df["trip_distance_miles"].map("{:.2f}".format)
        )

        clean_df["trip_id"] = [
            int(hashlib.sha256(s.encode("utf-8")).hexdigest()[:15], 16)
            for s in composite_str
        ]

        logger.info(
            f"Transformation completed for {report.total_input_rows:,} input rows:\n{report.summary()}"
        )
        return clean_df, report

    def transform_parquet_file(
        self, parquet_path: Path, cab_type: str = "yellow"
    ) -> Tuple[pd.DataFrame, TransformationReport]:
        """Read a Parquet file and apply batch transformation."""
        logger.info(f"Reading Parquet file for transformation: {parquet_path}")
        table = pq.read_table(parquet_path)
        df = table.to_pandas()
        return self.transform_dataframe(df, cab_type=cab_type)


def batch_insert_warehouse_trips(
    session: Any, clean_df: pd.DataFrame, batch_size: int = 5000
) -> int:
    """Bulk insert clean trip records into warehouse.trips table using ON CONFLICT DO NOTHING / bulk save."""
    if clean_df.empty:
        return 0

    from src.common.models import WarehouseTrip

    inserted_count = 0
    records = clean_df.to_dict(orient="records")

    for i in range(0, len(records), batch_size):
        chunk = records[i : i + batch_size]
        objects = []
        for r in chunk:
            objects.append(
                WarehouseTrip(
                    trip_id=r["trip_id"],
                    vendor_id=r["vendor_id"],
                    cab_type=r["cab_type"],
                    pickup_zone_id=r["pickup_zone_id"],
                    dropoff_zone_id=r["dropoff_zone_id"],
                    pickup_datetime=r["pickup_datetime"],
                    dropoff_datetime=r["dropoff_datetime"],
                    trip_duration_seconds=int(r["trip_duration_seconds"]),
                    time_bin_15m=r["time_bin_15m"],
                    day_of_week=int(r["day_of_week"]),
                    hour_of_day=int(r["hour_of_day"]),
                    is_weekend=bool(r["is_weekend"]),
                    passenger_count=int(r["passenger_count"]),
                    trip_distance_km=float(r["trip_distance_km"]),
                    fare_amount=float(r["fare_amount"]),
                    total_amount=float(r["total_amount"]),
                    source="historical",
                )
            )

        session.bulk_save_objects(objects)
        session.commit()
        inserted_count += len(chunk)

    logger.info(f"Bulk inserted {inserted_count:,} records into warehouse.trips.")
    return inserted_count
