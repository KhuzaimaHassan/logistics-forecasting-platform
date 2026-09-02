"""Historical TLC Trip Replay Producer for Redpanda/Kafka.

Reads historical NYC TLC trip records from PostgreSQL warehouse.trips (or raw Parquet)
and publishes them onto the Redpanda 'trip.events' topic, simulating real-time order flow
with configurable time acceleration and partition keying on pickup_zone_id.
"""

import argparse
import logging
import signal
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, Optional

import pandas as pd
from kafka import KafkaProducer
from sqlalchemy import text

from src.common.config import get_settings
from src.common.db import get_engine
from src.common.kafka_utils import (
    TOPIC_TRIP_EVENTS,
    ensure_topics_exist,
    get_kafka_producer,
    json_serializer,
)

logger = logging.getLogger(__name__)


class HistoricalReplayProducer:
    """Streams historical trip events into Redpanda to simulate live order flow."""

    def __init__(
        self,
        broker: Optional[str] = None,
        topic: str = TOPIC_TRIP_EVENTS,
        speed_multiplier: float = 1.0,
        rewrite_timestamps: bool = True,
        producer: Optional[KafkaProducer] = None,
    ) -> None:
        self.broker = broker or get_settings().redpanda_broker
        self.topic = topic
        self.speed_multiplier = float(speed_multiplier)
        self.rewrite_timestamps = rewrite_timestamps
        self._running = True
        self._producer = producer

        # Metrics
        self.records_read = 0
        self.records_published = 0
        self.publish_errors = 0

    @property
    def producer(self) -> KafkaProducer:
        """Lazy-initialize KafkaProducer if not provided."""
        if self._producer is None:
            self._producer = get_kafka_producer(
                broker=self.broker,
                value_serializer=json_serializer,
                key_serializer=lambda k: (
                    str(k).encode("utf-8") if k is not None else None
                ),
            )
        return self._producer

    def stop(self) -> None:
        """Signal producer to stop cleanly."""
        logger.info("Stopping HistoricalReplayProducer...")
        self._running = False

    def fetch_trips_from_db(
        self,
        limit: Optional[int] = None,
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None,
        batch_size: int = 1000,
    ) -> Iterator[Dict[str, Any]]:
        """Fetch trips from warehouse.trips in chronological order."""
        engine = get_engine()
        where_clauses = ["1=1"]
        params: Dict[str, Any] = {"batch_size": batch_size}

        if start_datetime:
            where_clauses.append("pickup_datetime >= :start_datetime")
            params["start_datetime"] = start_datetime
        if end_datetime:
            where_clauses.append("pickup_datetime <= :end_datetime")
            params["end_datetime"] = end_datetime

        where_sql = " AND ".join(where_clauses)
        limit_sql = f"LIMIT {int(limit)}" if limit else ""

        query = text(f"""
            SELECT
                trip_id, vendor_id, cab_type,
                pickup_zone_id, dropoff_zone_id,
                pickup_datetime, dropoff_datetime,
                trip_duration_seconds, passenger_count,
                trip_distance_km, fare_amount, tip_amount, total_amount
            FROM warehouse.trips
            WHERE {where_sql}
            ORDER BY pickup_datetime ASC
            {limit_sql}
        """)

        with engine.connect() as conn:
            result = conn.execution_options(stream_results=True).execute(query, params)
            for row in result.mappings():
                yield dict(row)

    def fetch_trips_from_parquet(
        self,
        parquet_path: str,
        limit: Optional[int] = None,
    ) -> Iterator[Dict[str, Any]]:
        """Fetch trips from a local Parquet file in chronological order."""
        df = pd.read_parquet(parquet_path)
        # Normalize column names if raw TLC parquet
        rename_map = {
            "tpep_pickup_datetime": "pickup_datetime",
            "tpep_dropoff_datetime": "dropoff_datetime",
            "lpep_pickup_datetime": "pickup_datetime",
            "lpep_dropoff_datetime": "dropoff_datetime",
            "PULocationID": "pickup_zone_id",
            "DOLocationID": "dropoff_zone_id",
            "trip_distance": "trip_distance_km",
            "VendorID": "vendor_id",
        }
        for old_col, new_col in rename_map.items():
            if old_col in df.columns and new_col not in df.columns:
                df[new_col] = df[old_col]

        if "pickup_datetime" in df.columns:
            df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], utc=True)
            df = df.sort_values("pickup_datetime")

        if limit:
            df = df.head(limit)

        for idx, row in df.iterrows():
            record = row.to_dict()
            if "trip_id" not in record or pd.isna(record["trip_id"]):
                record["trip_id"] = int(idx) + 1
            if "cab_type" not in record:
                record["cab_type"] = "yellow"
            yield record

    def prepare_trip_payload(
        self,
        record: Dict[str, Any],
        time_offset_seconds: float = 0.0,
    ) -> Dict[str, Any]:
        """Format and adjust timestamps for the streaming payload."""
        pickup_dt = record.get("pickup_datetime")
        dropoff_dt = record.get("dropoff_datetime")

        if isinstance(pickup_dt, str):
            pickup_dt = datetime.fromisoformat(pickup_dt.replace("Z", "+00:00"))
        if isinstance(dropoff_dt, str):
            dropoff_dt = datetime.fromisoformat(dropoff_dt.replace("Z", "+00:00"))

        if self.rewrite_timestamps and pickup_dt:
            # Adjust by simulated offset
            adjusted_pickup = pickup_dt + pd.Timedelta(seconds=time_offset_seconds)
            adjusted_dropoff = (
                dropoff_dt + pd.Timedelta(seconds=time_offset_seconds)
                if dropoff_dt
                else adjusted_pickup
            )
        else:
            adjusted_pickup = pickup_dt
            adjusted_dropoff = dropoff_dt

        duration_sec = record.get("trip_duration_seconds")
        if duration_sec is None and adjusted_pickup and adjusted_dropoff:
            duration_sec = int((adjusted_dropoff - adjusted_pickup).total_seconds())

        payload = {
            "trip_id": int(record.get("trip_id", 0)),
            "vendor_id": (
                int(record["vendor_id"])
                if record.get("vendor_id") is not None
                and not pd.isna(record["vendor_id"])
                else None
            ),
            "cab_type": str(record.get("cab_type", "yellow")),
            "pickup_zone_id": (
                int(record["pickup_zone_id"])
                if record.get("pickup_zone_id") is not None
                and not pd.isna(record["pickup_zone_id"])
                else None
            ),
            "dropoff_zone_id": (
                int(record["dropoff_zone_id"])
                if record.get("dropoff_zone_id") is not None
                and not pd.isna(record["dropoff_zone_id"])
                else None
            ),
            "pickup_datetime": (
                adjusted_pickup.isoformat()
                if hasattr(adjusted_pickup, "isoformat")
                else str(adjusted_pickup)
            ),
            "dropoff_datetime": (
                adjusted_dropoff.isoformat()
                if hasattr(adjusted_dropoff, "isoformat")
                else str(adjusted_dropoff)
            ),
            "trip_duration_seconds": (
                int(duration_sec) if duration_sec is not None else 0
            ),
            "passenger_count": (
                int(record["passenger_count"])
                if record.get("passenger_count") is not None
                and not pd.isna(record["passenger_count"])
                else 1
            ),
            "trip_distance_km": (
                float(record["trip_distance_km"])
                if record.get("trip_distance_km") is not None
                and not pd.isna(record["trip_distance_km"])
                else 0.0
            ),
            "fare_amount": (
                float(record["fare_amount"])
                if record.get("fare_amount") is not None
                and not pd.isna(record["fare_amount"])
                else 0.0
            ),
            "tip_amount": (
                float(record["tip_amount"])
                if record.get("tip_amount") is not None
                and not pd.isna(record["tip_amount"])
                else 0.0
            ),
            "total_amount": (
                float(record["total_amount"])
                if record.get("total_amount") is not None
                and not pd.isna(record["total_amount"])
                else 0.0
            ),
            "source": "replay",
            "replayed_at": datetime.now(timezone.utc).isoformat(),
        }
        return payload

    def _apply_pacing(
        self,
        prev_trip_ts: Optional[datetime],
        current_trip_ts: Optional[datetime],
    ) -> None:
        """Apply pacing delay between consecutive historical trips."""
        if self.speed_multiplier <= 0 or not prev_trip_ts or not current_trip_ts:
            return

        delta_orig = (current_trip_ts - prev_trip_ts).total_seconds()
        if delta_orig > 0:
            sleep_time = min(delta_orig / self.speed_multiplier, 2.0)
            if sleep_time > 0.001:
                time.sleep(sleep_time)

    def replay_stream(
        self,
        trips_iterator: Iterator[Dict[str, Any]],
        flush_interval: int = 100,
    ) -> Dict[str, Any]:
        """Stream trip records onto the Redpanda topic with pacing."""
        first_trip_ts: Optional[datetime] = None
        replay_start_wall: float = time.time()
        time_offset_seconds: float = 0.0
        prev_trip_ts: Optional[datetime] = None

        logger.info(
            "Starting replay stream to topic '%s' (speed_multiplier=%.1fx)...",
            self.topic,
            self.speed_multiplier,
        )

        for record in trips_iterator:
            if not self._running:
                logger.info("Replay stream stopped by signal.")
                break

            self.records_read += 1
            pickup_dt = record.get("pickup_datetime")
            if isinstance(pickup_dt, str):
                pickup_dt = datetime.fromisoformat(pickup_dt.replace("Z", "+00:00"))

            if first_trip_ts is None and pickup_dt:
                first_trip_ts = pickup_dt
                if self.rewrite_timestamps:
                    now_utc = datetime.now(timezone.utc)
                    time_offset_seconds = (now_utc - first_trip_ts).total_seconds()

            self._apply_pacing(prev_trip_ts, pickup_dt)
            prev_trip_ts = pickup_dt

            payload = self.prepare_trip_payload(
                record, time_offset_seconds=time_offset_seconds
            )
            partition_key = payload.get("pickup_zone_id")

            try:
                self.producer.send(
                    topic=self.topic,
                    key=partition_key,
                    value=payload,
                )
                self.records_published += 1
            except Exception as exc:
                self.publish_errors += 1
                logger.error(
                    "Failed to publish record %s: %s", payload.get("trip_id"), exc
                )

            if self.records_published % flush_interval == 0:
                self.producer.flush()
                logger.debug(
                    "Replayed %d records (%d errors)",
                    self.records_published,
                    self.publish_errors,
                )

        self.producer.flush()

        duration = time.time() - replay_start_wall
        logger.info(
            "Replay complete: published %d/%d records in %.2fs (%d errors)",
            self.records_published,
            self.records_read,
            duration,
            self.publish_errors,
        )

        return {
            "records_read": self.records_read,
            "records_published": self.records_published,
            "publish_errors": self.publish_errors,
            "duration_seconds": round(duration, 3),
        }

    def close(self) -> None:
        """Flush and close producer connection."""
        if self._producer:
            try:
                self._producer.flush(timeout=5)
                self._producer.close(timeout=5)
            except Exception as e:
                logger.warning("Error closing KafkaProducer: %s", e)
            finally:
                self._producer = None


def main() -> None:
    """CLI entrypoint for running historical trip replay producer."""
    parser = argparse.ArgumentParser(
        description="NYC TLC Historical Trip Replay Producer"
    )
    parser.add_argument(
        "--broker", type=str, default=None, help="Redpanda broker address (host:port)"
    )
    parser.add_argument(
        "--topic", type=str, default=TOPIC_TRIP_EVENTS, help="Destination topic"
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="Max number of records to replay"
    )
    parser.add_argument(
        "--speed", type=float, default=60.0, help="Speed multiplier (0=instant burst)"
    )
    parser.add_argument(
        "--parquet-path",
        type=str,
        default=None,
        help="Path to local Parquet file (if not DB)",
    )
    parser.add_argument(
        "--ensure-topics",
        action="store_true",
        help="Ensure Redpanda topics exist before replay",
    )

    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    if args.ensure_topics:
        logger.info("Ensuring Redpanda topics exist...")
        ensure_topics_exist(broker=args.broker)

    producer = HistoricalReplayProducer(
        broker=args.broker,
        topic=args.topic,
        speed_multiplier=args.speed,
    )

    def handle_signal(sig: int, frame: Any) -> None:
        producer.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        if args.parquet_path:
            logger.info("Reading trips from Parquet file: %s", args.parquet_path)
            iterator = producer.fetch_trips_from_parquet(
                args.parquet_path, limit=args.limit
            )
        else:
            logger.info(
                "Reading trips from PostgreSQL warehouse.trips (limit=%s)...",
                args.limit,
            )
            iterator = producer.fetch_trips_from_db(limit=args.limit)

        summary = producer.replay_stream(iterator)
        print(f"\nReplay Summary: {summary}")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
