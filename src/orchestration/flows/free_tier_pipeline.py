"""Bounded, authentic historical Kafka processing for a free portfolio deployment.

Original observation timestamps remain intact. This is scheduled processing of a
sample, not live NYC taxi telemetry. PostgreSQL is the durable replay checkpoint.
"""

import argparse
import hashlib
import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from zoneinfo import ZoneInfo

import numpy as np
import pyarrow.parquet as pq
import requests
from prefect import flow
from pydantic import ValidationError
from sqlalchemy import text

from src.common.db import get_engine, run_migrations
from src.common.kafka_utils import TOPIC_TRIP_EVENTS, get_kafka_producer
from src.extract.load_zones import load_taxi_zones_to_db
from src.features.config import get_feature_store
from src.features.materialize import materialize_features
from src.features.offline_extractor import extract_and_load_offline_features
from src.features.registry import apply_feature_definitions
from src.transform.batch_transformer import generate_deterministic_trip_id
from src.transform.schemas import TripEventPayload
from src.transform.stream_consumer import StreamConsumerService

SOURCE_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2023-01.parquet"
)
CURSOR_KEY = "free-green-2023-01-sample-v1"
MAX_SAMPLE = 10000
MAX_DATABASE_BYTES = 700 * 1024 * 1024


def require_capacity(engine):
    if engine.dialect.name != "postgresql":
        raise RuntimeError("Managed deployment requires actual PostgreSQL")
    with engine.connect() as conn:
        size = conn.execute(
            text("SELECT pg_database_size(current_database())")
        ).scalar_one()
    if size >= MAX_DATABASE_BYTES:
        raise RuntimeError(
            "Database safety budget reached (700 MiB); no data deleted and no upgrade requested"
        )
    return size


def download_source(destination: Path):
    """Bound bytes on disk as well as network time; never download during PR tests."""
    total = 0
    digest = hashlib.sha256()
    with requests.get(SOURCE_URL, stream=True, timeout=(10, 60)) as response:
        response.raise_for_status()
        with destination.open("wb") as target:
            for chunk in response.iter_content(65536):
                total += len(chunk)
                if total > 64 * 1024 * 1024:
                    raise RuntimeError(
                        "Official source exceeds the 64 MiB download budget"
                    )
                target.write(chunk)
                digest.update(chunk)
    return digest.hexdigest()


def sampled_events(path: Path, sample_size=MAX_SAMPLE):
    """Evenly sample row positions across the real file; validate before publish."""
    if not 1 <= sample_size <= MAX_SAMPLE:
        raise ValueError("sample_size must be between 1 and 10000")
    parquet = pq.ParquetFile(path)
    row_count = parquet.metadata.num_rows
    if row_count > 500000:
        raise RuntimeError("Source exceeds bounded scan budget of 500000 rows")
    selected = set(
        np.linspace(
            0, max(row_count - 1, 0), min(sample_size, row_count), dtype=int
        ).tolist()
    )
    base = 0
    valid, rejected = [], 0
    columns = [
        "VendorID",
        "lpep_pickup_datetime",
        "lpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "tip_amount",
        "total_amount",
    ]
    for batch in parquet.iter_batches(batch_size=4096, columns=columns):
        for index, row in enumerate(batch.to_pylist()):
            if base + index not in selected:
                continue
            try:
                event = TripEventPayload(
                    vendor_id=row["VendorID"],
                    cab_type="green",
                    pickup_zone_id=row["PULocationID"],
                    dropoff_zone_id=row["DOLocationID"],
                    pickup_datetime=row["lpep_pickup_datetime"]
                    .replace(tzinfo=ZoneInfo("America/New_York"))
                    .astimezone(timezone.utc),
                    dropoff_datetime=row["lpep_dropoff_datetime"]
                    .replace(tzinfo=ZoneInfo("America/New_York"))
                    .astimezone(timezone.utc),
                    passenger_count=row["passenger_count"],
                    trip_distance_miles=row["trip_distance"],
                    fare_amount=row["fare_amount"],
                    tip_amount=row["tip_amount"],
                    total_amount=row["total_amount"],
                    source="scheduled_sample",
                )
                if (
                    event.pickup_datetime.year != 2023
                    or event.pickup_datetime.month != 1
                ):
                    raise ValueError("Observation outside selected source month")
                # Exclude TLC unknown/non-geographic IDs from public forecasts.
                if event.pickup_zone_id > 263 or event.dropoff_zone_id > 263:
                    raise ValueError("No geographic zone")
                payload = event.model_dump(mode="json")
                payload["trip_id"] = generate_deterministic_trip_id(event.model_dump())
                valid.append(payload)
            except (ValidationError, ValueError, TypeError):
                rejected += 1
        base += batch.num_rows
    return valid, rejected


def existing_ids(engine, ids):
    if not ids:
        return set()
    with engine.connect() as conn:
        return set(
            conn.execute(
                text("SELECT trip_id FROM warehouse.trips WHERE trip_id = ANY(:ids)"),
                {"ids": list(ids)},
            ).scalars()
        )


def reconcile(engine):
    require_capacity(engine)
    with engine.connect() as conn:
        start, end = conn.execute(
            text(
                "SELECT min(pickup_datetime), max(pickup_datetime) FROM warehouse.trips"
            )
        ).one()
        count = conn.execute(text("SELECT count(*) FROM warehouse.trips")).scalar_one()
    if not start or not end or count > MAX_SAMPLE:
        raise RuntimeError(
            "Sample warehouse missing or above 10000-row budget; use a dedicated empty database"
        )
    start = start.replace(minute=0, second=0, microsecond=0)
    end = end.replace(minute=0, second=0, microsecond=0)
    if end - start > timedelta(days=32):
        raise RuntimeError("Feature extraction window exceeds one-month budget")
    zone_rows, corridor_rows = extract_and_load_offline_features(engine, start, end)
    store = apply_feature_definitions(store=get_feature_store())
    materialize_features(
        start_date=end - timedelta(days=13),
        end_date=end + timedelta(seconds=1),
        store=store,
    )
    return {
        "observation_start": start.isoformat(),
        "observation_end": end.isoformat(),
        "warehouse_rows": count,
        "zone_feature_rows": zone_rows,
        "corridor_feature_rows": corridor_rows,
    }


@flow(name="free-tier-scheduled-kafka-pipeline", retries=1, retry_delay_seconds=30)
def scheduled_sample_pipeline(max_events=1000, bootstrap=False):
    if not 1 <= max_events <= MAX_SAMPLE:
        raise ValueError("max_events must be between 1 and 10000")
    engine = get_engine()
    require_capacity(engine)
    if bootstrap:
        run_migrations()
    with engine.connect() as conn:
        other_rows = conn.execute(
            text(
                "SELECT count(*) FROM warehouse.trips WHERE source <> 'scheduled_sample'"
            )
        ).scalar_one()
    if other_rows:
        raise RuntimeError(
            "Free sample profile requires a dedicated database; existing warehouse data was left unchanged"
        )
    if bootstrap:
        from src.extract.authoritative_zones import fetch_authoritative_zones

        load_taxi_zones_to_db(zones=fetch_authoritative_zones(), engine=engine)
    started = datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO warehouse.pipeline_runs (run_id, job_name, status, started_at, triggered_by) VALUES (:id, 'free-tier-scheduled-kafka-pipeline', 'running', :at, 'github_actions')"
            ),
            {"id": run_id, "at": started},
        )
        conn.execute(
            text(
                "INSERT INTO warehouse.pipeline_cursors(cursor_key) VALUES (:key) ON CONFLICT DO NOTHING"
            ),
            {"key": CURSOR_KEY},
        )
    try:
        with TemporaryDirectory() as directory:
            source = Path(directory) / "source.parquet"
            digest = download_source(source)
            events, rejected = sampled_events(source)
        with engine.connect() as conn:
            cursor = conn.execute(
                text(
                    "SELECT next_index, metadata_json FROM warehouse.pipeline_cursors WHERE cursor_key=:key"
                ),
                {"key": CURSOR_KEY},
            ).one()
        prior = json.loads(cursor.metadata_json)
        if prior.get("source_sha256") not in (None, digest):
            raise RuntimeError(
                "Official source changed; review checkpoint before replay"
            )
        selection = events[cursor.next_index : cursor.next_index + max_events]
        # A distant managed DB can take several network roundtrips per record.
        # Checkpoint small confirmed batches so runner timeouts never require
        # restarting a whole bootstrap. Leave time for reconciliation as well.
        job_deadline = time.monotonic() + 15 * 60
        progress_index = cursor.next_index
        published = 0
        # Creating consumer first provisions only the two required topics.
        consumer = StreamConsumerService(
            engine=engine,
            group_id="logistics-free-scheduled-v1",
            topics=[TOPIC_TRIP_EVENTS],
            enable_feature_push=False,
        )
        producer = get_kafka_producer()
        try:
            for begin in range(0, len(selection), 100):
                if time.monotonic() >= job_deadline:
                    break
                batch = selection[begin : begin + 100]
                for event in batch:
                    producer.send(
                        TOPIC_TRIP_EVENTS, key=str(event["trip_id"]), value=event
                    ).get(timeout=30)
                    time.sleep(0.02)
                expected = {event["trip_id"] for event in batch}
                deadline = time.monotonic() + 180
                while expected - existing_ids(engine, expected):
                    if time.monotonic() >= deadline:
                        raise RuntimeError(
                            "Acknowledged batch not persisted; its checkpoint remains unchanged"
                        )
                    result = consumer.consume_batch(
                        max_messages=100, timeout_seconds=10
                    )
                    if result["deadlettered"]:
                        raise RuntimeError(
                            "Validated sample unexpectedly deadlettered; inspect DLQ before advancing"
                        )
                next_index = progress_index + len(batch)
                checkpoint = {
                    **prior,
                    "source": SOURCE_URL,
                    "source_sha256": digest,
                    "sample_size": len(events),
                    "next_index": next_index,
                }
                with engine.begin() as conn:
                    saved = conn.execute(
                        text(
                            "UPDATE warehouse.pipeline_cursors SET next_index=:next, metadata_json=:metadata WHERE cursor_key=:key AND next_index=:previous"
                        ),
                        {
                            "next": next_index,
                            "previous": progress_index,
                            "key": CURSOR_KEY,
                            "metadata": json.dumps(checkpoint),
                        },
                    )
                    if saved.rowcount != 1:
                        raise RuntimeError(
                            "Concurrent cursor update detected; retry with durable progress"
                        )
                progress_index = next_index
                published += len(batch)
        finally:
            consumer.close()
            producer.close()
        summary = {**prior, **reconcile(engine)}
        if bootstrap:
            summary["zone_reference"] = "official_tlc_polygon_centroids_v1"
        summary.update(
            source=SOURCE_URL,
            source_sha256=digest,
            sample_size=len(events),
            rejected_source_rows=rejected,
            next_index=progress_index,
            published_events=published,
            remaining_sample_events=len(events) - progress_index,
            feature_checkpoint_index=progress_index,
            mode="scheduled_historical_sample",
        )
        with engine.begin() as conn:
            updated = conn.execute(
                text(
                    "UPDATE warehouse.pipeline_cursors SET next_index=:index, metadata_json=:data, updated_at=now() WHERE cursor_key=:key AND next_index=:previous"
                ),
                {
                    "index": summary["next_index"],
                    "data": json.dumps(summary),
                    "key": CURSOR_KEY,
                    "previous": progress_index,
                },
            )
            if updated.rowcount != 1:
                raise RuntimeError(
                    "Another job advanced the cursor; retry with the new checkpoint"
                )
            conn.execute(
                text(
                    "UPDATE warehouse.pipeline_runs SET status='completed', finished_at=now(), records_processed=:count WHERE run_id=:id"
                ),
                {"id": run_id, "count": published},
            )
        return summary
    except Exception as exc:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE warehouse.pipeline_runs SET status='failed', finished_at=now(), error_message=:error WHERE run_id=:id"
                ),
                {"id": run_id, "error": type(exc).__name__},
            )
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", action="store_true")
    parser.add_argument("--max-events", type=int, default=1000)
    args = parser.parse_args()
    print(
        json.dumps(
            scheduled_sample_pipeline(
                max_events=args.max_events, bootstrap=args.bootstrap
            ),
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
