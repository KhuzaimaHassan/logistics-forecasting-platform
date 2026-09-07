"""End-to-End Real-Time Pipeline Integration & CI Smoke Verification (M4-5).

Verifies the complete real-time streaming pipeline under live infrastructure:
1. Stage 1: Infrastructure, DB migrations, topic provisioning, and Feast SQL registry apply.
2. Stage 2: Historical TLC Replay Producer bursts 100 trips to 'trip.events'.
3. Stage 3: Live External Feed Poller bursts NYC traffic, transit alerts, and weather snapshots.
4. Stage 4: Real-time Stream Consumer ingests records to PostgreSQL with zero deadlettering on happy path.
5. Stage 5: Feast online store receives pushed features and returns valid vectors with sub-second latency.
6. Stage 6: Dead-letter observability: poison message injection and quarantine verification on 'trip.events.deadletter'.
7. Stage 7: Best-effort push outage resilience: forced push failure confirms offset commit continuity,
   verifies online store un-pushed state, and runs Prefect reconciliation flow to catch up Redis from Postgres.
"""

import json
import logging
import os
import time
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List
from unittest.mock import MagicMock
from uuid import uuid4

import redis
from alembic.config import Config
from kafka import OffsetAndMetadata
from sqlalchemy import text
from sqlalchemy.engine import Engine

from alembic import command
from src.common.config import get_settings
from src.common.db import get_engine
from src.common.kafka_utils import (
    TOPIC_TRAFFIC_SNAPSHOTS,
    TOPIC_TRANSIT_POSITIONS,
    TOPIC_TRIP_DEADLETTER,
    TOPIC_TRIP_EVENTS,
    TOPIC_WEATHER_SNAPSHOTS,
    ensure_topics_exist,
    get_admin_client,
    get_kafka_consumer,
    get_kafka_producer,
    json_deserializer,
)
from src.extract.live_feed_producers import LiveFeedPollerManager
from src.extract.replay_producer import HistoricalReplayProducer
from src.features.client import FeastOnlineClient
from src.features.config import ensure_feast_schema, get_feature_store
from src.features.registry import apply_feature_definitions
from src.orchestration.flows.realtime_reconciliation_flow import (
    realtime_reconciliation_flow,
)
from src.transform.stream_consumer import StreamConsumerService

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("verify_streaming_live_smoke")


def run_alembic_migrations(db_url: str) -> None:
    """Run Alembic upgrade head to apply all database migrations."""
    print("Applying Alembic migrations to head...", flush=True)
    alembic_cfg = Config("alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url", db_url)
    command.upgrade(alembic_cfg, "head")
    print("Alembic migrations applied successfully.", flush=True)


def seed_taxi_zones_if_empty(engine: Engine) -> None:
    """Ensure reference taxi zones are present in warehouse.taxi_zones."""
    with engine.connect() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM warehouse.taxi_zones;")
        ).scalar()
        if count and count > 0:
            print(
                f"warehouse.taxi_zones already populated ({count} zones).", flush=True
            )
            return

    print("Seeding reference taxi zones into warehouse.taxi_zones...", flush=True)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO warehouse.taxi_zones (zone_id, borough, zone_name, service_zone, centroid_lat, centroid_lon)
            VALUES
                (142, 'Manhattan', 'Lincoln Square East', 'Yellow Zone', 40.7707, -73.9818),
                (161, 'Manhattan', 'Midtown Center', 'Yellow Zone', 40.7570, -73.9785),
                (236, 'Manhattan', 'Upper East Side North', 'Yellow Zone', 40.7760, -73.9542),
                (264, 'Unknown', 'NV', 'N/A', 40.7, -74.0),
                (265, 'Unknown', 'NA', 'N/A', 40.7, -74.0)
            ON CONFLICT (zone_id) DO NOTHING;
            """))


def verify_stage1_bootstrap(db_url: str, broker: str, redis_url: str) -> Any:
    """Stage 1: Bootstrap DB migrations, topics, Feast registry, and Redis."""
    print("\n" + "=" * 80)
    print("STAGE 1: BOOTSTRAP INFRASTRUCTURE, REGISTRY & TOPICS")
    print("=" * 80)

    run_alembic_migrations(db_url)
    engine = get_engine()
    seed_taxi_zones_if_empty(engine)

    ensure_topics_exist(broker=broker)
    admin = get_admin_client(broker=broker)
    topics = set(admin.list_topics())
    admin.close()

    required_topics = [
        TOPIC_TRIP_EVENTS,
        TOPIC_TRAFFIC_SNAPSHOTS,
        TOPIC_TRANSIT_POSITIONS,
        TOPIC_WEATHER_SNAPSHOTS,
        TOPIC_TRIP_DEADLETTER,
    ]
    for topic in required_topics:
        assert topic in topics, f"Missing topic on Redpanda broker: {topic}"
    print(f"Verified all 5 required Redpanda topics present: {sorted(required_topics)}")

    r = redis.Redis.from_url(redis_url)
    assert r.ping(), "Redis ping check failed!"
    print("Redis connection verified (PING -> PONG).")

    ensure_feast_schema()
    store = get_feature_store()
    apply_feature_definitions(store=store, include_push=True)
    views = [v.name for v in store.list_feature_views()]
    assert "zone_demand_features_push" in views, "zone_demand_features_push missing"
    assert (
        "corridor_duration_features_push" in views
    ), "corridor_duration_features_push missing"
    print(f"Feast SQL registry initialized with push views: {views}")

    return store, engine


def build_burst_trip_records(count: int = 100) -> List[Dict[str, Any]]:
    """Construct valid trip dictionaries within sliding 15m/1h window."""
    now_utc = datetime.now(timezone.utc)
    trips: List[Dict[str, Any]] = []
    zone_pairs = [(161, 236), (236, 161)]

    for i in range(count):
        pu_zone, do_zone = zone_pairs[i % len(zone_pairs)]
        # Timestamps spread over past 10 minutes
        offset_seconds = 600 - int(i * (500 / count))
        pu_dt = now_utc - timedelta(seconds=offset_seconds)
        duration = 300 + (i % 20) * 15
        do_dt = pu_dt + timedelta(seconds=duration)

        trips.append(
            {
                "trip_id": 800000 + i,
                "vendor_id": 1 if i % 2 == 0 else 2,
                "cab_type": "yellow",
                "pickup_zone_id": pu_zone,
                "dropoff_zone_id": do_zone,
                "pickup_datetime": pu_dt.isoformat(),
                "dropoff_datetime": do_dt.isoformat(),
                "trip_duration_seconds": duration,
                "passenger_count": 1 + (i % 3),
                "trip_distance_km": round(2.0 + (i % 10) * 0.4, 2),
                "fare_amount": round(12.0 + (i % 15) * 1.2, 2),
                "tip_amount": 2.50,
                "total_amount": round(15.50 + (i % 15) * 1.2, 2),
                "source": "replay",
            }
        )
    return trips


def verify_stage2_replay_burst(broker: str, trips: List[Dict[str, Any]]) -> int:
    """Stage 2: Historical TLC Replay Producer bursts 100 trips."""
    print("\n" + "=" * 80)
    print("STAGE 2: HISTORICAL REPLAY PRODUCER BURST (100 TRIPS)")
    print("=" * 80)

    replay_producer = HistoricalReplayProducer(
        broker=broker,
        topic=TOPIC_TRIP_EVENTS,
        speed_multiplier=0.0,
        rewrite_timestamps=False,
    )
    res = replay_producer.replay_stream(iter(trips))
    replay_producer.close()

    published = res.get("records_published", 0)
    assert published == len(
        trips
    ), f"Expected {len(trips)} trips published, got {published}"
    print(f"HistoricalReplayProducer successfully published {published} trips.")
    return published


def verify_stage3_external_feeds_burst(broker: str) -> int:
    """Stage 3: Live External Feed Poller bursts traffic, transit, and weather."""
    print("\n" + "=" * 80)
    print("STAGE 3: LIVE EXTERNAL FEED PRODUCER BURST")
    print("=" * 80)

    poller = LiveFeedPollerManager(broker=broker)
    poll_results = poller.poll_all_once()
    poller.close()

    traffic_cnt = poll_results["traffic"]["records_published"]
    transit_cnt = poll_results["transit"]["records_published"]
    weather_cnt = poll_results["weather"]["records_published"]
    total_snapshots = traffic_cnt + transit_cnt + weather_cnt

    print(
        f"LiveFeedPollerManager published {total_snapshots} external snapshots "
        f"(traffic={traffic_cnt}, transit={transit_cnt}, weather={weather_cnt})."
    )
    assert total_snapshots > 0, "Expected at least one external snapshot published"
    return total_snapshots


def verify_stage4_consumer_ingestion(
    engine: Engine,
    consumer: StreamConsumerService,
    expected_trips: int,
    expected_snapshots: int,
    init_trips: int,
) -> None:
    """Stage 4: Real-Time Stream Consumer ingests records to PostgreSQL."""
    print("\n" + "=" * 80)
    print("STAGE 4: STREAM CONSUMER PROCESSING & POSTGRESQL INGESTION")
    print("=" * 80)

    total_expected = expected_trips + expected_snapshots
    accumulated = {
        "processed": 0,
        "deadlettered": 0,
        "trips": 0,
        "traffic": 0,
        "transit": 0,
        "weather": 0,
    }
    timeout_seconds = 25.0
    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        remaining = total_expected - (
            accumulated["processed"] + accumulated["deadlettered"]
        )
        if remaining <= 0:
            break
        batch_res = consumer.consume_batch(max_messages=remaining, timeout_seconds=2.0)
        for k in accumulated:
            accumulated[k] += batch_res.get(k, 0)
        if (
            accumulated["trips"] >= expected_trips
            and (
                accumulated["traffic"] + accumulated["transit"] + accumulated["weather"]
            )
            >= expected_snapshots
        ):
            break

    print(f"Consumer batch ingestion results: {accumulated}")

    assert (
        accumulated["trips"] == expected_trips
    ), f"Expected {expected_trips} trips, got {accumulated['trips']}"
    assert (
        accumulated["deadlettered"] == 0
    ), f"Expected 0 deadlettered on happy path, got {accumulated['deadlettered']}"
    assert accumulated["processed"] >= total_expected

    with engine.connect() as conn:
        new_trips = (
            conn.execute(
                text("SELECT COUNT(*) FROM warehouse.trips WHERE source = 'replay';")
            ).scalar()
            or 0
        )
        assert new_trips >= init_trips + expected_trips, (
            f"Postgres warehouse.trips count did not increase by {expected_trips} "
            f"(before: {init_trips}, after: {new_trips})"
        )
        traffic_rows = (
            conn.execute(
                text("SELECT COUNT(*) FROM warehouse.traffic_snapshots;")
            ).scalar()
            or 0
        )
        weather_rows = (
            conn.execute(
                text("SELECT COUNT(*) FROM warehouse.weather_snapshots;")
            ).scalar()
            or 0
        )
        transit_rows = (
            conn.execute(
                text("SELECT COUNT(*) FROM warehouse.transit_snapshots;")
            ).scalar()
            or 0
        )

    assert traffic_rows > 0, "No rows in warehouse.traffic_snapshots"
    assert weather_rows > 0, "No rows in warehouse.weather_snapshots"
    assert transit_rows > 0, "No rows in warehouse.transit_snapshots"

    print(
        f"PostgreSQL Ingestion Verified: warehouse.trips delta=+{new_trips - init_trips}, "
        f"traffic={traffic_rows}, weather={weather_rows}, transit={transit_rows}."
    )


def verify_stage5_feast_online_push(store: Any) -> None:
    """Stage 5: Query Feast online store for real-time pushed feature vectors."""
    print("\n" + "=" * 80)
    print("STAGE 5: FEAST ONLINE STORE FEATURE PUSH VERIFICATION (ADR-018)")
    print("=" * 80)

    client = FeastOnlineClient(store=store)

    t0 = time.perf_counter()
    zone_features = client.get_zone_demand_features([161, 236], use_push_features=True)
    latency_ms = (time.perf_counter() - t0) * 1000.0

    print(
        f"Online retrieval completed in {latency_ms:.2f}ms (sub-second requirement: < 1000ms)."
    )
    assert (
        latency_ms < 1000.0
    ), f"Online retrieval latency exceeded 1000ms: {latency_ms:.2f}ms"
    assert (
        len(zone_features) == 2
    ), f"Expected 2 feature vectors, got {len(zone_features)}"

    z161 = zone_features[0]
    print(f"Zone 161 Pushed Online Features: {asdict(z161)}")
    assert z161.zone_id == 161
    assert z161.pickup_count_last_15m > 0, "pickup_count_last_15m should be > 0"
    assert z161.pickup_count_last_1h > 0, "pickup_count_last_1h should be > 0"
    assert z161.avg_temp_last_1h is not None, "avg_temp_last_1h should be non-null"

    # Query Corridor Duration
    t_corr0 = time.perf_counter()
    corridor_features = client.get_corridor_duration_features(
        ["161_236"], use_push_features=True
    )
    corr_latency_ms = (time.perf_counter() - t_corr0) * 1000.0
    print(f"Corridor retrieval completed in {corr_latency_ms:.2f}ms.")
    assert (
        corr_latency_ms < 1000.0
    ), f"Corridor retrieval latency exceeded 1000ms: {corr_latency_ms:.2f}ms"
    assert len(corridor_features) == 1
    c161_236 = corridor_features[0]
    print(f"Corridor 161_236 Pushed Online Features: {asdict(c161_236)}")
    assert (
        c161_236.avg_duration_last_15m is not None
        and c161_236.avg_duration_last_15m > 0
    )
    print("Feast Online Store Feature Push verified with sub-second retrieval.")


def verify_stage6_deadletter_quarantine(
    broker: str, consumer: StreamConsumerService
) -> None:
    """Stage 6: Verify poison pill quarantine to 'trip.events.deadletter'."""
    print("\n" + "=" * 80)
    print("STAGE 6: DEAD-LETTER OBSERVABILITY & QUARANTINE VERIFICATION")
    print("=" * 80)

    producer = get_kafka_producer(broker=broker)
    poison_payload = {
        "vendor_id": 1,
        "cab_type": "yellow",
        "pickup_zone_id": 999999,  # Deliberate invalid taxi zone ID
        "dropoff_zone_id": 236,
        "pickup_datetime": datetime.now(timezone.utc).isoformat(),
        "dropoff_datetime": (
            datetime.now(timezone.utc) + timedelta(minutes=10)
        ).isoformat(),
        "trip_duration_seconds": 600,
        "trip_distance_km": 2.5,
        "fare_amount": 12.0,
        "tip_amount": 2.0,
        "total_amount": 14.0,
    }
    producer.send(TOPIC_TRIP_EVENTS, value=poison_payload)
    producer.flush()
    producer.close()
    print("Published poison message with invalid pickup_zone_id=999999.")

    dl_res = consumer.consume_batch(max_messages=1, timeout_seconds=5.0)
    print(f"Consumer poison ingestion result: {dl_res}")
    assert (
        dl_res["deadlettered"] >= 1
    ), f"Expected at least 1 deadlettered message, got {dl_res['deadlettered']}"

    # Read back from deadletter topic
    dl_consumer = get_kafka_consumer(
        TOPIC_TRIP_DEADLETTER,
        broker=broker,
        group_id=f"verify-dl-smoke-{uuid4().hex[:8]}",
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
        value_deserializer=json_deserializer,
    )
    quarantined = []
    for msg in dl_consumer:
        val = msg.value
        if (
            isinstance(val, dict)
            and isinstance(val.get("raw_payload"), dict)
            and val["raw_payload"].get("pickup_zone_id") == 999999
        ):
            quarantined.append(val)
            break
    dl_consumer.close()

    assert quarantined, "Quarantined poison pill message not found on deadletter topic!"
    q_record = quarantined[0]
    print(f"Quarantined Message Record:\n{json.dumps(q_record, indent=2)}")
    assert "999999" in q_record["error_reason"]
    assert q_record["topic"] == TOPIC_TRIP_EVENTS
    assert q_record["failed_at"] is not None
    print("Dead-letter observability and quarantine isolation verified.")


def verify_stage7_push_resilience_and_reconciliation(
    engine: Engine,
    broker: str,
    store: Any,
    consumer: StreamConsumerService,
) -> None:
    """Stage 7: Push outage resilience and Prefect reconciliation catch-up."""
    print("\n" + "=" * 80)
    print("STAGE 7: PUSH OUTAGE RESILIENCE & PREFECT RECONCILIATION CATCH-UP")
    print("=" * 80)

    # 1. Monkeypatch store.push to simulate Redis / network failure
    real_push = store.push
    store.push = MagicMock(
        side_effect=RuntimeError("Simulated Redis online store push failure")
    )

    # Pre-burst baseline count of warehouse.trips for zone 142
    with engine.connect() as conn:
        init_142 = (
            conn.execute(
                text("SELECT COUNT(*) FROM warehouse.trips WHERE pickup_zone_id = 142;")
            ).scalar()
            or 0
        )

    # 2. Build 10 test trips specifically for zone 142 -> 236 within target observation window
    now_utc = datetime.now(timezone.utc)
    target_obs_hour = (now_utc + timedelta(hours=1)).replace(
        minute=0, second=0, microsecond=0
    )
    resilience_trips = [
        {
            "trip_id": 850000 + i,
            "vendor_id": 1,
            "cab_type": "yellow",
            "pickup_zone_id": 142,
            "dropoff_zone_id": 236,
            "pickup_datetime": (
                target_obs_hour - timedelta(minutes=30 - i)
            ).isoformat(),
            "dropoff_datetime": (
                target_obs_hour - timedelta(minutes=20 - i)
            ).isoformat(),
            "trip_duration_seconds": 600,
            "passenger_count": 1,
            "trip_distance_km": 3.0,
            "fare_amount": 14.0,
            "tip_amount": 2.0,
            "total_amount": 16.0,
            "source": "replay",
        }
        for i in range(10)
    ]

    replay_producer = HistoricalReplayProducer(
        broker=broker,
        topic=TOPIC_TRIP_EVENTS,
        speed_multiplier=0.0,
        rewrite_timestamps=False,
    )
    replay_producer.replay_stream(iter(resilience_trips))
    replay_producer.close()
    print("Published 10 trips during simulated push outage.")

    accumulated_res = {"processed": 0, "deadlettered": 0, "trips": 0}
    start_time = time.time()
    while time.time() - start_time < 15.0 and accumulated_res["processed"] < 10:
        res_batch = consumer.consume_batch(
            max_messages=10 - accumulated_res["processed"], timeout_seconds=2.0
        )
        for k in ["processed", "deadlettered", "trips"]:
            accumulated_res[k] += res_batch.get(k, 0)

    print(f"Consumer batch during outage: {accumulated_res}")

    # Consumer commits offset and continues; zero deadlettering
    assert accumulated_res["processed"] == 10
    assert (
        accumulated_res["deadlettered"] == 0
    ), "Push failure should be best-effort and must not dead-letter valid trips!"
    assert store.push.called, "store.push should have been attempted"

    # Verify trips persisted in Postgres
    with engine.connect() as conn:
        persisted_142 = (
            conn.execute(
                text("SELECT COUNT(*) FROM warehouse.trips WHERE pickup_zone_id = 142;")
            ).scalar()
            or 0
        )
    assert (
        persisted_142 - init_142 == 10
    ), f"Expected 10 new persisted trips in warehouse.trips for zone 142, got {persisted_142 - init_142}"

    # 3. Restore real push and verify online store does NOT reflect failed push
    store.push = real_push
    client = FeastOnlineClient(store=store)
    # Zone 142 push view should not have received these 10 trips via push
    pre_reconcile = client.get_zone_demand_features([142], use_push_features=True)
    print(f"Pre-reconciliation Zone 142 online features: {asdict(pre_reconcile[0])}")
    assert (
        pre_reconcile[0].pickup_count_last_15m is None
        or pre_reconcile[0].pickup_count_last_15m == 0
    ), f"Expected Redis not to reflect failed push (count is None or 0), got {pre_reconcile[0].pickup_count_last_15m}"

    # 4. Run Prefect realtime_reconciliation_flow to catch up Redis from Postgres
    print("Executing Prefect realtime_reconciliation_flow...")
    flow_res = realtime_reconciliation_flow(
        lookback_hours=3,
        lookback_days=1,
        end_datetime=target_obs_hour,
        engine=engine,
        store=store,
    )
    print(f"Reconciliation flow results: {flow_res}")
    assert flow_res["status"] == "success"
    assert flow_res["materialization"]["status"] == "success"

    # 5. Verify online store caught up after reconciliation
    post_reconcile = client.get_zone_demand_features([142], use_push_features=False)
    print(f"Post-reconciliation Zone 142 online features: {asdict(post_reconcile[0])}")
    assert post_reconcile[0].zone_id == 142
    assert (
        post_reconcile[0].pickup_count_last_1h is not None
        and post_reconcile[0].pickup_count_last_1h > 0
    ), f"Expected positive pickup count after reconciliation, got {asdict(post_reconcile[0])}"
    print(
        "Push outage resilience and Prefect reconciliation catch-up verified (100% PROVEN)."
    )


def main() -> None:
    settings = get_settings()
    broker = os.getenv("REDPANDA_BROKER", settings.redpanda_broker)
    db_url = settings.database_url
    redis_url = os.getenv("REDIS_URL", settings.redis_url)

    print("=" * 80)
    print("STARTING END-TO-END REAL-TIME PIPELINE LIVE SMOKE VERIFICATION (M4-5)")
    print(f"PostgreSQL URL:  {db_url.split('@')[-1]}")
    print(f"Redpanda Broker: {broker}")
    print(f"Redis URL:       {redis_url}")
    print("=" * 80)

    # Stage 1: Bootstrap
    store, engine = verify_stage1_bootstrap(db_url, broker, redis_url)

    # Initialize consumer service and seek to end to ensure isolation from prior CI test runs
    consumer = StreamConsumerService(
        broker=broker,
        engine=engine,
        group_id=f"smoke-live-{uuid4().hex[:8]}",
        feature_store=store,
        enable_feature_push=True,
    )
    start_seek = time.time()
    while not consumer.consumer.assignment() and time.time() - start_seek < 5.0:
        consumer.consumer.poll(timeout_ms=200)
    assigned = consumer.consumer.assignment()
    if assigned:
        end_offsets = consumer.consumer.end_offsets(list(assigned))
        for tp, offset in end_offsets.items():
            consumer.consumer.seek(tp, offset)
        offsets_to_commit = {
            tp: OffsetAndMetadata(offset, None) for tp, offset in end_offsets.items()
        }
        consumer.consumer.commit(offsets_to_commit)
        print(
            f"Consumer assigned and explicitly positioned at end offsets: {end_offsets}"
        )

    try:
        # Pre-burst baseline count of warehouse.trips
        with engine.connect() as conn:
            init_trips = (
                conn.execute(
                    text(
                        "SELECT COUNT(*) FROM warehouse.trips WHERE source = 'replay';"
                    )
                ).scalar()
                or 0
            )

        # Stage 2: Replay burst
        trips = build_burst_trip_records(count=100)
        expected_trips = verify_stage2_replay_burst(broker, trips)

        # Stage 3: Live external feeds burst
        expected_snapshots = verify_stage3_external_feeds_burst(broker)

        # Stage 4: Consumer processing & PostgreSQL ingestion
        verify_stage4_consumer_ingestion(
            engine=engine,
            consumer=consumer,
            expected_trips=expected_trips,
            expected_snapshots=expected_snapshots,
            init_trips=init_trips,
        )

        # Stage 5: Feast online store push verification
        verify_stage5_feast_online_push(store=store)

        # Stage 6: Deadletter observability & quarantine
        verify_stage6_deadletter_quarantine(broker=broker, consumer=consumer)

        # Stage 7: Push outage resilience & Prefect reconciliation catch-up
        verify_stage7_push_resilience_and_reconciliation(
            engine=engine, broker=broker, store=store, consumer=consumer
        )
    finally:
        consumer.close()

    print("\n" + "=" * 80)
    print("ALL 7 END-TO-END REAL-TIME PIPELINE VERIFICATIONS PASSED (100% PROVEN)")
    print("  - Stage 1: Infrastructure, DB migrations, topics & Feast registry: PROVEN")
    print("  - Stage 2: Historical TLC Replay Producer (100 trips burst): PROVEN")
    print("  - Stage 3: Live External Feed Poller (traffic, transit, weather): PROVEN")
    print("  - Stage 4: Stream Consumer Ingestion to PostgreSQL (0 deadletter): PROVEN")
    print("  - Stage 5: Feast Online Store Push (<1s sub-second retrieval): PROVEN")
    print("  - Stage 6: Deadletter Observability & Quarantine Isolation: PROVEN")
    print(
        "  - Stage 7: Push Outage Resilience & Prefect Reconciliation Catch-Up: PROVEN"
    )
    print("=" * 80)


if __name__ == "__main__":
    main()
