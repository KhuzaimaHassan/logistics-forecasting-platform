"""Real bounded Aiven checks; --initialize adds only authorized DB/topics, no services."""

import argparse
import json
import os
import time
import uuid
from pathlib import Path
from tempfile import TemporaryDirectory

from redis import Redis
from sqlalchemy import text

from src.common.config import get_settings
from src.common.db import get_engine
from src.common.kafka_utils import get_kafka_consumer, get_kafka_producer


def postgres(initialize=False):
    initialization = None
    if initialize:
        from src.orchestration.cloud_resources import ensure_database

        initialization = ensure_database()
    with get_engine().begin() as conn:
        version = conn.execute(text("SHOW server_version")).scalar_one()
        database = conn.execute(text("SELECT current_database()")).scalar_one()
        assert database == "logistics"
        assert int(version.split(".")[0]) >= 16
        tls = conn.execute(
            text("SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()")
        ).scalar_one()
        assert tls
        conn.execute(text("CREATE TEMP TABLE aiven_probe (value text) ON COMMIT DROP"))
        nonce = uuid.uuid4().hex
        conn.execute(text("INSERT INTO aiven_probe VALUES (:value)"), {"value": nonce})
        assert conn.execute(text("SELECT value FROM aiven_probe")).scalar_one() == nonce
        return {
            "server_version": version,
            "database": database,
            "tls": tls,
            "write_read": True,
            "scope": "session-local temporary table",
            "initialization": initialization,
        }


def valkey():
    client = Redis.from_url(
        get_settings().redis_url,
        ssl_cert_reqs="required",
        ssl_check_hostname=True,
        socket_timeout=15,
        socket_connect_timeout=15,
    )
    try:
        assert client.ping()
        nonce = uuid.uuid4().hex
        key = "deployment-verification:" + nonce
        assert client.set(key, nonce, ex=120, nx=True)
        assert client.get(key).decode() == nonce
        return {
            "tls": True,
            "write_read": True,
            "probe_ttl_seconds": 120,
            "feast_materialization": "NOT VERIFIED; requires bootstrap",
        }
    finally:
        client.close()


def kafka(initialize=False):
    assert get_settings().kafka_security_protocol == "SSL"
    from kafka.structs import TopicPartition

    from src.orchestration.flows.free_tier_pipeline import (
        download_source,
        sampled_events,
    )

    initialization = None
    if initialize:
        from src.orchestration.cloud_resources import ensure_topics

        initialization = ensure_topics()

    with TemporaryDirectory() as directory:
        path = Path(directory) / "source.parquet"
        download_source(path)
        events, _ = sampled_events(path)
    nonce = uuid.uuid4().hex
    event = {**events[0], "verification_id": nonce}
    consumer = get_kafka_consumer(group_id=None)
    producer = None
    try:
        topics = consumer.topics()
        assert {"trip.events", "trip.events.deadletter"}.issubset(topics)
        partitions = consumer.partitions_for_topic("trip.events")
        assert partitions and len(partitions) <= 2
        producer = get_kafka_producer()
        ack = producer.send("trip.events", key=str(event["trip_id"]), value=event).get(
            timeout=30
        )
        partition = TopicPartition(ack.topic, ack.partition)
        consumer.assign([partition])
        consumer.seek(partition, ack.offset)
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            for batch in consumer.poll(timeout_ms=1000).values():
                for message in batch:
                    if message.value.get("verification_id") == nonce:
                        assert message.offset == ack.offset
                        return {
                            "topic": ack.topic,
                            "partition": ack.partition,
                            "produced_offset": ack.offset,
                            "consumed_offset": message.offset,
                            "source": "authentic TLC sample",
                            "mutual_tls": True,
                            "initialization": initialization,
                        }
        raise TimeoutError("Kafka readback deadline exceeded")
    finally:
        consumer.close()
        if producer:
            producer.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--initialize",
        action="store_true",
        help="Create only missing logistics database and two required topics",
    )
    parser.add_argument(
        "--bootstrap",
        action="store_true",
        help="Run bounded authentic ingestion/migrations and verify Feast against managed Valkey",
    )
    args = parser.parse_args()
    results = {}
    for name, action in (
        ("postgresql", lambda: postgres(initialize=args.initialize)),
        ("valkey", valkey),
        ("kafka", lambda: kafka(initialize=args.initialize)),
    ):
        required = {
            "postgresql": ("DATABASE_URL", "POSTGRES_SSLROOTCERT"),
            "valkey": ("REDIS_URL",),
            "kafka": (
                "REDPANDA_BROKER",
                "KAFKA_SSL_CAFILE",
                "KAFKA_SSL_CERTFILE",
                "KAFKA_SSL_KEYFILE",
            ),
        }[name]
        missing = [key for key in required if not os.getenv(key)]
        if missing:
            results[name] = {
                "status": "NOT VERIFIED",
                "reason": "BLOCKED: missing " + ", ".join(missing),
            }
            continue
        try:
            results[name] = {"status": "VERIFIED", "evidence": action()}
        except Exception as exc:
            results[name] = {"status": "FAILED", "reason": type(exc).__name__}
    if args.bootstrap:
        if all(row["status"] == "VERIFIED" for row in results.values()):
            try:
                from src.orchestration.flows.free_tier_pipeline import (
                    scheduled_sample_pipeline,
                )

                results["pipeline"] = {
                    "status": "VERIFIED",
                    "evidence": scheduled_sample_pipeline(
                        max_events=10000, bootstrap=True
                    ),
                }
            except Exception as exc:
                results["pipeline"] = {"status": "FAILED", "reason": type(exc).__name__}
        else:
            results["pipeline"] = {
                "status": "NOT VERIFIED",
                "reason": "BLOCKED: prerequisite managed checks did not pass",
            }
        if results["pipeline"]["status"] == "VERIFIED":
            try:
                from src.features.client import FeastOnlineClient

                with get_engine().connect() as conn:
                    zone, expected = conn.execute(
                        text(
                            "SELECT zone_id, pickup_count_last_1h FROM warehouse.zone_demand_features_hourly ORDER BY pickup_datetime DESC, zone_id LIMIT 1"
                        )
                    ).one()
                    size = conn.execute(
                        text("SELECT pg_database_size(current_database())")
                    ).scalar_one()
                feature = FeastOnlineClient().get_zone_demand_features(
                    [zone], use_push_features=False
                )[0]
                assert (
                    feature.cache_hit
                    and feature.pickup_count_last_1h == expected
                    and feature.observed_at is not None
                )
                results["feast"] = {
                    "status": "VERIFIED",
                    "evidence": {
                        "zone_id": zone,
                        "pickups_last_hour": expected,
                        "online_matches_postgres": True,
                        "observed_at": feature.observed_at.isoformat(),
                        "database_bytes": size,
                    },
                }
            except Exception as exc:
                results["feast"] = {"status": "FAILED", "reason": type(exc).__name__}
        else:
            results["feast"] = {
                "status": "NOT VERIFIED",
                "reason": "BLOCKED: bootstrap did not complete",
            }
    output = {
        "scope": "Managed Aiven checks; Feast/pipeline only verified when explicitly listed; no public deployment proof",
        "checks": results,
    }
    path = Path("artifacts/aiven-verification.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(output, indent=2) + "\n")
    print(json.dumps(output, indent=2))
    raise SystemExit(
        0 if all(row["status"] == "VERIFIED" for row in results.values()) else 1
    )


if __name__ == "__main__":
    main()
