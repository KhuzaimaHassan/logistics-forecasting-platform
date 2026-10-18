"""Real bounded Aiven checks before MLflow/bootstrap; never creates services/topics."""

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


def postgres():
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


def kafka():
    assert get_settings().kafka_security_protocol == "SSL"
    from kafka.structs import TopicPartition

    from src.orchestration.flows.free_tier_pipeline import (
        download_source,
        sampled_events,
    )

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
                        }
        raise TimeoutError("Kafka readback deadline exceeded")
    finally:
        consumer.close()
        if producer:
            producer.close()


def main():
    results = {}
    for name, action in (
        ("postgresql", postgres),
        ("valkey", valkey),
        ("kafka", kafka),
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
    output = {
        "scope": "Managed Aiven connectivity/write/read only; no public deployment or Feast proof",
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
