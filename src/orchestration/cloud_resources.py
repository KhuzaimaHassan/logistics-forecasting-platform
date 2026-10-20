"""Add only the authorized database and two topics to existing Aiven services."""

from kafka import errors as Errors
from kafka.admin import NewTopic
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.pool import NullPool

from src.common.config import get_settings
from src.common.kafka_utils import get_admin_client

TOPICS = ("trip.events", "trip.events.deadletter")


def ensure_database():
    url = make_url(get_settings().database_url)
    if url.database != "logistics":
        raise ValueError(
            "CLOUD_DATABASE_URL must target logistics before initialization"
        )
    engine = create_engine(
        url.set(database="defaultdb"),
        isolation_level="AUTOCOMMIT",
        poolclass=NullPool,
        connect_args={"connect_timeout": 15},
    )
    try:
        with engine.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname='logistics'")
            ).scalar()
            if not exists:
                conn.execute(text('CREATE DATABASE "logistics"'))
            return {
                "database": "logistics",
                "created": not bool(exists),
                "defaultdb_preserved": True,
            }
    finally:
        engine.dispose()


def ensure_topics():
    admin = get_admin_client(request_timeout_ms=30000)
    try:
        existing = set(admin.list_topics())
        missing = [name for name in TOPICS if name not in existing]
        if (
            len([name for name in existing if not name.startswith("_")]) + len(missing)
            > 5
        ):
            raise ValueError("Free topic capacity would be exceeded; no topics deleted")
        if missing:
            # This client supports CreateTopics <=v3, which cannot request broker
            # defaults with replication_factor=-1. Use up to three advertised
            # brokers (Aiven documented default RF=3), never alter cluster settings.
            brokers = admin.describe_cluster()["brokers"]
            if not brokers:
                raise ValueError("No advertised Kafka brokers")
            factor = min(3, len(brokers))
            response = admin.create_topics(
                [
                    NewTopic(name, num_partitions=2, replication_factor=factor)
                    for name in missing
                ],
                timeout_ms=30000,
            )
            for row in response.topic_errors:
                if row[1] not in (0, 36):  # Concurrent TopicAlreadyExists is safe.
                    raise Errors.for_code(row[1])()
        descriptions = admin.describe_topics(list(TOPICS))
        result = {}
        for topic in descriptions:
            if topic["error_code"]:
                raise Errors.for_code(topic["error_code"])()
            partitions = topic["partitions"]
            if not 1 <= len(partitions) <= 2:
                raise ValueError("Existing topic partition count exceeds Free profile")
            result[topic["topic"]] = {
                "created": topic["topic"] in missing,
                "partitions": len(partitions),
                "replication_factors": sorted(
                    {len(part["replicas"]) for part in partitions}
                ),
                "retention": "provider default preserved",
            }
        return result
    finally:
        admin.close()
