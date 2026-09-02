"""Kafka and Redpanda client connection and topic management utilities."""

import json
import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional
from uuid import UUID

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

from src.common.config import get_settings

logger = logging.getLogger(__name__)

# Standard Topic Names
TOPIC_TRIP_EVENTS = "trip.events"
TOPIC_TRAFFIC_SNAPSHOTS = "traffic.snapshots"
TOPIC_TRANSIT_POSITIONS = "transit.positions"
TOPIC_WEATHER_SNAPSHOTS = "weather.snapshots"
TOPIC_TRIP_DEADLETTER = "trip.events.deadletter"

# Default Topic Configurations (partitions, replication_factor)
DEFAULT_TOPIC_CONFIGS: Dict[str, Dict[str, int]] = {
    TOPIC_TRIP_EVENTS: {"num_partitions": 3, "replication_factor": 1},
    TOPIC_TRAFFIC_SNAPSHOTS: {"num_partitions": 1, "replication_factor": 1},
    TOPIC_TRANSIT_POSITIONS: {"num_partitions": 1, "replication_factor": 1},
    TOPIC_WEATHER_SNAPSHOTS: {"num_partitions": 1, "replication_factor": 1},
    TOPIC_TRIP_DEADLETTER: {"num_partitions": 1, "replication_factor": 1},
}


class CustomJSONEncoder(json.JSONEncoder):
    """JSON Encoder that handles datetimes, decimals, dates, and UUIDs."""

    def default(self, o: Any) -> Any:
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        if isinstance(o, Decimal):
            return float(o)
        if isinstance(o, UUID):
            return str(o)
        return super().default(o)


def json_serializer(value: Any) -> bytes:
    """Serialize Python object to UTF-8 encoded JSON bytes."""
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    return json.dumps(value, cls=CustomJSONEncoder).encode("utf-8")


def json_deserializer(raw_bytes: bytes) -> Any:
    """Deserialize UTF-8 encoded JSON bytes to Python object."""
    if not raw_bytes:
        return None
    return json.loads(raw_bytes.decode("utf-8"))


def get_kafka_broker(broker: Optional[str] = None) -> str:
    """Resolve Kafka/Redpanda broker host:port from parameter or config."""
    if broker:
        return broker
    settings = get_settings()
    return settings.redpanda_broker


def get_kafka_producer(
    broker: Optional[str] = None,
    value_serializer: Any = json_serializer,
    key_serializer: Any = lambda k: str(k).encode("utf-8") if k is not None else None,
    retries: int = 3,
    acks: str = "all",
    **kwargs: Any,
) -> KafkaProducer:
    """Create and return a configured KafkaProducer instance."""
    resolved_broker = get_kafka_broker(broker)
    logger.info("Initializing KafkaProducer targeting broker: %s", resolved_broker)
    return KafkaProducer(
        bootstrap_servers=resolved_broker,
        value_serializer=value_serializer,
        key_serializer=key_serializer,
        retries=retries,
        acks=acks,
        **kwargs,
    )


def get_kafka_consumer(
    *topics: str,
    broker: Optional[str] = None,
    group_id: Optional[str] = None,
    auto_offset_reset: str = "earliest",
    enable_auto_commit: bool = False,
    value_deserializer: Any = json_deserializer,
    **kwargs: Any,
) -> KafkaConsumer:
    """Create and return a configured KafkaConsumer instance."""
    resolved_broker = get_kafka_broker(broker)
    logger.info(
        "Initializing KafkaConsumer for topics %s, group_id=%s, broker: %s",
        topics,
        group_id,
        resolved_broker,
    )
    return KafkaConsumer(
        *topics,
        bootstrap_servers=resolved_broker,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=enable_auto_commit,
        value_deserializer=value_deserializer,
        **kwargs,
    )


def get_admin_client(broker: Optional[str] = None, **kwargs: Any) -> KafkaAdminClient:
    """Create and return a KafkaAdminClient instance."""
    resolved_broker = get_kafka_broker(broker)
    return KafkaAdminClient(bootstrap_servers=resolved_broker, **kwargs)


def ensure_topics_exist(
    broker: Optional[str] = None,
    topic_configs: Optional[Dict[str, Dict[str, int]]] = None,
) -> List[str]:
    """Ensure standard or custom Redpanda topics exist, creating missing ones."""
    configs = topic_configs or DEFAULT_TOPIC_CONFIGS
    resolved_broker = get_kafka_broker(broker)
    admin = get_admin_client(broker=resolved_broker)

    created_topics: List[str] = []
    try:
        existing_topics = set(admin.list_topics())
        new_topics: List[NewTopic] = []

        for topic_name, cfg in configs.items():
            if topic_name not in existing_topics:
                num_partitions = cfg.get("num_partitions", 1)
                replication_factor = cfg.get("replication_factor", 1)
                new_topics.append(
                    NewTopic(
                        name=topic_name,
                        num_partitions=num_partitions,
                        replication_factor=replication_factor,
                    )
                )

        if new_topics:
            logger.info("Creating missing topics: %s", [t.name for t in new_topics])
            try:
                admin.create_topics(new_topics=new_topics, validate_only=False)
                created_topics = [t.name for t in new_topics]
            except TopicAlreadyExistsError:
                logger.warning(
                    "One or more topics already existed during creation race."
                )
        else:
            logger.info("All configured topics already exist.")
    finally:
        admin.close()

    return created_topics
