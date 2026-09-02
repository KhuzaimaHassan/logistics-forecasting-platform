from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pandas as pd
from kafka.admin import NewTopic

from src.common.kafka_utils import (
    TOPIC_TRIP_EVENTS,
    ensure_topics_exist,
    get_kafka_broker,
    json_deserializer,
    json_serializer,
)
from src.extract.replay_producer import HistoricalReplayProducer


def test_json_serializer_and_deserializer():
    """Verify serialization handles datetime, Decimal, UUID, and round-trips cleanly."""
    now = datetime(2026, 9, 2, 12, 0, 0, tzinfo=timezone.utc)
    uid = uuid4()
    data = {
        "id": 101,
        "amount": Decimal("25.50"),
        "timestamp": now,
        "uuid": uid,
        "text": "sample",
    }

    serialized = json_serializer(data)
    assert isinstance(serialized, bytes)

    deserialized = json_deserializer(serialized)
    assert deserialized["id"] == 101
    assert deserialized["amount"] == 25.50
    assert deserialized["timestamp"] == "2026-09-02T12:00:00+00:00"
    assert deserialized["uuid"] == str(uid)
    assert deserialized["text"] == "sample"

    # Edge cases
    assert json_serializer(None) == b""
    assert json_deserializer(b"") is None


def test_get_kafka_broker_resolution():
    """Verify broker resolution precedence (param > settings)."""
    assert get_kafka_broker("custom:9092") == "custom:9092"
    with patch("src.common.kafka_utils.get_settings") as mock_settings:
        mock_settings.return_value.redpanda_broker = "env-broker:9092"
        assert get_kafka_broker(None) == "env-broker:9092"


def test_ensure_topics_exist_creates_missing():
    """Verify ensure_topics_exist inspects existing topics and creates only missing ones."""
    mock_admin = MagicMock()
    mock_admin.list_topics.return_value = ["trip.events", "existing.topic"]

    with patch("src.common.kafka_utils.get_admin_client", return_value=mock_admin):
        created = ensure_topics_exist(
            broker="localhost:9092",
            topic_configs={
                "trip.events": {"num_partitions": 3, "replication_factor": 1},
                "traffic.snapshots": {"num_partitions": 1, "replication_factor": 1},
            },
        )

        assert created == ["traffic.snapshots"]
        mock_admin.create_topics.assert_called_once()
        args, kwargs = mock_admin.create_topics.call_args
        new_topics = kwargs["new_topics"]
        assert len(new_topics) == 1
        assert isinstance(new_topics[0], NewTopic)
        assert new_topics[0].name == "traffic.snapshots"
        assert new_topics[0].num_partitions == 1


def test_prepare_trip_payload_with_timestamp_rewrite():
    """Verify trip payload preparation and simulated timestamp offsetting."""
    producer = HistoricalReplayProducer(speed_multiplier=0.0, rewrite_timestamps=True)

    record = {
        "trip_id": 9999,
        "vendor_id": 1,
        "cab_type": "yellow",
        "pickup_zone_id": 161,
        "dropoff_zone_id": 236,
        "pickup_datetime": "2023-01-15T10:00:00+00:00",
        "dropoff_datetime": "2023-01-15T10:15:30+00:00",
        "trip_duration_seconds": 930,
        "passenger_count": 2,
        "trip_distance_km": 4.5,
        "fare_amount": 18.0,
        "tip_amount": 3.0,
        "total_amount": 24.5,
    }

    # Shift by 3600 seconds (1 hour)
    payload = producer.prepare_trip_payload(record, time_offset_seconds=3600.0)

    assert payload["trip_id"] == 9999
    assert payload["pickup_zone_id"] == 161
    assert payload["dropoff_zone_id"] == 236
    assert payload["pickup_datetime"] == "2023-01-15T11:00:00+00:00"
    assert payload["dropoff_datetime"] == "2023-01-15T11:15:30+00:00"
    assert payload["trip_duration_seconds"] == 930
    assert payload["source"] == "replay"
    assert "replayed_at" in payload


def test_prepare_trip_payload_no_rewrite():
    """Verify timestamps stay untouched when rewrite_timestamps=False."""
    producer = HistoricalReplayProducer(speed_multiplier=0.0, rewrite_timestamps=False)

    record = {
        "trip_id": 100,
        "pickup_zone_id": 43,
        "dropoff_zone_id": 140,
        "pickup_datetime": datetime(2023, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
        "dropoff_datetime": datetime(2023, 1, 15, 10, 20, 0, tzinfo=timezone.utc),
        "trip_duration_seconds": None,  # auto-computed
    }

    payload = producer.prepare_trip_payload(record, time_offset_seconds=0.0)
    assert payload["pickup_datetime"] == "2023-01-15T10:00:00+00:00"
    assert payload["dropoff_datetime"] == "2023-01-15T10:20:00+00:00"
    assert payload["trip_duration_seconds"] == 1200


def test_fetch_trips_from_parquet(tmp_path):
    """Verify parquet loading, column normalization, and sorting."""
    parquet_file = tmp_path / "sample_trips.parquet"
    df = pd.DataFrame(
        [
            {
                "tpep_pickup_datetime": "2023-01-01 00:30:00",
                "tpep_dropoff_datetime": "2023-01-01 00:45:00",
                "PULocationID": 100,
                "DOLocationID": 200,
                "trip_distance": 2.5,
                "fare_amount": 12.0,
            },
            {
                "tpep_pickup_datetime": "2023-01-01 00:10:00",
                "tpep_dropoff_datetime": "2023-01-01 00:25:00",
                "PULocationID": 50,
                "DOLocationID": 60,
                "trip_distance": 1.5,
                "fare_amount": 8.0,
            },
        ]
    )
    df.to_parquet(parquet_file)

    producer = HistoricalReplayProducer(speed_multiplier=0.0)
    records = list(producer.fetch_trips_from_parquet(str(parquet_file), limit=2))

    assert len(records) == 2
    # Should be sorted chronologically by pickup_datetime
    assert records[0]["pickup_zone_id"] == 50
    assert records[1]["pickup_zone_id"] == 100


def test_replay_stream_execution_and_metrics():
    """Verify streaming replay produces messages to Kafka with partition keys."""
    mock_producer = MagicMock()
    producer = HistoricalReplayProducer(
        speed_multiplier=0.0,
        rewrite_timestamps=True,
        producer=mock_producer,
    )

    test_records = [
        {
            "trip_id": i + 1,
            "vendor_id": 1,
            "cab_type": "yellow",
            "pickup_zone_id": 100 + i,
            "dropoff_zone_id": 200,
            "pickup_datetime": f"2023-01-01T00:{i:02d}:00+00:00",
            "dropoff_datetime": f"2023-01-01T00:{i+15:02d}:00+00:00",
            "trip_duration_seconds": 900,
            "passenger_count": 1,
            "trip_distance_km": 3.0,
            "fare_amount": 10.0,
            "tip_amount": 2.0,
            "total_amount": 14.0,
        }
        for i in range(5)
    ]

    summary = producer.replay_stream(iter(test_records), flush_interval=2)

    assert summary["records_read"] == 5
    assert summary["records_published"] == 5
    assert summary["publish_errors"] == 0
    assert mock_producer.send.call_count == 5

    # Check first call arguments
    call_args = mock_producer.send.call_args_list[0]
    kwargs = call_args[1]
    assert kwargs["topic"] == TOPIC_TRIP_EVENTS
    assert kwargs["key"] == 100
    assert kwargs["value"]["trip_id"] == 1


def test_replay_stream_stop_signal():
    """Verify producer stops prematurely when stop() is invoked."""
    mock_producer = MagicMock()
    producer = HistoricalReplayProducer(
        speed_multiplier=0.0,
        producer=mock_producer,
    )

    def generator():
        yield {
            "trip_id": 1,
            "pickup_zone_id": 10,
            "pickup_datetime": "2023-01-01T00:00:00Z",
        }
        producer.stop()
        yield {
            "trip_id": 2,
            "pickup_zone_id": 10,
            "pickup_datetime": "2023-01-01T00:01:00Z",
        }

    summary = producer.replay_stream(generator())
    assert summary["records_read"] == 1
    assert summary["records_published"] == 1
    assert mock_producer.send.call_count == 1
