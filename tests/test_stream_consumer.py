"""Unit tests for streaming consumer, Pydantic validation schemas, and deadletter routing."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.common.db import Base
from src.common.kafka_utils import (
    TOPIC_TRAFFIC_SNAPSHOTS,
    TOPIC_TRANSIT_POSITIONS,
    TOPIC_TRIP_DEADLETTER,
    TOPIC_TRIP_EVENTS,
    TOPIC_WEATHER_SNAPSHOTS,
)
from src.common.models import (
    TaxiZone,
    TrafficSnapshot,
    TransitSnapshot,
    WarehouseTrip,
    WeatherSnapshot,
)
from src.transform.schemas import (
    TrafficSnapshotPayload,
    TransitPositionPayload,
    TripEventPayload,
    WeatherSnapshotPayload,
)
from src.transform.stream_consumer import StreamConsumerService


@pytest.fixture
def test_engine():
    """In-memory SQLite database engine with all schema tables created."""
    from sqlalchemy import event

    engine = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False}
    )

    @event.listens_for(engine, "connect")
    def attach_schemas(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("ATTACH DATABASE ':memory:' AS raw;")
        cursor.execute("ATTACH DATABASE ':memory:' AS warehouse;")
        cursor.close()

    Base.metadata.create_all(engine)

    # Seed taxi zones reference data
    with Session(bind=engine) as session:
        for zid in [1, 2, 4, 100, 263, 264, 265]:
            session.add(
                TaxiZone(
                    zone_id=zid,
                    borough="Manhattan" if zid < 264 else "Unknown",
                    zone_name=f"Zone {zid}",
                    centroid_lat=40.7,
                    centroid_lon=-73.9,
                )
            )
        session.commit()

    return engine


def test_trip_schema_valid():
    """Valid trip event payload parses, normalizes UTC datetimes, and calculates duration."""
    payload = {
        "vendor_id": 1,
        "cab_type": "yellow",
        "pickup_zone_id": 100,
        "dropoff_zone_id": 263,
        "pickup_datetime": "2026-09-01T12:00:00Z",
        "dropoff_datetime": "2026-09-01T12:15:00Z",
        "passenger_count": 2,
        "trip_distance_miles": 3.5,
        "fare_amount": 15.0,
        "total_amount": 18.5,
    }
    trip = TripEventPayload(**payload)
    assert trip.pickup_zone_id == 100
    assert trip.dropoff_zone_id == 263
    assert trip.trip_duration_seconds == 900
    assert trip.trip_distance_km == round(3.5 * 1.60934, 2)
    assert trip.pickup_datetime.tzinfo == timezone.utc


def test_trip_schema_duration_outlier():
    """Trip with duration < 60s or > 86400s must raise ValidationError."""
    # Under 60 seconds
    with pytest.raises(ValidationError):
        TripEventPayload(
            pickup_zone_id=1,
            dropoff_zone_id=2,
            pickup_datetime="2026-09-01T12:00:00Z",
            dropoff_datetime="2026-09-01T12:00:30Z",  # 30 seconds
            trip_distance_miles=1.0,
        )

    # Over 24 hours (86400s)
    with pytest.raises(ValidationError):
        TripEventPayload(
            pickup_zone_id=1,
            dropoff_zone_id=2,
            pickup_datetime="2026-09-01T12:00:00Z",
            dropoff_datetime="2026-09-02T13:00:00Z",  # 25 hours
            trip_distance_miles=1.0,
        )


def test_trip_schema_distance_and_speed_bounds():
    """Trip with distance > 300mi or speed > 100mph must raise ValidationError."""
    # Distance over 300mi
    with pytest.raises(ValidationError):
        TripEventPayload(
            pickup_zone_id=1,
            dropoff_zone_id=2,
            pickup_datetime="2026-09-01T12:00:00Z",
            dropoff_datetime="2026-09-01T18:00:00Z",
            trip_distance_miles=350.0,
        )

    # Average speed over 100mph (e.g. 50 miles in 15 minutes = 200mph)
    with pytest.raises(ValidationError):
        TripEventPayload(
            pickup_zone_id=1,
            dropoff_zone_id=2,
            pickup_datetime="2026-09-01T12:00:00Z",
            dropoff_datetime="2026-09-01T12:15:00Z",
            trip_distance_miles=50.0,
        )


def test_trip_schema_zone_id_and_financial_bounds():
    """Invalid zone IDs and negative fares must raise ValidationError."""
    # Zone ID outside TLC range [1, 265]
    with pytest.raises(ValidationError):
        TripEventPayload(
            pickup_zone_id=999,
            dropoff_zone_id=2,
            pickup_datetime="2026-09-01T12:00:00Z",
            dropoff_datetime="2026-09-01T12:15:00Z",
            trip_distance_miles=2.0,
        )

    # Negative fare
    with pytest.raises(ValidationError):
        TripEventPayload(
            pickup_zone_id=1,
            dropoff_zone_id=2,
            pickup_datetime="2026-09-01T12:00:00Z",
            dropoff_datetime="2026-09-01T12:15:00Z",
            trip_distance_miles=2.0,
            fare_amount=-5.0,
        )


def test_traffic_snapshot_schema():
    """Traffic speed snapshot parses and rejects invalid speeds (> 100mph or < 0)."""
    valid = TrafficSnapshotPayload(
        segment_id="101",
        speed_mph=35.0,
        recorded_at=datetime.now(timezone.utc),
    )
    assert valid.segment_id == "101"
    assert valid.speed_kmh == round(35.0 * 1.60934, 2)

    with pytest.raises(ValidationError):
        TrafficSnapshotPayload(
            segment_id="101",
            speed_mph=150.0,  # exceeds 100mph
            recorded_at=datetime.now(timezone.utc),
        )


def test_transit_position_schema():
    """Transit position schema validates route, delay, and congestion enum."""
    valid = TransitPositionPayload(
        route_id="A",
        delay_seconds=120,
        congestion_level="MODERATE",
        recorded_at=datetime.now(timezone.utc),
    )
    assert valid.route_id == "A"
    assert valid.congestion_level == "MODERATE"

    # Invalid congestion enum
    with pytest.raises(ValidationError):
        TransitPositionPayload(
            route_id="A",
            congestion_level="SEVERE_GRIDLOCK",  # Invalid enum value
            recorded_at=datetime.now(timezone.utc),
        )


def test_weather_snapshot_schema_dedup_bucket():
    """Weather snapshot floors recorded_at to minute time_bucket for dedup."""
    now_utc = datetime(2026, 9, 5, 14, 25, 47, 123456, tzinfo=timezone.utc)
    weather = WeatherSnapshotPayload(
        temp_c=22.5,
        humidity_pct=65,
        precipitation_mm_1h=0.0,
        recorded_at=now_utc,
    )
    assert weather.temp_f == round((22.5 * 9.0 / 5.0) + 32.0, 2)
    assert weather.time_bucket == datetime(
        2026, 9, 5, 14, 25, 0, 0, tzinfo=timezone.utc
    )

    # Extreme temperature bounds
    with pytest.raises(ValidationError):
        WeatherSnapshotPayload(
            temp_c=75.0,  # exceeds 55C
            recorded_at=now_utc,
        )


@patch("src.transform.stream_consumer.ensure_topics_exist")
def test_consumer_trip_processing_and_dedup(mock_ensure, test_engine):
    """Consumer processes valid trip and idempotently ignores duplicates."""
    mock_consumer = MagicMock()
    mock_producer = MagicMock()

    consumer_service = StreamConsumerService(
        consumer=mock_consumer,
        producer=mock_producer,
        engine=test_engine,
        broker="localhost:9092",
    )

    valid_payload = {
        "vendor_id": 1,
        "cab_type": "yellow",
        "pickup_zone_id": 100,
        "dropoff_zone_id": 263,
        "pickup_datetime": "2026-09-01T12:00:00Z",
        "dropoff_datetime": "2026-09-01T12:15:00Z",
        "passenger_count": 1,
        "trip_distance_miles": 2.5,
        "fare_amount": 10.0,
        "total_amount": 12.0,
        "source": "replay",
    }

    with Session(bind=test_engine) as session:
        consumer_service.process_record(TOPIC_TRIP_EVENTS, valid_payload, session)
        session.commit()

    with Session(bind=test_engine) as session:
        trips = session.query(WarehouseTrip).all()
        assert len(trips) == 1
        assert trips[0].pickup_zone_id == 100
        assert trips[0].dropoff_zone_id == 263
        assert trips[0].trip_duration_seconds == 900
        assert trips[0].is_weekend is False  # 2026-09-01 was a Tuesday

    # Re-process exact same trip -> idempotent ON CONFLICT DO NOTHING
    with Session(bind=test_engine) as session:
        consumer_service.process_record(TOPIC_TRIP_EVENTS, valid_payload, session)
        session.commit()

    with Session(bind=test_engine) as session:
        trips = session.query(WarehouseTrip).all()
        assert len(trips) == 1  # No duplicate rows


@patch("src.transform.stream_consumer.ensure_topics_exist")
def test_consumer_snapshot_processing_and_dedup(mock_ensure, test_engine):
    """Consumer processes traffic, transit, and weather snapshots idempotently."""
    mock_consumer = MagicMock()
    mock_producer = MagicMock()

    consumer_service = StreamConsumerService(
        consumer=mock_consumer,
        producer=mock_producer,
        engine=test_engine,
        broker="localhost:9092",
    )

    # Traffic Snapshot
    traffic_payload = {
        "segment_id": "seg-101",
        "speed_mph": 28.5,
        "recorded_at": "2026-09-05T12:00:00Z",
        "source": "socrata_live",
    }
    # Transit Snapshot
    transit_payload = {
        "route_id": "L",
        "delay_seconds": 180,
        "congestion_level": "MODERATE",
        "recorded_at": "2026-09-05T12:00:00Z",
        "source": "mta_gtfs_live",
    }
    # Weather Snapshot
    weather_payload = {
        "temp_c": 21.0,
        "precipitation_mm_1h": 0.0,
        "recorded_at": "2026-09-05T12:00:35Z",
        "source": "openweathermap_live",
    }

    with Session(bind=test_engine) as session:
        consumer_service.process_record(
            TOPIC_TRAFFIC_SNAPSHOTS, traffic_payload, session
        )
        consumer_service.process_record(
            TOPIC_TRANSIT_POSITIONS, transit_payload, session
        )
        consumer_service.process_record(
            TOPIC_WEATHER_SNAPSHOTS, weather_payload, session
        )
        session.commit()

    with Session(bind=test_engine) as session:
        assert session.query(TrafficSnapshot).count() == 1
        assert session.query(TransitSnapshot).count() == 1
        assert session.query(WeatherSnapshot).count() == 1

    # Ingest retry within the same minute for weather
    retry_weather = weather_payload.copy()
    retry_weather["recorded_at"] = "2026-09-05T12:00:58Z"  # same minute bucket!
    with Session(bind=test_engine) as session:
        consumer_service.process_record(TOPIC_WEATHER_SNAPSHOTS, retry_weather, session)
        session.commit()

    with Session(bind=test_engine) as session:
        # Deduplicated by minute bucket 12:00:00
        assert session.query(WeatherSnapshot).count() == 1


@patch("src.transform.stream_consumer.ensure_topics_exist")
def test_consumer_deadletter_routing(mock_ensure, test_engine):
    """Consumer routes malformed messages to deadletter topic and commits offset."""
    mock_consumer = MagicMock()
    mock_producer = MagicMock()

    # Create a dummy Kafka message with a malformed payload (duration = 10s < 60s min)
    bad_msg = MagicMock()
    bad_msg.topic = TOPIC_TRIP_EVENTS
    bad_msg.partition = 0
    bad_msg.offset = 42
    bad_msg.key = b"bad-trip"
    bad_msg.value = {
        "vendor_id": 1,
        "pickup_zone_id": 1,
        "dropoff_zone_id": 2,
        "pickup_datetime": "2026-09-01T12:00:00Z",
        "dropoff_datetime": "2026-09-01T12:00:10Z",  # 10s duration -> rejected
        "trip_distance_miles": 1.0,
    }

    mock_consumer.poll.return_value = {MagicMock(): [bad_msg]}

    consumer_service = StreamConsumerService(
        consumer=mock_consumer,
        producer=mock_producer,
        engine=test_engine,
        broker="localhost:9092",
    )

    results = consumer_service.consume_batch(max_messages=1, timeout_seconds=1.0)
    assert results["deadlettered"] == 1
    assert results["processed"] == 0

    # Verify deadletter producer was called with proper topic and schema
    mock_producer.send.assert_called_once()
    call_args = mock_producer.send.call_args
    assert call_args.kwargs["topic"] == TOPIC_TRIP_DEADLETTER
    deadletter_val = call_args.kwargs["value"]
    assert (
        "Trip duration 10s outside plausible bounds" in deadletter_val["error_reason"]
    )
    assert deadletter_val["partition"] == 0
    assert deadletter_val["offset"] == 42

    # Verify consumer committed offset even on deadletter
    mock_consumer.commit.assert_called_once()
