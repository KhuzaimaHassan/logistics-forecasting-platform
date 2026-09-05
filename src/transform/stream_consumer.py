"""Real-time stream consumer service for NYC logistics forecasting platform.

Subscribes to Redpanda topics:
- trip.events: TLC trip events (historical replay or live orders)
- traffic.snapshots: NYC road speed snapshots (Socrata)
- transit.positions: MTA subway delay and congestion proxies
- weather.snapshots: NYC meteorological observations (OpenWeatherMap)

Validates, cleans, deduplicates, and ingests records into PostgreSQL warehouse tables:
- warehouse.trips (reusing deterministic trip_id)
- warehouse.traffic_snapshots (dedup on segment_id + recorded_at)
- warehouse.weather_snapshots (dedup on minute-floored time_bucket)
- warehouse.transit_snapshots (dedup on route_id + recorded_at)

Quarantines malformed/poison messages to trip.events.deadletter and enforces
at-least-once delivery by committing Kafka offsets only after successful DB writes.
"""

import argparse
import logging
import signal
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
from feast import FeatureStore
from feast.data_source import PushMode
from kafka import KafkaConsumer, KafkaProducer
from pydantic import ValidationError
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from src.common.config import get_settings
from src.common.db import get_engine
from src.common.kafka_utils import (
    DEFAULT_TOPIC_CONFIGS,
    TOPIC_TRAFFIC_SNAPSHOTS,
    TOPIC_TRANSIT_POSITIONS,
    TOPIC_TRIP_DEADLETTER,
    TOPIC_TRIP_EVENTS,
    TOPIC_WEATHER_SNAPSHOTS,
    ensure_topics_exist,
    get_kafka_consumer,
    get_kafka_producer,
)
from src.common.models import (
    TaxiZone,
    TrafficSnapshot,
    TransitSnapshot,
    WarehouseTrip,
    WeatherSnapshot,
)
from src.features.offline_extractor import is_us_holiday
from src.transform.batch_transformer import (
    VALID_ZONE_IDS as DEFAULT_ZONE_IDS,
)
from src.transform.batch_transformer import (
    generate_deterministic_trip_id,
)
from src.transform.schemas import (
    DeadletterPayload,
    TrafficSnapshotPayload,
    TransitPositionPayload,
    TripEventPayload,
    WeatherSnapshotPayload,
)

logger = logging.getLogger(__name__)


def load_valid_zone_ids(session: Optional[Session] = None) -> Set[int]:
    """Query warehouse.taxi_zones for all active zone IDs, falling back to standard range if unavailable."""
    if session is not None:
        try:
            zones = session.query(TaxiZone.zone_id).all()
            if zones:
                zone_set = {z[0] for z in zones}
                logger.info(
                    "Loaded %d active taxi zone IDs from warehouse.taxi_zones.",
                    len(zone_set),
                )
                return zone_set
        except Exception as exc:
            logger.warning(
                "Could not query warehouse.taxi_zones (%s). Using default TLC range [1, 265].",
                exc,
            )
    return set(DEFAULT_ZONE_IDS)


def execute_idempotent_insert(
    session: Session,
    model_cls: Any,
    values: Dict[str, Any],
    conflict_cols: List[str],
) -> None:
    """Execute dialect-aware ON CONFLICT DO NOTHING insert."""
    dialect_name = session.bind.dialect.name
    if dialect_name == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        stmt = (
            pg_insert(model_cls)
            .values(**values)
            .on_conflict_do_nothing(index_elements=conflict_cols)
        )
        session.execute(stmt)
    elif dialect_name == "sqlite":
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        stmt = (
            sqlite_insert(model_cls)
            .values(**values)
            .on_conflict_do_nothing(index_elements=conflict_cols)
        )
        session.execute(stmt)
    else:
        # Fallback for generic SQL dialects
        obj = model_cls(**values)
        session.merge(obj)


class StreamFeatureAggregator:
    """In-memory sliding-window aggregator for sub-second streaming feature pushes.

    Cross-reference: src/features/offline_extractor.py
    Maintains identical temporal invariants:
    - 15-minute rolling window: [T - 15m, T] with pickup_datetime <= T
    - 1-hour rolling window: [T - 1h, T] with pickup_datetime <= T
    - US holiday tagging via src.features.offline_extractor.is_us_holiday
    """

    def __init__(self) -> None:
        self.zone_pickups: Dict[int, deque] = {}
        self.corridor_durations: Dict[str, deque] = {}
        self.latest_temp_c: Optional[float] = None
        self.latest_is_precip: Optional[bool] = None
        self.segment_speeds: Dict[int, float] = {}
        self.latest_avg_speed_kmh: Optional[float] = None

    def update_weather(
        self, temp_c: Optional[float], is_precipitating: Optional[bool]
    ) -> None:
        """Update latest observed weather signals."""
        if temp_c is not None:
            self.latest_temp_c = float(temp_c)
        if is_precipitating is not None:
            self.latest_is_precip = bool(is_precipitating)

    def update_traffic(self, segment_id: int, speed_kmh: Optional[float]) -> None:
        """Update latest observed traffic speed signals."""
        if speed_kmh is not None:
            self.segment_speeds[segment_id] = float(speed_kmh)
            if self.segment_speeds:
                self.latest_avg_speed_kmh = sum(self.segment_speeds.values()) / len(
                    self.segment_speeds
                )

    def record_trip(
        self,
        pickup_zone_id: int,
        dropoff_zone_id: int,
        pickup_datetime: datetime,
        dropoff_datetime: datetime,
        trip_duration_seconds: float,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Record trip event, update rolling windows, and return push DataFrames.

        Cross-reference: src/features/offline_extractor.py
        Strictly adheres to:
        - [T - 15m, T] and [T - 1h, T] bounds for pickup counts
        - [T - 15m, T] and [T - 1h, T] bounds for corridor trip durations
        """
        now_utc = datetime.now(timezone.utc)
        pu_dt = (
            pickup_datetime
            if pickup_datetime.tzinfo
            else pickup_datetime.replace(tzinfo=timezone.utc)
        )
        do_dt = (
            dropoff_datetime
            if dropoff_datetime.tzinfo
            else dropoff_datetime.replace(tzinfo=timezone.utc)
        )

        # 1. Zone pickup rolling counts
        if pickup_zone_id not in self.zone_pickups:
            self.zone_pickups[pickup_zone_id] = deque()
        q = self.zone_pickups[pickup_zone_id]
        q.append(pu_dt)

        cutoff_1h = pu_dt - timedelta(hours=1)
        cutoff_15m = pu_dt - timedelta(minutes=15)
        while q and q[0] < cutoff_1h:
            q.popleft()

        pickup_count_1h = len(q)
        pickup_count_15m = sum(1 for t in q if t >= cutoff_15m)

        hour_of_day = pu_dt.hour
        day_of_week = pu_dt.weekday()
        is_weekend = day_of_week in (5, 6)
        is_holiday = is_us_holiday(pu_dt.date())

        zone_df = pd.DataFrame(
            [
                {
                    "zone_id": pickup_zone_id,
                    "pickup_datetime": pu_dt,
                    "created_at": now_utc,
                    "pickup_count_last_15m": pickup_count_15m,
                    "pickup_count_last_1h": pickup_count_1h,
                    "hour_of_day": hour_of_day,
                    "day_of_week": day_of_week,
                    "is_weekend": is_weekend,
                    "is_holiday": is_holiday,
                    "avg_temp_last_1h": (
                        float(self.latest_temp_c)
                        if self.latest_temp_c is not None
                        else 15.0
                    ),
                    "is_precipitating": (
                        bool(self.latest_is_precip)
                        if self.latest_is_precip is not None
                        else False
                    ),
                }
            ]
        )

        # 2. Corridor duration rolling averages
        corridor_id = f"{pickup_zone_id}_{dropoff_zone_id}"
        if corridor_id not in self.corridor_durations:
            self.corridor_durations[corridor_id] = deque()
        cq = self.corridor_durations[corridor_id]
        cq.append((do_dt, float(trip_duration_seconds)))

        c_cutoff_1h = do_dt - timedelta(hours=1)
        c_cutoff_15m = do_dt - timedelta(minutes=15)
        while cq and cq[0][0] < c_cutoff_1h:
            cq.popleft()

        recent_15m = [dur for dt, dur in cq if dt >= c_cutoff_15m]
        avg_dur_15m = (
            sum(recent_15m) / len(recent_15m)
            if recent_15m
            else float(trip_duration_seconds)
        )
        avg_dur_1h = (
            sum(dur for _, dur in cq) / len(cq) if cq else float(trip_duration_seconds)
        )

        corridor_df = pd.DataFrame(
            [
                {
                    "corridor_id": corridor_id,
                    "dropoff_datetime": do_dt,
                    "created_at": now_utc,
                    "avg_duration_last_15m": float(avg_dur_15m),
                    "avg_duration_last_1h": float(avg_dur_1h),
                    "avg_traffic_speed_current": (
                        float(self.latest_avg_speed_kmh)
                        if self.latest_avg_speed_kmh is not None
                        else 25.0
                    ),
                    "origin_zone_demand_pressure": int(pickup_count_1h),
                }
            ]
        )

        return zone_df, corridor_df


class StreamConsumerService:
    """Consumes, validates, cleans, and stores streaming data with deadletter routing."""

    def __init__(
        self,
        consumer: Optional[KafkaConsumer] = None,
        producer: Optional[KafkaProducer] = None,
        engine: Optional[Engine] = None,
        broker: Optional[str] = None,
        group_id: str = "logistics_stream_consumer",
        topics: Optional[List[str]] = None,
        deadletter_topic: str = TOPIC_TRIP_DEADLETTER,
        feature_store: Optional[FeatureStore] = None,
        enable_feature_push: bool = True,
    ) -> None:
        self.settings = get_settings()
        self.broker = broker or self.settings.redpanda_broker
        self.group_id = group_id
        self.deadletter_topic = deadletter_topic
        self.topics = topics or [
            TOPIC_TRIP_EVENTS,
            TOPIC_TRAFFIC_SNAPSHOTS,
            TOPIC_TRANSIT_POSITIONS,
            TOPIC_WEATHER_SNAPSHOTS,
        ]

        self.engine = engine or get_engine()
        self.producer = producer or get_kafka_producer(broker=self.broker)
        self.feature_store = feature_store
        self.enable_feature_push = enable_feature_push
        self.aggregator = StreamFeatureAggregator()

        # Cache valid taxi zone IDs from warehouse.taxi_zones
        with Session(bind=self.engine) as init_session:
            self.valid_zone_ids = load_valid_zone_ids(init_session)

        # Ensure topics exist in Redpanda
        topic_configs = {
            t: DEFAULT_TOPIC_CONFIGS.get(
                t, {"num_partitions": 1, "replication_factor": 1}
            )
            for t in (self.topics + [self.deadletter_topic])
        }
        ensure_topics_exist(broker=self.broker, topic_configs=topic_configs)

        self.consumer = consumer or get_kafka_consumer(
            *self.topics,
            broker=self.broker,
            group_id=self.group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )

        self._shutdown_requested = False

    def route_to_deadletter(
        self,
        topic: str,
        raw_payload: Any,
        reason: str,
        partition: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> None:
        """Publish malformed or unvalidated message to deadletter topic."""
        logger.warning(
            "Routing message to deadletter topic '%s'. Reason: %s",
            self.deadletter_topic,
            reason,
        )
        deadletter = DeadletterPayload(
            error_reason=reason,
            topic=topic,
            raw_payload=raw_payload,
            partition=partition,
            offset=offset,
            failed_at=datetime.now(timezone.utc),
        )
        self.producer.send(
            topic=self.deadletter_topic,
            key=(
                str(raw_payload.get("trip_id", "")).encode("utf-8")
                if isinstance(raw_payload, dict)
                else None
            ),
            value=deadletter.model_dump(mode="json"),
        )
        self.producer.flush()

    def process_trip_event(
        self, payload: Dict[str, Any], session: Session
    ) -> Dict[str, Any]:
        """Validate, normalize, and insert a trip event into warehouse.trips."""
        validated = TripEventPayload(**payload)

        # Validate zone IDs against dynamic DB lookup
        if validated.pickup_zone_id not in self.valid_zone_ids:
            raise ValueError(
                f"pickup_zone_id {validated.pickup_zone_id} is not present in warehouse.taxi_zones."
            )
        if validated.dropoff_zone_id not in self.valid_zone_ids:
            raise ValueError(
                f"dropoff_zone_id {validated.dropoff_zone_id} is not present in warehouse.taxi_zones."
            )

        # Generate deterministic 60-bit BigInteger trip_id
        trip_id = generate_deterministic_trip_id(validated.model_dump())

        # Compute derived spatial-temporal dimensions
        pu_dt = validated.pickup_datetime
        time_bin_15m = pu_dt.replace(
            minute=(pu_dt.minute // 15) * 15, second=0, microsecond=0
        )
        day_of_week = pu_dt.weekday()
        hour_of_day = pu_dt.hour
        is_weekend = day_of_week in (5, 6)

        db_record = {
            "trip_id": trip_id,
            "vendor_id": validated.vendor_id,
            "cab_type": validated.cab_type,
            "pickup_zone_id": validated.pickup_zone_id,
            "dropoff_zone_id": validated.dropoff_zone_id,
            "pickup_datetime": pu_dt,
            "dropoff_datetime": validated.dropoff_datetime,
            "trip_duration_seconds": validated.trip_duration_seconds,
            "time_bin_15m": time_bin_15m,
            "day_of_week": day_of_week,
            "hour_of_day": hour_of_day,
            "is_weekend": is_weekend,
            "passenger_count": validated.passenger_count,
            "trip_distance_km": validated.trip_distance_km,
            "fare_amount": validated.fare_amount,
            "tip_amount": validated.tip_amount,
            "total_amount": validated.total_amount,
            "source": validated.source or "replay",
        }

        execute_idempotent_insert(
            session=session,
            model_cls=WarehouseTrip,
            values=db_record,
            conflict_cols=["trip_id"],
        )
        return db_record

    def process_traffic_snapshot(
        self, payload: Dict[str, Any], session: Session
    ) -> Dict[str, Any]:
        """Validate and insert traffic speed snapshot into warehouse.traffic_snapshots."""
        validated = TrafficSnapshotPayload(**payload)
        db_record = {
            "segment_id": validated.segment_id,
            "recorded_at": validated.recorded_at,
            "speed_mph": validated.speed_mph,
            "speed_kmh": validated.speed_kmh,
            "travel_time_seconds": validated.travel_time_seconds,
            "borough": validated.borough,
            "link_name": validated.link_name,
            "source": validated.source,
        }
        execute_idempotent_insert(
            session=session,
            model_cls=TrafficSnapshot,
            values=db_record,
            conflict_cols=["segment_id", "recorded_at"],
        )
        return db_record

    def process_transit_snapshot(
        self, payload: Dict[str, Any], session: Session
    ) -> Dict[str, Any]:
        """Validate and insert transit position/delay into warehouse.transit_snapshots."""
        validated = TransitPositionPayload(**payload)
        db_record = {
            "route_id": validated.route_id,
            "recorded_at": validated.recorded_at,
            "trip_id": validated.trip_id,
            "vehicle_id": validated.vehicle_id,
            "current_status": validated.current_status,
            "stop_id": validated.stop_id,
            "delay_seconds": validated.delay_seconds,
            "congestion_level": validated.congestion_level,
            "source": validated.source,
        }
        execute_idempotent_insert(
            session=session,
            model_cls=TransitSnapshot,
            values=db_record,
            conflict_cols=["route_id", "recorded_at"],
        )
        return db_record

    def process_weather_snapshot(
        self, payload: Dict[str, Any], session: Session
    ) -> Dict[str, Any]:
        """Validate and insert weather snapshot into warehouse.weather_snapshots."""
        validated = WeatherSnapshotPayload(**payload)
        db_record = {
            "time_bucket": validated.time_bucket,
            "recorded_at": validated.recorded_at,
            "temp_c": validated.temp_c,
            "feels_like_c": validated.feels_like_c,
            "humidity_pct": validated.humidity_pct,
            "wind_speed_kmh": validated.wind_speed_kmh,
            "precipitation_mm_1h": validated.precipitation_mm_1h,
            "is_precipitating": validated.is_precipitating,
            "condition_main": validated.condition_main,
            "condition_description": validated.condition_description,
            "source": validated.source,
        }
        execute_idempotent_insert(
            session=session,
            model_cls=WeatherSnapshot,
            values=db_record,
            conflict_cols=["time_bucket"],
        )
        return db_record

    def process_record(
        self, topic: str, payload: Any, session: Session
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Dispatch record to appropriate topic validator and DB insert logic."""
        if not isinstance(payload, dict):
            raise ValueError(f"Payload must be a JSON object/dict, got {type(payload)}")

        db_record: Optional[Dict[str, Any]] = None
        if topic == TOPIC_TRIP_EVENTS:
            db_record = self.process_trip_event(payload, session)
        elif topic == TOPIC_TRAFFIC_SNAPSHOTS:
            db_record = self.process_traffic_snapshot(payload, session)
        elif topic == TOPIC_TRANSIT_POSITIONS:
            db_record = self.process_transit_snapshot(payload, session)
        elif topic == TOPIC_WEATHER_SNAPSHOTS:
            db_record = self.process_weather_snapshot(payload, session)
        else:
            raise ValueError(f"Unrecognized stream topic: {topic}")

        return True, db_record

    def _push_features(self, topic: str, record: Dict[str, Any]) -> None:
        """Push streaming features to Feast online store (best-effort)."""
        try:
            if topic == TOPIC_TRIP_EVENTS:
                zone_df, corridor_df = self.aggregator.record_trip(
                    pickup_zone_id=int(record["pickup_zone_id"]),
                    dropoff_zone_id=int(record["dropoff_zone_id"]),
                    pickup_datetime=record["pickup_datetime"],
                    dropoff_datetime=record["dropoff_datetime"],
                    trip_duration_seconds=float(record["trip_duration_seconds"]),
                )
                self.feature_store.push(
                    "zone_demand_push_source", zone_df, to=PushMode.ONLINE
                )
                self.feature_store.push(
                    "corridor_duration_push_source", corridor_df, to=PushMode.ONLINE
                )
            elif topic == TOPIC_WEATHER_SNAPSHOTS:
                self.aggregator.update_weather(
                    temp_c=record.get("temp_c"),
                    is_precipitating=record.get("is_precipitating"),
                )
            elif topic == TOPIC_TRAFFIC_SNAPSHOTS:
                self.aggregator.update_traffic(
                    segment_id=int(record["segment_id"]),
                    speed_kmh=record.get("speed_kmh"),
                )
        except Exception as exc:
            logger.warning(
                "Feast feature push failed for topic '%s' (reconciliation will catch up): %s",
                topic,
                exc,
            )

    def _process_single_message(self, msg: Any, counts: Dict[str, int]) -> None:
        """Process a single Kafka message, updating DB or routing to deadletter."""
        topic = msg.topic
        payload = msg.value

        with Session(bind=self.engine) as session:
            try:
                _, db_record = self.process_record(topic, payload, session)
                session.commit()
                counts["processed"] += 1
                if topic == TOPIC_TRIP_EVENTS:
                    counts["trips"] += 1
                elif topic == TOPIC_TRAFFIC_SNAPSHOTS:
                    counts["traffic"] += 1
                elif topic == TOPIC_TRANSIT_POSITIONS:
                    counts["transit"] += 1
                elif topic == TOPIC_WEATHER_SNAPSHOTS:
                    counts["weather"] += 1

                if (
                    self.enable_feature_push
                    and self.feature_store is not None
                    and db_record
                ):
                    self._push_features(topic, db_record)
            except (ValidationError, ValueError, Exception) as exc:
                session.rollback()
                counts["deadlettered"] += 1
                self.route_to_deadletter(
                    topic=topic,
                    raw_payload=payload,
                    reason=str(exc),
                    partition=msg.partition,
                    offset=msg.offset,
                )

        self.consumer.commit()

    def consume_batch(
        self,
        max_messages: Optional[int] = None,
        timeout_seconds: float = 5.0,
    ) -> Dict[str, int]:
        """Poll and process a batch of messages with offset commit and deadletter routing."""
        counts = {
            "processed": 0,
            "deadlettered": 0,
            "trips": 0,
            "traffic": 0,
            "transit": 0,
            "weather": 0,
        }
        start_time = time.time()
        batch_size = 50

        while True:
            total_handled = counts["processed"] + counts["deadlettered"]
            if max_messages and total_handled >= max_messages:
                break
            if time.time() - start_time > timeout_seconds:
                break

            records_dict = self.consumer.poll(timeout_ms=1000, max_records=batch_size)
            if not records_dict:
                if max_messages and total_handled > 0:
                    break
                continue

            for messages in records_dict.values():
                for msg in messages:
                    self._process_single_message(msg, counts)
                    if (
                        max_messages
                        and (counts["processed"] + counts["deadlettered"])
                        >= max_messages
                    ):
                        break

        return counts

    def run_forever(self) -> None:
        """Run continuous stream consumer loop until interrupted."""
        logger.info(
            "Starting StreamConsumerService on broker %s, group %s, topics: %s",
            self.broker,
            self.group_id,
            self.topics,
        )

        def handle_signal(sig: int, frame: Any) -> None:
            logger.info("Termination signal received. Exiting gracefully...")
            self._shutdown_requested = True

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)

        while not self._shutdown_requested:
            try:
                self.consume_batch(max_messages=100, timeout_seconds=2.0)
            except Exception as exc:
                logger.error("Consumer loop exception: %s", exc, exc_info=True)
                time.sleep(1.0)

        logger.info("StreamConsumerService shutdown complete.")


def main() -> None:
    """CLI entrypoint for running the stream consumer."""
    parser = argparse.ArgumentParser(
        description="Stream Consumer Service for NYC Logistics Platform"
    )
    parser.add_argument("--broker", type=str, default=None, help="Redpanda broker")
    parser.add_argument(
        "--group-id",
        type=str,
        default="logistics_stream_consumer",
        help="Consumer group ID",
    )
    parser.add_argument(
        "--max-messages", type=int, default=None, help="Stop after N messages"
    )
    parser.add_argument(
        "--timeout", type=float, default=10.0, help="Batch timeout in seconds"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    consumer = StreamConsumerService(
        broker=args.broker,
        group_id=args.group_id,
    )

    if args.max_messages:
        results = consumer.consume_batch(
            max_messages=args.max_messages, timeout_seconds=args.timeout
        )
        logger.info("Batch consumption results: %s", results)
    else:
        consumer.run_forever()


if __name__ == "__main__":
    main()
