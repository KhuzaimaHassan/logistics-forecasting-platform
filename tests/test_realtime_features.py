"""Unit and integration tests for Feast streaming push and reconciliation flow (M4-4, ADR-018)."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest
from feast import FeatureStore, RepoConfig
from feast.data_source import PushMode
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session

from src.common.kafka_utils import TOPIC_TRIP_EVENTS
from src.common.models import Base, TaxiZone
from src.features.client import (
    FeastOnlineClient,
    get_corridor_duration_online_features,
    get_zone_demand_online_features,
)
from src.features.entities import corridor_entity, zone_entity
from src.features.materialize import materialize_features
from src.features.push_sources import (
    CORRIDOR_DURATION_PUSH_SCHEMA,
    ZONE_DEMAND_PUSH_SCHEMA,
    create_file_backed_push_views,
    get_push_feature_views,
)
from src.features.views import (
    create_file_backed_feature_views,
    get_all_feature_views,
)
from src.orchestration.flows.realtime_reconciliation_flow import (
    materialize_online_store_task,
    realtime_reconciliation_flow,
    reconcile_offline_features_task,
)
from src.transform.stream_consumer import (
    StreamConsumerService,
    StreamFeatureAggregator,
)


@pytest.fixture
def test_feature_store(tmp_path: Path):
    """Fixture providing local SQLite-backed Feast store with push views applied."""
    zone_pq = tmp_path / "zone_features.parquet"
    corridor_pq = tmp_path / "corridor_features.parquet"
    registry_db = tmp_path / "registry.db"
    online_db = tmp_path / "online.db"

    t_base = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    df_zone = pd.DataFrame(
        [
            {
                "zone_id": 161,
                "pickup_datetime": t_base,
                "created_at": t_base,
                "pickup_count_last_15m": 5,
                "pickup_count_last_1h": 20,
                "pickup_count_last_24h": 50,
                "pickup_count_same_hour_last_week": 15,
                "hour_of_day": 12,
                "day_of_week": 6,
                "is_weekend": True,
                "is_holiday": True,  # Jan 1 is New Year's Day
                "avg_temp_last_1h": 10.0,
                "is_precipitating": False,
            }
        ]
    )
    df_zone.to_parquet(zone_pq)

    df_corridor = pd.DataFrame(
        [
            {
                "corridor_id": "161_236",
                "dropoff_datetime": t_base,
                "created_at": t_base,
                "avg_duration_last_15m": 900.0,
                "avg_duration_last_1h": 900.0,
                "distance_km": 4.5,
                "origin_zone_demand_pressure": 20,
                "avg_traffic_speed_current": 20.0,
            }
        ]
    )
    df_corridor.to_parquet(corridor_pq)

    views = create_file_backed_feature_views(str(zone_pq), str(corridor_pq))
    push_views = create_file_backed_push_views(str(zone_pq), str(corridor_pq))

    repo_cfg = RepoConfig(
        registry=str(registry_db),
        project="test_realtime_m44",
        provider="local",
        online_store={"type": "sqlite", "path": str(online_db)},
    )
    store = FeatureStore(config=repo_cfg)
    store.apply([zone_entity, corridor_entity, *views, *push_views])
    return {
        "store": store,
        "views": views,
        "push_views": push_views,
        "base_time": t_base,
    }


def test_push_sources_and_views_definitions() -> None:
    """Validate Feast push schemas, push source names, and view registry integration."""
    push_views = get_push_feature_views()
    assert len(push_views) == 2

    zone_push_view = next(
        v for v in push_views if v.name == "zone_demand_features_push"
    )
    corridor_push_view = next(
        v for v in push_views if v.name == "corridor_duration_features_push"
    )

    assert zone_push_view.entities == ["zone"]
    assert corridor_push_view.entities == ["corridor"]
    assert zone_push_view.batch_source is not None
    assert corridor_push_view.batch_source is not None

    zone_field_names = {f.name for f in ZONE_DEMAND_PUSH_SCHEMA}
    assert "pickup_count_last_15m" in zone_field_names
    assert "pickup_count_last_1h" in zone_field_names
    assert "avg_temp_last_1h" in zone_field_names
    assert "is_precipitating" in zone_field_names

    corridor_field_names = {f.name for f in CORRIDOR_DURATION_PUSH_SCHEMA}
    assert "avg_duration_last_15m" in corridor_field_names
    assert "avg_duration_last_1h" in corridor_field_names
    assert "avg_traffic_speed_current" in corridor_field_names
    assert "origin_zone_demand_pressure" in corridor_field_names

    # Check views.py get_all_feature_views with include_push flag
    all_with_push = get_all_feature_views(include_push=True)
    all_without_push = get_all_feature_views(include_push=False)
    assert len(all_with_push) == 4
    assert len(all_without_push) == 2


def test_stream_feature_aggregator_invariants() -> None:
    """Verify StreamFeatureAggregator adheres strictly to temporal invariants."""
    agg = StreamFeatureAggregator()

    t0 = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    t_minus_45m = t0 - timedelta(minutes=45)
    t_minus_10m = t0 - timedelta(minutes=10)
    t_minus_2m = t0 - timedelta(minutes=2)

    # Ingest weather and traffic snapshots
    agg.update_weather(temp_c=4.5, is_precipitating=True)
    agg.update_traffic(segment_id=101, speed_kmh=22.0)
    agg.update_traffic(segment_id=102, speed_kmh=28.0)
    assert agg.latest_avg_speed_kmh == 25.0

    # Trip 1 at T - 45m: in 1h window, outside 15m window
    z_df1, c_df1 = agg.record_trip(
        pickup_zone_id=161,
        dropoff_zone_id=236,
        pickup_datetime=t_minus_45m,
        dropoff_datetime=t_minus_45m + timedelta(minutes=10),
        trip_duration_seconds=600.0,
    )
    assert z_df1["pickup_count_last_15m"].iloc[0] == 1
    assert z_df1["pickup_count_last_1h"].iloc[0] == 1

    # Trip 2 at T - 10m: inside both 15m and 1h window
    z_df2, c_df2 = agg.record_trip(
        pickup_zone_id=161,
        dropoff_zone_id=236,
        pickup_datetime=t_minus_10m,
        dropoff_datetime=t_minus_10m + timedelta(minutes=15),
        trip_duration_seconds=900.0,
    )
    assert z_df2["pickup_count_last_15m"].iloc[0] == 1
    assert z_df2["pickup_count_last_1h"].iloc[0] == 2
    assert z_df2["avg_temp_last_1h"].iloc[0] == 4.5
    assert bool(z_df2["is_precipitating"].iloc[0]) is True
    assert bool(z_df2["is_holiday"].iloc[0]) is True  # 2023-01-01 is US Holiday

    # Trip 3 at T - 2m: inside both 15m and 1h window
    z_df3, c_df3 = agg.record_trip(
        pickup_zone_id=161,
        dropoff_zone_id=236,
        pickup_datetime=t_minus_2m,
        dropoff_datetime=t_minus_2m + timedelta(minutes=5),
        trip_duration_seconds=300.0,
    )
    # Pickup counts: 2 in last 15m (t_minus_10m and t_minus_2m), 3 in last 1h
    assert z_df3["pickup_count_last_15m"].iloc[0] == 2
    assert z_df3["pickup_count_last_1h"].iloc[0] == 3

    # Corridor durations:
    # 2 trips completed within last 15m: 900s and 300s -> average 600.0s
    assert c_df3["avg_duration_last_15m"].iloc[0] == 600.0
    # 3 trips completed in 1h: 600s, 900s, 300s -> average 600.0s
    assert c_df3["avg_duration_last_1h"].iloc[0] == 600.0
    assert c_df3["avg_traffic_speed_current"].iloc[0] == 25.0
    assert c_df3["origin_zone_demand_pressure"].iloc[0] == 3


def test_online_client_push_coalescing(test_feature_store) -> None:
    """Test online client coalesces real-time push values with batch historical features."""
    store = test_feature_store["store"]
    views = test_feature_store["views"]
    t_base = test_feature_store["base_time"]

    # 1. Materialize initial batch features into online store
    materialize_features(
        start_date=t_base - timedelta(hours=1),
        end_date=t_base + timedelta(hours=1),
        feature_views=[v.name for v in views],
        store=store,
        incremental=False,
    )

    client = FeastOnlineClient(store=store)

    # 2. Pre-push: client reads batch features
    pre_zones = client.get_zone_demand_features([161], use_push_features=True)
    assert pre_zones[0].pickup_count_last_15m == 5
    assert pre_zones[0].pickup_count_last_1h == 20
    assert pre_zones[0].pickup_count_last_24h == 50

    pre_corridor = client.get_corridor_duration_features(
        ["161_236"], use_push_features=True
    )
    assert pre_corridor[0].avg_duration_last_15m == 900.0
    assert pre_corridor[0].distance_km == 4.5

    # 3. Simulate real-time streaming push update
    now = datetime.now(timezone.utc)
    zone_push_df = pd.DataFrame(
        [
            {
                "zone_id": 161,
                "pickup_datetime": now,
                "created_at": now,
                "pickup_count_last_15m": 12,
                "pickup_count_last_1h": 35,
                "hour_of_day": now.hour,
                "day_of_week": now.weekday(),
                "is_weekend": now.weekday() >= 5,
                "is_holiday": False,
                "avg_temp_last_1h": 18.5,
                "is_precipitating": True,
            }
        ]
    )
    store.push("zone_demand_push_source", zone_push_df, to=PushMode.ONLINE)

    corridor_push_df = pd.DataFrame(
        [
            {
                "corridor_id": "161_236",
                "dropoff_datetime": now,
                "created_at": now,
                "avg_duration_last_15m": 720.0,
                "avg_duration_last_1h": 850.0,
                "avg_traffic_speed_current": 35.0,
                "origin_zone_demand_pressure": 35,
            }
        ]
    )
    store.push("corridor_duration_push_source", corridor_push_df, to=PushMode.ONLINE)

    # 4. Post-push: client coalesces pushed features over batch
    post_zones = client.get_zone_demand_features([161], use_push_features=True)
    assert post_zones[0].pickup_count_last_15m == 12  # Pushed value
    assert post_zones[0].pickup_count_last_1h == 35  # Pushed value
    assert post_zones[0].avg_temp_last_1h == 18.5  # Pushed value
    assert post_zones[0].pickup_count_last_24h == 50  # Batch fallback preserved!
    assert post_zones[0].cache_hit is True

    post_corridor = client.get_corridor_duration_features(
        ["161_236"], use_push_features=True
    )
    assert post_corridor[0].avg_duration_last_15m == 720.0  # Pushed value
    assert post_corridor[0].avg_traffic_speed_current == 35.0  # Pushed value
    assert post_corridor[0].distance_km == 4.5  # Batch fallback preserved!
    assert post_corridor[0].cache_hit is True

    # 5. Verify batch-only override when use_push_features=False
    batch_only_zones = client.get_zone_demand_features([161], use_push_features=False)
    assert batch_only_zones[0].pickup_count_last_15m == 5
    assert batch_only_zones[0].pickup_count_last_1h == 20

    # 6. Test top-level functions with push coalescing
    top_z = get_zone_demand_online_features(161, store=store, use_push_features=True)
    assert top_z[0].pickup_count_last_15m == 12

    top_c = get_corridor_duration_online_features(
        "161_236", store=store, use_push_features=True
    )
    assert top_c[0].avg_duration_last_15m == 720.0


def test_stream_consumer_push_and_resilience(test_feature_store) -> None:
    """Test StreamConsumerService feature push on message processing and best-effort resilience."""
    store = test_feature_store["store"]

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

    # Initialize taxi zones in DB
    with Session(bind=engine) as s:
        s.add(
            TaxiZone(
                zone_id=161,
                borough="Manhattan",
                zone_name="Midtown Center",
                centroid_lat=40.7,
                centroid_lon=-73.9,
            )
        )
        s.add(
            TaxiZone(
                zone_id=236,
                borough="Manhattan",
                zone_name="Upper East Side North",
                centroid_lat=40.7,
                centroid_lon=-73.9,
            )
        )
        s.commit()

    mock_consumer = MagicMock()
    mock_producer = MagicMock()

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "src.transform.stream_consumer.ensure_topics_exist",
            lambda **kwargs: None,
        )
        service = StreamConsumerService(
            consumer=mock_consumer,
            producer=mock_producer,
            engine=engine,
            feature_store=store,
            enable_feature_push=True,
        )

    t_trip = datetime(2023, 1, 1, 14, 0, 0, tzinfo=timezone.utc)
    trip_payload = {
        "vendor_id": 1,
        "cab_type": "yellow",
        "pickup_zone_id": 161,
        "dropoff_zone_id": 236,
        "pickup_datetime": t_trip.isoformat(),
        "dropoff_datetime": (t_trip + timedelta(minutes=15)).isoformat(),
        "trip_distance_km": 3.5,
        "passenger_count": 1,
        "fare_amount": 15.0,
        "tip_amount": 3.0,
        "total_amount": 20.0,
        "trip_duration_seconds": 900.0,
    }

    mock_msg = MagicMock()
    mock_msg.topic = TOPIC_TRIP_EVENTS
    mock_msg.value = trip_payload
    mock_msg.partition = 0
    mock_msg.offset = 100

    counts = {
        "processed": 0,
        "deadlettered": 0,
        "trips": 0,
        "traffic": 0,
        "transit": 0,
        "weather": 0,
    }

    # Process message: DB write + best-effort push
    service._process_single_message(mock_msg, counts)

    assert counts["processed"] == 1
    assert counts["trips"] == 1
    mock_consumer.commit.assert_called_once()

    # Query online store to verify push succeeded
    client = FeastOnlineClient(store=store)
    res = client.get_zone_demand_features([161], use_push_features=True)
    assert res[0].pickup_count_last_15m == 1
    assert res[0].pickup_count_last_1h == 1

    # Test best-effort push resilience: simulate store.push failure
    store.push = MagicMock(side_effect=RuntimeError("Redis connection refused"))
    mock_consumer.commit.reset_mock()

    trip_payload["fare_amount"] = 25.0
    trip_payload["total_amount"] = 30.0
    service._process_single_message(mock_msg, counts)

    # Offset MUST commit even if push failed, relying on reconciliation flow
    assert counts["processed"] == 2
    assert counts["deadlettered"] == 0
    mock_consumer.commit.assert_called_once()


def test_realtime_reconciliation_flow(tmp_path: Path) -> None:
    """Test realtime reconciliation Prefect flow execution and task outputs."""
    # Test tasks directly
    mock_engine = MagicMock()
    mock_store = MagicMock()

    # Test reconcile_offline_features_task with mocked extract
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "src.orchestration.flows.realtime_reconciliation_flow.extract_and_load_offline_features",
            lambda **kwargs: (5, 12),
        )
        res_task = reconcile_offline_features_task.fn(
            engine=mock_engine,
            start_datetime=datetime(2023, 1, 1, 10, 0, tzinfo=timezone.utc),
            end_datetime=datetime(2023, 1, 1, 12, 0, tzinfo=timezone.utc),
        )
        assert res_task["zone_rows_loaded"] == 5
        assert res_task["corridor_rows_loaded"] == 12

    # Test materialize_online_store_task
    mock_res = {
        "status": "success",
        "mode": "incremental",
        "end_date": "2023-01-01T12:00:00Z",
    }
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "src.orchestration.flows.realtime_reconciliation_flow.materialize_features",
            lambda **kwargs: mock_res,
        )
        mat_out = materialize_online_store_task.fn(
            end_date=datetime(2023, 1, 1, 12, 0, tzinfo=timezone.utc),
            store=mock_store,
        )
        assert mat_out["status"] == "success"

    # Test full flow invocation via .fn
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "src.orchestration.flows.realtime_reconciliation_flow.extract_and_load_offline_features",
            lambda **kwargs: (10, 25),
        )
        mp.setattr(
            "src.orchestration.flows.realtime_reconciliation_flow.materialize_features",
            lambda **kwargs: mock_res,
        )

        flow_out = realtime_reconciliation_flow.fn(
            lookback_hours=2,
            end_datetime=datetime(2023, 1, 1, 12, 0, tzinfo=timezone.utc),
            engine=mock_engine,
            store=mock_store,
        )
        assert flow_out["status"] == "success"
        assert flow_out["zone_rows_loaded"] == 10
        assert flow_out["corridor_rows_loaded"] == 25
        assert flow_out["materialization"]["status"] == "success"
