"""Unit and integration tests for Feast entity and feature view definitions and PIT anti-leakage."""

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
from feast import FeatureStore, RepoConfig, ValueType
from feast.types import Bool, Float32, Int32, Int64

from src.features.entities import (
    build_corridor_id,
    corridor_entity,
    get_all_entities,
    parse_corridor_id,
    zone_entity,
)
from src.features.registry import apply_feature_definitions
from src.features.views import (
    DEFAULT_FEATURE_TTL,
    corridor_duration_feature_view,
    create_file_backed_feature_views,
    get_all_feature_views,
    get_corridor_duration_postgres_source,
    get_zone_demand_postgres_source,
    zone_demand_feature_view,
)


def test_zone_entity_declarations():
    """Verify zone entity configuration and join key types."""
    assert zone_entity.name == "zone"
    assert zone_entity.join_key == "zone_id"
    assert zone_entity.value_type == ValueType.INT32
    assert "NYC taxi zone ID" in zone_entity.description


def test_corridor_entity_declarations():
    """Verify corridor entity configuration and join key types."""
    assert corridor_entity.name == "corridor"
    assert corridor_entity.join_key == "corridor_id"
    assert corridor_entity.value_type == ValueType.STRING
    assert "NYC taxi corridor ID" in corridor_entity.description


def test_build_corridor_id_valid_and_invalid():
    """Verify canonical corridor ID creation and validation."""
    assert build_corridor_id(142, 236) == "142_236"
    assert build_corridor_id(1, 1) == "1_1"

    with pytest.raises(ValueError, match="Zone IDs must be positive integers"):
        build_corridor_id(0, 236)

    with pytest.raises(ValueError, match="Zone IDs must be positive integers"):
        build_corridor_id(142, -5)


def test_parse_corridor_id_valid_and_invalid():
    """Verify corridor ID parsing back to integer tuple."""
    assert parse_corridor_id("142_236") == (142, 236)
    assert parse_corridor_id("7_9") == (7, 9)

    with pytest.raises(ValueError, match="Invalid corridor_id format"):
        parse_corridor_id("142-236")

    with pytest.raises(ValueError, match="Invalid corridor_id values"):
        parse_corridor_id("142_0")

    with pytest.raises(ValueError, match="Invalid corridor_id values"):
        parse_corridor_id("abc_236")


def test_get_all_entities_list():
    """Verify helper returns all configured entities."""
    entities = get_all_entities()
    assert len(entities) == 2
    assert {e.name for e in entities} == {"zone", "corridor"}


def test_zone_demand_feature_view_schema():
    """Verify zone demand feature view fields, types, and TTL."""
    assert zone_demand_feature_view.name == "zone_demand_features"
    assert zone_demand_feature_view.ttl == DEFAULT_FEATURE_TTL
    assert len(zone_demand_feature_view.entities) == 1
    assert zone_demand_feature_view.entities[0] == "zone"

    field_map = {f.name: f.dtype for f in zone_demand_feature_view.features}
    assert field_map["pickup_count_last_15m"] == Int64
    assert field_map["pickup_count_last_1h"] == Int64
    assert field_map["pickup_count_last_24h"] == Int64
    assert field_map["pickup_count_same_hour_last_week"] == Int64
    assert field_map["hour_of_day"] == Int32
    assert field_map["day_of_week"] == Int32
    assert field_map["is_weekend"] == Bool
    assert field_map["is_holiday"] == Bool
    assert field_map["avg_temp_last_1h"] == Float32
    assert field_map["is_precipitating"] == Bool


def test_corridor_duration_feature_view_schema():
    """Verify corridor duration feature view fields, types, and TTL."""
    assert corridor_duration_feature_view.name == "corridor_duration_features"
    assert corridor_duration_feature_view.ttl == DEFAULT_FEATURE_TTL
    assert len(corridor_duration_feature_view.entities) == 1
    assert corridor_duration_feature_view.entities[0] == "corridor"

    field_map = {f.name: f.dtype for f in corridor_duration_feature_view.features}
    assert field_map["avg_duration_last_15m"] == Float32
    assert field_map["avg_duration_last_1h"] == Float32
    assert field_map["distance_km"] == Float32
    assert field_map["origin_zone_demand_pressure"] == Int64
    assert field_map["avg_traffic_speed_current"] == Float32


def test_feature_view_sources_and_timestamp_fields():
    """Verify PostgreSQL sources configure the correct anti-leakage event timestamp columns."""
    zone_src = get_zone_demand_postgres_source()
    assert zone_src.timestamp_field == "pickup_datetime"
    assert zone_src.created_timestamp_column == "created_at"
    assert "warehouse.zone_demand_features_hourly" in zone_src.get_table_query_string()

    corridor_src = get_corridor_duration_postgres_source()
    assert corridor_src.timestamp_field == "dropoff_datetime"
    assert corridor_src.created_timestamp_column == "created_at"
    assert (
        "warehouse.corridor_duration_features_hourly"
        in corridor_src.get_table_query_string()
    )


def test_get_all_feature_views_list():
    """Verify get_all_feature_views returns all views."""
    views = get_all_feature_views()
    assert len(views) == 2
    assert {v.name for v in views} == {
        "zone_demand_features",
        "corridor_duration_features",
    }


def test_apply_feature_definitions_default_registration(tmp_path):
    """Verify apply_feature_definitions registers all platform entities and views by default."""
    db_path = tmp_path / "registry.db"
    config = RepoConfig(
        registry=str(db_path),
        project="logistics_forecasting",
        provider="local",
    )
    store = FeatureStore(config=config)

    # Call with default entities=None and views=None
    apply_feature_definitions(store=store, use_sqlite_fallback=True)

    registered_entities = {e.name for e in store.list_entities()}
    registered_views = {v.name for v in store.list_feature_views()}

    assert registered_entities == {"zone", "corridor"}
    assert registered_views == {
        "zone_demand_features",
        "corridor_duration_features",
    }


def test_zone_demand_point_in_time_anti_leakage(tmp_path):
    """Test Feast PIT join strictly gates zone demand features on pickup_datetime <= T."""
    zone_parquet = tmp_path / "zone_demand.parquet"
    corridor_parquet = tmp_path / "corridor_duration.parquet"
    registry_db = tmp_path / "pit_registry.db"

    # Create test zone demand events:
    # Event 1: T - 2h (past) -> count = 20
    # Event 2: T - 30m (past, closest) -> count = 55
    # Event 3: T + 1h (FUTURE) -> count = 999
    events_df = pd.DataFrame(
        [
            {
                "zone_id": 161,
                "pickup_datetime": datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
                "created_at": datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
                "pickup_count_last_15m": 5,
                "pickup_count_last_1h": 20,
                "pickup_count_last_24h": 150,
                "pickup_count_same_hour_last_week": 18,
                "hour_of_day": 10,
                "day_of_week": 6,
                "is_weekend": True,
                "is_holiday": True,
                "avg_temp_last_1h": 4.5,
                "is_precipitating": False,
            },
            {
                "zone_id": 161,
                "pickup_datetime": datetime(2023, 1, 1, 11, 30, 0, tzinfo=timezone.utc),
                "created_at": datetime(2023, 1, 1, 11, 30, 0, tzinfo=timezone.utc),
                "pickup_count_last_15m": 15,
                "pickup_count_last_1h": 55,
                "pickup_count_last_24h": 180,
                "pickup_count_same_hour_last_week": 45,
                "hour_of_day": 11,
                "day_of_week": 6,
                "is_weekend": True,
                "is_holiday": True,
                "avg_temp_last_1h": 5.0,
                "is_precipitating": True,
            },
            {
                "zone_id": 161,
                "pickup_datetime": datetime(2023, 1, 1, 13, 0, 0, tzinfo=timezone.utc),
                "created_at": datetime(2023, 1, 1, 13, 0, 0, tzinfo=timezone.utc),
                "pickup_count_last_15m": 500,
                "pickup_count_last_1h": 999,  # Future leak trap!
                "pickup_count_last_24h": 5000,
                "pickup_count_same_hour_last_week": 800,
                "hour_of_day": 13,
                "day_of_week": 6,
                "is_weekend": True,
                "is_holiday": True,
                "avg_temp_last_1h": 10.0,
                "is_precipitating": False,
            },
        ]
    )
    events_df.to_parquet(zone_parquet)

    # Empty corridor parquet for schema initialization
    pd.DataFrame(
        columns=[
            "corridor_id",
            "dropoff_datetime",
            "created_at",
            "avg_duration_last_15m",
            "avg_duration_last_1h",
            "distance_km",
            "origin_zone_demand_pressure",
            "avg_traffic_speed_current",
        ]
    ).to_parquet(corridor_parquet)

    views = create_file_backed_feature_views(
        zone_parquet_path=str(zone_parquet),
        corridor_parquet_path=str(corridor_parquet),
    )

    store = FeatureStore(
        config=RepoConfig(
            registry=str(registry_db),
            project="pit_test",
            provider="local",
        )
    )
    store.apply([zone_entity, corridor_entity] + views)

    # Observation at T = 2023-01-01 12:00:00 (between Event 2 and Future Event 3)
    observation_df = pd.DataFrame(
        [
            {
                "zone_id": 161,
                "event_timestamp": datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            }
        ]
    )

    features_to_fetch = [
        "zone_demand_features:pickup_count_last_1h",
        "zone_demand_features:is_precipitating",
        "zone_demand_features:pickup_count_last_15m",
    ]

    retrieved_df = store.get_historical_features(
        entity_df=observation_df,
        features=features_to_fetch,
    ).to_df()

    # Assert that PIT join selected Event 2 (11:30) and did NOT leak Event 3 (13:00)
    assert retrieved_df["pickup_count_last_1h"].iloc[0] == 55
    assert retrieved_df["pickup_count_last_15m"].iloc[0] == 15
    assert bool(retrieved_df["is_precipitating"].iloc[0]) is True


def test_corridor_duration_point_in_time_anti_leakage(tmp_path):
    """Test Feast PIT join strictly gates corridor duration on dropoff_datetime <= T."""
    zone_parquet = tmp_path / "zone_demand.parquet"
    corridor_parquet = tmp_path / "corridor_duration.parquet"
    registry_db = tmp_path / "pit_corridor_registry.db"

    # Empty zone parquet for initialization
    pd.DataFrame(
        columns=[
            "zone_id",
            "pickup_datetime",
            "created_at",
            "pickup_count_last_15m",
            "pickup_count_last_1h",
            "pickup_count_last_24h",
            "pickup_count_same_hour_last_week",
            "hour_of_day",
            "day_of_week",
            "is_weekend",
            "is_holiday",
            "avg_temp_last_1h",
            "is_precipitating",
        ]
    ).to_parquet(zone_parquet)

    # Corridor Events:
    # Event 1: Completed trip dropoff at 11:45 (past) -> duration = 720.0s, pressure = 30
    # Event 2: Trip in-progress at T=12:00, completed dropoff at 12:20 (FUTURE) -> duration = 2500.0s, pressure = 999
    corridor_events_df = pd.DataFrame(
        [
            {
                "corridor_id": "142_236",
                "dropoff_datetime": datetime(
                    2023, 1, 1, 11, 45, 0, tzinfo=timezone.utc
                ),
                "created_at": datetime(2023, 1, 1, 11, 45, 0, tzinfo=timezone.utc),
                "avg_duration_last_15m": 710.0,
                "avg_duration_last_1h": 720.0,
                "distance_km": 8.4,
                "origin_zone_demand_pressure": 30,
                "avg_traffic_speed_current": 28.5,
            },
            {
                "corridor_id": "142_236",
                "dropoff_datetime": datetime(
                    2023, 1, 1, 12, 20, 0, tzinfo=timezone.utc
                ),
                "created_at": datetime(2023, 1, 1, 12, 20, 0, tzinfo=timezone.utc),
                "avg_duration_last_15m": 2400.0,
                "avg_duration_last_1h": 2500.0,  # In-progress trip completed after T
                "distance_km": 8.4,
                "origin_zone_demand_pressure": 999,
                "avg_traffic_speed_current": 12.0,
            },
        ]
    )
    corridor_events_df.to_parquet(corridor_parquet)

    views = create_file_backed_feature_views(
        zone_parquet_path=str(zone_parquet),
        corridor_parquet_path=str(corridor_parquet),
    )

    store = FeatureStore(
        config=RepoConfig(
            registry=str(registry_db),
            project="pit_corridor_test",
            provider="local",
        )
    )
    store.apply([zone_entity, corridor_entity] + views)

    # Observation at T = 2023-01-01 12:00:00 (trip 2 has not dropped off yet)
    observation_df = pd.DataFrame(
        [
            {
                "corridor_id": "142_236",
                "event_timestamp": datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            }
        ]
    )

    features_to_fetch = [
        "corridor_duration_features:avg_duration_last_1h",
        "corridor_duration_features:distance_km",
        "corridor_duration_features:origin_zone_demand_pressure",
    ]

    retrieved_df = store.get_historical_features(
        entity_df=observation_df,
        features=features_to_fetch,
    ).to_df()

    # Assert that PIT join selected Event 1 (11:45 dropoff) and excluded Event 2 (12:20 dropoff)
    assert retrieved_df["avg_duration_last_1h"].iloc[0] == pytest.approx(
        720.0, abs=1e-2
    )
    assert retrieved_df["distance_km"].iloc[0] == pytest.approx(8.4, abs=1e-2)
    assert retrieved_df["origin_zone_demand_pressure"].iloc[0] == 30


def test_point_in_time_ttl_expiry(tmp_path):
    """Test records older than FeatureView TTL are not matched during PIT retrieval."""
    zone_parquet = tmp_path / "zone_demand.parquet"
    corridor_parquet = tmp_path / "corridor_duration.parquet"
    registry_db = tmp_path / "pit_ttl_registry.db"

    # Event occurred 20 days prior
    event_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    observation_time = event_time + timedelta(days=20)  # Exceeds 14 day TTL

    events_df = pd.DataFrame(
        [
            {
                "zone_id": 42,
                "pickup_datetime": event_time,
                "created_at": event_time,
                "pickup_count_last_15m": 10,
                "pickup_count_last_1h": 40,
                "pickup_count_last_24h": 200,
                "pickup_count_same_hour_last_week": 35,
                "hour_of_day": 12,
                "day_of_week": 6,
                "is_weekend": True,
                "is_holiday": True,
                "avg_temp_last_1h": 5.0,
                "is_precipitating": False,
            }
        ]
    )
    events_df.to_parquet(zone_parquet)

    pd.DataFrame(
        columns=[
            "corridor_id",
            "dropoff_datetime",
            "created_at",
            "avg_duration_last_15m",
            "avg_duration_last_1h",
            "distance_km",
            "origin_zone_demand_pressure",
            "avg_traffic_speed_current",
        ]
    ).to_parquet(corridor_parquet)

    # 14-day TTL
    views = create_file_backed_feature_views(
        zone_parquet_path=str(zone_parquet),
        corridor_parquet_path=str(corridor_parquet),
        ttl=timedelta(days=14),
    )

    store = FeatureStore(
        config=RepoConfig(
            registry=str(registry_db),
            project="pit_ttl_test",
            provider="local",
        )
    )
    store.apply([zone_entity, corridor_entity] + views)

    observation_df = pd.DataFrame(
        [
            {
                "zone_id": 42,
                "event_timestamp": observation_time,
            }
        ]
    )

    retrieved_df = store.get_historical_features(
        entity_df=observation_df,
        features=["zone_demand_features:pickup_count_last_1h"],
    ).to_df()

    # Outside TTL window, Feast does not join expired records
    assert retrieved_df.empty or pd.isna(retrieved_df["pickup_count_last_1h"].iloc[0])
