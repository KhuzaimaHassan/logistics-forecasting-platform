"""Feast Feature View definitions for zone demand and corridor duration forecasting."""

from datetime import timedelta
from typing import List, Optional

from feast import FeatureView, Field, FileSource
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    PostgreSQLSource,
)
from feast.types import Bool, Float32, Int32, Int64

from src.features.entities import corridor_entity, zone_entity

# Default TTL for feature lookups (14 days lookback buffer)
DEFAULT_FEATURE_TTL = timedelta(days=14)

# ---------------------------------------------------------------------------
# Offline Data Sources (PostgreSQL Warehouse Tables)
# ---------------------------------------------------------------------------


def get_zone_demand_postgres_source(
    table_name: str = "warehouse.zone_demand_features_hourly",
) -> PostgreSQLSource:
    """Construct Feast PostgreSQLSource for zone demand features.

    Point-in-time anti-leakage rule:
    timestamp_field='pickup_datetime' ensures observation at time T strictly
    incorporates trips with pickup_datetime <= T.
    """
    return PostgreSQLSource(
        name="zone_demand_features_source",
        query=f"SELECT * FROM {table_name}",
        timestamp_field="pickup_datetime",
        created_timestamp_column="created_at",
    )


def get_corridor_duration_postgres_source(
    table_name: str = "warehouse.corridor_duration_features_hourly",
) -> PostgreSQLSource:
    """Construct Feast PostgreSQLSource for corridor trip duration features.

    Point-in-time anti-leakage rule:
    timestamp_field='dropoff_datetime' ensures observation at time T strictly
    incorporates completed trips with dropoff_datetime <= T (trips in progress
    with dropoff_datetime > T are excluded from duration metrics).
    """
    return PostgreSQLSource(
        name="corridor_duration_features_source",
        query=f"SELECT * FROM {table_name}",
        timestamp_field="dropoff_datetime",
        created_timestamp_column="created_at",
    )


# ---------------------------------------------------------------------------
# Schema Field Definitions
# ---------------------------------------------------------------------------

ZONE_DEMAND_SCHEMA: List[Field] = [
    Field(
        name="pickup_count_last_15m",
        dtype=Int64,
        description="Rolling 15-minute pickup count in zone",
    ),
    Field(
        name="pickup_count_last_1h",
        dtype=Int64,
        description="Rolling 1-hour pickup count in zone",
    ),
    Field(
        name="pickup_count_last_24h",
        dtype=Int64,
        description="Rolling 24-hour pickup count in zone",
    ),
    Field(
        name="pickup_count_same_hour_last_week",
        dtype=Int64,
        description="Seasonal baseline pickup count for the same hour 7 days prior",
    ),
    Field(
        name="hour_of_day",
        dtype=Int32,
        description="Hour of the day (0-23)",
    ),
    Field(
        name="day_of_week",
        dtype=Int32,
        description="Day of the week (0=Monday, 6=Sunday)",
    ),
    Field(
        name="is_weekend",
        dtype=Bool,
        description="True if day is Saturday or Sunday",
    ),
    Field(
        name="is_holiday",
        dtype=Bool,
        description="True if date is a recognized NYC holiday",
    ),
    Field(
        name="avg_temp_last_1h",
        dtype=Float32,
        description="Average ambient temperature in Celsius over the last hour (nullable/default)",
    ),
    Field(
        name="is_precipitating",
        dtype=Bool,
        description="True if precipitation (rain/snow) was recorded in the last hour",
    ),
]

CORRIDOR_DURATION_SCHEMA: List[Field] = [
    Field(
        name="avg_duration_last_15m",
        dtype=Float32,
        description="Rolling 15-minute average trip duration in seconds for completed trips",
    ),
    Field(
        name="avg_duration_last_1h",
        dtype=Float32,
        description="Rolling 1-hour average trip duration in seconds for completed trips",
    ),
    Field(
        name="distance_km",
        dtype=Float32,
        description="Static / baseline centroid-to-centroid driving distance in kilometers",
    ),
    Field(
        name="origin_zone_demand_pressure",
        dtype=Int64,
        description="Raw rolling pickup count of origin zone from zone_demand_features (ADR-014)",
    ),
    Field(
        name="avg_traffic_speed_current",
        dtype=Float32,
        description="Average corridor traffic speed in km/h (nullable/default until live feed)",
    ),
]

# ---------------------------------------------------------------------------
# Default Production Feature Views (PostgreSQL-backed)
# ---------------------------------------------------------------------------

zone_demand_feature_view = FeatureView(
    name="zone_demand_features",
    entities=[zone_entity],
    ttl=DEFAULT_FEATURE_TTL,
    schema=ZONE_DEMAND_SCHEMA,
    source=get_zone_demand_postgres_source(),
    description="Rolling demand metrics and calendar/weather features for NYC taxi zones (pickup-gated)",
)

corridor_duration_feature_view = FeatureView(
    name="corridor_duration_features",
    entities=[corridor_entity],
    ttl=DEFAULT_FEATURE_TTL,
    schema=CORRIDOR_DURATION_SCHEMA,
    source=get_corridor_duration_postgres_source(),
    description="Rolling trip duration and topology features for NYC taxi corridors (dropoff-gated)",
)


def get_all_feature_views() -> List[FeatureView]:
    """Return all production FeatureView definitions configured for the platform.

    Returns:
        List of FeatureView objects [zone_demand_feature_view, corridor_duration_feature_view].
    """
    return [zone_demand_feature_view, corridor_duration_feature_view]


# ---------------------------------------------------------------------------
# Factory Helper for File/Testing Backed Feature Views
# ---------------------------------------------------------------------------


def create_file_backed_feature_views(
    zone_parquet_path: str,
    corridor_parquet_path: str,
    ttl: Optional[timedelta] = None,
) -> List[FeatureView]:
    """Construct FeatureViews backed by local Parquet FileSources for testing.

    Args:
        zone_parquet_path: Path to Parquet file containing zone demand records.
        corridor_parquet_path: Path to Parquet file containing corridor duration records.
        ttl: Optional TTL override (defaults to DEFAULT_FEATURE_TTL).

    Returns:
        List of FeatureViews configured with FileSources.
    """
    effective_ttl = ttl or DEFAULT_FEATURE_TTL

    zone_source = FileSource(
        name="zone_demand_file_source",
        path=zone_parquet_path,
        timestamp_field="pickup_datetime",
        created_timestamp_column="created_at",
    )

    corridor_source = FileSource(
        name="corridor_duration_file_source",
        path=corridor_parquet_path,
        timestamp_field="dropoff_datetime",
        created_timestamp_column="created_at",
    )

    return [
        FeatureView(
            name="zone_demand_features",
            entities=[zone_entity],
            ttl=effective_ttl,
            schema=ZONE_DEMAND_SCHEMA,
            source=zone_source,
            description="File-backed zone demand features for testing",
        ),
        FeatureView(
            name="corridor_duration_features",
            entities=[corridor_entity],
            ttl=effective_ttl,
            schema=CORRIDOR_DURATION_SCHEMA,
            source=corridor_source,
            description="File-backed corridor duration features for testing",
        ),
    ]
