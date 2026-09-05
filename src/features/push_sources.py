"""Feast PushSource and push-enabled FeatureView definitions for real-time streaming updates.

Provides dedicated push feature namespaces:
- zone_demand_features_push: real-time rolling pickup counts (15m, 1h) and weather signals
- corridor_duration_features_push: real-time corridor duration and traffic speed signals

Decoupled from batch hourly feature views (ADR-018) to prevent namespace collisions in Redis.
"""

from datetime import timedelta
from typing import List, Optional

from feast import FeatureView, Field, FileSource
from feast.data_source import PushSource
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    PostgreSQLSource,
)
from feast.types import Bool, Float32, Int32, Int64

from src.features.entities import corridor_entity, zone_entity
from src.features.views import (
    DEFAULT_FEATURE_TTL,
    get_corridor_duration_postgres_source,
    get_zone_demand_postgres_source,
)

# ---------------------------------------------------------------------------
# Schema Field Definitions for Streaming Pushes
# ---------------------------------------------------------------------------

ZONE_DEMAND_PUSH_SCHEMA: List[Field] = [
    Field(
        name="pickup_count_last_15m",
        dtype=Int64,
        description="Streaming 15-minute rolling pickup count in zone",
    ),
    Field(
        name="pickup_count_last_1h",
        dtype=Int64,
        description="Streaming 1-hour rolling pickup count in zone",
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
        description="True if date is a recognized US/NYC holiday",
    ),
    Field(
        name="avg_temp_last_1h",
        dtype=Float32,
        description="Current ambient temperature in Celsius from weather stream",
    ),
    Field(
        name="is_precipitating",
        dtype=Bool,
        description="True if current weather stream reports active precipitation",
    ),
]

CORRIDOR_DURATION_PUSH_SCHEMA: List[Field] = [
    Field(
        name="avg_duration_last_15m",
        dtype=Float32,
        description="Streaming 15-minute rolling average trip duration in seconds",
    ),
    Field(
        name="avg_duration_last_1h",
        dtype=Float32,
        description="Streaming 1-hour rolling average trip duration in seconds",
    ),
    Field(
        name="avg_traffic_speed_current",
        dtype=Float32,
        description="Current corridor traffic speed in km/h from traffic stream",
    ),
    Field(
        name="origin_zone_demand_pressure",
        dtype=Int64,
        description="Streaming pickup count of origin zone (ADR-014)",
    ),
]


# ---------------------------------------------------------------------------
# Push Source Definitions
# ---------------------------------------------------------------------------


def get_zone_demand_push_source(
    batch_source: Optional[PostgreSQLSource] = None,
) -> PushSource:
    """Construct PushSource for real-time zone demand feature updates."""
    bs = batch_source or get_zone_demand_postgres_source()
    return PushSource(
        name="zone_demand_push_source",
        batch_source=bs,
        description="Push source for streaming TLC trip events and weather updates",
    )


def get_corridor_duration_push_source(
    batch_source: Optional[PostgreSQLSource] = None,
) -> PushSource:
    """Construct PushSource for real-time corridor duration feature updates."""
    bs = batch_source or get_corridor_duration_postgres_source()
    return PushSource(
        name="corridor_duration_push_source",
        batch_source=bs,
        description="Push source for streaming completed corridor trips and traffic speed",
    )


# ---------------------------------------------------------------------------
# Default Production Push Feature Views
# ---------------------------------------------------------------------------

zone_demand_features_push_view = FeatureView(
    name="zone_demand_features_push",
    entities=[zone_entity],
    ttl=DEFAULT_FEATURE_TTL,
    schema=ZONE_DEMAND_PUSH_SCHEMA,
    source=get_zone_demand_push_source(),
    description="Sub-second streaming push feature view for taxi zone demand (ADR-018)",
)

corridor_duration_features_push_view = FeatureView(
    name="corridor_duration_features_push",
    entities=[corridor_entity],
    ttl=DEFAULT_FEATURE_TTL,
    schema=CORRIDOR_DURATION_PUSH_SCHEMA,
    source=get_corridor_duration_push_source(),
    description="Sub-second streaming push feature view for corridor ETA (ADR-018)",
)


def get_push_feature_views() -> List[FeatureView]:
    """Return list of all streaming push FeatureViews."""
    return [zone_demand_features_push_view, corridor_duration_features_push_view]


# ---------------------------------------------------------------------------
# Factory Helper for File/Testing Backed Push Feature Views
# ---------------------------------------------------------------------------


def create_file_backed_push_views(
    zone_parquet_path: str,
    corridor_parquet_path: str,
    ttl: Optional[timedelta] = None,
) -> List[FeatureView]:
    """Construct file-backed Push FeatureViews for local integration testing.

    Args:
        zone_parquet_path: Path to parquet file for zone demand batch fallback.
        corridor_parquet_path: Path to parquet file for corridor duration batch fallback.
        ttl: Optional TTL override.

    Returns:
        List of FeatureViews configured with PushSource backed by FileSources.
    """
    effective_ttl = ttl or DEFAULT_FEATURE_TTL

    zone_batch_source = FileSource(
        name="zone_demand_push_batch_source",
        path=zone_parquet_path,
        timestamp_field="pickup_datetime",
        created_timestamp_column="created_at",
    )

    corridor_batch_source = FileSource(
        name="corridor_duration_push_batch_source",
        path=corridor_parquet_path,
        timestamp_field="dropoff_datetime",
        created_timestamp_column="created_at",
    )

    zone_push_source = PushSource(
        name="zone_demand_push_source",
        batch_source=zone_batch_source,
    )

    corridor_push_source = PushSource(
        name="corridor_duration_push_source",
        batch_source=corridor_batch_source,
    )

    return [
        FeatureView(
            name="zone_demand_features_push",
            entities=[zone_entity],
            ttl=effective_ttl,
            schema=ZONE_DEMAND_PUSH_SCHEMA,
            source=zone_push_source,
            description="File-backed zone demand push feature view for testing",
        ),
        FeatureView(
            name="corridor_duration_features_push",
            entities=[corridor_entity],
            ttl=effective_ttl,
            schema=CORRIDOR_DURATION_PUSH_SCHEMA,
            source=corridor_push_source,
            description="File-backed corridor duration push feature view for testing",
        ),
    ]
