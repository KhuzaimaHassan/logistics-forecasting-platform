"""Feast feature store module for logistics forecasting platform."""

from src.features.client import (
    CorridorDurationOnlineFeatures,
    FeastOnlineClient,
    PredictionOnlineFeatures,
    ZoneDemandOnlineFeatures,
    get_corridor_duration_online_features,
    get_online_client,
    get_zone_demand_online_features,
)
from src.features.config import (
    ensure_feast_schema,
    expand_env_placeholders,
    get_default_repo_path,
    get_feast_repo_config,
    get_feature_store,
)
from src.features.entities import (
    build_corridor_id,
    corridor_entity,
    get_all_entities,
    parse_corridor_id,
    zone_entity,
)
from src.features.materialize import (
    ALL_ONLINE_FEATURE_VIEW_NAMES,
    materialize_features,
)
from src.features.offline_extractor import (
    backfill_all_loaded_months,
    compute_corridor_duration_features_hourly,
    compute_zone_demand_features_hourly,
    extract_and_load_offline_features,
    is_us_holiday,
    load_offline_features_to_db,
)
from src.features.registry import apply_feature_definitions
from src.features.views import (
    CORRIDOR_DURATION_SCHEMA,
    ZONE_DEMAND_SCHEMA,
    corridor_duration_feature_view,
    create_file_backed_feature_views,
    get_all_feature_views,
    get_corridor_duration_postgres_source,
    get_zone_demand_postgres_source,
    zone_demand_feature_view,
)

__all__ = [
    "ensure_feast_schema",
    "expand_env_placeholders",
    "get_default_repo_path",
    "get_feast_repo_config",
    "get_feature_store",
    "apply_feature_definitions",
    "zone_entity",
    "corridor_entity",
    "build_corridor_id",
    "parse_corridor_id",
    "get_all_entities",
    "zone_demand_feature_view",
    "corridor_duration_feature_view",
    "get_all_feature_views",
    "create_file_backed_feature_views",
    "get_zone_demand_postgres_source",
    "get_corridor_duration_postgres_source",
    "ZONE_DEMAND_SCHEMA",
    "CORRIDOR_DURATION_SCHEMA",
    "compute_zone_demand_features_hourly",
    "compute_corridor_duration_features_hourly",
    "extract_and_load_offline_features",
    "backfill_all_loaded_months",
    "load_offline_features_to_db",
    "is_us_holiday",
    "materialize_features",
    "ALL_ONLINE_FEATURE_VIEW_NAMES",
    "ZoneDemandOnlineFeatures",
    "CorridorDurationOnlineFeatures",
    "PredictionOnlineFeatures",
    "FeastOnlineClient",
    "get_online_client",
    "get_zone_demand_online_features",
    "get_corridor_duration_online_features",
]
