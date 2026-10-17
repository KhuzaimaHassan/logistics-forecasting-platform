"""Explicit production safeguards shared by cloud serving and its offline tests."""

import os


def is_cloud_deployment() -> bool:
    return os.getenv("DEPLOYMENT_MODE", "local").lower() == "cloud"


def missing_feature_fields(feature) -> list[str]:
    """Require actual model inputs; zero is a valid observed measurement."""
    if hasattr(feature, "zone_id"):
        required = (
            "pickup_count_last_15m",
            "pickup_count_last_1h",
            "pickup_count_last_24h",
            "pickup_count_same_hour_last_week",
            "hour_of_day",
            "day_of_week",
            "is_weekend",
            "is_holiday",
        )
    else:
        required = (
            "avg_duration_last_15m",
            "avg_duration_last_1h",
            "distance_km",
            "origin_zone_demand_pressure",
            "observed_at",
        )
    return [name for name in required if getattr(feature, name, None) is None]
