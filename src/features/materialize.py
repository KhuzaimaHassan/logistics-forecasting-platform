"""Feast online store materialization engine for Redis synchronization.

Syncs computed feature aggregations from PostgreSQL offline store (warehouse tables)
into Redis online store for sub-10ms inference retrieval.
"""

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from feast import FeatureStore

from src.features.config import get_feature_store
from src.features.views import (
    corridor_duration_feature_view,
    zone_demand_feature_view,
)

logger = logging.getLogger(__name__)

ALL_ONLINE_FEATURE_VIEW_NAMES = [
    zone_demand_feature_view.name,
    corridor_duration_feature_view.name,
]


def materialize_features(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    feature_views: Optional[List[str]] = None,
    incremental: bool = False,
    store: Optional[FeatureStore] = None,
    repo_path: Optional[Union[str, Path]] = None,
    use_sqlite_fallback: bool = False,
) -> Dict[str, Any]:
    """Materialize feature views from the offline store into the online store.

    Semantics:
    - Explicit window (incremental=False): Runs store.materialize(start_date, end_date).
      Deterministic and idempotent: re-running on the same window queries offline store,
      re-writes Redis keys with identical serialized protobuf payloads, and refreshes the
      24-hour key TTL.
    - Incremental (incremental=True): Runs store.materialize_incremental(end_date).
      Starts from the registry's most_recent_end_time (or end_date - TTL if first run).
      Re-running with the same end_date is a no-op since interval length is zero.

    Args:
        start_date: Beginning of time window for explicit materialization (required if incremental=False).
        end_date: End of time window (defaults to current UTC time if None).
        feature_views: Optional subset of feature view names. Defaults to all online views.
        incremental: If True, uses store.materialize_incremental.
        store: Optional pre-configured Feast FeatureStore instance.
        repo_path: Path to feature repository directory.
        use_sqlite_fallback: If True, uses local test sqlite fallback store.

    Returns:
        Dictionary containing execution summary and timing metrics.
    """
    if store is None:
        store = get_feature_store(
            repo_path=repo_path,
            use_sqlite_fallback=use_sqlite_fallback,
        )

    target_views = feature_views or ALL_ONLINE_FEATURE_VIEW_NAMES
    effective_end = end_date or datetime.now(timezone.utc)

    # Ensure UTC timezone awareness
    if effective_end.tzinfo is None:
        effective_end = effective_end.replace(tzinfo=timezone.utc)

    t0 = time.perf_counter()

    if incremental:
        logger.info(
            "Starting incremental materialization for feature views %s up to %s",
            target_views,
            effective_end.isoformat(),
        )
        store.materialize_incremental(
            end_date=effective_end,
            feature_views=target_views,
        )
        mode = "incremental"
        effective_start = None
    else:
        if start_date is None:
            raise ValueError(
                "start_date is required when incremental=False. "
                "Provide an explicit start_date or set incremental=True."
            )
        effective_start = start_date
        if effective_start.tzinfo is None:
            effective_start = effective_start.replace(tzinfo=timezone.utc)

        if effective_start > effective_end:
            raise ValueError(
                f"start_date ({effective_start.isoformat()}) cannot be later than "
                f"end_date ({effective_end.isoformat()})"
            )

        logger.info(
            "Starting explicit materialization for feature views %s from %s to %s",
            target_views,
            effective_start.isoformat(),
            effective_end.isoformat(),
        )
        store.materialize(
            start_date=effective_start,
            end_date=effective_end,
            feature_views=target_views,
        )
        mode = "explicit"

    elapsed = time.perf_counter() - t0
    logger.info(
        "Materialization completed successfully in %.3fs (mode=%s, views=%s)",
        elapsed,
        mode,
        target_views,
    )

    return {
        "status": "success",
        "mode": mode,
        "start_date": effective_start,
        "end_date": effective_end,
        "feature_views": target_views,
        "elapsed_seconds": round(elapsed, 4),
    }
