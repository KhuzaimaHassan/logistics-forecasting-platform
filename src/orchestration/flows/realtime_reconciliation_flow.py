"""Prefect flow for reconciling real-time streaming Feast online store with PostgreSQL warehouse.

Orchestrates a periodic two-stage reconciliation loop:
1. Reconcile Offline Features Task: Queries warehouse.trips over [T - lookback_hours, T]
   and populates hourly aggregated tables (warehouse.zone_demand_features_hourly,
   warehouse.corridor_duration_features_hourly).
2. Materialize Online Store Task: Executes incremental materialization into Redis
   online store up to T, ensuring online store consistency and refreshing key TTLs.
"""

import argparse
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from prefect import flow, task

from src.common.db import get_engine
from src.features.materialize import (
    ALL_ONLINE_FEATURE_VIEW_NAMES,
    materialize_features,
)
from src.features.offline_extractor import extract_and_load_offline_features

logger = logging.getLogger(__name__)


@task(
    name="reconcile_offline_features",
    retries=2,
    retry_delay_seconds=10,
    cache_policy=None,
)
def reconcile_offline_features_task(
    engine: Optional[Any] = None,
    start_datetime: Optional[datetime] = None,
    end_datetime: Optional[datetime] = None,
    lookback_days: int = 7,
) -> Dict[str, int]:
    """Extract recent warehouse.trips and update offline feature tables."""
    eng = engine or get_engine()
    zone_rows, corridor_rows = extract_and_load_offline_features(
        engine=eng,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        lookback_days=lookback_days,
    )
    logger.info(
        "Reconciled offline features: %d zone rows, %d corridor rows.",
        zone_rows,
        corridor_rows,
    )
    return {
        "zone_rows_loaded": zone_rows,
        "corridor_rows_loaded": corridor_rows,
    }


@task(
    name="materialize_online_store",
    retries=2,
    retry_delay_seconds=10,
    cache_policy=None,
)
def materialize_online_store_task(
    end_date: datetime,
    feature_views: Optional[List[str]] = None,
    store: Optional[Any] = None,
    use_sqlite_fallback: bool = False,
) -> Dict[str, Any]:
    """Incrementally materialize offline features into online store."""
    views = feature_views or ALL_ONLINE_FEATURE_VIEW_NAMES
    res = materialize_features(
        end_date=end_date,
        feature_views=views,
        incremental=True,
        store=store,
        use_sqlite_fallback=use_sqlite_fallback,
    )
    logger.info(
        "Materialized features into online store up to %s: %s",
        end_date,
        res.get("status"),
    )
    return res


@flow(name="realtime_reconciliation_flow")
def realtime_reconciliation_flow(
    lookback_hours: int = 3,
    lookback_days: int = 7,
    end_datetime: Optional[datetime] = None,
    engine: Optional[Any] = None,
    store: Optional[Any] = None,
    feature_views: Optional[List[str]] = None,
    use_sqlite_fallback: bool = False,
) -> Dict[str, Any]:
    """Execute end-to-end reconciliation flow: offline extraction -> online materialization.

    Resolves the streaming-batch staleness gap (ADR-018) by extracting recent live/replay
    trips into warehouse hourly feature tables before syncing to Redis.
    """
    end_t = end_datetime or datetime.now(timezone.utc)
    start_t = end_t - timedelta(hours=lookback_hours)

    logger.info(
        "Starting real-time reconciliation flow for window [%s -> %s]",
        start_t,
        end_t,
    )

    # 1. Update offline feature tables from warehouse.trips
    extract_res = reconcile_offline_features_task(
        engine=engine,
        start_datetime=start_t,
        end_datetime=end_t,
        lookback_days=lookback_days,
    )

    # 2. Materialize updated feature tables to Redis
    mat_res = materialize_online_store_task(
        end_date=end_t,
        feature_views=feature_views,
        store=store,
        use_sqlite_fallback=use_sqlite_fallback,
    )

    return {
        "status": "success",
        "start_datetime": start_t.isoformat(),
        "end_datetime": end_t.isoformat(),
        "zone_rows_loaded": extract_res["zone_rows_loaded"],
        "corridor_rows_loaded": extract_res["corridor_rows_loaded"],
        "materialization": mat_res,
    }


def main() -> None:
    """CLI entrypoint for running reconciliation flow."""
    parser = argparse.ArgumentParser(
        description="Run real-time Feast reconciliation flow"
    )
    parser.add_argument(
        "--lookback-hours",
        type=int,
        default=3,
        help="Hours to look back for trip extraction",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=7,
        help="Days to look back for rolling baseline",
    )
    parser.add_argument(
        "--use-sqlite-fallback",
        action="store_true",
        help="Use SQLite fallback for Feast",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    results = realtime_reconciliation_flow(
        lookback_hours=args.lookback_hours,
        lookback_days=args.lookback_days,
        use_sqlite_fallback=args.use_sqlite_fallback,
    )
    logger.info("Reconciliation flow completed successfully: %s", results)


if __name__ == "__main__":
    main()
