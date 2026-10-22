"""Eight real Feast PIT rows, read-only SQL, no training or materialization."""

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.common.db import get_engine
from src.features.config import get_feast_repo_config, get_feature_store


def probe(engine, store):
    checks = []
    # Fixed known sample entities; identifiers and query shape are not user input.
    for table, key, value, stamp, view, feature in [
        (
            "zone_demand_features_hourly",
            "zone_id",
            "74",
            "pickup_datetime",
            "zone_demand_features",
            "pickup_count_last_1h",
        ),
        (
            "corridor_duration_features_hourly",
            "corridor_id",
            "'75_237'",
            "dropoff_datetime",
            "corridor_duration_features",
            "avg_duration_last_1h",
        ),
    ]:
        entity_sql = (
            f"SELECT {key}, {stamp} AS event_timestamp FROM warehouse.{table} "
            f"WHERE {key}={value} AND {feature} IS NOT NULL ORDER BY {stamp} DESC LIMIT 4"
        )
        # embed_query avoids uploading or dropping any Feast entity table.
        query = store.get_historical_features(
            entity_df=entity_sql, features=[f"{view}:{feature}"]
        ).to_sql()
        started = time.monotonic()
        with engine.connect() as connection, connection.begin():
            connection.execute(text("SET TRANSACTION READ ONLY"))
            connection.execute(text("SET LOCAL statement_timeout = '8s'"))
            connection.execute(text("SET LOCAL lock_timeout = '2s'"))
            connection.execute(text("SET LOCAL work_mem = '2MB'"))
            connection.execute(text("SET LOCAL max_parallel_workers_per_gather = 0"))
            expected = pd.read_sql_query(
                text(
                    f"SELECT {key}, {stamp} AS event_timestamp, {feature} "
                    f"FROM warehouse.{table} WHERE {key}={value} AND {feature} IS NOT NULL "
                    f"ORDER BY {stamp} DESC LIMIT 4"
                ),
                connection,
            )
            actual = pd.read_sql_query(text(query), connection)
        sort = [key, "event_timestamp"]
        pd.testing.assert_frame_equal(
            actual[[*sort, feature]].sort_values(sort).reset_index(drop=True),
            expected.sort_values(sort).reset_index(drop=True),
            check_dtype=False,
        )
        if len(actual) != 4 or actual[feature].isna().any():
            raise RuntimeError("Expected four genuine non-null PIT feature rows")
        checks.append(
            {
                "view": view,
                "rows": len(actual),
                "matches_offline_source": True,
                "elapsed_seconds": round(time.monotonic() - started, 3),
            }
        )
    return checks


def main():
    result = {
        "status": "FAILED",
        "scope": "Eight-row read-only Feast PIT probe; not an OOM recovery certification",
    }
    engine = None
    try:
        config = get_feast_repo_config()
        # Retain the enum expected by pinned Feast0.65.
        from feast.infra.offline_stores.contrib.postgres_offline_store.postgres import (
            EntitySelectMode,
        )

        config.offline_store.entity_select_mode = EntitySelectMode.embed_query
        engine = get_engine()
        result["checks"] = probe(engine, get_feature_store(config=config))
        result["status"] = "VERIFIED"
    except Exception as exc:
        # Provider errors may contain SQL/credentials. Never serialize them.
        result["error_type"] = type(exc).__name__
    finally:
        if engine is not None:
            engine.dispose()
    result["generated_at"] = datetime.now(timezone.utc).isoformat()
    path = Path("artifacts/query-budget.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result))
    return 0 if result["status"] == "VERIFIED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
