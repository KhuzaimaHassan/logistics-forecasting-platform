"""Smoke test script verifying live Feast PostgreSQL schema, offline aggregation, idempotency, PIT retrieval, and Redis connectivity."""

import time
from datetime import datetime, timedelta, timezone

import mlflow
import numpy as np
import pandas as pd
import redis
from alembic.config import Config
from sqlalchemy import create_engine, text

from alembic import command
from src.common.config import get_settings
from src.common.mlflow_utils import (
    DEMAND_EXPERIMENT_NAME,
    DURATION_EXPERIMENT_NAME,
    get_mlflow_client,
    get_or_create_experiment,
    setup_mlflow,
)
from src.features.client import (
    FeastOnlineClient,
    get_corridor_duration_online_features,
    get_zone_demand_online_features,
)
from src.features.config import ensure_feast_schema, get_feature_store
from src.features.materialize import materialize_features
from src.features.offline_extractor import extract_and_load_offline_features
from src.features.registry import apply_feature_definitions
from src.training.baseline import (
    evaluate_corridor_duration_baseline,
    evaluate_demand_baseline,
)
from src.training.dataset import (
    CORRIDOR_FEATURES,
    DEMAND_FEATURES,
    generate_corridor_training_dataset,
    generate_demand_training_dataset,
    train_val_split_by_time,
    validate_dataset_integrity,
)
from src.training.train_demand import train_demand_lightgbm
from src.training.train_duration import train_duration_lightgbm


def run_alembic_migrations(db_url: str) -> None:
    """Run Alembic upgrade head to apply all database migrations."""
    alembic_cfg = Config("alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url", db_url)
    command.upgrade(alembic_cfg, "head")


def seed_test_trips_if_empty(engine) -> None:
    """Seed test trips for zones 161, 236, and 142 if warehouse.trips is empty."""
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM warehouse.trips;")).scalar()
        if count and count > 0:
            print(f"warehouse.trips already populated with {count} rows.", flush=True)
            return

    print(
        "Seeding test trip records into warehouse.trips for live smoke test...",
        flush=True,
    )
    # Ensure taxi_zones are loaded
    with engine.begin() as conn:
        conn.execute(text("""
                INSERT INTO warehouse.taxi_zones (zone_id, borough, zone_name, service_zone, centroid_lat, centroid_lon)
                VALUES
                    (161, 'Manhattan', 'Midtown Center', 'Yellow Zone', 40.757015, -73.981015),
                    (236, 'Manhattan', 'Upper East Side North', 'Yellow Zone', 40.780123, -73.955432),
                    (142, 'Manhattan', 'Lincoln Square East', 'Yellow Zone', 40.771234, -73.982345)
                ON CONFLICT (zone_id) DO NOTHING;
                """))

        t_base = datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        # Insert 30 trips for 161 -> 236 and 20 trips for 236 -> 142
        trip_id = 100000
        for i in range(30):
            trip_id += 1
            pu_time = t_base + timedelta(minutes=i * 3)  # spread across 10:00 to 11:30
            do_time = pu_time + timedelta(minutes=15)
            conn.execute(
                text("""
                    INSERT INTO warehouse.trips (
                        trip_id, vendor_id, cab_type, pickup_zone_id, dropoff_zone_id,
                        pickup_datetime, dropoff_datetime, trip_duration_seconds,
                        time_bin_15m, day_of_week, hour_of_day, is_weekend,
                        trip_distance_km, fare_amount, tip_amount, total_amount, source
                    ) VALUES (
                        :trip_id, 1, 'yellow', 161, 236,
                        :pu_time, :do_time, 900,
                        :time_bin_15m, 6, :hour, true,
                        4.5, 15.0, 3.0, 18.0, 'historical'
                    ) ON CONFLICT (trip_id) DO NOTHING;
                    """),
                {
                    "trip_id": trip_id,
                    "pu_time": pu_time,
                    "do_time": do_time,
                    "time_bin_15m": pu_time.replace(
                        minute=(pu_time.minute // 15) * 15, second=0, microsecond=0
                    ),
                    "hour": pu_time.hour,
                },
            )

        for i in range(20):
            trip_id += 1
            pu_time = t_base + timedelta(minutes=i * 5)
            do_time = pu_time + timedelta(minutes=20)
            conn.execute(
                text("""
                    INSERT INTO warehouse.trips (
                        trip_id, vendor_id, cab_type, pickup_zone_id, dropoff_zone_id,
                        pickup_datetime, dropoff_datetime, trip_duration_seconds,
                        time_bin_15m, day_of_week, hour_of_day, is_weekend,
                        trip_distance_km, fare_amount, tip_amount, total_amount, source
                    ) VALUES (
                        :trip_id, 1, 'yellow', 236, 142,
                        :pu_time, :do_time, 1200,
                        :time_bin_15m, 6, :hour, true,
                        6.0, 20.0, 4.0, 24.0, 'historical'
                    ) ON CONFLICT (trip_id) DO NOTHING;
                    """),
                {
                    "trip_id": trip_id,
                    "pu_time": pu_time,
                    "do_time": do_time,
                    "time_bin_15m": pu_time.replace(
                        minute=(pu_time.minute // 15) * 15, second=0, microsecond=0
                    ),
                    "hour": pu_time.hour,
                },
            )


def main() -> None:
    settings = get_settings()
    db_url = settings.database_url

    print("=== Step 1: Ensuring Feast Schema in Live PostgreSQL ===", flush=True)

    t0 = time.perf_counter()
    ensure_feast_schema()
    t1 = time.perf_counter()
    print(f"Feast schema ensured successfully in {t1 - t0:.3f}s.", flush=True)

    print("=== Step 2: Applying Alembic Migrations to Live PostgreSQL ===", flush=True)
    t2 = time.perf_counter()
    run_alembic_migrations(db_url)
    print(
        f"Alembic migrations upgraded to head in {time.perf_counter() - t2:.3f}s.",
        flush=True,
    )

    print(
        "=== Step 3: Initializing Feast FeatureStore with SQL Registry & Redis ===",
        flush=True,
    )
    t3 = time.perf_counter()
    store = get_feature_store()
    print(f"FeatureStore project: {store.project}", flush=True)
    print(f"Registry type: {store.config.registry.registry_type}", flush=True)
    print(f"Offline store type: {store.config.offline_store.type}", flush=True)
    print(f"Online store type: {store.config.online_store.type}", flush=True)
    print(
        f"FeatureStore initialized successfully in {time.perf_counter() - t3:.3f}s.",
        flush=True,
    )

    print(
        "=== Step 4: Applying Entities and Feature Views to Live PostgreSQL Registry ===",
        flush=True,
    )
    t4 = time.perf_counter()
    apply_feature_definitions(store=store)
    entities = store.list_entities()
    views = store.list_feature_views()
    entity_names = sorted([e.name for e in entities])
    view_names = sorted([v.name for v in views])
    assert (
        "zone" in entity_names and "corridor" in entity_names
    ), f"Missing entities: {entity_names}"
    assert (
        "zone_demand_features" in view_names
        and "corridor_duration_features" in view_names
    ), f"Missing views: {view_names}"
    print(f"Registered Entities ({len(entities)}): {entity_names}", flush=True)
    print(f"Registered Feature Views ({len(views)}): {view_names}", flush=True)
    print(
        f"Entities and views applied to live SQL registry in {time.perf_counter() - t4:.3f}s.",
        flush=True,
    )

    print("=== Step 5: Testing Redis Connectivity ===", flush=True)
    t5 = time.perf_counter()
    r = redis.Redis.from_url("redis://localhost:6379/0")
    assert r.ping(), "Redis ping failed!"
    print(
        f"Redis ping response: PONG (verified in {time.perf_counter() - t5:.3f}s).",
        flush=True,
    )

    print(
        "=== Step 6: Executing Live Offline Aggregation Pipeline (Run 1) ===",
        flush=True,
    )
    engine = create_engine(db_url)
    seed_test_trips_if_empty(engine)

    t6 = time.perf_counter()
    start_dt = datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    end_dt = datetime(2023, 1, 1, 13, 0, 0, tzinfo=timezone.utc)
    z_cnt1, c_cnt1 = extract_and_load_offline_features(
        engine=engine,
        start_datetime=start_dt,
        end_datetime=end_dt,
        lookback_days=1,
    )
    print(
        f"Run 1 completed in {time.perf_counter() - t6:.3f}s: loaded {z_cnt1} zone rows, {c_cnt1} corridor rows.",
        flush=True,
    )

    print(
        "=== Step 7: Executing Live Offline Aggregation Pipeline (Run 2 - Idempotency Proof) ===",
        flush=True,
    )
    t7 = time.perf_counter()
    z_cnt2, c_cnt2 = extract_and_load_offline_features(
        engine=engine,
        start_datetime=start_dt,
        end_datetime=end_dt,
        lookback_days=1,
    )
    assert z_cnt2 == z_cnt1, f"Zone row count mismatch: {z_cnt1} vs {z_cnt2}"
    assert c_cnt2 == c_cnt1, f"Corridor row count mismatch: {c_cnt1} vs {c_cnt2}"
    print(
        f"Run 2 completed in {time.perf_counter() - t7:.3f}s: identical row counts confirmed ({z_cnt2} zones, {c_cnt2} corridors).",
        flush=True,
    )

    print(
        "=== Step 8: Feast Point-in-Time Historical Retrieval vs. Independent SQL Spot-Checks ===",
        flush=True,
    )
    t8 = time.perf_counter()
    # Query Feast historical features across 5 distinct zone-time observations:
    # 1. Zone 161 at 11:00 (active non-zero 15m window: 5 trips, 1h: 20 trips)
    # 2. Zone 236 at 11:00 (active non-zero 15m window: 3 trips, 1h: 12 trips)
    # 3. Zone 161 at 12:00 (tail window, 15m: 0 trips, 1h: 10 trips)
    # 4. Zone 236 at 12:00 (tail window, 15m: 0 trips, 1h: 8 trips)
    # 5. Zone 142 at 11:00 (baseline zero activity, 15m: 0 trips, 1h: 0 trips)
    t_11 = datetime(2023, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
    t_12 = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    zone_entity_df = pd.DataFrame(
        [
            {"zone_id": 161, "event_timestamp": t_11},
            {"zone_id": 236, "event_timestamp": t_11},
            {"zone_id": 161, "event_timestamp": t_12},
            {"zone_id": 236, "event_timestamp": t_12},
            {"zone_id": 142, "event_timestamp": t_11},
        ]
    )
    features_to_fetch = [
        "zone_demand_features:pickup_count_last_1h",
        "zone_demand_features:pickup_count_last_15m",
        "zone_demand_features:pickup_count_last_24h",
    ]
    historical_df = store.get_historical_features(
        entity_df=zone_entity_df,
        features=features_to_fetch,
    ).to_df()

    print(
        f"Feast retrieved historical features for {len(historical_df)} observations in {time.perf_counter() - t8:.3f}s:\n{historical_df}",
        flush=True,
    )

    # Independent direct SQL spot-checks
    with engine.connect() as conn:
        for row in historical_df.itertuples():
            zid = int(row.zone_id)
            ts = pd.to_datetime(row.event_timestamp)
            row_time = (
                ts.tz_localize(timezone.utc)
                if ts.tz is None
                else ts.tz_convert(timezone.utc)
            )

            # Convert Feast values handling possible NaN for unobserved entities
            feast_1h = (
                0
                if pd.isna(row.pickup_count_last_1h)
                else int(row.pickup_count_last_1h)
            )
            feast_15m = (
                0
                if pd.isna(row.pickup_count_last_15m)
                else int(row.pickup_count_last_15m)
            )

            # Manual SQL query computing rolling 1h pickups directly from raw trips
            sql_1h = conn.execute(
                text("""
                    SELECT COUNT(*) FROM warehouse.trips
                    WHERE pickup_zone_id = :zid
                      AND pickup_datetime >= :t_minus_1h
                      AND pickup_datetime < :t_obs;
                    """),
                {
                    "zid": zid,
                    "t_minus_1h": row_time - timedelta(hours=1),
                    "t_obs": row_time,
                },
            ).scalar()

            sql_15m = conn.execute(
                text("""
                    SELECT COUNT(*) FROM warehouse.trips
                    WHERE pickup_zone_id = :zid
                      AND pickup_datetime >= :t_minus_15m
                      AND pickup_datetime < :t_obs;
                    """),
                {
                    "zid": zid,
                    "t_minus_15m": row_time - timedelta(minutes=15),
                    "t_obs": row_time,
                },
            ).scalar()

            print(
                f"Spot-check Zone {zid} at {row_time}: Feast 1h={feast_1h} vs SQL 1h={sql_1h} | Feast 15m={feast_15m} vs SQL 15m={sql_15m}",
                flush=True,
            )
            assert (
                feast_1h == sql_1h
            ), f"Zone {zid} at {row_time} 1h mismatch: Feast {feast_1h} vs SQL {sql_1h}"
            assert (
                feast_15m == sql_15m
            ), f"Zone {zid} at {row_time} 15m mismatch: Feast {feast_15m} vs SQL {sql_15m}"

    # Also spot-check corridor duration feature PIT retrieval
    corridor_entity_df = pd.DataFrame(
        [
            {"corridor_id": "161_236", "event_timestamp": t_11},
        ]
    )
    corridor_historical_df = store.get_historical_features(
        entity_df=corridor_entity_df,
        features=[
            "corridor_duration_features:avg_duration_last_1h",
            "corridor_duration_features:avg_duration_last_15m",
            "corridor_duration_features:origin_zone_demand_pressure",
        ],
    ).to_df()
    print(
        f"Feast retrieved corridor historical features:\n{corridor_historical_df}",
        flush=True,
    )
    assert corridor_historical_df["avg_duration_last_1h"].iloc[0] == 900.0
    assert corridor_historical_df["origin_zone_demand_pressure"].iloc[0] == 20

    print(
        "=== Live Feast Infrastructure Verification: ALL 5 CHECKS & CORRIDOR PIT RETRIEVAL PASSED ===",
        flush=True,
    )

    # === Step 9: Materialize Features into Redis Online Store (Run 1) ===
    print(
        "=== Step 9: Materializing Features into Redis Online Store (Run 1) ===",
        flush=True,
    )
    t9 = time.perf_counter()
    mat_res_1 = materialize_features(
        start_date=datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
        end_date=datetime(2023, 1, 1, 13, 0, 0, tzinfo=timezone.utc),
        store=store,
        incremental=False,
    )
    print(
        f"Materialization Run 1 completed in {time.perf_counter() - t9:.3f}s: {mat_res_1}",
        flush=True,
    )
    assert mat_res_1["status"] == "success"

    # === Step 10: Raw Redis Inspection & Physical Key / TTL Verification ===
    print(
        "=== Step 10: Inspecting Physical Redis Keys and Verifying 24h TTL ===",
        flush=True,
    )
    redis_client = redis.Redis.from_url(settings.redis_url)
    keys = redis_client.keys("*")
    print(f"Total keys found in Redis: {len(keys)}", flush=True)
    assert len(keys) > 0, "No keys found in Redis online store after materialization!"

    # Sample and inspect Redis keys
    sample_key = keys[0]
    sample_type = redis_client.type(sample_key).decode("utf-8")
    sample_ttl = redis_client.ttl(sample_key)
    print(
        f"Sample Redis key: {sample_key!r} | Type: {sample_type} | TTL: {sample_ttl}s (Expected <= 86400s)",
        flush=True,
    )
    assert (
        0 < sample_ttl <= 86400
    ), f"Invalid TTL on Redis key: {sample_ttl}s (expected >0 and <=86400)"

    # === Step 11: Direct Online Feature Retrieval via FeastOnlineClient ===
    print(
        "=== Step 11: Validating Online Feature Retrieval & Cache Miss Handling ===",
        flush=True,
    )
    online_client = FeastOnlineClient(store=store)

    # 1. Zone demand features: 161 (cached), 236 (cached), 999 (cache miss)
    z_features = online_client.get_zone_demand_features([161, 236, 999])
    print(f"Online Zone Demand Features:\n{z_features}", flush=True)
    assert z_features[0].zone_id == 161
    assert z_features[0].cache_hit is True
    assert z_features[0].pickup_count_last_24h == 30  # 13:00 snapshot value
    assert z_features[0].hour_of_day == 13
    assert z_features[1].zone_id == 236
    assert z_features[1].cache_hit is True
    assert z_features[1].pickup_count_last_24h == 20  # 13:00 snapshot value
    assert z_features[1].hour_of_day == 13
    # Cache miss
    assert z_features[2].zone_id == 999
    assert z_features[2].cache_hit is False
    assert z_features[2].pickup_count_last_1h is None

    # 2. Corridor duration features: 161_236 (cached), 999_999 (cache miss)
    c_features = online_client.get_corridor_duration_features(["161_236", "999_999"])
    print(f"Online Corridor Duration Features:\n{c_features}", flush=True)
    assert c_features[0].corridor_id == "161_236"
    assert c_features[0].cache_hit is True
    assert c_features[0].avg_duration_last_1h == 900.0
    # Cache miss
    assert c_features[1].corridor_id == "999_999"
    assert c_features[1].cache_hit is False

    # 3. Combined prediction features
    pred_feat = online_client.get_prediction_features(
        pickup_zone_id=161, dropoff_zone_id=236
    )
    assert pred_feat.all_cached is True
    assert pred_feat.corridor_id == "161_236"
    print(f"Combined Prediction Features:\n{pred_feat.to_dict()}", flush=True)

    # 4. Top-level convenience functions
    z_conv = get_zone_demand_online_features([161], store=store)
    assert z_conv[0].pickup_count_last_24h == 30
    c_conv = get_corridor_duration_online_features(["161_236"], store=store)
    assert c_conv[0].avg_duration_last_1h == 900.0

    # === Step 12: Re-Run Materialization (Idempotency & TTL Refresh) ===
    print(
        "=== Step 12: Re-Running Materialization (Run 2 - Idempotency Proof) ===",
        flush=True,
    )
    t12 = time.perf_counter()
    mat_res_2 = materialize_features(
        start_date=datetime(2023, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
        end_date=datetime(2023, 1, 1, 13, 0, 0, tzinfo=timezone.utc),
        store=store,
        incremental=False,
    )
    print(
        f"Materialization Run 2 completed in {time.perf_counter() - t12:.3f}s: {mat_res_2}",
        flush=True,
    )
    assert mat_res_2["status"] == "success"

    # Re-verify physical keys in Redis and active TTL
    keys_rerun = redis_client.keys("*")
    print(f"Total keys in Redis after Run 2: {len(keys_rerun)}", flush=True)
    assert len(keys_rerun) == len(
        keys
    ), f"Key count mismatch after Run 2: {len(keys_rerun)} vs {len(keys)}"
    sample_ttl_rerun = redis_client.ttl(keys_rerun[0])
    print(
        f"Sample key TTL after Run 2 refresh: {sample_ttl_rerun}s (Expected <= 86400s)",
        flush=True,
    )
    assert 0 < sample_ttl_rerun <= 86400

    # Re-query all entities after Run 2
    z_features_run2 = online_client.get_zone_demand_features([161, 236, 999])
    c_features_run2 = online_client.get_corridor_duration_features(
        ["161_236", "999_999"]
    )
    pred_feat_run2 = online_client.get_prediction_features(161, 236)

    print("--- Side-by-Side Comparison: Run 1 vs Run 2 ---", flush=True)
    print(f"Run 1 Zone Demand Features:\n{z_features}", flush=True)
    print(f"Run 2 Zone Demand Features:\n{z_features_run2}", flush=True)
    print(f"Run 1 Corridor Duration Features:\n{c_features}", flush=True)
    print(f"Run 2 Corridor Duration Features:\n{c_features_run2}", flush=True)
    print(f"Run 1 Prediction Features:\n{pred_feat.to_dict()}", flush=True)
    print(f"Run 2 Prediction Features:\n{pred_feat_run2.to_dict()}", flush=True)

    # Assert byte-for-byte exact value parity between Run 1 and Run 2
    assert (
        z_features == z_features_run2
    ), f"Zone demand mismatch: {z_features} != {z_features_run2}"
    assert (
        c_features == c_features_run2
    ), f"Corridor duration mismatch: {c_features} != {c_features_run2}"
    assert (
        pred_feat.to_dict() == pred_feat_run2.to_dict()
    ), "Prediction features mismatch between Run 1 and Run 2"

    # === Step 13: High-Frequency Wall-Clock Latency Benchmark against Live Redis ===
    print(
        "=== Step 13: Benchmarking Live Redis Online Lookup Latency (100 Requests) ===",
        flush=True,
    )
    latencies_ms = []
    # Warm up 5 requests
    for _ in range(5):
        online_client.get_prediction_features(161, 236)

    # Benchmark 100 requests
    n_benchmark = 100
    for _ in range(n_benchmark):
        t_req = time.perf_counter()
        online_client.get_prediction_features(161, 236)
        latencies_ms.append((time.perf_counter() - t_req) * 1000.0)

    p50 = float(np.percentile(latencies_ms, 50))
    p90 = float(np.percentile(latencies_ms, 90))
    p95 = float(np.percentile(latencies_ms, 95))
    p99 = float(np.percentile(latencies_ms, 99))
    mean_lat = float(np.mean(latencies_ms))
    min_lat = float(np.min(latencies_ms))
    max_lat = float(np.max(latencies_ms))

    print(
        f"Online Feature Lookup Latency Results (N={n_benchmark}):\n"
        f"  p50:  {p50:.2f} ms\n"
        f"  p90:  {p90:.2f} ms\n"
        f"  p95:  {p95:.2f} ms\n"
        f"  p99:  {p99:.2f} ms\n"
        f"  Mean: {mean_lat:.2f} ms (Min: {min_lat:.2f} ms, Max: {max_lat:.2f} ms)\n"
        f"  SLA Check (< 10ms p95): {'PASSED' if p95 < 10.0 else 'WARN - SLA threshold is 10ms'}",
        flush=True,
    )
    assert (
        p95 < 20.0
    ), f"Latency SLA severely degraded: p95={p95:.2f}ms (threshold 20.0ms on shared CI)"

    print(
        "=== Live Feast Online Store Materialization & Retrieval Verification: ALL CHECKS PASSED ===",
        flush=True,
    )

    # === Step 14: Live PostgreSQL-Backed MLflow Service Smoke Test (M3-1) ===
    print(
        "\n=== Step 14: Verifying Live MLflow Service & Logging Experiment Run ===",
        flush=True,
    )
    mlflow_uri = get_settings().mlflow_tracking_uri
    setup_mlflow(mlflow_uri)
    client = get_mlflow_client(mlflow_uri)

    exp_demand_id = get_or_create_experiment(DEMAND_EXPERIMENT_NAME, client=client)
    exp_duration_id = get_or_create_experiment(DURATION_EXPERIMENT_NAME, client=client)
    print(
        f"Live MLflow Experiment '{DEMAND_EXPERIMENT_NAME}' ID: {exp_demand_id}",
        flush=True,
    )
    print(
        f"Live MLflow Experiment '{DURATION_EXPERIMENT_NAME}' ID: {exp_duration_id}",
        flush=True,
    )

    run_name = f"ci_smoke_run_{int(time.time())}"
    with mlflow.start_run(experiment_id=exp_demand_id, run_name=run_name) as active_run:
        mlflow.log_param("pipeline_stage", "m3-1-smoke-test")
        mlflow.log_param("model_family", "seasonal_naive")
        mlflow.log_metric("baseline_val_mae", 4.12)
        mlflow.log_metric("baseline_val_rmse", 7.89)
        mlflow.set_tag("test_type", "live_docker_compose_smoke")
        active_run_id = active_run.info.run_id

    fetched_run = client.get_run(active_run_id)
    assert fetched_run is not None, "Failed to retrieve run from live MLflow service"
    assert (
        fetched_run.info.status == "FINISHED"
    ), f"Expected FINISHED run status, got {fetched_run.info.status}"
    assert fetched_run.data.params["pipeline_stage"] == "m3-1-smoke-test"
    assert fetched_run.data.metrics["baseline_val_mae"] == 4.12
    print(
        f"Successfully logged and verified live MLflow Run:\n"
        f"  Run ID:        {fetched_run.info.run_id}\n"
        f"  Experiment ID: {fetched_run.info.experiment_id}\n"
        f"  Status:        {fetched_run.info.status}\n"
        f"  Params:        {fetched_run.data.params}\n"
        f"  Metrics:       {fetched_run.data.metrics}\n"
        f"  Artifact URI:  {fetched_run.info.artifact_uri}",
        flush=True,
    )

    print(
        "\n=== Step 15: Generating Live Demand Training Dataset & Chronological Split (M3-2) ===",
        flush=True,
    )
    t15 = time.perf_counter()
    demand_dataset = generate_demand_training_dataset(
        store=store,
        engine=engine,
        start_time=start_dt,
        end_time=end_dt,
        zone_ids=[161, 236, 142],
        features=DEMAND_FEATURES,
    )
    assert len(demand_dataset) == 3 * 3, f"Expected 9 rows, got {len(demand_dataset)}"
    assert "target_pickup_count_next_1h" in demand_dataset.columns

    # Verify integrity and anti-leakage
    demand_check = validate_dataset_integrity(
        demand_dataset,
        required_features=["pickup_count_last_1h", "pickup_count_last_15m"],
        target_col="target_pickup_count_next_1h",
    )
    assert demand_check["status"] == "passed"

    # Chronological train/val split test
    split_dt = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    demand_train, demand_val = train_val_split_by_time(
        demand_dataset, split_timestamp=split_dt
    )
    assert len(demand_train) == 6  # Hours 10, 11 (3 zones * 2 = 6)
    assert len(demand_val) == 3  # Hour 12 (3 zones * 1 = 3)
    assert demand_train["event_timestamp"].max() < pd.Timestamp(split_dt)
    assert demand_val["event_timestamp"].min() >= pd.Timestamp(split_dt)

    print(
        f"Demand training dataset generated in {time.perf_counter() - t15:.3f}s:\n"
        f"  Total Rows:     {len(demand_dataset)} across zones [161, 236, 142]\n"
        f"  Target Range:   min={demand_check['target_min']}, max={demand_check['target_max']}, mean={demand_check['target_mean']:.2f}\n"
        f"  Train/Val Split: Train={len(demand_train)} rows, Val={len(demand_val)} rows (Zero Leakage: {demand_train['event_timestamp'].max()} < {demand_val['event_timestamp'].min()})\n"
        f"  Sample Record (Zone 161 @ 11:00):\n"
        f"    Target (trips departing in [11:00, 12:00)): {demand_dataset[(demand_dataset['zone_id'] == 161) & (demand_dataset['event_timestamp'] == t_11)]['target_pickup_count_next_1h'].iloc[0]}\n"
        f"    Feast Feature (pickup_count_last_1h @ T<=11:00): {demand_dataset[(demand_dataset['zone_id'] == 161) & (demand_dataset['event_timestamp'] == t_11)]['pickup_count_last_1h'].iloc[0]}",
        flush=True,
    )

    print(
        "\n=== Step 16: Generating Live Corridor Duration Training Dataset & Verification (M3-2) ===",
        flush=True,
    )
    t16 = time.perf_counter()
    corridor_dataset = generate_corridor_training_dataset(
        store=store,
        engine=engine,
        start_time=start_dt,
        end_time=end_dt,
        features=CORRIDOR_FEATURES,
    )
    assert not corridor_dataset.empty, "Corridor training dataset is empty"
    assert "target_avg_duration_next_1h" in corridor_dataset.columns
    assert "target_trip_count_next_1h" in corridor_dataset.columns

    corridor_check = validate_dataset_integrity(
        corridor_dataset,
        required_features=["avg_duration_last_1h", "distance_km"],
        target_col="target_avg_duration_next_1h",
    )
    assert corridor_check["status"] == "passed"

    corridor_split_dt = datetime(2023, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
    corridor_train, corridor_val = train_val_split_by_time(
        corridor_dataset, split_timestamp=corridor_split_dt, enforce_non_empty=True
    )
    assert len(corridor_train) + len(corridor_val) == len(corridor_dataset)
    assert len(corridor_train) > 0
    assert len(corridor_val) > 0
    assert corridor_train["event_timestamp"].max() < pd.Timestamp(corridor_split_dt)
    assert corridor_val["event_timestamp"].min() >= pd.Timestamp(corridor_split_dt)

    print(
        f"Corridor duration training dataset generated in {time.perf_counter() - t16:.3f}s:\n"
        f"  Total Active Observations: {len(corridor_dataset)}\n"
        f"  Target Duration Range:     min={corridor_check['target_min']:.1f}s, max={corridor_check['target_max']:.1f}s, mean={corridor_check['target_mean']:.1f}s\n"
        f"  Train/Val Split:           Train={len(corridor_train)} rows, Val={len(corridor_val)} rows (Zero Leakage: {corridor_train['event_timestamp'].max()} < {corridor_val['event_timestamp'].min()})\n"
        f"  Sample Record (Corridor 161_236 @ 11:00):\n"
        f"    Target Duration (departing in [11:00, 12:00)): {corridor_dataset[(corridor_dataset['corridor_id'] == '161_236') & (corridor_dataset['event_timestamp'] == t_11)]['target_avg_duration_next_1h'].iloc[0]:.1f}s\n"
        f"    Feast Feature (avg_duration_last_1h @ T<=11:00): {corridor_dataset[(corridor_dataset['corridor_id'] == '161_236') & (corridor_dataset['event_timestamp'] == t_11)]['avg_duration_last_1h'].iloc[0]:.1f}s",
        flush=True,
    )

    print(
        "\n=== Step 17: Evaluating Live Seasonal-Naive Baselines & Logging to MLflow (M3-3) ===",
        flush=True,
    )
    t17 = time.perf_counter()
    # 1. Demand Seasonal-Naive Baseline
    demand_eval = evaluate_demand_baseline(
        val_df=demand_val,
        experiment_name=DEMAND_EXPERIMENT_NAME,
        run_name="smoke_test_seasonal_naive_demand",
        log_to_mlflow=True,
    )
    assert demand_eval["run_id"] is not None
    assert "val_mae" in demand_eval["metrics"]
    assert demand_eval["metrics"]["val_mae"] >= 0.0

    # 2. Corridor Duration Baseline
    corridor_eval = evaluate_corridor_duration_baseline(
        val_df=corridor_val,
        experiment_name=DURATION_EXPERIMENT_NAME,
        run_name="smoke_test_moving_avg_duration",
        log_to_mlflow=True,
    )
    assert corridor_eval["run_id"] is not None
    assert "val_mae" in corridor_eval["metrics"]
    assert corridor_eval["metrics"]["val_mae"] >= 0.0

    print(
        f"Seasonal-Naive Baselines evaluated and logged to live MLflow in {time.perf_counter() - t17:.3f}s:\n"
        f"  Demand Baseline:   MAE={demand_eval['metrics']['val_mae']:.4f}, WAPE={demand_eval['metrics']['val_wape']:.2f}% (Run ID: {demand_eval['run_id']})\n"
        f"  Corridor Baseline: MAE={corridor_eval['metrics']['val_mae']:.2f}s, WAPE={corridor_eval['metrics']['val_wape']:.2f}% (Run ID: {corridor_eval['run_id']})",
        flush=True,
    )

    print(
        "\n=== Step 18: Training Live LightGBM Demand & Duration Models (M3-4) ===",
        flush=True,
    )
    t18 = time.perf_counter()
    # 1. Train Demand LightGBM Model
    demand_lgbm = train_demand_lightgbm(
        train_df=demand_train,
        val_df=demand_val,
        params={"n_estimators": 10, "min_child_samples": 2},
        baseline_mae=demand_eval["metrics"]["val_mae"],
        experiment_name=DEMAND_EXPERIMENT_NAME,
        run_name="smoke_test_lightgbm_demand",
        log_to_mlflow=True,
    )
    assert demand_lgbm["run_id"] is not None
    assert demand_lgbm["model"] is not None
    assert "val_mae" in demand_lgbm["metrics"]
    assert len(demand_lgbm["feature_importances"]) > 0

    # 2. Train Duration LightGBM Model on log1p targets
    corridor_lgbm = train_duration_lightgbm(
        train_df=corridor_train,
        val_df=corridor_val,
        params={"n_estimators": 10, "min_child_samples": 2},
        baseline_mae=corridor_eval["metrics"]["val_mae"],
        experiment_name=DURATION_EXPERIMENT_NAME,
        run_name="smoke_test_lightgbm_duration",
        log_to_mlflow=True,
    )
    assert corridor_lgbm["run_id"] is not None
    assert corridor_lgbm["model"] is not None
    assert "val_mae" in corridor_lgbm["metrics"]
    assert np.all(corridor_lgbm["predictions"] >= 60.0)
    assert len(corridor_lgbm["feature_importances"]) > 0

    print(
        f"LightGBM models trained, evaluated, and logged to live MLflow in {time.perf_counter() - t18:.3f}s:\n"
        f"  Demand Model:   MAE={demand_lgbm['metrics']['val_mae']:.4f}, Lift={demand_lgbm['lift_pct']:+.2f}% (Run ID: {demand_lgbm['run_id']})\n"
        f"  Corridor Model: MAE={corridor_lgbm['metrics']['val_mae']:.2f}s, Lift={corridor_lgbm['lift_pct']:+.2f}% (Run ID: {corridor_lgbm['run_id']})",
        flush=True,
    )

    print(
        "\n=== Live Feast, MLflow, Baseline & LightGBM Model Verification: ALL 18 CHECKS PASSED ===",
        flush=True,
    )


if __name__ == "__main__":
    main()
