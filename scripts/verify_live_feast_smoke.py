"""Smoke test script verifying live Feast PostgreSQL schema and Redis connectivity."""

import time

import psycopg
import redis

from src.features.config import ensure_feast_schema, get_feature_store


def main() -> None:
    print("=== Step 1: Ensuring Feast Schema in Live PostgreSQL ===", flush=True)
    t0 = time.perf_counter()
    ensure_feast_schema()
    t1 = time.perf_counter()
    print(f"Feast schema ensured successfully in {t1 - t0:.3f}s.", flush=True)

    print("=== Step 2: Verifying PostgreSQL Feast Schema Existence ===", flush=True)
    t2 = time.perf_counter()
    with psycopg.connect(
        "postgresql://postgres:ci_test_password_do_not_use_in_prod@localhost:5432/logistics"
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'feast';"
            )
            row = cur.fetchone()
            assert row is not None, "Schema 'feast' was not found in PostgreSQL!"
            print(
                f"Confirmed PostgreSQL schema exists: '{row[0]}' in {time.perf_counter() - t2:.3f}s.",
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

    print("=== Step 4: Testing Redis Connectivity ===", flush=True)
    t4 = time.perf_counter()
    r = redis.Redis.from_url("redis://localhost:6379/0")
    assert r.ping(), "Redis ping failed!"
    print(
        f"Redis ping response: PONG (verified in {time.perf_counter() - t4:.3f}s).",
        flush=True,
    )

    print(
        "=== Live Feast Infrastructure Verification: ALL CHECKS PASSED ===",
        flush=True,
    )


if __name__ == "__main__":
    main()
