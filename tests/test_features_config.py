"""Unit tests for Feast repository configuration and SQL registry initialization."""

import tempfile
from pathlib import Path

import pandas as pd
import pytest
from feast import Entity, FeatureStore, FeatureView, Field
from feast.infra.offline_stores.file_source import FileSource
from feast.types import Float32, Int32, ValueType

from src.features.config import (
    ensure_feast_schema,
    expand_env_placeholders,
    get_default_repo_path,
    get_feast_repo_config,
    get_feature_store,
)
from src.features.registry import apply_feature_definitions


def test_expand_env_placeholders_defaults():
    """Verify placeholder expansion against default application settings."""
    template = "host: ${POSTGRES_HOST} port: ${POSTGRES_PORT} url: ${REDIS_URL}"
    expanded = expand_env_placeholders(template)
    assert "host: localhost" in expanded
    assert "port: 5432" in expanded
    assert "url: redis://localhost:6379/0" in expanded


def test_expand_env_placeholders_with_custom_env(monkeypatch):
    """Verify placeholder expansion when environment variables are set."""
    monkeypatch.setenv("POSTGRES_HOST", "db.prod.internal")
    monkeypatch.setenv("POSTGRES_PORT", "5433")
    monkeypatch.setenv("REDIS_URL", "redis://redis.prod:6379/1")

    template = "host: ${POSTGRES_HOST} port: ${POSTGRES_PORT} url: ${REDIS_URL}"
    expanded = expand_env_placeholders(template)
    assert "host: db.prod.internal" in expanded
    assert "port: 5433" in expanded
    assert "url: redis://redis.prod:6379/1" in expanded


def test_expand_env_placeholders_with_fallback_defaults():
    """Verify ${VAR:-default} fallback when variable is unset."""
    template = "mode: ${UNKNOWN_VAR:-production} timeout: ${UNKNOWN_TIMEOUT:-30}"
    expanded = expand_env_placeholders(template)
    assert "mode: production" in expanded
    assert "timeout: 30" in expanded


def test_get_feast_repo_config_from_yaml():
    """Verify loading and parsing the project's src/features/feature_store.yaml."""
    repo_path = get_default_repo_path()
    assert (repo_path / "feature_store.yaml").exists()

    config = get_feast_repo_config(repo_path=repo_path)
    assert config.project == "logistics_forecasting"
    assert config.registry.registry_type == "sql"
    assert "postgresql+psycopg2://" in config.registry.path
    assert config.offline_store.type == "postgres"
    assert config.offline_store.db_schema == "warehouse"
    assert config.offline_store.sslmode == "disable"
    assert config.online_store.type == "redis"
    assert config.online_store.key_ttl_seconds == 86400


def test_get_feast_repo_config_sqlite_fallback():
    """Verify sqlite fallback configuration for fast in-memory tests."""
    config = get_feast_repo_config(use_sqlite_fallback=True)
    assert config.project == "logistics_forecasting_test"
    assert config.registry.registry_type == "sql"
    assert config.offline_store.type == "file"
    assert config.online_store.type == "sqlite"


def test_get_feast_repo_config_custom_overrides():
    """Verify custom overrides can be merged into repo configuration."""
    overrides = {"project": "custom_logistics_project"}
    config = get_feast_repo_config(use_sqlite_fallback=True, custom_overrides=overrides)
    assert config.project == "custom_logistics_project"


def test_get_feast_repo_config_missing_file_raises():
    """Verify FileNotFoundError if feature_store.yaml does not exist in path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        with pytest.raises(FileNotFoundError):
            get_feast_repo_config(repo_path=tmpdir)


def test_get_feature_store_instantiation_sqlite():
    """Verify FeatureStore can be instantiated and initialized with SQLite SQL registry."""
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        db_path = Path(tmpdir) / "registry.db"
        overrides = {
            "registry": {
                "registry_type": "sql",
                "path": f"sqlite:///{db_path.as_posix()}",
            },
            "online_store": {
                "type": "sqlite",
                "path": f"{tmpdir}/online.db",
            },
        }
        config = get_feast_repo_config(
            use_sqlite_fallback=True, custom_overrides=overrides
        )
        store = get_feature_store(config=config)
        assert store.project == "logistics_forecasting_test"
        assert store.config.registry.registry_type == "sql"


def test_apply_feature_definitions_programmatic_registry():
    """Verify registering Entities and FeatureViews programmatically into the SQL registry."""
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        db_path = Path(tmpdir) / "registry.db"
        parquet_source_path = Path(tmpdir) / "dummy.parquet"

        # Create a valid parquet file for Feast source schema inference
        df = pd.DataFrame(
            {
                "zone_id": [1, 2],
                "event_timestamp": [pd.Timestamp.now(), pd.Timestamp.now()],
                "created_timestamp": [pd.Timestamp.now(), pd.Timestamp.now()],
                "pickup_count_last_15m": [10, 20],
                "avg_temp_last_1h": [65.0, 70.0],
            }
        )
        df.to_parquet(parquet_source_path)

        overrides = {
            "registry": {
                "registry_type": "sql",
                "path": f"sqlite:///{db_path.as_posix()}",
            },
            "online_store": {
                "type": "sqlite",
                "path": f"{tmpdir}/online.db",
            },
        }
        config = get_feast_repo_config(
            use_sqlite_fallback=True, custom_overrides=overrides
        )
        store = FeatureStore(config=config)

        test_entity = Entity(
            name="test_zone",
            join_keys=["zone_id"],
            value_type=ValueType.INT32,
            description="Test Taxi Zone Entity",
        )

        source = FileSource(
            path=str(parquet_source_path),
            timestamp_field="event_timestamp",
            created_timestamp_column="created_timestamp",
        )

        test_view = FeatureView(
            name="test_zone_demand",
            entities=[test_entity],
            schema=[
                Field(name="pickup_count_last_15m", dtype=Int32),
                Field(name="avg_temp_last_1h", dtype=Float32),
            ],
            source=source,
        )

        # Apply definitions
        apply_feature_definitions(
            store=store,
            entities=[test_entity],
            views=[test_view],
        )

        # Retrieve definitions back from registry
        retrieved_entities = store.list_entities()
        retrieved_views = store.list_feature_views()

        assert len(retrieved_entities) == 1
        assert retrieved_entities[0].name == "test_zone"
        assert len(retrieved_views) == 1
        assert retrieved_views[0].name == "test_zone_demand"
        assert len(retrieved_views[0].features) == 2
        assert len(retrieved_views[0].entity_columns) == 1


def test_ensure_feast_schema_sqlite_noop():
    """Verify ensure_feast_schema is a no-op for non-postgres URLs."""
    # Should complete without error on sqlite URL
    ensure_feast_schema(database_url="sqlite:///:memory:")
