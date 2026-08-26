"""Feast repository and feature store configuration management."""

import os
import re
from pathlib import Path
from typing import Any, Dict, Optional, Union

import yaml
from feast import FeatureStore
from feast.repo_config import RepoConfig
from sqlalchemy import create_engine, text

from src.common.config import Settings, get_settings


def expand_env_placeholders(content: str, settings: Optional[Settings] = None) -> str:
    """Expand environment variable placeholders in configuration text.

    Supports:
      - ${VAR} or ${VAR:-default}
      - $VAR
      - %VAR% (Windows style)

    Falls back to application Settings defaults if variable is not in os.environ.
    """
    if settings is None:
        settings = get_settings()

    settings_defaults: Dict[str, Any] = {
        "POSTGRES_HOST": settings.postgres_host,
        "POSTGRES_PORT": str(settings.postgres_port),
        "POSTGRES_DB": settings.postgres_db,
        "POSTGRES_USER": settings.postgres_user,
        "POSTGRES_PASSWORD": settings.postgres_password,
        "POSTGRES_SSLMODE": "disable",
        "DATABASE_URL": settings.database_url,
        "REDIS_HOST": settings.redis_host,
        "REDIS_PORT": str(settings.redis_port),
        "REDIS_URL": settings.redis_url,
    }

    def _replace(match: re.Match) -> str:
        var_name = match.group("name1") or match.group("name2") or match.group("name3")
        default_val = match.group("default")
        val = os.environ.get(var_name)
        if val is None or val == "":
            val = settings_defaults.get(var_name)
        if val is None or val == "":
            return default_val if default_val is not None else ""
        return str(val)

    pattern = re.compile(
        r"\$\{(?P<name1>[A-Za-z0-9_]+)(?::-((?P<default>[^}]*)))?\}|"
        r"\$(?P<name2>[A-Za-z0-9_]+)|"
        r"%(?P<name3>[A-Za-z0-9_]+)%"
    )
    return pattern.sub(_replace, content)


def get_default_repo_path() -> Path:
    """Return the default path to the Feast repository directory (src/features)."""
    return Path(__file__).resolve().parent


def ensure_feast_schema(database_url: Optional[str] = None) -> None:
    """Ensure the 'feast' schema exists in PostgreSQL before registry operations."""
    if database_url is None:
        settings = get_settings()
        database_url = settings.database_url

    if not database_url.startswith("postgresql"):
        return

    engine = create_engine(database_url, isolation_level="AUTOCOMMIT")
    try:
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS feast;"))
    finally:
        engine.dispose()


def get_feast_repo_config(
    repo_path: Optional[Union[str, Path]] = None,
    settings: Optional[Settings] = None,
    use_sqlite_fallback: bool = False,
    custom_overrides: Optional[Dict[str, Any]] = None,
) -> RepoConfig:
    """Load and parse Feast RepoConfig with dynamic environment resolution.

    Args:
        repo_path: Optional path to directory containing feature_store.yaml.
        settings: Optional Settings instance (uses default if None).
        use_sqlite_fallback: If True, returns an in-memory/sqlite RepoConfig for fast offline tests.
        custom_overrides: Dictionary of config overrides to merge into the RepoConfig.

    Returns:
        Validated Feast RepoConfig instance.
    """
    if settings is None:
        settings = get_settings()

    path_obj = Path(repo_path) if repo_path else get_default_repo_path()
    yaml_file = path_obj / "feature_store.yaml"

    if use_sqlite_fallback:
        sqlite_config: Dict[str, Any] = {
            "project": "logistics_forecasting_test",
            "registry": {
                "registry_type": "sql",
                "path": "sqlite:///:memory:",
            },
            "provider": "local",
            "offline_store": {
                "type": "file",
            },
            "online_store": {
                "type": "sqlite",
                "path": str(path_obj / "online_test.db"),
            },
        }
        if custom_overrides:
            sqlite_config.update(custom_overrides)
        repo_cfg = RepoConfig(**sqlite_config)
        repo_cfg.repo_path = path_obj
        return repo_cfg

    if not yaml_file.exists():
        raise FileNotFoundError(f"Feast configuration file not found at {yaml_file}")

    with open(yaml_file, "r", encoding="utf-8") as f:
        raw_text = f.read()

    expanded_yaml = expand_env_placeholders(raw_text, settings=settings)
    config_dict = yaml.safe_load(expanded_yaml)

    if custom_overrides:
        config_dict.update(custom_overrides)

    repo_cfg = RepoConfig(**config_dict)
    repo_cfg.repo_path = path_obj
    return repo_cfg


def get_feature_store(
    repo_path: Optional[Union[str, Path]] = None,
    settings: Optional[Settings] = None,
    use_sqlite_fallback: bool = False,
    config: Optional[RepoConfig] = None,
) -> FeatureStore:
    """Instantiate and return a Feast FeatureStore object.

    Args:
        repo_path: Path to the Feast repository directory.
        settings: Optional application settings.
        use_sqlite_fallback: If True, uses local sqlite fallback.
        config: Explicit RepoConfig object (takes precedence).

    Returns:
        Configured Feast FeatureStore instance.
    """
    if config is not None:
        return FeatureStore(config=config)

    repo_cfg = get_feast_repo_config(
        repo_path=repo_path,
        settings=settings,
        use_sqlite_fallback=use_sqlite_fallback,
    )
    return FeatureStore(config=repo_cfg)
