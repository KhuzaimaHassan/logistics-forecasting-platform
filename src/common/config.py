"""Application configuration and environment settings."""

import os
from functools import lru_cache
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL, make_url


class Settings(BaseSettings):
    """Global configuration settings for database, message broker, and external APIs."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Database
    postgres_user: str = Field(default="postgres", alias="POSTGRES_USER")
    postgres_password: str = Field(default="postgres", alias="POSTGRES_PASSWORD")
    postgres_host: str = Field(default="localhost", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_db: str = Field(default="logistics", alias="POSTGRES_DB")
    database_url_override: Optional[str] = Field(default=None, alias="DATABASE_URL")
    db_pool_size: int = Field(default=2, ge=1, alias="DB_POOL_SIZE")
    db_max_overflow: int = Field(default=0, ge=0, alias="DB_MAX_OVERFLOW")

    # Redis
    redis_host: str = Field(default="localhost", alias="REDIS_HOST")
    redis_port: int = Field(default=6379, alias="REDIS_PORT")
    redis_url_override: Optional[str] = Field(default=None, alias="REDIS_URL")

    # Redpanda
    redpanda_broker: str = Field(default="localhost:9092", alias="REDPANDA_BROKER")
    kafka_security_protocol: str = Field(
        default="PLAINTEXT", alias="KAFKA_SECURITY_PROTOCOL"
    )
    kafka_ssl_cafile: Optional[str] = Field(default=None, alias="KAFKA_SSL_CAFILE")
    kafka_ssl_certfile: Optional[str] = Field(default=None, alias="KAFKA_SSL_CERTFILE")
    kafka_ssl_keyfile: Optional[str] = Field(default=None, alias="KAFKA_SSL_KEYFILE")
    kafka_sasl_mechanism: str = Field(
        default="SCRAM-SHA-256", alias="KAFKA_SASL_MECHANISM"
    )
    kafka_sasl_username: Optional[str] = Field(
        default=None, alias="KAFKA_SASL_USERNAME"
    )
    kafka_sasl_password: Optional[str] = Field(
        default=None, alias="KAFKA_SASL_PASSWORD"
    )
    kafka_topic_partitions: int = Field(
        default=2, ge=1, le=2, alias="KAFKA_TOPIC_PARTITIONS"
    )

    # Prefect Cloud
    prefect_api_key: Optional[str] = Field(default=None, alias="PREFECT_API_KEY")
    prefect_api_url: Optional[str] = Field(default=None, alias="PREFECT_API_URL")

    # External APIs
    mta_api_key: Optional[str] = Field(default=None, alias="MTA_API_KEY")
    nyc_traffic_app_token: Optional[str] = Field(
        default=None, alias="NYC_TRAFFIC_APP_TOKEN"
    )
    openweathermap_api_key: Optional[str] = Field(
        default=None, alias="OPENWEATHERMAP_API_KEY"
    )
    groq_api_key: Optional[str] = Field(default=None, alias="GROQ_API_KEY")
    gemini_api_key: Optional[str] = Field(default=None, alias="GEMINI_API_KEY")

    # MLflow
    mlflow_tracking_uri: str = Field(
        default="http://localhost:5000", alias="MLFLOW_TRACKING_URI"
    )

    # Cloudflare R2 (Model Artifact & Database Backup - ADR-007)
    r2_bucket_name: Optional[str] = Field(default=None, alias="R2_BUCKET_NAME")
    r2_endpoint_url: Optional[str] = Field(default=None, alias="R2_ENDPOINT_URL")
    r2_access_key_id: Optional[str] = Field(default=None, alias="R2_ACCESS_KEY_ID")
    r2_secret_access_key: Optional[str] = Field(
        default=None, alias="R2_SECRET_ACCESS_KEY"
    )

    # Monitoring & Automated Retraining (ADR-026)
    auto_retrain_on_drift: bool = Field(default=False, alias="AUTO_RETRAIN_ON_DRIFT")
    drift_retrain_cooldown_hours: int = Field(
        default=48, alias="DRIFT_RETRAIN_COOLDOWN_HOURS"
    )

    @property
    def database_url(self) -> str:
        """Construct the PostgreSQL SQLAlchemy connection URL."""
        if self.database_url_override:
            value = self.database_url_override.replace(
                "postgres://", "postgresql://", 1
            )
            url = make_url(value)
            if os.getenv("POSTGRES_SSLROOTCERT"):
                url = url.update_query_dict(
                    {"sslrootcert": os.environ["POSTGRES_SSLROOTCERT"]}
                )
            return url.render_as_string(hide_password=False)
        return URL.create(
            "postgresql+psycopg2",
            username=self.postgres_user,
            password=self.postgres_password,
            host=self.postgres_host,
            port=self.postgres_port,
            database=self.postgres_db,
        ).render_as_string(hide_password=False)

    @property
    def redis_url(self) -> str:
        """Construct the Redis connection URL."""
        if self.redis_url_override:
            if os.getenv("REDIS_SSL_CA"):
                parts = urlsplit(self.redis_url_override)
                query = dict(parse_qsl(parts.query))
                query.update(
                    ssl_ca_certs=os.environ["REDIS_SSL_CA"],
                    ssl_cert_reqs="required",
                    ssl_check_hostname="true",
                )
                return urlunsplit(parts._replace(query=urlencode(query)))
            return self.redis_url_override
        return f"redis://{self.redis_host}:{self.redis_port}/0"


@lru_cache()
def get_settings() -> Settings:
    """Return a cached instance of application settings."""
    return Settings()
