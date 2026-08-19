"""Application configuration and environment settings."""

from functools import lru_cache
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


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

    # Redis
    redis_host: str = Field(default="localhost", alias="REDIS_HOST")
    redis_port: int = Field(default=6379, alias="REDIS_PORT")

    # Redpanda
    redpanda_broker: str = Field(default="localhost:9092", alias="REDPANDA_BROKER")

    # Prefect Cloud
    prefect_api_key: Optional[str] = Field(default=None, alias="PREFECT_API_KEY")
    prefect_api_url: Optional[str] = Field(default=None, alias="PREFECT_API_URL")

    # External APIs
    mta_api_key: Optional[str] = Field(default=None, alias="MTA_API_KEY")
    openweathermap_api_key: Optional[str] = Field(
        default=None, alias="OPENWEATHERMAP_API_KEY"
    )
    groq_api_key: Optional[str] = Field(default=None, alias="GROQ_API_KEY")
    gemini_api_key: Optional[str] = Field(default=None, alias="GEMINI_API_KEY")

    # MLflow
    mlflow_tracking_uri: str = Field(
        default="http://localhost:5000", alias="MLFLOW_TRACKING_URI"
    )

    @property
    def database_url(self) -> str:
        """Construct the PostgreSQL SQLAlchemy connection URL."""
        if self.database_url_override:
            return self.database_url_override
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache()
def get_settings() -> Settings:
    """Return a cached instance of application settings."""
    return Settings()
