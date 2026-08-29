"""MLflow tracking client helpers and canonical experiment management."""

import logging
import os
from typing import Any, Dict, Optional

import mlflow
from mlflow.tracking import MlflowClient

from src.common.config import get_settings

logger = logging.getLogger(__name__)

DEMAND_EXPERIMENT_NAME = "nyc-taxi-demand-forecasting"
DURATION_EXPERIMENT_NAME = "nyc-taxi-corridor-eta"


def get_tracking_uri() -> str:
    """Return the configured MLflow tracking URI from settings or environment."""
    settings = get_settings()
    uri = os.getenv("MLFLOW_TRACKING_URI", settings.mlflow_tracking_uri)
    return uri


def setup_mlflow(tracking_uri: Optional[str] = None) -> str:
    """Configure MLflow tracking URI globally and return the active URI."""
    uri = tracking_uri or get_tracking_uri()
    mlflow.set_tracking_uri(uri)
    logger.info("Configured MLflow tracking URI: %s", uri)
    return uri


def get_mlflow_client(tracking_uri: Optional[str] = None) -> MlflowClient:
    """Return an instantiated MlflowClient pointed at the configured tracking URI."""
    uri = tracking_uri or setup_mlflow()
    return MlflowClient(tracking_uri=uri)


def get_or_create_experiment(
    experiment_name: str,
    artifact_location: Optional[str] = None,
    tags: Optional[Dict[str, Any]] = None,
    client: Optional[MlflowClient] = None,
) -> str:
    """Get an existing MLflow experiment ID or create it if not present.

    Args:
        experiment_name: Name of the MLflow experiment.
        artifact_location: Optional custom storage path for artifacts.
        tags: Optional metadata tags to attach during creation.
        client: Optional MlflowClient instance.

    Returns:
        The string experiment ID.
    """
    client = client or get_mlflow_client()
    exp = client.get_experiment_by_name(experiment_name)
    if exp is not None:
        return exp.experiment_id

    try:
        exp_id = client.create_experiment(
            name=experiment_name,
            artifact_location=artifact_location,
            tags=tags,
        )
        logger.info("Created MLflow experiment '%s' (ID: %s)", experiment_name, exp_id)
        return exp_id
    except Exception as exc:
        # Check if created concurrently by another process
        exp = client.get_experiment_by_name(experiment_name)
        if exp is not None:
            return exp.experiment_id
        logger.error(
            "Failed to get or create experiment '%s': %s", experiment_name, exc
        )
        raise exc
