"""Model Registry loader and in-memory model lifecycle manager for online serving (M5-1, ADR-020).

Resolves, eagerly loads, warms up, and caches trained LightGBM models in 'Production' stage
from the MLflow Model Registry at startup, with graceful fallback to local baseline heuristics.
"""

from __future__ import annotations

import logging
import warnings
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Protocol, runtime_checkable

import mlflow
import mlflow.pyfunc
import numpy as np
import pandas as pd
from mlflow.entities.model_registry import ModelVersion
from mlflow.tracking import MlflowClient

from src.common.mlflow_utils import (
    DEMAND_MODEL_NAME,
    DURATION_MODEL_NAME,
    get_mlflow_client,
    get_tracking_uri,
    setup_mlflow,
)
from src.training.baseline import (
    CorridorDurationBaseline,
    DemandSeasonalNaiveBaseline,
)

logger = logging.getLogger(__name__)


@runtime_checkable
class Predictor(Protocol):
    """Protocol for models capable of generating predictions on a DataFrame."""

    def predict(self, df: pd.DataFrame) -> np.ndarray:
        """Generate predictions for the input DataFrame."""
        ...


@dataclass
class LoadedModelInfo:
    """Encapsulates a loaded production model or fallback estimator with metadata."""

    model_name: str
    version: str
    stage: str
    run_id: str
    loaded_at: str
    status: str  # "production" | "baseline_fallback"
    is_fallback: bool
    model: Any
    metadata: Dict[str, Any] = field(default_factory=dict)

    def predict(self, df: pd.DataFrame) -> np.ndarray:
        """Execute model prediction and return 1D numpy array."""
        if hasattr(self.model, "predict"):
            preds = self.model.predict(df)
            if isinstance(preds, (pd.Series, pd.DataFrame)):
                return preds.to_numpy().flatten()
            return np.asarray(preds).flatten()
        raise TypeError(f"Loaded model {self.model_name} does not implement .predict()")


class ModelLoaderService:
    """Manages MLflow model registry resolution, eager startup loading, and baseline fallback."""

    def __init__(
        self,
        tracking_uri: Optional[str] = None,
        demand_model_name: str = DEMAND_MODEL_NAME,
        duration_model_name: str = DURATION_MODEL_NAME,
        client: Optional[MlflowClient] = None,
    ) -> None:
        self.tracking_uri = tracking_uri or get_tracking_uri()
        setup_mlflow(self.tracking_uri)
        self.demand_model_name = demand_model_name
        self.duration_model_name = duration_model_name
        self._client = client
        self._models: Dict[str, LoadedModelInfo] = {}

    @property
    def client(self) -> MlflowClient:
        """Instantiate or return the active MlflowClient."""
        if self._client is None:
            self._client = get_mlflow_client(self.tracking_uri)
        return self._client

    def resolve_production_model_version(
        self, model_name: str
    ) -> Optional[ModelVersion]:
        """Query MLflow registry for active version in 'Production' stage.

        Suppresses FutureWarning from MLflow 3.x stage API deprecation.
        """
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore", category=FutureWarning, module="mlflow.*"
                )
                versions = self.client.get_latest_versions(
                    model_name, stages=["Production"]
                )

            if not versions:
                logger.info(
                    "No version found in 'Production' stage for model '%s'", model_name
                )
                return None

            # Sort by version integer descending in case multiple versions exist
            sorted_versions = sorted(
                versions, key=lambda v: int(v.version), reverse=True
            )
            return sorted_versions[0]
        except Exception as exc:
            logger.warning(
                "Failed to query MLflow Model Registry for '%s' (URI: %s): %s",
                model_name,
                self.tracking_uri,
                exc,
            )
            return None

    def load_demand_model(self) -> LoadedModelInfo:
        """Load production demand model from MLflow or fallback to DemandSeasonalNaiveBaseline."""
        loaded_at = datetime.now(timezone.utc).isoformat()
        version_obj = self.resolve_production_model_version(self.demand_model_name)

        if version_obj is not None:
            model_uri = f"models:/{self.demand_model_name}/{version_obj.version}"
            try:
                logger.info(
                    "Loading Production demand model from MLflow registry: %s (v%s, run_id=%s)",
                    model_uri,
                    version_obj.version,
                    version_obj.run_id,
                )
                with warnings.catch_warnings():
                    warnings.filterwarnings(
                        "ignore", category=FutureWarning, module="mlflow.*"
                    )
                    model = mlflow.pyfunc.load_model(model_uri)

                info = LoadedModelInfo(
                    model_name=self.demand_model_name,
                    version=str(version_obj.version),
                    stage="Production",
                    run_id=str(version_obj.run_id),
                    loaded_at=loaded_at,
                    status="production",
                    is_fallback=False,
                    model=model,
                    metadata={"source": version_obj.source},
                )
                self._models[self.demand_model_name] = info
                return info
            except Exception as load_err:
                logger.error(
                    "Error loading model artifact from %s; falling back to baseline: %s",
                    model_uri,
                    load_err,
                )

        logger.warning(
            "Using fallback baseline estimator for demand: DemandSeasonalNaiveBaseline"
        )
        fallback_model = DemandSeasonalNaiveBaseline()
        info = LoadedModelInfo(
            model_name=self.demand_model_name,
            version="baseline-fallback",
            stage="None",
            run_id="local-baseline",
            loaded_at=loaded_at,
            status="baseline_fallback",
            is_fallback=True,
            model=fallback_model,
            metadata={"strategy": "seasonal_naive"},
        )
        self._models[self.demand_model_name] = info
        return info

    def load_duration_model(self) -> LoadedModelInfo:
        """Load production corridor duration model from MLflow or fallback to CorridorDurationBaseline."""
        loaded_at = datetime.now(timezone.utc).isoformat()
        version_obj = self.resolve_production_model_version(self.duration_model_name)

        if version_obj is not None:
            model_uri = f"models:/{self.duration_model_name}/{version_obj.version}"
            try:
                logger.info(
                    "Loading Production duration model from MLflow registry: %s (v%s, run_id=%s)",
                    model_uri,
                    version_obj.version,
                    version_obj.run_id,
                )
                with warnings.catch_warnings():
                    warnings.filterwarnings(
                        "ignore", category=FutureWarning, module="mlflow.*"
                    )
                    model = mlflow.pyfunc.load_model(model_uri)

                info = LoadedModelInfo(
                    model_name=self.duration_model_name,
                    version=str(version_obj.version),
                    stage="Production",
                    run_id=str(version_obj.run_id),
                    loaded_at=loaded_at,
                    status="production",
                    is_fallback=False,
                    model=model,
                    metadata={"source": version_obj.source},
                )
                self._models[self.duration_model_name] = info
                return info
            except Exception as load_err:
                logger.error(
                    "Error loading model artifact from %s; falling back to baseline: %s",
                    model_uri,
                    load_err,
                )

        logger.warning(
            "Using fallback baseline estimator for duration: CorridorDurationBaseline"
        )
        fallback_model = CorridorDurationBaseline()
        info = LoadedModelInfo(
            model_name=self.duration_model_name,
            version="baseline-fallback",
            stage="None",
            run_id="local-baseline",
            loaded_at=loaded_at,
            status="baseline_fallback",
            is_fallback=True,
            model=fallback_model,
            metadata={"strategy": "distance_velocity_heuristic"},
        )
        self._models[self.duration_model_name] = info
        return info

    def load_all(self) -> Dict[str, LoadedModelInfo]:
        """Eagerly load both demand and duration models at startup."""
        self.load_demand_model()
        self.load_duration_model()
        self.warmup_models()
        return self._models

    def warmup_models(self) -> None:
        """Run a single-row inference pass through each loaded model to warm up memory and runtime."""
        # Warmup demand model
        if self.demand_model_name in self._models:
            demand_info = self._models[self.demand_model_name]
            dummy_demand_df = pd.DataFrame(
                [
                    {
                        "zone_id": 161,
                        "pickup_count_last_15m": 10,
                        "pickup_count_last_1h": 40,
                        "pickup_count_last_24h": 900,
                        "pickup_count_same_hour_last_week": 35,
                        "hour_of_day": 12,
                        "day_of_week": 2,
                        "is_weekend": 0,
                        "is_holiday": 0,
                        "sin_hour": 0.0,
                        "cos_hour": 1.0,
                        "sin_day_of_week": 0.0,
                        "cos_day_of_week": 1.0,
                    }
                ]
            )
            dummy_demand_df["zone_id"] = dummy_demand_df["zone_id"].astype("category")
            try:
                demand_info.predict(dummy_demand_df)
                logger.info("Demand model warmup evaluation passed successfully.")
            except Exception as exc:
                logger.warning("Demand model warmup failed: %s", exc)

        # Warmup duration model
        if self.duration_model_name in self._models:
            duration_info = self._models[self.duration_model_name]
            dummy_duration_df = pd.DataFrame(
                [
                    {
                        "pickup_zone_id": 161,
                        "dropoff_zone_id": 236,
                        "avg_duration_last_15m": 900.0,
                        "avg_duration_last_1h": 950.0,
                        "log_avg_duration_last_1h": np.log1p(950.0),
                        "distance_km": 4.5,
                        "origin_zone_demand_pressure": 40,
                        "hour_of_day": 12,
                        "day_of_week": 2,
                        "is_weekend": 0,
                        "sin_hour": 0.0,
                        "cos_hour": 1.0,
                        "sin_day_of_week": 0.0,
                        "cos_day_of_week": 1.0,
                    }
                ]
            )
            dummy_duration_df["pickup_zone_id"] = dummy_duration_df[
                "pickup_zone_id"
            ].astype("category")
            dummy_duration_df["dropoff_zone_id"] = dummy_duration_df[
                "dropoff_zone_id"
            ].astype("category")
            try:
                duration_info.predict(dummy_duration_df)
                logger.info("Duration model warmup evaluation passed successfully.")
            except Exception as exc:
                logger.warning("Duration model warmup failed: %s", exc)

    def get_model(self, model_name: str) -> LoadedModelInfo:
        """Return the loaded model info by name, loading it if not already loaded."""
        if model_name not in self._models:
            if model_name == self.demand_model_name:
                return self.load_demand_model()
            elif model_name == self.duration_model_name:
                return self.load_duration_model()
            raise KeyError(f"Unknown model name: {model_name}")
        return self._models[model_name]

    def get_health_metadata(self) -> Dict[str, Any]:
        """Return model loading status for /health check endpoint."""
        models_meta = {}
        for name, info in self._models.items():
            models_meta[name] = {
                "name": info.model_name,
                "version": info.version,
                "stage": info.stage,
                "run_id": info.run_id,
                "loaded_at": info.loaded_at,
                "status": info.status,
                "is_fallback": info.is_fallback,
            }

        all_ready = len(self._models) >= 2
        any_fallback = any(info.is_fallback for info in self._models.values())

        return {
            "all_models_loaded": all_ready,
            "has_fallback_models": any_fallback,
            "fallback_active": any_fallback,
            "models": models_meta,
        }
