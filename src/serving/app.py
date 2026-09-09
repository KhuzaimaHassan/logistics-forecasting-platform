"""FastAPI Serving Application for Logistics Demand and ETA Forecasting.

Exposes low-latency online inference and inspection endpoints per docs/API.md and ADR-020:
- GET  /health: Liveness and readiness check with dependency inspection
- GET  /predict/demand/{zone_id}: Single zone demand prediction (15m horizon)
- POST /predict/demand/batch: Vectorized city-wide or multi-zone demand prediction
- GET  /predict/eta: Single origin-destination corridor trip duration prediction
- POST /predict/eta/batch: Vectorized corridor batch ETA prediction
- GET  /features/{entity_type}/{entity_id}: Online feature store inspection
- GET  /pipeline/status: Orchestration pipeline run history
"""

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Dict, List

import numpy as np
import redis
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from src.common.config import get_settings
from src.common.db import check_database_connection, get_db_session
from src.common.models import PipelineRun
from src.features.client import (
    CorridorDurationOnlineFeatures,
    FeastOnlineClient,
    ZoneDemandOnlineFeatures,
)
from src.serving.feature_extractor import (
    build_corridor_feature_df,
    build_demand_feature_df,
    invert_log_duration,
)
from src.serving.model_loader import (
    DEMAND_MODEL_NAME,
    DURATION_MODEL_NAME,
    ModelLoaderService,
)
from src.serving.schemas import (
    DemandBatchPredictionItem,
    DemandBatchPredictionRequest,
    DemandBatchPredictionResponse,
    DemandPredictionResponse,
    ETABatchPredictionItem,
    ETABatchPredictionRequest,
    ETABatchPredictionResponse,
    ETAPredictionResponse,
    FeatureLookupResponse,
    HealthCheckResponse,
    HealthDependencyStatus,
    ModelHealthInfo,
    PipelineRunItem,
    PipelineStatusResponse,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lifespan Management
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Eagerly load models and initialize Feast client at startup."""
    logger.info("Initializing FastAPI serving application lifespan...")

    # Initialize ModelLoaderService if not already provided (e.g. by tests)
    if not hasattr(app.state, "model_loader") or app.state.model_loader is None:
        try:
            loader = ModelLoaderService()
            loader.load_all()
            app.state.model_loader = loader
            logger.info("ModelLoaderService eagerly initialized at startup.")
        except Exception as exc:
            logger.warning(
                "ModelLoaderService live initialization failed (%s); falling back to offline baseline loader.",
                exc,
            )
            fallback_loader = ModelLoaderService(tracking_uri="sqlite:///fallback.db")
            fallback_loader.load_all()
            app.state.model_loader = fallback_loader

    # Initialize FeastOnlineClient if not already provided
    if not hasattr(app.state, "feast_client") or app.state.feast_client is None:
        try:
            app.state.feast_client = FeastOnlineClient()
            logger.info("FeastOnlineClient initialized successfully.")
        except Exception as exc:
            logger.warning("FeastOnlineClient initialization failed: %s", exc)
            app.state.feast_client = None

    yield
    logger.info("Shutting down FastAPI serving application.")


# ---------------------------------------------------------------------------
# FastAPI Application Declaration
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Logistics Demand & ETA Forecasting Platform API",
    description="Online inference serving endpoints for NYC taxi zone demand and corridor ETA forecasting.",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Exception Handlers (Standardized API.md Error Envelope)
# ---------------------------------------------------------------------------


@app.exception_handler(HTTPException)
async def custom_http_exception_handler(
    request: Request, exc: HTTPException
) -> JSONResponse:
    """Format HTTP exceptions into standard {'error': '...', 'detail': '...'} shape."""
    error_code_map = {
        400: "bad_request",
        404: "not_found",
        422: "unprocessable_entity",
        500: "internal_error",
        503: "service_unavailable",
    }
    error_type = error_code_map.get(exc.status_code, "error")
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": error_type, "detail": exc.detail},
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    """Format Pydantic schema validation errors into standard error shape."""
    return JSONResponse(
        status_code=422,
        content={"error": "validation_error", "detail": str(exc)},
    )


# ---------------------------------------------------------------------------
# Dependency Getters
# ---------------------------------------------------------------------------


def get_model_loader(request: Request) -> ModelLoaderService:
    """Retrieve ModelLoaderService singleton from app.state."""
    loader = getattr(request.app.state, "model_loader", None)
    if loader is None:
        loader = ModelLoaderService()
        loader.load_all()
        request.app.state.model_loader = loader
    return loader


def get_feast_client(request: Request) -> FeastOnlineClient:
    """Retrieve FeastOnlineClient singleton from app.state."""
    client = getattr(request.app.state, "feast_client", None)
    if client is None:
        client = FeastOnlineClient()
        request.app.state.feast_client = client
    return client


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get("/health", response_model=HealthCheckResponse)
def health_check(request: Request) -> HealthCheckResponse:
    """Liveness and readiness check with database, Redis, and MLflow inspection."""
    now_iso = datetime.now(timezone.utc).isoformat()

    # 1. PostgreSQL Database Check
    db_ok = check_database_connection()
    db_status = "connected" if db_ok else "unreachable"

    # 2. Redis Online Store Check
    redis_status = "unreachable"
    try:
        settings = get_settings()
        r = redis.from_url(settings.redis_url, socket_timeout=1.0)
        if r.ping():
            redis_status = "connected"
    except Exception as r_exc:
        logger.debug("Redis health ping failed: %s", r_exc)
        redis_status = "unreachable"

    # 3. Model Loader & MLflow Check
    loader = get_model_loader(request)
    meta = loader.get_health_metadata()
    mlflow_status = "reachable" if not meta.get("has_fallback_models") else "degraded"

    models_map: Dict[str, ModelHealthInfo] = {}
    for key, info in meta.get("models", {}).items():
        # Expose canonical alias keys 'demand' and 'duration' as well as full model names
        alias = (
            "demand" if "demand" in key else ("duration" if "duration" in key else key)
        )
        model_obj = ModelHealthInfo(
            name=info.get("name", key),
            version=str(info.get("version", "1")),
            stage=str(info.get("stage", "None")),
            status=str(info.get("status", "unknown")),
            is_fallback=bool(info.get("is_fallback", False)),
            run_id=info.get("run_id"),
            loaded_at=info.get("loaded_at"),
        )
        models_map[alias] = model_obj
        models_map[key] = model_obj

    overall = (
        "ok"
        if (db_status == "connected" and redis_status == "connected")
        else "degraded"
    )

    return HealthCheckResponse(
        status=overall,
        dependencies=HealthDependencyStatus(
            database=db_status,
            redis=redis_status,
            mlflow=mlflow_status,
        ),
        models=models_map,
        timestamp=now_iso,
    )


@app.get("/predict/demand/{zone_id}", response_model=DemandPredictionResponse)
def predict_demand(
    zone_id: int,
    request: Request,
    horizon_minutes: int = Query(default=15, ge=5, le=60),
) -> DemandPredictionResponse:
    """Predict taxi pickup demand for a single NYC taxi zone.

    Validates zone ID in [1, 263]. If outside [1, 263] (including 264 'NV' and 265 'NA'), returns 404.
    If online features are cold/unmaterialized in Redis, imputes defaults and returns 200 with degraded_fallback.
    """
    if zone_id < 1 or zone_id > 263:
        raise HTTPException(
            status_code=404,
            detail=f"Zone ID {zone_id} is outside the servable forecast range [1, 263].",
        )

    loader = get_model_loader(request)
    feast = get_feast_client(request)
    now_utc = datetime.now(timezone.utc)

    # Retrieve features from Feast
    online_features = feast.get_zone_demand_features([zone_id])
    feat = (
        online_features[0]
        if online_features
        else ZoneDemandOnlineFeatures(zone_id=zone_id, cache_hit=False)
    )

    # Build model input DataFrame
    df = build_demand_feature_df([feat], now=now_utc)

    # Model inference pass
    demand_model = loader.get_model(DEMAND_MODEL_NAME)
    raw_pred = demand_model.predict(df)
    predicted_val = float(np.maximum(0.0, np.asarray(raw_pred).flatten()[0]))

    status = "ok" if feat.cache_hit else "degraded_fallback"
    warning = (
        None
        if feat.cache_hit
        else "Online features unmaterialized in Redis; evaluated using imputed defaults and calendar harmonics."
    )

    return DemandPredictionResponse(
        zone_id=zone_id,
        horizon_minutes=horizon_minutes,
        predicted_pickups=round(predicted_val, 2),
        status=status,
        cache_hit=feat.cache_hit,
        model_version=str(demand_model.version),
        as_of=now_utc.isoformat(),
        warning=warning,
    )


@app.post("/predict/demand/batch", response_model=DemandBatchPredictionResponse)
def predict_demand_batch(
    payload: DemandBatchPredictionRequest,
    request: Request,
) -> DemandBatchPredictionResponse:
    """Batch demand prediction across multiple zones or entire city in a single vectorized call.

    If zone_ids is empty or omitted, defaults to all 263 active TLC zones (1 to 263).
    If any zone_id is outside [1, 263], returns HTTP 400 (malformed batch payload).
    """
    zone_ids = (
        payload.zone_ids
        if (payload.zone_ids is not None and len(payload.zone_ids) > 0)
        else list(range(1, 264))
    )

    # Validate all requested zones are within [1, 263]
    invalid_zones = [z for z in zone_ids if z < 1 or z > 263]
    if invalid_zones:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid zone ID(s) in batch request: {invalid_zones}. "
                "Servable forecast zones must be in [1, 263]."
            ),
        )

    loader = get_model_loader(request)
    feast = get_feast_client(request)
    now_utc = datetime.now(timezone.utc)

    # Vectorized Feast retrieval
    features_list = feast.get_zone_demand_features(zone_ids)

    # Vectorized DataFrame construction
    df = build_demand_feature_df(features_list, now=now_utc)

    # Single vectorized LightGBM matrix scoring pass
    demand_model = loader.get_model(DEMAND_MODEL_NAME)
    raw_preds = np.asarray(demand_model.predict(df)).flatten()
    bounded_preds = np.maximum(0.0, raw_preds)

    items: List[DemandBatchPredictionItem] = []
    for feat, pred_val in zip(features_list, bounded_preds, strict=True):
        status = "ok" if feat.cache_hit else "degraded_fallback"
        warning = (
            None
            if feat.cache_hit
            else "Online features unmaterialized in Redis; evaluated using imputed defaults and calendar harmonics."
        )
        items.append(
            DemandBatchPredictionItem(
                zone_id=feat.zone_id,
                horizon_minutes=payload.horizon_minutes,
                predicted_pickups=round(float(pred_val), 2),
                status=status,
                cache_hit=feat.cache_hit,
                warning=warning,
            )
        )

    return DemandBatchPredictionResponse(
        predictions=items,
        model_version=str(demand_model.version),
        as_of=now_utc.isoformat(),
        prediction_count=len(items),
    )


@app.get("/predict/eta", response_model=ETAPredictionResponse)
def predict_eta(
    origin: int = Query(..., description="Origin NYC TLC zone ID (1 to 263)"),
    dest: int = Query(..., description="Destination NYC TLC zone ID (1 to 263)"),
    request: Request = None,
) -> ETAPredictionResponse:
    """Predict trip duration for an origin-destination corridor under current conditions.

    Validates origin and dest in [1, 263]. If outside [1, 263], returns HTTP 404.
    Inverts log1p duration model prediction with a 60.0 second minimum floor constraint.
    """
    if origin < 1 or origin > 263:
        raise HTTPException(
            status_code=404,
            detail=f"Origin zone ID {origin} is outside the servable forecast range [1, 263].",
        )
    if dest < 1 or dest > 263:
        raise HTTPException(
            status_code=404,
            detail=f"Destination zone ID {dest} is outside the servable forecast range [1, 263].",
        )

    loader = get_model_loader(request)
    feast = get_feast_client(request)
    now_utc = datetime.now(timezone.utc)
    corridor_id = f"{origin}_{dest}"

    corridor_features = feast.get_corridor_duration_features([corridor_id])
    feat = (
        corridor_features[0]
        if corridor_features
        else CorridorDurationOnlineFeatures(corridor_id=corridor_id, cache_hit=False)
    )

    df = build_corridor_feature_df(
        [feat], origin_dest_pairs=[(origin, dest)], now=now_utc
    )

    duration_model = loader.get_model(DURATION_MODEL_NAME)
    raw_log_pred = duration_model.predict(df)
    is_log = not duration_model.is_fallback
    dur_sec, dur_min = invert_log_duration(raw_log_pred, is_log_space=is_log)

    status = "ok" if feat.cache_hit else "degraded_fallback"
    warning = (
        None
        if feat.cache_hit
        else "Corridor features unmaterialized in Redis; evaluated using imputed distance and calendar harmonics."
    )

    return ETAPredictionResponse(
        origin_zone_id=origin,
        dest_zone_id=dest,
        corridor_id=corridor_id,
        predicted_duration_seconds=dur_sec,
        predicted_duration_minutes=dur_min,
        status=status,
        cache_hit=feat.cache_hit,
        model_version=str(duration_model.version),
        as_of=now_utc.isoformat(),
        warning=warning,
    )


@app.post("/predict/eta/batch", response_model=ETABatchPredictionResponse)
def predict_eta_batch(
    payload: ETABatchPredictionRequest,
    request: Request,
) -> ETABatchPredictionResponse:
    """Batch corridor trip duration prediction.

    Validates all corridor origin/destination IDs in [1, 263]. If any are outside, returns HTTP 400.
    """
    invalid_corridors = []
    for c in payload.corridors:
        if (
            c.origin_zone_id < 1
            or c.origin_zone_id > 263
            or c.dest_zone_id < 1
            or c.dest_zone_id > 263
        ):
            invalid_corridors.append(f"{c.origin_zone_id}_{c.dest_zone_id}")

    if invalid_corridors:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid corridor pair(s) in batch request: {invalid_corridors}. "
                "Origin and destination zone IDs must be in [1, 263]."
            ),
        )

    loader = get_model_loader(request)
    feast = get_feast_client(request)
    now_utc = datetime.now(timezone.utc)
    corridor_ids = [f"{c.origin_zone_id}_{c.dest_zone_id}" for c in payload.corridors]
    pairs = [(c.origin_zone_id, c.dest_zone_id) for c in payload.corridors]

    features_list = feast.get_corridor_duration_features(corridor_ids)
    df = build_corridor_feature_df(features_list, origin_dest_pairs=pairs, now=now_utc)

    duration_model = loader.get_model(DURATION_MODEL_NAME)
    raw_log_preds = np.asarray(duration_model.predict(df)).flatten()
    is_log = not duration_model.is_fallback

    items: List[ETABatchPredictionItem] = []
    for (orig, dest), cid, feat, raw_val in zip(
        pairs, corridor_ids, features_list, raw_log_preds, strict=True
    ):
        dur_sec, dur_min = invert_log_duration(raw_val, is_log_space=is_log)
        status = "ok" if feat.cache_hit else "degraded_fallback"
        warning = (
            None
            if feat.cache_hit
            else "Corridor features unmaterialized in Redis; evaluated using imputed distance and calendar harmonics."
        )
        items.append(
            ETABatchPredictionItem(
                origin_zone_id=orig,
                dest_zone_id=dest,
                corridor_id=cid,
                predicted_duration_seconds=dur_sec,
                predicted_duration_minutes=dur_min,
                status=status,
                cache_hit=feat.cache_hit,
                warning=warning,
            )
        )

    return ETABatchPredictionResponse(
        predictions=items,
        model_version=str(duration_model.version),
        as_of=now_utc.isoformat(),
        prediction_count=len(items),
    )


@app.get("/features/{entity_type}/{entity_id}", response_model=FeatureLookupResponse)
def lookup_features(
    entity_type: str,
    entity_id: str,
    request: Request,
) -> FeatureLookupResponse:
    """Inspect raw online-store feature vector for a zone or corridor."""
    feast = get_feast_client(request)
    now_utc = datetime.now(timezone.utc)

    if entity_type == "zone":
        try:
            zid = int(entity_id)
        except ValueError as val_err:
            raise HTTPException(
                status_code=400,
                detail=f"Zone entity_id must be an integer, got '{entity_id}'.",
            ) from val_err
        if zid < 1 or zid > 263:
            raise HTTPException(
                status_code=404,
                detail=f"Zone ID {zid} is outside the servable forecast range [1, 263].",
            )
        features = feast.get_zone_demand_features([zid])[0]
        raw_dict = features.to_dict()
        cache_hit = raw_dict.pop("cache_hit", False)
        raw_dict.pop("zone_id", None)
        return FeatureLookupResponse(
            entity_type="zone",
            entity_id=zid,
            features=raw_dict,
            cache_hit=cache_hit,
            retrieved_at=now_utc.isoformat(),
        )
    elif entity_type == "corridor":
        parts = str(entity_id).split("_")
        if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
            raise HTTPException(
                status_code=400,
                detail=f"Corridor entity_id must be in '{{origin}}_{{dest}}' format, got '{entity_id}'.",
            )
        orig, dest = int(parts[0]), int(parts[1])
        if orig < 1 or orig > 263 or dest < 1 or dest > 263:
            raise HTTPException(
                status_code=404,
                detail=f"Corridor zone IDs '{entity_id}' are outside the servable forecast range [1, 263].",
            )
        features = feast.get_corridor_duration_features([entity_id])[0]
        raw_dict = features.to_dict()
        cache_hit = raw_dict.pop("cache_hit", False)
        raw_dict.pop("corridor_id", None)
        return FeatureLookupResponse(
            entity_type="corridor",
            entity_id=entity_id,
            features=raw_dict,
            cache_hit=cache_hit,
            retrieved_at=now_utc.isoformat(),
        )
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid entity_type '{entity_type}'. Must be 'zone' or 'corridor'.",
        )


@app.get("/pipeline/status", response_model=PipelineStatusResponse)
def pipeline_status() -> PipelineStatusResponse:
    """Query recent orchestration execution history from warehouse.pipeline_runs."""
    now_utc = datetime.now(timezone.utc)
    runs: List[PipelineRunItem] = []

    try:
        with get_db_session() as session:
            db_runs = (
                session.query(PipelineRun)
                .order_by(PipelineRun.started_at.desc())
                .limit(10)
                .all()
            )
            for r in db_runs:
                runs.append(
                    PipelineRunItem(
                        run_id=str(r.run_id),
                        job_name=str(r.job_name),
                        status=str(r.status),
                        started_at=(r.started_at.isoformat() if r.started_at else ""),
                        finished_at=(
                            r.finished_at.isoformat() if r.finished_at else None
                        ),
                        duration_seconds=(
                            float(r.duration_seconds)
                            if r.duration_seconds is not None
                            else None
                        ),
                        records_processed=int(r.records_processed or 0),
                        error_message=r.error_message,
                        triggered_by=r.triggered_by,
                    )
                )
        overall = (
            "healthy"
            if any(r.status == "completed" for r in runs)
            else ("empty" if not runs else "degraded")
        )
    except Exception as exc:
        logger.warning("Could not query pipeline_runs table: %s", exc)
        overall = "empty"

    return PipelineStatusResponse(
        status=overall,
        latest_runs=runs,
        checked_at=now_utc.isoformat(),
    )
