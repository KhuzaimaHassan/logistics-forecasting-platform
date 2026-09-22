"""Pydantic request and response schemas for FastAPI online serving endpoints.

Defines typed contracts for:
- Health check and dependency status (GET /health)
- Single and batch demand predictions (GET /predict/demand/{zone_id}, POST /predict/demand/batch)
- Single and batch corridor ETA predictions (GET /predict/eta, POST /predict/eta/batch)
- Online feature inspection (GET /features/{entity_type}/{entity_id})
- Pipeline orchestration status (GET /pipeline/status)
- Standardized REST error envelopes
"""

from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Error Envelope
# ---------------------------------------------------------------------------


class ErrorResponse(BaseModel):
    """Standard error response envelope matching docs/API.md."""

    error: str = Field(..., description="Short error code or classification")
    detail: str = Field(..., description="Human-readable explanation of the error")


# ---------------------------------------------------------------------------
# Health Check Schemas
# ---------------------------------------------------------------------------


class HealthDependencyStatus(BaseModel):
    """Status of upstream infrastructure dependencies."""

    database: str = Field(..., description="PostgreSQL status: connected | unreachable")
    redis: str = Field(
        ..., description="Redis online store status: connected | unreachable"
    )
    mlflow: str = Field(
        ..., description="MLflow registry status: reachable | unreachable"
    )


class ModelHealthInfo(BaseModel):
    """Metadata for a loaded model."""

    name: str
    version: str
    stage: str
    status: str
    is_fallback: bool
    run_id: Optional[str] = None
    loaded_at: Optional[str] = None


class HealthCheckResponse(BaseModel):
    """Response schema for GET /health endpoint."""

    status: str = Field(..., description="Overall service status: ok | degraded")
    dependencies: HealthDependencyStatus
    models: Dict[str, ModelHealthInfo]
    timestamp: str


# ---------------------------------------------------------------------------
# Demand Prediction Schemas
# ---------------------------------------------------------------------------


class DemandPredictionResponse(BaseModel):
    """Response schema for GET /predict/demand/{zone_id}."""

    zone_id: int = Field(
        ..., ge=1, le=263, description="NYC TLC Taxi Zone ID (1 to 263)"
    )
    horizon_minutes: int = Field(default=15, description="Forecast horizon in minutes")
    predicted_pickups: float = Field(
        ..., ge=0.0, description="Predicted pickup volume for the upcoming horizon"
    )
    status: str = Field(..., description="Prediction status: ok | degraded_fallback")
    cache_hit: bool = Field(
        ..., description="True if online features were available in Redis"
    )
    model_version: str = Field(
        ..., description="Version of model booster used for scoring"
    )
    as_of: str = Field(..., description="UTC ISO 8601 timestamp of forecast execution")
    warning: Optional[str] = Field(
        None, description="Descriptive warning when running in degraded fallback mode"
    )


class DemandBatchPredictionRequest(BaseModel):
    """Request schema for POST /predict/demand/batch."""

    zone_ids: Optional[List[int]] = Field(
        None,
        description=(
            "List of NYC TLC zone IDs (1-263). If omitted or empty, defaults to all 263 active zones."
        ),
    )
    horizon_minutes: int = Field(
        default=15, ge=5, le=60, description="Forecast horizon in minutes (5 to 60)"
    )


class DemandBatchPredictionItem(BaseModel):
    """Single zone item within a batch demand prediction response."""

    zone_id: int
    horizon_minutes: int
    predicted_pickups: float
    status: str
    cache_hit: bool
    warning: Optional[str] = None


class DemandBatchPredictionResponse(BaseModel):
    """Response schema for POST /predict/demand/batch."""

    predictions: List[DemandBatchPredictionItem]
    model_version: str
    as_of: str
    prediction_count: int


# ---------------------------------------------------------------------------
# ETA Prediction Schemas
# ---------------------------------------------------------------------------


class ETAPredictionResponse(BaseModel):
    """Response schema for GET /predict/eta."""

    origin_zone_id: int = Field(..., ge=1, le=263, description="Origin NYC TLC zone ID")
    dest_zone_id: int = Field(
        ..., ge=1, le=263, description="Destination NYC TLC zone ID"
    )
    corridor_id: str = Field(
        ..., description="Corridor key in '{origin}_{dest}' format"
    )
    predicted_duration_seconds: float = Field(
        ..., ge=60.0, description="Predicted trip duration in seconds (>= 60s)"
    )
    predicted_duration_minutes: float = Field(
        ..., ge=1.0, description="Predicted trip duration in minutes"
    )
    status: str = Field(..., description="Prediction status: ok | degraded_fallback")
    cache_hit: bool = Field(
        ..., description="True if corridor features were available in Redis"
    )
    model_version: str = Field(
        ..., description="Version of model booster used for scoring"
    )
    as_of: str = Field(..., description="UTC ISO 8601 timestamp of forecast execution")
    warning: Optional[str] = Field(
        None, description="Descriptive warning when running in degraded fallback mode"
    )


class CorridorItem(BaseModel):
    """Single origin-destination corridor pair."""

    origin_zone_id: int = Field(..., description="Origin zone ID (1-263)")
    dest_zone_id: int = Field(..., description="Destination zone ID (1-263)")


class ETABatchPredictionRequest(BaseModel):
    """Request schema for POST /predict/eta/batch."""

    corridors: List[CorridorItem] = Field(
        ..., min_length=1, description="List of corridor pairs to evaluate"
    )


class ETABatchPredictionItem(BaseModel):
    """Single corridor item within a batch ETA prediction response."""

    origin_zone_id: int
    dest_zone_id: int
    corridor_id: str
    predicted_duration_seconds: float
    predicted_duration_minutes: float
    status: str
    cache_hit: bool
    warning: Optional[str] = None


class ETABatchPredictionResponse(BaseModel):
    """Response schema for POST /predict/eta/batch."""

    predictions: List[ETABatchPredictionItem]
    model_version: str
    as_of: str
    prediction_count: int


# ---------------------------------------------------------------------------
# Feature Lookup Schemas
# ---------------------------------------------------------------------------


class FeatureLookupResponse(BaseModel):
    """Response schema for GET /features/{entity_type}/{entity_id}."""

    entity_type: str = Field(..., description="Entity category: zone | corridor")
    entity_id: Union[int, str] = Field(
        ..., description="Zone integer ID or corridor string ID"
    )
    features: Dict[str, Any] = Field(
        ..., description="Key-value dictionary of online feature values"
    )
    cache_hit: bool = Field(
        ..., description="True if feature values exist in Redis online store"
    )
    retrieved_at: str = Field(..., description="UTC ISO 8601 timestamp of retrieval")


# ---------------------------------------------------------------------------
# Pipeline Status Schemas
# ---------------------------------------------------------------------------


class PipelineRunItem(BaseModel):
    """Summary of a single orchestration pipeline execution run."""

    run_id: str
    job_name: str
    status: str
    started_at: str
    finished_at: Optional[str] = None
    duration_seconds: Optional[float] = None
    records_processed: int = 0
    error_message: Optional[str] = None
    triggered_by: Optional[str] = None


class PipelineStatusResponse(BaseModel):
    """Response schema for GET /pipeline/status."""

    status: str = Field(
        ..., description="Overall pipeline health: healthy | degraded | empty"
    )
    latest_runs: List[PipelineRunItem] = Field(
        default_factory=list, description="Recent pipeline runs ordered latest first"
    )
    checked_at: str


# ---------------------------------------------------------------------------
# Agent Ops Copilot Schemas (M7-4)
# ---------------------------------------------------------------------------


class AgentChatRequest(BaseModel):
    """Request payload for POST /agent/chat."""

    query: str = Field(
        ...,
        min_length=1,
        max_length=1000,
        description="User operational query or prompt.",
        examples=["What is the current demand and feature state for zone 161?"],
    )
    conversation_id: Optional[str] = Field(
        None,
        max_length=100,
        description="Optional client conversation/session identifier.",
    )
    history: Optional[List[Dict[str, str]]] = Field(
        default=None,
        description="Optional list of prior turn message dictionaries (role, content).",
    )


class AgentChatResponse(BaseModel):
    """Structured response from LangGraph Ops Copilot."""

    response: str = Field(..., description="Synthesized operational markdown response.")
    conversation_id: Optional[str] = Field(
        None, description="Echoed or generated conversation session identifier."
    )
    tools_used: List[str] = Field(
        default_factory=list,
        description="List of allowlisted read-only tools executed during this turn.",
    )
    sources: List[str] = Field(
        default_factory=list,
        description="List of external data sources or documentation cited.",
    )
    provider: str = Field(
        ..., description="LLM provider name: 'groq' | 'gemini' | 'mock'."
    )
    model_name: str = Field(
        ...,
        description="Model identifier used (e.g. 'llama-3.3-70b-versatile', 'gemini-2.0-flash', 'mock-rule-engine').",
    )
    latency_ms: float = Field(
        ..., description="End-to-end turn processing latency in milliseconds."
    )
    status: str = Field(
        ..., description="Turn outcome status: 'success' | 'blocked' | 'error'."
    )
    error: Optional[str] = Field(
        None, description="Error message if status is 'error'."
    )
    timestamp: str = Field(
        ..., description="ISO 8601 UTC timestamp of response creation."
    )


# ---------------------------------------------------------------------------
# Monitoring Reports Schemas (M8-3)
# ---------------------------------------------------------------------------


class MonitoringReportItem(BaseModel):
    """Metadata and summary of an Evidently AI monitoring report."""

    report_id: str
    report_type: str
    generated_at: str
    drift_detected: bool = False
    retrain_recommended: bool = False
    alert_severity: Optional[str] = None
    alert_reasons: List[str] = Field(default_factory=list)
    drift_share: Optional[float] = None
    number_of_drifted_columns: Optional[int] = None
    number_of_columns: Optional[int] = None
    drifted_features: List[str] = Field(default_factory=list)
    file_path: Optional[str] = None
    summary_json: Dict[str, Any] = Field(default_factory=dict)


class MonitoringReportsListResponse(BaseModel):
    """Response schema for GET /monitoring/reports."""

    status: str = Field(
        ..., description="Query outcome status: 'success' | 'empty' | 'error'"
    )
    count: int = Field(..., description="Number of reports returned")
    has_active_alerts: bool = Field(
        False,
        description="True if any returned report triggered drift or retraining",
    )
    reports: List[MonitoringReportItem] = Field(default_factory=list)
    retrieved_at: str
