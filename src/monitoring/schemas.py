"""Data structures and Pydantic schemas for data drift, prediction drift, and performance monitoring."""

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field


class ReportType(str, Enum):
    """Supported monitoring report types."""

    DATA_DRIFT = "data_drift"
    PREDICTION_DRIFT = "prediction_drift"
    PERFORMANCE_DECAY = "performance_decay"


class DriftMetricSummary(BaseModel):
    """Summary of drift evaluation for a single feature or column."""

    model_config = ConfigDict(extra="ignore")

    column_name: str
    drift_detected: bool
    p_value: Optional[float] = None
    stat_test: Optional[str] = None
    threshold: Optional[float] = 0.05
    current_mean: Optional[float] = None
    reference_mean: Optional[float] = None
    is_critical: bool = False


class PerformanceMetricSummary(BaseModel):
    """Summary of model performance metric degradation against benchmark."""

    model_config = ConfigDict(extra="ignore")

    metric_name: str
    current_value: float
    reference_value: Optional[float] = None
    decay_ratio: Optional[float] = None
    threshold_ratio: Optional[float] = 0.15
    decay_detected: bool = False


class MonitoringReportSummary(BaseModel):
    """Aggregated monitoring report output persisted to PostgreSQL warehouse.monitoring_reports."""

    model_config = ConfigDict(extra="ignore")

    report_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    report_type: ReportType
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    drift_detected: bool = False
    retrain_recommended: bool = False
    alert_severity: Optional[str] = None  # "INFO", "WARNING", "CRITICAL"
    alert_reasons: List[str] = Field(default_factory=list)
    drift_share: Optional[float] = None
    number_of_drifted_columns: Optional[int] = None
    number_of_columns: Optional[int] = None
    metrics: List[Union[DriftMetricSummary, PerformanceMetricSummary]] = Field(
        default_factory=list
    )
    summary_json: Dict[str, Any] = Field(default_factory=dict)
    html_content: Optional[str] = None
    file_path: Optional[str] = None

    def to_db_record(self) -> Dict[str, Any]:
        """Convert report summary to dictionary matching warehouse.monitoring_reports table schema."""
        return {
            "report_id": self.report_id,
            "report_type": (
                self.report_type.value
                if isinstance(self.report_type, ReportType)
                else str(self.report_type)
            ),
            "generated_at": self.generated_at,
            "summary_json": self.summary_json
            or {
                "report_id": self.report_id,
                "report_type": (
                    self.report_type.value
                    if isinstance(self.report_type, ReportType)
                    else str(self.report_type)
                ),
                "generated_at": self.generated_at.isoformat(),
                "drift_detected": self.drift_detected,
                "retrain_recommended": self.retrain_recommended,
                "alert_severity": self.alert_severity,
                "alert_reasons": self.alert_reasons,
                "drift_share": self.drift_share,
                "number_of_drifted_columns": self.number_of_drifted_columns,
                "number_of_columns": self.number_of_columns,
                "metrics": [m.model_dump() for m in self.metrics],
            },
            "file_path": self.file_path,
        }
