"""Evidently 0.7 Core drift and performance decay analyzers.

Complies strictly with ADR-026:
- Uses modern Evidently 0.7 Core API (Report, Dataset, DataDefinition, Regression, DataDriftPreset, RegressionPreset).
- Enforces mandatory keyword invocation: report.run(current_data=..., reference_data=...).
- Implements hybrid reference evaluation and alert-first staged retraining recommendations.
- Sources baseline performance metrics strictly from MLflow Production model benchmarks.
"""

import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Union

import numpy as np
import pandas as pd
from evidently import DataDefinition, Dataset, Regression, Report
from evidently.presets import DataDriftPreset, RegressionPreset

from src.monitoring.schemas import (
    DriftMetricSummary,
    MonitoringReportSummary,
    PerformanceMetricSummary,
    ReportType,
)

logger = logging.getLogger(__name__)

# Default thresholds per ADR-026
DEFAULT_DRIFT_SHARE_THRESHOLD = 0.40  # 40% of columns drifted flags dataset drift
DEFAULT_STAT_TEST_THRESHOLD = 0.05  # p-value threshold for individual features
DEFAULT_CRITICAL_P_THRESHOLD = 0.01  # p-value threshold for critical demand features
DEFAULT_DECAY_THRESHOLD = 0.15  # 15% MAE degradation flags performance decay
DEFAULT_CRITICAL_FEATURES = ["pickup_count_last_1h", "avg_trip_duration_last_1h"]


class BaseAnalyzer:
    """Base analyzer providing common DataDefinition builders and serialization routines."""

    @staticmethod
    def create_data_definition(
        df: pd.DataFrame,
        numerical_columns: Optional[List[str]] = None,
        categorical_columns: Optional[List[str]] = None,
        regression: Optional[List[Regression]] = None,
    ) -> DataDefinition:
        """Construct an Evidently DataDefinition with automatic column-type inference if not provided."""
        if numerical_columns is None and categorical_columns is None:
            # Auto-infer from DataFrame types
            num_cols = list(
                df.select_dtypes(include=[np.number, "float", "int"]).columns
            )
            cat_cols = list(
                df.select_dtypes(include=["object", "category", "bool"]).columns
            )
        else:
            num_cols = numerical_columns or []
            cat_cols = categorical_columns or []

        # If regression target/prediction exist in num_cols, exclude from feature columns if needed
        return DataDefinition(
            numerical_columns=num_cols if num_cols else None,
            categorical_columns=cat_cols if cat_cols else None,
            regression=regression,
        )


class DataDriftAnalyzer(BaseAnalyzer):
    """Evaluates statistical feature distribution drift between current inference and reference baseline."""

    def __init__(
        self,
        drift_share_threshold: float = DEFAULT_DRIFT_SHARE_THRESHOLD,
        stat_test_threshold: float = DEFAULT_STAT_TEST_THRESHOLD,
        critical_features: Optional[List[str]] = None,
        critical_p_threshold: float = DEFAULT_CRITICAL_P_THRESHOLD,
    ) -> None:
        self.drift_share_threshold = drift_share_threshold
        self.stat_test_threshold = stat_test_threshold
        self.critical_features = critical_features or list(DEFAULT_CRITICAL_FEATURES)
        self.critical_p_threshold = critical_p_threshold

    def analyze(
        self,
        current_df: pd.DataFrame,
        reference_df: pd.DataFrame,
        numerical_columns: Optional[List[str]] = None,
        categorical_columns: Optional[List[str]] = None,
        report_id: Optional[str] = None,
    ) -> MonitoringReportSummary:
        """Run statistical data drift report comparing current against reference dataset.

        Args:
            current_df: Active evaluation dataset (e.g. rolling 24-hour inference features).
            reference_df: Baseline reference dataset (e.g. 14-day rolling or training split).
            numerical_columns: Explicit numerical feature names.
            categorical_columns: Explicit categorical feature names.
            report_id: Optional custom UUID for the report.

        Returns:
            MonitoringReportSummary with per-feature p-values, drift share, and retrain recommendation.
        """
        if current_df.empty or reference_df.empty:
            raise ValueError("Input DataFrames for DataDriftAnalyzer cannot be empty.")

        rep_id = report_id or str(uuid.uuid4())
        gen_at = datetime.now(timezone.utc)

        # Build data definitions and wrap in Evidently Dataset
        common_cols = [c for c in current_df.columns if c in reference_df.columns]
        curr_sub = current_df[common_cols].copy()
        ref_sub = reference_df[common_cols].copy()

        data_def = self.create_data_definition(
            curr_sub,
            numerical_columns=[c for c in (numerical_columns or []) if c in common_cols]
            or None,
            categorical_columns=[
                c for c in (categorical_columns or []) if c in common_cols
            ]
            or None,
        )

        curr_ds = Dataset.from_pandas(curr_sub, data_definition=data_def)
        ref_ds = Dataset.from_pandas(ref_sub, data_definition=data_def)

        # Instantiate Evidently Report with DataDriftPreset
        report = Report([DataDriftPreset(drift_share=self.drift_share_threshold)])

        # MANDATORY ADR-026: Explicit keyword arguments to guarantee target/reference directionality
        snapshot = report.run(current_data=curr_ds, reference_data=ref_ds)
        snapshot_dict = snapshot.dict()

        # Parse metrics from snapshot
        drift_metrics: List[DriftMetricSummary] = []
        drift_share = 0.0
        number_of_drifted = 0
        total_columns = len(common_cols)
        alert_reasons: List[str] = []

        for metric in snapshot_dict.get("metrics", []):
            cfg = metric.get("config", {})
            val = metric.get("value")
            metric_type = cfg.get("type", "")

            if metric_type == "evidently:metric_v2:DriftedColumnsCount" and isinstance(
                val, dict
            ):
                drift_share = float(val.get("share", 0.0))
                number_of_drifted = int(val.get("count", 0))
            elif metric_type == "evidently:metric_v2:ValueDrift":
                col_name = cfg.get("column", "")
                method = cfg.get("method", "")
                thresh = float(cfg.get("threshold", self.stat_test_threshold))
                p_val = float(val) if val is not None else None

                drift_detected = p_val is not None and p_val < thresh
                is_crit = col_name in self.critical_features

                c_mean = (
                    float(curr_sub[col_name].mean())
                    if col_name in curr_sub
                    and pd.api.types.is_numeric_dtype(curr_sub[col_name])
                    else None
                )
                r_mean = (
                    float(ref_sub[col_name].mean())
                    if col_name in ref_sub
                    and pd.api.types.is_numeric_dtype(ref_sub[col_name])
                    else None
                )

                drift_metrics.append(
                    DriftMetricSummary(
                        column_name=col_name,
                        drift_detected=drift_detected,
                        p_value=p_val,
                        stat_test=method,
                        threshold=thresh,
                        current_mean=c_mean,
                        reference_mean=r_mean,
                        is_critical=is_crit,
                    )
                )

                if drift_detected:
                    if (
                        is_crit
                        and p_val is not None
                        and p_val < self.critical_p_threshold
                    ):
                        alert_reasons.append(
                            f"Critical feature '{col_name}' drifted severely (p={p_val:.4e} < {self.critical_p_threshold})"
                        )
                    else:
                        alert_reasons.append(
                            f"Feature '{col_name}' drifted (p={p_val:.4e} < {thresh})"
                        )

        # Dataset drift decision per ADR-026
        dataset_drift_detected = drift_share >= self.drift_share_threshold
        if dataset_drift_detected:
            alert_reasons.append(
                f"Overall dataset drift share ({drift_share:.1%}) exceeded threshold ({self.drift_share_threshold:.1%})"
            )

        # Retraining recommendation: dataset drift OR critical feature drift
        has_critical_drift = any(
            m.is_critical
            and m.drift_detected
            and m.p_value is not None
            and m.p_value < self.critical_p_threshold
            for m in drift_metrics
        )
        retrain_recommended = dataset_drift_detected or has_critical_drift

        alert_severity: Optional[str] = None
        if retrain_recommended:
            alert_severity = "CRITICAL"
        elif number_of_drifted > 0:
            alert_severity = "WARNING"
        else:
            alert_severity = "INFO"

        html_content = snapshot.get_html_str(as_iframe=False)

        summary_json = {
            "report_id": rep_id,
            "report_type": ReportType.DATA_DRIFT.value,
            "generated_at": gen_at.isoformat(),
            "drift_detected": dataset_drift_detected,
            "retrain_recommended": retrain_recommended,
            "alert_severity": alert_severity,
            "alert_reasons": alert_reasons,
            "drift_share": drift_share,
            "number_of_drifted_columns": number_of_drifted,
            "number_of_columns": total_columns,
            "metrics": [m.model_dump() for m in drift_metrics],
        }

        return MonitoringReportSummary(
            report_id=rep_id,
            report_type=ReportType.DATA_DRIFT,
            generated_at=gen_at,
            drift_detected=dataset_drift_detected,
            retrain_recommended=retrain_recommended,
            alert_severity=alert_severity,
            alert_reasons=alert_reasons,
            drift_share=drift_share,
            number_of_drifted_columns=number_of_drifted,
            number_of_columns=total_columns,
            metrics=drift_metrics,
            summary_json=summary_json,
            html_content=html_content,
        )


class PredictionDriftAnalyzer(BaseAnalyzer):
    """Evaluates distribution shifts in model prediction outputs (e.g. predicted_value)."""

    def __init__(
        self,
        prediction_col: str = "predicted_value",
        stat_test_threshold: float = DEFAULT_STAT_TEST_THRESHOLD,
    ) -> None:
        self.prediction_col = prediction_col
        self.stat_test_threshold = stat_test_threshold

    def analyze(
        self,
        current_df: pd.DataFrame,
        reference_df: pd.DataFrame,
        report_id: Optional[str] = None,
    ) -> MonitoringReportSummary:
        """Evaluate prediction distribution drift between current predictions and reference baseline."""
        if self.prediction_col not in current_df.columns:
            raise KeyError(f"Column '{self.prediction_col}' not found in current_df.")
        if self.prediction_col not in reference_df.columns:
            raise KeyError(f"Column '{self.prediction_col}' not found in reference_df.")

        rep_id = report_id or str(uuid.uuid4())
        gen_at = datetime.now(timezone.utc)

        curr_sub = current_df[[self.prediction_col]].copy()
        ref_sub = reference_df[[self.prediction_col]].copy()

        data_def = DataDefinition(numerical_columns=[self.prediction_col])
        curr_ds = Dataset.from_pandas(curr_sub, data_definition=data_def)
        ref_ds = Dataset.from_pandas(ref_sub, data_definition=data_def)

        report = Report([DataDriftPreset(drift_share=0.5)])

        # MANDATORY ADR-026: Explicit keyword arguments
        snapshot = report.run(current_data=curr_ds, reference_data=ref_ds)
        snapshot_dict = snapshot.dict()

        p_val: Optional[float] = None
        stat_method = ""
        thresh = self.stat_test_threshold

        for metric in snapshot_dict.get("metrics", []):
            cfg = metric.get("config", {})
            if cfg.get("type") == "evidently:metric_v2:ValueDrift":
                p_val = (
                    float(metric.get("value"))
                    if metric.get("value") is not None
                    else None
                )
                stat_method = cfg.get("method", "")
                thresh = float(cfg.get("threshold", self.stat_test_threshold))

        drift_detected = p_val is not None and p_val < thresh
        c_mean = float(curr_sub[self.prediction_col].mean())
        r_mean = float(ref_sub[self.prediction_col].mean())

        metric_summary = DriftMetricSummary(
            column_name=self.prediction_col,
            drift_detected=drift_detected,
            p_value=p_val,
            stat_test=stat_method,
            threshold=thresh,
            current_mean=c_mean,
            reference_mean=r_mean,
            is_critical=True,
        )

        alert_reasons = []
        if drift_detected:
            alert_reasons.append(
                f"Prediction output distribution drifted (p={p_val:.4e} < {thresh}, current mean={c_mean:.2f}, reference mean={r_mean:.2f})"
            )

        alert_severity = "WARNING" if drift_detected else "INFO"
        html_content = snapshot.get_html_str(as_iframe=False)

        summary_json = {
            "report_id": rep_id,
            "report_type": ReportType.PREDICTION_DRIFT.value,
            "generated_at": gen_at.isoformat(),
            "drift_detected": drift_detected,
            "retrain_recommended": False,  # Prediction drift triggers warning; retrain recommended when paired with feature drift or performance decay
            "alert_severity": alert_severity,
            "alert_reasons": alert_reasons,
            "drift_share": 1.0 if drift_detected else 0.0,
            "number_of_drifted_columns": 1 if drift_detected else 0,
            "number_of_columns": 1,
            "metrics": [metric_summary.model_dump()],
        }

        return MonitoringReportSummary(
            report_id=rep_id,
            report_type=ReportType.PREDICTION_DRIFT,
            generated_at=gen_at,
            drift_detected=drift_detected,
            retrain_recommended=False,
            alert_severity=alert_severity,
            alert_reasons=alert_reasons,
            drift_share=1.0 if drift_detected else 0.0,
            number_of_drifted_columns=1 if drift_detected else 0,
            number_of_columns=1,
            metrics=[metric_summary],
            summary_json=summary_json,
            html_content=html_content,
        )


class PerformanceDecayAnalyzer(BaseAnalyzer):
    """Evaluates actual prediction error degradation against the active Production champion MLflow baseline."""

    def __init__(
        self,
        baseline_mae: float,
        baseline_rmse: Optional[float] = None,
        decay_threshold: float = DEFAULT_DECAY_THRESHOLD,
        target_col: str = "actual_value",
        prediction_col: str = "predicted_value",
    ) -> None:
        """Initialize performance decay analyzer.

        Args:
            baseline_mae: Active Production model validation MAE from MLflow (single source of truth).
            baseline_rmse: Optional active Production model validation RMSE from MLflow.
            decay_threshold: Fractional degradation tolerance (default 0.15 = 15%).
            target_col: Column name containing ground truth actuals.
            prediction_col: Column name containing model predictions.
        """
        if baseline_mae <= 0:
            raise ValueError(f"baseline_mae must be positive, got {baseline_mae}")
        self.baseline_mae = baseline_mae
        self.baseline_rmse = baseline_rmse
        self.decay_threshold = decay_threshold
        self.target_col = target_col
        self.prediction_col = prediction_col

    def analyze(
        self,
        current_df: pd.DataFrame,
        reference_df: Optional[pd.DataFrame] = None,
        report_id: Optional[str] = None,
    ) -> MonitoringReportSummary:
        """Evaluate model error on current inference actuals and benchmark against MLflow baseline.

        Args:
            current_df: DataFrame containing target_col (actuals) and prediction_col (predictions).
            reference_df: Optional reference DataFrame for Evidently RegressionPreset comparison.
            report_id: Optional custom UUID for the report.
        """
        # Filter down to rows where both actual and prediction are present
        valid_df = current_df.dropna(
            subset=[self.target_col, self.prediction_col]
        ).copy()
        if len(valid_df) < 5:
            raise ValueError(
                f"Insufficient ground truth actuals for PerformanceDecayAnalyzer: got {len(valid_df)} rows, minimum 5 required."
            )

        rep_id = report_id or str(uuid.uuid4())
        gen_at = datetime.now(timezone.utc)

        actuals = valid_df[self.target_col].to_numpy(dtype=float)
        predictions = valid_df[self.prediction_col].to_numpy(dtype=float)

        # Direct mathematical computation of core metrics
        abs_errors = np.abs(actuals - predictions)
        sq_errors = (actuals - predictions) ** 2

        current_mae = float(np.mean(abs_errors))
        current_rmse = float(np.sqrt(np.mean(sq_errors)))

        mae_decay_ratio = (current_mae - self.baseline_mae) / self.baseline_mae
        decay_detected = mae_decay_ratio > self.decay_threshold

        rmse_decay_ratio = None
        if self.baseline_rmse and self.baseline_rmse > 0:
            rmse_decay_ratio = (current_rmse - self.baseline_rmse) / self.baseline_rmse

        # Run Evidently RegressionPreset for rich interactive HTML reporting
        data_def = DataDefinition(
            regression=[
                Regression(target=self.target_col, prediction=self.prediction_col)
            ]
        )
        curr_ds = Dataset.from_pandas(valid_df, data_definition=data_def)

        ref_ds = None
        if reference_df is not None and not reference_df.empty:
            ref_valid = reference_df.dropna(
                subset=[self.target_col, self.prediction_col]
            ).copy()
            if len(ref_valid) >= 5:
                ref_ds = Dataset.from_pandas(ref_valid, data_definition=data_def)

        report = Report([RegressionPreset()])

        # MANDATORY ADR-026: Explicit keyword arguments
        snapshot = report.run(current_data=curr_ds, reference_data=ref_ds)
        html_content = snapshot.get_html_str(as_iframe=False)

        alert_reasons = []
        if decay_detected:
            alert_reasons.append(
                f"Model MAE degraded by {mae_decay_ratio:.1%} (Current MAE {current_mae:.2f} vs MLflow Production Baseline {self.baseline_mae:.2f}, threshold {self.decay_threshold:.1%})"
            )

        alert_severity: Optional[str] = None
        if decay_detected:
            alert_severity = "CRITICAL"
        elif mae_decay_ratio > 0.05:
            alert_severity = "WARNING"
        else:
            alert_severity = "INFO"

        perf_metrics: List[PerformanceMetricSummary] = [
            PerformanceMetricSummary(
                metric_name="mae",
                current_value=current_mae,
                reference_value=self.baseline_mae,
                decay_ratio=mae_decay_ratio,
                threshold_ratio=self.decay_threshold,
                decay_detected=decay_detected,
            ),
            PerformanceMetricSummary(
                metric_name="rmse",
                current_value=current_rmse,
                reference_value=self.baseline_rmse,
                decay_ratio=rmse_decay_ratio,
                threshold_ratio=self.decay_threshold,
                decay_detected=bool(
                    rmse_decay_ratio is not None
                    and rmse_decay_ratio > self.decay_threshold
                ),
            ),
        ]

        summary_json = {
            "report_id": rep_id,
            "report_type": ReportType.PERFORMANCE_DECAY.value,
            "generated_at": gen_at.isoformat(),
            "drift_detected": decay_detected,
            "retrain_recommended": decay_detected,
            "alert_severity": alert_severity,
            "alert_reasons": alert_reasons,
            "current_mae": current_mae,
            "baseline_mae": self.baseline_mae,
            "mae_decay_ratio": mae_decay_ratio,
            "current_rmse": current_rmse,
            "baseline_rmse": self.baseline_rmse,
            "rmse_decay_ratio": rmse_decay_ratio,
            "metrics": [m.model_dump() for m in perf_metrics],
        }

        return MonitoringReportSummary(
            report_id=rep_id,
            report_type=ReportType.PERFORMANCE_DECAY,
            generated_at=gen_at,
            drift_detected=decay_detected,
            retrain_recommended=decay_detected,
            alert_severity=alert_severity,
            alert_reasons=alert_reasons,
            metrics=perf_metrics,
            summary_json=summary_json,
            html_content=html_content,
        )


def save_report_html(
    summary: MonitoringReportSummary, output_path: Union[str, Path]
) -> str:
    """Write self-contained HTML snapshot to target filesystem path."""
    if not summary.html_content:
        raise ValueError("MonitoringReportSummary does not contain html_content.")

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(summary.html_content, encoding="utf-8")
    summary.file_path = str(path.resolve())
    return summary.file_path


def save_report_json(
    summary: MonitoringReportSummary, output_path: Union[str, Path]
) -> str:
    """Write summary JSON dictionary to target filesystem path."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(summary.summary_json, indent=2, default=str)
    path.write_text(content, encoding="utf-8")
    return str(path.resolve())
