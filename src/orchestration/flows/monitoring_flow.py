"""Daily scheduled Prefect flow for data drift, prediction drift, and model performance monitoring.

ADR-026:
- Modern Evidently 0.7 Core API with mandatory keyword arguments (current_data=..., reference_data=...).
- Single source of truth: Baseline MAE fetched directly from MLflow Production model.
- Universal 48-hour cooldown check querying warehouse.pipeline_runs for ANY retraining run.
- Independent out-of-band trigger: Drift-triggered retraining does NOT reset weekly Sunday cron timer.
- Staged alert-first policy: Gated behind AUTO_RETRAIN_ON_DRIFT (default false).
"""

import argparse
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from prefect import flow, task
from sqlalchemy.engine import Engine

from src.common.config import get_settings
from src.common.db import get_engine
from src.common.mlflow_utils import DEMAND_MODEL_NAME
from src.monitoring.analyzers import (
    DataDriftAnalyzer,
    PerformanceDecayAnalyzer,
    PredictionDriftAnalyzer,
)
from src.monitoring.schemas import (
    MonitoringReportSummary,
    ReportType,
)
from src.monitoring.service import (
    DEFAULT_COOLDOWN_HOURS,
    DEFAULT_CURRENT_HOURS,
    DEFAULT_MIN_REFERENCE_SAMPLES,
    DEFAULT_REFERENCE_DAYS,
    fetch_mlflow_production_baseline,
    fetch_monitoring_datasets,
    is_retraining_in_cooldown,
    record_pipeline_run,
    save_monitoring_report_db,
)

logger = logging.getLogger(__name__)


@task(
    name="extract_monitoring_datasets",
    retries=2,
    retry_delay_seconds=10,
    cache_policy=None,
)
def extract_monitoring_datasets_task(
    engine: Optional[Engine] = None,
    now: Optional[datetime] = None,
    current_hours: int = DEFAULT_CURRENT_HOURS,
    reference_days: int = DEFAULT_REFERENCE_DAYS,
    min_reference_samples: int = DEFAULT_MIN_REFERENCE_SAMPLES,
) -> Dict[str, Any]:
    """Extract 24-hour current and 14-day rolling (or Jan 2024 fallback) monitoring datasets."""
    eng = engine or get_engine()
    return fetch_monitoring_datasets(
        engine=eng,
        now=now,
        current_hours=current_hours,
        reference_days=reference_days,
        min_reference_samples=min_reference_samples,
    )


@task(
    name="run_drift_analysis",
    retries=1,
    retry_delay_seconds=5,
    cache_policy=None,
)
def run_drift_analysis_task(
    current_df: pd.DataFrame,
    reference_df: pd.DataFrame,
    baseline_mae: Optional[float] = None,
    baseline_rmse: Optional[float] = None,
    critical_features: Optional[List[str]] = None,
) -> Dict[str, MonitoringReportSummary]:
    """Execute DataDriftAnalyzer, PredictionDriftAnalyzer, and PerformanceDecayAnalyzer.

    Mandatory ADR-026 Standard:
    - Passes current_data and reference_data with explicit keyword arguments.
    - Uses MLflow Production model val_mae as single source of truth for baseline performance.
    """
    results: Dict[str, MonitoringReportSummary] = {}

    # Feature columns for demand data drift
    feature_cols = [
        "pickup_count_last_15m",
        "pickup_count_last_1h",
        "pickup_count_last_24h",
        "pickup_count_same_hour_last_week",
        "hour_of_day",
        "day_of_week",
        "is_weekend",
        "is_holiday",
        "avg_temp_last_1h",
        "is_precipitating",
    ]
    avail_curr_features = [c for c in feature_cols if c in current_df.columns]
    avail_ref_features = [c for c in feature_cols if c in reference_df.columns]
    common_features = list(set(avail_curr_features).intersection(avail_ref_features))

    # 1. Data Drift Analysis
    if common_features and len(current_df) > 0 and len(reference_df) > 0:
        logger.info(
            "Executing DataDriftAnalyzer across %d common features...",
            len(common_features),
        )
        data_analyzer = DataDriftAnalyzer(
            critical_features=critical_features,
        )
        data_summary = data_analyzer.analyze(
            current_df=current_df[common_features],
            reference_df=reference_df[common_features],
        )
        results["data_drift"] = data_summary
    else:
        logger.warning(
            "Skipping DataDriftAnalyzer: Insufficient feature columns or empty data (curr=%d, ref=%d).",
            len(current_df),
            len(reference_df),
        )
        results["data_drift"] = MonitoringReportSummary(
            report_type=ReportType.DATA_DRIFT,
            drift_detected=False,
            retrain_recommended=False,
            alert_severity="INFO",
            alert_reasons=["Insufficient feature data for data drift analysis"],
        )

    # 2. Prediction Drift Analysis
    if (
        "predicted_value" in current_df.columns
        and "predicted_value" in reference_df.columns
        and len(current_df) > 0
        and len(reference_df) > 0
    ):
        logger.info("Executing PredictionDriftAnalyzer...")
        pred_analyzer = PredictionDriftAnalyzer()
        pred_summary = pred_analyzer.analyze(
            current_df=current_df[["predicted_value"]],
            reference_df=reference_df[["predicted_value"]],
        )
        results["prediction_drift"] = pred_summary
    else:
        logger.warning(
            "Skipping PredictionDriftAnalyzer: Missing predicted_value column or empty data."
        )
        results["prediction_drift"] = MonitoringReportSummary(
            report_type=ReportType.PREDICTION_DRIFT,
            drift_detected=False,
            retrain_recommended=False,
            alert_severity="INFO",
            alert_reasons=["Missing predicted_value column or empty data"],
        )

    # 3. Performance Decay Analysis
    has_labels = (
        "actual_value" in current_df.columns and "predicted_value" in current_df.columns
    )
    valid_labels_count = (
        len(current_df.dropna(subset=["actual_value", "predicted_value"]))
        if has_labels
        else 0
    )

    if baseline_mae is not None and baseline_mae > 0 and valid_labels_count >= 5:
        logger.info(
            "Executing PerformanceDecayAnalyzer (baseline MAE=%.4f, samples=%d)...",
            baseline_mae,
            valid_labels_count,
        )
        decay_analyzer = PerformanceDecayAnalyzer(
            baseline_mae=baseline_mae,
            baseline_rmse=baseline_rmse,
        )
        perf_summary = decay_analyzer.analyze(
            current_df=current_df,
            reference_df=(
                reference_df
                if "actual_value" in reference_df.columns
                and "predicted_value" in reference_df.columns
                else None
            ),
        )
        results["performance_decay"] = perf_summary
    else:
        reason = (
            f"Fewer than 5 evaluated actuals available (found {valid_labels_count})"
            if baseline_mae is not None
            else "MLflow Production baseline MAE not established"
        )
        logger.info("Skipping PerformanceDecayAnalyzer: %s.", reason)
        results["performance_decay"] = MonitoringReportSummary(
            report_type=ReportType.PERFORMANCE_DECAY,
            drift_detected=False,
            retrain_recommended=False,
            alert_severity="INFO",
            alert_reasons=[f"Performance decay analysis skipped: {reason}"],
        )

    return results


@task(
    name="persist_monitoring_reports",
    retries=2,
    retry_delay_seconds=5,
    cache_policy=None,
)
def persist_monitoring_reports_task(
    summaries: Dict[str, MonitoringReportSummary],
    engine: Optional[Engine] = None,
    html_dir: Optional[Path] = None,
) -> Dict[str, str]:
    """Persist all generated monitoring report summaries to database and disk HTML."""
    eng = engine or get_engine()
    report_ids = {}
    for key, summary in summaries.items():
        rep_id = save_monitoring_report_db(summary, engine=eng, html_dir=html_dir)
        report_ids[key] = rep_id
    logger.info("Successfully persisted %d monitoring reports to DB.", len(report_ids))
    return report_ids


@task(
    name="evaluate_retraining_trigger",
    cache_policy=None,
)
def evaluate_retraining_trigger_task(
    summaries: Dict[str, MonitoringReportSummary],
    auto_retrain: Optional[bool] = None,
    cooldown_hours: int = DEFAULT_COOLDOWN_HOURS,
    engine: Optional[Engine] = None,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Evaluate alert-first gate and trigger retraining if auto_retrain is enabled and cooldown is clear.

    ADR-026 Logic:
    1. Check if any analyzer flagged retrain_recommended=True.
    2. If NOT: Nominal operation. No alerts, no retraining.
    3. If YES:
       - Aggregate alert reasons and severity.
       - Check AUTO_RETRAIN_ON_DRIFT (default false):
         - If false: Log critical alert for human action; return action="alert_logged".
         - If true: Query warehouse.pipeline_runs for ANY completed retraining run in last cooldown_hours.
           - If in cooldown: Suppress retraining; return action="cooldown_suppressed".
           - If clear: Invoke scheduled_retraining_flow(triggered_by="evidently_drift_alert");
             return action="retrain_triggered".
    """
    eng = engine or get_engine()
    settings = get_settings()
    should_auto_retrain = (
        auto_retrain if auto_retrain is not None else settings.auto_retrain_on_drift
    )

    retrain_recommended = any(s.retrain_recommended for s in summaries.values())
    all_reasons = []
    for s in summaries.values():
        all_reasons.extend(s.alert_reasons)

    if not retrain_recommended:
        logger.info(
            "No drift or performance decay detected. Model operating within nominal parameters."
        )
        return {
            "retrain_recommended": False,
            "action": "none",
            "retrain_triggered": False,
            "alert_reasons": [],
        }

    logger.warning(
        "Monitoring detected model drift or performance decay! Recommended actions: %s",
        all_reasons,
    )

    if not should_auto_retrain:
        logger.warning(
            "ADR-026 Alert-First Gate: AUTO_RETRAIN_ON_DRIFT is False. Alert recorded in warehouse.monitoring_reports; manual engineer confirmation required."
        )
        return {
            "retrain_recommended": True,
            "action": "alert_logged",
            "retrain_triggered": False,
            "alert_reasons": all_reasons,
        }

    # Auto-retraining is enabled: verify 48-hour cooldown against warehouse.pipeline_runs
    in_cooldown = is_retraining_in_cooldown(
        engine=eng,
        cooldown_hours=cooldown_hours,
        now=now,
    )

    if in_cooldown:
        logger.warning(
            "Retraining suppressed by ADR-026 Cooldown: A successful retraining run completed within the last %d hours.",
            cooldown_hours,
        )
        return {
            "retrain_recommended": True,
            "action": "cooldown_suppressed",
            "retrain_triggered": False,
            "alert_reasons": all_reasons,
            "cooldown_hours": cooldown_hours,
        }

    # Cooldown is clear: Trigger automated retraining
    logger.info(
        "48-hour cooldown clear. Triggering drift-induced model retraining flow (triggered_by='evidently_drift_alert')..."
    )
    from src.orchestration.flows.retraining_flow import scheduled_retraining_flow

    retrain_summary = scheduled_retraining_flow(
        triggered_by="evidently_drift_alert",
    )
    return {
        "retrain_recommended": True,
        "action": "retrain_triggered",
        "retrain_triggered": True,
        "alert_reasons": all_reasons,
        "retraining_summary": retrain_summary,
    }


def run_monitoring_lifecycle(
    engine: Optional[Engine] = None,
    now: Optional[datetime] = None,
    current_hours: int = DEFAULT_CURRENT_HOURS,
    reference_days: int = DEFAULT_REFERENCE_DAYS,
    min_reference_samples: int = DEFAULT_MIN_REFERENCE_SAMPLES,
    auto_retrain: Optional[bool] = None,
    cooldown_hours: int = DEFAULT_COOLDOWN_HOURS,
    html_dir: Optional[Path] = None,
    current_df_override: Optional[pd.DataFrame] = None,
    reference_df_override: Optional[pd.DataFrame] = None,
    baseline_mae_override: Optional[float] = None,
) -> Dict[str, Any]:
    """Execute complete monitoring evaluation lifecycle.

    Reusable execution body callable directly or wrapped in Prefect flow.
    """
    t_start = time.perf_counter()
    flow_start_dt = datetime.now(timezone.utc)
    flow_run_id = f"monitor-{uuid.uuid4().hex[:12]}"
    eng = engine or get_engine()

    logger.info(
        "=== Starting Daily Model Monitoring Lifecycle (run_id=%s, now=%s) ===",
        flow_run_id,
        (now or flow_start_dt).isoformat(),
    )

    try:
        record_pipeline_run(
            engine=eng,
            run_id=flow_run_id,
            job_name="daily-model-monitoring-flow",
            status="running",
            started_at=flow_start_dt,
            triggered_by="cron",
        )
    except Exception as pr_err:
        logger.debug("Could not record initial monitoring pipeline_run: %s", pr_err)

    try:
        # 1. Extract datasets
        if current_df_override is not None and reference_df_override is not None:
            datasets = {
                "current_df": current_df_override,
                "reference_df": reference_df_override,
                "is_cold_start_fallback": False,
                "current_count": len(current_df_override),
                "reference_count": len(reference_df_override),
            }
        else:
            datasets = extract_monitoring_datasets_task.fn(
                engine=eng,
                now=now,
                current_hours=current_hours,
                reference_days=reference_days,
                min_reference_samples=min_reference_samples,
            )

        # 2. Query MLflow Production baseline MAE
        baseline_mae = baseline_mae_override
        if baseline_mae is None:
            baseline_mae = fetch_mlflow_production_baseline(DEMAND_MODEL_NAME)

        # 3. Run Drift & Performance Analyzers
        summaries = run_drift_analysis_task.fn(
            current_df=datasets["current_df"],
            reference_df=datasets["reference_df"],
            baseline_mae=baseline_mae,
        )

        # 4. Persist Monitoring Reports to DB and HTML
        report_ids = persist_monitoring_reports_task.fn(
            summaries=summaries,
            engine=eng,
            html_dir=html_dir,
        )

        # 5. Evaluate Retraining Trigger Gate
        trigger_decision = evaluate_retraining_trigger_task.fn(
            summaries=summaries,
            auto_retrain=auto_retrain,
            cooldown_hours=cooldown_hours,
            engine=eng,
            now=now,
        )

        elapsed = time.perf_counter() - t_start
        status = (
            "completed_with_alerts"
            if trigger_decision["retrain_recommended"]
            else "completed"
        )

        try:
            record_pipeline_run(
                engine=eng,
                run_id=flow_run_id,
                job_name="daily-model-monitoring-flow",
                status=status,
                started_at=flow_start_dt,
                finished_at=datetime.now(timezone.utc),
                duration_seconds=elapsed,
                records_processed=datasets["current_count"],
                triggered_by="cron",
            )
        except Exception as pr_err:
            logger.debug(
                "Could not record completed monitoring pipeline_run: %s", pr_err
            )

        return {
            "run_id": flow_run_id,
            "status": status,
            "elapsed_seconds": elapsed,
            "datasets": {
                "current_count": datasets["current_count"],
                "reference_count": datasets["reference_count"],
                "is_cold_start_fallback": datasets["is_cold_start_fallback"],
            },
            "report_ids": report_ids,
            "trigger_decision": trigger_decision,
            "baseline_mae": baseline_mae,
        }

    except Exception as exc:
        elapsed = time.perf_counter() - t_start
        logger.exception("Monitoring lifecycle failed: %s", exc)
        try:
            record_pipeline_run(
                engine=eng,
                run_id=flow_run_id,
                job_name="daily-model-monitoring-flow",
                status="failed",
                started_at=flow_start_dt,
                finished_at=datetime.now(timezone.utc),
                duration_seconds=elapsed,
                error_message=str(exc),
                triggered_by="cron",
            )
        except Exception as pr_err:
            logger.debug("Could not record failed monitoring pipeline_run: %s", pr_err)
        raise


@flow(name="daily-model-monitoring-flow")
def daily_model_monitoring_flow(
    now: Optional[datetime] = None,
    current_hours: int = DEFAULT_CURRENT_HOURS,
    reference_days: int = DEFAULT_REFERENCE_DAYS,
    min_reference_samples: int = DEFAULT_MIN_REFERENCE_SAMPLES,
    auto_retrain: Optional[bool] = None,
    cooldown_hours: int = DEFAULT_COOLDOWN_HOURS,
) -> Dict[str, Any]:
    """Top-level Prefect flow for daily model monitoring and drift evaluation."""
    return run_monitoring_lifecycle(
        now=now,
        current_hours=current_hours,
        reference_days=reference_days,
        min_reference_samples=min_reference_samples,
        auto_retrain=auto_retrain,
        cooldown_hours=cooldown_hours,
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description="Run daily model monitoring flow.")
    parser.add_argument(
        "--current-hours",
        type=int,
        default=DEFAULT_CURRENT_HOURS,
        help="Evaluation window in hours (default 24).",
    )
    parser.add_argument(
        "--auto-retrain",
        action="store_true",
        help="Override AUTO_RETRAIN_ON_DRIFT to True.",
    )
    parser.add_argument(
        "--cooldown-hours",
        type=int,
        default=DEFAULT_COOLDOWN_HOURS,
        help="Cooldown hours required between retrains (default 48).",
    )
    args = parser.parse_args()

    daily_model_monitoring_flow(
        current_hours=args.current_hours,
        auto_retrain=True if args.auto_retrain else None,
        cooldown_hours=args.cooldown_hours,
    )
