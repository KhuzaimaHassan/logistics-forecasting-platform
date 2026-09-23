"""Database and MLflow service operations for model monitoring, drift tracking, and trigger gating.

ADR-026:
- Universal cooldown check against warehouse.pipeline_runs for ANY retraining run.
- Staged alert-first persistence to warehouse.monitoring_reports and disk HTML artifacts.
- MLflow Production champion val_mae as the single source of truth for baseline performance.
- 14-day rolling reference extraction with automatic cold-start fallback to January 2024.
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
from mlflow.tracking import MlflowClient
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.common.db import get_engine
from src.common.mlflow_utils import DEMAND_MODEL_NAME, get_mlflow_client
from src.monitoring.schemas import MonitoringReportSummary
from src.training.promotion import ModelPromotionGate

logger = logging.getLogger(__name__)

# Canonical training baseline fallback date ranges.
# The platform's canonical TLC training data is January 2023 (src/training/dataset.py: DEFAULT_TRAIN_START = 2023-01-08).
# January 2024 is also supported for future training batches.
FALLBACK_BASELINE_START_2023 = "2023-01-01 00:00:00+00"
FALLBACK_BASELINE_END_2023 = "2023-01-29 00:00:00+00"
FALLBACK_BASELINE_START_2024 = "2024-01-01 00:00:00+00"
FALLBACK_BASELINE_END_2024 = "2024-01-29 00:00:00+00"

# Default monitoring parameters
DEFAULT_COOLDOWN_HOURS = 48
DEFAULT_CURRENT_HOURS = 24
DEFAULT_REFERENCE_DAYS = 14
DEFAULT_MIN_REFERENCE_SAMPLES = 500
REPORTS_ARTIFACT_DIR = Path("artifacts/monitoring_reports")


def is_retraining_in_cooldown(
    engine: Optional[Engine] = None,
    cooldown_hours: int = DEFAULT_COOLDOWN_HOURS,
    now: Optional[datetime] = None,
) -> bool:
    """Check if a retraining run successfully completed within the cooldown window.

    ADR-026 Invariant:
    Queries warehouse.pipeline_runs for ANY retraining run (scheduled or drift-triggered)
    that completed within the last `cooldown_hours`. The query deliberately does NOT
    filter on `triggered_by`, ensuring both scheduled and drift-triggered runs suppress
    redundant immediate retrains.

    Args:
        engine: SQLAlchemy Engine instance.
        cooldown_hours: Duration of cooldown in hours (default 48).
        now: Reference timestamp for evaluation (defaults to UTC now).

    Returns:
        True if an active retraining run completed within cooldown_hours, False otherwise.
    """
    eng = engine or get_engine()
    is_sqlite = eng.dialect.name == "sqlite"
    schema_prefix = "" if is_sqlite else "warehouse."

    ref_time = now or datetime.now(timezone.utc)
    cutoff = ref_time - timedelta(hours=cooldown_hours)

    query = text(f"""
        SELECT run_id, job_name, status, started_at, finished_at, triggered_by
        FROM {schema_prefix}pipeline_runs
        WHERE (job_name IN ('scheduled-model-retraining-flow', 'retraining_flow') OR LOWER(job_name) LIKE '%retrain%')
          AND status = 'completed'
          AND COALESCE(finished_at, started_at) >= :cutoff
        ORDER BY started_at DESC
        LIMIT 1
    """)

    with eng.connect() as conn:
        row = conn.execute(query, {"cutoff": cutoff}).fetchone()

    if row is not None:
        logger.info(
            "Retraining cooldown ACTIVE: Found completed run '%s' (job='%s', triggered_by='%s', finished_at='%s'). Cutoff was '%s'.",
            row[0],
            row[1],
            row[5],
            row[4],
            cutoff,
        )
        return True

    logger.debug(
        "Retraining cooldown CLEAR: No completed retraining runs found since '%s'.",
        cutoff,
    )
    return False


def fetch_mlflow_production_baseline(
    model_name: str = DEMAND_MODEL_NAME,
    client: Optional[MlflowClient] = None,
) -> Optional[float]:
    """Retrieve active Production model validation MAE from MLflow Model Registry.

    ADR-026: MLflow Production champion val_mae is the single source of truth for
    benchmarking performance decay, preventing silent drift between registry and monitoring.

    Args:
        model_name: Registered model name in MLflow.
        client: Optional instantiated MlflowClient.

    Returns:
        Float baseline MAE if found, or None if no Production stage model exists.
    """
    try:
        gate = ModelPromotionGate(client=client or get_mlflow_client())
        prod_model = gate.get_current_production_model(model_name)
        if prod_model and "metrics" in prod_model:
            val_mae = prod_model["metrics"].get("val_mae")
            if val_mae is not None:
                logger.info(
                    "Fetched MLflow Production baseline MAE for '%s': %.4f (run_id=%s, version=%s)",
                    model_name,
                    float(val_mae),
                    prod_model.get("run_id"),
                    prod_model.get("version"),
                )
                return float(val_mae)
    except Exception as exc:
        logger.warning(
            "Failed to retrieve Production baseline MAE from MLflow for '%s': %s",
            model_name,
            exc,
        )
    return None


def fetch_monitoring_datasets(
    engine: Optional[Engine] = None,
    now: Optional[datetime] = None,
    current_hours: int = DEFAULT_CURRENT_HOURS,
    reference_days: int = DEFAULT_REFERENCE_DAYS,
    min_reference_samples: int = DEFAULT_MIN_REFERENCE_SAMPLES,
) -> Dict[str, Any]:
    """Extract current evaluation window and historical reference window from database.

    ADR-026 Reference Window Strategy:
    1. Current window: [now - 24h, now] extracted from warehouse.predictions joined with
       warehouse.zone_demand_features_hourly.
    2. Reference window: Evaluates warehouse.predictions row count in [now - 15d, now - 1d].
       - If >= 500 rows: extracts 14-day rolling historical window.
       - If < 500 rows (cold start): falls back to historical January 2024 feature baseline.

    Args:
        engine: SQLAlchemy Engine instance.
        now: Evaluation timestamp (defaults to UTC now).
        current_hours: Number of hours in evaluation window (default 24).
        reference_days: Days in rolling reference window (default 14).
        min_reference_samples: Minimum row count required to use rolling predictions (default 500).

    Returns:
        Dict containing 'current_df', 'reference_df', 'is_cold_start_fallback',
        'current_count', and 'reference_count'.
    """
    eng = engine or get_engine()
    is_sqlite = eng.dialect.name == "sqlite"
    schema_prefix = "" if is_sqlite else "warehouse."

    ref_time = now or datetime.now(timezone.utc)
    curr_start = ref_time - timedelta(hours=current_hours)
    curr_end = ref_time

    ref_start = curr_start - timedelta(days=reference_days)
    ref_end = curr_start

    logger.info(
        "Extracting monitoring datasets: current window [%s to %s], reference window [%s to %s]",
        curr_start.isoformat(),
        curr_end.isoformat(),
        ref_start.isoformat(),
        ref_end.isoformat(),
    )

    if is_sqlite:
        hour_trunc = "strftime('%Y-%m-%d %H:00:00', p.predicted_at)"
        hour_extract = "CAST(strftime('%H', p.predicted_at) AS INT)"
        dow_extract = "CAST(strftime('%w', p.predicted_at) AS INT)"
        is_weekend_expr = "CAST(strftime('%w', p.predicted_at) AS INT) IN (0, 6)"
    else:
        hour_trunc = "date_trunc('hour', p.predicted_at)"
        hour_extract = "EXTRACT(HOUR FROM p.predicted_at)::INT"
        dow_extract = "EXTRACT(DOW FROM p.predicted_at)::INT"
        is_weekend_expr = "EXTRACT(DOW FROM p.predicted_at)::INT IN (0, 6)"

    pred_join_query = text(f"""
        SELECT
            p.prediction_id,
            p.entity_id,
            CAST(p.predicted_value AS FLOAT) AS predicted_value,
            p.predicted_at,
            CAST(p.actual_value AS FLOAT) AS actual_value,
            COALESCE(f.pickup_count_last_15m, 0) AS pickup_count_last_15m,
            COALESCE(f.pickup_count_last_1h, 0) AS pickup_count_last_1h,
            COALESCE(f.pickup_count_last_24h, 0) AS pickup_count_last_24h,
            COALESCE(f.pickup_count_same_hour_last_week, 0) AS pickup_count_same_hour_last_week,
            COALESCE(f.hour_of_day, {hour_extract}) AS hour_of_day,
            COALESCE(f.day_of_week, {dow_extract}) AS day_of_week,
            COALESCE(f.is_weekend, {is_weekend_expr}) AS is_weekend,
            COALESCE(f.is_holiday, FALSE) AS is_holiday,
            COALESCE(f.avg_temp_last_1h, 15.0) AS avg_temp_last_1h,
            COALESCE(f.is_precipitating, FALSE) AS is_precipitating
        FROM {schema_prefix}predictions p
        LEFT JOIN {schema_prefix}zone_demand_features_hourly f
          ON f.zone_id = CAST(p.entity_id AS INTEGER)
         AND f.pickup_datetime = {hour_trunc}
        WHERE p.entity_type = 'demand'
          AND p.predicted_at >= :start_time
          AND p.predicted_at <= :end_time
        ORDER BY p.predicted_at ASC
    """)

    with eng.connect() as conn:
        # 1. Fetch current window predictions
        curr_df = pd.read_sql(
            pred_join_query,
            conn,
            params={"start_time": curr_start, "end_time": curr_end},
        )

        # 2. Check rolling reference predictions count
        count_query = text(f"""
            SELECT COUNT(*)
            FROM {schema_prefix}predictions
            WHERE entity_type = 'demand'
              AND predicted_at >= :start_time
              AND predicted_at < :end_time
        """)
        ref_pred_count = (
            conn.execute(
                count_query, {"start_time": ref_start, "end_time": ref_end}
            ).scalar()
            or 0
        )

        is_fallback = ref_pred_count < min_reference_samples

        if not is_fallback:
            # Sufficient rolling history exists in warehouse.predictions
            logger.info(
                "Reference window has %d predictions (>= %d minimum). Using rolling historical window.",
                ref_pred_count,
                min_reference_samples,
            )
            ref_df = pd.read_sql(
                pred_join_query,
                conn,
                params={"start_time": ref_start, "end_time": ref_end},
            )
        else:
            # Cold-start fallback: query historical offline feature table for canonical baseline
            logger.info(
                "Reference window has %d predictions (< %d minimum). Falling back to canonical baseline (Jan 2023 / Jan 2024).",
                ref_pred_count,
                min_reference_samples,
            )
            s_2023 = (
                "2023-01-01 00:00:00" if is_sqlite else FALLBACK_BASELINE_START_2023
            )
            e_2023 = "2023-01-29 00:00:00" if is_sqlite else FALLBACK_BASELINE_END_2023
            s_2024 = (
                "2024-01-01 00:00:00" if is_sqlite else FALLBACK_BASELINE_START_2024
            )
            e_2024 = "2024-01-29 00:00:00" if is_sqlite else FALLBACK_BASELINE_END_2024

            fallback_query = text(f"""
                SELECT
                    zone_id,
                    pickup_datetime,
                    pickup_count_last_15m,
                    pickup_count_last_1h,
                    pickup_count_last_24h,
                    pickup_count_same_hour_last_week,
                    hour_of_day,
                    day_of_week,
                    is_weekend,
                    is_holiday,
                    avg_temp_last_1h,
                    is_precipitating,
                    CAST(pickup_count_same_hour_last_week AS FLOAT) AS predicted_value,
                    CAST(pickup_count_last_1h AS FLOAT) AS actual_value
                FROM {schema_prefix}zone_demand_features_hourly
                WHERE (
                    (pickup_datetime >= :start_2023 AND pickup_datetime < :end_2023)
                    OR (pickup_datetime >= :start_2024 AND pickup_datetime < :end_2024)
                )
                ORDER BY pickup_datetime ASC
            """)
            try:
                ref_df = pd.read_sql(
                    fallback_query,
                    conn,
                    params={
                        "start_2023": s_2023,
                        "end_2023": e_2023,
                        "start_2024": s_2024,
                        "end_2024": e_2024,
                    },
                )
            except Exception:
                ref_df = pd.DataFrame()

            if ref_df.empty:
                # Secondary fallback: if canonical feature window is not populated in this test/dev environment,
                # check any available offline features or generate deterministic synthetic distribution for cold start
                logger.info(
                    "Canonical offline feature table empty. Checking other offline features or building baseline."
                )
                alt_fallback_query = text(f"""
                    SELECT
                        zone_id,
                        pickup_datetime,
                        pickup_count_last_15m,
                        pickup_count_last_1h,
                        pickup_count_last_24h,
                        pickup_count_same_hour_last_week,
                        hour_of_day,
                        day_of_week,
                        is_weekend,
                        is_holiday,
                        avg_temp_last_1h,
                        is_precipitating,
                        CAST(pickup_count_same_hour_last_week AS FLOAT) AS predicted_value,
                        CAST(pickup_count_last_1h AS FLOAT) AS actual_value
                    FROM {schema_prefix}zone_demand_features_hourly
                    ORDER BY pickup_datetime ASC
                    LIMIT 2000
                """)
                try:
                    ref_df = pd.read_sql(alt_fallback_query, conn)
                except Exception:
                    ref_df = pd.DataFrame()

                if ref_df.empty:
                    logger.warning(
                        "No offline features in zone_demand_features_hourly. Creating synthetic cold-start reference baseline."
                    )
                    np.random.seed(42)
                    n_samples = 500
                    hours = np.random.randint(0, 24, n_samples)
                    dows = np.random.randint(0, 7, n_samples)
                    pickups = np.random.poisson(lam=18.0, size=n_samples).astype(float)
                    ref_df = pd.DataFrame(
                        {
                            "pickup_count_last_15m": pickups * 0.25,
                            "pickup_count_last_1h": pickups,
                            "pickup_count_last_24h": pickups * 20.0,
                            "pickup_count_same_hour_last_week": pickups
                            + np.random.normal(0, 2, n_samples),
                            "hour_of_day": hours,
                            "day_of_week": dows,
                            "is_weekend": np.isin(dows, [0, 6]),
                            "is_holiday": False,
                            "avg_temp_last_1h": np.random.normal(15.0, 3.0, n_samples),
                            "is_precipitating": False,
                            "predicted_value": pickups
                            + np.random.normal(0, 1.5, n_samples),
                            "actual_value": pickups,
                        }
                    )

    return {
        "current_df": curr_df,
        "reference_df": ref_df,
        "is_cold_start_fallback": is_fallback,
        "current_count": len(curr_df),
        "reference_count": len(ref_df),
    }


def save_monitoring_report_db(
    summary: MonitoringReportSummary,
    engine: Optional[Engine] = None,
    html_dir: Optional[Path] = None,
) -> str:
    """Persist MonitoringReportSummary to PostgreSQL warehouse.monitoring_reports and disk.

    Args:
        summary: Validated MonitoringReportSummary instance.
        engine: SQLAlchemy Engine instance.
        html_dir: Directory to store interactive HTML artifacts.

    Returns:
        String report_id.
    """
    eng = engine or get_engine()
    is_sqlite = eng.dialect.name == "sqlite"
    schema_prefix = "" if is_sqlite else "warehouse."

    target_dir = html_dir or REPORTS_ARTIFACT_DIR
    target_dir.mkdir(parents=True, exist_ok=True)

    file_path = None
    if summary.html_content:
        file_name = f"{summary.report_type.value}_{summary.report_id}.html"
        out_file = target_dir / file_name
        out_file.write_text(summary.html_content, encoding="utf-8")
        file_path = str(out_file)
        summary.file_path = file_path

    record = summary.to_db_record()
    summary_str = (
        json.dumps(record["summary_json"])
        if isinstance(record["summary_json"], dict)
        else record["summary_json"]
    )

    stmt = text(f"""
        INSERT INTO {schema_prefix}monitoring_reports (
            report_id, report_type, generated_at, summary_json, file_path
        ) VALUES (
            :report_id, :report_type, :generated_at, :summary_json, :file_path
        )
        ON CONFLICT (report_id) DO UPDATE SET
            summary_json = EXCLUDED.summary_json,
            file_path = EXCLUDED.file_path
    """)

    with eng.begin() as conn:
        conn.execute(
            stmt,
            {
                "report_id": record["report_id"],
                "report_type": record["report_type"],
                "generated_at": record["generated_at"],
                "summary_json": summary_str,
                "file_path": file_path,
            },
        )

    logger.info(
        "Persisted %s report '%s' to %smonitoring_reports (HTML: '%s')",
        record["report_type"],
        record["report_id"],
        schema_prefix,
        file_path,
    )
    return record["report_id"]


def record_pipeline_run(
    engine: Engine,
    run_id: str,
    job_name: str,
    status: str,
    started_at: datetime,
    finished_at: Optional[datetime] = None,
    duration_seconds: Optional[float] = None,
    records_processed: int = 0,
    error_message: Optional[str] = None,
    triggered_by: Optional[str] = None,
) -> None:
    """Insert or update execution record in warehouse.pipeline_runs.

    Args:
        engine: SQLAlchemy Engine instance.
        run_id: Unique string run identifier.
        job_name: Name of pipeline/flow.
        status: Execution status ('running', 'completed', 'completed_with_alerts', 'failed').
        started_at: Flow initiation timestamp.
        finished_at: Flow completion timestamp.
        duration_seconds: Elapsed duration in seconds.
        records_processed: Number of records processed or evaluated.
        error_message: Error traceback or explanation if failed.
        triggered_by: Initiation source ('cron', 'manual', 'evidently_drift_alert').
    """
    is_sqlite = engine.dialect.name == "sqlite"
    schema_prefix = "" if is_sqlite else "warehouse."

    stmt = text(f"""
        INSERT INTO {schema_prefix}pipeline_runs (
            run_id, job_name, status, started_at, finished_at,
            duration_seconds, records_processed, error_message, triggered_by
        ) VALUES (
            :run_id, :job_name, :status, :started_at, :finished_at,
            :duration_seconds, :records_processed, :error_message, :triggered_by
        )
        ON CONFLICT (run_id) DO UPDATE SET
            status = EXCLUDED.status,
            finished_at = EXCLUDED.finished_at,
            duration_seconds = EXCLUDED.duration_seconds,
            records_processed = EXCLUDED.records_processed,
            error_message = EXCLUDED.error_message
    """)

    with engine.begin() as conn:
        conn.execute(
            stmt,
            {
                "run_id": run_id,
                "job_name": job_name,
                "status": status,
                "started_at": started_at,
                "finished_at": finished_at,
                "duration_seconds": duration_seconds,
                "records_processed": records_processed,
                "error_message": error_message,
                "triggered_by": triggered_by,
            },
        )
