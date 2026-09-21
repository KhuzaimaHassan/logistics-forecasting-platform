"""Unit and integration tests for the daily model monitoring Prefect flow and trigger hook (M8-2).

Tests:
1. Universal cooldown check against warehouse.pipeline_runs for ANY trigger source (cron, manual, evidently_drift_alert).
2. Cooldown distinguishes completed vs failed runs and honors cutoff timestamp.
3. Dataset extraction with 14-day rolling window and automatic cold-start fallback (<500 samples).
4. Production baseline MAE retrieval from MLflow.
5. Report persistence to warehouse.monitoring_reports and disk HTML files.
6. Alert-first gate: AUTO_RETRAIN_ON_DRIFT=False logs alert, suppresses retrain.
7. Guarded-trigger gate: AUTO_RETRAIN_ON_DRIFT=True triggers scheduled_retraining_flow when cooldown is clear.
8. Retraining suppressed when in cooldown.
9. End-to-end execution of daily_model_monitoring_flow.
"""

import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from src.monitoring.schemas import (
    MonitoringReportSummary,
    ReportType,
)
from src.monitoring.service import (
    fetch_mlflow_production_baseline,
    fetch_monitoring_datasets,
    is_retraining_in_cooldown,
    save_monitoring_report_db,
)
from src.orchestration.deploy import deploy_daily_monitoring_flow
from src.orchestration.flows.monitoring_flow import (
    evaluate_retraining_trigger_task,
    run_monitoring_lifecycle,
)


@pytest.fixture
def sqlite_engine():
    """In-memory SQLite engine simulating warehouse schema for fast unit testing."""
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE pipeline_runs (
                run_id VARCHAR(100) PRIMARY KEY,
                job_name VARCHAR(100) NOT NULL,
                status VARCHAR(50) NOT NULL,
                started_at TIMESTAMP NOT NULL,
                finished_at TIMESTAMP,
                duration_seconds NUMERIC(8, 2),
                records_processed INTEGER DEFAULT 0,
                error_message TEXT,
                triggered_by VARCHAR(50)
            )
        """))
        conn.execute(text("""
            CREATE TABLE monitoring_reports (
                report_id VARCHAR(100) PRIMARY KEY,
                report_type VARCHAR(50) NOT NULL,
                generated_at TIMESTAMP NOT NULL,
                summary_json TEXT,
                file_path VARCHAR(255)
            )
        """))
        conn.execute(text("""
            CREATE TABLE predictions (
                prediction_id VARCHAR(100) PRIMARY KEY,
                entity_type VARCHAR(50) NOT NULL,
                entity_id VARCHAR(100) NOT NULL,
                model_version VARCHAR(100) NOT NULL,
                predicted_value NUMERIC(10, 2) NOT NULL,
                predicted_at TIMESTAMP NOT NULL,
                actual_value NUMERIC(10, 2),
                actual_recorded_at TIMESTAMP
            )
        """))
        conn.execute(text("""
            CREATE TABLE zone_demand_features_hourly (
                zone_id INTEGER,
                pickup_datetime TIMESTAMP,
                pickup_count_last_15m BIGINT DEFAULT 0,
                pickup_count_last_1h BIGINT DEFAULT 0,
                pickup_count_last_24h BIGINT DEFAULT 0,
                pickup_count_same_hour_last_week BIGINT DEFAULT 0,
                hour_of_day INTEGER,
                day_of_week INTEGER,
                is_weekend BOOLEAN,
                is_holiday BOOLEAN,
                avg_temp_last_1h FLOAT,
                is_precipitating BOOLEAN,
                PRIMARY KEY (zone_id, pickup_datetime)
            )
        """))
    return engine


@pytest.fixture
def sample_monitoring_dfs():
    """Create sample feature DataFrames for testing analyzers."""
    np.random.seed(42)
    n = 100
    ref_df = pd.DataFrame(
        {
            "pickup_count_last_15m": np.random.poisson(5, n).astype(float),
            "pickup_count_last_1h": np.random.poisson(20, n).astype(float),
            "pickup_count_last_24h": np.random.poisson(400, n).astype(float),
            "pickup_count_same_hour_last_week": np.random.poisson(20, n).astype(float),
            "hour_of_day": np.random.randint(0, 24, n),
            "day_of_week": np.random.randint(0, 7, n),
            "is_weekend": [False] * n,
            "is_holiday": [False] * n,
            "avg_temp_last_1h": np.random.normal(15.0, 3.0, n),
            "is_precipitating": [False] * n,
            "predicted_value": np.random.poisson(20, n).astype(float),
            "actual_value": np.random.poisson(20, n).astype(float),
        }
    )
    curr_df = ref_df.copy()
    return curr_df, ref_df


def test_cooldown_queries_any_trigger_source(sqlite_engine):
    """ADR-026 Universal Cooldown: Verify cooldown applies to ANY completed retraining run."""
    now = datetime(2026, 9, 21, 12, 0, 0, tzinfo=timezone.utc)

    # 1. No runs yet -> Not in cooldown
    assert not is_retraining_in_cooldown(sqlite_engine, cooldown_hours=48, now=now)

    # 2. Add completed scheduled run 10 hours ago -> Cooldown ACTIVE
    ten_hours_ago = now - timedelta(hours=10)
    with sqlite_engine.begin() as conn:
        conn.execute(
            text("""
            INSERT INTO pipeline_runs (run_id, job_name, status, started_at, finished_at, triggered_by)
            VALUES ('run-1', 'scheduled-model-retraining-flow', 'completed', :st, :ft, 'cron')
        """),
            {"st": ten_hours_ago, "ft": ten_hours_ago + timedelta(minutes=15)},
        )

    assert is_retraining_in_cooldown(sqlite_engine, cooldown_hours=48, now=now)

    # 3. Add drift-triggered run 5 hours ago -> Cooldown still ACTIVE regardless of triggered_by
    five_hours_ago = now - timedelta(hours=5)
    with sqlite_engine.begin() as conn:
        conn.execute(
            text("""
            INSERT INTO pipeline_runs (run_id, job_name, status, started_at, finished_at, triggered_by)
            VALUES ('run-2', 'scheduled-model-retraining-flow', 'completed', :st, :ft, 'evidently_drift_alert')
        """),
            {"st": five_hours_ago, "ft": five_hours_ago + timedelta(minutes=15)},
        )

    assert is_retraining_in_cooldown(sqlite_engine, cooldown_hours=48, now=now)

    # 4. Check future time past 48 hours -> Cooldown CLEAR
    future_time = now + timedelta(hours=50)
    assert not is_retraining_in_cooldown(
        sqlite_engine, cooldown_hours=48, now=future_time
    )


def test_cooldown_ignores_failed_runs(sqlite_engine):
    """Verify that a failed retraining run within 48h does NOT suppress retries."""
    now = datetime(2026, 9, 21, 12, 0, 0, tzinfo=timezone.utc)
    two_hours_ago = now - timedelta(hours=2)

    with sqlite_engine.begin() as conn:
        conn.execute(
            text("""
            INSERT INTO pipeline_runs (run_id, job_name, status, started_at, finished_at, triggered_by)
            VALUES ('run-fail', 'scheduled-model-retraining-flow', 'failed', :st, :ft, 'cron')
        """),
            {"st": two_hours_ago, "ft": two_hours_ago + timedelta(minutes=5)},
        )

    assert not is_retraining_in_cooldown(sqlite_engine, cooldown_hours=48, now=now)


def test_fetch_monitoring_datasets_cold_start_fallback(sqlite_engine):
    """ADR-026: Verify fallback to January 2024 features when reference predictions < 500."""
    now = datetime(2026, 9, 21, 12, 0, 0, tzinfo=timezone.utc)

    # Case A: 0 predictions in database -> fallback flagged
    datasets = fetch_monitoring_datasets(
        sqlite_engine, now=now, min_reference_samples=500
    )
    assert datasets["is_cold_start_fallback"] is True
    assert datasets["current_count"] == 0
    assert datasets["reference_count"] > 0  # Fallback generated synthetic baseline
    assert "pickup_count_last_1h" in datasets["reference_df"].columns

    # Case B: Insert 550 reference predictions -> rolling historical window used
    ref_time = now - timedelta(days=5)
    with sqlite_engine.begin() as conn:
        for i in range(550):
            conn.execute(
                text("""
                INSERT INTO predictions (prediction_id, entity_type, entity_id, model_version, predicted_value, predicted_at)
                VALUES (:pid, 'demand', '161', 'v1', 25.0, :p_at)
            """),
                {"pid": f"p-{i}", "p_at": ref_time},
            )

    datasets_loaded = fetch_monitoring_datasets(
        sqlite_engine, now=now, min_reference_samples=500
    )
    assert datasets_loaded["is_cold_start_fallback"] is False
    assert datasets_loaded["reference_count"] == 550


def test_fetch_mlflow_production_baseline_success():
    """Verify production baseline MAE is retrieved directly from MLflow model metadata."""
    with patch("src.monitoring.service.ModelPromotionGate") as mock_gate_cls:
        mock_gate = MagicMock()
        mock_gate.get_current_production_model.return_value = {
            "version": "3",
            "run_id": "mlflow-run-abc",
            "metrics": {"val_mae": 1.745, "val_rmse": 2.41},
        }
        mock_gate_cls.return_value = mock_gate

        baseline_mae = fetch_mlflow_production_baseline("demand_lightgbm_model")
        assert baseline_mae == 1.745
        mock_gate.get_current_production_model.assert_called_once_with(
            "demand_lightgbm_model"
        )


def test_fetch_mlflow_production_baseline_missing():
    """Verify fetch_mlflow_production_baseline returns None gracefully if no production model."""
    with patch("src.monitoring.service.ModelPromotionGate") as mock_gate_cls:
        mock_gate = MagicMock()
        mock_gate.get_current_production_model.return_value = None
        mock_gate_cls.return_value = mock_gate

        baseline_mae = fetch_mlflow_production_baseline("demand_lightgbm_model")
        assert baseline_mae is None


def test_save_monitoring_report_db_and_html(sqlite_engine, tmp_path):
    """Verify persisting report to SQLite and writing HTML artifact."""
    summary = MonitoringReportSummary(
        report_id=str(uuid.uuid4()),
        report_type=ReportType.DATA_DRIFT,
        drift_detected=False,
        retrain_recommended=False,
        alert_severity="INFO",
        html_content="<html><body>Test Report</body></html>",
    )

    report_id = save_monitoring_report_db(
        summary, engine=sqlite_engine, html_dir=tmp_path
    )
    assert report_id == summary.report_id

    # Verify DB record
    with sqlite_engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT report_id, report_type, file_path FROM monitoring_reports WHERE report_id = :id"
            ),
            {"id": report_id},
        ).fetchone()
        assert row is not None
        assert row[0] == report_id
        assert row[1] == "data_drift"
        assert Path(row[2]).exists()
        assert Path(row[2]).read_text() == "<html><body>Test Report</body></html>"


def test_evaluate_retraining_trigger_no_drift():
    """Verify evaluate_retraining_trigger returns action='none' when no drift flagged."""
    summaries = {
        "data_drift": MonitoringReportSummary(
            report_type=ReportType.DATA_DRIFT, retrain_recommended=False
        ),
        "prediction_drift": MonitoringReportSummary(
            report_type=ReportType.PREDICTION_DRIFT, retrain_recommended=False
        ),
        "performance_decay": MonitoringReportSummary(
            report_type=ReportType.PERFORMANCE_DECAY, retrain_recommended=False
        ),
    }
    decision = evaluate_retraining_trigger_task.fn(summaries=summaries)
    assert decision["retrain_recommended"] is False
    assert decision["action"] == "none"
    assert decision["retrain_triggered"] is False


def test_evaluate_retraining_trigger_alert_first_gate(sqlite_engine):
    """ADR-026 Alert-First: When drift is detected and AUTO_RETRAIN_ON_DRIFT=False, only log alert."""
    summaries = {
        "data_drift": MonitoringReportSummary(
            report_type=ReportType.DATA_DRIFT,
            retrain_recommended=True,
            alert_reasons=["Dataset drift share 50% exceeded"],
        ),
    }

    with patch("src.monitoring.service.is_retraining_in_cooldown") as mock_cooldown:
        decision = evaluate_retraining_trigger_task.fn(
            summaries=summaries,
            auto_retrain=False,  # default
            engine=sqlite_engine,
        )
        assert decision["retrain_recommended"] is True
        assert decision["action"] == "alert_logged"
        assert decision["retrain_triggered"] is False
        mock_cooldown.assert_not_called()


def test_evaluate_retraining_trigger_auto_retrain_suppressed_by_cooldown(sqlite_engine):
    """When AUTO_RETRAIN_ON_DRIFT=True but cooldown is active, suppress retraining."""
    summaries = {
        "performance_decay": MonitoringReportSummary(
            report_type=ReportType.PERFORMANCE_DECAY,
            retrain_recommended=True,
            alert_reasons=["MAE degraded 25%"],
        ),
    }

    with patch(
        "src.orchestration.flows.monitoring_flow.is_retraining_in_cooldown",
        return_value=True,
    ):
        decision = evaluate_retraining_trigger_task.fn(
            summaries=summaries,
            auto_retrain=True,
            cooldown_hours=48,
            engine=sqlite_engine,
        )
        assert decision["retrain_recommended"] is True
        assert decision["action"] == "cooldown_suppressed"
        assert decision["retrain_triggered"] is False


def test_evaluate_retraining_trigger_auto_retrain_fires(sqlite_engine):
    """When AUTO_RETRAIN_ON_DRIFT=True and cooldown is clear, trigger retraining flow."""
    summaries = {
        "data_drift": MonitoringReportSummary(
            report_type=ReportType.DATA_DRIFT,
            retrain_recommended=True,
            alert_reasons=["Critical feature shifted"],
        ),
    }

    with (
        patch(
            "src.orchestration.flows.monitoring_flow.is_retraining_in_cooldown",
            return_value=False,
        ),
        patch(
            "src.orchestration.flows.retraining_flow.scheduled_retraining_flow",
            return_value={"status": "completed", "champion_promoted": True},
        ) as mock_retrain,
    ):
        decision = evaluate_retraining_trigger_task.fn(
            summaries=summaries,
            auto_retrain=True,
            cooldown_hours=48,
            engine=sqlite_engine,
        )
        assert decision["retrain_recommended"] is True
        assert decision["action"] == "retrain_triggered"
        assert decision["retrain_triggered"] is True
        mock_retrain.assert_called_once_with(triggered_by="evidently_drift_alert")


def test_run_monitoring_lifecycle_nominal_execution(
    sqlite_engine, sample_monitoring_dfs, tmp_path
):
    """Verify full monitoring lifecycle execution with overrides."""
    curr_df, ref_df = sample_monitoring_dfs

    res = run_monitoring_lifecycle(
        engine=sqlite_engine,
        current_df_override=curr_df,
        reference_df_override=ref_df,
        baseline_mae_override=1.80,
        auto_retrain=False,
        html_dir=tmp_path,
    )

    assert res["status"] in ("completed", "completed_with_alerts")
    assert "data_drift" in res["report_ids"]
    assert "prediction_drift" in res["report_ids"]
    assert "performance_decay" in res["report_ids"]

    # Verify pipeline_runs table recorded the flow
    with sqlite_engine.connect() as conn:
        run_row = conn.execute(
            text(
                "SELECT run_id, job_name, status FROM pipeline_runs WHERE run_id = :id"
            ),
            {"id": res["run_id"]},
        ).fetchone()
        assert run_row is not None
        assert run_row[1] == "daily-model-monitoring-flow"
        assert run_row[2] == res["status"]


def test_deploy_daily_monitoring_flow_registration():
    """Verify deployment registration helper applies without exception."""
    with patch(
        "src.orchestration.flows.monitoring_flow.daily_model_monitoring_flow.to_deployment"
    ) as mock_to_dep:
        mock_dep = MagicMock()
        mock_dep.apply.return_value = "dep-monitor-123"
        mock_to_dep.return_value = mock_dep

        deploy_daily_monitoring_flow(
            work_pool_name="test-pool", cron_schedule="0 2 * * *"
        )
        mock_to_dep.assert_called_once()
        mock_dep.apply.assert_called_once()
