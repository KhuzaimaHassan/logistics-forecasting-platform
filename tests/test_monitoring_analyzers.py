"""Unit tests for Evidently 0.7 Core drift and performance analyzers per ADR-026."""

import ast
import inspect
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.monitoring.analyzers import (
    DataDriftAnalyzer,
    PerformanceDecayAnalyzer,
    PredictionDriftAnalyzer,
    save_report_html,
    save_report_json,
)
from src.monitoring.schemas import (
    DriftMetricSummary,
    MonitoringReportSummary,
    ReportType,
)


@pytest.fixture
def synthetic_data():
    """Generate reproducible synthetic datasets for drift and decay testing."""
    np.random.seed(42)
    n = 150

    reference_df = pd.DataFrame(
        {
            "pickup_count_last_1h": np.random.poisson(15, n).astype(float),
            "avg_trip_duration_last_1h": np.random.normal(12.0, 2.0, n),
            "avg_temp_last_1h": np.random.normal(18.0, 4.0, n),
            "precipitation_last_1h": np.random.exponential(0.5, n),
            "predicted_value": np.random.normal(15.0, 3.0, n),
            "actual_value": np.random.normal(15.0, 3.0, n),
        }
    )

    # Identical distribution
    current_identical_df = pd.DataFrame(
        {
            "pickup_count_last_1h": np.random.poisson(15, n).astype(float),
            "avg_trip_duration_last_1h": np.random.normal(12.0, 2.0, n),
            "avg_temp_last_1h": np.random.normal(18.0, 4.0, n),
            "precipitation_last_1h": np.random.exponential(0.5, n),
            "predicted_value": np.random.normal(15.0, 3.0, n),
            "actual_value": np.random.normal(15.0, 3.0, n),
        }
    )

    # Intentionally shifted dataset
    current_drifted_df = pd.DataFrame(
        {
            # Critical feature severely drifted
            "pickup_count_last_1h": np.random.poisson(35, n).astype(float),
            # Non-critical feature drifted
            "avg_temp_last_1h": np.random.normal(35.0, 4.0, n),
            # Undrifted features
            "avg_trip_duration_last_1h": np.random.normal(12.0, 2.0, n),
            "precipitation_last_1h": np.random.exponential(0.5, n),
            # Shifted prediction
            "predicted_value": np.random.normal(35.0, 5.0, n),
            "actual_value": np.random.normal(15.0, 3.0, n),
        }
    )

    return reference_df, current_identical_df, current_drifted_df


def test_data_drift_analyzer_identical(synthetic_data):
    """Verify that identical data distributions detect zero dataset drift."""
    ref_df, curr_df, _ = synthetic_data

    analyzer = DataDriftAnalyzer(drift_share_threshold=0.40)
    summary = analyzer.analyze(current_df=curr_df, reference_df=ref_df)

    assert isinstance(summary, MonitoringReportSummary)
    assert summary.report_type == ReportType.DATA_DRIFT
    assert summary.drift_detected is False
    assert summary.retrain_recommended is False
    assert summary.alert_severity == "INFO"
    assert summary.drift_share is not None
    assert summary.drift_share < 0.40
    assert summary.html_content is not None
    assert "<html" in summary.html_content.lower()


def test_data_drift_analyzer_shifted_dataset(synthetic_data):
    """Verify that shifted distributions flag drifted features and compute accurate drift share."""
    ref_df, _, drifted_df = synthetic_data

    analyzer = DataDriftAnalyzer(drift_share_threshold=0.40)
    summary = analyzer.analyze(current_df=drifted_df, reference_df=ref_df)

    assert isinstance(summary, MonitoringReportSummary)
    assert summary.report_type == ReportType.DATA_DRIFT

    # Check that pickup_count_last_1h and avg_temp_last_1h were flagged
    drifted_cols = [
        m.column_name
        for m in summary.metrics
        if isinstance(m, DriftMetricSummary) and m.drift_detected
    ]
    assert "pickup_count_last_1h" in drifted_cols
    assert "avg_temp_last_1h" in drifted_cols

    # Retrain recommended because critical feature pickup_count_last_1h drifted
    assert summary.retrain_recommended is True
    assert summary.alert_severity == "CRITICAL"
    assert any("pickup_count_last_1h" in r for r in summary.alert_reasons)


def test_data_drift_critical_feature_retrain_policy(synthetic_data):
    """Verify that critical feature drift triggers retrain recommendation even if overall drift share is low."""
    ref_df, curr_df, _ = synthetic_data

    # Drift ONLY the critical feature pickup_count_last_1h out of 5 columns (drift share = 0.20 < 0.40)
    single_drift_df = curr_df.copy()
    single_drift_df["pickup_count_last_1h"] = (
        single_drift_df["pickup_count_last_1h"] + 25.0
    )

    analyzer = DataDriftAnalyzer(
        drift_share_threshold=0.50,
        critical_features=["pickup_count_last_1h"],
        critical_p_threshold=0.01,
    )
    summary = analyzer.analyze(current_df=single_drift_df, reference_df=ref_df)

    assert summary.drift_share is not None
    assert summary.drift_share < 0.50
    assert summary.drift_detected is False  # Dataset drift share threshold not met
    # BUT retrain recommendation MUST be True due to critical demand feature drift
    assert summary.retrain_recommended is True
    assert summary.alert_severity == "CRITICAL"


def test_prediction_drift_analyzer_normal_and_shifted(synthetic_data):
    """Verify PredictionDriftAnalyzer evaluates predicted_value distribution shifts."""
    ref_df, curr_identical, curr_drifted = synthetic_data

    analyzer = PredictionDriftAnalyzer(prediction_col="predicted_value")

    # Identical predictions
    norm_summary = analyzer.analyze(current_df=curr_identical, reference_df=ref_df)
    assert norm_summary.report_type == ReportType.PREDICTION_DRIFT
    assert norm_summary.drift_detected is False
    assert norm_summary.alert_severity == "INFO"

    # Shifted predictions
    shift_summary = analyzer.analyze(current_df=curr_drifted, reference_df=ref_df)
    assert shift_summary.report_type == ReportType.PREDICTION_DRIFT
    assert shift_summary.drift_detected is True
    assert shift_summary.alert_severity == "WARNING"
    assert len(shift_summary.metrics) == 1
    assert shift_summary.metrics[0].column_name == "predicted_value"
    assert shift_summary.metrics[0].drift_detected is True


def test_performance_decay_analyzer_single_source_of_truth():
    """Verify PerformanceDecayAnalyzer computes MAE degradation against MLflow Production baseline."""
    np.random.seed(42)
    n = 100

    # Current inference data with actuals and predictions
    actuals = np.random.normal(20.0, 5.0, n)
    # Case A: Good predictions, MAE ~ 1.5
    good_preds = actuals + np.random.normal(0.0, 1.5, n)
    good_df = pd.DataFrame({"actual_value": actuals, "predicted_value": good_preds})

    # MLflow Production baseline MAE = 1.60
    mlflow_baseline_mae = 1.60
    mlflow_baseline_rmse = 2.00

    analyzer = PerformanceDecayAnalyzer(
        baseline_mae=mlflow_baseline_mae,
        baseline_rmse=mlflow_baseline_rmse,
        decay_threshold=0.15,
    )

    summary_good = analyzer.analyze(current_df=good_df)
    assert summary_good.report_type == ReportType.PERFORMANCE_DECAY
    assert summary_good.drift_detected is False
    assert summary_good.retrain_recommended is False
    assert summary_good.alert_severity == "INFO"

    # Case B: Severely degraded predictions, MAE ~ 4.5 (> 15% degradation over 1.60)
    bad_preds = actuals + np.random.normal(3.5, 2.5, n)
    bad_df = pd.DataFrame({"actual_value": actuals, "predicted_value": bad_preds})

    summary_bad = analyzer.analyze(current_df=bad_df)
    assert summary_bad.drift_detected is True
    assert summary_bad.retrain_recommended is True
    assert summary_bad.alert_severity == "CRITICAL"
    assert any("MAE degraded" in r for r in summary_bad.alert_reasons)

    # Verify metrics structure
    mae_metric = next(m for m in summary_bad.metrics if m.metric_name == "mae")
    assert mae_metric.current_value > mlflow_baseline_mae
    assert mae_metric.reference_value == mlflow_baseline_mae
    assert mae_metric.decay_ratio is not None and mae_metric.decay_ratio > 0.15
    assert mae_metric.decay_detected is True


def test_performance_decay_validation_errors():
    """Verify PerformanceDecayAnalyzer enforces input validation."""
    with pytest.raises(ValueError, match="baseline_mae must be positive"):
        PerformanceDecayAnalyzer(baseline_mae=0.0)

    analyzer = PerformanceDecayAnalyzer(baseline_mae=2.0)
    empty_df = pd.DataFrame({"actual_value": [], "predicted_value": []})
    with pytest.raises(ValueError, match="Insufficient ground truth actuals"):
        analyzer.analyze(empty_df)


def test_mandatory_explicit_keyword_argument_invariant():
    """ADR-026 Invariant Check: Verify that all Report.run calls in analyzers.py use keyword arguments."""
    from src.monitoring import analyzers

    source = inspect.getsource(analyzers)
    tree = ast.parse(source)

    run_calls = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Attribute) and func.attr == "run":
                run_calls.append(node)

    assert (
        len(run_calls) >= 3
    ), f"Expected at least 3 Report.run calls, found {len(run_calls)}"

    for call in run_calls:
        # Assert NO positional arguments are passed
        assert (
            len(call.args) == 0
        ), f"Report.run called with positional arguments at line {call.lineno}! ADR-026 strictly requires keyword arguments."
        # Assert keyword arguments include 'current_data'
        kw_names = [kw.arg for kw in call.keywords]
        assert (
            "current_data" in kw_names
        ), f"Report.run missing 'current_data' keyword argument at line {call.lineno}."


def test_save_report_artifacts(tmp_path, synthetic_data):
    """Verify HTML and JSON artifact serialization to disk."""
    ref_df, curr_df, _ = synthetic_data
    analyzer = DataDriftAnalyzer()
    summary = analyzer.analyze(current_df=curr_df, reference_df=ref_df)

    html_file = tmp_path / "reports" / "data_drift.html"
    json_file = tmp_path / "reports" / "data_drift.json"

    saved_html_path = save_report_html(summary, html_file)
    assert Path(saved_html_path).exists()
    assert summary.file_path == str(Path(saved_html_path).resolve())
    assert Path(saved_html_path).stat().st_size > 1000

    saved_json_path = save_report_json(summary, json_file)
    assert Path(saved_json_path).exists()
    loaded_json = json.loads(Path(saved_json_path).read_text(encoding="utf-8"))
    assert loaded_json["report_id"] == summary.report_id
    assert loaded_json["report_type"] == "data_drift"


def test_to_db_record(synthetic_data):
    """Verify MonitoringReportSummary converts cleanly to PostgreSQL warehouse.monitoring_reports schema."""
    ref_df, curr_df, _ = synthetic_data
    analyzer = DataDriftAnalyzer()
    summary = analyzer.analyze(current_df=curr_df, reference_df=ref_df)

    db_record = summary.to_db_record()
    assert "report_id" in db_record
    assert "report_type" in db_record
    assert "generated_at" in db_record
    assert "summary_json" in db_record
    assert "file_path" in db_record
    assert db_record["report_type"] == "data_drift"
    assert isinstance(db_record["summary_json"], dict)
