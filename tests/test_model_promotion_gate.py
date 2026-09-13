"""Unit tests for ModelPromotionGate and champion/challenger evaluation (M6-1)."""

from unittest.mock import MagicMock

import pytest

from src.training.promotion import (
    DEFAULT_MIN_IMPROVEMENT_PCT,
    ModelPromotionGate,
    promote_model_to_production,
)


@pytest.fixture
def mock_mlflow_client():
    """Create a mock MLflow client for testing."""
    client = MagicMock()
    # Mock search_model_versions and create_model_version
    version_mock = MagicMock()
    version_mock.version = "2"
    version_mock.run_id = "candidate_run_123"
    version_mock.current_stage = "None"
    client.search_model_versions.return_value = []
    client.create_model_version.return_value = version_mock
    client.create_registered_model.return_value = None
    return client


def test_cold_start_promotion_beats_baseline(mock_mlflow_client):
    """When no Production model exists, candidate beating baseline is promoted."""
    mock_mlflow_client.get_latest_versions.return_value = []
    gate = ModelPromotionGate(client=mock_mlflow_client, min_improvement_pct=0.02)

    outcome = gate.evaluate_and_promote(
        model_name="demand_lightgbm_model",
        candidate_run_id="candidate_run_123",
        candidate_mae=3.85,
        baseline_mae=4.13,
    )

    assert outcome["promoted"] is True
    assert outcome["stage"] == "Production"
    assert outcome["version"] == "2"
    assert "Inaugural promotion (cold start)" in outcome["reason"]
    mock_mlflow_client.transition_model_version_stage.assert_called_once_with(
        name="demand_lightgbm_model",
        version="2",
        stage="Production",
        archive_existing_versions=True,
    )


def test_cold_start_rejection_fails_baseline(mock_mlflow_client):
    """When candidate fails naive baseline, it is rejected even in cold start."""
    mock_mlflow_client.get_latest_versions.return_value = []
    gate = ModelPromotionGate(client=mock_mlflow_client, min_improvement_pct=0.02)

    outcome = gate.evaluate_and_promote(
        model_name="demand_lightgbm_model",
        candidate_run_id="candidate_run_123",
        candidate_mae=4.50,
        baseline_mae=4.13,
    )

    assert outcome["promoted"] is False
    assert outcome["stage"] == "Staging"
    assert "failed naive baseline" in outcome["reason"]
    mock_mlflow_client.transition_model_version_stage.assert_called_once_with(
        name="demand_lightgbm_model",
        version="2",
        stage="Staging",
        archive_existing_versions=False,
    )


def test_rejection_candidate_worse_than_production(mock_mlflow_client):
    """Candidate with higher MAE than current Production champion is rejected."""
    # Mock existing Production version 1 with MAE = 4.00
    prod_version = MagicMock()
    prod_version.version = "1"
    prod_version.run_id = "prod_run_001"
    prod_version.current_stage = "Production"
    mock_mlflow_client.get_latest_versions.return_value = [prod_version]

    prod_run = MagicMock()
    prod_run.data.metrics = {"val_mae": 4.00, "val_rmse": 6.00}
    mock_mlflow_client.get_run.return_value = prod_run

    gate = ModelPromotionGate(client=mock_mlflow_client, min_improvement_pct=0.02)

    # Candidate MAE = 4.05 (worse than Production 4.00, though beats baseline 4.50)
    outcome = gate.evaluate_and_promote(
        model_name="demand_lightgbm_model",
        candidate_run_id="candidate_run_123",
        candidate_mae=4.05,
        baseline_mae=4.50,
    )

    assert outcome["promoted"] is False
    assert outcome["stage"] == "Staging"
    assert outcome["production_mae"] == 4.00
    assert outcome["production_version"] == "1"
    assert "was worse than Production" in outcome["reason"]
    mock_mlflow_client.transition_model_version_stage.assert_called_once_with(
        name="demand_lightgbm_model",
        version="2",
        stage="Staging",
        archive_existing_versions=False,
    )


def test_rejection_candidate_fails_two_percent_hurdle(mock_mlflow_client):
    """Candidate with marginal improvement (< 2.0%) is rejected to prevent seed-jitter churn."""
    # Production MAE = 4.00 -> 2% hurdle requires MAE <= 4.00 * 0.98 = 3.92
    prod_version = MagicMock()
    prod_version.version = "1"
    prod_version.run_id = "prod_run_001"
    mock_mlflow_client.get_latest_versions.return_value = [prod_version]

    prod_run = MagicMock()
    prod_run.data.metrics = {"val_mae": 4.00}
    mock_mlflow_client.get_run.return_value = prod_run

    gate = ModelPromotionGate(client=mock_mlflow_client, min_improvement_pct=0.02)

    # Candidate MAE = 3.96 (1.0% improvement, beats 4.00 but fails 3.92 hurdle)
    outcome = gate.evaluate_and_promote(
        model_name="demand_lightgbm_model",
        candidate_run_id="candidate_run_123",
        candidate_mae=3.96,
        baseline_mae=4.50,
    )

    assert outcome["promoted"] is False
    assert outcome["stage"] == "Staging"
    assert outcome["hurdle_mae"] == 3.92
    assert "failing the 2.0% hurdle rate" in outcome["reason"]
    mock_mlflow_client.transition_model_version_stage.assert_called_once_with(
        name="demand_lightgbm_model",
        version="2",
        stage="Staging",
        archive_existing_versions=False,
    )


def test_approval_candidate_beats_two_percent_hurdle(mock_mlflow_client):
    """Candidate that beats Production by >= 2.0% is promoted to Production."""
    # Production MAE = 4.00 -> 2% hurdle requires MAE <= 3.92
    prod_version = MagicMock()
    prod_version.version = "1"
    prod_version.run_id = "prod_run_001"
    mock_mlflow_client.get_latest_versions.return_value = [prod_version]

    prod_run = MagicMock()
    prod_run.data.metrics = {"val_mae": 4.00}
    mock_mlflow_client.get_run.return_value = prod_run

    gate = ModelPromotionGate(client=mock_mlflow_client, min_improvement_pct=0.02)

    # Candidate MAE = 3.80 (5.0% improvement, beats 3.92 hurdle)
    outcome = gate.evaluate_and_promote(
        model_name="demand_lightgbm_model",
        candidate_run_id="candidate_run_123",
        candidate_mae=3.80,
        baseline_mae=4.50,
    )

    assert outcome["promoted"] is True
    assert outcome["stage"] == "Production"
    assert outcome["hurdle_mae"] == 3.92
    assert outcome["actual_improvement_pct"] == 0.05
    assert "beat Production v1" in outcome["reason"]
    mock_mlflow_client.transition_model_version_stage.assert_called_once_with(
        name="demand_lightgbm_model",
        version="2",
        stage="Production",
        archive_existing_versions=True,
    )


def test_exact_hurdle_boundary_promoted(mock_mlflow_client):
    """Candidate exactly meeting the 2.0% hurdle is promoted."""
    prod_version = MagicMock()
    prod_version.version = "1"
    prod_version.run_id = "prod_run_001"
    mock_mlflow_client.get_latest_versions.return_value = [prod_version]

    prod_run = MagicMock()
    prod_run.data.metrics = {"val_mae": 4.00}
    mock_mlflow_client.get_run.return_value = prod_run

    gate = ModelPromotionGate(client=mock_mlflow_client, min_improvement_pct=0.02)

    # Exactly 4.00 * 0.98 = 3.92
    outcome = gate.evaluate_and_promote(
        model_name="demand_lightgbm_model",
        candidate_run_id="candidate_run_123",
        candidate_mae=3.92,
        baseline_mae=4.50,
    )

    assert outcome["promoted"] is True
    assert outcome["stage"] == "Production"


def test_custom_hurdle_rate_override(mock_mlflow_client):
    """Verify custom hurdle rate override at evaluation time."""
    prod_version = MagicMock()
    prod_version.version = "1"
    prod_version.run_id = "prod_run_001"
    mock_mlflow_client.get_latest_versions.return_value = [prod_version]

    prod_run = MagicMock()
    prod_run.data.metrics = {"val_mae": 4.00}
    mock_mlflow_client.get_run.return_value = prod_run

    gate = ModelPromotionGate(client=mock_mlflow_client, min_improvement_pct=0.02)

    # If user requests a strict 5% hurdle (MAE <= 3.80), 3.85 fails
    outcome = gate.evaluate_and_promote(
        model_name="demand_lightgbm_model",
        candidate_run_id="candidate_run_123",
        candidate_mae=3.85,
        baseline_mae=4.50,
        min_improvement_pct=0.05,
    )
    assert outcome["promoted"] is False
    assert outcome["stage"] == "Staging"
    assert outcome["min_improvement_pct"] == 0.05


def test_backward_compatible_helper(mock_mlflow_client):
    """Verify promote_model_to_production helper behaves identically."""
    mock_mlflow_client.get_latest_versions.return_value = []

    outcome = promote_model_to_production(
        client=mock_mlflow_client,
        model_name="corridor_duration_lightgbm_model",
        candidate_run_id="candidate_run_123",
        candidate_mae=0.25,
        baseline_mae=0.35,
    )

    assert outcome["promoted"] is True
    assert outcome["stage"] == "Production"
    assert outcome["min_improvement_pct"] == DEFAULT_MIN_IMPROVEMENT_PCT
