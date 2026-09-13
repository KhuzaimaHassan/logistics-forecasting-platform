"""Unit and contract tests for the scheduled model retraining Prefect flow (M6-2)."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.orchestration.deploy import deploy_scheduled_retraining_flow
from src.orchestration.flows.retraining_flow import (
    backup_retraining_artifacts_task,
    evaluate_and_promote_candidates_task,
    evaluate_retraining_baselines_task,
    generate_retraining_summary_task,
    reconcile_offline_features_task,
    run_scheduled_retraining,
    train_candidate_models_task,
)


@pytest.fixture
def sample_validation_dfs():
    """Create sample dummy validation DataFrames for demand and corridors."""
    demand_val = pd.DataFrame(
        {
            "zone_id": [161, 236],
            "target_pickup_count_next_1h": [10.0, 20.0],
            "pickup_count_same_hour_last_week": [9.0, 19.0],
        }
    )
    corridor_val = pd.DataFrame(
        {
            "origin_zone_id": [161],
            "dest_zone_id": [236],
            "target_trip_duration_seconds": [600],
            "avg_trip_duration_last_1h": [620.0],
            "trip_distance_km": [3.5],
        }
    )
    return demand_val, corridor_val


def test_reconcile_offline_features_task():
    """Verify reconcile_offline_features_task invokes extractor with correct parameters."""
    with patch(
        "src.orchestration.flows.retraining_flow.extract_and_load_offline_features"
    ) as mock_extract:
        mock_extract.return_value = (150, 75)
        res = reconcile_offline_features_task.fn(
            engine=MagicMock(),
            lookback_days=14,
        )
        assert res["zone_rows_loaded"] == 150
        assert res["corridor_rows_loaded"] == 75
        mock_extract.assert_called_once()


def test_evaluate_retraining_baselines_task(sample_validation_dfs):
    """Verify baseline evaluation computes naive metrics for demand and corridor."""
    demand_val, corridor_val = sample_validation_dfs
    with (
        patch(
            "src.orchestration.flows.retraining_flow.evaluate_demand_baseline"
        ) as mock_demand_b,
        patch(
            "src.orchestration.flows.retraining_flow.evaluate_corridor_duration_baseline"
        ) as mock_corridor_b,
    ):
        mock_demand_b.return_value = {
            "metrics": {"val_mae": 4.12, "val_rmse": 6.10},
            "run_id": "run_b_demand",
        }
        mock_corridor_b.return_value = {
            "metrics": {"val_mae": 0.28, "val_rmse": 0.35},
            "run_id": "run_b_corridor",
        }

        res = evaluate_retraining_baselines_task.fn(
            demand_val=demand_val,
            corridor_val=corridor_val,
            log_to_mlflow=False,
        )

        assert res["demand_baseline"]["metrics"]["val_mae"] == 4.12
        assert res["corridor_baseline"]["metrics"]["val_mae"] == 0.28


def test_train_candidate_models_task(sample_validation_dfs):
    """Verify train_candidate_models_task invokes LightGBM trainers."""
    demand_val, corridor_val = sample_validation_dfs
    with (
        patch(
            "src.orchestration.flows.retraining_flow.train_demand_lightgbm"
        ) as mock_train_d,
        patch(
            "src.orchestration.flows.retraining_flow.train_duration_lightgbm"
        ) as mock_train_c,
    ):
        mock_train_d.return_value = {
            "run_id": "run_cand_demand",
            "metrics": {"val_mae": 3.75, "val_rmse": 5.40},
        }
        mock_train_c.return_value = {
            "run_id": "run_cand_corridor",
            "metrics": {"val_mae": 0.22, "val_rmse": 0.29},
        }

        res = train_candidate_models_task.fn(
            demand_train=demand_val,
            demand_val=demand_val,
            corridor_train=corridor_val,
            corridor_val=corridor_val,
            demand_baseline_mae=4.12,
            corridor_baseline_mae=0.28,
            log_to_mlflow=False,
        )

        assert res["demand_model"]["metrics"]["val_mae"] == 3.75
        assert res["corridor_model"]["metrics"]["val_mae"] == 0.22


def test_evaluate_and_promote_candidates_task():
    """Verify evaluate_and_promote_candidates_task executes ModelPromotionGate with hurdle rate."""
    mock_client = MagicMock()

    with patch(
        "src.orchestration.flows.retraining_flow.ModelPromotionGate"
    ) as MockGate:
        gate_instance = MockGate.return_value
        gate_instance.evaluate_and_promote.side_effect = [
            # Demand promotion
            {
                "model_name": "demand_lightgbm_model",
                "promoted": True,
                "stage": "Production",
                "version": "3",
                "reason": "Beat Production by 5%",
            },
            # Corridor promotion (failed hurdle)
            {
                "model_name": "corridor_duration_lightgbm_model",
                "promoted": False,
                "stage": "Staging",
                "version": "2",
                "reason": "Failed 2% hurdle",
            },
        ]

        res = evaluate_and_promote_candidates_task.fn(
            client=mock_client,
            demand_candidate_run_id="run_d_123",
            demand_candidate_mae=3.80,
            demand_baseline_mae=4.20,
            corridor_candidate_run_id="run_c_123",
            corridor_candidate_mae=0.27,
            corridor_baseline_mae=0.30,
            min_improvement_pct=0.02,
        )

        assert res["demand_promotion"]["promoted"] is True
        assert res["demand_promotion"]["stage"] == "Production"
        assert res["corridor_promotion"]["promoted"] is False
        assert res["corridor_promotion"]["stage"] == "Staging"
        MockGate.assert_called_once_with(client=mock_client, min_improvement_pct=0.02)


def test_backup_retraining_artifacts_task():
    """Verify backup_retraining_artifacts_task obeys toggle flag."""
    # When disabled
    res_disabled = backup_retraining_artifacts_task.fn(backup_to_r2=False)
    assert res_disabled["status"] == "skipped"
    assert res_disabled["reason"] == "backup_disabled"

    # When enabled
    with patch(
        "src.orchestration.flows.retraining_flow.backup_artifacts_to_r2_task"
    ) as mock_r2:
        mock_r2.fn.return_value = {"status": "success", "uploaded_files": 12}
        res_enabled = backup_retraining_artifacts_task.fn(backup_to_r2=True)
        assert res_enabled["status"] == "success"
        assert res_enabled["uploaded_files"] == 12


def test_generate_retraining_summary_task():
    """Verify generate_retraining_summary_task structures execution details."""
    res = generate_retraining_summary_task.fn(
        demand_baseline={"metrics": {"val_mae": 4.10}},
        demand_model={"metrics": {"val_mae": 3.80}},
        demand_promo={
            "promoted": True,
            "stage": "Production",
            "production_mae": 4.00,
            "hurdle_mae": 3.92,
            "reason": "Beat production",
        },
        corridor_baseline={"metrics": {"val_mae": 0.35}},
        corridor_model={"metrics": {"val_mae": 0.28}},
        corridor_promo={
            "promoted": True,
            "stage": "Production",
            "production_mae": 0.32,
            "hurdle_mae": 0.3136,
            "reason": "Beat production",
        },
        r2_status={"status": "success"},
        elapsed_seconds=42.5,
    )

    assert res["status"] == "success"
    assert res["elapsed_seconds"] == 42.5
    assert res["demand"]["candidate_mae"] == 3.80
    assert res["demand"]["promotion"]["promoted"] is True
    assert res["corridor"]["candidate_mae"] == 0.28


def test_run_scheduled_retraining_pipeline_mocked():
    """Verify run_scheduled_retraining orchestrates all tasks end-to-end."""
    mock_engine = MagicMock()
    mock_store = MagicMock()
    mock_client = MagicMock()

    dummy_df = pd.DataFrame({"feat": [1.0, 2.0]})

    with (
        patch(
            "src.orchestration.flows.retraining_flow.reconcile_offline_features_task"
        ) as mock_reconcile,
        patch(
            "src.orchestration.flows.retraining_flow.extract_retraining_datasets_task"
        ) as mock_extract,
        patch(
            "src.orchestration.flows.retraining_flow.evaluate_retraining_baselines_task"
        ) as mock_baselines,
        patch(
            "src.orchestration.flows.retraining_flow.train_candidate_models_task"
        ) as mock_train,
        patch(
            "src.orchestration.flows.retraining_flow.evaluate_and_promote_candidates_task"
        ) as mock_promo,
        patch(
            "src.orchestration.flows.retraining_flow.backup_retraining_artifacts_task"
        ) as mock_r2,
    ):

        mock_reconcile.fn.return_value = {
            "zone_rows_loaded": 100,
            "corridor_rows_loaded": 50,
        }
        mock_extract.fn.return_value = {
            "demand_train": dummy_df,
            "demand_val": dummy_df,
            "corridor_train": dummy_df,
            "corridor_val": dummy_df,
        }
        mock_baselines.fn.return_value = {
            "demand_baseline": {"metrics": {"val_mae": 4.2}},
            "corridor_baseline": {"metrics": {"val_mae": 0.3}},
        }
        mock_train.fn.return_value = {
            "demand_model": {"run_id": "r_d", "metrics": {"val_mae": 3.8}},
            "corridor_model": {"run_id": "r_c", "metrics": {"val_mae": 0.25}},
        }
        mock_promo.fn.return_value = {
            "demand_promotion": {"promoted": True, "stage": "Production"},
            "corridor_promotion": {"promoted": True, "stage": "Production"},
        }
        mock_r2.fn.return_value = {"status": "success"}

        summary = run_scheduled_retraining(
            lookback_days=14,
            val_days=3,
            min_improvement_pct=0.02,
            engine=mock_engine,
            store=mock_store,
            client=mock_client,
            log_to_mlflow=True,
            promote_models=True,
            backup_to_r2=True,
            start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2026, 1, 15, tzinfo=timezone.utc),
            split_timestamp=datetime(2026, 1, 12, tzinfo=timezone.utc),
        )

        assert summary["status"] == "success"
        assert summary["reconcile_stats"]["zone_rows_loaded"] == 100
        assert summary["demand"]["candidate_mae"] == 3.8
        assert summary["corridor"]["candidate_mae"] == 0.25
        mock_reconcile.fn.assert_called_once()
        mock_extract.fn.assert_called_once()
        mock_baselines.fn.assert_called_once()
        mock_train.fn.assert_called_once()
        mock_promo.fn.assert_called_once()
        mock_r2.fn.assert_called_once()


def test_deploy_scheduled_retraining_flow_registration():
    """Verify deployment registration builds correct Prefect deployment object."""
    with patch("src.orchestration.deploy.scheduled_retraining_flow") as mock_flow:
        mock_deployment = MagicMock()
        mock_deployment.apply.return_value = "deploy_id_999"
        mock_flow.to_deployment.return_value = mock_deployment

        deploy_scheduled_retraining_flow(
            work_pool_name="test-pool",
            cron_schedule="0 4 * * 1",
        )

        mock_flow.to_deployment.assert_called_once_with(
            name="scheduled-model-retraining",
            parameters={
                "lookback_days": 28,
                "val_days": 7,
                "min_improvement_pct": 0.02,
                "promote_models": True,
                "backup_to_r2": True,
            },
            cron="0 4 * * 1",
            work_pool_name="test-pool",
            tags=["retraining", "mlops", "scheduled"],
            description="Weekly automated retraining of demand and corridor duration LightGBM models with 2% promotion hurdle gate.",
        )
        mock_deployment.apply.assert_called_once()
