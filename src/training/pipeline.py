"""End-to-end model training, baseline evaluation, registry promotion, and backup pipeline (M3-5)."""

import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from feast import FeatureStore
from mlflow.tracking import MlflowClient
from prefect import flow, task
from sqlalchemy.engine import Engine

from src.common.mlflow_utils import (
    DEMAND_EXPERIMENT_NAME,
    DURATION_EXPERIMENT_NAME,
    get_mlflow_client,
    setup_mlflow,
)
from src.features.config import get_feature_store
from src.training.baseline import (
    evaluate_corridor_duration_baseline,
    evaluate_demand_baseline,
)
from src.training.dataset import (
    CORRIDOR_FEATURES,
    DEMAND_FEATURES,
    generate_corridor_training_dataset,
    generate_demand_training_dataset,
    train_val_split_by_time,
    validate_dataset_integrity,
)
from src.training.r2_backup import backup_artifacts_to_r2_task
from src.training.train_demand import train_demand_lightgbm
from src.training.train_duration import train_duration_lightgbm

logger = logging.getLogger(__name__)

DEMAND_MODEL_NAME = "demand_lightgbm_model"
DURATION_MODEL_NAME = "corridor_duration_lightgbm_model"


def _find_or_create_model_version(
    client: MlflowClient,
    model_name: str,
    candidate_run_id: str,
) -> Optional[Any]:
    """Find existing registered model version or register a new one from candidate run."""
    try:
        versions = client.search_model_versions(f"name='{model_name}'")
    except Exception:
        versions = []

    for v in versions:
        if v.run_id == candidate_run_id:
            return v

    if versions:
        return sorted(versions, key=lambda x: int(x.version))[-1]

    # Explicitly register model version from run artifact
    logger.info(
        "Registering model version for '%s' from run '%s'...",
        model_name,
        candidate_run_id,
    )
    try:
        client.create_registered_model(model_name)
    except Exception:
        pass  # Model already exists

    try:
        return client.create_model_version(
            name=model_name,
            source=f"runs:/{candidate_run_id}/model",
            run_id=candidate_run_id,
        )
    except Exception as reg_err:
        logger.warning("Failed to register version for '%s': %s", model_name, reg_err)
        return None


def _transition_model_stage(
    client: MlflowClient,
    model_name: str,
    target_version: Any,
    candidate_mae: float,
    baseline_mae: float,
    outcome: Dict[str, Any],
) -> None:
    """Transition model version stage to Production (if improved) or Staging."""
    version_num = target_version.version
    if candidate_mae <= baseline_mae:
        logger.info(
            "Promoting %s v%s to Production (Candidate MAE=%.4f <= Baseline MAE=%.4f)...",
            model_name,
            version_num,
            candidate_mae,
            baseline_mae,
        )
        try:
            client.transition_model_version_stage(
                name=model_name,
                version=version_num,
                stage="Production",
                archive_existing_versions=True,
            )
            outcome["stage"] = "Production"
            outcome["promoted"] = True
            outcome["reason"] = (
                f"Outperformed baseline (MAE {candidate_mae:.4f} <= {baseline_mae:.4f})"
            )
        except Exception:
            try:
                client.set_registered_model_alias(
                    name=model_name,
                    alias="champion",
                    version=version_num,
                )
                outcome["stage"] = "champion_alias"
                outcome["promoted"] = True
            except Exception as alias_err:
                outcome["reason"] = f"Registry update failed: {alias_err}"
    else:
        logger.warning(
            "Candidate %s v%s did not improve over baseline (Candidate MAE=%.4f > Baseline MAE=%.4f); moving to Staging.",
            model_name,
            version_num,
            candidate_mae,
            baseline_mae,
        )
        try:
            client.transition_model_version_stage(
                name=model_name,
                version=version_num,
                stage="Staging",
                archive_existing_versions=False,
            )
            outcome["stage"] = "Staging"
            outcome["reason"] = "Higher error than baseline"
        except Exception:
            outcome["stage"] = "None"


def promote_model_to_production(
    client: MlflowClient,
    model_name: str,
    candidate_run_id: str,
    candidate_mae: float,
    baseline_mae: float,
) -> Dict[str, Any]:
    """Promote registered model version to Production stage if it improves over baseline.

    Args:
        client: MLflow tracking and registry client.
        model_name: Registered model name.
        candidate_run_id: Run ID of the trained candidate model.
        candidate_mae: Candidate model validation MAE.
        baseline_mae: Baseline validation MAE.

    Returns:
        Dictionary detailing version number, stage, and promotion outcome.
    """
    outcome: Dict[str, Any] = {
        "model_name": model_name,
        "promoted": False,
        "version": None,
        "stage": "None",
        "reason": "no_versions_found",
    }

    try:
        target_version = _find_or_create_model_version(
            client=client, model_name=model_name, candidate_run_id=candidate_run_id
        )
        if target_version is None:
            logger.warning(
                "No registered model version found or created for model '%s'.",
                model_name,
            )
            return outcome

        outcome["version"] = str(target_version.version)
        _transition_model_stage(
            client=client,
            model_name=model_name,
            target_version=target_version,
            candidate_mae=candidate_mae,
            baseline_mae=baseline_mae,
            outcome=outcome,
        )
    except Exception as exc:
        logger.error("Error during model promotion for '%s': %s", model_name, exc)
        outcome["reason"] = f"Exception: {exc}"

    return outcome


@task(name="extract-training-datasets")
def extract_training_datasets_task(
    store: FeatureStore,
    engine: Engine,
    start_time: datetime,
    end_time: datetime,
    split_timestamp: datetime,
    zone_ids: Optional[List[int]] = None,
) -> Dict[str, Any]:
    """Generate point-in-time training and validation datasets for demand and corridors."""
    logger.info("Extracting point-in-time demand dataset from Feast...")
    demand_df = generate_demand_training_dataset(
        store=store,
        engine=engine,
        start_time=start_time,
        end_time=end_time,
        zone_ids=zone_ids,
        features=DEMAND_FEATURES,
    )
    # Validate dataset integrity
    validate_dataset_integrity(
        df=demand_df,
        required_features=[
            "pickup_count_last_15m",
            "pickup_count_last_1h",
            "pickup_count_last_24h",
            "pickup_count_same_hour_last_week",
        ],
        target_col="target_pickup_count_next_1h",
    )
    demand_train, demand_val = train_val_split_by_time(
        demand_df, split_timestamp=split_timestamp
    )

    logger.info("Extracting point-in-time corridor duration dataset from Feast...")

    corridor_df = generate_corridor_training_dataset(
        store=store,
        engine=engine,
        start_time=start_time,
        end_time=end_time,
        features=CORRIDOR_FEATURES,
    )
    corridor_train, corridor_val = train_val_split_by_time(
        corridor_df, split_timestamp=split_timestamp
    )
    validate_dataset_integrity(
        df=corridor_df,
        required_features=[
            "avg_duration_last_15m",
            "avg_duration_last_1h",
            "distance_km",
            "origin_zone_demand_pressure",
        ],
        target_col="target_avg_duration_next_1h",
    )

    return {
        "demand_train": demand_train,
        "demand_val": demand_val,
        "corridor_train": corridor_train,
        "corridor_val": corridor_val,
    }


def run_training_pipeline(
    start_time: datetime,
    end_time: datetime,
    split_timestamp: datetime,
    store: Optional[FeatureStore] = None,
    engine: Optional[Engine] = None,
    zone_ids: Optional[List[int]] = None,
    demand_params: Optional[Dict[str, Any]] = None,
    duration_params: Optional[Dict[str, Any]] = None,
    backup_to_r2: bool = True,
    log_to_mlflow: bool = True,
    promote_models: bool = True,
) -> Dict[str, Any]:
    """Execute the full end-to-end training and promotion pipeline.

    Workflow:
      1. Point-in-time dataset generation (M3-2)
      2. Seasonal-naive baseline benchmark evaluation (M3-3)
      3. LightGBM model training with log1p target on duration (M3-4)
      4. Evaluation and relative lift calculation
      5. MLflow Model Registry promotion to Production stage
      6. Cloudflare R2 artifact/model backup

    Returns:
        Complete execution summary dictionary.
    """
    t_start = time.perf_counter()
    setup_mlflow()
    client = get_mlflow_client()

    store = store or get_feature_store()
    if engine is None:
        from src.common.config import get_engine

        engine = get_engine()

    logger.info("=== Starting End-to-End Training Pipeline ===")
    logger.info("Period: %s -> %s (Split: %s)", start_time, end_time, split_timestamp)

    # 1. Dataset generation
    datasets = extract_training_datasets_task.fn(
        store=store,
        engine=engine,
        start_time=start_time,
        end_time=end_time,
        split_timestamp=split_timestamp,
        zone_ids=zone_ids,
    )
    demand_train = datasets["demand_train"]
    demand_val = datasets["demand_val"]
    corridor_train = datasets["corridor_train"]
    corridor_val = datasets["corridor_val"]

    # 2. Baseline benchmarks
    logger.info("Evaluating seasonal-naive baselines...")
    demand_baseline = evaluate_demand_baseline(
        val_df=demand_val,
        experiment_name=DEMAND_EXPERIMENT_NAME,
        run_name="pipeline_baseline_seasonal_naive_demand",
        log_to_mlflow=log_to_mlflow,
    )
    corridor_baseline = evaluate_corridor_duration_baseline(
        val_df=corridor_val,
        experiment_name=DURATION_EXPERIMENT_NAME,
        run_name="pipeline_baseline_moving_avg_duration",
        log_to_mlflow=log_to_mlflow,
    )

    # 3. Model training
    logger.info("Training LightGBM models...")
    demand_model_res = train_demand_lightgbm(
        train_df=demand_train,
        val_df=demand_val,
        params=demand_params,
        baseline_mae=demand_baseline["metrics"]["val_mae"],
        experiment_name=DEMAND_EXPERIMENT_NAME,
        run_name="pipeline_lightgbm_demand_regressor",
        log_to_mlflow=log_to_mlflow,
    )

    corridor_model_res = train_duration_lightgbm(
        train_df=corridor_train,
        val_df=corridor_val,
        params=duration_params,
        baseline_mae=corridor_baseline["metrics"]["val_mae"],
        experiment_name=DURATION_EXPERIMENT_NAME,
        run_name="pipeline_lightgbm_duration_log1p_regressor",
        log_to_mlflow=log_to_mlflow,
    )

    # 4. Model Registry Promotion
    demand_promo = None
    corridor_promo = None
    if promote_models and log_to_mlflow:
        logger.info("Evaluating candidate models for Model Registry promotion...")
        demand_promo = promote_model_to_production(
            client=client,
            model_name=DEMAND_MODEL_NAME,
            candidate_run_id=demand_model_res["run_id"],
            candidate_mae=demand_model_res["metrics"]["val_mae"],
            baseline_mae=demand_baseline["metrics"]["val_mae"],
        )
        corridor_promo = promote_model_to_production(
            client=client,
            model_name=DURATION_MODEL_NAME,
            candidate_run_id=corridor_model_res["run_id"],
            candidate_mae=corridor_model_res["metrics"]["val_mae"],
            baseline_mae=corridor_baseline["metrics"]["val_mae"],
        )

    # 5. Cloudflare R2 backup
    r2_status = {"status": "skipped", "reason": "backup_disabled"}
    if backup_to_r2:
        logger.info("Executing Cloudflare R2 backup...")
        r2_status = backup_artifacts_to_r2_task.fn()

    elapsed = time.perf_counter() - t_start
    logger.info("=== End-to-End Training Pipeline Complete in %.2fs ===", elapsed)

    return {
        "status": "success",
        "elapsed_seconds": elapsed,
        "datasets": {
            "demand_train_rows": len(demand_train),
            "demand_val_rows": len(demand_val),
            "corridor_train_rows": len(corridor_train),
            "corridor_val_rows": len(corridor_val),
        },
        "demand": {
            "baseline_mae": demand_baseline["metrics"]["val_mae"],
            "model_mae": demand_model_res["metrics"]["val_mae"],
            "lift_pct": demand_model_res["lift_pct"],
            "run_id": demand_model_res["run_id"],
            "promotion": demand_promo,
        },
        "duration": {
            "baseline_mae": corridor_baseline["metrics"]["val_mae"],
            "model_mae": corridor_model_res["metrics"]["val_mae"],
            "lift_pct": corridor_model_res["lift_pct"],
            "run_id": corridor_model_res["run_id"],
            "promotion": corridor_promo,
        },
        "r2_backup": r2_status,
    }


@flow(name="end-to-end-training-pipeline")
def training_pipeline_flow(
    start_time: datetime,
    end_time: datetime,
    split_timestamp: datetime,
    zone_ids: Optional[List[int]] = None,
    backup_to_r2: bool = True,
) -> Dict[str, Any]:
    """Prefect flow orchestrator for model training pipeline."""
    return run_training_pipeline(
        start_time=start_time,
        end_time=end_time,
        split_timestamp=split_timestamp,
        zone_ids=zone_ids,
        backup_to_r2=backup_to_r2,
    )
