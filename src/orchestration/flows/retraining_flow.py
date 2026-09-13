"""Prefect flow for scheduled automated model retraining and safe promotion (M6-2).

Orchestrates the complete scheduled retraining lifecycle:
1. Reconcile Offline Features: Extracts recent trip records from warehouse.trips and populates
   offline hourly feature tables (warehouse.zone_demand_features_hourly, warehouse.corridor_duration_features_hourly).
2. Point-in-Time Dataset Extraction: Queries Feast offline store for demand and corridor features,
   validates dataset integrity, and splits temporally into train and validation partitions.
3. Baseline Evaluation: Computes seasonal naive baselines for demand and corridor duration.
4. Model Training: Trains candidate LightGBM regressors for demand and corridor duration,
   logging metrics, parameters, and pyfunc artifacts to MLflow experiments.
5. Model Promotion Gate (ADR-021): Evaluates candidates against active Production champions using
   a 2.0% minimum improvement hurdle rate. Promotes to Production stage or relegates to Staging.
6. R2 Artifact Backup: Triggers Cloudflare R2 backup of MLflow artifacts and registry metadata.
7. Retraining Summary: Emits structured execution metrics and promotion decisions.
"""

import argparse
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
from feast import FeatureStore
from mlflow.tracking import MlflowClient
from prefect import flow, task
from sqlalchemy.engine import Engine

from src.common.db import get_engine
from src.common.mlflow_utils import (
    DEMAND_MODEL_NAME,
    DURATION_MODEL_NAME,
    get_mlflow_client,
    setup_mlflow,
)
from src.features.config import get_feature_store
from src.features.offline_extractor import extract_and_load_offline_features
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
from src.training.promotion import (
    DEFAULT_MIN_IMPROVEMENT_PCT,
    ModelPromotionGate,
)
from src.training.r2_backup import backup_artifacts_to_r2_task
from src.training.train_demand import train_demand_lightgbm
from src.training.train_duration import train_duration_lightgbm

logger = logging.getLogger(__name__)


@task(
    name="reconcile_offline_features_retraining",
    retries=2,
    retry_delay_seconds=10,
    cache_policy=None,
)
def reconcile_offline_features_task(
    engine: Optional[Engine] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    lookback_days: int = 28,
) -> Dict[str, int]:
    """Extract recent warehouse.trips and update offline feature tables."""
    eng = engine or get_engine()
    zone_rows, corridor_rows = extract_and_load_offline_features(
        engine=eng,
        start_datetime=start_time,
        end_datetime=end_time,
        lookback_days=lookback_days,
    )
    logger.info(
        "Reconciled offline features for retraining: %d zone rows, %d corridor rows.",
        zone_rows,
        corridor_rows,
    )
    return {
        "zone_rows_loaded": zone_rows,
        "corridor_rows_loaded": corridor_rows,
    }


@task(
    name="extract_retraining_datasets",
    retries=1,
    retry_delay_seconds=10,
    cache_policy=None,
)
def extract_retraining_datasets_task(
    store: FeatureStore,
    engine: Engine,
    start_time: datetime,
    end_time: datetime,
    split_timestamp: datetime,
    zone_ids: Optional[List[int]] = None,
) -> Dict[str, Any]:
    """Generate point-in-time training and validation datasets for demand and corridors."""
    logger.info(
        "Extracting point-in-time demand dataset (start=%s, end=%s, split=%s)...",
        start_time,
        end_time,
        split_timestamp,
    )
    demand_df = generate_demand_training_dataset(
        store=store,
        engine=engine,
        start_time=start_time,
        end_time=end_time,
        zone_ids=zone_ids,
        features=DEMAND_FEATURES,
    )
    validate_dataset_integrity(
        df=demand_df,
        required_features=[
            "pickup_count_last_15m",
            "pickup_count_last_1h",
            "pickup_count_last_24h",
            "pickup_count_same_hour_last_week",
        ],
    )
    demand_train, demand_val = train_val_split_by_time(demand_df, split_timestamp)

    logger.info("Extracting point-in-time corridor dataset for retraining...")
    corridor_df = generate_corridor_training_dataset(
        store=store,
        engine=engine,
        start_time=start_time,
        end_time=end_time,
        features=CORRIDOR_FEATURES,
    )
    validate_dataset_integrity(
        df=corridor_df,
        required_features=[
            "avg_trip_duration_last_1h",
            "trip_distance_km",
        ],
    )
    corridor_train, corridor_val = train_val_split_by_time(corridor_df, split_timestamp)

    logger.info(
        "Extracted datasets: demand_train=%d, demand_val=%d, corridor_train=%d, corridor_val=%d",
        len(demand_train),
        len(demand_val),
        len(corridor_train),
        len(corridor_val),
    )

    return {
        "demand_train": demand_train,
        "demand_val": demand_val,
        "corridor_train": corridor_train,
        "corridor_val": corridor_val,
    }


@task(name="evaluate_retraining_baselines", cache_policy=None)
def evaluate_retraining_baselines_task(
    demand_val: pd.DataFrame,
    corridor_val: pd.DataFrame,
    log_to_mlflow: bool = True,
) -> Dict[str, Any]:
    """Compute naive seasonal baselines on the validation splits."""
    logger.info("Evaluating naive baselines for demand and corridor duration...")
    demand_baseline = evaluate_demand_baseline(
        val_df=demand_val,
        log_to_mlflow=log_to_mlflow,
    )
    corridor_baseline = evaluate_corridor_duration_baseline(
        val_df=corridor_val,
        log_to_mlflow=log_to_mlflow,
    )
    return {
        "demand_baseline": demand_baseline,
        "corridor_baseline": corridor_baseline,
    }


@task(name="train_candidate_models", cache_policy=None)
def train_candidate_models_task(
    demand_train: pd.DataFrame,
    demand_val: pd.DataFrame,
    corridor_train: pd.DataFrame,
    corridor_val: pd.DataFrame,
    demand_baseline_mae: float,
    corridor_baseline_mae: float,
    log_to_mlflow: bool = True,
) -> Dict[str, Any]:
    """Train candidate LightGBM demand and duration models."""
    logger.info("Training candidate LightGBM demand model...")
    demand_res = train_demand_lightgbm(
        train_df=demand_train,
        val_df=demand_val,
        baseline_mae=demand_baseline_mae,
        log_to_mlflow=log_to_mlflow,
    )

    logger.info("Training candidate LightGBM corridor duration model...")
    corridor_res = train_duration_lightgbm(
        train_df=corridor_train,
        val_df=corridor_val,
        baseline_mae=corridor_baseline_mae,
        log_to_mlflow=log_to_mlflow,
    )

    return {
        "demand_model": demand_res,
        "corridor_model": corridor_res,
    }


@task(name="evaluate_and_promote_candidates", cache_policy=None)
def evaluate_and_promote_candidates_task(
    client: MlflowClient,
    demand_candidate_run_id: str,
    demand_candidate_mae: float,
    demand_baseline_mae: float,
    corridor_candidate_run_id: str,
    corridor_candidate_mae: float,
    corridor_baseline_mae: float,
    min_improvement_pct: float = DEFAULT_MIN_IMPROVEMENT_PCT,
) -> Dict[str, Any]:
    """Evaluate candidates against active Production champions using ModelPromotionGate."""
    gate = ModelPromotionGate(client=client, min_improvement_pct=min_improvement_pct)

    logger.info(
        "Evaluating demand candidate model for promotion (hurdle rate: %.1f%%)...",
        min_improvement_pct * 100.0,
    )
    demand_promo = gate.evaluate_and_promote(
        model_name=DEMAND_MODEL_NAME,
        candidate_run_id=demand_candidate_run_id,
        candidate_mae=demand_candidate_mae,
        baseline_mae=demand_baseline_mae,
        min_improvement_pct=min_improvement_pct,
    )

    logger.info(
        "Evaluating corridor duration candidate model for promotion (hurdle rate: %.1f%%)...",
        min_improvement_pct * 100.0,
    )
    corridor_promo = gate.evaluate_and_promote(
        model_name=DURATION_MODEL_NAME,
        candidate_run_id=corridor_candidate_run_id,
        candidate_mae=corridor_candidate_mae,
        baseline_mae=corridor_baseline_mae,
        min_improvement_pct=min_improvement_pct,
    )

    return {
        "demand_promotion": demand_promo,
        "corridor_promotion": corridor_promo,
    }


@task(name="backup_retraining_artifacts", cache_policy=None)
def backup_retraining_artifacts_task(backup_to_r2: bool = True) -> Dict[str, Any]:
    """Backup MLflow model artifacts and registry metadata to Cloudflare R2."""
    if not backup_to_r2:
        return {"status": "skipped", "reason": "backup_disabled"}
    logger.info("Executing Cloudflare R2 backup of retraining artifacts...")
    return backup_artifacts_to_r2_task.fn()


@task(name="generate_retraining_summary", cache_policy=None)
def generate_retraining_summary_task(
    demand_baseline: Dict[str, Any],
    demand_model: Dict[str, Any],
    demand_promo: Optional[Dict[str, Any]],
    corridor_baseline: Dict[str, Any],
    corridor_model: Dict[str, Any],
    corridor_promo: Optional[Dict[str, Any]],
    r2_status: Dict[str, Any],
    elapsed_seconds: float,
) -> Dict[str, Any]:
    """Generate and log a structured summary of retraining metrics and promotion decisions."""
    logger.info(
        "================================================================================"
    )
    logger.info(
        "                      SCHEDULED RETRAINING FLOW SUMMARY                         "
    )
    logger.info(
        "================================================================================"
    )

    def _format_model_row(
        name: str,
        baseline: Dict[str, Any],
        model: Dict[str, Any],
        promo: Optional[Dict[str, Any]],
    ) -> str:
        b_mae = baseline["metrics"]["val_mae"]
        m_mae = model["metrics"]["val_mae"]
        p_status = promo.get("stage", "None") if promo else "Skipped"
        p_dec = "PROMOTED" if promo and promo.get("promoted") else "NOT PROMOTED"
        prod_mae = promo.get("production_mae") if promo else None
        prod_str = f"{prod_mae:.4f}" if prod_mae is not None else "None (Cold Start)"
        hurdle_mae = promo.get("hurdle_mae") if promo else None
        hurdle_str = f"{hurdle_mae:.4f}" if hurdle_mae is not None else "N/A"
        return (
            f"  * {name}:\n"
            f"      - Baseline MAE:      {b_mae:.4f}\n"
            f"      - Production MAE:    {prod_str}\n"
            f"      - Hurdle MAE:        {hurdle_str}\n"
            f"      - Candidate MAE:     {m_mae:.4f}\n"
            f"      - Decision:          {p_dec} -> Stage: {p_status}\n"
            f"      - Reason:            {promo.get('reason') if promo else 'N/A'}"
        )

    logger.info(
        _format_model_row(
            DEMAND_MODEL_NAME, demand_baseline, demand_model, demand_promo
        )
    )
    logger.info(
        _format_model_row(
            DURATION_MODEL_NAME, corridor_baseline, corridor_model, corridor_promo
        )
    )
    logger.info("  * R2 Artifact Backup:     %s", r2_status.get("status", "unknown"))
    logger.info("  * Total Elapsed Time:     %.2fs", elapsed_seconds)
    logger.info(
        "================================================================================"
    )

    return {
        "status": "success",
        "elapsed_seconds": elapsed_seconds,
        "demand": {
            "baseline_mae": demand_baseline["metrics"]["val_mae"],
            "candidate_mae": demand_model["metrics"]["val_mae"],
            "promotion": demand_promo,
        },
        "corridor": {
            "baseline_mae": corridor_baseline["metrics"]["val_mae"],
            "candidate_mae": corridor_model["metrics"]["val_mae"],
            "promotion": corridor_promo,
        },
        "r2_backup": r2_status,
    }


# Alias for backward compatibility / direct import
generate_retraining_summary = generate_retraining_summary_task


def run_scheduled_retraining(
    lookback_days: int = 28,
    val_days: int = 7,
    zone_ids: Optional[List[int]] = None,
    min_improvement_pct: float = DEFAULT_MIN_IMPROVEMENT_PCT,
    promote_models: bool = True,
    backup_to_r2: bool = True,
    log_to_mlflow: bool = True,
    reconcile_features: bool = True,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    split_timestamp: Optional[datetime] = None,
    engine: Optional[Engine] = None,
    store: Optional[FeatureStore] = None,
    client: Optional[MlflowClient] = None,
) -> Dict[str, Any]:
    """Execute scheduled retraining logic with tasks."""
    t_start = time.perf_counter()
    logger.info(
        "=== Starting Scheduled Retraining Flow (lookback_days=%d, val_days=%d, hurdle=%.1f%%) ===",
        lookback_days,
        val_days,
        min_improvement_pct * 100.0,
    )

    eng = engine or get_engine()
    feat_store = store or get_feature_store()

    mlflow_client = None
    if log_to_mlflow:
        setup_mlflow()
        mlflow_client = client or get_mlflow_client()

    # Determine temporal partitions
    now_utc = datetime.now(timezone.utc)
    eff_end_time = end_time or now_utc
    eff_start_time = start_time or (eff_end_time - timedelta(days=lookback_days))
    eff_split_timestamp = split_timestamp or (eff_end_time - timedelta(days=val_days))

    # 1. Reconcile offline feature tables if requested
    reconcile_stats = {"zone_rows_loaded": 0, "corridor_rows_loaded": 0}
    if reconcile_features:
        logger.info("Executing offline feature reconciliation step...")
        reconcile_stats = reconcile_offline_features_task.fn(
            engine=eng,
            start_time=eff_start_time,
            end_time=eff_end_time,
            lookback_days=lookback_days,
        )

    # 2. Extract point-in-time datasets
    datasets = extract_retraining_datasets_task.fn(
        store=feat_store,
        engine=eng,
        start_time=eff_start_time,
        end_time=eff_end_time,
        split_timestamp=eff_split_timestamp,
        zone_ids=zone_ids,
    )

    # 3. Evaluate naive baselines
    baselines = evaluate_retraining_baselines_task.fn(
        demand_val=datasets["demand_val"],
        corridor_val=datasets["corridor_val"],
        log_to_mlflow=log_to_mlflow,
    )

    # 4. Train candidate models
    trained_models = train_candidate_models_task.fn(
        demand_train=datasets["demand_train"],
        demand_val=datasets["demand_val"],
        corridor_train=datasets["corridor_train"],
        corridor_val=datasets["corridor_val"],
        demand_baseline_mae=baselines["demand_baseline"]["metrics"]["val_mae"],
        corridor_baseline_mae=baselines["corridor_baseline"]["metrics"]["val_mae"],
        log_to_mlflow=log_to_mlflow,
    )

    # 5. Evaluate and promote candidates using ModelPromotionGate
    promotions = {"demand_promotion": None, "corridor_promotion": None}
    if promote_models and log_to_mlflow and mlflow_client:
        promotions = evaluate_and_promote_candidates_task.fn(
            client=mlflow_client,
            demand_candidate_run_id=trained_models["demand_model"]["run_id"],
            demand_candidate_mae=trained_models["demand_model"]["metrics"]["val_mae"],
            demand_baseline_mae=baselines["demand_baseline"]["metrics"]["val_mae"],
            corridor_candidate_run_id=trained_models["corridor_model"]["run_id"],
            corridor_candidate_mae=trained_models["corridor_model"]["metrics"][
                "val_mae"
            ],
            corridor_baseline_mae=baselines["corridor_baseline"]["metrics"]["val_mae"],
            min_improvement_pct=min_improvement_pct,
        )

    # 6. Backup artifacts to Cloudflare R2
    r2_status = backup_retraining_artifacts_task.fn(backup_to_r2=backup_to_r2)

    # 7. Retraining summary
    elapsed = time.perf_counter() - t_start
    summary = generate_retraining_summary_task.fn(
        demand_baseline=baselines["demand_baseline"],
        demand_model=trained_models["demand_model"],
        demand_promo=promotions.get("demand_promotion"),
        corridor_baseline=baselines["corridor_baseline"],
        corridor_model=trained_models["corridor_model"],
        corridor_promo=promotions.get("corridor_promotion"),
        r2_status=r2_status,
        elapsed_seconds=elapsed,
    )
    summary["reconcile_stats"] = reconcile_stats
    return summary


@flow(name="scheduled-model-retraining-flow")
def scheduled_retraining_flow(
    lookback_days: int = 28,
    val_days: int = 7,
    zone_ids: Optional[List[int]] = None,
    min_improvement_pct: float = DEFAULT_MIN_IMPROVEMENT_PCT,
    promote_models: bool = True,
    backup_to_r2: bool = True,
    log_to_mlflow: bool = True,
    reconcile_features: bool = True,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    split_timestamp: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Top-level Prefect flow for scheduled model retraining."""
    return run_scheduled_retraining(
        lookback_days=lookback_days,
        val_days=val_days,
        zone_ids=zone_ids,
        min_improvement_pct=min_improvement_pct,
        promote_models=promote_models,
        backup_to_r2=backup_to_r2,
        log_to_mlflow=log_to_mlflow,
        reconcile_features=reconcile_features,
        start_time=start_time,
        end_time=end_time,
        split_timestamp=split_timestamp,
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description="Run scheduled model retraining flow.")
    parser.add_argument(
        "--lookback-days", type=int, default=28, help="Days of history for training."
    )
    parser.add_argument(
        "--val-days", type=int, default=7, help="Days of history for validation."
    )
    parser.add_argument(
        "--hurdle-pct",
        type=float,
        default=DEFAULT_MIN_IMPROVEMENT_PCT,
        help="Hurdle rate over production MAE.",
    )
    parser.add_argument(
        "--skip-r2", action="store_true", help="Skip Cloudflare R2 backup."
    )
    parser.add_argument(
        "--skip-reconcile",
        action="store_true",
        help="Skip offline feature reconciliation.",
    )
    args = parser.parse_args()

    scheduled_retraining_flow(
        lookback_days=args.lookback_days,
        val_days=args.val_days,
        min_improvement_pct=args.hurdle_pct,
        backup_to_r2=not args.skip_r2,
        reconcile_features=not args.skip_reconcile,
    )
