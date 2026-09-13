"""Deployment registration script for Prefect batch ETL flows."""

import logging

from src.orchestration.flows.historical_etl import historical_tlc_batch_etl_flow
from src.orchestration.flows.retraining_flow import scheduled_retraining_flow

logger = logging.getLogger(__name__)


def deploy_historical_etl_flow(
    work_pool_name: str = "default-agent-pool",
) -> None:
    """Deploy the historical TLC batch ETL flow to the Prefect work pool."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    logger.info(
        f"Registering 'historical-tlc-batch-etl' deployment to work pool '{work_pool_name}'..."
    )

    try:
        deployment = historical_tlc_batch_etl_flow.to_deployment(
            name="historical-tlc-batch-etl",
            parameters={
                "cab_type": "yellow",
                "year": 2023,
                "month": 1,
                "force_reload": False,
            },
            work_pool_name=work_pool_name,
            tags=["etl", "batch", "tlc"],
            description="Monthly TLC batch ETL flow with idempotency checking against warehouse.loaded_months.",
        )
        deployment_id = deployment.apply()
        logger.info(
            f"Successfully registered Prefect deployment (ID: {deployment_id})."
        )
    except Exception as e:
        logger.warning(
            f"Prefect deployment registration skipped or encountered non-fatal notice ({e}). "
            "Flow can be run directly via CLI or Prefect server."
        )


def deploy_scheduled_retraining_flow(
    work_pool_name: str = "default-agent-pool",
    cron_schedule: str = "0 3 * * 0",
) -> None:
    """Deploy the scheduled model retraining flow to the Prefect work pool."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    logger.info(
        f"Registering 'scheduled-model-retraining' deployment to work pool '{work_pool_name}' "
        f"with schedule '{cron_schedule}'..."
    )

    try:
        deployment = scheduled_retraining_flow.to_deployment(
            name="scheduled-model-retraining",
            parameters={
                "lookback_days": 28,
                "val_days": 7,
                "min_improvement_pct": 0.02,
                "promote_models": True,
                "backup_to_r2": True,
            },
            cron=cron_schedule,
            work_pool_name=work_pool_name,
            tags=["retraining", "mlops", "scheduled"],
            description="Weekly automated retraining of demand and corridor duration LightGBM models with 2% promotion hurdle gate.",
        )
        deployment_id = deployment.apply()
        logger.info(
            f"Successfully registered scheduled retraining deployment (ID: {deployment_id})."
        )
    except Exception as e:
        logger.warning(
            f"Prefect retraining deployment registration skipped or encountered non-fatal notice ({e}). "
            "Flow can be run directly via CLI or Prefect server."
        )


if __name__ == "__main__":
    deploy_historical_etl_flow()
    deploy_scheduled_retraining_flow()
