"""Deployment registration script for Prefect batch ETL flows."""

import logging

from src.orchestration.flows.historical_etl import historical_tlc_batch_etl_flow

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


if __name__ == "__main__":
    deploy_historical_etl_flow()
