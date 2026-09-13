"""Model promotion safety gate and champion/challenger evaluation for MLflow Model Registry.

ADR-021: Implements automated champion/challenger comparison before registry promotion.
Retraining must never unconditionally replace an active Production model. Candidate models
must demonstrate statistically meaningful improvement beyond training noise.

Rationale for 2.0% default hurdle rate (min_improvement_pct = 0.02):
    Retraining gradient boosted decision trees (LightGBM/XGBoost) on large tabular trip datasets
    exhibits subtle run-to-run metric variance (+/-0.3% to 0.8%) arising from multithreaded tree
    building, stochastic feature/data subsampling, and floating-point non-associativity across CPU cores.
    A 0.0% threshold results in false-champion churn where models are repeatedly promoted solely
    due to stochastic seed jitter. Requiring a minimum 2.0% error reduction (candidate_mae <= prod_mae * 0.98)
    guarantees that registry promotion reflects genuine, statistically meaningful model discrimination
    and generalization gains on fresh production data.
"""

import logging
from typing import Any, Dict, Optional

from mlflow.tracking import MlflowClient

from src.common.mlflow_utils import get_mlflow_client

logger = logging.getLogger(__name__)

# Default hurdle rate: Candidate must reduce MAE by at least 2.0% relative to current Production champion
DEFAULT_MIN_IMPROVEMENT_PCT: float = 0.02


def find_or_create_model_version(
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

    logger.info(
        "Registering model version for '%s' from run '%s'...",
        model_name,
        candidate_run_id,
    )
    try:
        client.create_registered_model(model_name)
    except Exception:
        pass  # Model already exists in registry

    try:
        return client.create_model_version(
            name=model_name,
            source=f"runs:/{candidate_run_id}/model",
            run_id=candidate_run_id,
        )
    except Exception as reg_err:
        logger.warning("Failed to register version for '%s': %s", model_name, reg_err)
        return None


class ModelPromotionGate:
    """Evaluates candidate models against active Production models and naive baselines."""

    def __init__(
        self,
        client: Optional[MlflowClient] = None,
        min_improvement_pct: float = DEFAULT_MIN_IMPROVEMENT_PCT,
    ) -> None:
        """Initialize promotion gate.

        Args:
            client: MLflow tracking and registry client. Defaults to configured client.
            min_improvement_pct: Fractional hurdle rate required over Production MAE (default 0.02 = 2%).
        """
        self.client = client or get_mlflow_client()
        self.min_improvement_pct = min_improvement_pct

    def get_current_production_model(self, model_name: str) -> Optional[Dict[str, Any]]:
        """Retrieve current Production stage model version and its validation metrics.

        Args:
            model_name: Registered model name in MLflow.

        Returns:
            Dictionary with version, run_id, stage, and metrics, or None if no Production version exists.
        """
        try:
            prod_versions = self.client.get_latest_versions(
                model_name, stages=["Production"]
            )
        except Exception as e:
            logger.debug(
                "Could not query latest versions for '%s' via stages=['Production']: %s",
                model_name,
                e,
            )
            prod_versions = []

        if not prod_versions:
            # Fallback to search_model_versions in case stage filter differs
            try:
                all_versions = self.client.search_model_versions(f"name='{model_name}'")
                prod_versions = [
                    v
                    for v in all_versions
                    if getattr(v, "current_stage", None) == "Production"
                ]
            except Exception as e:
                logger.debug("Search model versions failed for '%s': %s", model_name, e)
                prod_versions = []

        if not prod_versions:
            logger.info(
                "No active 'Production' model version found for '%s'.", model_name
            )
            return None

        # Take the newest production version
        latest_prod = sorted(
            prod_versions, key=lambda v: int(getattr(v, "version", 0)), reverse=True
        )[0]
        version_num = str(latest_prod.version)
        run_id = latest_prod.run_id

        metrics: Dict[str, float] = {}
        if run_id:
            try:
                run = self.client.get_run(run_id)
                metrics = getattr(getattr(run, "data", None), "metrics", {}) or {}
            except Exception as run_err:
                logger.warning(
                    "Could not fetch run '%s' for production model '%s' v%s: %s",
                    run_id,
                    model_name,
                    version_num,
                    run_err,
                )

        return {
            "model_name": model_name,
            "version": version_num,
            "run_id": run_id,
            "stage": "Production",
            "metrics": metrics,
            "val_mae": metrics.get("val_mae"),
            "val_rmse": metrics.get("val_rmse"),
            "val_wape": metrics.get("val_wape"),
        }

    def evaluate_and_promote(
        self,
        model_name: str,
        candidate_run_id: str,
        candidate_mae: float,
        baseline_mae: float,
        candidate_metrics: Optional[Dict[str, float]] = None,
        min_improvement_pct: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Evaluate candidate model metrics against baseline and active Production champion.

        Promotes candidate to Production if:
        1. candidate_mae <= baseline_mae (beats naive baseline)
        2. candidate_mae <= prod_mae * (1.0 - min_improvement_pct) (beats current Production by hurdle rate)
           (or if no Production model exists, beats baseline as inaugural champion)

        Args:
            model_name: Registered model name.
            candidate_run_id: Run ID of the candidate model.
            candidate_mae: Candidate model validation MAE.
            baseline_mae: Naive baseline validation MAE.
            candidate_metrics: Optional full validation metrics dict for candidate.
            min_improvement_pct: Optional hurdle override (default: self.min_improvement_pct).

        Returns:
            Dictionary with promotion decision, target stage, and comparison details.
        """
        hurdle_pct = (
            self.min_improvement_pct
            if min_improvement_pct is None
            else min_improvement_pct
        )

        outcome: Dict[str, Any] = {
            "model_name": model_name,
            "promoted": False,
            "version": None,
            "stage": "None",
            "reason": "uninitialized",
            "candidate_mae": candidate_mae,
            "baseline_mae": baseline_mae,
            "production_mae": None,
            "production_version": None,
            "hurdle_mae": None,
            "min_improvement_pct": hurdle_pct,
            "actual_improvement_pct": None,
        }

        # 1. Register candidate version
        target_version = find_or_create_model_version(
            client=self.client,
            model_name=model_name,
            candidate_run_id=candidate_run_id,
        )
        if target_version is None:
            outcome["reason"] = "Failed to register candidate model version in MLflow."
            return outcome

        candidate_version_num = str(target_version.version)
        outcome["version"] = candidate_version_num

        # 2. Safety Gate 1: Outperform naive baseline
        if candidate_mae > baseline_mae:
            logger.warning(
                "Promotion REJECTED: Candidate %s v%s MAE=%.4f failed naive baseline MAE=%.4f.",
                model_name,
                candidate_version_num,
                candidate_mae,
                baseline_mae,
            )
            self._transition_to_staging(model_name, candidate_version_num)
            outcome["stage"] = "Staging"
            outcome["promoted"] = False
            outcome["reason"] = (
                f"Candidate MAE ({candidate_mae:.4f}) failed naive baseline MAE ({baseline_mae:.4f})"
            )
            return outcome

        # 3. Safety Gate 2: Outperform current Production champion by hurdle rate
        current_prod = self.get_current_production_model(model_name)

        if current_prod is None or current_prod.get("val_mae") is None:
            # Cold start: Inaugural production model
            logger.info(
                "Cold-start promotion: No active Production champion found for %s. "
                "Promoting candidate v%s to Production (Candidate MAE=%.4f <= Baseline MAE=%.4f)...",
                model_name,
                candidate_version_num,
                candidate_mae,
                baseline_mae,
            )
            self._promote_to_production(model_name, candidate_version_num)
            outcome["stage"] = "Production"
            outcome["promoted"] = True
            outcome["reason"] = (
                f"Inaugural promotion (cold start): Candidate MAE ({candidate_mae:.4f}) beat baseline ({baseline_mae:.4f})"
            )
            return outcome

        prod_version_num = current_prod["version"]
        prod_mae = float(current_prod["val_mae"])
        outcome["production_mae"] = prod_mae
        outcome["production_version"] = prod_version_num

        # Compute required hurdle MAE
        hurdle_mae = prod_mae * (1.0 - hurdle_pct)
        outcome["hurdle_mae"] = round(hurdle_mae, 4)

        actual_improvement_pct = (prod_mae - candidate_mae) / prod_mae
        outcome["actual_improvement_pct"] = round(actual_improvement_pct, 4)

        if candidate_mae <= hurdle_mae:
            # Candidate beats production by at least the hurdle rate
            logger.info(
                "Promotion APPROVED: %s candidate v%s (MAE=%.4f) outperformed Production v%s (MAE=%.4f) "
                "by %.2f%% (hurdle: %.1f%%, max allowed MAE=%.4f). Promoting to Production...",
                model_name,
                candidate_version_num,
                candidate_mae,
                prod_version_num,
                prod_mae,
                actual_improvement_pct * 100.0,
                hurdle_pct * 100.0,
                hurdle_mae,
            )
            self._promote_to_production(model_name, candidate_version_num)
            outcome["stage"] = "Production"
            outcome["promoted"] = True
            outcome["reason"] = (
                f"Candidate v{candidate_version_num} (MAE {candidate_mae:.4f}) beat Production v{prod_version_num} "
                f"(MAE {prod_mae:.4f}) by {actual_improvement_pct * 100.0:.2f}% (hurdle: {hurdle_pct * 100.0:.1f}%)"
            )
        else:
            # Candidate failed hurdle or had higher error
            self._transition_to_staging(model_name, candidate_version_num)
            outcome["stage"] = "Staging"
            outcome["promoted"] = False

            if candidate_mae > prod_mae:
                logger.warning(
                    "Promotion REJECTED: Candidate %s v%s (MAE=%.4f) has HIGHER error than Production v%s (MAE=%.4f). "
                    "Moved to Staging.",
                    model_name,
                    candidate_version_num,
                    candidate_mae,
                    prod_version_num,
                    prod_mae,
                )
                outcome["reason"] = (
                    f"Candidate v{candidate_version_num} MAE ({candidate_mae:.4f}) was worse than "
                    f"Production v{prod_version_num} MAE ({prod_mae:.4f})"
                )
            else:
                logger.warning(
                    "Promotion REJECTED: Candidate %s v%s (MAE=%.4f) improved over Production v%s (MAE=%.4f) "
                    "by only %.2f%%, failing the %.1f%% hurdle rate (hurdle MAE=%.4f). Moved to Staging.",
                    model_name,
                    candidate_version_num,
                    candidate_mae,
                    prod_version_num,
                    prod_mae,
                    actual_improvement_pct * 100.0,
                    hurdle_pct * 100.0,
                    hurdle_mae,
                )
                outcome["reason"] = (
                    f"Candidate v{candidate_version_num} improved by {actual_improvement_pct * 100.0:.2f}%, "
                    f"failing the {hurdle_pct * 100.0:.1f}% hurdle rate over Production v{prod_version_num} (MAE {prod_mae:.4f})"
                )

        return outcome

    def _promote_to_production(self, model_name: str, version: str) -> None:
        """Transition model version to Production and set champion alias."""
        try:
            self.client.transition_model_version_stage(
                name=model_name,
                version=version,
                stage="Production",
                archive_existing_versions=True,
            )
        except Exception as trans_err:
            logger.warning(
                "Stage transition to 'Production' failed for %s v%s: %s",
                model_name,
                version,
                trans_err,
            )

        # Set champion alias if supported by registry
        try:
            self.client.set_registered_model_alias(
                name=model_name,
                alias="champion",
                version=version,
            )
        except Exception as alias_err:
            logger.debug(
                "Setting 'champion' alias on %s v%s failed (non-fatal): %s",
                model_name,
                version,
                alias_err,
            )

    def _transition_to_staging(self, model_name: str, version: str) -> None:
        """Transition candidate model version to Staging stage."""
        try:
            self.client.transition_model_version_stage(
                name=model_name,
                version=version,
                stage="Staging",
                archive_existing_versions=False,
            )
        except Exception as trans_err:
            logger.warning(
                "Stage transition to 'Staging' failed for %s v%s: %s",
                model_name,
                version,
                trans_err,
            )


def promote_model_to_production(
    client: MlflowClient,
    model_name: str,
    candidate_run_id: str,
    candidate_mae: float,
    baseline_mae: float,
    min_improvement_pct: float = DEFAULT_MIN_IMPROVEMENT_PCT,
) -> Dict[str, Any]:
    """Backward-compatible helper invoking ModelPromotionGate."""
    gate = ModelPromotionGate(client=client, min_improvement_pct=min_improvement_pct)
    return gate.evaluate_and_promote(
        model_name=model_name,
        candidate_run_id=candidate_run_id,
        candidate_mae=candidate_mae,
        baseline_mae=baseline_mae,
        min_improvement_pct=min_improvement_pct,
    )
