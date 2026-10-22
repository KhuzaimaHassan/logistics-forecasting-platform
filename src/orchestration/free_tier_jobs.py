"""Runner-executed Prefect jobs anchored to observed historical sample dates."""

import argparse
import hashlib
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

from prefect import flow
from sqlalchemy import text

from src.common.db import get_engine
from src.orchestration.flows.free_tier_pipeline import CURSOR_KEY, require_capacity


def observation_window(engine):
    with engine.connect() as connection:
        value = connection.execute(
            text(
                "SELECT metadata_json FROM warehouse.pipeline_cursors WHERE cursor_key=:key"
            ),
            {"key": CURSOR_KEY},
        ).scalar_one()
    metadata = json.loads(value)
    if metadata.get("feature_checkpoint_index") != metadata.get("next_index"):
        raise RuntimeError(
            "Ingestion checkpoint is ahead of materialized features; rerun the pipeline before training/monitoring"
        )
    start, end = (
        datetime.fromisoformat(metadata[key])
        for key in ("observation_start", "observation_end")
    )
    if end - start < timedelta(days=20):
        raise RuntimeError(
            "At least 20 days of authentic source coverage required; finish sample bootstrap first"
        )
    return start + timedelta(days=8), end - timedelta(hours=1), end - timedelta(days=7)


def runtime_code_digest(root=None):
    """Invalidate on runtime/dependency changes, not documentation or Git ancestry."""
    root = Path(root) if root is not None else Path(__file__).resolve().parents[2]
    files = [root / "pyproject.toml", root / "uv.lock"]
    files.extend(sorted((root / "src").rglob("*.py")))
    files.extend(sorted((root / "src" / "features").glob("*.yaml")))
    digest = hashlib.sha256()
    for path in files:
        # Missing deployment inputs must fail rather than reuse a stale result.
        digest.update(path.relative_to(root).as_posix().encode())
        digest.update(b"\0")
        digest.update(path.read_bytes().replace(b"\r\n", b"\n"))
        digest.update(b"\0")
    return digest.hexdigest()


def job_fingerprint(engine, model_version=""):
    with engine.connect() as connection:
        value = connection.execute(
            text(
                "SELECT metadata_json FROM warehouse.pipeline_cursors WHERE cursor_key=:key"
            ),
            {"key": CURSOR_KEY},
        ).scalar_one()
    metadata = json.loads(value)
    payload = [
        metadata.get("source_sha256"),
        metadata.get("next_index"),
        runtime_code_digest(),
        model_version,
    ]
    return hashlib.sha256(json.dumps(payload).encode()).hexdigest(), metadata


def record_fingerprint(engine, name, fingerprint):
    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE warehouse.pipeline_cursors SET metadata_json=(metadata_json::jsonb || CAST(:metadata AS jsonb))::text WHERE cursor_key=:key"
            ),
            {"key": CURSOR_KEY, "metadata": json.dumps({name: fingerprint})},
        )


@flow(name="free-tier-historical-model-training")
def train():
    from src.features.config import get_feature_store
    from src.serving.model_loader import ModelLoaderService
    from src.training.pipeline import run_training_pipeline

    engine = get_engine()
    require_capacity(engine)
    fingerprint, metadata = job_fingerprint(engine)
    if metadata.get("training_fingerprint") == fingerprint:
        return {
            "status": "skipped",
            "reason": "same source sample and training code already evaluated",
        }
    start, end, split = observation_window(engine)
    result = run_training_pipeline(
        start,
        end,
        split,
        store=get_feature_store(),
        engine=engine,
        demand_params={"n_jobs": 2, "n_estimators": 150, "num_leaves": 31},
        duration_params={"n_jobs": 2, "n_estimators": 150, "num_leaves": 31},
        backup_to_r2=False,
    )
    # Do not report availability when the promotion gate rejected both candidates.
    loader = ModelLoaderService()
    loader.load_all()
    record_fingerprint(engine, "training_fingerprint", fingerprint)
    result["evaluation_mode"] = "historical_sample_holdout"
    return result


@flow(name="free-tier-historical-drift-monitoring")
def monitor():
    from src.features.config import get_feature_store
    from src.monitoring.service import fetch_mlflow_production_baseline
    from src.orchestration.flows.monitoring_flow import run_monitoring_lifecycle
    from src.serving.model_loader import ModelLoaderService
    from src.training.dataset import generate_demand_training_dataset
    from src.training.train_demand import prepare_demand_features

    engine = get_engine()
    require_capacity(engine)
    start, end, split = observation_window(engine)
    loader = ModelLoaderService()
    model = loader.load_demand_model()
    fingerprint, metadata = job_fingerprint(engine, model.version)
    if metadata.get("monitoring_fingerprint") == fingerprint:
        return {
            "status": "skipped",
            "reason": "same observed windows and champion already monitored",
        }
    dataset = generate_demand_training_dataset(
        store=get_feature_store(), engine=engine, start_time=start, end_time=end
    )
    if dataset.empty:
        raise RuntimeError(
            "No observed historical monitoring rows; synthetic references are disabled"
        )
    x, target = prepare_demand_features(dataset)
    dataset["predicted_value"] = model.predict(x).clip(min=0)
    dataset["actual_value"] = target.to_numpy()
    current = dataset[dataset.event_timestamp >= split].tail(2000).copy()
    reference = dataset[dataset.event_timestamp < split].tail(2000).copy()
    if len(current) < 50 or len(reference) < 50:
        raise RuntimeError("At least 50 real rows per monitoring window are required")
    baseline = fetch_mlflow_production_baseline()
    result = run_monitoring_lifecycle(
        engine=engine,
        current_df_override=current,
        reference_df_override=reference,
        baseline_mae_override=baseline,
        auto_retrain=False,
    )
    with engine.begin() as connection:
        for report_id in result["report_ids"].values():
            connection.execute(
                text(
                    "UPDATE warehouse.monitoring_reports SET summary_json = (summary_json::jsonb || CAST(:metadata AS jsonb))::text WHERE report_id=:id"
                ),
                {
                    "id": report_id,
                    "metadata": json.dumps(
                        {
                            "evaluation_mode": "historical_sample_backtest",
                            "observation_start": start.isoformat(),
                            "observation_end": end.isoformat(),
                            "model_version": model.version,
                        }
                    ),
                },
            )
    result["evaluation_mode"] = "historical_sample_backtest; not live prediction drift"
    record_fingerprint(engine, "monitoring_fingerprint", fingerprint)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("job", choices=["train", "monitor"])
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    training_logger = logging.getLogger("src.training")
    training_logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(name)s %(message)s"))
    training_logger.addHandler(handler)
    training_logger.propagate = False
    output = json.dumps(
        train() if args.job == "train" else monitor(), default=str, sort_keys=True
    )
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output + "\n", encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
