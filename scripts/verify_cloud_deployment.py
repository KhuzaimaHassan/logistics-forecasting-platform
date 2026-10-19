"""Opt-in real cloud checks. Missing credentials are NOT VERIFIED, never passed.

Output includes only sanitized evidence, resource IDs and public response fields.
No infrastructure is provisioned, deleted or upgraded. A small verification run
and MLflow artifact are intentionally retained as evidence.
"""

import argparse
import hashlib
import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import requests


def run_checks(only=None):
    results = {}

    def check(name, required, action):
        if only is not None and name not in only:
            return
        missing = [value for value in required if not os.getenv(value)]
        if missing:
            results[name] = {
                "status": "NOT VERIFIED",
                "reason": "BLOCKED: missing " + ", ".join(missing),
            }
            return
        try:
            results[name] = {"status": "VERIFIED", "evidence": action()}
        except Exception as exc:
            # Provider errors can embed URLs/passwords. Record type only.
            results[name] = {"status": "FAILED", "reason": type(exc).__name__}

    def postgres():
        from sqlalchemy import text

        from src.common.db import get_engine
        from src.orchestration.flows.free_tier_pipeline import require_capacity

        engine = get_engine()
        require_capacity(engine)
        run_id = "verification-" + uuid.uuid4().hex
        with engine.begin() as conn:
            version = conn.execute(text("SELECT version()")).scalar_one()
            assert "PostgreSQL" in version
            conn.execute(
                text(
                    "INSERT INTO warehouse.pipeline_runs(run_id,job_name,status,started_at,finished_at,triggered_by) VALUES (:id,'cloud-verification','completed',now(),now(),'manual_verification')"
                ),
                {"id": run_id},
            )
            assert (
                conn.execute(
                    text("SELECT status FROM warehouse.pipeline_runs WHERE run_id=:id"),
                    {"id": run_id},
                ).scalar_one()
                == "completed"
            )
        return {"engine": "PostgreSQL", "run_id": run_id, "write_read": True}

    def kafka():
        from kafka.structs import TopicPartition

        from src.common.db import get_engine
        from src.common.kafka_utils import (
            TOPIC_TRIP_EVENTS,
            get_kafka_consumer,
            get_kafka_producer,
        )
        from src.orchestration.flows.free_tier_pipeline import (
            download_source,
            existing_ids,
            sampled_events,
        )

        with TemporaryDirectory() as directory:
            path = Path(directory) / "authentic.parquet"
            download_source(path)
            events, _ = sampled_events(path)
        engine = get_engine()
        stored = existing_ids(engine, {event["trip_id"] for event in events})
        event = next(event for event in events if event["trip_id"] in stored)
        nonce = uuid.uuid4().hex
        event = {**event, "verification_id": nonce}
        consumer = get_kafka_consumer(group_id="cloud-verify-" + nonce)
        producer = get_kafka_producer()
        try:
            partitions = consumer.partitions_for_topic(TOPIC_TRIP_EVENTS)
            assert partitions
            assigned = [
                TopicPartition(TOPIC_TRIP_EVENTS, value) for value in partitions
            ]
            consumer.assign(assigned)
            for partition, offset in consumer.end_offsets(assigned).items():
                consumer.seek(partition, offset)
            ack = producer.send(
                TOPIC_TRIP_EVENTS, key=str(event["trip_id"]), value=event
            ).get(timeout=30)
            deadline = time.monotonic() + 45
            while time.monotonic() < deadline:
                for batch in consumer.poll(timeout_ms=1000).values():
                    for message in batch:
                        if message.value.get("verification_id") == nonce:
                            assert message.value["trip_id"] == event["trip_id"]
                            return {
                                "topic": message.topic,
                                "partition": message.partition,
                                "produced_offset": ack.offset,
                                "consumed_offset": message.offset,
                                "authentic_trip_id": event["trip_id"],
                            }
            raise TimeoutError("Acknowledged event was not consumed")
        finally:
            consumer.close()
            producer.close()

    def feast():
        from sqlalchemy import text

        from src.common.db import get_engine
        from src.features.client import FeastOnlineClient
        from src.orchestration.flows.free_tier_pipeline import reconcile

        engine = get_engine()
        summary = reconcile(engine)
        with engine.connect() as conn:
            zone, expected = conn.execute(
                text(
                    "SELECT zone_id, pickup_count_last_1h FROM warehouse.zone_demand_features_hourly ORDER BY pickup_datetime DESC, zone_id LIMIT 1"
                )
            ).one()
        feature = FeastOnlineClient().get_zone_demand_features(
            [zone], use_push_features=False
        )[0]
        assert feature.cache_hit and feature.pickup_count_last_1h == expected
        return {
            "zone_id": zone,
            "observed_pickups_last_hour": expected,
            "online_matches_postgres": True,
            "observation_end": summary["observation_end"],
        }

    def mlflow():
        import mlflow
        from mlflow.artifacts import download_artifacts

        from src.common.mlflow_utils import setup_mlflow

        setup_mlflow()
        mlflow.set_experiment("cloud-deployment-verification")
        with TemporaryDirectory() as directory:
            path = Path(directory) / "evidence.json"
            path.write_text(
                json.dumps(
                    {
                        "check": "real_cloud_artifact_roundtrip",
                        "nonce": uuid.uuid4().hex,
                    }
                )
            )
            expected = hashlib.sha256(path.read_bytes()).hexdigest()
            with mlflow.start_run(
                run_name="managed-tracking-artifact-roundtrip"
            ) as run:
                mlflow.log_metric("verification_metric", 1.0)
                mlflow.log_artifact(str(path), "verification")
            client = mlflow.tracking.MlflowClient()
            assert (
                client.get_run(run.info.run_id).data.metrics["verification_metric"]
                == 1.0
            )
            downloaded = download_artifacts(
                artifact_uri=f"runs:/{run.info.run_id}/verification/evidence.json",
                dst_path=str(Path(directory) / "readback"),
            )
            assert hashlib.sha256(Path(downloaded).read_bytes()).hexdigest() == expected
            client.search_registered_models(max_results=1)
            return {
                "run_id": run.info.run_id,
                "metric_readback": 1.0,
                "artifact_sha256": expected,
                "registry_list_access": True,
            }

    def scheduled():
        from sqlalchemy import text

        from src.common.db import get_engine

        with get_engine().connect() as conn:
            row = conn.execute(
                text(
                    "SELECT run_id, records_processed FROM warehouse.pipeline_runs WHERE job_name='free-tier-scheduled-kafka-pipeline' AND status='completed' ORDER BY finished_at DESC LIMIT 1"
                )
            ).first()
            assert row
        return {
            "completed_run_id": row[0],
            "records_processed": row[1],
            "scheduler_trigger": "Inspect corresponding GitHub Actions run; DB record alone does not prove cron execution",
        }

    def api():
        base = os.environ["FASTAPI_URL"].rstrip("/")
        health = requests.get(base + "/health/ready", timeout=(10, 120))
        health.raise_for_status()
        data = health.json()
        assert data["status"] == "ok" and all(
            not model["is_fallback"] for model in data["models"].values()
        )
        zones = requests.get(base + "/zones", timeout=30).json()["zones"]
        assert zones
        observed_zone = next(
            zone for zone in zones if zone.get("has_observed_features", True)
        )
        response = requests.get(
            base + f"/predict/demand/{observed_zone['zone_id']}",
            params={"horizon_minutes": 60},
            timeout=60,
        )
        response.raise_for_status()
        prediction = response.json()
        assert (
            prediction["status"] == "ok"
            and prediction["model_version"] != "baseline-fallback"
        )
        state = requests.get(base + "/deployment/status", timeout=30).json()
        corridors = state["observations"]["available_corridors"]
        assert corridors
        origin, dest = corridors[0].split("_")
        eta = requests.get(
            base + "/predict/eta", params={"origin": origin, "dest": dest}, timeout=60
        )
        eta.raise_for_status()
        assert eta.json()["status"] == "ok"
        return {
            "zone_id": prediction["zone_id"],
            "predicted_pickups": prediction["predicted_pickups"],
            "model_version": prediction["model_version"],
            "eta_seconds": eta.json()["predicted_duration_seconds"],
        }

    def ui_http():
        response = requests.get(os.environ["STREAMLIT_URL"], timeout=(10, 120))
        response.raise_for_status()
        assert "streamlit" in response.text.lower()
        return {
            "http_status": response.status_code,
            "scope": "HTTP shell only; visual prediction rendering still requires browser verification",
        }

    def agent():
        response = requests.post(
            os.environ["FASTAPI_URL"].rstrip("/") + "/agent/chat",
            json={
                "query": "Summarize recent pipeline status using the pipeline status tool."
            },
            headers={"X-Agent-Key": os.environ["AGENT_API_KEY"]},
            timeout=90,
        )
        response.raise_for_status()
        data = response.json()
        assert data["provider"] in {"groq", "gemini"} and data["status"] == "success"
        assert "query_pipeline_status" in data["tools_used"]
        return {
            "provider": data["provider"],
            "model_name": data["model_name"],
            "tools_used": data["tools_used"],
        }

    def monitoring():
        from sqlalchemy import text

        from src.common.db import get_engine

        with get_engine().connect() as conn:
            row = conn.execute(
                text(
                    "SELECT r.report_id,r.report_type,r.summary_json,length(h.html_content) FROM warehouse.monitoring_reports r JOIN warehouse.monitoring_html h USING(report_id) ORDER BY r.generated_at DESC LIMIT 1"
                )
            ).first()
            assert row and row[3] > 100
            summary = json.loads(row[2])
            assert summary.get("evaluation_mode") == "historical_sample_backtest"
        return {
            "report_id": row[0],
            "report_type": row[1],
            "html_characters": row[3],
            "evaluation_mode": summary["evaluation_mode"],
        }

    check("postgresql", ["DATABASE_URL"], postgres)
    check("kafka", ["REDPANDA_BROKER", "DATABASE_URL"], kafka)
    check("feast_valkey", ["DATABASE_URL", "REDIS_URL"], feast)
    check(
        "mlflow",
        ["MLFLOW_TRACKING_URI", "MLFLOW_TRACKING_USERNAME", "MLFLOW_TRACKING_PASSWORD"],
        mlflow,
    )
    check("pipeline_output", ["DATABASE_URL"], scheduled)
    check("fastapi_real_model", ["FASTAPI_URL"], api)
    check("streamlit_http", ["STREAMLIT_URL"], ui_http)
    check("llm_agent", ["FASTAPI_URL", "AGENT_API_KEY"], agent)
    check("monitoring", ["DATABASE_URL"], monitoring)
    results["streamlit_visual_predictions"] = {
        "status": "NOT VERIFIED",
        "reason": "Requires a browser session displaying actual API predictions",
    }
    results["cold_start_restart"] = {
        "status": "NOT VERIFIED",
        "reason": "Requires real service restart/sleep and repeat predictions",
    }
    results["cron_trigger"] = {
        "status": "NOT VERIFIED",
        "reason": "Requires successful GitHub scheduled event evidence",
    }
    return {"generated_at": datetime.now(timezone.utc).isoformat(), "checks": results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path, default=Path("artifacts/cloud-verification.json")
    )
    parser.add_argument("--only-mlflow", action="store_true")
    args = parser.parse_args()
    result = run_checks(only={"mlflow"} if args.only_mlflow else None)
    manual = {"streamlit_visual_predictions", "cold_start_restart", "cron_trigger"}
    automatic_ok = all(
        value["status"] == "VERIFIED"
        for key, value in result["checks"].items()
        if key not in manual
    )
    result["verification_scope"] = "mlflow_only" if args.only_mlflow else "full"
    result["selected_checks_complete"] = automatic_ok
    result["automated_checks_complete"] = automatic_ok and not args.only_mlflow
    result["manual_acceptance_complete"] = False
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, default=str))
    print(json.dumps(result, indent=2, default=str))
    raise SystemExit(0 if automatic_ok else 1)


if __name__ == "__main__":
    main()
