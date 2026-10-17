"""Manual authentic-data integration on disposable CI services, not managed cloud."""

import json
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import text

from src.common.db import get_engine
from src.orchestration.flows.free_tier_pipeline import scheduled_sample_pipeline
from src.orchestration.free_tier_jobs import monitor, train
from src.serving.app import app


def main():
    result = {
        "scope": "Authentic TLC data, real disposable CI PostgreSQL/Redis/Kafka/MLflow; no managed cloud verification"
    }
    path = Path("artifacts/free-profile-e2e.json")
    path.parent.mkdir(exist_ok=True, parents=True)
    try:
        result["bootstrap"] = scheduled_sample_pipeline(
            max_events=10000, bootstrap=True
        )
        training = train()
        result["training"] = training
        result["monitoring"] = monitor()
        with TestClient(app) as client:
            ready = client.get("/health/ready")
            assert ready.status_code == 200, ready.text
            result["health"] = ready.json()
            zones = client.get("/zones").json()["zones"]
            assert len(zones) == 263
            zone = next(
                zone["zone_id"]
                for zone in zones
                if zone.get("has_observed_features", True)
            )
            response = client.get(
                f"/predict/demand/{zone}", params={"horizon_minutes": 60}
            )
            assert response.status_code == 200, response.text
            result["demand"] = response.json()
            assert result["demand"]["status"] == "ok"
            state = client.get("/deployment/status").json()
            corridor = state["observations"]["available_corridors"][0]
            origin, dest = corridor.split("_")
            response = client.get(
                "/predict/eta", params={"origin": origin, "dest": dest}
            )
            assert response.status_code == 200, response.text
            result["eta"] = response.json()
            assert result["eta"]["status"] == "ok"
        with get_engine().connect() as connection:
            result["persisted_prediction_count"] = connection.execute(
                text("SELECT count(*) FROM warehouse.predictions")
            ).scalar_one()
            result["persisted_html_count"] = connection.execute(
                text("SELECT count(*) FROM warehouse.monitoring_html")
            ).scalar_one()
        assert result["persisted_prediction_count"] >= 2
        assert result["persisted_html_count"] >= 1
        result["status"] = "VERIFIED"
    except Exception as exc:
        result["status"] = "FAILED"
        result["error_type"] = type(exc).__name__
        raise
    finally:
        path.write_text(json.dumps(result, indent=2, default=str))
        print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
