"""Unit and endpoint tests for FastAPI online serving layer.

Tests all contracts in docs/API.md and ADR-020 using FastAPI TestClient:
- GET  /health (dependencies, model metadata)
- GET  /predict/demand/{zone_id} (valid, boundary, degraded fallback, 404 for <=0, >263, 264, 265)
- POST /predict/demand/batch (explicit zones, default all 263 zones, 400 for invalid zones)
- GET  /predict/eta (valid corridor, boundary, 404 for <=0, >263, 264, 265)
- POST /predict/eta/batch (multiple corridors, 400 for invalid zones)
- GET  /features/{entity_type}/{entity_id} (zone, corridor, 400/404 handling)
- GET  /pipeline/status (database query, graceful empty handling)
"""

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from src.features.client import (
    CorridorDurationOnlineFeatures,
    ZoneDemandOnlineFeatures,
)
from src.serving.app import app
from src.serving.cache import PredictionCache
from src.serving.feature_extractor import (
    build_corridor_feature_df,
    build_demand_feature_df,
)
from src.serving.model_loader import (
    DEMAND_MODEL_NAME,
    DURATION_MODEL_NAME,
    LoadedModelInfo,
    ModelLoaderService,
)
from src.training.baseline import (
    CorridorDurationBaseline,
    DemandSeasonalNaiveBaseline,
)


@pytest.fixture(autouse=True)
def clear_serving_cache():
    """Ensure prediction cache is empty before every test run."""
    if hasattr(app.state, "cache") and app.state.cache is not None:
        app.state.cache.clear()


@pytest.fixture(scope="module")
def mock_serving_environment():
    """Configure app.state with deterministic mock loader and Feast client for fast tests."""
    # 1. Mock baseline models
    demand_baseline = DemandSeasonalNaiveBaseline()
    duration_baseline = CorridorDurationBaseline()

    demand_info = LoadedModelInfo(
        model_name=DEMAND_MODEL_NAME,
        version="test-v1",
        stage="Production",
        run_id="test-run-demand",
        loaded_at="2026-09-09T12:00:00Z",
        status="production",
        is_fallback=False,
        model=demand_baseline,
        metadata={"source": "test"},
    )
    duration_info = LoadedModelInfo(
        model_name=DURATION_MODEL_NAME,
        version="test-v1",
        stage="Production",
        run_id="test-run-duration",
        loaded_at="2026-09-09T12:00:00Z",
        status="production",
        is_fallback=False,
        model=duration_baseline,
        metadata={"source": "test"},
    )

    mock_loader = MagicMock(spec=ModelLoaderService)
    mock_loader.get_model.side_effect = lambda name: (
        demand_info if name == DEMAND_MODEL_NAME else duration_info
    )
    mock_loader.get_health_metadata.return_value = {
        "all_models_loaded": True,
        "has_fallback_models": False,
        "fallback_active": False,
        "models": {
            DEMAND_MODEL_NAME: {
                "name": DEMAND_MODEL_NAME,
                "version": "test-v1",
                "stage": "Production",
                "run_id": "test-run-demand",
                "loaded_at": "2026-09-09T12:00:00Z",
                "status": "production",
                "is_fallback": False,
            },
            DURATION_MODEL_NAME: {
                "name": DURATION_MODEL_NAME,
                "version": "test-v1",
                "stage": "Production",
                "run_id": "test-run-duration",
                "loaded_at": "2026-09-09T12:00:00Z",
                "status": "production",
                "is_fallback": False,
            },
        },
    }

    # 2. Mock Feast client
    mock_feast = MagicMock()

    def mock_get_zone_features(zone_ids, use_push_features=True):
        results = []
        for zid in zone_ids:
            # Zone 161 has simulated live features (cache_hit=True)
            # Other zones simulate cold / unmaterialized (cache_hit=False)
            is_hit = zid == 161
            results.append(
                ZoneDemandOnlineFeatures(
                    zone_id=zid,
                    pickup_count_last_15m=12 if is_hit else None,
                    pickup_count_last_1h=45 if is_hit else None,
                    pickup_count_last_24h=850 if is_hit else None,
                    pickup_count_same_hour_last_week=40 if is_hit else None,
                    hour_of_day=14 if is_hit else None,
                    day_of_week=2 if is_hit else None,
                    is_weekend=False if is_hit else None,
                    is_holiday=False if is_hit else None,
                    cache_hit=is_hit,
                )
            )
        return results

    def mock_get_corridor_features(corridor_ids, use_push_features=True):
        results = []
        for cid in corridor_ids:
            is_hit = cid == "161_236"
            results.append(
                CorridorDurationOnlineFeatures(
                    corridor_id=cid,
                    avg_duration_last_15m=870.0 if is_hit else None,
                    avg_duration_last_1h=720.0 if is_hit else None,
                    distance_km=3.8 if is_hit else None,
                    origin_zone_demand_pressure=1.15 if is_hit else None,
                    cache_hit=is_hit,
                )
            )
        return results

    mock_feast.get_zone_demand_features.side_effect = mock_get_zone_features
    mock_feast.get_corridor_duration_features.side_effect = mock_get_corridor_features

    # Attach to app.state
    app.state.model_loader = mock_loader
    app.state.feast_client = mock_feast
    app.state.cache = PredictionCache(redis_url=None)

    return client


client = TestClient(app)


# ---------------------------------------------------------------------------
# Health Endpoint Tests
# ---------------------------------------------------------------------------


def test_health_endpoint(mock_serving_environment):
    """Test GET /health returns structured dependency and model metadata."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()

    assert "status" in data
    assert data["status"] in ("ok", "degraded")
    assert "dependencies" in data
    assert "database" in data["dependencies"]
    assert "redis" in data["dependencies"]
    assert "mlflow" in data["dependencies"]

    assert "models" in data
    assert "demand" in data["models"]
    assert "duration" in data["models"]
    assert data["models"]["demand"]["name"] == DEMAND_MODEL_NAME
    assert data["models"]["duration"]["name"] == DURATION_MODEL_NAME


# ---------------------------------------------------------------------------
# Demand Prediction Tests
# ---------------------------------------------------------------------------


def test_predict_demand_single_cache_hit(mock_serving_environment):
    """Test GET /predict/demand/161 with simulated online cache hit."""
    response = client.get("/predict/demand/161")
    assert response.status_code == 200
    data = response.json()

    assert data["zone_id"] == 161
    assert data["horizon_minutes"] == 15
    assert data["status"] == "ok"
    assert data["cache_hit"] is True
    assert data["predicted_pickups"] >= 0.0
    assert data["warning"] is None
    assert data["model_version"] == "test-v1"


def test_predict_demand_single_degraded_fallback(mock_serving_environment):
    """Test GET /predict/demand/236 with cache miss returns degraded_fallback and non-zero output."""
    response = client.get("/predict/demand/236")
    assert response.status_code == 200
    data = response.json()

    assert data["zone_id"] == 236
    assert data["status"] == "degraded_fallback"
    assert data["cache_hit"] is False
    assert data["predicted_pickups"] >= 0.0
    assert data["warning"] is not None
    assert "unmaterialized" in data["warning"]


@pytest.mark.parametrize("invalid_zone", [0, -1, 264, 265, 999])
def test_predict_demand_single_404_invalid_zones(
    invalid_zone, mock_serving_environment
):
    """Test GET /predict/demand/{zone} returns HTTP 404 for non-existent or placeholder zones."""
    response = client.get(f"/predict/demand/{invalid_zone}")
    assert response.status_code == 404
    data = response.json()
    assert data["error"] == "not_found"
    assert f"Zone ID {invalid_zone}" in data["detail"]


def test_predict_demand_batch_explicit_zones(mock_serving_environment):
    """Test POST /predict/demand/batch with explicit list of zones."""
    payload = {"zone_ids": [161, 236], "horizon_minutes": 15}
    response = client.post("/predict/demand/batch", json=payload)
    assert response.status_code == 200
    data = response.json()

    assert data["prediction_count"] == 2
    assert len(data["predictions"]) == 2
    assert data["predictions"][0]["zone_id"] == 161
    assert data["predictions"][0]["status"] == "ok"
    assert data["predictions"][1]["zone_id"] == 236
    assert data["predictions"][1]["status"] == "degraded_fallback"

    # Assert distinct inputs yield distinct batch predictions (no broadcast / vector reuse)
    pred_0 = data["predictions"][0]["predicted_pickups"]
    pred_1 = data["predictions"][1]["predicted_pickups"]
    assert (
        pred_0 != pred_1
    ), f"Batch predictions should differ for distinct zone features: {pred_0} == {pred_1}"


def test_demand_batch_sensitivity_to_varied_features(mock_serving_environment):
    """Test that varied synthetic feature inputs in a batch produce strictly distinct predictions."""
    feat_high = ZoneDemandOnlineFeatures(
        zone_id=161,
        pickup_count_same_hour_last_week=50,
        cache_hit=True,
    )
    feat_low = ZoneDemandOnlineFeatures(
        zone_id=236,
        pickup_count_same_hour_last_week=10,
        cache_hit=True,
    )
    batch_df = build_demand_feature_df([feat_high, feat_low])
    loader = app.state.model_loader
    model = loader.get_model(DEMAND_MODEL_NAME)
    preds = model.predict(batch_df)
    assert preds[0] != preds[1]
    assert preds[0] > preds[1]


def test_predict_demand_batch_all_active_zones_default(
    mock_serving_environment,
):
    """Test POST /predict/demand/batch with empty body defaults to all 263 active zones."""
    response = client.post("/predict/demand/batch", json={})
    assert response.status_code == 200
    data = response.json()

    assert data["prediction_count"] == 263
    assert len(data["predictions"]) == 263
    assert data["predictions"][0]["zone_id"] == 1
    assert data["predictions"][-1]["zone_id"] == 263


@pytest.mark.parametrize(
    "bad_payload",
    [
        {"zone_ids": [161, 264]},
        {"zone_ids": [0, 161]},
        {"zone_ids": [161, 999]},
        {"zone_ids": [265]},
    ],
)
def test_predict_demand_batch_400_invalid_zones(bad_payload, mock_serving_environment):
    """Test POST /predict/demand/batch returns HTTP 400 for unservable zone elements."""
    response = client.post("/predict/demand/batch", json=bad_payload)
    assert response.status_code == 400
    data = response.json()
    assert data["error"] == "bad_request"
    assert "Invalid zone ID" in data["detail"]


# ---------------------------------------------------------------------------
# ETA Prediction Tests
# ---------------------------------------------------------------------------


def test_predict_eta_single_cache_hit(mock_serving_environment):
    """Test GET /predict/eta for valid corridor 161->236."""
    response = client.get("/predict/eta?origin=161&dest=236")
    assert response.status_code == 200
    data = response.json()

    assert data["origin_zone_id"] == 161
    assert data["dest_zone_id"] == 236
    assert data["corridor_id"] == "161_236"
    assert data["status"] == "ok"
    assert data["cache_hit"] is True
    assert data["predicted_duration_seconds"] >= 60.0
    assert data["predicted_duration_minutes"] >= 1.0


def test_predict_eta_single_degraded_fallback(mock_serving_environment):
    """Test GET /predict/eta for unmaterialized corridor returns degraded_fallback."""
    response = client.get("/predict/eta?origin=100&dest=200")
    assert response.status_code == 200
    data = response.json()

    assert data["origin_zone_id"] == 100
    assert data["dest_zone_id"] == 200
    assert data["status"] == "degraded_fallback"
    assert data["cache_hit"] is False
    assert data["predicted_duration_seconds"] >= 60.0
    assert data["warning"] is not None


@pytest.mark.parametrize(
    "orig,dest",
    [
        (0, 236),
        (161, 0),
        (264, 236),
        (161, 264),
        (265, 100),
        (100, 265),
        (999, 100),
    ],
)
def test_predict_eta_single_404_invalid_corridors(orig, dest, mock_serving_environment):
    """Test GET /predict/eta returns HTTP 404 when origin or destination is unservable."""
    response = client.get(f"/predict/eta?origin={orig}&dest={dest}")
    assert response.status_code == 404
    data = response.json()
    assert data["error"] == "not_found"


def test_predict_eta_batch_ok(mock_serving_environment):
    """Test POST /predict/eta/batch with valid corridor pairs."""
    payload = {
        "corridors": [
            {"origin_zone_id": 161, "dest_zone_id": 236},
            {"origin_zone_id": 100, "dest_zone_id": 200},
        ]
    }
    response = client.post("/predict/eta/batch", json=payload)
    assert response.status_code == 200
    data = response.json()

    assert data["prediction_count"] == 2
    assert len(data["predictions"]) == 2
    assert data["predictions"][0]["corridor_id"] == "161_236"
    assert data["predictions"][0]["status"] == "ok"
    assert data["predictions"][1]["corridor_id"] == "100_200"
    assert data["predictions"][1]["status"] == "degraded_fallback"

    # Assert distinct inputs yield distinct batch predictions (no broadcast / vector reuse)
    eta_0 = data["predictions"][0]["predicted_duration_seconds"]
    eta_1 = data["predictions"][1]["predicted_duration_seconds"]
    assert (
        eta_0 != eta_1
    ), f"Batch ETA predictions should differ for distinct corridor features: {eta_0} == {eta_1}"


def test_eta_batch_sensitivity_to_varied_features(mock_serving_environment):
    """Test that varied synthetic corridor inputs in a batch produce strictly distinct ETA predictions."""
    feat_long = CorridorDurationOnlineFeatures(
        corridor_id="161_236",
        avg_duration_last_1h=1200.0,
        distance_km=8.5,
        cache_hit=True,
    )
    feat_short = CorridorDurationOnlineFeatures(
        corridor_id="236_142",
        avg_duration_last_1h=300.0,
        distance_km=1.2,
        cache_hit=True,
    )
    batch_df = build_corridor_feature_df(
        [feat_long, feat_short],
        origin_dest_pairs=[(161, 236), (236, 142)],
    )
    loader = app.state.model_loader
    model = loader.get_model(DURATION_MODEL_NAME)
    preds = model.predict(batch_df)
    assert preds[0] != preds[1]
    assert preds[0] > preds[1]


@pytest.mark.parametrize(
    "corridors_payload",
    [
        {"corridors": [{"origin_zone_id": 161, "dest_zone_id": 264}]},
        {"corridors": [{"origin_zone_id": 0, "dest_zone_id": 236}]},
        {"corridors": [{"origin_zone_id": 265, "dest_zone_id": 100}]},
    ],
)
def test_predict_eta_batch_400_invalid_corridors(
    corridors_payload, mock_serving_environment
):
    """Test POST /predict/eta/batch returns HTTP 400 when corridor contains invalid zone."""
    response = client.post("/predict/eta/batch", json=corridors_payload)
    assert response.status_code == 400
    data = response.json()
    assert data["error"] == "bad_request"


# ---------------------------------------------------------------------------
# Feature Lookup Endpoint Tests
# ---------------------------------------------------------------------------


def test_features_lookup_zone_ok(mock_serving_environment):
    """Test GET /features/zone/161 returns feature dictionary."""
    response = client.get("/features/zone/161")
    assert response.status_code == 200
    data = response.json()

    assert data["entity_type"] == "zone"
    assert data["entity_id"] == 161
    assert data["cache_hit"] is True
    assert "pickup_count_last_1h" in data["features"]


def test_features_lookup_corridor_ok(mock_serving_environment):
    """Test GET /features/corridor/161_236 returns feature dictionary."""
    response = client.get("/features/corridor/161_236")
    assert response.status_code == 200
    data = response.json()

    assert data["entity_type"] == "corridor"
    assert data["entity_id"] == "161_236"
    assert data["cache_hit"] is True
    assert "distance_km" in data["features"]


def test_features_lookup_zone_404_placeholder(mock_serving_environment):
    """Test GET /features/zone/264 returns HTTP 404 for placeholder zone."""
    response = client.get("/features/zone/264")
    assert response.status_code == 404
    assert response.json()["error"] == "not_found"


def test_features_lookup_corridor_404(mock_serving_environment):
    """Test GET /features/corridor/161_999 returns HTTP 404."""
    response = client.get("/features/corridor/161_999")
    assert response.status_code == 404
    assert response.json()["error"] == "not_found"


def test_features_lookup_invalid_type_400(mock_serving_environment):
    """Test GET /features/invalid_type/161 returns HTTP 400."""
    response = client.get("/features/invalid_type/161")
    assert response.status_code == 400
    assert response.json()["error"] == "bad_request"


# ---------------------------------------------------------------------------
# Pipeline Status Endpoint Tests
# ---------------------------------------------------------------------------


def test_pipeline_status(mock_serving_environment):
    """Test GET /pipeline/status returns status structure."""
    response = client.get("/pipeline/status")
    assert response.status_code == 200
    data = response.json()

    assert "status" in data
    assert data["status"] in ("healthy", "degraded", "empty")
    assert "latest_runs" in data
    assert "checked_at" in data
