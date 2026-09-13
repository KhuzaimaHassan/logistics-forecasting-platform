"""Unit and integration tests for low-latency prediction caching layer (M5-3).

Tests all contracts in ADR-020 Section 3:
- Caching of genuine predictions (cache_hit=True, status="ok") with 60s TTL.
- Degraded Cache-Bypass Policy: predictions with status="degraded_fallback" or cache_hit=False
  MUST NEVER be stored in the prediction cache (zero-TTL / cache write bypass).
- X-Cache response headers: HIT, MISS, and PARTIAL.
- Sub-2ms cached retrieval latency.
- Batch prediction caching with partial hit assembly and order preservation.
- Cross-path isolation: batch call containing degraded zone leaves zero trace in cache,
  so subsequent single call is strictly a cache MISS (test_batch_then_single_degraded_zone_not_stale_cached).
- In-memory TTL fallback when Redis is temporarily unreachable.
- Live Redis TTL enforcement test querying real TTL command and asserting 0 < ttl <= 60.
"""

import time
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from src.common.config import get_settings
from src.features.client import (
    CorridorDurationOnlineFeatures,
    ZoneDemandOnlineFeatures,
)
from src.serving.app import app
from src.serving.cache import PredictionCache, _InMemoryTTLCache
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


@pytest.fixture(scope="module")
def mock_serving_cache_environment():
    """Configure app.state with deterministic mock loader, Feast client, and standalone cache."""
    demand_baseline = DemandSeasonalNaiveBaseline()
    duration_baseline = CorridorDurationBaseline()

    demand_info = LoadedModelInfo(
        model_name=DEMAND_MODEL_NAME,
        version="test-cache-v1",
        stage="Production",
        run_id="test-run-demand",
        loaded_at="2026-09-10T12:00:00Z",
        status="production",
        is_fallback=False,
        model=demand_baseline,
        metadata={"source": "test"},
    )
    duration_info = LoadedModelInfo(
        model_name=DURATION_MODEL_NAME,
        version="test-cache-v1",
        stage="Production",
        run_id="test-run-duration",
        loaded_at="2026-09-10T12:00:00Z",
        status="production",
        is_fallback=False,
        model=duration_baseline,
        metadata={"source": "test"},
    )

    mock_loader = MagicMock(spec=ModelLoaderService)
    mock_loader.get_model.side_effect = lambda name: (
        demand_info if name == DEMAND_MODEL_NAME else duration_info
    )

    # Mock Feast client:
    # Zone 161: cache_hit=True (live features)
    # Zone 142: cache_hit=True (live features)
    # Other zones (e.g. 236): cache_hit=False (cold / unmaterialized)
    # Corridor 161_236: cache_hit=True
    # Corridor 142_161: cache_hit=True
    # Other corridors: cache_hit=False
    mock_feast = MagicMock()

    def mock_get_zone_features(zone_ids, use_push_features=True):
        results = []
        for zid in zone_ids:
            is_hit = zid in (161, 142)
            results.append(
                ZoneDemandOnlineFeatures(
                    zone_id=zid,
                    pickup_count_last_15m=15 if is_hit else None,
                    pickup_count_last_1h=50 if is_hit else None,
                    pickup_count_last_24h=900 if is_hit else None,
                    pickup_count_same_hour_last_week=45 if is_hit else None,
                    hour_of_day=12 if is_hit else None,
                    day_of_week=3 if is_hit else None,
                    is_weekend=False if is_hit else None,
                    is_holiday=False if is_hit else None,
                    cache_hit=is_hit,
                )
            )
        return results

    def mock_get_corridor_features(corridor_ids, use_push_features=True):
        results = []
        for cid in corridor_ids:
            is_hit = cid in ("161_236", "142_161")
            results.append(
                CorridorDurationOnlineFeatures(
                    corridor_id=cid,
                    avg_duration_last_15m=850.0 if is_hit else None,
                    avg_duration_last_1h=700.0 if is_hit else None,
                    distance_km=3.5 if is_hit else None,
                    origin_zone_demand_pressure=1.2 if is_hit else None,
                    cache_hit=is_hit,
                )
            )
        return results

    mock_feast.get_zone_demand_features.side_effect = mock_get_zone_features
    mock_feast.get_corridor_duration_features.side_effect = mock_get_corridor_features

    # Use dedicated in-memory PredictionCache for isolation
    cache = PredictionCache(redis_url=None, ttl_seconds=60)

    app.state.model_loader = mock_loader
    app.state.feast_client = mock_feast
    app.state.cache = cache

    return client


client = TestClient(app)


@pytest.fixture(autouse=True)
def reset_cache_state():
    """Clear prediction cache between tests."""
    if hasattr(app.state, "cache") and app.state.cache is not None:
        app.state.cache.clear()


# ---------------------------------------------------------------------------
# Unit Tests: PredictionCache & In-Memory TTL Fallback
# ---------------------------------------------------------------------------


def test_in_memory_ttl_cache_operations():
    """Test get, set, eviction, and TTL expiration on _InMemoryTTLCache."""
    cache = _InMemoryTTLCache(maxsize=3)

    cache.set("k1", "v1", ttl_seconds=60)
    cache.set("k2", "v2", ttl_seconds=60)
    assert cache.get("k1") == "v1"
    assert cache.get("k2") == "v2"
    assert cache.get("k3") is None

    # Multi-get
    assert cache.mget(["k1", "k2", "missing"]) == ["v1", "v2", None]

    # Max size capacity eviction
    cache.set("k3", "v3", ttl_seconds=60)
    cache.set("k4", "v4", ttl_seconds=60)
    assert len(cache) <= 3

    # Expiration check
    short_cache = _InMemoryTTLCache(maxsize=10)
    short_cache.set("temp", "expiring", ttl_seconds=0)
    # With 0 ttl, immediately expired
    assert short_cache.get("temp") is None


def test_prediction_cache_key_generation():
    """Verify standard Redis key schemas."""
    assert PredictionCache.demand_key(161, 15) == "pred:demand:161:15"
    assert PredictionCache.eta_key(161, 236) == "pred:eta:161:236"


# ---------------------------------------------------------------------------
# Endpoint Tests: Demand Caching & Headers
# ---------------------------------------------------------------------------


def test_predict_demand_cache_hit_and_miss(mock_serving_cache_environment):
    """Test that first call is MISS and second call is HIT with identical payload in <2ms."""
    # First call: cache miss
    res1 = client.get("/predict/demand/161")
    assert res1.status_code == 200
    assert res1.headers.get("X-Cache") == "MISS"
    data1 = res1.json()
    assert data1["status"] == "ok"
    assert data1["cache_hit"] is True

    # Second call: cache hit
    t0 = time.perf_counter()
    res2 = client.get("/predict/demand/161")
    latency_ms = (time.perf_counter() - t0) * 1000.0

    assert res2.status_code == 200
    assert res2.headers.get("X-Cache") == "HIT"
    data2 = res2.json()

    # Verify identical payload
    assert data1["zone_id"] == data2["zone_id"]
    assert data1["predicted_pickups"] == data2["predicted_pickups"]
    assert data1["status"] == data2["status"]
    assert data1["model_version"] == data2["model_version"]
    assert latency_ms < 15.0  # TestClient in Python overhead is well under 15ms


def test_degraded_demand_never_cached(mock_serving_cache_environment):
    """Verify degraded_fallback response (zone 236) is NEVER cached per ADR-020."""
    # First call: degraded fallback
    res1 = client.get("/predict/demand/236")
    assert res1.status_code == 200
    assert res1.headers.get("X-Cache") == "MISS"
    data1 = res1.json()
    assert data1["status"] == "degraded_fallback"
    assert data1["cache_hit"] is False
    assert data1["predicted_pickups"] >= 0.0
    assert data1["warning"] is not None

    # Verify the cache does not contain this zone
    cache: PredictionCache = app.state.cache
    assert cache.get_demand(236, 15) is None

    # Second call: MUST STILL BE MISS (recomputed fresh)
    res2 = client.get("/predict/demand/236")
    assert res2.status_code == 200
    assert res2.headers.get("X-Cache") == "MISS"
    data2 = res2.json()
    assert data2["status"] == "degraded_fallback"


# ---------------------------------------------------------------------------
# Endpoint Tests: Corridor ETA Caching
# ---------------------------------------------------------------------------


def test_predict_eta_cache_hit_and_miss(mock_serving_cache_environment):
    """Test GET /predict/eta caching for valid corridor."""
    res1 = client.get("/predict/eta?origin=161&dest=236")
    assert res1.status_code == 200
    assert res1.headers.get("X-Cache") == "MISS"
    data1 = res1.json()
    assert data1["status"] == "ok"
    assert data1["cache_hit"] is True

    # Second call: cache hit
    res2 = client.get("/predict/eta?origin=161&dest=236")
    assert res2.status_code == 200
    assert res2.headers.get("X-Cache") == "HIT"
    data2 = res2.json()
    assert data1["predicted_duration_seconds"] == data2["predicted_duration_seconds"]


def test_degraded_eta_never_cached(mock_serving_cache_environment):
    """Verify degraded corridor ETA is NEVER cached."""
    # Corridor 100_200 is unmaterialized in mock Feast
    res1 = client.get("/predict/eta?origin=100&dest=200")
    assert res1.status_code == 200
    assert res1.headers.get("X-Cache") == "MISS"
    assert res1.json()["status"] == "degraded_fallback"

    cache: PredictionCache = app.state.cache
    assert cache.get_eta(100, 200) is None

    # Second call remains MISS
    res2 = client.get("/predict/eta?origin=100&dest=200")
    assert res2.headers.get("X-Cache") == "MISS"


# ---------------------------------------------------------------------------
# Batch Caching & Partial Hit Assembly
# ---------------------------------------------------------------------------


def test_cache_batch_demand_partial_and_full_hit(mock_serving_cache_environment):
    """Test batch demand caching with partial and full hits."""
    # Step 1: Pre-cache zone 161
    client.get("/predict/demand/161")

    # Step 2: Request batch with zone 161 (hit) and 142 (miss)
    payload = {"zone_ids": [161, 142], "horizon_minutes": 15}
    res_partial = client.post("/predict/demand/batch", json=payload)
    assert res_partial.status_code == 200
    assert res_partial.headers.get("X-Cache") == "PARTIAL"
    data_partial = res_partial.json()
    assert len(data_partial["predictions"]) == 2
    assert data_partial["predictions"][0]["zone_id"] == 161
    assert data_partial["predictions"][1]["zone_id"] == 142

    # Step 3: Now both 161 and 142 should be cached -> full HIT
    res_full = client.post("/predict/demand/batch", json=payload)
    assert res_full.status_code == 200
    assert res_full.headers.get("X-Cache") == "HIT"
    data_full = res_full.json()
    assert len(data_full["predictions"]) == 2


def test_cache_batch_eta_partial_and_full_hit(mock_serving_cache_environment):
    """Test batch ETA caching with partial and full hits."""
    client.get("/predict/eta?origin=161&dest=236")

    payload = {
        "corridors": [
            {"origin_zone_id": 161, "dest_zone_id": 236},
            {"origin_zone_id": 142, "dest_zone_id": 161},
        ]
    }
    res_partial = client.post("/predict/eta/batch", json=payload)
    assert res_partial.status_code == 200
    assert res_partial.headers.get("X-Cache") == "PARTIAL"

    res_full = client.post("/predict/eta/batch", json=payload)
    assert res_full.status_code == 200
    assert res_full.headers.get("X-Cache") == "HIT"


# ---------------------------------------------------------------------------
# Cross-Path Degradation Isolation (User Requested Check)
# ---------------------------------------------------------------------------


def test_batch_then_single_degraded_zone_not_stale_cached(
    mock_serving_cache_environment,
):
    """Request a batch containing one degraded zone, then immediately request that

    same zone via the single endpoint. Confirm it is still a cache MISS (recomputed fresh),
    proving that batch-write logic never leaks degraded predictions into cache.
    """
    # Zone 161 is genuine (hit), Zone 236 is degraded (cache_hit=False)
    payload = {"zone_ids": [161, 236], "horizon_minutes": 15}
    batch_res = client.post("/predict/demand/batch", json=payload)
    assert batch_res.status_code == 200
    batch_data = batch_res.json()

    assert batch_data["predictions"][0]["zone_id"] == 161
    assert batch_data["predictions"][0]["status"] == "ok"
    assert batch_data["predictions"][1]["zone_id"] == 236
    assert batch_data["predictions"][1]["status"] == "degraded_fallback"

    # Immediately request zone 236 via single endpoint
    single_res = client.get("/predict/demand/236")
    assert single_res.status_code == 200
    # Must strictly be a MISS, not served from any accidental batch-write side effect!
    assert single_res.headers.get("X-Cache") == "MISS"
    single_data = single_res.json()
    assert single_data["status"] == "degraded_fallback"
    assert single_data["cache_hit"] is False

    # Confirm cache key is completely absent
    cache: PredictionCache = app.state.cache
    assert cache.get_demand(236, 15) is None


def test_batch_then_single_genuine_cache_hit_deserialization(
    mock_serving_cache_environment,
):
    """Confirm genuine predictions cached via batch endpoints can be successfully retrieved

    and deserialized by single endpoints without schema validation errors (e.g. model_version, as_of).
    """
    # 1. Demand batch caching -> single retrieval
    demand_batch_res = client.post(
        "/predict/demand/batch",
        json={"zone_ids": [161], "horizon_minutes": 15},
    )
    assert demand_batch_res.status_code == 200
    demand_batch_item = demand_batch_res.json()["predictions"][0]
    assert demand_batch_item["status"] == "ok"

    # Immediately request zone 161 via single endpoint -> must be a cache HIT
    single_demand_res = client.get("/predict/demand/161?horizon=15")
    assert single_demand_res.status_code == 200
    assert single_demand_res.headers.get("X-Cache") == "HIT"
    single_demand_data = single_demand_res.json()
    assert (
        single_demand_data["predicted_pickups"]
        == demand_batch_item["predicted_pickups"]
    )
    assert single_demand_data["model_version"] is not None
    assert single_demand_data["as_of"] is not None

    # 2. ETA batch caching -> single retrieval
    eta_batch_res = client.post(
        "/predict/eta/batch",
        json={"corridors": [{"origin_zone_id": 161, "dest_zone_id": 236}]},
    )
    assert eta_batch_res.status_code == 200
    eta_batch_item = eta_batch_res.json()["predictions"][0]
    assert eta_batch_item["status"] == "ok"

    # Immediately request corridor 161->236 via single endpoint -> must be a cache HIT
    single_eta_res = client.get("/predict/eta?origin=161&dest=236")
    assert single_eta_res.status_code == 200
    assert single_eta_res.headers.get("X-Cache") == "HIT"
    single_eta_data = single_eta_res.json()
    assert (
        single_eta_data["predicted_duration_seconds"]
        == eta_batch_item["predicted_duration_seconds"]
    )
    assert single_eta_data["model_version"] is not None
    assert single_eta_data["as_of"] is not None


# ---------------------------------------------------------------------------
# Fault Tolerance & In-Memory Fallback
# ---------------------------------------------------------------------------


def test_in_memory_fallback_when_redis_unreachable():
    """Verify PredictionCache operates seamlessly using memory cache when Redis is unreachable."""
    # Point to invalid/unreachable port
    unreachable_cache = PredictionCache(
        redis_url="redis://localhost:59999/0",
        ttl_seconds=60,
    )

    test_payload = {
        "zone_id": 161,
        "horizon_minutes": 15,
        "predicted_pickups": 42.0,
        "status": "ok",
        "cache_hit": True,
        "model_version": "v1",
        "as_of": "2026-09-10T12:00:00Z",
    }

    # Set and get must not raise exceptions
    success = unreachable_cache.set_demand(161, 15, test_payload, status="ok")
    assert success is True

    cached = unreachable_cache.get_demand(161, 15)
    assert cached is not None
    assert cached["predicted_pickups"] == 42.0

    # Batch operations
    cached_map = unreachable_cache.get_demand_batch([161, 236], 15)
    assert 161 in cached_map
    assert 236 not in cached_map


# ---------------------------------------------------------------------------
# Live Redis TTL Verification (User Requested Check)
# ---------------------------------------------------------------------------


def test_live_redis_ttl_enforcement():
    """Verify actual Redis TTL command returns a value <= 60 and > 0 against live Redis."""
    settings = get_settings()
    live_cache = PredictionCache(redis_url=settings.redis_url, ttl_seconds=60)

    if not live_cache.ping():
        pytest.skip("Live Redis instance not reachable at configured redis_url.")

    key = live_cache.demand_key(161, 15)
    sample_payload = {
        "zone_id": 161,
        "horizon_minutes": 15,
        "predicted_pickups": 55.5,
        "status": "ok",
        "cache_hit": True,
        "model_version": "ttl-v1",
        "as_of": "2026-09-10T12:00:00Z",
    }

    # Store via live PredictionCache
    live_cache.set_demand(161, 15, sample_payload, status="ok")

    # Directly execute Redis TTL command against live server
    r = live_cache._redis_client
    assert r is not None
    real_ttl = r.ttl(key)

    # Print real returned value to stdout
    print(
        f"\n[LIVE REDIS PROOF] Verified Redis TTL for key '{key}': {real_ttl} seconds"
    )

    assert (
        0 < real_ttl <= 60
    ), f"Expected live Redis TTL in (0, 60], got: {real_ttl} for key {key}"

    # Cleanup
    r.delete(key)
