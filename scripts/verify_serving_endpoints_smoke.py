"""Smoke verification script for FastAPI Serving Endpoints against live Compose services (M5-2).

Verifies the live HTTP API running in Docker Compose with real PostgreSQL, Redis,
and MLflow backing:
1. GET  /health: Liveness, readiness, dependency status, and active Production model metadata.
2. GET  /predict/demand/{zone_id}: Single zone demand prediction (200 with realistic float output).
3. GET  /predict/demand/264: TLC placeholder zone 264 ("NV") returning HTTP 404 Not Found.
4. GET  /predict/demand/999: Out-of-bounds zone returning HTTP 404 Not Found.
5. POST /predict/demand/batch: Vectorized multi-zone demand prediction (200, fast response).
6. POST /predict/demand/batch: Default to all 263 active zones when zone_ids is empty/omitted.
7. POST /predict/demand/batch: Malformed batch with invalid zone returning HTTP 400 Bad Request.
8. GET  /predict/eta: Single origin-destination corridor trip duration prediction (200, >= 60s floor).
9. GET  /predict/eta: Untrainable zone 265 ("NA") returning HTTP 404 Not Found.
10. POST /predict/eta/batch: Vectorized corridor batch ETA predictions (200).
11. POST /predict/eta/batch: Malformed corridor batch returning HTTP 400 Bad Request.
12. GET  /features/zone/161: Feature store inspection for zone entity.
13. GET  /features/corridor/161_236: Feature store inspection for corridor entity.
14. GET  /pipeline/status: Pipeline execution history inspection.
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Optional

import pandas as pd
import redis
import requests
from feast.data_source import PushMode

from src.common.config import get_settings
from src.features.client import (
    CorridorDurationOnlineFeatures,
    ZoneDemandOnlineFeatures,
)
from src.features.config import get_feature_store
from src.serving.feature_extractor import (
    build_corridor_feature_df,
    build_demand_feature_df,
    invert_log_duration,
)
from src.serving.model_loader import (
    DEMAND_MODEL_NAME,
    DURATION_MODEL_NAME,
    ModelLoaderService,
)

# Insulate against Windows console encoding errors when printing Unicode
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
os.environ["PYTHONIOENCODING"] = "utf-8"


def get_base_url() -> str:
    """Determine base URL for serving application.

    Prefers SERVING_URL or FASTAPI_URL if explicitly set.
    Otherwise attempts port 8000 first, falling back to port 80 (Caddy reverse proxy).
    """
    env_url = os.getenv("SERVING_URL") or os.getenv("FASTAPI_URL")
    if env_url:
        return env_url.rstrip("/")

    # Probe 8000 then 80
    for candidate in ["http://localhost:8000", "http://localhost"]:
        try:
            r = requests.get(f"{candidate}/health", timeout=2)
            if r.status_code == 200:
                return candidate
        except requests.RequestException:
            continue
    return "http://localhost:8000"


def wait_for_serving(base_url: str, timeout_seconds: int = 45) -> None:
    """Poll /health until serving is responsive."""
    print(f"Waiting for Serving API at {base_url}/health...", flush=True)
    start = time.time()
    last_err = None
    while time.time() - start < timeout_seconds:
        try:
            resp = requests.get(f"{base_url}/health", timeout=3)
            if resp.status_code == 200:
                print(f"Serving API responsive at {base_url} (HTTP 200).", flush=True)
                return
        except requests.RequestException as exc:
            last_err = exc
        time.sleep(1.0)
    raise RuntimeError(
        f"Serving API at {base_url} failed to respond within {timeout_seconds}s. Last error: {last_err}"
    )


def print_section(title: str) -> None:
    print(f"\n{'=' * 80}\n{title}\n{'=' * 80}", flush=True)


def print_http_exchange(
    method: str, url: str, req_body: Any, status: int, resp_body: Any, latency_ms: float
) -> None:
    print(f"\n>> {method} {url}", flush=True)
    if req_body is not None:
        print(f"   Request Body: {json.dumps(req_body)}", flush=True)
    print(f"<< HTTP {status} (Latency: {latency_ms:.1f}ms)", flush=True)
    print(f"   Response Body:\n{json.dumps(resp_body, indent=2)}", flush=True)


def seed_online_features_for_batch_smoke() -> None:
    """Push differentiated online features for zones and corridors to Feast online store (Redis).

    Ensures that when the live FastAPI endpoint reads online features from Redis,
    zones 161 and 236 have distinguishably different real online features, and corridors
    161_236 and 236_142 have distinguishably different real online features.
    """
    print(
        "\n   --- Seeding Differentiated Online Features into Redis (Feast Push) ---",
        flush=True,
    )
    try:
        from src.features.registry import apply_feature_definitions

        store = get_feature_store()
        apply_feature_definitions(store=store, include_push=True)
        now_utc = datetime.now(timezone.utc)
        zone_df = pd.DataFrame(
            [
                {
                    "zone_id": 161,
                    "pickup_datetime": now_utc,
                    "created_at": now_utc,
                    "pickup_count_last_15m": 25,
                    "pickup_count_last_1h": 75,
                    "hour_of_day": 14,
                    "day_of_week": 2,
                    "is_weekend": False,
                    "is_holiday": False,
                    "avg_temp_last_1h": 20.0,
                    "is_precipitating": False,
                },
                {
                    "zone_id": 236,
                    "pickup_datetime": now_utc,
                    "created_at": now_utc,
                    "pickup_count_last_15m": 2,
                    "pickup_count_last_1h": 8,
                    "hour_of_day": 14,
                    "day_of_week": 2,
                    "is_weekend": False,
                    "is_holiday": False,
                    "avg_temp_last_1h": 20.0,
                    "is_precipitating": False,
                },
            ]
        )
        corridor_df = pd.DataFrame(
            [
                {
                    "corridor_id": "161_236",
                    "dropoff_datetime": now_utc,
                    "created_at": now_utc,
                    "avg_duration_last_15m": 1200.0,
                    "avg_duration_last_1h": 1100.0,
                    "avg_traffic_speed_current": 18.0,
                    "origin_zone_demand_pressure": 75,
                },
                {
                    "corridor_id": "236_142",
                    "dropoff_datetime": now_utc,
                    "created_at": now_utc,
                    "avg_duration_last_15m": 300.0,
                    "avg_duration_last_1h": 350.0,
                    "avg_traffic_speed_current": 35.0,
                    "origin_zone_demand_pressure": 8,
                },
            ]
        )
        store.push("zone_demand_push_source", zone_df, to=PushMode.ONLINE)
        store.push("corridor_duration_push_source", corridor_df, to=PushMode.ONLINE)
        print(
            "   ✓ Seeded Redis online features via Feast push sources for batch sensitivity testing.",
            flush=True,
        )
    except Exception as exc:
        print(
            f"   [WARN] Could not seed Redis online features via Feast push: {exc}",
            flush=True,
        )


def verify_demand_batch_sensitivity(
    loader: Optional[ModelLoaderService] = None,
) -> None:
    """Verify that varied synthetic feature inputs produce strictly distinct batch demand predictions.

    Controlled test fixture validating:
    1. build_demand_feature_df constructs distinct feature vectors across batch rows (no vector reuse/broadcasting).
    2. Serving estimation pipeline with the real loaded LightGBM model produces distinct outputs when real-world features differ.
    """
    print("\n   --- Batch Demand Input Sensitivity Assertion ---", flush=True)

    feat_high = ZoneDemandOnlineFeatures(
        zone_id=161,
        pickup_count_last_15m=25,
        pickup_count_last_1h=75,
        pickup_count_last_24h=850,
        pickup_count_same_hour_last_week=40,
        hour_of_day=14,
        day_of_week=2,
        is_weekend=False,
        is_holiday=False,
        cache_hit=True,
    )
    feat_low = ZoneDemandOnlineFeatures(
        zone_id=236,
        pickup_count_last_15m=0,
        pickup_count_last_1h=0,
        pickup_count_last_24h=0,
        pickup_count_same_hour_last_week=5,
        hour_of_day=14,
        day_of_week=2,
        is_weekend=False,
        is_holiday=False,
        cache_hit=False,
    )

    batch_df = build_demand_feature_df([feat_high, feat_low])

    # Assert matrix rows are distinct (proves no broadcasting or row reuse)
    assert not batch_df.iloc[0].equals(
        batch_df.iloc[1]
    ), "Feature vectors for rows 0 and 1 are identical!"
    assert (
        batch_df.iloc[0]["pickup_count_same_hour_last_week"]
        != batch_df.iloc[1]["pickup_count_same_hour_last_week"]
    )
    assert (
        batch_df.iloc[0]["pickup_count_last_15m"]
        != batch_df.iloc[1]["pickup_count_last_15m"]
    )

    # Score with the actual loaded LightGBM model from ModelLoaderService
    assert (
        loader is not None
    ), "ModelLoaderService must be provided for sensitivity verification"
    loaded_model = loader.get_model(DEMAND_MODEL_NAME)
    assert (
        not loaded_model.is_fallback
    ), f"Expected real LightGBM model, but got fallback: {loaded_model.status}"
    preds = loaded_model.predict(batch_df)

    print(
        f"   Synthetic Row 0 (Zone 161, pickup_same_hour_last_week=40, pickup_15m=25): predicted_pickups={preds[0]:.2f}",
        flush=True,
    )
    print(
        f"   Synthetic Row 1 (Zone 236, pickup_same_hour_last_week=5,  pickup_15m=0):  predicted_pickups={preds[1]:.2f}",
        flush=True,
    )

    assert preds[0] != preds[1], (
        f"Batch demand sensitivity regression: distinct feature inputs produced identical predictions "
        f"({preds[0]:.2f} == {preds[1]:.2f})!"
    )
    diff = abs(preds[0] - preds[1])
    print(
        f"   ✓ Batch demand sensitivity verified: distinct inputs yielded distinct outputs (|Δ|={diff:.2f} pickups).",
        flush=True,
    )


def verify_eta_batch_sensitivity(
    loader: Optional[ModelLoaderService] = None,
) -> None:
    """Verify that varied synthetic corridor inputs produce strictly distinct batch ETA predictions.

    Controlled test fixture validating:
    1. build_corridor_feature_df constructs distinct feature vectors across batch rows (no vector reuse/broadcasting).
    2. Serving estimation pipeline with the real loaded LightGBM duration model produces distinct outputs.
    """
    print("\n   --- Batch ETA Input Sensitivity Assertion ---", flush=True)

    feat_long = CorridorDurationOnlineFeatures(
        corridor_id="161_236",
        avg_duration_last_15m=1200.0,
        avg_duration_last_1h=1100.0,
        distance_km=8.5,
        origin_zone_demand_pressure=5,
        cache_hit=True,
    )
    feat_short = CorridorDurationOnlineFeatures(
        corridor_id="236_142",
        avg_duration_last_15m=300.0,
        avg_duration_last_1h=350.0,
        distance_km=1.2,
        origin_zone_demand_pressure=1,
        cache_hit=True,
    )

    batch_df = build_corridor_feature_df(
        [feat_long, feat_short],
        origin_dest_pairs=[(161, 236), (236, 142)],
    )

    # Assert matrix rows are distinct (proves no broadcasting or row reuse)
    assert not batch_df.iloc[0].equals(
        batch_df.iloc[1]
    ), "Feature vectors for corridors 0 and 1 are identical!"
    assert batch_df.iloc[0]["distance_km"] != batch_df.iloc[1]["distance_km"]
    assert (
        batch_df.iloc[0]["avg_duration_last_1h"]
        != batch_df.iloc[1]["avg_duration_last_1h"]
    )

    # Score with the actual loaded LightGBM duration model from ModelLoaderService
    assert (
        loader is not None
    ), "ModelLoaderService must be provided for sensitivity verification"
    loaded_model = loader.get_model(DURATION_MODEL_NAME)
    assert (
        not loaded_model.is_fallback
    ), f"Expected real LightGBM model, but got fallback: {loaded_model.status}"
    preds = loaded_model.predict(batch_df)
    is_log_space = loaded_model.metadata.get("is_log_space", True)
    dur_sec_0, dur_min_0 = invert_log_duration(preds[0], is_log_space=is_log_space)
    dur_sec_1, dur_min_1 = invert_log_duration(preds[1], is_log_space=is_log_space)

    print(
        f"   Synthetic Row 0 (Corridor 161_236, dist=8.5km, hist=1200s): predicted_duration={dur_sec_0:.1f}s ({dur_min_0:.2f}m)",
        flush=True,
    )
    print(
        f"   Synthetic Row 1 (Corridor 236_142, dist=1.2km, hist=300s):  predicted_duration={dur_sec_1:.1f}s ({dur_min_1:.2f}m)",
        flush=True,
    )

    assert dur_sec_0 != dur_sec_1, (
        f"Batch ETA sensitivity regression: distinct corridor inputs produced identical predictions "
        f"({dur_sec_0:.1f}s == {dur_sec_1:.1f}s)!"
    )
    diff = abs(dur_sec_0 - dur_sec_1)
    print(
        f"   ✓ Batch ETA sensitivity verified: distinct inputs yielded distinct outputs (|Δ|={diff:.1f}s).",
        flush=True,
    )


def main() -> None:
    base_url = get_base_url()
    mlflow_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    loader = ModelLoaderService(tracking_uri=mlflow_uri)

    print_section(
        f"M5-2/M5-3 SMOKE VERIFICATION: FastAPI Prediction, Caching & Inspection Endpoints\nTarget URL: {base_url}"
    )
    wait_for_serving(base_url, timeout_seconds=45)

    passed_checks = 0
    total_checks = 18

    # -----------------------------------------------------------------------
    # 1. GET /health
    # -----------------------------------------------------------------------
    print_section("Check 1: GET /health (Liveness, Readiness & Model Metadata)")
    t0 = time.perf_counter()
    resp = requests.get(f"{base_url}/health")
    lat = (time.perf_counter() - t0) * 1000.0
    body = resp.json()
    print_http_exchange("GET", f"{base_url}/health", None, resp.status_code, body, lat)

    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
    assert body["status"] == "ok", f"Expected status 'ok', got {body['status']}"
    assert body["dependencies"]["database"] == "connected", "Database not connected"
    assert body["dependencies"]["redis"] == "connected", "Redis not connected"
    assert body["dependencies"]["mlflow"] == "reachable", "MLflow not reachable"

    # Confirm models are loaded and genuine
    models = body.get("models", {})
    assert "demand" in models, "Missing 'demand' model in health response"
    assert "duration" in models, "Missing 'duration' model in health response"
    assert (
        models["demand"]["is_fallback"] is False
    ), "Demand model is running in fallback baseline mode"
    assert (
        models["duration"]["is_fallback"] is False
    ), "Duration model is running in fallback baseline mode"
    assert (
        models["demand"]["stage"] == "Production"
    ), f"Demand model stage is {models['demand']['stage']}, not Production"
    assert (
        models["duration"]["stage"] == "Production"
    ), f"Duration model stage is {models['duration']['stage']}, not Production"
    passed_checks += 1
    print(
        "✓ Check 1 Passed: /health returns status=ok with genuine Production models loaded."
    )

    # -----------------------------------------------------------------------
    # 2. GET /predict/demand/161 (Single zone demand)
    # -----------------------------------------------------------------------
    print_section("Check 2: GET /predict/demand/161 (Midtown Center Demand)")
    t0 = time.perf_counter()
    resp = requests.get(f"{base_url}/predict/demand/161?horizon_minutes=15")
    lat = (time.perf_counter() - t0) * 1000.0
    body = resp.json()
    print_http_exchange(
        "GET",
        f"{base_url}/predict/demand/161?horizon_minutes=15",
        None,
        resp.status_code,
        body,
        lat,
    )

    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
    assert body["zone_id"] == 161
    assert body["horizon_minutes"] == 15
    assert isinstance(body["predicted_pickups"], (int, float))
    assert body["predicted_pickups"] >= 0.0
    assert (
        0.0 <= body["predicted_pickups"] <= 676.0
    ), f"Implausible prediction: {body['predicted_pickups']}"
    assert body["status"] in ("ok", "degraded_fallback")
    assert body["model_version"] is not None
    passed_checks += 1
    print(
        f"✓ Check 2 Passed: Single demand prediction returned {body['predicted_pickups']} pickups (status={body['status']})."
    )

    # -----------------------------------------------------------------------
    # 3. GET /predict/demand/264 (404 boundary for TLC non-geographic zone "NV")
    # -----------------------------------------------------------------------
    print_section(
        "Check 3: GET /predict/demand/264 (Deliberate 404 for Unservable TLC Zone 'NV')"
    )
    t0 = time.perf_counter()
    resp = requests.get(f"{base_url}/predict/demand/264")
    lat = (time.perf_counter() - t0) * 1000.0
    body = resp.json()
    print_http_exchange(
        "GET", f"{base_url}/predict/demand/264", None, resp.status_code, body, lat
    )

    assert resp.status_code == 404, f"Expected 404, got {resp.status_code}"
    assert body.get("error") == "not_found"
    assert "outside the servable forecast range [1, 263]" in body.get("detail", "")
    passed_checks += 1
    print(
        "✓ Check 3 Passed: Non-geographic zone 264 correctly rejected with HTTP 404 Not Found."
    )

    # -----------------------------------------------------------------------
    # 4. GET /predict/demand/999 (404 for out-of-bounds zone)
    # -----------------------------------------------------------------------
    print_section(
        "Check 4: GET /predict/demand/999 (Deliberate 404 for Out-of-Bounds Zone)"
    )
    t0 = time.perf_counter()
    resp = requests.get(f"{base_url}/predict/demand/999")
    lat = (time.perf_counter() - t0) * 1000.0
    body = resp.json()
    print_http_exchange(
        "GET", f"{base_url}/predict/demand/999", None, resp.status_code, body, lat
    )

    assert resp.status_code == 404, f"Expected 404, got {resp.status_code}"
    assert body.get("error") == "not_found"
    passed_checks += 1
    print(
        "✓ Check 4 Passed: Out-of-bounds zone 999 correctly rejected with HTTP 404 Not Found."
    )

    # -----------------------------------------------------------------------
    # 5. POST /predict/demand/batch (Explicit zones [161, 236, 237, 142])
    # -----------------------------------------------------------------------
    print_section("Check 5: POST /predict/demand/batch (Vectorized Multi-Zone Demand)")
    # Seed differentiated online features into Redis so zones 161 and 236 have distinct online features
    seed_online_features_for_batch_smoke()

    payload = {"zone_ids": [161, 236, 237, 142], "horizon_minutes": 15}
    t0 = time.perf_counter()
    resp = requests.post(f"{base_url}/predict/demand/batch", json=payload)
    lat = (time.perf_counter() - t0) * 1000.0
    body = resp.json()
    print_http_exchange(
        "POST", f"{base_url}/predict/demand/batch", payload, resp.status_code, body, lat
    )

    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
    assert body["prediction_count"] == 4
    assert len(body["predictions"]) == 4
    p_map = {}
    for item in body["predictions"]:
        assert item["zone_id"] in [161, 236, 237, 142]
        assert isinstance(item["predicted_pickups"], (int, float))
        assert item["predicted_pickups"] >= 0.0
        p_map[item["zone_id"]] = item["predicted_pickups"]

    # Assert directly on actual live HTTP response that predicted_pickups differ across zones
    p_161 = p_map.get(161)
    p_236 = p_map.get(236)
    print(
        f"   Live batch endpoint predictions: Zone 161={p_161:.2f} pickups, Zone 236={p_236:.2f} pickups",
        flush=True,
    )
    assert (
        p_161 is not None and p_236 is not None
    ), "Missing zones 161 or 236 in batch response"
    assert p_161 != p_236, (
        f"Live batch demand prediction regression: distinct online features produced identical predictions: "
        f"Zone 161 ({p_161}) == Zone 236 ({p_236})!"
    )
    print(
        f"   ✓ Live batch HTTP response differential verified: Zone 161 ({p_161:.2f}) != Zone 236 ({p_236:.2f}) (|Δ|={abs(p_161 - p_236):.2f} pickups).",
        flush=True,
    )

    # Sensitivity assertion: verify deliberately varied synthetic feature inputs produce different predictions
    verify_demand_batch_sensitivity(loader)
    passed_checks += 1
    print(
        f"✓ Check 5 Passed: Vectorized demand batch returned 4 predictions in {lat:.1f}ms (differential sensitivity verified)."
    )

    # -----------------------------------------------------------------------
    # 6. POST /predict/demand/batch (Default to all 263 active zones)
    # -----------------------------------------------------------------------
    print_section(
        "Check 6: POST /predict/demand/batch (City-Wide Default: All 263 Active Zones)"
    )
    payload = {}  # Empty payload defaults to all 263 zones
    t0 = time.perf_counter()
    resp = requests.post(f"{base_url}/predict/demand/batch", json=payload)
    lat = (time.perf_counter() - t0) * 1000.0
    body = resp.json()

    # Print summary rather than all 263 items
    sample_summary = {
        "prediction_count": body.get("prediction_count"),
        "model_version": body.get("model_version"),
        "as_of": body.get("as_of"),
        "sample_first_2": body.get("predictions", [])[:2],
        "sample_last_2": body.get("predictions", [])[-2:],
    }
    print_http_exchange(
        "POST",
        f"{base_url}/predict/demand/batch",
        payload,
        resp.status_code,
        sample_summary,
        lat,
    )

    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
    assert (
        body["prediction_count"] == 263
    ), f"Expected 263 predictions, got {body['prediction_count']}"
    assert len(body["predictions"]) == 263
    returned_zones = [p["zone_id"] for p in body["predictions"]]
    assert returned_zones == list(
        range(1, 264)
    ), "Batch did not return all 263 active zones in order"
    passed_checks += 1
    print(
        f"✓ Check 6 Passed: Full city-wide batch scored all 263 zones in {lat:.1f}ms."
    )

    # -----------------------------------------------------------------------
    # 7. POST /predict/demand/batch (Deliberate 400 for invalid zone in batch)
    # -----------------------------------------------------------------------
    print_section(
        "Check 7: POST /predict/demand/batch (Deliberate 400 Bad Request for Invalid Zone)"
    )
    payload = {"zone_ids": [161, 264]}  # 264 is unservable TLC non-geographic zone
    t0 = time.perf_counter()
    resp = requests.post(f"{base_url}/predict/demand/batch", json=payload)
    lat = (time.perf_counter() - t0) * 1000.0
    body = resp.json()
    print_http_exchange(
        "POST", f"{base_url}/predict/demand/batch", payload, resp.status_code, body, lat
    )

    assert resp.status_code == 400, f"Expected 400, got {resp.status_code}"
    assert body.get("error") == "bad_request"
    assert "Invalid zone ID(s) in batch request" in body.get("detail", "")
    passed_checks += 1
    print(
        "✓ Check 7 Passed: Batch payload with invalid zone 264 correctly rejected with HTTP 400 Bad Request."
    )

    # -----------------------------------------------------------------------
    # 8. GET /predict/eta (Single corridor ETA 161 -> 236)
    # -----------------------------------------------------------------------
    print_section(
        "Check 8: GET /predict/eta?origin=161&dest=236 (Midtown Center -> UES North)"
    )
    t0 = time.perf_counter()
    resp = requests.get(f"{base_url}/predict/eta?origin=161&dest=236")
    lat = (time.perf_counter() - t0) * 1000.0
    body = resp.json()
    print_http_exchange(
        "GET",
        f"{base_url}/predict/eta?origin=161&dest=236",
        None,
        resp.status_code,
        body,
        lat,
    )

    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
    assert body["origin_zone_id"] == 161
    assert body["dest_zone_id"] == 236
    assert body["corridor_id"] == "161_236"
    assert isinstance(body["predicted_duration_seconds"], (int, float))
    assert (
        body["predicted_duration_seconds"] >= 60.0
    ), "Trip duration must satisfy >= 60.0s floor"
    assert body["predicted_duration_minutes"] == round(
        body["predicted_duration_seconds"] / 60.0, 2
    )
    assert body["model_version"] is not None
    passed_checks += 1
    print(
        f"✓ Check 8 Passed: ETA predicted at {body['predicted_duration_minutes']} min ({body['predicted_duration_seconds']}s)."
    )

    # -----------------------------------------------------------------------
    # 9. GET /predict/eta (404 boundary for untrainable destination zone 265)
    # -----------------------------------------------------------------------
    print_section(
        "Check 9: GET /predict/eta?origin=161&dest=265 (Deliberate 404 for Untrained Zone 'NA')"
    )
    t0 = time.perf_counter()
    resp = requests.get(f"{base_url}/predict/eta?origin=161&dest=265")
    lat = (time.perf_counter() - t0) * 1000.0
    body = resp.json()
    print_http_exchange(
        "GET",
        f"{base_url}/predict/eta?origin=161&dest=265",
        None,
        resp.status_code,
        body,
        lat,
    )

    assert resp.status_code == 404, f"Expected 404, got {resp.status_code}"
    assert body.get("error") == "not_found"
    assert "outside the servable forecast range [1, 263]" in body.get("detail", "")
    passed_checks += 1
    print(
        "✓ Check 9 Passed: Destination zone 265 correctly rejected with HTTP 404 Not Found."
    )

    # -----------------------------------------------------------------------
    # 10. POST /predict/eta/batch (Vectorized corridor batch)
    # -----------------------------------------------------------------------
    print_section(
        "Check 10: POST /predict/eta/batch (Vectorized Corridor Duration Batch)"
    )
    payload = {
        "corridors": [
            {"origin_zone_id": 161, "dest_zone_id": 236},
            {"origin_zone_id": 236, "dest_zone_id": 142},
            {"origin_zone_id": 142, "dest_zone_id": 161},
        ]
    }
    t0 = time.perf_counter()
    resp = requests.post(f"{base_url}/predict/eta/batch", json=payload)
    lat = (time.perf_counter() - t0) * 1000.0
    body = resp.json()
    print_http_exchange(
        "POST", f"{base_url}/predict/eta/batch", payload, resp.status_code, body, lat
    )

    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
    assert body["prediction_count"] == 3
    assert len(body["predictions"]) == 3
    eta_map = {}
    for pred in body["predictions"]:
        assert pred["predicted_duration_seconds"] >= 60.0
        assert pred["predicted_duration_minutes"] == round(
            pred["predicted_duration_seconds"] / 60.0, 2
        )
        corridor_k = f"{pred['origin_zone_id']}_{pred['dest_zone_id']}"
        eta_map[corridor_k] = pred["predicted_duration_seconds"]

    # Assert directly on actual live HTTP response that predicted durations differ across corridors
    d_161_236 = eta_map.get("161_236")
    d_236_142 = eta_map.get("236_142")
    print(
        f"   Live batch endpoint predictions: Corridor 161_236={d_161_236:.1f}s, Corridor 236_142={d_236_142:.1f}s",
        flush=True,
    )
    assert (
        d_161_236 is not None and d_236_142 is not None
    ), "Missing corridors 161_236 or 236_142 in batch response"
    assert d_161_236 != d_236_142, (
        f"Live batch ETA prediction regression: distinct corridor features produced identical predictions: "
        f"Corridor 161_236 ({d_161_236}s) == Corridor 236_142 ({d_236_142}s)!"
    )
    print(
        f"   ✓ Live batch HTTP response differential verified: Corridor 161_236 ({d_161_236:.1f}s) != Corridor 236_142 ({d_236_142:.1f}s) (|Δ|={abs(d_161_236 - d_236_142):.1f}s).",
        flush=True,
    )

    # Sensitivity assertion: verify deliberately varied synthetic corridor inputs produce different predictions
    verify_eta_batch_sensitivity(loader)
    passed_checks += 1
    print(
        f"✓ Check 10 Passed: Vectorized ETA batch returned 3 predictions in {lat:.1f}ms (differential sensitivity verified)."
    )

    # -----------------------------------------------------------------------
    # 11. POST /predict/eta/batch (Deliberate 400 for invalid corridor in batch)
    # -----------------------------------------------------------------------
    print_section(
        "Check 11: POST /predict/eta/batch (Deliberate 400 for Invalid Corridor in Batch)"
    )
    payload = {
        "corridors": [
            {"origin_zone_id": 161, "dest_zone_id": 236},
            {"origin_zone_id": 161, "dest_zone_id": 264},  # 264 invalid
        ]
    }
    t0 = time.perf_counter()
    resp = requests.post(f"{base_url}/predict/eta/batch", json=payload)
    lat = (time.perf_counter() - t0) * 1000.0
    body = resp.json()
    print_http_exchange(
        "POST", f"{base_url}/predict/eta/batch", payload, resp.status_code, body, lat
    )

    assert resp.status_code == 400, f"Expected 400, got {resp.status_code}"
    assert body.get("error") == "bad_request"
    assert "Invalid corridor pair(s) in batch request" in body.get("detail", "")
    passed_checks += 1
    print(
        "✓ Check 11 Passed: Malformed corridor batch correctly rejected with HTTP 400 Bad Request."
    )

    # -----------------------------------------------------------------------
    # 12. GET /features/zone/161
    # -----------------------------------------------------------------------
    print_section("Check 12: GET /features/zone/161 (Online Feature Store Inspection)")
    t0 = time.perf_counter()
    resp = requests.get(f"{base_url}/features/zone/161")
    lat = (time.perf_counter() - t0) * 1000.0
    body = resp.json()
    print_http_exchange(
        "GET", f"{base_url}/features/zone/161", None, resp.status_code, body, lat
    )

    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
    assert body["entity_type"] == "zone"
    assert body["entity_id"] in (161, "161")
    assert "pickup_count_last_15m" in body["features"]
    passed_checks += 1
    print(
        f"✓ Check 12 Passed: Zone features inspected (cache_hit={body['cache_hit']})."
    )

    # -----------------------------------------------------------------------
    # 13. GET /features/corridor/161_236
    # -----------------------------------------------------------------------
    print_section(
        "Check 13: GET /features/corridor/161_236 (Corridor Feature Store Inspection)"
    )
    t0 = time.perf_counter()
    resp = requests.get(f"{base_url}/features/corridor/161_236")
    lat = (time.perf_counter() - t0) * 1000.0
    body = resp.json()
    print_http_exchange(
        "GET",
        f"{base_url}/features/corridor/161_236",
        None,
        resp.status_code,
        body,
        lat,
    )

    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
    assert body["entity_type"] == "corridor"
    assert body["entity_id"] == "161_236"
    assert "avg_duration_last_15m" in body["features"]
    passed_checks += 1
    print(
        f"✓ Check 13 Passed: Corridor features inspected (cache_hit={body['cache_hit']})."
    )

    # -----------------------------------------------------------------------
    # 14. GET /pipeline/status
    # -----------------------------------------------------------------------
    print_section("Check 14: GET /pipeline/status (Pipeline Run History)")
    t0 = time.perf_counter()
    resp = requests.get(f"{base_url}/pipeline/status")
    lat = (time.perf_counter() - t0) * 1000.0
    body = resp.json()
    print_http_exchange(
        "GET", f"{base_url}/pipeline/status", None, resp.status_code, body, lat
    )

    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
    assert "status" in body
    assert isinstance(body["latest_runs"], list)
    passed_checks += 1
    print(
        f"✓ Check 14 Passed: Pipeline status returned '{body['status']}' with {len(body['latest_runs'])} recorded runs."
    )

    # -----------------------------------------------------------------------
    # 15. Prediction Caching Verification (X-Cache: HIT & Low Latency)
    # -----------------------------------------------------------------------
    print_section(
        "Check 15: Prediction Caching Verification (X-Cache: HIT & Low Latency)"
    )
    # Zone 161 was evaluated in Check 2 and should now be cached in Redis with a 60s TTL
    t0 = time.perf_counter()
    resp_warm = requests.get(f"{base_url}/predict/demand/161")
    lat_warm = (time.perf_counter() - t0) * 1000.0
    body_warm = resp_warm.json()
    cache_header = resp_warm.headers.get("X-Cache")
    print_http_exchange(
        "GET",
        f"{base_url}/predict/demand/161",
        None,
        resp_warm.status_code,
        body_warm,
        lat_warm,
    )

    assert resp_warm.status_code == 200, f"Expected 200, got {resp_warm.status_code}"
    assert cache_header == "HIT", f"Expected X-Cache: HIT, got {cache_header}"
    assert lat_warm < 25.0, f"Expected cached latency < 25ms, got {lat_warm:.2f}ms"
    passed_checks += 1
    print(
        f"✓ Check 15 Passed: Prediction cache hit verified (X-Cache: HIT, latency={lat_warm:.2f}ms < 25ms)."
    )

    # -----------------------------------------------------------------------
    # 16. Live Redis TTL Inspection (assert 0 < TTL <= 60s)
    # -----------------------------------------------------------------------
    print_section("Check 16: Live Redis TTL Inspection (ADR-020 Section 3)")
    redis_url = os.getenv("REDIS_URL") or get_settings().redis_url
    r_client = redis.from_url(redis_url, decode_responses=True, socket_timeout=2.0)
    cache_key = "pred:demand:161:15"
    real_ttl = r_client.ttl(cache_key)
    print(
        f"\n[LIVE REDIS PROOF] Queried live Redis key '{cache_key}' via real TTL command."
    )
    print(f"[LIVE REDIS PROOF] Actual returned Redis TTL: {real_ttl} seconds")

    assert (
        0 < real_ttl <= 60
    ), f"Expected live Redis TTL in (0, 60], got: {real_ttl} for key '{cache_key}'"
    passed_checks += 1
    print(
        f"✓ Check 16 Passed: Live Redis TTL confirmed at {real_ttl}s (strictly <= 60s and > 0)."
    )

    # -----------------------------------------------------------------------
    # 17. Degraded Cache-Bypass Policy Verification (ADR-020 Section 3)
    # -----------------------------------------------------------------------
    print_section(
        "Check 17: Degraded Cache-Bypass Verification (Zero-TTL / Never Cached)"
    )
    # Test with unmaterialized cold zone (e.g. 200)
    resp_deg = requests.get(f"{base_url}/predict/demand/200")
    lat_deg = (time.perf_counter() - t0) * 1000.0
    body_deg = resp_deg.json()
    print_http_exchange(
        "GET",
        f"{base_url}/predict/demand/200",
        None,
        resp_deg.status_code,
        body_deg,
        lat_deg,
    )

    assert resp_deg.status_code == 200
    assert body_deg.get("status") == "degraded_fallback"
    assert body_deg.get("cache_hit") is False
    assert body_deg.get("predicted_pickups") >= 0.0

    deg_key = "pred:demand:200:15"
    key_exists = r_client.exists(deg_key)
    assert (
        key_exists == 0
    ), f"Degraded response was erroneously cached in Redis key '{deg_key}'"

    # Subsequent single call must remain MISS
    resp_deg2 = requests.get(f"{base_url}/predict/demand/200")
    assert (
        resp_deg2.headers.get("X-Cache") == "MISS"
    ), "Degraded response should never be served as X-Cache: HIT"
    passed_checks += 1
    print(
        f"✓ Check 17 Passed: Degraded prediction confirmed not cached in Redis (exists={key_exists}, X-Cache=MISS)."
    )

    # -----------------------------------------------------------------------
    # 18. Cross-Path Batch-then-Single Degraded Isolation
    # -----------------------------------------------------------------------
    print_section("Check 18: Cross-Path Batch-then-Single Degraded Isolation")
    # Batch request containing warm zone 161 and degraded zone 200
    batch_payload = {"zone_ids": [161, 200], "horizon_minutes": 15}
    resp_batch = requests.post(f"{base_url}/predict/demand/batch", json=batch_payload)
    assert resp_batch.status_code == 200
    batch_items = resp_batch.json()["predictions"]
    deg_item = next((it for it in batch_items if it["zone_id"] == 200), None)
    assert deg_item is not None
    assert deg_item["status"] == "degraded_fallback"

    # Immediately request zone 200 via single endpoint
    resp_single = requests.get(f"{base_url}/predict/demand/200")
    assert resp_single.status_code == 200
    assert (
        resp_single.headers.get("X-Cache") == "MISS"
    ), "Batch execution leaked degraded zone into prediction cache! Expected X-Cache: MISS on subsequent single call."
    assert resp_single.json()["status"] == "degraded_fallback"
    assert r_client.exists("pred:demand:200:15") == 0
    passed_checks += 1
    print(
        "✓ Check 18 Passed: Cross-path isolation verified (batch path never leaks degraded items to cache)."
    )

    # -----------------------------------------------------------------------
    # Final Summary
    # -----------------------------------------------------------------------
    print_section(
        f"ALL {passed_checks}/{total_checks} M5-2 & M5-3 SERVING SMOKE CHECKS PASSED"
    )
    print(
        "✓ Health, dependencies (Postgres/Redis/MLflow), and loaded Production models verified."
    )
    print("✓ Single and batch demand prediction contracts validated.")
    print(
        "✓ Single and batch ETA prediction contracts validated (including 60s floor and log-inversion)."
    )
    print(
        "✓ Deliberate 404 (single invalid zone) vs 400 (batch invalid element) split verified."
    )
    print("✓ Online feature inspection and pipeline status endpoints verified.")
    print(
        "✓ Low-latency prediction caching verified (X-Cache: HIT, sub-25ms over HTTP)."
    )
    print("✓ Live Redis key TTL verified <= 60s with real TTL command proof.")
    print(
        "✓ Degraded mode cache-bypass and cross-path batch-single isolation verified."
    )


if __name__ == "__main__":
    main()
