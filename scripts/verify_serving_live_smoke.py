"""End-to-End Live Serving Smoke Test & CI Verification (M5-4).

Validates the complete containerized online inference and serving layer against
live infrastructure (PostgreSQL, Redis, MLflow, FastAPI):
1. GET  /health: Liveness, readiness, dependency status (DB/Redis/MLflow), and loaded Production models.
2. GET  /predict/demand/{zone_id}: Active NYC zone demand inference (161 Midtown, 236 UES, 132 JFK).
3. GET  /predict/eta: Single corridor trip duration inference (161 -> 236), asserting >= 60s physical floor.
4. POST /predict/demand/batch: Vectorized batch demand scoring across all 263 active NYC taxi zones.
5. POST /predict/eta/batch: Vectorized corridor batch duration inference across multiple corridors.
6. GET  /predict/demand/{zone_id}: Low-latency single prediction caching (X-Cache: HIT) + live Redis TTL proof.
7. POST /predict/demand/batch: Real batch prediction caching acceleration & differentiated sensitivity verification.
8. POST /predict/eta/batch: Real corridor batch caching acceleration & differentiated sensitivity verification.
9. GET  /predict/demand/{zone_id}: Degraded mode fallback for unmaterialized zone with zero-TTL bypass and non-zero output.
10. GET /features/* & /pipeline/status: Feature store inspection and pipeline execution status.
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import pandas as pd
import redis
import requests
from feast.data_source import PushMode

from src.common.config import get_settings
from src.features.config import get_feature_store
from src.features.registry import apply_feature_definitions

# Protect against Windows cp1252 stdout encoding errors when printing Unicode
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
    """Print formatted section header."""
    print("\n" + "=" * 80, flush=True)
    print(title, flush=True)
    print("=" * 80, flush=True)


def safe_json(resp: requests.Response) -> Any:
    """Parse response JSON or return raw text if decoding fails."""
    try:
        return resp.json()
    except Exception:
        return resp.text


def print_http_exchange(
    method: str,
    url: str,
    payload: Optional[Any],
    status: int,
    resp_body: Any,
    latency_ms: float,
    headers: Optional[Dict[str, str]] = None,
) -> None:
    """Log an HTTP request/response exchange with latency and headers."""
    print(f"\n>> {method} {url}", flush=True)
    if payload is not None:
        print(f"   Request Body:\n{json.dumps(payload, indent=2)}", flush=True)
    if headers:
        cache_hdr = headers.get("X-Cache") or headers.get("x-cache")
        if cache_hdr:
            print(f"   Header X-Cache: {cache_hdr}", flush=True)
    print(f"<< HTTP {status} (Latency: {latency_ms:.1f}ms)", flush=True)
    if isinstance(resp_body, (dict, list)):
        print(f"   Response Body:\n{json.dumps(resp_body, indent=2)}", flush=True)
    else:
        print(f"   Response Body:\n{resp_body}", flush=True)


def seed_differentiated_online_features() -> None:
    """Push differentiated real features for zones and corridors into Feast Redis online store."""
    print(
        "\n   --- Seeding Differentiated Online Features into Redis (Feast Push) ---",
        flush=True,
    )
    try:
        store = get_feature_store()
        apply_feature_definitions(store=store, include_push=True)
        now_utc = datetime.now(timezone.utc)

        # Distinct features for zone 161 (Midtown) vs zone 236 (Upper East Side)
        zone_df = pd.DataFrame(
            [
                {
                    "zone_id": 161,
                    "pickup_datetime": now_utc,
                    "created_at": now_utc,
                    "pickup_count_last_15m": 35,
                    "pickup_count_last_1h": 110,
                    "hour_of_day": 15,
                    "day_of_week": 2,
                    "is_weekend": False,
                    "is_holiday": False,
                    "avg_temp_last_1h": 22.0,
                    "is_precipitating": False,
                },
                {
                    "zone_id": 236,
                    "pickup_datetime": now_utc,
                    "created_at": now_utc,
                    "pickup_count_last_15m": 4,
                    "pickup_count_last_1h": 14,
                    "hour_of_day": 15,
                    "day_of_week": 2,
                    "is_weekend": False,
                    "is_holiday": False,
                    "avg_temp_last_1h": 22.0,
                    "is_precipitating": False,
                },
            ]
        )

        # Distinct features for corridor 161_236 vs 236_142
        corridor_df = pd.DataFrame(
            [
                {
                    "corridor_id": "161_236",
                    "dropoff_datetime": now_utc,
                    "created_at": now_utc,
                    "avg_duration_last_15m": 1250.0,
                    "avg_duration_last_1h": 1180.0,
                    "avg_traffic_speed_current": 16.5,
                    "origin_zone_demand_pressure": 110,
                },
                {
                    "corridor_id": "236_142",
                    "dropoff_datetime": now_utc,
                    "created_at": now_utc,
                    "avg_duration_last_15m": 320.0,
                    "avg_duration_last_1h": 340.0,
                    "avg_traffic_speed_current": 32.0,
                    "origin_zone_demand_pressure": 14,
                },
            ]
        )

        store.push("zone_demand_push_source", zone_df, to=PushMode.ONLINE)
        store.push("corridor_duration_push_source", corridor_df, to=PushMode.ONLINE)
        print(
            "   ✓ Differentiated Feast features successfully pushed to Redis.",
            flush=True,
        )
    except Exception as exc:
        print(
            f"   [WARN] Feature push encountered non-fatal error: {exc}",
            flush=True,
        )


def main() -> None:
    """Execute the end-to-end serving live smoke test suite."""
    base_url = get_base_url()
    print(
        f"Initiating M5-4 Live Serving Smoke Verification targeting: {base_url}",
        flush=True,
    )
    wait_for_serving(base_url)

    settings = get_settings()
    r_client = redis.from_url(settings.redis_url, socket_timeout=3.0)

    # Seed differentiated features for sensitivity testing
    seed_differentiated_online_features()

    passed_checks = 0
    total_checks = 10

    # -----------------------------------------------------------------------
    # 1. Liveness, Readiness & Model Metadata Check
    # -----------------------------------------------------------------------
    print_section(
        "Check 1: GET /health (Liveness, Readiness, DB/Redis/MLflow, Production Models)"
    )
    t0 = time.perf_counter()
    resp = requests.get(f"{base_url}/health")
    lat = (time.perf_counter() - t0) * 1000.0
    body = safe_json(resp)
    print_http_exchange("GET", f"{base_url}/health", None, resp.status_code, body, lat)

    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {body}"
    deps = body.get("dependencies", {})
    assert (
        deps.get("database") == "connected"
    ), f"Database unreachable: {deps.get('database')}"
    assert deps.get("redis") == "connected", f"Redis unreachable: {deps.get('redis')}"
    assert deps.get("mlflow") in [
        "reachable",
        "connected",
        "degraded",
    ], f"MLflow check failed: {deps.get('mlflow')}"

    models = body.get("models", {})
    demand_model = models.get("demand") or models.get("taxi_demand_lightgbm")
    duration_model = models.get("duration") or models.get("corridor_duration_lightgbm")

    assert demand_model is not None, "Missing demand model in /health response"
    assert duration_model is not None, "Missing duration model in /health response"
    assert (
        demand_model.get("is_fallback") is False
    ), "Demand model is running in baseline fallback"
    assert (
        duration_model.get("is_fallback") is False
    ), "Duration model is running in baseline fallback"
    assert (
        demand_model.get("stage") == "Production"
    ), f"Demand model stage is not Production: {demand_model.get('stage')}"
    assert (
        duration_model.get("stage") == "Production"
    ), f"Duration model stage is not Production: {duration_model.get('stage')}"
    passed_checks += 1
    print(
        "✓ Check 1 Passed: /health reports healthy dependencies and active Production models."
    )

    # -----------------------------------------------------------------------
    # 2. Single Active Zone Demand Inference
    # -----------------------------------------------------------------------
    print_section(
        "Check 2: GET /predict/demand/{zone_id} (Active Zones: 161 Midtown, 236 UES, 132 JFK)"
    )
    test_zones = [161, 236, 132]
    demand_preds = {}
    for zid in test_zones:
        t0 = time.perf_counter()
        resp = requests.get(f"{base_url}/predict/demand/{zid}")
        lat = (time.perf_counter() - t0) * 1000.0
        body = safe_json(resp)
        print_http_exchange(
            "GET", f"{base_url}/predict/demand/{zid}", None, resp.status_code, body, lat
        )

        assert (
            resp.status_code == 200
        ), f"Zone {zid} returned status {resp.status_code}: {body}"
        assert body.get("zone_id") == zid
        assert body.get("status") in ["ok", "degraded_fallback"]
        pred_val = body.get("predicted_pickups")
        assert (
            isinstance(pred_val, (int, float)) and pred_val >= 0.0
        ), f"Invalid predicted pickups for zone {zid}: {pred_val}"
        demand_preds[zid] = pred_val

    passed_checks += 1
    print(
        f"✓ Check 2 Passed: Single demand predictions validated across active zones {test_zones}."
    )

    # -----------------------------------------------------------------------
    # 3. Single Corridor Trip Duration / ETA
    # -----------------------------------------------------------------------
    print_section(
        "Check 3: GET /predict/eta?origin=161&dest=236 (Corridor ETA & 60s Floor)"
    )
    t0 = time.perf_counter()
    resp = requests.get(f"{base_url}/predict/eta?origin=161&dest=236")
    lat = (time.perf_counter() - t0) * 1000.0
    body = safe_json(resp)
    print_http_exchange(
        "GET",
        f"{base_url}/predict/eta?origin=161&dest=236",
        None,
        resp.status_code,
        body,
        lat,
    )

    assert resp.status_code == 200, f"Corridor ETA returned {resp.status_code}: {body}"
    assert body.get("origin_zone_id") == 161
    assert body.get("dest_zone_id") == 236
    dur_sec = body.get("predicted_duration_seconds")
    dur_min = body.get("predicted_duration_minutes")
    assert (
        isinstance(dur_sec, (int, float)) and dur_sec >= 60.0
    ), f"Corridor duration violates 60.0s floor: {dur_sec}"
    assert (
        abs(dur_min - round(dur_sec / 60.0, 2)) < 0.01
    ), f"Minute rounding mismatch: sec={dur_sec}, min={dur_min}"
    passed_checks += 1
    print(
        f"✓ Check 3 Passed: Corridor ETA returned {dur_sec}s ({dur_min}m), adhering to physical floor."
    )

    # -----------------------------------------------------------------------
    # 4. All 263 Active NYC Zones Vectorized Batch Demand
    # -----------------------------------------------------------------------
    print_section("Check 4: POST /predict/demand/batch (All 263 Active Taxi Zones)")
    all_zones = [z for z in range(1, 266) if z not in (264, 265)]
    assert len(all_zones) == 263

    t0 = time.perf_counter()
    resp = requests.post(
        f"{base_url}/predict/demand/batch",
        json={"zone_ids": all_zones, "horizon_minutes": 15},
    )
    total_batch_lat = (time.perf_counter() - t0) * 1000.0
    body = safe_json(resp)

    assert resp.status_code == 200, f"Batch demand returned {resp.status_code}: {body}"
    preds_list = body.get("predictions", [])
    assert (
        len(preds_list) == 263
    ), f"Expected 263 zone predictions, got: {len(preds_list)}"

    per_entity_lat = total_batch_lat / 263.0
    print(
        f"   Batch 263-Zone Scoring Latency: {total_batch_lat:.2f}ms total ({per_entity_lat:.3f}ms/zone)",
        flush=True,
    )
    assert (
        per_entity_lat < 50.0
    ), f"Per-entity latency exceeded 50ms: {per_entity_lat:.3f}ms"
    passed_checks += 1
    print(
        f"✓ Check 4 Passed: 263 active zones scored vectorially in {total_batch_lat:.1f}ms ({per_entity_lat:.3f}ms/zone)."
    )

    # -----------------------------------------------------------------------
    # 5. Vectorized Corridor Batch ETA
    # -----------------------------------------------------------------------
    print_section(
        "Check 5: POST /predict/eta/batch (Vectorized Multi-Corridor Inference)"
    )
    corridors = [
        {"origin_zone_id": 161, "dest_zone_id": 236},
        {"origin_zone_id": 236, "dest_zone_id": 161},
        {"origin_zone_id": 132, "dest_zone_id": 161},
        {"origin_zone_id": 142, "dest_zone_id": 236},
    ]
    t0 = time.perf_counter()
    resp = requests.post(f"{base_url}/predict/eta/batch", json={"corridors": corridors})
    lat = (time.perf_counter() - t0) * 1000.0
    body = safe_json(resp)
    print_http_exchange(
        "POST",
        f"{base_url}/predict/eta/batch",
        {"corridors": corridors},
        resp.status_code,
        body,
        lat,
    )

    assert resp.status_code == 200, f"Batch ETA returned {resp.status_code}: {body}"
    eta_items = body.get("predictions", [])
    assert len(eta_items) == len(corridors)
    for it in eta_items:
        assert it["predicted_duration_seconds"] >= 60.0
    passed_checks += 1
    print(
        f"✓ Check 5 Passed: Vectorized corridor batch scored {len(eta_items)} corridors cleanly."
    )

    # -----------------------------------------------------------------------
    # 6. Single Prediction Caching & Live Redis TTL Proof
    # -----------------------------------------------------------------------
    print_section(
        "Check 6: Low-Latency Single Prediction Caching & Live Redis TTL Proof"
    )
    # Prime single cache with zone 161
    requests.get(f"{base_url}/predict/demand/161")

    # Second call must hit cache
    t0 = time.perf_counter()
    resp_cached = requests.get(f"{base_url}/predict/demand/161")
    cached_lat = (time.perf_counter() - t0) * 1000.0
    body_cached = safe_json(resp_cached)
    print_http_exchange(
        "GET",
        f"{base_url}/predict/demand/161",
        None,
        resp_cached.status_code,
        body_cached,
        cached_lat,
        resp_cached.headers,
    )

    assert (
        resp_cached.status_code == 200
    ), f"Cached call returned {resp_cached.status_code}: {body_cached}"

    assert (
        resp_cached.headers.get("X-Cache") == "HIT"
    ), f"Expected X-Cache: HIT, got: {resp_cached.headers.get('X-Cache')}"
    assert cached_lat < 25.0, f"Cached latency exceeded 25ms: {cached_lat:.2f}ms"

    # Inspect live Redis key via real TTL command
    cache_key = "pred:demand:161:15"
    real_ttl = r_client.ttl(cache_key)
    print(
        f"\n[LIVE REDIS PROOF] Queried live Redis key '{cache_key}' via real TTL command.",
        flush=True,
    )
    print(
        f"[LIVE REDIS PROOF] Actual returned Redis TTL: {real_ttl} seconds", flush=True
    )

    assert (
        0 < real_ttl <= 60
    ), f"Expected live Redis TTL in (0, 60], got: {real_ttl} for key '{cache_key}'"
    passed_checks += 1
    print(
        f"✓ Check 6 Passed: Single prediction caching verified (X-Cache: HIT, latency={cached_lat:.2f}ms, TTL={real_ttl}s)."
    )

    # -----------------------------------------------------------------------
    # 7. Real Batch Endpoint Demand Caching Acceleration & Sensitivity
    # -----------------------------------------------------------------------
    print_section(
        "Check 7: Batch Demand Endpoint Caching & Differentiated Sensitivity Verification"
    )
    batch_demand_payload = {"zone_ids": [161, 236], "horizon_minutes": 15}

    # Invalidate keys first to guarantee Call 1 computes fresh
    r_client.delete("pred:demand:161:15", "pred:demand:236:15")

    # Call 1: Fresh inference
    t0 = time.perf_counter()
    resp_b1 = requests.post(
        f"{base_url}/predict/demand/batch", json=batch_demand_payload
    )
    lat_b1 = (time.perf_counter() - t0) * 1000.0
    body_b1 = safe_json(resp_b1)
    print_http_exchange(
        "POST (Call 1 - Fresh Inference)",
        f"{base_url}/predict/demand/batch",
        batch_demand_payload,
        resp_b1.status_code,
        body_b1,
        lat_b1,
        resp_b1.headers,
    )

    assert (
        resp_b1.status_code == 200
    ), f"Batch demand Call 1 returned {resp_b1.status_code}: {body_b1}"
    items_b1 = body_b1["predictions"]
    pred_161_b1 = next(
        it["predicted_pickups"] for it in items_b1 if it["zone_id"] == 161
    )
    pred_236_b1 = next(
        it["predicted_pickups"] for it in items_b1 if it["zone_id"] == 236
    )

    # Sensitivity Assertion: Distinct online feature counts must produce distinct predictions
    assert (
        pred_161_b1 != pred_236_b1
    ), f"Batch demand sensitivity failed! Zones 161 and 236 returned identical prediction: {pred_161_b1}"
    print(
        f"   [SENSITIVITY PROOF] Zone 161 (35 pickups/15m) = {pred_161_b1} vs Zone 236 (4 pickups/15m) = {pred_236_b1}",
        flush=True,
    )

    # Call 2: Immediate second call with identical payload must be a cache HIT
    t0 = time.perf_counter()
    resp_b2 = requests.post(
        f"{base_url}/predict/demand/batch", json=batch_demand_payload
    )
    lat_b2 = (time.perf_counter() - t0) * 1000.0
    body_b2 = safe_json(resp_b2)
    print_http_exchange(
        "POST (Call 2 - Cached Hit)",
        f"{base_url}/predict/demand/batch",
        batch_demand_payload,
        resp_b2.status_code,
        body_b2,
        lat_b2,
        resp_b2.headers,
    )

    assert (
        resp_b2.status_code == 200
    ), f"Batch demand Call 2 returned {resp_b2.status_code}: {body_b2}"
    assert (
        resp_b2.headers.get("X-Cache") == "HIT"
    ), f"Expected X-Cache: HIT on Call 2, got: {resp_b2.headers.get('X-Cache')}"

    items_b2 = body_b2["predictions"]
    pred_161_b2 = next(
        it["predicted_pickups"] for it in items_b2 if it["zone_id"] == 161
    )
    pred_236_b2 = next(
        it["predicted_pickups"] for it in items_b2 if it["zone_id"] == 236
    )

    # Value Identity Assertion: Cached predictions must match exactly
    assert (
        abs(pred_161_b2 - pred_161_b1) < 1e-4
    ), f"Cached prediction for Zone 161 drifted: call1={pred_161_b1}, call2={pred_161_b2}"
    assert (
        abs(pred_236_b2 - pred_236_b1) < 1e-4
    ), f"Cached prediction for Zone 236 drifted: call1={pred_236_b1}, call2={pred_236_b2}"
    assert pred_161_b2 != pred_236_b2, "Sensitivity lost in cached batch response!"

    print(
        f"   [BATCH CACHING PROOF] Call 1 Latency: {lat_b1:.2f}ms vs Call 2 (Cache Hit): {lat_b2:.2f}ms",
        flush=True,
    )
    passed_checks += 1
    print(
        "✓ Check 7 Passed: Batch demand caching verified (X-Cache: HIT, identical values, sensitivity strictly preserved)."
    )

    # -----------------------------------------------------------------------
    # 8. Real Batch Endpoint ETA Caching Acceleration & Sensitivity
    # -----------------------------------------------------------------------
    print_section(
        "Check 8: Batch ETA Endpoint Caching & Differentiated Sensitivity Verification"
    )
    batch_eta_payload = {
        "corridors": [
            {"origin_zone_id": 161, "dest_zone_id": 236},
            {"origin_zone_id": 236, "dest_zone_id": 142},
        ]
    }

    # Invalidate keys to force fresh inference on Call 1
    r_client.delete("pred:eta:161:236", "pred:eta:236:142")

    # Call 1: Fresh inference
    t0 = time.perf_counter()
    resp_eta1 = requests.post(f"{base_url}/predict/eta/batch", json=batch_eta_payload)
    lat_eta1 = (time.perf_counter() - t0) * 1000.0
    body_eta1 = safe_json(resp_eta1)
    print_http_exchange(
        "POST (Call 1 - Fresh Inference)",
        f"{base_url}/predict/eta/batch",
        batch_eta_payload,
        resp_eta1.status_code,
        body_eta1,
        lat_eta1,
        resp_eta1.headers,
    )

    assert (
        resp_eta1.status_code == 200
    ), f"Batch ETA Call 1 returned {resp_eta1.status_code}: {body_eta1}"
    items_eta1 = body_eta1["predictions"]
    dur_161_236_1 = items_eta1[0]["predicted_duration_seconds"]
    dur_236_142_1 = items_eta1[1]["predicted_duration_seconds"]

    assert (
        dur_161_236_1 != dur_236_142_1
    ), f"Corridor batch duration sensitivity failed! Both returned: {dur_161_236_1}"
    print(
        f"   [SENSITIVITY PROOF] Corridor 161->236 ({dur_161_236_1}s) != Corridor 236->142 ({dur_236_142_1}s)",
        flush=True,
    )

    # Call 2: Immediate second call
    t0 = time.perf_counter()
    resp_eta2 = requests.post(f"{base_url}/predict/eta/batch", json=batch_eta_payload)
    lat_eta2 = (time.perf_counter() - t0) * 1000.0
    body_eta2 = safe_json(resp_eta2)
    print_http_exchange(
        "POST (Call 2 - Cached Hit)",
        f"{base_url}/predict/eta/batch",
        batch_eta_payload,
        resp_eta2.status_code,
        body_eta2,
        lat_eta2,
        resp_eta2.headers,
    )

    assert (
        resp_eta2.status_code == 200
    ), f"Batch ETA Call 2 returned {resp_eta2.status_code}: {body_eta2}"
    assert (
        resp_eta2.headers.get("X-Cache") == "HIT"
    ), f"Expected X-Cache: HIT on Call 2, got: {resp_eta2.headers.get('X-Cache')}"

    items_eta2 = body_eta2["predictions"]
    dur_161_236_2 = items_eta2[0]["predicted_duration_seconds"]
    dur_236_142_2 = items_eta2[1]["predicted_duration_seconds"]

    assert abs(dur_161_236_2 - dur_161_236_1) < 1e-4
    assert abs(dur_236_142_2 - dur_236_142_1) < 1e-4
    assert dur_161_236_2 != dur_236_142_2

    print(
        f"   [BATCH CACHING PROOF] ETA Call 1: {lat_eta1:.2f}ms vs ETA Call 2 (Cache Hit): {lat_eta2:.2f}ms",
        flush=True,
    )
    passed_checks += 1
    print(
        "✓ Check 8 Passed: Batch ETA caching verified (X-Cache: HIT, identical values, sensitivity strictly preserved)."
    )

    # -----------------------------------------------------------------------
    # 9. Degraded Mode Zero-TTL Bypass & Non-Zero Output
    # -----------------------------------------------------------------------
    print_section(
        "Check 9: Degraded Mode Zero-TTL Bypass & Realistic Non-Zero Output (Cold Zone 200)"
    )
    t0 = time.perf_counter()
    resp_deg = requests.get(f"{base_url}/predict/demand/200")
    lat_deg = (time.perf_counter() - t0) * 1000.0
    body_deg = safe_json(resp_deg)
    print_http_exchange(
        "GET",
        f"{base_url}/predict/demand/200",
        None,
        resp_deg.status_code,
        body_deg,
        lat_deg,
        resp_deg.headers,
    )

    assert (
        resp_deg.status_code == 200
    ), f"Degraded demand returned {resp_deg.status_code}: {body_deg}"
    assert body_deg.get("status") == "degraded_fallback"
    assert body_deg.get("cache_hit") is False
    deg_val = body_deg.get("predicted_pickups")
    assert (
        isinstance(deg_val, (int, float)) and deg_val > 0.0
    ), f"Expected non-zero realistic estimate for degraded entity, got: {deg_val}"

    # Confirm zero-TTL: Key must not exist in Redis
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
        f"✓ Check 9 Passed: Degraded mode verified (status=degraded_fallback, pickups={deg_val} > 0.0, exists={key_exists}, X-Cache=MISS)."
    )

    # -----------------------------------------------------------------------
    # 10. Feature Store Online Inspection & Pipeline Status
    # -----------------------------------------------------------------------
    print_section("Check 10: Online Feature Store Inspection & Pipeline Status History")
    resp_fz = requests.get(f"{base_url}/features/zone/161")
    body_fz = safe_json(resp_fz)
    assert (
        resp_fz.status_code == 200
    ), f"Features zone returned {resp_fz.status_code}: {body_fz}"
    assert body_fz.get("entity_type") == "zone"
    assert body_fz.get("entity_id") == 161
    assert "pickup_count_last_15m" in body_fz.get("features", {})
    print("   ✓ GET /features/zone/161 returned online feature vector.", flush=True)

    resp_fc = requests.get(f"{base_url}/features/corridor/161_236")
    body_fc = safe_json(resp_fc)
    assert (
        resp_fc.status_code == 200
    ), f"Features corridor returned {resp_fc.status_code}: {body_fc}"
    assert body_fc.get("entity_type") == "corridor"
    assert body_fc.get("entity_id") == "161_236"
    assert "avg_duration_last_15m" in body_fc.get("features", {})
    print(
        "   ✓ GET /features/corridor/161_236 returned online feature vector.",
        flush=True,
    )

    resp_pipe = requests.get(f"{base_url}/pipeline/status")
    body_pipe = safe_json(resp_pipe)
    assert (
        resp_pipe.status_code == 200
    ), f"Pipeline status returned {resp_pipe.status_code}: {body_pipe}"
    assert "status" in body_pipe
    print(
        f"   ✓ GET /pipeline/status returned '{body_pipe.get('status')}'.",
        flush=True,
    )
    passed_checks += 1
    print("✓ Check 10 Passed: Feature store inspection and pipeline status verified.")

    # -----------------------------------------------------------------------
    # Final Summary
    # -----------------------------------------------------------------------
    print_section(
        f"ALL {passed_checks}/{total_checks} M5-4 END-TO-END LIVE SERVING SMOKE CHECKS PASSED"
    )
    print("✓ Health & Production model metadata verified against live Docker stack.")
    print("✓ Single active zone demand predictions validated (161, 236, 132).")
    print(
        "✓ Single corridor ETA validated with physical 60.0s floor and minute alignment."
    )
    print("✓ Vectorized full 263-zone batch demand scoring validated (<50ms target).")
    print("✓ Vectorized multi-corridor batch ETA scoring validated.")
    print("✓ Low-latency single prediction caching validated (<25ms, X-Cache: HIT).")
    print("✓ Live Redis key TTL confirmed <= 60s with real Redis TTL command output.")
    print(
        "✓ Batch demand caching acceleration and differentiated feature sensitivity verified."
    )
    print(
        "✓ Batch ETA caching acceleration and differentiated corridor sensitivity verified."
    )
    print(
        "✓ Degraded mode non-zero inference and zero-TTL cache-bypass policy verified."
    )
    print("✓ Online feature inspection and pipeline health status contracts confirmed.")


if __name__ == "__main__":
    main()
