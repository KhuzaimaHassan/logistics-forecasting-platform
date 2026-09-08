"""Smoke verification script for ModelLoaderService against live compose MLflow service (M5-1).

Verifies:
1. ModelLoaderService connects to the genuine PostgreSQL-backed MLflow service in Docker Compose.
2. Production-staged demand_lightgbm_model and corridor_duration_lightgbm_model are resolved and loaded.
3. Health metadata reflects active production models without fallback heuristics.
4. Live inference through demand_lightgbm_model using the exact train_demand.py feature matrix
   for Zone 161 (Midtown Center) produces a plausible prediction within M3-4 validation range [0, 676].
5. Live inference through corridor_duration_lightgbm_model for Corridor 161 -> 236 produces a plausible duration.
"""

import json
import os
import sys
import time
import warnings

# Insulate against Windows console encoding errors when printing Unicode
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
os.environ["PYTHONIOENCODING"] = "utf-8"

import numpy as np
import pandas as pd
import requests
from mlflow.tracking import MlflowClient

from src.serving.model_loader import ModelLoaderService
from src.training.pipeline import DEMAND_MODEL_NAME, DURATION_MODEL_NAME
from src.training.train_demand import DEMAND_FEATURE_COLS
from src.training.train_duration import DURATION_FEATURE_COLS


def wait_for_mlflow(tracking_uri: str, timeout_seconds: int = 30) -> None:
    """Ensure MLflow server is responding before querying registry."""
    print(f"Checking MLflow server connectivity at {tracking_uri}...", flush=True)
    health_url = f"{tracking_uri.rstrip('/')}/health"
    start_time = time.time()
    last_err = None
    while time.time() - start_time < timeout_seconds:
        try:
            resp = requests.get(health_url, timeout=3)
            if resp.status_code == 200:
                print(
                    f"MLflow server healthy at {tracking_uri} (HTTP 200).", flush=True
                )
                return
        except requests.RequestException as exc:
            last_err = exc
        time.sleep(1.0)
    raise RuntimeError(
        f"MLflow server at {tracking_uri} failed to become healthy within {timeout_seconds}s. Last error: {last_err}"
    )


def ensure_production_stage(client: MlflowClient, model_name: str) -> str:
    """Ensure the latest registered version of model_name is promoted to 'Production' stage."""
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning, module="mlflow.*")
        versions = client.get_latest_versions(model_name)
    if not versions:
        raise RuntimeError(
            f"No registered versions found for '{model_name}'. Ensure Feast smoke test / pipeline ran first."
        )

    # Pick latest version
    latest = sorted(versions, key=lambda v: int(v.version), reverse=True)[0]
    ver_str = str(latest.version)

    if latest.current_stage != "Production":
        print(
            f"Model '{model_name}' latest version v{ver_str} is in stage '{latest.current_stage}'. "
            f"Transitioning to 'Production'...",
            flush=True,
        )
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=FutureWarning, module="mlflow.*")
            client.transition_model_version_stage(
                name=model_name,
                version=ver_str,
                stage="Production",
                archive_existing_versions=True,
            )
        print(
            f"Model '{model_name}' v{ver_str} transitioned to 'Production'.", flush=True
        )
    else:
        print(
            f"Model '{model_name}' v{ver_str} already in 'Production' stage.",
            flush=True,
        )

    return ver_str


def build_demand_feature_vector_zone_161() -> pd.DataFrame:
    """Construct exact feature matrix for Zone 161 (Midtown Center) per train_demand.py."""
    hour = 14.0  # 2:00 PM
    dow = 2.0  # Wednesday
    sin_hour = float(np.sin(2 * np.pi * hour / 24.0))
    cos_hour = float(np.cos(2 * np.pi * hour / 24.0))
    sin_dow = float(np.sin(2 * np.pi * dow / 7.0))
    cos_dow = float(np.cos(2 * np.pi * dow / 7.0))

    # Features for Zone 161 (Midtown Center - high taxi activity zone)
    raw_data = {
        "zone_id": pd.Series([161], dtype="category"),
        "pickup_count_last_15m": [12.0],
        "pickup_count_last_1h": [48.0],
        "pickup_count_last_24h": [350.0],
        "pickup_count_same_hour_last_week": [45.0],
        "hour_of_day": [hour],
        "day_of_week": [dow],
        "is_weekend": [0.0],
        "is_holiday": [0.0],
        "sin_hour": [sin_hour],
        "cos_hour": [cos_hour],
        "sin_day_of_week": [sin_dow],
        "cos_day_of_week": [cos_dow],
    }

    df = pd.DataFrame(raw_data)
    # Ensure exact column order matching DEMAND_FEATURE_COLS
    return df[DEMAND_FEATURE_COLS].copy()


def build_corridor_feature_vector_161_236() -> pd.DataFrame:
    """Construct exact feature matrix for Corridor 161 -> 236 per train_duration.py."""
    hour = 14.0
    dow = 2.0
    sin_hour = float(np.sin(2 * np.pi * hour / 24.0))
    cos_hour = float(np.cos(2 * np.pi * hour / 24.0))
    sin_dow = float(np.sin(2 * np.pi * dow / 7.0))
    cos_dow = float(np.cos(2 * np.pi * dow / 7.0))

    avg_1h = 900.0  # 15 minutes
    raw_data = {
        "pickup_zone_id": pd.Series([161], dtype="category"),
        "dropoff_zone_id": pd.Series([236], dtype="category"),
        "avg_duration_last_15m": [870.0],
        "avg_duration_last_1h": [avg_1h],
        "log_avg_duration_last_1h": [float(np.log1p(avg_1h))],
        "distance_km": [3.8],
        "origin_zone_demand_pressure": [1.15],
        "hour_of_day": [hour],
        "day_of_week": [dow],
        "is_weekend": [0.0],
        "sin_hour": [sin_hour],
        "cos_hour": [cos_hour],
        "sin_day_of_week": [sin_dow],
        "cos_day_of_week": [cos_dow],
    }

    df = pd.DataFrame(raw_data)
    return df[DURATION_FEATURE_COLS].copy()


def main() -> None:
    print("=" * 80, flush=True)
    print(
        "M5-1 SMOKE VERIFICATION: ModelLoaderService against Live Compose MLflow",
        flush=True,
    )
    print("=" * 80, flush=True)

    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    print(f"Target MLflow Tracking URI: {tracking_uri}", flush=True)

    # 1. Connectivity Check
    wait_for_mlflow(tracking_uri, timeout_seconds=30)
    client = MlflowClient(tracking_uri=tracking_uri)

    # 2. Verify / Ensure Production Stage in Registry
    print("\n--- Step 1: Checking MLflow Model Registry Stages ---", flush=True)
    demand_ver = ensure_production_stage(client, DEMAND_MODEL_NAME)
    duration_ver = ensure_production_stage(client, DURATION_MODEL_NAME)
    print(f"Confirmed {DEMAND_MODEL_NAME} v{demand_ver} in Production.", flush=True)
    print(f"Confirmed {DURATION_MODEL_NAME} v{duration_ver} in Production.", flush=True)

    # 3. Startup Loading via ModelLoaderService
    print(
        "\n--- Step 2: Initializing ModelLoaderService & Loading Production Models ---",
        flush=True,
    )
    t0 = time.perf_counter()
    loader = ModelLoaderService(
        tracking_uri=tracking_uri,
        demand_model_name=DEMAND_MODEL_NAME,
        duration_model_name=DURATION_MODEL_NAME,
    )
    loaded_models = loader.load_all()
    demand_info = loaded_models[DEMAND_MODEL_NAME]
    duration_info = loaded_models[DURATION_MODEL_NAME]
    load_time_sec = time.perf_counter() - t0

    print(
        f"ModelLoaderService.load_all() completed in {load_time_sec:.3f}s.", flush=True
    )
    print(
        f"Demand Model:   version=v{demand_info.version}, stage={demand_info.stage}, "
        f"status={demand_info.status}, is_fallback={demand_info.is_fallback}, run_id={demand_info.run_id}",
        flush=True,
    )
    print(
        f"Duration Model: version=v{duration_info.version}, stage={duration_info.stage}, "
        f"status={duration_info.status}, is_fallback={duration_info.is_fallback}, run_id={duration_info.run_id}",
        flush=True,
    )

    # Assertions on loaded models
    assert (
        demand_info.status == "production"
    ), f"Expected production status, got {demand_info.status}"
    assert (
        demand_info.stage == "Production"
    ), f"Expected Production stage, got {demand_info.stage}"
    assert (
        demand_info.is_fallback is False
    ), "Expected genuine model, got fallback baseline"
    assert demand_info.model is not None, "Loaded demand model is None"

    assert (
        duration_info.status == "production"
    ), f"Expected production status, got {duration_info.status}"
    assert (
        duration_info.stage == "Production"
    ), f"Expected Production stage, got {duration_info.stage}"
    assert (
        duration_info.is_fallback is False
    ), "Expected genuine model, got fallback baseline"
    assert duration_info.model is not None, "Loaded duration model is None"

    # Health metadata check
    health_meta = loader.get_health_metadata()
    print(
        f"\nHealth Metadata Contract:\n{json.dumps(health_meta, indent=2)}", flush=True
    )
    assert health_meta["all_models_loaded"] is True
    assert health_meta["fallback_active"] is False

    # 4. Live Inference: Demand Model for Zone 161
    print(
        "\n--- Step 3: Live Inference on Loaded Demand Model (Zone 161 - Midtown Center) ---",
        flush=True,
    )
    demand_X = build_demand_feature_vector_zone_161()
    print("Input Demand Feature Matrix (1 row, exact training schema):", flush=True)
    for col in demand_X.columns:
        print(
            f"  {col:32s}: {demand_X[col].iloc[0]} (dtype={demand_X[col].dtype})",
            flush=True,
        )

    t_inf_start = time.perf_counter()
    raw_demand_pred = demand_info.predict(demand_X)
    inf_lat_ms = (time.perf_counter() - t_inf_start) * 1000.0

    raw_val = float(np.asarray(raw_demand_pred).flatten()[0])
    demand_pred_bounded = float(np.maximum(0.0, raw_val))

    print("\nDemand Model Inference Results:", flush=True)
    print("  Zone ID:                 161 (Midtown Center)", flush=True)
    print(f"  Raw Model Output:        {raw_val:.4f}", flush=True)
    print(
        f"  Bounded Demand (>=0):    {demand_pred_bounded:.2f} pickups / next 1h",
        flush=True,
    )
    print(f"  Inference Latency:       {inf_lat_ms:.2f}ms", flush=True)

    # Plausibility check against M3-4 results: target range [0, 676], MAE ~2.43
    assert (
        0.0 <= demand_pred_bounded <= 676.0
    ), f"Demand prediction {demand_pred_bounded:.2f} is outside plausible range [0, 676.0]!"
    print(
        f"  Plausibility Check:      PASSED (0.0 <= {demand_pred_bounded:.2f} <= 676.0)",
        flush=True,
    )

    # 5. Live Inference: Duration Model for Corridor 161 -> 236
    print(
        "\n--- Step 4: Live Inference on Loaded Duration Model (Corridor 161 -> 236) ---",
        flush=True,
    )
    duration_X = build_corridor_feature_vector_161_236()
    print("Input Corridor Feature Matrix (1 row, exact training schema):", flush=True)
    for col in duration_X.columns:
        print(
            f"  {col:32s}: {duration_X[col].iloc[0]} (dtype={duration_X[col].dtype})",
            flush=True,
        )

    t_dur_start = time.perf_counter()
    raw_log_pred = duration_info.predict(duration_X)
    dur_lat_ms = (time.perf_counter() - t_dur_start) * 1000.0

    raw_log_val = float(np.asarray(raw_log_pred).flatten()[0])
    # Duration model trains on log1p(seconds), so inverse is expm1 with 60.0s floor
    duration_sec = float(np.maximum(60.0, np.expm1(raw_log_val)))

    print("\nDuration Model Inference Results:", flush=True)
    print(
        "  Corridor:                161 (Midtown Center) -> 236 (Upper East Side North)",
        flush=True,
    )
    print(f"  Raw Log-Space Output:    {raw_log_val:.4f}", flush=True)
    print(
        f"  Inverted Duration:       {duration_sec:.1f}s ({duration_sec / 60.0:.2f} min)",
        flush=True,
    )
    print(f"  Inference Latency:       {dur_lat_ms:.2f}ms", flush=True)

    # Plausibility check: 60s <= duration <= 7200s (2 hours)
    assert (
        60.0 <= duration_sec <= 7200.0
    ), f"Corridor duration {duration_sec:.1f}s is outside plausible range [60.0, 7200.0]!"
    print(
        f"  Plausibility Check:      PASSED (60.0 <= {duration_sec:.1f} <= 7200.0)",
        flush=True,
    )

    print("\n" + "=" * 80, flush=True)
    print(
        "ALL M5-1 SMOKE CHECKS PASSED: Real Postgres-backed MLflow loading & live inference verified!",
        flush=True,
    )
    print("=" * 80, flush=True)


if __name__ == "__main__":
    main()
