"""Live verification script for Milestone 5-1: ModelLoaderService against real MLflow HTTP server.

Demonstrates:
1. Spawning a genuine live MLflow tracking server over an HTTP network socket (port 5005).
2. Registering and transitioning genuine LightGBM models for Demand and Duration to 'Production'.
3. Real startup loading by ModelLoaderService over HTTP from the active registry.
4. Live inference execution and warmup validation.
5. Graceful fallback to baseline heuristics when tracking server is unreachable.
6. Health metadata contract verification.
"""

import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import warnings

# Insulate against Windows console encoding errors when MLflow emits Unicode run icons
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
os.environ["PYTHONIOENCODING"] = "utf-8"

import lightgbm as lgb
import mlflow
import mlflow.lightgbm
import numpy as np
import pandas as pd
import requests
from mlflow.tracking import MlflowClient

from src.serving.model_loader import ModelLoaderService
from src.training.pipeline import DEMAND_MODEL_NAME, DURATION_MODEL_NAME


def get_free_port(preferred: int = 5005) -> int:
    """Find a free TCP port on localhost, preferring standard test ports."""
    for port in [preferred, 5006, 5007, 5008]:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("127.0.0.1", port))
                return port
        except OSError:
            continue
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_server(uri: str, timeout_seconds: int = 45) -> bool:
    """Poll MLflow server health endpoint until responsive."""
    health_url = f"{uri}/health"
    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        try:
            resp = requests.get(health_url, timeout=2)
            if resp.status_code == 200:
                return True
        except requests.RequestException:
            pass
        time.sleep(0.5)
    return False


def train_and_register_live_model(
    tracking_uri: str,
    model_name: str,
    feature_cols: list[str],
    is_duration: bool = False,
) -> tuple[str, str]:
    """Train a real LightGBM model and register it in 'Production' stage in live MLflow."""
    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient(tracking_uri=tracking_uri)

    exp_name = f"verify_{model_name}"
    exp = client.get_experiment_by_name(exp_name)
    if exp is None:
        exp_id = client.create_experiment(exp_name)
    else:
        exp_id = exp.experiment_id

    n_samples = 60
    data = {c: np.random.randn(n_samples) for c in feature_cols}
    if not is_duration:
        data["zone_id"] = np.random.randint(1, 264, size=n_samples)
        y = np.random.uniform(5.0, 80.0, size=n_samples)
    else:
        data["pickup_zone_id"] = np.random.randint(1, 264, size=n_samples)
        data["dropoff_zone_id"] = np.random.randint(1, 264, size=n_samples)
        y = np.random.uniform(300.0, 2400.0, size=n_samples)

    df_train = pd.DataFrame(data)
    cat_cols = ["zone_id"] if not is_duration else ["pickup_zone_id", "dropoff_zone_id"]
    for cat in cat_cols:
        if cat in df_train.columns:
            df_train[cat] = df_train[cat].astype("category")

    model = lgb.LGBMRegressor(n_estimators=10, min_child_samples=2, verbose=-1)
    model.fit(df_train, y)

    with mlflow.start_run(experiment_id=exp_id) as run:
        run_id = run.info.run_id
        mlflow.log_param("n_estimators", 10)
        mlflow.log_metric("train_rmse", 0.42)
        mlflow.lightgbm.log_model(
            lgb_model=model.booster_,
            artifact_path="model",
            registered_model_name=model_name,
            pip_requirements=["lightgbm>=4.3.0"],
        )

    # Transition to Production stage
    versions = client.get_latest_versions(model_name)
    version = versions[0].version
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning, module="mlflow.*")
        client.transition_model_version_stage(
            name=model_name,
            version=version,
            stage="Production",
            archive_existing_versions=True,
        )

    return run_id, str(version)


def main():
    print("=" * 80)
    print("M5-1 LIVE VERIFICATION: ModelLoaderService Startup Loading via Real MLflow")
    print("=" * 80)

    temp_dir = tempfile.mkdtemp(prefix="mlflow_live_test_")
    db_file = os.path.join(temp_dir, "mlflow_live.db")
    artifacts_dir = os.path.join(temp_dir, "artifacts")
    os.makedirs(artifacts_dir, exist_ok=True)
    artifacts_uri = f"file:///{os.path.abspath(artifacts_dir).replace(os.sep, '/')}"
    backend_uri = f"sqlite:///{os.path.abspath(db_file).replace(os.sep, '/')}"

    port = get_free_port()
    tracking_uri = f"http://127.0.0.1:{port}"

    server_process = None
    server_log_file = None
    try:
        # -------------------------------------------------------------
        # Stage 1: Spawn real live MLflow HTTP tracking server
        # -------------------------------------------------------------
        print(
            f"\n[Stage 1/5] Launching live MLflow tracking server on {tracking_uri}..."
        )
        cmd = [
            sys.executable,
            "-m",
            "mlflow",
            "server",
            "--backend-store-uri",
            backend_uri,
            "--default-artifact-root",
            artifacts_uri,
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ]
        server_log_path = os.path.join(temp_dir, "server.log")
        server_log_file = open(server_log_path, "w")
        server_process = subprocess.Popen(
            cmd,
            stdout=server_log_file,
            stderr=server_log_file,
            text=True,
        )

        ready = wait_for_server(tracking_uri, timeout_seconds=45)
        if not ready:
            server_log_file.flush()
            with open(server_log_path, "r") as lf:
                err = lf.read()
            raise RuntimeError(
                f"MLflow server failed to start on {tracking_uri}. Error:\n{err}"
            )

        print(f"  [OK] Live MLflow server running (PID: {server_process.pid})")

        # -------------------------------------------------------------
        # Stage 2: Register & promote LightGBM models in live MLflow
        # -------------------------------------------------------------
        print(
            "\n[Stage 2/5] Training and registering Production models via HTTP API..."
        )

        demand_cols = [
            "zone_id",
            "pickup_count_last_15m",
            "pickup_count_last_1h",
            "pickup_count_last_24h",
            "pickup_count_same_hour_last_week",
            "hour_of_day",
            "day_of_week",
            "is_weekend",
            "is_holiday",
            "sin_hour",
            "cos_hour",
            "sin_day_of_week",
            "cos_day_of_week",
        ]
        demand_run_id, demand_v = train_and_register_live_model(
            tracking_uri=tracking_uri,
            model_name=DEMAND_MODEL_NAME,
            feature_cols=demand_cols,
            is_duration=False,
        )
        print(
            f"  [OK] Registered {DEMAND_MODEL_NAME} v{demand_v} (run_id={demand_run_id[:8]}) -> 'Production'"
        )

        duration_cols = [
            "pickup_zone_id",
            "dropoff_zone_id",
            "avg_duration_last_15m",
            "avg_duration_last_1h",
            "log_avg_duration_last_1h",
            "distance_km",
            "origin_zone_demand_pressure",
            "hour_of_day",
            "day_of_week",
            "is_weekend",
            "sin_hour",
            "cos_hour",
            "sin_day_of_week",
            "cos_day_of_week",
        ]
        dur_run_id, dur_v = train_and_register_live_model(
            tracking_uri=tracking_uri,
            model_name=DURATION_MODEL_NAME,
            feature_cols=duration_cols,
            is_duration=True,
        )
        print(
            f"  [OK] Registered {DURATION_MODEL_NAME} v{dur_v} (run_id={dur_run_id[:8]}) -> 'Production'"
        )

        # -------------------------------------------------------------
        # Stage 3: Load production models via ModelLoaderService
        # -------------------------------------------------------------
        print(
            f"\n[Stage 3/5] Instantiating ModelLoaderService pointed at {tracking_uri}..."
        )
        loader = ModelLoaderService(tracking_uri=tracking_uri)
        models = loader.load_all()

        assert DEMAND_MODEL_NAME in models, "Demand model not loaded!"
        assert DURATION_MODEL_NAME in models, "Duration model not loaded!"

        demand_info = models[DEMAND_MODEL_NAME]
        dur_info = models[DURATION_MODEL_NAME]

        print(
            f"  [Demand Model]   status={demand_info.status}, stage={demand_info.stage}, v={demand_info.version}, fallback={demand_info.is_fallback}"
        )
        print(
            f"  [Duration Model] status={dur_info.status}, stage={dur_info.stage}, v={dur_info.version}, fallback={dur_info.is_fallback}"
        )

        assert demand_info.status == "production"
        assert demand_info.is_fallback is False
        assert dur_info.status == "production"
        assert dur_info.is_fallback is False

        # Live prediction execution
        test_demand_df = pd.DataFrame(
            [
                {
                    "zone_id": 161,
                    "pickup_count_last_15m": 14,
                    "pickup_count_last_1h": 48,
                    "pickup_count_last_24h": 910,
                    "pickup_count_same_hour_last_week": 42,
                    "hour_of_day": 15,
                    "day_of_week": 2,
                    "is_weekend": 0,
                    "is_holiday": 0,
                    "sin_hour": 0.707,
                    "cos_hour": -0.707,
                    "sin_day_of_week": 0.97,
                    "cos_day_of_week": -0.22,
                }
            ]
        )
        test_demand_df["zone_id"] = test_demand_df["zone_id"].astype("category")
        pred_demand = demand_info.predict(test_demand_df)
        print(
            f"  [Live Inference] Demand prediction for Zone 161: {pred_demand[0]:.2f} pickups"
        )
        assert pred_demand[0] > 0

        test_dur_df = pd.DataFrame(
            [
                {
                    "pickup_zone_id": 161,
                    "dropoff_zone_id": 236,
                    "avg_duration_last_15m": 880.0,
                    "avg_duration_last_1h": 920.0,
                    "log_avg_duration_last_1h": np.log1p(920.0),
                    "distance_km": 4.5,
                    "origin_zone_demand_pressure": 48,
                    "hour_of_day": 15,
                    "day_of_week": 2,
                    "is_weekend": 0,
                    "sin_hour": 0.707,
                    "cos_hour": -0.707,
                    "sin_day_of_week": 0.97,
                    "cos_day_of_week": -0.22,
                }
            ]
        )
        test_dur_df["pickup_zone_id"] = test_dur_df["pickup_zone_id"].astype("category")
        test_dur_df["dropoff_zone_id"] = test_dur_df["dropoff_zone_id"].astype(
            "category"
        )
        pred_dur = dur_info.predict(test_dur_df)
        print(
            f"  [Live Inference] Duration prediction for Corridor (161->236): {pred_dur[0]:.1f} seconds ({pred_dur[0]/60.0:.1f} mins)"
        )
        assert pred_dur[0] > 0

        # -------------------------------------------------------------
        # Stage 4: Test Graceful Baseline Fallback on Server Disconnect
        # -------------------------------------------------------------
        print(
            "\n[Stage 4/5] Testing graceful baseline fallback against unreachable tracking URI..."
        )
        os.environ["MLFLOW_HTTP_REQUEST_MAX_RETRIES"] = "0"
        os.environ["MLFLOW_HTTP_REQUEST_TIMEOUT"] = "1"
        fallback_loader = ModelLoaderService(tracking_uri="http://127.0.0.1:59999")
        fallback_models = fallback_loader.load_all()

        fb_demand = fallback_models[DEMAND_MODEL_NAME]
        fb_dur = fallback_models[DURATION_MODEL_NAME]
        print(
            f"  [Fallback Demand]   status={fb_demand.status}, fallback={fb_demand.is_fallback}, version={fb_demand.version}"
        )
        print(
            f"  [Fallback Duration] status={fb_dur.status}, fallback={fb_dur.is_fallback}, version={fb_dur.version}"
        )

        assert fb_demand.is_fallback is True
        assert fb_demand.status == "baseline_fallback"
        assert fb_dur.is_fallback is True
        assert fb_dur.status == "baseline_fallback"

        fb_pred_demand = fb_demand.predict(
            pd.DataFrame([{"pickup_count_same_hour_last_week": 25.0}])
        )
        fb_pred_dur = fb_dur.predict(pd.DataFrame([{"avg_duration_last_1h": 720.0}]))
        print(
            f"  [Fallback Inference] Demand: {fb_pred_demand[0]} pickups | Duration: {fb_pred_dur[0]}s"
        )
        assert fb_pred_demand[0] == 25.0
        assert fb_pred_dur[0] == 720.0

        # -------------------------------------------------------------
        # Stage 5: Verify Health Metadata Contract
        # -------------------------------------------------------------
        print("\n[Stage 5/5] Verifying /health check metadata contract...")
        health_meta = loader.get_health_metadata()
        print(f"  all_models_loaded: {health_meta['all_models_loaded']}")
        print(f"  has_fallback_models: {health_meta['has_fallback_models']}")
        print(f"  demand model info: {health_meta['models'][DEMAND_MODEL_NAME]}")
        print(f"  duration model info: {health_meta['models'][DURATION_MODEL_NAME]}")

        assert health_meta["all_models_loaded"] is True
        assert health_meta["has_fallback_models"] is False
        assert health_meta["models"][DEMAND_MODEL_NAME]["status"] == "production"
        assert health_meta["models"][DURATION_MODEL_NAME]["status"] == "production"

        print("\n" + "=" * 80)
        print("ALL VERIFICATION CHECKS PASSED: M5-1 Real Live Model Loading Proven!")
        print("=" * 80)

    finally:
        if server_process is not None:
            print("\nShutting down live MLflow server process...")
            server_process.terminate()
            try:
                server_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server_process.kill()
            print("MLflow server stopped.")

        if server_log_file is not None and not server_log_file.closed:
            server_log_file.close()

        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
