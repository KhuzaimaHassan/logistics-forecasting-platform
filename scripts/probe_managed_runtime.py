"""Real native-model HTTP inference in the constrained API image; no public URL proof."""

import json
import math
import os
import resource
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.request import Request, urlopen

import uvicorn
from sqlalchemy import text

import src.agents.graph  # noqa: F401 - include lazy agent imports in memory peak
from src.common.db import get_engine
from src.serving.app import app


def request(path, payload=None):
    body = json.dumps(payload).encode() if payload is not None else None
    req = Request(
        "http://127.0.0.1:8080" + path,
        data=body,
        headers={"Content-Type": "application/json"},
    )
    with urlopen(req, timeout=120) as response:
        return json.load(response)


def main():
    result = {
        "scope": "Real DagsHub Production models + Aiven features/storage in disposable Linux API container; not a hosted Render endpoint",
        "status": "FAILED",
    }
    server = uvicorn.Server(
        uvicorn.Config(
            app, host="127.0.0.1", port=8080, log_level="warning", limit_concurrency=8
        )
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        result["phase"] = "readiness"
        deadline = time.monotonic() + 240
        while True:
            try:
                health = request("/health/ready")
                break
            except Exception:
                if time.monotonic() > deadline:
                    raise TimeoutError(
                        "Real model/managed dependencies did not become ready"
                    ) from None
                time.sleep(3)
        assert health["status"] == "ok"
        assert all(not model["is_fallback"] for model in health["models"].values())
        result["health"] = health
        result["phase"] = "reference_data"
        zones = request("/zones")["zones"]
        observed = [
            zone["zone_id"] for zone in zones if zone.get("has_observed_features")
        ]
        assert len(zones) == 263 and observed
        state = request("/deployment/status")
        origin, dest = state["observations"]["available_corridors"][0].split("_")
        with get_engine().connect() as conn:
            before = conn.execute(
                text("SELECT count(*) FROM warehouse.predictions")
            ).scalar_one()
        result["phase"] = "demand_eta"
        result["demand"] = request(f"/predict/demand/{observed[0]}?horizon_minutes=60")
        result["eta"] = request(f"/predict/eta?origin={origin}&dest={dest}")
        for prediction, field in (
            (result["demand"], "predicted_pickups"),
            (result["eta"], "predicted_duration_seconds"),
        ):
            assert prediction["status"] == "ok" and prediction["data_as_of"]
            assert prediction["model_version"] != "baseline-fallback"
            assert math.isfinite(prediction[field])

        def batch(_):
            data = request(
                "/predict/demand/batch", {"zone_ids": observed, "horizon_minutes": 60}
            )
            assert len(data["predictions"]) == len(observed)
            assert all(
                item["status"] == "ok"
                and item["data_as_of"]
                and math.isfinite(item["predicted_pickups"])
                for item in data["predictions"]
            )
            return data["prediction_count"]

        result["phase"] = "concurrent_batch"
        with ThreadPoolExecutor(max_workers=4) as pool:
            counts = list(pool.map(batch, range(4)))
        with get_engine().connect() as conn:
            after = conn.execute(
                text("SELECT count(*) FROM warehouse.predictions")
            ).scalar_one()
        assert after - before >= 2 + sum(counts)
        result.update(
            concurrent_requests=4,
            zones_per_batch=len(observed),
            persisted_prediction_delta=after - before,
        )
        result["peak_rss_mib"] = round(
            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1
        )
        peak = Path("/sys/fs/cgroup/memory.peak")
        if peak.exists():
            result["cgroup_peak_mib"] = round(int(peak.read_text()) / 1024**2, 1)
        assert result["peak_rss_mib"] < 480, "Real model memory exceeds headroom gate"
        result["status"] = "VERIFIED"
        result["phase"] = "completed"
    except Exception as exc:
        result["error_type"] = type(exc).__name__
        if isinstance(getattr(exc, "code", None), int):
            result["http_status"] = exc.code
    finally:
        server.should_exit = True
        thread.join(timeout=15)
        path = Path("/evidence") / (
            "runtime-" + os.getenv("RUNTIME_PROBE_ATTEMPT", "1") + ".json"
        )
        path.write_text(json.dumps(result, indent=2, default=str) + "\n")
        print(json.dumps(result, indent=2, default=str))
    if result["status"] != "VERIFIED":
        raise SystemExit("Managed runtime check failed; see sanitized evidence")


if __name__ == "__main__":
    main()
