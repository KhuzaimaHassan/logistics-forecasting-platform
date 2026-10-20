"""Offline manifest invariants; no providers, credentials or source downloads."""

from pathlib import Path

import yaml


def main():
    manifest = yaml.safe_load(Path("render.yaml").read_text())
    assert len(manifest["services"]) == 1
    service = manifest["services"][0]
    assert service["type"] == "web" and service["plan"] == "free"
    assert "disks" not in service and "databases" not in manifest
    assert service["healthCheckPath"] == "/health/live"
    values = {value["key"]: value for value in service["envVars"]}
    for key in (
        "DATABASE_URL",
        "REDIS_URL",
        "MLFLOW_TRACKING_PASSWORD",
        "AGENT_API_KEY",
    ):
        assert values[key].get("sync") is False
    assert values["DEPLOYMENT_MODE"]["value"] == "cloud"
    assert values["MLFLOW_MODEL_LOAD_MODE"]["value"] == "native"
    for path in Path(".github/workflows").glob("*.yml"):
        yaml.safe_load(path.read_text())
    docker = Path("src/serving/Dockerfile.cloud").read_text()
    assert "--extra cloud-serving" in docker and "--extra training" not in docker
    assert "USER app" in docker
    print(
        "VERIFIED: offline free-plan, secret indirection and dependency manifest checks"
    )


if __name__ == "__main__":
    main()
