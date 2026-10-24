"""HEAD monitors must preserve readiness failures without returning a body."""

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from src.serving import app as serving


@pytest.mark.parametrize("method", ["get", "head"])
@pytest.mark.parametrize("failure", [None, "database", "redis", "model"])
def test_readiness_methods_preserve_dependency_failures(monkeypatch, method, failure):
    database = MagicMock(return_value=failure != "database")
    monkeypatch.setattr(serving, "check_database_connection", database)
    redis_client = MagicMock()
    redis_client.ping.return_value = failure != "redis"
    monkeypatch.setattr(serving.redis, "from_url", lambda *a, **kw: redis_client)
    loader = MagicMock()
    loader.get_health_metadata.return_value = {
        "models": {},
        "all_models_loaded": failure != "model",
        "has_fallback_models": failure == "model",
    }
    monkeypatch.setattr(serving.app.state, "model_loader", loader, raising=False)

    response = getattr(TestClient(serving.app), method)("/health/ready")

    assert response.status_code == (503 if failure else 200)
    database.assert_called_once_with()
    redis_client.ping.assert_called_once_with()
    loader.get_health_metadata.assert_called_once_with()
    if method == "head":
        assert response.content == b""
    else:
        assert response.json()["status"] == ("degraded" if failure else "ok")


def test_head_liveness_does_not_probe_dependencies(monkeypatch):
    probe = MagicMock(side_effect=AssertionError("liveness contacted dependencies"))
    monkeypatch.setattr(serving, "health_check", probe)
    response = TestClient(serving.app).head("/health/live")
    assert response.status_code == 200
    assert response.content == b""
    probe.assert_not_called()


def test_head_legacy_health_preserves_get_status(monkeypatch):
    probe = MagicMock(return_value=MagicMock(status="degraded"))
    monkeypatch.setattr(serving, "health_check", probe)
    response = TestClient(serving.app).head("/health")
    assert response.status_code == 200
    assert response.content == b""
    probe.assert_called_once()
