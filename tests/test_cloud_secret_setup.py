"""Credential handoff/preflight contracts; never contact a provider."""

from unittest.mock import patch

import pytest

from scripts.cloud_preflight import REQUIRED, prepare
from scripts.configure_cloud_secrets import upload
from src.features.config import feast_redis_connection


def configure(monkeypatch):
    for name in REQUIRED:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.delenv("VALKEY_CA_PEM", raising=False)
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql://user:password@db.example:1234/logistics?sslmode=verify-full",
    )
    monkeypatch.setenv("REDIS_URL", "rediss://default:password@cache.example:2345/0")
    monkeypatch.setenv("REDPANDA_BROKER", "broker.example:3456")
    for name in ("KAFKA_CA_PEM", "KAFKA_CERT_PEM", "KAFKA_KEY_PEM", "POSTGRES_CA_PEM"):
        monkeypatch.setenv(name, "-----BEGIN TEST-----\nfixture\n-----END TEST-----")


def test_aiven_preflight_works_before_mlflow_with_system_valkey_trust(
    monkeypatch, tmp_path
):
    configure(monkeypatch)
    exports = prepare(tmp_path / "certs", aiven_only=True)
    assert exports["KAFKA_SECURITY_PROTOCOL"] == "SSL"
    assert "REDIS_SSL_CA" not in exports
    assert len(list((tmp_path / "certs").glob("*.pem"))) == 4
    connection = feast_redis_connection(
        "rediss://default:password@cache.example:2345/0"
    )
    assert 'ssl_cert_reqs="required"' in connection
    assert "ssl_check_hostname=true" in connection
    assert "ssl_ca_certs" not in connection
    with pytest.raises(ValueError, match="MLFLOW_TRACKING_URI"):
        prepare(tmp_path / "full")


def test_secret_upload_uses_stdin_and_suppresses_provider_output(capsys):
    value = "private-fixture-value"
    with patch("scripts.configure_cloud_secrets.subprocess.run") as run:
        run.return_value.returncode = 0
        upload("CLOUD_DATABASE_URL", value)
        args, kwargs = run.call_args
        assert value not in args[0]
        assert kwargs["input"] == value
        assert kwargs["capture_output"] is True
        assert "free-cloud" in args[0]
        assert value not in capsys.readouterr().out
        run.return_value.returncode = 1
        run.return_value.stderr = value
        with pytest.raises(RuntimeError) as error:
            upload("CLOUD_DATABASE_URL", value)
        assert value not in str(error.value)


def test_preflight_rejects_url_that_disables_valkey_tls_checks(monkeypatch, tmp_path):
    configure(monkeypatch)
    monkeypatch.setenv(
        "REDIS_URL", "rediss://default:password@cache.example:2345/0?ssl_cert_reqs=none"
    )
    with pytest.raises(ValueError, match="must not disable"):
        prepare(tmp_path / "certs", aiven_only=True)
