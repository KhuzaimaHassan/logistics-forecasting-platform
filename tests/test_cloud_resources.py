"""Additive initialization contracts; no provider credentials or connections."""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.orchestration.cloud_resources import ensure_database, ensure_topics


def test_database_initialization_preserves_defaultdb_and_tls():
    connection = MagicMock()
    connection.execute.return_value.scalar.return_value = None
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = connection
    with (
        patch(
            "src.orchestration.cloud_resources.get_settings",
            return_value=SimpleNamespace(
                database_url="postgresql://user:password@host/logistics?sslmode=verify-full&sslrootcert=/ca.pem"
            ),
        ),
        patch(
            "src.orchestration.cloud_resources.create_engine", return_value=engine
        ) as create,
    ):
        assert ensure_database()["created"] is True
        url = create.call_args.args[0]
        assert url.database == "defaultdb" and url.query["sslmode"] == "verify-full"
        statements = [str(call.args[0]) for call in connection.execute.call_args_list]
        assert statements[-1] == 'CREATE DATABASE "logistics"'
        assert not any("DROP" in query for query in statements)
        connection.execute.reset_mock()
        connection.execute.return_value.scalar.return_value = 1
        assert ensure_database()["created"] is False
        assert connection.execute.call_count == 1


def test_topics_create_only_missing_with_topology_replication_and_no_overrides():
    admin = MagicMock()
    admin.list_topics.return_value = ["trip.events"]
    admin.describe_cluster.return_value = {"brokers": [{}, {}, {}]}
    admin.create_topics.return_value.topic_errors = [
        ("trip.events.deadletter", 0, None)
    ]
    admin.describe_topics.return_value = [
        {
            "topic": name,
            "error_code": 0,
            "partitions": [{"replicas": [1, 2, 3]}, {"replicas": [1, 2, 3]}],
        }
        for name in ("trip.events", "trip.events.deadletter")
    ]
    with patch(
        "src.orchestration.cloud_resources.get_admin_client", return_value=admin
    ):
        result = ensure_topics()
        new = admin.create_topics.call_args.args[0]
        assert len(new) == 1 and new[0].name == "trip.events.deadletter"
        assert new[0].num_partitions == 2 and new[0].replication_factor == 3
        assert new[0].topic_configs == {}
        assert result["trip.events"]["created"] is False
        admin.create_topics.return_value.topic_errors = [
            ("trip.events.deadletter", 29, "sensitive provider output")
        ]
        with pytest.raises(Exception) as error:
            ensure_topics()
        assert "sensitive provider output" not in str(error.value)


def test_bootstrap_missing_credentials_never_attempts_provider_calls(
    monkeypatch, tmp_path
):
    from scripts import verify_aiven_services

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.argv", ["verify_aiven_services.py", "--bootstrap"])
    for name in (
        "DATABASE_URL",
        "POSTGRES_SSLROOTCERT",
        "REDIS_URL",
        "REDPANDA_BROKER",
        "KAFKA_SSL_CAFILE",
        "KAFKA_SSL_CERTFILE",
        "KAFKA_SSL_KEYFILE",
    ):
        monkeypatch.delenv(name, raising=False)
    with (
        patch.object(verify_aiven_services, "get_engine") as engine,
        patch.object(verify_aiven_services, "get_kafka_producer") as producer,
    ):
        with pytest.raises(SystemExit) as exit_code:
            verify_aiven_services.main()
        assert exit_code.value.code == 1
        engine.assert_not_called()
        producer.assert_not_called()
    results = json.loads((tmp_path / "artifacts/aiven-verification.json").read_text())
    assert len(results["checks"]) == 5
    assert all(row["status"] == "NOT VERIFIED" for row in results["checks"].values())


def test_mlflow_only_missing_credentials_does_not_call_other_services(monkeypatch):
    from scripts.verify_cloud_deployment import run_checks

    for name in (
        "MLFLOW_TRACKING_URI",
        "MLFLOW_TRACKING_USERNAME",
        "MLFLOW_TRACKING_PASSWORD",
    ):
        monkeypatch.delenv(name, raising=False)
    with patch("src.common.db.get_engine") as engine:
        result = run_checks(only={"mlflow"})
        engine.assert_not_called()
    assert result["checks"]["mlflow"]["status"] == "NOT VERIFIED"
    assert "postgresql" not in result["checks"]


@pytest.mark.parametrize(
    "flag,check",
    [
        ("--only-mlflow", "mlflow"),
        ("--only-api", "fastapi_real_model"),
        ("--only-agent", "llm_agent"),
    ],
)
def test_partial_success_never_marks_full_acceptance_complete(
    monkeypatch, tmp_path, flag, check
):
    from scripts import verify_cloud_deployment

    output = tmp_path / "mlflow.json"
    monkeypatch.setattr("sys.argv", ["verify", flag, "--output", str(output)])
    with patch.object(
        verify_cloud_deployment,
        "run_checks",
        return_value={"checks": {check: {"status": "VERIFIED"}}},
    ):
        with pytest.raises(SystemExit) as exit_code:
            verify_cloud_deployment.main()
        assert exit_code.value.code == 0
    result = json.loads(output.read_text())
    assert result["selected_checks_complete"] is True
    assert result["automated_checks_complete"] is False
    assert result["manual_acceptance_complete"] is False
