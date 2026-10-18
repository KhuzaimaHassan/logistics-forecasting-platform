"""Additive initialization contracts; no provider credentials or connections."""

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
