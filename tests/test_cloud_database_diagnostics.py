"""Database diagnostics must be read-only and never disclose raw SQL."""

import json
from unittest.mock import MagicMock

from scripts.diagnose_cloud_database import collect


def test_diagnostics_enforce_readonly_and_return_only_sanitized_fields():
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = [
        {
            "pid": 42,
            "state": "active",
            "wait_event_type": None,
            "wait_event": None,
            "elapsed_seconds": 300,
            "operation": "Feast PIT join",
        }
    ]
    cursor.fetchone.return_value = {"database_bytes": 36312767}
    result = collect(connection)
    connection.set_session.assert_called_once_with(readonly=True, autocommit=False)
    statements = [call.args[0].strip() for call in cursor.execute.call_args_list]
    assert statements[0] == "SET LOCAL statement_timeout = '10s'"
    assert all(query.startswith("SELECT") for query in statements[1:])
    assert "pg_cancel_backend" not in " ".join(statements)
    assert result["sessions"][0]["operation"] == "Feast PIT join"
    assert "query" not in json.dumps(result)
    assert result["status"] == "VERIFIED"
