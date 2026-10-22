"""Database diagnostics must be read-only and never disclose raw SQL."""

import json
from unittest.mock import MagicMock

from scripts.diagnose_cloud_database import collect


def test_diagnostics_enforce_readonly_and_return_only_sanitized_fields():
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchall.side_effect = [
        [
            {
                "pid": 42,
                "state": "active",
                "wait_event_type": None,
                "wait_event": None,
                "elapsed_seconds": 300,
                "operation": "Feast PIT join",
            }
        ],
        [{"name": "work_mem", "setting": "4096", "unit": "kB", "source": "default"}],
        [{"schemaname": "warehouse", "relname": "trips", "total_bytes": 1048576}],
    ]
    cursor.fetchone.side_effect = [
        {"database_bytes": 36312767},
        {"numbackends": 1, "temp_bytes": 1234, "temp_files": 2},
    ]
    result = collect(connection)
    connection.set_session.assert_called_once_with(readonly=True, autocommit=False)
    statements = [call.args[0].strip() for call in cursor.execute.call_args_list]
    assert statements[0] == "SET LOCAL statement_timeout = '10s'"
    assert all(query.startswith("SELECT") for query in statements[1:])
    assert "pg_cancel_backend" not in " ".join(statements)
    assert result["sessions"][0]["operation"] == "Feast PIT join"
    assert '"query"' not in json.dumps(result)
    assert result["settings"][0]["name"] == "work_mem"
    assert result["statistics"]["temp_bytes"] == 1234
    assert result["largest_relations"][0]["relname"] == "trips"
    assert "not RAM telemetry" in result["limitations"]
    assert "LIMIT 15" in statements[-1]
    assert result["status"] == "VERIFIED"
