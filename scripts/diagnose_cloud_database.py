"""Read-only managed PostgreSQL timing diagnostics; never print SQL or URLs."""

import json
import os
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import parse_qs, urlsplit

import psycopg2
from psycopg2.extras import RealDictCursor


def collect(connection):
    connection.set_session(readonly=True, autocommit=False)
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("SET LOCAL statement_timeout = '10s'")
        cursor.execute("""
            SELECT pid, state, wait_event_type, wait_event,
                   EXTRACT(EPOCH FROM clock_timestamp()-query_start)::int AS elapsed_seconds,
                   CASE WHEN query ILIKE '%entity_dataframe%' THEN 'Feast PIT join'
                        WHEN query ILIKE '%INSERT INTO feast_entity%' THEN 'Feast entity upload'
                        ELSE 'other' END AS operation
            FROM pg_stat_activity
            WHERE datname=current_database() AND pid <> pg_backend_pid()
            ORDER BY query_start
        """)
        sessions = [dict(row) for row in cursor.fetchall()]
        cursor.execute("SELECT pg_database_size(current_database()) AS database_bytes")
        capacity = dict(cursor.fetchone())
    return {
        "status": "VERIFIED",
        "scope": "Read-only database diagnostics; no model or deployment acceptance",
        "sessions": sessions,
        **capacity,
    }


def main():
    result = {"status": "FAILED", "scope": "Read-only database diagnostics"}
    try:
        url = os.environ["DATABASE_URL"]
        parsed = urlsplit(url)
        if parsed.scheme not in {"postgresql", "postgresql+psycopg2"} or parse_qs(
            parsed.query
        ).get("sslmode") != ["verify-full"]:
            raise ValueError("Verified PostgreSQL TLS required")
        with TemporaryDirectory() as directory:
            ca = Path(directory) / "postgres-ca.pem"
            ca.write_text(os.environ["POSTGRES_CA_PEM"], encoding="utf-8")
            ca.chmod(0o600)
            with closing(
                psycopg2.connect(
                    url.replace("postgresql+psycopg2://", "postgresql://", 1),
                    sslrootcert=str(ca),
                    connect_timeout=10,
                )
            ) as connection:
                result = collect(connection)
    except Exception as exc:
        result["error_type"] = type(exc).__name__
    result["generated_at"] = datetime.now(timezone.utc).isoformat()
    path = Path("artifacts/database-diagnostics.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result))
    return 0 if result["status"] == "VERIFIED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
