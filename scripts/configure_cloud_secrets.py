"""Run interactively in a private terminal; secrets go to gh through stdin only."""

import argparse
import getpass
import subprocess
import sys
from pathlib import Path
from urllib.parse import quote

REPOSITORY = "KhuzaimaHassan/logistics-forecasting-platform"


def upload(name, value):
    if not value.strip():
        raise ValueError(f"{name} must not be empty")
    result = subprocess.run(
        ["gh", "secret", "set", name, "--repo", REPOSITORY, "--env", "free-cloud"],
        input=value,
        text=True,
        capture_output=True,
    )
    if result.returncode:
        raise RuntimeError(
            f"Secret upload failed for {name}; provider output suppressed"
        )
    print(f"Configured {name} (value hidden)")


def connection(scheme, database):
    host = input("Service hostname (no scheme/port): ").strip()
    port = int(input("Service port from Console: "))
    if not host or any(char in host for char in "/:@ ") or not 0 < port < 65536:
        raise ValueError("Invalid hostname or port")
    user = input("Service username: ").strip()
    password = getpass.getpass("Service password (hidden): ")
    if not user or not password:
        raise ValueError("Username and password are required")
    return f"{scheme}://{quote(user, safe='')}:{quote(password, safe='')}@{host}:{port}/{database}"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("group", choices=["aiven", "mlflow", "agent"])
    args = parser.parse_args()
    if not sys.stdin.isatty():
        raise ValueError(
            "Run in a private interactive terminal; hidden prompts require a TTY"
        )
    if args.group == "aiven":
        print("PostgreSQL: use the existing logistics database.")
        upload(
            "CLOUD_DATABASE_URL",
            connection("postgresql+psycopg2", "logistics") + "?sslmode=verify-full",
        )
        print("Valkey: copy the actual Console username; do not assume avnadmin.")
        upload("CLOUD_REDIS_URL", connection("rediss", "0"))
        broker = input("Kafka client-certificate bootstrap HOST:PORT: ").strip()
        host, port = broker.rsplit(":", 1)
        if not host or any(char in host for char in "/@ ") or not 0 < int(port) < 65536:
            raise ValueError("Invalid Kafka endpoint")
        upload("CLOUD_KAFKA_BROKER", broker)
        for name in (
            "CLOUD_KAFKA_CA_PEM",
            "CLOUD_KAFKA_CERT_PEM",
            "CLOUD_KAFKA_KEY_PEM",
            "CLOUD_POSTGRES_CA_PEM",
        ):
            path = Path(input(f"Local file path for {name}: ").strip().strip('"'))
            content = path.read_text(encoding="utf-8-sig")
            if "-----BEGIN " not in content or "-----END " not in content:
                raise ValueError(f"{name} is not PEM")
            upload(name, content)
    elif args.group == "mlflow":
        uri = input("DagsHub HTTPS .mlflow endpoint: ").strip()
        if not uri.startswith("https://dagshub.com/") or not uri.endswith(".mlflow"):
            raise ValueError("Expected a DagsHub HTTPS .mlflow endpoint")
        upload("CLOUD_MLFLOW_TRACKING_URI", uri)
        upload("CLOUD_MLFLOW_USERNAME", input("DagsHub username: ").strip())
        upload("CLOUD_MLFLOW_TOKEN", getpass.getpass("DagsHub token (hidden): "))
    else:
        upload(
            "CLOUD_AGENT_ACCESS_KEY",
            getpass.getpass(
                "Shared API/UI access key from your password manager (hidden): "
            ),
        )


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, RuntimeError, EOFError) as exc:
        raise SystemExit(
            f"Configuration incomplete: {type(exc).__name__}; secret values suppressed"
        ) from None
