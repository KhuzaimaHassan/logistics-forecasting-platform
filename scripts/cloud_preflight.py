"""Validate cloud configuration and materialize PEM secrets without printing them.

Run in the same shell as jobs with --github-env on GitHub Actions. Never sources
credentials from a committed file. Does not provision or upgrade any service.
"""

import argparse
import os
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

REQUIRED = (
    "DATABASE_URL",
    "REDIS_URL",
    "REDPANDA_BROKER",
    "MLFLOW_TRACKING_URI",
    "MLFLOW_TRACKING_USERNAME",
    "MLFLOW_TRACKING_PASSWORD",
    "KAFKA_CA_PEM",
    "KAFKA_CERT_PEM",
    "KAFKA_KEY_PEM",
    "POSTGRES_CA_PEM",
)


def prepare(directory: Path, aiven_only: bool = False):
    required = [
        name for name in REQUIRED if not (aiven_only and name.startswith("MLFLOW_"))
    ]
    missing = [name for name in required if not os.getenv(name, "").strip()]
    if missing:
        raise ValueError(
            "BLOCKED — NOT VERIFIED: missing configuration names: " + ", ".join(missing)
        )
    db = urlsplit(os.environ["DATABASE_URL"])
    if db.scheme not in {"postgresql", "postgresql+psycopg2"} or parse_qs(db.query).get(
        "sslmode"
    ) != ["verify-full"]:
        raise ValueError("DATABASE_URL must specify PostgreSQL and sslmode=verify-full")
    redis = urlsplit(os.environ["REDIS_URL"])
    if redis.scheme != "rediss":
        raise ValueError("REDIS_URL must specify rediss:// with verified TLS")
    query = parse_qs(redis.query)
    if query.get("ssl_cert_reqs", ["required"])[0].lower() not in {
        "required",
        "2",
    } or query.get("ssl_check_hostname", ["true"])[0].lower() not in {"true", "1"}:
        raise ValueError(
            "REDIS_URL must not disable certificate or hostname verification"
        )
    if not aiven_only and not os.environ["MLFLOW_TRACKING_URI"].startswith(
        "https://dagshub.com/"
    ):
        raise ValueError(
            "Free cloud profile requires the DagsHub HTTPS tracking endpoint"
        )
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    exports = {"KAFKA_SECURITY_PROTOCOL": "SSL", "DEPLOYMENT_MODE": "cloud"}
    for secret, target in {
        "KAFKA_CA_PEM": "KAFKA_SSL_CAFILE",
        "KAFKA_CERT_PEM": "KAFKA_SSL_CERTFILE",
        "KAFKA_KEY_PEM": "KAFKA_SSL_KEYFILE",
        "POSTGRES_CA_PEM": "POSTGRES_SSLROOTCERT",
        "VALKEY_CA_PEM": "REDIS_SSL_CA",
    }.items():
        if secret == "VALKEY_CA_PEM" and not os.getenv(secret, "").strip():
            continue  # Aiven Valkey normally uses public CA roots; verify with system trust.
        content = os.environ[secret]
        if "-----BEGIN " not in content or "-----END " not in content:
            raise ValueError(f"{secret} must contain a PEM certificate/key")
        path = (directory / (target.lower() + ".pem")).resolve()
        path.write_text(content, encoding="utf-8")
        path.chmod(0o600)
        exports[target] = str(path)
    return exports


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--github-env", action="store_true")
    parser.add_argument("--aiven-only", action="store_true")
    args = parser.parse_args()
    try:
        exports = prepare(Path(".certs"), aiven_only=args.aiven_only)
    except ValueError as exc:
        raise SystemExit(str(exc)) from None
    if args.github_env:
        with open(os.environ["GITHUB_ENV"], "a", encoding="utf-8") as file:
            for name, value in exports.items():
                file.write(f"{name}={value}\n")
    else:
        print(
            "PEM files prepared in .certs. Export the corresponding *_FILE/SSLROOTCERT/REDIS_SSL_CA paths in the current shell."
        )
    print("Configuration shape verified; cloud connectivity has NOT been verified.")


if __name__ == "__main__":
    main()
