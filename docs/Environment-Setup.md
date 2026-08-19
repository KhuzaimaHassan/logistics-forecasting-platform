# Environment Setup

## 1. Prerequisites

- Docker + Docker Compose
- Python 3.11+
- Package manager: `uv` (fast Python package and project manager)
- Free API keys: MTA GTFS-RT, OpenWeatherMap, Groq, Gemini, Prefect Cloud (see Data-Sources.md, Agents.md, and Decisions.md ADR-005)

## 2. Python Tooling & Dependency Convention

- Single root `pyproject.toml` managing project dependencies via `uv` optional-dependency groups (ADR-009).
- Code formatting and linting: `ruff` + `black`, configured directly in `pyproject.toml`.
- Virtual environment setup:
  ```bash
  uv venv
  uv sync --all-extras          # install full workspace dependencies for development
  # or scoped per service:
  uv sync --extra core --extra <service>
  ```

## 3. `.env` template

```
# Database
POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_DB=logistics

# Redis
REDIS_HOST=redis
REDIS_PORT=6379

# Redpanda
REDPANDA_BROKER=redpanda:9092

# Prefect Cloud
PREFECT_API_KEY=
PREFECT_API_URL=

# External APIs
MTA_API_KEY=
OPENWEATHERMAP_API_KEY=
GROQ_API_KEY=
GEMINI_API_KEY=

# MLflow
MLFLOW_TRACKING_URI=http://mlflow:5000
```

An `.env.example` with these keys (no values) lives at the repo root — copy to `.env` and fill in locally; `.env` itself is gitignored.

## 4. Local development

```bash
docker compose up -d          # starts Postgres, Redis, Redpanda, MLflow, Caddy
docker compose up -d --build  # rebuilds custom service images after code changes
docker compose logs -f <service>
```

## 5. Running tests & linters locally

```bash
pytest tests/
ruff check .
black --check .
```

## 6. Resolved & Open Questions

- Standard direct `uv` and `docker compose` commands are preferred over a wrapper `Makefile` to keep cross-platform (Windows/Linux/macOS) execution frictionless.

