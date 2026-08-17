# Logistics Demand & ETA Forecasting Platform

A live, production-grade MLOps platform forecasting NYC taxi zone demand and corridor trip duration in real time, featuring an integrated LangGraph Ops Copilot.

**Stack**: Python 3.11+ · FastAPI · Streamlit · Feast · Redpanda · PostgreSQL 16 · Redis 7 · MLflow · Prefect Cloud · LangGraph · Caddy 2 · Docker Compose · GitHub Actions

---

## Architecture Map

```
logistics-forecasting-platform/
├── docs/                 # System architecture, ADRs (Decisions.md), component designs & runbooks
├── src/                  # Core application modules
│   ├── extract/          # Batch extractor for TLC Parquet data (src/extract/Dockerfile)
│   ├── transform/        # Stream consumer, validation, cleaning & normalization (src/transform/Dockerfile)
│   ├── features/         # Feast feature repository, entities & feature views
│   ├── training/         # Baseline, LightGBM/XGBoost training pipelines & MLflow logging
│   ├── serving/          # FastAPI prediction, feature lookup & health endpoints (src/serving/Dockerfile)
│   ├── agents/           # LangGraph Ops Copilot with Feast, MLflow, DB, and FAISS RAG tools
│   ├── monitoring/       # Evidently AI data & prediction drift analysis
│   └── orchestration/    # Prefect flow definitions and schedules
├── ui/                   # Streamlit live map, monitoring dashboard & copilot chat (ui/Dockerfile)
├── infra/                # Infrastructure definitions
│   ├── docker-compose.yml# Single-host multi-container deployment topology
│   └── oracle-vm/        # Oracle Cloud Ampere A1 provisioning and firewall setup scripts
├── tests/                # Unit, integration, and contract tests (pytest)
├── .github/
│   ├── workflows/        # GitHub Actions CI/CD workflows (lint, test, deploy)
│   └── ISSUE_TEMPLATE/   # Milestone tracking issue template
├── .agents/              # Antigravity rules and agent customization
└── pyproject.toml        # Root dependency and tool configuration (uv, ruff, black, pytest)
```

---

## Ground Rules & Conventions

1. **Package & Environment Management**:
   - Use `uv` with a single root `pyproject.toml` managing project dependencies and dev tools.
   - Code formatting & linting: `black` and `ruff` configured in `pyproject.toml`, strictly checked in CI.
2. **Container Standards**:
   - Every service containerizes with a colocated `Dockerfile` (`src/<service>/Dockerfile`, `ui/Dockerfile`).
   - All base images MUST use official multi-arch (`linux/amd64`, `linux/arm64`) tags to support both local x86_64 dev and Oracle Ampere A1 ARM64 production.
3. **Database & Storage**:
   - Single PostgreSQL 16 instance with distinct schemas: `raw`, `warehouse`, `feast`, `mlflow`.
   - Use precomputed numeric centroids (`centroid_lat`, `centroid_lon`) for taxi zones.
   - All predictions and monitoring reports must be logged to their respective tables (`predictions`, `monitoring_reports`).
4. **Networking & Security**:
   - Only Caddy ports `80` and `443` are exposed externally with automated TLS.
   - Internal services (Postgres, Redis, Redpanda, FastAPI, Streamlit, MLflow) communicate exclusively over the internal Docker bridge network.
   - Secrets are loaded from `.env` locally and GitHub Actions secrets in CI; never commit credentials or hardcode tokens.
5. **Git Workflow**:
   - Follow branch strategy: `main` (protected), `dev` (milestone integration), `feature/<milestone>-<name>`.
   - Commit messages must follow Conventional Commits (`feat:`, `fix:`, `docs:`, `chore:`, `test:`).
   - PRs must link to milestone issues (`Closes #<id>` or `Relates to #<id>`). Details in [.agents/rules/conventions.md](file:///.agents/rules/conventions.md).

---

## Working Principles & Agent Steering

- **Plan First**: Always formulate and verify an implementation plan before writing or refactoring non-trivial code.
- **Maintain Documentation Integrity**: Any architectural change or deviation from `docs/` MUST be recorded as a new ADR entry in `docs/Decisions.md`. Never silently drift from documentation.
- **Fail Fast & Explicit Errors**: Prefer explicit typed schemas (Pydantic / SQL DDL) and actionable error logs over defensive swallowing of exceptions.
- **Scope Discipline**: Implement the minimal vertical slice required for the current task/ticket. Do not prematurely build speculative features.

---

## Essential Commands

```bash
# Environment & Dependencies (uv)
uv venv
uv pip install -e ".[dev]"

# Linting & Formatting
ruff check .
black --check .
black .              # auto-format

# Testing
pytest tests/ -v

# Local Docker Services
docker compose up -d          # Start full stack (or core infra)
docker compose up -d --build  # Rebuild and run updated images
docker compose logs -f <service>
docker compose down
```

---

## Documentation Pointers

- System Architecture & Flow: [docs/Architecture.md](file:///docs/Architecture.md)
- Architectural Decision Records: [docs/Decisions.md](file:///docs/Decisions.md)
- Database Schema & Tables: [docs/Database.md](file:///docs/Database.md)
- Deployment & Compose Topology: [docs/Deployment.md](file:///docs/Deployment.md)
- GitHub Milestones & CI/CD Setup: [docs/GitHub-Setup.md](file:///docs/GitHub-Setup.md)
- Local Environment Setup: [docs/Environment-Setup.md](file:///docs/Environment-Setup.md)
- Project Roadmap & Phases: [docs/Roadmap.md](file:///docs/Roadmap.md)
