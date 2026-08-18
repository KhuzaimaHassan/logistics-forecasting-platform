# Decisions

Architecture Decision Record (ADR) log. Each entry: context, decision, alternatives considered, consequences. New entries append to the bottom — never edit history, add a superseding entry instead.

---

## ADR-001: Geography — NYC data, geography-agnostic design

**Context:** Project builder is based in Karachi; initial instinct was to use Karachi data for local relevance.

**Decision:** Use NYC as the data source. Design zone/city identifiers as configuration, not hardcoded, so a second city is a data-mapping exercise rather than a rewrite.

**Alternatives considered:**
- Karachi real data — rejected. No open GTFS-realtime feed, no open real-time traffic-speed API, no open ride-hailing dataset. Would force synthetic data, undermining the "real production-grade data" goal.
- Karachi synthetic simulation (OSM road network + generated trips) — deferred, not rejected. Viable future extension once the NYC pipeline is proven; premature now.

**Consequences:** Data is not locally relevant to Pakistan, but the pipeline skills (ETL, streaming, feature store, CI/CD, agent layer) are geography-independent and transfer to any employer's data. Zone-config abstraction adds minor upfront design cost, paid back if a second city is ever added.

---

## ADR-002: Compute — Oracle Cloud Always Free over AWS/GCP

**Context:** Builder has prior hands-on experience with AWS EC2 and GCP, and asked why the plan defaulted to an unfamiliar provider.

**Decision:** Self-host the core stack (Redpanda, Postgres, Feast online store, MLflow) on a single Oracle Cloud Always Free Ampere A1 VM.

**Alternatives considered:**
- AWS EC2 — rejected as the primary host. Free tier is time-boxed: legacy accounts get 12 months from account creation, accounts created after July 2025 get a $200 credit expiring after 6 months. Not viable as a perpetual host for a project with no fixed end date.
- GCP e2-micro — rejected as the primary host. Always Free, but only 2 shared vCPU threads / 1GB RAM (US regions only) — not enough to co-host the full self-hosted stack.

**Consequences:** Oracle Always Free currently provides 2 OCPU / 12GB RAM / 200GB storage / 1 reserved public IP, perpetually free (Oracle reduced this from 4 OCPU/24GB in Aug 2026 — noted here since it's the kind of platform detail that will drift again). AWS/GCP free tiers remain usable for specific stateless managed services (e.g., Cloud Run, R2/S3 for backups) where a persistent VM isn't required.

---

## ADR-003: Streaming broker — self-hosted Redpanda over managed Kafka

**Context:** Needed a Kafka-API-compatible broker for the real-time layer.

**Decision:** Self-host a single-node Redpanda broker on the Oracle VM via Docker.

**Alternatives considered:**
- Upstash Kafka — discontinued by Upstash in 2025. No longer an option.
- Confluent Cloud / Redpanda Cloud (managed) — pay-as-you-go, not free-forever; unsuitable for an indefinite personal project.

**Consequences:** Full operational ownership of the broker (setup, monitoring, upgrades) — more work than a managed service, but demonstrates real streaming-infra skill and removes dependency on a third party's free-tier terms changing.

---

## ADR-004: Feature store — Feast over Hopsworks

**Context:** Builder has prior experience with Hopsworks from an earlier project.

**Decision:** Use Feast (open-source, self-hosted) for this project.

**Alternatives considered:**
- Hopsworks (managed free tier) — familiar tool, would reduce setup time, but ties the project to a specific vendor's free-tier terms and doesn't demonstrate the self-hosted/open-source path.

**Consequences:** More setup work (offline store on Postgres, online store on Redis, both self-managed) in exchange for full control and a second, distinct feature-store skill on the portfolio rather than repeating the same tool.

---

## ADR-005: Orchestrator — Prefect over Airflow/Dagster

**Context:** Needed a scheduler for batch ETL, feature materialization, training, and monitoring jobs.

**Decision:** Prefect Cloud free "Hobby" tier (1 workspace, 5 deployments, 500 min/month serverless compute — confirmed free-forever as of 2026).

**Alternatives considered:**
- Airflow (self-hosted) — heavier operational footprint on an already-constrained 2 OCPU/12GB VM.
- Dagster Cloud — comparable option, not chosen primarily for familiarity/consistency with prior project usage.

**Consequences:** Orchestration is decoupled from the Oracle VM's compute budget (Prefect Cloud runs the scheduler; work executions still happen on our infra via a Prefect worker). Free-tier deployment limit (5) means batch jobs need to be grouped thoughtfully rather than one deployment per task.

---

## ADR-006: UI — Streamlit over Next.js

**Context:** Builder has Next.js experience from a prior project (Quasar) and asked whether to reuse it here.

**Decision:** Streamlit as the primary UI for this project; Next.js deferred to an optional later polish phase.

**Alternatives considered:**
- Next.js — rejected as the *primary* UI. Adds a second real service to build/deploy/maintain without adding pipeline-engineering signal, which is this project's actual point.

**Consequences:** Faster path to a demoable UI (live prediction map, monitoring dashboard, agent chat) in Python, colocated with the model/agent code. A Next.js rebuild remains a cheap future upgrade given existing familiarity, not a blocker now.

---

## ADR-007: Model artifact durability

**Context:** MLflow tracking/registry runs on the Oracle VM; artifacts stored on VM disk are at risk if the VM is ever reclaimed or reset.

**Decision:** MLflow artifacts stored on VM block storage for active development; periodic backup to Cloudflare R2 (10GB free-forever tier) once the training pipeline stabilizes.

**Consequences:** Some risk window before backups are wired up (tracked as a Roadmap/Phase 3 follow-up task, not a blocker for early development).

---

## ADR-008: Infra bootstrap conventions — PostGIS simplification, Caddy reverse proxy, and colocated Dockerfiles

**Context:** Phase 0 bootstrap required resolving ambiguity on spatial extensions, TLS reverse-proxy topology, and Dockerfile directory layout.

**Decision:**
1. **Spatial storage:** Use standard `postgres:16` image with `centroid_lat` / `centroid_lon` numeric fields on `taxi_zones` for UI map plotting. Defer PostGIS extension as unnecessary complexity for centroid-based zone aggregations.
2. **Reverse proxy:** Use Caddy (`caddy:2-alpine`) in Docker Compose as the single internet-facing gateway on ports 80/443 for zero-config automatic TLS, reverse-proxying to FastAPI and Streamlit over the internal Docker network. All other ports remain unexposed.
3. **Dockerfile organization & packaging:** Colocate Dockerfiles with service code (`src/<service>/Dockerfile`, `ui/Dockerfile`). Use a single root `pyproject.toml` managed via `uv` with multi-arch base images.

**Alternatives considered:**
- PostGIS (`postgis/postgis:16`) — heavier container and migration burden without functional benefit over centroid coordinates for v1.
- Nginx — requires manual certbot cron setup/renewal scripts on host; Caddy provides zero-touch Let's Encrypt TLS.
- Centralized `infra/docker/*.Dockerfile` — separates container build definitions from service code and dependencies.

**Consequences:** Leaner Docker topology, simpler database migrations, automatic HTTPS on public VM deployment, and modular service directories.

---

## ADR-009: Dependency management — scoped optional-dependency groups in pyproject.toml

**Context:** All runtime dependencies were initially defined in a single flat `dependencies` list in `pyproject.toml`. This caused every service container (e.g., lightweight serving API or UI dashboard) to install all 225+ dependencies across the entire stack (including heavy ML training libraries, Kafka consumers, and feature store engines), bloating image sizes and increasing build times.

**Decision:** Organize project dependencies into scoped `[project.optional-dependencies]` groups:
- `core`: Shared baseline dependencies (`pydantic`, `pydantic-settings`, `psycopg2-binary`, `sqlalchemy`, `redis`, `requests`, `python-dotenv`).
- `extract`: Data ingestion & extraction (`feast`, `pyarrow`, `pandas`, `numpy`).
- `transform`: Streaming transform & consumer (`kafka-python-ng`, `feast`, `pyarrow`, `pandas`, `numpy`).
- `serving`: FastAPI prediction & agent endpoints (`fastapi`, `uvicorn`, `feast`, `lightgbm`, `xgboost`, `mlflow`, `langgraph`, `langchain-core`, `faiss-cpu`).
- `ui`: Streamlit frontend (`streamlit`, `requests`, `pydeck`, `pandas`).
- `training`: Model training pipelines (`feast`, `lightgbm`, `xgboost`, `mlflow`, `pyarrow`, `pandas`, `numpy`).
- `monitoring`: Drift detection & orchestration (`evidently`, `prefect`, `pandas`).
- `dev`: Developer tooling (`pytest`, `pytest-asyncio`, `pytest-cov`, `ruff`, `black`, `httpx`).

Maintain a single unified `uv.lock` at the root for deterministic resolution, and configure each service Dockerfile to install strictly its required slices via `RUN uv sync --frozen --no-dev --extra core --extra <service-name>`.

**Alternatives considered:**
- Multiple independent `pyproject.toml` files per service — creates fragmented dependency management, version drift between services, and complex monorepo tooling.
- Single flat `dependencies` list — bloated containers (~88 unnecessary packages in serving/ui), longer CI build times, and larger container attack surfaces.

**Consequences:** Lean, isolated container images built from a single unified repository lockfile. CI continues to validate the entire dependency graph via full extras sync, while each deployed service runs only its necessary footprint.
