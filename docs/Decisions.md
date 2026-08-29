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

---

## ADR-010: Database schema migrations — Alembic for raw and warehouse schemas

**Context:** Phase 1 requires managing schema evolution and table DDL for PostgreSQL (`raw` and `warehouse` schemas, including `taxi_zones`, `trips`, `loaded_months`, and `pipeline_runs`). A consistent, reproducible migration mechanism is needed across local development, CI automated testing, and Oracle VM container deployment.

**Decision:** Use Alembic (`alembic>=1.13.0`) as the schema migration tool, added to the `core` optional-dependencies group in `pyproject.toml`. Migrations are managed under an `alembic/` directory with standard linear revision history.

**Alternatives considered:**
- Plain versioned SQL scripts (`001_init.sql`, etc.) with a custom runner — simple, but lacks built-in version tracking tables (`alembic_version`), downgrade/rollback capabilities, branching detection, and programmatic integration with Python/SQLAlchemy test fixtures.
- Flyway / Liquibase — robust, but requires a JVM runtime or separate binary CLI inside container images, introducing unnecessary tool sprawl and image weight.

**Consequences:** Programmatic and CLI migration management using existing Python/SQLAlchemy tooling (`alembic upgrade head`). Migrations are version-tracked in PostgreSQL, idempotent, and testable directly within `pytest` harnesses using temporary test schemas or in-memory fixtures.

---

## ADR-011: Temporary representative taxi zone centroids and technical debt tracking

**Context:** Initial bootstrapping of `warehouse.taxi_zones` uses representative centroid coordinates (`centroid_lat`, `centroid_lon`) scoped strictly to UI map rendering. Replacing these with exact shapefile polygon centroids computed via `shapely`/`pyproj` against official TLC GeoJSON boundaries requires dedicated geospatial dependencies.

**Decision:** Maintain representative coordinates in `src/extract/zones_reference.py` for UI map visualization, explicitly scoped away from spatial distance calculations. Track polygon centroid derivation from official TLC shapefiles as technical debt to be resolved before Phase 9 polish at the latest.

**Alternatives considered:**
- Adding heavy geospatial dependencies (`geopandas`, `shapely`, `pyproj`) to the core dependency group immediately — adds container weight and build overhead before live geometric polygon processing is required.
- Blocking Phase 1 ETL pipeline development — unnecessary block on core batch extraction, cleaning, and database loading.

**Consequences:** Enables unimpeded development of Phase 1 ETL pipelines and Streamlit map layouts while explicitly tracking geospatial centroid refinement as technical debt.

**Status / Retried Check:** Retried the Socrata GeoJSON fetch for dataset `d3c5-ddgc` (`https://data.cityofnewyork.us/api/geospatial/d3c5-ddgc?method=export&format=GeoJSON`) via `urllib.request`. The endpoint returned `HTTP Error 404: Not Found` (endpoint deprecated/removed by provider). ADR-011 stands as written; centroid refinement remains tracked as technical debt.

---

## ADR-012: Orchestration engine upgrade — Prefect 3.x and work-pool/worker deployment model

**Context:** Phase 1 (M1-4) introduces batch ETL flow orchestration, and Phase 8 will introduce scheduled model retraining and Evidently AI data drift monitoring. ADR-005 established Prefect Cloud as the orchestrator. Prefect 3.x refactors orchestration around work pools (`work-pool`) and process/docker workers (`prefect worker start --pool ...`), deprecating legacy Prefect 2.x agent/block patterns.

**Decision:** Standardize on Prefect 3.x (`prefect>=3.0.0`) and adopt the unified work-pool / worker deployment model with a default work pool (`default-agent-pool`). Build a dedicated `prefect-worker` container image (`src/orchestration/Dockerfile`) containing application dependencies and source modules (`src.extract`, `src.transform`, `src.common`).

**Alternatives considered:**
- Legacy Prefect 2.x agent/block deployment (`prefect agent start`) — deprecated in Prefect 3.x, leads to technical debt and incompatibility with future Prefect Cloud workspace updates.
- In-process cron triggers inside FastAPI / Streamlit — couples orchestration directly to UI/API lifecycle, lacks execution logs, retry state tracking, and cloud observability.

**Consequences:** Locks in a consistent flow deployment pattern (`flow.deploy(...)` / `prefect.yaml`) across batch ETL (Phase 1), feature materialization (Phase 2), model training (Phase 3), and Evidently drift monitoring (Phase 8). The worker runs as a dedicated service in `docker-compose.yml` polling `default-agent-pool` for scheduled flow runs.

---

## ADR-013: Feast registry backend — PostgreSQL SQL registry (feast schema)

**Context:** Phase 2 introduces Feast as the feature store. Feast requires a central registry to store and synchronize entity and feature view definitions across offline training pipelines, background materialization workers, and online serving APIs.

**Decision:** Use Feast's native SQL Registry backend (`registry_type: sql`) pointed to the shared PostgreSQL 16 database under the `feast` schema (`postgresql+psycopg2://...`).

**Alternatives considered:**
- File-based SQLite / Protobuf registry (`data/registry.db`) — rejected. In multi-container Docker topologies, file-based registries require mounting shared volumes across multiple containers, introducing file-locking contention, write race conditions, and cache invalidation lag during simultaneous read/write operations.
- Remote Object Storage registry (Cloudflare R2 / AWS S3) — rejected. Introduces external network latency and third-party credential dependencies to every local feature lookup and container startup, unnecessary for a single-host colocated stack.

**Consequences:** The feature registry is centralized in PostgreSQL, transactional, concurrent, and directly accessible by all containers over the internal Docker network (`logistics-net`). Reuses existing PostgreSQL resources within the single-VM budget without adding operational complexity.

**Driver Note (psycopg2 vs. psycopg3):** Feast's PostgreSQL offline store (`feast.infra.offline_stores.contrib.postgres_offline_store`) specifically mandates `psycopg` 3 (`psycopg[binary,pool]>=3.1.0`) for connection pooling and PostgreSQL type conversion. The rest of the project's SQLAlchemy and Alembic models continue using `psycopg2-binary` via `postgresql+psycopg2://`. Maintaining both drivers in `pyproject.toml` is a deliberate, explicit accommodation of Feast's internal requirements alongside the established SQLAlchemy stack.


---

## ADR-014: Feature definition — origin_zone_demand_pressure as raw rolling count

**Context:** `Feature-Store.md` defined `origin_zone_demand_pressure` as a linkage feature for `corridor_duration_features` (corridor trip duration prediction) derived from origin zone demand. An open question existed regarding whether this value should be the prediction output of the demand forecast model or the raw rolling pickup count from `zone_demand_features`.

**Decision:** Formally define `origin_zone_demand_pressure` as the raw rolling pickup count (`pickup_count_last_15m` / `pickup_count_last_1h`) of the origin zone from `zone_demand_features`, not model-predicted demand.

**Alternatives considered:**
- Model-predicted demand (output of the demand model) — rejected. Introduces a circular dependency where training the duration model requires historical inference logs or re-evaluating the demand model across all historical training timestamps. Any retraining or architecture update of the demand model would invalidate all historical duration training datasets. Furthermore, online serving would require chained synchronous model inferences, increasing prediction latency and failure modes.

**Consequences:** Eliminates circular model dependencies, prevents training-time data leakage, and ensures point-in-time correctness during historical feature extraction. At inference time, `origin_zone_demand_pressure` is a direct sub-10ms key lookup in Redis from `zone_demand_features`.

---

## ADR-015: Historical offline feature aggregation — 1-hour row grain with time-windowed incremental range compute and 7-day lookback buffer

**Context:** Phase 2 introduces historical feature aggregation tables (`warehouse.zone_demand_features_hourly` and `warehouse.corridor_duration_features_hourly`) in PostgreSQL to serve as Feast offline store sources. We need to establish the row granularity (1-hour vs. 15-minute) and execution strategy (unconditional full recompute vs. parameterized time-windowed range compute).

**Decision:**
1. **1-Hour Timestamp Row Grain (`HH:00:00` UTC):** Store offline features at 1-hour snapshot intervals. Each hourly row contains both 15-minute rolling metrics (`pickup_count_last_15m`, `avg_duration_last_15m` representing the window $[T-15\text{m}, T]$) and multi-scale rolling metrics (`1h`, `24h`, `7d`).
2. **Parameterized Time-Windowed Range Compute (with 7-Day Lookback Buffer):** Compute aggregations over target time ranges $[T_{\text{start}}, T_{\text{end}}]$ by querying raw trips from $[T_{\text{start}} - 7\text{ days}, T_{\text{end}}]$. Results are written idempotently using PostgreSQL `ON CONFLICT (...) DO UPDATE`.
3. **Multi-Scale Anti-Leakage Gating:** `zone_demand_features_hourly` strictly gates rolling windows on `pickup_datetime <= T`; `corridor_duration_features_hourly` strictly gates rolling duration statistics on completed trips where `dropoff_datetime <= T`.

**Alternatives considered:**
- **15-minute row grain:** Rejected for offline storage. Inflates table cardinality 4x (~800k zone rows/month and ~6M corridor rows/month), straining PostgreSQL buffer cache and memory budgets (ADR-002) without offering distinct prediction targets, since the models forecast hourly demand and ETA.
- **Unconditional full-history recompute on every ETL run:** Rejected. Scales at $O(\text{all time})$; as historical data expands past 30M+ trips, full Cartesian joins will cause CPU thrashing and memory exhaustion on the single-host VM.

**Consequences:**
- Avoids 4x storage bloat (~196k zone rows and ~1.5M corridor rows per month) while maintaining complete schema and feature definition parity with online feature views.
- **Training Sampling Constraint:** Training dataset generators (Phase 3) must sample observation timestamps at hour boundaries (`HH:00:00` UTC) to prevent sub-hour feature snapshot staleness during Feast point-in-time joins.

---

## ADR-016: Training dataset generation — Grid-based demand sampling, active-corridor duration sampling, and 7-day holdout time split

**Context:** Phase 3 introduces model training pipelines for zone demand and corridor trip duration (ETA). We need to formalize:
1. What determines the entity observation rows `(entity, event_timestamp)` passed to Feast's `store.get_historical_features(entity_df=...)`?
2. How ground-truth targets are computed without data leakage?
3. How train and validation splits are partitioned across time?

**Decision:**
1. **Demand Dataset Sampling Strategy (Full Spatial-Temporal Grid):**
   - Sample every NYC taxi zone ($Z \in \{1 \dots 263\}$) at every hour boundary ($T \in \{\text{HH:00:00 UTC}\}$) over the effective training range.
   - **Rationale:** Constructing a complete Cartesian grid $\text{Zone} \times \text{Hour}$ ($263 \times 576 = 151,488$ rows for Jan 8–31) explicitly preserves zero-demand observations in low-volume zones, eliminating survivorship bias where models only train on zones with active trips.
2. **Corridor Duration Dataset Sampling Strategy (Active Corridor-Hours Grid, Pickup-Anchored Target Window):**
   - Sample active corridor-hours $(C, T)$ where $\ge 1$ trip departed (i.e. `pickup_datetime` $\in [T, T+1\text{h})$) on corridor $C$.
   - **Pickup-Anchored Target Alignment:** Both demand and duration targets are strictly pickup-anchored: $Y_{\text{demand}}$ counts pickups departing in $[T, T+1\text{h})$, and $Y_{\text{duration}}$ is the mean duration (in seconds) of trips departing in $[T, T+1\text{h})$, matching the serving semantics of `/predict/eta` in `API.md` (which forecasts expected duration for a trip starting at prediction time $T$).
   - **Feature Gating Stays Separate:** Historical feature values at observation timestamp $T$ remain strictly anti-leakage gated per ADR-015: `zone_demand_features` gate on `pickup_datetime <= T`, while `corridor_duration_features` gate on completed trips with `dropoff_datetime <= T`.
   - **Rationale:** A full Cartesian grid of $263 \times 263 = 69,169$ corridors $\times 576$ hours generates 39.8M rows where >95% are empty. Filtering to active corridor-hours bounds dataset cardinality (~100k–300k rows) while ensuring robust duration targets.
   - **Target ($Y_{\text{duration}}$):** Mean trip duration in seconds for trips on corridor $C$ with `pickup_datetime` in $[T, T+1\text{h})$.

3. **7-Day Lookback Buffer:**
   - Reserve the first 7 days of available historical data (Jan 1–7, 2023) strictly as a lookback feature window. Training observations begin at `2023-01-08 00:00:00 UTC` so that `pickup_count_same_hour_last_week` is 100% observed without null imputation artifacts.
4. **Time-Based Train / Validation Split:**
   - **Train partition:** `2023-01-08 00:00:00` to `2023-01-24 23:59:59` UTC (17 days, ~70% of dataset).
   - **Validation partition:** `2023-01-25 00:00:00` to `2023-01-31 23:59:59` UTC (7 full days = 1 complete weekly cycle, ~30% of dataset).
   - **Rationale:** Strict chronological splitting prevents temporal data leakage inherent in random cross-validation. Evaluating on a full 7-day holdout guarantees balanced representation of all days of the week and intraday seasonality.

**Alternatives considered:**
- **Random K-Fold splitting:** Rejected. Randomly partitioning time-series rows leaks future seasonal patterns and autocorrelation into the training set, producing over-optimistic evaluation metrics that fail in live production.
- **Sparse demand sampling (only hours with pickups > 0):** Rejected. Skews the model's loss landscape toward over-predicting demand in quiet residential or outer-borough zones during late night/early morning hours.

**Consequences:** Clean, reproducible, point-in-time correct training datasets aligned with Feast's hourly offline feature grain, zero feature leakage, and realistic out-of-time validation metrics.

