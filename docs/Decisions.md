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

**Consequences:** Some risk window before backups are wired up (tracked as a Roadmap/Phase 3 follow-up task, not a blocker for early development). Verified 2026-09-01 — live upload of 262 real MLflow artifacts (10.82MB) to Cloudflare R2, confirmed via list_objects_v2. ADR-007 closed.


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

---

## ADR-017: Cyclical Harmonic Encodings & Log1p Target Transformation for Corridor Duration Regression

**Context:** During baseline evaluation (M3-3), two data/modeling characteristics were discovered:
1. **Hour & Day Boundary Discontinuity:** Integer `hour_of_day` (0–23) and `day_of_week` (0–6) treat 23:00 and 00:00 as maximally distant (distance 23) despite being adjacent in real time.
2. **Right-Tail Duration Distortion:** Trip duration has an extreme heavy right-tail distribution ($p_{50}=698\text{s}$, $p_{99}=3,428\text{s}$, max $86,388\text{s}$) due to rare overnight unclosed-meter shift anomalies (~0.088% of trips). Standard $L_2$ regression on raw seconds generates gradient magnitudes $> 1.7 \times 10^5$, pulling tree leaf predictions upward and distorting normal 10–30 minute trip ETA predictions.

**Decision:**
1. **Continuous Cyclical Harmonic Encodings:**
   - Add sine and cosine features: $\sin(2\pi h / 24)$, $\cos(2\pi h / 24)$, $\sin(2\pi d / 7)$, $\cos(2\pi d / 7)$.
   - Preserves smooth cyclical continuity across midnight and week boundaries without arbitrary split boundaries.
2. **Corridor Categorical Feature Extraction:**
   - Parse `pickup_zone_id` and `dropoff_zone_id` from composite `corridor_id` (e.g. `161_236`) and treat them as categorical features in LightGBM, allowing tree partitions to learn zone-specific origin/destination bias.
3. **Log1p Duration Target Transformation:**
   - Train the corridor ETA model on log-transformed targets $y_{\text{log}} = \ln(1 + \text{duration\_sec})$.
   - Invert predictions during evaluation and serving via $\hat{y} = \max(60.0, \exp(\hat{y}_{\text{log}}) - 1.0)$, respecting the 60s physical minimum trip duration floor.
   - Compute all validation metrics (MAE, RMSE, WAPE, MedAE) in **original seconds** against ground truth $y_{\text{val}}$ for direct, unskewed comparison against baseline benchmarks.

**Consequences:**
- Variance-stabilized target distribution, eliminating outlier-induced gradient skew.
- Inherent physical guarantee of positive duration predictions ($\hat{y} \ge 60.0\text{s}$).
- Demonstrated $+39.09\%$ MAE reduction (278.35s vs 456.95s) on the 256k-row validation split.

---

## ADR-018: Real-Time Online Feature Store Updates — Feast Push API with Tightened Incremental Materialization Reconciliation

**Context:** In Phase 4, the real-time layer introduces streaming trip events and external data feeds (traffic, transit, weather) over Redpanda. Online inference endpoints (`/predict/demand`, `/predict/eta` in Phase 5) require up-to-date feature values in the Redis online store. We must decide how streaming events update the online feature store: direct Feast push API (`store.push()`) vs. tightened batch materialization (`materialize_incremental()`) schedule.

**Decision:**
1. **Sub-Second Streaming Push Path (Feast Push API):**
   - The stream consumer validates incoming events, updates short-window aggregations in memory/Redis, and immediately pushes updated feature vectors into the Redis online store via Feast's Push API (`store.push(feature_view_name, df, to=PushMode.ONLINE)`).
   - This provides instantaneous sub-second feature freshness for online inference endpoints without waiting for batch processing intervals.
2. **Batch Reconciliation Path (Scheduled Incremental Extraction & Materialization):**
   - A scheduled Prefect flow (`realtime_reconciliation_flow.py`) executes an incremental offline aggregation step (`src/features/offline_extractor.py` on the recent sliding window $[T - \text{lookback}, T]$) against `warehouse.trips` **prior** to invoking `store.materialize_incremental()`.
   - This ensures newly-ingested live/replay trips are properly aggregated into `warehouse.zone_demand_features_hourly` and `warehouse.corridor_duration_features_hourly`, preventing `materialize_incremental()` from overwriting fresh pushed values with stale offline data.
   - Separate Feast Feature View namespaces (e.g. `zone_demand_features_push` for streaming push metrics and `zone_demand_features_hourly` for deep lag features) ensure streaming and batch features coexist cleanly in the Redis online store.
3. **At-Least-Once Delivery & Idempotency:**
   - The stream consumer commits Redpanda consumer offsets only after both the PostgreSQL write (`warehouse.trips`) and the Redis feature push succeed.
   - Deduplication is guaranteed at the database layer using deterministic 64-bit BigInteger `trip_id` (`ON CONFLICT (trip_id) DO NOTHING`).


**Alternatives considered:**
- **Pure Push Only (No Materialization Reconciliation):** Requires keeping extensive multi-day rolling state in stream consumer memory to calculate cold 7-day lag features, risking state loss and memory bloat on container restarts.
- **Pure Materialization Only (Tightened Schedule):** Creates an unavoidable 1–5 minute latency delay before new trip completions are visible in Redis, making the online store lag behind fast-moving demand spikes.

**Consequences:**
- Sub-second feature freshness for real-time model inference.
- Guaranteed long-term feature consistency and resilience against streaming worker restarts through regular batch reconciliation.

---

## ADR-019: MTA Transit Congestion Proxy — Subway Alerts JSON vs. Protobuf Vehicle Positions

**Context:** In Phase 4 (M4-2), external streaming feeds are ingested to provide live contextual signals for road traffic and corridor trip duration (ETA) forecasting. Transit data was specified as a congestion proxy (transit bunching/delays correlate with roadway congestion). The MTA developer portal exposes both raw GTFS-Realtime Protocol Buffer (Protobuf) feeds for vehicle coordinates/trip updates and a REST JSON feed for subway service alerts and line status (`/camsys%2Fsubway-alerts.json`).

**Decision:** Ingest the MTA Subway Alerts REST JSON endpoint directly (`https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fsubway-alerts.json`) rather than compiling and decoding raw GTFS-RT Protobuf vehicle position binaries.
1. **Direct Signal Alignment:** The ETA forecasting model needs line-level transit delays and congestion severity (`delay_seconds`, `congestion_level`, `current_status`), which the alerts JSON feed supplies directly per subway line (`route_id`).
2. **Operational Simplicity & Dependency Hygiene:** Avoids introducing `gtfs-realtime-bindings` or custom Protobuf compilation pipelines (`google.protobuf` stubs) and complex coordinate-to-schedule reverse-matching algorithms on the single-host VM.
3. **Topic Payload Contract:** The stream consumer and Redpanda topic `transit.positions` carry normalized transit delay and congestion state (`route_id`, `stop_id`, `delay_seconds`, `congestion_level`, `recorded_at`).

**Alternatives considered:**
- **Raw GTFS-RT Protobuf Vehicle Positions (`gtfs-realtime.proto`):** Provides raw train coordinate points and stop sequence IDs. Requires adding compiled Protobuf bindings, decoding binary streams every 30s, and maintaining static GTFS schedule tables to infer train delays from coordinate timestamps — substantial compute and dependency complexity for an indirect congestion proxy when direct traffic speed readings (`traffic.snapshots` from Socrata) already provide primary roadway speed data.
- **Dropping Transit Feed Entirely:** Transit delay status provides useful multimodal congestion context during subway signal disruptions that cause surface road spillover.

**Consequences:** Clean, maintainable, lightweight JSON ingestion with zero Protobuf compiler dependencies. Payload accurately delivers line-level delay and congestion severity directly to `transit.positions` and `warehouse.transit_snapshots`.

---

## ADR-020: Online Serving Architecture — MLflow Model Lifecycle, Startup Loading, Batch Inference Contracts, 60s Prediction Caching, and Degraded Fallbacks

**Context:** In Phase 5, the online serving layer exposes low-latency HTTP prediction endpoints (`/predict/demand`, `/predict/eta`) and feature/health inspection endpoints (`/features/*`, `/health`, `/pipeline/status`) via FastAPI, reading real-time feature vectors from the Feast Redis online store and serving predictions from trained LightGBM models registered in MLflow. Four core architectural design decisions must be established:
1. Model loading and in-memory caching at startup, lifecycle for picking up newly-promoted models, and MLflow registry stage API verification.
2. Batch endpoint support and exact request/response shapes for demand and ETA predictions (closing the open question in `docs/API.md`).
3. Concrete prediction caching time-to-live (TTL) in Redis.
4. Feature-unavailable response contract when `FeastOnlineClient.get_online_features()` returns `cache_hit=False` (addressing the behavior proven in M2-4).

**Decision:**

1. **Model Loading at Startup & Lifecycle (Deferring Hot-Reload to Phase 6):**
   - **Startup Eager Load:** On FastAPI startup (`lifespan` handler), `ModelLoader` connects to the MLflow tracking server, queries the Model Registry for active `Production` stage versions of `demand_lightgbm_model` and `corridor_duration_lightgbm_model`, loads the model artifacts into memory, and performs a warmup evaluation. If MLflow is temporarily unreachable, it logs a warning and loads local baseline models (`src/training/baseline.py`) as a graceful fallback.
   - **Newly-Promoted Model Pickup (Container Restart):** In Phase 5, picking up a newly-promoted model is handled simply and reliably via container restart (`docker compose restart serving` or redeployment via CI/CD), which `Deployment.md` already specifies. This avoids premature complexity (managing staging slots, atomic pointer swapping under concurrent inference, and in-flight request drains) before the automated retraining and drift pipeline exists.
   - **Deferred to Phase 6:** Zero-downtime atomic hot-reload (`POST /models/reload` or webhook-driven reloading) is formally tracked as a design item for Phase 6 (CI/CD & Retraining Automation), where retraining orchestration (`prefect` flow + deploy hooks) actually lives.
   - **MLflow Registry API Confirmation:** Verified against the pinned MLflow version (`mlflow>=2.11.0` in `pyproject.toml`, resolved to `3.15.1` in `uv.lock`): `MlflowClient.transition_model_version_stage` and `MlflowClient.get_latest_versions(..., stages=['Production'])` remain fully functional and supported. Because `src/training/pipeline.py` explicitly promotes models to `stage="Production"`, `ModelLoader` targets `stages=["Production"]` directly without needing speculative alias dual-querying.

2. **Batch Endpoint Support & Request/Response Contracts:**
   - To support high-cardinality UI map rendering (all 263 active NYC taxi zones and top corridors) without incurring hundreds of sequential HTTP roundtrips, the service exposes dedicated batch POST endpoints alongside single-entity GET endpoints:
     - **Demand Batch (`POST /predict/demand/batch`):**
       - Request: `{"zone_ids": [161, 236, ...], "horizon_minutes": 15}`. If `zone_ids` is empty or omitted, defaults to all 263 active TLC zones.
       - Vectorized Processing: Executes a single vectorized Redis lookup via `FeastOnlineClient.get_zone_demand_features(zone_ids)` (single Redis MGET) and scores the entire feature matrix in a single C++ LightGBM `predict()` call.
       - Response: Returns a list of per-zone predictions with `status`, `cache_hit`, and model metadata.
     - **ETA Batch (`POST /predict/eta/batch`):**
       - Request: `{"corridors": [{"origin_zone_id": 161, "dest_zone_id": 236}, ...]}`.
       - Vectorized Processing: Performs batched corridor and zone feature lookups and evaluates log1p duration predictions in a single matrix pass, inverting via $\hat{y} = \max(60.0, \exp(\hat{y}_{\text{log}}) - 1.0)$.
       - Response: Returns an array of corridor ETA predictions in seconds and minutes.

3. **Concrete 60-Second Prediction Caching TTL:**
   - Prediction results are cached in Redis (with in-memory LRU fallback) using a fixed **60-second (60s)** TTL.
   - Cache keys follow the schema `pred:demand:{zone_id}:{horizon_minutes}` and `pred:eta:{origin_zone_id}:{dest_zone_id}`.
   - **Rationale:** While historical TLC batch features step at 15-minute intervals, streaming traffic speed updates, transit delays, and weather snapshots arrive at 10–60s intervals via Redpanda. A 60s TTL prevents duplicate inference stampedes when dashboards or multiple users refresh simultaneously, reduces p99 endpoint latency to sub-2ms, and guarantees prediction freshness stays bounded within 1 minute of live real-world updates.

4. **Feature-Unavailable Response Contract (Degraded Fallback on Cache Miss):**
   - In M2-4, `FeastOnlineClient` establishes that when an entity has never been materialized or its Redis TTL has expired, `get_online_features()` returns `cache_hit=False` with `None` fields rather than raising an error.
   - **Invalid Entity ID ($< 1$ or $> 265$):** Returns **HTTP 404 Not Found** (`{"error": "ZoneNotFound", "detail": "Zone ID 999 does not exist"}`).
   - **Valid Entity but Online Features Missing (`cache_hit=False`):** Returns **HTTP 200 with degraded status metadata** and a **genuine, non-zero prediction** computed by feeding the imputed feature vector (calendar harmonics calculated from current UTC time + zero rolling counts + zone base categorical encoding) directly into the LightGBM booster:
     ```json
     {
       "zone_id": 263,
       "horizon_minutes": 15,
       "predicted_pickups": 4.2,
       "status": "degraded_fallback",
       "cache_hit": false,
       "warning": "Real-time features unavailable in Redis; model inferred using default/historical feature imputation.",
       "model_version": "1",
       "as_of": "2026-09-08T15:30:00Z"
     }
     ```
   - **Rationale:** Imputed inference is NOT a hardcoded 0.0 placeholder; the LightGBM model naturally accounts for zone-level baselines and time-of-day/day-of-week seasonality even when short-term rolling trip counters are cold. Returning HTTP 503 or 404 for quiet zones would crash frontend dashboards and agent tool calls. HTTP 200 with explicit degraded status metadata allows clients to render best-effort baseline predictions while displaying visual warning badges to operators.
   - **HTTP 503 Service Unavailable** is strictly reserved for fatal serving failures where both the primary MLflow model and the local baseline fallback fail to execute.

**Consequences:**
- Sub-5ms cached response latency, sub-20ms uncached single-entity latency, and sub-50ms full-city batch latency.
- Lean, robust startup model loading in Phase 5 without speculative hot-reload machinery; atomic reload tracked cleanly for Phase 6.
- Direct alignment with verified MLflow 3.x Production stage registry API.
- Genuine model predictions on imputed features during feature cache misses with clear degradation metadata.
- Clean contract separation between non-existent entities (HTTP 404), unmaterialized features (HTTP 200 degraded), and system outages (HTTP 503).
