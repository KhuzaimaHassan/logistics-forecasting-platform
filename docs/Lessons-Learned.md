# Lessons Learned

Filled in per milestone, not at the end — the point is to capture what actually happened vs. what the docs predicted, while it's fresh. Empty right now since no phase has started.

## Template (copy per milestone)

### M# — <Milestone name>

**What went as planned:**

**What didn't, and why:**

**What would change if starting this phase over:**

**Docs that needed updating after the fact:**
<!-- Link back to any Decisions.md entries or doc edits this milestone caused -->

---

<!-- Entries appended below as each milestone completes -->
 
### M0 — Infra Bootstrap

**What went as planned:**
- Multi-service single-host Docker Compose stack (10 services) configured cleanly using official images and colocated multi-arch service Dockerfiles.
- Fast Python environment and tooling established with `uv`, `black`, `ruff`, and `pytest`.
- Automated CI workflow executing both Python lint/testing and full Docker Compose multi-container build and health smoke test with zero startup crashes.
- Host provisioning and security automation on Oracle Cloud Ampere A1 automated via `infra/oracle-vm/provision.sh`.

**What didn't, and why:**
- *Dependency Bloat:* A single flat dependency list caused lightweight services (serving API, UI) to install 220+ packages across training/streaming libraries. Resolved in ADR-009 by structuring scoped optional dependency groups (`core`, `extract`, `transform`, `serving`, `ui`, `training`, `monitoring`, `dev`) while maintaining a single root lockfile.
- *Docker Build Context & Hatchling Wheel Caching:* Service Docker builds failed in container environments because Hatchling required `README.md` and attempted early wheel installation before service code was copied. Resolved by copying `README.md` into build contexts and passing `--no-install-project` during dependency caching layers.
- *Insecure Compose Credential Fallbacks:* Fallback defaults (`${VAR:-default}`) allowed compose stacks to silently run with unvalidated placeholder passwords. Resolved by strictly enforcing `${VAR:?VAR must be set}` syntax across database credentials and introducing `.env.ci` for automated CI execution.
- *Squash-Merge History Divergence:* Squash-merging `dev` into `main` produced distinct commit SHAs on `main` that caused 3-way merge conflicts on subsequent PRs from `dev`. Resolved by establishing a standing post-squash-merge sync PR rule.

**What would change if starting this phase over:**
- Define scoped optional dependency groups in `pyproject.toml` from day one rather than refactoring later.
- Enforce explicit fail-fast credential patterns (`${VAR:?error}`) from the initial compose file draft.

**Docs that needed updating after the fact:**
- [Decisions.md (ADR-008, ADR-009)](file:///docs/Decisions.md)
- [GitHub-Setup.md (Branch Strategy & Post-Squash Sync Rule)](file:///docs/GitHub-Setup.md)
- [Roadmap.md (Build-Layer Caching Note & Phase 0 Status)](file:///docs/Roadmap.md)
- [Deployment.md & Security.md](file:///docs/Deployment.md)

---

### M1 — Historical ETL

**What went as planned:**
- TLC Parquet batch extractor cleanly downloaded and processed monthly NYC yellow taxi records directly from official AWS CloudFront endpoints without intermediary storage bottlenecks.
- Two-stage ingestion architecture cleanly separated unvalidated raw records (`raw.tlc_yellow_trips`) from validated, cleaned, and normalized warehouse tables (`warehouse.trips`).
- Prefect orchestration flow (`historical_tlc_batch_etl_flow`) cleanly encapsulated the pipeline with automated task retries and run logging.

**What didn't, and why:**
- *Socrata GeoJSON 404 Deprecation (ADR-011):* The original architecture assumed NYC Open Data Socrata GeoJSON endpoints would supply taxi zone spatial boundaries. During M1-1 implementation, the endpoint returned HTTP 404 due to upstream NYC Open Data portal schema migrations. This was resolved by pivoting to official TLC Shapefiles and CSV tables, implementing custom polygon centroid math (`centroid_lat`, `centroid_lon`) via `shapely` in `src/extract/taxi_zones.py`, and explicitly scoping centroids to lightweight UI map plotting (ADR-011).
- *Bulk Ingestion Scaling & Memory Pressure:* Naive SQLAlchemy ORM insertion (`session.add_all()`) took over 4 hours for a single month of data (3M+ records) and exhausted container memory. Resolved by rewriting the loader in `src/transform/batch_transformer.py` to use chunked PostgreSQL `COPY` / bulk `insert()` in 50,000-row chunks with explicit batch commits, completing the entire 3,066,766-record load in 102 seconds.
- *Strict Rejection Semantics:* Data cleaning surfaced 126,625 malformed or physically implausible records (~4.13% of January 2023 trips) violating duration bounds ($60\text{s} \le \text{duration} \le 86,400\text{s}$), distance bounds ($0.01\text{ mi} \le \text{distance} \le 300\text{ mi}$), or speed bounds ($\le 100\text{ mph}$). These were quarantined with explicit rejection metrics rather than silently dropped or imputed.
- *Second-Run Idempotency:* Rerunning the batch ETL flow without partition checks created duplicate rows in raw tables. Resolved by implementing SHA-256 file signature tracking and partition-aware skip checks in Prefect tasks.

**What would change if starting this phase over:**
- Bypass third-party Open Data APIs for static geospatial boundaries from the outset and use canonical TLC Shapefile packages directly.
- Implement chunked PostgreSQL `COPY` streaming directly from Parquet chunk readers instead of initially attempting ORM-level batched inserts.

**Docs that needed updating after the fact:**
- [Decisions.md (ADR-010, ADR-011)](file:///docs/Decisions.md)
- [Database.md (Raw and Warehouse Schema DDL)](file:///docs/Database.md)
- [Roadmap.md (Historical ETL Verification Record)](file:///docs/Roadmap.md)

---

### M2 — Feature Store

**What went as planned:**
- Dual-entity Feast architecture (`zone_id` for demand, `corridor_id` for trip duration) mapped cleanly to business forecasting requirements.
- PostgreSQL SQL Registry (`warehouse.feast_metadata`) and Redis online store (`localhost:6379`) integrated cleanly within the single-host Docker Compose topology.
- Point-in-time (PIT) join engine successfully retrieved historical feature vectors for training without lookahead bias or future data leakage.

**What didn't, and why:**
- *SQLite Adapter Test Leak in Offline Retrieval:* When running unit tests with `pytest`, an unrelated test fixture registered a global Python SQLite adapter (`sqlite3.register_adapter(datetime.datetime, ...)`). This global adapter leaked into Feast's offline SQL retrieval engine, causing UTC datetime series to serialize as truncated strings without timezone offsets, triggering downstream pandas datetime parsing exceptions. Resolved by implementing the `to_utc_datetime_series` helper in `src/features/views.py` with explicit `pd.to_datetime(..., utc=True)`.
- *Corridor ID String Mismatch:* Initial implementations alternated between tuple representations `(161, 236)` and hyphenated strings `"161-236"`. Mismatched representations caused Feast online feature lookups to return empty feature vectors (`cache_hit=False`). Resolved by establishing a strict canonical string convention `"{PULocationID}-{DOLocationID}"` (e.g. `"161-236"`) enforced across entity definitions, SQL aggregations, and online retrieval clients (ADR-013).
- *15-Minute Tumbling Aggregations vs. Real-Time Latency:* Deep rolling aggregations computed on-the-fly during offline feature generation created significant query latency on PostgreSQL. Resolved by pre-aggregating 15-minute and 1-hour metrics into dedicated feature tables (`warehouse.zone_demand_features_hourly` and `warehouse.corridor_duration_features_hourly`).

**What would change if starting this phase over:**
- Enforce strict typing and canonical string formatting for composite entity keys (`corridor_id`) in the shared schemas before writing feature views.
- Isolate test database engines completely to prevent third-party database adapter pollution in the global Python runtime.

**Docs that needed updating after the fact:**
- [Decisions.md (ADR-012, ADR-013, ADR-014)](file:///docs/Decisions.md)
- [Feature-Store.md (Entity & Feature View Definitions)](file:///docs/Feature-Store.md)
- [Database.md (Feast Metadata & Feature Tables)](file:///docs/Database.md)

---

### M3 — Baseline Models & Training

**What went as planned:**
- Seasonal naive baseline models (hourly day-of-week demand and historical median corridor duration) established clear, defensible benchmark hurdles.
- LightGBM Poisson regressor for zone demand and LightGBM regressor for corridor ETA comfortably beat naive baselines.
- MLflow tracking server integrated cleanly with PostgreSQL backend store and artifact logging.

**What didn't, and why:**
- *The R2 Credential-Gap Pattern (ADR-007, PR #75, #78, #81):* Cloudflare R2 was selected in Phase 0 as S3-compatible cold storage for MLflow artifacts and database dumps. In local development, `.env` contained R2 credentials and uploads worked. However, in GitHub Actions CI, R2 secrets (`R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET_NAME`) were initially absent. This surfaced the recurring **credential-gap pattern** between local development and CI. Because CI could not reach R2, backup steps failed. Resolved by building a live verification harness (`scripts/verify_r2_live_smoke.py`), refactoring `R2BackupManager` to handle explicit parameter evaluation gracefully, and wiring genuine Cloudflare R2 repository secrets into GitHub Actions, verifying round-trip uploads and listing (`list_objects`) against bucket `mlflow-artifacts-logistics-forecasting-platform`.
- *Duration Target Outlier Skew & Log1p Transformation:* Initial duration models trained on raw trip seconds suffered severe gradient skew from extreme highway congestion outliers (trips > 10,000s). Predicted durations were frequently negative or wildly inflated. Resolved by training on log-transformed targets $y_{\text{log}} = \ln(1 + \text{duration\_sec})$ and inverting during inference via $\hat{y} = \max(60.0, \exp(\hat{y}_{\text{log}}) - 1.0)$, respecting the 60s physical trip floor and yielding a $+39.09\%$ MAE reduction (278.35s vs 456.95s) on the 256k-row validation split (ADR-017).
- *Initial Categorical Encoding Flaw:* In M3-4, training scripts inferred categorical categories dynamically from pandas slices or used string-split corridor tokens (`['161', '236']`). While this functioned during isolated training splits, it created a latent categorical misalignment bug that manifested later in Phase 5 serving.

**What would change if starting this phase over:**
- Audit and provision all cloud credentials (R2, third-party APIs) in GitHub Actions secrets simultaneously with local `.env` creation to avoid CI credential-gap hurdles.
- Standardize all categorical feature domains globally across training and serving from the first model prototype.

**Docs that needed updating after the fact:**
- [Decisions.md (ADR-007, ADR-015, ADR-016, ADR-017)](file:///docs/Decisions.md)
- [AI-Pipeline.md (Model Architectures, Metrics & Benchmarks)](file:///docs/AI-Pipeline.md)

---

### M4 — Real-Time Layer

**What went as planned:**
- Single-broker Redpanda instance operated with near-zero latency, full Kafka API compatibility, and significantly lower memory overhead than a JVM-based Kafka cluster.
- Configurable historical trip replay producer successfully streamed January 2023 TLC trips at controllable acceleration factors ($1\times$ to $60\times$) partitioned by `pickup_zone_id`.
- Stream consumer validated events with Pydantic, ingested valid records to `warehouse.trips`, pushed real-time feature updates to Redis via Feast Push API (`store.push()`), and isolated invalid records to `trip.events.deadletter`.

**What didn't, and why:**
- *Both `-s ours` Merge Investigations:*
  - *Merge Investigation 1 (PR #99 → PR #100 / #101):* When PR #99 (`release: stream consumer, real-time validation & PostgreSQL ingestion (M4-3)`) was squash-merged into `main` as commit `9b2c0f6`, attempting a standard 3-way `git merge origin/main` into `dev` produced severe textual merge conflicts in `.github/workflows/ci.yml` (`3bffb69`). Because squash-merging creates a new commit on `main` that shares a common ancestor far in the past, git's 3-way merge attempted to reconcile files already present on `dev` against the squash commit, causing spurious conflicts. PR #100 required manual conflict resolution and PR #101 followed.
  - *Merge Investigation 2 (PR #103 → PR #104, PR #106 → PR #107, PR #109 → PR #110):* To permanently eliminate recurring merge conflicts after every milestone squash-merge, the team investigated the git `-s ours` strategy (`git merge -s ours origin/main`). Because `dev` already contains all code from the squash merge in its granular commit history, `-s ours` records the squash commit from `main` as a parent of `dev` without touching a single file in the working tree. This immediately advances the merge-base between `dev` and `main` to the tip of `main`. Subsequent PRs from `dev` to `main` became clean fast-forwards with zero conflicts. This was proven in PR #104 (`534bdc0`), repeated in PR #107 (`8b804f5`) and PR #110 (`1c50b16`), and formalized as a standing post-squash sync rule in `docs/GitHub-Setup.md`.
- *The MTA & OpenWeatherMap Credential-Gap Pattern (PR #93 / PR #115):* In M4-2, external feed producers supported synthetic fallback when API keys were missing. In local development, developers placed keys in `.env`, which silently leaked into unit tests expecting fallback behavior, while in CI tests failed or were skipped because repository secrets were unconfigured. PR #115 resolved this by explicitly passing `api_key=""` in fallback unit tests to guarantee hermetic test isolation, while wiring real `MTA_API_KEY` and `OPENWEATHERMAP_API_KEY` into GitHub Actions repository secrets, proving live polling with 196 genuine MTA subway alert records and live NYC weather observations.
- *Dual-Maintenance Drift Risk (DRIFT-001):* In-memory sliding deques in `StreamFeatureAggregator` ($O(1)$ amortized updates) and vectorized Pandas/SQL windowing in `offline_extractor.py` implemented identical temporal logic across two different execution models. This created a dual-maintenance drift risk requiring documented parity checks to prevent training-serving skew.

**What would change if starting this phase over:**
- Adopt the `-s ours` post-squash sync strategy from milestone 1 instead of discovering the squash-merge ancestor divergence the hard way in Phase 4.
- Mock environment variables explicitly using `monkeypatch.delenv` or parameter overrides in test suites from day one.

**Docs that needed updating after the fact:**
- [Decisions.md (ADR-018, ADR-019)](file:///docs/Decisions.md)
- [GitHub-Setup.md (Standing Post-Squash Sync Rule)](file:///docs/GitHub-Setup.md)
- [ETL-Streaming.md (Redpanda Topics & Consumer Specifications)](file:///docs/ETL-Streaming.md)
- [tickets/phase-4-real-time-layer.md (DRIFT-001 Risk Analysis)](file:///docs/tickets/phase-4-real-time-layer.md)

---

### M5 — Online Serving

**What went as planned:**
- FastAPI serving application delivered sub-15ms p95 prediction latency across `/predict/demand` and `/predict/eta`.
- ModelLoaderService dynamically loaded promoted Production models from MLflow at startup with graceful fallback to seasonal naive baselines.
- Two-tier prediction caching in Redis with 60s TTL achieved sub-5ms cached response times and 100% cache hit validation.
- Vectorized batch prediction endpoints successfully scored all 263 NYC taxi zones in a single call.

**What didn't, and why:**
- *The Categorical Encoding Bug (M5-2 / ADR-017 Addendum):*
  - *Symptom:* During batch prediction sensitivity testing in `scripts/verify_serving_endpoints_smoke.py`, LightGBM ETA and demand predictions returned identical, flat values regardless of input zones (e.g., JFK Airport Zone 132 and Midtown Manhattan Zone 230 yielded nearly the exact same predicted travel duration).
  - *Root Cause:* In M3, training pipelines used dynamically inferred categories or string-split corridor tokens (`['161', '236']`). At serving time, incoming integer zone IDs (`pickup_zone_id: 161`, `dropoff_zone_id: 236`) were passed to `model.predict()` without matching categorical metadata. LightGBM treated incoming integer categories as unseen categories, silently routing every inference query down the default root leaf branch of each decision tree.
  - *Fix:* Standardized the categorical domain across the entire repository: defined `ACTIVE_ZONE_CATEGORIES = list(range(1, 264))` and enforced explicit conversion `pd.Categorical(..., categories=ACTIVE_ZONE_CATEGORIES)` across `train_duration.py`, `train_demand.py`, and `feature_extractor.py`. This ensured identical categorical index mapping between offline training and online serving, restoring full model sensitivity across all 263 zones.
- *MLflow Internal DNS Rebinding (HTTP 403):* When FastAPI in Docker attempted to load models from the internal MLflow container (`http://mlflow:5000`), MLflow rejected the requests with `403 Forbidden` due to Host header DNS rebinding protections. Resolved by configuring `MLFLOW_SERVER_DISABLE_SECURITY_MIDDLEWARE=true` on the internal MLflow container.
- *Missing OpenMP Runtime (`libgomp1`):* In containerized environments, the FastAPI service failed to start with `ImportError: libgomp.so.1: cannot open shared object file` because Debian slim base images omit GCC OpenMP runtime libraries required by LightGBM C extensions. Resolved by installing `libgomp1` in `src/serving/Dockerfile` and `src/orchestration/Dockerfile`.
- *Degraded Fallback Caching:* Unmaterialized zones returning default imputed features originally risked polluting Redis with long-lived cached fallback predictions. Resolved by implementing a zero-TTL policy for degraded predictions, ensuring fresh features are fetched as soon as Feast materialization completes.

**What would change if starting this phase over:**
- Define global categorical constants (`ACTIVE_ZONE_CATEGORIES`) in a shared module during Phase 2/3 and enforce them with schema unit tests.
- Include all native C-extension dependencies (`libgomp1`) in base Dockerfiles from Phase 0.

**Docs that needed updating after the fact:**
- [Decisions.md (ADR-017 Addendum, ADR-020)](file:///docs/Decisions.md)
- [API.md (Endpoints, Schemas & Latency Budgets)](file:///docs/API.md)

---

### M6 — CI/CD & Retraining

**What went as planned:**
- Scheduled retraining flow (`scheduled-model-retraining-flow`) orchestrated reliably on Prefect Cloud.
- ModelPromotionGate successfully evaluated 4-stage criteria: seasonal naive baseline check, champion/challenger comparison against active Production models, 2.0% minimum improvement hurdle (`min_improvement_pct=0.02`), and atomic MLflow stage transitions.
- Automated deployment script `src/orchestration/deploy.py` registered flows and schedules without manual UI interaction.

**What didn't, and why:**
- *Slow Multi-Container Docker Builds in CI:* As the repository grew to 5 custom container images (`serving`, `ui`, `extract`, `transform`, `prefect-worker`), GitHub Actions CI build times ballooned to 12–15 minutes per PR. Resolved by implementing Docker Buildx with GitHub Actions cache backend (`cache-from: type=gha`, `cache-to: type=gha,mode=max`), slashing rebuild times to under 3 minutes.
- *Speculative Drift-Triggered Retraining Deferral (ADR-022):* Initial plans called for triggering model retraining directly on incoming streaming data variance in Phase 6. Analysis revealed that triggering retraining without formal statistical drift monitoring (Evidently AI) and cooldown throttles risked unstable retraining churn. Per ADR-022, drift-triggered retraining was explicitly deferred to Phase 8, preserving Phase 6 focus on scheduled retraining and safe promotion gating.
- *Prefect Worker Dependency Coupling:* Serving containers initially imported orchestration modules for model loading, inadvertently pulling in heavy Prefect dependencies. Resolved by decoupling `model_loader.py` to depend strictly on MLflow Client.

**What would change if starting this phase over:**
- Configure Docker Buildx GHA caching in CI during Phase 0 rather than retrofitting it in Phase 6.
- Keep serving and orchestration dependency boundaries strictly decoupled from day one.

**Docs that needed updating after the fact:**
- [Decisions.md (ADR-021, ADR-022)](file:///docs/Decisions.md)
- [Deployment.md (Prefect Cloud & Retraining Topology)](file:///docs/Deployment.md)
- [Roadmap.md (Phase 6 Status & Promotion Gate Specifications)](file:///docs/Roadmap.md)

---

### M7 — Agent Layer (LangGraph Ops Copilot)

**What went as planned:**
- LangGraph Ops Copilot delivered conversational diagnostics over pipeline status, Feast features, predictions, and model cards.
- Dual-provider LLM cascade (Groq `llama-3.3-70b-versatile` → Gemini `gemini-2.0-flash` → Hermetic `MockLLMProvider`) provided 100% uptime with zero external outage sensitivity (ADR-024).
- Sub-millisecond FAISS CPU vector index with deterministic hashing fallback enabled fast, zero-GPU semantic retrieval (ADR-025).
- Multi-tab Streamlit Ops Copilot UI integrated smoothly with quick-action chips and tool call inspection expanders.

**What didn't, and why:**
- *Adversarial Tool Invocations & Security Boundaries (ADR-023):* Initial LLM prototypes occasionally attempted to execute destructive SQL queries (e.g. `DROP TABLE`, `UPDATE warehouse.trips`) or call non-existent mutative tools when prompted with jailbreak phrases. Resolved by establishing an immutable read-only tool allowlist (`get_features`, `query_recent_predictions`, `query_pipeline_status`, `search_logs_and_model_cards`), enforcing parameterized SQL bindings, and verifying security boundaries against adversarial injection suites in `tests/test_agent_guardrails.py`.
- *Provider Rate Limits & API Outages:* External LLM endpoints experienced sporadic rate-limit spikes (HTTP 429). Resolved by engineering a cascading fallback provider: if Groq fails or times out (15s), the agent falls back to Gemini; if Gemini fails or credentials are unconfigured, it transparently degrades to `MockLLMProvider` with explicit visual warning badges in the UI.
- *Heavy Sentence-Transformers Embeddings in CI:* Pulling multi-hundred megabyte transformer models for FAISS embeddings slowed CI runs and introduced external HuggingFace hub network dependencies. Resolved by building a lightweight deterministic hashing vectorizer fallback for CI and air-gapped test environments.

**What would change if starting this phase over:**
- Establish the immutable read-only tool allowlisting constraint from the very first graph specification.
- Design the multi-provider fallback cascade architecture before writing provider integrations.

**Docs that needed updating after the fact:**
- [Decisions.md (ADR-023, ADR-024, ADR-025)](file:///docs/Decisions.md)
- [Agents.md (LangGraph Topology, Tools & Guardrails)](file:///docs/Agents.md)
- [UI.md (Copilot Chat Tab & Inspection Panels)](file:///docs/UI.md)

---

### M8 — Monitoring (Evidently Drift Reports & Dashboard)

**What went as planned:**
- Evidently AI 0.7.x integrated cleanly across Data Drift, Prediction Drift, and Performance Decay analyzers.
- Scheduled Prefect monitoring flow (`daily-monitoring-flow`) successfully generated and persisted structured JSON metrics and interactive HTML reports to PostgreSQL `warehouse.monitoring_reports`.
- Streamlit Tab 4 ("Drift Monitoring") surfaced real-time scorecards, feature drift tables, alert banners, and embedded standalone HTML reports.
- LangGraph Ops Copilot incorporated Tool 5 (`query_drift_reports`) to answer natural-language operational questions about model drift.

**What didn't, and why:**
- *The Evidently Argument-Order Proof (ADR-026):*
  - *Issue:* In Evidently `0.7.x`, `Report.run()` takes `current_data` as the first positional argument and `reference_data` as the second. Passing datasets positionally (`report.run(reference, current)`) silently inverts all statistical drift delta calculations, reporting negative shifts instead of positive shifts and corrupting Wasserstein distance and KS statistics.
  - *Proof:* Empirically proven in `tests/test_monitoring_analyzers.py` using asymmetric distributions (reference $\mathcal{N}(10, 1)$, current $\mathcal{N}(50, 1)$). Positional argument passing reported an inverted negative shift, while keyword passing `report.run(current_data=curr, reference_data=ref)` accurately captured positive drift ($p < 0.001$). Positional argument passing was strictly prohibited and mandatory keyword arguments were enforced across the codebase.
- *Seasonality False Alarms & Hybrid Reference Windows:* Static baseline comparison (comparing current data against January 2023) flagged normal weekend commute variance as severe data drift. Conversely, purely rolling windows failed on cold starts. Resolved by designing a hybrid window strategy: 14-day rolling historical window (two full weekly cycles) with automatic cold-start fallback to the fixed January 2023 baseline when prediction history is under 500 rows or 7 days (ADR-026).
- *Automated Retraining Thrashing & 48-Hour Cooldown:* Automated drift-triggered retraining risked entering runaway retraining loops on persistent distribution shifts. Resolved by implementing a 48-hour universal cooldown period checked directly against prior execution records in `warehouse.monitoring_reports`.
- *Timezone-Naive Datetime Bug in HTML Serving Endpoint:* Live testing of `GET /monitoring/reports/{id}/html` failed because SQLAlchemy returned naive datetimes from PostgreSQL, causing `generated_at.isoformat()` and HTML rendering utilities to throw formatting exceptions. Resolved by coercing naive datetimes to timezone-aware UTC prior to template interpolation.
- *RAG Index Staleness vs. Live Query Coupling:* Identified that FAISS RAG index freshness is coupled to daily rebuild execution. Documented the failure mode across code and documentation: if the daily rebuild fails, the RAG index continues serving the prior build's chunks aging past 14 days without pruning, while live Copilot queries bypass RAG via Tool 5 against PostgreSQL.

**What would change if starting this phase over:**
- Empirically verify third-party library method signatures and argument conventions with synthetic asymmetric fixtures before building analyzer abstractions.
- Design cold-start fallback paths for rolling reference windows in the initial specification.

**Docs that needed updating after the fact:**
- [Decisions.md (ADR-026)](file:///docs/Decisions.md)
- [Monitoring.md (Analyzers, Thresholds & RAG Ingestion Policy)](file:///docs/Monitoring.md)
- [AGENTS.md (Ground Rules & Storage Invariants)](file:///AGENTS.md)
- [Roadmap.md (Phase 8 Completion Record)](file:///docs/Roadmap.md)

