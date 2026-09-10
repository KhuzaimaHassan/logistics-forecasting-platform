# Ticket Breakdown — Phase 5: Online Serving

## Epic Summary
Implement the production-grade online inference and serving layer for real-time demand and ETA forecasting in NYC taxi zones and corridors. Build a high-performance FastAPI service that resolves and loads trained LightGBM models from the MLflow Model Registry in `Production` stage at startup with graceful baseline fallback, exposes single and batch prediction endpoints per `docs/API.md` and ADR-020, retrieves real-time feature vectors with sub-second freshness from the Feast Redis online store (`FeastOnlineClient`), provides sub-5ms low-latency prediction caching with a 60-second TTL in Redis, manages container lifecycle via standard restarts/redeployments, and implements robust graceful degradation when online features are cold or unmaterialized (evaluating genuine non-zero model predictions on imputed feature defaults).

---

## Proposed Architecture Decisions

1. **ADR-020: Online Serving Architecture — MLflow Model Lifecycle, Startup Loading, Batch Inference Contracts, 60s Prediction Caching, and Degraded Fallbacks**:
   - **Startup Eager Load & Container Restart Lifecycle:** Models are eagerly loaded and warmed up into memory on FastAPI startup (`lifespan` handler). Picking up newly-promoted models in Phase 5 is handled via standard container restarts/redeployments (`docker compose restart serving`), deferring dynamic in-memory hot-reload machinery to Phase 6 CI/CD.
   - **MLflow Production Stage API Direct Targeting:** Verified against MLflow 3.15.1 (`pyproject.toml` pins `>=2.11.0`): `MlflowClient.get_latest_versions(..., stages=['Production'])` remains fully functional and directly matches `src/training/pipeline.py`'s model promotions.
   - **Batch Endpoint Support (`POST /predict/demand/batch`, `POST /predict/eta/batch`):** Vectorized single-call endpoints for high-throughput UI live map rendering across all 263 active zones and corridors, using single Redis MGET queries and matrix LightGBM inference.
   - **60-Second Prediction Caching TTL:** Bounded 60s Redis TTL prevents redundant inference stampedes while bounding prediction staleness to 1 minute of real-world streaming updates.
   - **Degraded Fallback Contract:** Returns HTTP 404 for invalid zone IDs ($< 1$ or $> 265$), HTTP 200 with `"status": "degraded_fallback"` and a genuine non-zero model output (e.g. `4.2` pickups) computed by running LightGBM over the imputed feature vector when Feast online features are missing (`cache_hit=False`), and HTTP 503 strictly for fatal service runtime failures.

---

## Tickets

### M5-1: Model Registry Loader, Production Stage Resolution & Startup Caching (ADR-020) [COMPLETED]
- **Scope / Acceptance Criteria:**
  - Implement `ModelLoaderService` in `src/serving/model_loader.py`:
    - Connects to MLflow tracking server and queries the Model Registry for registered models:
      - `demand_lightgbm_model`
      - `corridor_duration_lightgbm_model`
    - Resolves active versions in `Production` stage directly via `client.get_latest_versions(model_name, stages=['Production'])`.
    - Implements startup eager loading: loads LightGBM model boosters / MLflow pyfunc artifacts into memory and executes a dummy warmup inference.
    - Implements graceful baseline fallback: if MLflow is unreachable or models are missing in the registry, falls back to deterministic local baseline estimators (`src/training/baseline.py` / serialized fallback artifacts) with logged warnings and degraded status indicators.
    - Exposes model metadata inspectable by health endpoints (`model_name`, `version`, `stage`, `run_id`, `loaded_at`, `status`).
  - Unit & contract tests in `tests/test_model_loader.py`:
    - Test model loading from mock/live MLflow registry with `stage='Production'`.
    - Test fallback to baseline when registry is empty or network fails.
    - Test metadata inspection and warmup execution.
  - Live socket verification in `scripts/verify_m5_1_live_model_loading.py` proving startup loading from live MLflow server over HTTP, production stage resolution, inference execution, and graceful baseline fallback.
- **Per-Ticket Context:** `docs/Decisions.md` (ADR-020), `src/training/pipeline.py`, `src/common/mlflow_utils.py`.
- **Files Touched:** `src/serving/model_loader.py`, `tests/test_model_loader.py`, `scripts/verify_m5_1_live_model_loading.py`.
- **Estimated Size:** ~250 lines.
- **Depends On:** Phase 3 baseline models, Phase 4 real-time layer.

### M5-2: FastAPI Prediction & Inspection Endpoints (Single & Batch) (ADR-020, API.md)
- **Scope / Acceptance Criteria:**
  - Implement Pydantic request and response schemas in `src/serving/schemas.py`:
    - `DemandPredictionRequest`, `DemandPredictionResponse`
    - `DemandBatchPredictionRequest`, `DemandBatchPredictionResponse`
    - `ETAPredictionRequest`, `ETAPredictionResponse`
    - `ETABatchPredictionRequest`, `ETABatchPredictionResponse`
    - `FeatureLookupResponse`
    - `HealthCheckResponse`
    - `PipelineStatusResponse`
  - Implement FastAPI endpoint routing in `src/serving/app.py` (and modular routers in `src/serving/routers/` if needed):
    - `GET /health`: Comprehensive liveness and readiness check reporting database status (`warehouse.trips`), Redis online store connectivity, MLflow registry connectivity, and loaded model versions/stages.
    - `GET /predict/demand/{zone_id}`: Validates zone ID in $[1, 265]$; retrieves Feast online features via `FeastOnlineClient`; prepares model feature matrix; scores demand prediction; returns structured response.
    - `POST /predict/demand/batch`: Batches multiple `zone_ids` (or all 263 active zones); performs vectorized Feast lookup and vectorized LightGBM matrix scoring.
    - `GET /predict/eta?origin={origin_id}&dest={dest_id}`: Validates origin and destination zones; queries Feast corridor duration and origin/dest demand features; evaluates log1p duration model; inverts via $\max(60.0, \exp(\hat{y}) - 1.0)$; returns duration in seconds and minutes.
    - `POST /predict/eta/batch`: Vectorized corridor batch inference.
    - `GET /features/{entity_type}/{entity_id}`: Inspects online store feature vector for `zone` or `corridor` with `cache_hit` flag and raw feature dictionary.
    - `GET /pipeline/status`: Queries `warehouse.pipeline_runs` or Prefect execution state for recent pipeline health.
  - Comprehensive unit and endpoint tests in `tests/test_serving_endpoints.py`:
    - Tests all endpoints using `TestClient`.
    - Asserts HTTP 200, 400 (bad parameters), 404 (non-existent zone IDs), 422 (validation errors).
- **Per-Ticket Context:** `docs/API.md`, `docs/Decisions.md` (ADR-020), `src/features/client.py`.
- **Files Touched:** `src/serving/app.py`, `src/serving/schemas.py`, `tests/test_serving_endpoints.py`.
- **Estimated Size:** ~400 lines.
- **Depends On:** M5-1.

### M5-3: Low-Latency Prediction Caching (Redis 60s TTL) & Degraded Mode Fallback (ADR-020) [COMPLETED]
- **Scope / Acceptance Criteria:**
  - Implement transparent prediction caching layer in `src/serving/cache.py`:
    - Transparently caches prediction responses in Redis with keys `pred:demand:{zone_id}:{horizon}` and `pred:eta:{origin}:{dest}`.
    - Sets strict **60-second (60s)** TTL.
    - Falls back to in-memory TTL LRU cache if Redis is temporarily unreachable.
  - Implement resilient Degraded Mode handling (referencing M2-4):
    - When `FeastOnlineClient` returns `cache_hit=False` (unmaterialized entity or expired past 24h TTL):
      - Returns HTTP 200 with `"status": "degraded_fallback"`, `"cache_hit": false`, and explicit warning string.
      - **Non-Zero Imputed Inference:** Imputes missing rolling counts to 0, computes continuous calendar harmonics from current UTC `now`, and feeds the feature vector through the actual LightGBM booster. Asserts `predicted_pickups` produces a realistic, non-zero estimate (e.g. `4.2`) reflecting zone base rate and time-of-day/day-of-week seasonality, rather than a hardcoded 0.0 placeholder.
      - Validates that HTTP 404 is strictly reserved for invalid zone IDs ($< 1$ or $> 265$).
  - Unit and integration tests in `tests/test_serving_cache.py`:
    - Verifies cache hit returns identical prediction with sub-2ms latency.
    - Verifies cache expiration after 60 seconds.
    - Verifies degraded response contract when Feast features are missing, asserting non-zero output.
- **Per-Ticket Context:** `docs/Decisions.md` (ADR-020), `src/features/client.py`.
- **Files Touched:** `src/serving/cache.py`, `src/serving/app.py`, `tests/test_serving_cache.py`.
- **Estimated Size:** ~300 lines.
- **Depends On:** M5-2.

### M5-4: End-to-End Live Serving Smoke Test, Docker Integration & CI Verification
- **Scope / Acceptance Criteria:**
  - Implement comprehensive live smoke verification script in `scripts/verify_serving_live_smoke.py`:
    - Connects to real local/Docker dependencies (PostgreSQL, Redis, MLflow, FastAPI).
    - Verifies `/health` endpoint reports healthy dependencies and loaded `Production` models.
    - Tests `/predict/demand/{zone_id}` against real active zones (e.g. 161 Midtown, 236 UES, 132 JFK).
    - Tests `/predict/eta` against real corridors (e.g. 161 to 236).
    - Tests `/predict/demand/batch` across all 263 NYC taxi zones, asserting latency $< 50\text{ms}$.
    - Tests prediction caching latency ($< 5\text{ms}$ on cache hit).
    - Tests degraded mode response for cold/unmaterialized entities, asserting genuine non-zero model output.
  - Update `src/serving/Dockerfile` and `infra/docker-compose.yml` to include the serving service with health checks and proper environment wiring.
  - Wire live serving smoke verification into `.github/workflows/ci.yml`.
  - Update `docs/Roadmap.md` marking Phase 5 as complete.
- **Per-Ticket Context:** `docs/Architecture.md`, `docs/Deployment.md`, `docs/Roadmap.md`.
- **Files Touched:** `scripts/verify_serving_live_smoke.py`, `infra/docker-compose.yml`, `src/serving/Dockerfile`, `.github/workflows/ci.yml`, `docs/Roadmap.md`, `docs/tickets/phase-5-online-serving.md`.
- **Estimated Size:** ~400 lines.
- **Depends On:** M5-1, M5-2, M5-3.

---

## Tracked Design Items

### DESIGN-001: Dynamic In-Memory Model Hot-Reload (Deferred to Phase 6 CI/CD)
- **Description:** Loading newly promoted models in Phase 5 relies on standard container restart / redeployment (`docker compose restart serving`). In Phase 6 (CI/CD & Retraining Automation), when automated retraining flows, drift detection triggers, and CD webhooks are implemented, dynamic in-memory hot-reload (`POST /models/reload` or internal signal) can be evaluated to enable zero-downtime model swaps without container cycling.
