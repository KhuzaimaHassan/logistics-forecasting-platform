# Ticket Breakdown — Phase 6: CI/CD

## Epic Summary
Implement the automated continuous integration, continuous delivery, and model retraining pipeline for the Logistics Demand & ETA Forecasting Platform. Build a safe model promotion gate that evaluates candidate retrained models against active Production champion models before registry transition, implement a scheduled Prefect retraining flow coordinated on Prefect Cloud (ADR-021) with historical warehouse extraction and R2 artifact backup, introduce Docker Buildx GitHub Actions layer caching (fast-follow since M2) to accelerate CI builds, author a deploy-on-merge GitHub Actions workflow (marked as blocked pending manual Oracle VM host provisioning), and verify the complete lifecycle with end-to-end integration smoke tests.

---

## Architecture Decisions & Constraints

1. **ADR-021: Retraining Orchestration — Prefect Flow vs. GitHub Actions Cron**:
   - **Decision:** Orchestrate periodic model retraining via a Prefect flow (`retraining_flow` in `src/orchestration/flows/retraining_flow.py`) scheduled on Prefect Cloud and executed on our dedicated worker/host, rather than GitHub Actions cron.
   - **Rationale:** Retraining is data- and compute-intensive (querying millions of trips from `warehouse.trips`, generating Feast offline feature matrices, fitting gradient boosted trees, logging artifacts to MLflow, and backing up to Cloudflare R2). GitHub Actions hosted runners have strict compute/memory bounds (2 vCPUs, 7GB RAM, 6-hour ceilings) and lack private network access to internal PostgreSQL and MLflow services without fragile tunnels or exposing internal databases to the public internet (violating ADR-002 and Ground Rule #4). Prefect is already established as the platform's scheduler (ADR-005), offering unified state management, observability, retries, and direct network locality to the warehouse and feature store.

2. **ADR-022: Drift-Triggered Retraining Deferred to Phase 8 (Evidently AI)**:
   - **Decision:** Phase 6 implements the scheduled retraining cadence (weekly Sunday off-peak cron) and model promotion gate. Drift-triggered retraining (triggering retrains automatically when data or prediction drift exceeds tolerances) is explicitly deferred to Phase 8.
   - **Rationale:** Evidently AI drift metrics calculations, data drift test suites, and monitoring dashboards are Phase 8 deliverables. Attempting to implement drift-triggered retrain execution in Phase 6 would require mocking nonexistent Phase 8 drift collectors. Vertical slicing discipline dictates establishing the safe retraining pipeline and promotion gate first in Phase 6; Phase 8 will then wire its drift alert webhooks directly into this tested flow.

3. **Oracle Cloud VM Status (Deploy-on-Merge Prerequisite)**:
   - **Status: Host Not Provisioned / Blocked.**
   - While provisioning scripts and network specifications exist in `infra/oracle-vm/`, the instance has not yet been provisioned in the Oracle Cloud Console, and no host IP or SSH credentials exist in `.env` or GitHub repository secrets.
   - Deploy-to-VM automation (M6-4) will be authored as a fully valid GitHub Actions workflow (`.github/workflows/deploy.yml`) gated on secret presence, logging an informative skip notice when credentials are absent, and documented in `docs/Deployment.md` as pending manual host provisioning. Today's execution scopes strictly to components that do not depend on external host connectivity (M6-1, M6-2, M6-3, M6-5).

4. **Model Promotion Safety Gate Contract**:
   - Retraining must NEVER unconditionally replace the `Production` model.
   - The candidate model must satisfy two gates:
     1. Outperform the naive baseline: $\text{MAE}_{\text{cand}} \le \text{MAE}_{\text{baseline}}$.
     2. Outperform the active `Production` model in the MLflow Model Registry: $\text{MAE}_{\text{cand}} \le \text{MAE}_{\text{prod}} \times (1.0 - \delta)$ (where default improvement threshold $\delta = 0.0$, requiring candidate error to be strictly less than or equal to current production).
   - If no model exists in `Production` stage (cold start / fresh registry), beating the naive baseline is sufficient for initial promotion.
   - If the candidate fails either condition, it transitions to `Staging` (or remains unpromoted), logs detailed comparative evaluation metrics, and prevents unauthorized regression in serving.

---

## Tickets

### M6-1: Model Promotion Safety Gate & Champion/Challenger Metric Comparison (ADR-021)
- **Scope / Acceptance Criteria:**
  - Implement `ModelPromotionGate` in `src/training/promotion.py`:
    - Queries MLflow Model Registry for the current `Production` version of a target registered model (`demand_lightgbm_model`, `corridor_duration_lightgbm_model`).
    - Fetches the active production model's validation metrics from its associated MLflow run (`val_mae`, `val_rmse`, `val_wape`).
    - Evaluates the new candidate model's validation metrics against:
      1. Naive baseline error (`candidate_mae <= baseline_mae`).
      2. Active production model error (`candidate_mae <= production_mae * (1.0 - min_improvement_threshold)`).
    - If candidate wins:
      - Transitions candidate version to `stage="Production"` with `archive_existing_versions=True`.
      - Optionally sets alias `"champion"` on the new version and tags the previous champion as `"previous_champion"`.
      - Returns outcome dictionary indicating `promoted=True`, `stage="Production"`, and metric comparison summary.
    - If candidate loses (higher error than production or fails baseline):
      - Transitions candidate version to `stage="Staging"` (or leaves stage unassigned).
      - Returns outcome dictionary indicating `promoted=False`, `stage="Staging"`, and explicit failure reason (e.g. `Candidate MAE 4.85 > Production MAE 4.20`).
    - Handles cold start: if no `Production` version exists in MLflow, candidate is promoted as the inaugural champion if it outperforms the naive baseline.
  - Refactor `src/training/pipeline.py` to use `ModelPromotionGate` rather than unconditional baseline-only promotion.
  - Unit and contract tests in `tests/test_model_promotion_gate.py`:
    - Cold-start promotion when beating naive baseline.
    - Rejection when candidate MAE > active Production MAE (remains in Staging, production unmutated).
    - Approval and stage transition when candidate MAE < active Production MAE.
    - Rejection when candidate beats Production but fails baseline.
    - Boundary tolerance check with non-zero improvement threshold ($\delta > 0$).
- **Per-Ticket Context:** `src/training/pipeline.py`, `src/common/mlflow_utils.py`, `docs/Decisions.md` (ADR-021).
- **Files Touched:** `src/training/promotion.py`, `src/training/pipeline.py`, `tests/test_model_promotion_gate.py`.
- **Estimated Size:** ~350 lines.
- **Depends On:** Phase 3 training pipelines, Phase 5 model loader.

### M6-2: Scheduled Retraining Prefect Flow & Prefect Cloud Deployment (ADR-021, ADR-022)
- **Scope / Acceptance Criteria:**
  - Implement `retraining_flow` in `src/orchestration/flows/retraining_flow.py`:
    - Task 1 (`reconcile_offline_features_task`): Pulls recent `warehouse.trips` records into offline hourly feature tables (`warehouse.zone_demand_features_hourly`, `warehouse.corridor_duration_features_hourly`).
    - Task 2 (`train_demand_model_task`): Extracts demand training dataset, generates temporal train/val split, trains LightGBM booster, evaluates validation metrics, and logs run artifacts to MLflow experiment `nyc-taxi-demand`.
    - Task 3 (`train_duration_model_task`): Extracts corridor duration training dataset, trains LightGBM booster, evaluates log1p duration metrics, and logs run artifacts to MLflow experiment `nyc-taxi-corridor-duration`.
    - Task 4 (`evaluate_and_promote_task`): Invokes `ModelPromotionGate` for both demand and duration models, comparing new candidate metrics against active `Production` registry models.
    - Task 5 (`backup_artifacts_task`): Triggers Cloudflare R2 backup of MLflow model artifacts and registry metadata (ADR-007).
    - Task 6 (`notify_retraining_summary_task`): Generates structured execution summary with metric deltas, promotion statuses, and warnings.
  - Register scheduled deployment in `src/orchestration/deploy.py`:
    - Deployment name: `scheduled-model-retraining`.
    - Schedule: Weekly off-peak cron (e.g. `0 3 * * 0` — Sunday 03:00 UTC).
    - Work pool: `default-agent-pool` (or configured Prefect worker pool).
    - Tags: `["retraining", "mlops", "scheduled"]`.
  - Comprehensive unit and integration tests in `tests/test_retraining_flow.py` mocking/verifying each task in the flow sequence.
- **Per-Ticket Context:** `src/orchestration/flows/historical_etl.py`, `src/orchestration/flows/realtime_reconciliation_flow.py`, `src/training/pipeline.py`.
- **Files Touched:** `src/orchestration/flows/retraining_flow.py`, `src/orchestration/deploy.py`, `tests/test_retraining_flow.py`.
- **Estimated Size:** ~400 lines.
- **Depends On:** M6-1.

### M6-3: Docker Buildx Layer Caching for GitHub Actions CI (Fast-Follow from M2)
- **Scope / Acceptance Criteria:**
  - Modernize `.github/workflows/ci.yml` to utilize GitHub Actions Docker Buildx caching (`type=gha`):
    - Add `docker/setup-buildx-action@v3` step before image build stages.
    - Configure Buildx caching across all custom Docker images (`fastapi`, `streamlit`, `stream-consumer`, `stream-producer`, `extract-batch`, `prefect-worker`).
    - Cache scope configured per image (`cache-from: type=gha,scope=...`, `cache-to: type=gha,mode=max,scope=...`).
    - Dramatically reduces CI build turnaround times for PRs and pushes from ~5-8 minutes down to ~1-2 minutes by reusing cached base layers and uv wheels.
    - Resolves the fast-follow item tracked in `docs/Roadmap.md` since Phase 2.
- **Per-Ticket Context:** `.github/workflows/ci.yml`, `infra/docker-compose.yml`, `docs/Roadmap.md`.
- **Files Touched:** `.github/workflows/ci.yml`.
- **Estimated Size:** ~120 lines.
- **Depends On:** M0 CI skeleton.

### M6-4: Deploy-on-Merge GitHub Actions Automation (Blocked/Pending VM Provisioning)
- **Scope / Acceptance Criteria:**
  - Implement `.github/workflows/deploy.yml`:
    - Triggers automatically on `push` to `main` branch (post PR merge), with optional `workflow_dispatch` for manual operator triggering.
    - Secret Presence Guard: Checks if `ORACLE_HOST` and `ORACLE_SSH_KEY` repository secrets exist.
      - If absent: Outputs a clear, non-failing workflow notice: `Deployment skipped: Oracle Cloud VM credentials (ORACLE_HOST, ORACLE_SSH_KEY) are not configured in GitHub repository secrets. Deploy-on-merge remains paused pending manual VM provisioning (see docs/Deployment.md).` Exits with success.
      - If present:
        - Sets up SSH key via `webfactory/ssh-agent` or `appleboy/ssh-action`.
        - Connects to the Oracle Ampere A1 VM.
        - Executes remote deployment script: pulls latest `main`, runs `docker compose up -d --build`, and probes `/health` endpoint for readiness.
        - Triggers zero-downtime serving restart to pick up newly promoted models if applicable.
  - Update `docs/Deployment.md` with clear instructions on generating and adding the required GitHub repository secrets (`ORACLE_HOST`, `ORACLE_SSH_KEY`, `ORACLE_USER`) once the VM is provisioned.
- **Per-Ticket Context:** `docs/Deployment.md`, `infra/oracle-vm/`, `.github/workflows/ci.yml`.
- **Files Touched:** `.github/workflows/deploy.yml`, `docs/Deployment.md`.
- **Estimated Size:** ~100 lines.
- **Depends On:** Manual Oracle VM provisioning (external blocker).

### M6-5: CI/CD Pipeline Verification & Integration Smoke Tests
- **Scope / Acceptance Criteria:**
  - Implement automated retraining verification script `scripts/verify_retraining_smoke.py`:
    - Connects to local/CI PostgreSQL and MLflow tracking server.
    - Step 1: Prepares a baseline Production model version in MLflow with known validation error (e.g. MAE = 4.00).
    - Step 2: Trains a degraded candidate model (e.g. high noise or small epoch) with MAE = 5.20; verifies `ModelPromotionGate` rejects the candidate, leaves production version untouched at v1, and places candidate in `Staging`.
    - Step 3: Trains an improved candidate model with MAE = 3.50; verifies `ModelPromotionGate` approves promotion, transitions candidate to `Production`, and archives the previous champion.
    - Step 4: Prints a formatted metric comparison table detailing Candidate vs. Production vs. Baseline MAE, RMSE, and WAPE with clear promotion outcomes.
  - Wire retraining smoke verification into `.github/workflows/ci.yml` in the integration test job.
  - Validate that `pytest tests/` runs clean with all new unit and contract tests passing.
  - Update `docs/Roadmap.md` marking Phase 6 complete.
- **Per-Ticket Context:** `scripts/verify_serving_live_smoke.py`, `.github/workflows/ci.yml`, `docs/Roadmap.md`.
- **Files Touched:** `scripts/verify_retraining_smoke.py`, `.github/workflows/ci.yml`, `docs/Roadmap.md`, `docs/tickets/phase-6-cicd.md`.
- **Estimated Size:** ~350 lines.
- **Depends On:** M6-1, M6-2, M6-3, M6-4.

---

## Tracking & Issue Linkage
- **Milestone:** `M6 - CI/CD` (Milestone #7)
- **Tracking Issue:** To be opened upon approval
- **Branch Strategy:** `dev` -> `feature/m6-cicd` -> PR to `dev` -> merge to `main`
