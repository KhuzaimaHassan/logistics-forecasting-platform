# Ticket Breakdown — Phase 8: Monitoring (Evidently AI Drift Reports & Dashboard)

## Epic Summary
Implement the data and model monitoring layer for the Logistics Demand & ETA Forecasting Platform using Evidently AI `0.7.x`. Phase 8 separates pipeline execution observability (`warehouse.pipeline_runs`) from statistical data and model drift (`warehouse.monitoring_reports`). The system continuously computes feature data drift, prediction distribution drift, and model performance decay (MAE/RMSE tracking against baseline). Monitoring runs are orchestrated daily via a Prefect Cloud flow, persisting structured JSON summaries and self-contained interactive HTML dashboards to PostgreSQL and disk. When statistically significant drift is detected or model performance decays beyond safety thresholds, the flow automatically triggers the scheduled retraining pipeline established in Phase 6 (closing ADR-022). Finally, drift reports and metrics are surfaced interactively in the Streamlit UI dashboard and made queryable to the LangGraph Ops Copilot via a read-only monitoring tool.

---

## Architectural Decisions & Constraints

1. **ADR-026: Evidently 0.7 Core API, Explicit Keyword Convention, Hybrid Reference Windows, and Drift Retraining Trigger**:
   - **API Choice:** Use the new Evidently `0.7.x` Core API (`from evidently import Report, Dataset, DataDefinition, Regression` and `from evidently.presets import DataDriftPreset, RegressionPreset`). The legacy 0.4.x API (`evidently.report`, `evidently.metric_preset`) was moved to `evidently.legacy` and is deprecated.
   - **Invocation Standard:** All calls to `Report.run(...)` MUST explicitly name arguments: `report.run(current_data=curr_dataset, reference_data=ref_dataset)`. Positional argument passing is forbidden to prevent accidental transposition of the evaluation target and baseline reference.
   - **Hybrid Reference Window Strategy:**
     - *Feature & Prediction Drift:* 14-day rolling historical window (excluding the active 24-hour evaluation window) captures organic seasonality and day-of-week demand patterns without triggering false alarms. If fewer than 7 days of historical predictions exist in `warehouse.predictions`, fall back automatically to the fixed training baseline split (January 2024).
     - *Performance Decay:* Uses the champion model validation baseline recorded in MLflow (MAE/RMSE) as the static reference benchmark against rolling actuals in `warehouse.predictions`.
   - **Automated Drift-Triggered Retraining Hook (ADR-022 Closeout):**
      - When daily monitoring detects `dataset_drift_detected == True` (drift share $\ge 0.40$ or critical demand features drift with $p < 0.01$) OR performance degrades by $> 15\%$ (`current_mae > baseline_mae * 1.15`), the monitoring flow records an alert with `retrain_recommended: true`.
      - **Staged Rollout Policy:** Defaults to `AUTO_RETRAIN_ON_DRIFT=false` (logs high-severity alert to `warehouse.monitoring_reports` and `warehouse.pipeline_runs`, and surfaces recommendations to the Streamlit Dashboard and Ops Copilot for human confirmation). Setting `AUTO_RETRAIN_ON_DRIFT=true` enables direct autonomous invocation of `retraining_flow`, guarded by a 48-hour cooldown throttle.

2. **Security & Read-Only Tool Boundary (ADR-023 Compliance)**:
   - The agent's new monitoring tool (`query_drift_reports`) is strictly read-only, querying `warehouse.monitoring_reports` via SQLAlchemy `text()` without modification handles. Added to `ALLOWLISTED_TOOL_NAMES`.

3. **Hermetic Offline Execution**:
   - Evidently reports run 100% locally on CPU without external telemetry, SaaS accounts, or cloud API calls.
   - Reports output lightweight self-contained HTML snapshots (<4MB) rendered directly in Streamlit via `st.components.v1.html()`.

---

## Tickets

### M8-1: Core Evidently 0.7 Drift & Performance Analyzers
- **Scope / Acceptance Criteria:**
  - Implement `src/monitoring/schemas.py`:
    - `DriftMetricSummary`: Pydantic model for individual feature drift (`column_name`, `drift_detected`, `p_value`, `stat_test`, `threshold`).
    - `MonitoringReportSummary`: Pydantic model for aggregated report metadata (`report_id`, `report_type`, `generated_at`, `dataset_drift_detected`, `drift_share`, `number_of_drifted_columns`, `metrics`, `summary_json`).
  - Implement `src/monitoring/analyzers.py`:
    - `DataDriftAnalyzer`: Builds `DataDefinition`, wraps DataFrames into `Dataset`, and runs `Report([DataDriftPreset()])` with explicit `current_data` and `reference_data`.
    - `PredictionDriftAnalyzer`: Evaluates prediction distribution shifts across zone pickups and corridor travel durations.
    - `PerformanceDecayAnalyzer`: Runs `Report([RegressionPreset()])` comparing actuals against predictions, extracting current vs reference MAE, RMSE, and error distributions.
    - Extract standardized `MonitoringReportSummary` dictionaries from `snapshot.dict()`.
    - Extract HTML string from `snapshot.get_html_str()` and save to target path.
  - Comprehensive unit test suite in `tests/test_monitoring_analyzers.py` verifying analyzers against synthetic drifted and non-drifted datasets, testing both `Dataset` inputs and raw `pd.DataFrame` fallbacks.
- **Per-Ticket Context:** `docs/Monitoring.md`, `docs/Decisions.md` (ADR-026).
- **Files Touched:** `src/monitoring/schemas.py`, `src/monitoring/analyzers.py`, `src/monitoring/__init__.py`, `tests/test_monitoring_analyzers.py`.
- **Estimated Size:** ~350 lines.
- **Depends On:** Phase 5 serving models and Phase 7 predictions logging.

---

### M8-2: Scheduled Prefect Monitoring Flow & Retraining Trigger (ADR-022)
- **Scope / Acceptance Criteria:**
  - Implement `src/monitoring/service.py`:
    - `MonitoringService`: Coordinates data extraction from `warehouse.predictions` and `warehouse.trips`, reference window slicing, analyzer execution, and persistence to `warehouse.monitoring_reports`.
    - Implements hybrid reference window resolution: checks historical row count; uses rolling 14-day window if $>500$ rows, otherwise falls back to training baseline.
    - Persists reports to PostgreSQL `warehouse.monitoring_reports` (`report_id`, `report_type`, `generated_at`, `summary_json`, `file_path`).
  - Implement `src/orchestration/flows/monitoring_flow.py`:
    - Prefect flow `daily_model_monitoring_flow` scheduled daily at off-peak hours (e.g. `0 2 * * *`).
    - Task 1: Fetch and validate evaluation data.
    - Task 2: Run Data Drift, Prediction Drift, and Performance Decay analyzers.
    - Task 3: Persist reports and write HTML artifacts to `artifacts/monitoring_reports/`.
    - Task 4: Evaluate drift alert conditions. If alert threshold exceeded, record alert in `warehouse.monitoring_reports` (`retrain_recommended: true`); if `AUTO_RETRAIN_ON_DRIFT=true`, trigger `retraining_flow` deployment (closing ADR-022) with 48h cooldown guard.
  - Unit and integration tests in `tests/test_monitoring_flow.py` mocking DB and Prefect tasks.
- **Per-Ticket Context:** `src/orchestration/flows/retraining_flow.py`, `src/common/models.py`.
- **Files Touched:** `src/monitoring/service.py`, `src/orchestration/flows/monitoring_flow.py`, `tests/test_monitoring_flow.py`.
- **Estimated Size:** ~350 lines.
- **Depends On:** M8-1, Phase 6 retraining flow.

---

### M8-3: LangGraph Ops Copilot Monitoring Tool & RAG Integration
- **Scope / Acceptance Criteria:**
  - Implement Tool 5 in `src/agents/tools.py`: `query_drift_reports`.
    - Inputs: `report_type` (optional: 'data_drift', 'prediction_drift', 'performance_decay', or None for all), `limit` (default: 5).
    - Queries `warehouse.monitoring_reports` ordered by `generated_at DESC`.
    - Returns structured summary: report type, timestamp, dataset drift detected (True/False), drift share %, top drifted features, and MAE/RMSE comparisons.
  - Update `src/agents/guardrails.py`:
    - Register `query_drift_reports` in `ALLOWLISTED_TOOL_NAMES` maintaining the ADR-023 read-only boundary.
  - Update `src/agents/graph.py`:
    - Teach copilot router and synthesizer to invoke `query_drift_reports` when user asks about drift, model decay, or feature distribution shifts.
  - Update RAG indexer in `src/agents/rag/indexer.py`:
    - Ingest latest monitoring report summaries into FAISS index metadata.
  - Unit tests in `tests/test_agent_tools.py` and `tests/test_agent_graph.py` verifying copilot responses to monitoring queries.
- **Per-Ticket Context:** `src/agents/tools.py`, `src/agents/guardrails.py`, `src/agents/graph.py`.
- **Files Touched:** `src/agents/tools.py`, `src/agents/guardrails.py`, `src/agents/graph.py`, `src/agents/rag/indexer.py`, `tests/test_agent_tools.py`.
- **Estimated Size:** ~200 lines.
- **Depends On:** M8-1, M8-2, Phase 7 LangGraph agent.

---

### M8-4: Streamlit Monitoring Dashboard UI
- **Scope / Acceptance Criteria:**
  - Update `ui/app.py` to add a dedicated "Model Monitoring" tab alongside "Live Forecast Map" and "Ops Copilot".
  - **Monitoring Tab Components:**
    1. **High-Level Status Scorecards:**
       - Feature Data Drift status badge (`🟢 Normal` vs `🔴 Drift Detected`).
       - Prediction Distribution Drift status badge.
       - Performance Decay metric (Current MAE vs Reference Baseline MAE).
    2. **Interactive Evidently Report Viewer:**
       - Dropdown selector for available historical reports in `warehouse.monitoring_reports`.
       - Renders the full interactive HTML report using `streamlit.components.v1.html(html_str, height=800, scrolling=True)`.
    3. **Historical Drift Trend Chart:**
       - Line chart tracking `drift_share` and `mae` over time across previous monitoring runs.
  - Contract and helper tests in `tests/test_ui_monitoring.py`.
- **Per-Ticket Context:** `ui/app.py`, `src/monitoring/schemas.py`.
- **Files Touched:** `ui/app.py`, `tests/test_ui_monitoring.py`.
- **Estimated Size:** ~250 lines.
- **Depends On:** M8-1, M8-2.

---

### M8-5: End-to-End Live CI Smoke Verification & Milestone Closeout
- **Scope / Acceptance Criteria:**
  - Implement `scripts/verify_monitoring_smoke.py`:
    1. Verify PostgreSQL `warehouse.monitoring_reports` connectivity.
    2. Seed `warehouse.predictions` with synthetic baseline (Jan 2024) and intentionally drifted inference data (higher demand, skewed distribution).
    3. Run `MonitoringService.run_all_reports()` and verify 3 reports generated (`data_drift`, `prediction_drift`, `performance_decay`).
    4. Assert `data_drift` correctly flags drifted features and dataset drift.
    5. Assert reports are persisted in PostgreSQL with valid `summary_json` and HTML files created on disk.
    6. Verify `query_drift_reports` tool retrieves persisted reports.
    7. Verify drift retraining trigger logic evaluates correctly.
  - Wire Step 23 into `.github/workflows/ci.yml`.
  - Update `docs/Roadmap.md` marking Phase 8 complete.
  - Release `dev` to `main`, close Issue #143, close Milestone #9.
- **Per-Ticket Context:** `.github/workflows/ci.yml`, `docs/Roadmap.md`.
- **Files Touched:** `scripts/verify_monitoring_smoke.py`, `.github/workflows/ci.yml`, `docs/Roadmap.md`.
- **Estimated Size:** ~350 lines.
- **Depends On:** M8-1 through M8-4.

---

## Tracking & Issue Linkage
- **Milestone:** `M8 - Monitoring` (Milestone #9)
- **Tracking Issue:** Closes #143
- **Branch Strategy:** `dev` -> `feature/m8-monitoring-<submilestone>` -> PR to `dev` -> merge to `main`
