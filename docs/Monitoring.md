# Monitoring

## 1. Scope

Two distinct things, not to be conflated:
- **Pipeline observability** — did the scheduled jobs run, and did they succeed? (Prefect's job, surfaced via the `pipeline_runs` table and `/pipeline/status`.)
- **Data/model monitoring** — is the data or the model's behavior drifting? (Evidently AI's job.)

## 2. Evidently AI reports

- **Data drift** — compares recent feature distributions (rolling demand counts, traffic speeds) against a reference window, flags significant shifts.
- **Prediction drift** — compares recent prediction distributions against a reference window.
- **Model performance decay** — once enough ground truth has accumulated (actual pickups/durations vs. predictions), tracks MAE/RMSE trend over time against the baseline.

## 3. Scheduling

- Run daily via Prefect, not on every request — drift is a slow-moving signal, no need for per-prediction overhead.
- Reports written to a `monitoring_reports` table/directory, surfaced on the UI's Model Monitoring tab and directly queryable by the agent's `query_drift_reports` tool and FAISS RAG knowledge base.

## 4. Alerting (lightweight, v1)

- No external alerting service for v1 — a personal project doesn't need PagerDuty. Drift/failure surfaces on the dashboard and is queryable by the agent ("has anything drifted this week").
- Worth revisiting only if the project moves toward the side-hustle-product direction mentioned as a possible purpose.

## 5. Architectural Decisions & Reference Strategy (ADR-026)

- **Reference Window Definition (Resolved):**
  - *Feature & Prediction Drift:* A 14-day rolling historical window (excluding the active 24-hour evaluation window). Captures bi-weekly demand cycles and eliminates weekday/weekend seasonality false alarms. If historical records in `warehouse.predictions` are fewer than 500 rows (cold start), falls back automatically to the static training baseline split (January 2023 TLC dataset, with January 2024 compatibility).
  - *Performance Decay:* Benchmark rolling 24-hour actuals against the static champion model validation baseline metrics (MAE/RMSE) logged in MLflow.
- **Evidently 0.7 Core API & Calling Convention:**
  - Uses `from evidently import Report, Dataset, DataDefinition, Regression` and `from evidently.presets import DataDriftPreset, RegressionPreset`.
  - Strictly requires explicit keyword invocation: `report.run(current_data=curr_dataset, reference_data=ref_dataset)` to prevent target/reference transposition.
- **Drift-Triggered Retraining Hook (ADR-022 Fulfillment):**
  - If dataset drift share $\ge 0.40$ or critical demand features show drift at $p < 0.01$, or if MAE degrades by $> 15\%$, the Prefect daily flow raises a structured alert (`retrain_recommended: true`).
  - **Staged Safety Policy:** Defaults to alert-only (`AUTO_RETRAIN_ON_DRIFT=false`), logging the alert to `warehouse.monitoring_reports` and `warehouse.pipeline_runs`, and surfacing it on the UI and Ops Copilot for operator confirmation. Setting `AUTO_RETRAIN_ON_DRIFT=true` enables direct autonomous invocation of `retraining_flow`, guarded by a 48-hour cooldown.

## 6. Ops Copilot & Dashboard Integration (M8-3)

- **Ops Copilot Tool 5 (`query_drift_reports`):**
  - Implements an allowlisted read-only query tool executing parameterized SELECT queries against `warehouse.monitoring_reports`.
  - Supports filtering by `report_type` (`data_drift`, `prediction_drift`, `performance_decay`) and `limit` clamping ($1 \le \text{limit} \le 20$).
  - Neutralizes SQL injection attempts through SQLAlchemy ORM parameterization and strictly prevents schema inspection or state mutation.
- **FAISS RAG Ingestion & Pruning Policy:**
  - Ingests recent monitoring report summaries alongside documentation, model cards, and pipeline runs.
  - Retention is bounded to a strict 14-day rolling window with at most 10 reports per type (max 30 individual report chunks) plus 1 consolidated rolling health overview chunk (`monitoring_health_summary_14d`).
  - Pruning is atomic on each index rebuild. If a daily re-index job fails, the index remains bounded at the prior build's 30 chunks without unbounded growth, while live Copilot queries bypass the vector index and retrieve fresh state directly from PostgreSQL.
- **Streamlit Model Monitoring Tab (`ui/app.py`):**
  - Four KPI scorecards: active data drift, prediction drift, MAE performance decay, and retraining recommendation status.
  - Trend charts tracking drift share percentages and count of drifted features over time.
  - Interactive Evidently HTML report viewer embedded via `streamlit.components.v1.html`, complete with report selection and one-click HTML download.
- **Serving Endpoints (`src/serving/app.py`):**
  - `GET /monitoring/reports`: Lists recent monitoring report summaries with active alert indicators.
  - `GET /monitoring/reports/{report_id}/html`: Returns full standalone interactive Evidently HTML reports.



