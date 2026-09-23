# Roadmap

Each phase = one GitHub Milestone (see GitHub-Setup.md) with one tracking issue opened at the start of that phase. Each phase produces something runnable/demoable on its own — the project is a legitimate portfolio piece even if it stops after Phase 5.

## Phase 0 — Infra Bootstrap
Oracle VM provisioned, Docker Compose skeleton (empty services), GitHub repo + branch protection + CI skeleton (lint/test on PR, no deploy yet).

## Phase 1 — Historical ETL
Batch extractor pulls TLC data, cleans/transforms, loads to Postgres (`raw` → `warehouse`). `taxi_zones` reference table loaded.
*Note: The complete Prefect orchestration flow (`historical_tlc_batch_etl_flow`), two-stage bulk load (3,066,766 raw records -> 2,940,141 warehouse records), and second-run idempotency skip logic are verified and proven against live PostgreSQL in CI (`etl_live_smoke.yml`).*



## Phase 2 — Feature Store
Feast feature definitions (`zone_demand_features`, `corridor_duration_features`), offline materialization working, features validated against known trip data.

## Phase 3 — Baseline Models
Seasonal-naive baseline + LightGBM/XGBoost for demand and duration, both logged to MLflow with evaluation metrics vs. baseline; wire R2 backup for MLflow artifacts + Postgres dumps (ADR-007).

## Phase 4 — Real-Time Layer
Redpanda broker up, streaming producer (replay + live MTA/traffic/weather polling), stream consumer writing to Postgres and updating the Feast online store.

## Phase 5 — Online Serving
FastAPI `/predict/demand`, `/predict/eta`, `/features/*`, `/health` live, reading from the Feast online store, deployed on the Oracle VM.
*Note: Full production-grade serving stack verified live against Docker Compose in CI (`docker-compose-validate`). Includes ModelLoaderService resolving Production-stage LightGBM models from MLflow with baseline fallback (ADR-020), Feast online store real-time feature retrieval, low-latency Redis prediction caching with strict 60s TTL and sub-5ms cached latency, resilient zero-TTL degraded fallback mode for unmaterialized entities evaluating genuine non-zero model predictions on imputed defaults, and vectorized batch scoring across all 263 NYC taxi zones.*

## Phase 6 — CI/CD
Scheduled retraining Prefect flow orchestrated on Prefect Cloud (ADR-021); safe model promotion gate comparing candidate models against active Production models in MLflow; Docker Buildx layer caching (`type=gha`) in GitHub Actions to accelerate CI builds (fast-follow since M2); deploy-on-merge GitHub Actions workflow (authored and gated, pending manual Oracle VM provisioning). Note: Drift-triggered retraining is explicitly deferred to Phase 8 (Evidently AI) per ADR-022.
*Note: Complete CI/CD and retraining stack verified in CI (`docker-compose-validate`). Includes ModelPromotionGate with a default 2.0% hurdle rate (min_improvement_pct=0.02) enforcing 4-stage validation (seasonal naive baseline check, champion/challenger comparison, 2.0% minimum improvement, and MLflow stage transitions) proven against live warehouse.trips data and MLflow Production models; scheduled-model-retraining-flow registered on Prefect Cloud work pool logistics-pool with weekly cron (0 3 * * 0); Docker Buildx GHA layer caching across all 5 custom services; and deploy.yml with secret-guard validation tested live on push to main.*

## Phase 7 — Agent Layer
LangGraph Ops Copilot live behind `/agent/chat`, all four tools working, guardrails tested against adversarial input.
*Note: Complete LangGraph Ops Copilot and Agent Serving layer verified in CI (`docker-compose-validate`). Includes 4 read-only diagnostic tools (`get_features` against Feast Redis, `query_recent_predictions` against PostgreSQL, `query_pipeline_status` against PostgreSQL, and `search_logs_and_model_cards` over FAISS CPU RAG index); strict read-only tool allowlisting security boundary (ADR-023) proven against adversarial prompt injection; dual-provider LLM fallback cascade (Groq `llama-3.3-70b-versatile`, Gemini `gemini-2.0-flash`, hermetic `MockLLMProvider` with explicit mock labeling) (ADR-024); sub-millisecond FAISS CPU vector index with deterministic hashing fallback (ADR-025); FastAPI `POST /agent/chat` endpoint with Pydantic request/response validation and error isolation; and interactive multi-tab Streamlit Ops Copilot UI with quick-action prompt chips, message thread, provider/model metadata badges, and tool/source inspection expanders.*

## Phase 8 — Monitoring
Evidently drift reports scheduled via Prefect, surfaced on the UI dashboard, queryable by the agent.
*Note: Full Evidently AI 0.7.x model and data monitoring layer verified end-to-end against live PostgreSQL. Includes DataDriftAnalyzer, PredictionDriftAnalyzer, and PerformanceDecayAnalyzer with explicit keyword convention and hybrid 14-day rolling reference windows (with January 2023 training baseline cold-start fallback) (ADR-026); daily Prefect monitoring flow (`daily_model_monitoring_flow`) scheduled at 02:00 UTC daily (`0 2 * * *`); dual-policy retraining trigger supporting default alert-first safety mode (`AUTO_RETRAIN_ON_DRIFT=false`) and guarded-trigger mode (`AUTO_RETRAIN_ON_DRIFT=true`) throttled by a 48-hour universal cooldown across scheduled and drift-triggered runs; Tool 5 `query_drift_reports` registered in the immutable read-only agent allowlist (ADR-023); LangGraph Ops Copilot drift detection and markdown synthesis; FAISS CPU RAG ingestion with 14-day rolling retention and atomic rebuild pruning; FastAPI `/monitoring/reports` and `/monitoring/reports/{id}/html` serving endpoints; and dedicated Streamlit Model Monitoring dashboard tab with real-time scorecards, feature drift trend charts, and embedded interactive Evidently HTML reports.*

## Phase 9 — Polish
README, architecture diagram, live demo link, portfolio write-up, Lessons-Learned.md filled in retrospectively.

## Status
 
- **Phase 0 — Infra Bootstrap:** Done (completed 2026-08-18)
- **Phase 1 — Historical ETL:** Done (completed 2026-08-23)
- **Phase 2 — Feature Store:** Done (completed 2026-08-28)
- **Phase 3 — Baseline Models:** Done (completed 2026-08-31)
- **Phase 4 — Real-Time Layer:** Done (completed 2026-09-07)
- **Phase 5 — Online Serving:** Done (completed 2026-09-11)
- **Phase 6 — CI/CD:** Done (completed 2026-09-13)
- **Phase 7 — Agent Layer:** Done (completed 2026-09-16)
- **Phase 8 — Monitoring:** Done (completed 2026-09-23)
- **Phase 9 — Polish:** Up next




