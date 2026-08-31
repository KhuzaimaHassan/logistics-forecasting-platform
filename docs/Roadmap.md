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

## Phase 6 — CI/CD
GitHub Actions: build/test/deploy on merge to `main`, scheduled/drift-triggered retrain workflow; add Docker build-layer caching (Buildx / Actions cache) once real ML/agent dependencies land to keep PR turnaround fast.

## Phase 7 — Agent Layer
LangGraph Ops Copilot live behind `/agent/chat`, all four tools working, guardrails tested against adversarial input.

## Phase 8 — Monitoring
Evidently drift reports scheduled via Prefect, surfaced on the UI dashboard, queryable by the agent.

## Phase 9 — Polish
README, architecture diagram, live demo link, portfolio write-up, Lessons-Learned.md filled in retrospectively.

## Status
 
- **Phase 0 — Infra Bootstrap:** Done (completed 2026-08-18)
- **Phase 1 — Historical ETL:** Done (completed 2026-08-23)
- **Phase 2 — Feature Store:** Done (completed 2026-08-28)
- **Phase 3 — Baseline Models:** Done (completed 2026-08-31)
- **Phases 4–9:** Not yet started




