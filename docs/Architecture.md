# Architecture

The full single-host reference below remains the Compose design. The free-cloud
portfolio profile in ADR-028 uses managed Aiven stores, DagsHub MLflow, Render,
Community Cloud and bounded scheduled Prefect jobs on GitHub Actions. Its source
is explicitly labelled historical sampled data, not continuous live taxi data.
See [FREE_CLOUD_SETUP.md](FREE_CLOUD_SETUP.md) and the verification matrix in
[DEPLOYMENT_REPORT.md](DEPLOYMENT_REPORT.md). No cloud deployment is inferred from
the reference diagram or prior CI runs.

## 1. What this system does

The platform demonstrates an end-to-end pipeline that predicts for NYC taxi zones: (1) **demand** — expected pickup volume in the next time window, and (2) **ETA/duration** — expected trip duration for a zone-to-zone trip. The deployed free-cloud profile trains and serves from a bounded January 2023 TLC sample, so its forecasts are historical-sample outputs rather than current city traffic. The local Compose reference includes the streaming producer/consumer path and optional live external feeds. Demand and duration are trained as independent models, connected through shared raw features such as recent pickup counts rather than chaining one model's prediction into the other. The LangGraph agent answers operational questions using available pipeline state and typed tools.

## 2. Non-goals

- Not a ride-hailing dispatch system — no routing/matching logic, no driver assignment.
- Not trying to beat state-of-the-art forecasting accuracy — the point is the pipeline, not squeezing the last 2% MAE out of a model.
- Not multi-city at launch — see [Decisions.md](./Decisions.md) for why NYC, and the geography-agnostic design that keeps a second city cheap to add later.

## 3. Reference system diagram (local streaming data flow)

This is the fuller local Compose reference architecture. It is not a diagram of the bounded free-cloud deployment: that profile uses scheduled, bounded jobs and historical sample data, as described in [FREE_CLOUD_SETUP.md](FREE_CLOUD_SETUP.md).

```
                        ┌─────────────────────────┐
                        │   NYC TLC Trip Data      │  (monthly Parquet, historical)
                        │   MTA GTFS-RT             │  (live, ~30s cadence)
                        │   NYC Traffic Speed API   │  (live)
                        │   OpenWeatherMap          │  (live)
                        └───────────┬──────────────┘
                                    │
                     ┌──────────────┴───────────────┐
                     ▼                               ▼
            [Batch Extractor]                [Streaming Producer]
            pulls TLC Parquet                 replays historical trips
            monthly                           at real-time pace +
                     │                         polls live feeds every 30-60s
                     │                               │
                     │                               ▼
                     │                        [Redpanda topics]
                     │                     trip.events / traffic.snapshots
                     │                               │
                     ▼                               ▼
              [Transform/Load]  ◄────────── [Stream Consumer]
              clean, validate, normalize
                     │
                     ▼
              [Postgres: warehouse]
              raw + cleaned trip & traffic tables
                     │
                     ▼
              [Feast Feature Store]
              offline store (Postgres/Parquet) ──► materialize ──► online store (Redis)
                     │                                                   │
                     ▼                                                   │
              [Training: LightGBM/XGBoost                                │
               demand + duration models]                                 │
                     │                                                   │
                     ▼                                                   │
              [MLflow: tracking + registry]                              │
                     │                                                   │
                     ▼                                                   ▼
              [FastAPI serving] ◄── reads latest online features ────────┘
                     │
          ┌──────────┼─────────────┐
          ▼          ▼             ▼
   /predict/demand  /predict/eta  /agent/chat
                                     │
                                     ▼
                          [LangGraph Ops Agent]
                          tools: Feast, MLflow, Postgres, FAISS (logs/model cards)
                     │
                     ▼
              [Streamlit UI]
              live map, monitoring dashboard, agent chat
```

In the local reference topology, Prefect schedules batch extraction, feature materialization, training/retraining, and monitoring (Evidently) jobs; the streaming producer/consumer pair runs separately. In the free-cloud profile, bounded Prefect OSS flows are triggered by GitHub Actions instead of an always-on worker.

## 4. Components at a glance

| Component | Tech | Doc |
|---|---|---|
| Historical + optional live data sources | TLC sample; local reference can poll MTA, NYC traffic API, and OpenWeatherMap | [Data-Sources.md](./Data-Sources.md) |
| Extract / streaming | Python producer/consumer, Redpanda | [ETL-Streaming.md](./ETL-Streaming.md) |
| Warehouse | Postgres | [Database.md](./Database.md) |
| Feature store | Feast (Postgres offline / Redis online) | [Feature-Store.md](./Feature-Store.md) |
| Training & registry | LightGBM/XGBoost, MLflow | [AI-Pipeline.md](./AI-Pipeline.md) |
| Orchestration | Prefect | [ETL-Streaming.md](./ETL-Streaming.md), [Monitoring.md](./Monitoring.md) |
| Serving | FastAPI | [API.md](./API.md) |
| Agent | LangGraph + Groq/Gemini + FAISS | [Agents.md](./Agents.md) |
| UI | Streamlit | [UI.md](./UI.md) |
| Monitoring | Evidently AI | [Monitoring.md](./Monitoring.md) |
| Infra | Managed free-cloud profile; local Docker Compose reference (Oracle VM scripts are optional legacy helpers) | [Deployment.md](./Deployment.md), [FREE_CLOUD_SETUP.md](./FREE_CLOUD_SETUP.md) |
| CI/CD & project mgmt | GitHub Actions, Milestones/Issues | [GitHub-Setup.md](./GitHub-Setup.md) |

## 5. Design principles

- **Geography-agnostic where it costs nothing.** City/zone identifiers are config, not hardcoded — not because a second city is planned soon, but because it's the honest, low-cost way to keep the door open (see Decisions.md).
- **The local reference is self-hosted on one box.** The deployed portfolio profile uses managed free-tier services and bounded scheduled jobs, as recorded in ADR-028.
- **Every prediction is explainable via the agent, not just a number.** The agent's tools read the same feature/prediction/log state a human debugging the system would.
- **Docs and decisions are versioned alongside code.** See [Decisions.md](./Decisions.md) — no undocumented architecture choices.
