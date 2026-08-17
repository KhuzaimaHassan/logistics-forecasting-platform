# Architecture

## 1. What this system does

A live pipeline that predicts, for NYC taxi zones: (1) **demand** — expected pickup volume in the next time window, and (2) **ETA/duration** — expected trip duration given current conditions. Demand and duration are trained as independent models, connected through a shared raw feature (recent pickup counts) rather than model-output chaining (higher recent demand in a zone correlates with longer pickup/dispatch delay). An LLM agent sits on top and answers natural-language questions about both, using live pipeline state as its source of truth.

## 2. Non-goals

- Not a ride-hailing dispatch system — no routing/matching logic, no driver assignment.
- Not trying to beat state-of-the-art forecasting accuracy — the point is the pipeline, not squeezing the last 2% MAE out of a model.
- Not multi-city at launch — see [Decisions.md](./Decisions.md) for why NYC, and the geography-agnostic design that keeps a second city cheap to add later.

## 3. System diagram (data flow)

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

Orchestration (Prefect) schedules the batch extractor, feature materialization, training/retraining, and monitoring (Evidently) jobs. The streaming producer/consumer pair runs as a standalone always-on process, not on a schedule.

## 4. Components at a glance

| Component | Tech | Doc |
|---|---|---|
| Historical + live data sources | TLC, MTA GTFS-RT, NYC traffic API, OpenWeatherMap | [Data-Sources.md](./Data-Sources.md) |
| Extract / streaming | Python producer/consumer, Redpanda | [ETL-Streaming.md](./ETL-Streaming.md) |
| Warehouse | Postgres | [Database.md](./Database.md) |
| Feature store | Feast (Postgres offline / Redis online) | [Feature-Store.md](./Feature-Store.md) |
| Training & registry | LightGBM/XGBoost, MLflow | [AI-Pipeline.md](./AI-Pipeline.md) |
| Orchestration | Prefect | [ETL-Streaming.md](./ETL-Streaming.md), [Monitoring.md](./Monitoring.md) |
| Serving | FastAPI | [API.md](./API.md) |
| Agent | LangGraph + Groq/Gemini + FAISS | [Agents.md](./Agents.md) |
| UI | Streamlit | [UI.md](./UI.md) |
| Monitoring | Evidently AI | [Monitoring.md](./Monitoring.md) |
| Infra | Oracle Cloud Always Free VM, Docker Compose | [Deployment.md](./Deployment.md) |
| CI/CD & project mgmt | GitHub Actions, Milestones/Issues | [GitHub-Setup.md](./GitHub-Setup.md) |

## 5. Design principles

- **Geography-agnostic where it costs nothing.** City/zone identifiers are config, not hardcoded — not because a second city is planned soon, but because it's the honest, low-cost way to keep the door open (see Decisions.md).
- **Everything self-hosted runs on one box.** Deliberate simplicity — one Oracle VM via Docker Compose, not a sprawl of managed services with their own free-tier expiry clocks.
- **Every prediction is explainable via the agent, not just a number.** The agent's tools read the same feature/prediction/log state a human debugging the system would.
- **Docs and decisions are versioned alongside code.** See [Decisions.md](./Decisions.md) — no undocumented architecture choices.
