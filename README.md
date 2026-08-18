# Logistics Demand + ETA Forecasting Platform

A live, production-shaped MLOps pipeline: real-time + historical NYC data → ETL → feature store → model training → deployment → CI/CD, with an LLM ops-agent layer on top.

**Status:** Design phase. Full docs finalized before any code — start here: [`docs/Architecture.md`](./docs/Architecture.md).

## Docs

| Doc | Covers |
|---|---|
| [Architecture.md](./docs/Architecture.md) | System diagram, components, design principles |
| [Decisions.md](./docs/Decisions.md) | ADR log — every major choice and why |
| [Data-Sources.md](./docs/Data-Sources.md) | TLC, MTA GTFS-RT, NYC traffic, weather |
| [ETL-Streaming.md](./docs/ETL-Streaming.md) | Batch extractor, streaming producer/consumer, Redpanda |
| [Feature-Store.md](./docs/Feature-Store.md) | Feast entities, feature views, materialization |
| [Database.md](./docs/Database.md) | Postgres schema |
| [AI-Pipeline.md](./docs/AI-Pipeline.md) | Models, baselines, training, evaluation |
| [API.md](./docs/API.md) | FastAPI endpoints |
| [Agents.md](./docs/Agents.md) | LangGraph Ops Copilot, tools, RAG |
| [UI.md](./docs/UI.md) | Streamlit app |
| [Monitoring.md](./docs/Monitoring.md) | Evidently drift reports, pipeline observability |
| [Deployment.md](./docs/Deployment.md) | Oracle VM, Docker Compose topology |
| [GitHub-Setup.md](./docs/GitHub-Setup.md) | Branching, milestones/issues, CI secrets |
| [Environment-Setup.md](./docs/Environment-Setup.md) | Local dev setup |
| [Security.md](./docs/Security.md) | Secrets, least-privilege, prompt-injection resistance |
| [Performance.md](./docs/Performance.md) | Latency budgets, load testing |
| [Roadmap.md](./docs/Roadmap.md) | Phase-by-phase build order |
| [Contributing.md](./docs/Contributing.md) | Workflow, code style |
| [Lessons-Learned.md](./docs/Lessons-Learned.md) | Filled in per milestone as the project progresses |

## Stack

Python · FastAPI · Streamlit · Feast · Redpanda · Postgres · Redis · MLflow · Prefect · LangGraph · Groq/Gemini · Docker · Oracle Cloud · GitHub Actions
