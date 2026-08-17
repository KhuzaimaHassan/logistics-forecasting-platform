# UI

Streamlit, chosen over Next.js as the primary UI (see Decisions.md ADR-006) — Python-native, colocated with the model/agent code, fastest path to a demoable interface for a project where the UI is not the point.

## Pages

### 1. Live Map
- NYC taxi zones colored by predicted demand (choropleth), using zone centroids/geometry from `taxi_zones`.
- Click a zone → shows current demand prediction, recent actuals, and feature values (calls `/predict/demand` and `/features/zone/{id}`).
- Corridor selector for ETA predictions between two zones.

### 2. Monitoring Dashboard
- Evidently drift reports (data drift, prediction drift) rendered inline.
- Pipeline run history from `/pipeline/status` — recent ETL/training/monitoring job outcomes, at a glance.
- Model performance over time (MAE/RMSE trend from MLflow, vs. seasonal-naive baseline) — this is the chart that actually proves the project works.

### 3. Agent Chat
- Simple chat interface calling `/agent/chat`.
- Shows which tools the agent used for each answer (transparency, and useful for debugging the agent itself during development).

## Notes

- No auth in v1, matching the API (see Security.md for the gap this leaves before any public link is shared).
- Deployed alongside the FastAPI service on the Oracle VM (separate container, same Docker Compose stack) — calls the API over the internal Docker network, not the public URL.

## Open questions

- Whether the live map needs real-time auto-refresh (polling) or a manual refresh button is sufficient for a portfolio demo — leaning toward manual + a visible "last updated" timestamp, simpler and avoids hammering the API.
