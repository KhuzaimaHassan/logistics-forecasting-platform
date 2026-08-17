# API

FastAPI service, single app, deployed on the Oracle VM (see Deployment.md). Hosts prediction endpoints, feature lookups, and the agent chat endpoint.

## Endpoints (v1 draft)

### `GET /health`
Liveness/readiness check. Returns status of DB, Redis, and Redpanda consumer lag.

### `GET /predict/demand/{zone_id}`
Returns predicted pickup demand for the given zone over the configured horizon.
```json
{
  "zone_id": 161,
  "horizon_minutes": 15,
  "predicted_pickups": 42,
  "model_version": "demand-v3",
  "as_of": "2026-08-16T14:32:00Z"
}
```

### `GET /predict/eta?origin={zone_id}&dest={zone_id}`
Returns predicted trip duration for the given corridor under current conditions.
```json
{
  "origin_zone_id": 161,
  "dest_zone_id": 236,
  "predicted_duration_minutes": 18.4,
  "model_version": "duration-v2",
  "as_of": "2026-08-16T14:32:00Z"
}
```

### `GET /features/{entity_type}/{entity_id}`
Debug/inspection endpoint — returns the current online-store feature vector for a zone or corridor. Used by the UI and by the agent's tools.

### `POST /agent/chat`
```json
// request
{ "message": "why is the ETA for zone 161 spiking right now?" }
// response
{ "reply": "...", "tools_used": ["get_features", "query_recent_predictions"] }
```

### `GET /pipeline/status`
Returns recent orchestration run history (from the `pipeline_runs` table) — "is the pipeline healthy" at a glance, also used by the agent.

## Conventions

- All timestamps UTC, ISO 8601.
- Errors follow a consistent shape: `{"error": "...", "detail": "..."}` with appropriate HTTP status codes.
- No auth in v1 (personal project, not public-facing at first) — noted as a gap to close before any public demo link is shared; see Security.md.

## Open questions

- Whether `/predict/demand` and `/predict/eta` should support batch requests (multiple zones/corridors in one call) — likely needed once the UI's live map is built, since it'll want many zones at once rather than one call per zone.
