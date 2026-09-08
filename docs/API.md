# API

FastAPI service, single app, deployed on the Oracle VM (see Deployment.md). Hosts prediction endpoints, feature lookups, dynamic model reload, and the agent chat endpoint.

## Endpoints

### `GET /health`
Liveness/readiness check. Returns overall system status and dependency checks (PostgreSQL database, Redis online store, MLflow model registry status, and active model versions).
```json
{
  "status": "ok",
  "dependencies": {
    "database": "connected",
    "redis": "connected",
    "mlflow": "reachable"
  },
  "models": {
    "demand": {"name": "demand_lightgbm_model", "version": "1", "stage": "Production"},
    "duration": {"name": "corridor_duration_lightgbm_model", "version": "1", "stage": "Production"}
  },
  "timestamp": "2026-09-08T15:30:00Z"
}
```

### `GET /predict/demand/{zone_id}`
Returns predicted pickup demand for the given NYC taxi zone over the configured horizon (default 15 minutes).
```json
{
  "zone_id": 161,
  "horizon_minutes": 15,
  "predicted_pickups": 42.0,
  "status": "ok",
  "cache_hit": true,
  "model_version": "1",
  "as_of": "2026-09-08T15:30:00Z"
}
```
- If `zone_id` is invalid ($< 1$ or $> 265$): returns `404 Not Found`.
- If `zone_id` features are missing in Redis online store (`cache_hit=False`): returns `200 OK` with `"status": "degraded_fallback"`, a genuine non-zero `predicted_pickups` output (e.g. `4.2`) evaluated by running LightGBM over the imputed feature vector (calendar harmonics + zero rolling counts + zone base bias), and `"warning"` explaining feature imputation (ADR-020).

### `POST /predict/demand/batch`
Batch demand prediction for multiple zones (or entire city) in a single vectorized call. Used by the Streamlit live map to render choropleths without per-zone HTTP overhead.
```json
// request
{
  "zone_ids": [161, 236, 142],
  "horizon_minutes": 15
}
// response
{
  "predictions": [
    {
      "zone_id": 161,
      "horizon_minutes": 15,
      "predicted_pickups": 42.0,
      "status": "ok",
      "cache_hit": true
    },
    {
      "zone_id": 236,
      "horizon_minutes": 15,
      "predicted_pickups": 15.3,
      "status": "ok",
      "cache_hit": true
    }
  ],
  "model_version": "1",
  "as_of": "2026-09-08T15:30:00Z",
  "prediction_count": 2
}
```
*Note: If `zone_ids` is empty or omitted, defaults to all 263 valid TLC zones.*

### `GET /predict/eta?origin={zone_id}&dest={zone_id}`
Returns predicted trip duration for the given corridor under current conditions.
```json
{
  "origin_zone_id": 161,
  "dest_zone_id": 236,
  "corridor_id": "161_236",
  "predicted_duration_seconds": 1104.0,
  "predicted_duration_minutes": 18.4,
  "status": "ok",
  "cache_hit": true,
  "model_version": "1",
  "as_of": "2026-09-08T15:30:00Z"
}
```

### `POST /predict/eta/batch`
Batch trip duration ETA prediction for multiple origin-destination corridor pairs.
```json
// request
{
  "corridors": [
    {"origin_zone_id": 161, "dest_zone_id": 236},
    {"origin_zone_id": 237, "dest_zone_id": 161}
  ]
}
// response
{
  "predictions": [
    {
      "origin_zone_id": 161,
      "dest_zone_id": 236,
      "corridor_id": "161_236",
      "predicted_duration_seconds": 1104.0,
      "predicted_duration_minutes": 18.4,
      "status": "ok",
      "cache_hit": true
    },
    {
      "origin_zone_id": 237,
      "dest_zone_id": 161,
      "corridor_id": "237_161",
      "predicted_duration_seconds": 720.0,
      "predicted_duration_minutes": 12.0,
      "status": "ok",
      "cache_hit": true
    }
  ],
  "model_version": "1",
  "as_of": "2026-09-08T15:30:00Z",
  "prediction_count": 2
}
```

### `GET /features/{entity_type}/{entity_id}`
Debug/inspection endpoint — returns the current online-store feature vector for a zone or corridor. Used by the UI and by the agent's tools.
- `entity_type`: `zone` (entity_id is int, e.g. `161`) or `corridor` (entity_id is string, e.g. `161_236`).
```json
{
  "entity_type": "zone",
  "entity_id": 161,
  "features": {
    "pickup_count_last_15m": 12,
    "pickup_count_last_1h": 48,
    "pickup_count_last_24h": 910,
    "pickup_count_same_hour_last_week": 45,
    "hour_of_day": 15,
    "day_of_week": 1,
    "is_weekend": false,
    "is_holiday": false,
    "avg_temp_last_1h": 21.5,
    "is_precipitating": false
  },
  "cache_hit": true,
  "retrieved_at": "2026-09-08T15:30:00Z"
}
```

### `POST /agent/chat` (Phase 7)
```json
// request
{ "message": "why is the ETA for zone 161 spiking right now?" }
// response
{ "reply": "...", "tools_used": ["get_features", "query_recent_predictions"] }
```

### `GET /pipeline/status`
Returns recent orchestration run history (from the `pipeline_runs` table) — "is the pipeline healthy" at a glance, also used by the agent.

## Caching & Latency SLAs

- **Prediction Caching TTL:** 60 seconds in Redis (keyed `pred:demand:...` and `pred:eta:...`), falling back to an in-memory TTL cache if Redis is temporarily unreachable (ADR-020).
- **Latency SLAs:**
  - Cached prediction: $< 5\text{ms}$
  - Single uncached prediction (Feast Redis lookup + LightGBM inference): $< 20\text{ms}$
  - Batch demand prediction (all 263 zones): $< 50\text{ms}$

## Error Handling & Degraded State Conventions

- All timestamps UTC, ISO 8601.
- Standard errors follow a consistent shape: `{"error": "...", "detail": "..."}` with appropriate HTTP status codes (400 for bad parameters, 404 for nonexistent entities, 422 for unprocessable payloads, 503 for unrecoverable service crashes).
- **Graceful Degradation (ADR-020):** If an entity is valid but online features are missing in Redis (`cache_hit=False`), `/predict/*` returns HTTP 200 with `"status": "degraded_fallback"`, `"cache_hit": false`, and a genuine model prediction computed over the imputed feature vector (e.g. calendar harmonics calculated from UTC `now` + zero rolling counts + zone base categorical bias, yielding realistic outputs like `4.2` pickups rather than hardcoded zero). Clients display visual warning indicators rather than failing.
- No public auth in v1 (internal Docker bridge network).

## Resolved Architectural Decisions

- **Batch Requests:** Supported via `POST /predict/demand/batch` and `POST /predict/eta/batch` (resolved in ADR-020).
- **Model Promotion Pickup:** Handled via container restart / redeployment in Phase 5; atomic hot-reload deferred to Phase 6 CI/CD (resolved in ADR-020).
- **Prediction Caching:** Standardized at 60s TTL in Redis (resolved in ADR-020).
