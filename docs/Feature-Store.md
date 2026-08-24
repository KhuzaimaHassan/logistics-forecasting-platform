# Feature Store

## 1. Why Feast, and how it's deployed

Open-source, self-hosted (see [Decisions.md (ADR-004, ADR-013)](file:///docs/Decisions.md)).
- **Registry:** PostgreSQL SQL Registry (`registry_type: sql`) under the dedicated `feast` schema.
- **Offline Store:** Backed by PostgreSQL (`warehouse` schema).
- **Online Store:** Backed by Redis (co-located on the Oracle VM via Docker Compose).

## 2. Entities

| Entity | Description | Join Key | Value Type |
|---|---|---|---|
| `zone` | NYC taxi zone ID — the primary grouping for demand features | `zone_id` | `INT32` |
| `corridor` | Pickup-zone → dropoff-zone pair — used for duration/ETA features | `corridor_id` (`{pu}_{do}`) | `STRING` |

## 3. Feature views (initial set)

**`zone_demand_features`** (entity: `zone`)
- `pickup_count_last_15m`, `pickup_count_last_1h`, `pickup_count_last_24h` — rolling pickup counts
- `pickup_count_same_hour_last_week` — seasonal baseline signal
- `avg_temp_last_1h`, `is_precipitating` — weather context (nullable/default until real-time weather feed lands)
- `hour_of_day`, `day_of_week`, `is_weekend`, `is_holiday` — calendar and temporal features

**`corridor_duration_features`** (entity: `corridor`)
- `avg_duration_last_15m`, `avg_duration_last_1h` — rolling actual trip durations
- `avg_traffic_speed_current` — traffic context (nullable/default until real-time traffic feed lands)
- `distance_km` — static baseline distance from historical trip actuals
- `origin_zone_demand_pressure` — raw rolling pickup count pulled from `zone_demand_features` for the origin zone ([ADR-014](file:///docs/Decisions.md))

## 4. Offline vs online

- **Offline (Postgres / warehouse):** Full historical feature values used for point-in-time correct training dataset generation.
  - **Anti-Leakage Gating:**
    - `zone_demand_features` aggregates strictly where `pickup_datetime <= T`.
    - `corridor_duration_features` aggregates strictly where `dropoff_datetime <= T` (duration is only observed after trip completion).
- **Online (Redis):** Latest feature values only, indexed by entity key for fast inference serving.
  - **Latency SLA:** Sub-10ms lookup time (< 10ms SLA target for online batch entity lookups).
  - **Key TTL / Staleness Policy:** 24-hour expiration (86,400s TTL) on Redis feature keys to bound cache footprint while retaining lookback history.

## 5. Materialization

- **Incremental Materialization:** Triggered by the stream consumer for near-real-time rolling counts.
- **Scheduled Materialization:** Prefect flow running on a 15-minute schedule as a backstop/reconciliation pass to ensure Redis parity with the warehouse.

## 6. Resolved Decisions & Open Questions

- **`origin_zone_demand_pressure` (Closed in ADR-014):** Formally resolved as the raw rolling pickup count from `zone_demand_features` (not demand model predictions) to avoid circular model dependencies and training data leakage.
- **Online Store TTL & Staleness (Closed):** Formally resolved as a 24-hour key TTL in Redis with a < 10ms lookup SLA target.
