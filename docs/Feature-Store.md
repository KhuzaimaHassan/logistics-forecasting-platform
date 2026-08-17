# Feature Store

## 1. Why Feast, and how it's deployed

Open-source, self-hosted (see Decisions.md ADR-004). Offline store backed by Postgres; online store backed by Redis (co-located on the Oracle VM via Docker).

## 2. Entities

| Entity | Description |
|---|---|
| `zone` | NYC taxi zone ID — the primary grouping for demand features |
| `corridor` | Pickup-zone → dropoff-zone pair — used for duration/ETA features |

## 3. Feature views (initial set)

**`zone_demand_features`** (entity: `zone`)
- `pickup_count_last_15m`, `pickup_count_last_1h`, `pickup_count_last_24h` — rolling counts
- `pickup_count_same_hour_last_week` — seasonal baseline signal
- `avg_temp_last_1h`, `is_precipitating` — weather context (if available)
- `hour_of_day`, `day_of_week`, `is_holiday` — time features

**`corridor_duration_features`** (entity: `corridor`)
- `avg_duration_last_15m`, `avg_duration_last_1h` — rolling actuals
- `avg_traffic_speed_current` — from `traffic.snapshots`
- `distance_km` — static, from historical trip data
- `origin_zone_demand_pressure` — pulled from `zone_demand_features` for the origin zone (this is the demand→ETA link described in Architecture.md)

## 4. Offline vs online

- **Offline (Postgres/Parquet):** full historical feature values, used for training-set generation with point-in-time correctness (no future leakage — Feast handles this natively via event timestamps).
- **Online (Redis):** latest feature values only, used for low-latency serving at inference time.

## 5. Materialization

- Incremental materialization triggered by the stream consumer for near-real-time features (rolling counts).
- Scheduled materialization (Prefect, every 15 min) as a backstop/reconciliation pass in case the incremental path misses updates.

## 6. Open questions

- Whether `origin_zone_demand_pressure` should be the raw predicted demand (from the demand model) or the raw rolling count — using the raw count avoids a training-time circular dependency on the demand model's output; likely the correct choice, to confirm once training data volume is visible.
- Feature TTL / staleness policy for the online store — needs a concrete value before Phase 5 (serving).
