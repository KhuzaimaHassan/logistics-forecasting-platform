# Ticket Breakdown — Phase 4: Real-Time Layer

## Epic Summary
Implement the streaming data infrastructure for real-time demand and ETA forecasting in NYC taxi zones and corridors. Deploy and configure Redpanda topics with partitioned event streams, implement a configurable historical TLC trip replay producer to simulate realistic live order flow, implement resilient polling producers for external NYC live feeds (NYC Open Data traffic speed, MTA GTFS-RT transit delay proxy, and OpenWeatherMap), build an at-least-once streaming consumer in Python that cleans, validates, and writes streaming events into PostgreSQL (warehouse.trips, warehouse.traffic_snapshots, warehouse.weather_snapshots), and implement real-time Feast online store updates via Feast Push API (store.push()) with periodic incremental materialization reconciliation (ADR-018).

---

## Proposed Architecture Decisions

1. **ADR-018: Real-Time Feature Store Updates — Feast Push API with Tightened Materialize-Incremental Reconciliation**:
   - **Streaming Push Path (Sub-Second Latency)**: For streaming trip events and live traffic feeds, the stream consumer directly pushes real-time feature updates into the Redis online store using Feast's Push API (store.push(..., to=PushMode.ONLINE)). This provides instantaneous sub-second feature freshness for live inference endpoints (/predict/demand, /predict/eta).
   - **Batch Reconciliation Path (Data Durability & Cold Lags)**: A scheduled Prefect flow executes store.materialize_incremental() on a regular cadence (e.g. 1–5 min) to reconcile the Redis online store with the PostgreSQL offline store (warehouse.zone_demand_features_hourly, warehouse.corridor_duration_features_hourly), ensuring historical 7-day lag features and any out-of-order stream events remain consistent.
   - **At-Least-Once Delivery**: Stream consumer commits Redpanda offsets only after database write and Redis push succeed, utilizing deterministic 	rip_id deduplication at the DB layer (ON CONFLICT (trip_id) DO NOTHING).

---

## Tickets

### M4-1: Redpanda Topic Topology & Historical TLC Replay Producer
- **Scope / Acceptance Criteria:**
  - Verify and configure Redpanda topic definitions with optimal partition counts:
    - 	rip.events (simulated live trip stream, partitioned by pickup_zone_id)
    - 	raffic.snapshots (NYC traffic speed by segment)
    - 	ransit.positions (MTA transit positions/delays)
    - weather.snapshots (current weather observations)
    - 	rip.events.deadletter (poison/malformed trip payload quarantine)
  - Implement historical TLC replay producer in src/extract/replay_producer.py:
    - Reads existing warehouse.trips records (or local raw Parquet files) in chronological order.
    - Simulates streaming by rewriting trip pickup_datetime / dropoff_datetime relative to simulated current time.
    - Supports configurable playback speed multipliers (\times$ real-time, \times$, \times$, \times$ acceleration for smoke testing).
    - Publishes JSON-serialized payloads to Redpanda topic 	rip.events with partition key = pickup_zone_id.
    - Implements backpressure, rate limiting, and graceful shutdown signal handlers.
  - Unit tests verifying JSON serialization, timestamp offset transformation, partition key assignment, and rate limiting with mocked Kafka producer.
- **Per-Ticket Context:** docs/ETL-Streaming.md, docs/Data-Sources.md, docs/Decisions.md (ADR-003, ADR-018).
- **Files Touched:** src/extract/replay_producer.py, src/common/kafka_utils.py, 	ests/test_replay_producer.py.
- **Estimated Size:** ~250 lines.
- **Depends On:** Phase 3 baseline.

### M4-2: Live External Feed Polling Producers (Traffic, Transit, Weather)
- **Scope / Acceptance Criteria:**
  - Implement resilient polling producers in src/extract/live_feed_producers.py:
    - **NYC Traffic Speed Producer**: Polls NYC Open Data Socrata API (https://data.cityofnewyork.us/resource/i4gi-tjb9.json), extracts segment speed / travel time, maps coordinates to nearest taxi zone, and publishes to 	raffic.snapshots.
    - **MTA GTFS-RT Transit Producer**: Polls MTA GTFS-RT feed (or fallback REST transit alerts endpoint), extracts delay/congestion proxies, and publishes to 	ransit.positions.
    - **OpenWeatherMap Producer**: Polls NYC current weather (temperature, precipitation rate, rain/snow flags), and publishes to weather.snapshots.
  - Resilience & Config Discipline:
    - Dedicated polling schedules with exponential backoff and jitter.
    - Graceful no-op / synthetic fallback mode when external API keys (MTA_API_KEY, OPENWEATHERMAP_API_KEY, NYC_TRAFFIC_APP_TOKEN) are unconfigured, preventing pipeline crashes.
  - Unit tests with mocked HTTP responses verifying payload parsing, schema validation, and error handling.
- **Per-Ticket Context:** docs/Data-Sources.md, docs/ETL-Streaming.md.
- **Files Touched:** src/extract/live_feed_producers.py, 	ests/test_live_feed_producers.py.
- **Estimated Size:** ~300 lines.
- **Depends On:** M4-1.

### M4-3: Stream Consumer, Real-Time Validation & PostgreSQL Ingestion
- **Scope / Acceptance Criteria:**
  - Implement stream consumer service in src/transform/stream_consumer.py:
    - Subscribes to 	rip.events, 	raffic.snapshots, 	ransit.positions, weather.snapshots.
    - Validates incoming payloads using Pydantic schemas.
    - Applies real-time cleaning & normalization rules:
      - UTC timestamp conversion.
      - Taxi zone ID validation against warehouse.taxi_zones (1–263).
      - Outlier filtering (\text{s} \le \text{duration} \le 86,400\text{s}$,  < \text{distance} \le 300\text{mi}$, etc.).
    - Generates deterministic 64-bit 	rip_id and writes validated records to PostgreSQL warehouse.trips (source='replay' or 'live') with idempotent ON CONFLICT (trip_id) DO NOTHING.
    - Writes traffic and weather snapshots to warehouse.traffic_snapshots and warehouse.weather_snapshots.
    - Routes unparseable/poison records to 	rip.events.deadletter with failure reason metadata.
    - Implements at-least-once delivery (commits Redpanda consumer offset only after successful DB transaction).
  - Unit tests verifying deserialization, validation errors, dead-letter routing, and database insert operations.
- **Per-Ticket Context:** docs/ETL-Streaming.md, docs/Database.md.
- **Files Touched:** src/transform/stream_consumer.py, src/transform/schemas.py, 	ests/test_stream_consumer.py.
- **Depends On:** M4-1, M4-2.

### M4-4: Feast Online Store Streaming Push & Reconciliation Flow (ADR-018)
- **Scope / Acceptance Criteria:**
  - Implement Feast real-time push update path in `src/features/push_sources.py` & `src/transform/stream_consumer.py`:
    - Define dedicated Feast `PushFeatureView` / `PushSource` namespace (e.g. `zone_demand_features_push`) so streaming pushes do not conflict with batch hourly views.
    - Stream consumer pushes latest streaming feature updates directly into Redis online store via `store.push(..., to=PushMode.ONLINE)`.
    - Verify sub-second retrieval of updated feature values via `store.get_online_features(...)`.
  - Implement Prefect streaming reconciliation flow in `src/orchestration/flows/realtime_reconciliation_flow.py`:
    - **Reconciliation Staleness Resolution:** Triggers an incremental offline feature extraction step (`offline_extractor.py` on sliding lookback window $[T - \text{lookback}, T]$) against `warehouse.trips` **prior** to invoking `store.materialize_incremental()`. This guarantees newly-ingested live/replay trips are reflected in offline aggregation tables, preventing `materialize_incremental()` from overwriting fresh pushed values with stale offline data.
  - Integration tests verifying that published stream events immediately update Redis feature vectors and match subsequent materialization states.
- **Per-Ticket Context:** `docs/Feature-Store.md`, `docs/Decisions.md` (ADR-018).
- **Files Touched:** `src/features/push_sources.py`, `src/orchestration/flows/realtime_reconciliation_flow.py`, `tests/test_realtime_features.py`.
- **Estimated Size:** ~250 lines.
- **Depends On:** M4-3.

### M4-5: End-to-End Real-Time Pipeline Integration & CI Smoke Verification
- **Scope / Acceptance Criteria:**
  - Implement end-to-end integration test and live smoke verification script in `scripts/verify_streaming_live_smoke.py`:
    - Spins up Redpanda, Postgres, Redis, and runs replay producer + live feed producers + stream consumer.
    - Produces a burst of 100 historical trip events and live traffic snapshots.
    - Asserts stream consumer processes records, populates `warehouse.trips`, and routes zero records to dead-letter on happy path.
    - **Dead-Letter Observability Assertion:** Explicitly verifies dead-letter routing by producing a deliberate malformed record and executing a real count/read query against `trip.events.deadletter` or `warehouse.deadletter_events`, proving bad records are isolated rather than silently dropped.
    - Asserts Feast online store receives pushed features and returns valid non-null feature vectors for active zones.
  - Wire live streaming smoke verification into CI workflow (`.github/workflows/docker-smoke.yml` / `verify_live_feast_smoke.py`).
  - Update `docs/Roadmap.md` marking Phase 4 as complete.
- **Per-Ticket Context:** `docs/Architecture.md`, `docs/Roadmap.md`.
- **Files Touched:** `scripts/verify_streaming_live_smoke.py`, `scripts/verify_live_feast_smoke.py`, `.github/workflows/docker-smoke.yml`, `docs/Roadmap.md`.
- **Estimated Size:** ~250 lines.
- **Depends On:** M4-1, M4-2, M4-3, M4-4.

