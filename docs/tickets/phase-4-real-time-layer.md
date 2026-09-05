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
    - **NYC Traffic Speed Producer**: Polls NYC Open Data Socrata API (https://data.cityofnewyork.us/resource/i4gi-tjb9.json), extracts segment speed / travel time, maps coordinates to nearest taxi zone, and publishes to traffic.snapshots.
    - **MTA GTFS-RT Transit Producer**: Polls MTA GTFS-RT feed (or fallback REST transit alerts endpoint), extracts delay/congestion proxies, and publishes to transit.positions.
    - **OpenWeatherMap Producer**: Polls NYC current weather (temperature, precipitation rate, rain/snow flags), and publishes to weather.snapshots.
  - Resilience & Config Discipline:
    - Dedicated polling schedules with exponential backoff and jitter.
    - Graceful no-op / synthetic fallback mode when external API keys (MTA_API_KEY, OPENWEATHERMAP_API_KEY, NYC_TRAFFIC_APP_TOKEN) are unconfigured, preventing pipeline crashes.
  - Unit tests with mocked HTTP responses verifying payload parsing, schema validation, and error handling.
- **Verification Note & Tracked Fast-Follow:**
  - `transit.positions` and `weather.snapshots` are currently proven only via synthetic fallback — `MTA_API_KEY` and `OPENWEATHERMAP_API_KEY` were never obtained. Real-data proof for these two feeds is a fast-follow, same pattern as ADR-007's R2 credentialing (tracked in #93).
- **Per-Ticket Context:** docs/Data-Sources.md, docs/ETL-Streaming.md.
- **Files Touched:** src/extract/live_feed_producers.py, tests/test_live_feed_producers.py, scripts/verify_live_feeds_stream.py.
- **Estimated Size:** ~300 lines.
- **Depends On:** M4-1.

### M4-3: Stream Consumer, Real-Time Validation & PostgreSQL Ingestion
- **Cleaning & Validation Specification (Reused vs. Newly Defined):**
  - **Reused Rules from `batch_transformer.py` (Trip Events on `trip.events`)**:
    - `trip_duration_seconds`: $60\text{s} \le \text{duration} \le 86,400\text{s}$ (1 minute to 24 hours).
    - `trip_distance_miles`: $0.01\text{ mi} \le \text{distance} \le 300.0\text{ mi}$ ($0.016\text{ km} \le \text{distance\_km} \le 482.8\text{ km}$).
    - `passenger_count`: $1 \le \text{passengers} \le 9$ (if provided; nullable).
    - `average_speed_mph`: $\le 100.0\text{ mph}$ (computed from distance / duration).
    - `fare_amount`, `total_amount`, `tip_amount`: $\ge 0.0$ (non-negative).
    - `pickup_zone_id`, `dropoff_zone_id`: Valid NYC taxi zone IDs in $[1, 265]$.
    - `trip_id`: Deterministic positive 60-bit BigInteger generated via `generate_deterministic_trip_id`.
  - **Newly Defined Plausibility Rules (Traffic, Transit, Weather Feeds)**:
    - **Traffic (`traffic.snapshots`)**:
      - `speed_mph`: $0.0 \le \text{speed\_mph} \le 100.0\text{ mph}$ ($0.0 \le \text{speed\_kmh} \le 160.93\text{ km/h}$). Speeds $< 0$ or $> 100\text{ mph}$ are rejected as sensor anomalies.
      - `travel_time_seconds`: $0 \le \text{travel\_time\_seconds} \le 7,200\text{s}$ (max 2 hours per road segment).
      - `segment_id`: Non-empty string.
      - `recorded_at`: Valid ISO-8601 UTC timestamp within $[\text{now} - 30\text{d}, \text{now} + 24\text{h}]$.
    - **Transit (`transit.positions`)**:
      - `route_id`: Non-empty string (e.g. subway route `1`, `A`, `L`, `NYCT`).
      - `delay_seconds`: $0 \le \text{delay\_seconds} \le 86,400\text{s}$ (non-negative, max 24 hours).
      - `congestion_level`: Must match valid enum `{"NORMAL", "MODERATE", "HEAVY_DELAY", "UNKNOWN"}`.
      - `recorded_at`: Valid ISO-8601 UTC timestamp within $[\text{now} - 30\text{d}, \text{now} + 24\text{h}]$.
    - **Weather (`weather.snapshots`)**:
      - `temp_c`: $-35.0^\circ\text{C} \le \text{temp\_c} \le 55.0^\circ\text{C}$ ($-31^\circ\text{F}$ to $131^\circ\text{F}$, NYC meteorological extreme bounds).
      - `precipitation_mm_1h`: $0.0 \le \text{precipitation\_mm\_1h} \le 300.0\text{ mm}$ (non-negative, max cloudburst rate).
      - `wind_speed_kmh`: $0.0 \le \text{wind\_speed\_kmh} \le 250.0\text{ km/h}$ (non-negative, hurricane bounds).
      - `recorded_at`: Valid ISO-8601 UTC timestamp within $[\text{now} - 30\text{d}, \text{now} + 24\text{h}]$.
- **Scope / Acceptance Criteria:**
  - Implement Pydantic validation schemas in `src/transform/schemas.py` for all 4 stream topics (`TripEventPayload`, `TrafficSnapshotPayload`, `TransitPositionPayload`, `WeatherSnapshotPayload`) and dead-letter payloads (`DeadletterPayload`).
  - Implement stream consumer service in `src/transform/stream_consumer.py`:
    - Subscribes to `trip.events`, `traffic.snapshots`, `transit.positions`, `weather.snapshots`.
    - Validates incoming payloads with Pydantic; cleans and normalizes timestamps to UTC.
    - Reuses `generate_deterministic_trip_id` from `src/transform/batch_transformer.py` for trip records.
    - Writes valid records to PostgreSQL `warehouse.trips` (`source='replay'` or `'live'`), `warehouse.traffic_snapshots`, `warehouse.weather_snapshots`, `warehouse.transit_snapshots` with idempotent `ON CONFLICT DO NOTHING`.
    - Routes validation and deserialization failures directly to Redpanda topic `trip.events.deadletter` with detailed error reason, raw payload, and timestamp.
    - Enforces at-least-once delivery: commits consumer group offsets only after the PostgreSQL transaction succeeds.
  - Unit tests in `tests/test_stream_consumer.py` verifying deserialization, schema validation rules, dead-letter routing, and DB upsert logic.
- **Per-Ticket Context:** `docs/ETL-Streaming.md`, `docs/Database.md`, `docs/Decisions.md` (ADR-018, ADR-019).
- **Files Touched:** `src/transform/stream_consumer.py`, `src/transform/schemas.py`, `src/common/models.py`, `tests/test_stream_consumer.py`, `scripts/verify_stream_consumer_smoke.py`.
- **Estimated Size:** ~350 lines.
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
- **Files Touched:** `src/features/push_sources.py`, `src/features/views.py`, `src/features/client.py`, `src/transform/stream_consumer.py`, `src/orchestration/flows/realtime_reconciliation_flow.py`, `tests/test_realtime_features.py`.
- **Estimated Size:** ~450 lines.
- **Depends On:** M4-3.
- **Status:** Complete. Dedicated Feast `PushSource` and `FeatureView` definitions registered in `src/features/push_sources.py`. `FeastOnlineClient` supports hybrid coalescing (`use_push_features=True`) with transparent fallback to batch features. `StreamFeatureAggregator` computes 15m/1h sliding deques preserving offline extractor invariants; `StreamConsumerService` performs best-effort online store pushes with guaranteed partition commit continuity. Prefect two-stage `realtime_reconciliation_flow.py` extracts recent trips prior to incremental Redis materialization. Validated across 21 unit/integration tests in `test_realtime_features.py`, `test_online_features.py`, and `test_stream_consumer.py`.

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

---

## Tracked Fast-Follows

### M4-2-FF: Real-Data Proof for MTA & OpenWeather Live Feeds (#93)
- **Scope / Acceptance Criteria:**
  - `transit.positions` and `weather.snapshots` are currently proven only via synthetic fallback — `MTA_API_KEY` and `OPENWEATHERMAP_API_KEY` were never obtained.
  - Real-data proof for these two feeds is a fast-follow, same pattern as ADR-007's R2 credentialing.
  - When API keys are provisioned in `.env` / CI secrets, execute `scripts/verify_live_feeds_stream.py` to assert live responses (`source: 'mta_live'`, `source: 'openweathermap_live'`).


