# ETL & Streaming

## 1. Batch extractor & two-stage warehouse loading

- Python extraction (`src/extract/batch_puller.py`) downloads TLC monthly Parquet files to local staging (`data/raw/`).
- **Two-stage loading pattern**:
  1. **Raw Staging (`raw.trips`)**: Bulk loads raw Parquet rows near-verbatim into PostgreSQL table `raw.trips` with `source_file` metadata.
  2. **Warehouse Transformation (`warehouse.trips`)**: Validates, cleans, derives features, and loads cleaned trips into `warehouse.trips`.
- **Concrete Batch Outlier & Cleaning Thresholds**:
  - `trip_duration_seconds`: $60 \le \text{duration} \le 86,400$ (1 minute to 24 hours).
  - `trip_distance`: $0.0 < \text{distance} \le 300.0$ miles.
  - `passenger_count`: $1 \le \text{passengers} \le 9$.
  - `average_speed_mph`: $\le 100.0$ mph (computed as $\text{distance} / (\text{duration} / 3600)$).
  - `fare_amount` & `total_amount`: $\ge 0.0$.
- **Rejection Audit Breakdown Semantics**: Per-reason rejection counts are **overlapping checks** (a single rejected row can violate multiple rules, e.g. invalid distance AND invalid passenger count). Therefore, individual reason counts reflect total rule violations across all records and do not sum up to the total net count of rejected rows.
- **Zone-ID Validation**: Drop rows with unmapped `PULocationID` or `DOLocationID` (outside standard NYC TLC Taxi Zones 1–265). Log summary counts and specific unmapped location IDs seen for run auditability without quarantine table overhead.
- **Deterministic `trip_id` Generation**: Generate a deterministic positive 60-bit BigInteger `trip_id` from a stable composite string (`vendor_id`, `pickup_datetime`, `dropoff_datetime`, `pickup_zone_id`, `dropoff_zone_id`, `fare_amount`, `trip_distance_miles`). Ensures re-running transformation against the same source file is strictly idempotent (`ON CONFLICT (trip_id) DO NOTHING`).


## 2. Streaming producer (live)


Two responsibilities, one process:

1. **Historical replay** — reads already-loaded TLC trip records and republishes them onto Redpanda at (configurable) real-time pace, keyed by original pickup timestamp offset from "now." This simulates a live order stream using genuinely real trip patterns rather than synthetic data.
2. **Live polling** — polls MTA subway alerts API (~30-60s), NYC traffic-speed Socrata API (~30-60s), and OpenWeatherMap (~10min) on independent schedules, publishing each to its own topic.

## 3. Redpanda topics

| Topic | Producer | Payload | Notes |
|---|---|---|---|
| `trip.events` | Replay producer | pickup/dropoff zone, timestamps, distance | Simulated live order flow |
| `traffic.snapshots` | Live poller | segment ID, avg speed, timestamp | Real live data |
| `transit.positions` | Live poller | route ID, delay seconds, congestion level, timestamp | Real live data (transit alert delay/congestion proxy) |
| `weather.snapshots` | Live poller | temp, precipitation, timestamp | Real live data, optional |

Single-node Redpanda broker (see Decisions.md ADR-003), 4 topics, low partition count (1-3) — deliberately small-scale, matching a personal-project traffic profile, not over-provisioned.

## 4. Stream consumer (transform)

- Consumes all four topics, applies cleaning/validation per payload type:
  - Timezone normalization to UTC.
  - Zone-ID validation against the Taxi Zone Lookup reference table (drop/flag unmapped IDs).
  - Outlier filtering (e.g., trip duration ≤ 0 or implausibly long, speed readings outside plausible range).
- Writes cleaned records to Postgres (see Database.md) and triggers Feast online-store updates for the affected entities (zone) where low latency matters.

## 5. Failure handling

- Consumer commits offsets only after a successful DB write — at-least-once delivery, dedup on (source, natural key) at the DB layer.
- Dead-letter topic (`*.deadletter`) for records that fail validation repeatedly, reviewed manually rather than silently dropped.

## 6. Open questions

- Replay speed factor (1x real-time vs accelerated) — likely configurable, accelerated for faster local testing.
- Whether `transit.positions` is worth the complexity for v1 given `traffic.snapshots` is a more direct signal — candidate to defer to Phase 4 stretch goal if time-constrained.
