# ETL & Streaming

## 1. Batch extractor (historical)

- Python job (`src/extract/batch_puller.py`) that downloads new TLC monthly Parquet files, plus the one-time backfill run for the initial historical window.
- Idempotent: re-running for a month that's already loaded is a no-op (checked against a `loaded_months` tracking table in Postgres).
- Scheduled monthly via Prefect (new-month check), plus a manual backfill flow for initial setup.

## 2. Streaming producer (live)

Two responsibilities, one process:

1. **Historical replay** — reads already-loaded TLC trip records and republishes them onto Redpanda at (configurable) real-time pace, keyed by original pickup timestamp offset from "now." This simulates a live order stream using genuinely real trip patterns rather than synthetic data.
2. **Live polling** — polls MTA GTFS-RT (~30s), NYC traffic-speed API (~30-60s), and OpenWeatherMap (~10min) on independent schedules, publishing each to its own topic.

## 3. Redpanda topics

| Topic | Producer | Payload | Notes |
|---|---|---|---|
| `trip.events` | Replay producer | pickup/dropoff zone, timestamps, distance | Simulated live order flow |
| `traffic.snapshots` | Live poller | segment ID, avg speed, timestamp | Real live data |
| `transit.positions` | Live poller | vehicle ID, route, position, timestamp | Real live data (congestion proxy) |
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
