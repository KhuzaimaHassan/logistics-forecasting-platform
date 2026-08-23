# Database

Single Postgres instance (Docker, on the Oracle VM) serves three roles: warehouse for cleaned data, Feast's offline store, and MLflow's backend store. Kept as one instance deliberately — separate schemas, not separate databases, to stay within the VM's resource budget.

## Schemas

- `raw` — landing zone for extracted-but-unvalidated data (mirrors source structure closely).
- `warehouse` — cleaned, validated tables used for training and feature computation.
- `feast` — Feast's own offline-store tables (managed by Feast, not hand-edited).
- `mlflow` — MLflow's backend store (managed by MLflow, not hand-edited).

## Core `warehouse` tables (initial draft)

**`trips`**
| Column | Type | Notes |
|---|---|---|
| trip_id | bigint | PK (Identity) |
| vendor_id | int | TLC vendor identifier |
| cab_type | text | 'yellow' or 'green' |
| pickup_zone_id | int | FK → taxi_zones |
| dropoff_zone_id | int | FK → taxi_zones |
| pickup_datetime | timestamptz | Trip start timestamp |
| dropoff_datetime | timestamptz | Trip end timestamp |
| trip_duration_seconds | int | Duration in seconds |
| time_bin_15m | timestamptz | 15-minute floor of pickup_datetime |
| day_of_week | int | 0=Monday to 6=Sunday |
| hour_of_day | int | 0 to 23 |
| is_weekend | boolean | True for Saturday/Sunday |
| trip_distance_km | numeric | Distance in kilometers |
| fare_amount | numeric | Base fare |
| tip_amount | numeric | Tip amount |
| total_amount | numeric | Total charged amount |
| source | text | 'historical' or 'replay' |


**`taxi_zones`** (static reference, loaded once from TLC's Taxi Zone Lookup)
| Column | Type | Notes |
|---|---|---|
| zone_id | int | PK |
| borough | text | |
| zone_name | text | |
| centroid_lat | numeric | centroid latitude for UI map plotting |
| centroid_lon | numeric | centroid longitude for UI map plotting |

**`predictions`** (prediction log for monitoring and agent queries)
| Column | Type | Notes |
|---|---|---|
| prediction_id | uuid / bigint | PK |
| entity_type | text | 'zone' or 'corridor' |
| entity_id | text | e.g. zone ID or origin-dest corridor string |
| model_version | text | MLflow registered model version |
| predicted_value | numeric | predicted pickups or duration in minutes |
| predicted_at | timestamptz | inference timestamp |
| actual_value | numeric | nullable, recorded once ground truth arrives |
| actual_recorded_at | timestamptz | nullable, ground truth timestamp |

**`monitoring_reports`** (Evidently drift/performance report outputs)
| Column | Type | Notes |
|---|---|---|
| report_id | uuid / bigint | PK |
| report_type | text | 'data_drift', 'prediction_drift', 'performance_decay' |
| generated_at | timestamptz | report generation timestamp |
| summary_json | jsonb | aggregated metrics summary |
| file_path | text | path/URI to full HTML/JSON report artifact |

**`traffic_snapshots`**
| Column | Type | Notes |
|---|---|---|
| segment_id | text | |
| avg_speed_kmh | numeric | |
| recorded_at | timestamptz | |

**`weather_snapshots`**
| Column | Type | Notes |
|---|---|---|
| recorded_at | timestamptz | |
| temp_c | numeric | |
| is_precipitating | boolean | |

**`loaded_months`** — tracks which TLC monthly files have already been ingested (idempotency for the batch extractor).

**`pipeline_runs`** — lightweight log of orchestrated job runs (job name, status, duration, triggered_by), queried by the Ops Agent when asked "did the pipeline run OK."

## Open questions

- Retention policy for raw `trip.events`/`traffic.snapshots` once aggregated into features — needed before storage becomes a constraint on the VM's 200GB.
