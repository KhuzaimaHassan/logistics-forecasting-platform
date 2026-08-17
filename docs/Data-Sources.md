# Data Sources

## 1. NYC TLC Trip Record Data (historical, batch)

- **What:** Yellow/Green taxi and High-Volume For-Hire (HVFHV, i.e. Uber/Lyft-scale) trip records. Pickup/dropoff datetime, pickup/dropoff taxi zone ID, trip distance, fare, passenger count (yellow/green only).
- **Access:** Monthly Parquet files, published by NYC TLC, mirrored on the AWS Open Data Registry. No auth required; anonymous S3 read or direct HTTPS download.
- **Cadence:** New month's file published with ~1-2 month lag. We'll backfill 12-24 months of history for training, then pull new months as they land.
- **Used for:** Training data for both demand (aggregate pickups per zone/hour) and duration/ETA (dropoff_time - pickup_time) models. Also the source for the streaming *replay* (see ETL-Streaming.md).
- **Reference data:** NYC also publishes a static Taxi Zone Lookup table (zone ID → borough/zone name/geometry) — needed to join zone IDs to anything human-readable or map-plottable.

## 2. MTA GTFS-Realtime feeds (live, streaming)

- **What:** Live vehicle position feeds for NYC subway/bus.
- **Access:** Free API key via MTA developer portal. Protobuf format, refreshed roughly every 30 seconds.
- **Used for:** A live congestion proxy — transit delay/bunching correlates with road congestion, which is a real feature for the ETA model. Also serves as a genuine "live system integration" component distinct from the replayed historical stream.

## 3. NYC Open Data — Real-Time Traffic Speed Data (live, streaming)

- **What:** Live average traffic speed by road segment, published via Socrata API.
- **Access:** Free, no auth required for reasonable request volumes (Socrata app token recommended to avoid throttling).
- **Used for:** Direct congestion feature for the ETA model — more directly relevant than the transit-delay proxy above.

## 4. OpenWeatherMap (live, streaming — optional)

- **What:** Current weather conditions for NYC.
- **Access:** Free tier API key.
- **Used for:** Weather feature (precipitation, temperature) for both demand and ETA models. Treated as optional/stretch — the pipeline should work without it, and it should be easy to drop if the free tier proves too restrictive.

## 5. Licensing / attribution notes

- TLC trip data: public domain, NYC.gov terms of use — attribute as "NYC Taxi & Limousine Commission."
- MTA GTFS-RT: MTA Developer Terms of Use apply — no redistribution of raw feed data outside the project.
- NYC Open Data: Socrata/NYC Open Data terms — public, attribution appreciated not required.
- OpenWeatherMap free tier: attribution required per their ToS if displaying data publicly (UI should credit "Weather data from OpenWeatherMap").

## 6. Open questions

- Exact historical backfill window (12 vs 24 months) — decide during Phase 1 based on how large the cleaned Postgres table gets on the VM's storage budget.
- Whether HVFHV data (much larger volume than Yellow/Green) is worth including for v1, or added later once the pipeline is proven on the smaller Yellow/Green dataset.
