# Monitoring

## 1. Scope

Two distinct things, not to be conflated:
- **Pipeline observability** — did the scheduled jobs run, and did they succeed? (Prefect's job, surfaced via the `pipeline_runs` table and `/pipeline/status`.)
- **Data/model monitoring** — is the data or the model's behavior drifting? (Evidently AI's job.)

## 2. Evidently AI reports

- **Data drift** — compares recent feature distributions (rolling demand counts, traffic speeds) against a reference window, flags significant shifts.
- **Prediction drift** — compares recent prediction distributions against a reference window.
- **Model performance decay** — once enough ground truth has accumulated (actual pickups/durations vs. predictions), tracks MAE/RMSE trend over time against the baseline.

## 3. Scheduling

- Run daily via Prefect, not on every request — drift is a slow-moving signal, no need for per-prediction overhead.
- Reports written to a `monitoring_reports` table/directory, surfaced on the UI's Monitoring Dashboard and readable by the agent's `search_logs_and_model_cards` tool.

## 4. Alerting (lightweight, v1)

- No external alerting service for v1 — a personal project doesn't need PagerDuty. Drift/failure surfaces on the dashboard and is queryable by the agent ("has anything drifted this week").
- Worth revisiting only if the project moves toward the side-hustle-product direction mentioned as a possible purpose.

## 5. Open questions

- Reference window definition (fixed baseline vs. rolling N-day-ago window) — rolling is more realistic for a live system, fixed is simpler to reason about; leaning rolling, to confirm once real drift patterns are visible.
