# AI Pipeline

## 1. Models

Two prediction targets, trained and versioned independently, connected via the `origin_zone_demand_pressure` feature (see Feature-Store.md):

**Demand model** — predicts pickup count for a zone over the next window (e.g., next 15/60 min).
**Duration/ETA model** — predicts trip duration for a given corridor under current conditions.

## 2. Baselines (mandatory before any "real" model)

- **Seasonal-naive:** "same time last week" — this is the bar every model must beat. No forecasting project's results mean anything without this comparison, and it's cheap to compute.
- Logged to MLflow alongside real models so lift is always visible, not asserted.

## 3. Model candidates

| Stage | Model | Why |
|---|---|---|
| v1 | LightGBM / XGBoost (regression) | Strong tabular baseline, fast to train/retrain, feature importances are directly useful to the Ops Agent's explanations |
| Stretch | Prophet or a small LSTM/TFT (demand only) | Only pursued if v1 baselines are solid and time remains — not a Phase 3 blocker |

## 4. Evaluation metrics

- Demand: MAE, RMSE, WAPE (Weighted Absolute Percentage Error), and % improvement over seasonal-naive.
- Duration: MAE (minutes/seconds), RMSE, WAPE, % improvement over seasonal-naive.
- Both: evaluated per-zone/per-corridor, not just in aggregate — aggregate metrics can hide poor performance in low-volume zones, which matters more for the agent's "why is this specific zone off" use case.

### Known Data Characteristics & Outlier Tail Handling (ADR-016)
- **TLC Taximeter Shift Overlap Tail**: In raw TLC data, a tiny fraction of trips (~0.088%, 2,594 out of 2.94M trips in 2023-01) have recorded durations between 10 hours and 23.99 hours (max 86,388s) with short distances (e.g., 2–4 km) and small fares ($10–$25). These occur when taxi meters are inadvertently left running overnight until the driver's next shift the following day.
- **Aggregation Impact**: When sampled into corridor-hour observations where an obscure corridor has only 1 active trip in that hour, the ground-truth target duration reflects this extreme value.
- **Evaluation Discipline**: MAE and WAPE are preferred as primary optimization and evaluation metrics because they are robust against extreme tail distortions, while RMSE will naturally reflect high penalties on rare unclosed-meter anomalies.
- **Baseline Duration $R^2$ Artifact ($R^2 \approx -0.96$)**: The moving-average baseline produces a negative $R^2$ solely because squared error on rare 10h–24h tail targets dwarfs the variance around the mean ($\text{RSS} > \text{TSS}$), even though 50% of predictions are within 176s (2.93 min MedAE) and overall MAE is 456.95s (7.62 min).

### Feature Engineering & Target Transformations (ADR-017 / M3-4)
- **Cyclical Harmonic Encodings**: `hour_of_day` and `day_of_week` are encoded into continuous cyclical features ($\sin(2\pi h / 24)$, $\cos(2\pi h / 24)$, $\sin(2\pi d / 7)$, $\cos(2\pi d / 7)$) to maintain smooth boundary continuity (e.g. 23:00 to 00:00).
- **Corridor Categorical Entities**: `pickup_zone_id` and `dropoff_zone_id` are parsed from `corridor_id` and treated as categorical features for tree gradient partition learning.
- **Log1p Duration Target Transformation**: Given the log-normal distribution and right-tail meter anomalies, the corridor duration model trains on $\ln(1 + \text{duration})$, inverting predictions via $\hat{y} = \max(60.0, \exp(\hat{y}_{\log}) - 1.0)$ during evaluation and serving.


## 5. Training pipeline

1. Pull training set from Feast offline store (point-in-time correct).
   - **Sampling Constraint (ADR-015):** Training entity observation timestamps must be sampled on **hour boundaries (`HH:00:00` UTC)** to align with the 1-hour offline table grain and avoid sub-hour feature snapshot staleness.
2. Train/validation split by time (not random) — standard practice for forecasting, avoids leakage.
3. Train LightGBM / XGBoost regressors on training split (Jan 8–24) and evaluate against seasonal-naive baseline on validation split (Jan 25–31).
4. Log params, validation metrics (MAE, RMSE, WAPE, % lift), feature importances (gain & split), and serialized model binaries to MLflow tracking and Model Registry.
5. Manual promotion to "production" stage in MLflow registry initially; automated promotion-on-improvement is a Phase 6+ CI/CD enhancement, not v1.



## 6. Retraining triggers

- Scheduled (weekly, via Prefect) as the default.
- Drift-triggered (Evidently flags significant data or prediction drift) as an earlier trigger — see Monitoring.md.

## 7. Open questions

- Exact prediction horizon(s) to support (single 15-min horizon vs multiple) — start with one horizon per model, expand only if the pipeline handles it cleanly.
- Whether duration model needs to be per-corridor or can generalize across corridors with corridor-level features — likely the latter, to avoid a combinatorial explosion of low-data corridor-specific models.
