# Deployment report — 2026-10-19

## Outcome

**The public Streamlit dashboard and FastAPI forecasts are working. Complete
platform acceptance is still partial.** These are actual deployed URLs:

- [Streamlit dashboard](https://logistics-forecasting-platform.streamlit.app/)
- [FastAPI documentation](https://logistics-forecasting-platform.onrender.com/docs)

Browser verification displayed genuine demand for zones 74/75/166
(3.69/5.55/2.26 pickups), an official-centroid map and corridor 75_237 ETA
11.9 minutes (715.8 seconds). Predictions describe a bounded January 2023 TLC
sample; they are not current NYC telemetry or full-population forecasts.

Aiven ingestion, PostgreSQL persistence, Feast/Valkey, DagsHub tracking/artifacts,
real model training, public forecasts and durable monitoring HTML are verified.
Acceptance exposed defects missed by prior tests. Corrected monitoring has been
rerun and verified. Forecast persistence/labeling fixes are pushed but public UI
rollout is still NOT VERIFIED. A real Groq call reached the agent but its pipeline
tool failed argument validation; this is FAILED, not a successful LLM integration.
The fix is pushed and requires a live repeat after Render deploys it.

No paid resource, automatic upgrade, data deletion or local Docker run was used.

## D0 feasibility and source audit

The installed lock/runtime is Feast 0.65.0, MLflow 3.15.1, Prefect 3.8.3,
Evidently 0.7.21 and LightGBM 4.7.0. Source confirms TLC extraction -> normalized
warehouse trips -> PostgreSQL hourly features -> Feast PIT training/Redis online
materialization -> MLflow Production -> FastAPI -> Streamlit. LangGraph tools
read Feast/PostgreSQL/FAISS; Evidently reports persist to monitoring tables.

The complete Compose stack requires continuous Redpanda/PostgreSQL/Redis/API/UI
and an MLflow server, plus producer/consumer/Prefect worker processes. Redpanda
alone reserves 1 GB. The existing 8 GB host budget and multi-million-row warehouse
cannot fit a free 512 MB web service or 1 GB managed database. Extract/training/
feature materialization/monitoring are bounded batch candidates. Continuous Kafka
broker/storage move to managed Free services; workers become scheduled.

| Component | Feasibility and required changes |
|---|---|
| Redpanda/Kafka clients | Kafka protocol preserved with managed broker; add mTLS/SASL_SSL, two-partition topic cap, individual durable commits and acked DLQ |
| PostgreSQL | Actual managed PostgreSQL, no SQLite cloud substitution; bounded sample and connections; additive cursor/report-HTML migration |
| Feast | Feature definitions/PIT logic preserved; managed URL/TLS translation required; native Valkey materialization/retrieval verified against Aiven |
| MLflow | Training/metrics/artifacts/registry preserved on DagsHub; native Booster serving avoids full pyfunc dependency load; live metrics/artifacts/registry-list proof verified; model-version promotion checked separately |
| Prefect | Existing flows retained; OSS execution on Actions because current Hobby plan does not support the existing BYO process pool |
| FastAPI | Strict cloud models/features, synchronous successful persistence, native loading, health/live/ready, CORS and authenticated limited agent |
| Streamlit | Existing forecast tab was a placeholder; now requests actual demand/ETA, authentic centroids and renders a map/table; API wakeup handling |
| Monitoring | Cloud synthetic baseline disabled; real historical backtest path with durable HTML; no live drift claim |
| Full historical ETL/live feeds | Original implementations retained; 10k-row authentic historical sample is the free profile; synthetic feed fallback disabled in cloud |

The audit also found whole-poll Kafka commits could lose buffered messages,
transient DB errors could be deadlettered/committed, app startup could use SQLite,
and models could silently be untrained baselines. These are addressed in the cloud
path and focused regression checks. The bundled zone reference explicitly labels
coordinates representative; cloud bootstrap computes official polygon centroids.
Demand models predict next-hour counts; cloud API requires `horizon_minutes=60`.

## Selected architecture and honest limits

| Capability | Actual implementation | Trade-off |
|---|---|---|
| Streaming broker | Existing Aiven Free Kafka, mutual TLS | Real transport; bounded scheduled consumers, no continuous worker |
| Storage | Existing Aiven PostgreSQL 18.6, `logistics`, verified TLS | 1 GB Free storage; bounded sample, 700 MiB safety stop; no automatic deletion |
| Online features | Feast 0.65.0 with existing Aiven Free Valkey over verified TLS | Features expire after 24 hours; scheduling must be enabled for continued availability |
| Experiments/models | DagsHub MLflow metrics, artifacts and Production v1 registry | Real model artifacts; native Booster loading reduces API memory |
| API | Render Free Docker, 512 MiB/0.1 CPU | Sleeps after inactivity; ephemeral disk, models restored from DagsHub |
| Dashboard | Streamlit Community Cloud thin API client | Public forecasts verified; sleeps and may need an owner reboot/update |
| Pipelines | Existing Prefect OSS flows on public-repository GitHub Actions | Finite scheduled jobs; current Prefect Hobby does not support the existing BYO worker |
| Copilot | LangGraph with Groq Free, authenticated API/tool allowlist | Real provider reached; live tool verification failed and is being repaired |
| Monitoring | Evidently historical sample backtests persisted in PostgreSQL | Historical analysis, not live prediction drift; automatic retraining disabled |

The complete multi-process stack cannot fit a single free 512 MB instance.
Original local Compose and full-capacity implementations remain available.
See [official provider limits](FREE_TIER_RESEARCH.md) and
[exact configuration, secrets and commands](FREE_CLOUD_SETUP.md).

## Verification matrix

Only genuine observations below certify deployed behavior. Disposable CI services
and mocked unit tests do not certify Aiven, DagsHub or public applications.

| Service/capability | Status | Actual evidence / remaining condition |
|---|---|---|
| Managed Kafka produce/consume | **VERIFIED** | [Aggregate 39242666434](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/actions/runs/39242666434): partition 1 produced/consumed offset 4669; authentic TLC trip, mutual TLS |
| Managed PostgreSQL write/read | **VERIFIED** | Same aggregate wrote/read a retained verification record; prior bootstrap persisted 9,232 authentic warehouse trips; actual PostgreSQL 18.6 |
| Valkey TLS + Feast materialization/retrieval | **VERIFIED** | Same aggregate rematerialized real features; zone 7 pickups=1 matched PostgreSQL with historical event timestamp |
| MLflow metrics/artifacts/registry | **VERIFIED** | Same aggregate read metric 1.0, verified downloaded artifact SHA-256 and registry listing; real training/promotion evidence below |
| Real pipeline output | **VERIFIED** | Completed managed pipeline processed 8,632 remaining events after checkpoint 600, ending at 9,232 unique rows; manually dispatched, not cron evidence |
| Actual scheduled trigger | **NOT VERIFIED** | BLOCKED — NOT VERIFIED: deployment workflow must reach default branch through protected review and be explicitly enabled; no scheduled-event run claimed |
| Real training/promotion | **VERIFIED** | [39228745442](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/actions/runs/39228745442): 635.55s managed run, both genuine LightGBM Production v1 models reloaded natively; [evidence](managed-training-evidence.json) |
| Public FastAPI genuine demand/ETA | **VERIFIED** | Aggregate produced demand 0.71 and ETA 715.8s, matched two newly persisted Aiven prediction records; no fallback |
| Streamlit forecast table/map/ETA | **VERIFIED** | Browser actions displayed real 74/75/166 values 3.69/5.55/2.26, map, and 75_237 ETA 11.9m; [screenshot](evidence/streamlit-public-demand.png) |
| Public pipeline/report UI | **VERIFIED** | Browser showed real pipeline history, monitoring cards and actual embedded Evidently performance HTML |
| UI fixes deployed | **NOT VERIFIED** | BLOCKED — NOT VERIFIED: public UI still shows old header/tab and loses demand after ETA rerun; source session-persistence/label fixes pass AppTest but require public rollout confirmation |
| Live LLM tool execution | **FAILED** | Aggregate assertion failed; browser showed Groq gpt-oss-20b with invalid empty-name argument to zero-argument query_pipeline_status. Fix pushed; successful managed tool call still required |
| Corrected monitoring decisions | **VERIFIED** | [39242531954](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/actions/runs/39242531954): authoritative Evidently flags match summaries; actual public readback checks per-column count and distance direction; [evidence](corrected-monitoring-evidence.json) |
| Monitoring HTML persistence | **VERIFIED** | Latest performance report read back 3,701,744 HTML characters; genuine historical backtest |
| Hosted redeploy recovery | **VERIFIED** | Render revision 751a075 reloaded the same Production v1 models with new loaded_at; readiness and both forecasts passed; [evidence](public-redeploy-evidence.json) |
| Controlled hosted idle cold start | **NOT VERIFIED** | No controlled sleep/wake timing performed; initial observed 93.4s request is not such proof |
| Genuine-model Free runtime limits | **VERIFIED** | [39230564684](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/actions/runs/39230564684): two fresh 512 MiB/0.1 CPU containers, concurrent 145-zone batches and durable predictions; peak process RSS 386.7 MiB |
| Full CI including Compose | **VERIFIED** | [39244387332](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/actions/runs/39244387332), 0bcad87: all three jobs passed; 388 tests passed, one skipped, 45 warnings in 79.39s; Compose includes real disposable-service integrations |
| ARM64 images | **VERIFIED** | [39244382411](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/actions/runs/39244382411), 0bcad87, successful multi-arch build |

[Latest aggregate JSON](cloud-verification.json) preserves its actual failure and
manual acceptance=false. Browser/redeploy evidence is separate: an automated
HTTP-shell check does not certify visualization or a controlled restart.

## Acceptance defects and corrections

Evidently 0.7.21 chooses distance tests for the exercised 2,000-row windows.
Previous code interpreted every score as a p-value, reversing several flags and
creating a false prediction-drift alert. Corrected analyzers use Evidently's own
test decisions and explicit score types. The new reports have
`score_semantics_version=2`; distances have `p_value=null`. Data drift reports
one of eight columns drifted (`hour_of_day`), prediction drift is false, and the
actual MAE degradation alert remains 29.9%. No retraining was triggered.
The old [monitoring evidence](managed-monitoring-evidence.json) is retained for
audit; its custom drift flags are legacy and must not be treated as current truth.

The dashboard fix retains demand/map and ETA across unrelated reruns, invalidates
results after selection changes, exposes actual MAE metrics and labels historical
scheduled data honestly. A successful materialization now records its own time;
a cursor's modification timestamp cannot stand in for feature freshness.
Actual managed refresh: 2026-10-19T11:18:10.259847+00:00.

The real Groq acceptance failure led to closed argument schemas, strict empty
objects for zero-argument tools, typed validation and explicit error status.
An execution warning must not be presented as success. See ADR-029 and the
[deployment log](DEPLOYMENT_LOG.md) for actual failed/cancelled runs and repairs.

## Data, model quality and resource scope

The source is official Green TLC January 2023 Parquet with recorded SHA-256.
10,000 source positions yielded 9,232 valid trips and 768 validation rejections.
107,155 zone and 8,760 corridor feature rows use official polygon centroids.
All 263 reference zones are available; 145 have observations in this sample.
Demand holdout MAE 0.0493 versus baseline 0.0540; ETA MAE 430.30s versus baseline
666.49s. These sparse historical sample results are not live/full-population
benchmarks. No promotion gate was bypassed; no synthetic labels or predictions
stand in for missing online data.

The initial full Compose budget was about 8 GB including a broker reservation of
1 GB. API import RSS alone was 204.8 MiB on Windows; this is not a capacity proof.
The actual Linux managed-model runtime probe is the relevant Free compute gate.
Managed training originally exceeded the 25-minute job budget; bounded 250-entity
corridor PIT batches subsequently passed without changing entities or labels.

## Security, setup and recurring cost

Expected recurring hosting cost is **$0/month within existing Free quotas**.
No card, paid plan, promotional credit, paid disk/worker or automatic upgrade is
required by this implementation. Do not add payment methods or enable billed
usage. Quota exhaustion can suspend service; storage cannot grow indefinitely.

GitHub environment `free-cloud` holds CLOUD_DATABASE_URL, CLOUD_REDIS_URL,
CLOUD_KAFKA_BROKER, CLOUD_KAFKA_CA_PEM, CLOUD_KAFKA_CERT_PEM,
CLOUD_KAFKA_KEY_PEM, CLOUD_POSTGRES_CA_PEM, CLOUD_MLFLOW_TRACKING_URI,
CLOUD_MLFLOW_USERNAME, CLOUD_MLFLOW_TOKEN and CLOUD_AGENT_ACCESS_KEY.
CLOUD_VALKEY_CA_PEM is optional for custom roots; Aiven uses public trust here.
Public URLs are environment variables CLOUD_FASTAPI_URL/CLOUD_STREAMLIT_URL.
An extra owner-created GROQ_API_KEY GitHub secret is unused by these workflows;
only Render needs the provider credential. No secret value is recorded here.

Render uses PostgreSQL verify-full plus its CA secret file, Valkey rediss with
hostname verification, DagsHub tracking credentials, GROQ_API_KEY and the shared
AGENT_API_KEY. Kafka private keys are not sent to Render. Streamlit uses FASTAPI_URL
and the same shared AGENT_API_KEY; it does not need database/provider credentials.
The CLI helper uploads via stdin and does not write credentials to `.env`.

Existing Aiven Free services support authenticated public TLS rather than VPC
networking. Preserve certificate/hostname validation; do not expose unauthenticated
ports. Topics trip.events and trip.events.deadletter each have two partitions;
observed replication factor two and provider retention are unchanged. Defaultdb
is retained alongside logistics. Production managed PostgreSQL 18.6 differs from
local Compose 16 and has passed actual migrations/Feast queries here.

Setup, certificate retrieval, exact commands, connection grammar and Free-plan
limits are in [FREE_CLOUD_SETUP.md](FREE_CLOUD_SETUP.md) and
[FREE_TIER_RESEARCH.md](FREE_TIER_RESEARCH.md). Render can sleep after 15 minutes,
Community Cloud after inactivity, Kafka has finite retention/throughput, and the
GitHub scheduler is best effort. Continuous real-time trip processing is not
claimed. GitHub public standard runners replace paid always-on workers.

## Outstanding acceptance work

1. Verify public Streamlit has loaded the pushed source and repeat demand→ETA
   rerun checks. Owner has rebooted it; current public labels are still stale.
2. Wait for Render to deploy 0bcad87, then repeat scoped verify-agent and confirm
   genuine managed tool output in the public dashboard; keys are present but this
   is not yet successful tool execution.
3. Review/integrate through protected branches, explicitly enable scheduling and
   capture an actual scheduled-event run. Until then manually refresh before the
   online feature TTL expires; manual pipeline history does not prove automation.
4. Perform a controlled Render idle sleep/wake request and capture recovery timing.

## Complete change inventory

Relative to dev; the owner's .devcontainer addition is identified separately.
The pre-existing Oracle retry script edit remains untouched and excluded.

81 changed/new files (including the owner-provided dev container):

- `.devcontainer/devcontainer.json` (owner-provided)
- `.dockerignore`
- `.env.example`
- `.github/workflows/ci.yml`
- `.github/workflows/deploy.yml`
- `.github/workflows/etl_live_smoke.yml`
- `.github/workflows/free-cloud.yml`
- `.gitignore`
- `.streamlit/config.toml`
- `README.md`
- `alembic/versions/2026_10_17_0004_cloud_state.py`
- `docs/Architecture.md`
- `docs/DEPLOYMENT_LOG.md`
- `docs/DEPLOYMENT_REPORT.md`
- `docs/Decisions.md`
- `docs/Deployment.md`
- `docs/FREE_CLOUD_SETUP.md`
- `docs/FREE_TIER_RESEARCH.md`
- `docs/aiven-bootstrap-evidence.json`
- `docs/aiven-initialization-evidence.json`
- `docs/cloud-verification.json`
- `docs/corrected-monitoring-evidence.json`
- `docs/evidence/streamlit-demand-before-fixes.png`
- `docs/evidence/streamlit-public-demand.png`
- `docs/free-profile-e2e-evidence.json`
- `docs/managed-monitoring-evidence.json`
- `docs/managed-runtime-evidence.json`
- `docs/managed-runtime-failure-evidence.json`
- `docs/managed-training-evidence.json`
- `docs/mlflow-verification-evidence.json`
- `docs/public-redeploy-evidence.json`
- `docs/public-render-evidence.json`
- `docs/streamlit-acceptance-evidence.json`
- `pyproject.toml`
- `render.yaml`
- `scripts/cloud_preflight.py`
- `scripts/configure_cloud_secrets.py`
- `scripts/diagnose_cloud_database.py`
- `scripts/probe_cloud_memory.py`
- `scripts/probe_managed_runtime.py`
- `scripts/validate_cloud_config.py`
- `scripts/verify_aiven_services.py`
- `scripts/verify_cloud_deployment.py`
- `scripts/verify_free_profile_e2e.py`
- `src/agents/graph.py`
- `src/agents/providers.py`
- `src/common/config.py`
- `src/common/db.py`
- `src/common/kafka_utils.py`
- `src/extract/authoritative_zones.py`
- `src/extract/live_feed_producers.py`
- `src/features/client.py`
- `src/features/config.py`
- `src/features/offline_extractor.py`
- `src/monitoring/analyzers.py`
- `src/monitoring/schemas.py`
- `src/monitoring/service.py`
- `src/orchestration/cloud_resources.py`
- `src/orchestration/flows/free_tier_pipeline.py`
- `src/orchestration/free_tier_jobs.py`
- `src/serving/Dockerfile.cloud`
- `src/serving/app.py`
- `src/serving/cloud_policy.py`
- `src/serving/feature_extractor.py`
- `src/serving/model_loader.py`
- `src/serving/schemas.py`
- `src/training/dataset.py`
- `src/training/pipeline.py`
- `src/transform/stream_consumer.py`
- `tests/test_agent_graph.py`
- `tests/test_cloud_database_diagnostics.py`
- `tests/test_cloud_deployment.py`
- `tests/test_cloud_resources.py`
- `tests/test_cloud_secret_setup.py`
- `tests/test_monitoring_analyzers.py`
- `tests/test_stream_consumer.py`
- `tests/test_training_dataset.py`
- `tests/test_ui_forecasts.py`
- `ui/app.py`
- `ui/requirements.txt`
- `uv.lock`
