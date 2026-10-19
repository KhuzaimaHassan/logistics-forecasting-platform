# Deployment report — 2026-10-19

## Outcome

**The platform is not publicly deployed yet.** No verified public application URL
exists. Aiven Kafka, PostgreSQL 18.6 and Valkey are running on existing Free plans.
Actual managed bootstrap persisted 9,232 authentic TLC trips and materialized
Feast online features into Valkey. DagsHub metric/artifact readback and registry
listing are verified. Managed training exceeded its 25-minute job budget; the
constrained API check then failed readiness because Production models were absent. Public hosting and live LLM verification remain
outstanding. No paid resource, trial, automatic upgrade or data deletion was used.

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

## Final selected architecture and trade-offs

Aiven Free Kafka + PostgreSQL + Valkey; DagsHub Individual MLflow; Render Free
FastAPI; Streamlit Community Cloud UI; standard public-repository GitHub Actions
executing Prefect OSS flows; optional Free Groq/Gemini agent. See
[provider evidence](FREE_TIER_RESEARCH.md) and
[complete setup/secrets/commands](FREE_CLOUD_SETUP.md).

The authentic bounded profile passed end to end on disposable CI services.
This deployment remains conditional on account eligibility
and live resource proof. Render full-model Linux memory remains an acceptance
gate. It has no continuous trip worker/SLA, and source trips are an explicitly
labelled historical sample. Kafka processing is real but scheduled. Finite free
storage stops at a safety threshold instead of deleting records or upgrading.
All original local/full-capacity technology remains available.

## Verification matrix

Only actual check evidence may promote a row. A unit mock, SQLite fixture, old
CI result, imported module or HTTP shell is not managed integration evidence.

| Service/capability | Status | Evidence or blocker |
|---|---|---|
| Managed Kafka produce + consume | **VERIFIED** | [Aiven run 39120936927](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/actions/runs/39120936927): authentic TLC event produced/consumed at partition 1, offset 0, with mutual TLS; both topics created with 2 partitions/RF 2 |
| Managed PostgreSQL write/read | **VERIFIED** | PostgreSQL 18.6/logistics TLS write/read plus durable application migrations and 9,232 warehouse trips in [managed bootstrap evidence](aiven-bootstrap-evidence.json); defaultdb preserved |
| Managed Valkey TLS write/read | **VERIFIED** | TLS PING/SET NX/GET and actual Feast retrieval matched PostgreSQL; public trust and hostname/certificate verification enabled |
| Feast materialization + Valkey retrieval | **VERIFIED** | [Run 39121607882](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/actions/runs/39121607882): 107,155 zone/8,760 corridor rows; zone 7 online pickups=1 equals PostgreSQL, observed_at=2023-01-31T23:00:00Z |
| DagsHub MLflow metrics/artifacts/registry | **VERIFIED** | [Run 39138839993](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/actions/runs/39138839993): metric 1.0 read back, artifact SHA-256 matched, registry list accessible; model-version promotion verified separately |
| Scheduled cloud pipeline output | **VERIFIED** | Genuine managed pipeline manually invoked: checkpoint advanced 600 -> 9,232, remaining_sample_events=0, official centroids; this does not prove cron execution |
| Managed training/promotion | **FAILED** | [Run 39139455829](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/actions/runs/39139455829) exceeded 25-minute budget (Actions conclusion: cancelled). No successful managed promotion claimed; smaller corridor batches and timing logs prepared for retry |
| Managed-model constrained API probe | **FAILED** | [Run 39139567159](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/actions/runs/39139567159) failed readiness with unavailable Production models; [sanitized readiness result](managed-runtime-failure-evidence.json); this is not evidence of a Render memory failure |
| Actual cron execution | **NOT VERIFIED** | Requires scheduled-event Actions run, not a manual invocation |
| Public FastAPI real-model demand/ETA | **NOT VERIFIED** | BLOCKED: Render service and genuinely trained promoted artifacts absent |
| Streamlit actual forecast rendering | **NOT VERIFIED** | BLOCKED: Community Cloud OAuth/app and API absent |
| Live LangGraph LLM + dependencies | **NOT VERIFIED** | BLOCKED: Free provider key/hosted dependencies absent |
| Persisted managed monitoring report | **NOT VERIFIED** | Managed warehouse exists; actual champion/backtest/HTML verification pending |
| Restart/cold-start behavior | **NOT VERIFIED** | Requires actual hosted restart/sleep and repeated predictions |
| Offline cloud/consumer regression checks | **VERIFIED** | 20 tests passed on local Python 3.11; scope is hermetic contracts only |
| Cloud API import memory | **VERIFIED** | Windows process RSS 204.8 MiB; full MLflow pyfunc not imported; not Linux full-model peak proof |
| Full revised unit/contract suite | **VERIFIED** | [Run 39228738178](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/actions/runs/39228738178), commit 373c53b: 376 passed, 1 skipped, 45 warnings in 82.99s; all three CI jobs passed |
| Compose build/integration | **VERIFIED** | Corrected [full CI run](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/actions/runs/39228738178) passed Docker builds and real disposable-service integration; fixture tests are not managed-cloud proof |
| Linux free-runtime memory gate | **VERIFIED** | [Memory job](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/actions/runs/39044393912/job/117430124274) measured 296.8 MiB peak inside 512 MiB; includes API/agent imports and fixture-trained Booster, not hosted production models |
| Authentic bounded profile on CI services | **VERIFIED** | [Run 39046508012](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/actions/runs/39046508012), commit 62717dd; actual TLC -> Kafka -> PostgreSQL -> Feast/Redis -> MLflow -> FastAPI -> persisted reports; [sanitized evidence](free-profile-e2e-evidence.json) |
| ARM64 image builds | **VERIFIED** | [Run 39228731042](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/actions/runs/39228731042), commit 373c53b, succeeded |

### Authentic-data results and limitations

The manual CI run published/consumed and persisted **9,232 valid trips** from
10,000 deterministically sampled source rows (768 rejected by validation). It
created 107,155 zone and 8,760 corridor feature rows using official zone centroids.
Training completed in 15.31 seconds after feature construction. The existing
promotion gates accepted both genuine models: demand MAE 0.0493 versus baseline
0.0540; ETA MAE 430.30 seconds versus baseline 666.49 seconds. These are sparse
historical-sample holdout results, not full-population or live benchmarks.

FastAPI readiness confirmed PostgreSQL, Redis and MLflow; both model versions
were Production version 1 with `is_fallback=false`. Demand returned 0.71 pickups
for zone 7; ETA returned 1,310.7 seconds for corridor 244_262. Both identified
`data_as_of=2023-01-31T23:00:00+00:00`, and two predictions were persisted.
Monitoring persisted three HTML reports and **raised drift/performance alerts**;
its status was `completed_with_alerts`, with automatic retraining disabled. It is
a historical backtest, not live drift evidence. Disposable CI artifacts and model
registrations are not production storage and must be rebuilt on managed services.

The first authentic run failed because the initial migrations lacked the Feast
schema; the explicit schema initialization fixed it. A subsequent superseded run
was cancelled while diagnosing costly point-in-time joins. The cloud profile
uses a one-hour zone snapshot TTL and bulk PostgreSQL feature upserts; the local
feature definition remains unchanged. Corrected corridor training no longer
fills missing history using the future target. No failed promotion gate was bypassed.

### Aiven handoff update — 2026-10-18

- Owner screenshot: `pg-11e9a1b` (Free-1-1gb), `valkey-21c27f8`
  (Free-1), `kafka-1b938f` (Free-0), all Running. PostgreSQL/Valkey show
  DigitalOcean `blr`, India. This verifies the supplied Console evidence only.
- GitHub authenticated account has repository admin permission and `repo`/`workflow`
  token scopes. Created the `free-cloud` environment successfully. Initial secret-name
  listing returned no configured names. Subsequently the owner supplied local
  certificate files: all four PEM secrets were uploaded directly through stdin
  and their names confirmed. Local validity/issuer/key-match checks passed.
  The owner subsequently configured all three connection secrets; all seven names
  are confirmed present. No token, connection value or PEM value recorded.
- Added an interactive stdin-only helper and manual `verify-aiven` workflow path.
  Automatic jobs remain opt-in; this check needs no DagsHub account and creates
  no services/topics. It preserves existing data and uses only bounded test writes.
- Corrected Valkey TLS configuration: public system roots by default, optional
  custom CA, certificate/hostname verification always required. Kafka/PG CAs
  remain required. Render no longer points to a nonexistent Valkey CA file.
- Corrected managed run 39120936927 passed preflight and actual service tests.
  [Sanitized evidence](aiven-initialization-evidence.json) records PostgreSQL
  18.6/logistics, Valkey TLS write/read and an exact-offset real Kafka roundtrip.
  Both topics were created with two partitions and observed replication two;
  defaultdb and provider retention were preserved. Subsequent schema/Feast/bootstrap
  checks passed as recorded below. Public deployment is still NOT VERIFIED.

### Managed bootstrap and DagsHub evidence

The earlier managed run completed safely at 600 trips after its time budget.
The optimized run resumed at that durable checkpoint, published the remaining
8,632 events, and finished at exactly 9,232 unique warehouse rows. It produced
107,155 zone and 8,760 corridor feature rows. Actual Feast online retrieval
returned zone 7's one pickup for the last hour, matching PostgreSQL and preserving
the historical timestamp. Database size was 36,312,767 bytes (about 34.6 MiB),
below the 700 MiB safety threshold. PostgreSQL 18.6 migrations/Feast registry and
managed Valkey compatibility are therefore verified for this exercised path.
See [complete bootstrap output](aiven-bootstrap-evidence.json).

DagsHub run `40cb7a97e0ac4ed9ad7a8540113e5617` persisted/read metric 1.0 and an
artifact whose downloaded SHA-256 matched the upload. Registry listing passed.
The scoped check explicitly retains automated_checks_complete=false and
manual_acceptance_complete=false; it cannot certify the platform. See
[complete MLflow evidence](mlflow-verification-evidence.json).

The prior authorization-service usage blocker is resolved. Existing jobs
continued successfully; no failed or cancelled run is described as passed.
Training, monitoring and real-model container results are recorded only after
actual output is inspected. Public API/UI, live LLM and hosted cold starts remain
NOT VERIFIED.

## Infrastructure, security and cost

The Free manifest creates one Free web service only. No paid disk/worker/database
is declared. Credentials live in hosting secret UIs and GitHub encrypted secrets;
CA paths and URI grammar are documented in the runbook. Aiven Free requires
authenticated TLS public endpoints because VPCs are unavailable. Owner review of
dynamic runner/hosting egress policy remains necessary. API role does not need
Kafka credentials or schema administration. No `.env` values are reported here.

Expected recurring hosting cost: **$0/month within Free quotas and availability**.
Use no-card Free accounts; do not enable billed usage. Render overages can bill
if a payment method is added, so no-card suspension is part of this cost boundary.
Free quotas can suspend the project, and free storage cannot retain unlimited
history. Aiven signup/provisioning from Pakistan is now owner-confirmed (Console screenshot);
PostgreSQL/Valkey Bangalore placement is visible, Kafka region and other providers
remain unverified;
Gemini's official country list explicitly includes Pakistan.

## Exact unresolved owner actions

1. Aiven connection fields/certificates, database and topics are configured and
   verified; retain these existing Free services. DagsHub is configured and verified.
   Prepare remaining hosting Free accounts as documented, without paid trials/cards.
2. Add optional agent/provider keys and hosting variables, Render secret files/environment
   and Community Cloud API/access-key secrets. Do not send credentials in chat.
3. Review and integrate the deployment branch through the existing protected
   branch process after engineering checks pass. Bootstrap is already complete.
   Training/promotion, monitoring and managed runtime checks are agent work; no
   further owner credentials are required for those checks.
4. Verify browser-rendered predictions and genuine cold-start/restart persistence;
   update this matrix with sanitized output and actual URLs only afterward.

No cloud URL or successful deployment is fabricated to fill these blockers.

Managed training retry [39228745442](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/actions/runs/39228745442)
uses commit 373c53b and passed configuration preflight. Its outcome is pending;
this does not supersede the recorded failed acceptance checks.

## Change inventory

The inventory below covers the deployment branch relative to dev.
The pre-existing Oracle retry script edit is preserved and excluded from this
deployment change set.


### Deployment file inventory

65 changed or new deployment files:

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
- `docs/free-profile-e2e-evidence.json`
- `docs/mlflow-verification-evidence.json`
- `docs/managed-runtime-failure-evidence.json`
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
- `src/agents/providers.py`
- `src/common/config.py`
- `src/common/db.py`
- `src/common/kafka_utils.py`
- `src/extract/authoritative_zones.py`
- `src/extract/live_feed_producers.py`
- `src/features/client.py`
- `src/features/config.py`
- `src/features/offline_extractor.py`
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
- `tests/test_cloud_deployment.py`
- `tests/test_cloud_database_diagnostics.py`
- `tests/test_cloud_resources.py`
- `tests/test_cloud_secret_setup.py`
- `tests/test_stream_consumer.py`
- `tests/test_training_dataset.py`
- `ui/app.py`
- `ui/requirements.txt`
- `uv.lock`
