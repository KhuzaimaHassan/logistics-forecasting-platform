# Deployment report — 2026-10-17

## Outcome

**The platform is not publicly deployed yet.** No live application URL exists in
the verified evidence. The owner confirmed there are no existing cloud accounts;
managed resource creation, OAuth and credentials are external blockers.
Implementation proceeds independently and cloud checks remain
**BLOCKED — NOT VERIFIED**. No paid resource, trial, automatic upgrade or deletion
has been requested.

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
| Feast | Feature definitions/PIT logic preserved; managed URL/TLS translation required; native Valkey compatibility needs live verification |
| MLflow | Training/metrics/artifacts/registry preserved on DagsHub; native Booster serving avoids full pyfunc dependency load; live registry/artifact proof pending |
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

This is a working-code deployment candidate, conditional on account eligibility
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
| Managed Kafka produce + consume | **NOT VERIFIED** | BLOCKED: Aiven account, broker, CA/client credentials absent |
| Managed PostgreSQL write/read | **NOT VERIFIED** | BLOCKED: service/user/CA absent |
| Feast materialization + Valkey retrieval | **NOT VERIFIED** | BLOCKED: managed PostgreSQL/Valkey and credentials absent |
| DagsHub MLflow metrics/artifacts/registry | **NOT VERIFIED** | BLOCKED: account/repository/token absent |
| Scheduled cloud pipeline output | **NOT VERIFIED** | BLOCKED: managed stores and workflow activation absent |
| Actual cron execution | **NOT VERIFIED** | Requires scheduled-event Actions run, not a manual invocation |
| Public FastAPI real-model demand/ETA | **NOT VERIFIED** | BLOCKED: Render service and genuinely trained promoted artifacts absent |
| Streamlit actual forecast rendering | **NOT VERIFIED** | BLOCKED: Community Cloud OAuth/app and API absent |
| Live LangGraph LLM + dependencies | **NOT VERIFIED** | BLOCKED: Free provider key/hosted dependencies absent |
| Persisted managed monitoring report | **NOT VERIFIED** | BLOCKED: real champion/data/managed PostgreSQL absent |
| Restart/cold-start behavior | **NOT VERIFIED** | Requires actual hosted restart/sleep and repeated predictions |
| Offline cloud/consumer regression checks | **VERIFIED** | 20 tests passed on local Python 3.11; scope is hermetic contracts only |
| Cloud API import memory | **VERIFIED** | Windows process RSS 204.8 MiB; full MLflow pyfunc not imported; not Linux full-model peak proof |
| Full revised test suite / CI | **NOT VERIFIED** | Broader validation underway; final actual results will be recorded |

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
history. Region/Pakistan signup eligibility is account-specific and NOT VERIFIED;
Gemini's official country list explicitly includes Pakistan.

## Exact unresolved owner actions

1. Create and verify Free accounts/resources from the runbook without paid trials
   or cards; confirm regional/Pakistan eligibility and PostgreSQL 16 availability.
2. Add the enumerated GitHub secrets/variables, Render secret files/environment
   and Community Cloud API/access-key secrets. Do not send credentials in chat.
3. Complete reviewed branch integration, bootstrap, real training/promotion,
   monitoring and managed smoke workflow. Keep failed gates visible.
4. Verify browser-rendered predictions and genuine cold-start/restart persistence;
   update this matrix with sanitized output and actual URLs only afterward.

No cloud URL or successful deployment is fabricated to fill these blockers.

## Change inventory

The final generated inventory and check results are appended after validation.
The pre-existing Oracle retry script edit is preserved and excluded from this
deployment change set.


### Deployment file inventory

- `.dockerignore`
- `.env.example`
- `.github/workflows/ci.yml`
- `.github/workflows/deploy.yml`
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
- `docs/cloud-verification.json`
- `pyproject.toml`
- `render.yaml`
- `scripts/cloud_preflight.py`
- `scripts/validate_cloud_config.py`
- `scripts/verify_cloud_deployment.py`
- `src/agents/providers.py`
- `src/common/config.py`
- `src/common/db.py`
- `src/common/kafka_utils.py`
- `src/extract/authoritative_zones.py`
- `src/extract/live_feed_producers.py`
- `src/features/config.py`
- `src/monitoring/service.py`
- `src/orchestration/flows/free_tier_pipeline.py`
- `src/orchestration/free_tier_jobs.py`
- `src/serving/Dockerfile.cloud`
- `src/serving/app.py`
- `src/serving/cloud_policy.py`
- `src/serving/feature_extractor.py`
- `src/serving/model_loader.py`
- `src/training/pipeline.py`
- `src/transform/stream_consumer.py`
- `tests/test_cloud_deployment.py`
- `tests/test_stream_consumer.py`
- `ui/app.py`
- `ui/requirements.txt`
- `uv.lock`
