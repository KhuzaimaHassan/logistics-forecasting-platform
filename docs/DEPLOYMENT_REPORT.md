# Deployment report — 2026-10-21

## Outcome

**Responsive review 2026-10-27:** reproduced cramped tablet action columns and
truncated monitoring alerts on the public UI. Candidate styling was measured at
seven sizes320–1440px with no page-level horizontal overflow. Real API demand,
ETA, map, pipeline and monitoring views passed local-preview browser checks;
one real Groq pipeline query passed on the existing public app. After owner reboot,
public viewport/monitoring/demand/map/ETA checks passed. A final collapsed-sidebar
default addresses an overlay reopening after the mobile ETA rerun; that last
deployed behavior remains pending retest. See
the [responsive review](RESPONSIVE_REVIEW_2026-10-27.md) for screenshots and limits.

**Latest browser review2026-10-22: core UI paths passed after owner reboot.**
Demand/map/ETA, registry, pipeline history, actual drift HTML and
Groq queries against PostgreSQL/Feast worked. An original shortcut requested
unobserved zone161 and returned503; corrected zone74/corridor75_237 buttons are
now publicly visible and passed real Groq/Feast retests. First-load90s timeout
recovered on rerun. RAM stability and file-download completion remain unverified;
documentation answer relevance is limited. The model's29.9%
historical MAE decay alert remains genuine. See the
[full browser verification matrix and evidence](STREAMLIT_ACCEPTANCE_2026-10-22.md).

**Open operational issue, 2026-10-21:** the owner supplied an Aiven PostgreSQL
out-of-memory interruption warning and a RAM graph rising to about95% before
dropping near03:00UTC. Cause and sustained recovery are **NOT VERIFIED**.
Historical acceptance below does not certify current memory stability. New
read-only managed diagnostics succeeded: database64,829,119 bytes (61.83MiB),
work_mem2MiB, no other sessions at collection. No data deletion or upgrade.
See the [memory investigation](POSTGRES_MEMORY_RUNBOOK.md) and
[sanitized live result](postgres-memory-diagnostics-evidence.json).

**The bounded free-cloud profile has completed its required acceptance checks:**
public dashboard, real-model API, Groq tools, nine managed checks, application
recovery after a measured idle interval, and a genuine scheduled pipeline.
Evidence spans the recorded verification dates; this is not a concurrent health
snapshot or an uptime guarantee. These are actual deployed URLs:

- [Streamlit dashboard](https://logistics-forecasting-platform.streamlit.app/)
- [FastAPI documentation](https://logistics-forecasting-platform.onrender.com/docs)

Browser verification displayed genuine demand for zones 74/75/166
(3.69/5.55/2.26 pickups), an official-centroid map and corridor 75_237 ETA
11.9 minutes (715.8 seconds). Predictions describe a bounded January 2023 TLC
sample; they are not current NYC telemetry or full-population forecasts.

Aiven ingestion, PostgreSQL persistence, Feast/Valkey, DagsHub tracking/artifacts,
real model training, public forecasts and durable monitoring HTML are verified.
Acceptance exposed defects missed by prior tests. Corrected monitoring has been
rerun and verified. Public forecast persistence, correct historical labels and
monitoring metrics are now verified after the owner reboot. A corrected real Groq
call on GitHub Actions read nine Aiven PostgreSQL pipeline records. The public Render agent subsequently passed the same real-provider check and
retrieved actual Feast features through Streamlit. No agent credential blocker remains.

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
| Copilot | LangGraph with Groq Free, authenticated API/tool allowlist | Real Groq calls and PostgreSQL/Feast tools verified; Free rate limits apply |
| Monitoring | Evidently historical sample backtests persisted in PostgreSQL | Historical analysis, not live prediction drift; automatic retraining disabled |

The complete multi-process stack cannot fit a single free 512 MB instance.
Original local Compose and full-capacity implementations remain available.
See [official provider limits](FREE_TIER_RESEARCH.md) and
[exact configuration, secrets and commands](FREE_CLOUD_SETUP.md).

## Verification matrix

Resource-saving follow-up2026-10-22 (ADR-030): monitoring now checks idempotency
before historical Feast extraction; runtime/dependency fingerprints avoid
docs-only re-evaluation. Five focused local checks passed. Source CI39753862820
on4c169fa passed402 tests,1 skipped,45 warnings (57.66s). No full managed training,
feature-table rebuild or stress run was launched for this follow-up.

Only genuine observations below certify deployed behavior. Disposable CI services
and mocked unit tests do not certify Aiven, DagsHub or public applications.

| Service/capability | Status | Actual evidence / remaining condition |
|---|---|---|
| Bounded managed PIT query probe | **VERIFIED** | [39753857626](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/actions/runs/39753857626),4c169fa: four zone and four corridor rows matched real PostgreSQL values/timestamps. Read-only,8s statement/2s lock timeout,2MiB work_mem,no parallel gather. Read phases5.68s/2.35s. [Evidence](query-budget-evidence.json); not a full training benchmark |
| Resource follow-up public API | **VERIFIED** | [39754119428](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/actions/runs/39754119428): demand0.71 and ETA715.8s, Productionv1, two new durable prediction rows matched. [Scoped evidence](resource-api-evidence.json); other acceptance scopes were not rerun |
| Resource follow-up database health | **VERIFIED** | [39754124002](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/actions/runs/39754124002):70,407,871 bytes (67.15MiB), two backends including diagnostic, zero recorded deadlocks. [Snapshot](postgres-resource-health-evidence.json). Not host RAM telemetry or sustained OOM recovery |
| PostgreSQL OOM cause / sustained recovery | **NOT VERIFIED** | Owner warning and RAM graph establish an incident; read-only run39578655289 succeeded but does not identify the killed process. Provider logs around03:00–03:15UTC required |
| Managed Kafka produce/consume | **VERIFIED** | [Aggregate 39272455345](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/actions/runs/39272455345): partition 1 produced/consumed offset 4672; authentic TLC trip, mutual TLS |
| Managed PostgreSQL write/read | **VERIFIED** | Same aggregate wrote/read a retained verification record; prior bootstrap persisted 9,232 authentic warehouse trips; actual PostgreSQL 18.6 |
| Valkey TLS + Feast materialization/retrieval | **VERIFIED** | Same aggregate rematerialized real features; zone 7 pickups=1 matched PostgreSQL with historical event timestamp |
| MLflow metrics/artifacts/registry | **VERIFIED** | Same aggregate read metric 1.0, verified downloaded artifact SHA-256 and registry listing; real training/promotion evidence below |
| Real pipeline output | **VERIFIED** | Initial managed pipeline processed 8,632 remaining events after checkpoint 600, ending at 9,232 unique rows; latest main run 39401271976 succeeded with 9,232 retained trips, zero new sample publications and 1,319 exact keys renewed to 86,400s from minimum 19,279s. Manual workflow_dispatch is not cron evidence |
| Actual scheduled trigger | **VERIFIED** | [39482347476](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/actions/runs/39482347476), event=schedule, main 8a9b4d0, success. Started 2026-10-21 00:18:36 Pakistan, 4h1m36s after intended 20:17 slot; actual pipeline step retained 9,232 trips and renewed 1,319 keys. [Evidence](scheduled-pipeline-evidence.json). Punctuality and separate train/monitor cron executions are not certified |
| Protected default-branch release | **VERIFIED** | PR 166 merged to dev; fully green PR 167 squash-merged to main as required by linear history, release 4450d95; no administrator bypass, branch/data deletion or history rewrite. [Evidence](main-release-evidence.json) |
| Real training/promotion | **VERIFIED** | [39228745442](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/actions/runs/39228745442): 635.55s managed run, both genuine LightGBM Production v1 models reloaded natively; [evidence](managed-training-evidence.json) |
| Public FastAPI genuine demand/ETA | **VERIFIED** | Aggregate produced demand 0.71 and ETA 715.8s, matched two newly persisted Aiven prediction records; no fallback |
| Streamlit forecast table/map/ETA | **VERIFIED** | Browser actions displayed real 74/75/166 values 3.69/5.55/2.26, map, and 75_237 ETA 11.9m; [screenshot](evidence/streamlit-public-demand.png) |
| Public pipeline/report UI | **VERIFIED** | Browser showed real pipeline history, monitoring cards and actual embedded Evidently performance HTML |
| UI fixes deployed | **VERIFIED** | Corrected header/tab, MAE and drift labels displayed after owner reboot; demand table/map remained after actual ETA request. [Evidence](streamlit-acceptance-evidence.json) and [screenshot](evidence/streamlit-corrected-demand.png) |
| Live LLM + managed tools on CI | **VERIFIED** | [39268227159](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/actions/runs/39268227159) diagnostic used real Groq gpt-oss-20b, executed query_pipeline_status with valid empty arguments and read nine Aiven records; [evidence](agent-diagnostics-evidence.json). Workflow overall failed its separate public endpoint check against older source |
| Public Render LLM tool execution | **VERIFIED** | Full aggregate 39272455345 confirmed real Groq gpt-oss-20b and query_pipeline_status with warehouse.pipeline_runs citation; browser showed ten actual records and a separate genuine Feast zone 74 tool response. [Evidence](public-agent-evidence.json) |
| Corrected monitoring decisions | **VERIFIED** | [39242531954](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/actions/runs/39242531954): authoritative Evidently flags match summaries; actual public readback checks per-column count and distance direction; [evidence](corrected-monitoring-evidence.json) |
| Monitoring HTML persistence | **VERIFIED** | Latest performance report read back 3,701,744 HTML characters; genuine historical backtest |
| Hosted redeploy recovery | **VERIFIED** | Render 92bddd7 initially returned readiness 503 with no loaded models; a real request recovered through lazy Production loading in 31.55s, then readiness and demand/ETA passed. Same genuine v1 artifacts; [failure + recovery evidence](public-redeploy-evidence.json) |
| Recovery after controlled idle interval | **VERIFIED** | No agent API requests for 973.664s; first demand HTTP 200 in 140.968s, X-Cache MISS, ETA MISS 200, readiness 200. Both real Production v1 models reloaded after the interval on unchanged aefb096; all 9,232 rows retained. Fresh dashboard demand/ETA passed after retry; main API check 39402814242 matched two newly persisted prediction rows. [Evidence](controlled-idle-recovery-evidence.json). Provider-internal sleep events and other public traffic remain unobserved |
| Render no-payment-method billing condition | **VERIFIED** | Owner explicitly confirms card removed and Free compute retained; private Billing page not independently inspected. Official no-card overages suspend service/builds. Earlier attached-card exposure retained in evidence; no charge observed |
| Genuine-model Free runtime limits | **VERIFIED** | [39230564684](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/actions/runs/39230564684): two fresh 512 MiB/0.1 CPU containers, concurrent 145-zone batches and durable predictions; peak process RSS 386.7 MiB |
| Full CI including Compose | **VERIFIED** | Protected-release [39400299729](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/actions/runs/39400299729), b2e56d4: all three jobs passed; 398 tests passed, one skipped, 45 warnings in 66.56s. Feature-head CI 39282433306 also passed (76.39s). Real disposable-service Compose integrations remain separate from managed infrastructure checks |
| ARM64 images | **VERIFIED** | Protected-release [39400243947](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/actions/runs/39400243947), b2e56d4, all jobs passed; feature aefb096 ARM64 run 39282424920 also passed |

[Latest aggregate JSON](cloud-verification.json) records all nine automated checks
as verified while preserving manual acceptance=false. Browser/redeploy evidence is separate: an automated
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
Groq also rejected an unnecessary required=[] clause after normalizing an empty
properties map; removing that clause passed the actual provider. Returned tool
dependency errors and cloud fallback states also propagate as errors. An execution
warning must not be presented as success. See ADR-029 and the
[deployment log](DEPLOYMENT_LOG.md) for actual failed/cancelled runs and repairs.

### Unchanged historical snapshots and Redis retention

Acceptance after a later restart found that real online fields had disappeared.
Both Render and the runner returned missing fields, so this was not a fabricated
UI response or a Render-only connection failure. Feast 0.65 skips writes for equal
event timestamps before renewing its Redis TTL; the original keys expired despite
subsequent rematerialization. The cloud flow now renews EXPIRE on only the exact
bounded sample entity keys generated by the pinned Feast codec after successful
materialization. Values and historical timestamps remain intact. Missing keys fail
before a successful refresh timestamp is recorded; no unrelated key scan/deletion
or unlimited retention is introduced.

Managed aggregate 39268160345 restored real features and public forecasts with
matching new PostgreSQL prediction rows. The browser repeated demand and ETA
successfully. Repeated run 39271963729 renewed 1,319 keys to 86,400 seconds from a prior
minimum TTL of 84,727 seconds, with no duplicate publication and all 9,232 trips
unchanged. Full aggregate 39272455345 repeated renewal from a prior minimum
86,121 seconds and again matched Feast to PostgreSQL.
[Actual retention evidence](retention-verification-evidence.json).

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

Owner confirms Render remains on Free compute and **the attached card has now
been removed**. Groq has no payment method. This resolves the known attached-card
overage condition based on owner confirmation; the private Billing page was not
independently inspected. Under [official Render Free limits](https://render.com/docs/free),
no-payment-method quota exhaustion suspends affected services or disables builds
rather than purchasing overages. Keep payment methods absent and plans Free.
No actual overage charge has been observed or claimed. Prior attached-card
exposure and the subsequent removal are retained in the acceptance evidence.
No card, paid plan, promotional credit, paid disk/worker or automatic upgrade is
required by this implementation. Do not add payment methods or enable billed
usage. Quota exhaustion can suspend service; storage cannot grow indefinitely.

GitHub environment `free-cloud` holds CLOUD_DATABASE_URL, CLOUD_REDIS_URL,
CLOUD_KAFKA_BROKER, CLOUD_KAFKA_CA_PEM, CLOUD_KAFKA_CERT_PEM,
CLOUD_KAFKA_KEY_PEM, CLOUD_POSTGRES_CA_PEM, CLOUD_MLFLOW_TRACKING_URI,
CLOUD_MLFLOW_USERNAME, CLOUD_MLFLOW_TOKEN and CLOUD_AGENT_ACCESS_KEY.
CLOUD_VALKEY_CA_PEM is optional for custom roots; Aiven uses public trust here.
Public URLs are environment variables CLOUD_FASTAPI_URL/CLOUD_STREAMLIT_URL.
An owner-created GROQ_API_KEY GitHub secret is optional for failure diagnostics.
Render needs the provider credential; the opt-in diagnostic uses it on a runner
only after a failed agent check. Normal public-agent verification uses the shared
access key only. No secret value is recorded here.

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

## Scheduled pipeline acceptance — 2026-10-21

Actual run 39482347476 has event=schedule, conclusion=success, and main 8a9b4d0.
Its logged CRON value is `17 3,15 * * *`. It was created at 2026-10-21 00:18:36
Pakistan and completed around 00:22:50, after the intended 2026-10-20 20:17 slot.
The observed creation delay was 14,496 seconds (4h1m36s); its internal cause is not
established. The earlier 22:34 no-run observation was correct at that time and
is retained rather than rewritten.

The actual pipeline step succeeded: warehouse 9,232 trips, zone features 107,155,
corridor features 8,760, source cursor 9,232, zero new sample publications. Actual
materialization at 2026-10-20T19:22:43.403031UTC renewed 1,319 known online keys to
86,400 seconds from a prior minimum 51,222 seconds, without changing values/event
timestamps. Zero publications reflect the already-completed authentic sample;
this run does not claim new Kafka events. Genuine broker roundtrip and ingestion
are certified by their separate managed checks above.

[Sanitized scheduled-run evidence](scheduled-pipeline-evidence.json) contains
GitHub run/job/step metadata and the actual result extracted from its log.
[Acceptance rollup](main-release-evidence.json) combines this with earlier
managed, browser, recovery and owner billing evidence. Original automated
artifacts retain their original scope and manual_acceptance=false fields;
they were not rewritten to fabricate later proof.

## Scheduled-event follow-up — 2026-10-20 evening

At 22:34 Pakistan time, workflow-specific and repository-wide GitHub APIs
showed zero schedule-event runs for free-cloud.yml after its intended 20:17 slot.
Workflow is active on default main, Actions is enabled, opt-in is true, and the
published cron/blob matches the verified configuration. Repository is neither
archived nor disabled. No configuration blocker was identified.

At that observation, **cron acceptance was NOT VERIFIED**. No execution record existed to classify
as a pipeline success or failure. [GitHub documents possible delays/dropped events](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule),
but the cause here is not established. GitHub status reports Actions operational.
The next intended slot recorded then was 2026-10-21 08:17 Pakistan. The earlier
event subsequently arrived late and is verified above. No cron edits, workflow toggles, manual substitution or
Codex automation were used. [Exact observation](main-release-evidence.json).

## Optional verification and ongoing constraints

The core acceptance checklist is complete. A real scheduled pipeline now has
execution and managed-output evidence; no manual dispatch is substituted.

1. GitHub cron is best effort. The first verified run arrived 4h1m36s late;
   subsequent timings, training/monitoring cron executions and future uptime
   are not certified by this observation. Feature refresh must stay within its
   24-hour retention. Existing manual commands remain recovery options, not proof
   that automatic scheduling occurred.
2. Optional Gemini fallback and deployed FAISS index freshness are not verified.
   The exercised Copilot uses Groq with actual PostgreSQL and Feast tools; this
   does not certify every optional provider/retrieval path.

## Protected release and refresh — 2026-10-20

PR 166 merged into dev after all checks passed. PR 167 passed full CI and ARM64,
then squash-merged to main 4450d95 under the required linear-history rule. The
public aefb096 file tree exactly matches that release. The hosting feature branch
is retained. Post-merge CI 39401266610 passed and legacy VM deploy 39401266605 was
skipped; no paid or self-hosted deployment was started.

Manual main-branch pipeline 39401271976 passed, preserving 9,232 warehouse trips,
107,155 zone-feature rows and 8,760 corridor-feature rows, with zero new sample
publications. Actual materialization at 2026-10-20T09:36:24.899356 UTC renewed 1,319
known keys to 86,400 seconds (prior minimum 19,279 seconds). Historical event times
and feature values were unchanged. [Actual metadata/output](main-release-evidence.json).
This is a manual refresh, not a scheduled-event run. The next intended pipeline
slot is 2026-10-20 20:17 Pakistan time; GitHub can delay or drop it. Owner chose to
return afterward, so no Codex follow-up automation was created.

## Idle recovery acceptance — 2026-10-20

Recorded an actual baseline, made no agent API requests for 973.664 seconds, then
requested real demand/ETA again. The first request took 140.968 seconds, returned
HTTP 200 and X-Cache MISS. Both native Production v1 models have new load timestamps
after that interval; deployed revision remained aefb096 and artifact run IDs stayed
unchanged. ETA was also a cache miss, readiness returned 200, and warehouse row
count remained 9,232. No restart/redeploy was forced and no model fallback was used.
This verifies application recovery after controlled agent inactivity; private
provider sleep/start events and unrelated public requests were not observable.

The browser initially retained an earlier 90-second timeout. After retry, it showed
Serving Online (OK), fresh demand 3.69/5.55/2.26, the map and ETA 715.8 seconds; demand
remained after ETA rerun. Both the initial error screenshot and recovered UI are
preserved in [the evidence](controlled-idle-recovery-evidence.json). A narrow
main-branch API verification 39402814242 independently matched two newly persisted
PostgreSQL prediction rows. Its api_only scope does not overwrite the original
nine-service matrix or certify cron. Slow free-host wakeup can exceed the UI's
90-second request timeout; retry remains necessary in that case.

## Historical acceptance follow-up — 2026-10-19

On 2026-10-19, normal protected merge of PR #166 into dev was refused while the required
Lint & Test check remained pending on 7a389f7. The latest CI log records 398 tests
passed, one skipped and 45 warnings (59.74s); Compose and memory checks succeeded.
GitHub read/cancel/retry endpoints returned inconsistent completion information;
the direct retry API returned HTTP403, already running. No required check was
forged or bypassed. This was resolved on 2026-10-20 by successful CI on aefb096 and the release head.
The required branch path feature -> dev -> main is now integrated. No actual
schedule-triggered execution is claimed before the first post-merge cron slot.

New public recovery evidence and the owner's card attachment/removal confirmations are in
[idle-recovery-evidence.json](idle-recovery-evidence.json). Core acceptance is now complete with the genuine scheduled run above. Recovery
after a measured quiet interval is verified separately; provider-internal sleep
event logs remain unavailable. Owner confirms card removal; the current public service remains on Free.

## Complete change inventory

Relative to pre-deployment dev dca4b70. Owner-provided dev container identified
separately. Current unrelated local .env.example and Oracle retry edits are
excluded from this final commit; the .env.example inventory entry describes only
the previously committed deployment configuration.

103 changed/new deployment files:

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
- `docs/agent-diagnostics-evidence.json`
- `docs/aiven-bootstrap-evidence.json`
- `docs/aiven-initialization-evidence.json`
- `docs/cloud-verification.json`
- `docs/controlled-idle-recovery-evidence.json`
- `docs/corrected-monitoring-evidence.json`
- `docs/evidence/streamlit-after-idle-recovery.png`
- `docs/evidence/streamlit-controlled-idle-recovered.png`
- `docs/evidence/streamlit-corrected-demand.png`
- `docs/evidence/streamlit-corrected-eta.png`
- `docs/evidence/streamlit-corrected-monitoring.png`
- `docs/evidence/streamlit-demand-before-fixes.png`
- `docs/evidence/streamlit-feast-copilot.png`
- `docs/evidence/streamlit-idle-timeout-before-retry.png`
- `docs/evidence/streamlit-live-copilot.png`
- `docs/evidence/streamlit-public-demand.png`
- `docs/free-profile-e2e-evidence.json`
- `docs/idle-recovery-evidence.json`
- `docs/main-release-evidence.json`
- `docs/managed-monitoring-evidence.json`
- `docs/managed-runtime-evidence.json`
- `docs/managed-runtime-failure-evidence.json`
- `docs/managed-training-evidence.json`
- `docs/mlflow-verification-evidence.json`
- `docs/public-agent-evidence.json`
- `docs/public-redeploy-evidence.json`
- `docs/public-render-evidence.json`
- `docs/retention-verification-evidence.json`
- `docs/scheduled-pipeline-evidence.json`
- `docs/streamlit-acceptance-evidence.json`
- `pyproject.toml`
- `render.yaml`
- `scripts/cloud_preflight.py`
- `scripts/configure_cloud_secrets.py`
- `scripts/diagnose_cloud_agent.py`
- `scripts/diagnose_cloud_database.py`
- `scripts/probe_cloud_memory.py`
- `scripts/probe_managed_runtime.py`
- `scripts/validate_cloud_config.py`
- `scripts/verify_aiven_services.py`
- `scripts/verify_cloud_deployment.py`
- `scripts/verify_free_profile_e2e.py`
- `src/agents/graph.py`
- `src/agents/providers.py`
- `src/agents/tools.py`
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
- `src/orchestration/online_retention.py`
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
- `tests/test_agent_serving.py`
- `tests/test_agent_tools.py`
- `tests/test_cloud_agent_diagnostics.py`
- `tests/test_cloud_database_diagnostics.py`
- `tests/test_cloud_deployment.py`
- `tests/test_cloud_resources.py`
- `tests/test_cloud_secret_setup.py`
- `tests/test_monitoring_analyzers.py`
- `tests/test_online_retention.py`
- `tests/test_stream_consumer.py`
- `tests/test_training_dataset.py`
- `tests/test_ui_forecasts.py`
- `ui/app.py`
- `ui/requirements.txt`
- `uv.lock`
