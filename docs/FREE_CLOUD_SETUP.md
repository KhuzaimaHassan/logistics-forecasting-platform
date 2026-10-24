# Free cloud setup

**Public dashboard and API are deployed; dated managed-service and browser
checks are recorded in the report. The PostgreSQL memory investigation remains
open; historical acceptance is not an uptime or RAM-stability guarantee.** Read
[DEPLOYMENT_REPORT.md](DEPLOYMENT_REPORT.md) for verification and
[FREE_TIER_RESEARCH.md](FREE_TIER_RESEARCH.md) for current official offers.

**Provisioning update (2026-10-18):** the owner successfully provisioned Aiven
from Pakistan. The supplied Console screenshot shows `pg-11e9a1b`,
`valkey-21c27f8` and `kafka-1b938f` Running on Free plans. PostgreSQL/Valkey
show DigitalOcean Bangalore (`blr`); Kafka's region is not shown. These existing
services must be reused. Managed TLS write/read, actual Kafka ingestion and Feast
retrieval have now passed; PostgreSQL reports version 18.6. Follow [Aiven credential handoff](#aiven-credential-handoff).

## Architecture and data meaning

```mermaid
flowchart LR
    TLC[Official TLC recorded trips] --> GHA[GitHub Actions / Prefect OSS flows]
    GHA --> K[Aiven Free Kafka / mTLS]
    K --> C[Scheduled bounded consumer]
    C --> PG[(Aiven Free PostgreSQL)]
    PG --> F[Feast offline features and PIT training]
    F --> V[(Aiven Free Valkey / TLS)]
    F --> L[LightGBM / existing promotion gate]
    L --> M[DagsHub MLflow / artifacts and registry]
    V --> API[Render Free FastAPI]
    M --> API
    API --> PG
    API --> UI[Streamlit Community Cloud]
    API --> A[LangGraph / Free Groq or Gemini]
    GHA --> E[Evidently historical backtests]
    E --> PG
```

The free profile uses at most 10,000 genuine January 2023 green-taxi observations,
sampled evenly across the official file. NYC local observation times normalize to
UTC; they are never rewritten to look current. Counts/metrics describe the
**sample**, not all NYC taxis. The dashboard requests actual model predictions;
there is no static predictions dataset.

Twice-daily Kafka processing is **scheduled, not continuous real-time streaming**.
After the sample is exhausted, jobs refresh features without inventing new trips.
Identical training/monitoring inputs are skipped. All original full ETL, live
feed clients, local Compose, training and monitoring capabilities remain.
Cloud mode disables synthetic feed fallbacks and untrained model fallbacks.

## Owner actions: accounts and resources

Signup/email verification/GitHub OAuth and regional eligibility require the owner.
Do not paste credentials into chat. Do not add a card, choose a trial, upgrade or
accept paid resources. If a provider requires billing verification, record the
blocker rather than proceeding.

1. Reuse the existing **Free** Kafka, **Free** PostgreSQL and **Free** Valkey.
   Kafka Free is currently Console-only. Select only an actually offered Free
   region. The existing service runs PostgreSQL 18.6 (owner-confirmed); retain it and
   verify migrations/Feast against that version. The local Compose target remains 16. Use a dedicated `logistics` database. Download each service CA and
   Kafka's client/service certificate and private key.
2. Provision only `trip.events` and `trip.events.deadletter` initially, with up to
   two partitions each and provider-compatible replication/retention. If the
   broker requires manual topic creation, create these before bootstrap. The
   existing full design has five topics; never exceed Free's five-topic cap.
3. Create DagsHub **Individual / Free**, connect this public repository and obtain
   a project-scoped token. Tracking endpoint is its HTTPS `.mlflow` URL. Confirm
   actual artifact quota and registry access in the account.
4. Create Render Free and authorize this repo only. Use `render.yaml`, or a **Free
   Docker Web Service**, root build context and `src/serving/Dockerfile.cloud`.
   Frankfurt is proposed if offered; no paid fallback. Keep one Free web service,
   no paid disks, databases or workers. `checksPass` controls GitHub autodeploy.
5. Create Streamlit Community Cloud, select this repo, reviewed deployment branch
   (ultimately `main`), `ui/app.py`, Python 3.11. Its requirements are a generated
   snapshot of the root uv `ui` extra. Set secrets below.
6. Optionally create a Groq **Free** organization/key. Default model is now
   `openai/gpt-oss-20b`; check actual account limits. For Gemini, select a current
   Free model explicitly using `GEMINI_MODEL` and keep billing disabled. Old model
   identifiers are not certified by this deployment.

Free Aiven has no VPC. Use provider public endpoints with authentication and
CA/hostname verification; never expose unauthenticated local ports or disable
TLS checks. Apply least-privilege users/ACLs and available IP controls. Dynamic
standard GitHub runner egress requires an operator-reviewed network policy.

## GitHub secrets and TLS

Create the GitHub Actions environment `free-cloud` and store these secrets there
(repository secrets with identical names are also supported):

| Secret | Value/purpose |
|---|---|
| `CLOUD_DATABASE_URL` | `postgresql+psycopg2://USER:URL_ENCODED_PASSWORD@HOST:PORT/logistics?sslmode=verify-full` |
| `CLOUD_REDIS_URL` | `rediss://USER:URL_ENCODED_PASSWORD@HOST:PORT/0` |
| `CLOUD_KAFKA_BROKER` | Actual Kafka `HOST:PORT` |
| `CLOUD_KAFKA_CA_PEM` | Multiline Kafka CA |
| `CLOUD_KAFKA_CERT_PEM` | Multiline Kafka client certificate |
| `CLOUD_KAFKA_KEY_PEM` | Multiline Kafka private key |
| `CLOUD_POSTGRES_CA_PEM` | Multiline PostgreSQL CA |
| `CLOUD_VALKEY_CA_PEM` | Optional custom Valkey CA only; normally omit for Aiven's publicly trusted certificate |
| `CLOUD_MLFLOW_TRACKING_URI` | Actual `https://dagshub.com/OWNER/REPO.mlflow` |
| `CLOUD_MLFLOW_USERNAME` | DagsHub username |
| `CLOUD_MLFLOW_TOKEN` | Scoped DagsHub token |
| `CLOUD_AGENT_ACCESS_KEY` | Random shared API access key matching API/UI |

Use encrypted-secret UI or `gh secret set NAME --env free-cloud` with interactive
stdin; never put values in shell history. Set repository variable
`CLOUD_DEPLOYMENT_ENABLED=true` only after the Free resources are confirmed.
Set `CLOUD_FASTAPI_URL` and `CLOUD_STREAMLIT_URL` after actual hostnames exist.

Preflight writes private PEM files under ignored `.certs/` and exports their
paths through `$GITHUB_ENV`; it verifies configuration shape, not connectivity.
PR checks never receive cloud credentials. Bootstrap needs a migration owner;
afterward use a job role restricted to its tables. The API needs SELECT on
warehouse/features/Feast registry plus INSERT on `warehouse.predictions`, no
broker or service-administrator credentials. Use read-only DagsHub serving
credentials where available. Valkey permissions must cover actual Feast/cache
commands. No DROP permissions are needed in serving.

## Bootstrap, training and monitoring

After the workflow is available on GitHub:

```powershell
gh workflow run free-cloud.yml --ref feature/d0-free-cloud-deployment -f job=bootstrap
gh run list --workflow free-cloud.yml --limit 5
# Wait for successful bootstrap and inspect source/row/materialization counts.
gh workflow run free-cloud.yml --ref feature/d0-free-cloud-deployment -f job=train
# After genuine Production models are promoted:
gh workflow run free-cloud.yml --ref feature/d0-free-cloud-deployment -f job=monitor
```

GitHub may require the workflow on the default branch before dispatch. Preserve
protected branches and the reviewed `dev` -> `main` workflow. Scheduled triggers
run only on the default branch.

Equivalent configured runner/remote-shell entry points:

```bash
uv sync --frozen --extra core --extra extract --extra transform --extra features --extra training --extra orchestration --extra monitoring --extra cloud-serving
uv run python -m src.orchestration.flows.free_tier_pipeline --bootstrap --max-events 10000
uv run python -m src.orchestration.free_tier_jobs train
uv run python -m src.orchestration.free_tier_jobs monitor
uv run python scripts/verify_cloud_deployment.py --output artifacts/cloud-verification.json
```

Supply credentials and TLS paths first. The additive migration creates durable
cursors and report-HTML tables. The profile refuses a mixed existing warehouse;
it never deletes existing data. A source SHA-256 change requires checkpoint
review. Broker acknowledgements precede consumer persistence, and commits contain
only each handled partition's offset+1. Infrastructure failures remain retryable;
DLQ writes must be acknowledged. The PostgreSQL cursor advances only after the
selected real trip IDs exist in storage, so events can be republished after a
broker retention gap without duplicate warehouse rows.

Confirmed progress is saved in 100-event batches. A job stops starting new
batches after 15 minutes and then reconciles features, so `--max-events` is an
upper bound. Re-run bootstrap/pipeline if `remaining_sample_events` is nonzero;
completed batches survive a timeout. Training refuses a cursor ahead of the last
completed materialization. Predictions expose `data_as_of` from Feast event
timestamps separately from scoring time. Historical ETA calendar inputs use the
actual observed timestamp.

The existing baseline/promotion gate applies. If sampled data cannot produce
acceptable real models, availability checks fail and API predictions return 503.
Never force a dummy model into Production. This profile uses 150 trees and two
training threads; its evaluation cannot inherit full-dataset benchmark claims.
Monitoring evaluates historical holdout predictions from the actual champion,
labels reports as historical sample backtests, and persists HTML in PostgreSQL.
It does not claim live prediction drift without live observations.

## API and dashboard hosting settings

Render variables are enumerated in `render.yaml`. Upload secret files in
Render settings only when needed (one required, one optional):

- `/etc/secrets/postgres-ca.pem`: actual PostgreSQL CA.
- Optional `/etc/secrets/valkey-ca.pem`: custom Valkey CA only if the service
  actually needs one; set `REDIS_SSL_CA` to this path only in that case. Normally
  omit both and use system trust with hostname/certificate verification enabled.

Set `DATABASE_URL`, `REDIS_URL`, DagsHub endpoint/auth, `DEPLOYMENT_MODE=cloud`,
`MLFLOW_MODEL_LOAD_MODE=native`, and the actual dashboard origin in `CORS_ORIGINS`.
Optional LLM keys and `AGENT_API_KEY` enable the cloud agent. No access key means
503; an unavailable live provider means 503, not mock success. The server limits
agent requests to three/minute per process. Keep the LLM organization Free so
exhaustion cannot become billed usage.

`/health/live` checks process liveness; `/health/ready` returns 503 until the
database, Valkey and both genuine models are ready. `/health` retains inspection
compatibility. Startup failures remain explicit and later prediction requests can
retry model initialization. There is no application SQLite fallback. Native
LightGBM reloads from DagsHub after ephemeral disk loss and avoids pyfunc/pickle
execution. The image has one worker/eight concurrent requests and excludes
XGBoost, full MLflow server, training and Evidently. The managed-model Linux gate
passed twice at 512 MiB/0.1 CPU, with peak process RSS 386.7 MiB. Actual Render
startup, resource behavior and public predictions remain separate acceptance gates.

Community Cloud secrets (top-level values become environment variables):

```toml
FASTAPI_URL = "https://ACTUAL-DEPLOYED-API.onrender.com"
AGENT_API_KEY = "SET_IN_SECRET_UI_ONLY"
```

The UI retrieves PostgreSQL reference centroids computed from official polygons,
requests selected-zone demand forecasts, and renders a prediction table/map plus
materialized corridor ETA. Missing features/models and cold starts display an
unavailable message. Forecast/chat requests allow 90 seconds for wakeup. There
is no artificial keepalive. Update API CORS after the actual UI URL is assigned.

## Schedules, resources, cost and acceptance

UTC schedules: pipeline 03:17/15:17 (08:17/20:17 Pakistan); training Sunday 04:43
(09:43 Pakistan); monitoring daily 05:47 (10:47 Pakistan). GitHub can delay/drop
runs and disable schedules after repository inactivity. One shared non-cancelling
concurrency group and 25-minute job timeout prevent overlapping cloud mutations.
Prefect OSS runs on runners; no Hobby process-worker or free continuous compute
is claimed. Optional Prefect Serverless is not configured or verified.

| Component | Bounded resource plan |
|---|---|
| API | One process/OpenMP thread; genuine managed models + 4 concurrent 145-zone batches passed twice at 512 MiB/0.1 CPU; peak RSS 386.7 MiB. Hosted Render behavior remains unverified |
| UI | Thin Streamlit/requests/Pandas/PyDeck process, no local model/Feast stack |
| Jobs | Two training threads; <=10k sample rows, <=500k scanned rows, <=64 MiB source, <=32-day feature window; standard Linux runner |
| PostgreSQL | 1 GB provider disk, stop at 700 MiB; low pools (API 2+0, registry 1+0, offline max 2) leave room below 20 connections |
| Valkey | ~512 MB usable; bounded sample entities, 24-hour feature keys, 60-second prediction cache |
| Kafka | Two initial topics, <=2 partitions/topic, throttled publishing, <=3-day retention |
| Reports/artifacts | PostgreSQL summaries/HTML and DagsHub models; identical jobs skipped; runner/API local disk never treated as durable |

Expected recurring hosting cost: **$0/month within the selected Free plans**.
Quota exhaustion may suspend availability; finite free storage cannot keep
unlimited history. Owner action is required at capacity; nothing automatically
deletes history or upgrades. No paid VPS, trial credits, R2 bucket, paid disk or
larger runner is required.

After bootstrap/training/hosting, dispatch `job=verify`. Its JSON records actual
Kafka offsets, PostgreSQL IDs, Feast/SQL equality, MLflow artifact SHA-256 readback,
prediction versions and persisted monitoring HTML. Missing configuration is
**BLOCKED — NOT VERIFIED**, failures are **FAILED**. Container smoke fixtures are
not managed-cloud deployment evidence.

Finally open the real UI in a browser, generate visible forecasts, and record the
actual response/table/map. Let Render sleep naturally or restart it, then repeat
ready/demand/ETA checks and inspect persisted predictions. Inspect an actual
scheduled-event Actions run to prove cron execution. An HTTP shell or manual DB
run row alone proves neither visual rendering nor scheduler execution.

## Aiven credential handoff

The `free-cloud` GitHub environment now exists. CLI authentication has `repo` and
`workflow` scopes and repository admin permission; its environment API creation
and secret-name listing succeeded. Secret values cannot be read back from GitHub.
No Aiven token or Console session has been supplied to this agent. Do not create
another service, enable a trial or change any plan.

### Retrieve the existing connection details

Open the existing project in Aiven Console, then each existing service:

1. **Kafka `kafka-1b938f`:** Overview -> Connection information / Quick connect ->
   Client certificate (SSL). Copy the bootstrap hostname and **that SSL port**,
   without a URI scheme. Download the CA certificate (`ca.pem`), service/client
   certificate (`service.cert` or `.crt`) and access/private key (`service.key`).
   Some Console versions offer a combined download. Use the certificate/key for
   the selected service user; do not mix files from another project/user. Do not
   copy the Schema Registry URL or SASL port. Our profile uses `SSL` mutual TLS,
   not SASL, with hostname verification. See [official connection instructions](https://aiven.io/docs/products/kafka/howto/kcat).
2. **PostgreSQL `pg-11e9a1b`:** Overview -> Connection information. Record hostname,
   port, username, password and server major version; download its CA certificate.
   In **Databases**, check whether `logistics` already exists. If absent, create
   only this database in the existing Free service (no new service). Preserve
   `defaultdb` and all existing databases. Use the migration-capable service user
   for bootstrap; a restricted job/API role follows after schema setup. The
   target is PostgreSQL 16; a higher provisioned major version requires recorded
   compatibility verification, not recreation/downgrade. This stage's probe reports
   the actual version and requires at least 16. See [database creation](https://aiven.io/docs/products/postgresql/howto/create-database).
3. **Valkey `valkey-21c27f8`:** Overview -> Connection information. Record its
   hostname, TLS port, actual username and password; use logical database `0`.
   Do not assume the username matches PostgreSQL. Aiven normally uses a publicly
   trusted certificate, **no downloadable project CA is needed**. Never substitute
   Kafka/PG CA or disable verification. See [Aiven TLS trust](https://aiven.io/docs/platform/concepts/tls-ssl-certificates)
   and [Valkey connection details](https://aiven.io/docs/products/valkey/howto/connect-python).

Save downloaded certificates/keys in a private folder outside the repository and
OneDrive (for example a folder under your local profile). Do not upload them to
chat, commit them, or paste service URIs/passwords into commands. The passwords
below are entered raw; the helper percent-encodes them exactly once.

### Database and topics

The connection database is **`logistics`**, not `defaultdb`. Bootstrap applies the
existing Alembic migrations for `raw`/`warehouse` and explicitly creates `feast`
for its SQL registry. DagsHub owns MLflow storage separately. Do not precreate
application tables manually or grant the eventual API role administrative rights.

In Kafka -> **Topics**, inspect before creating anything:

| Setting | Expected |
|---|---|
| Topic names | `trip.events`, `trip.events.deadletter` only initially |
| Partitions | Two per topic if fixed by Free; existing one/two partitions are supported |
| Replication | Keep provider-managed Free default; do not force the local single-broker setting |
| Retention | Keep fixed Free default; the application does not extend it |
| Cleanup | Preserve default delete/retention behavior; do not enable compaction for trip events |
| Consumer group | `logistics-free-scheduled-v1`, created automatically by the consumer |
| Producer acknowledgements | `all`; consumer auto-commit disabled |

Do not enable sample-data generators or add three unused live-feed topics. Free
allows up to five topics, each with two partitions; throughput is 250 KiB/s in
and out and retention is provider-fixed. If topic creation controls are absent,
report that limitation rather than upgrading. The initial Aiven probe **does not
create topics**; bootstrap reuses existing topics. Limited topic/group read/write
ACLs are preferred where Free permits them. See [current Free limits](https://aiven.io/docs/products/kafka/free-tier/kafka-free-tier).

### Configure encrypted environment secrets

From a **private interactive PowerShell terminal**, in the project folder:

```powershell
.venv/Scripts/python.exe scripts/configure_cloud_secrets.py aiven
```

The helper prompts for PostgreSQL and Valkey fields, Kafka `HOST:PORT` and the
four certificate/key file paths. Passwords are hidden. It sends values directly
to `gh secret set --env free-cloud --repo KhuzaimaHassan/logistics-forecasting-platform`
via stdin; no credential-bearing command arguments, local secret file or printed
secret values are produced. Uploads are incremental; a failed run may be rerun.
The helper does not create or change any cloud resource. Downloaded files remain
under your control; it does not delete them. Check names only afterward:

```powershell
gh secret list --repo KhuzaimaHassan/logistics-forecasting-platform --env free-cloud
```

Expected Aiven names: `CLOUD_DATABASE_URL`, `CLOUD_REDIS_URL`,
`CLOUD_KAFKA_BROKER`, `CLOUD_KAFKA_CA_PEM`, `CLOUD_KAFKA_CERT_PEM`,
`CLOUD_KAFKA_KEY_PEM`, `CLOUD_POSTGRES_CA_PEM`. `CLOUD_VALKEY_CA_PEM` is optional.
The helper constructs `sslmode=verify-full` PostgreSQL and `rediss://` Valkey URLs.
If the four PEM secrets were already securely uploaded and validated, configure
only the three remaining connection secrets without re-uploading certificates:

```powershell
.venv/Scripts/python.exe scripts/configure_cloud_secrets.py aiven --connections-only
```

Alternatively use GitHub Settings -> Environments -> free-cloud -> Add environment
secret with the exact names in the table above; this does not require the helper.

After configuring these names, tell the agent **Aiven secrets configured** (no
values). The new workflow is not on `main` yet (confirmed HTTP 404), so direct dispatch
will not be available until reviewed integration. The already registered ETL
workflow provides a manual bridge to the exact same Aiven job on this branch,
without merging or changing protected branches. The agent can dispatch:

```powershell
gh workflow run etl_live_smoke.yml --ref feature/d0-free-cloud-deployment -f verify_aiven=true
```

This manual-only check does not require `CLOUD_DEPLOYMENT_ENABLED=true`, DagsHub
or hosting. Preflight checks configuration/TLS file shape; connectivity is a
separate step. Actual tests write/read a PostgreSQL session temporary table,
write/read a unique Valkey key with 120-second expiry, and publish/read one
validated authentic TLC event at its exact Kafka offset. No existing data is
deleted. The trip event may subsequently be consumed by the normal idempotent
pipeline. JSON contains sanitized evidence or exception type only. Feast online
materialization is verified separately by the completed bootstrap and aggregate
checks linked in DEPLOYMENT_REPORT.md.

### Remaining providers and full verification

DagsHub tracking/artifacts/registry, Render API, Community Cloud UI and optional
LLM credentials require owner account access for a fresh deployment; these are
already configured and verified for this deployment. For DagsHub, after creating
its Free repository/token, run the helper's `mlflow` group. The `agent` group
uploads a shared API/UI access key from your password manager; it is optional for
initial infrastructure checks. The LLM provider key belongs on the API host.
Do not enable full scheduled operations until MLflow and managed checks pass.
The agent can independently inspect secret names, dispatch checks, fix code,
review sanitized artifacts and update the report; it cannot retrieve passwords,
private keys or service details from your screenshot or GitHub secrets.

### Authorized initial database/topic creation

The owner has now configured all seven Aiven secrets and explicitly authorized
creation of the missing database/topics. Use the existing registered workflow:

```powershell
gh workflow run etl_live_smoke.yml --ref feature/d0-free-cloud-deployment -f initialize_aiven=true
```

This manual operation connects to existing `defaultdb` with the configured TLS
credentials, creates `logistics` only if absent, and then tests `logistics`.
`CLOUD_DATABASE_URL` must already target `logistics` (the helper constructs this).
It never deletes, renames or recreates `defaultdb`. For the two topics it checks
existing metadata, creates only missing names with two partitions, and uses up to
three advertised brokers for replication (documented Aiven default is three;
this installed Kafka client cannot express broker-default replication in its
CreateTopics v3 request). Retention and other topic configuration are left to
the provider. Existing topics are not repartitioned or reconfigured. No service,
plan, disk or paid resource is created. Free five-topic capacity is checked.

A first run exposed missing secret inheritance in the reusable workflow caller;
that run stopped at preflight, before connecting to Aiven. The caller now
explicitly inherits secrets while the called job selects environment `free-cloud`.

After successful initialization, bootstrap/verify Aiven-only ingestion and Feast
without requiring MLflow or enabling schedules:

```powershell
gh workflow run etl_live_smoke.yml --ref feature/d0-free-cloud-deployment -f verify_aiven=true -f aiven_operation=bootstrap-aiven
```

This applies the existing additive migrations and official zone centroids,
processes bounded authentic Kafka batches into PostgreSQL and materializes real
Feast online features into Valkey, then compares an online value/event timestamp
against PostgreSQL. A partial batch checkpoint is durable: repeat this operation
if remaining_sample_events is nonzero. It cannot train/promote models or prove
public deployment before DagsHub/hosting configuration.


## Explicit managed jobs before enabling schedules

The registered ETL workflow also exposes `cloud_job` for manual operations on the
review branch without enabling cron or merging protected branches:

```powershell
gh workflow run etl_live_smoke.yml --ref feature/d0-free-cloud-deployment -f cloud_job=verify-mlflow
gh workflow run etl_live_smoke.yml --ref feature/d0-free-cloud-deployment -f cloud_job=train
gh workflow run etl_live_smoke.yml --ref feature/d0-free-cloud-deployment -f cloud_job=monitor
gh workflow run etl_live_smoke.yml --ref feature/d0-free-cloud-deployment -f cloud_job=runtime
```

Wait for each result before dispatching the next; GitHub permits only one pending
run per concurrency group. `verify-mlflow` is scoped: successful metrics/artifact
readback and registry-list access do not mark full acceptance complete. `runtime`
builds the real minimal API image on GitHub Actions and tests genuine promoted
models against Aiven in two separate fresh containers with 512 MiB/0.1 CPU limits.
It persists real predictions, compares four concurrent batch responses and records
memory peaks. This proves neither a public Render URL nor hosted cold-start timing.
Certificate files remain private; the API probe receives only its PostgreSQL CA,
never Kafka client keys. No local Docker is required.


### Manual Render form values

Choose Docker Web Service, Free, repository KhuzaimaHassan/logistics-forecasting-platform,
branch feature/d0-free-cloud-deployment, Frankfurt if Free is offered. Root directory
blank, build context `.`, Dockerfile `src/serving/Dockerfile.cloud`, default image
command, health check `/health/live`, auto-deploy after CI checks if available.
No attached disk, database or worker is needed. Enter secrets only in Render's UI.
Use the environment names in render.yaml: strict cloud/native model flags, managed
PostgreSQL URL targeting logistics with verify-full, Valkey rediss URL, DagsHub
tracking URI/username/password, PostgreSQL CA secret file. Leave REDIS_SSL_CA unset
for Aiven public trust. Kafka credentials do not belong on the API. Optional
agent/provider/CORS values can follow when those components are configured.
A live process is not readiness: `/health/ready` and real predictions must pass
before claiming the deployment complete. If billing/card/paid fallback is required,
record a blocker rather than enabling it.

### Read-only training diagnostics

`gh workflow run etl_live_smoke.yml --ref feature/d0-free-cloud-deployment -f cloud_job=diagnose-db`

This independent read-only job reports database session timings/wait states and
size without exposing raw SQL or connection values. It can run while a training
job occupies the mutation concurrency group. It does not cancel queries or
modify records; its VERIFIED status covers diagnostics only.

### Feature refresh before hosting handoff

Online feature keys expire after 24 hours. Until the reviewed workflows reach the
default branch and scheduled operations are explicitly enabled, refresh the real
bounded sample before testing a newly created Render app:

```powershell
gh workflow run etl_live_smoke.yml --ref feature/d0-free-cloud-deployment -f verify_aiven=true -f aiven_operation=bootstrap-aiven
```

The completed source cursor is reused; this does not recreate services, topics or
the database. Wait for success before testing `/health/ready` and predictions.
A later successful aggregate `verify` also reconciles Feast from PostgreSQL.
Without these refreshes or active schedules, feature availability is not continuous.

### Public API now verified

API: https://logistics-forecasting-platform.onrender.com

Set Community Cloud `FASTAPI_URL` to that URL (without `/docs`). For an API-only
managed verification, avoiding unrelated re-materialization:

```powershell
gh workflow run etl_live_smoke.yml --ref feature/d0-free-cloud-deployment -f cloud_job=verify-api
```

This checks actual Production demand/ETA and reads back matching new predictions
from managed PostgreSQL. A scoped success cannot mark the complete deployment
accepted. Initial public request latency was 93.4s in one observation; give the
service time to wake and retry unavailable dashboard requests. Controlled hosted
sleep/restart remains unverified. Public Streamlit forecast rendering is now verified; live agent tool execution and UI fix rollout remain under acceptance testing.


### Public dashboard and Copilot access

Dashboard: https://logistics-forecasting-platform.streamlit.app/

Community Cloud secrets must contain `FASTAPI_URL` pointing to the Render URL.
`AGENT_API_KEY` is an access password you generate yourself, separate from the
Groq provider key. Generate it only in a private terminal:

```powershell
.venv/Scripts/python.exe -c "import secrets; print(secrets.token_urlsafe(32))"
```

Save the output in a password manager. Enter the same value as Render environment
variable `AGENT_API_KEY` and Community Cloud TOML secret `AGENT_API_KEY`.
Run the hidden prompt to upload it as GitHub environment secret
`CLOUD_AGENT_ACCESS_KEY`:

```powershell
.venv/Scripts/python.exe scripts/configure_cloud_secrets.py agent
```

Do not paste keys into chat, commit them, or store them in a synced repository.
The helper sends the access key to `gh secret set` through stdin; it does not write
it to `.env`. GitHub does not let you read back secret values. Keep your own copy
or rotate the same shared password in all three stores if lost.

Obtain `GROQ_API_KEY` in your Groq Free account's API Keys page and configure it
on Render. GitHub's normal agent verifier calls Render; it does not need a
provider key. An optional environment secret GROQ_API_KEY lets the failure-only
diagnostic distinguish provider/schema errors from hosted failures; the owner has
configured it for this deployment. Leave billing disabled. Never put either key in a browser URL. The public
Streamlit server sends the shared key to Render server-side, subject to the API's
three-requests-per-minute agent limit. For a scoped real provider/tool check:

```powershell
gh workflow run etl_live_smoke.yml --ref feature/d0-free-cloud-deployment -f cloud_job=verify-agent
```

Wait until Render `/deployment/status` reports the intended source revision before
running this check. A successful scoped operation never marks full acceptance
complete. The public dashboard should use branch `feature/d0-free-cloud-deployment`,
entrypoint `ui/app.py`; compare the deployed labels to the current source after a
Community Cloud restart. Application build logs and settings require owner access.

### Retention and rollout acceptance

Feast 0.65 skips equal event timestamps before renewing Redis expiry. The bounded
cloud flow therefore renews only its exact entity keys after materialization,
without changing historical feature values/event times. A completed source cursor
still requires periodic materialization/retention renewal. The flow fails before
recording refresh success if any expected online key is missing.

Wait for the selected Render source revision before dispatching public acceptance.
A pre-deploy check of the old endpoint can fail and block a checksPass deployment;
use an owner manual deploy of the already-tested commit if necessary. Provider
diagnostics from an Actions runner are separate from public endpoint acceptance.
Do not run repeated queued mutations: the shared cloud concurrency group permits
only one running and one pending job.

Once the reviewed workflow reaches the default branch and the repository opt-in
variable is enabled, the configured schedule is:

| Operation | UTC | Pakistan (Asia/Karachi) |
|---|---|---|
| Bounded Kafka processing/feature retention refresh | Daily 03:17 and 15:17 | Daily 08:17 and 20:17 |
| Training with existing promotion gates | Sunday 04:43 | Sunday 09:43 |
| Historical monitoring | Daily 05:47 | Daily 10:47 |

These are intended schedules, not verified execution times. GitHub scheduling can
be delayed/dropped; a manual dispatch does not prove a scheduled-event run.

Current acceptance (2026-10-20): all nine managed/public checks passed in run
39272455345. PR #166 is merged to dev; fully green PR #167 is squash-merged into
protected main 4450d95. Release CI run 39400299729 passed 398 tests plus Compose/memory;
ARM64 run 39400243947 and post-main CI run 39401266610 passed. The cloud workflow is active
on main and CLOUD_DEPLOYMENT_ENABLED=true. Manual pipeline 39401271976 renewed
1,319 exact online keys to 24 hours while preserving 9,232 trips and publishing no
new sample duplicates. Actual schedule execution remains NOT VERIFIED until the
next genuine cron event (first post-merge pipeline slot 20:17 PKT today).

Recovery after 973.664 seconds of agent inactivity passed: fresh demand/ETA,
new actual model-load timestamps on the unchanged revision, readiness 200,
warehouse persistence and browser retry. The first request took 140.968 seconds;
this can exceed the UI 90 second timeout, so retry after temporary unavailability.
Main API check 39402814242 matched two newly persisted PostgreSQL forecast rows.
Private provider sleep/start logs were not available. See the deployment report
and controlled-idle-recovery-evidence.json for exact scope.

Default-branch commands now work directly:

```powershell
gh workflow run free-cloud.yml --ref main -f job=pipeline
gh workflow run free-cloud.yml --ref main -f job=verify-api
gh run list --workflow free-cloud.yml --event schedule
```

The first two are manual operations; only the third can find actual cron events.
Do not claim schedule acceptance from workflow_dispatch.
Keep using the existing Free services; do not recreate them or deploy duplicate
Blueprint resources.

### Attached-card billing acceptance (2026-10-19)

The owner confirms an attached Render card despite selecting Free compute. This
allows bandwidth/build-minute overage charges under the [official Free terms](https://render.com/docs/free).
Do not claim a guaranteed $0 bill from the Free badge. No charge is observed here.

In Workspace Settings -> Build Pipeline -> Set/Edit spend limit, set additional
monthly pipeline spend to $0 if supported and verify the saved value. This limit
covers pipeline minutes only, not outbound bandwidth. See [the official limit instructions](https://render.com/docs/build-pipeline#setting-a-spend-limit).
Have the owner check whether the card can be removed without deleting/changing
the existing Free service, or obtain provider confirmation of a complete overage
block. Do not remove payment methods, upgrade, delete resources or start new
billable resources automatically. Usage alerts alone do not establish a hard cap.
Update: owner subsequently confirmed the card was removed. The attached-card
blocker is resolved on owner confirmation; private Billing was not independently
inspected. Preserve no-payment-method status and Free compute.

### Genuine scheduled acceptance — 2026-10-21

Operational follow-up: the owner later reported a PostgreSQL OOM interruption.
Do not delete data or upgrade to treat a RAM alert. Follow the
[memory investigation/runbook](POSTGRES_MEMORY_RUNBOOK.md); current cause and
sustained recovery are not verified by historical deployment acceptance.

Run39482347476 on main8a9b4d0 was genuinely triggered by schedule and succeeded.
It started00:18:36 Pakistan on October21, about4hours after the intended20:17
October20 slot. Actual managed output retained9,232 trips and renewed1,319 online
keys to86,400seconds; no new sample publications were needed. The core bounded
profile acceptance checklist is now evidenced. Prior NOT VERIFIED observations
were correct at their recorded times and remain in the log. See the final report
and scheduled-pipeline-evidence.json. This does not promise punctual cron delivery
or certify separate train/monitor schedule execution. Do not treat manual dispatch
or continuous polling as scheduled-event proof.

## HEAD-only availability monitoring

For a monitor that permits only HEAD, use
`https://logistics-forecasting-platform.onrender.com/health/ready` with expected
HTTP 200. GET and HEAD readiness use the same dependency/model checks; HEAD
returns an empty body and 503 when readiness is degraded. `/health/live` supports
HEAD but checks process liveness only. Legacy `/health` retains HTTP 200 for
its diagnostic JSON contract and is unsuitable for HEAD-based readiness checks.
Do not treat every 405 or cold-start timeout as evidence of a database outage.
Probe readiness sparingly because each check contacts managed dependencies.
The route update must be deployed before switching an existing monitor to it.
