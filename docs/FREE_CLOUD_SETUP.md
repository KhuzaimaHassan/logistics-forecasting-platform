# Free cloud setup

**No cloud service is currently claimed live.** Read
[DEPLOYMENT_REPORT.md](DEPLOYMENT_REPORT.md) for verification and
[FREE_TIER_RESEARCH.md](FREE_TIER_RESEARCH.md) for current official offers.

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

1. In Aiven Console create **Free** Kafka, **Free** PostgreSQL and **Free** Valkey.
   Kafka Free is currently Console-only. Select only an actually offered Free
   region. PostgreSQL 16 must be available; otherwise record the compatibility
   blocker. Use a dedicated `logistics` database. Download each service CA and
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
| `CLOUD_VALKEY_CA_PEM` | Multiline Valkey CA |
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

Render variables are enumerated in `render.yaml`. Upload two secret files in
Render settings before first launch:

- `/etc/secrets/postgres-ca.pem`: actual PostgreSQL CA.
- `/etc/secrets/valkey-ca.pem`: actual Valkey CA.

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
XGBoost, full MLflow server, training and Evidently. Full-model Linux peak memory
must still pass the 512 MB acceptance check before claiming Render suitability.

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
| API | One process/OpenMP thread; Windows import RSS 204.8 MiB measured; Render 512 MB full-model/concurrency/restart peak remains unverified |
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
