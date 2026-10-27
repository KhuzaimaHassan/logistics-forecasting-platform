# Free cloud deployment log

### PostgreSQL memory incident — 2026-10-21

- Owner supplied Aiven OOM warning and graph of RAM gradually rising near95%,
  dropping around03:00UTC. No data deletion or paid plan authorized/performed.
- Baseline read-only run39578320625 and expanded run39578655289 succeeded.
  Database61.83MiB, work_mem2MiB, no other sessions at collection. Cumulative
  temp writes1.49TB are historical I/O, not current disk occupation.
- Added bounded, sanitized catalog diagnostics; focused unit test passed1/1,
  Ruff/Black passed. Raw live evidence and memory runbook recorded. No heavy
  reproduction, database-wide tuning, restart or speculative fix applied.
- Only evening cloud job in queried18:00–04:00UTC history ended19:22UTC, not
  overlapping apparent03:00UTC drop. Exact victim/cause needs provider logs;
  root cause and sustained recovery NOT VERIFIED. Prior acceptance is historical.

Dates use Asia/Karachi. No cloud deployment is claimed without service evidence.

## 2026-10-17 — D0 audit and implementation plan

- Inspected source, tests, root Compose files, all Dockerfiles, dependency groups,
  CI/deploy workflows, README, architecture, ADRs, roadmap and milestone reports.
- Working branch: `feature/d0-free-cloud-deployment`, created from `dev`.
  Preserved the pre-existing edit to `infra/oracle-vm/launch_ampere_retry.py`.
- GitHub authenticated successfully outside the network sandbox. Repository is
  public. Existing secret names are MTA_API_KEY and OPENWEATHERMAP_API_KEY only.
- Operator confirmed no existing managed-cloud accounts; account provisioning and
  all managed-infrastructure checks are **BLOCKED — NOT VERIFIED**.
- Source audit found unsafe whole-poll Kafka commits, unauthenticated local-only
  Kafka/Feast configuration, an application SQLite fallback, untrained model
  fallbacks and an unimplemented forecast dashboard tab.
- Plan: (D1) authenticated TLS clients and safe bounded consumers; (D2) managed
  MLflow plus runner-executed Prefect flows; (D3) strict, smaller API deployment;
  (D4) real forecast UI; (D5) bounded scheduled ingestion/training/monitoring;
  (D6) deterministic tests, CI and opt-in cloud smoke evidence; (D7) setup guide,
  ADR and report with explicit verification matrix.
- Constraints: no local Docker; no paid resources, trials, deletion or automatic
  upgrades. A full monthly yellow-taxi warehouse exceeds a 1 GB database; a bounded
  sample of authentic historical observations must be labelled as such.

Implementation results and actual check output are appended as work completes.

## D1–D5 implementation

- Added verified PostgreSQL/Valkey TLS URL handling and shared Kafka mTLS/SASL
  settings; capped default topics to two partitions and small connection pools.
- Fixed whole-poll offset commits and transient-error deadlettering. Added
  exact-offset, DB retry, and failed-DLQ-ack regression checks.
- Added authentic bounded TLC sample jobs, persistent cursors, broker ack/readback
  checkpointing, official polygon centroid bootstrap, feature reconciliation,
  historical-window training and labelled backtest monitoring. Original local
  Compose/full data workflows remain available. No existing data was deleted.
- Added cloud-native LightGBM loading, strict cloud feature/model failures,
  liveness/readiness, CORS, synchronous prediction persistence and agent access
  control. Removed application SQLite fallback. API import measured 204.8 MiB
  RSS on Windows with mlflow.pyfunc absent from sys.modules.
- Replaced the forecast UI placeholder with real API demand/ETA calls and a
  centroid map/table; moved rendering under main() so importing helpers performs
  no HTTP calls. Added generated thin UI dependencies and Free Render manifest.
- Added an opt-in credential-gated cloud workflow with bounded schedules,
  non-cancelling concurrency and sanitized evidence upload. Preserved required
  Lint & Test context; gated the external live feed CI step behind manual opt-in.
- Recorded ADR-028 and current provider research/setup instructions.

## D6 checks so far

- `20 passed in 31.99s`: cloud configuration/native-model tests and consumer
  regression tests on local Python 3.11. These are hermetic tests, not managed
  integration evidence.
- Offline Free-manifest/secret/dependency validation passed.
- Ruff passed for the deployment files; the separate pre-existing Oracle retry
  edit has an import-order issue and remains untouched/excluded from our commit.
- `git diff --check` passed.
- Broad local test collection is being diagnosed; no full-suite pass claimed.
- Subsequent GitHub CI passed all 362 tests (one skipped), lint, formatting and
  offline manifests. The first container attempt exposed a health-contract
  regression, fixed with an actual MLflow probe; the corrected full Compose CI
  subsequently passed. Linux 512 MiB image probe passed at 296.8 MiB peak RSS
  including API/agent imports and an explicitly fixture-trained Booster.
- Authentic-data CI found that fresh migrations did not create the Feast schema;
  fixed by ensuring it before registry apply. Retest is running. The source audit
  also found corridor training imputed missing historical duration from its future
  label. Removed this target leakage and added a missing-history regression check;
  historical ETA benchmark numbers are not recertified by prior green tests.
- `verify_cloud_deployment.py` ran without cloud credentials and correctly
  returned nonzero with all cloud checks NOT VERIFIED. Sanitized output is
  retained at `docs/cloud-verification.json`.

## D6–D7 validation closeout — 2026-10-18

- Authentic-data run 39046508012 succeeded on commit 62717dd: 9,232 valid TLC
  trips passed through real CI Kafka/PostgreSQL, 107,155 zone/8,760 corridor
  feature rows, Feast/Redis materialization, genuine MLflow training/promotion,
  FastAPI demand/ETA and durable predictions/monitoring HTML. Sanitized complete
  output is retained in docs/free-profile-e2e-evidence.json.
- Both existing promotion gates passed without overrides. ETA holdout MAE was
  430.30 seconds versus 666.49 baseline after removing future-label imputation.
  Monitoring completed with alerts; this result is not described as healthy live
  monitoring. The source remains explicitly historical January 2023 observations.
- Full CI run 39046510008 and ARM64 builds 39046506079 succeeded. Prior full unit
  output was 363 passed, one skipped; latest lint/test, Compose and 512 MiB runtime
  jobs all passed. No local Docker was used.
- Report and README updated with actual evidence, complete inventory, account
  setup/secrets guide, conditional $0 cost and outstanding managed verification.
  No account/resource/public URL exists yet. Managed checks remain NOT VERIFIED.

## Aiven provisioning handoff — 2026-10-18

- Owner supplied Running/Free service evidence for existing PostgreSQL, Valkey
  and Kafka. PostgreSQL/Valkey show Bangalore; Kafka region not visible. No
  services recreated, upgraded or reconfigured.
- Confirmed GitHub CLI identity/scopes/admin permission without exposing token;
  created `free-cloud` environment, verified it has no configured secret names.
- Added secure interactive stdin secret uploader, optional public-root Valkey TLS,
  and manual Aiven-only preflight/real PG temporary write-read, Valkey TTL key
  write-read and authentic Kafka event exact-offset roundtrip workflow.
- Existing full operations remain gated. No Aiven credentials are available;
  Aiven-only preflight correctly stopped with seven missing configuration names.
  Owner must retrieve Console fields/certificates and configure encrypted secrets.
- The new free-cloud workflow is not registered on main (confirmed API 404).
  Added a manual boolean bridge in the existing registered ETL smoke workflow to
  call its Aiven verification job from this branch, preserving branch protection
  and avoiding any merge just to run checks. Legacy/default jobs remain unchanged.
- Validation: 10 focused cloud/handoff tests passed in 43.15s; Ruff, Black
  formatting and offline Free-manifest validation passed. No managed connectivity
  pass is claimed until encrypted credentials are available.

## Aiven certificate configuration — 2026-10-18

- Owner provided local download-folder paths. Read only the four designated
  files; parsed certificates/private key without printing contents. Verified
  certificate validity periods, Kafka client issuance by its CA, private-key/public
  key match and SSL context loading. These are local cryptographic checks only.
- Uploaded CLOUD_KAFKA_CA_PEM, CLOUD_KAFKA_CERT_PEM, CLOUD_KAFKA_KEY_PEM and
  CLOUD_POSTGRES_CA_PEM via gh stdin directly to environment free-cloud. Secret-name
  listing confirms all four. No credential material was copied into the repository.
- Connection URL/broker secrets remain absent. Added --connections-only to the
  interactive helper to preserve certificate secrets while entering remaining
  Console fields. PostgreSQL version/database and Kafka topic confirmations remain
  pending. Managed data-plane connectivity remains NOT VERIFIED.

## Authorized Aiven initialization — 2026-10-18

- Owner configured connection values; seven required Aiven secret names are now
  present. Owner reports PostgreSQL 18.6 and absent logistics database/topics,
  explicitly authorizing their additive creation on existing Free services.
- Added idempotent database/topic initialization, two focused regression tests
  passed (1.84s), lint/format/offline workflow validation passed. Local Compose
  PostgreSQL 16 remains unchanged. Actual PG18 schema/Feast compatibility pending.
- Run 39120857573 failed preflight because environment secrets resolved empty
  through the reusable-workflow call. No managed connection or creation occurred.
  Added explicit secrets inheritance; corrected run 39120936927 passed preflight
  and began actual initialization/roundtrip checks. Results pending.

- Corrected run 39120936927 PASSED all actual Aiven checks. Created logistics;
  server reports 18.6; TLS active and temporary-table write/read passed. Valkey
  TLS PING/SET NX/GET passed with a 120-second key expiry. Both Kafka topics
  created: two partitions, observed RF two, provider retention preserved. Authentic
  TLC event produced and consumed at partition 1/offset 0 using mutual TLS.
  Sanitized output is in docs/aiven-initialization-evidence.json.
- Added a separate manual bootstrap-aiven operation so migrations, actual bounded
  Kafka ingestion and Feast/Valkey materialization can proceed before DagsHub.
  It uses the existing strict pipeline; no model or monitoring success claimed.

- Source review of the managed bootstrap identified per-message PostgreSQL and
  Kafka commits in the scheduled path. Added bounded trip-only multi-row writes
  and one exact per-partition offset commit after durable database/DLQ effects;
  original continuous consumer path remains unchanged. Shared normalization keeps
  validation/IDs identical. Failure rewinds all assigned partitions. Focused
  20-test run passed in 20.79s, including rollback, DLQ failure, buffered offsets
  and replay idempotency; additional post-DB offset-failure regression added.

- Bounded producer sends now collect at most 100 in-flight acknowledgements,
  retaining byte-rate throttling and requiring every ack before checkpointing.
  Optimized bootstrap was dispatched on commit abb1cbd behind the existing
  non-cancelling cloud concurrency group. The earlier running bootstrap was not
  cancelled: the subsequent cancellation/status command was rejected before
  execution because automatic approval review could not run due to a usage limit.
  This is an authorization-service availability blocker, not a cloud test failure.
- Owner reports MLflow secrets configured. Secret names, endpoint connectivity,
  metrics/artifacts/registry and model training have not yet been verified.
  Running/queued GitHub jobs may continue independently; their latest outcomes
  must be retrieved after tool authorization becomes available. Do not mark the
  managed bootstrap or public deployment verified based on dispatch alone.
- Unaffected local validation completed after the authorization blocker: **21
  tests passed in 31.71s**, including the post-database offset-commit failure/replay
  test; Ruff and whitespace checks passed. Latest documentation and sanitized
  initialization evidence are saved locally and still require commit/push.

## Managed bootstrap and DagsHub verification — 2026-10-18

- Tool authorization resumed. Confirmed all three MLflow secret names without
  reading values. Retrieved both real managed bootstrap artifacts: 39121078588
  passed with 600 durable trips; 39121607882 resumed and persisted the remaining
  8,632, reaching 9,232 unique trips and no remaining sample events. Feast matched
  PostgreSQL (zone 7, pickups_last_1h=1, source timestamp Jan 31 2023 23:00 UTC).
  Feature rows: 107,155 zone / 8,760 corridor. DB bytes: 36,312,767. Actual PG18.6
  migrations, SQL registry and Valkey materialization/readback passed.
- MLflow scoped check 39138839993 passed: run 40cb7a97e0ac4ed9ad7a8540113e5617,
  metric 1.0 readback, artifact SHA-256 matched, registry list succeeded. Evidence
  saved without endpoints/credentials. Partial check cannot mark full acceptance.
- Started managed training 39138902348 with existing promotion gates. Added a
  real API-image runtime probe with two fresh 512 MiB/0.1 CPU containers, genuine
  managed features/models and durable prediction assertions; queued 39139225794.
- Five local initialization/scoped-acceptance tests passed in 3.45s. Initial
  local tmp_path errors were sandbox temporary-directory permissions; rerunning
  with the existing workspace TEMP/TMP configuration passed. No code failure
  was hidden. Hosting configuration was supplied while owner prepares accounts.

- Training attempt 39138902348 was cancelled during investigation of entity upload
  performance; queued runtime 39139225794 was cancelled before executing. Driver
  review clarified that Feast uses Psycopg 3 (executemany may pipeline), so a
  row-per-RTT claim was not established. Demand join had completed after about
  four minutes. No speculative SQL-entity optimization was implemented. Restarted
  unchanged training path as 39139455829 and will allow its configured timeout.

- Latest full CI 39139531145 (3159201) passed all three jobs: unit/contracts
  375 passed, one skipped, 45 warnings, 76.32s; Compose integration and Linux
  fixture memory gate also passed. Managed models remain a separate live gate.
- Training/monitor CLI now supports explicit JSON output artifacts without
  changing model parameters or gates; future managed jobs retain sanitized
  structured results in addition to logs.

- Added training-namespace INFO logs and per-Feast-batch row-count/timing messages
  to make future managed-run stalls diagnosable. No query, data, model or gate
  semantics changed. Seven training dataset tests passed in 35.35s; Ruff passed.

### 2026-10-19 — managed training retry preparation

- Managed training 39139455829 exhausted the 25-minute execution budget; GitHub
  marked it cancelled. Runtime 39139567159 subsequently failed readiness because
  Production models were unavailable. Neither run demonstrates a memory failure.
- Bounded cloud corridor Feast batches to 250 entities with stable grouping;
  retained every entity, timestamp and label, existing TTL and promotion gates.
  Eight dataset tests passed in 30.23s; Ruff passed.
- Latest completed full CI 39141169493 and ARM64 39141166641 passed on 1b64de2.
  Real managed model/promotion and public endpoint acceptance remain separate.
- Dispatched training retry 39228745442 on 373c53b; full cloud preflight passed.
  Unit/contract CI job passed: 376 passed, one skipped, 45 warnings in 82.99s.
  Added sanitized failed-runtime evidence; its scope remains unavailable models,
  not measured full-model memory. Owner will configure Render after independent
  engineering work; no public URL exists yet.
- Full CI 39228738178 completed successfully, including Compose integration and
  fixture memory gate; ARM64 build 39228731042 also passed on 373c53b.
- Added an independent read-only PostgreSQL diagnostic operation while training
  continues. Reports session wait states, elapsed seconds and database bytes,
  excludes SQL/connection text, enforces a read-only transaction and 10s statement
  timeout. It cannot cancel queries or mutate infrastructure/data.
- Managed training retry 39228745442 SUCCEEDED in 635.55s. All 6,647 corridor
  entities traversed 27 PIT batches. Demand MAE 0.0493 vs 0.0540; ETA 430.3019s
  vs 666.4875s. Both genuine models promoted to Production v1 and loaded natively.
  Persisted sanitized actual training artifact. Runtime retry 39230564684 started;
  queued monitoring after it. Read-only diagnostic 39230529574 passed, no remaining
  sessions at capture time, database 37,418,687 bytes.
- Managed runtime 39230564684 SUCCEEDED twice in fresh 512 MiB/0.1 CPU
  containers: both Production v1 models, actual demand=0.71 and ETA=715.8s at
  source timestamp Jan 31 2023 23:00 UTC; four concurrent 145-zone batches,
  582 durable predictions per start. Peak process RSS 386.7 MiB/cgroup 373.6 MiB.
  This is not hosted Render cold-start proof. Full CI 39230526314 passed all jobs
  (377 passed, 1 skipped, 45 warnings, 80.27s); ARM64 39230518994 passed.
- Managed monitoring 39230637533 SUCCEEDED: actual 2,000-row reference/current
  windows, three report IDs, Production baseline MAE 0.0493, drift/performance
  alerts logged, retrain recommended but not triggered. Actual result persisted
  as managed-monitoring-evidence.json; aggregate check 39231511998 will verify
  HTML readback and retain missing-public-hosting gates explicitly.
- Aggregate 39231511998 finished: PostgreSQL, Kafka (partition 1 offset 4666),
  Feast/Valkey, MLflow metric/artifact, pipeline output and persisted monitoring
  HTML all VERIFIED. Workflow failure is expected from missing public API/UI URLs
  and agent key; hosted cold start/visual UI/cron also remain NOT VERIFIED.
  Replaced initial missing-config baseline with actual sanitized aggregate artifact.
- Corrected monitoring verifier output label html_bytes -> html_characters: SQL
  length(text) measured 3,701,744 characters. Original artifact retained with an
  explicit explanatory note; no byte measurement was invented.
- Independent backend work is complete. Remaining owner steps: create Render
  Free and Community Cloud apps, configure their private settings, optional Free
  LLM key, and reviewed default-branch integration/schedule enablement. No paid
  resources, service recreation or existing-data deletion occurred.
- Final focused initialization/scoped-acceptance/read-only diagnostic tests:
  6 passed in 4.42s; Ruff and offline deployment configuration validator passed.

### 2026-10-19 — actual public Render API

- Owner supplied https://logistics-forecasting-platform.onrender.com; configured
  CLOUD_FASTAPI_URL in GitHub environment free-cloud without credential changes.
- Direct HTTPS readiness HTTP 200: PostgreSQL/Valkey/MLflow connected, both genuine
  Production v1 models. All 263 official reference zones, 145 observed; scheduled
  historical mode, agent_enabled=false. Demand zone 7=0.71, ETA 75_237=715.8s,
  three-zone dashboard batch passed. Saved actual public-render-evidence.json.
- First observed liveness request took 93.4s; this is not controlled cold-start
  proof. Later readiness 2.14s, demand 15.59s, ETA 1.42s, batch 1.44s.
- Started aggregate run 39239093732 with real public URL. Added scoped verify-api
  operation and matching new PostgreSQL prediction readback, rather than relying
  on HTTP success alone. Partial acceptance guard covers API-only checks too.
  Six focused tests passed in 3.27s, Ruff and offline validator passed.
- Aggregate 39239093732 passed all seven configured checks, including actual
  public demand/ETA. It correctly remained incomplete for absent Streamlit URL,
  agent access key, visual UI, controlled hosted cold start and cron acceptance.
- Scoped API verification 39239560159 SUCCEEDED on 30e9172. Prediction count
  increased by two; both public forecast entity/model/value rows matched new
  managed PostgreSQL records. Saved readback evidence within public-render JSON.
  API-only success retains automated/manual whole-platform acceptance=false.
- Public-verifier source commit 30e9172 passed Lint & Test: 378 passed, one
  skipped, 45 warnings in 68.50s; fixture memory gate passed. Compose/ARM64
  reruns were still in progress at this evidence checkpoint.

### 2026-10-19 — public Streamlit acceptance and correctness fixes

- Owner supplied https://logistics-forecasting-platform.streamlit.app/. Configured
  CLOUD_STREAMLIT_URL and started aggregate check 39240590438. Public browser
  confirmed real serving status and actual demand for 74/75/166: 3.69/5.55/2.26,
  official-centroid map, ETA 11.9 minutes (715.8s), six real pipeline records and
  embedded Evidently performance HTML with the actual degradation alert.
- Found real acceptance defects: misleading real-time/mock-provider labels,
  demand/map disappearing on ETA rerun, MAE metric casing mismatch, and cursor
  updated_at incorrectly shown as feature freshness.
- More serious: actual 2,000-row monitoring summaries misread distance scores as
  p-values, reversing per-column/prediction flags relative to Evidently's own
  count. Preserved old evidence; marked its statistical alerts untrusted pending
  corrected generation. ADR-029 records the correction without model retraining.
- Added authoritative Evidently decisions and explicit score semantics, forecast
  session persistence with selection invalidation, honest disabled-agent controls,
  and a post-success materialization timestamp. Eighteen focused analyzer/UI
  tests passed in 47.26s, including real Evidently large-sample regressions and
  actual Streamlit AppTest reruns (external services mocked in these unit tests).
- Initial sandbox-local Evidently import blocked in asyncio loopback socketpair;
  interrupted only the two diagnostic processes, then reran with approved local
  loopback access and telemetry disabled. This was an environment restriction,
  not a passed test or a hidden application failure.
- Actual /agent/chat returned HTTP503 agent_disabled; no live agent claimed.
  Owner asked to configure free-provider/shared access credentials or leave blocked.
- Final freshness-source and forecast-rerun checks: 10 passed in 24.52s.
  Ruff, Black formatting and offline manifest validation passed before push.
- Remote branch gained owner commit 7d83bdd (.devcontainer). Initial push was
  rejected; fetched and merged normally as 067e963, preserving both histories and
  the unrelated Oracle edit. Cancelled mis-dispatched old-source monitoring
  39242429156, then started corrected-source monitoring 39242531954.
- CI 39242532012 stopped at Black: two new test files needed the configured
  Python 3.11 target formatting. Applied the exact target; no test failure was
  hidden and monitoring behavior was not changed by the formatting correction.

- Corrected managed monitoring 39242531954 passed: v2 summaries agree with
  Evidently, only hour_of_day feature drift and actual MAE degradation remain;
  prediction drift is false. Direct public readback validated each flag.
- Aggregate 39242666434 verified eight infrastructure/public checks but FAILED
  its real LLM tool assertion. Browser reproduced Groq routing with an invalid
  empty-name argument for the zero-argument pipeline tool; UI incorrectly showed
  OK. Added closed/typed argument validation, strict zero-argument decoding and
  failure propagation, plus a scoped agent verification operation.
- Real Render redeploy loaded the same genuine Production v1 models afresh,
  readiness and both forecasts passed on revision 751a075; this proves hosted
  redeploy recovery, not controlled idle wake latency. Successful materialization
  recorded 2026-10-19T11:18:10.259847+00:00. Owner rebooted Streamlit for new source.

- Agent fix 0bcad87 passed 28 focused agent/API tests (94.09s) and seven cloud
  verifier tests (2.40s); Ruff, exact Python 3.11 Black check and offline manifest
  validation passed. Black CLI required approved loopback access on this host;
  only its two blocked diagnostic invocations were interrupted.
- Full CI 39244387332 on 0bcad87 SUCCEEDED, all three jobs: 388 passed, one
  skipped, 45 warnings in 79.39s, actual disposable Compose integration and
  fixture memory probe. ARM64 run 39244382411 SUCCEEDED.
- Scoped agent run 39244420687 failed its real-tool assertion while public
  Render still reported pre-fix revision 751a075. Kept the failure; no successful
  post-fix hosted tool execution claimed. Public Streamlit still showed older
  labels after owner reboot; requested owner source settings/build-log check.

### 2026-10-19 — owner hosting rollout and continued agent diagnosis

- Owner confirmed both hosting updates. Render now reports 3e66304 with real
  Production models ready; browser shows corrected historical header/tab labels.
- Repeated live Copilot still returned explicit HTTP503; scoped run 39247478996
  failed. It is not a successful agent check. Added opt-in failure diagnostics
  using the owner-configured Groq secret on CI, with only error class/status/code,
  tool argument names and managed tool statuses serialized; no exception text.

- Managed diagnostic 39248151952 identified Groq HTTP400: zero-argument
  schema required=[] rejected after empty-properties normalization. Removed only
  the unnecessary required clause; strict closed-object validation remains.
- Both runner and Render read back missing real online features after the
  original sample keys expired. Feast 0.65 skips equal event timestamps before
  EXPIRE, so prior rematerializations had not renewed retention. Added exact-key
  expiry renewal after successful materialization, preserving values/event times
  and failing on missing keys before recording a successful refresh.
- Two retention regressions passed, proving exact pinned-codec zone/corridor
  keys, no HSET/DELETE and explicit missing-key failure. Full focused suite in
  progress; real managed restoration still requires the following cloud run.

- Seventeen focused retention/agent tests passed (62.77s). Managed aggregate
  39268160345 restored real features and passed eight infrastructure/public
  checks; its agent check still failed against old public source.
- Diagnostic 39268227159 executed real Groq gpt-oss-20b with an empty argument
  object, successfully read nine actual pipeline records from Aiven, and returned
  complete real online fields for zones 7/74/75/166. This is CI agent integration,
  not public Render agent acceptance.
- Browser verified corrected demand table/map remain after ETA 11.9m request;
  genuine three-zone values and 2023 observation timestamps remained intact.
  Corrected monitoring score/MAE labels and embedded HTML also displayed.
- Full CI 39268169927 on e4d5834 and ARM64 39268160966 SUCCEEDED. Added returned
  dependency-error/cloud fallback rejection and five focused checks passed
  (3.04s). Future aggregate evidence includes actual retention counts/TTL.

- CI 39269723316 exposed three old unit assertions that treated unavailable
  localhost PostgreSQL calls as successful tools (390 passed, three failed).
  Preserved strict error propagation; replaced only those unit dependencies with
  explicit successful tool fixtures and verified the requested tools are called.
  Managed database checks remain separate and real. No live download or cloud
  secret is added to required PR unit checks.

- Final source 92bddd7: CI 39270540972 SUCCEEDED (393 passed, one skipped,
  45 warnings in 80.14s), Compose and memory checks passed; ARM64 39270528695
  SUCCEEDED. Render automatically rolled out 92bddd7.
- Public Streamlit Copilot then used real Groq gpt-oss-20b, returned ten actual
  pipeline records and cited warehouse.pipeline_runs (observed latency 1107.3ms).
  Found presentation bug: running/completed_with_alerts were shown as FAILED;
  corrected labels to RUNNING/ALERT, retained actual failures, eight focused
  schema/dependency/status tests passed (1.90s).
- Initial post-redeploy readiness was HTTP503 with no models loaded. A real
  demand request recovered through the existing lazy Production load in 31.55s
  and returned 0.71, model v1, historical source timestamp intact. No dummy model
  or synthetic forecast substituted; the earlier failed readiness is preserved.

- Repeated managed pipeline 39271963729 SUCCEEDED: all 9,232 trips preserved,
  published_events=0, 1,319 exact online keys renewed to 86,400s from prior
  minimum TTL 84,727s, values/event timestamps unchanged.
- Full managed run 39272455345 SUCCEEDED on 92bddd7: all nine automated checks,
  including public Groq tool execution, forecasts plus two matching new PostgreSQL
  rows, Kafka exact-offset roundtrip, real MLflow artifact readback and monitoring
  HTML. Manual acceptance remains false for default-branch cron evidence.
- Copilot integrity follow-up excludes untrusted legacy drift flags from active
  alerts and surfaces monitoring DB failures explicitly. Corrected one further
  old structural-guard unit dependency fixture; malicious tool remains blocked.
  All 76 agent/tool/API checks passed (67.90s); no external fixtures are called
  to make hermetic success assertions pass.

- Final f9a5605 CI 39274527934 SUCCEEDED: 398 passed, one skipped, 45 warnings
  in 54.61s, Compose and memory passed. ARM64 39274521143 SUCCEEDED.
- Final Render revision f9a5605: readiness HTTP200 before/after requests, demand
  0.71 and ETA 715.8s, genuine Production v1 models, no fallback.
- Public Copilot retrieved actual zone 74 Feast values (3 last-hour pickups, 66
  last-24-hour pickups, historical event timestamp) via Groq; separate monitoring
  query returned five real reports, correct MAE metric, normal current prediction
  drift and explicit warning for the legacy prediction-drift report.
- Set repository CLOUD_DEPLOYMENT_ENABLED=true only after all nine real checks
  passed. Read-only GitHub tree confirmed default main lacks free-cloud.yml.
  No scheduled-event execution claimed; protected integration is still required.

- Final owner confirmation: Render shows Free compute and Groq has no payment
  method. Render payment-method/overage settings remain NOT VERIFIED; documented
  official overage behavior without claiming a guaranteed billing cap.
- Final evidence links distinguish the actual Feast screenshot from the later
  pipeline screenshot; retained successful nine-service run source 92bddd7 and
  final public/CI source f9a5605 separately. Complete change inventory refreshed.

### Acceptance continuation — 2026-10-19

- Read dev/main protection rules: strict required checks and admin enforcement;
  preserve feature -> dev -> main. PR166 normal merge refused pending required CI.
- Latest head 7a389f7 CI39278167895 logs 398 passed, one skipped, 45 warnings,
  59.74s, then cleanup/cache-service warning. Compose and memory check success;
  Lint & Test remains in_progress in API. Cancel said completed while direct
  rerun returned HTTP403 already running. No admin bypass or fake success used.
- Timed idle-interval public demand request started 15:50UTC (20:50PKT), HTTP200
  after130.328s; actual Production model load timestamps changed, same f9a5605
  revision. Correct source-confirmed ETA/readiness/deployment requests passed.
  Initial guessed ETA/deployment URLs returned404 and are retained as probe errors.
- PostgreSQL warehouse remains9,232 rows; UI fresh demand3.69/5.55/2.26 and
  ETA715.8s confirmed, retained demand after ETA rerun, screenshot saved.
  Private Render sleep/start logs unavailable: controlled sleep still NOT VERIFIED.
- Owner now confirms Render card attached; Free badge does not cap overages.
  Documented official bandwidth/build billing and pipeline-only spending control.
  No actual charge claimed. Further Render deployments paused pending owner billing
  controls; no new feature, paid resource, payment-method change or deletion.

2026-10-19 subsequent owner confirmation: Render card removed. This resolves
the known attached-card overage exposure on owner attestation, without independent
Billing-page inspection. Official no-card quota exhaustion suspends service/builds.
Free plans preserved; no payment-method change was performed by the agent.

### D6/D7 acceptance continuation — 2026-10-20

- Feature aefb096 full CI39282433306 and ARM6439282424920 passed; unit log398
  passed, one skipped,45warnings76.39s. PR166 merged normally into dev b2e56d4.
- PR167 dev->main passed CI39400299729:398passed, one skipped,45warnings66.56s,
  Compose and memory passed; ARM6439400243947 all passed. Normal merge commit was
  rejected by main linear-history rule; used permitted squash to4450d95 at
  09:31:12UTC (14:31:12PKT). No admin bypass, force push or branch deletion.
- free-cloud.yml now registered active on default main; opt-in true. Legacy VM
  deploy39401266605 skipped. Manually dispatched pipeline39401271976 on main to
  renew online retention before expiry; this is explicitly workflow_dispatch.
- Next genuine pipeline slot20:17PKT today. Owner declined a Codex follow-up and
  will return after that run; no scheduling workaround or fake cron evidence.
- Baseline actual public demand0.71/ETA715.8/ready200 on aefb096; warehouse9232,
  both genuine Productionv1 native models. Quiet interval started09:24:27UTC;
  no agent requests or app pushes during the following planned16minutes.

- Main manual pipeline39401271976 SUCCEEDED: real source checkpoint9,232,
  warehouse9,232, zone features107,155, corridor features8,760, published0;
  materialization09:36:24.899356UTC renewed1,319 keys to86,400s from minimum19,279s.
  Feature values/event timestamps unchanged. Actual summary extracted from log;
  manual event is workflow_dispatch, not schedule. Post-main CI39401266610 passed.

- Controlled agent quiet interval973.664s completed. First demand HTTP200 after
  140.968s with X-Cache MISS; ETA MISS200, readiness200, same aefb096 revision.
  Real Productionv1 models reloaded09:42:29/09:42:35UTC, unchanged artifact run IDs;
  warehouse9,232 preserved. No forced restart or synthetic fallback. Provider
  internal sleep events/other public traffic unobserved; scope recorded explicitly.
- Browser retained an earlier90s timeout before retry (cause/time not inferred).
  Actual retry showed Serving Online (OK), fresh demand3.69/5.55/2.26 at09:44:46UTC,
  map and ETA715.8s at09:45:27UTC; demand retained across ETA rerun. Error and
  recovery screenshots saved. Narrow API check39402814242 on main SUCCEEDED,
  matching two newly persisted PostgreSQL prediction rows. Cron remains unverified.

### D6 cron observation — 2026-10-20 22:34 Pakistan

- Workflow-specific and repository-wide queries show zero free-cloud schedule
  runs after intended 20:17 slot. Active workflow, enabled Actions, default main,
  opt-in true and published cron/blob verified. No archived/disabled repository.
- Cron NOT VERIFIED: no execution record, root cause not established. Official
  delay/drop caveat does not prove the cause. GitHub status reports operational.
- Next configured pipeline slot: 2026-10-21 08:17 Pakistan; earlier event may
  arrive late. No cron edits/toggles, substituted manual job or automation.

### D6/D7 final core acceptance — 2026-10-21

- Genuine schedule run39482347476 SUCCEEDED on main8a9b4d0. Log CRON17 3,15 * * *;
  created2026-10-20 19:18:36UTC /2026-10-21 00:18:36PKT; completed19:22:50UTC.
  Actual creation delay14,496s (4h1m36s). Cause not established; earlier no-run
  observation retained with its timestamp. No manual trigger counted as cron.
- Actual pipeline step passed: warehouse9,232, zone features107,155, corridors8,760,
  next source index9,232, published0. Feature materialization19:22:43.403031UTC
  renewed1,319 exact keys to86,400s from prior minimum51,222s, values/timestamps
  unchanged. Sanitized metadata and log result saved in scheduled-pipeline-evidence.
- Required bounded-profile acceptance checks now have genuine evidence. No uptime
  or punctuality guarantee; optional Gemini/FAISS and separate train/monitor cron
  events remain uncertified. Historical automated artifacts retain original scopes.
- Unrelated current local .env.example and Oracle retry edits remain excluded.

### Resource improvements — 2026-10-22

- Moved monitoring's idempotency check ahead of historical Feast extraction.
  Runtime/dependency digests replace Git SHA so docs-only commits do not trigger
  unnecessary retraining/monitoring. One initial cache miss is expected; no full
  training run was launched to seed the new fingerprint.
- Added an opt-in eight-row genuine Feast PIT query-budget probe using read-only
  SELECT, 8s statement timeout, 2s lock timeout, 2MiB work_mem and no parallel
  gather workers. No feature rebuild, entity-table upload, deletion or stress test.
- Local and managed validation results are recorded after execution. A passing
  bounded probe does not certify the unresolved OOM cause or sustained recovery.

### Resource validation results — 2026-10-22

- Five focused local tests, Ruff/Black and offline cloud-config validation passed.
  Source CI39753862820 on4c169fa:402 passed,1 skipped,45 warnings in57.66s.
- Managed query-budget39753857626 VERIFIED: four zone and four corridor Feast PIT
  rows matched actual offline values and timestamps. Read phases5.68s and2.35s;
  read-only,8s statement/2s lock timeout,2MiB work_mem,no parallel gather workers.
- Scoped public API39754119428 VERIFIED: demand0.71, ETA715.8s, real Productionv1,
  two newly persisted forecast rows matched. No extra load/stress repetitions.
- Read-only health39754124002 VERIFIED: database70,407,871 bytes (~67MiB), two
  backends including diagnostic, zero recorded deadlocks. Postmaster still Oct18.
  Cumulative temporary writes cannot be attributed to this probe from day-apart
  snapshots. No claim of measured peak RAM, solved OOM cause or sustained recovery.
- Sanitized query-budget, API and database-health artifacts committed separately
  from prior acceptance artifacts, whose original scopes remain unchanged.

### Browser acceptance and observed-entity shortcuts — 2026-10-22

- Public Streamlit loaded real demand3.69/5.55/2.26 and ETA715.8s, kept demand
  after ETA rerun, displayed HEALTHY10 recent pipeline runs, Productionv1 models,
  drift trends and actual interactive performance/data-drift HTML reports.
- Initial90s health timeout recovered on rerun; no dummy predictions. A separate
  readiness200 confirmed PostgreSQL/Redis/MLflow and genuine loaded models.
- Original Zone161 quick action returned503. Actual /zones reports161 unobserved,
  while74 is observed. Pipeline question passed in Streamlit with real Groq
  gpt-oss-20b,query_pipeline_status and warehouse.pipeline_runs citation873.3ms.
  Scoped agent check39759151789 also passed; the provider is not globally down.
- Corrected cloud shortcuts to select observed zone/corridor metadata, disable
  missing examples, and distinguish feature inspection from model ETA. Reuse
  the existing /zones read rather than adding a per-rerun request. Seven focused
  offline UI tests and Ruff/Black passed. Live corrected shortcut retest pending.
- No training, rebuilding, destructive action, new account or paid plan used.

### Browser verification results and remaining owner action — 2026-10-22

- All UI fix checks passed on eba7168:source CI39760092043 (403 tests passed,
  one skipped),Compose,512MiB runtime and all ARM64 builds39760078913.
- Additional public Streamlit prompts passed with real Groq:drift1124.8ms,
  zone74 Feast6793.2ms,corridor75_237 Feast1283.7ms. Documentation search746.7ms
  retrieved sources but answer relevance remains limited; FAISS not certified.
- Actual fullscreen map displayed three forecast markers. Captured screenshots
  and accessibility snapshots; full scoped matrix in STREAMLIT_ACCEPTANCE_2026-10-22.md.
- The public app still served original161 shortcuts after reload. Owner reboot
  requested through the account-access question; corrected control deployment
  and retest NOT VERIFIED. Supported feature prompts worked when entered manually.
- HTML download button exercised but saved file not independently confirmed.
  Startup90s timeout recovered; no blanket all-targets/uptime/OOM recovery claim.

### Owner reboot and actual shortcut retest — 2026-10-22

- Owner confirmed reboot. Public dashboard now displayed Zone74 Features and
  Corridor75→237 controls and the updated placeholder; Serving Online (OK).
- Clicked both actual buttons. Zone74:real Groq gpt-oss-20b,get_features,live Redis
  hit,15m3/1h3/24h66/weekly1,14487.0ms,statussuccess,zone-feature citation.
- Corridor75_237:real Groq,get_features,live Redis hit,duration1003s,distance3.14km,
  demand pressure9,14697.2ms,statussuccess,corridor-feature citation. Both retain
  genuine historical observation timestamp Jan31 2023 23:00UTC.
- Corrected control deployment/retest VERIFIED; source-fix owner blocker resolved.
  Earlier failed shortcut and initial timeout retained as historical observations.
  No claim of sustained OOM recovery, perfect document QA or verified saved HTML.

## 2026-10-22 — GitHub About and deployment README

Updated repository About with a concise platform description, public Streamlit
website and relevant technology topics. Rewrote README around the actual deployed
Free profile, historical sample provenance, current dashboard tabs, managed
architecture, real API routes and dated verification evidence. Removed unqualified
real-time, latency, mock-uptime and obsolete benchmark claims from the project
landing page. Preserved reference-stack documentation links and explicitly retained
the open PostgreSQL OOM issue, cold-start behavior and verification limits.
Corrected the stale setup introduction that still claimed no public application.
No managed test, training, data deletion, resource provisioning or plan change was
performed for this documentation update. Validation: README relative links and
endpoint methods checked against repository files/source; Git diff whitespace
check passed. Publication follows the existing protected PR workflow.

## 2026-10-24 — HEAD-only uptime monitor compatibility

Owner screenshots showed a UptimeRobot incident for HEAD `/health`: 405 with
Allow: GET. Three bounded live requests confirmed HEAD `/health` 405, GET
`/health` 200 with status ok (7.99s), and GET `/health/live` 200 (0.26s).
This establishes a method mismatch, not six hours of proven application downtime.
Added explicit HEAD handlers for the three health routes. Readiness reuses GET's
checks and status; liveness contacts no dependencies; legacy diagnostic status
is preserved. HEAD responses contain no body. Documented HEAD `/health/ready`
as the required monitor target. No managed training, prediction or data change.
Initial local test collection was blocked by sandbox access to Feast metrics
files; rerun outside that restriction uses mocked dependencies only.
Validation: all 10 focused offline tests passed (17.96s); Ruff and Black passed
for changed Python files, and Git whitespace validation passed. Managed-service
and deployed HEAD checks remain pending until the new commit is served.

## 2026-10-27 — Responsive browser review

Public app woke successfully after Community Cloud inactivity. Reproduced cramped
five-column quick actions and truncated monitoring values at768px with sidebar
open. Implemented content-width wrapping, auto sidebar, concise tab labels,
metric wrapping, mobile spacing and44px action targets. Preserved real alerts,
map, forecast rerun state and native scrolling. Added forecast request feedback
and an accurate cold-start caption. No backend or architecture replacement.

Candidate preview measured seven viewport sizes320–1440px without page-level
horizontal overflow. Detailed browser checks confirmed tablet sidebar layout,
320px tab navigation/full MAE warning, real demand3.69/5.55/2.26, mobile map,
real ETA11.9min with retained demand, pipeline history and stored report HTML.
Saved HTML download remains NOT VERIFIED after a browser event-capture timeout.
No local agent credentials were copied; the local Copilot stays disabled.
The responsive report separates candidate evidence from pending public deployment.
Seven focused offline UI tests passed; Ruff, Black and whitespace checks passed.
A public Copilot query used real Groq openai/gpt-oss-20b in1337.4ms and cited
warehouse.pipeline_runs. Candidate viewport checks and actual before/after images
are in docs/RESPONSIVE_REVIEW_2026-10-27.md. Public updated-style acceptance is
still pending; no hosted result is inferred from local screenshots.
