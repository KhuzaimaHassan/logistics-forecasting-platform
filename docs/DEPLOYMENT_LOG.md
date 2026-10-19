# Free cloud deployment log

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
