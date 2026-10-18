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
