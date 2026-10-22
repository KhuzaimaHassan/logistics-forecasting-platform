# Live Streamlit acceptance — 2026-10-22

Tested the public [dashboard](https://logistics-forecasting-platform.streamlit.app/)
through browser controls against the bounded Free-cloud targets. These are actual
observations, not mocked tests. No training, feature rebuild, load test, deletion
or paid resource was used.

**Outcome: core paths work, but blanket acceptance is not complete.** An original
Copilot shortcut selected an unobserved entity and failed. A fix is published in
[PR174](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/pull/174);
the public page still showed the old shortcuts after reload. Owner Community
Cloud reboot and the corrected control-level retest remain **NOT VERIFIED**.

| Target | Status | Actual observation |
|---|---|---|
| Public dashboard | VERIFIED | Loaded without application sign-in; backend recovered to Serving Online (OK) |
| Demand inference | VERIFIED | Browser requested zones74/75/166:3.69/5.55/2.26, all status=ok, modelv1. Observations Jan31 2023 23:00UTC; score09:49:30.640451UTC and later10:10:47.772891UTC |
| Forecast map | VERIFIED | Three blue markers rendered in NYC on the actual interactive map; fullscreen control worked. Zone74 metadata40.801169,-73.937346 came from the public API |
| Corridor ETA | VERIFIED | Browser requested75_237:715.8s /11.93min, rendered11.9min, status=ok, modelv1, cache_hit=true. Demand remained visible after ETA rerun |
| Feature/data provenance | VERIFIED | UI labelled the bounded historical sample and Jan2023 observed window; actual feature_materialized_at2026-10-22T08:44:24.773562Z |
| Model registry/readiness | VERIFIED | Sidebar showed both genuine Productionv1 models. Separate readiness200 after62.55s reported PostgreSQL/Redis connected, MLflow reachable, is_fallback=false, models loaded09:48:39/09:48:44UTC |
| Pipeline history | VERIFIED | HEALTHY10 recent records, actual completed Kafka sample jobs and completed_with_alerts monitoring; manual runs not presented as cron proof |
| Monitoring scorecards | VERIFIED | Feature share12.5% (1/8), prediction drift score0.0232 normal; actual MAE decay29.9% alert, not suppressed |
| Interactive reports | VERIFIED | Performance HTML and data-drift HTML rendered. Selected data report65f75419:dataset threshold0.4, one of eight columns drifted, distance methods displayed. Nine reports offered |
| HTML file download | NOT VERIFIED | Button exercised; no saved file was independently located. Embedded HTML viewer is verified separately; no assertion that download failed |
| Copilot pipeline query | VERIFIED | Real Groq openai/gpt-oss-20b,873.3ms, query_pipeline_status, warehouse.pipeline_runs citation, ten genuine records |
| Copilot drift query | VERIFIED | Real Groq,1124.8ms, five stored reports including actual29.9% decay warning, warehouse.monitoring_reports citation |
| Copilot zone features | VERIFIED | Real Groq,6793.2ms, get_features, feast_redis:zone_features. Zone74 live Redis hit:15m3,1h3,24h66,weekly1; observed Jan31 23:00UTC |
| Copilot corridor features | VERIFIED | Exact proposed corridor prompt submitted manually on existing UI:real Groq,1283.7ms,75_237 live Redis hit,1h/15m duration1003s,distance3.1400001km,demand pressure9; feast_redis:corridor_features citation |
| Documentation retrieval | VERIFIED | Real Groq746.7ms returned repository snippets and citations. Limited answer relevance: broad snippets did not precisely explain the promotion rule. FAISS execution not verified |
| Original zone161 shortcut | FAILED | HTTP503, no offline substitute. Separate actual /zones read reports161 has_observed_features=false,74=true. Other agent queries succeeded; this is not evidence of a global provider outage |
| Corrected shortcuts deployed | NOT VERIFIED | Offline helper/AppTest checks cover observed entity selection, empty metadata and preserved forecast reruns. Seven focused tests passed; source CI403 passed,1 skipped,45 warnings. Existing public page still showed161 after reload |
| First-load timeout | FAILED | Initial health read exceeded90s and sidebar showed Serving Offline. Later readiness200 and a normal rerun recovered. Provider-internal sleep/deploy cause not observed; do not call this seamless startup |
| Sustained RAM/OOM recovery | NOT VERIFIED | No provider RAM trace or reproduction during this browser session. Prior PostgreSQL incident remains open |

The combined zone/corridor question returned zone74 only. The subsequent
single-corridor prompt verified the corridor separately. Do not claim complete
multi-entity conversational planning from this test.

## Scope against the complete platform

The dashboard proves public interaction with inference, online features, model
metadata, stored pipeline history and monitoring. It cannot independently prove
fresh Kafka produce/consume, MLflow metric/artifact writes, full training, cron
trigger identity, billing controls or host RAM. Those have separately dated
evidence in [DEPLOYMENT_REPORT.md](DEPLOYMENT_REPORT.md). This session deliberately
did not rerun heavy jobs to inflate the acceptance claim.

The additional narrow public-agent API check
[39759151789](https://github.com/KhuzaimaHassan/logistics-forecasting-platform/actions/runs/39759151789)
passed actual Groq routing and pipeline-tool access. Its raw artifact retains
agent_only scope and does not stand in for browser acceptance.

## Fix and remaining account action

Cloud examples now select an observed zone (prefer74) and materialized corridor
(prefer75_237), disabling unavailable examples. The existing zone request is
reused, with no extra request per rerun. Feature inspection is labelled separately
from the real-model forecasts on Forecasts & Map. Local examples remain available.

Owner action pending: Community Cloud → Manage app → Reboot app on the existing
Free app. Confirm the new Zone74 Features / Corridor75→237 labels, then retest
those controls. Supported prompts already work when entered manually; that does
not prove deployment of the new controls. No credentials should be shared.

## Captured evidence

- [Demand results](evidence/streamlit-2026-10-22-demand.png)
- [Actual map markers](evidence/streamlit-2026-10-22-map.png)
- [ETA](evidence/streamlit-2026-10-22-eta.png)
- [Pipeline](evidence/streamlit-2026-10-22-pipeline.png)
- [Monitoring](evidence/streamlit-2026-10-22-monitoring.png)
- [Actual drift report accessibility snapshot](evidence/streamlit-2026-10-22-drift.txt)
- [Original shortcut failure](evidence/streamlit-2026-10-22-copilot-failure.png)
- [Successful Copilot/tool citation](evidence/streamlit-2026-10-22-agent-success.png)
- [Actual Feast feature result](evidence/streamlit-2026-10-22-agent-features.png)
- [Corridor response snapshot](evidence/streamlit-2026-10-22-agent-corridor.txt)
- [Raw scoped API artifact](streamlit-agent-api-check-evidence.json)
