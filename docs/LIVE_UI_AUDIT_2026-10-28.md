# Live dashboard audit — 2026-10-28

Target: https://logistics-forecasting-platform.streamlit.app/ in the in-app Chromium
browser. This audit uses the public application, not a local preview. It is a
small functional sample, not a load test or physical-device certification.

## Responsive layout

All four tabs were opened at each size below. Saved DOM measurements have four
records per viewport; document scroll width equals viewport width in all 28 checks.

| Viewport | Tabs checked | Page overflow |
|---|---|---|
| 320×740 | 4 | None |
| 360×800 | 4 | None |
| 390×844 | 4 | None |
| 768×1024 | 4 | None |
| 844×390 | 4 | None |
| 1024×768 | 4 | None |
| 1440×900 | 4 | None |

Measurements are in [the evidence directory](evidence/live-ui-2026-10-28/),
`layout-<width>.json`. Two long automation batches timed out; incomplete results
were not inferred as passed. Smaller batches saved all 28 measurements.

Screenshot inspection exposed a defect that page overflow checks missed: the
768px monitoring card displayed an ellipsis in “Performance Decay”. The metric
wrapper and child div already allowed wrapping, but the nested Markdown `p`
still used `white-space: nowrap; overflow: hidden; text-overflow: ellipsis`.
The targeted CSS correction extends the existing rule to metric Markdown
paragraphs. After the owner reboot, the public768px paragraph used normal wrapping,
visible overflow and clipped text-overflow styling; the complete label occupied
two lines (65.6px high). The actual screenshot confirms the full words, not only
DOM text. The320px monitoring view still fit, and at768px with sidebar open all
five Copilot shortcuts measured425.6px wide. Evidence: `tablet-metric-after.png`,
`tablet-metric-after.json`, `mobile-monitoring-after.png`, `tablet-sidebar-actions.png`.
After deployment, all seven sizes were checked again for the actual metric
paragraphs: no ellipsis styling or paragraph/page overflow remained. These
measurements are saved in `metric-retest.json`.
This supersedes the earlier assumption that checking the immediate metric child
alone established complete label visibility.

## Functional results

| Feature | Status | Actual evidence |
|---|---|---|
| Sidebar / registry / tools panels | VERIFIED | Opens/closes; Production v1 for demand and duration; five allowlisted tools shown |
| Demand prediction | VERIFIED | Zones74/75/166:3.69/5.55/2.26, modelv1, scored2026-10-28T09:12:01.686019UTC |
| Map | VERIFIED | Public UI rendered the prediction map and zoom controls |
| ETA / demand persistence | VERIFIED | 75_237:11.93min, modelv1, timestamp2026-10-28T09:12:40.230506UTC; demand table retained |
| Pipeline history | VERIFIED | HEALTHY;10 persisted recent runs, including scheduled Kafka jobs and monitoring with alerts |
| Monitoring scorecards / trends | VERIFIED after fix | Feature/prediction drift normal; genuine29.9% MAE decay and retrain recommendation; tablet label clipping fixed and publicly retested |
| Performance HTML report | VERIFIED | Stored regression report rendered model quality and error charts |
| Prediction-drift report selection | VERIFIED | Selectedc5ffe03f; genuine HTML with predicted_value Wasserstein score0.023155 and no drift |
| Feature-drift report selection | VERIFIED | Selected093e930c; genuine HTML with eight feature rows and named statistical tests |
| HTML report download | NOT VERIFIED | One download action; browser event wait timed out after10s. No saved file confirmed; not counted as success or proven product failure |
| Copilot zone shortcut / metadata | VERIFIED | Groq openai/gpt-oss-20b,13039.6ms,OK;get_features;feast_redis:zone_features citation |
| Copilot corridor shortcut | VERIFIED | Groq720.8ms; actual75_237 features;feast_redis:corridor_features citation |
| Copilot retraining shortcut | FAILED answer relevance | Groq1125.3ms returned HEALTHY pipeline history/Kafka jobs, not evidence that retraining ran this week; no retraining success inferred |
| Copilot drift shortcut | VERIFIED | Groq1033.0ms; five stored monitoring records;29.9% decay;warehouse.monitoring_reports citation |
| Copilot promotion shortcut | FAILED answer relevance | Groq888.8ms retrieved four document citations, including broad deployment/template excerpts; retrieval worked but did not clearly answer promotion gates |
| Custom chat / persisted predictions | VERIFIED | Groq1086.1ms;warehouse.predictions returned zone74's3.69 prediction at the exact audit timestamp and two older records |
| Conversation metadata / clear history | VERIFIED | Tools/citations expand; messages persist across queries; clearing this audit's session leaves zero chat messages |
| Empty selection guard / selection search | VERIFIED | Clear all disables demand request after rerun; searching74 shows actual74/174 options; selecting74 re-enables request |
| Restored single-zone prediction | VERIFIED | Zone74 returned3.69 from modelv1 at2026-10-28T09:53:53.886808UTC; actual basemap/marker and zoom controls rendered |
| Chart data / fullscreen / alert expander | VERIFIED | Visible toolbar opens full-screen trend then five-row data table; alert-reason details close and reopen |
| Newly corrected tablet label | VERIFIED | Public768px screenshot shows full Performance Decay on two lines after owner reboot |
| Prediction CSV export / table search | NOT VERIFIED | CSV file completion not attempted; search-control attempt did not visibly open search and is not counted as passed |

Raw accessibility snapshots and screenshots accompany this report. Predictions
use observed January2023 TLC features; current prediction timestamps do not imply
live NYC observations. Provider/tool labels are inspected alongside the response,
not treated as independent certification of every backend path.

## Local verification and limits

Seven existing focused forecast/rerun and monitoring UI tests passed in30.82s.
The process exited0, then Python's temporary-directory finalizer emitted a Windows
permission error during shutdown. No managed training, pipeline dispatch, service
creation, data deletion, secret copying or plan change was performed.
Ruff, Black and Git whitespace checks passed. A pending corridor request was
interrupted by the owner reboot; the completed post-reboot request above is the
one counted. The download-event wait timed out; later chart controls required
retargeting the visible toolbar after inspection before the state changed.

No claim of physical-phone, Safari/Firefox, comprehensive accessibility, sustained
database memory stability, full model quality or every possible agent query is
made. Embedded Evidently HTML and wide data tables can scroll internally.
