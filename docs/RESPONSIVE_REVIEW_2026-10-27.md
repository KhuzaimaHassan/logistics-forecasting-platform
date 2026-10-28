# Responsive browser review — 2026-10-27

## Scope

Inspected the real public Streamlit application in the in-app Chromium browser,
then tested a local preview of the candidate UI against the actual public API.
This is viewport testing, not physical-device, Safari or Firefox certification.
No Docker, training, pipeline dispatch, load test, deletion or paid service was used.

The public app initially showed Community Cloud's sleep screen. Waking it
succeeded and the sidebar reported Serving Online (OK). Hosting wake-up latency
is separate from responsive layout behavior.

## Reproduced issues and changes

- At 768×1024 with the sidebar open, five fixed columns split button labels into
  tiny fragments. Monitoring cards truncated Normal/Performance Decay and MAE
  details. Before screenshots capture both failures.
- Add content-width container queries, so an open sidebar is accounted for when
  wrapping columns. Use collapsed sidebar initialization, shorter tab labels, readable
  metric values/deltas, smaller mobile headings and 44px mobile action/tab targets.
- Preserve native tab scrolling, table scrolling, charts, maps and report embeds.
  Do not hide overflow globally or remove alerts to make the page fit.
- Correct the promotion shortcut to ask for repository policy without assuming an
  unrelated ADR number. Add request spinners and describe cold starts as possibly
  taking a few minutes.

Styles use Streamlit data-testid attributes rather than generated class names.
These DOM hooks are not a guaranteed external API; repeat browser checks when
upgrading Streamlit. The existing dependency lock is unchanged.

## Candidate measurements

| Viewport | Document width | Result |
|---|---|---|
| 320×740 | 320 | No page-level horizontal overflow |
| 360×800 | 360 | No page-level horizontal overflow |
| 390×844 | 390 | No page-level horizontal overflow |
| 768×1024 | 768 | No page-level horizontal overflow |
| 844×390 | 844 | No page-level horizontal overflow |
| 1024×768 | 1024 | No page-level horizontal overflow |
| 1440×900 | 1440 | No page-level horizontal overflow |

[DOM measurements](evidence/responsive-2026-10-27/local-measurements.json) include
actual dimensions and sidebar state; an intentionally opened sidebar is preserved
across some resizes. Not every interaction was repeated at all seven sizes.

## Interaction results

| Check | Result | Evidence / limitation |
|---|---|---|
| Tablet sidebar open | VERIFIED locally | Buttons wrap into readable rows instead of splitting words |
| Monitoring labels | VERIFIED locally | Full Performance Decay and +29.9% MAE details visible at 320px; 1440px metric children use normal wrapping |
| Narrow tab navigation | VERIFIED locally | Native Scroll tabs right reached Monitoring at 320px; tab heights 44px at 390px |
| Demand forecast | VERIFIED locally against real API | Zones74/75/166 returned3.69/5.55/2.26, modelv1, score2026-10-27T15:49:57.842875UTC |
| Map | VERIFIED locally | Three genuine prediction markers; mobile canvases348px wide within390px viewport |
| ETA + rerun persistence | VERIFIED locally against real API | 75_237 rendered11.9minutes (11.93 in response); demand rows remained after ETA rerun |
| Pipeline history | VERIFIED locally against real API | HEALTHY,10recent runs; table scroll stays inside page |
| Monitoring report | VERIFIED locally against real API | Actual stored performance HTML rendered;29.9% alert preserved |
| Saved HTML download | NOT VERIFIED | Browser download-event capture timed out; no saved file located. Do not label as passed or proven broken |
| Public Copilot pipeline query | VERIFIED on existing public UI | Real Groq openai/gpt-oss-20b,1337.4ms,tool/citation metadata and warehouse.pipeline_runs; returned genuine recent runs |
| Local Copilot | NOT VERIFIED | Intentionally disabled: no owner agent credential copied to local preview |
| Public responsive layout | VERIFIED after owner reboot | Seven public viewport measurements320–1440px have no page-level overflow;320px native tab scrolling and complete MAE warning passed |
| Public demand/ETA/map | VERIFIED after owner reboot | Real modelv1 demand3.69/5.55/2.26 at2026-10-27T16:18:56.887982UTC; ETA11.9min; demand retained |
| Final sidebar rerun | VERIFIED publicly October 28 | At390px, manually closed sidebar stayed hidden during and after a real ETA rerun; prior open preference persisted on reload, so fresh-user default is not certified |

## Screenshots

- [Public tablet buttons before](evidence/responsive-2026-10-27/tablet-sidebar-before.png)
- [Public tablet monitoring before](evidence/responsive-2026-10-27/tablet-monitoring-before.png)
- [Candidate tablet buttons after](evidence/responsive-2026-10-27/tablet-sidebar-local-after.png)
- [Candidate tablet monitoring after](evidence/responsive-2026-10-27/tablet-monitoring-local-after.png)
- [Candidate mobile real demand](evidence/responsive-2026-10-27/mobile-demand-local-after.png)
- [Candidate mobile map and ETA](evidence/responsive-2026-10-27/mobile-map-eta-local-after.png)
- [Candidate mobile pipeline](evidence/responsive-2026-10-27/mobile-pipeline-local-after.png)
- [Candidate 320px MAE warning](evidence/responsive-2026-10-27/mobile-320-monitoring-local-after.png)
- [Public real-provider response](evidence/responsive-2026-10-27/public-copilot.txt)

## Offline validation

Seven existing focused UI tests passed (two forecast/rerun tests and five
monitoring-score tests). Ruff, Black and whitespace checks passed for the changed
Python source. An attempted nonexistent extra test path collected no tests and
was corrected; it is not counted as a pass. No new implementation-mirroring CSS
unit tests were added; actual browser evidence checks the rendered behavior.

## Public follow-up

The owner rebooted Community Cloud and the new tabs/styles appeared. Actual
[public viewport measurements](evidence/responsive-2026-10-27/public-measurements.json)
show no page overflow at all seven sizes. At768px with sidebar open, actions
were426px wide instead of fragmented columns. At320px, metric child text did
not overflow and Performance Decay/+29.9% remained visible. Public demand/ETA/map
and rerun persistence passed. The auto sidebar reopened over the mobile results
after ETA; the final revision uses an explicitly collapsed default, preserving
the manual toggle. On October 28, the rebooted app's real ETA rerun returned
11.93 minutes from model v1 (prediction timestamp2026-10-28T08:40:36.591892UTC).
After closing the sidebar using its UI toggle, it remained hidden during and
after the request (aria-expanded=false, x=-300px). Document width and scroll width
both measured390px. This browser retained its prior open preference on reload;
the test certifies rerun behavior, not a clean browser's initial preference.
No extra training, ingestion, stress test or resource change was performed.

- [Final mobile ETA/sidebar screenshot](evidence/responsive-2026-10-27/public-mobile-final-sidebar.png)
- [Final DOM measurements](evidence/responsive-2026-10-27/public-mobile-final-sidebar.json)

- [Actual public320px warning](evidence/responsive-2026-10-27/public-320-monitoring.png)
- [Public real demand](evidence/responsive-2026-10-27/public-demand.png)
- [Public ETA with reopened sidebar](evidence/responsive-2026-10-27/public-map-eta.png)

## Verification limits

No claim of comprehensive accessibility, touch-keyboard behavior, hosted FAISS,
all agent tool paths, sustained PostgreSQL memory stability or improved model
quality is made. Native report HTML may still require scrolling within its own
iframe. Free-host startup and external API/provider latency remain possible.
