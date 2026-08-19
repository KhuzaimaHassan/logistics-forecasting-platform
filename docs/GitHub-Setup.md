# GitHub Setup

## 1. Repo structure

```
logistics-forecasting-platform/
├── docs/           # this docs folder
├── src/            # common, extract, transform, features, training, serving, agents, monitoring, orchestration
│                   # (each service's Dockerfile lives with its code, e.g. src/<service>/Dockerfile)
├── ui/             # Streamlit app (ui/Dockerfile)
├── infra/          # Docker Compose (docker-compose.yml at root/infra), Oracle VM setup scripts
├── tests/
└── .github/
    ├── workflows/       # CI/CD (triggers on pull_request to dev & main, push to main)
    └── ISSUE_TEMPLATE/  # milestone issue template
```

## 2. Branch strategy

- `main` — always deployable. Protected: no direct pushes, PR + passing CI required.
- `dev` — integration branch for the current milestone.
- `feature/<milestone-number>-<short-name>` — e.g. `feature/02-historical-etl`. Branched from `dev`, PR'd back into `dev`.
- Fixes to already-merged milestone infrastructure (security, correctness) sync `dev` → `main` promptly via PR, same as any other PR; the "merge at milestone completion" rule applies to accumulating new milestone feature work, not patching what's already shipped.
- Immediately after every `dev` → `main` squash-merge, sync `dev` with a "chore: sync dev with main after squash-merge" PR merging `origin/main` into `dev` — before starting any new feature work on `dev`. This is a required standing step at the close of every milestone because squash-merging creates a new commit on `main`.



## 3. Milestones ↔ Issues (same pattern as Quasar)

Every phase from the Roadmap becomes a **GitHub Milestone**, and every milestone gets **one tracking issue opened at the start of that phase**, with sub-tasks as a checklist. Sub-tasks that turn out to need real independent discussion get split into their own linked issue; otherwise they stay as checklist items on the milestone issue.

| Milestone | Roadmap Phase | Tracking issue title |
|---|---|---|
| M0 — Infra Bootstrap | Phase 0 | `[M0] Infra Bootstrap: Oracle VM, Docker Compose skeleton, CI skeleton` |
| M1 — Historical ETL | Phase 1 | `[M1] Historical ETL: TLC data pull, clean, load to Postgres` |
| M2 — Feature Store | Phase 2 | `[M2] Feature Store: Feast definitions, offline materialization` |
| M3 — Baseline Models | Phase 3 | `[M3] Baseline Models: demand + duration, MLflow tracking` |
| M4 — Real-Time Layer | Phase 4 | `[M4] Real-Time Layer: Redpanda, replay + live feed producer/consumer` |
| M5 — Online Serving | Phase 5 | `[M5] Online Serving: FastAPI + Feast online store` |
| M6 — CI/CD | Phase 6 | `[M6] CI/CD: GitHub Actions build/test/deploy, scheduled retrain` |
| M7 — Agent Layer | Phase 7 | `[M7] Agent Layer: LangGraph Ops Copilot` |
| M8 — Monitoring | Phase 8 | `[M8] Monitoring: Evidently drift reports + dashboard` |
| M9 — Polish | Phase 9 | `[M9] Polish: README, architecture diagram, demo, write-up` |

Each GitHub Milestone gets a due-date-free target (personal project, no external deadline pressure) and its description links back to the relevant `docs/*.md` file(s) for that phase.

## 4. Issue template

See `.github/ISSUE_TEMPLATE/milestone.md` — every milestone tracking issue uses this template: goal, acceptance criteria, checklist of sub-tasks, linked docs.

## 5. GitHub Actions secrets needed

| Secret | Used for |
|---|---|
| `ORACLE_VM_SSH_KEY` | Deploy workflow |
| `ORACLE_VM_HOST` | Deploy workflow |
| `PREFECT_API_KEY` | Prefect Cloud auth & worker flow registration |
| `PREFECT_API_URL` | Prefect Cloud workspace API endpoint |
| `GROQ_API_KEY` | Agent tests/CI (if any live-call tests exist) |
| `GEMINI_API_KEY` | Agent tests/CI |
| `MTA_API_KEY` | Streaming producer (not needed in CI, but documented here for completeness) |

Actual values live in GitHub repo secrets, never committed — see Security.md.

## 6. Commit / PR conventions

- Conventional commits (`feat:`, `fix:`, `docs:`, `chore:`, `test:`) — searchable history, and pairs cleanly with milestone tracking.
- PR description references the milestone issue (`Closes #<issue>` or `Relates to #<issue>` for partial work).
- CI workflow triggers on `pull_request` targeting `dev` and `main`, as well as on `push` to `main`.

## 7. Open questions

- Whether to auto-close milestone issues via `Closes #N` on the final PR of that phase, or close manually after a demo/review pass — leaning manual close, to actually verify the phase is demoable before marking it done.
