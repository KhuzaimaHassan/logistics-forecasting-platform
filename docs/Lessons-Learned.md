# Lessons Learned

Filled in per milestone, not at the end — the point is to capture what actually happened vs. what the docs predicted, while it's fresh. Empty right now since no phase has started.

## Template (copy per milestone)

### M# — <Milestone name>

**What went as planned:**

**What didn't, and why:**

**What would change if starting this phase over:**

**Docs that needed updating after the fact:**
<!-- Link back to any Decisions.md entries or doc edits this milestone caused -->

---

<!-- Entries appended below as each milestone completes -->
 
### M0 — Infra Bootstrap

**What went as planned:**
- Multi-service single-host Docker Compose stack (10 services) configured cleanly using official images and colocated multi-arch service Dockerfiles.
- Fast Python environment and tooling established with `uv`, `black`, `ruff`, and `pytest`.
- Automated CI workflow executing both Python lint/testing and full Docker Compose multi-container build and health smoke test with zero startup crashes.
- Host provisioning and security automation on Oracle Cloud Ampere A1 automated via `infra/oracle-vm/provision.sh`.

**What didn't, and why:**
- *Dependency Bloat:* A single flat dependency list caused lightweight services (serving API, UI) to install 220+ packages across training/streaming libraries. Resolved in ADR-009 by structuring scoped optional dependency groups (`core`, `extract`, `transform`, `serving`, `ui`, `training`, `monitoring`, `dev`) while maintaining a single root lockfile.
- *Docker Build Context & Hatchling Wheel Caching:* Service Docker builds failed in container environments because Hatchling required `README.md` and attempted early wheel installation before service code was copied. Resolved by copying `README.md` into build contexts and passing `--no-install-project` during dependency caching layers.
- *Insecure Compose Credential Fallbacks:* Fallback defaults (`${VAR:-default}`) allowed compose stacks to silently run with unvalidated placeholder passwords. Resolved by strictly enforcing `${VAR:?VAR must be set}` syntax across database credentials and introducing `.env.ci` for automated CI execution.
- *Squash-Merge History Divergence:* Squash-merging `dev` into `main` produced distinct commit SHAs on `main` that caused 3-way merge conflicts on subsequent PRs from `dev`. Resolved by establishing a standing post-squash-merge sync PR rule.

**What would change if starting this phase over:**
- Define scoped optional dependency groups in `pyproject.toml` from day one rather than refactoring later.
- Enforce explicit fail-fast credential patterns (`${VAR:?error}`) from the initial compose file draft.

**Docs that needed updating after the fact:**
- [Decisions.md (ADR-008, ADR-009)](file:///docs/Decisions.md)
- [GitHub-Setup.md (Branch Strategy & Post-Squash Sync Rule)](file:///docs/GitHub-Setup.md)
- [Roadmap.md (Build-Layer Caching Note & Phase 0 Status)](file:///docs/Roadmap.md)
- [Deployment.md & Security.md](file:///docs/Deployment.md)
