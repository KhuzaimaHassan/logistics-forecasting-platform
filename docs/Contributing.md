# Contributing

Solo project, but kept to the same discipline as a team repo — mainly so the habit transfers, and so the repo reads well to anyone (recruiters, collaborators) who opens it later.

## Workflow

1. Pick up a sub-task from the current milestone's tracking issue (see GitHub-Setup.md).
2. Branch from `dev`: `feature/<milestone-number>-<short-name>`.
3. Commit using conventional commits (`feat:`, `fix:`, `docs:`, `chore:`, `test:`).
4. Open a PR into `dev`, reference the milestone issue.
5. CI must pass before merge (see GitHub-Setup.md for what CI checks).
6. Merge `dev` → `main` at the end of each milestone, after a manual demo/review pass.

## Code style & Tooling
 
- Python package manager: `uv` with a single root `pyproject.toml` managing project dependencies.
- Code formatting & linting: `black` + `ruff` configured in `pyproject.toml`, enforced in CI.
- Docker: All Dockerfiles must use official multi-arch (`amd64`/`arm64`) base images (for local x86_64 dev and Oracle Ampere A1 ARM64 deployment).
- Docstrings on all public functions in `src/` — not optional, given how much of this project's value is "can someone else (or future-you) read this pipeline."

## Documentation discipline

- Any architecture decision that deviates from what's written in `docs/` gets a new entry in `Decisions.md`, not a silent code change. This is the rule that keeps the docs trustworthy over the life of the project.
