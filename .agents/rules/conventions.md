# Git & Repository Conventions

These conventions apply to all branches, commits, pull requests, and development workflows in this repository.

## 1. Branch Strategy

- **`main`**: Production branch. Always deployable. Protected: no direct pushes allowed, PR + passing CI required.
- **`dev`**: Active integration branch for the current milestone.
- **`feature/<milestone-number>-<short-name>`**: Feature branches (e.g., `feature/00-infra-bootstrap`, `feature/01-historical-etl`).
  - Branched from: `dev`
  - Merged into: `dev` via Pull Request
- **Milestone Integration**: `dev` is merged into `main` at the conclusion of each milestone after validation and review.

## 2. Commit Message Format

Follow the Conventional Commits specification:
- `feat`: New user-facing feature or pipeline capability (e.g., `feat(extract): add TLC monthly batch puller`)
- `fix`: Bug fix in code or configuration (e.g., `fix(serving): resolve Redis connection timeout`)
- `docs`: Documentation updates or ADR additions (e.g., `docs: add ADR-008 for Caddy and multi-arch`)
- `chore`: Tooling, dependency, or configuration changes (e.g., `chore: configure uv and pyproject.toml`)
- `test`: Adding or updating test suites (e.g., `test(transform): add outlier filter tests`)

Structure:
```
<type>(<optional-scope>): <clear, imperative description>

[optional body explaining rationale and context]
```

## 3. Pull Request Conventions

- **Target Branch**: Feature PRs target `dev`. Milestone completion PRs target `main`.
- **Issue Reference**: Every PR must reference the relevant milestone or sub-task tracking issue:
  - `Closes #<issue_number>` for complete sub-tasks/milestones
  - `Relates to #<issue_number>` for partial or incremental work
- **CI Gate**: GitHub Actions CI (linting with `ruff`/`black`, type checks, and `pytest`) must pass cleanly before any PR is merged.
- **Code Reviews**: Pre-commit validation and code quality checks must be verified prior to approval.
