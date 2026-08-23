# Multi-arch base image (amd64/arm64)
FROM python:3.11-slim

WORKDIR /app

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Ensure the virtualenv created by uv is on PATH
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app"

# Copy project definition, lockfile, and readme
COPY pyproject.toml uv.lock README.md ./

# Install dependencies using the frozen lockfile scoped to required components
RUN uv sync --frozen --no-dev --no-install-project --extra core --extra extract --extra transform --extra orchestration

# Copy application source
COPY src/ /app/src/

# Default entrypoint starts the Prefect worker polling the default agent work pool
CMD ["prefect", "worker", "start", "--pool", "default-agent-pool"]
