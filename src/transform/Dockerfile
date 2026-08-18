# Multi-arch base image (amd64/arm64)
FROM python:3.11-slim

WORKDIR /app

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Ensure the virtualenv created by uv is on PATH
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

# Copy project definition and validated lockfile
COPY pyproject.toml uv.lock ./

# Install dependencies using the frozen lockfile scoped to transform
RUN uv sync --frozen --no-dev --extra core --extra transform

# Copy application source
COPY src/ /app/src/

# Default entrypoint (to be filled in during Phase 4)
CMD ["python", "-m", "src.transform.stream_consumer"]
