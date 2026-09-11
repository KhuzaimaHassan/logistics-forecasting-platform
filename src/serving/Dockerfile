# Multi-arch base image (amd64/arm64)
FROM python:3.11-slim

WORKDIR /app

# Install system runtime dependencies required by LightGBM (OpenMP) and container healthchecks
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Ensure the virtualenv created by uv is on PATH
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app"

# Copy project definition, lockfile, and readme
COPY pyproject.toml uv.lock README.md ./

# Install dependencies using the frozen lockfile scoped to serving and features
RUN uv sync --frozen --no-dev --no-install-project --extra core --extra serving --extra features


# Copy application source
COPY src/ /app/src/

EXPOSE 8000

# Default FastAPI entrypoint
CMD ["uvicorn", "src.serving.app:app", "--host", "0.0.0.0", "--port", "8000"]
