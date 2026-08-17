# Multi-arch base image (amd64/arm64)
FROM python:3.11-slim

WORKDIR /app

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Copy project definition
COPY pyproject.toml .

# Install dependencies
RUN uv pip install --system -e .

# Copy application source
COPY src/ /app/src/

# Default entrypoint (to be filled in during Phase 1)
CMD ["python", "-m", "src.extract.batch_puller"]
