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

# Install dependencies using the frozen lockfile
RUN uv sync --frozen --no-dev

# Copy UI source
COPY ui/ /app/ui/

EXPOSE 8501

# Streamlit entrypoint
CMD ["streamlit", "run", "ui/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
