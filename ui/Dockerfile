# Multi-arch base image (amd64/arm64)
FROM python:3.11-slim

WORKDIR /app

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Copy project definition
COPY pyproject.toml .

# Install dependencies
RUN uv pip install --system -e .

# Copy UI source
COPY ui/ /app/ui/

EXPOSE 8501

# Streamlit entrypoint
CMD ["streamlit", "run", "ui/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
