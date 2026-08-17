"""FastAPI serving application placeholder (Phase 5)."""

from fastapi import FastAPI

app = FastAPI(
    title="Logistics Forecasting Platform API",
    description="Serving endpoints for demand and ETA predictions",
    version="0.1.0",
)


@app.get("/health")
def health_check() -> dict[str, str]:
    """Health check endpoint for container orchestration and Caddy proxy."""
    return {"status": "ok", "service": "fastapi"}
