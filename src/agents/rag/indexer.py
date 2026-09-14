"""Document ingestion, markdown chunking, and knowledge base extraction for FAISS RAG."""

import hashlib
import logging
import os
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


@dataclass
class DocumentChunk:
    """Represents a discrete indexed text chunk with provenance metadata."""

    chunk_id: str
    title: str
    source: str
    heading: str
    content: str
    metadata: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """Convert chunk to serializable dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DocumentChunk":
        """Instantiate DocumentChunk from dictionary."""
        return cls(**data)


def _clean_markdown_text(text: str) -> str:
    """Normalize markdown whitespace and strip excessive line breaks."""
    text = re.sub(r"\r\n|\r", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _split_body_into_subchunks(body: str, chunk_size: int) -> List[str]:
    """Split section body into paragraph-bounded sub-chunks under chunk_size."""
    if len(body) <= chunk_size:
        return [body]

    sub_bodies: List[str] = []
    paragraphs = body.split("\n\n")
    current_sub: List[str] = []
    current_len = 0

    for para in paragraphs:
        p_clean = para.strip()
        if not p_clean:
            continue
        if current_len + len(p_clean) > chunk_size and current_sub:
            sub_bodies.append("\n\n".join(current_sub))
            current_sub = [p_clean]
            current_len = len(p_clean)
        else:
            current_sub.append(p_clean)
            current_len += len(p_clean)

    if current_sub:
        sub_bodies.append("\n\n".join(current_sub))

    return sub_bodies


def _process_single_markdown_file(
    md_path: Path,
    docs_parent: Path,
    chunk_size: int,
) -> List[DocumentChunk]:
    """Parse and chunk a single markdown file."""
    try:
        raw_text = md_path.read_text(encoding="utf-8")
    except Exception as exc:
        logger.warning("Failed to read markdown file %s: %s", md_path, exc)
        return []

    text = _clean_markdown_text(raw_text)
    if not text:
        return []

    try:
        rel_source = str(md_path.relative_to(docs_parent)).replace("\\", "/")
    except ValueError:
        rel_source = str(md_path.name)

    doc_title = md_path.stem.replace("-", " ").replace("_", " ").title()
    title_match = re.search(r"^#\s+(.+)$", text, flags=re.MULTILINE)
    if title_match:
        doc_title = title_match.group(1).strip()

    file_chunks: List[DocumentChunk] = []
    sections = re.split(r"\n(?=#{1,3}\s+)", text)
    for sec_idx, section in enumerate(sections):
        sec_clean = section.strip()
        if not sec_clean:
            continue

        lines = sec_clean.split("\n")
        first_line = lines[0].strip()
        if first_line.startswith("#"):
            heading = re.sub(r"^#{1,4}\s+", "", first_line).strip()
            body = "\n".join(lines[1:]).strip() or first_line
        else:
            heading = "Overview" if sec_idx == 0 else f"Section {sec_idx + 1}"
            body = sec_clean

        sub_bodies = _split_body_into_subchunks(body, chunk_size)
        for sub_idx, sub_content in enumerate(sub_bodies):
            clean_sub = sub_content.strip()
            if len(clean_sub) < 30:
                continue

            contextual = (
                f"[{doc_title} > {heading}]\n{clean_sub}"
                if not clean_sub.startswith("[")
                else clean_sub
            )
            chunk_id_raw = f"{rel_source}::{heading}::{sub_idx}"
            chunk_id = hashlib.sha256(chunk_id_raw.encode("utf-8")).hexdigest()[:16]

            file_chunks.append(
                DocumentChunk(
                    chunk_id=chunk_id,
                    title=doc_title,
                    source=rel_source,
                    heading=heading,
                    content=contextual,
                    metadata={
                        "doc_type": "markdown_doc",
                        "file_name": md_path.name,
                        "section_index": sec_idx,
                        "sub_index": sub_idx,
                        "char_count": len(contextual),
                    },
                )
            )

    return file_chunks


def extract_markdown_chunks(
    docs_dir: Path,
    chunk_size: int = 900,
    chunk_overlap: int = 150,
) -> List[DocumentChunk]:
    """Ingest and chunk markdown files from documentation directory.

    Preserves hierarchical markdown headers (#, ##, ###) so each chunk retains
    semantic section and document context.

    Args:
        docs_dir: Path to documentation root directory.
        chunk_size: Target maximum characters per chunk.
        chunk_overlap: Character overlap between consecutive sub-chunks.

    Returns:
        List of DocumentChunk instances.
    """
    if not docs_dir.exists():
        logger.warning("Docs directory does not exist: %s", docs_dir)
        return []

    chunks: List[DocumentChunk] = []
    md_files = sorted(docs_dir.rglob("*.md"))
    for md_path in md_files:
        chunks.extend(
            _process_single_markdown_file(md_path, docs_dir.parent, chunk_size)
        )

    logger.info(
        "Extracted %d markdown chunks from %d files in %s",
        len(chunks),
        len(md_files),
        docs_dir,
    )
    return chunks


def _is_tcp_port_open(host: str, port: int, timeout: float = 0.3) -> bool:
    """Fast probe to determine if a remote host:port is listening without retries."""
    import socket

    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (OSError, TimeoutError):
        return False


def _build_model_card_chunk(
    mlflow_client: Any,
    model_name: str,
    version_obj: Any,
) -> DocumentChunk:
    """Build a DocumentChunk from a live MLflow model version."""
    run_metrics: Dict[str, float] = {}
    run_params: Dict[str, str] = {}
    try:
        run = mlflow_client.get_run(version_obj.run_id)
        run_metrics = run.data.metrics or {}
        run_params = run.data.params or {}
    except Exception as run_err:
        logger.debug(
            "Could not fetch run info for model %s v%s: %s",
            model_name,
            version_obj.version,
            run_err,
        )

    metrics_str = (
        ", ".join(
            f"{k}={v_val:.4f}" if isinstance(v_val, float) else f"{k}={v_val}"
            for k, v_val in run_metrics.items()
        )
        or "No logged metrics"
    )

    params_str = (
        ", ".join(f"{k}={p_val}" for k, p_val in list(run_params.items())[:6])
        or "Standard hyperparameters"
    )

    stage = getattr(version_obj, "current_stage", "None") or "None"
    card_text = (
        f"Model Name: {model_name}\n"
        f"Version: {version_obj.version}\n"
        f"Stage: {stage}\n"
        f"Run ID: {version_obj.run_id}\n"
        f"Evaluation Metrics: {metrics_str}\n"
        f"Hyperparameters: {params_str}\n"
        f"Description: Registered MLflow model in platform model registry. "
        f"Monitored by champion/challenger ModelPromotionGate with a 2.0% hurdle rate."
    )
    chunk_id = hashlib.sha256(
        f"mlflow::{model_name}::{version_obj.version}".encode("utf-8")
    ).hexdigest()[:16]

    return DocumentChunk(
        chunk_id=chunk_id,
        title=f"Model Card: {model_name} v{version_obj.version}",
        source=f"mlflow://models/{model_name}/versions/{version_obj.version}",
        heading=f"Model Registry Metadata (Stage: {stage})",
        content=card_text,
        metadata={
            "doc_type": "model_card",
            "model_name": model_name,
            "version": str(version_obj.version),
            "stage": stage,
            "run_id": version_obj.run_id,
        },
    )


def _get_baseline_model_cards() -> List[DocumentChunk]:
    """Return fallback baseline model cards when live MLflow server is offline."""
    logger.info("Using baseline platform model card catalog.")
    baseline_cards = [
        {
            "id": "demand_lightgbm_card",
            "title": "Model Card: demand_lightgbm_model (Production)",
            "source": "mlflow://catalog/demand_lightgbm_model",
            "heading": "NYC Taxi Zone Demand Forecasting Model Architecture",
            "content": (
                "Model Name: demand_lightgbm_model\n"
                "Architecture: LightGBM Regressor (GBDT)\n"
                "Target: Total pickup counts per NYC TLC taxi zone aggregated in 15-minute time windows.\n"
                "Scope: 263 NYC taxi zones.\n"
                "Key Features: 8 tabular features from Feast Redis online store including pickup_count_lag_15m, "
                "pickup_count_lag_30m, pickup_count_lag_60m, rolling_mean_1h, rolling_std_1h, rolling_mean_4h, "
                "hour_of_day, and day_of_week.\n"
                "Baseline Evaluation Target: MAE <= 3.8 pickups per 15-minute window.\n"
                "Promotion Hurdle Rate: 2.0% improvement over previous champion (ADR-021).\n"
                "Degraded Fallback Behavior: When Redis online features are cold or missing, serving invokes "
                "default historical feature imputation with explicit degraded status metadata (ADR-020)."
            ),
            "metadata": {
                "model_name": "demand_lightgbm_model",
                "stage": "Production",
            },
        },
        {
            "id": "corridor_duration_card",
            "title": "Model Card: corridor_duration_lightgbm_model (Production)",
            "source": "mlflow://catalog/corridor_duration_lightgbm_model",
            "heading": "Corridor Travel Duration ETA Forecasting Model Architecture",
            "content": (
                "Model Name: corridor_duration_lightgbm_model\n"
                "Architecture: LightGBM Regressor (GBDT)\n"
                "Target: Estimated corridor trip duration in seconds between origin and destination taxi zones.\n"
                "Scope: Top NYC airport and inter-borough taxi corridors (e.g. JFK/LaGuardia to Midtown Manhattan).\n"
                "Key Features: 8 corridor features including avg_duration_1h, avg_duration_24h, trip_count_1h, "
                "trip_distance_miles, origin_zone_id, dest_zone_id, hour_of_day, and day_of_week.\n"
                "Baseline Evaluation Target: MAE <= 140 seconds.\n"
                "Serving Latency: Sub-5ms cached, sub-20ms uncached, sub-50ms full-city batch (ADR-019)."
            ),
            "metadata": {
                "model_name": "corridor_duration_lightgbm_model",
                "stage": "Production",
            },
        },
    ]

    return [
        DocumentChunk(
            chunk_id=card["id"],
            title=card["title"],
            source=card["source"],
            heading=card["heading"],
            content=card["content"],
            metadata={
                "doc_type": "model_card",
                "is_baseline": True,
                **card["metadata"],
            },
        )
        for card in baseline_cards
    ]


def _can_connect_to_mlflow(
    client: Optional[Any],
    effective_uri: str,
) -> bool:
    """Check if MLflow client is supplied or remote HTTP server is listening."""
    if client is not None:
        return True

    parsed = urlparse(effective_uri)
    if parsed.scheme in ("http", "https"):
        host = parsed.hostname or "localhost"
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        return _is_tcp_port_open(host, port, timeout=0.3)

    return True


def extract_mlflow_model_cards(
    tracking_uri: Optional[str] = None,
    client: Optional[Any] = None,
) -> List[DocumentChunk]:
    """Extract model cards and registry metadata from MLflow tracking/registry.

    Gracefully falls back to platform baseline model cards if MLflow server is offline.

    Args:
        tracking_uri: Optional MLflow tracking URI.
        client: Optional instantiated MlflowClient.

    Returns:
        List of DocumentChunk instances.
    """
    chunks: List[DocumentChunk] = []

    try:
        from src.common.mlflow_utils import (
            DEMAND_MODEL_NAME,
            DURATION_MODEL_NAME,
            get_mlflow_client,
            get_tracking_uri,
        )

        effective_uri = tracking_uri or get_tracking_uri()
        if _can_connect_to_mlflow(client, effective_uri):
            os.environ.setdefault("MLFLOW_HTTP_REQUEST_MAX_RETRIES", "1")
            os.environ.setdefault("MLFLOW_HTTP_REQUEST_TIMEOUT", "2")

            mlflow_client = client or get_mlflow_client(effective_uri)
            for model_name in [DEMAND_MODEL_NAME, DURATION_MODEL_NAME]:
                try:
                    versions = mlflow_client.search_model_versions(
                        f"name='{model_name}'"
                    )
                    for v in versions:
                        chunks.append(
                            _build_model_card_chunk(mlflow_client, model_name, v)
                        )
                except Exception as model_err:
                    logger.debug("Model %s lookup failed: %s", model_name, model_err)
    except Exception as exc:
        logger.debug("Live MLflow client query unavailable: %s", exc)

    if not chunks:
        chunks = _get_baseline_model_cards()

    return chunks


def _summarize_single_flow_runs(
    flow_name: str,
    flow_runs: List[Any],
) -> DocumentChunk:
    """Summarize execution metrics for a specific pipeline flow."""
    total_runs = len(flow_runs)
    completed = sum(1 for r in flow_runs if r.status == "completed")
    failed = sum(1 for r in flow_runs if r.status in ("failed", "error"))
    durations = [
        float(r.duration_seconds) for r in flow_runs if r.duration_seconds is not None
    ]
    avg_dur = sum(durations) / len(durations) if durations else 0.0
    latest_run = flow_runs[0]
    latest_time = (
        latest_run.started_at.isoformat() if latest_run.started_at else "Unknown"
    )

    content = (
        f"Pipeline Flow: {flow_name}\n"
        f"Recent Runs Evaluated: {total_runs}\n"
        f"Completed: {completed}, Failed: {failed}\n"
        f"Average Duration: {avg_dur:.1f} seconds\n"
        f"Latest Run Status: {latest_run.status} at {latest_time}\n"
        f"Total Records Processed in Recent Window: "
        f"{sum(int(r.records_processed or 0) for r in flow_runs)}\n"
        f"Latest Error Message: {latest_run.error_message or 'None'}"
    )

    chunk_id = hashlib.sha256(f"pipeline::{flow_name}".encode("utf-8")).hexdigest()[:16]

    return DocumentChunk(
        chunk_id=chunk_id,
        title=f"Pipeline Health Summary: {flow_name}",
        source=f"db://warehouse/pipeline_runs/{flow_name}",
        heading="Live Pipeline Execution Status",
        content=content,
        metadata={
            "doc_type": "pipeline_summary",
            "flow_name": flow_name,
            "status": latest_run.status,
            "completed_ratio": completed / max(1, total_runs),
        },
    )


def _get_baseline_pipeline_topology() -> List[DocumentChunk]:
    """Return fallback pipeline topology chunks when database is offline."""
    logger.info("Using baseline pipeline topology catalog.")
    topology_chunks = [
        {
            "id": "pipeline_retraining_spec",
            "title": "Pipeline Architecture: Automated Retraining Flow",
            "source": "orchestration/flows/retraining_flow.py",
            "heading": "Weekly Retraining & Promotion Architecture (ADR-021)",
            "content": (
                "Flow Name: retraining_flow\n"
                "Schedule: Weekly cron scheduled via Prefect Cloud (Sunday 02:00 UTC off-peak).\n"
                "Process: Extracts latest historical trip records from warehouse.trips, materializes offline "
                "Feast features, splits training/validation sets, trains candidate LightGBM models, logs metrics "
                "and artifacts to MLflow tracking server, and executes ModelPromotionGate.\n"
                "Promotion Gate: Candidate must beat naive baseline and beat active Production model by at least "
                "2.0% MAE hurdle rate. If candidate fails, it transitions to Staging and leaves Production serving intact."
            ),
        },
        {
            "id": "pipeline_etl_spec",
            "title": "Pipeline Architecture: TLC Parquet Batch ETL & Streaming",
            "source": "docs/ETL-Streaming.md",
            "heading": "Batch and Streaming Ingestion Flow Topology",
            "content": (
                "Batch ETL: Ingests NYC TLC Yellow Taxi monthly Parquet datasets into raw.trips and transforms "
                "into warehouse.trips with zone validation, outlier cleaning, and trip duration calculation.\n"
                "Streaming Ingestion: Redpanda Kafka streaming consumer processing real-time trip completion events, "
                "updating rolling window feature counters and writing directly to Feast Redis online store."
            ),
        },
    ]

    return [
        DocumentChunk(
            chunk_id=topo["id"],
            title=topo["title"],
            source=topo["source"],
            heading=topo["heading"],
            content=topo["content"],
            metadata={"doc_type": "pipeline_summary", "is_baseline": True},
        )
        for topo in topology_chunks
    ]


def extract_pipeline_run_summaries(
    db_session: Optional[Any] = None,
) -> List[DocumentChunk]:
    """Summarize recent pipeline executions from warehouse.pipeline_runs.

    Falls back to pipeline topology specifications if database is offline.

    Args:
        db_session: Optional SQLAlchemy database session.

    Returns:
        List of DocumentChunk instances.
    """
    chunks: List[DocumentChunk] = []

    try:
        from contextlib import nullcontext

        from src.common.config import get_settings
        from src.common.db import get_db_session
        from src.common.models import PipelineRun

        session_ctx = None
        if db_session is not None:
            session_ctx = (
                db_session
                if hasattr(db_session, "__enter__")
                else nullcontext(db_session)
            )
        else:
            settings = get_settings()
            if _is_tcp_port_open(
                settings.postgres_host, settings.postgres_port, timeout=0.3
            ):
                session_ctx = get_db_session()

        if session_ctx:
            with session_ctx as session:
                runs = (
                    session.query(PipelineRun)
                    .order_by(PipelineRun.started_at.desc())
                    .limit(30)
                    .all()
                )
                if runs:
                    by_flow: Dict[str, List[Any]] = {}
                    for r in runs:
                        by_flow.setdefault(r.flow_name, []).append(r)

                    for flow_name, flow_runs in by_flow.items():
                        chunks.append(_summarize_single_flow_runs(flow_name, flow_runs))
    except Exception as exc:
        logger.debug("Database pipeline runs query unavailable: %s", exc)

    if not chunks:
        chunks = _get_baseline_pipeline_topology()

    return chunks


def build_knowledge_base(
    docs_dir: Optional[Path] = None,
    include_mlflow: bool = True,
    include_db: bool = True,
) -> List[DocumentChunk]:
    """Assemble all documentation, model cards, and operational summaries into a unified knowledge base.

    Args:
        docs_dir: Optional custom path to docs/ directory.
        include_mlflow: Whether to query MLflow registry/catalog.
        include_db: Whether to query database pipeline execution status.

    Returns:
        Unified list of DocumentChunk instances.
    """
    if docs_dir is None:
        docs_dir = Path(__file__).resolve().parent.parent.parent.parent / "docs"

    all_chunks: List[DocumentChunk] = []

    # 1. Markdown documentation chunks
    md_chunks = extract_markdown_chunks(docs_dir)
    all_chunks.extend(md_chunks)

    # 2. MLflow model cards
    if include_mlflow:
        model_chunks = extract_mlflow_model_cards()
        all_chunks.extend(model_chunks)

    # 3. Pipeline execution summaries
    if include_db:
        pipe_chunks = extract_pipeline_run_summaries()
        all_chunks.extend(pipe_chunks)

    logger.info("Assembled complete knowledge base with %d chunks.", len(all_chunks))
    return all_chunks
