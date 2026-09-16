"""Read-only operational tools for the LangGraph Ops Copilot.

ADR-023: Tools provide structured, read-only access to live pipeline state (Feast Redis
online store, PostgreSQL predictions and pipeline runs logs, and platform documentation).
Strict allowlisting of these four tools serves as the primary architectural security
boundary against prompt-injection and unauthorized data mutation.
"""

import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

from langchain_core.tools import tool

from src.common.db import get_db_session
from src.common.models import PipelineRun, Prediction

logger = logging.getLogger(__name__)


def get_features(entity_type: str, entity_id: str) -> Dict[str, Any]:
    """Retrieve the current feature state for an entity from the Feast online store (Redis).

    Args:
        entity_type: Entity domain ('zone' for taxi pickup demand, 'corridor' for origin-destination ETA).
        entity_id: For 'zone', integer ID in range 1 to 263. For 'corridor', string '{origin}_{dest}' (e.g. '161_237').

    Returns:
        Structured dictionary containing status, feature mappings, cache-hit indicator, and retrieval timestamp.
    """
    clean_type = str(entity_type).strip().lower()
    now_utc = datetime.now(timezone.utc)

    if clean_type == "zone":
        try:
            zid = int(entity_id)
        except (ValueError, TypeError):
            return {
                "status": "error",
                "entity_type": "zone",
                "entity_id": str(entity_id),
                "error": f"Zone entity_id must be an integer, got '{entity_id}'.",
                "cache_hit": False,
                "retrieved_at": now_utc.isoformat(),
            }
        if zid < 1 or zid > 263:
            return {
                "status": "error",
                "entity_type": "zone",
                "entity_id": zid,
                "error": f"Zone ID {zid} is outside the valid NYC TLC zone range [1, 263].",
                "cache_hit": False,
                "retrieved_at": now_utc.isoformat(),
            }

        try:
            from src.features.client import get_online_client

            client = get_online_client()
            features = client.get_zone_demand_features([zid])[0]
            raw_dict = features.to_dict()
            cache_hit = raw_dict.pop("cache_hit", False)
            raw_dict.pop("zone_id", None)
            status = "success" if cache_hit else "degraded_fallback"
            warning = (
                None
                if cache_hit
                else "Online features unmaterialized in Redis; returned imputed defaults."
            )
            return {
                "status": status,
                "entity_type": "zone",
                "entity_id": zid,
                "features": raw_dict,
                "cache_hit": cache_hit,
                "warning": warning,
                "retrieved_at": now_utc.isoformat(),
            }
        except Exception as exc:
            logger.warning("Failed to retrieve zone features from Feast: %s", exc)
            from src.features.client import ZoneDemandOnlineFeatures

            raw_dict = ZoneDemandOnlineFeatures(zone_id=zid, cache_hit=False).to_dict()
            raw_dict.pop("cache_hit", None)
            raw_dict.pop("zone_id", None)
            return {
                "status": "degraded_fallback",
                "entity_type": "zone",
                "entity_id": zid,
                "features": raw_dict,
                "cache_hit": False,
                "warning": f"Feast online feature store query failed ({exc}); returned imputed baseline defaults.",
                "retrieved_at": now_utc.isoformat(),
            }

    elif clean_type == "corridor":
        parts = str(entity_id).strip().split("_")
        if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
            return {
                "status": "error",
                "entity_type": "corridor",
                "entity_id": str(entity_id),
                "error": f"Corridor entity_id must be formatted as '{{origin}}_{{dest}}' (e.g. '161_237'), got '{entity_id}'.",
                "cache_hit": False,
                "retrieved_at": now_utc.isoformat(),
            }
        orig, dest = int(parts[0]), int(parts[1])
        if orig < 1 or orig > 263 or dest < 1 or dest > 263:
            return {
                "status": "error",
                "entity_type": "corridor",
                "entity_id": str(entity_id),
                "error": f"Corridor zone IDs '{entity_id}' are outside the valid NYC TLC zone range [1, 263].",
                "cache_hit": False,
                "retrieved_at": now_utc.isoformat(),
            }

        try:
            from src.features.client import get_online_client

            client = get_online_client()
            corridor_key = f"{orig}_{dest}"
            features = client.get_corridor_duration_features([corridor_key])[0]
            raw_dict = features.to_dict()
            cache_hit = raw_dict.pop("cache_hit", False)
            raw_dict.pop("corridor_id", None)
            status = "success" if cache_hit else "degraded_fallback"
            warning = (
                None
                if cache_hit
                else "Corridor features unmaterialized in Redis; returned imputed defaults."
            )
            return {
                "status": status,
                "entity_type": "corridor",
                "entity_id": corridor_key,
                "features": raw_dict,
                "cache_hit": cache_hit,
                "warning": warning,
                "retrieved_at": now_utc.isoformat(),
            }
        except Exception as exc:
            logger.warning("Failed to retrieve corridor features from Feast: %s", exc)
            from src.features.client import CorridorDurationOnlineFeatures

            raw_dict = CorridorDurationOnlineFeatures(
                corridor_id=f"{orig}_{dest}", cache_hit=False
            ).to_dict()
            raw_dict.pop("cache_hit", None)
            raw_dict.pop("corridor_id", None)
            return {
                "status": "degraded_fallback",
                "entity_type": "corridor",
                "entity_id": f"{orig}_{dest}",
                "features": raw_dict,
                "cache_hit": False,
                "warning": f"Feast online feature store query failed ({exc}); returned imputed baseline defaults.",
                "retrieved_at": now_utc.isoformat(),
            }
    else:
        return {
            "status": "error",
            "entity_type": str(entity_type),
            "entity_id": str(entity_id),
            "error": f"Invalid entity_type '{entity_type}'. Must be 'zone' or 'corridor'.",
            "cache_hit": False,
            "retrieved_at": now_utc.isoformat(),
        }


def query_recent_predictions(
    entity_type: str, entity_id: str, window_hours: int = 24
) -> Dict[str, Any]:
    """Query recent model inference predictions from the database prediction log.

    Args:
        entity_type: Entity domain ('zone' or 'corridor').
        entity_id: For 'zone', zone ID (e.g. '161'). For 'corridor', '{orig}_{dest}' (e.g. '161_237').
        window_hours: Lookback window in hours (default 24 hours, max 168 hours / 1 week).

    Returns:
        Structured dictionary containing query parameters, prediction count, and chronological list of past predictions.
    """
    clean_type = str(entity_type).strip().lower()
    clean_id = str(entity_id).strip()
    safe_window = max(1, min(int(window_hours), 168))
    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(hours=safe_window)

    if clean_type not in ("zone", "corridor"):
        return {
            "status": "error",
            "entity_type": clean_type,
            "entity_id": clean_id,
            "error": f"Invalid entity_type '{entity_type}'. Must be 'zone' or 'corridor'.",
            "prediction_count": 0,
            "predictions": [],
            "checked_at": now_utc.isoformat(),
        }

    try:
        with get_db_session() as session:
            records = (
                session.query(Prediction)
                .filter(
                    Prediction.entity_type == clean_type,
                    Prediction.entity_id == clean_id,
                    Prediction.predicted_at >= cutoff,
                )
                .order_by(Prediction.predicted_at.desc())
                .limit(50)
                .all()
            )

            items: List[Dict[str, Any]] = []
            for r in records:
                items.append(
                    {
                        "prediction_id": str(r.prediction_id),
                        "model_version": str(r.model_version),
                        "predicted_value": float(r.predicted_value),
                        "predicted_at": (
                            r.predicted_at.isoformat() if r.predicted_at else None
                        ),
                        "actual_value": (
                            float(r.actual_value)
                            if r.actual_value is not None
                            else None
                        ),
                        "actual_recorded_at": (
                            r.actual_recorded_at.isoformat()
                            if r.actual_recorded_at
                            else None
                        ),
                    }
                )

        status_label = "success" if items else "empty"
        message = (
            f"Retrieved {len(items)} prediction(s) for {clean_type} '{clean_id}' in the last {safe_window} hours."
            if items
            else f"No predictions found for {clean_type} '{clean_id}' in the last {safe_window} hours."
        )

        return {
            "status": status_label,
            "message": message,
            "entity_type": clean_type,
            "entity_id": clean_id,
            "window_hours": safe_window,
            "prediction_count": len(items),
            "predictions": items,
            "checked_at": now_utc.isoformat(),
        }

    except Exception as exc:
        logger.warning("Failed to query predictions from database: %s", exc)
        return {
            "status": "error",
            "error": f"Database prediction query failed: {exc}",
            "entity_type": clean_type,
            "entity_id": clean_id,
            "window_hours": safe_window,
            "prediction_count": 0,
            "predictions": [],
            "checked_at": now_utc.isoformat(),
        }


def query_pipeline_status() -> Dict[str, Any]:
    """Query recent orchestration execution history and health across all data and model pipelines.

    Returns:
        Structured dictionary containing overall status ('healthy', 'degraded', 'empty'),
        latest run details across ETL, streaming reconciliation, and scheduled retraining flows.
    """
    now_utc = datetime.now(timezone.utc)
    runs: List[Dict[str, Any]] = []

    try:
        with get_db_session() as session:
            db_runs = (
                session.query(PipelineRun)
                .order_by(PipelineRun.started_at.desc())
                .limit(10)
                .all()
            )
            for r in db_runs:
                runs.append(
                    {
                        "run_id": str(r.run_id),
                        "job_name": str(r.job_name),
                        "status": str(r.status),
                        "started_at": (
                            r.started_at.isoformat() if r.started_at else None
                        ),
                        "finished_at": (
                            r.finished_at.isoformat() if r.finished_at else None
                        ),
                        "duration_seconds": (
                            float(r.duration_seconds)
                            if r.duration_seconds is not None
                            else None
                        ),
                        "records_processed": int(r.records_processed or 0),
                        "error_message": r.error_message,
                        "triggered_by": r.triggered_by,
                    }
                )

        has_failures = any(
            r["status"] in ("failed", "error") for r in runs[:3]
        )  # most recent 3 runs
        has_completed = any(r["status"] == "completed" for r in runs)

        if not runs:
            overall = "empty"
        elif has_failures:
            overall = "degraded"
        elif has_completed:
            overall = "healthy"
        else:
            overall = "in_progress"

        return {
            "status": "success",
            "overall_health": overall,
            "overall_status": overall,
            "run_count": len(runs),
            "total_runs": len(runs),
            "latest_runs": runs,
            "runs": runs,
            "checked_at": now_utc.isoformat(),
        }

    except Exception as exc:
        logger.warning("Failed to query pipeline_runs from database: %s", exc)
        return {
            "status": "error",
            "overall_health": "unknown",
            "overall_status": "unknown",
            "error": f"Database pipeline status query failed: {exc}",
            "run_count": 0,
            "total_runs": 0,
            "latest_runs": [],
            "runs": [],
            "checked_at": now_utc.isoformat(),
        }


def search_logs_and_model_cards(query: str, top_k: int = 3) -> Dict[str, Any]:
    """Search platform architecture documents, ADR decisions, model cards, and operational runbooks.

    Args:
        query: Natural language query string describing system behavior, design rationale, or model metrics.
        top_k: Number of most relevant matching sections to return (default 3, max 10).

    Returns:
        Structured dictionary containing the query, matched document snippets, sources, and relevance scores.
    """
    clean_query = str(query).strip()
    safe_k = max(1, min(int(top_k), 10))

    if not clean_query:
        return {
            "status": "empty",
            "query": clean_query,
            "results_count": 0,
            "results": [],
            "source": "empty_query",
        }

    # Attempt FAISS index retrieval if available (M7-2 integration)
    try:
        from src.agents.rag.retriever import get_rag_retriever

        retriever = get_rag_retriever()
        if retriever.is_ready():
            results = retriever.search(clean_query, top_k=safe_k)
            return {
                "status": "success",
                "query": clean_query,
                "results_count": len(results),
                "results": results,
                "source": "faiss_index",
            }
    except (ImportError, Exception) as rag_exc:
        logger.debug(
            "FAISS retriever unavailable or unindexed; falling back to direct markdown search: %s",
            rag_exc,
        )

    # Resilient fallback: Direct markdown section scanner over docs/
    results: List[Dict[str, Any]] = []
    docs_dir = Path(__file__).resolve().parent.parent.parent / "docs"

    if docs_dir.exists():
        query_terms = [t for t in re.split(r"\W+", clean_query.lower()) if len(t) > 2]

        scored_chunks = []
        for md_file in docs_dir.glob("*.md"):
            try:
                content = md_file.read_text(encoding="utf-8")
                sections = re.split(r"\n(?=##?\s+)", content)
                for sec in sections:
                    sec_clean = sec.strip()
                    if not sec_clean:
                        continue
                    sec_lower = sec_clean.lower()
                    # Calculate term occurrence score
                    score = sum(sec_lower.count(term) for term in query_terms)
                    if score > 0:
                        first_line = (
                            sec_clean.split("\n", 1)[0].replace("#", "").strip()
                        )
                        snippet = sec_clean[:400] + (
                            "..." if len(sec_clean) > 400 else ""
                        )
                        scored_chunks.append(
                            {
                                "title": f"{md_file.name}: {first_line}",
                                "source": f"docs/{md_file.name}",
                                "score": float(score),
                                "content": snippet,
                            }
                        )
            except Exception as file_err:
                logger.debug(
                    "Could not read %s for fallback RAG: %s", md_file, file_err
                )

        scored_chunks.sort(key=lambda x: x["score"], reverse=True)
        results = scored_chunks[:safe_k]

    status = "success" if results else "empty"
    return {
        "status": status,
        "query": clean_query,
        "results_count": len(results),
        "results": results,
        "source": "docs_direct_search",
    }


# LangChain StructuredTool wrappers for LangGraph integration
get_features_tool = tool(get_features)
query_recent_predictions_tool = tool(query_recent_predictions)
query_pipeline_status_tool = tool(query_pipeline_status)
search_logs_and_model_cards_tool = tool(search_logs_and_model_cards)

# Registry of allowable tools
AGENT_TOOLS = [
    get_features_tool,
    query_recent_predictions_tool,
    query_pipeline_status_tool,
    search_logs_and_model_cards_tool,
]

# Read-only Allowlist dictionary (ADR-023 Definitive Security Boundary)
TOOL_ALLOWLIST = {t.name: t for t in AGENT_TOOLS}
