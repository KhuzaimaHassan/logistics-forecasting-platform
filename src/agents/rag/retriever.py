"""Semantic search and context retrieval engine for LangGraph Ops Copilot."""

import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from src.agents.rag.indexer import build_knowledge_base
from src.agents.rag.store import VectorStore

logger = logging.getLogger(__name__)

_GLOBAL_RETRIEVER: Optional["RAGRetriever"] = None


class RAGRetriever:
    """Semantic retrieval engine querying the FAISS vector index."""

    def __init__(
        self,
        vector_store: Optional[VectorStore] = None,
        index_dir: Optional[Union[str, Path]] = None,
    ):
        self.vector_store = vector_store
        self.index_dir = Path(index_dir).resolve() if index_dir else None

    def is_ready(self) -> bool:
        """Return True if vector store is initialized with indexed chunks."""
        return self.vector_store is not None and self.vector_store.size > 0

    def search(
        self,
        query: str,
        top_k: int = 3,
        score_threshold: float = 0.0,
    ) -> List[Dict[str, Any]]:
        """Retrieve top matching document chunks ranked by cosine similarity.

        Args:
            query: Natural language query string.
            top_k: Maximum number of relevant chunks to return (default 3, max 10).
            score_threshold: Minimum cosine similarity score required (default 0.0).

        Returns:
            List of structured dictionary results containing title, source, heading,
            score, content, and metadata.
        """
        if not self.is_ready():
            logger.debug("RAGRetriever searched while not ready; returning empty list.")
            return []

        clean_query = query.strip()
        if not clean_query:
            return []

        safe_k = max(1, min(int(top_k), 10))
        raw_matches = self.vector_store.search(clean_query, top_k=safe_k * 2)

        results: List[Dict[str, Any]] = []
        for chunk, score in raw_matches:
            if score < score_threshold:
                continue

            results.append(
                {
                    "title": chunk.title,
                    "source": chunk.source,
                    "heading": chunk.heading,
                    "score": round(float(score), 4),
                    "content": chunk.content,
                    "metadata": chunk.metadata,
                }
            )

            if len(results) >= safe_k:
                break

        return results


def get_rag_retriever(
    index_dir: Optional[Union[str, Path]] = None,
    reload: bool = False,
    build_if_missing: bool = True,
) -> RAGRetriever:
    """Return a singleton RAGRetriever instance.

    Loads from persisted index if present, or dynamically compiles an in-memory
    knowledge base from docs/ if unindexed, guaranteeing zero-cold-start failures.

    Args:
        index_dir: Optional custom path to FAISS index directory.
        reload: If True, forces re-instantiating the retriever.
        build_if_missing: If True and on-disk index is missing, builds in-memory.

    Returns:
        Instantiated, ready RAGRetriever instance.
    """
    global _GLOBAL_RETRIEVER

    if _GLOBAL_RETRIEVER is not None and not reload:
        return _GLOBAL_RETRIEVER

    # Determine index directory path
    target_dir: Optional[Path] = None
    if index_dir:
        target_dir = Path(index_dir).resolve()
    else:
        env_dir = os.getenv("RAG_INDEX_DIR")
        if env_dir:
            target_dir = Path(env_dir).resolve()
        else:
            # Default location: repo_root / artifacts / rag_index
            repo_root = Path(__file__).resolve().parent.parent.parent.parent
            target_dir = repo_root / "artifacts" / "rag_index"

    # 1. Attempt loading from disk
    if (
        target_dir
        and (target_dir / "index.faiss").exists()
        and (target_dir / "chunks.json").exists()
    ):
        try:
            store = VectorStore.load(target_dir)
            _GLOBAL_RETRIEVER = RAGRetriever(vector_store=store, index_dir=target_dir)
            logger.info(
                "Initialized RAGRetriever from persisted index at %s", target_dir
            )
            return _GLOBAL_RETRIEVER
        except Exception as load_err:
            logger.warning(
                "Failed to load persisted FAISS index from %s (%s); will attempt rebuild.",
                target_dir,
                load_err,
            )

    # 2. Build in-memory if requested and missing
    if build_if_missing:
        try:
            logger.info("Building in-memory knowledge base for RAGRetriever...")
            chunks = build_knowledge_base(include_mlflow=False, include_db=False)
            store = VectorStore()
            store.add_chunks(chunks)
            _GLOBAL_RETRIEVER = RAGRetriever(vector_store=store, index_dir=target_dir)
            logger.info(
                "Initialized in-memory RAGRetriever with %d chunks.", store.size
            )
            return _GLOBAL_RETRIEVER
        except Exception as build_err:
            logger.error("Failed to build in-memory RAG index: %s", build_err)

    _GLOBAL_RETRIEVER = RAGRetriever(vector_store=None, index_dir=target_dir)
    return _GLOBAL_RETRIEVER
