#!/usr/bin/env python3
"""CLI utility to compile and serialize the FAISS RAG index for the LangGraph Ops Copilot."""

import argparse
import logging
import sys
import time
from pathlib import Path

# Ensure repository root is on sys.path
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.agents.rag.indexer import (  # noqa: E402
    extract_markdown_chunks,
    extract_mlflow_model_cards,
    extract_pipeline_run_summaries,
)
from src.agents.rag.store import VectorStore, get_vectorizer  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("build_rag_index")


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Build and serialize CPU-backed FAISS index from platform docs and metadata."
    )
    parser.add_argument(
        "--docs-dir",
        type=str,
        default=str(REPO_ROOT / "docs"),
        help="Path to documentation root directory (default: docs/)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(REPO_ROOT / "artifacts" / "rag_index"),
        help="Target output directory for FAISS index (default: artifacts/rag_index)",
    )
    parser.add_argument(
        "--vectorizer",
        type=str,
        choices=["auto", "fastembed", "deterministic"],
        default="auto",
        help="Embedding vectorizer provider (default: auto)",
    )
    parser.add_argument(
        "--skip-mlflow",
        action="store_true",
        help="Skip querying live MLflow tracking server (use static model catalog)",
    )
    parser.add_argument(
        "--skip-db",
        action="store_true",
        help="Skip querying database pipeline runs (use static pipeline topology)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=900,
        help="Target maximum character length per chunk (default: 900)",
    )
    parser.add_argument(
        "--chunk-overlap",
        type=int,
        default=150,
        help="Character overlap between consecutive chunks (default: 150)",
    )
    parser.add_argument(
        "--test-query",
        type=str,
        default="Why NYC taxi demand and not Karachi?",
        help="Verification query to test against the built index",
    )
    return parser.parse_args()


def main() -> int:
    """Execute knowledge base compilation and index persistence."""
    args = parse_args()
    start_time = time.perf_counter()

    docs_path = Path(args.docs_dir).resolve()
    out_path = Path(args.output_dir).resolve()

    logger.info("==================================================")
    logger.info("Building Platform FAISS RAG Knowledge Index")
    logger.info("Docs Directory:    %s", docs_path)
    logger.info("Output Directory:  %s", out_path)
    logger.info("Vectorizer Mode:   %s", args.vectorizer)
    logger.info("==================================================")

    # 1. Ingest markdown documents
    logger.info("Ingesting markdown documentation from %s...", docs_path)
    md_chunks = extract_markdown_chunks(
        docs_path,
        chunk_size=args.chunk_size,
        chunk_overlap=args.chunk_overlap,
    )
    logger.info("Extracted %d documentation chunks.", len(md_chunks))

    # 2. Extract MLflow model cards
    logger.info("Extracting model cards (skip_live=%s)...", args.skip_mlflow)
    model_chunks = extract_mlflow_model_cards() if not args.skip_mlflow else []
    if not model_chunks:
        # Fallback to static model cards
        from src.agents.rag.indexer import extract_mlflow_model_cards as extract_models

        model_chunks = extract_models(client=None)
    logger.info("Extracted %d model card chunks.", len(model_chunks))

    # 3. Extract pipeline execution summaries
    logger.info("Extracting pipeline run summaries (skip_live=%s)...", args.skip_db)
    pipe_chunks = extract_pipeline_run_summaries() if not args.skip_db else []
    if not pipe_chunks:
        from src.agents.rag.indexer import (
            extract_pipeline_run_summaries as extract_pipes,
        )

        pipe_chunks = extract_pipes(db_session=None)
    logger.info("Extracted %d pipeline summary chunks.", len(pipe_chunks))

    # 4. Combine all chunks
    all_chunks = md_chunks + model_chunks + pipe_chunks
    if not all_chunks:
        logger.error(
            "No chunks extracted! Ensure docs directory exists and contains markdown files."
        )
        return 1

    logger.info("Total knowledge chunks to index: %d", len(all_chunks))

    # 5. Initialize vectorizer and FAISS vector store
    vectorizer = get_vectorizer(args.vectorizer)
    logger.info(
        "Using vectorizer: %s (dimension=%d)",
        vectorizer.__class__.__name__,
        vectorizer.dimension,
    )

    store = VectorStore(dimension=vectorizer.dimension, vectorizer=vectorizer)
    store.add_chunks(all_chunks)

    # 6. Save index to disk
    saved_path = store.save(out_path)
    elapsed = time.perf_counter() - start_time
    logger.info("FAISS index successfully saved to %s in %.2fs", saved_path, elapsed)

    # 7. Verification search query
    if args.test_query:
        logger.info("--------------------------------------------------")
        logger.info("Running verification search for: '%s'", args.test_query)
        matches = store.search(args.test_query, top_k=3)
        for i, (chunk, score) in enumerate(matches, start=1):
            logger.info(
                "Match #%d: [score=%.4f] [%s > %s] (source=%s)",
                i,
                score,
                chunk.title,
                chunk.heading,
                chunk.source,
            )
            preview = chunk.content.replace("\n", " ")[:160] + "..."
            logger.info("          %s", preview)
        logger.info("--------------------------------------------------")

    logger.info("Knowledge base indexing complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
