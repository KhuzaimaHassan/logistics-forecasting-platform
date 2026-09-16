"""Vector store builder and FAISS index manager for LangGraph Ops Copilot."""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple, Union

import faiss
import numpy as np

from src.agents.rag.indexer import DocumentChunk

logger = logging.getLogger(__name__)

DEFAULT_DIMENSION = 384


class BaseVectorizer:
    """Abstract interface for text embedding vectorizers."""

    dimension: int

    def embed_documents(self, texts: List[str]) -> np.ndarray:
        """Compute L2-normalized dense embeddings for a list of document strings."""
        raise NotImplementedError

    def embed_query(self, text: str) -> np.ndarray:
        """Compute L2-normalized dense embedding for a single query string."""
        raise NotImplementedError


class DeterministicVectorizer(BaseVectorizer):
    """Hermetic, deterministic text vectorizer using subword/word n-gram hashing.

    Produces fixed-dimension L2-normalized embeddings without external network requests
    or heavyweight model weights, guaranteed to execute sub-millisecond on CPU.
    """

    def __init__(self, dimension: int = DEFAULT_DIMENSION):
        self.dimension = dimension
        from sklearn.feature_extraction.text import HashingVectorizer

        self._vectorizer = HashingVectorizer(
            n_features=self.dimension,
            norm="l2",
            alternate_sign=True,
            ngram_range=(1, 2),
            token_pattern=r"(?u)\b\w+\b",
        )

    def embed_documents(self, texts: List[str]) -> np.ndarray:
        if not texts:
            return np.empty((0, self.dimension), dtype=np.float32)

        # Sanitize texts
        safe_texts = [t if t and t.strip() else "empty document" for t in texts]
        matrix = self._vectorizer.transform(safe_texts).toarray().astype(np.float32)

        # Guarantee strict L2 normalization for inner product similarity
        norms = np.linalg.norm(matrix, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        normalized = matrix / norms
        return np.ascontiguousarray(normalized, dtype=np.float32)

    def embed_query(self, text: str) -> np.ndarray:
        safe_text = text.strip() if text and text.strip() else "empty query"
        matrix = self._vectorizer.transform([safe_text]).toarray().astype(np.float32)
        norm = np.linalg.norm(matrix)
        if norm > 0:
            matrix = matrix / norm
        else:
            matrix[0, 0] = 1.0
        return np.ascontiguousarray(matrix, dtype=np.float32)


class FastEmbedVectorizer(BaseVectorizer):
    """ONNX-accelerated neural embedding vectorizer using fastembed."""

    def __init__(
        self,
        model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
        dimension: int = DEFAULT_DIMENSION,
    ):
        try:
            from fastembed import TextEmbedding

            self._model = TextEmbedding(model_name=model_name)
            self.model_name = model_name
            self.dimension = dimension
        except ImportError as exc:
            raise ImportError(
                "fastembed is not installed. Install with 'pip install fastembed' or use deterministic vectorizer."
            ) from exc

    def embed_documents(self, texts: List[str]) -> np.ndarray:
        safe_texts = [t if t and t.strip() else "empty document" for t in texts]
        embeddings = list(self._model.embed(safe_texts))
        matrix = np.array(embeddings, dtype=np.float32)

        norms = np.linalg.norm(matrix, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        normalized = matrix / norms
        return np.ascontiguousarray(normalized, dtype=np.float32)

    def embed_query(self, text: str) -> np.ndarray:
        safe_text = text.strip() if text and text.strip() else "empty query"
        embeddings = list(self._model.embed([safe_text]))
        matrix = np.array(embeddings, dtype=np.float32)
        norm = np.linalg.norm(matrix)
        if norm > 0:
            matrix = matrix / norm
        else:
            matrix[0, 0] = 1.0
        return np.ascontiguousarray(matrix, dtype=np.float32)


def get_vectorizer(
    name: str = "auto",
    dimension: int = DEFAULT_DIMENSION,
) -> BaseVectorizer:
    """Factory creating an embedding vectorizer based on requested provider.

    Args:
        name: 'auto', 'fastembed', or 'deterministic'.
        dimension: Embedding dimension (default 384).

    Returns:
        Instantiated BaseVectorizer.
    """
    clean_name = name.strip().lower()

    if clean_name == "fastembed":
        try:
            return FastEmbedVectorizer(dimension=dimension)
        except Exception as exc:
            logger.warning(
                "FastEmbed requested but unavailable (%s); falling back to DeterministicVectorizer.",
                exc,
            )
            return DeterministicVectorizer(dimension=dimension)

    elif clean_name == "deterministic":
        return DeterministicVectorizer(dimension=dimension)

    # 'auto' mode: try fastembed first; on any failure, use deterministic
    try:
        return FastEmbedVectorizer(dimension=dimension)
    except Exception:
        return DeterministicVectorizer(dimension=dimension)


class VectorStore:
    """CPU-backed FAISS vector index maintaining document chunks and similarity search."""

    def __init__(
        self,
        dimension: int = DEFAULT_DIMENSION,
        vectorizer: Optional[BaseVectorizer] = None,
    ):
        self.dimension = dimension
        self.vectorizer = vectorizer or DeterministicVectorizer(dimension=dimension)
        self.index: faiss.IndexFlatIP = faiss.IndexFlatIP(self.dimension)
        self.chunks: List[DocumentChunk] = []

    @property
    def size(self) -> int:
        """Return number of indexed chunks."""
        return len(self.chunks)

    def add_chunks(self, chunks: List[DocumentChunk]) -> int:
        """Embed and append document chunks to the FAISS index.

        Args:
            chunks: List of DocumentChunk objects.

        Returns:
            Number of chunks successfully added.
        """
        if not chunks:
            return 0

        texts = [c.content for c in chunks]
        vectors = self.vectorizer.embed_documents(texts)

        if vectors.shape[1] != self.dimension:
            raise ValueError(
                f"Vector dimension mismatch: expected {self.dimension}, got {vectors.shape[1]}"
            )

        self.index.add(vectors)
        self.chunks.extend(chunks)
        logger.info(
            "Added %d chunks to FAISS index (total: %d).", len(chunks), self.size
        )
        return len(chunks)

    def search(
        self,
        query: str,
        top_k: int = 3,
    ) -> List[Tuple[DocumentChunk, float]]:
        """Perform cosine similarity search against the FAISS index.

        Args:
            query: Natural language query string.
            top_k: Maximum number of relevant chunks to retrieve.

        Returns:
            List of (DocumentChunk, similarity_score) tuples sorted in descending order.
        """
        if self.size == 0 or not query.strip():
            return []

        safe_k = max(1, min(int(top_k), self.size))
        query_vec = self.vectorizer.embed_query(query)

        scores, indices = self.index.search(query_vec, safe_k)
        results: List[Tuple[DocumentChunk, float]] = []

        for idx, score in zip(indices[0], scores[0], strict=False):
            if 0 <= idx < len(self.chunks):
                results.append((self.chunks[idx], float(score)))

        return results

    def save(self, directory: Union[str, Path]) -> Path:
        """Persist FAISS index, chunk metadata, and index configuration to disk.

        Args:
            directory: Target output directory (e.g. artifacts/rag_index).

        Returns:
            Resolved Path of the saved directory.
        """
        out_dir = Path(directory).resolve()
        out_dir.mkdir(parents=True, exist_ok=True)

        # 1. Write binary FAISS index
        faiss_file = out_dir / "index.faiss"
        faiss.write_index(self.index, str(faiss_file))

        # 2. Write chunk metadata
        chunks_file = out_dir / "chunks.json"
        chunks_payload = [c.to_dict() for c in self.chunks]
        chunks_file.write_text(
            json.dumps(chunks_payload, indent=2, ensure_ascii=False), encoding="utf-8"
        )

        # 3. Write index metadata configuration
        meta_file = out_dir / "metadata.json"
        metadata = {
            "dimension": self.dimension,
            "vectorizer_type": self.vectorizer.__class__.__name__,
            "chunk_count": self.size,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        meta_file.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

        logger.info(
            "Persisted FAISS vector index (%d chunks) to %s", self.size, out_dir
        )
        return out_dir

    @classmethod
    def load(
        cls,
        directory: Union[str, Path],
        vectorizer: Optional[BaseVectorizer] = None,
    ) -> "VectorStore":
        """Load persisted FAISS index and chunk metadata from disk.

        Args:
            directory: Directory containing index.faiss, chunks.json, and metadata.json.
            vectorizer: Optional vectorizer instance (if None, instantiates based on metadata).

        Returns:
            Instantiated VectorStore.
        """
        in_dir = Path(directory).resolve()
        faiss_file = in_dir / "index.faiss"
        chunks_file = in_dir / "chunks.json"
        meta_file = in_dir / "metadata.json"

        if not faiss_file.exists() or not chunks_file.exists():
            raise FileNotFoundError(
                f"Missing required FAISS index files in {in_dir} (found: {[p.name for p in in_dir.glob('*')]})"
            )

        dimension = DEFAULT_DIMENSION
        if meta_file.exists():
            try:
                meta = json.loads(meta_file.read_text(encoding="utf-8"))
                dimension = int(meta.get("dimension", DEFAULT_DIMENSION))
            except Exception as exc:
                logger.warning("Could not parse metadata.json in %s: %s", in_dir, exc)

        active_vec = vectorizer or get_vectorizer("auto", dimension=dimension)
        store = cls(dimension=dimension, vectorizer=active_vec)

        # Read FAISS binary index
        store.index = faiss.read_index(str(faiss_file))

        # Read chunk payload
        raw_chunks = json.loads(chunks_file.read_text(encoding="utf-8"))
        store.chunks = [DocumentChunk.from_dict(item) for item in raw_chunks]

        logger.info(
            "Loaded FAISS vector store from %s (%d chunks, dimension %d).",
            in_dir,
            store.size,
            store.dimension,
        )
        return store
