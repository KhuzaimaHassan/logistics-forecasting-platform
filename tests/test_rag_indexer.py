"""Unit and integration tests for FAISS RAG indexer, vector store, and retriever."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np

from src.agents.rag.indexer import (
    DocumentChunk,
    extract_markdown_chunks,
    extract_mlflow_model_cards,
    extract_monitoring_report_summaries,
    extract_pipeline_run_summaries,
)
from src.agents.rag.retriever import RAGRetriever
from src.agents.rag.store import (
    DEFAULT_DIMENSION,
    DeterministicVectorizer,
    VectorStore,
    get_vectorizer,
)
from src.agents.tools import search_logs_and_model_cards


class TestDocumentChunk:
    """Tests for DocumentChunk data structure."""

    def test_chunk_serialization_cycle(self):
        chunk = DocumentChunk(
            chunk_id="test_001",
            title="Test Document",
            source="docs/Test.md",
            heading="Section 1",
            content="This is sample content for testing.",
            metadata={"priority": "high", "char_count": 35},
        )
        as_dict = chunk.to_dict()
        assert as_dict["chunk_id"] == "test_001"
        assert as_dict["title"] == "Test Document"
        assert as_dict["metadata"]["priority"] == "high"

        reconstructed = DocumentChunk.from_dict(as_dict)
        assert reconstructed.chunk_id == chunk.chunk_id
        assert reconstructed.title == chunk.title
        assert reconstructed.content == chunk.content
        assert reconstructed.metadata == chunk.metadata


class TestMarkdownChunker:
    """Tests for markdown parsing, section splitting, and header preservation."""

    def test_extract_markdown_chunks_from_temp_dir(self, tmp_path: Path):
        test_file = tmp_path / "architecture.md"
        test_file.write_text(
            "# System Architecture\n\n"
            "Overview of the logistics platform.\n\n"
            "## Storage Engine\n\n"
            "PostgreSQL 16 is used for warehouse tables.\n"
            "Redis 7 is used for online feature caching.\n\n"
            "### Network Topologies\n\n"
            "All internal services communicate on internal bridge.\n",
            encoding="utf-8",
        )

        chunks = extract_markdown_chunks(tmp_path, chunk_size=300)
        assert len(chunks) >= 3

        headings = [c.heading for c in chunks]
        assert "Overview" in headings or "Storage Engine" in headings
        assert any("Storage Engine" in c.heading for c in chunks)
        assert any("Network Topologies" in c.heading for c in chunks)

        for c in chunks:
            assert c.title == "System Architecture"
            assert "architecture.md" in c.source
            assert len(c.chunk_id) > 0
            assert "[" in c.content  # context prefix present

    def test_extract_markdown_chunks_missing_dir(self, tmp_path: Path):
        missing = tmp_path / "non_existent_folder"
        chunks = extract_markdown_chunks(missing)
        assert chunks == []

    def test_large_section_subchunking(self, tmp_path: Path):
        test_file = tmp_path / "long_doc.md"
        long_paragraph = "A" * 400 + "\n\n" + "B" * 400 + "\n\n" + "C" * 400
        test_file.write_text(
            f"# Big Document\n\n## Deep Dive\n\n{long_paragraph}", encoding="utf-8"
        )

        chunks = extract_markdown_chunks(tmp_path, chunk_size=450)
        assert len(chunks) >= 2
        for c in chunks:
            assert c.title == "Big Document"
            assert c.heading == "Deep Dive"


class TestModelCardsAndPipelineSummaries:
    """Tests for model card extraction and pipeline run summaries."""

    def test_extract_mlflow_model_cards_fallback(self):
        # When no client is provided and live MLflow is unreachable
        with patch("src.agents.rag.indexer._is_tcp_port_open", return_value=False):
            cards = extract_mlflow_model_cards(client=None)
            assert len(cards) >= 2
            card_titles = [c.title for c in cards]
            assert any("demand_lightgbm_model" in t for t in card_titles)
            assert any("corridor_duration_lightgbm_model" in t for t in card_titles)
            assert all(c.metadata.get("is_baseline") for c in cards)

    def test_extract_mlflow_model_cards_with_mock_client(self):
        mock_client = MagicMock()
        mock_version = MagicMock()
        mock_version.version = "3"
        mock_version.current_stage = "Production"
        mock_version.run_id = "run_abc_123"

        mock_client.search_model_versions.return_value = [mock_version]
        mock_run = MagicMock()
        mock_run.data.metrics = {"val_mae": 3.14, "val_rmse": 4.52}
        mock_run.data.params = {"n_estimators": "100", "learning_rate": "0.05"}
        mock_client.get_run.return_value = mock_run

        cards = extract_mlflow_model_cards(client=mock_client)
        assert len(cards) >= 2
        first = cards[0]
        assert "Production" in first.content
        assert "val_mae=3.1400" in first.content
        assert first.metadata["run_id"] == "run_abc_123"

    def test_extract_pipeline_run_summaries_fallback(self):
        with patch("src.agents.rag.indexer._is_tcp_port_open", return_value=False):
            summaries = extract_pipeline_run_summaries(db_session=None)
            assert len(summaries) >= 2
            titles = [s.title for s in summaries]
            assert any("Retraining Flow" in t for t in titles)
            assert any("ETL" in t for t in titles)

    def test_extract_pipeline_run_summaries_with_mock_db(self):
        mock_session = MagicMock()
        mock_run1 = MagicMock(
            flow_name="retraining_flow",
            status="completed",
            duration_seconds=42.5,
            records_processed=1500,
            started_at=MagicMock(isoformat=lambda: "2026-09-14T02:00:00Z"),
            error_message=None,
        )
        mock_run2 = MagicMock(
            flow_name="retraining_flow",
            status="failed",
            duration_seconds=10.0,
            records_processed=0,
            started_at=MagicMock(isoformat=lambda: "2026-09-13T02:00:00Z"),
            error_message="Feature store timeout",
        )

        mock_query = MagicMock()
        mock_query.order_by.return_value.limit.return_value.all.return_value = [
            mock_run1,
            mock_run2,
        ]
        mock_session.query.return_value = mock_query
        mock_session.__enter__.return_value = mock_session

        summaries = extract_pipeline_run_summaries(db_session=mock_session)
        assert len(summaries) == 1
        summary = summaries[0]
        assert summary.title == "Pipeline Health Summary: retraining_flow"
        assert "Completed: 1, Failed: 1" in summary.content
        assert summary.metadata["flow_name"] == "retraining_flow"

    def test_extract_monitoring_report_summaries_fallback(self):
        """When DB is unavailable, returns baseline monitoring architecture catalog."""
        with patch("src.agents.rag.indexer._is_tcp_port_open", return_value=False):
            summaries = extract_monitoring_report_summaries(db_session=None)
            assert len(summaries) >= 2
            titles = [s.title for s in summaries]
            assert any("Evidently AI Drift Analyzers" in t for t in titles)
            assert any("Staged Drift Retraining Gate" in t for t in titles)
            for s in summaries:
                assert s.metadata.get("is_baseline") is True

    def test_extract_monitoring_report_summaries_with_mock_db_retention_and_pruning(
        self,
    ):
        """Validates ADR-026 14-day rolling window, consolidated overview chunk, and report metadata."""
        now = datetime(2026, 9, 22, 12, 0, 0, tzinfo=timezone.utc)
        mock_session = MagicMock()

        # Report within 14-day window (generated 2 days ago)
        report_recent = MagicMock(
            report_id="rep_recent_01",
            report_type="data_drift",
            generated_at=now - timedelta(days=2),
            summary_json={
                "drift_detected": True,
                "retrain_recommended": True,
                "drift_share": 0.45,
                "alert_severity": "CRITICAL",
                "alert_reasons": ["Feature drift share 45.0% exceeded threshold 40.0%"],
                "metrics": [{"column_name": "passenger_count", "drift_detected": True}],
            },
            file_path="artifacts/monitoring_reports/rep_recent_01.html",
        )

        mock_query = MagicMock()
        # Query filter mock returns only the recent report (matching SQL filter >= cutoff)
        mock_query.filter.return_value.order_by.return_value.all.return_value = [
            report_recent
        ]
        mock_session.query.return_value = mock_query
        mock_session.__enter__.return_value = mock_session

        chunks = extract_monitoring_report_summaries(db_session=mock_session, now=now)

        # Expected: 1 consolidated health overview chunk + 1 individual report chunk = 2 chunks
        assert len(chunks) == 2
        overview = chunks[0]
        assert overview.title == "14-Day Monitoring Health Overview"
        assert overview.metadata["doc_type"] == "monitoring_summary_14d"
        assert overview.metadata["active_alerts"] == 1
        assert overview.metadata["total_reports"] == 1
        assert "Average Feature Drift Share: 45.0%" in overview.content
        assert "Retraining Recommended" in overview.content

        rep_chunk = chunks[1]
        assert "Monitoring Report: data_drift" in rep_chunk.title
        assert rep_chunk.metadata["report_id"] == "rep_recent_01"
        assert rep_chunk.metadata["drift_detected"] is True
        assert rep_chunk.metadata["retrain_recommended"] is True
        assert "Drifted Columns: passenger_count" in rep_chunk.content

    def test_extract_monitoring_report_summaries_capping_max_10_per_type(self):
        """Proves maximum 10 reports per type retention bound (max 30 total across 3 types)."""
        now = datetime(2026, 9, 22, 12, 0, 0, tzinfo=timezone.utc)
        mock_session = MagicMock()

        # 15 data_drift reports generated within window
        data_drift_reports = [
            MagicMock(
                report_id=f"rep_dd_{i:02d}",
                report_type="data_drift",
                generated_at=now - timedelta(hours=i),
                summary_json={"drift_detected": False, "drift_share": 0.10},
                file_path=None,
            )
            for i in range(15)
        ]

        mock_query = MagicMock()
        mock_query.filter.return_value.order_by.return_value.all.return_value = (
            data_drift_reports
        )
        mock_session.query.return_value = mock_query
        mock_session.__enter__.return_value = mock_session

        chunks = extract_monitoring_report_summaries(db_session=mock_session, now=now)

        # 1 consolidated summary chunk + max 10 individual report chunks = 11 chunks
        assert len(chunks) == 11
        # Overview chunk + 10 data_drift chunks
        assert chunks[0].title == "14-Day Monitoring Health Overview"
        individual_chunks = chunks[1:]
        assert len(individual_chunks) == 10
        for c in individual_chunks:
            assert c.metadata["report_type"] == "data_drift"


class TestVectorizers:
    """Tests for vectorizer behavior, dimension guarantees, and normalization."""

    def test_deterministic_vectorizer_properties(self):
        vec = DeterministicVectorizer(dimension=DEFAULT_DIMENSION)
        texts = [
            "NYC taxi demand forecasting in Manhattan",
            "ETA corridor travel duration in seconds",
        ]

        vectors = vec.embed_documents(texts)
        assert vectors.shape == (2, DEFAULT_DIMENSION)
        assert vectors.dtype == np.float32

        # Verify strict unit L2 norm
        norms = np.linalg.norm(vectors, axis=1)
        np.testing.assert_allclose(norms, [1.0, 1.0], atol=1e-5)

        # Query embedding
        q_vec = vec.embed_query("taxi demand")
        assert q_vec.shape == (1, DEFAULT_DIMENSION)
        np.testing.assert_allclose(np.linalg.norm(q_vec), 1.0, atol=1e-5)

    def test_deterministic_vectorizer_repeatability(self):
        vec1 = DeterministicVectorizer(dimension=128)
        vec2 = DeterministicVectorizer(dimension=128)
        text = "Champion challenger model promotion hurdle rate of 2.0%"

        v1 = vec1.embed_query(text)
        v2 = vec2.embed_query(text)
        np.testing.assert_array_almost_equal(v1, v2)

    def test_vectorizer_factory_fallback(self):
        # Requesting fastembed when not installed or forced fallback
        vec = get_vectorizer("deterministic")
        assert isinstance(vec, DeterministicVectorizer)
        assert vec.dimension == DEFAULT_DIMENSION


class TestVectorStore:
    """Tests for FAISS vector store creation, indexing, search, and disk serialization."""

    def test_vector_store_add_search_cycle(self, tmp_path: Path):
        store = VectorStore(
            dimension=DEFAULT_DIMENSION,
            vectorizer=DeterministicVectorizer(dimension=DEFAULT_DIMENSION),
        )
        chunks = [
            DocumentChunk(
                "c1",
                "Demand Model",
                "docs/AI.md",
                "Demand",
                "LightGBM model predicting NYC taxi pickup demand across zones",
                {},
            ),
            DocumentChunk(
                "c2",
                "Corridor Model",
                "docs/AI.md",
                "ETA",
                "Corridor travel duration in seconds between origin and destination pairs",
                {},
            ),
            DocumentChunk(
                "c3",
                "PostgreSQL",
                "docs/DB.md",
                "Storage",
                "PostgreSQL relational warehouse schema with partitioned trips",
                {},
            ),
        ]

        added = store.add_chunks(chunks)
        assert added == 3
        assert store.size == 3

        # Search for demand
        matches = store.search("taxi pickup demand", top_k=2)
        assert len(matches) == 2
        top_chunk, score = matches[0]
        assert top_chunk.chunk_id == "c1"
        assert score > 0.0

        # Save to disk
        out_dir = tmp_path / "test_faiss_index"
        store.save(out_dir)

        assert (out_dir / "index.faiss").exists()
        assert (out_dir / "chunks.json").exists()
        assert (out_dir / "metadata.json").exists()

        # Reload from disk
        loaded_store = VectorStore.load(
            out_dir,
            vectorizer=DeterministicVectorizer(dimension=DEFAULT_DIMENSION),
        )
        assert loaded_store.size == 3
        assert loaded_store.dimension == DEFAULT_DIMENSION

        loaded_matches = loaded_store.search("taxi pickup demand", top_k=2)
        assert len(loaded_matches) == 2
        assert loaded_matches[0][0].chunk_id == "c1"

    def test_vector_store_empty_queries(self):
        store = VectorStore(
            dimension=64, vectorizer=DeterministicVectorizer(dimension=64)
        )
        assert store.search("anything") == []
        assert store.search("") == []


class TestRAGRetriever:
    """Tests for RAGRetriever wrapper and singleton factory."""

    def test_retriever_search_and_threshold(self):
        store = VectorStore(
            dimension=128, vectorizer=DeterministicVectorizer(dimension=128)
        )
        store.add_chunks(
            [
                DocumentChunk(
                    "k1",
                    "ADR-021",
                    "docs/Decisions.md",
                    "ADR-021",
                    "Model promotion hurdle rate 2.0%",
                    {},
                ),
                DocumentChunk(
                    "k2",
                    "ADR-020",
                    "docs/Decisions.md",
                    "ADR-020",
                    "Online serving degraded fallbacks",
                    {},
                ),
            ]
        )

        retriever = RAGRetriever(vector_store=store)
        assert retriever.is_ready() is True

        results = retriever.search("hurdle rate promotion", top_k=1)
        assert len(results) == 1
        assert results[0]["title"] == "ADR-021"
        assert "score" in results[0]
        assert "content" in results[0]

        # High threshold filtering
        filtered = retriever.search(
            "hurdle rate promotion", top_k=1, score_threshold=0.99
        )
        # Score likely < 0.99 for sparse hashing
        assert len(filtered) <= 1

    def test_search_logs_and_model_cards_tool_integration(self):
        # Verify the agent tool search_logs_and_model_cards connects to the FAISS retriever
        result = search_logs_and_model_cards("ModelPromotionGate hurdle rate", top_k=3)
        assert result["status"] == "success"
        assert result["source"] == "faiss_index"
        assert result["results_count"] > 0
        assert len(result["results"]) > 0
        first = result["results"][0]
        assert "title" in first
        assert "source" in first
        assert "content" in first
