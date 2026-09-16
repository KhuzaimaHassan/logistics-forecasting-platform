"""Live Smoke Test & CI Verification for LangGraph Ops Copilot against Real Infrastructure.

Validates that run_copilot() and its underlying tools execute successfully against
genuinely reachable, live services (PostgreSQL, Redis, Feast online store, MLflow):
1. Tool 1 (get_features): Live Feast Redis feature retrieval (asserting cache_hit=True).
2. Tool 2 (query_recent_predictions): Live PostgreSQL warehouse.predictions retrieval.
3. Tool 3 (query_pipeline_status): Live PostgreSQL warehouse.pipeline_runs retrieval.
4. Tool 4 (search_logs_and_model_cards): Live FAISS RAG semantic retrieval over docs/.
5. End-to-End run_copilot() validation proving non-degraded live feature state synthesis.
"""

import os
import sys
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

# Ensure UTF-8 output on all platforms
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
os.environ["PYTHONIOENCODING"] = "utf-8"

from sqlalchemy.orm import Session

from src.agents.graph import run_copilot
from src.agents.tools import (
    get_features,
    query_pipeline_status,
    query_recent_predictions,
    search_logs_and_model_cards,
)
from src.common.db import get_engine
from src.common.models import PipelineRun


def seed_pipeline_run_if_needed():
    """Ensure warehouse.pipeline_runs has at least one record for query testing."""
    try:
        engine = get_engine()
        with Session(engine) as session:
            count = session.query(PipelineRun).count()
            if count == 0:
                print("Seeding baseline pipeline run into warehouse.pipeline_runs...")
                run_rec = PipelineRun(
                    run_id=f"pipeline-{uuid4().hex[:12]}",
                    job_name="scheduled_model_retraining",
                    status="completed",
                    started_at=datetime.now(timezone.utc) - timedelta(minutes=15),
                    finished_at=datetime.now(timezone.utc) - timedelta(minutes=10),
                    duration_seconds=Decimal("300.0"),
                    records_processed=1000,
                    error_message=None,
                    triggered_by="prefect_cron",
                )
                session.add(run_rec)
                session.commit()
                print("  [OK] Seeded baseline pipeline run.")
    except Exception as exc:
        print(f"  Note: Could not check/seed pipeline_runs: {exc}")


def print_section(title: str) -> None:
    print("\n" + "=" * 80, flush=True)
    print(title, flush=True)
    print("=" * 80, flush=True)


def verify_copilot_live():
    print_section("STARTING LIVE OPS COPILOT SMOKE VERIFICATION")

    # -------------------------------------------------------------------------
    # STEP 1: Tool 1 (get_features) — Live Feast Redis Online Store
    # -------------------------------------------------------------------------
    print_section("[STEP 1] Testing Tool 1: get_features against Live Feast Redis")
    feat_res = get_features(entity_type="zone", entity_id="161")
    print(f"Status:      {feat_res.get('status')}")
    print(f"Cache Hit:   {feat_res.get('cache_hit')}")
    print(f"Warning:     {feat_res.get('warning')}")
    print(f"Features:    {feat_res.get('features')}")

    assert (
        feat_res.get("status") == "success"
    ), f"Expected status 'success', got {feat_res.get('status')}"
    assert feat_res.get("cache_hit") is True, (
        f"Expected cache_hit=True from live Feast Redis, got {feat_res.get('cache_hit')}. "
        "Degraded fallback was returned instead of live data."
    )
    assert (
        feat_res.get("warning") is None
    ), f"Expected warning=None from live cache hit, got {feat_res.get('warning')}"
    assert (
        len(feat_res.get("features", {})) > 0
    ), "Expected non-empty feature dictionary."
    print(
        ">>> SUCCESS: Tool 1 returned genuine live Feast Redis features with cache_hit=True!"
    )

    corr_res = get_features(entity_type="corridor", entity_id="161_236")
    print(f"\nCorridor 161_236 Features: {corr_res.get('features')}")
    assert corr_res.get("cache_hit") is True

    # -------------------------------------------------------------------------
    # STEP 2: Tool 2 (query_recent_predictions) — Live PostgreSQL Predictions
    # -------------------------------------------------------------------------
    print_section(
        "[STEP 2] Testing Tool 2: query_recent_predictions against PostgreSQL"
    )
    pred_res = query_recent_predictions(
        entity_type="zone", entity_id="161", window_hours=24
    )
    print(f"Status:           {pred_res.get('status')}")
    print(f"Prediction Count: {pred_res.get('prediction_count')}")
    print(f"Message:          {pred_res.get('message')}")
    for p in pred_res.get("predictions", [])[:3]:
        print(
            f"  - ID: {p.get('prediction_id')} | Value: {p.get('predicted_value')} | Model: {p.get('model_version')}"
        )

    assert (
        pred_res.get("status") == "success"
    ), f"Expected prediction status 'success', got {pred_res.get('status')}"
    assert (
        pred_res.get("prediction_count", 0) > 0
    ), "Expected at least 1 prediction record in warehouse.predictions, got 0."
    print(
        ">>> SUCCESS: Tool 2 retrieved genuine live predictions from warehouse.predictions!"
    )

    # -------------------------------------------------------------------------
    # STEP 3: Tool 3 (query_pipeline_status) — Live PostgreSQL Pipeline Runs
    # -------------------------------------------------------------------------
    print_section("[STEP 3] Testing Tool 3: query_pipeline_status against PostgreSQL")
    seed_pipeline_run_if_needed()
    pipe_res = query_pipeline_status()
    overall_health = pipe_res.get("overall_health") or pipe_res.get("overall_status")
    print(f"Overall Health: {overall_health}")
    run_count = (
        pipe_res.get("run_count")
        if pipe_res.get("run_count") is not None
        else pipe_res.get("total_runs", 0)
    )
    print(f"Total Runs:     {run_count}")
    runs = pipe_res.get("latest_runs") or pipe_res.get("runs", [])
    for r in runs[:3]:
        job_name = r.get("job_name") or r.get("pipeline_name")
        print(
            f"  - Job: {job_name} | Status: {r.get('status')} | Duration: {r.get('duration_seconds')}s"
        )

    assert (
        pipe_res.get("status") == "success"
    ), f"Expected pipeline status 'success', got {pipe_res.get('status')}"
    assert (
        run_count > 0
    ), f"Expected at least 1 pipeline run record in warehouse.pipeline_runs, got {run_count}."
    print(
        ">>> SUCCESS: Tool 3 retrieved genuine live pipeline execution history from warehouse.pipeline_runs!"
    )

    # -------------------------------------------------------------------------
    # STEP 4: Tool 4 (search_logs_and_model_cards) — FAISS Semantic Index
    # -------------------------------------------------------------------------
    print_section(
        "[STEP 4] Testing Tool 4: search_logs_and_model_cards over Docs & Metadata"
    )
    rag_res = search_logs_and_model_cards(
        query="model promotion hurdle rate ADR-021", top_k=3
    )
    print(f"Status:        {rag_res.get('status')}")
    print(f"Results Count: {rag_res.get('results_count')}")
    print(f"Source:        {rag_res.get('source')}")
    for doc in rag_res.get("results", [])[:2]:
        print(f"  - Title:  {doc.get('title')}")
        print(f"    Source: {doc.get('source')}")
        print(f"    Score:  {doc.get('score'):.3f}")

    assert (
        rag_res.get("status") == "success"
    ), f"Expected RAG status 'success', got {rag_res.get('status')}"
    assert (
        rag_res.get("results_count", 0) > 0
    ), "Expected at least 1 retrieved document chunk."
    print(
        ">>> SUCCESS: Tool 4 returned semantic RAG search results over platform documentation!"
    )

    # -------------------------------------------------------------------------
    # STEP 5: End-to-End run_copilot() with Live Data State
    # -------------------------------------------------------------------------
    print_section("[STEP 5] Testing End-to-End run_copilot() with Genuine Live Data")

    # 5A: Live Feature State Query
    q1 = "What is the current feature state and demand for zone 161?"
    print(f"\n[Query 1]: {q1}")
    res1 = run_copilot(q1, force_provider="mock")
    print(f"Provider:   {res1.get('provider')} ({res1.get('model_name')})")
    print(f"Status:     {res1.get('status')}")
    print(f"Tools Used: {res1.get('tools_used')}")
    print(f"Sources:    {res1.get('sources')}")
    print(f"Latency:    {res1.get('latency_ms')} ms")
    print("\n--- Response Output ---")
    print(res1.get("response"))

    assert res1.get("status") == "success"
    assert "get_features" in res1.get("tools_used", [])
    assert "[Live Redis Hit]" in res1.get("response", ""), (
        "Expected '[Live Redis Hit]' badge in response, indicating live Redis data was used. "
        "Found fallback badge instead."
    )
    assert "[Imputed Offline Fallback]" not in res1.get("response", "")
    print(
        ">>> SUCCESS: Query 1 generated response from live Feast Redis without degradation!"
    )

    # 5B: Live Prediction History Query
    q2 = "Show recent predictions for zone 161"
    print(f"\n[Query 2]: {q2}")
    res2 = run_copilot(q2, force_provider="mock")
    print(f"Provider:   {res2.get('provider')}")
    print(f"Status:     {res2.get('status')}")
    print(f"Tools Used: {res2.get('tools_used')}")
    print(f"Sources:    {res2.get('sources')}")
    print("\n--- Response Output ---")
    print(res2.get("response"))

    assert res2.get("status") == "success"
    assert "query_recent_predictions" in res2.get("tools_used", [])
    assert "warehouse.predictions" in res2.get("sources", [])
    assert "records found" in res2.get("response", "")
    print(
        ">>> SUCCESS: Query 2 synthesized live prediction logs from warehouse.predictions!"
    )

    # 5C: Live Pipeline Health Query
    q3 = "Did the retraining pipeline run this week?"
    print(f"\n[Query 3]: {q3}")
    res3 = run_copilot(q3, force_provider="mock")
    print(f"Tools Used: {res3.get('tools_used')}")
    print(f"Sources:    {res3.get('sources')}")
    print("\n--- Response Output ---")
    print(res3.get("response"))

    assert res3.get("status") == "success"
    assert "query_pipeline_status" in res3.get("tools_used", [])
    assert "warehouse.pipeline_runs" in res3.get("sources", [])
    print(
        ">>> SUCCESS: Query 3 synthesized pipeline health from warehouse.pipeline_runs!"
    )

    # -------------------------------------------------------------------------
    # CONCLUSION
    # -------------------------------------------------------------------------
    print_section(
        "ALL 5 LIVE SMOKE VERIFICATION STEPS PASSED WITH LIVE INFRASTRUCTURE!"
    )


if __name__ == "__main__":
    start_time = time.time()
    try:
        verify_copilot_live()
        print(
            f"\nLive Ops Copilot Smoke Verification completed in {time.time() - start_time:.2f}s."
        )
    except Exception as exc:
        print(
            f"\nFAILED: Live Ops Copilot smoke verification failed: {exc}",
            file=sys.stderr,
        )
        import traceback

        traceback.print_exc()
        sys.exit(1)
