"""Unit and integration tests for M7-1 Agent Layer Core Tools and Prediction Logging.

Tests cover:
1. get_features tool (zone & corridor, cache hit, degraded fallback, error cases, bounds checking)
2. query_recent_predictions tool (zone & corridor, window bounds, empty, error handling)
3. query_pipeline_status tool (health calculation: healthy, degraded, empty)
4. search_logs_and_model_cards tool (query matching, empty query, fallback scoring)
5. TOOL_ALLOWLIST and read-only security boundary validation
6. LangChain StructuredTool interface invocation (.invoke)
7. Prediction logging background tasks in FastAPI serving endpoints
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src.agents.tools import (
    AGENT_TOOLS,
    TOOL_ALLOWLIST,
    get_features,
    get_features_tool,
    query_pipeline_status,
    query_pipeline_status_tool,
    query_recent_predictions,
    query_recent_predictions_tool,
    search_logs_and_model_cards,
    search_logs_and_model_cards_tool,
)
from src.common.models import PipelineRun, Prediction
from src.features.client import (
    CorridorDurationOnlineFeatures,
    ZoneDemandOnlineFeatures,
)
from src.serving.app import (
    app,
    log_prediction_background,
    log_predictions_batch_background,
)
from src.serving.cache import PredictionCache
from src.serving.model_loader import (
    DEMAND_MODEL_NAME,
    DURATION_MODEL_NAME,
    LoadedModelInfo,
)
from src.training.baseline import (
    CorridorDurationBaseline,
    DemandSeasonalNaiveBaseline,
)


@pytest.fixture(autouse=True)
def configure_mock_serving_state():
    """Ensure app.state has fast deterministic mocks for all serving endpoint tests."""
    demand_info = LoadedModelInfo(
        model_name=DEMAND_MODEL_NAME,
        version="test-agent-v1",
        stage="Production",
        run_id="test-run-demand",
        loaded_at="2026-09-14T12:00:00Z",
        status="production",
        is_fallback=False,
        model=DemandSeasonalNaiveBaseline(),
        metadata={"source": "test"},
    )
    duration_info = LoadedModelInfo(
        model_name=DURATION_MODEL_NAME,
        version="test-agent-v1",
        stage="Production",
        run_id="test-run-duration",
        loaded_at="2026-09-14T12:00:00Z",
        status="production",
        is_fallback=False,
        model=CorridorDurationBaseline(),
        metadata={"source": "test"},
    )

    mock_loader = MagicMock()
    mock_loader.get_model.side_effect = lambda name: (
        demand_info if name == DEMAND_MODEL_NAME else duration_info
    )

    mock_feast = MagicMock()
    mock_feast.get_zone_demand_features.side_effect = lambda zids: [
        ZoneDemandOnlineFeatures(
            zone_id=z,
            pickup_count_last_15m=12,
            pickup_count_last_1h=45,
            pickup_count_last_24h=850,
            hour_of_day=14,
            day_of_week=2,
            cache_hit=True,
        )
        for z in zids
    ]
    mock_feast.get_corridor_duration_features.side_effect = lambda cids: [
        CorridorDurationOnlineFeatures(
            corridor_id=c,
            avg_duration_last_15m=870.0,
            avg_duration_last_1h=720.0,
            distance_km=3.8,
            cache_hit=True,
        )
        for c in cids
    ]

    app.state.model_loader = mock_loader
    app.state.feast_client = mock_feast
    app.state.cache = PredictionCache(redis_url=None)
    app.state.disable_prediction_logging = False

    yield

    if hasattr(app.state, "cache") and app.state.cache is not None:
        app.state.cache.clear()


# ===========================================================================
# Tool 1: get_features Tests
# ===========================================================================


def test_get_features_zone_cache_hit():
    """Verify get_features for zone with active Redis features."""
    mock_feast = MagicMock()
    mock_feat = ZoneDemandOnlineFeatures(
        zone_id=161,
        pickup_count_last_15m=18,
        pickup_count_last_1h=65,
        pickup_count_last_24h=1100,
        pickup_count_same_hour_last_week=55,
        hour_of_day=14,
        day_of_week=3,
        is_weekend=False,
        is_holiday=False,
        cache_hit=True,
    )
    mock_feast.get_zone_demand_features.return_value = [mock_feat]

    with patch("src.features.client.get_online_client", return_value=mock_feast):
        res = get_features("zone", "161")

    assert res["status"] == "success"
    assert res["entity_type"] == "zone"
    assert res["entity_id"] == 161
    assert res["cache_hit"] is True
    assert res["warning"] is None
    assert res["features"]["pickup_count_last_15m"] == 18
    assert "zone_id" not in res["features"]


def test_get_features_zone_degraded_fallback():
    """Verify get_features for zone with cold/unmaterialized features returns degraded status."""
    mock_feast = MagicMock()
    mock_feat = ZoneDemandOnlineFeatures(zone_id=236, cache_hit=False)
    mock_feast.get_zone_demand_features.return_value = [mock_feat]

    with patch("src.features.client.get_online_client", return_value=mock_feast):
        res = get_features("zone", 236)

    assert res["status"] == "degraded_fallback"
    assert res["entity_type"] == "zone"
    assert res["entity_id"] == 236
    assert res["cache_hit"] is False
    assert res["warning"] is not None
    assert "unmaterialized" in res["warning"]


@pytest.mark.parametrize("invalid_zid", [0, -5, 264, 265, 999])
def test_get_features_zone_out_of_bounds(invalid_zid):
    """Verify out-of-bounds zone IDs return structured error."""
    res = get_features("zone", str(invalid_zid))
    assert res["status"] == "error"
    assert res["cache_hit"] is False
    assert "outside the valid NYC TLC zone range" in res["error"]


def test_get_features_zone_invalid_type():
    """Verify non-integer zone ID returns structured error."""
    res = get_features("zone", "midtown_central")
    assert res["status"] == "error"
    assert "must be an integer" in res["error"]


def test_get_features_corridor_cache_hit():
    """Verify get_features for corridor with active Redis features."""
    mock_feast = MagicMock()
    mock_feat = CorridorDurationOnlineFeatures(
        corridor_id="161_237",
        avg_duration_last_15m=750.0,
        avg_duration_last_1h=720.0,
        distance_km=3.2,
        origin_zone_demand_pressure=45,
        avg_traffic_speed_current=22.5,
        cache_hit=True,
    )
    mock_feast.get_corridor_duration_features.return_value = [mock_feat]

    with patch("src.features.client.get_online_client", return_value=mock_feast):
        res = get_features("corridor", "161_237")

    assert res["status"] == "success"
    assert res["entity_type"] == "corridor"
    assert res["entity_id"] == "161_237"
    assert res["cache_hit"] is True
    assert res["features"]["distance_km"] == 3.2
    assert "corridor_id" not in res["features"]


def test_get_features_corridor_degraded_fallback():
    """Verify get_features for corridor with unmaterialized features returns degraded status."""
    mock_feast = MagicMock()
    mock_feat = CorridorDurationOnlineFeatures(corridor_id="142_236", cache_hit=False)
    mock_feast.get_corridor_duration_features.return_value = [mock_feat]

    with patch("src.features.client.get_online_client", return_value=mock_feast):
        res = get_features("corridor", "142_236")

    assert res["status"] == "degraded_fallback"
    assert res["cache_hit"] is False
    assert res["warning"] is not None


@pytest.mark.parametrize("invalid_cid", ["161-237", "abc_123", "161", "161_237_999"])
def test_get_features_corridor_malformed_format(invalid_cid):
    """Verify malformed corridor ID format returns error."""
    res = get_features("corridor", invalid_cid)
    assert res["status"] == "error"
    assert "must be formatted as '{origin}_{dest}'" in res["error"]


@pytest.mark.parametrize("out_bounds_cid", ["0_237", "161_0", "264_100", "161_265"])
def test_get_features_corridor_out_of_bounds(out_bounds_cid):
    """Verify corridor containing out-of-bounds zone IDs returns error."""
    res = get_features("corridor", out_bounds_cid)
    assert res["status"] == "error"
    assert "outside the valid NYC TLC zone range" in res["error"]


def test_get_features_invalid_entity_domain():
    """Verify invalid entity domain returns error."""
    res = get_features("vehicle", "100")
    assert res["status"] == "error"
    assert "Invalid entity_type" in res["error"]


def test_get_features_feast_connection_exception_fallback():
    """Verify that Feast connection exceptions fall back to imputed baseline features."""
    with patch(
        "src.features.client.get_online_client",
        side_effect=Exception("Redis connection refused"),
    ):
        res = get_features("zone", "161")
    assert res["status"] == "degraded_fallback"
    assert res["cache_hit"] is False
    assert "imputed baseline defaults" in res["warning"]
    assert "pickup_count_last_15m" in res["features"]


# ===========================================================================
# Tool 2: query_recent_predictions Tests
# ===========================================================================


def test_query_recent_predictions_success():
    """Verify querying predictions returns structured list and counts."""
    now_utc = datetime.now(timezone.utc)
    mock_preds = [
        Prediction(
            prediction_id="p-1",
            entity_type="zone",
            entity_id="161",
            model_version="demand-v2",
            predicted_value=Decimal("24.50"),
            predicted_at=now_utc - timedelta(minutes=10),
            actual_value=Decimal("22.00"),
            actual_recorded_at=now_utc - timedelta(minutes=5),
        ),
        Prediction(
            prediction_id="p-2",
            entity_type="zone",
            entity_id="161",
            model_version="demand-v2",
            predicted_value=Decimal("21.00"),
            predicted_at=now_utc - timedelta(minutes=25),
            actual_value=None,
            actual_recorded_at=None,
        ),
    ]

    mock_session = MagicMock()
    mock_query = mock_session.query.return_value
    mock_filter = mock_query.filter.return_value
    mock_order = mock_filter.order_by.return_value
    mock_limit = mock_order.limit.return_value
    mock_limit.all.return_value = mock_preds

    with patch("src.agents.tools.get_db_session") as mock_get_session:
        mock_get_session.return_value.__enter__.return_value = mock_session
        res = query_recent_predictions("zone", "161", window_hours=6)

    assert res["status"] == "success"
    assert res["prediction_count"] == 2
    assert len(res["predictions"]) == 2
    assert res["predictions"][0]["prediction_id"] == "p-1"
    assert res["predictions"][0]["predicted_value"] == 24.50
    assert res["predictions"][0]["actual_value"] == 22.00
    assert res["predictions"][1]["actual_value"] is None


def test_query_recent_predictions_empty():
    """Verify querying empty predictions returns empty status."""
    mock_session = MagicMock()
    mock_session.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = (
        []
    )

    with patch("src.agents.tools.get_db_session") as mock_get_session:
        mock_get_session.return_value.__enter__.return_value = mock_session
        res = query_recent_predictions("corridor", "161_237", window_hours=12)

    assert res["status"] == "empty"
    assert res["prediction_count"] == 0
    assert res["predictions"] == []
    assert "No predictions found" in res["message"]


def test_query_recent_predictions_invalid_type():
    """Verify invalid entity type returns error."""
    res = query_recent_predictions("fleet", "1")
    assert res["status"] == "error"
    assert "Invalid entity_type" in res["error"]


def test_query_recent_predictions_db_error_resilience():
    """Verify database errors are caught and return error status without crashing."""
    with patch(
        "src.agents.tools.get_db_session",
        side_effect=Exception("Database connection timeout"),
    ):
        res = query_recent_predictions("zone", "161")

    assert res["status"] == "error"
    assert "Database prediction query failed" in res["error"]
    assert res["prediction_count"] == 0


# ===========================================================================
# Tool 3: query_pipeline_status Tests
# ===========================================================================


def test_query_pipeline_status_healthy():
    """Verify overall health calculation when pipeline runs completed successfully."""
    now_utc = datetime.now(timezone.utc)
    mock_runs = [
        PipelineRun(
            run_id="run-1",
            job_name="scheduled_retraining",
            status="completed",
            started_at=now_utc - timedelta(hours=1),
            finished_at=now_utc - timedelta(minutes=50),
            duration_seconds=Decimal("600.0"),
            records_processed=15000,
            error_message=None,
            triggered_by="prefect_cron",
        ),
        PipelineRun(
            run_id="run-2",
            job_name="tlc_hourly_sync",
            status="completed",
            started_at=now_utc - timedelta(hours=2),
            finished_at=now_utc - timedelta(hours=2, minutes=-5),
            duration_seconds=Decimal("300.0"),
            records_processed=4500,
            error_message=None,
            triggered_by="prefect_schedule",
        ),
    ]

    mock_session = MagicMock()
    mock_session.query.return_value.order_by.return_value.limit.return_value.all.return_value = (
        mock_runs
    )

    with patch("src.agents.tools.get_db_session") as mock_get_session:
        mock_get_session.return_value.__enter__.return_value = mock_session
        res = query_pipeline_status()

    assert res["status"] == "success"
    assert res["overall_health"] == "healthy"
    assert res["run_count"] == 2
    assert res["latest_runs"][0]["job_name"] == "scheduled_retraining"
    assert res["latest_runs"][0]["records_processed"] == 15000


def test_query_pipeline_status_degraded_on_recent_failure():
    """Verify status is degraded when recent run failed."""
    now_utc = datetime.now(timezone.utc)
    mock_runs = [
        PipelineRun(
            run_id="run-failed",
            job_name="stream_reconciliation",
            status="failed",
            started_at=now_utc - timedelta(minutes=15),
            finished_at=now_utc - timedelta(minutes=14),
            duration_seconds=Decimal("60.0"),
            records_processed=0,
            error_message="Redpanda broker unreachable",
            triggered_by="prefect_cron",
        )
    ]

    mock_session = MagicMock()
    mock_session.query.return_value.order_by.return_value.limit.return_value.all.return_value = (
        mock_runs
    )

    with patch("src.agents.tools.get_db_session") as mock_get_session:
        mock_get_session.return_value.__enter__.return_value = mock_session
        res = query_pipeline_status()

    assert res["status"] == "success"
    assert res["overall_health"] == "degraded"
    assert res["latest_runs"][0]["error_message"] == "Redpanda broker unreachable"


def test_query_pipeline_status_empty():
    """Verify status is empty when table has zero records."""
    mock_session = MagicMock()
    mock_session.query.return_value.order_by.return_value.limit.return_value.all.return_value = (
        []
    )

    with patch("src.agents.tools.get_db_session") as mock_get_session:
        mock_get_session.return_value.__enter__.return_value = mock_session
        res = query_pipeline_status()

    assert res["status"] == "success"
    assert res["overall_health"] == "empty"
    assert res["run_count"] == 0


def test_query_pipeline_status_db_error_resilience():
    """Verify database error is handled without crashing."""
    with patch(
        "src.agents.tools.get_db_session", side_effect=Exception("PostgreSQL offline")
    ):
        res = query_pipeline_status()

    assert res["status"] == "error"
    assert res["overall_health"] == "unknown"


# ===========================================================================
# Tool 4: search_logs_and_model_cards Tests
# ===========================================================================


def test_search_logs_and_model_cards_docs_retrieval():
    """Verify direct docs search retrieves real design decisions from docs/."""
    res = search_logs_and_model_cards(
        "ADR model promotion gate min_improvement_pct", top_k=5
    )
    assert res["status"] == "success"
    assert res["results_count"] > 0
    assert any(
        "Decisions.md" in r["source"] or "phase-6-cicd.md" in r["source"]
        for r in res["results"]
    )
    assert res["results"][0]["score"] > 0


def test_search_logs_and_model_cards_empty_query():
    """Verify empty query returns empty results cleanly."""
    res = search_logs_and_model_cards("   ")
    assert res["status"] == "empty"
    assert res["results_count"] == 0
    assert res["results"] == []


# ===========================================================================
# Tool Allowlist & Security Boundary (ADR-023)
# ===========================================================================


def test_tool_allowlist_structure_and_read_only_boundary():
    """Verify TOOL_ALLOWLIST contains exactly the 4 read-only tools and no mutating tools."""
    expected_tools = {
        "get_features",
        "query_recent_predictions",
        "query_pipeline_status",
        "search_logs_and_model_cards",
    }
    assert set(TOOL_ALLOWLIST.keys()) == expected_tools
    assert len(AGENT_TOOLS) == 4

    # Security verification: Ensure no write, mutation, or execution tools exist
    disallowed_prefixes = (
        "write",
        "set",
        "update",
        "delete",
        "insert",
        "execute",
        "retrain",
        "promote",
        "run",
    )
    for tool_name in TOOL_ALLOWLIST:
        for prefix in disallowed_prefixes:
            assert not tool_name.startswith(
                prefix
            ), f"Allowlisted tool '{tool_name}' starts with prohibited mutation prefix '{prefix}'."


def test_langchain_structured_tool_invocations():
    """Verify all 4 tools implement the LangChain StructuredTool interface (.invoke)."""
    # 1. get_features_tool
    res1 = get_features_tool.invoke({"entity_type": "zone", "entity_id": "161"})
    assert isinstance(res1, dict)
    assert res1["entity_type"] == "zone"

    # 2. query_recent_predictions_tool
    with patch("src.agents.tools.get_db_session") as mock_session:
        mock_session.return_value.__enter__.return_value.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = (
            []
        )
        res2 = query_recent_predictions_tool.invoke(
            {"entity_type": "zone", "entity_id": "161", "window_hours": 24}
        )
        assert isinstance(res2, dict)
        assert res2["entity_type"] == "zone"

    # 3. query_pipeline_status_tool
    with patch("src.agents.tools.get_db_session") as mock_session:
        mock_session.return_value.__enter__.return_value.query.return_value.order_by.return_value.limit.return_value.all.return_value = (
            []
        )
        res3 = query_pipeline_status_tool.invoke({})
        assert isinstance(res3, dict)
        assert "overall_health" in res3

    # 4. search_logs_and_model_cards_tool
    res4 = search_logs_and_model_cards_tool.invoke(
        {"query": "NYC taxi architecture", "top_k": 1}
    )
    assert isinstance(res4, dict)
    assert res4["status"] in ("success", "empty")


# ===========================================================================
# Prediction Logging Integration in Serving Endpoints
# ===========================================================================


def test_log_prediction_background_function():
    """Verify single prediction background logging function creates and adds Prediction ORM object."""
    mock_session = MagicMock()
    now_utc = datetime.now(timezone.utc)

    with patch("src.serving.app.get_db_session") as mock_get_session:
        mock_get_session.return_value.__enter__.return_value = mock_session
        log_prediction_background(
            entity_type="zone",
            entity_id="161",
            model_version="demand-test-v1",
            predicted_value=32.45,
            predicted_at=now_utc,
        )

    mock_session.add.assert_called_once()
    added_obj = mock_session.add.call_args[0][0]
    assert isinstance(added_obj, Prediction)
    assert added_obj.entity_type == "zone"
    assert added_obj.entity_id == "161"
    assert added_obj.model_version == "demand-test-v1"
    assert added_obj.predicted_value == Decimal("32.45")
    assert added_obj.predicted_at == now_utc


def test_log_predictions_batch_background_function():
    """Verify batch prediction background logging adds all records to session."""
    mock_session = MagicMock()
    now_utc = datetime.now(timezone.utc)
    records = [
        {
            "entity_type": "zone",
            "entity_id": "161",
            "model_version": "demand-v1",
            "predicted_value": 20.5,
            "predicted_at": now_utc,
        },
        {
            "entity_type": "zone",
            "entity_id": "236",
            "model_version": "demand-v1",
            "predicted_value": 15.0,
            "predicted_at": now_utc,
        },
    ]

    with patch("src.serving.app.get_db_session") as mock_get_session:
        mock_get_session.return_value.__enter__.return_value = mock_session
        log_predictions_batch_background(records)

    mock_session.add_all.assert_called_once()
    added_list = mock_session.add_all.call_args[0][0]
    assert len(added_list) == 2
    assert added_list[0].entity_id == "161"
    assert added_list[1].entity_id == "236"


def test_serving_endpoint_prediction_logging_triggers_background_tasks():
    """Verify FastAPI prediction endpoints trigger prediction background tasks."""
    client = TestClient(app)

    # Enable logging for this test explicitly
    app.state.disable_prediction_logging = False

    with patch("src.serving.app.log_prediction_background") as mock_log_single:
        resp = client.get("/predict/demand/161")
        assert resp.status_code == 200
        # Background task executed by TestClient
        mock_log_single.assert_called_once()
        call_args = mock_log_single.call_args[0]
        assert call_args[0] == "zone"
        assert call_args[1] == "161"

    with patch("src.serving.app.log_prediction_background") as mock_log_single:
        resp = client.get("/predict/eta?origin=161&dest=237")
        assert resp.status_code == 200
        mock_log_single.assert_called_once()
        call_args = mock_log_single.call_args[0]
        assert call_args[0] == "corridor"
        assert call_args[1] == "161_237"

    with patch("src.serving.app.log_predictions_batch_background") as mock_log_batch:
        resp = client.post(
            "/predict/demand/batch",
            json={"zone_ids": [161, 236], "horizon_minutes": 15},
        )
        assert resp.status_code == 200
        mock_log_batch.assert_called_once()
        batch_items = mock_log_batch.call_args[0][0]
        assert len(batch_items) == 2
        assert batch_items[0]["entity_id"] == "161"
        assert batch_items[1]["entity_id"] == "236"

    with patch("src.serving.app.log_predictions_batch_background") as mock_log_batch:
        resp = client.post(
            "/predict/eta/batch",
            json={
                "corridors": [
                    {"origin_zone_id": 161, "dest_zone_id": 236},
                    {"origin_zone_id": 142, "dest_zone_id": 161},
                ]
            },
        )
        assert resp.status_code == 200
        mock_log_batch.assert_called_once()
        batch_items = mock_log_batch.call_args[0][0]
        assert len(batch_items) == 2
        assert batch_items[0]["entity_id"] == "161_236"
        assert batch_items[1]["entity_id"] == "142_161"
