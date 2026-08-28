"""Unit and integration tests for Feast online materialization and low-latency client."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest
from feast import FeatureStore, RepoConfig

from src.features.client import (
    CorridorDurationOnlineFeatures,
    FeastOnlineClient,
    PredictionOnlineFeatures,
    ZoneDemandOnlineFeatures,
    get_corridor_duration_online_features,
    get_zone_demand_online_features,
)
from src.features.entities import corridor_entity, zone_entity
from src.features.materialize import materialize_features
from src.features.views import create_file_backed_feature_views


def test_dataclass_models_serialization() -> None:
    """Test dataclass representation, default values, and serialization."""
    z = ZoneDemandOnlineFeatures(
        zone_id=161,
        pickup_count_last_15m=5,
        pickup_count_last_1h=20,
        pickup_count_last_24h=50,
        hour_of_day=11,
        day_of_week=6,
        is_weekend=True,
        is_holiday=False,
        cache_hit=True,
    )
    d = z.to_dict()
    assert d["zone_id"] == 161
    assert d["pickup_count_last_1h"] == 20
    assert d["cache_hit"] is True

    c = CorridorDurationOnlineFeatures(
        corridor_id="161_236",
        avg_duration_last_15m=900.0,
        avg_duration_last_1h=900.0,
        distance_km=4.5,
        origin_zone_demand_pressure=20,
        cache_hit=True,
    )
    cd = c.to_dict()
    assert cd["corridor_id"] == "161_236"
    assert cd["avg_duration_last_1h"] == 900.0
    assert cd["cache_hit"] is True

    pred = PredictionOnlineFeatures(
        pickup_zone_id=161,
        dropoff_zone_id=236,
        corridor_id="161_236",
        origin_demand=z,
        destination_demand=ZoneDemandOnlineFeatures(zone_id=236, cache_hit=True),
        corridor_duration=c,
        all_cached=True,
    )
    pd_dict = pred.to_dict()
    assert pd_dict["corridor_id"] == "161_236"
    assert pd_dict["all_cached"] is True


def test_materialize_features_argument_validation() -> None:
    """Test validation errors for invalid materialization parameters."""
    with pytest.raises(ValueError, match="start_date is required"):
        materialize_features(incremental=False, start_date=None)

    with pytest.raises(ValueError, match="cannot be later than end_date"):
        materialize_features(
            start_date=datetime(2023, 1, 2, tzinfo=timezone.utc),
            end_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
            incremental=False,
            use_sqlite_fallback=True,
        )


def test_materialize_and_online_retrieval_flow(tmp_path: Path) -> None:
    """Test complete flow: cache miss -> materialization -> cache hit -> prediction retrieval."""
    zone_pq = tmp_path / "zone_features.parquet"
    corridor_pq = tmp_path / "corridor_features.parquet"
    registry_db = tmp_path / "registry.db"
    online_db = tmp_path / "online.db"

    # Create test offline Parquet data
    t_base = datetime(2023, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
    df_zone = pd.DataFrame(
        [
            {
                "zone_id": 161,
                "pickup_datetime": t_base,
                "created_at": t_base,
                "pickup_count_last_15m": 5,
                "pickup_count_last_1h": 20,
                "pickup_count_last_24h": 40,
                "pickup_count_same_hour_last_week": 0,
                "hour_of_day": 11,
                "day_of_week": 6,
                "is_weekend": True,
                "is_holiday": False,
                "avg_temp_last_1h": 12.5,
                "is_precipitating": False,
            },
            {
                "zone_id": 236,
                "pickup_datetime": t_base,
                "created_at": t_base,
                "pickup_count_last_15m": 3,
                "pickup_count_last_1h": 12,
                "pickup_count_last_24h": 25,
                "pickup_count_same_hour_last_week": 0,
                "hour_of_day": 11,
                "day_of_week": 6,
                "is_weekend": True,
                "is_holiday": False,
                "avg_temp_last_1h": 12.5,
                "is_precipitating": False,
            },
        ]
    )
    df_zone.to_parquet(zone_pq)

    df_corridor = pd.DataFrame(
        [
            {
                "corridor_id": "161_236",
                "dropoff_datetime": t_base,
                "created_at": t_base,
                "avg_duration_last_15m": 900.0,
                "avg_duration_last_1h": 900.0,
                "distance_km": 4.5,
                "origin_zone_demand_pressure": 20,
                "avg_traffic_speed_current": 18.0,
            }
        ]
    )
    df_corridor.to_parquet(corridor_pq)

    # Build FeatureStore with local sqlite
    feature_views = create_file_backed_feature_views(
        zone_parquet_path=str(zone_pq),
        corridor_parquet_path=str(corridor_pq),
    )
    repo_cfg = RepoConfig(
        registry=str(registry_db),
        project="test_online_flow",
        provider="local",
        online_store={"type": "sqlite", "path": str(online_db)},
    )
    store = FeatureStore(config=repo_cfg)
    store.apply([zone_entity, corridor_entity, *feature_views])

    client = FeastOnlineClient(store=store)

    # 1. Verify Genuine Cache Miss (Before Materialization)
    misses = client.get_zone_demand_features([161, 999])
    assert len(misses) == 2
    assert misses[0].zone_id == 161
    assert misses[0].cache_hit is False
    assert misses[0].pickup_count_last_1h is None
    assert misses[1].zone_id == 999
    assert misses[1].cache_hit is False
    assert misses[1].pickup_count_last_1h is None

    # 2. Run Materialization
    res = materialize_features(
        start_date=t_base - timedelta(hours=1),
        end_date=t_base + timedelta(hours=1),
        feature_views=[fv.name for fv in feature_views],
        incremental=False,
        store=store,
    )
    assert res["status"] == "success"
    assert res["mode"] == "explicit"

    # 3. Verify Cache Hit After Materialization
    hits = client.get_zone_demand_features([161, 236, 999])
    assert len(hits) == 3
    # Zone 161
    assert hits[0].zone_id == 161
    assert hits[0].cache_hit is True
    assert hits[0].pickup_count_last_15m == 5
    assert hits[0].pickup_count_last_1h == 20
    assert hits[0].is_weekend is True
    # Zone 236
    assert hits[1].zone_id == 236
    assert hits[1].cache_hit is True
    assert hits[1].pickup_count_last_1h == 12
    # Zone 999 (unmaterialized entity)
    assert hits[2].zone_id == 999
    assert hits[2].cache_hit is False
    assert hits[2].pickup_count_last_1h is None

    # 4. Verify Corridor Duration Online Retrieval
    corridor_res = client.get_corridor_duration_features(["161_236", "999_999"])
    assert len(corridor_res) == 2
    assert corridor_res[0].corridor_id == "161_236"
    assert corridor_res[0].cache_hit is True
    assert corridor_res[0].avg_duration_last_1h == 900.0
    assert corridor_res[0].origin_zone_demand_pressure == 20
    assert corridor_res[1].corridor_id == "999_999"
    assert corridor_res[1].cache_hit is False

    # 5. Verify Combined Prediction Features
    pred_features = client.get_prediction_features(
        pickup_zone_id=161, dropoff_zone_id=236
    )
    assert pred_features.corridor_id == "161_236"
    assert pred_features.all_cached is True
    assert pred_features.origin_demand.pickup_count_last_1h == 20
    assert pred_features.destination_demand.pickup_count_last_1h == 12
    assert pred_features.corridor_duration.avg_duration_last_1h == 900.0

    # Partial cache hit test (origin cached, destination missing)
    partial_pred = client.get_prediction_features(
        pickup_zone_id=161, dropoff_zone_id=999
    )
    assert partial_pred.all_cached is False
    assert partial_pred.origin_demand.cache_hit is True
    assert partial_pred.destination_demand.cache_hit is False

    # 6. Verify Top-Level Convenience Functions
    z_conv = get_zone_demand_online_features([161], client=client)
    assert len(z_conv) == 1
    assert z_conv[0].pickup_count_last_1h == 20

    c_conv = get_corridor_duration_online_features(["161_236"], client=client)
    assert len(c_conv) == 1
    assert c_conv[0].avg_duration_last_1h == 900.0

    # 7. Idempotent Re-Run Materialization (Second Run Over Same Window)
    res_rerun = materialize_features(
        start_date=t_base - timedelta(hours=1),
        end_date=t_base + timedelta(hours=1),
        feature_views=[fv.name for fv in feature_views],
        incremental=False,
        store=store,
    )
    assert res_rerun["status"] == "success"
    hits_rerun = client.get_zone_demand_features([161])
    assert hits_rerun[0].pickup_count_last_1h == 20


def test_empty_entities_retrieval(tmp_path: Path) -> None:
    """Test passing empty collections to online client returns empty lists."""
    registry_db = tmp_path / "registry.db"
    online_db = tmp_path / "online.db"

    repo_cfg = RepoConfig(
        registry=str(registry_db),
        project="test_empty",
        provider="local",
        online_store={"type": "sqlite", "path": str(online_db)},
    )
    store = FeatureStore(config=repo_cfg)
    client = FeastOnlineClient(store=store)

    assert client.get_zone_demand_features([]) == []
    assert client.get_corridor_duration_features([]) == []


def test_incremental_materialization_flow(tmp_path: Path) -> None:
    """Test incremental materialization and re-run behavior."""
    zone_pq = tmp_path / "zone_features.parquet"
    corridor_pq = tmp_path / "corridor_features.parquet"
    registry_db = tmp_path / "registry.db"
    online_db = tmp_path / "online.db"

    t_base = datetime(2023, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
    df_zone = pd.DataFrame(
        [
            {
                "zone_id": 161,
                "pickup_datetime": t_base,
                "created_at": t_base,
                "pickup_count_last_15m": 5,
                "pickup_count_last_1h": 20,
                "pickup_count_last_24h": 40,
                "pickup_count_same_hour_last_week": 0,
                "hour_of_day": 11,
                "day_of_week": 6,
                "is_weekend": True,
                "is_holiday": False,
                "avg_temp_last_1h": 12.5,
                "is_precipitating": False,
            }
        ]
    )
    df_zone.to_parquet(zone_pq)

    df_corridor = pd.DataFrame(
        [
            {
                "corridor_id": "161_236",
                "dropoff_datetime": t_base,
                "created_at": t_base,
                "avg_duration_last_15m": 900.0,
                "avg_duration_last_1h": 900.0,
                "distance_km": 4.5,
                "origin_zone_demand_pressure": 20,
                "avg_traffic_speed_current": 18.0,
            }
        ]
    )
    df_corridor.to_parquet(corridor_pq)

    feature_views = create_file_backed_feature_views(
        zone_parquet_path=str(zone_pq),
        corridor_parquet_path=str(corridor_pq),
    )
    repo_cfg = RepoConfig(
        registry=str(registry_db),
        project="test_incremental",
        provider="local",
        online_store={"type": "sqlite", "path": str(online_db)},
    )
    store = FeatureStore(config=repo_cfg)
    store.apply([zone_entity, corridor_entity, *feature_views])

    # Initial explicit materialize to set registry end_date cursor at t_base - 1h
    materialize_features(
        start_date=t_base - timedelta(hours=2),
        end_date=t_base - timedelta(hours=1),
        feature_views=[fv.name for fv in feature_views],
        incremental=False,
        store=store,
    )

    # Now run incremental materialization up to t_base + 1h
    # (starts from previous end_time cursor at t_base - 1h)
    res_inc = materialize_features(
        end_date=t_base + timedelta(hours=1),
        feature_views=[fv.name for fv in feature_views],
        incremental=True,
        store=store,
    )
    assert res_inc["status"] == "success"
    assert res_inc["mode"] == "incremental"

    client = FeastOnlineClient(store=store)
    hit = client.get_zone_demand_features(161)
    assert len(hit) == 1
    assert hit[0].cache_hit is True
    assert hit[0].pickup_count_last_1h == 20

    # Re-run incremental with same end_date (no-op since end_date <= cursor)
    res_inc_2 = materialize_features(
        end_date=t_base + timedelta(hours=1),
        feature_views=[fv.name for fv in feature_views],
        incremental=True,
        store=store,
    )
    assert res_inc_2["status"] == "success"
