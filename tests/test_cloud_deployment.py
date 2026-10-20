"""Hermetic cloud safety contracts; these do not certify managed infrastructure."""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
from fastapi import HTTPException

from src.common.config import Settings
from src.common.kafka_utils import kafka_security_options
from src.features.client import FeastOnlineClient, ZoneDemandOnlineFeatures
from src.features.config import feast_redis_connection, get_feast_repo_config
from src.serving.feature_extractor import (
    ACTIVE_ZONE_CATEGORIES,
    build_corridor_feature_df,
    build_demand_feature_df,
)
from src.serving.model_loader import (
    ModelLoaderService,
    ModelUnavailableError,
    load_model_artifact,
)


def test_managed_urls_preserve_tls_and_escaped_credentials():
    from feast.infra.online_stores.redis import RedisOnlineStore

    settings = Settings(
        _env_file=None,
        DATABASE_URL="postgresql://user:p%23ss%40word@managed.test:25432/logistics?sslmode=verify-full&sslrootcert=/tmp/pg-ca.pem",
        REDIS_URL="rediss://default:comma%2Ccolon%3Aequals%3D@valkey.test:26379/0?ssl_ca_certs=/tmp/ca.pem",
    )
    config = get_feast_repo_config(settings=settings)
    assert config.offline_store.password == "p#ss@word"
    assert config.offline_store.sslmode == "verify-full"
    assert config.offline_store.sslrootcert_path == "/tmp/pg-ca.pem"
    assert config.offline_store.max_conn == 2
    assert config.registry.sqlalchemy_params["max_overflow"] == 0
    nodes, kwargs = RedisOnlineStore._parse_connection_string(
        config.online_store.connection_string
    )
    assert nodes == [{"host": "valkey.test", "port": "26379"}]
    assert kwargs["password"] == "comma,colon:equals="
    assert kwargs["ssl"] is True and kwargs["ssl_check_hostname"] is True
    assert kwargs["ssl_cert_reqs"] == "required"


def test_insecure_redis_scheme_rejected():
    with pytest.raises(ValueError):
        feast_redis_connection("http://store.test")


def test_cloud_eta_uses_actual_feast_event_timestamp(monkeypatch):
    monkeypatch.setenv("DEPLOYMENT_MODE", "cloud")
    observed = datetime(2023, 1, 27, 3, tzinfo=timezone.utc)
    response = MagicMock()
    response.to_dict.return_value = {
        "avg_duration_last_15m": [720.0],
        "avg_duration_last_1h": [800.0],
        "distance_km": [3.2],
        "origin_zone_demand_pressure": [4],
        "avg_duration_last_1h__ts": [int(observed.timestamp())],
    }
    store = MagicMock()
    store.get_online_features.return_value = response
    feature = FeastOnlineClient(store=store).get_corridor_duration_features(["74_75"])[
        0
    ]
    assert feature.observed_at == observed
    response.to_dict.assert_called_with(include_event_timestamps=True)
    frame = build_corridor_feature_df(
        [feature], now=datetime(2026, 10, 17, 19, tzinfo=timezone.utc)
    )
    assert frame.iloc[0].hour_of_day == 3
    assert frame.iloc[0].day_of_week == observed.weekday()


def test_kafka_mtls_shared_configuration(monkeypatch):
    config = Settings(
        _env_file=None,
        KAFKA_SECURITY_PROTOCOL="SSL",
        KAFKA_SSL_CAFILE="ca.pem",
        KAFKA_SSL_CERTFILE="cert.pem",
        KAFKA_SSL_KEYFILE="key.pem",
    )
    with patch("src.common.kafka_utils.get_settings", return_value=config):
        assert kafka_security_options() == {
            "security_protocol": "SSL",
            "ssl_check_hostname": True,
            "ssl_cafile": "ca.pem",
            "ssl_certfile": "cert.pem",
            "ssl_keyfile": "key.pem",
        }


def test_cloud_missing_model_never_returns_baseline(monkeypatch):
    monkeypatch.setenv("DEPLOYMENT_MODE", "cloud")
    client = MagicMock()
    client.get_latest_versions.return_value = []
    loader = ModelLoaderService(tracking_uri="http://tracking.test", client=client)
    with pytest.raises(ModelUnavailableError):
        loader.load_demand_model()


def test_cloud_requires_complete_observed_features_and_accepts_zero(monkeypatch):
    monkeypatch.setenv("DEPLOYMENT_MODE", "cloud")
    with pytest.raises(HTTPException) as failure:
        build_demand_feature_df(
            [
                ZoneDemandOnlineFeatures(
                    zone_id=74, pickup_count_last_1h=1, cache_hit=True
                )
            ]
        )
    assert failure.value.status_code == 503
    observed = ZoneDemandOnlineFeatures(
        zone_id=74,
        pickup_count_last_15m=0,
        pickup_count_last_1h=0,
        pickup_count_last_24h=0,
        pickup_count_same_hour_last_week=0,
        hour_of_day=3,
        day_of_week=2,
        is_weekend=False,
        is_holiday=False,
        cache_hit=True,
    )
    frame = build_demand_feature_df([observed])
    assert frame.zone_id.cat.categories.tolist() == ACTIVE_ZONE_CATEGORIES
    assert frame.iloc[0].sin_hour == pytest.approx(np.sin(2 * np.pi * 3 / 24))


def test_native_booster_matches_training_predictions_and_blocks_path_escape(
    tmp_path, monkeypatch
):
    import lightgbm as lgb

    monkeypatch.setenv("MLFLOW_MODEL_LOAD_MODE", "native")
    frame = pd.DataFrame(
        {
            "zone_id": pd.Categorical(
                [74, 75, 74, 75], categories=ACTIVE_ZONE_CATEGORIES
            ),
            "count": [0.0, 5.0, 1.0, 6.0],
        }
    )
    model = lgb.LGBMRegressor(
        n_estimators=3, num_leaves=2, min_child_samples=1, n_jobs=1, verbosity=-1
    ).fit(frame, [1, 6, 2, 7])
    model.booster_.save_model(str(tmp_path / "model.lgb"))
    manifest = {
        "flavors": {
            "lightgbm": {"model_class": "lightgbm.basic.Booster", "data": "model.lgb"}
        }
    }
    (tmp_path / "MLmodel").write_text(json.dumps(manifest))
    with patch("mlflow.artifacts.download_artifacts", return_value=str(tmp_path)):
        loaded = load_model_artifact("models:/unit-test/1")
        np.testing.assert_allclose(loaded.predict(frame), model.predict(frame))
        manifest["flavors"]["lightgbm"]["data"] = "../outside.lgb"
        (tmp_path / "MLmodel").write_text(json.dumps(manifest))
        with pytest.raises(ModelUnavailableError):
            load_model_artifact("models:/unit-test/1")


@pytest.mark.parametrize("materialized_at", [None, "2026-10-19T11:00:00+00:00"])
def test_deployment_freshness_never_uses_cursor_update_as_materialization(
    materialized_at,
):
    from src.serving.app import deployment_status

    metadata = (
        {} if materialized_at is None else {"feature_materialized_at": materialized_at}
    )
    updated = datetime(2026, 10, 18, tzinfo=timezone.utc)
    session = MagicMock()
    first = MagicMock()
    first.first.return_value = (json.dumps(metadata), updated)
    second = MagicMock()
    second.scalars.return_value = ["75_237"]
    session.execute.side_effect = [first, second]
    with (
        patch("src.serving.app.is_cloud_deployment", return_value=True),
        patch("src.serving.app.get_db_session") as manager,
    ):
        manager.return_value.__enter__.return_value = session
        result = deployment_status()
    assert result["observations"]["refreshed_at"] == materialized_at
    assert result["observations"]["checkpoint_updated_at"] == updated.isoformat()
