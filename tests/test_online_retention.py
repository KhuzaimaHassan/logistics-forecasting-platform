from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from feast.infra.online_stores.helpers import _redis_key
from feast.protos.feast.types.EntityKey_pb2 import EntityKey
from feast.protos.feast.types.Value_pb2 import Value

from src.orchestration.online_retention import renew_sample_retention


@pytest.mark.parametrize("present", [True, False])
def test_unchanged_sample_renews_only_exact_entity_keys_without_rewriting(present):
    config = SimpleNamespace(
        project="sample",
        entity_key_serialization_version=3,
        online_store=SimpleNamespace(key_ttl_seconds=86400),
    )
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value
    zones, corridors = MagicMock(), MagicMock()
    zones.scalars.return_value.all.return_value = [74]
    corridors.scalars.return_value.all.return_value = ["75_237"]
    connection.execute.side_effect = [zones, corridors]
    client = MagicMock()
    pipeline = client.pipeline.return_value.__enter__.return_value
    pipeline.execute.side_effect = [[120, 30], [True, present]]
    with patch(
        "src.orchestration.online_retention.RedisOnlineStore._get_client",
        return_value=client,
    ):
        if present:
            result = renew_sample_retention(
                engine,
                SimpleNamespace(config=config),
                datetime(2023, 1, 31, tzinfo=timezone.utc),
            )
            assert result["renewed_entity_keys"] == 2
            assert result["previous_min_ttl_seconds"] == 30
        else:
            with pytest.raises(RuntimeError, match="missing online entity"):
                renew_sample_retention(
                    engine,
                    SimpleNamespace(config=config),
                    datetime(2023, 1, 31, tzinfo=timezone.utc),
                )
    keys = [
        _redis_key(
            "sample",
            EntityKey(join_keys=["zone_id"], entity_values=[Value(int32_val=74)]),
        ),
        _redis_key(
            "sample",
            EntityKey(
                join_keys=["corridor_id"], entity_values=[Value(string_val="75_237")]
            ),
        ),
    ]
    assert [call.args for call in pipeline.expire.call_args_list] == [
        (key, 86400) for key in keys
    ]
    pipeline.hset.assert_not_called()
    pipeline.delete.assert_not_called()
