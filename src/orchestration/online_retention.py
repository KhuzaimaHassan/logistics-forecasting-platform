"""Renew only the bounded sample's Feast keys after successful materialization.

Feast 0.65 skips equal event timestamps before renewing Redis expiry. Historical
replays therefore need explicit EXPIRE, without rewriting values or event times.
The key codec/client below deliberately use the pinned Feast Redis implementation.
"""

from datetime import timedelta

from feast.infra.online_stores.redis import RedisOnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey
from feast.protos.feast.types.Value_pb2 import Value
from sqlalchemy import text


def renew_sample_retention(engine, store, observation_end):
    config = store.config
    ttl = config.online_store.key_ttl_seconds
    if not ttl or ttl < 1:
        raise ValueError("Bounded online sample requires a positive retention TTL")
    params = {
        "start": observation_end - timedelta(days=13),
        "end": observation_end + timedelta(seconds=1),
    }
    with engine.connect() as connection:
        zones = (
            connection.execute(
                text(
                    "SELECT DISTINCT zone_id FROM warehouse.zone_demand_features_hourly WHERE pickup_datetime >= :start AND pickup_datetime <= :end"
                ),
                params,
            )
            .scalars()
            .all()
        )
        corridors = (
            connection.execute(
                text(
                    "SELECT DISTINCT corridor_id FROM warehouse.corridor_duration_features_hourly WHERE dropoff_datetime >= :start AND dropoff_datetime <= :end"
                ),
                params,
            )
            .scalars()
            .all()
        )
    entities = [
        EntityKey(join_keys=["zone_id"], entity_values=[Value(int32_val=int(zone))])
        for zone in zones
    ] + [
        EntityKey(
            join_keys=["corridor_id"], entity_values=[Value(string_val=str(corridor))]
        )
        for corridor in corridors
    ]
    if not entities or len(entities) > 10000:
        raise ValueError("Online sample entity count outside bounded retention budget")
    backend = RedisOnlineStore()
    keys = backend._generate_redis_keys_for_entities(config, entities)
    client = backend._get_client(config.online_store)
    with client.pipeline(transaction=False) as pipeline:
        for key in keys:
            pipeline.ttl(key)
        previous = pipeline.execute()
    with client.pipeline(transaction=False) as pipeline:
        for key in keys:
            pipeline.expire(key, ttl)
        renewed = pipeline.execute()
    if not all(renewed):
        raise RuntimeError(
            "Materialized Feast sample contains missing online entity keys"
        )
    return {
        "renewed_entity_keys": len(keys),
        "retention_seconds": ttl,
        "previous_min_ttl_seconds": min(previous),
        "feature_values_and_timestamps_unchanged": True,
    }
