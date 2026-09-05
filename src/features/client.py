"""Feast low-latency online feature retrieval client for inference serving.

Provides sub-10ms feature lookup functions reading directly from the Redis online store
with explicit cache hit/miss detection and typing for downstream serving.
"""

import logging
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from feast import FeatureStore

from src.features.config import get_feature_store
from src.features.entities import build_corridor_id

logger = logging.getLogger(__name__)

ZONE_DEMAND_FEATURE_NAMES = [
    "zone_demand_features:pickup_count_last_15m",
    "zone_demand_features:pickup_count_last_1h",
    "zone_demand_features:pickup_count_last_24h",
    "zone_demand_features:pickup_count_same_hour_last_week",
    "zone_demand_features:hour_of_day",
    "zone_demand_features:day_of_week",
    "zone_demand_features:is_weekend",
    "zone_demand_features:is_holiday",
    "zone_demand_features:avg_temp_last_1h",
    "zone_demand_features:is_precipitating",
]

ZONE_DEMAND_PUSH_FEATURE_NAMES = [
    "zone_demand_features_push:pickup_count_last_15m",
    "zone_demand_features_push:pickup_count_last_1h",
    "zone_demand_features_push:hour_of_day",
    "zone_demand_features_push:day_of_week",
    "zone_demand_features_push:is_weekend",
    "zone_demand_features_push:is_holiday",
    "zone_demand_features_push:avg_temp_last_1h",
    "zone_demand_features_push:is_precipitating",
]

CORRIDOR_DURATION_FEATURE_NAMES = [
    "corridor_duration_features:avg_duration_last_15m",
    "corridor_duration_features:avg_duration_last_1h",
    "corridor_duration_features:distance_km",
    "corridor_duration_features:origin_zone_demand_pressure",
    "corridor_duration_features:avg_traffic_speed_current",
]

CORRIDOR_DURATION_PUSH_FEATURE_NAMES = [
    "corridor_duration_features_push:avg_duration_last_15m",
    "corridor_duration_features_push:avg_duration_last_1h",
    "corridor_duration_features_push:avg_traffic_speed_current",
    "corridor_duration_features_push:origin_zone_demand_pressure",
]


def _coalesce_feature(
    data: Dict[str, List[Any]],
    idx: int,
    field: str,
    push_view: Optional[str],
    batch_view: str,
    is_full: bool,
) -> Any:
    """Retrieve pushed value if present, falling back to batch value."""
    if not is_full:
        vals = data.get(field)
        return vals[idx] if vals is not None else None

    if push_view:
        push_key = f"{push_view}__{field}"
        push_vals = data.get(push_key)
        if push_vals is not None and push_vals[idx] is not None:
            return push_vals[idx]

    batch_key = f"{batch_view}__{field}"
    batch_vals = data.get(batch_key)
    if batch_vals is not None:
        return batch_vals[idx]
    return None


@dataclass(frozen=True)
class ZoneDemandOnlineFeatures:
    """Online feature payload for taxi zone demand forecasting."""

    zone_id: int
    pickup_count_last_15m: Optional[int] = None
    pickup_count_last_1h: Optional[int] = None
    pickup_count_last_24h: Optional[int] = None
    pickup_count_same_hour_last_week: Optional[int] = None
    hour_of_day: Optional[int] = None
    day_of_week: Optional[int] = None
    is_weekend: Optional[bool] = None
    is_holiday: Optional[bool] = None
    avg_temp_last_1h: Optional[float] = None
    is_precipitating: Optional[bool] = None
    cache_hit: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to plain dictionary."""
        return asdict(self)


@dataclass(frozen=True)
class CorridorDurationOnlineFeatures:
    """Online feature payload for taxi corridor trip duration forecasting."""

    corridor_id: str
    avg_duration_last_15m: Optional[float] = None
    avg_duration_last_1h: Optional[float] = None
    distance_km: Optional[float] = None
    origin_zone_demand_pressure: Optional[int] = None
    avg_traffic_speed_current: Optional[float] = None
    cache_hit: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to plain dictionary."""
        return asdict(self)


@dataclass(frozen=True)
class PredictionOnlineFeatures:
    """Combined online feature set for an origin-destination trip pair."""

    pickup_zone_id: int
    dropoff_zone_id: int
    corridor_id: str
    origin_demand: ZoneDemandOnlineFeatures
    destination_demand: ZoneDemandOnlineFeatures
    corridor_duration: CorridorDurationOnlineFeatures
    all_cached: bool

    def to_dict(self) -> Dict[str, Any]:
        """Convert to nested dictionary."""
        return {
            "pickup_zone_id": self.pickup_zone_id,
            "dropoff_zone_id": self.dropoff_zone_id,
            "corridor_id": self.corridor_id,
            "origin_demand": self.origin_demand.to_dict(),
            "destination_demand": self.destination_demand.to_dict(),
            "corridor_duration": self.corridor_duration.to_dict(),
            "all_cached": self.all_cached,
        }


class FeastOnlineClient:
    """High-performance client for low-latency online feature retrieval."""

    def __init__(
        self,
        store: Optional[FeatureStore] = None,
        repo_path: Optional[Union[str, Path]] = None,
        use_sqlite_fallback: bool = False,
    ) -> None:
        if store is not None:
            self._store = store
        else:
            self._store = get_feature_store(
                repo_path=repo_path,
                use_sqlite_fallback=use_sqlite_fallback,
            )

    @property
    def store(self) -> FeatureStore:
        """Underlying Feast FeatureStore instance."""
        return self._store

    def _fetch_zone_features(
        self,
        entity_rows: List[Dict[str, Any]],
        use_push_features: bool,
    ) -> tuple[Dict[str, List[Any]], bool]:
        """Fetch raw zone feature dictionary from Feast store."""
        if use_push_features:
            try:
                features_to_query = (
                    ZONE_DEMAND_FEATURE_NAMES + ZONE_DEMAND_PUSH_FEATURE_NAMES
                )
                response = self._store.get_online_features(
                    features=features_to_query,
                    entity_rows=entity_rows,
                    full_feature_names=True,
                )
                return response.to_dict(), True
            except Exception as exc:
                logger.debug("Falling back to batch-only zone features: %s", exc)

        response = self._store.get_online_features(
            features=ZONE_DEMAND_FEATURE_NAMES,
            entity_rows=entity_rows,
        )
        return response.to_dict(), False

    def _fetch_corridor_features(
        self,
        entity_rows: List[Dict[str, Any]],
        use_push_features: bool,
    ) -> tuple[Dict[str, List[Any]], bool]:
        """Fetch raw corridor feature dictionary from Feast store."""
        if use_push_features:
            try:
                features_to_query = (
                    CORRIDOR_DURATION_FEATURE_NAMES
                    + CORRIDOR_DURATION_PUSH_FEATURE_NAMES
                )
                response = self._store.get_online_features(
                    features=features_to_query,
                    entity_rows=entity_rows,
                    full_feature_names=True,
                )
                return response.to_dict(), True
            except Exception as exc:
                logger.debug("Falling back to batch-only corridor features: %s", exc)

        response = self._store.get_online_features(
            features=CORRIDOR_DURATION_FEATURE_NAMES,
            entity_rows=entity_rows,
        )
        return response.to_dict(), False

    def get_zone_demand_features(
        self,
        zone_ids: Union[int, List[int]],
        use_push_features: bool = True,
    ) -> List[ZoneDemandOnlineFeatures]:
        """Retrieve latest zone demand features for one or more zone IDs from online store.

        Cache miss behavior:
        If an entity was never materialized or expired past 24h TTL, feature fields are None
        and cache_hit=False.

        Args:
            zone_ids: Single integer zone ID or list of zone IDs.
            use_push_features: If True, query streaming push features and coalesce with batch.

        Returns:
            List of ZoneDemandOnlineFeatures matching the order of input zone_ids.
        """
        is_scalar = isinstance(zone_ids, int)
        ids_list = [zone_ids] if is_scalar else list(zone_ids)

        if not ids_list:
            return []

        entity_rows = [{"zone_id": zid} for zid in ids_list]
        data, is_full = self._fetch_zone_features(entity_rows, use_push_features)

        results: List[ZoneDemandOnlineFeatures] = []
        n_entities = len(ids_list)
        for i in range(n_entities):
            zid = ids_list[i]

            val_15m = _coalesce_feature(
                data,
                i,
                "pickup_count_last_15m",
                "zone_demand_features_push",
                "zone_demand_features",
                is_full,
            )
            val_1h = _coalesce_feature(
                data,
                i,
                "pickup_count_last_1h",
                "zone_demand_features_push",
                "zone_demand_features",
                is_full,
            )
            val_24h = _coalesce_feature(
                data,
                i,
                "pickup_count_last_24h",
                None,
                "zone_demand_features",
                is_full,
            )
            val_last_week = _coalesce_feature(
                data,
                i,
                "pickup_count_same_hour_last_week",
                None,
                "zone_demand_features",
                is_full,
            )
            hour_of_day = _coalesce_feature(
                data,
                i,
                "hour_of_day",
                "zone_demand_features_push",
                "zone_demand_features",
                is_full,
            )
            day_of_week = _coalesce_feature(
                data,
                i,
                "day_of_week",
                "zone_demand_features_push",
                "zone_demand_features",
                is_full,
            )
            is_weekend = _coalesce_feature(
                data,
                i,
                "is_weekend",
                "zone_demand_features_push",
                "zone_demand_features",
                is_full,
            )
            is_holiday = _coalesce_feature(
                data,
                i,
                "is_holiday",
                "zone_demand_features_push",
                "zone_demand_features",
                is_full,
            )
            avg_temp = _coalesce_feature(
                data,
                i,
                "avg_temp_last_1h",
                "zone_demand_features_push",
                "zone_demand_features",
                is_full,
            )
            is_precip = _coalesce_feature(
                data,
                i,
                "is_precipitating",
                "zone_demand_features_push",
                "zone_demand_features",
                is_full,
            )

            # Cache hit check: any non-None feature returned
            cache_hit = not (
                val_15m is None
                and val_1h is None
                and val_24h is None
                and val_last_week is None
                and hour_of_day is None
            )

            results.append(
                ZoneDemandOnlineFeatures(
                    zone_id=zid,
                    pickup_count_last_15m=val_15m if val_15m is not None else None,
                    pickup_count_last_1h=val_1h if val_1h is not None else None,
                    pickup_count_last_24h=val_24h if val_24h is not None else None,
                    pickup_count_same_hour_last_week=(
                        val_last_week if val_last_week is not None else None
                    ),
                    hour_of_day=hour_of_day if hour_of_day is not None else None,
                    day_of_week=day_of_week if day_of_week is not None else None,
                    is_weekend=bool(is_weekend) if is_weekend is not None else None,
                    is_holiday=bool(is_holiday) if is_holiday is not None else None,
                    avg_temp_last_1h=(
                        float(avg_temp) if avg_temp is not None else None
                    ),
                    is_precipitating=(
                        bool(is_precip) if is_precip is not None else None
                    ),
                    cache_hit=cache_hit,
                )
            )

        return results

    def get_corridor_duration_features(
        self,
        corridor_ids: Union[str, List[str]],
        use_push_features: bool = True,
    ) -> List[CorridorDurationOnlineFeatures]:
        """Retrieve latest corridor duration features for one or more corridor IDs.

        Cache miss behavior:
        If an entity was never materialized or expired past 24h TTL, feature fields are None
        and cache_hit=False.

        Args:
            corridor_ids: Single string corridor ID (e.g. '161_236') or list of corridor IDs.
            use_push_features: If True, query streaming push features and coalesce with batch.

        Returns:
            List of CorridorDurationOnlineFeatures matching the order of input corridor_ids.
        """
        is_scalar = isinstance(corridor_ids, str)
        ids_list = [corridor_ids] if is_scalar else list(corridor_ids)

        if not ids_list:
            return []

        entity_rows = [{"corridor_id": cid} for cid in ids_list]
        data, is_full = self._fetch_corridor_features(entity_rows, use_push_features)

        results: List[CorridorDurationOnlineFeatures] = []
        n_entities = len(ids_list)
        for i in range(n_entities):
            cid = ids_list[i]

            avg_15m = _coalesce_feature(
                data,
                i,
                "avg_duration_last_15m",
                "corridor_duration_features_push",
                "corridor_duration_features",
                is_full,
            )
            avg_1h = _coalesce_feature(
                data,
                i,
                "avg_duration_last_1h",
                "corridor_duration_features_push",
                "corridor_duration_features",
                is_full,
            )
            dist_km = _coalesce_feature(
                data,
                i,
                "distance_km",
                None,
                "corridor_duration_features",
                is_full,
            )
            demand_pressure = _coalesce_feature(
                data,
                i,
                "origin_zone_demand_pressure",
                "corridor_duration_features_push",
                "corridor_duration_features",
                is_full,
            )
            speed_current = _coalesce_feature(
                data,
                i,
                "avg_traffic_speed_current",
                "corridor_duration_features_push",
                "corridor_duration_features",
                is_full,
            )

            cache_hit = not (
                avg_15m is None
                and avg_1h is None
                and dist_km is None
                and demand_pressure is None
            )

            results.append(
                CorridorDurationOnlineFeatures(
                    corridor_id=cid,
                    avg_duration_last_15m=(
                        float(avg_15m) if avg_15m is not None else None
                    ),
                    avg_duration_last_1h=(
                        float(avg_1h) if avg_1h is not None else None
                    ),
                    distance_km=float(dist_km) if dist_km is not None else None,
                    origin_zone_demand_pressure=(
                        int(demand_pressure) if demand_pressure is not None else None
                    ),
                    avg_traffic_speed_current=(
                        float(speed_current) if speed_current is not None else None
                    ),
                    cache_hit=cache_hit,
                )
            )

        return results

    def get_prediction_features(
        self,
        pickup_zone_id: int,
        dropoff_zone_id: int,
        use_push_features: bool = True,
    ) -> PredictionOnlineFeatures:
        """Fetch all required features for a trip duration / demand prediction query.

        Queries origin zone demand, destination zone demand, and corridor duration.

        Args:
            pickup_zone_id: Origin taxi zone ID.
            dropoff_zone_id: Destination taxi zone ID.
            use_push_features: If True, coalesce streaming push features with batch features.

        Returns:
            PredictionOnlineFeatures with combined feature payloads and cache status.
        """
        cid = build_corridor_id(pickup_zone_id, dropoff_zone_id)
        zones = self.get_zone_demand_features(
            [pickup_zone_id, dropoff_zone_id],
            use_push_features=use_push_features,
        )
        corridors = self.get_corridor_duration_features(
            [cid],
            use_push_features=use_push_features,
        )

        origin_demand = zones[0]
        destination_demand = zones[1]
        corridor_dur = corridors[0]

        all_cached = (
            origin_demand.cache_hit
            and destination_demand.cache_hit
            and corridor_dur.cache_hit
        )

        return PredictionOnlineFeatures(
            pickup_zone_id=pickup_zone_id,
            dropoff_zone_id=dropoff_zone_id,
            corridor_id=cid,
            origin_demand=origin_demand,
            destination_demand=destination_demand,
            corridor_duration=corridor_dur,
            all_cached=all_cached,
        )


# Global cached client instance for process-level reuse
_default_client: Optional[FeastOnlineClient] = None


def get_online_client(
    store: Optional[FeatureStore] = None,
    repo_path: Optional[Union[str, Path]] = None,
    use_sqlite_fallback: bool = False,
    refresh: bool = False,
) -> FeastOnlineClient:
    """Return a shared or newly initialized FeastOnlineClient instance."""
    global _default_client
    if _default_client is None or refresh or store is not None:
        _default_client = FeastOnlineClient(
            store=store,
            repo_path=repo_path,
            use_sqlite_fallback=use_sqlite_fallback,
        )
    return _default_client


def get_zone_demand_online_features(
    zone_ids: Union[int, List[int]],
    client: Optional[FeastOnlineClient] = None,
    store: Optional[FeatureStore] = None,
    use_push_features: bool = True,
) -> List[ZoneDemandOnlineFeatures]:
    """Top-level convenience function to fetch zone demand online features."""
    c = client or (get_online_client(store=store) if store else get_online_client())
    return c.get_zone_demand_features(zone_ids, use_push_features=use_push_features)


def get_corridor_duration_online_features(
    corridor_ids: Union[str, List[str]],
    client: Optional[FeastOnlineClient] = None,
    store: Optional[FeatureStore] = None,
    use_push_features: bool = True,
) -> List[CorridorDurationOnlineFeatures]:
    """Top-level convenience function to fetch corridor duration online features."""
    c = client or (get_online_client(store=store) if store else get_online_client())
    return c.get_corridor_duration_features(
        corridor_ids, use_push_features=use_push_features
    )
