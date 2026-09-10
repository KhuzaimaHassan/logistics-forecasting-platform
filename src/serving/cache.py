"""Low-latency prediction caching layer for logistics forecasting endpoints.

Implements ADR-020 Section 3:
- Caches genuine predictions (cache_hit=True, status="ok") with strict 60s TTL.
- Enforces Degraded Cache-Bypass Policy: predictions with status="degraded_fallback"
  or cache_hit=False are NEVER cached (zero-TTL / cache write bypass).
- Provides vectorized Redis MGET/pipeline operations for batch endpoints.
- Provides seamless thread-safe in-memory TTL/LRU fallback if Redis is temporarily
  unreachable or disconnected, ensuring zero service failure.
"""

import json
import logging
import threading
import time
from typing import Dict, List, Optional, Tuple

import redis

logger = logging.getLogger(__name__)

DEFAULT_PREDICTION_TTL_SECONDS = 60
DEFAULT_MEMORY_CACHE_MAXSIZE = 5000


class _InMemoryTTLCache:
    """Thread-safe in-memory TTL cache with bounded size and monotonic expiry."""

    def __init__(self, maxsize: int = DEFAULT_MEMORY_CACHE_MAXSIZE):
        self._maxsize = maxsize
        self._cache: Dict[str, Tuple[str, float]] = (
            {}
        )  # key -> (serialized_val, expire_at)
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[str]:
        now = time.monotonic()
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return None
            val, expire_at = entry
            if now >= expire_at:
                del self._cache[key]
                return None
            return val

    def set(self, key: str, val: str, ttl_seconds: int) -> None:
        now = time.monotonic()
        expire_at = now + ttl_seconds
        with self._lock:
            # If cache exceeds limit, prune expired items first
            if len(self._cache) >= self._maxsize and key not in self._cache:
                expired = [k for k, (_, exp) in self._cache.items() if now >= exp]
                for k in expired:
                    del self._cache[k]
                # If still at max capacity, evict oldest
                if len(self._cache) >= self._maxsize:
                    oldest_key = next(iter(self._cache))
                    del self._cache[oldest_key]
            self._cache[key] = (val, expire_at)

    def mget(self, keys: List[str]) -> List[Optional[str]]:
        now = time.monotonic()
        results: List[Optional[str]] = []
        with self._lock:
            for k in keys:
                entry = self._cache.get(k)
                if entry is None:
                    results.append(None)
                else:
                    val, expire_at = entry
                    if now >= expire_at:
                        del self._cache[k]
                        results.append(None)
                    else:
                        results.append(val)
        return results

    def mset(self, items: Dict[str, str], ttl_seconds: int) -> None:
        for k, v in items.items():
            self.set(k, v, ttl_seconds)

    def delete(self, key: str) -> None:
        with self._lock:
            self._cache.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()

    def __len__(self) -> int:
        now = time.monotonic()
        with self._lock:
            return sum(1 for _, exp in self._cache.values() if now < exp)


class PredictionCache:
    """Prediction caching service supporting Redis with in-memory TTL fallback."""

    def __init__(
        self,
        redis_url: Optional[str] = None,
        ttl_seconds: int = DEFAULT_PREDICTION_TTL_SECONDS,
        max_memory_items: int = DEFAULT_MEMORY_CACHE_MAXSIZE,
    ):
        self.ttl_seconds = ttl_seconds
        self.redis_url = redis_url
        self._memory_cache = _InMemoryTTLCache(maxsize=max_memory_items)
        self._redis_client: Optional[redis.Redis] = None

        if redis_url:
            try:
                self._redis_client = redis.Redis.from_url(
                    redis_url,
                    decode_responses=True,
                    socket_timeout=1.0,
                    socket_connect_timeout=1.0,
                )
            except Exception as exc:
                logger.warning(
                    "PredictionCache failed to initialize Redis client (%s); using in-memory cache.",
                    exc,
                )
                self._redis_client = None

    @staticmethod
    def demand_key(zone_id: int, horizon_minutes: int) -> str:
        """Cache key for zone demand predictions: pred:demand:{zone_id}:{horizon}."""
        return f"pred:demand:{zone_id}:{horizon_minutes}"

    @staticmethod
    def eta_key(origin_zone_id: int, dest_zone_id: int) -> str:
        """Cache key for corridor ETA predictions: pred:eta:{origin}:{dest}."""
        return f"pred:eta:{origin_zone_id}:{dest_zone_id}"

    def ping(self) -> bool:
        """Return True if the backing Redis instance is reachable."""
        if not self._redis_client:
            return False
        try:
            return bool(self._redis_client.ping())
        except Exception as exc:
            logger.debug("Redis ping failed: %s", exc)
            return False

    # -----------------------------------------------------------------------
    # Demand Caching
    # -----------------------------------------------------------------------

    def get_demand(self, zone_id: int, horizon_minutes: int = 15) -> Optional[dict]:
        """Retrieve cached demand prediction response dict if present."""
        key = self.demand_key(zone_id, horizon_minutes)
        raw = self._get(key)
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except Exception as exc:
            logger.warning("Failed to deserialize cached demand for %s: %s", key, exc)
            return None

    def set_demand(
        self,
        zone_id: int,
        horizon_minutes: int,
        response_dict: dict,
        status: str = "ok",
    ) -> bool:
        """Cache demand prediction if status is 'ok' and cache_hit is True.

        Bypasses write if status is 'degraded_fallback' or cache_hit is False (ADR-020).
        """
        if status == "degraded_fallback" or not response_dict.get("cache_hit", True):
            logger.debug(
                "Bypassing cache write for degraded demand prediction (zone=%d)",
                zone_id,
            )
            return False

        key = self.demand_key(zone_id, horizon_minutes)
        raw = json.dumps(response_dict)
        return self._set(key, raw, self.ttl_seconds)

    def get_demand_batch(
        self, zone_ids: List[int], horizon_minutes: int = 15
    ) -> Dict[int, dict]:
        """Vectorized lookup for multiple zones. Returns map of zone_id -> cached payload."""
        keys = [self.demand_key(z, horizon_minutes) for z in zone_ids]
        raw_list = self._mget(keys)
        results: Dict[int, dict] = {}
        for zid, raw in zip(zone_ids, raw_list, strict=True):
            if raw is not None:
                try:
                    results[zid] = json.loads(raw)
                except Exception as exc:
                    logger.warning(
                        "Failed to deserialize batch demand item %d: %s", zid, exc
                    )
        return results

    def set_demand_batch(self, items: List[dict], horizon_minutes: int = 15) -> int:
        """Vectorized store of genuine predictions. Bypasses any degraded items."""
        valid_items: Dict[str, str] = {}
        for item in items:
            is_hit = bool(item.get("cache_hit", True))
            status = str(item.get("status", "ok"))
            if is_hit and status != "degraded_fallback":
                zid = int(item["zone_id"])
                key = self.demand_key(zid, horizon_minutes)
                valid_items[key] = json.dumps(item)

        if not valid_items:
            return 0

        self._mset(valid_items, self.ttl_seconds)
        return len(valid_items)

    # -----------------------------------------------------------------------
    # ETA / Corridor Caching
    # -----------------------------------------------------------------------

    def get_eta(self, origin_zone_id: int, dest_zone_id: int) -> Optional[dict]:
        """Retrieve cached ETA prediction response dict if present."""
        key = self.eta_key(origin_zone_id, dest_zone_id)
        raw = self._get(key)
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except Exception as exc:
            logger.warning("Failed to deserialize cached ETA for %s: %s", key, exc)
            return None

    def set_eta(
        self,
        origin_zone_id: int,
        dest_zone_id: int,
        response_dict: dict,
        status: str = "ok",
    ) -> bool:
        """Cache corridor ETA prediction if status is 'ok' and cache_hit is True.

        Bypasses write if status is 'degraded_fallback' or cache_hit is False (ADR-020).
        """
        if status == "degraded_fallback" or not response_dict.get("cache_hit", True):
            logger.debug(
                "Bypassing cache write for degraded ETA prediction (%d_%d)",
                origin_zone_id,
                dest_zone_id,
            )
            return False

        key = self.eta_key(origin_zone_id, dest_zone_id)
        raw = json.dumps(response_dict)
        return self._set(key, raw, self.ttl_seconds)

    def get_eta_batch(
        self, pairs: List[Tuple[int, int]]
    ) -> Dict[Tuple[int, int], dict]:
        """Vectorized lookup for corridor pairs. Returns map of (origin, dest) -> cached payload."""
        keys = [self.eta_key(o, d) for o, d in pairs]
        raw_list = self._mget(keys)
        results: Dict[Tuple[int, int], dict] = {}
        for pair, raw in zip(pairs, raw_list, strict=True):
            if raw is not None:
                try:
                    results[pair] = json.loads(raw)
                except Exception as exc:
                    logger.warning(
                        "Failed to deserialize batch ETA item %s: %s", pair, exc
                    )
        return results

    def set_eta_batch(self, items: List[dict]) -> int:
        """Vectorized store of genuine corridor ETA predictions. Bypasses degraded items."""
        valid_items: Dict[str, str] = {}
        for item in items:
            is_hit = bool(item.get("cache_hit", True))
            status = str(item.get("status", "ok"))
            if is_hit and status != "degraded_fallback":
                orig = int(item["origin_zone_id"])
                dest = int(item["dest_zone_id"])
                key = self.eta_key(orig, dest)
                valid_items[key] = json.dumps(item)

        if not valid_items:
            return 0

        self._mset(valid_items, self.ttl_seconds)
        return len(valid_items)

    # -----------------------------------------------------------------------
    # Maintenance & Clearing
    # -----------------------------------------------------------------------

    def clear(self) -> None:
        """Clear all cached predictions from Redis and in-memory cache."""
        self._memory_cache.clear()
        if self._redis_client:
            try:
                # Scan and delete prediction keys pred:*
                keys_to_del = []
                for k in self._redis_client.scan_iter(match="pred:*", count=200):
                    keys_to_del.append(k)
                if keys_to_del:
                    self._redis_client.delete(*keys_to_del)
            except Exception as exc:
                logger.warning("Failed to clear Redis prediction keys: %s", exc)

    # -----------------------------------------------------------------------
    # Internal Redis / Memory Fallback Primitives
    # -----------------------------------------------------------------------

    def _get(self, key: str) -> Optional[str]:
        if self._redis_client:
            try:
                val = self._redis_client.get(key)
                if val is not None:
                    return str(val)
            except Exception as exc:
                logger.debug(
                    "Redis get(%s) failed (%s); falling back to memory.", key, exc
                )
        return self._memory_cache.get(key)

    def _set(self, key: str, val: str, ttl: int) -> bool:
        if self._redis_client:
            try:
                self._redis_client.set(key, val, ex=ttl)
            except Exception as exc:
                logger.debug(
                    "Redis set(%s) failed (%s); falling back to memory.", key, exc
                )
        self._memory_cache.set(key, val, ttl)
        return True

    def _mget(self, keys: List[str]) -> List[Optional[str]]:
        if not keys:
            return []
        if self._redis_client:
            try:
                raw_redis = self._redis_client.mget(keys)
                # If any missing in Redis, check memory cache for those
                combined = []
                for k, v in zip(keys, raw_redis, strict=True):
                    if v is not None:
                        combined.append(str(v))
                    else:
                        combined.append(self._memory_cache.get(k))
                return combined
            except Exception as exc:
                logger.debug("Redis mget failed (%s); falling back to memory.", exc)
        return self._memory_cache.mget(keys)

    def _mset(self, items: Dict[str, str], ttl: int) -> None:
        if not items:
            return
        if self._redis_client:
            try:
                pipe = self._redis_client.pipeline(transaction=False)
                for k, v in items.items():
                    pipe.set(k, v, ex=ttl)
                pipe.execute()
            except Exception as exc:
                logger.debug("Redis pipeline mset failed (%s); writing to memory.", exc)
        self._memory_cache.mset(items, ttl)
