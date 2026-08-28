"""Feast entity definitions for NYC taxi zone demand and corridor trip forecasting."""

from typing import List, Tuple

from feast import Entity, ValueType

# Zone entity representing an individual NYC taxi zone (TLC location ID 1..263)
zone_entity = Entity(
    name="zone",
    join_keys=["zone_id"],
    value_type=ValueType.INT32,
    description="NYC taxi zone ID (TLC location ID) — primary entity for zone demand features",
)

# Corridor entity representing an origin-destination zone pair ({pickup_zone_id}_{dropoff_zone_id})
corridor_entity = Entity(
    name="corridor",
    join_keys=["corridor_id"],
    value_type=ValueType.STRING,
    description="NYC taxi corridor ID formatted as {pickup_zone_id}_{dropoff_zone_id} — primary entity for corridor trip duration features",
)


def build_corridor_id(pickup_zone_id: int, dropoff_zone_id: int) -> str:
    """Format canonical corridor ID from pickup and dropoff zone IDs.

    Args:
        pickup_zone_id: Origin taxi zone ID.
        dropoff_zone_id: Destination taxi zone ID.

    Returns:
        Canonical corridor ID string formatted as '{pickup_zone_id}_{dropoff_zone_id}'.
    """
    if pickup_zone_id <= 0 or dropoff_zone_id <= 0:
        raise ValueError(
            f"Zone IDs must be positive integers: pickup={pickup_zone_id}, dropoff={dropoff_zone_id}"
        )
    return f"{pickup_zone_id}_{dropoff_zone_id}"


def parse_corridor_id(corridor_id: str) -> Tuple[int, int]:
    """Parse canonical corridor ID string into (pickup_zone_id, dropoff_zone_id) tuple.

    Args:
        corridor_id: Canonical corridor string formatted as '{pickup_zone_id}_{dropoff_zone_id}'.

    Returns:
        Tuple of (pickup_zone_id, dropoff_zone_id) as integers.

    Raises:
        ValueError: If the corridor ID format is invalid or zone IDs are non-numeric.
    """
    if not isinstance(corridor_id, str) or "_" not in corridor_id:
        raise ValueError(
            f"Invalid corridor_id format: '{corridor_id}'. Expected format '{{pickup_zone_id}}_{{dropoff_zone_id}}'."
        )
    parts = corridor_id.split("_", 1)
    try:
        pu_id, do_id = int(parts[0]), int(parts[1])
        if pu_id <= 0 or do_id <= 0:
            raise ValueError
        return pu_id, do_id
    except ValueError as err:
        raise ValueError(
            f"Invalid corridor_id values: '{corridor_id}'. Zone IDs must be positive integers."
        ) from err


def get_all_entities() -> List[Entity]:
    """Return all Feast entities configured for the platform.

    Returns:
        List of Feast Entity objects [zone_entity, corridor_entity].
    """
    return [zone_entity, corridor_entity]
