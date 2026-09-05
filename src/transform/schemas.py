"""Pydantic validation schemas for real-time streaming payloads and deadletter records."""

from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Optional, Set

from pydantic import BaseModel, Field, field_validator, model_validator

# NYC TLC Taxi Zone Location IDs
VALID_ZONE_RANGE: Set[int] = set(range(1, 266))
MILES_TO_KM: float = 1.60934


def ensure_utc(dt: datetime) -> datetime:
    """Normalize datetime to timezone-aware UTC datetime."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class TripEventPayload(BaseModel):
    """Payload schema for simulated or live trip events (trip.events topic)."""

    trip_id: Optional[int] = None
    vendor_id: Optional[int] = None
    cab_type: str = Field(default="yellow")
    pickup_zone_id: int
    dropoff_zone_id: int
    pickup_datetime: datetime
    dropoff_datetime: datetime
    trip_duration_seconds: Optional[int] = None
    passenger_count: Optional[int] = 1
    trip_distance_km: Optional[float] = None
    trip_distance_miles: Optional[float] = None
    fare_amount: Optional[float] = 0.0
    tip_amount: Optional[float] = 0.0
    total_amount: Optional[float] = 0.0
    source: str = Field(default="replay")
    replayed_at: Optional[datetime] = None

    @field_validator("pickup_zone_id", "dropoff_zone_id")
    @classmethod
    def validate_zone_id(cls, v: int) -> int:
        if v not in VALID_ZONE_RANGE:
            raise ValueError(
                f"Zone ID {v} is outside valid NYC TLC taxi zone range [1, 265]."
            )
        return v

    @field_validator("fare_amount", "tip_amount", "total_amount")
    @classmethod
    def validate_financials(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and v < 0.0:
            raise ValueError(f"Financial amount cannot be negative: {v}")
        return v

    @field_validator("passenger_count")
    @classmethod
    def validate_passengers(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and not (1 <= v <= 9):
            raise ValueError(f"Passenger count {v} outside plausible bounds [1, 9].")
        return v

    @model_validator(mode="after")
    def validate_trip_lifecycle_and_distance(self) -> "TripEventPayload":
        # Normalize datetimes to UTC
        self.pickup_datetime = ensure_utc(self.pickup_datetime)
        self.dropoff_datetime = ensure_utc(self.dropoff_datetime)

        # Duration validation
        calculated_duration = int(
            (self.dropoff_datetime - self.pickup_datetime).total_seconds()
        )
        if self.trip_duration_seconds is None:
            self.trip_duration_seconds = calculated_duration

        if not (60 <= self.trip_duration_seconds <= 86400):
            raise ValueError(
                f"Trip duration {self.trip_duration_seconds}s outside plausible bounds [60s, 86400s]."
            )

        # Distance normalization & validation
        if self.trip_distance_miles is None and self.trip_distance_km is not None:
            self.trip_distance_miles = round(self.trip_distance_km / MILES_TO_KM, 2)
        elif self.trip_distance_km is None and self.trip_distance_miles is not None:
            self.trip_distance_km = round(self.trip_distance_miles * MILES_TO_KM, 2)

        if self.trip_distance_miles is None:
            raise ValueError("Trip distance must be provided in miles or kilometers.")

        if not (0.01 <= self.trip_distance_miles <= 300.0):
            raise ValueError(
                f"Trip distance {self.trip_distance_miles}mi outside plausible bounds [0.01mi, 300.0mi]."
            )

        # Average speed check
        hours = self.trip_duration_seconds / 3600.0
        avg_speed_mph = self.trip_distance_miles / hours if hours > 0 else 0.0
        if avg_speed_mph > 100.0:
            raise ValueError(
                f"Implausible average speed {avg_speed_mph:.1f}mph exceeds threshold 100.0mph."
            )

        return self


class TrafficSnapshotPayload(BaseModel):
    """Payload schema for NYC real-time traffic speed snapshots (traffic.snapshots)."""

    segment_id: str = Field(..., min_length=1)
    speed_mph: float = Field(..., ge=0.0, le=100.0)
    speed_kmh: Optional[float] = None
    travel_time_seconds: int = Field(default=0, ge=0, le=7200)
    borough: Optional[str] = None
    link_name: Optional[str] = None
    recorded_at: datetime
    source: str = Field(default="socrata_live")

    @model_validator(mode="after")
    def validate_and_normalize(self) -> "TrafficSnapshotPayload":
        self.recorded_at = ensure_utc(self.recorded_at)
        now = datetime.now(timezone.utc)
        if self.recorded_at > now + timedelta(hours=24):
            raise ValueError(
                f"Traffic snapshot timestamp {self.recorded_at} is too far in the future."
            )
        if self.recorded_at < now - timedelta(days=30):
            raise ValueError(
                f"Traffic snapshot timestamp {self.recorded_at} is older than 30 days."
            )

        if self.speed_kmh is None:
            self.speed_kmh = round(self.speed_mph * MILES_TO_KM, 2)

        return self


class TransitPositionPayload(BaseModel):
    """Payload schema for MTA transit status and delay proxies (transit.positions)."""

    route_id: str = Field(..., min_length=1)
    trip_id: Optional[str] = None
    vehicle_id: Optional[str] = None
    current_status: Optional[str] = None
    stop_id: Optional[str] = None
    delay_seconds: int = Field(default=0, ge=0, le=86400)
    congestion_level: Literal["NORMAL", "MODERATE", "HEAVY_DELAY", "UNKNOWN"] = Field(
        default="NORMAL"
    )
    recorded_at: datetime
    source: str = Field(default="mta_gtfs_live")

    @model_validator(mode="after")
    def validate_and_normalize(self) -> "TransitPositionPayload":
        self.recorded_at = ensure_utc(self.recorded_at)
        now = datetime.now(timezone.utc)
        if self.recorded_at > now + timedelta(hours=24):
            raise ValueError(
                f"Transit timestamp {self.recorded_at} is too far in the future."
            )
        if self.recorded_at < now - timedelta(days=30):
            raise ValueError(
                f"Transit timestamp {self.recorded_at} is older than 30 days."
            )
        return self


class WeatherSnapshotPayload(BaseModel):
    """Payload schema for NYC weather observations (weather.snapshots)."""

    temp_c: float = Field(..., ge=-35.0, le=55.0)
    temp_f: Optional[float] = None
    feels_like_c: Optional[float] = None
    humidity_pct: Optional[int] = Field(default=None, ge=0, le=100)
    wind_speed_kmh: Optional[float] = Field(default=None, ge=0.0, le=250.0)
    precipitation_mm_1h: float = Field(default=0.0, ge=0.0, le=300.0)
    is_precipitating: bool = Field(default=False)
    condition: Optional[str] = None
    condition_main: Optional[str] = None
    condition_description: Optional[str] = None
    recorded_at: datetime
    time_bucket: Optional[datetime] = None
    source: str = Field(default="openweathermap_live")

    @model_validator(mode="after")
    def validate_and_normalize(self) -> "WeatherSnapshotPayload":
        self.recorded_at = ensure_utc(self.recorded_at)
        now = datetime.now(timezone.utc)
        if self.recorded_at > now + timedelta(hours=24):
            raise ValueError(
                f"Weather timestamp {self.recorded_at} is too far in the future."
            )
        if self.recorded_at < now - timedelta(days=30):
            raise ValueError(
                f"Weather timestamp {self.recorded_at} is older than 30 days."
            )

        if self.condition and not self.condition_main:
            self.condition_main = self.condition

        # Floor to minute bucket for deduplication (per ADR & design resolution)
        if self.time_bucket is None:
            self.time_bucket = self.recorded_at.replace(second=0, microsecond=0)
        else:
            self.time_bucket = ensure_utc(self.time_bucket).replace(
                second=0, microsecond=0
            )

        if self.temp_f is None:
            self.temp_f = round((self.temp_c * 9.0 / 5.0) + 32.0, 2)

        return self


class DeadletterPayload(BaseModel):
    """Payload schema for quarantined records published to trip.events.deadletter."""

    error_reason: str
    topic: str
    raw_payload: Any
    partition: Optional[int] = None
    offset: Optional[int] = None
    failed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
