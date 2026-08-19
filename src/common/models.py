"""SQLAlchemy ORM models for raw and warehouse database tables."""

from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    func,
)
from sqlalchemy.orm import relationship

from src.common.db import Base


class TaxiZone(Base):
    """NYC Taxi Zone reference model containing precomputed numeric centroids."""

    __tablename__ = "taxi_zones"
    __table_args__ = {"schema": "warehouse"}

    zone_id: int = Column(Integer, primary_key=True, autoincrement=False)
    borough: str = Column(String(50), nullable=False)
    zone_name: str = Column(String(255), nullable=False)
    service_zone: Optional[str] = Column(String(50), nullable=True)
    centroid_lat: Decimal = Column(Numeric(9, 6), nullable=False)
    centroid_lon: Decimal = Column(Numeric(9, 6), nullable=False)
    created_at: datetime = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    pickup_trips = relationship(
        "WarehouseTrip",
        foreign_keys="WarehouseTrip.pickup_zone_id",
        back_populates="pickup_zone",
    )
    dropoff_trips = relationship(
        "WarehouseTrip",
        foreign_keys="WarehouseTrip.dropoff_zone_id",
        back_populates="dropoff_zone",
    )


class RawTrip(Base):
    """Raw staging model for extracted TLC trip records before validation."""

    __tablename__ = "trips"
    __table_args__ = {"schema": "raw"}

    raw_id: int = Column(BigInteger, primary_key=True, autoincrement=True)
    vendor_id: Optional[int] = Column(Integer, nullable=True)
    pickup_datetime: Optional[datetime] = Column(DateTime(timezone=True), nullable=True)
    dropoff_datetime: Optional[datetime] = Column(
        DateTime(timezone=True), nullable=True
    )
    passenger_count: Optional[Decimal] = Column(Numeric, nullable=True)
    trip_distance: Optional[Decimal] = Column(Numeric, nullable=True)
    rate_code_id: Optional[Decimal] = Column(Numeric, nullable=True)
    store_and_fwd_flag: Optional[str] = Column(String(5), nullable=True)
    pickup_location_id: Optional[int] = Column(Integer, nullable=True)
    dropoff_location_id: Optional[int] = Column(Integer, nullable=True)
    payment_type: Optional[Decimal] = Column(Numeric, nullable=True)
    fare_amount: Optional[Decimal] = Column(Numeric, nullable=True)
    extra: Optional[Decimal] = Column(Numeric, nullable=True)
    mta_tax: Optional[Decimal] = Column(Numeric, nullable=True)
    tip_amount: Optional[Decimal] = Column(Numeric, nullable=True)
    tolls_amount: Optional[Decimal] = Column(Numeric, nullable=True)
    improvement_surcharge: Optional[Decimal] = Column(Numeric, nullable=True)
    total_amount: Optional[Decimal] = Column(Numeric, nullable=True)
    congestion_surcharge: Optional[Decimal] = Column(Numeric, nullable=True)
    airport_fee: Optional[Decimal] = Column(Numeric, nullable=True)
    cab_type: Optional[str] = Column(String(10), nullable=True)
    source_file: Optional[str] = Column(String(255), nullable=True)
    ingested_at: datetime = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class WarehouseTrip(Base):
    """Cleaned and validated historical trips with derived features and time bins."""

    __tablename__ = "trips"
    __table_args__ = (
        Index("ix_warehouse_trips_pickup_timebin", "pickup_zone_id", "time_bin_15m"),
        Index(
            "ix_warehouse_trips_corridor_timebin",
            "pickup_zone_id",
            "dropoff_zone_id",
            "time_bin_15m",
        ),
        Index("ix_warehouse_trips_pickup_datetime", "pickup_datetime"),
        Index("ix_warehouse_trips_dropoff_datetime", "dropoff_datetime"),
        {"schema": "warehouse"},
    )

    trip_id: int = Column(BigInteger, primary_key=True, autoincrement=True)
    vendor_id: Optional[int] = Column(Integer, nullable=True)
    cab_type: str = Column(String(10), nullable=False)
    pickup_zone_id: int = Column(
        Integer, ForeignKey("warehouse.taxi_zones.zone_id"), nullable=False
    )
    dropoff_zone_id: int = Column(
        Integer, ForeignKey("warehouse.taxi_zones.zone_id"), nullable=False
    )
    pickup_datetime: datetime = Column(DateTime(timezone=True), nullable=False)
    dropoff_datetime: datetime = Column(DateTime(timezone=True), nullable=False)
    trip_duration_seconds: int = Column(Integer, nullable=False)
    time_bin_15m: datetime = Column(DateTime(timezone=True), nullable=False)
    day_of_week: int = Column(Integer, nullable=False)
    hour_of_day: int = Column(Integer, nullable=False)
    is_weekend: bool = Column(Boolean, nullable=False)
    passenger_count: Optional[int] = Column(Integer, nullable=True)
    trip_distance_km: Optional[Decimal] = Column(Numeric(8, 2), nullable=True)
    fare_amount: Optional[Decimal] = Column(Numeric(8, 2), nullable=True)
    tip_amount: Optional[Decimal] = Column(Numeric(8, 2), nullable=True)
    total_amount: Optional[Decimal] = Column(Numeric(8, 2), nullable=True)
    source: str = Column(String(20), server_default="historical", nullable=False)
    created_at: datetime = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    pickup_zone = relationship(
        "TaxiZone", foreign_keys=[pickup_zone_id], back_populates="pickup_trips"
    )
    dropoff_zone = relationship(
        "TaxiZone", foreign_keys=[dropoff_zone_id], back_populates="dropoff_trips"
    )


class LoadedMonth(Base):
    """Tracking table for historical TLC monthly batches to ensure idempotent ETL."""

    __tablename__ = "loaded_months"
    __table_args__ = {"schema": "warehouse"}

    month_key: str = Column(String(50), primary_key=True)
    record_count: int = Column(Integer, nullable=False)
    loaded_at: datetime = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class PipelineRun(Base):
    """Log of orchestrated batch/streaming pipeline runs and task outcomes."""

    __tablename__ = "pipeline_runs"
    __table_args__ = {"schema": "warehouse"}

    run_id: str = Column(String(100), primary_key=True)
    job_name: str = Column(String(100), nullable=False)
    status: str = Column(String(50), nullable=False)
    started_at: datetime = Column(DateTime(timezone=True), nullable=False)
    finished_at: Optional[datetime] = Column(DateTime(timezone=True), nullable=True)
    duration_seconds: Optional[Decimal] = Column(Numeric(8, 2), nullable=True)
    records_processed: int = Column(Integer, server_default="0", nullable=False)
    error_message: Optional[str] = Column(Text, nullable=True)
    triggered_by: Optional[str] = Column(String(50), nullable=True)
    created_at: datetime = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class Prediction(Base):
    """Inference predictions log for monitoring drift and agent query resolution."""

    __tablename__ = "predictions"
    __table_args__ = {"schema": "warehouse"}

    prediction_id: str = Column(String(100), primary_key=True)
    entity_type: str = Column(String(50), nullable=False)
    entity_id: str = Column(String(100), nullable=False)
    model_version: str = Column(String(100), nullable=False)
    predicted_value: Decimal = Column(Numeric(10, 2), nullable=False)
    predicted_at: datetime = Column(DateTime(timezone=True), nullable=False)
    actual_value: Optional[Decimal] = Column(Numeric(10, 2), nullable=True)
    actual_recorded_at: Optional[datetime] = Column(
        DateTime(timezone=True), nullable=True
    )
    created_at: datetime = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class MonitoringReport(Base):
    """Evidently AI data drift and model performance report metadata."""

    __tablename__ = "monitoring_reports"
    __table_args__ = {"schema": "warehouse"}

    report_id: str = Column(String(100), primary_key=True)
    report_type: str = Column(String(50), nullable=False)
    generated_at: datetime = Column(DateTime(timezone=True), nullable=False)
    summary_json: Optional[str] = Column(Text, nullable=True)
    file_path: Optional[str] = Column(String(255), nullable=True)
    created_at: datetime = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
