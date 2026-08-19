"""Initial schemas and core warehouse tables.

Revision ID: 0001_initial_schemas
Revises: None
Create Date: 2026-08-19 20:00:00.000000 UTC
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0001_initial_schemas"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Create schemas
    op.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    op.execute("CREATE SCHEMA IF NOT EXISTS warehouse;")

    # 2. Taxi zones reference table
    op.create_table(
        "taxi_zones",
        sa.Column("zone_id", sa.Integer(), primary_key=True, autoincrement=False),
        sa.Column("borough", sa.String(length=50), nullable=False),
        sa.Column("zone_name", sa.String(length=255), nullable=False),
        sa.Column("service_zone", sa.String(length=50), nullable=True),
        sa.Column("centroid_lat", sa.Numeric(precision=9, scale=6), nullable=False),
        sa.Column("centroid_lon", sa.Numeric(precision=9, scale=6), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        schema="warehouse",
    )

    # 3. Raw trip staging table
    op.create_table(
        "trips",
        sa.Column("raw_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("vendor_id", sa.Integer(), nullable=True),
        sa.Column("pickup_datetime", sa.DateTime(timezone=True), nullable=True),
        sa.Column("dropoff_datetime", sa.DateTime(timezone=True), nullable=True),
        sa.Column("passenger_count", sa.Numeric(), nullable=True),
        sa.Column("trip_distance", sa.Numeric(), nullable=True),
        sa.Column("rate_code_id", sa.Numeric(), nullable=True),
        sa.Column("store_and_fwd_flag", sa.String(length=5), nullable=True),
        sa.Column("pickup_location_id", sa.Integer(), nullable=True),
        sa.Column("dropoff_location_id", sa.Integer(), nullable=True),
        sa.Column("payment_type", sa.Numeric(), nullable=True),
        sa.Column("fare_amount", sa.Numeric(), nullable=True),
        sa.Column("extra", sa.Numeric(), nullable=True),
        sa.Column("mta_tax", sa.Numeric(), nullable=True),
        sa.Column("tip_amount", sa.Numeric(), nullable=True),
        sa.Column("tolls_amount", sa.Numeric(), nullable=True),
        sa.Column("improvement_surcharge", sa.Numeric(), nullable=True),
        sa.Column("total_amount", sa.Numeric(), nullable=True),
        sa.Column("congestion_surcharge", sa.Numeric(), nullable=True),
        sa.Column("airport_fee", sa.Numeric(), nullable=True),
        sa.Column("cab_type", sa.String(length=10), nullable=True),
        sa.Column("source_file", sa.String(length=255), nullable=True),
        sa.Column(
            "ingested_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        schema="raw",
    )

    # 4. Cleaned warehouse trips table
    op.create_table(
        "trips",
        sa.Column("trip_id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("vendor_id", sa.Integer(), nullable=True),
        sa.Column("cab_type", sa.String(length=10), nullable=False),
        sa.Column(
            "pickup_zone_id",
            sa.Integer(),
            sa.ForeignKey("warehouse.taxi_zones.zone_id"),
            nullable=False,
        ),
        sa.Column(
            "dropoff_zone_id",
            sa.Integer(),
            sa.ForeignKey("warehouse.taxi_zones.zone_id"),
            nullable=False,
        ),
        sa.Column("pickup_datetime", sa.DateTime(timezone=True), nullable=False),
        sa.Column("dropoff_datetime", sa.DateTime(timezone=True), nullable=False),
        sa.Column("trip_duration_seconds", sa.Integer(), nullable=False),
        sa.Column("time_bin_15m", sa.DateTime(timezone=True), nullable=False),
        sa.Column("day_of_week", sa.Integer(), nullable=False),
        sa.Column("hour_of_day", sa.Integer(), nullable=False),
        sa.Column("is_weekend", sa.Boolean(), nullable=False),
        sa.Column("passenger_count", sa.Integer(), nullable=True),
        sa.Column("trip_distance_km", sa.Numeric(precision=8, scale=2), nullable=True),
        sa.Column("fare_amount", sa.Numeric(precision=8, scale=2), nullable=True),
        sa.Column("tip_amount", sa.Numeric(precision=8, scale=2), nullable=True),
        sa.Column("total_amount", sa.Numeric(precision=8, scale=2), nullable=True),
        sa.Column(
            "source",
            sa.String(length=20),
            server_default="historical",
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        schema="warehouse",
    )

    # Indexes on warehouse.trips
    op.create_index(
        "ix_warehouse_trips_pickup_timebin",
        "trips",
        ["pickup_zone_id", "time_bin_15m"],
        schema="warehouse",
    )
    op.create_index(
        "ix_warehouse_trips_corridor_timebin",
        "trips",
        ["pickup_zone_id", "dropoff_zone_id", "time_bin_15m"],
        schema="warehouse",
    )
    op.create_index(
        "ix_warehouse_trips_pickup_datetime",
        "trips",
        ["pickup_datetime"],
        schema="warehouse",
    )
    op.create_index(
        "ix_warehouse_trips_dropoff_datetime",
        "trips",
        ["dropoff_datetime"],
        schema="warehouse",
    )

    # 5. Loaded months tracker (idempotency)
    op.create_table(
        "loaded_months",
        sa.Column("month_key", sa.String(length=50), primary_key=True),
        sa.Column("record_count", sa.Integer(), nullable=False),
        sa.Column(
            "loaded_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        schema="warehouse",
    )

    # 6. Pipeline execution runs log
    op.create_table(
        "pipeline_runs",
        sa.Column("run_id", sa.String(length=100), primary_key=True),
        sa.Column("job_name", sa.String(length=100), nullable=False),
        sa.Column("status", sa.String(length=50), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_seconds", sa.Numeric(precision=8, scale=2), nullable=True),
        sa.Column(
            "records_processed", sa.Integer(), server_default="0", nullable=False
        ),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("triggered_by", sa.String(length=50), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        schema="warehouse",
    )

    # 7. Predictions log table
    op.create_table(
        "predictions",
        sa.Column("prediction_id", sa.String(length=100), primary_key=True),
        sa.Column("entity_type", sa.String(length=50), nullable=False),
        sa.Column("entity_id", sa.String(length=100), nullable=False),
        sa.Column("model_version", sa.String(length=100), nullable=False),
        sa.Column("predicted_value", sa.Numeric(precision=10, scale=2), nullable=False),
        sa.Column("predicted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("actual_value", sa.Numeric(precision=10, scale=2), nullable=True),
        sa.Column("actual_recorded_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        schema="warehouse",
    )

    # 8. Monitoring reports table
    op.create_table(
        "monitoring_reports",
        sa.Column("report_id", sa.String(length=100), primary_key=True),
        sa.Column("report_type", sa.String(length=50), nullable=False),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("summary_json", sa.Text(), nullable=True),
        sa.Column("file_path", sa.String(length=255), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        schema="warehouse",
    )


def downgrade() -> None:
    op.drop_table("monitoring_reports", schema="warehouse")
    op.drop_table("predictions", schema="warehouse")
    op.drop_table("pipeline_runs", schema="warehouse")
    op.drop_table("loaded_months", schema="warehouse")
    op.drop_table("trips", schema="warehouse")
    op.drop_table("trips", schema="raw")
    op.drop_table("taxi_zones", schema="warehouse")
    op.execute("DROP SCHEMA IF EXISTS warehouse CASCADE;")
    op.execute("DROP SCHEMA IF EXISTS raw CASCADE;")
