"""Realtime streaming snapshot tables for traffic, weather, and transit feeds.

Revision ID: 0003_realtime_snapshot_tables
Revises: 0002_feature_aggregation_tables
Create Date: 2026-09-05 15:00:00.000000 UTC
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0003_realtime_snapshot_tables"
down_revision: Union[str, None] = "0002_feature_aggregation_tables"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Traffic Snapshots Table (dedup on segment_id, recorded_at)
    op.create_table(
        "traffic_snapshots",
        sa.Column("segment_id", sa.String(length=50), nullable=False),
        sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("speed_mph", sa.Float(), nullable=False),
        sa.Column("speed_kmh", sa.Float(), nullable=False),
        sa.Column(
            "travel_time_seconds",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column("borough", sa.String(length=50), nullable=True),
        sa.Column("link_name", sa.String(length=255), nullable=True),
        sa.Column("source", sa.String(length=50), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint(
            "segment_id", "recorded_at", name="pk_traffic_snapshots"
        ),
        schema="warehouse",
    )
    op.create_index(
        "ix_traffic_snapshots_recorded_at",
        "traffic_snapshots",
        ["recorded_at"],
        unique=False,
        schema="warehouse",
    )

    # 2. Weather Snapshots Table (dedup on minute-floored time_bucket)
    op.create_table(
        "weather_snapshots",
        sa.Column("time_bucket", sa.DateTime(timezone=True), nullable=False),
        sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("temp_c", sa.Float(), nullable=False),
        sa.Column("feels_like_c", sa.Float(), nullable=True),
        sa.Column("humidity_pct", sa.Integer(), nullable=True),
        sa.Column("wind_speed_kmh", sa.Float(), nullable=True),
        sa.Column(
            "precipitation_mm_1h",
            sa.Float(),
            server_default=sa.text("0.0"),
            nullable=False,
        ),
        sa.Column(
            "is_precipitating",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column("condition_main", sa.String(length=50), nullable=True),
        sa.Column("condition_description", sa.String(length=100), nullable=True),
        sa.Column("source", sa.String(length=50), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("time_bucket", name="pk_weather_snapshots"),
        schema="warehouse",
    )
    op.create_index(
        "ix_weather_snapshots_recorded_at",
        "weather_snapshots",
        ["recorded_at"],
        unique=False,
        schema="warehouse",
    )

    # 3. Transit Snapshots Table (dedup on route_id, recorded_at)
    op.create_table(
        "transit_snapshots",
        sa.Column("route_id", sa.String(length=50), nullable=False),
        sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("trip_id", sa.String(length=100), nullable=True),
        sa.Column("vehicle_id", sa.String(length=100), nullable=True),
        sa.Column("current_status", sa.String(length=50), nullable=True),
        sa.Column("stop_id", sa.String(length=50), nullable=True),
        sa.Column(
            "delay_seconds", sa.Integer(), server_default=sa.text("0"), nullable=False
        ),
        sa.Column(
            "congestion_level",
            sa.String(length=50),
            server_default=sa.text("'NORMAL'"),
            nullable=False,
        ),
        sa.Column("source", sa.String(length=50), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("route_id", "recorded_at", name="pk_transit_snapshots"),
        schema="warehouse",
    )
    op.create_index(
        "ix_transit_snapshots_recorded_at",
        "transit_snapshots",
        ["recorded_at"],
        unique=False,
        schema="warehouse",
    )


def downgrade() -> None:
    op.drop_table("transit_snapshots", schema="warehouse")
    op.drop_table("weather_snapshots", schema="warehouse")
    op.drop_table("traffic_snapshots", schema="warehouse")
