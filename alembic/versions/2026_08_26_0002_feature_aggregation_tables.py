"""Feature aggregation tables for Feast offline store.

Revision ID: 0002_feature_aggregation_tables
Revises: 0001_initial_schemas
Create Date: 2026-08-26 15:00:00.000000 UTC
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0002_feature_aggregation_tables"
down_revision: Union[str, None] = "0001_initial_schemas"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Zone Demand Features Hourly Table
    op.create_table(
        "zone_demand_features_hourly",
        sa.Column("zone_id", sa.Integer(), nullable=False),
        sa.Column("pickup_datetime", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "pickup_count_last_15m",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "pickup_count_last_1h",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "pickup_count_last_24h",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "pickup_count_same_hour_last_week",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column("hour_of_day", sa.Integer(), nullable=False),
        sa.Column("day_of_week", sa.Integer(), nullable=False),
        sa.Column("is_weekend", sa.Boolean(), nullable=False),
        sa.Column("is_holiday", sa.Boolean(), nullable=False),
        sa.Column("avg_temp_last_1h", sa.Float(), nullable=True),
        sa.Column(
            "is_precipitating",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint(
            "zone_id", "pickup_datetime", name="pk_zone_demand_features_hourly"
        ),
        schema="warehouse",
    )
    op.create_index(
        "ix_zone_demand_features_hourly_lookup",
        "zone_demand_features_hourly",
        ["zone_id", "pickup_datetime"],
        unique=False,
        schema="warehouse",
    )

    # 2. Corridor Duration Features Hourly Table
    op.create_table(
        "corridor_duration_features_hourly",
        sa.Column("corridor_id", sa.String(length=16), nullable=False),
        sa.Column("dropoff_datetime", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "avg_duration_last_15m",
            sa.Float(),
            server_default=sa.text("0.0"),
            nullable=False,
        ),
        sa.Column(
            "avg_duration_last_1h",
            sa.Float(),
            server_default=sa.text("0.0"),
            nullable=False,
        ),
        sa.Column(
            "distance_km",
            sa.Float(),
            server_default=sa.text("0.0"),
            nullable=False,
        ),
        sa.Column(
            "origin_zone_demand_pressure",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column("avg_traffic_speed_current", sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint(
            "corridor_id",
            "dropoff_datetime",
            name="pk_corridor_duration_features_hourly",
        ),
        schema="warehouse",
    )
    op.create_index(
        "ix_corridor_duration_features_hourly_lookup",
        "corridor_duration_features_hourly",
        ["corridor_id", "dropoff_datetime"],
        unique=False,
        schema="warehouse",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_corridor_duration_features_hourly_lookup",
        table_name="corridor_duration_features_hourly",
        schema="warehouse",
    )
    op.drop_table("corridor_duration_features_hourly", schema="warehouse")
    op.drop_index(
        "ix_zone_demand_features_hourly_lookup",
        table_name="zone_demand_features_hourly",
        schema="warehouse",
    )
    op.drop_table("zone_demand_features_hourly", schema="warehouse")
