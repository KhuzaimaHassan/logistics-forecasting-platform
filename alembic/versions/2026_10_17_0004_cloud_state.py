"""Durable bounded-ingestion cursors and monitoring HTML, additive only."""

import sqlalchemy as sa

from alembic import op

revision = "0004_cloud_state"
down_revision = "0003_realtime_snapshot_tables"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "pipeline_cursors",
        sa.Column("cursor_key", sa.String(150), primary_key=True),
        sa.Column("next_index", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("metadata_json", sa.Text(), nullable=False, server_default="{}"),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        schema="warehouse",
    )
    op.create_table(
        "monitoring_html",
        sa.Column(
            "report_id",
            sa.String(100),
            sa.ForeignKey("warehouse.monitoring_reports.report_id"),
            primary_key=True,
        ),
        sa.Column("html_content", sa.Text(), nullable=False),
        schema="warehouse",
    )


def downgrade():
    op.drop_table("monitoring_html", schema="warehouse")
    op.drop_table("pipeline_cursors", schema="warehouse")
