"""Persist versioned Daily Snapshot response publications."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260905_0027"
down_revision = "20260905_0026"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "daily_snapshot_publication_inputs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("market", sa.String(length=8), nullable=False),
        sa.Column("scan_id", sa.String(length=36), nullable=False),
        sa.Column("analytics_version", sa.String(length=64), nullable=False),
        sa.Column("snapshot_schema_version", sa.Integer(), nullable=False),
        sa.Column("input_version", sa.String(length=64), nullable=False),
        sa.Column("key_markets_json", sa.Text(), nullable=False),
        sa.Column("key_markets_sha256", sa.String(length=64), nullable=False),
        sa.Column(
            "captured_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["scan_id"], ["scans.scan_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "market",
            "scan_id",
            "analytics_version",
            "snapshot_schema_version",
            name="uq_daily_snapshot_publication_input_identity",
        ),
    )
    op.create_index(
        "idx_daily_snapshot_publication_input_lookup",
        "daily_snapshot_publication_inputs",
        ["market", "scan_id", "analytics_version", "snapshot_schema_version"],
        unique=False,
    )
    op.create_table(
        "daily_snapshot_publications",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("market", sa.String(length=8), nullable=False),
        sa.Column("scan_id", sa.String(length=36), nullable=False),
        sa.Column("analytics_version", sa.String(length=64), nullable=False),
        sa.Column("snapshot_schema_version", sa.Integer(), nullable=False),
        sa.Column("publication_version", sa.String(length=64), nullable=False),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("payload_sha256", sa.String(length=64), nullable=False),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "published_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["scan_id"], ["scans.scan_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "market",
            "scan_id",
            "analytics_version",
            "snapshot_schema_version",
            name="uq_daily_snapshot_publication_identity",
        ),
    )
    op.create_index(
        "idx_daily_snapshot_publication_lookup",
        "daily_snapshot_publications",
        ["market", "scan_id", "analytics_version", "snapshot_schema_version"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "idx_daily_snapshot_publication_lookup",
        table_name="daily_snapshot_publications",
    )
    op.drop_table("daily_snapshot_publications")
    op.drop_index(
        "idx_daily_snapshot_publication_input_lookup",
        table_name="daily_snapshot_publication_inputs",
    )
    op.drop_table("daily_snapshot_publication_inputs")
