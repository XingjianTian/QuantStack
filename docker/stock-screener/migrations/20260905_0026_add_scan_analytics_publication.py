"""Record when a scan's local analytics are safe to publish.

The scan task marks its base result complete before the QuantStack overlay
derives dollar volume, RS, breadth, exposure, and group ranks.  Daily Snapshot
must therefore distinguish a base-complete scan from one whose local analytics
transaction completed under the currently required semantics.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260905_0026"
down_revision = "20260826_0025"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("scans", sa.Column("analytics_version", sa.String(64), nullable=True))
    op.add_column(
        "scans",
        sa.Column("analytics_completed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "idx_scans_market_analytics_ready",
        "scans",
        ["universe_market", "status", "analytics_version", "completed_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_scans_market_analytics_ready", table_name="scans")
    op.drop_column("scans", "analytics_completed_at")
    op.drop_column("scans", "analytics_version")

