"""Index the JSON fields used by the Daily Snapshot candidate queries.

The Daily Snapshot endpoint filters every feature row by local-currency
average dollar volume and filters its leaders by industry-group rank.  Both
values live in ``details_json``.  Without matching expression indexes,
PostgreSQL must detoast and inspect every feature payload on each cache miss.

These expressions intentionally match ``feature_store_query.json_number()``.
The indexes cover existing rows when the migration runs and future rows on
upsert; no data rewrite or deletion is involved.
"""

from __future__ import annotations

from alembic import op

revision = "20260826_0025"
down_revision = "20260701_0024"
branch_labels = None
depends_on = None

_FIELDS = (
    "avg_dollar_volume",
    "ibd_group_rank",
)


def _index_name(field: str) -> str:
    return f"ix_sfd_run_{field}"


def _index_expr(field: str) -> str:
    return f"CAST(details_json ->> '{field}' AS FLOAT)"


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    for field in _FIELDS:
        op.execute(
            f"CREATE INDEX IF NOT EXISTS {_index_name(field)} "
            f"ON stock_feature_daily (run_id, ({_index_expr(field)}))"
        )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return
    for field in _FIELDS:
        op.execute(f"DROP INDEX IF EXISTS {_index_name(field)}")
