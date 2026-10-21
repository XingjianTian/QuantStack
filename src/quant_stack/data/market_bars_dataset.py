from __future__ import annotations

from pathlib import Path

from .cn_qfq_dataset import cn_qfq_relation_sql
from .us_total_return_dataset import us_total_return_relation_sql


def market_bars_relation_sql(
    path: Path, *, instrument_ids: tuple[str, ...] | None = None
) -> str:
    cn_relation = cn_qfq_relation_sql(path, instrument_ids=instrument_ids)
    if cn_relation is not None:
        return cn_relation
    return us_total_return_relation_sql(path, instrument_ids=instrument_ids)
