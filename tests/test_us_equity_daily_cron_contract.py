from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).parents[1]


def test_us_daily_cron_requires_canonical_publication_permissions() -> None:
    source = (ROOT / "deploy/cron/quantstack-us-equity-daily.sh").read_text(
        encoding="utf-8"
    )

    for name in (
        "QUANT_DATA_SHARED_GID",
        "QUANT_DATA_PERMISSION_ROOT",
        "QUANT_DATA_FILE_MODE",
        "QUANT_DATA_DIRECTORY_MODE",
    ):
        assert f'${{{name}:?missing {name}}}' in source
