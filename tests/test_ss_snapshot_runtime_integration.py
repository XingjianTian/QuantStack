r"""Fresh-process integration against the locked Stock Screener runtime.

The default QuantStack development environment intentionally does not install
the upstream service's SQLAlchemy/Celery stack. In that environment these two
tests report an explicit dependency skip. Run the required runtime verification
without a skip using::

    uv run --with-requirements \
      ../upstream/stock-screener/backend/requirements-runtime.txt \
      pytest -q tests/test_ss_snapshot_runtime_integration.py

When the upstream modules are present, every runtime/bootstrap failure remains
a normal test failure.
"""

from __future__ import annotations

import importlib.util
import json
import os
import runpy
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
UPSTREAM = ROOT.parent / "upstream" / "stock-screener" / "backend"
OVERLAY = ROOT / "docker" / "stock-screener"
UPSTREAM_RUNTIME_MODULES = (
    "alembic",
    "celery",
    "pydantic_settings",
    "redis",
    "sqlalchemy",
)
MISSING_UPSTREAM_RUNTIME_MODULES = tuple(
    name for name in UPSTREAM_RUNTIME_MODULES if importlib.util.find_spec(name) is None
)

pytestmark = pytest.mark.skipif(
    bool(MISSING_UPSTREAM_RUNTIME_MODULES),
    reason=(
        "locked Stock Screener runtime dependencies are not installed: "
        + ", ".join(MISSING_UPSTREAM_RUNTIME_MODULES)
    ),
)


def _materialize_runtime(tmp_path: Path) -> Path:
    target = tmp_path / "backend"
    shutil.copytree(UPSTREAM, target)
    patcher = runpy.run_path(
        str(OVERLAY / "patches" / "patch_snapshot_analytics_readiness.py")
    )["main"]
    patcher(target)
    shutil.copy2(
        OVERLAY / "daily_snapshot_publication_model.py",
        target / "app" / "models" / "daily_snapshot_publication.py",
    )
    for name in (
        "daily_snapshot_publication.py",
        "quantstack_consumer_adapter.py",
        "quantstack_scan_analytics.py",
        "quant_workspace_celery.py",
        "quant_workspace_compat.py",
        "reconcile_scan_analytics.py",
    ):
        shutil.copy2(OVERLAY / name, target / name)
    return target


@pytest.mark.parametrize("market", ["CN", "US"])
def test_real_market_runtime_publishes_atomically_and_fails_closed(
    tmp_path: Path, market: str
) -> None:
    target = _materialize_runtime(tmp_path)
    database = tmp_path / f"{market.lower()}-snapshot.sqlite"
    env = {
        **os.environ,
        "DATABASE_URL": f"sqlite:///{database}",
        "STOCKSCANNER_TEST_ALLOW_SQLITE": "1",
        "SS_QUANTSTACK_READ_MODE": "quantstack_primary",
        "SS_QUANTSTACK_BASE_URL": "http://127.0.0.1:9",
        "SS_QUANTSTACK_API_KEY": "isolated-runtime-probe",
        "SS_QUANTSTACK_BARS_DATASET_ID": "cn-runtime-probe",
        "SS_QUANTSTACK_US_BARS_DATASET_ID": "us-runtime-probe",
        "SS_QUANTSTACK_US_INSTRUMENTS_DATASET_ID": "us-instruments-probe",
        "SS_REQUIRED_SCAN_ANALYTICS_VERSION": "quantstack.scan-analytics.v1",
        "PYTHONPATH": str(target),
    }
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "tests" / "fixtures" / "ss_snapshot_runtime_probe.py"),
            market,
        ],
        cwd=target,
        env=env,
        text=True,
        capture_output=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout.strip().splitlines()[-1])
    assert report == {
        "failed_closed": True,
        "general_worker_runtime": True,
        "market": market,
        "published": "published",
        "repeat": "current",
    }
