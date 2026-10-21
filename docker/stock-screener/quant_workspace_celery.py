"""Celery entrypoint with the QuantStack primary read overlay installed."""

from __future__ import annotations

# Import the compatibility module before Celery loads scan tasks.  Its startup
# hooks replace PriceCacheService/YFinance reads with the exact pinned qfq
# release in quantstack_primary mode.
import quant_workspace_compat as _quant_workspace_compat  # noqa: F401
from app.celery_app import _ensure_worker_runtime_services, celery_app

# The upstream worker signals also refresh this container after a prefork, but
# the QuantStack entrypoint must be safe in every Celery execution pool and in
# direct task integration tests. The initializer is idempotent and the prefork
# hook still forces a child-process rebuild.
_runtime_services = _ensure_worker_runtime_services()

__all__ = ["celery_app"]
