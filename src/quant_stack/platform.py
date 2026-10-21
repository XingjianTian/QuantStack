from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .models import load_dataset_manifest, load_strategy_spec, load_yaml
from .nautilus_runner import engine_info


class PlatformRepository:
    def __init__(
        self,
        root: str | Path | None = None,
        experiment_root: str | Path | None = None,
        data_root: str | Path | None = None,
    ) -> None:
        self.root = Path(root or os.environ.get("QUANT_STACK_ROOT") or Path.cwd()).resolve()
        self.experiment_root = Path(
            experiment_root
            or os.environ.get("QUANT_EXPERIMENT_ROOT")
            or self.root / "experiments"
        ).resolve()
        configured_data_root = data_root or os.environ.get("QUANT_DATA_ROOT")
        self.data_root = Path(configured_data_root).resolve() if configured_data_root else None

    def upstream_versions(self) -> dict[str, str]:
        lock_path = self.root / "VERSION_LOCK.json"
        if not lock_path.is_file():
            return {}
        payload = json.loads(lock_path.read_text(encoding="utf-8"))
        return {
            name: details.get("commit", "unknown")
            for name, details in payload.get("upstreams", {}).items()
        }

    def strategies(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for path in sorted((self.root / "strategies").glob("**/strategy_spec.yaml")):
            spec = load_strategy_spec(path)
            records.append(
                {
                    "strategy_id": spec.strategy_id,
                    "name": spec.name,
                    "version": spec.version,
                    "status": spec.status.value,
                    "market": spec.market_scope.regions,
                    "asset_classes": spec.market_scope.asset_classes,
                    "instruments": spec.market_scope.instruments,
                    "engine": spec.backtest.engine,
                    "path": str(path.relative_to(self.root)),
                }
            )
        return records

    def experiments(self, limit: int = 20) -> list[dict[str, Any]]:
        if not self.experiment_root.exists():
            return []
        records: list[dict[str, Any]] = []
        for path in sorted(self.experiment_root.glob("*/experiment_manifest.yaml"), reverse=True):
            try:
                manifest = load_yaml(path)
                result_path = path.parent / str(manifest.get("result_file", "backtest_result.json"))
                result = (
                    json.loads(result_path.read_text(encoding="utf-8"))
                    if result_path.is_file()
                    else {}
                )
                records.append(
                    {
                        "experiment_id": manifest.get("experiment_id", path.parent.name),
                        "created_at": manifest.get("created_at"),
                        "strategy_id": manifest.get("strategy_id"),
                        "strategy_version": manifest.get("strategy_version"),
                        "status": manifest.get("status", "unknown"),
                        "engine": result.get("engine"),
                        "metrics": result.get("metrics", {}),
                        "equity_curve": result.get("equity_curve", []),
                        "warnings": result.get("warnings", []),
                    }
                )
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                continue
            if len(records) >= limit:
                break
        return records

    def datasets(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for path in sorted((self.root / "datasets/catalog").glob("**/*.yaml")):
            try:
                manifest = load_dataset_manifest(path)
            except (OSError, ValueError, TypeError):
                continue
            records.append(
                {
                    "dataset_id": manifest.dataset_id,
                    "version": manifest.version,
                    "kind": manifest.kind,
                    "market": manifest.market,
                    "provider": manifest.provider,
                    "frequency": manifest.frequency,
                    "format": manifest.format,
                    "storage_uri": manifest.storage_uri,
                    "fetched_at": manifest.fetched_at.isoformat(),
                    "row_count": manifest.row_count,
                    "quality_status": manifest.quality_status,
                    "revision_policy": manifest.revision_policy,
                    "path": str(path.relative_to(self.root)),
                }
            )
        return records

    def overview(self) -> dict[str, Any]:
        engine = engine_info()
        strategies = self.strategies()
        experiments = self.experiments(limit=5)
        return {
            "platform": "Personal Quant Platform",
            "mode": "research_only",
            "live_trading_enabled": False,
            "engine": {
                "name": "NautilusTrader",
                "available": engine.available,
                "version": engine.version,
                "error": engine.error,
            },
            "strategy_count": len(strategies),
            "experiment_count": len(self.experiments(limit=10_000)),
            "data_root": {
                "configured": self.data_root is not None,
                "available": bool(self.data_root and self.data_root.is_dir()),
            },
            "latest_experiment": experiments[0] if experiments else None,
            "upstream_versions": self.upstream_versions(),
            "jupyter_url": os.environ.get("QUANT_JUPYTER_URL"),
        }
