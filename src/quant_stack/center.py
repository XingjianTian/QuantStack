from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class CenterModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class NavigationConfig(CenterModel):
    mode: Literal["services", "custom"] = "services"
    item_ids: list[str] = Field(default_factory=list, max_length=64)

    @field_validator("item_ids")
    @classmethod
    def item_ids_are_unique(cls, value: list[str]) -> list[str]:
        if len(value) != len(set(value)):
            raise ValueError("Navigation item IDs must be unique")
        return value


class CenterRepository:
    default_item_ids = [
        "center-overview",
        "screener-daily",
        "screener-scan",
        "vibe-agent",
        "nautilus-lab",
        "center-strategies",
        "center-experiments",
        "center-data",
        "center-navigation",
    ]

    def __init__(self, root: Path, data_root: Path | None) -> None:
        self.root = root
        self.data_root = data_root
        self.catalog_path = self.root / "configs/quant_center_services.json"

    @property
    def navigation_path(self) -> Path | None:
        if self.data_root is None:
            return None
        return self.data_root / "config/quant-center/navigation.json"

    def catalog(self) -> dict:
        payload = json.loads(self.catalog_path.read_text(encoding="utf-8"))
        replacements = {
            "{vibe_url}": os.environ.get(
                "QUANT_CENTER_VIBE_URL", "https://vibe-trading.example.com:11443"
            ).rstrip("/"),
            "{screener_url}": os.environ.get(
                "QUANT_CENTER_SCREENER_URL", "https://stock-screener.example.com:11443"
            ).rstrip("/"),
            "{nautilus_url}": os.environ.get(
                "QUANT_CENTER_NAUTILUS_URL", "https://nautilus.example.com:11443"
            ).rstrip("/"),
        }
        for item in payload["items"]:
            for marker, replacement in replacements.items():
                item["href"] = item["href"].replace(marker, replacement)
        return payload

    def navigation(self) -> NavigationConfig:
        path = self.navigation_path
        if path is None or not path.is_file():
            return NavigationConfig(item_ids=self.default_item_ids)
        try:
            config = NavigationConfig.model_validate_json(path.read_text(encoding="utf-8"))
            known_ids = {item["id"] for item in self.catalog()["items"]}
            item_ids = [item_id for item_id in config.item_ids if item_id in known_ids]
            return config.model_copy(
                update={"item_ids": item_ids}
            )
        except (OSError, ValueError):
            return NavigationConfig(item_ids=self.default_item_ids)

    def save_navigation(self, config: NavigationConfig) -> NavigationConfig:
        known_ids = {item["id"] for item in self.catalog()["items"]}
        unknown = sorted(set(config.item_ids) - known_ids)
        if unknown:
            raise ValueError(f"Unknown navigation item IDs: {', '.join(unknown)}")
        path = self.navigation_path
        if path is None:
            raise RuntimeError("QUANT_DATA_ROOT is required to persist navigation")
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        temporary.write_text(
            config.model_dump_json(indent=2) + "\n",
            encoding="utf-8",
        )
        os.chmod(temporary, 0o600)
        temporary.replace(path)
        return config

    def service_statuses(self, engine_available: bool) -> list[dict]:
        targets = [
            (
                "quant-center",
                "Quant Center",
                os.environ.get(
                    "QUANT_CENTER_WEB_HEALTH_URL", "http://center-web:8080/healthz"
                ),
            ),
            (
                "vibe-trading",
                "Vibe-Trading",
                os.environ.get(
                    "QUANT_CENTER_VIBE_HEALTH_URL", "http://vibe-web:8899/live"
                ),
            ),
            (
                "stock-screener",
                "Stock Screener",
                os.environ.get(
                    "QUANT_CENTER_SCREENER_HEALTH_URL", "http://frontend/"
                ),
            ),
            (
                "nautilus-trader",
                "Nautilus 研究台",
                os.environ.get(
                    "QUANT_CENTER_NAUTILUS_HEALTH_URL",
                    "http://nautilus-lab-web:8080/healthz",
                ),
            ),
        ]
        with ThreadPoolExecutor(max_workers=len(targets)) as pool:
            records = list(pool.map(self._probe_service, targets))
        for record in records:
            if record["id"] == "nautilus-trader":
                if not engine_available and record["state"] == "online":
                    record["state"] = "degraded"
                record["detail"] = (
                    "Web 在线，回测引擎已加载"
                    if engine_available and record["state"] == "online"
                    else "Nautilus Web 或回测引擎不可用"
                )
        return records

    @staticmethod
    def _probe_service(target: tuple[str, str, str]) -> dict:
        source, label, url = target
        started = time.perf_counter()
        state = "offline"
        detail = "连接失败"
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "quant-center/0.4"})
            with urllib.request.urlopen(request, timeout=2) as response:
                state = "online" if response.status < 500 else "degraded"
                detail = f"HTTP {response.status}"
        except urllib.error.HTTPError as exc:
            state = "online" if exc.code < 500 else "degraded"
            detail = f"HTTP {exc.code}"
        except (OSError, ValueError, urllib.error.URLError):
            pass
        return {
            "id": source,
            "label": label,
            "source": source,
            "state": state,
            "latency_ms": round((time.perf_counter() - started) * 1000),
            "checked_at": datetime.now(UTC).isoformat(),
            "detail": detail,
        }
