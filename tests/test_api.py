from pathlib import Path

from fastapi.testclient import TestClient

from quant_stack.api import create_app
from quant_stack.center import CenterRepository
from quant_stack.platform import PlatformRepository

ROOT = Path(__file__).parents[1]


def test_health_and_research_only_overview(tmp_path: Path) -> None:
    client = TestClient(create_app(PlatformRepository(ROOT, tmp_path, tmp_path)))
    assert client.get("/health").json()["status"] == "ok"
    response = client.get("/v1/overview")
    assert response.status_code == 200
    payload = response.json()
    assert payload["mode"] == "research_only"
    assert payload["live_trading_enabled"] is False
    assert payload["engine"]["available"] is True


def test_api_key_is_enforced(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("QUANT_API_KEY", "test-secret")
    client = TestClient(create_app(PlatformRepository(ROOT, tmp_path, tmp_path)))
    assert client.get("/v1/strategies").status_code == 401
    response = client.get(
        "/v1/strategies",
        headers={"X-Quant-API-Key": "test-secret"},
    )
    assert response.status_code == 200


def test_quant_center_bootstrap_and_navigation_persist_on_data_root(tmp_path: Path) -> None:
    client = TestClient(create_app(PlatformRepository(ROOT, tmp_path / "experiments", tmp_path)))

    bootstrap = client.get("/v1/center/bootstrap")
    assert bootstrap.status_code == 200
    payload = bootstrap.json()
    assert payload["navigation"]["mode"] == "services"
    assert payload["persistence"] == {"configured": True, "available": True}
    assert {group["source"] for group in payload["catalog"]["groups"]} >= {
        "quant-center",
        "stock-screener",
        "vibe-trading",
        "nautilus-trader",
    }
    assert "center-research" not in {item["id"] for item in payload["catalog"]["items"]}
    assert all(item["href"] != "/research" for item in payload["catalog"]["items"])
    expected = {
        "mode": "custom",
        "item_ids": ["center-overview", "screener-scan", "vibe-agent"],
    }
    response = client.put("/v1/center/navigation", json=expected)
    assert response.status_code == 200
    assert response.json() == expected
    assert client.get("/v1/center/bootstrap").json()["navigation"] == expected
    stored = tmp_path / "config/quant-center/navigation.json"
    assert stored.is_file()
    assert stored.stat().st_mode & 0o777 == 0o600


def test_quant_center_rejects_unknown_navigation_item(tmp_path: Path) -> None:
    client = TestClient(create_app(PlatformRepository(ROOT, tmp_path / "experiments", tmp_path)))
    response = client.put(
        "/v1/center/navigation",
        json={"mode": "custom", "item_ids": ["not-a-real-page"]},
    )
    assert response.status_code == 422
    assert "Unknown navigation item IDs" in response.json()["detail"]


def test_quant_center_discards_removed_items_from_saved_navigation(tmp_path: Path) -> None:
    navigation_path = tmp_path / "config/quant-center/navigation.json"
    navigation_path.parent.mkdir(parents=True)
    navigation_path.write_text(
        '{"mode":"custom","item_ids":["center-overview","center-research","vibe-agent"]}',
        encoding="utf-8",
    )
    client = TestClient(create_app(PlatformRepository(ROOT, tmp_path / "experiments", tmp_path)))

    navigation = client.get("/v1/center/bootstrap").json()["navigation"]

    assert navigation == {
        "mode": "custom",
        "item_ids": ["center-overview", "vibe-agent"],
    }


def test_dataset_catalog_is_exposed_as_metadata_only(tmp_path: Path) -> None:
    client = TestClient(create_app(PlatformRepository(ROOT, tmp_path, tmp_path)))
    response = client.get("/v1/datasets")
    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["dataset_id"] == "us_equity_daily_sample"
    assert payload[0]["storage_uri"].startswith("quant-data://")
    assert "source_url" not in payload[0]


def test_service_status_endpoint_uses_sanitized_probe_results(
    monkeypatch, tmp_path: Path
) -> None:
    expected = [
        {
            "id": "quant-center",
            "label": "Quant Center",
            "source": "quant-center",
            "state": "online",
            "latency_ms": 4,
            "checked_at": "2026-07-28T00:00:00+00:00",
            "detail": "HTTP 200",
        }
    ]
    monkeypatch.setattr(
        CenterRepository,
        "service_statuses",
        lambda self, engine_available: expected,
    )
    client = TestClient(create_app(PlatformRepository(ROOT, tmp_path, tmp_path)))
    response = client.get("/v1/center/services")
    assert response.status_code == 200
    assert response.json() == expected
    assert "url" not in response.text
