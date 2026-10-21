"""Exercise the local workbench through real fixture sync and proposal services."""

import socket
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from quant_stack.brokerage_sample_api import create_app


def client_for(root: Path, **kwargs):
    return TestClient(
        create_app(root),
        base_url="http://127.0.0.1:8917",
        client=("127.0.0.1", 50000),
        **kwargs,
    )


@pytest.fixture
def client(tmp_path, monkeypatch):
    def deny_network(*args, **kwargs):
        raise AssertionError("sample workbench must never open a network socket")

    monkeypatch.setattr(socket, "create_connection", deny_network)
    monkeypatch.setattr(socket.socket, "connect", deny_network)
    with client_for(tmp_path / "runtime") as value:
        yield value


def test_explicit_sync_propose_and_persisted_idempotent_budget(client, tmp_path):
    initial = client.get("/v1/brokerage-sample/state").json()
    assert initial["snapshot"] is None
    assert initial["proposal"] is None
    assert initial["operations"]["sync_count"] == 0
    assert client.post("/v1/brokerage-sample/propose", json={"profile": "ready"}).status_code == 409
    synced = client.post("/v1/brokerage-sample/sync", json={"profile": "ready"}).json()
    assert synced["snapshot"]["complete"] is True
    assert synced["snapshot"]["positions"][0]["cost_basis"] is not None
    assert len(synced["snapshot"]["transactions"]) == 2
    assert synced["proposal"] is None
    assert synced["operations"]["sync_count"] == 1
    first = client.post("/v1/brokerage-sample/propose", json={"profile": "ready"}).json()
    assert [item["quantity"] for item in first["proposal"]["items"]] == ["5", "3"]
    assert first["funding"]["proposal_cash_use"] == "302.80"
    assert first["funding"]["reservation_included"] is True
    assert first["calculations"][0]["budget_before"] == "200.00"
    assert first["calculations"][0]["budget_after_proposal"] == "23.50"
    repeated = client.post("/v1/brokerage-sample/propose", json={"profile": "ready"}).json()
    assert repeated["proposal"]["batch_id"] == first["proposal"]["batch_id"]
    assert repeated["ledger"]["ledger_version"] == first["ledger"]["ledger_version"]
    assert repeated["ledger"]["entries"] == first["ledger"]["entries"]
    assert repeated["operations"]["propose_count"] == 2
    assert repeated["operations"]["last_propose_at"]
    client.post("/v1/brokerage-sample/sync", json={"profile": "ready"})
    with client_for(tmp_path / "runtime") as restarted:
        saved = restarted.get("/v1/brokerage-sample/state").json()
        assert saved["operations"]["sync_count"] == 2
        assert saved["operations"]["propose_count"] == 2
        assert saved["ledger"]["ledger_version"] == first["ledger"]["ledger_version"]
        assert saved["proposal"]["batch_id"] == first["proposal"]["batch_id"]
    assert (tmp_path / "runtime" / "ready").stat().st_mode & 0o777 == 0o700
    assert (tmp_path / "runtime" / "ready" / "operations.json").stat().st_mode & 0o777 == 0o600


def test_profiles_isolated_blocked_and_stale_hide_proposals(client):
    client.post("/v1/brokerage-sample/sync", json={"profile": "ready"})
    client.post("/v1/brokerage-sample/propose", json={"profile": "ready"})
    stale = client.get("/v1/brokerage-sample/state?profile=ready&stale=true").json()
    assert stale["state"]["current_proposal_state"] == "stale"
    assert stale["proposal"] is None
    assert stale["calculations"] == []
    assert stale["operations"]["sync_count"] == 1
    assert client.get("/v1/brokerage-sample/state?profile=blocked").json()["snapshot"] is None
    client.post("/v1/brokerage-sample/sync", json={"profile": "blocked"})
    blocked = client.post("/v1/brokerage-sample/propose", json={"profile": "blocked"}).json()
    assert blocked["proposal"]["status"] == "blocked"
    assert all(item["quantity"] == "0" for item in blocked["proposal"]["items"])
    assert blocked["funding"]["cash_after_open_orders"] is None
    assert blocked["funding"]["blocking_reasons"]
    assert blocked["operations"]["sync_count"] == 1
    ready = client.get("/v1/brokerage-sample/state?profile=ready").json()
    assert ready["proposal"]["status"] != "blocked"


@pytest.mark.parametrize(
    "path,body",
    [
        ("sync", {"profile": "live"}),
        ("sync", {"profile": "ready", "account_hash": "credential"}),
        ("propose", {"profile": "ready", "at": "2026-10-16"}),
        ("sync", {"profile": "ready", "state_root": "/tmp/other"}),
        ("sync", {"profile": "ready", "url": "https://api.schwabapi.com"}),
    ],
)
def test_no_live_credentials_paths_or_clock_in_request(client, path, body):
    assert client.post(f"/v1/brokerage-sample/{path}", json=body).status_code == 422


def test_origin_host_client_and_unknown_query_guards(client, tmp_path):
    endpoint = "/v1/brokerage-sample/sync"
    assert (
        client.post(
            endpoint, json={"profile": "ready"}, headers={"Origin": "https://evil.example"}
        ).status_code
        == 403
    )
    assert (
        client.post(endpoint, json={"profile": "ready"}, headers={"Origin": "null"}).status_code
        == 403
    )
    assert (
        client.get("/v1/brokerage-sample/state", headers={"Host": "example.com"}).status_code == 403
    )
    assert client.get("/v1/brokerage-sample/state?url=https://example.com").status_code == 422
    assert client.post(endpoint + "?stale=true", json={"profile": "ready"}).status_code == 422
    assert (
        client.post(
            endpoint, json={"profile": "ready"}, headers={"Origin": "http://127.0.0.1:5187"}
        ).status_code
        == 200
    )
    with TestClient(
        create_app(tmp_path / "remote"),
        base_url="http://127.0.0.1:8917",
        client=("192.0.2.4", 1200),
    ) as remote:
        assert remote.get("/v1/brokerage-sample/state").status_code == 403


def test_internal_error_redacts_runtime_path_and_retains_prior_state(client, monkeypatch):
    client.post("/v1/brokerage-sample/sync", json={"profile": "ready"})
    client.post("/v1/brokerage-sample/propose", json={"profile": "ready"})
    from quant_stack import brokerage_sample_api

    def fail(*args, **kwargs):
        raise RuntimeError("/private/path/credential.json")

    monkeypatch.setattr(brokerage_sample_api.cli, "command_sync", fail)
    response = client.post("/v1/brokerage-sample/sync", json={"profile": "ready"})
    assert response.status_code == 500
    assert "private" not in response.text
    retained = client.get("/v1/brokerage-sample/state").json()
    assert retained["proposal"] is not None
    assert retained["operations"]["sync_count"] == 1


def test_deployed_origin_is_explicit_and_does_not_enable_remote_clients(tmp_path, monkeypatch):
    monkeypatch.setenv("QUANT_SAMPLE_SITE_ORIGIN", "https://quant.example.com:11443")
    with client_for(tmp_path / "runtime") as client:
        allowed = {"Origin": "https://quant.example.com:11443"}
        assert (
            client.post(
                "/v1/brokerage-sample/sync", json={"profile": "ready"}, headers=allowed
            ).status_code
            == 200
        )
        assert (
            client.post(
                "/v1/brokerage-sample/sync",
                json={"profile": "ready"},
                headers={"Origin": "https://other.example"},
            ).status_code
            == 403
        )
    with TestClient(
        create_app(tmp_path / "runtime"),
        base_url="http://127.0.0.1:8917",
        client=("192.0.2.4", 50000),
    ) as remote:
        assert remote.get("/v1/brokerage-sample/state", headers=allowed).status_code == 403
