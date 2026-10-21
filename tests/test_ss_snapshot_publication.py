from __future__ import annotations

import json
import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

MODULE_PATH = (
    Path(__file__).parents[1]
    / "docker"
    / "stock-screener"
    / "daily_snapshot_publication.py"
)
SPEC = spec_from_file_location("daily_snapshot_publication", MODULE_PATH)
assert SPEC and SPEC.loader
publication = module_from_spec(SPEC)
SPEC.loader.exec_module(publication)


def _payload_json(*, market: str = "US", scan_id: str = "scan-us") -> str:
    return json.dumps(
        {
            "schema_version": 5,
            "market": market,
            "scan_id": scan_id,
            "freshness": {
                "scan_id": scan_id,
                "analytics_version": "quantstack.scan-analytics.v1",
            },
        },
        separators=(",", ":"),
    )


class _Query:
    def __init__(self, record):
        self.record = record

    def filter_by(self, **_kwargs):
        return self

    def one_or_none(self):
        return self.record


class _Session:
    def __init__(self, record):
        self.record = record

    def query(self, _model):
        return _Query(self.record)


class _PlanStatement:
    def __init__(self, market: str):
        self.market = market

    def limit(self, value: int):
        assert value == 0
        return (self.market, value)


class _PlanSession:
    def __init__(self):
        self.executed: list[tuple[str, int]] = []

    def execute(self, statement: tuple[str, int]) -> None:
        self.executed.append(statement)


def _install_read_modules(monkeypatch: pytest.MonkeyPatch) -> None:
    modules = {
        "app": ModuleType("app"),
        "app.models": ModuleType("app.models"),
        "app.models.daily_snapshot_publication": ModuleType(
            "app.models.daily_snapshot_publication"
        ),
        "app.services": ModuleType("app.services"),
        "app.services.daily_snapshot_service": ModuleType(
            "app.services.daily_snapshot_service"
        ),
    }
    modules["app.models.daily_snapshot_publication"].DailySnapshotPublication = object
    modules["app.services"].daily_snapshot_service = modules[
        "app.services.daily_snapshot_service"
    ]
    modules["app.services.daily_snapshot_service"].DAILY_SNAPSHOT_SCHEMA_VERSION = 5
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)


def test_query_plan_warmup_compiles_both_markets_without_reading_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _PlanSession()

    def build_statement(*, market: str, analytics_version: str) -> _PlanStatement:
        assert analytics_version == "quantstack.scan-analytics.v1"
        return _PlanStatement(market)

    monkeypatch.setattr(publication, "_latest_snapshot_publication_statement", build_statement)

    publication.warm_latest_snapshot_publication_query_plan(
        session,
        analytics_version="quantstack.scan-analytics.v1",
    )

    assert session.executed == [("CN", 0), ("US", 0)]


def test_durable_publication_load_is_independent_of_redis_and_memory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_read_modules(monkeypatch)
    payload_json = _payload_json()
    record = SimpleNamespace(
        payload_json=payload_json,
        payload_sha256=publication._payload_digest(payload_json),
    )
    scan = SimpleNamespace(
        scan_id="scan-us",
        analytics_version="quantstack.scan-analytics.v1",
    )

    loaded = publication.load_snapshot_publication_json(
        _Session(record),
        scan=scan,
        market="US",
        analytics_version="quantstack.scan-analytics.v1",
    )

    assert loaded == payload_json


@pytest.mark.parametrize(
    ("record", "message"),
    [
        (None, "unavailable"),
        (
            SimpleNamespace(payload_json=_payload_json(), payload_sha256="wrong"),
            "digest does not match",
        ),
        (
            SimpleNamespace(
                payload_json=_payload_json(market="CN"),
                payload_sha256=publication._payload_digest(_payload_json(market="CN")),
            ),
            "identity does not match",
        ),
    ],
)
def test_durable_publication_read_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    record: object,
    message: str,
) -> None:
    _install_read_modules(monkeypatch)
    scan = SimpleNamespace(
        scan_id="scan-us",
        analytics_version="quantstack.scan-analytics.v1",
    )

    with pytest.raises(publication.SnapshotPublicationUnavailable, match=message):
        publication.load_snapshot_publication_json(
            _Session(record),
            scan=scan,
            market="US",
            analytics_version="quantstack.scan-analytics.v1",
        )
