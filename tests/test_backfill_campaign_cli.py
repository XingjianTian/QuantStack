from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from quant_stack.data.backfill import RuntimeProvenanceError, RuntimeProvenanceReport
from scripts import run_cn_backfill_campaign as campaign_cli


class _FakeResult:
    def model_dump(self, **_kwargs):
        return {}

    def model_dump_json(self, **_kwargs):
        return "{}"


@pytest.mark.parametrize(
    ("command", "expected_command", "expected_kwargs"),
    [
        ("wave", "run_campaign_wave", {"shard_id": "shard-002"}),
        ("requeue-failures", "requeue_campaign_failures", {"shard_id": "shard-002"}),
    ],
)
def test_cli_passes_explicit_shard_selector_to_campaign_api(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    command: str,
    expected_command: str,
    expected_kwargs: dict[str, str],
) -> None:
    captured: dict[str, object] = {}
    args = SimpleNamespace(
        command=command,
        universe=tmp_path / "universe.json",
        plan=tmp_path / "plan.json",
        data_root=tmp_path,
        max_attempts=3,
        backoff_seconds=1,
        progress_output=tmp_path / "progress.json",
        max_shards=1,
        max_batches=144,
        max_requests=49_000,
        shard_id="shard-002",
        error_class=["ConnectionError"],
        additional_attempts=3,
        source_commit="a" * 40,
        provenance_output=tmp_path / "provenance.json",
        repo_root=tmp_path,
        environment_id="test-worker",
    )

    monkeypatch.setattr(
        campaign_cli,
        "_parser",
        lambda: SimpleNamespace(parse_args=lambda: args),
    )
    monkeypatch.setattr(campaign_cli, "_load", lambda _args: (object(), object()))
    monkeypatch.setattr(campaign_cli, "_runner", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(campaign_cli, "_preflight", lambda _args: None)
    monkeypatch.setattr(campaign_cli, "_write_json", lambda *_args, **_kwargs: None)

    def fake_api(*_args, **kwargs):
        captured.update(kwargs)
        return _FakeResult()

    monkeypatch.setattr(campaign_cli, expected_command, fake_api)

    campaign_cli.main()

    for key, value in expected_kwargs.items():
        assert captured[key] == value


def test_cli_provenance_failure_is_recorded_before_loading_campaign(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = RuntimeProvenanceReport(
        schema_version="quantstack.runtime-provenance.v1",
        checked_at="2026-08-07T00:00:00+00:00",
        environment_id="test-worker",
        python_version="3.11.9",
        supported_python=">=3.12,<3.15",
        declared_source_commit="a" * 40,
        actual_checkout_commit="a" * 40,
        source_tree_clean=True,
        result="fail",
        runtime_source_paths=(),
        runtime_source_paths_tracked=False,
        failure_reasons=("unsupported Python runtime 3.11.9",),
    )
    args = SimpleNamespace(
        command="wave",
        source_commit="a" * 40,
        repo_root=tmp_path,
        environment_id="test-worker",
        provenance_output=tmp_path / "provenance.json",
    )
    writes: list[dict[str, object]] = []

    def fail_preflight(**_kwargs):
        raise RuntimeProvenanceError(report)

    monkeypatch.setattr(campaign_cli, "run_runtime_provenance_preflight", fail_preflight)
    monkeypatch.setattr(
        campaign_cli,
        "_write_json",
        lambda _path, payload: writes.append(payload),
    )

    with pytest.raises(SystemExit, match="runtime provenance preflight failed"):
        campaign_cli._preflight(args)

    assert writes == [report.model_dump()]
