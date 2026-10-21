import hashlib
from collections.abc import Sequence
from pathlib import Path

import pytest

from quant_stack.data.providers.cn.reliability import (
    EmptyProviderResponseError,
    ProviderProbeFailure,
    classify_probe_failure,
    require_non_empty,
)
from quant_stack.data.quality.provider_schema import (
    ProviderSchemaDriftError,
    ProviderSchemaValidationReport,
)
from scripts import validate_cn_provider_reliability as reliability_script

SCHEMA_DRIFT = ProviderSchemaDriftError(
    ProviderSchemaValidationReport(
        provider="controlled",
        checked_rows=1,
        required_fields=["volume"],
        invalid_types=["row 0: provider field volume must be int, got str"],
    )
)


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (TimeoutError(), ProviderProbeFailure.TIMEOUT),
        (ConnectionError(), ProviderProbeFailure.CONNECTION_FAILURE),
        (EmptyProviderResponseError(), ProviderProbeFailure.EMPTY_RESPONSE),
        (SCHEMA_DRIFT, ProviderProbeFailure.SCHEMA_ANOMALY),
        (RuntimeError(), ProviderProbeFailure.PROVIDER_FAILURE),
    ],
)
def test_classify_probe_failure(exc: BaseException, expected: ProviderProbeFailure) -> None:
    assert classify_probe_failure(exc) is expected


def test_known_session_empty_response_is_not_success() -> None:
    with pytest.raises(EmptyProviderResponseError):
        require_non_empty([])
    records: Sequence[object] = ({"date": "2026-07-31"},)
    require_non_empty(records)


def _write_pinned_asl_sources(root: Path) -> Path:
    protocol = root / "src" / "ashare_lake" / "adapters" / "tdx_protocol"
    protocol.mkdir(parents=True)
    for name in reliability_script.PINNED_ASL_SOURCE_SHA256:
        (protocol / name).write_bytes(f"controlled {name}\n".encode())
    return root


def _trust_controlled_asl_sources(monkeypatch: pytest.MonkeyPatch, asl_root: Path) -> None:
    protocol = asl_root / "src" / "ashare_lake" / "adapters" / "tdx_protocol"
    monkeypatch.setattr(
        reliability_script,
        "PINNED_ASL_SOURCE_SHA256",
        {
            name: hashlib.sha256((protocol / name).read_bytes()).hexdigest()
            for name in reliability_script.PINNED_ASL_SOURCE_SHA256
        },
    )


def test_asl_reference_can_be_verified_by_pinned_source_hashes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    asl_root = _write_pinned_asl_sources(tmp_path / "asl")
    _trust_controlled_asl_sources(monkeypatch, asl_root)
    monkeypatch.setattr(
        reliability_script.subprocess,
        "run",
        lambda *args, **kwargs: (_ for _ in ()).throw(FileNotFoundError("git")),
    )

    result = reliability_script._verify_asl_checkout(asl_root)

    assert result["pinned_commit"] == reliability_script.PINNED_ASL_COMMIT
    assert result["observed_commit"] is None
    assert result["verification_mode"] == "pinned_source_sha256"


def test_asl_reference_rejects_source_hash_drift(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    asl_root = _write_pinned_asl_sources(tmp_path / "asl")
    _trust_controlled_asl_sources(monkeypatch, asl_root)
    quotes = asl_root / "src" / "ashare_lake" / "adapters" / "tdx_protocol" / "quotes.py"
    quotes.write_text("# drift\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="pinned file hashes"):
        reliability_script._verify_asl_checkout(asl_root)
