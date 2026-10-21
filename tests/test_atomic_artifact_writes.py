from pathlib import Path

import pytest

import quant_stack.data.storage.atomic as atomic_module
import quant_stack.data.storage.local as local_module
from quant_stack.data.storage import StagingFileSystemStorage


def test_raw_write_is_atomic_under_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    storage = StagingFileSystemStorage(tmp_path)
    final = tmp_path / "raw/test.json"
    real_fdopen = atomic_module.os.fdopen
    real_replace = atomic_module.os.replace

    class InterruptedStream:
        def __init__(self, stream) -> None:
            self.stream = stream

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return self.stream.__exit__(*args)

        def write(self, payload: bytes) -> None:
            self.stream.write(payload[:4])
            raise OSError("interrupted raw write")

        def flush(self) -> None:
            self.stream.flush()

        def fileno(self) -> int:
            return self.stream.fileno()

    def interrupt_fdopen(descriptor: int, mode: str):
        return InterruptedStream(real_fdopen(descriptor, mode))

    monkeypatch.setattr(atomic_module.os, "fdopen", interrupt_fdopen)
    with pytest.raises(OSError, match="interrupted raw write"):
        storage.write_raw_object("raw/test.json", b'{"complete":true}\n')
    assert not final.exists()
    assert not list(final.parent.glob(".test.json.*.tmp"))
    monkeypatch.setattr(atomic_module.os, "fdopen", real_fdopen)

    def fail_before_replace(source, destination) -> None:
        if Path(destination) == final:
            raise OSError("fault before raw replace")
        real_replace(source, destination)

    monkeypatch.setattr(atomic_module.os, "replace", fail_before_replace)
    with pytest.raises(OSError, match="raw replace"):
        storage.write_raw_object("raw/test.json", b'{"complete":true}\n')
    assert not final.exists()
    assert not list(final.parent.glob(".test.json.*.tmp"))

    stale = final.parent / ".test.json.stale.tmp"
    stale.write_bytes(b"partial")
    monkeypatch.setattr(atomic_module.os, "replace", real_replace)
    artifact = storage.write_raw_object("raw/test.json", b'{"complete":true}\n')
    assert artifact.path.read_bytes() == b'{"complete":true}\n'
    assert stale.read_bytes() == b"partial"
    storage.write_raw_object("raw/test.json", b'{"complete":true}\n')
    with pytest.raises(ValueError, match="conflict"):
        storage.write_raw_object("raw/test.json", b'{"complete":false}\n')


def test_parquet_write_is_atomic_under_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    storage = StagingFileSystemStorage(tmp_path)
    final = tmp_path / "normalized/bars.parquet"
    records = [{"instrument_id": "XSHG.600000", "close": 10.5}]
    real_write_table = local_module.pq.write_table
    real_replace = atomic_module.os.replace

    def interrupt_parquet_write(*args, **kwargs) -> None:
        real_write_table(*args, **kwargs)
        raise OSError("interrupted parquet write")

    monkeypatch.setattr(local_module.pq, "write_table", interrupt_parquet_write)
    with pytest.raises(OSError, match="interrupted parquet write"):
        storage.write_parquet_dataset("normalized/bars.parquet", records)
    assert not final.exists()
    assert not list(final.parent.glob(".bars.parquet.*.tmp"))
    monkeypatch.setattr(local_module.pq, "write_table", real_write_table)

    def fail_before_replace(source, destination) -> None:
        if Path(destination) == final:
            raise OSError("fault before parquet replace")
        real_replace(source, destination)

    monkeypatch.setattr(atomic_module.os, "replace", fail_before_replace)
    with pytest.raises(OSError, match="parquet replace"):
        storage.write_parquet_dataset("normalized/bars.parquet", records)
    assert not final.exists()
    assert not list(final.parent.glob(".bars.parquet.*.tmp"))

    stale = final.parent / ".bars.parquet.stale.tmp"
    stale.write_bytes(b"not parquet")
    monkeypatch.setattr(atomic_module.os, "replace", real_replace)
    artifact = storage.write_parquet_dataset("normalized/bars.parquet", records)
    assert artifact.path.is_file()
    assert storage.read_dataset(artifact.uri) == records
    storage.write_parquet_dataset("normalized/bars.parquet", records)
    with pytest.raises(ValueError, match="conflict"):
        storage.write_parquet_dataset(
            "normalized/bars.parquet",
            [{"instrument_id": "XSHG.600000", "close": 11.0}],
        )
