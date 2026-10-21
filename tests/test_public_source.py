from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
from pathlib import Path

import pandas as pd

from scripts.generate_public_replay_fixture import generate

ROOT = Path(__file__).parents[1]
FIXTURE = ROOT / "tests/fixtures/stock_screener/legacy_yfinance_cn_t110"


def test_public_index_excludes_private_configuration_and_history_evidence() -> None:
    files = subprocess.check_output(["git", "ls-files", "-z"], cwd=ROOT).decode().split("\0")
    for name in filter(None, files):
        path = Path(name)
        assert not set(path.parts).intersection({".venv", "node_modules", ".ai-bridge", "secrets"})
        assert path.name != ".env"
        assert ".private." not in path.name
        assert not path.name.endswith((".local.json", ".pem", ".key", ".sqlite", ".dump"))
        assert not name.startswith("docs/evidence/")
    assert ".env.example" in files


def test_public_market_prices_are_generated_and_explicitly_synthetic(tmp_path: Path) -> None:
    for name in ["calendar.json", "symbols.json"]:
        shutil.copyfile(FIXTURE / name, tmp_path / name)
    generate(tmp_path)
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert manifest["synthetic"] is True and manifest["provider_calls"] == 0
    for number, item in enumerate(manifest["artifacts"]):
        path = tmp_path / item["artifact"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == item["sha256"]
        generated = pd.read_parquet(path)
        committed = pd.read_parquet(FIXTURE / item["artifact"])
        pd.testing.assert_frame_equal(generated, committed)
        assert list(committed.Close) == [
            20.0 + number * 10 + (i % 100) / 10 + 0.25 for i in range(len(committed))
        ]
