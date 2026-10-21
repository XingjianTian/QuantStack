import json
from pathlib import Path

from quant_stack.data.tasks import FileTaskStore, Task
from quant_stack.ingest_cli import main

ROOT = Path(__file__).parents[1]


def test_cli_run_inspect_resume_and_publish(tmp_path: Path, capsys) -> None:
    spec_path = ROOT / "configs/cn_ingest_task.example.json"
    task = Task.model_validate_json(spec_path.read_text(encoding="utf-8"))
    data_root = tmp_path / "data"
    state_root = tmp_path / "state"

    assert main([
        "run-task",
        str(spec_path),
        "--data-root",
        str(data_root),
        "--state-root",
        str(state_root),
    ]) == 0
    run_output = json.loads(capsys.readouterr().out)
    assert run_output["state"] == "succeeded"

    assert main(["inspect-task", task.task_id, "--state-root", str(state_root)]) == 0
    inspected = json.loads(capsys.readouterr().out)
    assert inspected["task"]["state"] == "succeeded"
    bundle_uri = inspected["publication_bundle_uri"]
    bundle_path = data_root / bundle_uri.removeprefix("quant-data://")

    assert main([
        "publish-bundle",
        str(bundle_path),
        "--data-root",
        str(data_root),
    ]) == 0
    receipt = json.loads(capsys.readouterr().out)
    assert receipt["idempotent_replay"] is False

    assert main([
        "resume-task",
        task.task_id,
        "--data-root",
        str(data_root),
        "--state-root",
        str(state_root),
    ]) == 0
    resumed = json.loads(capsys.readouterr().out)
    assert resumed["state"] == "succeeded"
    persisted = FileTaskStore(state_root).load(task.task_id)
    assert persisted is not None
    assert len(persisted.attempts) == 1


def test_cli_returns_two_for_unknown_task(tmp_path: Path, capsys) -> None:
    assert main([
        "inspect-task",
        "task-000000000000000000000000",
        "--state-root",
        str(tmp_path),
    ]) == 2
    assert "unknown task" in capsys.readouterr().out
