#!/usr/bin/env python3
"""Materialize one immutable qfq release on NAS from a small factor input archive."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date
from pathlib import Path

import duckdb

from quant_stack.data.cn_operations_audit import _bars_profile, _factor_profile
from quant_stack.data.qfq_derivative import build_qfq_derivative
from quant_stack.data.qfq_input_bundle import read_qfq_factor_input_archive
from quant_stack.data.storage.atomic import atomic_replace_bytes
from quant_stack.data.worker_bundle import WorkerImportEvidence, import_worker_archive
from scripts.build_cn_qfq_worker_archive import build_archive
from scripts.build_cn_raw_union import build_raw_union
from scripts.plan_daily_qfq_promotion import plan_daily_qfq_promotion


def _write_json(path: Path, payload: object) -> None:
    atomic_replace_bytes(
        path,
        (
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str)
            + "\n"
        ).encode(),
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sessions(calendar_path: Path, *, after: date, through: date) -> list[date]:
    payload = json.loads(calendar_path.read_text(encoding="utf-8"))
    return [
        date.fromisoformat(item["trading_date"])
        for item in payload["days"]
        if item["state"] == "session"
        and after < date.fromisoformat(item["trading_date"]) <= through
    ]


def _watermark(path: Path) -> date:
    connection = duckdb.connect()
    try:
        row = connection.execute(
            "SELECT max(trading_date) FROM read_parquet(?)", [str(path)]
        ).fetchone()
    finally:
        connection.close()
    if row is None or row[0] is None:
        raise ValueError("raw baseline has no watermark")
    return row[0]


def _receipt_path(root: Path, value: object) -> Path:
    relative = Path(str(value))
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("daily receipt path is outside the imported data root")
    resolved = (root / relative).resolve()
    if not resolved.is_relative_to(root):
        raise ValueError("daily receipt path escapes the imported data root")
    return resolved


def daily_bars_from_report(
    *, data_root: Path, report_path: Path, expected_session: date
) -> list[Path]:
    report = json.loads(report_path.read_text(encoding="utf-8"))
    entries = report.get("entries")
    if (
        report.get("session_date") != expected_session.isoformat()
        or not isinstance(entries, list)
        or not entries
        or report.get("imported_archives") != len(entries)
        or report.get("catalog_published") is not False
        or report.get("production_pointer_updated") not in {None, False}
    ):
        raise ValueError(f"daily import report is not eligible for {expected_session}")
    bars: list[Path] = []
    source_commits: set[str] = set()
    for entry in entries:
        receipt_path = _receipt_path(data_root, entry.get("receipt_relative_path"))
        evidence = WorkerImportEvidence.model_validate_json(
            receipt_path.read_text(encoding="utf-8")
        )
        if (
            evidence.bundle_id != entry.get("worker_bundle_id")
            or evidence.catalog_published is not False
        ):
            raise ValueError("daily import receipt does not match its report")
        source_commits.add(evidence.source_commit)
        for artifact in evidence.envelope.artifacts:
            if artifact.kind != "bars":
                continue
            path = (data_root / artifact.relative_path).resolve()
            if (
                not path.is_relative_to(data_root)
                or not path.is_file()
                or path.stat().st_size != artifact.size_bytes
                or _sha256_file(path) != artifact.sha256
            ):
                raise ValueError("daily bar artifact failed authenticated readback")
            bars.append(path)
    if len(source_commits) != 1 or report.get("source_commit") not in source_commits:
        raise ValueError("daily import report has inconsistent source provenance")
    if not bars or len(bars) != len(set(bars)):
        raise ValueError("daily import report did not resolve unique bar artifacts")
    return bars


def materialize(args: argparse.Namespace) -> dict[str, object]:
    data_root = args.data_root.resolve()
    work_root = args.work_root.resolve()
    work_root.mkdir(parents=True, exist_ok=True)
    hmac_key = args.hmac_key_file.read_bytes()
    verified = read_qfq_factor_input_archive(args.input_archive, hmac_key=hmac_key)
    envelope = verified.envelope
    if envelope.source_commit != args.source_commit:
        raise ValueError("factor input source commit does not match NAS runtime source")
    if envelope.dataset_version != args.dataset_version:
        raise ValueError("factor input dataset version does not match materialization target")

    target_report_path = (
        args.daily_reports_root / f"{envelope.session_date:%Y%m%d}-first-import.json"
    )
    target_report = json.loads(target_report_path.read_text(encoding="utf-8"))
    if target_report != verified.daily_import_report:
        raise ValueError("factor input is not bound to the NAS daily import report")

    baseline_watermark = _watermark(args.raw_baseline)
    expected_sessions = _sessions(
        args.calendar_normalized,
        after=baseline_watermark,
        through=envelope.session_date,
    )
    if not expected_sessions or expected_sessions[-1] != envelope.session_date:
        raise ValueError("calendar does not contain the target materialization session")
    daily_by_session: dict[date, list[Path]] = {}
    for session in expected_sessions:
        report_path = args.daily_reports_root / f"{session:%Y%m%d}-first-import.json"
        daily_by_session[session] = daily_bars_from_report(
            data_root=data_root,
            report_path=report_path,
            expected_session=session,
        )

    factor_path = work_root / "factor-acquisition.json"
    _write_json(factor_path, verified.factor)
    raw_path = work_root / f"raw-through-{envelope.session_date:%Y%m%d}.parquet"
    qfq_path = work_root / f"qfq-through-{envelope.session_date:%Y%m%d}.parquet"
    daily_paths = [path for session in expected_sessions for path in daily_by_session[session]]
    raw = build_raw_union(
        historical_path=args.raw_baseline,
        daily_paths=daily_paths,
        expected_sessions=expected_sessions,
        output_path=raw_path,
    )
    target_daily = daily_by_session[envelope.session_date]
    plan = plan_daily_qfq_promotion(
        current_qfq_path=args.current_qfq,
        daily_raw_paths=target_daily,
        factor_path=factor_path,
        expected_sessions=[envelope.session_date],
        scratch_parent=work_root,
    )
    _write_json(work_root / "promotion-plan.json", plan)
    qfq = build_qfq_derivative(
        raw_bars_path=raw_path,
        factor_path=factor_path,
        corpus_path=None,
        output_path=qfq_path,
    )
    archive_path = work_root / "qfq-release.tar"
    archive_result = (
        {"created": False}
        if archive_path.is_file()
        else build_archive(
            qfq_path=qfq_path,
            factor_path=factor_path,
            staging_root=work_root / "release-staging",
            dataset_version=args.dataset_version,
            source_commit=args.source_commit,
            hmac_key=hmac_key,
            output_path=archive_path,
        )
    )
    first = import_worker_archive(
        archive_path=archive_path,
        data_root=data_root,
        hmac_key=hmac_key,
    )
    replay = import_worker_archive(
        archive_path=archive_path,
        data_root=data_root,
        hmac_key=hmac_key,
    )
    if (
        first.envelope.dataset_version != args.dataset_version
        or first.envelope.source_commit != args.source_commit
    ):
        raise RuntimeError("NAS-local qfq archive identity does not match the target")
    if not replay.idempotent_replay:
        raise RuntimeError("NAS-local qfq import and replay did not converge")
    if first.envelope.bundle_id != replay.envelope.bundle_id:
        raise RuntimeError("NAS-local qfq replay changed bundle identity")
    factor_profile = _factor_profile(factor_path)
    factor_profile.pop("instrument_ids", None)
    return {
        "schema_version": "quantstack.cn-qfq-nas-materialization.v1",
        "status": "completed",
        "session_date": envelope.session_date.isoformat(),
        "dataset_version": args.dataset_version,
        "source_commit": args.source_commit,
        "factor_input_archive_size_bytes": verified.archive_size_bytes,
        "wan_contains_raw_parquet": False,
        "wan_contains_qfq_parquet": False,
        "expected_sessions": [item.isoformat() for item in expected_sessions],
        "daily_report_count": len(expected_sessions),
        "raw": _bars_profile(raw_path),
        "factors": factor_profile,
        "qfq": _bars_profile(qfq_path),
        "plan": {
            key: plan[key]
            for key in (
                "status",
                "strategy",
                "target_watermark",
                "target_instrument_count",
                "raw_inputs_unchanged",
            )
        },
        "materialization": {"raw": raw, "qfq": qfq},
        "exact_key_coverage": True,
        "qfq_release_archive_size_bytes": archive_path.stat().st_size,
        "qfq_release_archive_location": "nas-local",
        "bundle_id": first.envelope.bundle_id,
        "import_receipt_relative_path": first.receipt_path.relative_to(data_root).as_posix(),
        "first_import_idempotent_replay": first.idempotent_replay,
        "replay_import_idempotent_replay": replay.idempotent_replay,
        "import_resumed": first.idempotent_replay,
        "artifact_count": len(first.envelope.artifacts),
        "archive_created": archive_result.get("created"),
        "catalog_published": False,
        "production_pointer_updated": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-archive", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--raw-baseline", type=Path, required=True)
    parser.add_argument("--current-qfq", type=Path, required=True)
    parser.add_argument("--calendar-normalized", type=Path, required=True)
    parser.add_argument("--daily-reports-root", type=Path, required=True)
    parser.add_argument("--work-root", type=Path, required=True)
    parser.add_argument("--hmac-key-file", type=Path, required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    report = materialize(args)
    _write_json(args.report, report)
    print(
        json.dumps(
            {
                "status": report["status"],
                "session_date": report["session_date"],
                "dataset_version": report["dataset_version"],
                "factor_input_archive_size_bytes": report["factor_input_archive_size_bytes"],
                "bundle_id": report["bundle_id"],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
