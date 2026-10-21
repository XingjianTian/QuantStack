from __future__ import annotations

import argparse
import gzip
import json
import re
import sys
from datetime import UTC, date, datetime, time
from importlib import import_module
from importlib.metadata import version
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

from pydantic import Field, ValidationError, model_validator

from .data.calendars.observed import ObservedSessionCalendar
from .data.daily import (
    DailyProviderContract,
    DailyRunPlan,
    DailyRunReport,
    DailyUniverse,
    DailyWorker,
    VerifiedTradingSession,
    build_daily_run_plan,
)
from .data.evidence import load_official_calendar_evidence
from .data.ingestion import IngestionService
from .data.models import CalendarEvidence, DataModel, Instrument
from .data.providers.cn.live import (
    BAOSTOCK_LIVE_DAILY_SCHEMA,
    BAOSTOCK_LIVE_RUNTIME_CONTRACT,
    BaoStockLiveProvider,
    BaoStockRequestBudget,
    normalize_baostock_live_bars,
)
from .data.providers.cn.replay import (
    AKShareReplayProvider,
    BaoStockReplayProvider,
    normalize_akshare_replay_bars,
    normalize_baostock_replay_bars,
)
from .data.publication import load_publication_bundle
from .data.session_parquet import install_session_package
from .data.storage import StagingFileSystemStorage
from .data.storage.atomic import atomic_replace_bytes
from .data.tasks import FileTaskStore, TaskRuntime
from .data.worker_bundle import build_worker_envelope, create_worker_archive

_BOUNDED_LIVE_SESSION = date(2026, 5, 7)
_BOUNDED_LIVE_INSTRUMENTS = frozenset({"XSHG.600000", "XSHG.600519", "XSHE.000001"})
_FULL_MARKET_MIN_INSTRUMENTS = 1_000
_FULL_MARKET_MAX_INSTRUMENTS = 10_000
_CN_TIMEZONE = ZoneInfo("Asia/Shanghai")
_DAILY_RELEASE_GATE = time(18, 0)


class DailyPlanSpec(DataModel):
    session: VerifiedTradingSession
    universe: DailyUniverse
    provider_contract: DailyProviderContract
    created_at: str | None = None


class ControlledLiveInstrumentSet(DataModel):
    schema_version: Literal["1.0.0"]
    universe_id: str
    version: str
    instruments: tuple[Instrument, ...]


class BaoStockDailyUniverseSnapshot(DataModel):
    """Provider-observed membership evidence; it is not an official PIT universe."""

    schema_version: Literal["1.0.0"] = "1.0.0"
    session_date: date
    fetched_at: datetime
    provider: Literal["baostock"] = "baostock"
    provider_version: Literal["0.9.3"] = "0.9.3"
    endpoint: Literal["query_all_stock"] = "query_all_stock"
    native_schema_version: Literal["baostock.query_all_stock.code-tradestatus-code_name.v1"] = (
        "baostock.query_all_stock.code-tradestatus-code_name.v1"
    )
    fields: tuple[Literal["code", "tradeStatus", "code_name"], ...]
    records: tuple[dict[str, str], ...]
    evidence_scope: Literal["provider_observed_membership_only"] = (
        "provider_observed_membership_only"
    )

    @property
    def version(self) -> str:
        return f"{self.session_date}.baostock-active-a-share-v1"

    @model_validator(mode="after")
    def validate_records(self) -> BaoStockDailyUniverseSnapshot:
        if self.fields != ("code", "tradeStatus", "code_name"):
            raise ValueError("BaoStock universe snapshot field contract changed")
        expected = {"code", "tradeStatus", "code_name"}
        codes: set[str] = set()
        for record in self.records:
            if set(record) != expected:
                raise ValueError("BaoStock universe snapshot native schema drift")
            if record["tradeStatus"] not in {"0", "1"}:
                raise ValueError("BaoStock universe snapshot has an unknown tradeStatus")
            if record["code"] in codes:
                raise ValueError("BaoStock universe snapshot contains duplicate codes")
            codes.add(record["code"])
        return self


class FullMarketArchiveEntry(DataModel):
    task_id: str
    requested_instruments: int = Field(ge=1)
    records_received: int = Field(ge=0)
    publication_bundle_uri: str = Field(pattern=r"^quant-data://")
    worker_bundle_id: str
    archive_relative_path: str
    archive_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    archive_size_bytes: int = Field(ge=1)


class FullMarketArchiveManifest(DataModel):
    schema_version: Literal["1.0.0"] = "1.0.0"
    dataset_version: str
    session_date: date
    universe_version: str
    generated_at: datetime
    entries: tuple[FullMarketArchiveEntry, ...]
    provider_snapshot_rows: int = Field(ge=0)
    active_universe_instruments: int = Field(ge=1)
    inactive_a_share_observations: int = Field(ge=0)
    planned_batches: int = Field(ge=1)
    succeeded_batches: int = Field(ge=0)
    failed_batches: int = Field(ge=0)
    requested_instruments: int = Field(ge=1)
    records_received: int = Field(ge=0)
    unresolved_instruments: int = Field(ge=0)
    duplicate_keys: Literal[0] = 0
    duration_seconds: float = Field(ge=0)
    archive_size_bytes: int = Field(ge=0)
    canonical_eligible: Literal[False] = False
    production_pointer_updated: Literal[False] = False


def _a_share_identity(code: str) -> tuple[str, str, str, str] | None:
    if len(code) != 9 or code[2] != "." or not code[3:].isdigit():
        return None
    prefix = code[3:6]
    if code.startswith("sh.") and prefix in {"600", "601", "603", "605", "688"}:
        return f"XSHG.{code[3:]}", code[3:], "XSHG", "sh"
    if code.startswith("sz.") and prefix in {"000", "001", "002", "003", "300", "301"}:
        return f"XSHE.{code[3:]}", code[3:], "XSHE", "sz"
    return None


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="quant-stack-daily")
    commands = parser.add_subparsers(dest="command", required=True)

    build = commands.add_parser("build-plan", help="build an auditable single-session plan")
    build.add_argument("spec", type=Path)
    build.add_argument("--output", required=True, type=Path)

    universe = commands.add_parser(
        "build-live-universe",
        help="fetch one BaoStock provider-observed active A-share universe",
    )
    universe.add_argument("--session", required=True, type=date.fromisoformat)
    universe.add_argument("--snapshot", required=True, type=Path)
    universe.add_argument("--instruments", required=True, type=Path)
    universe.add_argument("--spec", required=True, type=Path)
    universe.add_argument("--calendar-source", required=True)
    universe.add_argument("--calendar-version", required=True)
    universe.add_argument("--calendar-raw", required=True, type=Path)
    universe.add_argument("--calendar-normalized", required=True, type=Path)
    universe.add_argument("--calendar-raw-uri", required=True)
    universe.add_argument("--calendar-evidence-ref", required=True)
    universe.add_argument("--snapshot-evidence-ref", required=True)
    universe.add_argument("--provider-control-root", type=Path)
    universe.add_argument("--batch-size", type=int, default=50)
    universe.add_argument("--max-attempts", type=int, default=3)
    universe.add_argument("--retry-backoff-seconds", type=float, default=5)

    run = commands.add_parser("run-replay", help="execute a saved plan with frozen replay data")
    run.add_argument("plan", type=Path)
    run.add_argument("--data-root", required=True, type=Path)
    run.add_argument("--state-root", required=True, type=Path)
    run.add_argument("--report", required=True, type=Path)

    live = commands.add_parser(
        "run-live", help="execute one bounded BaoStock session and package a worker bundle"
    )
    live.add_argument("plan", type=Path)
    live.add_argument("--instruments", required=True, type=Path)
    live.add_argument("--calendar-raw", required=True, type=Path)
    live.add_argument("--calendar-normalized", required=True, type=Path)
    live.add_argument("--calendar-raw-uri", required=True)
    live.add_argument("--calendar-normalized-uri", required=True)
    live.add_argument("--data-root", required=True, type=Path)
    live.add_argument("--state-root", required=True, type=Path)
    live.add_argument("--report", required=True, type=Path)
    live.add_argument("--archive", required=True, type=Path)
    live.add_argument("--hmac-key-file", required=True, type=Path)
    live.add_argument("--source-commit", required=True)
    live.add_argument("--environment-id", required=True)
    live.add_argument(
        "--disable-expired-session-reauth-replay",
        "--disable-10001001-reauth-query-replay",
        action="store_true",
        help="fail on the first BaoStock 10001001 without logout/login/query replay",
    )

    full = commands.add_parser(
        "run-live-full-market",
        help="execute one provider-observed full-market BaoStock session",
    )
    full.add_argument("plan", type=Path)
    full.add_argument("--instruments", required=True, type=Path)
    full.add_argument("--universe-snapshot", required=True, type=Path)
    full.add_argument("--calendar-raw", required=True, type=Path)
    full.add_argument("--calendar-normalized", required=True, type=Path)
    full.add_argument("--calendar-raw-uri", required=True)
    full.add_argument("--calendar-normalized-uri", required=True)
    full.add_argument("--data-root", required=True, type=Path)
    full.add_argument("--provider-control-root", type=Path)
    full.add_argument("--state-root", required=True, type=Path)
    full.add_argument("--report", required=True, type=Path)
    full.add_argument("--archive-root", type=Path)
    full.add_argument("--archive-manifest", type=Path)
    full.add_argument("--hmac-key-file", type=Path)
    full.add_argument("--session-parquet-root", type=Path)
    full.add_argument("--session-cache-root", type=Path)
    full.add_argument("--source-commit", required=True)
    full.add_argument("--environment-id", required=True)
    full.add_argument(
        "--disable-expired-session-reauth-replay",
        "--disable-10001001-reauth-query-replay",
        action="store_true",
        help="fail on the first BaoStock 10001001 without logout/login/query replay",
    )
    return parser


def _write_model(path: Path, model: DataModel) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        model.model_dump_json(indent=2, exclude_computed_fields=True),
        encoding="utf-8",
    )
    temporary.replace(path)


def _assert_daily_release_gate(session_date: date, *, now: datetime | None = None) -> None:
    observed = now or datetime.now(_CN_TIMEZONE)
    if observed.tzinfo is None:
        raise ValueError("daily release gate requires a timezone-aware clock")
    observed_cn = observed.astimezone(_CN_TIMEZONE)
    if session_date > observed_cn.date():
        raise ValueError("daily session cannot be in the future")
    if session_date == observed_cn.date() and observed_cn.time() < _DAILY_RELEASE_GATE:
        raise ValueError("current-session BaoStock data is not eligible before 18:00 Asia/Shanghai")


def _verified_calendar(
    *,
    raw_path: Path,
    normalized_path: Path,
    raw_uri: str,
    normalized_uri: str,
    observed_at: datetime | None = None,
) -> tuple[ObservedSessionCalendar, CalendarEvidence]:
    evidence = load_official_calendar_evidence(
        raw_path=raw_path,
        normalized_path=normalized_path,
        observed_at=observed_at or datetime.now(_CN_TIMEZONE),
        raw_uri=raw_uri,
        normalized_uri=normalized_uri,
    )
    return ObservedSessionCalendar(evidence), evidence


def _assert_verified_session(
    *,
    session_date: date,
    calendar: ObservedSessionCalendar,
) -> None:
    sessions = tuple(calendar.sessions("CN", session_date, session_date))
    if len(sessions) != 1 or sessions[0].trading_date != session_date:
        raise ValueError("session date is not one verified complete trading session")


def _build_live_universe(args: argparse.Namespace) -> dict[str, object]:
    _assert_daily_release_gate(args.session)
    calendar, evidence = _verified_calendar(
        raw_path=args.calendar_raw,
        normalized_path=args.calendar_normalized,
        raw_uri=args.calendar_raw_uri,
        normalized_uri=args.calendar_evidence_ref,
    )
    if args.calendar_version != evidence.version or args.calendar_source != evidence.source:
        raise ValueError("live universe calendar identity does not match supplied evidence")
    _assert_verified_session(session_date=args.session, calendar=calendar)
    if version("baostock") != BAOSTOCK_LIVE_RUNTIME_CONTRACT.provider_version:
        raise RuntimeError("baostock version does not match the live Provider contract")
    if not 1 <= args.batch_size <= 500:
        raise ValueError("full-market batch size must be from 1 to 500")
    request_budget = (
        BaoStockRequestBudget.for_control_root(args.provider_control_root)
        if args.provider_control_root is not None
        else BaoStockRequestBudget.from_default()
    )
    client = import_module("baostock")
    with request_budget.session_lock():
        request_budget.ensure_capacity(2)
        request_budget.reserve(1)
        login = client.login()
        if login.error_code != "0":
            raise ConnectionError("BaoStock login failed")
        try:
            request_budget.reserve(1)
            result = client.query_all_stock(day=args.session.isoformat())
            if result.error_code != "0":
                raise ConnectionError("BaoStock query_all_stock failed")
            if tuple(result.fields) != ("code", "tradeStatus", "code_name"):
                raise ValueError("BaoStock query_all_stock schema drift")
            records: list[dict[str, str]] = []
            while result.error_code == "0" and result.next():
                records.append(dict(zip(result.fields, result.get_row_data(), strict=True)))
        finally:
            client.logout()

    snapshot = BaoStockDailyUniverseSnapshot(
        session_date=args.session,
        fetched_at=datetime.now(UTC),
        fields=("code", "tradeStatus", "code_name"),
        records=tuple(records),
    )
    instruments: list[Instrument] = []
    inactive_equities = 0
    for record in snapshot.records:
        identity = _a_share_identity(record["code"])
        if identity is None:
            continue
        if record["tradeStatus"] != "1":
            inactive_equities += 1
            continue
        instrument_id, symbol, venue, exchange_alias = identity
        instruments.append(
            Instrument(
                instrument_id=instrument_id,
                symbol=symbol,
                market="CN",
                venue=venue,
                provider_symbol=record["code"],
                provider_exchange_alias=exchange_alias,
                currency="CNY",
                name=record["code_name"] or None,
                timezone="Asia/Shanghai",
                active=True,
            )
        )
    instruments.sort(key=lambda item: item.instrument_id)
    instrument_ids = tuple(item.instrument_id for item in instruments)
    if len(instrument_ids) != len(set(instrument_ids)):
        raise ValueError("active A-share universe contains duplicate instrument identities")
    if not _FULL_MARKET_MIN_INSTRUMENTS <= len(instruments) <= _FULL_MARKET_MAX_INSTRUMENTS:
        raise ValueError("active A-share universe is outside the 1,000..10,000 safety range")

    configured = ControlledLiveInstrumentSet(
        schema_version="1.0.0",
        universe_id="cn-daily-baostock-active-a-share",
        version=snapshot.version,
        instruments=tuple(instruments),
    )
    contract = BAOSTOCK_LIVE_RUNTIME_CONTRACT
    spec = DailyPlanSpec(
        session=VerifiedTradingSession(
            session_date=args.session,
            calendar_source=args.calendar_source,
            calendar_version=args.calendar_version,
            evidence_ref=args.calendar_evidence_ref,
        ),
        universe=DailyUniverse(
            universe_id=configured.universe_id,
            version=configured.version,
            instrument_ids=instrument_ids,
            evidence_ref=args.snapshot_evidence_ref,
        ),
        provider_contract=DailyProviderContract(
            provider=contract.provider,
            provider_version=contract.provider_version,
            endpoint=contract.endpoint,
            adapter_version=contract.adapter_version,
            native_schema_version=contract.native_schema_version,
            batch_size=args.batch_size,
            max_attempts=args.max_attempts,
            retry_backoff_seconds=args.retry_backoff_seconds,
        ),
        created_at=datetime.now(UTC).isoformat(),
    )
    _write_model(args.snapshot, snapshot)
    _write_model(args.instruments, configured)
    _write_model(args.spec, spec)
    return {
        "session_date": args.session.isoformat(),
        "provider_rows": len(snapshot.records),
        "active_a_share_instruments": len(instruments),
        "inactive_a_share_observations": inactive_equities,
        "xshg": sum(item.venue == "XSHG" for item in instruments),
        "xshe": sum(item.venue == "XSHE" for item in instruments),
        "universe_version": configured.version,
    }


def _build_plan(spec_path: Path) -> DailyRunPlan:
    spec = DailyPlanSpec.model_validate_json(spec_path.read_text(encoding="utf-8"))
    created_at = None
    if spec.created_at is not None:
        from datetime import datetime

        created_at = datetime.fromisoformat(spec.created_at)
    return build_daily_run_plan(
        session=spec.session,
        universe=spec.universe,
        provider_contract=spec.provider_contract,
        created_at=created_at,
    )


def _live_runtime(
    *,
    plan: DailyRunPlan,
    instrument_path: Path,
    calendar_raw_path: Path,
    calendar_normalized_path: Path,
    calendar_raw_uri: str,
    calendar_normalized_uri: str,
    data_root: Path,
    state_root: Path,
    provider_control_root: Path | None = None,
    full_market: bool = False,
    universe_snapshot_path: Path | None = None,
    allow_expired_session_reauth_replay: bool = True,
) -> tuple[TaskRuntime, IngestionService, BaoStockLiveProvider]:
    contract = BAOSTOCK_LIVE_RUNTIME_CONTRACT
    expected = {
        "provider": contract.provider,
        "provider_version": contract.provider_version,
        "endpoint": contract.endpoint,
        "adapter_version": contract.adapter_version,
        "native_schema_version": contract.native_schema_version,
    }
    observed = {
        "provider": plan.provider_contract.provider,
        "provider_version": plan.provider_contract.provider_version,
        "endpoint": plan.provider_contract.endpoint,
        "adapter_version": plan.provider_contract.adapter_version,
        "native_schema_version": plan.provider_contract.native_schema_version,
    }
    if observed != expected:
        raise ValueError("run-live requires the exact BaoStock live provider contract")
    if any(not item.startswith(("XSHG.", "XSHE.")) for item in plan.universe.instrument_ids):
        raise ValueError("run-live accepts only XSHG and XSHE instruments")
    if full_market:
        if universe_snapshot_path is None:
            raise ValueError("full-market run requires its provider universe snapshot")
        if not (
            _FULL_MARKET_MIN_INSTRUMENTS
            <= len(plan.universe.instrument_ids)
            <= _FULL_MARKET_MAX_INSTRUMENTS
        ):
            raise ValueError("full-market run requires 1,000..10,000 active A-share instruments")
        if not 1 <= plan.provider_contract.batch_size <= 500:
            raise ValueError("full-market run batch size must be from 1 to 500")
        snapshot = BaoStockDailyUniverseSnapshot.model_validate_json(
            universe_snapshot_path.read_text(encoding="utf-8")
        )
        if snapshot.session_date != plan.session.session_date:
            raise ValueError("full-market universe snapshot session does not match the plan")
        if snapshot.version != plan.universe.version:
            raise ValueError("full-market universe snapshot version does not match the plan")
        snapshot_active = {
            identity[0]
            for record in snapshot.records
            if record["tradeStatus"] == "1"
            and (identity := _a_share_identity(record["code"])) is not None
        }
        if snapshot_active != set(plan.universe.instrument_ids):
            raise ValueError("full-market plan does not exactly cover the active snapshot universe")
    else:
        if len(plan.tasks) != 1 or len(plan.universe.instrument_ids) > 3:
            raise ValueError("run-live accepts exactly one batch with at most 3 instruments")
        if plan.session.session_date != _BOUNDED_LIVE_SESSION:
            raise ValueError("run-live accepts only the authorized 2026-05-07 session")
        if not set(plan.universe.instrument_ids) <= _BOUNDED_LIVE_INSTRUMENTS:
            raise ValueError(
                "run-live instruments must stay inside the authorized sample allowlist"
            )

    configured = ControlledLiveInstrumentSet.model_validate_json(
        instrument_path.read_text(encoding="utf-8")
    )
    if (
        configured.universe_id != plan.universe.universe_id
        or configured.version != plan.universe.version
    ):
        raise ValueError("instrument set identity does not match the saved plan")
    instruments = configured.instruments
    configured_ids = tuple(sorted(item.instrument_id for item in instruments))
    if configured_ids != tuple(sorted(plan.universe.instrument_ids)):
        raise ValueError("instrument set does not cover the saved plan exactly")
    if len(instruments) != len(configured_ids) or any(
        item.market != "CN"
        or item.venue not in {"XSHG", "XSHE"}
        or item.provider_exchange_alias not in {"sh", "sz"}
        or item.provider_symbol != f"{item.provider_exchange_alias}.{item.symbol}"
        or (item.venue == "XSHG") != (item.provider_exchange_alias == "sh")
        for item in instruments
    ):
        raise ValueError("instrument set contains an invalid venue or BaoStock symbol mapping")

    calendar, evidence = _verified_calendar(
        raw_path=calendar_raw_path,
        normalized_path=calendar_normalized_path,
        raw_uri=calendar_raw_uri,
        normalized_uri=calendar_normalized_uri,
    )
    if (
        plan.session.calendar_version != evidence.version
        or plan.session.calendar_source != evidence.source
        or plan.session.evidence_ref != calendar_normalized_uri
    ):
        raise ValueError("saved plan calendar identity does not match the supplied evidence")
    _assert_daily_release_gate(plan.session.session_date)
    _assert_verified_session(session_date=plan.session.session_date, calendar=calendar)

    provider = BaoStockLiveProvider(
        instruments,
        request_budget=(
            BaoStockRequestBudget.for_control_root(provider_control_root)
            if provider_control_root is not None
            else BaoStockRequestBudget.for_data_root(data_root)
        ),
        allow_expired_session_reauth_replay=allow_expired_session_reauth_replay,
    )
    service = IngestionService(
        providers={provider.name: provider},
        storage=StagingFileSystemStorage(data_root),
        bar_normalizers={provider.name: normalize_baostock_live_bars},
        calendar=calendar,
        provider_schemas={provider.name: BAOSTOCK_LIVE_DAILY_SCHEMA},
    )
    return TaskRuntime(FileTaskStore(state_root)), service, provider


def _bundle_transport_evidence(
    data_root: Path, bundle_uri: str
) -> tuple[Path, dict[str, object], datetime]:
    bundle_path = data_root / bundle_uri.removeprefix("quant-data://")
    bundle = load_publication_bundle(bundle_path)
    raw_artifact = next(item for item in bundle.artifacts if item.kind == "raw")
    raw_path = data_root / raw_artifact.uri.removeprefix("quant-data://")
    raw = json.loads(raw_path.read_text(encoding="utf-8"))
    audit = raw.get("transport_audit")
    if not isinstance(audit, dict):
        raise ValueError("successful full-market bundle is missing transport audit evidence")
    retrievals = [
        datetime.fromisoformat(str(item["_retrieval_timestamp"]))
        for item in raw.get("records", [])
        if isinstance(item, dict) and item.get("_retrieval_timestamp")
    ]
    if not retrievals or any(item.tzinfo is None for item in retrievals):
        raise ValueError("successful full-market bundle lacks stable retrieval timestamps")
    return bundle_path, audit, max(retrievals).astimezone(UTC)


def _run_live_full_market(args: argparse.Namespace) -> int:
    plan = DailyRunPlan.model_validate_json(args.plan.read_text(encoding="utf-8"))
    if not re.fullmatch(r"[a-f0-9]{40}", args.source_commit):
        raise ValueError("source commit must be a 40-character lowercase Git SHA")
    session_mode = args.session_parquet_root is not None
    if session_mode and any(
        value is not None
        for value in (args.archive_root, args.archive_manifest, args.hmac_key_file)
    ):
        raise ValueError("session Parquet mode does not create worker archives")
    if not session_mode and any(
        value is None for value in (args.archive_root, args.archive_manifest, args.hmac_key_file)
    ):
        raise ValueError("legacy full-market mode requires archive paths and HMAC key")
    runtime, service, provider = _live_runtime(
        plan=plan,
        instrument_path=args.instruments,
        calendar_raw_path=args.calendar_raw,
        calendar_normalized_path=args.calendar_normalized,
        calendar_raw_uri=args.calendar_raw_uri,
        calendar_normalized_uri=args.calendar_normalized_uri,
        data_root=args.data_root,
        state_root=args.state_root,
        provider_control_root=args.provider_control_root,
        full_market=True,
        universe_snapshot_path=args.universe_snapshot,
        allow_expired_session_reauth_replay=(
            not args.disable_expired_session_reauth_replay
        ),
    )
    if session_mode:
        if args.session_cache_root is None:
            raise ValueError("session Parquet mode requires --session-cache-root")
        args.session_cache_root.mkdir(parents=True, exist_ok=True)
        records: list[dict[str, object]] = []
        for index, task in enumerate(plan.tasks, start=1):
            cache_path = args.session_cache_root / f"batch-{index:04d}.json.gz"
            if cache_path.is_file():
                cached = json.loads(gzip.decompress(cache_path.read_bytes()))
                if (
                    cached.get("schema_version") != "quantstack.cn-session-batch-cache.v1"
                    or cached.get("session") != plan.session.session_date.isoformat()
                    or cached.get("requested_instruments") != list(task.instrument_scope)
                ):
                    raise ValueError(f"CN session batch cache identity mismatch: {cache_path}")
                raw_records = [dict(item) for item in cached.get("records", [])]
            else:
                raw_records = [
                    dict(item)
                    for item in provider.fetch_bars(
                        task.market,
                        task.instrument_scope,
                        task.start_date,
                        task.end_date,
                        "1d",
                    )
                ]
                service.provider_schema_validator.validate(
                    provider=provider.name,
                    records=raw_records,
                    schema=BAOSTOCK_LIVE_DAILY_SCHEMA,
                )
                atomic_replace_bytes(
                    cache_path,
                    gzip.compress(
                        json.dumps(
                            {
                                "schema_version": "quantstack.cn-session-batch-cache.v1",
                                "session": plan.session.session_date.isoformat(),
                                "requested_instruments": list(task.instrument_scope),
                                "records": raw_records,
                            },
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ).encode(),
                        mtime=0,
                    ),
                )
            instruments = list(provider.list_instruments(task.market, task.instrument_scope))
            lookup = {item.instrument_id: item for item in instruments}
            lookup.update({item.symbol: item for item in instruments})
            normalized = [
                dict(item)
                for item in normalize_baostock_live_bars(raw_records, lookup, service.calendar)
            ]
            quality = service.quality_checker.check_bars(
                dataset_id=plan.dataset_id,
                provider=provider.name,
                records=normalized,
                instruments=lookup,
            )
            if quality.quality_status == "failed":
                raise ValueError(
                    "CN session batch failed quality checks: " + ", ".join(quality.failed_checks)
                )
            preclose = {
                identity[0]: float(item["preclose"])
                for item in raw_records
                if (identity := _a_share_identity(str(item.get("code", "")))) is not None
            }
            for item in normalized:
                item["market"] = "CN"
                item["trading_date"] = plan.session.session_date
                item["provider_preclose"] = preclose.get(str(item["instrument_id"]))
            records.extend(normalized)
        returned = {str(item["instrument_id"]) for item in records}
        requested = set(plan.universe.instrument_ids)
        package = install_session_package(
            records=records,
            output_root=args.session_parquet_root,
            market="CN",
            session=plan.session.session_date,
            dataset_id=plan.dataset_id,
            version=plan.dataset_version,
            adjustment="raw",
            requested_instruments=len(requested),
            omissions=sorted(requested.difference(returned)),
        )
        result = {
            "schema_version": "quantstack.cn-session-collection.v1",
            "status": "completed",
            "session": plan.session.session_date.isoformat(),
            "bars": package.row_count,
            "instruments": package.instrument_count,
            "omissions": package.omission_count,
            "package": str(package.path),
            "provider_batches": len(plan.tasks),
            "provider_retries": 0,
            "persistence": "single_session_parquet",
        }
        atomic_replace_bytes(
            args.report,
            (json.dumps(result, indent=2, sort_keys=True) + "\n").encode(),
        )
        print(json.dumps(result, sort_keys=True))
        return 0

    report = DailyWorker(runtime, service).run(plan)
    _write_model(args.report, report)

    assert args.hmac_key_file is not None
    assert args.archive_root is not None
    assert args.archive_manifest is not None
    hmac_key = args.hmac_key_file.read_bytes()
    if len(hmac_key) < 32:
        raise ValueError("worker HMAC key must contain at least 32 bytes")
    archive_root = args.archive_root.resolve()
    manifest_parent = args.archive_manifest.parent.resolve()
    if not archive_root.is_relative_to(manifest_parent):
        raise ValueError("full-market archive root must be below the manifest directory")

    entries: list[FullMarketArchiveEntry] = []
    for batch in report.batches:
        if batch.publication_bundle_uri is None:
            continue
        bundle_path, transport_audit, created_at = _bundle_transport_evidence(
            args.data_root, batch.publication_bundle_uri
        )
        envelope = build_worker_envelope(
            data_root=args.data_root,
            publication_bundle_path=bundle_path,
            source_commit=args.source_commit,
            environment_id=args.environment_id,
            provider_contract=BAOSTOCK_LIVE_RUNTIME_CONTRACT,
            provider_transport_audit=transport_audit,
            created_at=created_at,
        )
        archive_path = archive_root / f"{batch.task_id}.tar"
        archive = create_worker_archive(
            data_root=args.data_root,
            envelope=envelope,
            output_path=archive_path,
            hmac_key=hmac_key,
        )
        entries.append(
            FullMarketArchiveEntry(
                task_id=batch.task_id,
                requested_instruments=len(batch.instrument_ids),
                records_received=batch.records_received,
                publication_bundle_uri=batch.publication_bundle_uri,
                worker_bundle_id=envelope.bundle_id,
                archive_relative_path=archive_path.relative_to(manifest_parent).as_posix(),
                archive_sha256=archive.sha256,
                archive_size_bytes=archive.size_bytes,
            )
        )
    if len(entries) != report.succeeded_batches:
        raise ValueError("full-market archive count does not match succeeded batches")
    snapshot = BaoStockDailyUniverseSnapshot.model_validate_json(
        args.universe_snapshot.read_text(encoding="utf-8")
    )
    inactive_a_share_observations = sum(
        record["tradeStatus"] == "0" and _a_share_identity(record["code"]) is not None
        for record in snapshot.records
    )
    failed_batches = len(report.batches) - report.succeeded_batches
    records_received = sum(item.records_received for item in report.batches)
    unresolved_instruments = sum(
        len(item.instrument_ids) - item.records_received for item in report.batches
    )
    manifest = FullMarketArchiveManifest(
        dataset_version=report.dataset_version,
        session_date=report.session_date,
        universe_version=report.universe_version,
        generated_at=datetime.now(UTC),
        entries=tuple(entries),
        provider_snapshot_rows=len(snapshot.records),
        active_universe_instruments=len(plan.universe.instrument_ids),
        inactive_a_share_observations=inactive_a_share_observations,
        planned_batches=len(plan.tasks),
        succeeded_batches=report.succeeded_batches,
        failed_batches=failed_batches,
        requested_instruments=len(plan.universe.instrument_ids),
        records_received=records_received,
        unresolved_instruments=unresolved_instruments,
        duration_seconds=(report.completed_at - report.started_at).total_seconds(),
        archive_size_bytes=sum(item.archive_size_bytes for item in entries),
    )
    _write_model(args.archive_manifest, manifest)
    print(
        json.dumps(
            {
                "archives": len(entries),
                "bars": records_received,
                "failed_batches": failed_batches,
                "manifest": str(args.archive_manifest),
                "succeeded_batches": report.succeeded_batches,
                "universe": len(plan.universe.instrument_ids),
                "unresolved_instruments": unresolved_instruments,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return report_exit_code(report)


def _run_live(args: argparse.Namespace) -> int:
    plan = DailyRunPlan.model_validate_json(args.plan.read_text(encoding="utf-8"))
    if not re.fullmatch(r"[a-f0-9]{40}", args.source_commit):
        raise ValueError("source commit must be a 40-character lowercase Git SHA")
    hmac_key = args.hmac_key_file.read_bytes()
    if len(hmac_key) < 32:
        raise ValueError("worker HMAC key must contain at least 32 bytes")
    runtime, service, provider = _live_runtime(
        plan=plan,
        instrument_path=args.instruments,
        calendar_raw_path=args.calendar_raw,
        calendar_normalized_path=args.calendar_normalized,
        calendar_raw_uri=args.calendar_raw_uri,
        calendar_normalized_uri=args.calendar_normalized_uri,
        data_root=args.data_root,
        state_root=args.state_root,
        allow_expired_session_reauth_replay=(
            not args.disable_expired_session_reauth_replay
        ),
    )
    report = DailyWorker(runtime, service).run(plan)
    _write_model(args.report, report)
    exit_code = report_exit_code(report)
    if exit_code:
        print(report.model_dump_json())
        return exit_code
    if len(report.publication_bundle_uris) != 1:
        raise ValueError("successful bounded live run must produce exactly one publication bundle")
    publication_path = args.data_root / report.publication_bundle_uris[0].removeprefix(
        "quant-data://"
    )
    envelope = build_worker_envelope(
        data_root=args.data_root,
        publication_bundle_path=publication_path,
        source_commit=args.source_commit,
        environment_id=args.environment_id,
        provider_contract=BAOSTOCK_LIVE_RUNTIME_CONTRACT,
        provider_transport_audit=provider.transport_audit(),
    )
    archive = create_worker_archive(
        data_root=args.data_root,
        envelope=envelope,
        output_path=args.archive,
        hmac_key=hmac_key,
    )
    print(
        json.dumps(
            {
                "archive_created": archive.created,
                "archive_sha256": archive.sha256,
                "archive_size_bytes": archive.size_bytes,
                "bundle_id": archive.envelope.bundle_id,
                "canonical_eligible": report.canonical_eligible,
                "dataset_version": report.dataset_version,
                "production_pointer_updated": report.production_pointer_updated,
            },
            sort_keys=True,
        )
    )
    return 0


def _replay_runtime(
    plan: DailyRunPlan, data_root: Path, state_root: Path
) -> tuple[TaskRuntime, IngestionService]:
    providers = {
        "akshare_replay": (
            AKShareReplayProvider(),
            normalize_akshare_replay_bars,
        ),
        "baostock_replay": (
            BaoStockReplayProvider(),
            normalize_baostock_replay_bars,
        ),
    }
    selected = providers.get(plan.provider_contract.provider)
    if selected is None:
        raise ValueError("run-replay only accepts akshare_replay or baostock_replay provider plans")
    provider, normalizer = selected
    runtime_contract = provider.runtime_contract
    expected = {
        "provider_version": runtime_contract.provider_version,
        "endpoint": runtime_contract.endpoint,
        "adapter_version": runtime_contract.adapter_version,
        "native_schema_version": runtime_contract.native_schema_version,
    }
    observed = {
        "provider_version": plan.provider_contract.provider_version,
        "endpoint": plan.provider_contract.endpoint,
        "adapter_version": plan.provider_contract.adapter_version,
        "native_schema_version": plan.provider_contract.native_schema_version,
    }
    if observed != expected:
        raise ValueError("saved plan provider contract does not match the frozen replay adapter")
    service = IngestionService(
        providers={provider.name: provider},
        storage=StagingFileSystemStorage(data_root),
        bar_normalizers={provider.name: normalizer},
    )
    return TaskRuntime(FileTaskStore(state_root)), service


def report_exit_code(report: DailyRunReport) -> int:
    if report.permanently_failed_batches:
        return 20
    if report.quarantined_batches:
        return 30
    if report.cancelled_batches:
        return 40
    if report.retryable_failed_batches:
        return 75
    return 0


def _summary(plan: DailyRunPlan) -> dict[str, object]:
    return {
        "dataset_id": plan.dataset_id,
        "dataset_version": plan.dataset_version,
        "session_date": plan.session.session_date.isoformat(),
        "universe_version": plan.universe.version,
        "provider": plan.provider_contract.provider,
        "tasks": len(plan.tasks),
        "canonical_eligible": plan.canonical_eligible,
        "production_pointer_updated": plan.production_pointer_updated,
    }


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "build-plan":
            plan = _build_plan(args.spec)
            _write_model(args.output, plan)
            print(json.dumps(_summary(plan), sort_keys=True))
            return 0
        if args.command == "build-live-universe":
            print(json.dumps(_build_live_universe(args), ensure_ascii=False, sort_keys=True))
            return 0
        if args.command == "run-replay":
            plan = DailyRunPlan.model_validate_json(args.plan.read_text(encoding="utf-8"))
            runtime, service = _replay_runtime(plan, args.data_root, args.state_root)
            report = DailyWorker(runtime, service).run(plan)
            _write_model(args.report, report)
            print(report.model_dump_json())
            return report_exit_code(report)
        if args.command == "run-live":
            return _run_live(args)
        if args.command == "run-live-full-market":
            return _run_live_full_market(args)
    except (OSError, RuntimeError, ValueError, ValidationError, json.JSONDecodeError) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
