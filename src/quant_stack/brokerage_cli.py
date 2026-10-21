"""Fixture-only local entry point for the read-only brokerage sample.

The module deliberately exposes no live mode, credential option, URL option, or
order-writing subcommand.  It is a small operational wrapper around the
fixture transport, synchronization store, budget ledger, and proposal models.
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from quant_stack.brokerage.schwab_adapter import SchwabAdapter
from quant_stack.brokerage.sync import PrivateSnapshotStore, SyncService
from quant_stack.investing.budgets import build_ledger, load_ledger
from quant_stack.investing.plans import EquityLayer, ETFPlan, InvestmentPlan
from quant_stack.investing.proposals import (
    MAX_SNAPSHOT_AGE,
    _valid_quote,
    generate_proposals,
    load_current_proposal,
    load_plan,
    persist_ledger_and_proposal,
    persist_proposal,
    review_proposal,
)

DEFAULT_ACCOUNT_ID = "fictional-account"
DEFAULT_ACCOUNT_HASH = "fictional-account-hash"
DEFAULT_FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "brokerage"
DEFAULT_STATE_ROOT = Path("/tmp/quantstack-schwab-sample")


def _parse_time(value: str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("--at must include a timezone")
    return parsed.astimezone(UTC)


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _fixture_payloads(root: Path) -> dict[str, Any]:
    required = {
        "account": "normal_account.json",
        "orders": "normal_orders.json",
        "transactions": "normal_transactions.json",
        "quotes": "normal_quotes.json",
        "coverage": "normal_coverage.json",
    }
    payloads: dict[str, Any] = {}
    for key, filename in required.items():
        path = root / filename
        if not path.is_file():
            raise FileNotFoundError(f"fixture-only CLI requires {path}")
        payloads[key] = _load_json(path)
    return payloads


def _profile_fixture_root(args: argparse.Namespace) -> Path:
    root = Path(args.fixture_root).expanduser().resolve()
    if args.profile == "ready":
        root = root / "ready"
    return root


def _store(args: argparse.Namespace) -> PrivateSnapshotStore:
    return PrivateSnapshotStore(
        Path(args.state_root).expanduser(),
        source_root=Path(__file__).resolve().parents[2],
    )


def _sync(args: argparse.Namespace, store: PrivateSnapshotStore, at: datetime):
    fixture_root = _profile_fixture_root(args)
    payloads = _fixture_payloads(fixture_root)
    adapter = SchwabAdapter.from_fixture(
        account_hash=args.account_hash,
        payloads=payloads,
        clock=lambda: at,
    )
    try:
        return SyncService(adapter, store, clock=lambda: at).sync_account(
            args.account_id,
            args.account_hash,
        )
    finally:
        adapter.close()


def _current_sample_view(
    store: PrivateSnapshotStore,
    account_id: str,
) -> tuple[Any | None, Any]:
    """Read the current snapshot and cumulative fact waterline atomically."""

    with store.transaction() as tx:
        state = tx.state(account_id)
        snapshot = tx.latest(account_id)
        if snapshot is None:
            return None, state
        reader = getattr(tx, "facts", None) or getattr(tx, "read_facts", None)
        if not callable(reader):
            raise RuntimeError("snapshot transaction does not expose cumulative facts")
        values = {
            fact_type: tuple(reader(account_id, fact_type=fact_type))
            for fact_type in ("position", "order", "transaction", "fill")
        }
        snapshot = snapshot.model_copy(
            update={
                "positions": values["position"],
                "orders": values["order"],
                "transactions": values["transaction"],
                "fills": values["fill"],
            }
        )
    return snapshot, state


def _sync_failure_generation(store: PrivateSnapshotStore, account_id: str) -> int:
    return store.sync_failure_generation(account_id)


def _sample_plan(account_id: str) -> InvestmentPlan:
    """Return a fully fictional plan used only by ``propose-sample``."""

    return InvestmentPlan(
        account_id=account_id,
        version=1,
        etf_plans=(
            ETFPlan(
                plan_id="fictional-etf-plan",
                account_id=account_id,
                symbol="QXEN",
                currency="USD",
                amount_per_period="100.00",
                period="monthly",
                start_date=date(2026, 1, 1),
                timezone="UTC",
                execution_day=1,
                annual_cap="1000.00",
                fee_per_order="1.00",
                cash_reserve="10.00",
            ),
        ),
        equity_layers=(
            EquityLayer(
                layer_id="fictional-equity-layer-1",
                account_id=account_id,
                symbol="QXEN",
                currency="USD",
                budget="200.00",
                trigger_price="36.00",
                total_position_limit="10.00",
                sequence=1,
                require_review=True,
                fee_per_order="1.00",
                cash_reserve="10.00",
            ),
        ),
    )


def _result_json(value: Any) -> str:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    elif isinstance(value, dict):
        value = {key: json.loads(_result_json(item)) for key, item in value.items()}
    elif isinstance(value, (list, tuple)):
        value = [json.loads(_result_json(item)) for item in value]
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, default=str)


def _snapshot_summary(result: Any) -> dict[str, Any]:
    snapshot = result.snapshot
    return {
        "attempt": result.attempt.model_dump(mode="json"),
        "state": result.state.model_dump(mode="json"),
        "published": result.published,
        "proposal_blocked": result.proposal_blocked,
        "snapshot": (
            {
                "snapshot_id": snapshot.snapshot_id,
                "account_id": snapshot.account.account_id,
                "alias": snapshot.account.alias,
                "business_time": snapshot.business_time.isoformat()
                if snapshot.business_time
                else None,
                "positions": len(snapshot.positions),
                "orders": len(snapshot.orders),
                "transactions": len(snapshot.transactions),
                "quotes": len(snapshot.quotes),
                "complete": snapshot.complete,
                "conflicts": snapshot.conflicts,
                "non_executable": snapshot.non_executable,
            }
            if snapshot is not None
            else None
        ),
    }


def command_sync(args: argparse.Namespace) -> int:
    at = _parse_time(args.at)
    result = _sync(args, _store(args), at)
    print(_result_json(_snapshot_summary(result)))
    return 0 if not result.proposal_blocked else 2


def command_propose(args: argparse.Namespace) -> int:
    at = _parse_time(args.at)
    store = _store(args)
    snapshot, state = _current_sample_view(store, args.account_id)
    if snapshot is None:
        _sync(args, store, at)
        snapshot, state = _current_sample_view(store, args.account_id)
    if snapshot is None:
        print(
            _result_json(
                {
                    "status": "blocked",
                    "reasons": ["SNAPSHOT_UNAVAILABLE"],
                    "non_executable": True,
                }
            )
        )
        return 2
    plan = _sample_plan(args.account_id)
    prior_ledger = load_ledger(store, args.account_id)
    # The fixture transaction is explicitly attributed to the fictional ETF
    # rule.  Real account events require a separate human attribution record.
    ledger = build_ledger(
        snapshot,
        plan,
        prior=prior_ledger,
        attributions={"SAMPLE-TRANSACTION-001": "fictional-etf-plan"},
        clock=lambda: at,
    )
    previous = load_current_proposal(store, args.account_id)
    batch = generate_proposals(
        snapshot,
        plan,
        ledger,
        as_of=at,
        sync_state=state,
        previous=previous,
        sync_failure_generation=_sync_failure_generation(store, args.account_id),
    )
    persist_ledger_and_proposal(
        store,
        ledger,
        batch,
        plan=plan,
        expected_prior_ledger=prior_ledger,
        commit_at=at,
    )
    print(_result_json(batch))
    return 0 if batch.status.value != "blocked" else 2


def command_state(args: argparse.Namespace) -> int:
    store = _store(args)
    snapshot, state = _current_sample_view(store, args.account_id)
    proposal = load_current_proposal(store, args.account_id)
    ledger = load_ledger(store, args.account_id)
    persisted_plan = load_plan(store, args.account_id)
    failure_generation = _sync_failure_generation(store, args.account_id)
    at = _parse_time(args.at)
    freshness_reasons: list[str] = []
    if snapshot is None:
        freshness_reasons.append("SNAPSHOT_UNAVAILABLE")
    else:
        if not snapshot.complete or not snapshot.coverage.complete:
            freshness_reasons.append("SNAPSHOT_INCOMPLETE")
        if snapshot.conflicts:
            freshness_reasons.extend(
                f"SNAPSHOT_CONFLICT:{item}" for item in snapshot.conflicts
            )
        if snapshot.coverage.truncated:
            freshness_reasons.append("COVERAGE_INCOMPLETE")
        if snapshot.business_time is None:
            freshness_reasons.append("BUSINESS_TIME_UNKNOWN")
        else:
            if snapshot.business_time > at + timedelta(seconds=1):
                freshness_reasons.append("SNAPSHOT_FUTURE")
            elif at - snapshot.business_time > MAX_SNAPSHOT_AGE:
                freshness_reasons.append("SNAPSHOT_STALE")
        if not state.complete or state.last_success_snapshot_id != snapshot.snapshot_id:
            freshness_reasons.append("SYNC_INCOMPLETE")
        quotes = {quote.instrument.symbol: quote for quote in snapshot.quotes}
        sample_plan = persisted_plan or _sample_plan(args.account_id)
        for rule in (*sample_plan.etf_plans, *sample_plan.equity_layers):
            _, quote_reasons = _valid_quote(quotes.get(rule.symbol), as_of=at)
            freshness_reasons.extend(
                f"{reason}:{rule.symbol}" for reason in quote_reasons
            )
    freshness_reasons = list(dict.fromkeys(freshness_reasons))
    stale_proposal_id = None
    proposal_state = "unavailable"
    if proposal is not None:
        if persisted_plan is None:
            freshness_reasons.append("PLAN_UNAVAILABLE")
        elif proposal.plan_fingerprint != persisted_plan.fingerprint:
            freshness_reasons.append("PLAN_CHANGED")
        if proposal.sync_failure_generation != failure_generation:
            freshness_reasons.append("SYNC_FAILURE_GENERATION_CHANGED")
        proposal_matches_current = (
            snapshot is not None
            and ledger is not None
            and state.complete
            and state.last_success_snapshot_id == snapshot.snapshot_id
            and proposal.snapshot_id == snapshot.snapshot_id
            and proposal.ledger_version == ledger.ledger_version
            and not freshness_reasons
        )
        if proposal_matches_current:
            proposal_state = "current"
        else:
            proposal_state = "stale"
            stale_proposal_id = proposal.batch_id
    print(
        _result_json(
            {
                "state": state,
                "snapshot_id": snapshot.snapshot_id if snapshot else None,
                "ledger_version": ledger.ledger_version if ledger else None,
                "current_proposal": proposal if proposal_state == "current" else None,
                "current_proposal_state": proposal_state,
                "stale_proposal_batch_id": stale_proposal_id,
                "proposal_freshness_reasons": freshness_reasons,
                "as_of": at.isoformat(),
                "non_executable": True,
            }
        )
    )
    return 0


def command_review(args: argparse.Namespace) -> int:
    store = _store(args)
    batch = load_current_proposal(store, args.account_id)
    if batch is None:
        print(
            _result_json(
                {
                    "status": "blocked",
                    "reasons": ["PROPOSAL_UNAVAILABLE"],
                    "non_executable": True,
                }
            )
        )
        return 2
    reviewed_at = _parse_time(args.at)
    reviewed = review_proposal(
        batch,
        reviewer=args.reviewer,
        expected_input_fingerprint=args.input_fingerprint,
        at=reviewed_at,
    )
    persist_proposal(store, reviewed, commit_at=reviewed_at)
    print(_result_json(reviewed))
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="quant-stack-brokerage-sample",
        description="Fixture-only read-only account sync and non-executable proposals",
    )
    parser.add_argument("--state-root", default=str(DEFAULT_STATE_ROOT))
    parser.add_argument("--fixture-root", default=str(DEFAULT_FIXTURE_ROOT))
    parser.add_argument("--account-id", default=DEFAULT_ACCOUNT_ID)
    parser.add_argument("--account-hash", default=DEFAULT_ACCOUNT_HASH)
    parser.add_argument(
        "--profile",
        choices=("blocked", "ready"),
        default="blocked",
        help="fictional fixture metadata profile; ready is still offline and non-executable",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sync = sub.add_parser("sync-sample", help="synchronize fictional fixture data")
    sync.add_argument("--at", help="UTC or offset-aware ISO timestamp")
    sync.set_defaults(handler=command_sync)

    proposal = sub.add_parser("propose-sample", help="generate a fictional proposal batch")
    proposal.add_argument("--at", help="UTC or offset-aware ISO timestamp")
    proposal.set_defaults(handler=command_propose)

    state = sub.add_parser("show-state", help="show persisted sample state")
    state.add_argument("--at", help="UTC or offset-aware ISO timestamp for freshness checks")
    state.set_defaults(handler=command_state)

    review = sub.add_parser("review", help="record review state for an exact proposal version")
    review.add_argument("--reviewer", required=True)
    review.add_argument("--input-fingerprint", required=True)
    review.add_argument("--at", help="UTC or offset-aware ISO timestamp")
    review.set_defaults(handler=command_review)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        return int(args.handler(args))
    except (ValueError, FileNotFoundError, RuntimeError) as exc:
        print(
            _result_json(
                {
                    "status": "blocked",
                    "error": type(exc).__name__,
                    "message": str(exc),
                    "non_executable": True,
                }
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["main"]
