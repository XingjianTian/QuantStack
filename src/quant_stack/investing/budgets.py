"""Budget accounting for normalized brokerage snapshots.

This module consumes only :mod:`quant_stack.brokerage.contracts`.  It treats a
snapshot as evidence, never as an instruction to trade.  In particular,
positions are an opening/current fact and do not become budget spend; only
identified, attributed buy executions (plus explicit fee/correction entries)
change a plan's used amount.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from quant_stack.brokerage.contracts import (
    AccountSnapshot,
    Fill,
    OrderSide,
    OrderStatus,
    Transaction,
)

from .plans import EquityLayer, InvestmentPlan

ZERO = Decimal("0")


class BudgetModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        validate_assignment=True,
    )

    def canonical_json(self) -> str:
        return json.dumps(
            self.model_dump(mode="json"),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )


def _dec(value: Any, *, allow_negative: bool = True) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise TypeError("a decimal value must be an integer, string, or Decimal")
    if isinstance(value, float):
        raise TypeError("floating-point values are not accepted for money or quantity")
    result = value if isinstance(value, Decimal) else Decimal(str(value))
    if not result.is_finite():
        raise ValueError("decimal values must be finite")
    if not allow_negative and result < 0:
        raise ValueError("value must be non-negative")
    return result


def _utc(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamps must include a timezone")
    return value.astimezone(UTC)


class AttributionStatus(StrEnum):
    ATTRIBUTED = "attributed"
    UNATTRIBUTED = "unattributed"
    CORRECTED = "corrected"
    REVERSED = "reversed"
    UNKNOWN = "unknown"


class EntryKind(StrEnum):
    BUY = "buy"
    FEE = "fee"
    CORRECTION = "correction"
    REVERSAL = "reversal"
    SELL = "sell"


class Attribution(BudgetModel):
    """Explicit human/configuration attribution for one evidence identifier."""

    evidence_id: str = Field(min_length=1)
    plan_id: str = Field(min_length=1)
    layer_id: str | None = None
    status: AttributionStatus = AttributionStatus.ATTRIBUTED
    reason: str = Field(min_length=1)
    recorded_at: datetime
    recorded_by: str = Field(default="fixture-review", min_length=1)
    supersedes: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _normalize_time(cls, values: Any) -> Any:
        if isinstance(values, dict) and values.get("recorded_at") is not None:
            values = dict(values)
            values["recorded_at"] = _utc(values["recorded_at"])
        return values


class LedgerEntry(BudgetModel):
    """One immutable, auditable budget fact.

    ``cost`` accepts the alias ``amount`` on input for convenience.  The
    persisted representation always uses ``cost``.  A correction/reversal may
    have a negative cost; a normal buy must never have one.
    """

    entry_id: str = Field(min_length=1)
    account_id: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    currency: str = Field(default="USD", min_length=3, max_length=3, pattern=r"^[A-Z]{3}$")
    kind: EntryKind = EntryKind.BUY
    cost: Decimal = Decimal("0")
    fee: Decimal = Decimal("0")
    quantity: Decimal | None = None
    plan_id: str | None = None
    layer_id: str | None = None
    status: AttributionStatus = AttributionStatus.UNATTRIBUTED
    source_transaction_id: str | None = None
    source_execution_id: str | None = None
    source_order_id: str | None = None
    source_fingerprint: str | None = None
    evidence_id: str = Field(min_length=1)
    effective_at: datetime | None = None
    reverses_entry_id: str | None = None
    correction_of: str | None = None
    note: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _parse_costs(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        values = dict(values)
        if values.get("cost") is None and values.get("amount") is not None:
            values["cost"] = values["amount"]
        values.pop("amount", None)
        for name in ("cost", "fee", "quantity"):
            if values.get(name) is not None:
                values[name] = _dec(values[name])
        if values.get("effective_at") is not None:
            values["effective_at"] = _utc(values["effective_at"])
        return values

    @model_validator(mode="after")
    def _validate_entry(self) -> LedgerEntry:
        if self.kind is EntryKind.BUY and self.cost < 0:
            raise ValueError("buy cost cannot be negative; use a correction or reversal")
        if self.kind is EntryKind.FEE and self.cost != 0:
            raise ValueError("fee entries use fee, not cost")
        if self.fee < 0 and self.kind not in {EntryKind.CORRECTION, EntryKind.REVERSAL}:
            raise ValueError("normal fees cannot be negative")
        if self.status in {AttributionStatus.ATTRIBUTED, AttributionStatus.CORRECTED}:
            if not self.plan_id:
                raise ValueError("attributed entries require a plan_id")
        if self.kind in {EntryKind.CORRECTION, EntryKind.REVERSAL} and not (
            self.reverses_entry_id or self.correction_of
        ):
            raise ValueError("correction/reversal entries require a referenced entry")
        return self

    @property
    def total_cost(self) -> Decimal:
        return self.cost + self.fee


class PositionWatermark(BudgetModel):
    """Signed position quantity retained as the ledger's reconciliation baseline.

    A position is an account fact, not budget spend.  Retaining its quantity
    alongside the ledger nevertheless gives the next complete snapshot an
    explicit opening/current baseline.  A quantity change must then be
    explained by normalized trade evidence before a proposal can proceed.
    """

    symbol: str = Field(min_length=1)
    quantity: Decimal

    @model_validator(mode="before")
    @classmethod
    def _parse_quantity(cls, values: Any) -> Any:
        if isinstance(values, dict) and values.get("quantity") is not None:
            values = dict(values)
            values["quantity"] = _dec(values["quantity"])
        return values


class LedgerSnapshot(BudgetModel):
    """Persisted, versioned budget ledger bound to one complete snapshot."""

    ledger_id: str
    ledger_version: str
    account_id: str
    snapshot_id: str
    plan_version: int
    entries: tuple[LedgerEntry, ...] = ()
    attributions: tuple[Attribution, ...] = ()
    unattributed_evidence: tuple[str, ...] = ()
    blocking_reasons: tuple[str, ...] = ()
    built_at: datetime
    facts_fingerprint: str | None = None
    fact_watermark: tuple[str, ...] = ()
    position_watermark: tuple[PositionWatermark, ...] = ()
    position_baseline_recorded: bool = False
    non_executable: Literal[True] = True

    @model_validator(mode="before")
    @classmethod
    def _normalize_dates(cls, values: Any) -> Any:
        if isinstance(values, dict) and values.get("built_at") is not None:
            values = dict(values)
            values["built_at"] = _utc(values["built_at"])
        return values

    @property
    def complete(self) -> bool:
        return not self.blocking_reasons and not self.unattributed_evidence

    def spent_for(self, plan_id: str, *, layer_id: str | None = None) -> Decimal:
        total = ZERO
        for entry in self.entries:
            if entry.status not in {AttributionStatus.ATTRIBUTED, AttributionStatus.CORRECTED}:
                continue
            if entry.plan_id != plan_id or (layer_id is not None and entry.layer_id != layer_id):
                continue
            if entry.kind in {EntryKind.BUY, EntryKind.CORRECTION, EntryKind.REVERSAL}:
                total += entry.total_cost
            elif entry.kind is EntryKind.FEE:
                total += entry.fee
        return total


class BudgetSummary(BudgetModel):
    account_id: str
    plan_id: str
    layer_id: str | None = None
    accrued: Decimal
    spent: Decimal
    remaining: Decimal
    currency: str = "USD"
    source_ledger_version: str
    non_executable: Literal[True] = True

    @model_validator(mode="before")
    @classmethod
    def _parse_values(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        values = dict(values)
        for name in ("accrued", "spent", "remaining"):
            if values.get(name) is not None:
                values[name] = _dec(values[name])
        return values


class OrderOccupation(BudgetModel):
    """Known open-order cash occupation, kept separate from filled spend."""

    account_id: str
    currency: str
    amount: Decimal
    order_ids: tuple[str, ...] = ()
    uncertain_order_ids: tuple[str, ...] = ()
    blocking_reasons: tuple[str, ...] = ()
    reservation_included: bool | None = None
    non_executable: Literal[True] = True

    @model_validator(mode="before")
    @classmethod
    def _parse_values(cls, values: Any) -> Any:
        if isinstance(values, dict) and values.get("amount") is not None:
            values = dict(values)
            values["amount"] = _dec(values["amount"], allow_negative=False)
        return values


def _is_buy_transaction(transaction: Transaction) -> bool | None:
    subtype = (transaction.transaction_subtype or "").strip().lower()
    kind = transaction.transaction_type.strip().lower()
    if any(token in subtype for token in ("buy", "purchase")):
        return True
    if any(token in subtype for token in ("sell", "sale")):
        return False
    if kind in {"buy", "purchase", "trade_buy"}:
        return True
    if kind in {"sell", "sale", "trade_sell"}:
        return False
    if kind == "trade":
        if transaction.net_amount is not None and transaction.net_amount.amount < 0:
            return True
        if transaction.net_amount is not None and transaction.net_amount.amount > 0:
            return False
    return None


def _is_structural_transaction(transaction: Transaction) -> bool:
    kind = (f"{transaction.transaction_type} {transaction.transaction_subtype or ''}").lower()
    return any(
        token in kind
        for token in (
            "corporate",
            "corp ",
            "split",
            "spinoff",
            "spin-off",
            "merger",
            "conversion",
            "reorganization",
            "reorg",
            "option_assignment",
            "option assignment",
            "exercise",
        )
    )


def _is_instrument_trade(transaction: Transaction) -> bool:
    return transaction.instrument is not None and transaction.quantity is not None


def _fill_cost(fill: Fill) -> tuple[Decimal, Decimal] | None:
    if fill.amount is not None:
        cost = abs(fill.amount.amount)
    elif fill.price is not None:
        cost = abs(fill.price.amount * fill.quantity)
    else:
        return None
    fee = abs(fill.fee.amount) if fill.fee is not None else ZERO
    return cost, fee


def _transaction_cost(transaction: Transaction) -> tuple[Decimal, Decimal] | None:
    if transaction.net_amount is not None:
        return abs(transaction.net_amount.amount), ZERO
    if transaction.price is not None and transaction.quantity is not None:
        return abs(transaction.price.amount * transaction.quantity), ZERO
    return None


_NON_INVESTMENT_CASH_TRANSACTION_TYPES = frozenset(
    {
        "cash_receipt",
        "cash_disbursement",
        "cash_transfer",
        "cash_deposit",
        "cash_withdrawal",
    }
)


def _normalized_transaction_type(transaction: Transaction) -> str:
    return transaction.transaction_type.strip().lower().replace("-", "_").replace(" ", "_")


def _is_known_non_investment_cash_event(transaction: Transaction) -> bool:
    return _normalized_transaction_type(transaction) in _NON_INVESTMENT_CASH_TRANSACTION_TYPES


def _attribution_for(
    evidence_id: str,
    source_ids: Iterable[str],
    attributions: Mapping[str, Attribution | str | tuple[str, str | None]] | None,
    *,
    recorded_at: datetime,
) -> Attribution | None:
    if not attributions:
        return None
    for key in (evidence_id, *source_ids):
        value = attributions.get(key)
        if value is None:
            continue
        if isinstance(value, Attribution):
            return value
        if isinstance(value, tuple):
            plan_id, layer_id = value
            return Attribution(
                evidence_id=evidence_id,
                plan_id=plan_id,
                layer_id=layer_id,
                reason="explicit fixture attribution",
                recorded_at=recorded_at,
            )
        return Attribution(
            evidence_id=evidence_id,
            plan_id=str(value),
            reason="explicit fixture attribution",
            recorded_at=recorded_at,
        )
    return None


def _source_ids(entry: LedgerEntry) -> tuple[str, ...]:
    """Return stable evidence keys used to carry facts across rolling windows."""

    result = [entry.evidence_id]
    if entry.source_execution_id:
        result.extend((entry.source_execution_id, f"execution:{entry.source_execution_id}"))
    if entry.source_transaction_id:
        result.extend((entry.source_transaction_id, f"transaction:{entry.source_transaction_id}"))
    return tuple(dict.fromkeys(result))


def _source_fingerprint(source_body: str) -> str:
    return hashlib.sha256(source_body.encode("utf-8")).hexdigest()


def facts_fingerprint(snapshot: AccountSnapshot) -> str:
    """Hash the normalized fact set used to build a ledger.

    The synchronization store retains immutable transaction/fill evidence and
    the current position/order facts independently of the latest snapshot.
    Including all four fact sets in the ledger version lets a later proposal
    detect a late window fact even when the provider keeps the latest snapshot
    pointer unchanged.
    """

    payload: list[tuple[str, str, str]] = []
    for fact_type, values, key in (
        ("position", snapshot.positions, "position_id"),
        ("order", snapshot.orders, "order_id"),
        ("transaction", snapshot.transactions, "transaction_id"),
        ("fill", snapshot.fills, "execution_id"),
    ):
        payload.extend(
            (fact_type, str(getattr(value, key)), value.canonical_json()) for value in values
        )
    payload.sort(key=lambda item: (item[0], item[1], item[2]))
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


def _position_watermark(snapshot: AccountSnapshot) -> tuple[PositionWatermark, ...]:
    quantities: dict[str, Decimal] = {}
    for position in snapshot.positions:
        quantity = position.quantity
        if position.side.value == "short":
            quantity = -abs(quantity)
        quantities[position.instrument.symbol] = (
            quantities.get(position.instrument.symbol, ZERO) + quantity
        )
    return tuple(
        PositionWatermark(symbol=symbol, quantity=quantity)
        for symbol, quantity in sorted(quantities.items())
    )


def _fact_watermark(snapshot: AccountSnapshot) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                *(f"position:{item.position_id}" for item in snapshot.positions),
                *(f"order:{item.order_id}" for item in snapshot.orders),
                *(f"transaction:{item.transaction_id}" for item in snapshot.transactions),
                *(f"fill:{item.execution_id}" for item in snapshot.fills),
            }
        )
    )


def _position_trade_evidence(
    snapshot: AccountSnapshot,
    *,
    excluded_fact_ids: set[str] | None = None,
) -> tuple[dict[str, Decimal], set[str]]:
    """Return signed quantities that explain position changes and unknown symbols.

    Fills are preferred for transactions that have them so an aggregate
    transaction and its executions do not double count.  A transaction with
    no executions remains usable evidence.  Unknown side/type or unmappable
    corporate-action evidence is reported separately and remains fail-closed
    when a position quantity changes.
    """

    quantities: dict[str, Decimal] = {}
    unknown_symbols: set[str] = set()
    excluded_fact_ids = excluded_fact_ids or set()
    fills_by_transaction: dict[str, list[Fill]] = {}
    seen_fill_ids: set[str] = set()
    for fill in snapshot.fills:
        if f"fill:{fill.execution_id}" in excluded_fact_ids:
            continue
        if fill.execution_id in seen_fill_ids:
            continue
        seen_fill_ids.add(fill.execution_id)
        if fill.source_transaction_id:
            fills_by_transaction.setdefault(fill.source_transaction_id, []).append(fill)

    def add(symbol: str | None, quantity: Decimal, side: OrderSide) -> None:
        if not symbol:
            unknown_symbols.add("*")
            return
        if side in {OrderSide.BUY, OrderSide.BUY_TO_COVER}:
            signed = abs(quantity)
        elif side in {OrderSide.SELL, OrderSide.SELL_SHORT}:
            signed = -abs(quantity)
        else:
            unknown_symbols.add(symbol)
            return
        quantities[symbol] = quantities.get(symbol, ZERO) + signed

    seen_transaction_ids: set[str] = set()
    for transaction in snapshot.transactions:
        if f"transaction:{transaction.transaction_id}" in excluded_fact_ids:
            continue
        if transaction.transaction_id in seen_transaction_ids:
            continue
        seen_transaction_ids.add(transaction.transaction_id)
        linked_fills = fills_by_transaction.get(transaction.transaction_id, [])
        if linked_fills:
            for fill in linked_fills:
                add(
                    fill.instrument.symbol
                    if fill.instrument is not None
                    else (
                        transaction.instrument.symbol
                        if transaction.instrument is not None
                        else None
                    ),
                    fill.quantity,
                    fill.side,
                )
            continue
        if transaction.instrument is None or transaction.quantity is None:
            continue
        if _is_structural_transaction(transaction):
            unknown_symbols.add(transaction.instrument.symbol)
            continue
        side = _is_buy_transaction(transaction)
        if side is None:
            unknown_symbols.add(transaction.instrument.symbol)
            continue
        add(
            transaction.instrument.symbol,
            transaction.quantity,
            OrderSide.BUY if side else OrderSide.SELL,
        )
    # An unlinked fill is already a ledger blocking condition.  Retain its
    # symbol here as well so a position movement cannot be cleared by an
    # otherwise ambiguous execution.
    for fill in snapshot.fills:
        if f"fill:{fill.execution_id}" in excluded_fact_ids:
            continue
        if fill.execution_id in seen_fill_ids and fill.source_transaction_id:
            continue
        if fill.instrument is not None:
            unknown_symbols.add(fill.instrument.symbol)
        else:
            unknown_symbols.add("*")
    return quantities, unknown_symbols


def _position_reconciliation_reasons(
    snapshot: AccountSnapshot,
    prior: LedgerSnapshot | None,
) -> list[str]:
    """Fail closed when a new position quantity has no matching trade fact."""

    if prior is None or not prior.position_baseline_recorded:
        # The first persisted watermark is the explicit opening baseline.
        return []
    previous = {item.symbol: item.quantity for item in prior.position_watermark}
    current = {item.symbol: item.quantity for item in _position_watermark(snapshot)}
    excluded_fact_ids = set(prior.fact_watermark)
    if not excluded_fact_ids:
        for entry in prior.entries:
            if entry.source_transaction_id:
                excluded_fact_ids.add(f"transaction:{entry.source_transaction_id}")
            if entry.source_execution_id:
                excluded_fact_ids.add(f"fill:{entry.source_execution_id}")
    evidence, unknown_symbols = _position_trade_evidence(
        snapshot,
        excluded_fact_ids=excluded_fact_ids,
    )
    reasons: list[str] = []
    for symbol in sorted(set(previous) | set(current)):
        delta = current.get(symbol, ZERO) - previous.get(symbol, ZERO)
        if delta == ZERO:
            continue
        if symbol in unknown_symbols or "*" in unknown_symbols:
            reasons.append(f"UNKNOWN_POSITION_EVENT:{symbol}")
            continue
        if evidence.get(symbol, ZERO) != delta:
            reasons.append(f"UNEXPLAINED_POSITION_CHANGE:{symbol}")
    return reasons


class LedgerBuilder:
    """Build a deterministic ledger from one normalized account snapshot."""

    def __init__(self, *, clock: Any = None) -> None:
        self.clock = clock or (lambda: datetime.now(UTC))

    def build(
        self,
        snapshot: AccountSnapshot,
        plan: InvestmentPlan,
        *,
        attributions: Mapping[str, Attribution | str | tuple[str, str | None]] | None = None,
        initial_entries: Sequence[LedgerEntry] = (),
        prior: LedgerSnapshot | None = None,
    ) -> LedgerSnapshot:
        built_at = _utc(self.clock()) or datetime.now(UTC)
        blocking: list[str] = []
        unattributed: list[str] = []
        entries: list[LedgerEntry] = []
        attribution_records: list[Attribution] = []
        seen: dict[str, int] = {}
        attribution_keys: set[str] = set()

        if snapshot.account.account_id != plan.account_id:
            blocking.append("ACCOUNT_PLAN_MISMATCH")
        if not snapshot.complete or not snapshot.coverage.complete:
            blocking.append("SNAPSHOT_INCOMPLETE")
        if snapshot.conflicts:
            blocking.extend(f"SNAPSHOT_CONFLICT:{item}" for item in snapshot.conflicts)
        usable_prior = prior is not None
        if prior is not None:
            if prior.account_id != plan.account_id:
                blocking.append("LEDGER_ACCOUNT_VERSION_MISMATCH")
                usable_prior = False
            if prior.plan_version != plan.version:
                blocking.append("LEDGER_PLAN_VERSION_MISMATCH")
                usable_prior = False
        if usable_prior and prior is not None:
            entries.extend(prior.entries)
            attribution_records.extend(prior.attributions)
            attribution_keys.update(
                _source_fingerprint(item.canonical_json()) for item in prior.attributions
            )
            blocking.extend(_position_reconciliation_reasons(snapshot, prior))

        for entry in initial_entries:
            entries.append(entry)

        for index, entry in enumerate(entries):
            for source_id in _source_ids(entry):
                prior_index = seen.get(source_id)
                if prior_index is not None and prior_index != index:
                    blocking.append(f"DUPLICATE_EVIDENCE_CONFLICT:{source_id}")
                else:
                    seen[source_id] = index

        def record_attribution(attribution: Attribution) -> None:
            key = _source_fingerprint(attribution.canonical_json())
            if key not in attribution_keys:
                attribution_records.append(attribution)
                attribution_keys.add(key)

        def apply_attribution(entry: LedgerEntry, attribution: Attribution) -> LedgerEntry:
            attribution_layer = attribution.layer_id
            if attribution_layer is None:
                try:
                    rule = plan.rule(attribution.plan_id)
                except KeyError:
                    rule = None
                if isinstance(rule, EquityLayer):
                    attribution_layer = rule.layer_id
            return entry.model_copy(
                update={
                    "plan_id": attribution.plan_id,
                    "layer_id": attribution_layer,
                    "status": attribution.status,
                }
            )

        def add_entry(
            *,
            entry: LedgerEntry,
            source_id: str,
            source_body: str,
            evidence_attribution: Attribution | None,
        ) -> None:
            # A previous broker window may have exposed only a transaction
            # aggregate.  When a later window supplies the linked execution,
            # replace that provisional evidence in place instead of charging
            # the same trade a second time.  A changed amount is ambiguous and
            # must fail closed rather than silently choosing one value.
            if entry.source_execution_id and entry.source_transaction_id:
                transaction_key = f"transaction:{entry.source_transaction_id}"
                related_index = seen.get(transaction_key)
                if related_index is not None:
                    related_entry = entries[related_index]
                    if (
                        related_entry.source_execution_id is None
                        and related_entry.source_transaction_id == entry.source_transaction_id
                    ):
                        if related_entry.total_cost != entry.total_cost:
                            blocking.append(
                                f"TRANSACTION_FILL_VALUE_MISMATCH:{entry.source_transaction_id}"
                            )
                            return
                        incoming_fingerprint = _source_fingerprint(source_body)
                        replacement = entry.model_copy(
                            update={"source_fingerprint": incoming_fingerprint}
                        )
                        if evidence_attribution is not None:
                            record_attribution(evidence_attribution)
                            replacement = apply_attribution(replacement, evidence_attribution)
                        elif related_entry.status in {
                            AttributionStatus.ATTRIBUTED,
                            AttributionStatus.CORRECTED,
                        }:
                            # Carry a human attribution made against the
                            # provisional transaction evidence to the more
                            # precise execution evidence.
                            replacement = replacement.model_copy(
                                update={
                                    "plan_id": related_entry.plan_id,
                                    "layer_id": related_entry.layer_id,
                                    "status": related_entry.status,
                                }
                            )
                        entries[related_index] = replacement
                        for key in (*_source_ids(related_entry), *_source_ids(replacement)):
                            seen[key] = related_index
                        seen[source_id] = related_index
                        if replacement.status not in {
                            AttributionStatus.ATTRIBUTED,
                            AttributionStatus.CORRECTED,
                        }:
                            unattributed.append(source_id)
                        return
            old = seen.get(source_id)
            if old is not None:
                prior_entry = entries[old]
                incoming_fingerprint = _source_fingerprint(source_body)
                if prior_entry.source_fingerprint is not None:
                    if prior_entry.source_fingerprint != incoming_fingerprint:
                        blocking.append(f"DUPLICATE_EVIDENCE_CONFLICT:{source_id}")
                elif (
                    prior_entry.symbol != entry.symbol
                    or prior_entry.quantity != entry.quantity
                    or prior_entry.total_cost != entry.total_cost
                ):
                    blocking.append(f"DUPLICATE_EVIDENCE_CONFLICT:{source_id}")
                if evidence_attribution is not None:
                    record_attribution(evidence_attribution)
                    entries[old] = apply_attribution(prior_entry, evidence_attribution)
                return
            entry = entry.model_copy(
                update={"source_fingerprint": _source_fingerprint(source_body)}
            )
            if evidence_attribution is not None:
                record_attribution(evidence_attribution)
                if evidence_attribution.status not in {
                    AttributionStatus.ATTRIBUTED,
                    AttributionStatus.CORRECTED,
                }:
                    unattributed.append(source_id)
                    seen[source_id] = len(entries)
                    entries.append(entry)
                    return
                entry = apply_attribution(entry, evidence_attribution)
            if entry.status not in {AttributionStatus.ATTRIBUTED, AttributionStatus.CORRECTED}:
                unattributed.append(source_id)
                seen[source_id] = len(entries)
                entries.append(entry)
                return
            if entry.plan_id is None:
                unattributed.append(source_id)
                seen[source_id] = len(entries)
                entries.append(entry)
                return
            seen[source_id] = len(entries)
            entries.append(entry)

        fills_by_transaction: dict[str, list[Fill]] = {}
        seen_fill_ids: set[str] = set()
        for fill in snapshot.fills:
            body = fill.canonical_json()
            if fill.execution_id in seen_fill_ids:
                # Repeated identical fill evidence is idempotent; conflicting
                # duplicate IDs are caught by the canonical body check below.
                previous = next(
                    (item for item in snapshot.fills if item.execution_id == fill.execution_id),
                    None,
                )
                if previous is not None and previous.canonical_json() != body:
                    blocking.append(f"DUPLICATE_EVIDENCE_CONFLICT:{fill.execution_id}")
                continue
            seen_fill_ids.add(fill.execution_id)
            if fill.source_transaction_id:
                fills_by_transaction.setdefault(fill.source_transaction_id, []).append(fill)

        covered_transactions: set[str] = set()
        seen_transaction_bodies: dict[str, str] = {}
        for transaction in snapshot.transactions:
            transaction_body = transaction.canonical_json()
            previous_transaction = seen_transaction_bodies.get(transaction.transaction_id)
            if previous_transaction is not None:
                if previous_transaction != transaction_body:
                    blocking.append(f"DUPLICATE_EVIDENCE_CONFLICT:{transaction.transaction_id}")
                continue
            seen_transaction_bodies[transaction.transaction_id] = transaction_body
            transaction_kind = (
                f"{transaction.transaction_type} {transaction.transaction_subtype or ''}"
            ).lower()
            is_correction = any(
                token in transaction_kind
                for token in ("reversal", "cancel", "correction", "adjust")
            )
            is_fee = any(token in transaction_kind for token in ("fee", "commission"))
            if _is_structural_transaction(transaction):
                blocking.append(f"UNKNOWN_STRUCTURAL_EVENT:{transaction.transaction_id}")
                continue
            if "transaction_shape" in transaction.unknown_fields:
                blocking.append(f"UNKNOWN_TRANSACTION_SHAPE:{transaction.transaction_id}")
                continue
            if transaction.instrument is None and transaction.quantity is None:
                if is_correction or is_fee:
                    pass
                elif _is_known_non_investment_cash_event(transaction):
                    # Known cash receipts/transfers do not consume an investment
                    # plan.  A missing trade or unknown event is evidence that
                    # must remain visible and block the ledger instead.
                    continue
                else:
                    blocking.append(f"UNKNOWN_TRANSACTION_SHAPE:{transaction.transaction_id}")
                    continue
            if not is_correction and is_fee:
                amount = _transaction_cost(transaction)
                if amount is None:
                    blocking.append(f"UNKNOWN_TRANSACTION_VALUE:{transaction.transaction_id}")
                    continue
                evidence_id = f"transaction:{transaction.transaction_id}"
                attribution = _attribution_for(
                    evidence_id,
                    (transaction.transaction_id, transaction.order_id or ""),
                    attributions,
                    recorded_at=built_at,
                )
                add_entry(
                    entry=LedgerEntry(
                        entry_id=evidence_id,
                        account_id=transaction.account_id,
                        symbol=(
                            transaction.instrument.symbol
                            if transaction.instrument is not None
                            else "CASH"
                        ),
                        currency=(transaction.net_amount or transaction.price).currency
                        if (transaction.net_amount or transaction.price)
                        else "USD",
                        kind=EntryKind.FEE,
                        cost=ZERO,
                        fee=amount[0],
                        quantity=transaction.quantity,
                        source_transaction_id=transaction.transaction_id,
                        source_order_id=transaction.order_id,
                        evidence_id=evidence_id,
                        effective_at=transaction.transaction_time,
                        status=AttributionStatus.UNATTRIBUTED,
                    ),
                    source_id=evidence_id,
                    source_body=transaction_body,
                    evidence_attribution=attribution,
                )
                continue
            if any(
                token in transaction_kind
                for token in ("reversal", "cancel", "correction", "adjust")
            ):
                related = transaction.related_transaction_id
                if not related:
                    blocking.append(f"UNKNOWN_CORRECTION_REFERENCE:{transaction.transaction_id}")
                    continue
                amount = _transaction_cost(transaction)
                if amount is None:
                    blocking.append(f"UNKNOWN_TRANSACTION_VALUE:{transaction.transaction_id}")
                    continue
                evidence_id = f"transaction:{transaction.transaction_id}"
                attribution = _attribution_for(
                    evidence_id,
                    (transaction.transaction_id, related),
                    attributions,
                    recorded_at=built_at,
                )
                add_entry(
                    entry=LedgerEntry(
                        entry_id=evidence_id,
                        account_id=transaction.account_id,
                        symbol=transaction.instrument.symbol if transaction.instrument else "CASH",
                        currency=(transaction.net_amount or transaction.price).currency
                        if (transaction.net_amount or transaction.price)
                        else "USD",
                        kind=(
                            EntryKind.REVERSAL
                            if "reversal" in transaction_kind or "cancel" in transaction_kind
                            else EntryKind.CORRECTION
                        ),
                        cost=-amount[0],
                        fee=ZERO,
                        quantity=transaction.quantity,
                        source_transaction_id=transaction.transaction_id,
                        source_order_id=transaction.order_id,
                        evidence_id=evidence_id,
                        effective_at=transaction.transaction_time,
                        reverses_entry_id=f"transaction:{related}",
                        correction_of=f"transaction:{related}",
                        status=AttributionStatus.UNATTRIBUTED,
                    ),
                    source_id=evidence_id,
                    source_body=transaction_body,
                    evidence_attribution=attribution,
                )
                continue
            if not _is_instrument_trade(transaction):
                blocking.append(f"UNKNOWN_TRANSACTION_SHAPE:{transaction.transaction_id}")
                continue
            side = _is_buy_transaction(transaction)
            if side is None:
                blocking.append(f"UNKNOWN_TRANSACTION_TYPE:{transaction.transaction_id}")
                continue
            fills = fills_by_transaction.get(transaction.transaction_id, [])
            if fills:
                covered_transactions.add(transaction.transaction_id)
                for fill in fills:
                    if fill.side is OrderSide.UNKNOWN:
                        blocking.append(f"UNKNOWN_FILL_SIDE:{fill.execution_id}")
                        continue
                    if fill.side is not OrderSide.BUY:
                        continue
                    amount = _fill_cost(fill)
                    if amount is None:
                        blocking.append(f"UNKNOWN_FILL_VALUE:{fill.execution_id}")
                        continue
                    cost, fee = amount
                    evidence_id = f"execution:{fill.execution_id}"
                    attribution = _attribution_for(
                        evidence_id,
                        (
                            fill.execution_id,
                            fill.source_transaction_id or "",
                            transaction.transaction_id,
                        ),
                        attributions,
                        recorded_at=built_at,
                    )
                    source_body = fill.canonical_json()
                    add_entry(
                        entry=LedgerEntry(
                            entry_id=evidence_id,
                            account_id=fill.account_id,
                            symbol=(
                                fill.instrument.symbol
                                if fill.instrument
                                else transaction.instrument.symbol
                            ),
                            currency=(fill.amount or fill.price or transaction.price).currency
                            if (fill.amount or fill.price or transaction.price)
                            else plan.etf_plans[0].currency
                            if plan.etf_plans
                            else "USD",
                            kind=EntryKind.BUY,
                            cost=cost,
                            fee=fee,
                            quantity=fill.quantity,
                            source_transaction_id=(
                                fill.source_transaction_id or transaction.transaction_id
                            ),
                            source_execution_id=fill.execution_id,
                            source_order_id=fill.order_id or transaction.order_id,
                            evidence_id=evidence_id,
                            effective_at=fill.executed_at or transaction.transaction_time,
                            plan_id=None,
                            status=AttributionStatus.UNATTRIBUTED,
                        ),
                        source_id=evidence_id,
                        source_body=source_body,
                        evidence_attribution=attribution,
                    )
                continue

            if not side:
                # A sell can affect cash, but it does not refill a buy budget.
                continue
            amount = _transaction_cost(transaction)
            if amount is None:
                blocking.append(f"UNKNOWN_TRANSACTION_VALUE:{transaction.transaction_id}")
                continue
            cost, fee = amount
            evidence_id = f"transaction:{transaction.transaction_id}"
            attribution = _attribution_for(
                evidence_id,
                (transaction.transaction_id, transaction.order_id or ""),
                attributions,
                recorded_at=built_at,
            )
            add_entry(
                entry=LedgerEntry(
                    entry_id=evidence_id,
                    account_id=transaction.account_id,
                    symbol=transaction.instrument.symbol,
                    currency=(transaction.net_amount or transaction.price).currency
                    if (transaction.net_amount or transaction.price)
                    else "USD",
                    kind=EntryKind.BUY,
                    cost=cost,
                    fee=fee,
                    quantity=transaction.quantity,
                    source_transaction_id=transaction.transaction_id,
                    source_order_id=transaction.order_id,
                    evidence_id=evidence_id,
                    effective_at=transaction.transaction_time,
                    status=AttributionStatus.UNATTRIBUTED,
                ),
                source_id=evidence_id,
                source_body=transaction.canonical_json(),
                evidence_attribution=attribution,
            )

        # A fill with no corresponding transaction is still real execution
        # evidence, but its relationship is uncertain and must be reconciled.
        for fill in snapshot.fills:
            if fill.source_transaction_id and fill.source_transaction_id in covered_transactions:
                continue
            if fill.instrument is None:
                continue
            if fill.side is OrderSide.BUY:
                blocking.append(f"UNLINKED_FILL:{fill.execution_id}")

        def entry_reference_keys(entry: LedgerEntry) -> set[str]:
            keys = set(_source_ids(entry))
            keys.update({entry.entry_id, entry.evidence_id})
            return keys

        reference_to_entries: dict[str, set[int]] = {}
        for index, entry in enumerate(entries):
            for reference in entry_reference_keys(entry):
                reference_to_entries.setdefault(reference, set()).add(index)

        def target_candidates(target: str | None) -> set[int]:
            if not target:
                return set()
            keys = {target}
            if not target.startswith(("transaction:", "execution:")):
                keys.update((f"transaction:{target}", f"execution:{target}"))
            result: set[int] = set()
            for key in keys:
                result.update(reference_to_entries.get(key, set()))
            return result

        def resolve_entry_rule(entry: LedgerEntry) -> tuple[Any | None, str | None]:
            """Resolve and validate the flat plan/layer identity carried by an entry."""

            if entry.plan_id is None:
                if entry.layer_id is not None and entry.layer_id.strip():
                    return None, "missing_plan_id"
                return None, None
            try:
                rule = plan.rule(entry.plan_id)
            except KeyError:
                return None, "unknown_plan"
            layer_id = entry.layer_id.strip() if entry.layer_id is not None else ""
            if isinstance(rule, EquityLayer):
                if not layer_id:
                    return rule, None
                try:
                    layer_rule = plan.rule(layer_id)
                except KeyError:
                    return None, "unknown_layer"
                if not isinstance(layer_rule, EquityLayer) or layer_rule.layer_id != rule.layer_id:
                    return None, "layer_rule_mismatch"
                return rule, None
            if layer_id:
                return None, "etf_layer_id"
            return rule, None

        def entry_rule_symbol_issue(entry: LedgerEntry, rule: Any) -> str | None:
            """Return the security attribution issue for a budget-bearing entry."""

            if entry.kind is EntryKind.BUY:
                if not entry.symbol.strip():
                    return "missing"
            elif entry.kind is EntryKind.FEE:
                # A fee without an instrument is represented as CASH.  It is
                # still a valid account-level cost and must not be rejected
                # merely because it has no security association.
                if not entry.symbol.strip() or entry.symbol.strip().upper() == "CASH":
                    return None
            else:
                return None
            return "mismatch" if entry.symbol != rule.symbol else None

        correction_targets: dict[int, int] = {}
        for index, entry in enumerate(entries):
            if entry.kind not in {EntryKind.CORRECTION, EntryKind.REVERSAL}:
                continue
            target = entry.reverses_entry_id or entry.correction_of
            candidates = target_candidates(target)
            if not candidates:
                blocking.append(f"UNKNOWN_CORRECTION_TARGET:{entry.entry_id}")
                entries[index] = entry.model_copy(update={"status": AttributionStatus.UNATTRIBUTED})
                unattributed.append(entry.evidence_id)
                continue
            if len(candidates) != 1:
                blocking.append(f"AMBIGUOUS_CORRECTION_TARGET:{entry.entry_id}")
                entries[index] = entry.model_copy(update={"status": AttributionStatus.UNATTRIBUTED})
                unattributed.append(entry.evidence_id)
                continue
            correction_targets[index] = next(iter(candidates))

        def correction_reaches(start: int, target: int) -> bool:
            visited: set[int] = set()
            current = target
            while current in correction_targets:
                if current == start or current in visited:
                    return True
                visited.add(current)
                current = correction_targets[current]
            return False

        corrected_targets: dict[int, int] = {}
        for index, entry in enumerate(entries):
            if entry.kind in {EntryKind.CORRECTION, EntryKind.REVERSAL}:
                target_index = correction_targets.get(index)
                if target_index is None:
                    continue
                target_entry = entries[target_index]
                target_ref = entry.reverses_entry_id or entry.correction_of or ""
                invalid_reason: str | None = None
                target_rule, target_identity_issue = resolve_entry_rule(target_entry)
                target_symbol_issue = (
                    entry_rule_symbol_issue(target_entry, target_rule)
                    if target_rule is not None and target_identity_issue is None
                    else None
                )
                if target_index == index:
                    invalid_reason = f"CORRECTION_SELF_REFERENCE:{entry.entry_id}"
                elif correction_reaches(index, target_index):
                    invalid_reason = f"CORRECTION_TARGET_CYCLE:{entry.entry_id}"
                elif target_entry.kind not in {EntryKind.BUY, EntryKind.FEE}:
                    invalid_reason = f"CORRECTION_TARGET_NOT_BASE:{entry.entry_id}"
                elif target_entry.account_id != entry.account_id:
                    invalid_reason = f"CORRECTION_TARGET_ACCOUNT_MISMATCH:{entry.entry_id}"
                elif (
                    target_entry.plan_id is None
                    or entry.plan_id != target_entry.plan_id
                    or entry.layer_id != target_entry.layer_id
                ):
                    invalid_reason = f"CORRECTION_TARGET_RULE_MISMATCH:{entry.entry_id}"
                elif target_entry.currency != entry.currency:
                    invalid_reason = f"CORRECTION_TARGET_CURRENCY_MISMATCH:{entry.entry_id}"
                elif target_entry.status not in {
                    AttributionStatus.ATTRIBUTED,
                    AttributionStatus.CORRECTED,
                }:
                    invalid_reason = f"CORRECTION_TARGET_UNATTRIBUTED:{entry.entry_id}"
                elif target_identity_issue is not None:
                    invalid_reason = f"CORRECTION_TARGET_RULE_IDENTITY_MISMATCH:{entry.entry_id}"
                elif target_symbol_issue in {"missing", "mismatch"}:
                    invalid_reason = f"CORRECTION_TARGET_RULE_SYMBOL_MISMATCH:{entry.entry_id}"
                elif entry.total_cost >= ZERO:
                    invalid_reason = f"INVALID_CORRECTION_AMOUNT:{entry.entry_id}"
                elif target_entry.total_cost <= ZERO or -entry.total_cost > target_entry.total_cost:
                    invalid_reason = f"CORRECTION_EXCEEDS_TARGET:{entry.entry_id}"
                elif target_index in corrected_targets:
                    invalid_reason = f"DUPLICATE_CORRECTION_TARGET:{target_ref}"

                if invalid_reason is not None:
                    blocking.append(invalid_reason)
                    entry = entry.model_copy(update={"status": AttributionStatus.UNATTRIBUTED})
                    entries[index] = entry
                    unattributed.append(entry.evidence_id)
                else:
                    corrected_targets[target_index] = index
            if entry.account_id != plan.account_id:
                blocking.append(f"ENTRY_ACCOUNT_MISMATCH:{entry.entry_id}")
            if entry.status not in {AttributionStatus.ATTRIBUTED, AttributionStatus.CORRECTED}:
                unattributed.append(entry.evidence_id)
            rule, identity_issue = resolve_entry_rule(entry)
            if identity_issue is not None:
                if identity_issue in {"unknown_plan", "unknown_layer"}:
                    blocking.append(f"UNKNOWN_RULE:{entry.entry_id}")
                blocking.append(f"ENTRY_RULE_IDENTITY_MISMATCH:{entry.entry_id}")
                entries[index] = entry.model_copy(update={"status": AttributionStatus.UNATTRIBUTED})
                unattributed.append(entry.evidence_id)
            elif rule is not None:
                # Canonicalize an equity-layer attribution that omitted the
                # redundant layer_id; this keeps direct initial entries and
                # attribution-created entries on the same identity shape.
                canonical_layer_id = rule.layer_id if isinstance(rule, EquityLayer) else None
                if entry.layer_id != canonical_layer_id:
                    entry = entry.model_copy(update={"layer_id": canonical_layer_id})
                    entries[index] = entry
                # A manual attribution is evidence about one concrete
                # security, not permission to move that spend between rules.
                # Keep the original fact and attribution record for review,
                # but do not let a mismatched entry consume the target budget.
                symbol_issue = entry_rule_symbol_issue(entry, rule)
                if symbol_issue == "missing":
                    blocking.append(f"ENTRY_RULE_SYMBOL_MISSING:{entry.entry_id}")
                    entries[index] = entry.model_copy(
                        update={"status": AttributionStatus.UNATTRIBUTED}
                    )
                    unattributed.append(entry.evidence_id)
                elif symbol_issue == "mismatch":
                    blocking.append(f"ENTRY_RULE_SYMBOL_MISMATCH:{entry.entry_id}")
                    entries[index] = entry.model_copy(
                        update={"status": AttributionStatus.UNATTRIBUTED}
                    )
                    unattributed.append(entry.evidence_id)

        # Preserve order while removing duplicate blocking codes.
        blocking = list(dict.fromkeys(blocking))
        unattributed = list(dict.fromkeys(unattributed))
        payload = {
            "account_id": plan.account_id,
            "snapshot_id": snapshot.snapshot_id,
            "plan_version": plan.version,
            "entries": [entry.model_dump(mode="json") for entry in entries],
            "attributions": [item.model_dump(mode="json") for item in attribution_records],
            "unattributed_evidence": unattributed,
            "blocking_reasons": blocking,
            "facts_fingerprint": facts_fingerprint(snapshot),
            "fact_watermark": _fact_watermark(snapshot),
            "position_watermark": [
                item.model_dump(mode="json") for item in _position_watermark(snapshot)
            ],
            "position_baseline_recorded": True,
        }
        ledger_version = hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return LedgerSnapshot(
            ledger_id=f"ledger:{plan.account_id}:{snapshot.snapshot_id}:{plan.version}",
            ledger_version=ledger_version,
            account_id=plan.account_id,
            snapshot_id=snapshot.snapshot_id,
            plan_version=plan.version,
            entries=tuple(entries),
            attributions=tuple(attribution_records),
            unattributed_evidence=tuple(unattributed),
            blocking_reasons=tuple(blocking),
            built_at=built_at,
            facts_fingerprint=facts_fingerprint(snapshot),
            fact_watermark=_fact_watermark(snapshot),
            position_watermark=_position_watermark(snapshot),
            position_baseline_recorded=True,
        )


def build_ledger(
    snapshot: AccountSnapshot,
    plan: InvestmentPlan,
    *,
    attributions: Mapping[str, Attribution | str | tuple[str, str | None]] | None = None,
    initial_entries: Sequence[LedgerEntry] = (),
    prior: LedgerSnapshot | None = None,
    clock: Any = None,
) -> LedgerSnapshot:
    return LedgerBuilder(clock=clock).build(
        snapshot,
        plan,
        attributions=attributions,
        initial_entries=initial_entries,
        prior=prior,
    )


def budget_summaries(
    ledger: LedgerSnapshot,
    plan: InvestmentPlan,
    *,
    as_of: datetime,
) -> tuple[BudgetSummary, ...]:
    result: list[BudgetSummary] = []
    for item in plan.etf_plans:
        accrued = item.accrued_amount(as_of)
        spent = ledger.spent_for(item.plan_id)
        result.append(
            BudgetSummary(
                account_id=plan.account_id,
                plan_id=item.plan_id,
                accrued=accrued,
                spent=spent,
                remaining=max(accrued - spent, ZERO),
                currency=item.currency,
                source_ledger_version=ledger.ledger_version,
            )
        )
    for item in plan.equity_layers:
        spent = ledger.spent_for(item.layer_id, layer_id=item.layer_id)
        result.append(
            BudgetSummary(
                account_id=plan.account_id,
                plan_id=item.layer_id,
                layer_id=item.layer_id,
                accrued=item.budget,
                spent=spent,
                remaining=max(item.budget - spent, ZERO),
                currency=item.currency,
                source_ledger_version=ledger.ledger_version,
            )
        )
    return tuple(result)


def order_occupation(snapshot: AccountSnapshot, *, currency: str = "USD") -> OrderOccupation:
    """Calculate known buy-side open-order occupation without double counting.

    ``CashBalances.order_reservation_included`` is intentionally required by
    the caller before a proposal can pass.  This function reports the order
    amount independently so the UI can show why an account is blocked.
    """

    amount = ZERO
    ids: list[str] = []
    uncertain: list[str] = []
    reasons: list[str] = []
    orders_by_id = {order.order_id: order for order in snapshot.orders}
    terminal_statuses = {
        OrderStatus.CANCELED,
        OrderStatus.EXPIRED,
        OrderStatus.FILLED,
        OrderStatus.REJECTED,
        OrderStatus.REPLACED,
    }
    payment_sides = {OrderSide.BUY, OrderSide.BUY_TO_COVER}

    def uncertain_order(order: Any, reason: str) -> None:
        if order.order_id not in uncertain:
            uncertain.append(order.order_id)
        reasons.append(f"{reason}:{order.order_id}")

    def known_value(order: Any) -> Decimal | None:
        if order.instrument is None or order.instrument.currency != currency:
            return None
        if order.remaining_quantity is None or order.remaining_quantity <= 0:
            return None
        if order.limit_price is None or order.limit_price.amount <= 0:
            return None
        return order.remaining_quantity * order.limit_price.amount

    # Validate replacement references and terminal residuals before using
    # ``is_open`` to release any cash.  A closed flag is not evidence that an
    # unresolved replacement or non-zero terminal remainder disappeared.
    for order in snapshot.orders:
        if order.replaces_order_id:
            predecessor = orders_by_id.get(order.replaces_order_id)
            if predecessor is None:
                uncertain_order(order, "UNKNOWN_ORDER_REPLACEMENT")
            elif predecessor.replaced_by_order_id != order.order_id:
                uncertain_order(order, "ORDER_REPLACEMENT_CHAIN_MISMATCH")
        if order.replaced_by_order_id:
            replacement = orders_by_id.get(order.replaced_by_order_id)
            if replacement is None:
                uncertain_order(order, "UNKNOWN_ORDER_REPLACEMENT")
            elif replacement.replaces_order_id != order.order_id:
                uncertain_order(order, "ORDER_REPLACEMENT_CHAIN_MISMATCH")
        if order.status is OrderStatus.UNKNOWN:
            uncertain_order(order, "UNKNOWN_ORDER_STATUS")
            continue
        if order.status is OrderStatus.REPLACED and not order.replaced_by_order_id:
            uncertain_order(order, "UNKNOWN_ORDER_REPLACEMENT")
        if order.status in terminal_statuses:
            if order.is_open:
                uncertain_order(order, "TERMINAL_ORDER_STILL_OPEN")
            if order.remaining_quantity is None:
                uncertain_order(order, "UNKNOWN_TERMINAL_REMAINING")
            elif order.remaining_quantity != ZERO:
                uncertain_order(order, "TERMINAL_ORDER_REMAINDER")

    for order in snapshot.orders:
        if not order.is_open:
            continue
        if order.side is OrderSide.UNKNOWN:
            uncertain_order(order, "UNKNOWN_ORDER_SIDE")
            continue
        if order.side not in payment_sides:
            continue
        ids.append(order.order_id)
        raw_status = (order.raw_status or "").strip().upper()
        if order.status is OrderStatus.PENDING or raw_status in {
            "PENDING_CANCEL",
            "PENDING_REPLACE",
        }:
            uncertain_order(order, "UNCONFIRMED_ORDER_CANCELLATION")
        value = known_value(order)
        if value is None:
            uncertain_order(order, "UNKNOWN_ORDER_VALUE")
            continue
        amount += value

    # Keep a conservative value in the diagnostic occupation for a terminal
    # order that still reports shares.  The blocking reason above prevents a
    # proposal from using it, while the UI still shows the cash at risk.
    for order in snapshot.orders:
        if order.is_open or order.status not in terminal_statuses:
            continue
        if order.remaining_quantity is None or order.remaining_quantity <= 0:
            continue
        if order.side not in payment_sides:
            continue
        value = known_value(order)
        if value is not None:
            amount += value
    balance = next((item for item in snapshot.account.balances if item.currency == currency), None)
    included = balance.order_reservation_included if balance is not None else None
    if included is None:
        reasons.append("UNKNOWN_ORDER_RESERVATION_SEMANTICS")
    elif balance is not None and balance.open_order_reservation is not None:
        explicit = balance.open_order_reservation
        if explicit < 0:
            reasons.append("INVALID_ORDER_RESERVATION")
        elif explicit != amount:
            reasons.append("ORDER_RESERVATION_MISMATCH")
            # Keep the larger known reservation in the report.  Proposal
            # generation will still fail closed on the mismatch code.
            amount = max(amount, explicit)
    return OrderOccupation(
        account_id=snapshot.account.account_id,
        currency=currency,
        amount=amount,
        order_ids=tuple(ids),
        uncertain_order_ids=tuple(uncertain),
        blocking_reasons=tuple(dict.fromkeys(reasons)),
        reservation_included=included,
    )


def persist_ledger(store: Any, ledger: LedgerSnapshot) -> None:
    """Persist one ledger and clear a now-stale current proposal atomically.

    Manual attribution revisions intentionally do not regenerate a proposal in
    this low-level API.  If the ledger version moves, the old proposal pointer
    is removed in the same transaction so a restart or ``show-state`` cannot
    present a ready batch calculated from superseded budget facts.
    """

    with store.transaction() as tx:
        previous_payload = tx.get("investing.ledger", ledger.account_id)
        tx.set("investing.ledger", ledger.account_id, ledger.model_dump(mode="json"))
        if previous_payload is not None:
            previous = LedgerSnapshot.model_validate(previous_payload)
            if (
                previous.snapshot_id != ledger.snapshot_id
                or previous.ledger_version != ledger.ledger_version
            ):
                tx.delete("investing.proposals.current", ledger.account_id)


def load_ledger(store: Any, account_id: str) -> LedgerSnapshot | None:
    payload = store.get_json("investing.ledger", account_id)
    if payload is None:
        return None
    return LedgerSnapshot.model_validate(payload)


__all__ = [
    "Attribution",
    "AttributionStatus",
    "BudgetSummary",
    "EntryKind",
    "LedgerBuilder",
    "LedgerEntry",
    "LedgerSnapshot",
    "OrderOccupation",
    "PositionWatermark",
    "build_ledger",
    "budget_summaries",
    "facts_fingerprint",
    "load_ledger",
    "order_occupation",
    "persist_ledger",
]
