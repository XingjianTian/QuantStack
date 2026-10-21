"""Safe, non-executable proposal generation.

The generator is intentionally a pure calculation over normalized contracts,
versioned plans, and a versioned budget ledger.  It never constructs a broker
order payload and never calls a broker client.  Persistence helpers use one
store transaction per mutation so a current proposal replacement is atomic.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from decimal import ROUND_DOWN, Decimal
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from quant_stack.brokerage.contracts import (
    AccountSnapshot,
    AssetType,
    CashBalances,
    CurrencyAmount,
    OrderSide,
    PositionSide,
    Quote,
    SyncState,
    SyncStatus,
)
from quant_stack.brokerage.sync import SYNC_FAILURE_GENERATION_NAMESPACE

from .budgets import (
    BudgetSummary,
    LedgerSnapshot,
    OrderOccupation,
    facts_fingerprint,
    order_occupation,
)
from .plans import EquityLayer, ETFPlan, InvestmentPlan

ZERO = Decimal("0")
MAX_SNAPSHOT_AGE = timedelta(hours=24)
MAX_REVIEW_AGE = timedelta(hours=24)


class ProposalModel(BaseModel):
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


class ProposalStatus(StrEnum):
    READY = "ready"
    BLOCKED = "blocked"
    INSUFFICIENT_BUDGET = "insufficient_budget"
    INSUFFICIENT_CASH = "insufficient_cash"
    WAITING_FOR_TRIGGER = "waiting_for_trigger"
    ALREADY_RESERVED = "already_reserved"
    WAITING_REVIEW = "waiting_review"
    NO_ACTION = "no_action"
    INVALIDATED = "invalidated"


class ReviewStatus(StrEnum):
    PENDING = "pending"
    REVIEWED = "reviewed"
    INVALIDATED = "invalidated"


class BlockCode(StrEnum):
    SYNC_FAILED = "SYNC_FAILED"
    SNAPSHOT_INCOMPLETE = "SNAPSHOT_INCOMPLETE"
    SNAPSHOT_CONFLICT = "SNAPSHOT_CONFLICT"
    COVERAGE_INCOMPLETE = "COVERAGE_INCOMPLETE"
    LEDGER_UNAVAILABLE = "LEDGER_UNAVAILABLE"
    LEDGER_ACCOUNT_MISMATCH = "LEDGER_ACCOUNT_MISMATCH"
    LEDGER_SNAPSHOT_MISMATCH = "LEDGER_SNAPSHOT_MISMATCH"
    LEDGER_PLAN_MISMATCH = "LEDGER_PLAN_MISMATCH"
    UNATTRIBUTED_EVIDENCE = "UNATTRIBUTED_EVIDENCE"
    UNKNOWN_FUNDING_CURRENCY = "UNKNOWN_FUNDING_CURRENCY"
    UNKNOWN_AVAILABLE_CASH = "UNKNOWN_AVAILABLE_CASH"
    UNKNOWN_RESERVATION_SEMANTICS = "UNKNOWN_RESERVATION_SEMANTICS"
    UNKNOWN_OPEN_ORDER = "UNKNOWN_OPEN_ORDER"
    UNKNOWN_OPTION_OBLIGATION = "UNKNOWN_OPTION_OBLIGATION"
    SNAPSHOT_STALE = "SNAPSHOT_STALE"
    SNAPSHOT_FUTURE = "SNAPSHOT_FUTURE"
    QUOTE_MISSING = "QUOTE_MISSING"
    QUOTE_STALE = "QUOTE_STALE"
    QUOTE_FUTURE = "QUOTE_FUTURE"
    QUOTE_INVALID = "QUOTE_INVALID"
    CURRENCY_MISMATCH = "CURRENCY_MISMATCH"
    VERSION_DRIFT = "VERSION_DRIFT"
    PLAN_DISABLED = "PLAN_DISABLED"


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


def _utc(value: datetime | str) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamps must include a timezone")
    return value.astimezone(UTC)


def _floor_decimal(value: Decimal) -> int:
    if value <= 0:
        return 0
    return int(value.to_integral_value(rounding=ROUND_DOWN))


class ProposalItem(ProposalModel):
    item_id: str
    symbol: str
    side: OrderSide = OrderSide.BUY
    source: str
    plan_id: str
    layer_id: str | None = None
    currency: str = "USD"
    strategy_budget: Decimal = Decimal("0")
    spent_budget: Decimal = Decimal("0")
    remaining_budget: Decimal = Decimal("0")
    trigger_price: Decimal | None = None
    assumed_limit_price: Decimal | None = None
    quantity: Decimal = Decimal("0")
    gross_amount: Decimal = Decimal("0")
    estimated_fee: Decimal = Decimal("0")
    cash_reserve: Decimal = Decimal("0")
    maximum_cash_use: Decimal = Decimal("0")
    status: ProposalStatus = ProposalStatus.NO_ACTION
    reasons: tuple[str, ...] = ()
    data_references: tuple[str, ...] = ()
    quote_time: datetime | None = None
    non_executable: Literal[True] = True

    @model_validator(mode="before")
    @classmethod
    def _parse_decimals(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        values = dict(values)
        for name in (
            "strategy_budget",
            "spent_budget",
            "remaining_budget",
            "trigger_price",
            "assumed_limit_price",
            "quantity",
            "gross_amount",
            "estimated_fee",
            "cash_reserve",
            "maximum_cash_use",
        ):
            if values.get(name) is not None:
                values[name] = _dec(values[name])
        if values.get("quote_time") is not None:
            values["quote_time"] = _utc(values["quote_time"])
        return values

    @property
    def total_estimated_cost(self) -> Decimal:
        return self.gross_amount + self.estimated_fee


class ProposalBatch(ProposalModel):
    batch_id: str
    account_id: str
    snapshot_id: str
    ledger_version: str
    plan_version: int
    plan_fingerprint: str
    input_fingerprint: str
    generated_at: datetime
    items: tuple[ProposalItem, ...] = ()
    status: ProposalStatus = ProposalStatus.NO_ACTION
    blocking_reasons: tuple[str, ...] = ()
    review_status: ReviewStatus = ReviewStatus.PENDING
    reviewed_at: datetime | None = None
    reviewed_by: str | None = None
    reviewed_input_fingerprint: str | None = None
    replaced_batch_id: str | None = None
    cash_currency: str | None = None
    available_cash: Decimal | None = None
    open_order_occupation: Decimal | None = None
    sync_failure_generation: int = Field(default=0, ge=0)
    non_executable: Literal[True] = True
    fictional: Literal[True] = True

    @model_validator(mode="before")
    @classmethod
    def _parse_values(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        values = dict(values)
        for name in ("available_cash", "open_order_occupation"):
            if values.get(name) is not None:
                values[name] = _dec(values[name], allow_negative=False)
        for name in ("generated_at", "reviewed_at"):
            if values.get(name) is not None:
                values[name] = _utc(values[name])
        return values

    @property
    def current(self) -> bool:
        return self.status is not ProposalStatus.INVALIDATED and (
            self.review_status is not ReviewStatus.INVALIDATED
        )


class StaleProposalError(RuntimeError):
    """Raised when a review or replacement uses an input version that moved."""


def _currency_amount(currency: str, amount: Decimal) -> CurrencyAmount:
    return CurrencyAmount(currency=currency, amount=amount)


def _quote_map(snapshot: AccountSnapshot) -> dict[str, Quote]:
    return {quote.instrument.symbol: quote for quote in snapshot.quotes}


def _position_map(snapshot: AccountSnapshot) -> dict[str, Decimal]:
    result: dict[str, Decimal] = {}
    for position in snapshot.positions:
        quantity = position.quantity
        if position.side.value == "short":
            quantity = -abs(quantity)
        result[position.instrument.symbol] = result.get(position.instrument.symbol, ZERO) + quantity
    return result


def _open_order_remaining(snapshot: AccountSnapshot, symbol: str) -> Decimal:
    amount = ZERO
    for order in snapshot.orders:
        if order.is_open and order.instrument and order.instrument.symbol == symbol:
            if order.side is OrderSide.BUY and order.remaining_quantity is not None:
                amount += order.remaining_quantity
    return amount


def _balance(snapshot: AccountSnapshot, currency: str) -> CashBalances | None:
    balances = [item for item in snapshot.account.balances if item.currency == currency]
    return balances[0] if len(balances) == 1 else None


def _valid_quote(
    quote: Quote | None,
    *,
    as_of: datetime,
) -> tuple[Decimal | None, tuple[str, ...]]:
    if quote is None:
        return None, (BlockCode.QUOTE_MISSING.value,)
    if quote.quote_time is None:
        return None, (BlockCode.QUOTE_MISSING.value,)
    quote_time = _utc(quote.quote_time)
    if quote_time > as_of + timedelta(seconds=1):
        return None, (BlockCode.QUOTE_FUTURE.value,)
    age = (as_of - quote_time).total_seconds()
    if quote.is_stale or age > quote.stale_after_seconds:
        return None, (BlockCode.QUOTE_STALE.value,)
    # Ask is preferred because it makes the resulting limit assumption
    # conservative.  Last is accepted only when no ask exists.
    price = quote.ask or quote.last
    if price is None or price.amount <= 0:
        return None, (BlockCode.QUOTE_INVALID.value,)
    return price.amount, ()


def _quote_price_ref(quote: Quote | None) -> str:
    if quote is None or quote.quote_time is None:
        return "quote:missing"
    return f"quote:{quote.instrument.symbol}:{quote.quote_time.isoformat()}"


def _has_option_uncertainty(snapshot: AccountSnapshot) -> bool:
    for position in snapshot.positions:
        if position.instrument.asset_type is AssetType.OPTION:
            option = position.instrument.option
            if (
                option is None
                or option.contract_multiplier is None
                or (option.put_call or "").upper() not in {"PUT", "CALL"}
                or not (option.deliverable or "").strip()
                or bool(option.unknown_fields)
            ):
                return True
    for order in snapshot.orders:
        if not order.is_open or order.instrument is None:
            continue
        if order.instrument.asset_type is AssetType.OPTION:
            option = order.instrument.option
            if (
                option is None
                or option.contract_multiplier is None
                or (option.put_call or "").upper() not in {"PUT", "CALL"}
                or not (option.deliverable or "").strip()
                or bool(option.unknown_fields)
            ):
                return True
    return False


def _option_delivery_obligations(snapshot: AccountSnapshot) -> dict[str, Decimal]:
    """Return known short-put share obligations by underlying symbol."""

    obligations: dict[str, Decimal] = {}
    for position in snapshot.positions:
        instrument = position.instrument
        option = instrument.option
        if (
            instrument.asset_type is AssetType.OPTION
            and option is not None
            and option.contract_multiplier is not None
            and (option.put_call or "").upper() == "PUT"
            and position.side is PositionSide.SHORT
        ):
            obligations[option.underlying_symbol] = (
                obligations.get(option.underlying_symbol, ZERO)
                + abs(position.quantity) * option.contract_multiplier
            )
    for order in snapshot.orders:
        instrument = order.instrument
        option = instrument.option if instrument is not None else None
        if (
            order.is_open
            and instrument is not None
            and instrument.asset_type is AssetType.OPTION
            and option is not None
            and option.contract_multiplier is not None
            and (option.put_call or "").upper() == "PUT"
            and order.side in {OrderSide.SELL, OrderSide.SELL_SHORT}
            and order.remaining_quantity is not None
        ):
            obligations[option.underlying_symbol] = (
                obligations.get(option.underlying_symbol, ZERO)
                + abs(order.remaining_quantity) * option.contract_multiplier
            )
    return obligations


class ProposalGenerator:
    """Create one current, reviewable and non-executable proposal batch."""

    def __init__(self, *, clock: Any = None) -> None:
        self.clock = clock or (lambda: datetime.now(UTC))

    def _preflight(
        self,
        snapshot: AccountSnapshot,
        plan: InvestmentPlan,
        ledger: LedgerSnapshot | None,
        *,
        sync_state: SyncState | None,
        as_of: datetime,
    ) -> tuple[list[str], CashBalances | None, OrderOccupation]:
        reasons: list[str] = []
        if not plan.enabled:
            reasons.append(BlockCode.PLAN_DISABLED.value)
        if snapshot.account.account_id != plan.account_id:
            reasons.append("ACCOUNT_PLAN_MISMATCH")
        if sync_state is not None:
            if (
                not sync_state.complete
                or sync_state.last_success_snapshot_id != snapshot.snapshot_id
            ):
                reasons.append(BlockCode.SYNC_FAILED.value)
            if (
                sync_state.last_attempt_status is not None
                and sync_state.last_attempt_status.value != "succeeded"
            ):
                reasons.append(BlockCode.SYNC_FAILED.value)
        if not snapshot.complete:
            reasons.append(BlockCode.SNAPSHOT_INCOMPLETE.value)
        if snapshot.business_time is None:
            reasons.append("BUSINESS_TIME_UNKNOWN")
        else:
            snapshot_time = _utc(snapshot.business_time)
            if snapshot_time > as_of + timedelta(seconds=1):
                reasons.append(BlockCode.SNAPSHOT_FUTURE.value)
            elif as_of - snapshot_time > MAX_SNAPSHOT_AGE:
                reasons.append(BlockCode.SNAPSHOT_STALE.value)
        if snapshot.conflicts:
            reasons.extend(
                f"{BlockCode.SNAPSHOT_CONFLICT.value}:{item}" for item in snapshot.conflicts
            )
        if not snapshot.coverage.complete or snapshot.coverage.truncated:
            reasons.append(BlockCode.COVERAGE_INCOMPLETE.value)
        if ledger is None:
            reasons.append(BlockCode.LEDGER_UNAVAILABLE.value)
        else:
            if (
                ledger.account_id != plan.account_id
                or ledger.account_id != snapshot.account.account_id
            ):
                reasons.append(BlockCode.LEDGER_ACCOUNT_MISMATCH.value)
            if ledger.snapshot_id != snapshot.snapshot_id:
                reasons.append(BlockCode.LEDGER_SNAPSHOT_MISMATCH.value)
            if ledger.plan_version != plan.version:
                reasons.append(BlockCode.LEDGER_PLAN_MISMATCH.value)
            reasons.extend(ledger.blocking_reasons)
            if ledger.unattributed_evidence:
                reasons.append(BlockCode.UNATTRIBUTED_EVIDENCE.value)
        currencies = {item.currency for item in (*plan.etf_plans, *plan.equity_layers)}
        if len(currencies) != 1 or (
            next(iter(currencies), snapshot.account.base_currency)
            != snapshot.account.base_currency
        ):
            reasons.append(BlockCode.UNKNOWN_FUNDING_CURRENCY.value)
        currency = next(iter(currencies), snapshot.account.base_currency)
        if currency != snapshot.account.base_currency:
            reasons.append(BlockCode.CURRENCY_MISMATCH.value)
        balance = _balance(snapshot, currency)
        occupation = order_occupation(snapshot, currency=currency)
        if balance is None:
            reasons.append(BlockCode.UNKNOWN_FUNDING_CURRENCY.value)
        elif balance.available_cash is None:
            reasons.append(BlockCode.UNKNOWN_AVAILABLE_CASH.value)
        if occupation.reservation_included is None:
            reasons.append(BlockCode.UNKNOWN_RESERVATION_SEMANTICS.value)
        reasons.extend(occupation.blocking_reasons)
        if _has_option_uncertainty(snapshot):
            reasons.append(BlockCode.UNKNOWN_OPTION_OBLIGATION.value)
        return list(dict.fromkeys(reasons)), balance, occupation

    def generate(
        self,
        snapshot: AccountSnapshot,
        plan: InvestmentPlan,
        ledger: LedgerSnapshot | None,
        *,
        as_of: datetime | None = None,
        sync_state: SyncState | None = None,
        previous: ProposalBatch | None = None,
        sync_failure_generation: int = 0,
    ) -> ProposalBatch:
        if sync_failure_generation < 0:
            raise ValueError("sync failure generation must not be negative")
        generated_at = _utc(as_of or self.clock())
        reasons, balance, occupation = self._preflight(
            snapshot,
            plan,
            ledger,
            sync_state=sync_state,
            as_of=generated_at,
        )
        currency = snapshot.account.base_currency
        available_cash = balance.available_cash if balance is not None else None
        if available_cash is None:
            cash_pool = ZERO
        elif occupation.reservation_included is False:
            cash_pool = max(available_cash - occupation.amount, ZERO)
        else:
            cash_pool = available_cash
        quote_by_symbol = _quote_map(snapshot)
        positions = _position_map(snapshot)
        option_obligations = _option_delivery_obligations(snapshot)
        position_caps: dict[str, Decimal] = {}
        for layer in plan.equity_layers:
            prior_cap = position_caps.get(layer.symbol)
            position_caps[layer.symbol] = (
                layer.total_position_limit
                if prior_cap is None
                else min(prior_cap, layer.total_position_limit)
            )
        allocated_by_symbol: dict[str, Decimal] = {}
        summaries: dict[str, BudgetSummary] = {}
        if ledger is not None:
            for summary in (
                *self._summaries_for_etf(ledger, plan, generated_at),
                *self._summaries_for_equity(ledger, plan),
            ):
                summaries[summary.plan_id] = summary

        # Deterministic shared cash ordering: equity layers first by explicit
        # sequence, then ETF plans by priority and plan id.
        rules: list[tuple[int, str, ETFPlan | EquityLayer]] = [
            (item.sequence, item.layer_id, item) for item in plan.equity_layers if item.enabled
        ]
        rules.extend((item.priority, item.plan_id, item) for item in plan.etf_plans)
        rules.sort(key=lambda item: (item[0], item[1]))

        items: list[ProposalItem] = []
        for _, rule_id, rule in rules:
            is_etf = isinstance(rule, ETFPlan)
            source = f"etf:{rule.plan_id}" if is_etf else f"equity:{rule.layer_id}"
            summary = summaries.get(rule_id)
            spent = summary.spent if summary else ZERO
            budget = summary.accrued if summary else (rule.budget if not is_etf else ZERO)
            remaining_budget = summary.remaining if summary else ZERO
            quote = quote_by_symbol.get(rule.symbol)
            price, quote_reasons = _valid_quote(quote, as_of=generated_at)
            if quote is not None and quote.instrument.currency != rule.currency:
                quote_reasons = (*quote_reasons, BlockCode.CURRENCY_MISMATCH.value)
            item_reasons = list(reasons)
            item_reasons.extend(quote_reasons)
            refs = [
                f"snapshot:{snapshot.snapshot_id}",
                f"plan:{plan.version}",
                _quote_price_ref(quote),
            ]
            status = ProposalStatus.NO_ACTION
            trigger_price = None if is_etf else rule.trigger_price
            if quote_reasons:
                status = ProposalStatus.BLOCKED
            elif reasons:
                status = ProposalStatus.BLOCKED
            elif price is not None and not is_etf and price > rule.trigger_price:
                status = ProposalStatus.WAITING_FOR_TRIGGER
                item_reasons.append("TRIGGER_NOT_MET")
            elif remaining_budget <= 0:
                status = ProposalStatus.INSUFFICIENT_BUDGET
                item_reasons.append("NO_REMAINING_BUDGET")
            elif _open_order_remaining(snapshot, rule.symbol) > 0:
                status = ProposalStatus.ALREADY_RESERVED
                item_reasons.append("OPEN_ORDER_EXISTS")
            elif price is None:
                status = ProposalStatus.BLOCKED
            elif (
                rule.symbol in position_caps
                and positions.get(rule.symbol, ZERO)
                + option_obligations.get(rule.symbol, ZERO)
                >= position_caps[rule.symbol]
            ):
                status = ProposalStatus.INSUFFICIENT_BUDGET
                item_reasons.append(
                    "OPTION_DELIVERY_CAP"
                    if option_obligations.get(rule.symbol, ZERO) > 0
                    else "POSITION_LIMIT_REACHED"
                )

            fee = rule.fee_per_order
            reserve = rule.cash_reserve
            if is_etf:
                configured_price = rule.limit_price
            else:
                configured_price = rule.limit_price
            limit_price = configured_price or price
            if (
                status in {ProposalStatus.NO_ACTION, ProposalStatus.WAITING_FOR_TRIGGER}
                and limit_price is not None
            ):
                # Equity rules waiting for trigger remain quantity zero, while
                # all cash calculations use the same explicit limit formula.
                if status is ProposalStatus.WAITING_FOR_TRIGGER:
                    quantity = 0
                else:
                    budget_headroom = remaining_budget - fee - reserve
                    cash_headroom = cash_pool - fee - reserve
                    budget_quantity = _floor_decimal(budget_headroom / limit_price)
                    cash_quantity = _floor_decimal(cash_headroom / limit_price)
                    quantity_int = min(budget_quantity, cash_quantity)
                    if rule.symbol in position_caps:
                        cap = max(
                            position_caps[rule.symbol]
                            - positions.get(rule.symbol, ZERO)
                            - option_obligations.get(rule.symbol, ZERO)
                            - allocated_by_symbol.get(rule.symbol, ZERO),
                            ZERO,
                        )
                        # Open buy orders are already dealt with above.  Keep
                        # any pre-existing fractional position and known put
                        # delivery obligation in the shared cap.
                        quantity_int = min(quantity_int, _floor_decimal(cap))
                    quantity = Decimal(quantity_int)
                    if quantity <= 0:
                        status = (
                            ProposalStatus.INSUFFICIENT_CASH
                            if cash_quantity <= 0
                            else ProposalStatus.INSUFFICIENT_BUDGET
                        )
                        item_reasons.append(
                            "INSUFFICIENT_CASH" if cash_quantity <= 0 else "INSUFFICIENT_BUDGET"
                        )
                    else:
                        status = (
                            ProposalStatus.WAITING_REVIEW
                            if (not is_etf and rule.require_review)
                            else ProposalStatus.READY
                        )
            else:
                quantity = ZERO
            gross = quantity * (limit_price or ZERO)
            total_cash = gross + (fee if quantity > 0 else ZERO) + (
                reserve if quantity > 0 else ZERO
            )
            if quantity > 0:
                cash_pool = max(cash_pool - total_cash, ZERO)
                allocated_by_symbol[rule.symbol] = (
                    allocated_by_symbol.get(rule.symbol, ZERO) + quantity
                )
            remaining_after = max(remaining_budget - gross - (fee if quantity > 0 else ZERO), ZERO)
            item_id = f"item:{rule_id}"
            items.append(
                ProposalItem(
                    item_id=item_id,
                    symbol=rule.symbol,
                    source=source,
                    plan_id=rule.plan_id if is_etf else rule.layer_id,
                    layer_id=None if is_etf else rule.layer_id,
                    currency=rule.currency,
                    strategy_budget=budget,
                    spent_budget=spent,
                    remaining_budget=remaining_after,
                    trigger_price=trigger_price,
                    assumed_limit_price=limit_price,
                    quantity=quantity,
                    gross_amount=gross,
                    estimated_fee=fee if quantity > 0 else ZERO,
                    cash_reserve=reserve if quantity > 0 else ZERO,
                    maximum_cash_use=total_cash,
                    status=status,
                    reasons=tuple(dict.fromkeys(item_reasons)),
                    data_references=tuple(refs),
                    quote_time=quote.quote_time if quote else None,
                )
            )

        item_blocking_reasons = tuple(
            dict.fromkeys(
                reason
                for item in items
                if item.status is ProposalStatus.BLOCKED
                for reason in item.reasons
            )
        )
        batch_blocking_reasons = tuple(dict.fromkeys((*reasons, *item_blocking_reasons)))
        payload = {
            "account_id": plan.account_id,
            "snapshot_id": snapshot.snapshot_id,
            "ledger_version": ledger.ledger_version if ledger else "missing",
            "plan_version": plan.version,
            "plan_fingerprint": plan.fingerprint,
            "sync_failure_generation": sync_failure_generation,
            "as_of": generated_at.isoformat(),
            "items": [item.model_dump(mode="json") for item in items],
            "blocking_reasons": batch_blocking_reasons,
        }
        input_fingerprint = hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        batch_id = f"proposal:{input_fingerprint}"
        if previous is not None and previous.input_fingerprint == input_fingerprint:
            replaced = previous.replaced_batch_id
            review_status = previous.review_status
            reviewed_at = previous.reviewed_at
            reviewed_by = previous.reviewed_by
            reviewed_input = previous.reviewed_input_fingerprint
        else:
            replaced = previous.batch_id if previous is not None else None
            review_status = ReviewStatus.PENDING
            reviewed_at = None
            reviewed_by = None
            reviewed_input = None
        if reasons or any(item.status is ProposalStatus.BLOCKED for item in items):
            batch_status = ProposalStatus.BLOCKED
        elif any(
            item.status in {ProposalStatus.READY, ProposalStatus.WAITING_REVIEW}
            for item in items
        ):
            batch_status = ProposalStatus.READY
        elif any(item.status is ProposalStatus.WAITING_FOR_TRIGGER for item in items):
            batch_status = ProposalStatus.WAITING_FOR_TRIGGER
        else:
            batch_status = ProposalStatus.NO_ACTION
        return ProposalBatch(
            batch_id=batch_id,
            account_id=plan.account_id,
            snapshot_id=snapshot.snapshot_id,
            ledger_version=ledger.ledger_version if ledger else "missing",
            plan_version=plan.version,
            plan_fingerprint=plan.fingerprint,
            input_fingerprint=input_fingerprint,
            generated_at=generated_at,
            items=tuple(items),
            status=batch_status,
            blocking_reasons=batch_blocking_reasons,
            review_status=review_status,
            reviewed_at=reviewed_at,
            reviewed_by=reviewed_by,
            reviewed_input_fingerprint=reviewed_input,
            replaced_batch_id=replaced,
            cash_currency=currency,
            available_cash=available_cash,
            open_order_occupation=occupation.amount,
            sync_failure_generation=sync_failure_generation,
        )

    @staticmethod
    def _summaries_for_etf(
        ledger: LedgerSnapshot,
        plan: InvestmentPlan,
        as_of: datetime,
    ) -> tuple[BudgetSummary, ...]:
        return tuple(
            BudgetSummary(
                account_id=plan.account_id,
                plan_id=item.plan_id,
                accrued=item.accrued_amount(as_of),
                spent=ledger.spent_for(item.plan_id),
                remaining=max(item.accrued_amount(as_of) - ledger.spent_for(item.plan_id), ZERO),
                currency=item.currency,
                source_ledger_version=ledger.ledger_version,
            )
            for item in plan.etf_plans
        )

    @staticmethod
    def _summaries_for_equity(
        ledger: LedgerSnapshot,
        plan: InvestmentPlan,
    ) -> tuple[BudgetSummary, ...]:
        return tuple(
            BudgetSummary(
                account_id=plan.account_id,
                plan_id=item.layer_id,
                layer_id=item.layer_id,
                accrued=item.budget,
                spent=ledger.spent_for(item.layer_id, layer_id=item.layer_id),
                remaining=max(
                    item.budget - ledger.spent_for(item.layer_id, layer_id=item.layer_id),
                    ZERO,
                ),
                currency=item.currency,
                source_ledger_version=ledger.ledger_version,
            )
            for item in plan.equity_layers
        )


def generate_proposals(
    snapshot: AccountSnapshot,
    plan: InvestmentPlan,
    ledger: LedgerSnapshot | None,
    *,
    as_of: datetime | None = None,
    sync_state: SyncState | None = None,
    previous: ProposalBatch | None = None,
    clock: Any = None,
    sync_failure_generation: int = 0,
) -> ProposalBatch:
    return ProposalGenerator(clock=clock).generate(
        snapshot,
        plan,
        ledger,
        as_of=as_of,
        sync_state=sync_state,
        previous=previous,
        sync_failure_generation=sync_failure_generation,
    )


def review_proposal(
    batch: ProposalBatch,
    *,
    reviewer: str,
    expected_input_fingerprint: str,
    at: datetime | None = None,
) -> ProposalBatch:
    if not reviewer.strip():
        raise ValueError("reviewer is required")
    if batch.input_fingerprint != expected_input_fingerprint:
        raise StaleProposalError("proposal input version changed before review")
    if (
        batch.status is ProposalStatus.INVALIDATED
        or batch.review_status is ReviewStatus.INVALIDATED
    ):
        raise StaleProposalError("an invalidated proposal cannot be reviewed")
    if batch.status is ProposalStatus.BLOCKED:
        raise StaleProposalError("a blocked proposal cannot be approved by review")
    reviewed_at = _utc(at or datetime.now(UTC))
    if reviewed_at < batch.generated_at - timedelta(seconds=1):
        raise StaleProposalError("proposal review time predates proposal generation")
    if reviewed_at - batch.generated_at > MAX_REVIEW_AGE:
        raise StaleProposalError("proposal is too old to review")
    return batch.model_copy(
        update={
            "review_status": ReviewStatus.REVIEWED,
            "reviewed_at": reviewed_at,
            "reviewed_by": reviewer,
            "reviewed_input_fingerprint": expected_input_fingerprint,
        }
    )


def persist_proposal(
    store: Any,
    batch: ProposalBatch,
    *,
    commit_at: datetime | None = None,
) -> None:
    """Atomically replace a current batch after revalidating live sync state."""

    with store.transaction() as tx:
        _validate_commit_view(tx, batch, commit_at=commit_at)
        tx.set("investing.proposals", batch.batch_id, batch.model_dump(mode="json"))
        tx.set("investing.proposals.current", batch.account_id, batch.batch_id)


def persist_plan(store: Any, plan: InvestmentPlan) -> None:
    """Persist one immutable plan version for commit-time validation."""

    with store.transaction() as tx:
        previous_payload = tx.get("investing.plans", plan.account_id)
        tx.set("investing.plans", plan.account_id, plan.model_dump(mode="json"))
        if previous_payload is not None:
            previous = InvestmentPlan.model_validate(previous_payload)
            if previous.fingerprint != plan.fingerprint:
                tx.delete("investing.proposals.current", plan.account_id)


def load_plan(store: Any, account_id: str) -> InvestmentPlan | None:
    payload = store.get_json("investing.plans", account_id)
    return None if payload is None else InvestmentPlan.model_validate(payload)


def _transaction_view(tx: Any, account_id: str) -> tuple[SyncState, AccountSnapshot]:
    """Read sync state and current snapshot without taking a second lock."""

    state_reader = getattr(tx, "state", None)
    snapshot_reader = getattr(tx, "latest", None)
    if not callable(state_reader) or not callable(snapshot_reader):
        raise RuntimeError(
            "snapshot store transaction must expose state(account_id) and latest(account_id)"
        )
    state = state_reader(account_id)
    snapshot = snapshot_reader(account_id)
    if snapshot is None:
        raise StaleProposalError("current synchronized snapshot is unavailable")
    return state, snapshot


def _transaction_facts_view(tx: Any, snapshot: AccountSnapshot) -> AccountSnapshot:
    """Compose the current snapshot with the store's cumulative fact waterline."""

    reader = getattr(tx, "facts", None) or getattr(tx, "read_facts", None)
    if not callable(reader):
        raise StaleProposalError("transaction facts view is unavailable")
    try:
        values = {
            fact_type: tuple(reader(snapshot.account.account_id, fact_type=fact_type))
            for fact_type in ("position", "order", "transaction", "fill")
        }
    except (TypeError, ValueError, RuntimeError) as exc:
        raise StaleProposalError("current cumulative facts are unavailable") from exc
    return snapshot.model_copy(
        update={
            "positions": values["position"],
            "orders": values["order"],
            "transactions": values["transaction"],
            "fills": values["fill"],
        }
    )


def _validate_commit_view(
    tx: Any,
    batch: ProposalBatch,
    *,
    ledger: LedgerSnapshot | None = None,
    plan: InvestmentPlan | None = None,
    expected_prior_ledger: LedgerSnapshot | None = None,
    commit_at: datetime | None = None,
) -> None:
    validation_at = _utc(commit_at or datetime.now(UTC))
    state, snapshot = _transaction_view(tx, batch.account_id)
    stored_failure_generation = tx.get(
        SYNC_FAILURE_GENERATION_NAMESPACE, batch.account_id, 0
    )
    try:
        stored_failure_generation = int(stored_failure_generation)
    except (TypeError, ValueError) as exc:
        raise StaleProposalError("sync failure generation is invalid") from exc
    if stored_failure_generation < 0:
        raise StaleProposalError("sync failure generation is negative")
    if batch.sync_failure_generation != stored_failure_generation:
        raise StaleProposalError("sync failure generation changed before commit")
    if (
        not state.complete
        or state.last_attempt_status is not SyncStatus.SUCCEEDED
        or state.last_success_snapshot_id != batch.snapshot_id
        or snapshot.snapshot_id != batch.snapshot_id
        or not snapshot.complete
        or snapshot.conflicts
        or not snapshot.coverage.complete
    ):
        raise StaleProposalError("proposal snapshot is no longer the complete current sync")
    if snapshot.business_time is None:
        raise StaleProposalError("current synchronized snapshot has no business time")
    snapshot_time = _utc(snapshot.business_time)
    if snapshot_time > validation_at + timedelta(seconds=1):
        raise StaleProposalError("current synchronized snapshot is newer than commit time")
    if validation_at - snapshot_time > MAX_SNAPSHOT_AGE:
        raise StaleProposalError("current synchronized snapshot is too old to commit")

    stored_plan = tx.get("investing.plans", batch.account_id)
    if plan is not None:
        if plan.account_id != batch.account_id:
            raise StaleProposalError("proposal plan account changed before commit")
        if plan.version != batch.plan_version or plan.fingerprint != batch.plan_fingerprint:
            raise StaleProposalError("proposal plan version changed before commit")
        if stored_plan is not None:
            persisted_plan = InvestmentPlan.model_validate(stored_plan)
            if persisted_plan.fingerprint != plan.fingerprint:
                raise StaleProposalError("stored plan changed before commit")
    elif stored_plan is None:
        raise StaleProposalError("proposal plan is not persisted")
    elif InvestmentPlan.model_validate(stored_plan).fingerprint != batch.plan_fingerprint:
        raise StaleProposalError("proposal plan version changed before commit")

    stored_ledger = tx.get("investing.ledger", batch.account_id)
    if ledger is not None:
        if ledger.snapshot_id != batch.snapshot_id or ledger.ledger_version != batch.ledger_version:
            raise StaleProposalError("ledger and proposal versions differ before commit")
    candidate_ledger: LedgerSnapshot | None = None
    if expected_prior_ledger is not None:
        if expected_prior_ledger.account_id != batch.account_id:
            raise StaleProposalError("expected prior ledger account changed before commit")
        if stored_ledger is None:
            raise StaleProposalError("expected prior ledger is unavailable")
        stored = LedgerSnapshot.model_validate(stored_ledger)
        if (
            stored.account_id != expected_prior_ledger.account_id
            or stored.snapshot_id != expected_prior_ledger.snapshot_id
            or stored.ledger_version != expected_prior_ledger.ledger_version
        ):
            raise StaleProposalError("current ledger changed before commit")
        candidate_ledger = LedgerSnapshot.model_validate(ledger) if ledger is not None else None
    elif stored_ledger is None:
        if ledger is None:
            raise StaleProposalError("current ledger is unavailable")
        candidate_ledger = ledger
    else:
        stored = LedgerSnapshot.model_validate(stored_ledger)
        if (
            stored.snapshot_id != batch.snapshot_id
            or stored.ledger_version != batch.ledger_version
        ):
            raise StaleProposalError(
                "current ledger changed before commit; expected_prior_ledger is required"
            )
        candidate_ledger = stored if ledger is None else ledger

    if candidate_ledger is None:
        raise StaleProposalError("current ledger is unavailable")
    if candidate_ledger.facts_fingerprint is None:
        raise StaleProposalError("current ledger facts version is unavailable")
    current_facts = _transaction_facts_view(tx, snapshot)
    if candidate_ledger.facts_fingerprint != facts_fingerprint(current_facts):
        raise StaleProposalError("current cumulative facts changed before commit")

    quote_by_symbol = _quote_map(snapshot)
    for item in batch.items:
        if item.status not in {ProposalStatus.READY, ProposalStatus.WAITING_REVIEW}:
            continue
        _, reasons = _valid_quote(quote_by_symbol.get(item.symbol), as_of=validation_at)
        if reasons:
            raise StaleProposalError(
                f"proposal quote is no longer usable: {','.join(reasons)}"
            )


def persist_ledger_and_proposal(
    store: Any,
    ledger: LedgerSnapshot,
    batch: ProposalBatch,
    *,
    plan: InvestmentPlan | None = None,
    expected_prior_ledger: LedgerSnapshot | None = None,
    commit_at: datetime | None = None,
) -> None:
    """Commit a matching ledger and current proposal in one store transaction.

    This helper intentionally uses only the transaction view.  It never calls
    ``store.state()``, ``store.latest()`` or another lock-taking method while
    the transaction is open.
    """

    if ledger.account_id != batch.account_id:
        raise ValueError("ledger and proposal accounts must match")
    if ledger.snapshot_id != batch.snapshot_id:
        raise StaleProposalError("ledger and proposal snapshot versions differ")
    if ledger.ledger_version != batch.ledger_version:
        raise StaleProposalError("ledger and proposal ledger versions differ")
    with store.transaction() as tx:
        _validate_commit_view(
            tx,
            batch,
            ledger=ledger,
            plan=plan,
            expected_prior_ledger=expected_prior_ledger,
            commit_at=commit_at,
        )
        if plan is not None:
            tx.set("investing.plans", plan.account_id, plan.model_dump(mode="json"))
        tx.set("investing.ledger", ledger.account_id, ledger.model_dump(mode="json"))
        tx.set("investing.proposals", batch.batch_id, batch.model_dump(mode="json"))
        tx.set("investing.proposals.current", batch.account_id, batch.batch_id)


def load_proposal(store: Any, batch_id: str) -> ProposalBatch | None:
    payload = store.get_json("investing.proposals", batch_id)
    return None if payload is None else ProposalBatch.model_validate(payload)


def load_current_proposal(store: Any, account_id: str) -> ProposalBatch | None:
    batch_id = store.get_json("investing.proposals.current", account_id)
    if batch_id is None:
        return None
    return load_proposal(store, str(batch_id))


__all__ = [
    "BlockCode",
    "ProposalBatch",
    "ProposalGenerator",
    "ProposalItem",
    "ProposalStatus",
    "ReviewStatus",
    "StaleProposalError",
    "generate_proposals",
    "load_current_proposal",
    "load_plan",
    "load_proposal",
    "persist_ledger_and_proposal",
    "persist_proposal",
    "persist_plan",
    "review_proposal",
]
