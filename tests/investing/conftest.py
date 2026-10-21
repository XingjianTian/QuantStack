from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from quant_stack.brokerage.contracts import (
    Account,
    AccountSnapshot,
    AccountType,
    AssetType,
    CashBalances,
    Coverage,
    CurrencyAmount,
    Fill,
    Instrument,
    Order,
    OrderSide,
    OrderStatus,
    Position,
    Quote,
    Transaction,
)

NOW = datetime(2026, 10, 16, 12, 0, tzinfo=UTC)


def make_snapshot(
    *,
    snapshot_id: str = "snapshot-1",
    account_id: str = "fictional-account",
    symbol: str = "QXEN",
    cash: str = "1000.00",
    reservation_included: bool | None = True,
    positions: tuple[Position, ...] = (),
    orders: tuple[Order, ...] = (),
    transactions: tuple[Transaction, ...] = (),
    fills: tuple[Fill, ...] = (),
    quotes: tuple[Quote, ...] | None = None,
    complete: bool = True,
    conflicts: tuple[str, ...] = (),
    business_time: datetime | None = NOW,
) -> AccountSnapshot:
    instrument = Instrument(symbol=symbol, asset_type=AssetType.EQUITY, currency="USD")
    account = Account(
        account_id=account_id,
        alias="Fictional Test Account",
        account_type=AccountType.MARGIN,
        base_currency="USD",
        balances=(
            CashBalances(
                currency="USD",
                balance=Decimal(cash),
                cash=Decimal(cash),
                settled_cash=Decimal(cash),
                available_cash=Decimal(cash),
                order_reservation_included=reservation_included,
            ),
        ),
    )
    if quotes is None:
        quotes = (
            Quote(
                instrument=instrument,
                bid=CurrencyAmount(currency="USD", amount=Decimal("100.00")),
                ask=CurrencyAmount(currency="USD", amount=Decimal("101.00")),
                last=CurrencyAmount(currency="USD", amount=Decimal("100.50")),
                quote_time=NOW,
                stale_after_seconds=900,
            ),
        )
    coverage = Coverage(
        sections=("account", "positions", "orders", "transactions", "fills", "quotes"),
        complete=complete,
        source_as_of=business_time,
    )
    return AccountSnapshot(
        snapshot_id=snapshot_id,
        account=account,
        positions=positions,
        orders=orders,
        transactions=transactions,
        fills=fills,
        quotes=quotes,
        business_time=business_time,
        received_at=NOW,
        sync_batch_id=f"batch:{snapshot_id}",
        mapping_version="fictional-v1",
        coverage=coverage,
        complete=complete,
        conflicts=conflicts,
    )


def make_trade(
    *,
    transaction_id: str = "transaction-1",
    execution_id: str | None = None,
    order_id: str = "order-1",
    amount: str = "60.00",
    quantity: str = "1",
    symbol: str = "QXEN",
) -> tuple[Transaction, Fill]:
    instrument = Instrument(symbol=symbol, asset_type=AssetType.EQUITY, currency="USD")
    transaction = Transaction(
        transaction_id=transaction_id,
        account_id="fictional-account",
        transaction_type="trade",
        transaction_subtype="BUY",
        instrument=instrument,
        quantity=Decimal(quantity),
        price=CurrencyAmount(currency="USD", amount=Decimal(amount) / Decimal(quantity)),
        net_amount=CurrencyAmount(currency="USD", amount=-Decimal(amount)),
        transaction_time=NOW,
        order_id=order_id,
    )
    fill = Fill(
        execution_id=execution_id or f"{transaction_id}:execution",
        account_id="fictional-account",
        order_id=order_id,
        instrument=instrument,
        side=OrderSide.BUY,
        quantity=Decimal(quantity),
        price=transaction.price,
        amount=CurrencyAmount(currency="USD", amount=Decimal(amount)),
        executed_at=NOW,
        source_transaction_id=transaction_id,
    )
    return transaction, fill


def make_open_order(
    *,
    order_id: str = "open-order-1",
    symbol: str = "QXEN",
    remaining: str = "2",
    price: str = "101.00",
) -> Order:
    instrument = Instrument(symbol=symbol, asset_type=AssetType.EQUITY, currency="USD")
    return Order(
        order_id=order_id,
        account_id="fictional-account",
        instrument=instrument,
        side=OrderSide.BUY,
        order_type="LIMIT",
        status=OrderStatus.WORKING,
        requested_quantity=Decimal(remaining),
        filled_quantity=Decimal("0"),
        remaining_quantity=Decimal(remaining),
        limit_price=CurrencyAmount(currency="USD", amount=Decimal(price)),
        entered_at=NOW,
        updated_at=NOW,
        is_open=True,
    )


@pytest.fixture
def fictional_now() -> datetime:
    return NOW
