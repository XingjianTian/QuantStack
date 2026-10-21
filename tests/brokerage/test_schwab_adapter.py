from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from quant_stack.brokerage.contracts import AssetType, OrderStatus
from quant_stack.brokerage.schwab_adapter import (
    FixtureTransportError,
    SchwabAdapter,
    SchwabResponseError,
)

FIXTURE_ROOT = Path(__file__).parents[1] / "fixtures" / "brokerage"
NOW = datetime(2026, 10, 16, tzinfo=UTC)


def _payload(name: str):
    return json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))


def _adapter(*, quotes: str = "normal_quotes.json") -> SchwabAdapter:
    return SchwabAdapter.from_fixture(
        account_hash="fictional-account-hash",
        payloads={
            "account": _payload("normal_account.json"),
            "orders": _payload("normal_orders.json"),
            "transactions": _payload("normal_transactions.json"),
            "quotes": _payload(quotes),
            "coverage": _payload("normal_coverage.json"),
        },
        clock=lambda: NOW,
    )


def test_fixture_adapter_uses_real_sdk_and_normalizes_all_read_sections() -> None:
    adapter = _adapter()
    try:
        data = adapter.read_snapshot(
            "fictional-account-hash",
            account_id="fictional-account",
            received_at=NOW,
        )
    finally:
        adapter.close()

    assert data.account.alias == "Fictional Margin Sample"
    assert data.account.balances[0].cash == Decimal("500.00")
    assert data.account.balances[0].buying_power == Decimal("980.00")
    assert any(item.quantity == Decimal("1.5") for item in data.positions)
    option = next(item for item in data.positions if item.instrument.asset_type is AssetType.OPTION)
    assert option.instrument.option is not None
    assert option.instrument.option.contract_multiplier is None
    order = next(item for item in data.orders if item.order_id == "SAMPLE-ORDER-001")
    assert order.status is OrderStatus.PARTIALLY_FILLED
    assert order.remaining_quantity == Decimal("3")
    assert len(data.transactions) == 2
    assert len(data.fills) == 1
    assert len(data.quotes) == 2
    cash_receipt = next(
        item for item in data.transactions if item.transaction_id == "SAMPLE-CASH-001"
    )
    assert cash_receipt.instrument is None
    assert cash_receipt.quantity is None
    assert cash_receipt.unknown_fields == ()
    assert data.business_time == NOW
    assert data.coverage.source_as_of == NOW
    assert data.coverage.complete is True


def test_fixture_transport_rejects_unknown_routes_and_non_get() -> None:
    adapter = _adapter()
    try:
        with pytest.raises(FixtureTransportError):
            adapter._client.session.get("https://api.schwabapi.com/trader/v1/orders")
        with pytest.raises(FixtureTransportError):
            adapter._client.session.post(
                "https://api.schwabapi.com/trader/v1/accounts/fictional-account-hash/orders",
                json={"orderType": "LIMIT"},
            )
        with pytest.raises(FixtureTransportError):
            adapter._client.session.get("http://api.schwabapi.com/trader/v1/accounts/x")
        with pytest.raises(FixtureTransportError):
            adapter._client.session.get("https://other.example/trader/v1/accounts/x")
        with pytest.raises(FixtureTransportError):
            adapter._client.session.get("https://api.schwabapi.com:444/trader/v1/accounts/x")
    finally:
        adapter.close()


def test_adapter_rejects_a_client_with_an_uncontrolled_transport() -> None:
    class UncontrolledClient:
        session = httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200)))

    try:
        with pytest.raises(FixtureTransportError):
            SchwabAdapter(UncontrolledClient())  # type: ignore[arg-type]
    finally:
        UncontrolledClient.session.close()


def test_fixture_requests_are_get_only_and_use_bounded_windows() -> None:
    adapter = _adapter()
    try:
        adapter.read_snapshot(
            "fictional-account-hash",
            account_id="fictional-account",
            received_at=NOW,
        )
        transport = adapter._client.session._transport
        requests = transport.requests
    finally:
        adapter.close()

    assert requests
    assert all(request.method == "GET" for request in requests)
    order_request = next(request for request in requests if request.url.path.endswith("/orders"))
    transaction_request = next(
        request for request in requests if request.url.path.endswith("/transactions")
    )
    assert order_request.url.params.get("fromEnteredTime")
    assert order_request.url.params.get("toEnteredTime")
    assert transaction_request.url.params.get("startDate")
    assert transaction_request.url.params.get("endDate")


def test_missing_funding_fields_remain_unknown() -> None:
    adapter = SchwabAdapter.from_fixture(
        account_hash="fictional-account-hash",
        payloads={
            "account": _payload("missing_balance_account.json"),
            "orders": [],
            "transactions": [],
            "quotes": {},
        },
        clock=lambda: NOW,
    )
    try:
        account = adapter.read_account("fictional-account-hash", account_id="fictional-account")
    finally:
        adapter.close()
    balances = account.balances[0]
    assert balances.cash == Decimal("500.00")
    assert balances.buying_power is None
    assert "buyingPower" in balances.unknown_fields


def test_stale_quote_is_explicitly_marked() -> None:
    adapter = _adapter(quotes="stale_quotes.json")
    try:
        quotes = adapter.read_quotes(("QXEN",), quote_time=NOW)
    finally:
        adapter.close()
    assert quotes[0].is_stale is True


def test_missing_coverage_and_quote_time_fail_closed_without_received_time_fallback() -> None:
    quotes = _payload("normal_quotes.json")
    del quotes["QXEN"]["quote"]["quoteTime"]
    adapter = SchwabAdapter.from_fixture(
        account_hash="fictional-account-hash",
        payloads={
            "account": _payload("normal_account.json"),
            "orders": _payload("normal_orders.json"),
            "transactions": _payload("normal_transactions.json"),
            "quotes": quotes,
        },
        clock=lambda: NOW,
    )
    try:
        data = adapter.read_snapshot(
            "fictional-account-hash",
            account_id="fictional-account",
            received_at=NOW,
        )
    finally:
        adapter.close()

    missing_time_quote = next(item for item in data.quotes if item.instrument.symbol == "QXEN")
    assert missing_time_quote.quote_time is None
    assert missing_time_quote.is_stale is True
    assert data.business_time is None
    assert data.coverage.complete is False
    assert "coverage:metadata_missing" in data.conflicts
    assert "quote:QXEN:quote_time_missing" in data.conflicts


def test_unknown_order_structure_is_retained_but_blocks_snapshot() -> None:
    orders = _payload("normal_orders.json")
    orders[0]["orderLegCollection"].append(
        {
            "instruction": "SELL",
            "quantity": "1",
            "instrument": {"assetType": "EQUITY", "symbol": "QXEN", "currency": "USD"},
        }
    )
    orders[0]["childOrderStrategies"] = [{"orderId": "SAMPLE-CHILD-001"}]
    adapter = SchwabAdapter.from_fixture(
        account_hash="fictional-account-hash",
        payloads={
            "account": _payload("normal_account.json"),
            "orders": orders,
            "transactions": _payload("normal_transactions.json"),
            "quotes": _payload("normal_quotes.json"),
            "coverage": _payload("normal_coverage.json"),
        },
        clock=lambda: NOW,
    )
    try:
        data = adapter.read_snapshot(
            "fictional-account-hash",
            account_id="fictional-account",
            received_at=NOW,
        )
    finally:
        adapter.close()

    order = next(item for item in data.orders if item.order_id == "SAMPLE-ORDER-001")
    assert order.child_order_ids == ("SAMPLE-CHILD-001",)
    assert "multi_leg_order" in order.unknown_fields
    assert "child_order_strategies" in order.unknown_fields
    assert data.coverage.complete is False
    assert "order:SAMPLE-ORDER-001:structure_unknown" in data.conflicts


@pytest.mark.parametrize(
    ("transaction_type", "net_amount"),
    [
        ("TRADE", "-50.00"),
        ("TRADE", "0.00"),
        ("UNKNOWN_NON_CASH", "0.00"),
    ],
)
def test_missing_non_cash_transaction_details_are_retained_and_block_snapshot(
    transaction_type: str, net_amount: str
) -> None:
    transaction_id = f"SAMPLE-MISSING-DETAIL-{transaction_type}-{net_amount}"
    transactions = _payload("normal_transactions.json")
    transactions.append(
        {
            "transactionId": transaction_id,
            "type": transaction_type,
            "transactionDate": "2026-10-16T00:00:00+00:00",
            "netAmount": net_amount,
        }
    )
    adapter = SchwabAdapter.from_fixture(
        account_hash="fictional-account-hash",
        payloads={
            "account": _payload("normal_account.json"),
            "orders": _payload("normal_orders.json"),
            "transactions": transactions,
            "quotes": _payload("normal_quotes.json"),
            "coverage": _payload("normal_coverage.json"),
        },
        clock=lambda: NOW,
    )
    try:
        data = adapter.read_snapshot(
            "fictional-account-hash",
            account_id="fictional-account",
            received_at=NOW,
        )
    finally:
        adapter.close()

    transaction = next(
        item for item in data.transactions if item.transaction_id == transaction_id
    )
    assert transaction.instrument is None
    assert transaction.quantity is None
    assert "transaction_shape" in transaction.unknown_fields
    assert data.coverage.complete is False
    assert f"transaction:{transaction_id}:shape_unknown" in data.conflicts


@pytest.mark.parametrize("missing_field", ["underlyingSymbol", "deliverable"])
def test_option_source_gaps_are_explicitly_unknown(
    missing_field: str,
) -> None:
    account = _payload("normal_account.json")
    position = account["securitiesAccount"]["positions"][1]
    position["longQuantity"] = "0"
    position["shortQuantity"] = "1"
    option = position["instrument"]
    option.update(
        {
            "underlyingSymbol": "XYZ",
            "putCall": "PUT",
            "contractMultiplier": "100",
            "deliverable": "100 shares of XYZ",
        }
    )
    if missing_field == "underlyingSymbol":
        option.pop("underlyingSymbol")
    else:
        option["deliverable"] = "cash settled at provider discretion"

    adapter = SchwabAdapter.from_fixture(
        account_hash="fictional-account-hash",
        payloads={
            "account": account,
            "orders": _payload("normal_orders.json"),
            "transactions": _payload("normal_transactions.json"),
            "quotes": _payload("normal_quotes.json"),
            "coverage": _payload("normal_coverage.json"),
        },
        clock=lambda: NOW,
    )
    try:
        data = adapter.read_snapshot(
            "fictional-account-hash",
            account_id="fictional-account",
            received_at=NOW,
        )
    finally:
        adapter.close()

    position = next(
        item for item in data.positions if item.position_id == "SAMPLE-POS-OPTION"
    )
    normalized_option = position.instrument.option
    assert normalized_option is not None
    assert missing_field in normalized_option.unknown_fields
    if missing_field == "underlyingSymbol":
        assert normalized_option.underlying_symbol != position.instrument.symbol
        assert normalized_option.underlying_symbol == "<unknown>"
    else:
        assert normalized_option.underlying_symbol == "XYZ"


@pytest.mark.parametrize("deliverable", [None, "", "   "])
def test_option_missing_or_blank_deliverable_is_explicitly_unknown(
    deliverable: str | None,
) -> None:
    account = _payload("normal_account.json")
    position = account["securitiesAccount"]["positions"][1]
    option = position["instrument"]
    option.update(
        {
            "underlyingSymbol": "XYZ",
            "putCall": "PUT",
            "contractMultiplier": "100",
        }
    )
    if deliverable is None:
        option.pop("deliverable", None)
    else:
        option["deliverable"] = deliverable

    adapter = SchwabAdapter.from_fixture(
        account_hash="fictional-account-hash",
        payloads={
            "account": account,
            "orders": _payload("normal_orders.json"),
            "transactions": _payload("normal_transactions.json"),
            "quotes": _payload("normal_quotes.json"),
            "coverage": _payload("normal_coverage.json"),
        },
        clock=lambda: NOW,
    )
    try:
        data = adapter.read_snapshot(
            "fictional-account-hash",
            account_id="fictional-account",
            received_at=NOW,
        )
    finally:
        adapter.close()

    normalized = next(
        item for item in data.positions if item.position_id == "SAMPLE-POS-OPTION"
    ).instrument.option
    assert normalized is not None
    assert normalized.deliverable is None
    assert "deliverable" in normalized.unknown_fields


@pytest.mark.parametrize(
    "deliverable",
    ["100 shares of XYZ", "100 XYZ shares", " 100 shares of XYZ "],
)
def test_only_explicit_standard_option_deliverable_is_recognized(
    deliverable: str,
) -> None:
    account = _payload("normal_account.json")
    position = account["securitiesAccount"]["positions"][1]
    position["instrument"].update(
        {
            "underlyingSymbol": "XYZ",
            "putCall": "PUT",
            "contractMultiplier": "100",
            "deliverable": deliverable,
        }
    )

    adapter = SchwabAdapter.from_fixture(
        account_hash="fictional-account-hash",
        payloads={
            "account": account,
            "orders": _payload("normal_orders.json"),
            "transactions": _payload("normal_transactions.json"),
            "quotes": _payload("normal_quotes.json"),
            "coverage": _payload("normal_coverage.json"),
        },
        clock=lambda: NOW,
    )
    try:
        data = adapter.read_snapshot(
            "fictional-account-hash",
            account_id="fictional-account",
            received_at=NOW,
        )
    finally:
        adapter.close()

    normalized = next(
        item for item in data.positions if item.position_id == "SAMPLE-POS-OPTION"
    ).instrument.option
    assert normalized is not None
    assert normalized.deliverable == deliverable.strip()
    assert "deliverable" not in normalized.unknown_fields


def test_fixture_identity_binds_account_id_and_hash() -> None:
    adapter = _adapter()
    try:
        with pytest.raises(SchwabResponseError, match="account identity"):
            adapter.read_snapshot(
                "fictional-account-hash",
                account_id="another-account",
                received_at=NOW,
            )
        with pytest.raises(FixtureTransportError, match="account hash"):
            adapter.read_account("another-account-hash", account_id="fictional-account")
    finally:
        adapter.close()


def test_provider_identity_mismatch_is_rejected() -> None:
    account = _payload("normal_account.json")
    account["securitiesAccount"]["accountId"] = "external-account"
    adapter = SchwabAdapter.from_fixture(
        account_hash="fictional-account-hash",
        payloads={
            "account": account,
            "orders": _payload("normal_orders.json"),
            "transactions": _payload("normal_transactions.json"),
            "quotes": _payload("normal_quotes.json"),
            "coverage": _payload("normal_coverage.json"),
        },
        clock=lambda: NOW,
    )
    try:
        with pytest.raises(SchwabResponseError, match="account identity"):
            adapter.read_snapshot(
                "fictional-account-hash",
                account_id="fictional-account",
                received_at=NOW,
            )
    finally:
        adapter.close()


@pytest.mark.parametrize(
    ("section", "mutation", "message"),
    [
        (
            "orders",
            lambda value: value.append("malformed-order"),
            "non-object",
        ),
        (
            "orders",
            lambda value: value[0].pop("orderId"),
            "explicit identifier",
        ),
        (
            "positions",
            lambda value: (
                value[0].pop("longQuantity"),
                value[0].pop("shortQuantity"),
            ),
            "no quantity side",
        ),
        (
            "transactions",
            lambda value: (
                value[0].pop("transactionId"),
                value[0].pop("activityId"),
            ),
            "explicit identifier",
        ),
    ],
)
def test_malformed_facts_fail_closed(
    section: str, mutation, message: str
) -> None:
    account = _payload("normal_account.json")
    orders = _payload("normal_orders.json")
    transactions = _payload("normal_transactions.json")
    if section == "orders":
        mutation(orders)
    elif section == "positions":
        mutation(account["securitiesAccount"]["positions"])
    else:
        mutation(transactions)
    adapter = SchwabAdapter.from_fixture(
        account_hash="fictional-account-hash",
        payloads={
            "account": account,
            "orders": orders,
            "transactions": transactions,
            "quotes": _payload("normal_quotes.json"),
            "coverage": _payload("normal_coverage.json"),
        },
        clock=lambda: NOW,
    )
    try:
        with pytest.raises(SchwabResponseError, match=message):
            adapter.read_snapshot(
                "fictional-account-hash",
                account_id="fictional-account",
                received_at=NOW,
            )
    finally:
        adapter.close()


def test_missing_account_currency_fails_closed() -> None:
    account = _payload("normal_account.json")
    del account["securitiesAccount"]["currency"]
    adapter = SchwabAdapter.from_fixture(
        account_hash="fictional-account-hash",
        payloads={
            "account": account,
            "orders": [],
            "transactions": [],
            "quotes": {},
        },
        clock=lambda: NOW,
    )
    try:
        with pytest.raises(SchwabResponseError, match="currency is missing"):
            adapter.read_account("fictional-account-hash", account_id="fictional-account")
    finally:
        adapter.close()


def test_order_filled_quantity_must_match_derived_fills() -> None:
    transactions = _payload("normal_transactions.json")
    transactions[0]["transferItems"][0]["amount"] = "1"
    adapter = SchwabAdapter.from_fixture(
        account_hash="fictional-account-hash",
        payloads={
            "account": _payload("normal_account.json"),
            "orders": _payload("normal_orders.json"),
            "transactions": transactions,
            "quotes": _payload("normal_quotes.json"),
            "coverage": _payload("normal_coverage.json"),
        },
        clock=lambda: NOW,
    )
    try:
        data = adapter.read_snapshot(
            "fictional-account-hash",
            account_id="fictional-account",
            received_at=NOW,
        )
    finally:
        adapter.close()
    assert "order:SAMPLE-ORDER-001:filled_quantity_mismatch" in data.conflicts
    assert data.coverage.complete is False
