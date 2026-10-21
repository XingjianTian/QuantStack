"""The sole Schwab SDK boundary.

Only this module imports ``schwab-py``.  It exposes normalized provider-neutral
objects to the rest of QuantStack and contains no order, replace, or cancel
operation.  ``from_fixture`` builds a real ``schwab.client.Client`` over an
``httpx.MockTransport``; fixture execution therefore cannot leave the process
through the network.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import unquote

import httpx
from schwab.client import Client as SchwabClient

from .contracts import (
    Account,
    AccountData,
    AccountType,
    AssetType,
    CashBalances,
    Coverage,
    CurrencyAmount,
    Fill,
    Instrument,
    OptionContract,
    Order,
    OrderSide,
    OrderStatus,
    Position,
    PositionSide,
    Quote,
    Transaction,
)

_NON_INVESTMENT_CASH_TRANSACTION_TYPES = frozenset(
    {
        "cash_receipt",
        "cash_disbursement",
        "cash_transfer",
        "cash_deposit",
        "cash_withdrawal",
    }
)
_UNKNOWN_UNDERLYING_SYMBOL = "<unknown>"


class FixtureTransportError(RuntimeError):
    """Raised when a fixture client attempts an unsupported or non-GET request."""


class SchwabResponseError(RuntimeError):
    """A provider response could not be converted into a normalized contract."""


def _as_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    if isinstance(value, Decimal):
        result = value
    else:
        try:
            result = Decimal(str(value))
        except (InvalidOperation, ValueError) as exc:
            raise SchwabResponseError(f"invalid provider decimal: {value!r}") from exc
    if not result.is_finite():
        raise SchwabResponseError("provider decimal must be finite")
    return result


def _timestamp(value: Any, *, fallback: datetime | None) -> datetime | None:
    if value is None or value == "":
        return fallback
    if isinstance(value, (int, float, Decimal)):
        # Schwab's *InLong fields are epoch milliseconds.
        number = _as_decimal(value)
        if number is None:
            return fallback
        return datetime.fromtimestamp(float(number) / 1000, tz=UTC)
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise SchwabResponseError(f"invalid provider timestamp: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise SchwabResponseError(f"provider timestamp lacks timezone: {value!r}")
    return parsed.astimezone(UTC)


def _bounded_window(
    start: datetime, end: datetime, *, label: str
) -> tuple[datetime, datetime]:
    if start.tzinfo is None or start.utcoffset() is None:
        raise ValueError(f"{label} start must include a timezone")
    if end.tzinfo is None or end.utcoffset() is None:
        raise ValueError(f"{label} end must include a timezone")
    start = start.astimezone(UTC)
    end = end.astimezone(UTC)
    if start > end:
        raise ValueError(f"{label} start must be before end")
    if end - start > timedelta(days=60):
        raise ValueError(f"Schwab {label} windows may not exceed 60 days")
    return start, end


def _date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError as exc:
        raise SchwabResponseError(f"invalid provider date: {value!r}") from exc


def _is_standard_option_deliverable(
    value: Any,
    *,
    contract_multiplier: Decimal | None,
    underlying_symbol: str,
) -> bool:
    """Accept only an explicit, whole-contract share deliverable."""

    if (
        not isinstance(value, str)
        or contract_multiplier is None
        or underlying_symbol == _UNKNOWN_UNDERLYING_SYMBOL
    ):
        return False
    normalized = " ".join(value.split()).casefold()
    if contract_multiplier == contract_multiplier.to_integral_value():
        multiplier_text = str(int(contract_multiplier))
    else:
        multiplier_text = format(contract_multiplier, "f").rstrip("0").rstrip(".")
    expected = {
        f"{multiplier_text} shares of {underlying_symbol}".casefold(),
        f"{multiplier_text} {underlying_symbol} shares".casefold(),
    }
    return normalized in expected


def _raw_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _list(value: Any, *, allow_none: bool = True) -> list[Mapping[str, Any]]:
    if value is None:
        if allow_none:
            return []
        raise SchwabResponseError("provider list is missing")
    if not isinstance(value, list):
        raise SchwabResponseError(f"expected provider list, got {type(value).__name__}")
    malformed = [index for index, item in enumerate(value) if not isinstance(item, Mapping)]
    if malformed:
        raise SchwabResponseError(
            "provider list contains non-object item(s): "
            + ",".join(str(index) for index in malformed)
        )
    return list(value)


def _identifier(raw: Mapping[str, Any], *keys: str, prefix: str) -> str:
    for key in keys:
        value = raw.get(key)
        if value not in (None, ""):
            return str(value)
    raise SchwabResponseError(
        f"provider {prefix} is missing an explicit identifier ({', '.join(keys)})"
    )


def _currency(raw: Mapping[str, Any]) -> str:
    value = raw.get("currency") or raw.get("currencyType")
    if value in (None, ""):
        raise SchwabResponseError("provider currency is missing")
    text = str(value).strip().upper()
    if len(text) != 3 or not all("A" <= char <= "Z" for char in text):
        raise SchwabResponseError(f"provider currency is invalid: {value!r}")
    return text


def _amount(value: Any, currency: str | None = None) -> CurrencyAmount | None:
    parsed = _as_decimal(value)
    if parsed is None:
        return None
    if currency is None:
        raise SchwabResponseError("currency is required for a provider amount")
    return CurrencyAmount(currency=currency, amount=parsed)


def _status(value: Any) -> OrderStatus:
    normalized = str(value or "").strip().upper()
    return {
        "NEW": OrderStatus.NEW,
        "ACCEPTED": OrderStatus.WORKING,
        "QUEUED": OrderStatus.WORKING,
        "WORKING": OrderStatus.WORKING,
        "PENDING_ACTIVATION": OrderStatus.PENDING,
        "PENDING_ACKNOWLEDGEMENT": OrderStatus.PENDING,
        "PENDING_CANCEL": OrderStatus.PENDING,
        "PENDING_REPLACE": OrderStatus.PENDING,
        "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
        "FILLED": OrderStatus.FILLED,
        "CANCELED": OrderStatus.CANCELED,
        "CANCELLED": OrderStatus.CANCELED,
        "REJECTED": OrderStatus.REJECTED,
        "EXPIRED": OrderStatus.EXPIRED,
        "REPLACED": OrderStatus.REPLACED,
    }.get(normalized, OrderStatus.UNKNOWN)


def _side(value: Any) -> OrderSide:
    normalized = str(value or "").strip().upper()
    return {
        "BUY": OrderSide.BUY,
        "SELL": OrderSide.SELL,
        "BUY_TO_COVER": OrderSide.BUY_TO_COVER,
        "SELL_SHORT": OrderSide.SELL_SHORT,
    }.get(normalized, OrderSide.UNKNOWN)


def _position_side(quantity: Decimal) -> PositionSide:
    if quantity > 0:
        return PositionSide.LONG
    if quantity < 0:
        return PositionSide.SHORT
    return PositionSide.FLAT


def _response_json(response: Any) -> Any:
    response.raise_for_status()
    content = getattr(response, "content", None)
    if isinstance(content, bytes):
        try:
            # Keep decimal tokens exact instead of round-tripping through float.
            return json.loads(content, parse_float=Decimal, parse_int=int)
        except json.JSONDecodeError as exc:
            raise SchwabResponseError("provider returned invalid JSON") from exc
    try:
        return response.json()
    except Exception as exc:  # pragma: no cover - defensive for injected clients
        raise SchwabResponseError("provider returned unreadable JSON") from exc


class FixtureTransport(httpx.MockTransport):
    """A strict GET-only transport used for all sample-mode adapter tests."""

    SCHEME = "https"
    HOST = "api.schwabapi.com"
    PORT = 443

    def __init__(
        self,
        routes: Mapping[str, Any],
        *,
        coverage: Mapping[str, Any] | None = None,
        account_hash: str | None = None,
    ) -> None:
        self._routes = dict(routes)
        self.coverage = dict(coverage or {})
        self.account_hash = account_hash
        self.requests: list[httpx.Request] = []
        super().__init__(self._handle)

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.method != "GET":
            raise FixtureTransportError(
                f"sample Schwab transport rejects non-GET method {request.method}"
            )
        if (
            request.url.scheme != self.SCHEME
            or request.url.host != self.HOST
            or request.url.port not in (None, self.PORT)
        ):
            raise FixtureTransportError(
                "sample Schwab transport rejects an unapproved scheme, host, or port"
            )
        path = request.url.raw_path.decode().split("?", 1)[0]
        path = unquote(path)
        if path not in self._routes:
            raise FixtureTransportError(f"sample Schwab route is not allow-listed: {path}")
        result = self._routes[path]
        if callable(result):
            result = result(request)
        status_code = 200
        payload = result
        if isinstance(result, tuple) and len(result) == 2:
            status_code, payload = result
        return httpx.Response(status_code, json=payload, request=request)


class SchwabAdapter:
    """Read-only normalized facade over a ``schwab-py`` client."""

    MAPPING_VERSION = "schwab-py-1.5.1-readonly-v1"

    def __init__(
        self,
        client: SchwabClient,
        *,
        mapping_version: str = MAPPING_VERSION,
        clock: Callable[[], datetime] | None = None,
        owns_client: bool = False,
    ) -> None:
        session = getattr(client, "session", None)
        transport = getattr(session, "_transport", None)
        if not isinstance(transport, FixtureTransport):
            raise FixtureTransportError(
                "sample SchwabAdapter accepts only its controlled FixtureTransport"
            )
        self._client = client
        self.mapping_version = mapping_version
        self._clock = clock or (lambda: datetime.now(UTC))
        self._owns_client = owns_client
        self._fixture_coverage = dict(transport.coverage)
        self._fixture_account_hash = transport.account_hash
        if not self._fixture_account_hash:
            raise FixtureTransportError("fixture transport must bind one account hash")

    @classmethod
    def from_fixture(
        cls,
        *,
        account_hash: str,
        payloads: Mapping[str, Any],
        api_key: str = "fixture-api-key",
        clock: Callable[[], datetime] | None = None,
    ) -> SchwabAdapter:
        """Build a real Schwab client backed solely by an in-memory fixture route."""

        required = {"account", "orders", "transactions"}
        missing = required.difference(payloads)
        if missing:
            raise ValueError(f"fixture payloads missing required sections: {sorted(missing)}")
        account_path = f"/trader/v1/accounts/{account_hash}"
        routes: dict[str, Any] = {
            account_path: payloads["account"],
            f"{account_path}/orders": payloads["orders"],
            f"{account_path}/transactions": payloads["transactions"],
        }
        if "quotes" in payloads:
            routes["/marketdata/v1/quotes"] = payloads["quotes"]
        coverage = payloads.get("coverage")
        if coverage is not None and not isinstance(coverage, Mapping):
            raise ValueError("fixture coverage metadata must be an object")
        transport = FixtureTransport(routes, coverage=coverage, account_hash=account_hash)
        session = httpx.Client(transport=transport)
        client = SchwabClient(api_key, session)
        return cls(client, clock=clock, owns_client=True)

    def close(self) -> None:
        if self._owns_client:
            self._client.session.close()

    def read_account_payload(self, account_hash: str) -> Mapping[str, Any]:
        if account_hash != self._fixture_account_hash:
            raise FixtureTransportError("fixture account hash does not match the bound client")
        response = self._client.get_account(
            account_hash,
            fields=self._client.Account.Fields.POSITIONS,
        )
        payload = _response_json(response)
        if not isinstance(payload, Mapping):
            raise SchwabResponseError("account response must be an object")
        return payload

    def read_orders_payload(
        self,
        account_hash: str,
        *,
        from_entered_datetime: datetime,
        to_entered_datetime: datetime,
    ) -> list[Mapping[str, Any]]:
        from_entered_datetime, to_entered_datetime = _bounded_window(
            from_entered_datetime,
            to_entered_datetime,
            label="order",
        )
        response = self._client.get_orders_for_account(
            account_hash,
            from_entered_datetime=from_entered_datetime,
            to_entered_datetime=to_entered_datetime,
        )
        payload = _response_json(response)
        return _list(payload, allow_none=False)

    def read_transactions_payload(
        self,
        account_hash: str,
        *,
        start_date: datetime,
        end_date: datetime,
    ) -> list[Mapping[str, Any]]:
        start_date, end_date = _bounded_window(start_date, end_date, label="transaction")
        response = self._client.get_transactions(
            account_hash,
            start_date=start_date,
            end_date=end_date,
        )
        payload = _response_json(response)
        return _list(payload, allow_none=False)

    def read_quotes_payload(self, symbols: Sequence[str]) -> Mapping[str, Any]:
        if not symbols:
            return {}
        response = self._client.get_quotes(tuple(dict.fromkeys(symbols)))
        payload = _response_json(response)
        if not isinstance(payload, Mapping):
            raise SchwabResponseError("quotes response must be an object")
        return payload

    def read_account(
        self, account_hash: str, *, account_id: str | None = None
    ) -> Account:
        raw = _raw_mapping(self.read_account_payload(account_hash).get("securitiesAccount"))
        return self._account_from_payload(raw, account_id, account_hash)

    def read_positions(
        self,
        account_hash: str,
        *,
        account_id: str | None = None,
        account: Account | None = None,
        as_of: datetime | None = None,
    ) -> tuple[Position, ...]:
        payload = self.read_account_payload(account_hash)
        raw_account = _raw_mapping(payload.get("securitiesAccount"))
        provider_account = self._account_from_payload(raw_account, account_id, account_hash)
        if account is not None and account.account_id != provider_account.account_id:
            raise SchwabResponseError("provided account does not match provider account identity")
        account = account or provider_account
        timestamp = as_of
        return self._positions_from_payload(raw_account, account.account_id, timestamp)

    def read_orders(
        self,
        account_hash: str,
        *,
        account_id: str,
        from_entered_datetime: datetime,
        to_entered_datetime: datetime,
        currency: str | None = None,
    ) -> tuple[Order, ...]:
        return tuple(
            self._order_from_payload(raw, account_id, None, currency=currency)
            for raw in self.read_orders_payload(
                account_hash,
                from_entered_datetime=from_entered_datetime,
                to_entered_datetime=to_entered_datetime,
            )
        )

    def read_transactions(
        self,
        account_hash: str,
        *,
        account_id: str,
        start_date: datetime,
        end_date: datetime,
        currency: str | None = None,
    ) -> tuple[Transaction, ...]:
        return tuple(
            self._transaction_from_payload(raw, account_id, None, currency=currency)
            for raw in self.read_transactions_payload(
                account_hash,
                start_date=start_date,
                end_date=end_date,
            )
        )

    def read_quotes(
        self,
        symbols: Sequence[str],
        *,
        quote_time: datetime | None = None,
        stale_after_seconds: int = 900,
    ) -> tuple[Quote, ...]:
        now = quote_time or self._clock()
        payload = self.read_quotes_payload(symbols)
        quotes: list[Quote] = []
        for symbol, raw in payload.items():
            if not isinstance(raw, Mapping):
                raise SchwabResponseError(f"provider quote {symbol!r} is not an object")
            quotes.append(
                self._quote_from_payload(symbol, raw, now, stale_after_seconds)
            )
        return tuple(quotes)

    def read_snapshot(
        self,
        account_hash: str,
        *,
        account_id: str,
        received_at: datetime | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        symbols: Sequence[str] | None = None,
    ) -> AccountData:
        received_at = received_at or self._clock()
        if received_at.tzinfo is None or received_at.utcoffset() is None:
            raise ValueError("received_at must include a timezone")
        received_at = received_at.astimezone(UTC)
        end_date = (end_date or received_at).astimezone(UTC)
        start_date = (start_date or end_date - timedelta(days=60)).astimezone(UTC)
        start_date, end_date = _bounded_window(
            start_date,
            end_date,
            label="order and transaction",
        )

        payload = self.read_account_payload(account_hash)
        raw_account = _raw_mapping(payload.get("securitiesAccount"))
        account = self._account_from_payload(raw_account, account_id, account_hash)
        positions = self._positions_from_payload(raw_account, account.account_id, None)
        orders = self.read_orders(
            account_hash,
            account_id=account.account_id,
            from_entered_datetime=start_date,
            to_entered_datetime=end_date,
            currency=account.base_currency,
        )
        transactions = self.read_transactions(
            account_hash,
            account_id=account.account_id,
            start_date=start_date,
            end_date=end_date,
            currency=account.base_currency,
        )
        fills = self._fills_from_transactions(transactions)
        symbols_to_read = tuple(
            dict.fromkeys(
                [*(symbol for symbol in (item.instrument.symbol for item in positions))]
                + list(symbols or ())
            )
        )
        quotes = (
            self.read_quotes(symbols_to_read, quote_time=received_at)
            if symbols_to_read
            else ()
        )
        sections = ["account", "positions", "orders", "transactions", "fills"]
        if symbols_to_read:
            sections.append("quotes")
        metadata = self._fixture_coverage
        source_as_of_raw = next(
            (
                metadata.get(name)
                for name in ("sourceAsOf", "source_as_of", "asOf", "as_of")
                if metadata.get(name) not in (None, "")
            ),
            None,
        )
        source_as_of = _timestamp(source_as_of_raw, fallback=None)
        metadata_complete = metadata.get("complete") is True
        metadata_truncated = metadata.get("truncated") is True
        metadata_sections = metadata.get("sections")
        if isinstance(metadata_sections, list):
            sections = [str(section) for section in metadata_sections]
        conflicts = list(self._detect_conflicts(positions, orders, transactions, fills))
        if not raw_account:
            conflicts.append("account:missing")
        elif "positions" not in raw_account:
            conflicts.append("positions:missing")
        if not metadata:
            conflicts.append("coverage:metadata_missing")
        else:
            if "complete" not in metadata:
                conflicts.append("coverage:complete_unknown")
            if "truncated" not in metadata:
                conflicts.append("coverage:truncated_unknown")
            if "sections" not in metadata:
                conflicts.append("coverage:sections_unknown")
            if source_as_of is None:
                conflicts.append("coverage:source_as_of_missing")
            elif source_as_of > received_at:
                conflicts.append("coverage:source_as_of_in_future")
            if metadata_sections is not None and not isinstance(metadata_sections, list):
                conflicts.append("coverage:sections_invalid")
            elif metadata_sections is not None:
                required_sections = {"account", "positions", "orders", "transactions"}
                missing_sections = required_sections.difference(metadata_sections)
                if missing_sections:
                    conflicts.append(
                        "coverage:sections_missing:" + ",".join(sorted(missing_sections))
                    )
                if symbols_to_read and "quotes" not in metadata_sections:
                    conflicts.append("coverage:quotes_missing")
        if metadata_truncated:
            conflicts.append("coverage:truncated")
        if not metadata_complete:
            conflicts.append("coverage:incomplete")
        for position in positions:
            if position.as_of is None:
                conflicts.append(f"position:{position.position_id}:as_of_missing")
        for order in orders:
            if order.entered_at is None:
                conflicts.append(f"order:{order.order_id}:entered_at_missing")
            if order.unknown_fields:
                conflicts.append(f"order:{order.order_id}:structure_unknown")
        for transaction in transactions:
            if transaction.transaction_time is None:
                conflicts.append(f"transaction:{transaction.transaction_id}:time_missing")
            if "transaction_shape" in transaction.unknown_fields:
                conflicts.append(f"transaction:{transaction.transaction_id}:shape_unknown")
        for quote in quotes:
            if quote.quote_time is None:
                conflicts.append(f"quote:{quote.instrument.symbol}:quote_time_missing")
            elif quote.quote_time > received_at:
                conflicts.append(f"quote:{quote.instrument.symbol}:quote_time_in_future")
        conflicts = tuple(sorted(set(conflicts)))
        coverage = Coverage(
            sections=tuple(sections),
            start=start_date,
            end=end_date,
            complete=metadata_complete and not metadata_truncated and not conflicts,
            truncated=metadata_truncated,
            next_cursor=(
                str(metadata.get("nextCursor"))
                if metadata.get("nextCursor") not in (None, "")
                else None
            ),
            source_as_of=source_as_of,
        )
        return AccountData(
            account=account,
            positions=tuple(sorted(positions, key=lambda item: item.position_id)),
            orders=tuple(sorted(orders, key=lambda item: item.order_id)),
            transactions=tuple(sorted(transactions, key=lambda item: item.transaction_id)),
            fills=tuple(sorted(fills, key=lambda item: item.execution_id)),
            quotes=tuple(sorted(quotes, key=lambda item: item.instrument.symbol)),
            business_time=source_as_of,
            received_at=received_at,
            mapping_version=self.mapping_version,
            coverage=coverage,
            conflicts=conflicts,
            cursor=coverage.next_cursor,
        )

    def _account_from_payload(
        self,
        raw_account: Mapping[str, Any],
        account_id: str | None,
        account_hash: str,
    ) -> Account:
        # Avoid a second SDK request when read_positions/read_snapshot already have payload.
        if not raw_account:
            raise SchwabResponseError("provider account object is missing")
        provider_account_id = next(
            (
                str(raw_account.get(name)).strip()
                for name in ("accountId", "accountNumber", "account_id")
                if raw_account.get(name) not in (None, "")
            ),
            None,
        )
        if not provider_account_id:
            raise SchwabResponseError("provider account identity is missing")
        provider_account_hash = next(
            (
                str(raw_account.get(name)).strip()
                for name in ("accountHash", "hashValue", "account_hash")
                if raw_account.get(name) not in (None, "")
            ),
            None,
        )
        if provider_account_hash is None:
            raise SchwabResponseError("provider account hash identity is missing")
        if provider_account_hash != account_hash:
            raise SchwabResponseError("provider account hash does not match the requested account")
        if account_id is not None and account_id != provider_account_id:
            raise SchwabResponseError("provider account identity does not match account_id")
        account_type_raw = str(raw_account.get("type") or "").strip().upper()
        account_type = {
            "CASH": AccountType.CASH,
            "MARGIN": AccountType.MARGIN,
            "IRA": AccountType.IRA,
            "ROTH_IRA": AccountType.ROTH_IRA,
        }.get(account_type_raw, AccountType.BROKERAGE if account_type_raw else AccountType.UNKNOWN)
        currency = _currency(raw_account)
        balances_raw = _raw_mapping(
            raw_account.get("currentBalances") or raw_account.get("initialBalances") or {}
        )
        reservation_included = balances_raw.get("orderReservationIncluded")
        if reservation_included is not None and not isinstance(reservation_included, bool):
            raise SchwabResponseError(
                "provider orderReservationIncluded must be a boolean when present"
            )
        balances = CashBalances(
            currency=currency,
            balance=_as_decimal(balances_raw.get("liquidationValue")),
            cash=_as_decimal(balances_raw.get("cashBalance")),
            settled_cash=_as_decimal(balances_raw.get("settledCash")),
            available_cash=_as_decimal(
                balances_raw.get("cashAvailableForTrading")
                if balances_raw.get("cashAvailableForTrading") is not None
                else balances_raw.get("availableFunds")
            ),
            buying_power=_as_decimal(balances_raw.get("buyingPower")),
            withdrawable_cash=_as_decimal(balances_raw.get("cashAvailableForWithdrawal")),
            open_order_reservation=_as_decimal(balances_raw.get("pendingOrders")),
            order_reservation_included=reservation_included,
            margin_enabled=account_type is AccountType.MARGIN,
            source_fields=tuple(sorted(balances_raw.keys())),
            unknown_fields=tuple(
                name
                for name in (
                    "liquidationValue",
                    "cashBalance",
                    "settledCash",
                    "cashAvailableForTrading",
                    "buyingPower",
                    "cashAvailableForWithdrawal",
                )
                if name not in balances_raw
            ),
        )
        normalized_id = account_id or provider_account_id
        return Account(
            account_id=normalized_id,
            alias=str(
                raw_account.get("nickname")
                or raw_account.get("displayName")
                or normalized_id
            ),
            account_type=account_type,
            base_currency=currency,
            balances=(balances,),
            source_account_type=account_type_raw or None,
        )

    def _instrument(
        self, raw: Mapping[str, Any], *, fallback_symbol: str | None = None
    ) -> Instrument:
        symbol_value = raw.get("symbol") or raw.get("underlyingSymbol") or fallback_symbol
        if symbol_value in (None, ""):
            raise SchwabResponseError("provider instrument symbol is missing")
        symbol = str(symbol_value).strip()
        if not symbol:
            raise SchwabResponseError("provider instrument symbol is empty")
        asset_raw = str(raw.get("assetType") or "").strip().upper()
        asset_type = {
            "EQUITY": AssetType.EQUITY,
            "ETF": AssetType.ETF,
            "OPTION": AssetType.OPTION,
            "MUTUAL_FUND": AssetType.MUTUAL_FUND,
            "FIXED_INCOME": AssetType.FIXED_INCOME,
            "CASH_EQUIVALENT": AssetType.CASH,
        }.get(asset_raw, AssetType.UNKNOWN)
        option = None
        if asset_type is AssetType.OPTION:
            underlying_raw = raw.get("underlyingSymbol")
            underlying_symbol = (
                str(underlying_raw).strip()
                if underlying_raw not in (None, "")
                else _UNKNOWN_UNDERLYING_SYMBOL
            )
            option_unknown_fields: list[str] = []
            if underlying_symbol == _UNKNOWN_UNDERLYING_SYMBOL:
                option_unknown_fields.append("underlyingSymbol")
            contract_multiplier = _as_decimal(raw.get("contractMultiplier"))
            if contract_multiplier is None:
                option_unknown_fields.append("contractMultiplier")
            deliverable_raw = raw.get("deliverable")
            if isinstance(deliverable_raw, str):
                deliverable = deliverable_raw.strip() or None
            elif deliverable_raw in (None, ""):
                deliverable = None
            else:
                deliverable = str(deliverable_raw).strip() or None
            if not _is_standard_option_deliverable(
                deliverable,
                contract_multiplier=contract_multiplier,
                underlying_symbol=underlying_symbol,
            ):
                option_unknown_fields.append("deliverable")
            option = OptionContract(
                underlying_symbol=underlying_symbol,
                expiration_date=_date(raw.get("optionExpirationDate")),
                put_call=str(raw.get("putCall")) if raw.get("putCall") else None,
                strike_price=_as_decimal(raw.get("strikePrice")),
                contract_multiplier=contract_multiplier,
                deliverable=deliverable,
                contract_symbol=symbol,
                unknown_fields=tuple(sorted(set(option_unknown_fields))),
            )
        return Instrument(
            symbol=symbol,
            asset_type=asset_type,
            description=str(raw.get("description")) if raw.get("description") else None,
            currency=_currency(raw),
            cusip=str(raw.get("cusip")) if raw.get("cusip") else None,
            option=option,
        )

    def _positions_from_payload(
        self, raw_account: Mapping[str, Any], account_id: str, as_of: datetime | None
    ) -> tuple[Position, ...]:
        positions: list[Position] = []
        for raw in _list(raw_account.get("positions")):
            instrument_raw = raw.get("instrument")
            if not isinstance(instrument_raw, Mapping):
                raise SchwabResponseError("provider position instrument is missing")
            instrument = self._instrument(instrument_raw)
            long_quantity = _as_decimal(raw.get("longQuantity"))
            short_quantity = _as_decimal(raw.get("shortQuantity"))
            if long_quantity is None and short_quantity is None:
                raise SchwabResponseError(
                    f"provider position {raw.get('positionId')!r} has no quantity side"
                )
            long_quantity = long_quantity if long_quantity is not None else Decimal("0")
            short_quantity = short_quantity if short_quantity is not None else Decimal("0")
            quantity = long_quantity - short_quantity
            currency = instrument.currency
            position_time = _timestamp(raw.get("asOf") or raw.get("asOfDate"), fallback=as_of)
            unknown_fields = (
                ("as_of",) if raw.get("asOf") is None and raw.get("asOfDate") is None else ()
            )
            positions.append(
                Position(
                    position_id=_identifier(raw, "positionId", prefix="position"),
                    account_id=account_id,
                    instrument=instrument,
                    quantity=quantity,
                    side=_position_side(quantity),
                    average_price=_amount(raw.get("averagePrice"), currency),
                    market_value=_amount(raw.get("marketValue"), currency),
                    cost_basis=_amount(raw.get("costBasis"), currency),
                    as_of=position_time,
                    source_position_id=(
                        str(raw.get("positionId")) if raw.get("positionId") else None
                    ),
                    unknown_fields=unknown_fields,
                )
            )
        return tuple(positions)

    def _order_from_payload(
        self,
        raw: Mapping[str, Any],
        account_id: str,
        fallback_time: datetime | None,
        *,
        currency: str | None,
    ) -> Order:
        legs = _list(raw.get("orderLegCollection"))
        leg = legs[0] if legs else {}
        instrument = None
        if leg:
            instrument_raw = leg.get("instrument")
            if not isinstance(instrument_raw, Mapping):
                raise SchwabResponseError("provider order instrument is missing")
            instrument = self._instrument(instrument_raw)
        requested = _as_decimal(raw.get("quantity"))
        if requested is None and leg:
            requested = _as_decimal(leg.get("quantity"))
        filled = _as_decimal(raw.get("filledQuantity"))
        remaining = _as_decimal(raw.get("remainingQuantity"))
        if remaining is None and requested is not None and filled is not None:
            remaining = max(requested - filled, Decimal("0"))
        status_raw = str(raw.get("status") or "")
        status = _status(status_raw)
        unknown_fields = []
        child = []
        for item in _list(raw.get("childOrderStrategies")):
            if item.get("orderId") in (None, ""):
                unknown_fields.append("child_order_id")
            else:
                child.append(str(item["orderId"]))
        if len(legs) > 1:
            unknown_fields.append("multi_leg_order")
        if raw.get("childOrderStrategies"):
            unknown_fields.append("child_order_strategies")
        if not legs:
            unknown_fields.append("order_leg_collection")
        if status is OrderStatus.UNKNOWN:
            unknown_fields.append("order_status")
        if leg and _side(leg.get("instruction")) is OrderSide.UNKNOWN:
            unknown_fields.append("order_side")
        return Order(
            order_id=_identifier(raw, "orderId", prefix="order"),
            account_id=account_id,
            instrument=instrument,
            side=_side(leg.get("instruction")),
            order_type=str(raw.get("orderType")) if raw.get("orderType") else None,
            status=status,
            raw_status=status_raw or None,
            requested_quantity=requested,
            filled_quantity=filled,
            remaining_quantity=remaining,
            limit_price=_amount(raw.get("price"), instrument.currency if instrument else currency),
            entered_at=_timestamp(raw.get("enteredTime"), fallback=fallback_time)
            if raw.get("enteredTime")
            else None,
            updated_at=_timestamp(
                raw.get("closeTime") or raw.get("lastUpdatedTime"), fallback=fallback_time
            )
            if raw.get("closeTime") or raw.get("lastUpdatedTime")
            else None,
            replaces_order_id=(
                str(raw.get("replacingOrderId") or raw.get("replacesOrderId"))
                if raw.get("replacingOrderId") or raw.get("replacesOrderId")
                else None
            ),
            replaced_by_order_id=str(raw.get("replacedByOrderId"))
            if raw.get("replacedByOrderId")
            else None,
            child_order_ids=tuple(child),
            is_open=status
            in {
                OrderStatus.NEW,
                OrderStatus.WORKING,
                OrderStatus.PARTIALLY_FILLED,
                OrderStatus.PENDING,
            },
            unknown_fields=tuple(sorted(unknown_fields)),
        )

    def _transaction_from_payload(
        self,
        raw: Mapping[str, Any],
        account_id: str,
        fallback_time: datetime | None,
        *,
        currency: str | None,
    ) -> Transaction:
        items = _list(raw.get("transferItems"))
        if len(items) > 1:
            raise SchwabResponseError(
                "provider transaction contains multiple transfer items; refusing to drop facts"
            )
        item = items[0] if items else {}
        instrument = None
        if item:
            instrument_raw = item.get("instrument")
            if not isinstance(instrument_raw, Mapping):
                raise SchwabResponseError("provider transaction instrument is missing")
            instrument = self._instrument(instrument_raw)
        amount_currency = instrument.currency if instrument else currency
        transaction_type = str(raw.get("type") or "unknown").strip().lower()
        transaction_subtype = (
            str(raw.get("transactionSubType")).strip()
            if raw.get("transactionSubType")
            else None
        )
        transaction_kind = f"{transaction_type} {transaction_subtype or ''}".lower()
        is_correction = any(
            token in transaction_kind
            for token in ("reversal", "cancel", "correction", "adjust")
        )
        is_fee = any(token in transaction_kind for token in ("fee", "commission"))
        quantity = _as_decimal(item.get("amount")) if item else None
        transaction_time = _timestamp(
            raw.get("transactionDate") or raw.get("transactionTime") or raw.get("tradeDate"),
            fallback=fallback_time,
        )
        unknown_fields: list[str] = []
        if transaction_time is None:
            unknown_fields.append("transaction_time")
        if (
            transaction_type not in _NON_INVESTMENT_CASH_TRANSACTION_TYPES
            and not is_correction
            and not is_fee
            and (not items or instrument is None or quantity is None)
        ):
            unknown_fields.append("transaction_shape")
            if not items:
                unknown_fields.append("transferItems")
            elif instrument is None:
                unknown_fields.append("instrument")
            elif quantity is None:
                unknown_fields.append("quantity")
        return Transaction(
            transaction_id=_identifier(raw, "transactionId", "activityId", prefix="transaction"),
            account_id=account_id,
            transaction_type=transaction_type,
            transaction_subtype=transaction_subtype,
            instrument=instrument,
            quantity=quantity,
            price=_amount(item.get("price"), amount_currency) if item else None,
            net_amount=_amount(raw.get("netAmount"), amount_currency),
            transaction_time=transaction_time,
            order_id=str(raw.get("orderId")) if raw.get("orderId") else None,
            related_transaction_id=(
                str(raw.get("relatedTransactionId") or raw.get("relatedTransactionID"))
                if raw.get("relatedTransactionId") or raw.get("relatedTransactionID")
                else None
            ),
            unknown_fields=tuple(sorted(set(unknown_fields))),
        )

    def _fills_from_transactions(self, transactions: Sequence[Transaction]) -> tuple[Fill, ...]:
        fills: list[Fill] = []
        for transaction in transactions:
            if transaction.transaction_type != "trade" or transaction.instrument is None:
                continue
            quantity = transaction.quantity
            if quantity is None:
                continue
            fills.append(
                Fill(
                    execution_id=f"{transaction.transaction_id}:execution",
                    account_id=transaction.account_id,
                    order_id=transaction.order_id,
                    instrument=transaction.instrument,
                    side=OrderSide.BUY
                    if (transaction.transaction_subtype or "").upper().startswith("BUY")
                    else OrderSide.SELL
                    if (transaction.transaction_subtype or "").upper().startswith("SELL")
                    else OrderSide.UNKNOWN,
                    quantity=quantity,
                    price=transaction.price,
                    amount=transaction.net_amount,
                    executed_at=transaction.transaction_time,
                    source_transaction_id=transaction.transaction_id,
                    unknown_fields=transaction.unknown_fields,
                )
            )
        return tuple(fills)

    def _quote_from_payload(
        self,
        symbol: str,
        raw: Mapping[str, Any],
        now: datetime,
        stale_after_seconds: int,
    ) -> Quote:
        quote = _raw_mapping(raw.get("quote") or raw)
        instrument_raw = _raw_mapping(raw.get("reference"))
        if not instrument_raw:
            instrument_raw = {"symbol": symbol, "assetType": raw.get("assetMainType", "UNKNOWN")}
        instrument = self._instrument(instrument_raw, fallback_symbol=symbol)
        quote_time = _timestamp(
            quote.get("quoteTime") or quote.get("quoteTimeInLong") or quote.get("tradeTimeInLong"),
            fallback=None,
        )
        unknown_fields = ("quote_time",) if quote_time is None else ()
        return Quote(
            instrument=instrument,
            bid=_amount(quote.get("bidPrice"), instrument.currency),
            ask=_amount(quote.get("askPrice"), instrument.currency),
            last=_amount(quote.get("lastPrice"), instrument.currency),
            quote_time=quote_time,
            stale_after_seconds=stale_after_seconds,
            is_stale=(
                True
                if quote_time is None
                else (now - quote_time).total_seconds() > stale_after_seconds
            ),
            unknown_fields=unknown_fields,
        )

    @staticmethod
    def _detect_conflicts(
        positions: Sequence[Position],
        orders: Sequence[Order],
        transactions: Sequence[Transaction],
        fills: Sequence[Fill],
    ) -> tuple[str, ...]:
        conflicts: list[str] = []
        seen_positions: dict[str, str] = {}
        for position in positions:
            body = position.canonical_json()
            prior = seen_positions.get(position.position_id)
            if prior is not None and prior != body:
                conflicts.append(f"position:{position.position_id}")
            seen_positions[position.position_id] = body
        seen_orders: dict[str, str] = {}
        for order in orders:
            body = order.canonical_json()
            prior = seen_orders.get(order.order_id)
            if prior is not None and prior != body:
                conflicts.append(f"order:{order.order_id}")
            seen_orders[order.order_id] = body
        seen_transactions: dict[str, str] = {}
        for transaction in transactions:
            body = transaction.canonical_json()
            prior = seen_transactions.get(transaction.transaction_id)
            if prior is not None and prior != body:
                conflicts.append(f"transaction:{transaction.transaction_id}")
            seen_transactions[transaction.transaction_id] = body
        fill_quantities: dict[str, Decimal] = {}
        for fill in fills:
            if fill.order_id is None:
                continue
            fill_quantities[fill.order_id] = fill_quantities.get(fill.order_id, Decimal("0")) + (
                fill.quantity
            )
        for order in orders:
            derived_quantity = fill_quantities.get(order.order_id)
            if (
                derived_quantity is not None
                and order.filled_quantity is not None
                and derived_quantity != order.filled_quantity
            ):
                conflicts.append(f"order:{order.order_id}:filled_quantity_mismatch")
        return tuple(sorted(set(conflicts)))


__all__ = ["FixtureTransport", "FixtureTransportError", "SchwabAdapter", "SchwabResponseError"]
