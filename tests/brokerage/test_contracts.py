from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from quant_stack.brokerage.contracts import (
    AccountSnapshot,
    AssetType,
    CurrencyAmount,
    Instrument,
    OptionContract,
)


def test_decimal_contracts_keep_exact_text_and_reject_float() -> None:
    value = CurrencyAmount(currency="USD", amount="0.10")
    assert value.amount == Decimal("0.10")
    with pytest.raises((TypeError, ValidationError)):
        CurrencyAmount(currency="USD", amount=0.1)


def test_option_multiplier_is_unknown_until_explicitly_present() -> None:
    option = OptionContract(
        underlying_symbol="QXEN",
        expiration_date="2026-10-16",
        put_call="CALL",
        strike_price="200.00",
    )
    instrument = Instrument(
        symbol="QXEN  261016C00200000",
        asset_type=AssetType.OPTION,
        option=option,
    )
    assert instrument.option is not None
    assert instrument.option.contract_multiplier is None
    assert "contractMultiplier" not in instrument.option.unknown_fields


def test_non_executable_snapshot_cannot_be_deserialized_as_executable() -> None:
    with pytest.raises(ValidationError):
        AccountSnapshot.model_validate(
            {
                "snapshot_id": "sample",
                "account": {
                    "account_id": "sample-account",
                    "alias": "fictional",
                },
                "business_time": datetime(2026, 10, 16, tzinfo=UTC).isoformat(),
                "received_at": datetime(2026, 10, 16, tzinfo=UTC).isoformat(),
                "sync_batch_id": "batch",
                "mapping_version": "sample",
                "coverage": {},
                "non_executable": False,
            }
        )
