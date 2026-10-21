from collections.abc import Iterable, Mapping, Sequence
from datetime import date

from quant_stack.data.ingestion import IngestionService
from quant_stack.data.models import Instrument, Market
from quant_stack.data.providers.cn import (
    AKSHARE_CANDIDATE_CAPABILITIES,
    BAOSTOCK_CANDIDATE_CAPABILITIES,
    CNFundamentalsProvider,
    CNProvider,
    CNProviderCapabilities,
)
from quant_stack.data.storage import LocalFileSystemStorage


class OfflineCNProvider:
    source_url = "fixture://cn-provider"
    license_scope = "offline-test-only"
    available_at_policy = "fixture_timestamp"

    def __init__(self, name: str, capabilities: CNProviderCapabilities) -> None:
        self.name = name
        self.cn_capabilities = capabilities

    def list_instruments(
        self,
        market: Market,
        symbols: Sequence[str] | None = None,
    ) -> Iterable[Instrument]:
        assert market == "CN"
        symbol = (symbols or ["600000"])[0]
        yield Instrument(
            instrument_id=f"XSHG.{symbol}",
            symbol=symbol,
            market="CN",
            venue="XSHG",
            currency="CNY",
            timezone="Asia/Shanghai",
        )

    def fetch_bars(
        self,
        market: Market,
        symbols: Sequence[str],
        start: date,
        end: date,
        frequency: str = "1d",
    ) -> Iterable[Mapping[str, object]]:
        assert market == "CN"
        assert frequency == "1d"
        return [{"symbol": symbols[0], "trade_date": start.isoformat()}]


def test_baostock_and_akshare_candidates_share_data_provider_contract(tmp_path) -> None:
    providers = [
        OfflineCNProvider("baostock", BAOSTOCK_CANDIDATE_CAPABILITIES),
        OfflineCNProvider("akshare", AKSHARE_CANDIDATE_CAPABILITIES),
    ]

    assert all(isinstance(provider, CNProvider) for provider in providers)
    assert all(not isinstance(provider, CNFundamentalsProvider) for provider in providers)
    service = IngestionService(
        providers={provider.name: provider for provider in providers},
        storage=LocalFileSystemStorage(tmp_path),
    )
    assert set(service.providers) == {"baostock", "akshare"}
    assert next(iter(providers[0].list_instruments("CN"))).instrument_id == "XSHG.600000"
    assert next(
        iter(providers[1].fetch_bars("CN", ["600000"], date(2026, 1, 1), date(2026, 1, 2)))
    )["symbol"] == "600000"


def test_candidate_capabilities_are_endpoint_scoped_and_versioned() -> None:
    baostock = BAOSTOCK_CANDIDATE_CAPABILITIES
    akshare = AKSHARE_CANDIDATE_CAPABILITIES

    assert baostock.schema_version == "2.0.0"
    assert baostock.endpoints[0].metadata.endpoint == "query_history_k_data_plus"
    assert baostock.exchanges == {"XSHG", "XSHE"}
    assert baostock.endpoints[0].historical_daily_bars
    assert not baostock.endpoints[0].daily_increment

    assert {endpoint.metadata.endpoint for endpoint in akshare.endpoints} == {
        "stock_zh_a_spot_em",
        "stock_zh_a_hist",
    }
    assert akshare.exchanges == {"XSHG", "XSHE", "BJSE"}
    assert any(endpoint.instrument_discovery for endpoint in akshare.endpoints)
    daily = next(endpoint for endpoint in akshare.endpoints if endpoint.daily_increment)
    assert daily.gap_repair
    assert not akshare.fundamentals
