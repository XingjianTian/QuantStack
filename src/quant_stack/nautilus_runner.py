from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .experiments import create_experiment


@dataclass(frozen=True)
class NautilusEngineInfo:
    available: bool
    version: str | None
    error: str | None = None


def engine_info() -> NautilusEngineInfo:
    try:
        import nautilus_trader

        return NautilusEngineInfo(available=True, version=nautilus_trader.__version__)
    except Exception as exc:  # pragma: no cover - platform import failure is deployment-specific
        return NautilusEngineInfo(available=False, version=None, error=str(exc))


def _json_number(value: Any) -> float | int | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return int(number) if number.is_integer() else number


def _max_drawdown(equity: list[float]) -> float:
    peak = equity[0]
    worst = 0.0
    for value in equity:
        peak = max(peak, value)
        if peak > 0:
            worst = max(worst, (peak - value) / peak)
    return worst


def run_synthetic_backtest(
    *,
    strategy_path: str | Path,
    data_manifest_path: str | Path,
    experiment_root: str | Path,
    repo_root: str | Path,
    upstream_versions: dict[str, str],
) -> dict[str, Any]:
    """Run a deterministic NautilusTrader engine check and persist its evidence bundle."""
    from nautilus_trader.backtest.engine import BacktestEngine
    from nautilus_trader.config import BacktestEngineConfig, LoggingConfig, StrategyConfig
    from nautilus_trader.core.datetime import dt_to_unix_nanos
    from nautilus_trader.model.currencies import USD
    from nautilus_trader.model.data import Bar, BarSpecification, BarType
    from nautilus_trader.model.enums import (
        AccountType,
        BarAggregation,
        OmsType,
        OrderSide,
        PriceType,
        TimeInForce,
    )
    from nautilus_trader.model.identifiers import InstrumentId, TraderId, Venue
    from nautilus_trader.model.objects import Money, Quantity
    from nautilus_trader.test_kit.providers import TestInstrumentProvider
    from nautilus_trader.trading.strategy import Strategy

    class SmokeStrategyConfig(StrategyConfig, frozen=True):
        instrument_id: InstrumentId
        bar_type: BarType

    class SmokeStrategy(Strategy):
        def __init__(self, config: SmokeStrategyConfig):
            super().__init__(config)
            self.bars_processed = 0

        def on_start(self) -> None:
            self.subscribe_bars(self.config.bar_type)

        def on_bar(self, bar: Bar) -> None:
            self.bars_processed += 1
            if self.bars_processed == 5:
                self.submit_order(
                    self.order_factory.market(
                        instrument_id=self.config.instrument_id,
                        order_side=OrderSide.BUY,
                        quantity=Quantity.from_int(100),
                        time_in_force=TimeInForce.GTC,
                    )
                )
            elif self.bars_processed == 45 and not self.portfolio.is_completely_flat():
                self.close_all_positions(self.config.instrument_id)

    info = engine_info()
    if not info.available:
        raise RuntimeError(f"NautilusTrader is unavailable: {info.error}")

    destination = create_experiment(
        strategy_path,
        [data_manifest_path],
        experiment_root,
        repo_root,
        upstream_versions,
    )
    started_at = datetime.now(UTC)
    engine = BacktestEngine(
        BacktestEngineConfig(
            trader_id=TraderId("QUANT-STACK-001"),
            logging=LoggingConfig(bypass_logging=True),
        )
    )
    venue = Venue("XNAS")
    try:
        engine.add_venue(
            venue=venue,
            oms_type=OmsType.NETTING,
            account_type=AccountType.CASH,
            base_currency=USD,
            starting_balances=[Money(100_000, USD)],
        )
        instrument = TestInstrumentProvider.equity("SPY", "XNAS")
        engine.add_instrument(instrument)
        bar_type = BarType(
            instrument.id,
            BarSpecification(1, BarAggregation.DAY, PriceType.LAST),
        )
        base_ns = dt_to_unix_nanos(datetime(2024, 1, 1, tzinfo=UTC))
        day_ns = 86_400_000_000_000
        bars: list[Bar] = []
        for index in range(60):
            trend = 100.0 + index * 0.35
            cycle = math.sin(index / 4.0) * 1.25
            close = trend + cycle
            bars.append(
                Bar(
                    bar_type=bar_type,
                    open=instrument.make_price(close - 0.15),
                    high=instrument.make_price(close + 0.75),
                    low=instrument.make_price(close - 0.80),
                    close=instrument.make_price(close),
                    volume=Quantity.from_int(1_000_000 + index * 1_000),
                    ts_event=base_ns + index * day_ns,
                    ts_init=base_ns + index * day_ns,
                )
            )
        engine.add_data(bars)
        strategy = SmokeStrategy(
            SmokeStrategyConfig(instrument_id=instrument.id, bar_type=bar_type)
        )
        engine.add_strategy(strategy)
        engine.run()

        fills_report = engine.trader.generate_order_fills_report()
        account_report = engine.trader.generate_account_report(venue)
        equity_curve = [float(str(value)) for value in account_report["total"].tolist()]
        timestamps = [timestamp.isoformat() for timestamp in account_report.index]
        final_equity = equity_curve[-1]
        pnl_stats = engine.portfolio.analyzer.get_performance_stats_pnls()
        return_stats = engine.portfolio.analyzer.get_performance_stats_returns()
        metrics = {
            "initial_equity": equity_curve[0],
            "final_equity": final_equity,
            "total_return": final_equity / equity_curve[0] - 1.0,
            "max_drawdown": _max_drawdown(equity_curve),
            "trade_count": int(len(fills_report.index)),
            "position_count": int(len(engine.cache.positions_closed())),
            "pnl_total": _json_number(pnl_stats.get("PnL (total)")),
            "win_rate": _json_number(pnl_stats.get("Win Rate")),
            "profit_factor": _json_number(return_stats.get("Profit Factor")),
        }
        result = {
            "status": "completed",
            "engine": {"name": "NautilusTrader", "version": info.version},
            "mode": "deterministic_synthetic_validation",
            "instrument": str(instrument.id),
            "bars_processed": strategy.bars_processed,
            "started_at": started_at.isoformat(),
            "completed_at": datetime.now(UTC).isoformat(),
            "metrics": metrics,
            "equity_curve": [
                {"timestamp": timestamp, "equity": value}
                for timestamp, value in zip(timestamps, equity_curve, strict=True)
            ],
            "warnings": [
                "Synthetic validation data only; this result is not investment evidence.",
                "No broker connection or live-order path is enabled.",
            ],
        }
        import json

        (destination / "backtest_result.json").write_text(
            json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
        (destination / "report.md").write_text(
            "\n".join(
                [
                    f"# Nautilus validation — {destination.name}",
                    "",
                    f"- Engine: NautilusTrader {info.version}",
                    f"- Bars processed: {strategy.bars_processed}",
                    f"- Order fills: {metrics['trade_count']}",
                    f"- Final equity: {final_equity:.2f} USD",
                    f"- Total return: {metrics['total_return']:.4%}",
                    "- Scope: deterministic engine/integration validation only",
                    "- Live trading: disabled",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        manifest_path = destination / "experiment_manifest.yaml"
        import yaml

        manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
        manifest["status"] = "completed"
        manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")
        result["experiment_id"] = destination.name
        return result
    except Exception:
        manifest_path = destination / "experiment_manifest.yaml"
        if manifest_path.exists():
            import yaml

            manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
            manifest["status"] = "failed"
            manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")
        raise
    finally:
        engine.dispose()
