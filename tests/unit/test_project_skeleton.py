from __future__ import annotations

import importlib


def test_realtime_scanner_packages_are_importable() -> None:
    modules = [
        "src.app",
        "src.clock",
        "src.ingest.upstox_ws",
        "src.ingest.upstox_rest",
        "src.ingest.bar_aggregator",
        "src.ingest.feeds",
        "src.analytics.rolling_state",
        "src.analytics.indicators",
        "src.analytics.tod_baseline",
        "src.signals.base",
        "src.signals.breakout",
        "src.signals.filters",
        "src.risk.sizer",
        "src.risk.portfolio",
        "src.risk.costs",
        "src.execution.base",
        "src.execution.paper",
        "src.execution.upstox_live",
        "src.backtest.runner",
        "src.backtest.data_loader",
        "src.backtest.metrics",
        "src.backtest.walk_forward",
        "src.obs.metrics",
    ]

    for module in modules:
        importlib.import_module(module)
