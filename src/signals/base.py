"""Signal and market-bar DTOs shared by live and backtest code."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True, frozen=True)
class Bar:
    """Closed OHLCV bar for a single instrument."""

    symbol: str
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    instrument_key: str | None = None
    trading_symbol: str | None = None
    open_interest: float | None = None
    source: str = "live_ws"
    previous_close: float | None = None
    tainted: bool = False
    taint_reason: str | None = None


@dataclass(slots=True, frozen=True)
class Signal:
    """Trade signal emitted by a strategy module."""

    symbol: str
    side: str
    entry: float
    sl: float
    tp: float
    r_inr: float
    generated_at: datetime
    reason: str
    volume_multiple: float | None = None
    instrument_key: str | None = None
    profile_name: str = "slow"
    profile_label: str = "Slow breakout"
