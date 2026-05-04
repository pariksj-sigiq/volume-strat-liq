"""Allocation-light tick-to-bar aggregation."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime

from src.ingest.feeds import MarketTick
from src.signals.base import Bar


@dataclass(slots=True)
class _MutableBar:
    symbol: str
    instrument_key: str
    minute_ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    open_interest: float | None = None
    previous_close: float | None = None
    tainted: bool = False
    taint_reason: str | None = None

    def update(self, tick: MarketTick, volume_delta: float) -> None:
        self.high = max(self.high, tick.ltp)
        self.low = min(self.low, tick.ltp)
        self.close = tick.ltp
        self.volume += max(volume_delta, 0.0)
        self.open_interest = tick.open_interest if tick.open_interest is not None else self.open_interest

    def taint(self, reason: str) -> None:
        """Mark this in-flight bar unsafe for signal evaluation."""

        self.tainted = True
        self.taint_reason = reason

    def freeze(self) -> Bar:
        return Bar(
            symbol=self.symbol,
            ts=self.minute_ts,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            instrument_key=self.instrument_key,
            open_interest=self.open_interest,
            previous_close=self.previous_close,
            tainted=self.tainted,
            taint_reason=self.taint_reason,
        )


class OneMinuteBarAggregator:
    """Aggregate live ticks into closed one-minute bars."""

    def __init__(self, *, max_tick_gap_sec: int = 60) -> None:
        self._open_bars: dict[str, _MutableBar] = {}
        self._last_cumulative_volume: dict[str, int] = {}
        self._last_tick_ts: dict[str, datetime] = {}
        self._taint_next_bar_reason: dict[str, str] = {}
        self._max_tick_gap_sec = max_tick_gap_sec

    def update(self, tick: MarketTick) -> list[Bar]:
        """Update state with a tick and return any bars closed by this tick."""

        minute_ts = tick.ts.replace(second=0, microsecond=0)
        volume_delta = self._volume_delta(tick)
        current = self._open_bars.get(tick.instrument_key)
        previous_tick_ts = self._last_tick_ts.get(tick.instrument_key)
        if previous_tick_ts is not None and (tick.ts - previous_tick_ts).total_seconds() > self._max_tick_gap_sec:
            if current is not None:
                current.taint("tick_gap")
            self._taint_next_bar_reason[tick.instrument_key] = "tick_gap_resume"
        self._last_tick_ts[tick.instrument_key] = tick.ts
        closed: list[Bar] = []
        if current is not None and current.minute_ts < minute_ts:
            closed.append(current.freeze())
            current = None
        if current is None:
            taint_reason = self._taint_next_bar_reason.pop(tick.instrument_key, None)
            current = _MutableBar(
                symbol=tick.symbol,
                instrument_key=tick.instrument_key,
                minute_ts=minute_ts,
                open=tick.ltp,
                high=tick.ltp,
                low=tick.ltp,
                close=tick.ltp,
                open_interest=tick.open_interest,
                previous_close=tick.close_price,
                tainted=taint_reason is not None,
                taint_reason=taint_reason,
            )
            self._open_bars[tick.instrument_key] = current
        else:
            current.update(tick, volume_delta)
        return closed

    def mark_instruments_tainted(self, instrument_keys: Iterable[str], *, reason: str) -> None:
        """Mark current and next bars for instruments unsafe after feed disruption."""

        for instrument_key in instrument_keys:
            current = self._open_bars.get(instrument_key)
            if current is not None:
                current.taint(reason)
            self._taint_next_bar_reason[instrument_key] = reason

    def reset_volume_reference(self, instrument_keys: Iterable[str] | None = None) -> None:
        """Reset cumulative-volume anchors, useful after reconnect snapshots."""

        if instrument_keys is None:
            self._last_cumulative_volume.clear()
            return
        for instrument_key in instrument_keys:
            self._last_cumulative_volume.pop(instrument_key, None)

    def flush(self) -> list[Bar]:
        """Return all currently open bars as closed bars and clear state."""

        closed = [bar.freeze() for bar in self._open_bars.values()]
        self._open_bars.clear()
        return closed

    def _volume_delta(self, tick: MarketTick) -> float:
        cumulative = tick.volume_traded_today
        if cumulative is None:
            return float(tick.last_quantity or 0)
        previous = self._last_cumulative_volume.get(tick.instrument_key)
        self._last_cumulative_volume[tick.instrument_key] = cumulative
        if previous is None:
            return 0.0
        return float(max(cumulative - previous, 0))
