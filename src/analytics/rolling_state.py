"""Per-symbol rolling state backed by bounded deques."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import date, time

from src.analytics.indicators import atr_wilder
from src.signals.base import Bar


SESSION_OPEN_MINUTE = 9 * 60 + 15
SESSION_MINUTES = 375


def minute_of_day_index(value: time) -> int:
    """Map an IST wall-clock time to the NSE regular-session minute index."""

    return value.hour * 60 + value.minute - SESSION_OPEN_MINUTE


@dataclass(slots=True)
class SymbolState:
    """Hot-path state for one symbol."""

    symbol: str
    baseline_by_minute: list[float] | None
    max_bars: int = 128
    bars: deque[Bar] = field(init=False)
    bars_seen: int = 0
    vwap_session: float | None = None
    atr14: float | None = None
    blocked_reason: str | None = None
    session_date: date | None = None
    previous_close_seed: float | None = None
    _cum_pv: float = 0.0
    _cum_volume: float = 0.0

    def __post_init__(self) -> None:
        self.bars = deque(maxlen=self.max_bars)

    @property
    def tod_baseline_ready(self) -> bool:
        """Return whether this symbol has a loaded TOD baseline."""

        return bool(self.baseline_by_minute)

    def update_bar(self, bar: Bar) -> None:
        """Append a closed bar and recompute close-driven indicators."""

        bar_date = bar.ts.date()
        if self.session_date != bar_date:
            self._cum_pv = 0.0
            self._cum_volume = 0.0
            self.vwap_session = None
            self.atr14 = None
            if not self.bars:
                self.previous_close_seed = bar.previous_close
            self.session_date = bar_date
        self.bars.append(bar)
        self.bars_seen += 1
        typical_price = (bar.high + bar.low + bar.close) / 3.0
        self._cum_pv += typical_price * bar.volume
        self._cum_volume += bar.volume
        self.vwap_session = self._cum_pv / self._cum_volume if self._cum_volume > 0 else bar.close
        self.atr14 = atr_wilder(list(self.bars), 14, previous_close=self.previous_close_seed)
        self.blocked_reason = None

    def prewarm(self, bars: list[Bar]) -> None:
        """Seed rolling bars from the prior session without starting VWAP."""

        for bar in sorted(bars, key=lambda item: item.ts):
            self.bars.append(bar)
        self.bars_seen = len(self.bars)
        if self.bars:
            self.session_date = self.bars[-1].ts.date()
            self.previous_close_seed = self.bars[0].previous_close
            self.atr14 = atr_wilder(list(self.bars), 14, previous_close=self.previous_close_seed)
        self._cum_pv = 0.0
        self._cum_volume = 0.0
        self.vwap_session = None
        self.blocked_reason = None

    def tod_baseline_at(self, bar_time: time) -> float | None:
        """Return the precomputed time-of-day baseline for a bar time."""

        if not self.baseline_by_minute:
            self.blocked_reason = "baseline_not_ready"
            return None
        minute_index = minute_of_day_index(bar_time)
        if minute_index < 0 or minute_index >= len(self.baseline_by_minute):
            self.blocked_reason = "outside_regular_session"
            return None
        baseline = self.baseline_by_minute[minute_index]
        if baseline <= 0:
            self.blocked_reason = "baseline_not_ready"
            return None
        return baseline
