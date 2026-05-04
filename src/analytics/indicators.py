"""Rolling high/low, ATR, and VWAP calculations."""

from __future__ import annotations

from collections.abc import Sequence

from src.signals.base import Bar


def true_range(bar: Bar, previous_close: float | None) -> float:
    """Return the true range for a bar."""

    if previous_close is None:
        return bar.high - bar.low
    return max(
        bar.high - bar.low,
        abs(bar.high - previous_close),
        abs(bar.low - previous_close),
    )


def atr_wilder(
    bars: Sequence[Bar],
    period: int,
    *,
    previous_close: float | None = None,
) -> float | None:
    """Compute a Wilder-style ATR from the latest closed bars.

    Wilder ATR seeds with a simple average of the first ``period`` true ranges,
    then applies the recursive Wilder smoothing formula to later ranges.
    """

    if period <= 0:
        raise ValueError("period must be positive")
    if len(bars) < period:
        return None
    ranges: list[float] = []
    for index, bar in enumerate(bars):
        if index == 0:
            seed_close = previous_close if previous_close is not None else bar.previous_close
        else:
            seed_close = bars[index - 1].close
        ranges.append(true_range(bar, seed_close))
    atr = sum(ranges[:period]) / period
    for value in ranges[period:]:
        atr = ((atr * (period - 1)) + value) / period
    return atr


def rolling_high(bars: Sequence[Bar], lookback: int) -> float | None:
    """Return the high over the latest lookback bars."""

    if lookback <= 0:
        raise ValueError("lookback must be positive")
    if len(bars) < lookback:
        return None
    return max(bar.high for bar in bars[-lookback:])


def rolling_low(bars: Sequence[Bar], lookback: int) -> float | None:
    """Return the low over the latest lookback bars."""

    if lookback <= 0:
        raise ValueError("lookback must be positive")
    if len(bars) < lookback:
        return None
    return min(bar.low for bar in bars[-lookback:])
