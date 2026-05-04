"""Pre-signal gates and post-signal filter primitives."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import time
from typing import Protocol

from src.analytics.rolling_state import SymbolState
from src.signals.base import Bar


@dataclass(slots=True, frozen=True)
class GateDecision:
    """Decision returned by a pre-signal gate."""

    allowed: bool
    reason: str | None = None

    @classmethod
    def pass_(cls) -> "GateDecision":
        """Return an allowed decision."""

        return cls(True, None)

    @classmethod
    def block(cls, reason: str) -> "GateDecision":
        """Return a blocked decision with an audit reason."""

        return cls(False, reason)


class PreSignalGate(Protocol):
    """Protocol for checks that must pass before signal evaluation."""

    def check(self, *, symbol: str, bar: Bar, state: SymbolState) -> GateDecision:
        """Return whether the scanner may evaluate the signal for this bar."""


@dataclass(slots=True, frozen=True)
class TimeWindow:
    """Closed-open intraday time window."""

    start: time
    end: time

    def contains(self, value: time) -> bool:
        """Return whether ``value`` is inside this closed-open window."""

        return self.start <= value < self.end


@dataclass(slots=True, frozen=True)
class SessionWindowGate:
    """Block bars outside the regular session or inside configured skip windows."""

    market_open: time
    market_close: time
    skip_windows: tuple[TimeWindow, ...]

    def check(self, *, symbol: str, bar: Bar, state: SymbolState) -> GateDecision:
        """Return whether the bar is inside a tradable regular-session minute."""

        del symbol, state
        bar_time = bar.ts.timetz().replace(tzinfo=None)
        if not (self.market_open <= bar_time < self.market_close):
            return GateDecision.block("outside_regular_session")
        if any(window.contains(bar_time) for window in self.skip_windows):
            return GateDecision.block("skip_window")
        return GateDecision.pass_()


@dataclass(slots=True, frozen=True)
class WarmupGate:
    """Block signal evaluation until enough closed bars are available."""

    min_bars: int

    def check(self, *, symbol: str, bar: Bar, state: SymbolState) -> GateDecision:
        """Return whether the per-symbol rolling state is seasoned enough."""

        del symbol, bar
        if state.bars_seen < self.min_bars:
            return GateDecision.block("not_enough_bars")
        return GateDecision.pass_()


@dataclass(slots=True, frozen=True)
class TodBaselineGate:
    """Block signal evaluation when the symbol's baseline is unavailable."""

    def check(self, *, symbol: str, bar: Bar, state: SymbolState) -> GateDecision:
        """Return whether the symbol has a usable baseline array."""

        del symbol, bar
        if not state.tod_baseline_ready:
            return GateDecision.block("baseline_not_ready")
        return GateDecision.pass_()


@dataclass(slots=True, frozen=True)
class TaintedBarGate:
    """Block bars produced across websocket reconnects, gaps, or halts."""

    def check(self, *, symbol: str, bar: Bar, state: SymbolState) -> GateDecision:
        """Return whether this closed bar is safe for signal evaluation."""

        del symbol, state
        if bar.tainted:
            return GateDecision.block(bar.taint_reason or "tainted_bar")
        return GateDecision.pass_()


class PreSignalGateChain:
    """Run pre-signal gates in deterministic order."""

    def __init__(self, gates: Iterable[PreSignalGate]) -> None:
        self._gates = tuple(gates)

    def check(self, *, symbol: str, bar: Bar, state: SymbolState) -> GateDecision:
        """Return the first blocking gate decision, or pass."""

        for gate in self._gates:
            decision = gate.check(symbol=symbol, bar=bar, state=state)
            if not decision.allowed:
                return decision
        return GateDecision.pass_()
