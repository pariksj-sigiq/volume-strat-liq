"""In-memory realtime trading terminal state."""

from __future__ import annotations

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from threading import RLock
from typing import Any
from zoneinfo import ZoneInfo

from src.analytics.rolling_state import SymbolState
from src.ingest.feeds import MarketTick
from src.signals.base import Bar, Signal
from src.signals.breakout import BreakoutSignal
from src.signals.filters import PreSignalGateChain


IST = ZoneInfo("Asia/Kolkata")


@dataclass(slots=True, frozen=True)
class TerminalInstrument:
    """Instrument metadata shown and scanned by the terminal."""

    symbol: str
    instrument_key: str
    trading_symbol: str
    sector: str = "Unknown"
    avg_turnover_cr: float = 0.0


@dataclass(slots=True, frozen=True)
class TerminalAlert:
    """Operator-facing alert emitted by the live scanner."""

    symbol: str
    sector: str
    entry: float
    sl: float
    tp: float
    volume_multiple: float
    reason: str
    generated_at: datetime
    profile_name: str = "slow"
    profile_label: str = "Slow breakout"

    @property
    def risk_reward(self) -> str:
        risk = self.entry - self.sl
        reward = self.tp - self.entry
        multiple = reward / risk if risk > 0 else 0.0
        return f"1:{multiple:.2f}"

    def as_dict(self) -> dict[str, Any]:
        """Serialize this alert for the browser terminal."""

        return {
            "symbol": self.symbol,
            "sector": self.sector,
            "entry": round(self.entry, 2),
            "sl": round(self.sl, 2),
            "tp": round(self.tp, 2),
            "risk_reward": self.risk_reward,
            "volume_multiple": round(self.volume_multiple, 2),
            "reason": self.reason,
            "generated_at": self.generated_at.isoformat(),
            "profile_name": self.profile_name,
            "profile_label": self.profile_label,
        }


class TerminalAlertEngine:
    """Bridge closed bars into official signal alerts."""

    def __init__(
        self,
        *,
        signal: BreakoutSignal | None = None,
        signals: tuple[BreakoutSignal, ...] | None = None,
        baseline_by_symbol: dict[str, list[float]],
        gate_chain: PreSignalGateChain | None = None,
        on_blocked: Callable[[str, str, datetime], None] | None = None,
    ) -> None:
        if signal is not None and signals is not None:
            raise ValueError("pass either signal or signals, not both")
        self._signals = signals or ((signal,) if signal is not None else ())
        if not self._signals:
            raise ValueError("at least one breakout signal profile is required")
        self._baseline_by_symbol = baseline_by_symbol
        self._gate_chain = gate_chain
        self._on_blocked = on_blocked
        self._states: dict[str, SymbolState] = {}

    def prewarm(self, instrument: TerminalInstrument, bars: list[Bar]) -> None:
        """Seed a symbol's rolling state from historical prior-session bars."""

        if not bars:
            return
        state = self._state_for(instrument)
        state.prewarm(bars)

    def on_bar(self, instrument: TerminalInstrument, bar: Bar) -> list[TerminalAlert]:
        """Update symbol state and return alerts for profiles that fire."""

        state = self._state_for(instrument)
        state.update_bar(bar)
        if self._gate_chain is not None:
            decision = self._gate_chain.check(symbol=instrument.symbol, bar=bar, state=state)
            if not decision.allowed:
                state.blocked_reason = decision.reason
                if decision.reason and self._on_blocked is not None:
                    self._on_blocked(instrument.symbol, decision.reason, bar.ts)
                return []
        alerts: list[TerminalAlert] = []
        for signal_profile in self._signals:
            signal = signal_profile.on_bar_close(instrument.symbol, bar, state)
            if signal is None:
                if state.blocked_reason and self._on_blocked is not None:
                    self._on_blocked(instrument.symbol, state.blocked_reason, bar.ts)
                continue
            alerts.append(self._alert_from_signal(instrument, signal))
        return alerts

    def blocked_reasons(self) -> dict[str, str]:
        """Return the latest per-symbol blocked reason for diagnostics."""

        return {
            symbol: state.blocked_reason
            for symbol, state in self._states.items()
            if state.blocked_reason
        }

    @staticmethod
    def _alert_from_signal(instrument: TerminalInstrument, signal: Signal) -> TerminalAlert:
        return TerminalAlert(
            symbol=signal.symbol,
            sector=instrument.sector,
            entry=signal.entry,
            sl=signal.sl,
            tp=signal.tp,
            volume_multiple=signal.volume_multiple or 0.0,
            reason=signal.reason,
            generated_at=signal.generated_at,
            profile_name=signal.profile_name,
            profile_label=signal.profile_label,
        )

    def _state_for(self, instrument: TerminalInstrument) -> SymbolState:
        state = self._states.get(instrument.symbol)
        if state is None:
            state = SymbolState(
                symbol=instrument.symbol,
                baseline_by_minute=self._baseline_by_symbol.get(instrument.symbol),
            )
            self._states[instrument.symbol] = state
        return state


class LiveTerminalState:
    """Thread-safe in-memory state backing the browser terminal."""

    def __init__(self, *, max_events: int = 300) -> None:
        self._lock = RLock()
        self._instruments: dict[str, TerminalInstrument] = {}
        self._ticks: dict[str, MarketTick] = {}
        self._tick_receipts: deque[datetime] = deque(maxlen=20000)
        self._alerts: deque[TerminalAlert] = deque(maxlen=max_events)
        self._events: deque[dict[str, Any]] = deque(maxlen=max_events)
        self._signal_block_counts: dict[str, int] = {}
        self._latest_signal_blocks: dict[str, dict[str, Any]] = {}
        self.connected = False
        self.feed_mode = "ltpc"
        self.last_error: str | None = None
        self.last_tick_at: datetime | None = None
        self.signals_enabled = False
        self.signal_mode = "warmup_only"
        self.signal_status_reason = "TOD baseline not loaded"
        self.baseline_count = 0
        self.baseline_required = 0
        self.warmup_seed_count = 0
        self.warmup_required_count = 0
        self.warmup_status_reason = "Rolling state not prewarmed"
        self.ticks_total = 0
        self.bar_interval = "1min"
        self.min_bars_for_signal = 25

    def set_universe(self, instruments: list[TerminalInstrument]) -> None:
        """Set the currently scanned universe."""

        with self._lock:
            self._instruments = {item.instrument_key: item for item in instruments}

    def instrument_for_key(self, instrument_key: str) -> TerminalInstrument | None:
        """Return terminal metadata for an Upstox instrument key."""

        with self._lock:
            return self._instruments.get(instrument_key)

    def symbol_by_instrument(self) -> dict[str, str]:
        """Return the instrument-key to symbol mapping for feed decoding."""

        with self._lock:
            return {key: item.symbol for key, item in self._instruments.items()}

    def record_tick(self, tick: MarketTick) -> None:
        """Record the latest tick for a symbol."""

        with self._lock:
            self._ticks[tick.symbol] = tick
            self.last_tick_at = tick.ts
            self.ticks_total += 1
            self._tick_receipts.append(datetime.now(tz=IST))

    def record_alert(
        self,
        *,
        symbol: str,
        sector: str,
        entry: float,
        sl: float,
        tp: float,
        volume_multiple: float,
        reason: str,
        generated_at: datetime,
        profile_name: str = "slow",
        profile_label: str = "Slow breakout",
    ) -> TerminalAlert:
        """Record and return an operator alert."""

        alert = TerminalAlert(
            symbol=symbol,
            sector=sector,
            entry=entry,
            sl=sl,
            tp=tp,
            volume_multiple=volume_multiple,
            reason=reason,
            generated_at=generated_at,
            profile_name=profile_name,
            profile_label=profile_label,
        )
        with self._lock:
            self._alerts.appendleft(alert)
            self._events.appendleft({"type": "alert", **alert.as_dict()})
        return alert

    def record_engine_alert(self, alert: TerminalAlert) -> None:
        """Record an alert created by the alert engine."""

        with self._lock:
            self._alerts.appendleft(alert)
            self._events.appendleft({"type": "alert", **alert.as_dict()})

    def record_signal_block(self, symbol: str, reason: str, bar_ts: datetime) -> None:
        """Record the latest reason a symbol was blocked before alerting."""

        with self._lock:
            self._signal_block_counts[reason] = self._signal_block_counts.get(reason, 0) + 1
            self._latest_signal_blocks[symbol] = {
                "symbol": symbol,
                "reason": reason,
                "bar_ts": bar_ts.isoformat(),
            }

    def set_warmup_status(self, *, seed_count: int, required_count: int, reason: str) -> None:
        """Expose prior-session rolling-state warmup coverage."""

        with self._lock:
            self.warmup_seed_count = seed_count
            self.warmup_required_count = required_count
            self.warmup_status_reason = reason
            self._events.appendleft(
                {
                    "type": "warmup_status",
                    "seed_count": seed_count,
                    "required_count": required_count,
                    "reason": reason,
                    "ts": datetime.now(tz=IST).isoformat(),
                }
            )

    def set_connection(self, *, connected: bool, mode: str | None = None, error: str | None = None) -> None:
        """Update connection status without exposing credentials."""

        with self._lock:
            self.connected = connected
            if mode:
                self.feed_mode = mode
            self.last_error = error
            self._events.appendleft(
                {
                    "type": "connection",
                    "connected": connected,
                    "mode": self.feed_mode,
                    "error": error,
                    "ts": datetime.now(tz=IST).isoformat(),
                }
            )

    def set_signal_status(
        self,
        *,
        enabled: bool,
        mode: str,
        baseline_count: int,
        baseline_required: int,
        reason: str,
        bar_interval: str | None = None,
        min_bars_for_signal: int | None = None,
    ) -> None:
        """Expose whether signal alerts are structurally enabled."""

        with self._lock:
            self.signals_enabled = enabled
            self.signal_mode = mode
            self.baseline_count = baseline_count
            self.baseline_required = baseline_required
            self.signal_status_reason = reason
            if bar_interval is not None:
                self.bar_interval = bar_interval
            if min_bars_for_signal is not None:
                self.min_bars_for_signal = min_bars_for_signal
            self._events.appendleft(
                {
                    "type": "signal_status",
                    "enabled": enabled,
                    "mode": mode,
                    "reason": reason,
                    "ts": datetime.now(tz=IST).isoformat(),
                }
            )

    def snapshot(self, *, now: datetime | None = None) -> dict[str, Any]:
        """Return a browser-ready snapshot of the terminal."""

        current = now or datetime.now(tz=IST)
        with self._lock:
            ticks = sorted(self._ticks.values(), key=lambda item: item.ts, reverse=True)
            tick_rate_cutoff = current - timedelta(seconds=60)
            tick_rate_per_min = sum(1 for item in self._tick_receipts if tick_rate_cutoff <= item <= current)
            return {
                "ok": True,
                "connected": self.connected,
                "feed_mode": self.feed_mode,
                "universe_count": len(self._instruments),
                "tick_count": len(self._ticks),
                "subscribed_instrument_count": len(self._instruments),
                "active_instrument_count": len(self._ticks),
                "ticks_total": self.ticks_total,
                "tick_rate_per_min": tick_rate_per_min,
                "bar_interval": self.bar_interval,
                "min_bars_for_signal": self.min_bars_for_signal,
                "alert_count": len(self._alerts),
                "last_error": self.last_error,
                "last_tick_at": self.last_tick_at.isoformat() if self.last_tick_at else None,
                "signals_enabled": self.signals_enabled,
                "signal_mode": self.signal_mode,
                "signal_status_reason": self.signal_status_reason,
                "baseline_count": self.baseline_count,
                "baseline_required": self.baseline_required,
                "warmup_seed_count": self.warmup_seed_count,
                "warmup_required_count": self.warmup_required_count,
                "warmup_status_reason": self.warmup_status_reason,
                "signal_block_counts": dict(sorted(self._signal_block_counts.items())),
                "latest_signal_blocks": list(self._latest_signal_blocks.values())[:100],
                "server_time": current.isoformat(),
                "ticks": [self._tick_to_dict(tick) for tick in ticks[:100]],
                "alerts": [alert.as_dict() for alert in list(self._alerts)[:50]],
                "events": list(self._events)[:100],
            }

    @staticmethod
    def _tick_to_dict(tick: MarketTick) -> dict[str, Any]:
        return {
            "symbol": tick.symbol,
            "instrument_key": tick.instrument_key,
            "ts": tick.ts.isoformat(),
            "ltp": tick.ltp,
            "close_price": tick.close_price,
            "change_pct": (
                ((tick.ltp - tick.close_price) / tick.close_price) * 100.0
                if tick.close_price
                else None
            ),
            "last_quantity": tick.last_quantity,
            "volume_traded_today": tick.volume_traded_today,
            "open_interest": tick.open_interest,
            "best_bid": tick.best_bid,
            "best_ask": tick.best_ask,
        }
