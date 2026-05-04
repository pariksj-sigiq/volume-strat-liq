"""Volume-confirmed intraday breakout signal."""

from __future__ import annotations

from dataclasses import dataclass
import logging

from src.analytics.rolling_state import SymbolState
from src.signals.base import Bar, Signal


LOGGER = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class BreakoutSignal:
    """Primary long-only volume breakout strategy."""

    min_bars: int
    lookback_bars: int
    volume_mult: float
    min_range_atr: float
    tp_r_multiple: float
    profile_name: str = "slow"
    profile_label: str = "Slow breakout"

    def on_bar_close(self, symbol: str, bar: Bar, state: SymbolState) -> Signal | None:
        """Return a long signal if the closed bar satisfies all rules."""

        if state.bars_seen < self.min_bars:
            state.blocked_reason = "not_enough_bars"
            return None
        if not state.tod_baseline_ready:
            state.blocked_reason = "baseline_not_ready"
            return None
        prior_bars = list(state.bars)[:-1]
        if len(prior_bars) < self.lookback_bars:
            state.blocked_reason = "not_enough_bars"
            return None
        rolling_high = max(item.high for item in prior_bars[-self.lookback_bars :])
        if bar.close <= rolling_high:
            state.blocked_reason = "no_breakout"
            return None
        baseline_vol = state.tod_baseline_at(bar.ts.timetz().replace(tzinfo=None))
        if baseline_vol is None:
            return None
        volume_multiple = bar.volume / baseline_vol if baseline_vol > 0 else 0.0
        if volume_multiple < self.volume_mult:
            self._log_candidate(
                symbol=symbol,
                bar=bar,
                rolling_high=rolling_high,
                baseline_vol=baseline_vol,
                volume_multiple=volume_multiple,
                state=state,
                failed_rule="volume_below_tod_baseline",
            )
            state.blocked_reason = "volume_below_tod_baseline"
            return None
        if state.vwap_session is None or bar.close <= state.vwap_session:
            self._log_candidate(
                symbol=symbol,
                bar=bar,
                rolling_high=rolling_high,
                baseline_vol=baseline_vol,
                volume_multiple=volume_multiple,
                state=state,
                failed_rule="below_vwap",
            )
            state.blocked_reason = "below_vwap"
            return None
        if state.atr14 is None or state.atr14 <= 0:
            self._log_candidate(
                symbol=symbol,
                bar=bar,
                rolling_high=rolling_high,
                baseline_vol=baseline_vol,
                volume_multiple=volume_multiple,
                state=state,
                failed_rule="atr_not_ready",
            )
            state.blocked_reason = "atr_not_ready"
            return None
        if (bar.high - bar.low) / state.atr14 < self.min_range_atr:
            self._log_candidate(
                symbol=symbol,
                bar=bar,
                rolling_high=rolling_high,
                baseline_vol=baseline_vol,
                volume_multiple=volume_multiple,
                state=state,
                failed_rule="range_too_small",
            )
            state.blocked_reason = "range_too_small"
            return None

        recent = list(state.bars)[-5:]
        swing_low = min(item.low for item in recent)
        sl = max(swing_low, bar.close - state.atr14)
        r_value = bar.close - sl
        if r_value <= 0:
            self._log_candidate(
                symbol=symbol,
                bar=bar,
                rolling_high=rolling_high,
                baseline_vol=baseline_vol,
                volume_multiple=volume_multiple,
                state=state,
                failed_rule="degenerate_risk",
            )
            state.blocked_reason = "degenerate_risk"
            return None
        tp = bar.close + self.tp_r_multiple * r_value
        self._log_candidate(
            symbol=symbol,
            bar=bar,
            rolling_high=rolling_high,
            baseline_vol=baseline_vol,
            volume_multiple=volume_multiple,
            state=state,
            failed_rule=None,
        )
        return Signal(
            symbol=symbol,
            side="LONG",
            entry=bar.close,
            sl=sl,
            tp=tp,
            r_inr=r_value,
            generated_at=bar.ts,
            reason="vol_breakout_v1",
            volume_multiple=volume_multiple,
            instrument_key=bar.instrument_key,
            profile_name=self.profile_name,
            profile_label=self.profile_label,
        )

    def _log_candidate(
        self,
        *,
        symbol: str,
        bar: Bar,
        rolling_high: float,
        baseline_vol: float,
        volume_multiple: float,
        state: SymbolState,
        failed_rule: str | None,
    ) -> None:
        """Log rule values for bars that have already cleared breakout."""

        range_multiple = (
            (bar.high - bar.low) / state.atr14
            if state.atr14 is not None and state.atr14 > 0
            else None
        )
        LOGGER.debug(
            "breakout_candidate_evaluated",
            extra={
                "event": "breakout_candidate_evaluated",
                "symbol": symbol,
                "profile": self.profile_name,
                "bar_ts": bar.ts.isoformat(),
                "close": bar.close,
                "rolling_high": rolling_high,
                "volume": bar.volume,
                "baseline_volume": baseline_vol,
                "volume_multiple": volume_multiple,
                "volume_required_multiple": self.volume_mult,
                "vwap_session": state.vwap_session,
                "atr14": state.atr14,
                "range_multiple": range_multiple,
                "range_required_multiple": self.min_range_atr,
                "failed_rule": failed_rule,
                "passed": failed_rule is None,
            },
        )
