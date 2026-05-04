from __future__ import annotations

from dataclasses import replace
from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo

from src.analytics.indicators import atr_wilder
from src.analytics.rolling_state import SymbolState
from src.ingest.bar_aggregator import OneMinuteBarAggregator
from src.ingest.feeds import feed_dict_to_ticks
from src.ingest.upstox_ws import UpstoxWsConfig, build_subscription_message
from src.signals.breakout import BreakoutSignal
from src.signals.base import Bar
from src.signals.filters import PreSignalGateChain, SessionWindowGate, TaintedBarGate, TimeWindow
from src.terminal import LiveTerminalState, TerminalAlertEngine, TerminalInstrument


IST = ZoneInfo("Asia/Kolkata")


def _bar(minute: int, close: float, *, high: float | None = None, low: float | None = None, volume: float = 100.0) -> Bar:
    high_value = high if high is not None else close + 0.4
    low_value = low if low is not None else close - 0.4
    return Bar(
        symbol="TEST",
        ts=datetime(2026, 5, 4, 9, 15 + minute, tzinfo=IST),
        open=close - 0.2,
        high=high_value,
        low=low_value,
        close=close,
        volume=volume,
        instrument_key="NSE_EQ|TEST",
    )


def _previous_day_bar(minute: int, close: float, *, high: float | None = None, low: float | None = None, volume: float = 100.0) -> Bar:
    high_value = high if high is not None else close + 0.4
    low_value = low if low is not None else close - 0.4
    return Bar(
        symbol="TEST",
        ts=datetime(2026, 5, 1, 15, 10 + minute, tzinfo=IST),
        open=close - 0.2,
        high=high_value,
        low=low_value,
        close=close,
        volume=volume,
        instrument_key="NSE_EQ|TEST",
    )


def test_feed_dict_to_ticks_extracts_ltp_full_depth_and_minute_ohlc() -> None:
    payload = {
        "type": "live_feed",
        "currentTs": "1777886105123",
        "feeds": {
            "NSE_EQ|TEST": {
                "fullFeed": {
                    "marketFF": {
                        "ltpc": {"ltp": 101.25, "ltt": "1777886105000", "ltq": "25", "cp": 100.0},
                        "marketLevel": {
                            "bidAskQuote": [
                                {"bidQ": "100", "bidP": 101.2, "askQ": "150", "askP": 101.3},
                            ],
                        },
                        "marketOHLC": {
                            "ohlc": [
                                {"interval": "I1", "open": 101, "high": 102, "low": 100.5, "close": 101.25, "vol": "500", "ts": "1777886100000"},
                            ],
                        },
                        "vtt": "1200",
                        "oi": 50000,
                    },
                },
            },
        },
    }

    ticks = feed_dict_to_ticks(payload, symbol_by_instrument={"NSE_EQ|TEST": "TEST"})

    assert len(ticks) == 1
    tick = ticks[0]
    assert tick.symbol == "TEST"
    assert tick.instrument_key == "NSE_EQ|TEST"
    assert tick.ltp == 101.25
    assert tick.last_quantity == 25
    assert tick.volume_traded_today == 1200
    assert tick.open_interest == 50000
    assert tick.best_bid == 101.2
    assert tick.best_ask == 101.3
    assert tick.minute_ohlc is not None
    assert tick.minute_ohlc.close == 101.25


def test_one_minute_bar_aggregator_closes_previous_minute_with_volume_delta() -> None:
    aggregator = OneMinuteBarAggregator()
    ticks = feed_dict_to_ticks(
        {
            "type": "live_feed",
            "feeds": {
                "NSE_EQ|TEST": {"ltpc": {"ltp": 100.0, "ltt": "1777886105000", "ltq": "10", "cp": 99.0}},
            },
            "currentTs": "1777886105000",
        },
        symbol_by_instrument={"NSE_EQ|TEST": "TEST"},
    )
    first_tick = ticks[0].with_price(
        100.0,
        ts=datetime(2026, 5, 4, 9, 15, 5, tzinfo=IST),
        volume_traded_today=1000,
    )
    second_tick = first_tick.with_price(101.0, ts=datetime(2026, 5, 4, 9, 15, 40, tzinfo=IST), volume_traded_today=1040)
    next_minute = first_tick.with_price(102.0, ts=datetime(2026, 5, 4, 9, 16, 2, tzinfo=IST), volume_traded_today=1100)

    assert aggregator.update(first_tick) == []
    assert aggregator.update(second_tick) == []
    closed = aggregator.update(next_minute)

    assert len(closed) == 1
    assert closed[0].symbol == "TEST"
    assert closed[0].open == 100.0
    assert closed[0].high == 101.0
    assert closed[0].low == 100.0
    assert closed[0].close == 101.0
    assert closed[0].volume == 40


def test_one_minute_bar_aggregator_taints_bars_across_tick_gaps() -> None:
    aggregator = OneMinuteBarAggregator(max_tick_gap_sec=60)
    first_tick = feed_dict_to_ticks(
        {
            "type": "live_feed",
            "feeds": {
                "NSE_EQ|TEST": {
                    "fullFeed": {
                        "marketFF": {
                            "ltpc": {"ltp": 100.0, "ltt": "1777886105000", "ltq": "10", "cp": 99.0},
                            "vtt": "1000",
                        }
                    }
                }
            },
            "currentTs": "1777886105000",
        },
        symbol_by_instrument={"NSE_EQ|TEST": "TEST"},
    )[0].with_price(100.0, ts=datetime(2026, 5, 4, 9, 15, 5, tzinfo=IST), volume_traded_today=1000)
    resume_tick = first_tick.with_price(102.0, ts=datetime(2026, 5, 4, 9, 17, 5, tzinfo=IST), volume_traded_today=1500)
    next_tick = first_tick.with_price(103.0, ts=datetime(2026, 5, 4, 9, 18, 1, tzinfo=IST), volume_traded_today=1600)

    assert aggregator.update(first_tick) == []
    closed_gap_bar = aggregator.update(resume_tick)
    closed_resume_bar = aggregator.update(next_tick)

    assert closed_gap_bar[0].tainted is True
    assert closed_gap_bar[0].taint_reason == "tick_gap"
    assert closed_resume_bar[0].tainted is True
    assert closed_resume_bar[0].taint_reason == "tick_gap_resume"


def test_breakout_signal_requires_tod_baseline_before_alerting() -> None:
    signal = BreakoutSignal(
        min_bars=25,
        lookback_bars=20,
        volume_mult=2.5,
        min_range_atr=0.5,
        tp_r_multiple=2.0,
    )
    state = SymbolState(symbol="TEST", baseline_by_minute=None)
    for minute in range(25):
        state.update_bar(_bar(minute, 100 + minute * 0.1, volume=100))

    candidate = _bar(25, 104.0, high=105.5, low=102.5, volume=10_000)
    state.update_bar(candidate)

    assert signal.on_bar_close("TEST", candidate, state) is None
    assert state.blocked_reason == "baseline_not_ready"


def test_symbol_state_resets_session_vwap_on_new_trading_day() -> None:
    state = SymbolState(symbol="TEST", baseline_by_minute=[100.0] * 375)
    state.update_bar(_bar(0, 100.0, volume=100))
    state.update_bar(_bar(1, 102.0, volume=100))
    assert state.bars_seen == 2

    next_day = replace(
        _bar(0, 110.0, high=111.0, low=109.0, volume=50),
        ts=datetime(2026, 5, 5, 9, 15, tzinfo=IST),
        previous_close=102.0,
    )
    state.update_bar(next_day)

    assert state.bars_seen == 3
    assert len(state.bars) == 3
    assert state.previous_close_seed is None
    assert state.vwap_session == 110.0


def test_terminal_alert_engine_emits_breakout_when_baseline_and_state_are_ready() -> None:
    baseline = [100.0] * 375
    engine = TerminalAlertEngine(
        signal=BreakoutSignal(
            min_bars=25,
            lookback_bars=20,
            volume_mult=2.5,
            min_range_atr=0.5,
            tp_r_multiple=2.0,
        ),
        baseline_by_symbol={"TEST": baseline},
    )
    instrument = TerminalInstrument(
        symbol="TEST",
        instrument_key="NSE_EQ|TEST",
        trading_symbol="TEST",
        sector="Banking",
        avg_turnover_cr=100.0,
    )
    for minute in range(25):
        bar = _bar(minute, 100 + minute * 0.1, volume=100)
        assert engine.on_bar(instrument, bar) == []

    signal_bar = _bar(25, 104.0, high=105.5, low=102.5, volume=500.0)
    alerts = engine.on_bar(instrument, signal_bar)

    assert len(alerts) == 1
    alert = alerts[0]
    assert alert.symbol == "TEST"
    assert alert.sector == "Banking"
    assert alert.entry == 104.0
    assert alert.tp > alert.entry
    assert alert.risk_reward == "1:2.00"
    assert alert.volume_multiple == 5.0


def test_terminal_alert_engine_tags_fast_and_slow_profiles_separately() -> None:
    baseline = [100.0] * 375
    engine = TerminalAlertEngine(
        signals=(
            BreakoutSignal(
                profile_name="slow",
                profile_label="Slow breakout",
                min_bars=25,
                lookback_bars=20,
                volume_mult=2.5,
                min_range_atr=0.5,
                tp_r_multiple=2.0,
            ),
            BreakoutSignal(
                profile_name="fast",
                profile_label="Fast ignition",
                min_bars=25,
                lookback_bars=5,
                volume_mult=4.0,
                min_range_atr=0.5,
                tp_r_multiple=2.0,
            ),
        ),
        baseline_by_symbol={"TEST": baseline},
    )
    instrument = TerminalInstrument(
        symbol="TEST",
        instrument_key="NSE_EQ|TEST",
        trading_symbol="TEST",
    )
    for minute in range(20):
        assert engine.on_bar(instrument, _bar(minute, 100.0, high=106.0, volume=100)) == []
    for minute in range(20, 25):
        assert engine.on_bar(instrument, _bar(minute, 101.0, high=103.0, volume=100)) == []

    alerts = engine.on_bar(instrument, _bar(25, 104.0, high=105.5, low=102.5, volume=500.0))

    assert [alert.profile_name for alert in alerts] == ["fast"]
    assert alerts[0].profile_label == "Fast ignition"
    assert alerts[0].volume_multiple == 5.0


def test_terminal_alert_engine_prewarm_allows_first_post_skip_bar_to_signal() -> None:
    baseline = [100.0] * 375
    engine = TerminalAlertEngine(
        signal=BreakoutSignal(
            min_bars=25,
            lookback_bars=20,
            volume_mult=2.5,
            min_range_atr=0.5,
            tp_r_multiple=2.0,
        ),
        baseline_by_symbol={"TEST": baseline},
        gate_chain=PreSignalGateChain(
            [
                SessionWindowGate(
                    market_open=time(9, 15),
                    market_close=time(15, 30),
                    skip_windows=(TimeWindow(time(9, 15), time(9, 20)),),
                )
            ]
        ),
    )
    instrument = TerminalInstrument(
        symbol="TEST",
        instrument_key="NSE_EQ|TEST",
        trading_symbol="TEST",
    )
    engine.prewarm(instrument, [_previous_day_bar(minute, 100.0 + minute * 0.01, high=101.0) for minute in range(20)])

    for minute in range(5):
        assert engine.on_bar(instrument, _bar(minute, 100.5 + minute * 0.05, high=101.5, volume=100)) == []
    alerts = engine.on_bar(instrument, _bar(5, 104.0, high=105.5, low=102.5, volume=500.0))

    assert len(alerts) == 1
    assert alerts[0].generated_at.time() == time(9, 20)


def test_terminal_alert_engine_gates_skip_windows_before_signal_evaluation() -> None:
    baseline = [100.0] * 375
    engine = TerminalAlertEngine(
        signal=BreakoutSignal(
            min_bars=25,
            lookback_bars=20,
            volume_mult=2.5,
            min_range_atr=0.5,
            tp_r_multiple=2.0,
        ),
        baseline_by_symbol={"TEST": baseline},
        gate_chain=PreSignalGateChain(
            [
                SessionWindowGate(
                    market_open=time(9, 15),
                    market_close=time(15, 30),
                    skip_windows=(TimeWindow(time(9, 40), time(9, 41)),),
                )
            ]
        ),
    )
    instrument = TerminalInstrument(
        symbol="TEST",
        instrument_key="NSE_EQ|TEST",
        trading_symbol="TEST",
    )
    for minute in range(25):
        assert engine.on_bar(instrument, _bar(minute, 100 + minute * 0.1, volume=100)) == []

    assert engine.on_bar(instrument, _bar(25, 104.0, high=105.5, low=102.5, volume=500.0)) == []
    assert engine.blocked_reasons()["TEST"] == "skip_window"


def test_terminal_alert_engine_gates_tainted_bars_before_signal_evaluation() -> None:
    baseline = [100.0] * 375
    engine = TerminalAlertEngine(
        signal=BreakoutSignal(
            min_bars=25,
            lookback_bars=20,
            volume_mult=2.5,
            min_range_atr=0.5,
            tp_r_multiple=2.0,
        ),
        baseline_by_symbol={"TEST": baseline},
        gate_chain=PreSignalGateChain([TaintedBarGate()]),
    )
    instrument = TerminalInstrument(
        symbol="TEST",
        instrument_key="NSE_EQ|TEST",
        trading_symbol="TEST",
    )
    for minute in range(25):
        assert engine.on_bar(instrument, _bar(minute, 100 + minute * 0.1, volume=100)) == []

    tainted = replace(_bar(25, 104.0, high=105.5, low=102.5, volume=500.0), tainted=True, taint_reason="reconnect")

    assert engine.on_bar(instrument, tainted) == []
    assert engine.blocked_reasons()["TEST"] == "reconnect"


def test_breakout_lookback_uses_twenty_bars_before_current_not_current_high() -> None:
    baseline = [100.0] * 375
    engine = TerminalAlertEngine(
        signal=BreakoutSignal(
            min_bars=25,
            lookback_bars=20,
            volume_mult=2.5,
            min_range_atr=0.5,
            tp_r_multiple=2.0,
        ),
        baseline_by_symbol={"TEST": baseline},
    )
    instrument = TerminalInstrument(
        symbol="TEST",
        instrument_key="NSE_EQ|TEST",
        trading_symbol="TEST",
    )
    for minute in range(25):
        # Prior highs top out below 104, while the current signal bar's own high is 110.
        assert engine.on_bar(instrument, _bar(minute, 100 + minute * 0.1, high=103.5, volume=100)) == []

    alerts = engine.on_bar(instrument, _bar(25, 104.0, high=110.0, low=102.5, volume=500.0))

    assert len(alerts) == 1
    assert alerts[0].entry == 104.0


def test_live_terminal_state_snapshot_is_sorted_by_newest_alert_and_ticks() -> None:
    state = LiveTerminalState(max_events=10)
    instrument = TerminalInstrument(
        symbol="AAA",
        instrument_key="NSE_EQ|AAA",
        trading_symbol="AAA",
        sector="Energy",
        avg_turnover_cr=80.0,
    )
    state.set_universe([instrument])
    tick = feed_dict_to_ticks(
        {
            "type": "live_feed",
            "currentTs": "1777886105123",
            "feeds": {"NSE_EQ|AAA": {"ltpc": {"ltp": 51.0, "ltt": "1777886105000", "ltq": "10", "cp": 50.0}}},
        },
        symbol_by_instrument={"NSE_EQ|AAA": "AAA"},
    )[0]
    state.record_tick(tick)
    state.record_alert(
        symbol="AAA",
        sector="Energy",
        entry=51.0,
        sl=50.0,
        tp=53.0,
        volume_multiple=3.0,
        reason="vol_breakout_v1",
        generated_at=datetime(2026, 5, 4, 9, 40, tzinfo=IST),
    )
    state.record_signal_block("AAA", "skip_window", datetime(2026, 5, 4, 9, 17, tzinfo=IST))
    state.set_warmup_status(seed_count=1, required_count=1, reason="Prewarmed from prior session")

    payload = state.snapshot(now=datetime(2026, 5, 4, 9, 41, tzinfo=IST))

    assert payload["connected"] is False
    assert payload["universe_count"] == 1
    assert payload["subscribed_instrument_count"] == 1
    assert payload["tick_count"] == 1
    assert payload["active_instrument_count"] == 1
    assert payload["ticks_total"] == 1
    assert payload["tick_rate_per_min"] >= 0
    assert payload["bar_interval"] == "1min"
    assert payload["min_bars_for_signal"] == 25
    assert payload["signal_block_counts"]["skip_window"] == 1
    assert payload["latest_signal_blocks"][0]["reason"] == "skip_window"
    assert payload["warmup_seed_count"] == 1
    assert payload["warmup_required_count"] == 1
    assert payload["warmup_status_reason"] == "Prewarmed from prior session"
    assert payload["ticks"][0]["symbol"] == "AAA"
    assert payload["alerts"][0]["symbol"] == "AAA"
    assert payload["alerts"][0]["risk_reward"] == "1:2.00"


def test_atr_wilder_uses_recursive_wilder_smoothing_and_seed_close() -> None:
    bars = [
        _bar(0, 100, high=101, low=99),
        _bar(1, 101, high=103, low=100),
        _bar(2, 102, high=105, low=101),
    ]

    assert atr_wilder(bars[:1], period=2) is None
    assert atr_wilder(bars[:2], period=2, previous_close=98.0) == 3.0
    assert atr_wilder(bars, period=2, previous_close=98.0) == 3.5


def test_subscription_message_is_binary_json_and_config_hides_token() -> None:
    payload = build_subscription_message(["NSE_EQ|AAA", "NSE_EQ|BBB"], mode="full", guid="fixed")

    assert isinstance(payload, bytes)
    assert b'"method":"sub"' in payload
    assert b'"mode":"full"' in payload
    assert b"NSE_EQ|AAA" in payload

    config = UpstoxWsConfig(
        ws_url="wss://example.test/feed",
        access_token="secret-token",
        mode="full",
    )
    assert "secret-token" not in repr(config)
