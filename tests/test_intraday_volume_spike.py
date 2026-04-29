from __future__ import annotations

import unittest
import sqlite3
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

from backtest.intraday_volume_spike import (
    IntradayBar,
    IntradayScalpConfig,
    build_intraday_analysis_payload,
    build_intraday_symbol_payload,
    compute_intraday_summary,
    find_volume_spike_setups,
    load_intraday_bars,
    run_intraday_bucketed_backtest,
    run_intraday_universe_backtest,
    simulate_intraday_exits,
)
from app.server import (
    build_intraday_payload_from_query,
    build_intraday_report_from_query,
    build_precomputed_intraday_report,
)


def _bar(
    offset: int,
    *,
    open_price: float = 100.0,
    high: float = 100.4,
    low: float = 99.8,
    close: float = 100.1,
    volume: float = 100.0,
    symbol: str = "TEST",
) -> IntradayBar:
    return IntradayBar(
        symbol=symbol,
        timestamp=datetime(2026, 4, 27, 9, 15) + timedelta(minutes=offset),
        open=open_price,
        high=high,
        low=low,
        close=close,
        volume=volume,
        turnover=volume * close,
        instrument_key="NSE_FO|TEST|2026-04-30",
        trading_symbol="TEST APR FUT",
        contract_expiry="2026-04-30",
        lot_size=125,
        open_interest=50_000,
        source="synthetic",
    )


def _bars_with_spike(
    *,
    spike_volume: float = 500.0,
    spike_high: float = 101.2,
    spike_close: float = 101.05,
    spike_low: float = 99.7,
) -> list[IntradayBar]:
    bars = [
        _bar(0, high=100.2, low=99.9, close=100.0, volume=90.0),
        _bar(1, high=100.3, low=99.85, close=100.05, volume=100.0),
        _bar(2, high=100.25, low=99.8, close=100.1, volume=110.0),
        _bar(3, high=100.35, low=99.75, close=100.0, volume=100.0),
        _bar(4, high=100.3, low=99.9, close=100.05, volume=95.0),
        _bar(5, open_price=100.2, high=spike_high, low=spike_low, close=spike_close, volume=spike_volume),
    ]
    bars.append(_bar(6, open_price=101.1, high=102.4, low=100.8, close=102.1, volume=180.0))
    bars.append(_bar(7, open_price=102.1, high=102.3, low=101.7, close=101.9, volume=130.0))
    return bars


class IntradayVolumeSpikeTests(unittest.TestCase):
    def test_detects_volume_spike_breakout_and_uses_next_bar_open_entry(self) -> None:
        config = IntradayScalpConfig(
            base_lookback=5,
            spike_multiple=3.0,
            min_turnover=25_000.0,
            close_location_threshold=0.75,
            risk_reward=2.0,
            max_hold_bars=5,
        )

        setups = find_volume_spike_setups(_bars_with_spike(), config)

        self.assertEqual(len(setups), 1)
        setup = setups[0]
        self.assertEqual(setup.symbol, "TEST")
        self.assertEqual(setup.signal_index, 5)
        self.assertEqual(setup.entry_index, 6)
        self.assertAlmostEqual(setup.entry_price, 101.1)
        self.assertAlmostEqual(setup.stop_loss, 99.7)
        self.assertAlmostEqual(setup.target_price, 103.9)
        self.assertEqual(setup.risk_reward_label, "1:2")
        self.assertAlmostEqual(setup.rolling_median_volume, 100.0)
        self.assertEqual(setup.lot_size, 125)
        self.assertEqual(setup.instrument_key, "NSE_FO|TEST|2026-04-30")
        self.assertEqual(setup.timeframe_sec, 60)

    def test_rejects_weak_volume_spike(self) -> None:
        config = IntradayScalpConfig(
            base_lookback=5,
            spike_multiple=3.0,
            min_turnover=25_000.0,
            close_location_threshold=0.75,
        )

        setups = find_volume_spike_setups(_bars_with_spike(spike_volume=250.0), config)

        self.assertEqual(setups, [])

    def test_simulates_target_stop_timeout_and_ambiguous_bar_ordering(self) -> None:
        config = IntradayScalpConfig(
            base_lookback=5,
            spike_multiple=3.0,
            min_turnover=25_000.0,
            close_location_threshold=0.75,
            risk_reward=1.0,
            max_hold_bars=2,
        )

        target_bars = _bars_with_spike()
        target_setup = find_volume_spike_setups(target_bars, config)[0]
        target_bars[6] = _bar(6, open_price=101.1, high=102.6, low=100.8, close=102.2, volume=180.0)
        target_result = simulate_intraday_exits([target_setup], target_bars, config)[0]
        self.assertEqual(target_result.exit_reason, "target")
        self.assertAlmostEqual(target_result.exit_price, target_setup.target_price)
        self.assertEqual(target_result.bars_held, 1)
        self.assertGreater(target_result.pnl_rupees, 0.0)

        stop_bars = _bars_with_spike()
        stop_setup = find_volume_spike_setups(stop_bars, config)[0]
        stop_bars[6] = _bar(6, open_price=101.1, high=101.2, low=99.4, close=99.8, volume=180.0)
        stop_result = simulate_intraday_exits([stop_setup], stop_bars, config)[0]
        self.assertEqual(stop_result.exit_reason, "stop")
        self.assertAlmostEqual(stop_result.exit_price, stop_setup.stop_loss)
        self.assertLess(stop_result.pnl_rupees, 0.0)

        ambiguous_green_bars = _bars_with_spike()
        green_setup = find_volume_spike_setups(ambiguous_green_bars, config)[0]
        ambiguous_green_bars[6] = _bar(6, open_price=101.1, high=103.0, low=99.4, close=102.5, volume=180.0)
        green_result = simulate_intraday_exits([green_setup], ambiguous_green_bars, config)[0]
        self.assertEqual(green_result.exit_reason, "target_first_ambiguous_green")
        self.assertAlmostEqual(green_result.exit_price, green_setup.target_price)

        ambiguous_red_bars = _bars_with_spike()
        red_setup = find_volume_spike_setups(ambiguous_red_bars, config)[0]
        ambiguous_red_bars[6] = _bar(6, open_price=101.1, high=103.0, low=99.4, close=100.5, volume=180.0)
        red_result = simulate_intraday_exits([red_setup], ambiguous_red_bars, config)[0]
        self.assertEqual(red_result.exit_reason, "stop_first_ambiguous_red")
        self.assertAlmostEqual(red_result.exit_price, red_setup.stop_loss)

        timeout_bars = _bars_with_spike()
        timeout_setup = find_volume_spike_setups(timeout_bars, config)[0]
        timeout_bars[6] = _bar(6, open_price=101.1, high=101.4, low=100.8, close=101.2, volume=180.0)
        timeout_bars[7] = _bar(7, open_price=101.2, high=101.5, low=100.9, close=101.3, volume=150.0)
        timeout_result = simulate_intraday_exits([timeout_setup], timeout_bars, config)[0]
        self.assertEqual(timeout_result.exit_reason, "timeout")
        self.assertAlmostEqual(timeout_result.exit_price, 101.3)
        self.assertEqual(timeout_result.bars_held, 2)

    def test_intraday_scalp_does_not_enter_or_hold_overnight_by_default(self) -> None:
        config = IntradayScalpConfig(
            base_lookback=5,
            spike_multiple=3.0,
            min_turnover=25_000.0,
            close_location_threshold=0.75,
            risk_reward=1.0,
            max_hold_bars=3,
        )

        next_day_entry_bars = _bars_with_spike()
        next_day_entry_bars[6] = _bar(24 * 60, open_price=101.1, high=103.0, low=100.8, close=102.5, volume=180.0)
        self.assertEqual(find_volume_spike_setups(next_day_entry_bars, config), [])

        overnight_hold_bars = _bars_with_spike()
        setup = find_volume_spike_setups(overnight_hold_bars, config)[0]
        overnight_hold_bars[6] = _bar(6, open_price=101.1, high=101.4, low=100.8, close=101.2, volume=180.0)
        overnight_hold_bars[7] = _bar(24 * 60, open_price=101.2, high=103.0, low=101.1, close=102.5, volume=150.0)

        result = simulate_intraday_exits([setup], overnight_hold_bars, config)[0]

        self.assertEqual(result.exit_reason, "timeout")
        self.assertEqual(result.exit_timestamp, overnight_hold_bars[6].timestamp)
        self.assertAlmostEqual(result.exit_price, 101.2)

    def test_payload_labels_intraday_mode_and_includes_replay_window(self) -> None:
        config = IntradayScalpConfig(
            base_lookback=5,
            spike_multiple=3.0,
            min_turnover=25_000.0,
            close_location_threshold=0.75,
            risk_reward=1.0,
            max_hold_bars=2,
            replay_pre_bars=3,
            replay_post_bars=2,
        )
        bars = _bars_with_spike()
        setups = find_volume_spike_setups(bars, config)
        results = simulate_intraday_exits(setups, bars, config)
        summary = compute_intraday_summary(results)

        payload = build_intraday_analysis_payload(bars, setups, results, config)

        self.assertEqual(payload["data_mode"], "intraday_volume_spike")
        self.assertEqual(payload["timeframe_sec"], 60)
        self.assertEqual(payload["summary"], summary)
        self.assertEqual(len(payload["bars"]), len(bars))
        self.assertEqual(len(payload["setups"]), 1)
        self.assertEqual(len(payload["results"]), 1)
        self.assertEqual(len(payload["replay_windows"]), 1)
        replay = payload["replay_windows"][0]
        self.assertEqual(replay["signal_index"], 5)
        self.assertEqual(replay["entry_index"], 6)
        self.assertEqual(replay["start_index"], 2)
        self.assertEqual(replay["end_index"], 7)
        self.assertEqual(replay["bars"][0]["timestamp"], bars[2].timestamp.isoformat())

    def test_load_intraday_bars_preserves_sqlite_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "intraday.db"
            _create_intraday_db(db_path)

            bars = load_intraday_bars(
                db_path,
                "TEST",
                data_mode="equity_signal_proxy_1m",
                start_date="2026-04-27",
                end_date="2026-04-27",
            )

        self.assertEqual(len(bars), 8)
        self.assertEqual(bars[0].symbol, "TEST")
        self.assertEqual(bars[0].timestamp.isoformat(), "2026-04-27T09:15:00+05:30")
        self.assertEqual(bars[0].instrument_key, "NSE_EQ|TEST")
        self.assertEqual(bars[0].trading_symbol, "TEST")
        self.assertEqual(bars[0].lot_size, 1)
        self.assertEqual(bars[0].data_mode, "equity_signal_proxy_1m")
        self.assertEqual(bars[0].timeframe_sec, 60)

    def test_build_intraday_symbol_payload_runs_from_sqlite(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "intraday.db"
            _create_intraday_db(db_path)

            payload = build_intraday_symbol_payload(
                db_path,
                symbol="TEST",
                data_mode="equity_signal_proxy_1m",
                config=IntradayScalpConfig(
                    base_lookback=5,
                    spike_multiple=3.0,
                    min_turnover=25_000.0,
                    close_location_threshold=0.75,
                    risk_reward=1.0,
                    max_hold_bars=2,
                ),
            )

        self.assertEqual(payload["symbol"], "TEST")
        self.assertEqual(payload["data_mode"], "equity_signal_proxy_1m")
        self.assertEqual(payload["timeframe_sec"], 60)
        self.assertEqual(payload["summary"]["total_trades"], 1)
        self.assertEqual(payload["results"][0]["exit_reason"], "target")

    def test_server_intraday_payload_helper_parses_query_controls(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "intraday.db"
            _create_intraday_db(db_path)

            payload, status = build_intraday_payload_from_query(
                db_path,
                {
                    "symbol": ["TEST"],
                    "data_mode": ["equity_signal_proxy_1m"],
                    "risk_reward": ["1"],
                    "base_lookback": ["5"],
                    "spike_multiple": ["3"],
                    "min_turnover": ["25000"],
                    "close_location": ["0.75"],
                    "max_hold_bars": ["2"],
                },
            )

        self.assertEqual(status.value, 200)
        self.assertEqual(payload["symbol"], "TEST")
        self.assertEqual(payload["data_mode"], "equity_signal_proxy_1m")
        self.assertEqual(payload["controls"]["risk_reward"], 1.0)
        self.assertEqual(payload["summary"]["total_trades"], 1)

    def test_server_intraday_report_helper_returns_bucketed_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "intraday.db"
            _create_intraday_db(db_path, extra_bars=_bucket_fixture_bars())

            payload, status = build_intraday_report_from_query(
                db_path,
                {
                    "symbols": ["TEST,NEXTMORN,TWODAY"],
                    "data_mode": ["equity_signal_proxy_1m"],
                    "risk_reward": ["1"],
                    "base_lookback": ["5"],
                    "spike_multiple": ["3"],
                    "min_turnover": ["25000"],
                    "close_location": ["0.75"],
                    "max_hold_bars": ["2"],
                    "two_day_hold_bars": ["4"],
                    "max_instances": ["0"],
                },
            )

        self.assertEqual(status.value, 200)
        self.assertEqual(payload["bucket_summaries"]["next_morning_entry"]["total_trades"], 1)
        self.assertEqual(payload["bucket_summaries"]["two_day_hold"]["total_trades"], 1)
        self.assertEqual(payload["controls"]["two_day_hold_bars"], 4)

    def test_run_intraday_universe_backtest_summarizes_symbols(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "intraday.db"
            _create_intraday_db(db_path)

            payload = run_intraday_universe_backtest(
                db_path,
                data_mode="equity_signal_proxy_1m",
                symbols=("TEST",),
                config=IntradayScalpConfig(
                    base_lookback=5,
                    spike_multiple=3.0,
                    min_turnover=25_000.0,
                    close_location_threshold=0.75,
                    risk_reward=1.0,
                    max_hold_bars=2,
                ),
            )

        self.assertEqual(payload["data_mode"], "equity_signal_proxy_1m")
        self.assertEqual(payload["symbols_scanned"], 1)
        self.assertEqual(payload["summary"]["total_trades"], 1)
        self.assertEqual(payload["by_symbol"][0]["symbol"], "TEST")
        self.assertEqual(payload["by_symbol"][0]["trades"], 1)

    def test_run_intraday_universe_backtest_returns_ranked_trade_instances(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "intraday.db"
            _create_intraday_db(db_path)

            payload = run_intraday_universe_backtest(
                db_path,
                data_mode="equity_signal_proxy_1m",
                symbols=("TEST",),
                config=IntradayScalpConfig(
                    base_lookback=5,
                    spike_multiple=3.0,
                    min_turnover=25_000.0,
                    close_location_threshold=0.75,
                    risk_reward=1.0,
                    max_hold_bars=2,
                ),
                max_instances=1,
                min_instance_rr=0.5,
                instance_sort="best_rr",
            )

        self.assertEqual(payload["instances_returned"], 1)
        self.assertEqual(payload["instances_total"], 1)
        self.assertEqual(len(payload["instances"]), 1)
        instance = payload["instances"][0]
        self.assertEqual(instance["symbol"], "TEST")
        self.assertEqual(instance["data_mode"], "equity_signal_proxy_1m")
        self.assertEqual(instance["risk_reward_label"], "1:1")
        self.assertAlmostEqual(instance["rr"], 1.0)
        self.assertGreaterEqual(instance["volume_multiple"], 3.0)
        self.assertEqual(instance["exit_reason"], "target")
        self.assertIn("/api/intraday/analyze?symbol=TEST", instance["review_url"])

    def test_run_intraday_bucketed_backtest_separates_next_morning_and_two_day_cases(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "intraday.db"
            _create_intraday_db(db_path, extra_bars=_bucket_fixture_bars())

            payload = run_intraday_bucketed_backtest(
                db_path,
                data_mode="equity_signal_proxy_1m",
                symbols=("TEST", "NEXTMORN", "TWODAY"),
                config=IntradayScalpConfig(
                    base_lookback=5,
                    spike_multiple=3.0,
                    min_turnover=25_000.0,
                    close_location_threshold=0.75,
                    risk_reward=1.0,
                    max_hold_bars=2,
                ),
                two_day_hold_bars=4,
                max_instances=0,
                instance_sort="latest",
            )

        self.assertEqual(payload["bucket_summaries"]["same_day"]["total_trades"], 2)
        self.assertEqual(payload["bucket_summaries"]["next_morning_entry"]["total_trades"], 1)
        self.assertEqual(payload["bucket_summaries"]["two_day_hold"]["total_trades"], 1)
        buckets_by_symbol = {
            (instance["symbol"], instance["bucket"])
            for instance in payload["instances"]
            if instance["symbol"] in {"NEXTMORN", "TWODAY"}
        }
        self.assertIn(("NEXTMORN", "next_morning_entry"), buckets_by_symbol)
        self.assertIn(("TWODAY", "two_day_hold"), buckets_by_symbol)

    def test_precomputed_intraday_report_loads_csv_for_ui(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "report.csv"
            report_path.write_text(
                "\n".join(
                    [
                        "bucket,symbol,signal_timestamp,entry_timestamp,exit_timestamp,entry_price,stop_loss,target_price,exit_price,exit_reason,bars_held,rr,max_favorable_rr,max_adverse_rr,return_pct,pnl_points,volume_multiple,spike_volume,rolling_median_volume,turnover,close_location,risk_reward_label,data_mode,review_url",
                        "same_day,TEST,2026-04-27T09:21:00+05:30,2026-04-27T09:22:00+05:30,2026-04-27T09:23:00+05:30,101,100,102,102,target,1,1,1.5,-0.1,0.99,1,4.2,4200,1000,424200,0.9,1:1,equity_signal_proxy_1m,/api/intraday/analyze?symbol=TEST",
                        "next_morning_entry,NEXT,2026-04-27T15:28:00+05:30,2026-04-28T09:15:00+05:30,2026-04-28T09:16:00+05:30,201,200,202,200,stop,1,-1,0.2,-1,-0.5,-1,6,6000,1000,1206000,0.8,1:1,equity_signal_proxy_1m,/api/intraday/analyze?symbol=NEXT",
                    ]
                ),
                encoding="utf-8",
            )

            payload = build_precomputed_intraday_report(report_path)

        self.assertEqual(payload["source"], "precomputed_csv")
        self.assertEqual(payload["instances_total"], 2)
        self.assertEqual(payload["symbols_scanned"], 2)
        self.assertEqual(payload["summary"]["wins"], 1)
        self.assertEqual(payload["summary"]["losses"], 1)
        self.assertEqual(payload["bucket_summaries"]["same_day"]["total_trades"], 1)
        first = payload["instances"][0]
        self.assertEqual(first["symbol"], "TEST")
        self.assertIsInstance(first["entry_price"], float)
        self.assertIsInstance(first["bars_held"], int)


def _create_intraday_db(db_path: Path, extra_bars: list[IntradayBar] | None = None) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE ohlcv_intraday (
                symbol TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                date TEXT NOT NULL,
                timeframe_sec INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume INTEGER NOT NULL,
                open_interest INTEGER NOT NULL DEFAULT 0,
                instrument_key TEXT NOT NULL,
                trading_symbol TEXT NOT NULL,
                market_segment TEXT NOT NULL,
                instrument_type TEXT NOT NULL,
                contract_expiry TEXT,
                lot_size INTEGER NOT NULL,
                source TEXT NOT NULL,
                data_mode TEXT NOT NULL
            )
            """
        )
        rows = []
        bars = _bars_with_spike()
        bars[6] = _bar(6, open_price=101.1, high=102.6, low=100.8, close=102.2, volume=180.0)
        for bar in [*bars, *(extra_bars or [])]:
            timestamp = bar.timestamp.replace(tzinfo=None).isoformat() + "+05:30"
            rows.append(
                (
                    bar.symbol,
                    timestamp,
                    timestamp[:10],
                    60,
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    bar.volume,
                    bar.open_interest or 0,
                    "NSE_EQ|TEST",
                    "TEST",
                    "NSE_EQ",
                    "EQ",
                    None,
                    1,
                    "synthetic",
                    "equity_signal_proxy_1m",
                )
            )
        conn.executemany(
            """
            INSERT INTO ohlcv_intraday (
                symbol, timestamp, date, timeframe_sec, open, high, low, close, volume,
                open_interest, instrument_key, trading_symbol, market_segment, instrument_type,
                contract_expiry, lot_size, source, data_mode
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
    finally:
        conn.close()


def _bucket_fixture_bars() -> list[IntradayBar]:
    next_morning = [
        _bar(369, high=100.2, low=99.9, close=100.0, volume=90.0, symbol="NEXTMORN"),
        _bar(370, high=100.3, low=99.85, close=100.05, volume=100.0, symbol="NEXTMORN"),
        _bar(371, high=100.25, low=99.8, close=100.1, volume=110.0, symbol="NEXTMORN"),
        _bar(372, high=100.35, low=99.75, close=100.0, volume=100.0, symbol="NEXTMORN"),
        _bar(373, high=100.3, low=99.9, close=100.05, volume=95.0, symbol="NEXTMORN"),
        _bar(374, open_price=100.2, high=101.2, low=99.7, close=101.05, volume=500.0, symbol="NEXTMORN"),
        _bar(24 * 60, open_price=101.1, high=102.6, low=100.8, close=102.2, volume=180.0, symbol="NEXTMORN"),
    ]
    two_day = [
        _bar(300, high=100.2, low=99.9, close=100.0, volume=90.0, symbol="TWODAY"),
        _bar(301, high=100.3, low=99.85, close=100.05, volume=100.0, symbol="TWODAY"),
        _bar(302, high=100.25, low=99.8, close=100.1, volume=110.0, symbol="TWODAY"),
        _bar(303, high=100.35, low=99.75, close=100.0, volume=100.0, symbol="TWODAY"),
        _bar(304, high=100.3, low=99.9, close=100.05, volume=95.0, symbol="TWODAY"),
        _bar(305, open_price=100.2, high=101.2, low=99.7, close=101.05, volume=500.0, symbol="TWODAY"),
        _bar(306, open_price=101.1, high=101.3, low=100.8, close=101.2, volume=180.0, symbol="TWODAY"),
        _bar(24 * 60, open_price=101.2, high=102.6, low=101.1, close=102.2, volume=150.0, symbol="TWODAY"),
    ]
    return [*next_morning, *two_day]


if __name__ == "__main__":
    unittest.main()
