from __future__ import annotations

import sqlite3
import tempfile
import unittest
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

from backtest.liquidity_sweep_backtest import (
    BENCHMARK_SYMBOL,
    Bar,
    BacktestConfig,
    BacktestData,
    Match,
    TradeSetup,
    apply_config_to_matches,
    build_analysis_payload,
    build_visual_review_payload,
    filter_backtest_data,
    find_pattern_matches,
    load_data,
    simulate_exits,
    simulate_rr_exits,
)


def _make_bar(symbol: str, day: date, price: float, *, green: bool = True, low: float | None = None) -> tuple:
    open_price = price - 0.5 if green else price + 0.5
    close_price = price + 0.5 if green else price - 0.5
    high = max(open_price, close_price) + 1.0
    bar_low = low if low is not None else min(open_price, close_price) - 1.0
    return (symbol, day.isoformat(), open_price, high, bar_low, close_price, close_price, 1_000_000)


class LiquiditySweepBacktestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "sample.db"
        self._create_db(self.db_path)

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def _create_db(self, db_path: Path) -> None:
        conn = sqlite3.connect(db_path)
        try:
            conn.execute(
                "CREATE TABLE stocks(symbol TEXT, theme TEXT, sub_theme TEXT, is_active INTEGER)"
            )
            conn.execute(
                """
                CREATE TABLE ohlcv_daily(
                    symbol TEXT,
                    date TEXT,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    adj_close REAL,
                    volume REAL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE futures_contracts(
                    symbol TEXT,
                    expiry_date TEXT,
                    instrument_key TEXT,
                    trading_symbol TEXT,
                    exchange TEXT,
                    segment TEXT,
                    instrument_type TEXT,
                    lot_size INTEGER,
                    tick_size REAL,
                    underlying_key TEXT,
                    underlying_type TEXT,
                    underlying_symbol TEXT,
                    source TEXT,
                    is_active INTEGER,
                    updated_at TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE ohlcv_futures_daily(
                    symbol TEXT,
                    expiry_date TEXT,
                    date TEXT,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    adj_close REAL,
                    volume REAL,
                    open_interest REAL,
                    instrument_key TEXT,
                    trading_symbol TEXT,
                    lot_size INTEGER,
                    source TEXT
                )
                """
            )
            conn.execute("INSERT INTO stocks(symbol, theme, sub_theme, is_active) VALUES (?, ?, ?, ?)", ("TEST", "Theme", "Sub", 1))
            conn.execute("INSERT INTO stocks(symbol, theme, sub_theme, is_active) VALUES (?, ?, ?, ?)", (BENCHMARK_SYMBOL, "Index", "Index", 1))

            start = date(2024, 1, 1)
            rows: list[tuple] = []
            benchmark_rows: list[tuple] = []
            futures_rows: list[tuple] = []
            for idx in range(70):
                day = start + timedelta(days=idx)
                benchmark_rows.append(_make_bar(BENCHMARK_SYMBOL, day, 100 + idx * 0.1))
                if idx == 60:
                    row = _make_bar("TEST", day, 121.0, green=False)
                    rows.append(row)
                elif idx < 63:
                    row = _make_bar("TEST", day, 100 + idx * 0.4)
                    rows.append(row)
                elif idx == 63:
                    # Signal bar: green candle, lower low than D-1.
                    row = ("TEST", day.isoformat(), 124.5, 127.5, 119.0, 126.5, 126.5, 1_000_000)
                    rows.append(row)
                else:
                    row = _make_bar("TEST", day, 126 + idx * 0.2)
                    rows.append(row)
                futures_rows.append(
                    (
                        "TEST",
                        "2024-03-28",
                        row[1],
                        row[2],
                        row[3],
                        row[4],
                        row[5],
                        row[6],
                        row[7],
                        250_000,
                        "NSE_FO|TEST|28-03-2024",
                        "TEST FUT 28 MAR 24",
                        250,
                        "upstox_expired_v2",
                    )
                )

            conn.executemany(
                "INSERT INTO ohlcv_daily(symbol, date, open, high, low, close, adj_close, volume) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                rows + benchmark_rows,
            )
            conn.execute(
                """
                INSERT INTO futures_contracts(
                    symbol, expiry_date, instrument_key, trading_symbol, exchange, segment, instrument_type,
                    lot_size, tick_size, underlying_key, underlying_type, underlying_symbol, source, is_active, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "TEST",
                    "2024-03-28",
                    "NSE_FO|TEST|28-03-2024",
                    "TEST FUT 28 MAR 24",
                    "NSE",
                    "NSE_FO",
                    "FUT",
                    250,
                    5.0,
                    "NSE_EQ|TEST",
                    "EQUITY",
                    "TEST",
                    "upstox_expired_v2",
                    0,
                    "2026-04-22T00:00:00Z",
                ),
            )
            conn.executemany(
                """
                INSERT INTO ohlcv_futures_daily(
                    symbol, expiry_date, date, open, high, low, close, adj_close, volume, open_interest,
                    instrument_key, trading_symbol, lot_size, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                futures_rows,
            )
            conn.commit()
        finally:
            conn.close()

    def test_load_data_and_find_pattern_matches(self) -> None:
        data = load_data(self.db_path, ["TEST"])
        config = BacktestConfig(db_path=self.db_path, symbols=("TEST",), output_path=Path(self.tmpdir.name) / "report.html")
        matches = find_pattern_matches(data, config)
        self.assertEqual(len(matches), 1)
        match = matches[0]
        self.assertEqual(match.symbol, "TEST")
        self.assertEqual(match.signal_bar.lot_size, 250)
        self.assertEqual(match.signal_bar.contract_expiry.isoformat(), "2024-03-28")
        self.assertTrue(match.d_green)
        self.assertTrue(match.low_sweep)
        self.assertGreater(match.stock_return_63, 0.0)
        self.assertGreater(match.stock_return_63, match.benchmark_return_63)

    def test_apply_config_uses_last_red_candle_low_and_stop_based_offset_entry(self) -> None:
        data = load_data(self.db_path, ["TEST"])
        config = BacktestConfig(
            db_path=self.db_path,
            symbols=("TEST",),
            output_path=Path(self.tmpdir.name) / "report.html",
            risk_reward=2.0,
            entry_offset_pct=0.5,
        )

        matches = find_pattern_matches(data, config)
        setups = apply_config_to_matches(matches, config)

        self.assertEqual(len(setups), 1)
        setup = setups[0]
        self.assertAlmostEqual(setup.stop_loss, 119.5)
        self.assertAlmostEqual(setup.entry_price, 119.5 * 1.005)
        expected_target = setup.entry_price + (setup.entry_price - setup.stop_loss) * 2.0
        self.assertAlmostEqual(setup.target_price, expected_target)

    def test_simulate_rr_exits_ambiguous_green_targets_first(self) -> None:
        signal_date = date(2024, 3, 1)
        signal_bar = Bar("TEST", signal_date, 100.0, 106.0, 94.0, 105.0, 105.0, 1_000_000)
        prev_bar = Bar("TEST", signal_date - timedelta(days=1), 101.0, 103.0, 95.0, 99.0, 99.0, 1_000_000)
        setup = TradeSetup(
            symbol="TEST",
            signal_date=signal_date,
            signal_index=63,
            signal_bar=signal_bar,
            prev_bar=prev_bar,
            stock_return_63=0.12,
            benchmark_return_63=0.03,
            d1_red=True,
            d_green=True,
            low_sweep=True,
            entry_price=105.0,
            stop_loss=94.0,
            target_price=127.0,
            target_label="2R",
            risk_reward=2.0,
        )
        bars = [Bar("TEST", signal_date + timedelta(days=i), 110.0, 120.0, 90.0, 115.0, 115.0, 1_000_000) for i in range(70)]
        bars[63] = signal_bar
        bars[64] = Bar("TEST", signal_date + timedelta(days=1), 106.0, 130.0, 93.0, 120.0, 120.0, 1_000_000)
        data = BacktestData(
            bars_by_symbol={"TEST": bars},
            stock_meta={},
            benchmark_symbol=BENCHMARK_SYMBOL,
            benchmark_bars=[],
            benchmark_by_date={},
            symbols=("TEST",),
        )
        config = BacktestConfig(db_path=self.db_path, symbols=("TEST",), output_path=Path(self.tmpdir.name) / "report.html")
        result = simulate_rr_exits([setup], data, config)[0]
        self.assertEqual(result.exit_reason, "target_first_ambiguous_green")
        self.assertEqual(result.exit_price, setup.target_price)

    def test_simulate_rr_exits_ambiguous_red_stops_first(self) -> None:
        signal_date = date(2024, 3, 1)
        signal_bar = Bar("TEST", signal_date, 100.0, 106.0, 94.0, 105.0, 105.0, 1_000_000)
        prev_bar = Bar("TEST", signal_date - timedelta(days=1), 101.0, 103.0, 95.0, 99.0, 99.0, 1_000_000)
        setup = TradeSetup(
            symbol="TEST",
            signal_date=signal_date,
            signal_index=63,
            signal_bar=signal_bar,
            prev_bar=prev_bar,
            stock_return_63=0.12,
            benchmark_return_63=0.03,
            d1_red=True,
            d_green=True,
            low_sweep=True,
            entry_price=105.0,
            stop_loss=94.0,
            target_price=127.0,
            target_label="2R",
            risk_reward=2.0,
        )
        bars = [Bar("TEST", signal_date + timedelta(days=i), 110.0, 120.0, 90.0, 115.0, 115.0, 1_000_000) for i in range(70)]
        bars[63] = signal_bar
        bars[64] = Bar("TEST", signal_date + timedelta(days=1), 120.0, 130.0, 93.0, 100.0, 100.0, 1_000_000)
        data = BacktestData(
            bars_by_symbol={"TEST": bars},
            stock_meta={},
            benchmark_symbol=BENCHMARK_SYMBOL,
            benchmark_bars=[],
            benchmark_by_date={},
            symbols=("TEST",),
        )
        config = BacktestConfig(db_path=self.db_path, symbols=("TEST",), output_path=Path(self.tmpdir.name) / "report.html")
        result = simulate_rr_exits([setup], data, config)[0]
        self.assertEqual(result.exit_reason, "stop_first_ambiguous_red")
        self.assertEqual(result.exit_price, setup.stop_loss)

    def test_simulate_exits_blocks_only_until_actual_exit_when_overlap_disabled(self) -> None:
        start = date(2024, 1, 1)
        bars = [Bar("TEST", start + timedelta(days=i), 100.0, 101.0, 99.0, 100.0, 100.0, 1_000_000) for i in range(80)]

        signal_one = Bar("TEST", start + timedelta(days=63), 100.0, 102.0, 99.0, 101.0, 101.0, 1_000_000)
        exit_one = Bar("TEST", start + timedelta(days=64), 101.0, 103.0, 100.5, 102.5, 102.5, 1_000_000)
        signal_two = Bar("TEST", start + timedelta(days=66), 109.0, 111.0, 108.5, 110.0, 110.0, 1_000_000)
        exit_two = Bar("TEST", start + timedelta(days=67), 110.0, 112.0, 109.0, 111.5, 111.5, 1_000_000)

        bars[63] = signal_one
        bars[64] = exit_one
        bars[66] = signal_two
        bars[67] = exit_two

        setup_one = TradeSetup(
            symbol="TEST",
            signal_date=signal_one.date,
            signal_index=63,
            signal_bar=signal_one,
            prev_bar=bars[62],
            stock_return_63=0.12,
            benchmark_return_63=0.03,
            d1_red=True,
            d_green=True,
            low_sweep=True,
            entry_price=101.0,
            stop_loss=100.0,
            target_price=102.0,
            target_label="1R",
            risk_reward=1.0,
        )
        setup_two = TradeSetup(
            symbol="TEST",
            signal_date=signal_two.date,
            signal_index=66,
            signal_bar=signal_two,
            prev_bar=bars[65],
            stock_return_63=0.12,
            benchmark_return_63=0.03,
            d1_red=True,
            d_green=True,
            low_sweep=True,
            entry_price=110.0,
            stop_loss=109.0,
            target_price=111.0,
            target_label="1R",
            risk_reward=1.0,
        )
        data = BacktestData(
            bars_by_symbol={"TEST": bars},
            stock_meta={},
            benchmark_symbol=BENCHMARK_SYMBOL,
            benchmark_bars=[],
            benchmark_by_date={},
            symbols=("TEST",),
        )
        config = BacktestConfig(db_path=self.db_path, symbols=("TEST",), output_path=Path(self.tmpdir.name) / "report.html")

        results = simulate_exits([setup_one, setup_two], data, config)

        self.assertEqual([result.signal_date for result in results], [signal_one.date, signal_two.date])
        self.assertEqual(results[0].exit_date, exit_one.date)
        self.assertEqual(results[1].exit_date, exit_two.date)

    def test_build_visual_review_payload_includes_rationale_and_trade_window(self) -> None:
        data = load_data(self.db_path, ["TEST"])
        config = BacktestConfig(
            db_path=self.db_path,
            symbols=("TEST",),
            output_path=Path(self.tmpdir.name) / "report.html",
        )
        matches = find_pattern_matches(data, config)
        setups = apply_config_to_matches(matches, config)
        results = simulate_rr_exits(setups, data, config)

        payload = build_visual_review_payload(results, matches, data, config, lookback_bars=3, lookahead_bars=2)

        self.assertEqual(payload["count"], 1)
        review = payload["trades"][0]
        self.assertEqual(review["symbol"], "TEST")
        self.assertIn("rationale", review)
        self.assertEqual(review["rationale"][0]["label"], "Green signal candle")
        self.assertTrue(review["rationale"][0]["passed"])
        self.assertEqual(review["markers"]["signal_index"], 3)
        self.assertEqual(review["markers"]["prev_index"], 2)
        self.assertGreaterEqual(review["markers"]["exit_index"], review["markers"]["signal_index"])
        self.assertGreater(len(review["bars"]), 4)
        self.assertEqual(review["signal_bar"]["close"], review["levels"]["signal_close"])
        self.assertTrue(review["entry_valid_on_signal"])
        self.assertIsNotNone(review["stop_reference_bar"])
        self.assertIsNotNone(review["exit_bar"])
        self.assertIn("Entry from stop anchor", [item["label"] for item in review["rationale"]])
        self.assertIn("Exit bar evidence", [item["label"] for item in review["rationale"]])
        self.assertEqual(review["levels"]["entry"], review["entry_price"])
        self.assertEqual(review["levels"]["stop"], review["stop_loss"])
        self.assertEqual(review["levels"]["target"], review["target_price"])

    def test_filter_backtest_data_limits_symbol_and_dates(self) -> None:
        data = load_data(self.db_path, ["TEST"])
        filtered = filter_backtest_data(
            data,
            symbols=("TEST",),
            start_date=date(2024, 2, 20),
            end_date=date(2024, 3, 5),
        )

        self.assertEqual(filtered.symbols, ("TEST",))
        self.assertEqual(filtered.bars_by_symbol["TEST"][0].date, date(2024, 2, 20))
        self.assertEqual(filtered.bars_by_symbol["TEST"][-1].date, date(2024, 3, 5))
        self.assertEqual(filtered.benchmark_bars[0].date, date(2024, 2, 20))
        self.assertEqual(filtered.benchmark_bars[-1].date, date(2024, 3, 5))

    def test_build_analysis_payload_returns_frontend_ready_sections(self) -> None:
        data = load_data(self.db_path, ["TEST"])
        filtered = filter_backtest_data(
            data,
            symbols=("TEST",),
            start_date=date(2024, 1, 1),
            end_date=date(2024, 3, 31),
        )
        config = BacktestConfig(
            db_path=self.db_path,
            symbols=("TEST",),
            output_path=Path(self.tmpdir.name) / "report.html",
        )

        payload = build_analysis_payload(filtered, config)

        self.assertEqual(payload["symbol_count"], 1)
        self.assertEqual(payload["symbols"], ["TEST"])
        self.assertIn("summary", payload)
        self.assertIn("visual_review", payload)
        self.assertEqual(payload["visual_review"]["count"], 1)
        self.assertEqual(payload["results"][0]["symbol"], "TEST")
        self.assertIn("offset mapped entry from stop", payload["visual_review"]["trades"][0]["reason_summary"])


if __name__ == "__main__":
    unittest.main()
