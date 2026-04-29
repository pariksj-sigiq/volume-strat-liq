from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import date
from pathlib import Path
from urllib.parse import parse_qs

from backtest.options_overlay import (
    annotate_option_expiry,
    build_option_probe_payload,
    nearest_monthly_option_expiry,
)


class OptionsOverlayTest(unittest.TestCase):
    def test_uses_thursday_monthly_expiry_before_september_2025(self) -> None:
        expiry = nearest_monthly_option_expiry("2025-08-20")

        self.assertEqual(date(2025, 8, 28), expiry.expiry_date)
        self.assertFalse(expiry.is_expiry_day)
        self.assertEqual(8, expiry.calendar_days_to_expiry)

    def test_uses_tuesday_monthly_expiry_from_september_2025(self) -> None:
        expiry = nearest_monthly_option_expiry("2025-09-20")

        self.assertEqual(date(2025, 9, 30), expiry.expiry_date)
        self.assertFalse(expiry.is_expiry_day)
        self.assertEqual(10, expiry.calendar_days_to_expiry)

    def test_expiry_adjusts_to_previous_market_date_and_counts_sessions(self) -> None:
        market_dates = [
            "2025-09-26",
            "2025-09-29",
            "2025-10-01",
        ]

        expiry = nearest_monthly_option_expiry("2025-09-26", market_dates)

        self.assertEqual(date(2025, 9, 29), expiry.expiry_date)
        self.assertFalse(expiry.is_expiry_day)
        self.assertEqual(1, expiry.trading_sessions_to_expiry)

    def test_does_not_holiday_adjust_when_expiry_is_outside_calendar_coverage(self) -> None:
        expiry = nearest_monthly_option_expiry("2026-04-28", ["2026-04-27"])

        self.assertEqual(date(2026, 4, 28), expiry.expiry_date)
        self.assertTrue(expiry.is_expiry_day)
        self.assertEqual(0, expiry.trading_sessions_to_expiry)

    def test_marks_expiry_day(self) -> None:
        row = annotate_option_expiry(
            {"signal_timestamp": "2025-09-29T10:15:00+05:30"},
            market_dates=["2025-09-29", "2025-10-01"],
        )

        self.assertEqual("2025-09-29", row["option_expiry_date"])
        self.assertTrue(row["is_option_expiry_day"])
        self.assertEqual(0, row["option_dte_calendar"])
        self.assertEqual(0, row["option_dte_trading"])

    def test_option_probe_uses_cached_upstox_stock_expiry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "options.db"
            self._create_probe_db(db_path)
            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    CREATE TABLE option_expiries (
                      symbol TEXT NOT NULL,
                      underlying_key TEXT NOT NULL,
                      expiry_date TEXT NOT NULL,
                      source TEXT NOT NULL,
                      updated_at TEXT NOT NULL,
                      PRIMARY KEY (symbol, expiry_date)
                    );
                    """
                )
                conn.execute(
                    """
                    INSERT INTO option_expiries (
                      symbol, underlying_key, expiry_date, source, updated_at
                    ) VALUES (
                      'ICICIBANK', 'NSE_EQ|INE090A01021', '2026-03-30',
                      'upstox_expired_expiries_v2', '2026-04-29T00:00:00Z'
                    )
                    """
                )
                conn.executemany(
                    """
                    INSERT INTO option_contracts (
                      symbol, expiry_date, option_type, strike_price, instrument_key, trading_symbol,
                      exchange, segment, lot_size, tick_size, underlying_key, underlying_type,
                      underlying_symbol, weekly, source, is_active, updated_at
                    ) VALUES (
                      'ICICIBANK', '2026-03-30', ?, 1250, ?, ?, 'NSE', 'NSE_FO', 700, 0.05,
                      'NSE_EQ|INE090A01021', 'EQUITY', 'ICICIBANK', 0, 'test', 0,
                      '2026-04-29T00:00:00Z'
                    )
                    """,
                    [
                        ("CE", "ICICI_CE_1250", "ICICIBANK 1250 CE 30 MAR 26"),
                        ("PE", "ICICI_PE_1250", "ICICIBANK 1250 PE 30 MAR 26"),
                    ],
                )
                conn.commit()
            finally:
                conn.close()

            payload = build_option_probe_payload(
                db_path,
                parse_qs(
                    "symbol=ICICIBANK"
                    "&signal_timestamp=2026-03-24T15:07:00%2B05:30"
                    "&entry_timestamp=2026-03-24T15:08:00%2B05:30"
                    "&underlying_entry_price=1252.7"
                ),
            )

        self.assertEqual("2026-03-30", payload["option_expiry_date"])
        self.assertEqual("missing_candles", payload["status"])
        self.assertEqual({"CE", "PE"}, {leg["option_type"] for leg in payload["legs"]})

    def test_option_probe_selects_atm_call_and_put_and_calculates_returns(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "options.db"
            self._create_probe_db(db_path)

            payload = build_option_probe_payload(
                db_path,
                parse_qs(
                    "symbol=RELIANCE"
                    "&signal_timestamp=2025-09-29T10:00:00%2B05:30"
                    "&entry_timestamp=2025-09-29T10:01:00%2B05:30"
                    "&exit_timestamp=2025-09-29T10:03:00%2B05:30"
                    "&underlying_entry_price=103"
                ),
            )

        self.assertEqual("ok", payload["status"])
        self.assertEqual("2025-09-29", payload["option_expiry_date"])
        self.assertTrue(payload["is_option_expiry_day"])
        legs = {leg["option_type"]: leg for leg in payload["legs"]}
        self.assertEqual({"CE", "PE"}, set(legs))
        self.assertEqual(100.0, legs["CE"]["contract"]["strike_price"])
        self.assertEqual(100.0, legs["PE"]["contract"]["strike_price"])
        self.assertAlmostEqual(50.0, legs["CE"]["max_return_pct"])
        self.assertAlmostEqual(25.0, legs["PE"]["max_return_pct"])
        self.assertEqual(3, legs["CE"]["bars"])

    def test_option_probe_reports_missing_contracts_without_option_cache(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "empty.db"
            sqlite3.connect(db_path).close()

            payload = build_option_probe_payload(
                db_path,
                parse_qs(
                    "symbol=RELIANCE"
                    "&signal_timestamp=2025-09-29T10:00:00%2B05:30"
                    "&entry_timestamp=2025-09-29T10:01:00%2B05:30"
                    "&underlying_entry_price=103"
                ),
            )

        self.assertEqual("missing_contracts", payload["status"])
        self.assertEqual([], payload["legs"])

    def _create_probe_db(self, db_path: Path) -> None:
        conn = sqlite3.connect(db_path)
        try:
            conn.executescript(
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
                  data_mode TEXT NOT NULL,
                  PRIMARY KEY (instrument_key, timestamp, timeframe_sec, data_mode)
                );

                CREATE TABLE option_contracts (
                  symbol TEXT NOT NULL,
                  expiry_date TEXT NOT NULL,
                  option_type TEXT NOT NULL,
                  strike_price REAL NOT NULL,
                  instrument_key TEXT NOT NULL UNIQUE,
                  trading_symbol TEXT NOT NULL,
                  exchange TEXT,
                  segment TEXT,
                  lot_size INTEGER NOT NULL,
                  tick_size REAL,
                  underlying_key TEXT,
                  underlying_type TEXT,
                  underlying_symbol TEXT NOT NULL,
                  weekly INTEGER NOT NULL DEFAULT 0,
                  source TEXT NOT NULL,
                  is_active INTEGER NOT NULL DEFAULT 0,
                  updated_at TEXT NOT NULL,
                  PRIMARY KEY (symbol, expiry_date, option_type, strike_price)
                );
                """
            )
            conn.execute(
                """
                INSERT INTO ohlcv_intraday (
                  symbol, timestamp, date, timeframe_sec, open, high, low, close, volume, open_interest,
                  instrument_key, trading_symbol, market_segment, instrument_type, contract_expiry,
                  lot_size, source, data_mode
                ) VALUES (
                  'RELIANCE', '2025-09-29T10:00:00+05:30', '2025-09-29', 60,
                  103, 104, 102, 103, 1000, 0, 'NSE_EQ|RELIANCE', 'RELIANCE',
                  'NSE_EQ', 'EQ', NULL, 1, 'test', 'equity_signal_proxy_1m'
                )
                """
            )
            conn.execute(
                """
                INSERT INTO ohlcv_intraday (
                  symbol, timestamp, date, timeframe_sec, open, high, low, close, volume, open_interest,
                  instrument_key, trading_symbol, market_segment, instrument_type, contract_expiry,
                  lot_size, source, data_mode
                ) VALUES (
                  'RELIANCE', '2025-10-01T10:00:00+05:30', '2025-10-01', 60,
                  103, 104, 102, 103, 1000, 0, 'NSE_EQ|RELIANCE', 'RELIANCE',
                  'NSE_EQ', 'EQ', NULL, 1, 'test', 'equity_signal_proxy_1m'
                )
                """
            )
            conn.executemany(
                """
                INSERT INTO ohlcv_intraday (
                  symbol, timestamp, date, timeframe_sec, open, high, low, close, volume, open_interest,
                  instrument_key, trading_symbol, market_segment, instrument_type, contract_expiry,
                  lot_size, source, data_mode
                ) VALUES (?, ?, ?, 60, ?, ?, ?, ?, ?, ?, ?, ?, 'NSE_FO', ?, '2025-09-29', 500, 'test', ?)
                """,
                [
                    (
                        "RELIANCE",
                        "2025-09-29T10:01:00+05:30",
                        "2025-09-29",
                        10,
                        12,
                        9,
                        11,
                        1000,
                        100,
                        "CE100",
                        "RELIANCE 100 CE",
                        "CE",
                        "options_1m",
                    ),
                    (
                        "RELIANCE",
                        "2025-09-29T10:02:00+05:30",
                        "2025-09-29",
                        11,
                        15,
                        10,
                        14,
                        1000,
                        100,
                        "CE100",
                        "RELIANCE 100 CE",
                        "CE",
                        "options_1m",
                    ),
                    (
                        "RELIANCE",
                        "2025-09-29T10:03:00+05:30",
                        "2025-09-29",
                        14,
                        14,
                        11,
                        12,
                        1000,
                        100,
                        "CE100",
                        "RELIANCE 100 CE",
                        "CE",
                        "options_1m",
                    ),
                    (
                        "RELIANCE",
                        "2025-09-29T10:01:00+05:30",
                        "2025-09-29",
                        20,
                        21,
                        18,
                        19,
                        1000,
                        100,
                        "PE100",
                        "RELIANCE 100 PE",
                        "PE",
                        "options_1m",
                    ),
                    (
                        "RELIANCE",
                        "2025-09-29T10:02:00+05:30",
                        "2025-09-29",
                        19,
                        25,
                        17,
                        24,
                        1000,
                        100,
                        "PE100",
                        "RELIANCE 100 PE",
                        "PE",
                        "options_1m",
                    ),
                ],
            )
            conn.executemany(
                """
                INSERT INTO option_contracts (
                  symbol, expiry_date, option_type, strike_price, instrument_key, trading_symbol,
                  exchange, segment, lot_size, tick_size, underlying_key, underlying_type,
                  underlying_symbol, weekly, source, is_active, updated_at
                ) VALUES (
                  'RELIANCE', '2025-09-29', ?, ?, ?, ?, 'NSE', 'NSE_FO', 500, 0.05,
                  'NSE_EQ|RELIANCE', 'EQUITY', 'RELIANCE', 0, 'test', 0, '2026-04-29T00:00:00Z'
                )
                """,
                [
                    ("CE", 100.0, "CE100", "RELIANCE 100 CE"),
                    ("CE", 110.0, "CE110", "RELIANCE 110 CE"),
                    ("PE", 100.0, "PE100", "RELIANCE 100 PE"),
                    ("PE", 110.0, "PE110", "RELIANCE 110 PE"),
                ],
            )
            conn.commit()
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
