from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime
from zoneinfo import ZoneInfo

from src.ingest.upstox_rest import UpstoxRestClient, parse_historical_candles
from src.terminal import TerminalInstrument
from src.signals.base import Bar
from src.terminal_runtime import load_cached_previous_session_warmup_bars, load_previous_session_warmup_bars, persist_warmup_bars


IST = ZoneInfo("Asia/Kolkata")


def _create_intraday_table(db_path) -> None:
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
              open_interest INTEGER NOT NULL,
              instrument_key TEXT NOT NULL,
              trading_symbol TEXT NOT NULL,
              market_segment TEXT NOT NULL,
              instrument_type TEXT NOT NULL,
              contract_expiry TEXT,
              lot_size INTEGER NOT NULL,
              source TEXT NOT NULL,
              data_mode TEXT NOT NULL,
              PRIMARY KEY (instrument_key, timestamp, timeframe_sec, data_mode)
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def test_parse_historical_candles_returns_sorted_one_minute_bars() -> None:
    payload = {
        "status": "success",
        "data": {
            "candles": [
                ["2026-05-01T15:29:00+05:30", 100, 103, 99, 102, 2000, 10],
                ["2026-05-01T15:28:00+05:30", 98, 101, 97, 100, 1500, 9],
            ]
        },
    }

    bars = parse_historical_candles(
        json.dumps(payload).encode("utf-8"),
        symbol="TEST",
        instrument_key="NSE_EQ|TEST",
        source="upstox_rest_warmup",
    )

    assert [bar.ts.minute for bar in bars] == [28, 29]
    assert bars[0].symbol == "TEST"
    assert bars[0].instrument_key == "NSE_EQ|TEST"
    assert bars[0].volume == 1500
    assert bars[0].source == "upstox_rest_warmup"
    assert bars[0].ts.tzinfo is not None


def test_upstox_rest_client_builds_historical_candle_url_without_leaking_token() -> None:
    requested: dict[str, object] = {}

    def fake_get(url: str, headers: dict[str, str], timeout: float) -> bytes:
        requested["url"] = url
        requested["headers"] = headers
        requested["timeout"] = timeout
        return b'{"status":"success","data":{"candles":[]}}'

    client = UpstoxRestClient(access_token="secret-token", get_bytes=fake_get)

    assert client.fetch_historical_candles(
        instrument_key="NSE_EQ|TEST",
        interval="1minute",
        from_date=date(2026, 5, 1),
        to_date=date(2026, 5, 1),
        symbol="TEST",
    ) == []

    assert requested["url"] == "https://api.upstox.com/v3/historical-candle/NSE_EQ%7CTEST/minutes/1/2026-05-01/2026-05-01"
    assert requested["headers"]["Authorization"] == "Bearer secret-token"
    assert requested["headers"]["User-Agent"].startswith("Mozilla/5.0")
    assert "secret-token" not in repr(client)


def test_load_previous_session_warmup_bars_uses_last_twenty_bars_per_symbol() -> None:
    instrument = TerminalInstrument(symbol="TEST", instrument_key="NSE_EQ|TEST", trading_symbol="TEST")
    fetched = [
        parse_historical_candles(
            json.dumps(
                {
                    "status": "success",
                    "data": {
                        "candles": [
                            [
                                f"2026-05-01T15:{minute:02d}:00+05:30",
                                100 + minute,
                                101 + minute,
                                99 + minute,
                                100 + minute,
                                1000 + minute,
                            ]
                            for minute in range(5, 30)
                        ]
                    },
                }
            ).encode("utf-8"),
            symbol="TEST",
            instrument_key="NSE_EQ|TEST",
            source="upstox_rest_warmup",
        )
    ][0]

    class FakeClient:
        def fetch_historical_candles(self, **kwargs: object):
            return fetched

    result = load_previous_session_warmup_bars(
        rest_client=FakeClient(),
        instruments=[instrument],
        as_of=date(2026, 5, 4),
        bars_per_symbol=20,
    )

    assert list(result) == ["TEST"]
    assert len(result["TEST"]) == 20
    assert result["TEST"][0].ts.minute == 10
    assert result["TEST"][-1].ts.minute == 29


def test_load_previous_session_warmup_bars_keeps_successes_when_one_symbol_fails() -> None:
    instruments = [
        TerminalInstrument(symbol="GOOD", instrument_key="NSE_EQ|GOOD", trading_symbol="GOOD"),
        TerminalInstrument(symbol="BAD", instrument_key="NSE_EQ|BAD", trading_symbol="BAD"),
    ]
    good_bars = [
        parse_historical_candles(
            json.dumps(
                {
                    "status": "success",
                    "data": {
                        "candles": [
                            [f"2026-04-30T15:{minute:02d}:00+05:30", 100, 101, 99, 100, 1000]
                            for minute in range(10, 30)
                        ]
                    },
                }
            ).encode("utf-8"),
            symbol="GOOD",
            instrument_key="NSE_EQ|GOOD",
            source="upstox_rest_warmup",
        )
    ][0]

    class PartiallyFailingClient:
        def fetch_historical_candles(self, **kwargs: object):
            if kwargs["symbol"] == "BAD":
                raise RuntimeError("bad instrument")
            return good_bars

    result = load_previous_session_warmup_bars(
        rest_client=PartiallyFailingClient(),
        instruments=instruments,
        as_of=date(2026, 5, 4),
        bars_per_symbol=20,
    )

    assert list(result) == ["GOOD"]
    assert len(result["GOOD"]) == 20


def test_load_cached_previous_session_warmup_bars_uses_latest_prior_cached_session(tmp_path) -> None:
    db_path = tmp_path / "nse_data.db"
    _create_intraday_table(db_path)
    conn = sqlite3.connect(db_path)
    try:
        for minute in range(5, 30):
            conn.execute(
                """
                INSERT INTO ohlcv_intraday VALUES (
                  ?, ?, ?, 60, ?, ?, ?, ?, ?, 0, ?, ?, 'NSE_EQ', 'EQ', NULL, 1, 'upstox_v3', 'equity_signal_proxy_1m'
                )
                """,
                (
                    "TEST",
                    f"2026-04-30T15:{minute:02d}:00+05:30",
                    "2026-04-30",
                    100 + minute,
                    101 + minute,
                    99 + minute,
                    100 + minute,
                    1000 + minute,
                    "NSE_EQ|TEST",
                    "TEST",
                ),
            )
        conn.commit()
    finally:
        conn.close()

    result = load_cached_previous_session_warmup_bars(
        db_path,
        instruments=[TerminalInstrument(symbol="TEST", instrument_key="NSE_EQ|TEST", trading_symbol="TEST")],
        as_of=date(2026, 5, 4),
        bars_per_symbol=20,
    )

    assert list(result) == ["TEST"]
    assert len(result["TEST"]) == 20
    assert result["TEST"][0].ts.minute == 10
    assert result["TEST"][-1].ts.minute == 29
    assert result["TEST"][-1].source == "upstox_intraday_cache_warmup"


def test_persist_warmup_bars_upserts_rest_fallback_rows(tmp_path) -> None:
    db_path = tmp_path / "nse_data.db"
    _create_intraday_table(db_path)
    bars = {
        "TEST": [
            Bar(
                symbol="TEST",
                ts=datetime(2026, 4, 30, 15, 29, tzinfo=IST),
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.5,
                volume=1234,
                instrument_key="NSE_EQ|TEST",
                trading_symbol="TEST",
                open_interest=12,
                source="upstox_rest_warmup",
            )
        ]
    }

    assert persist_warmup_bars(db_path, bars) == 1
    assert persist_warmup_bars(db_path, bars) == 1

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT symbol, timestamp, date, timeframe_sec, close, volume,
                   open_interest, instrument_key, trading_symbol, market_segment,
                   instrument_type, lot_size, source, data_mode
            FROM ohlcv_intraday
            """
        ).fetchall()
    finally:
        conn.close()

    assert len(rows) == 1
    row = rows[0]
    assert row[0] == "TEST"
    assert row[2] == "2026-04-30"
    assert row[3] == 60
    assert row[4] == 100.5
    assert row[5] == 1234
    assert row[6] == 12
    assert row[7] == "NSE_EQ|TEST"
    assert row[8] == "TEST"
    assert row[9] == "NSE_EQ"
    assert row[10] == "EQ"
    assert row[11] == 1
    assert row[12] == "upstox_rest_warmup"
    assert row[13] == "equity_signal_proxy_1m"
