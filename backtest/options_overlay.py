from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable


NSE_TUESDAY_EXPIRY_START = date(2025, 9, 1)
OPTION_DATA_MODE = "options_1m"
PREFERRED_CALENDAR_SYMBOLS = ("RELIANCE", "HDFCBANK", "ICICIBANK", "INFOSYS", "SBIN", "ETERNAL")


@dataclass(frozen=True, slots=True)
class OptionExpiryInfo:
    expiry_date: date
    is_expiry_day: bool
    calendar_days_to_expiry: int
    trading_sessions_to_expiry: int | None


def nearest_monthly_option_expiry(
    signal_date: date | datetime | str,
    market_dates: Iterable[date | str] | None = None,
) -> OptionExpiryInfo:
    signal = _as_date(signal_date)
    trading_dates = _market_date_set(market_dates)
    for month_offset in range(0, 4):
        candidate_month = _add_months(signal, month_offset)
        expiry = _nominal_monthly_expiry(candidate_month.year, candidate_month.month)
        expiry = _adjust_to_previous_market_date(expiry, trading_dates)
        if expiry >= signal:
            return OptionExpiryInfo(
                expiry_date=expiry,
                is_expiry_day=expiry == signal,
                calendar_days_to_expiry=(expiry - signal).days,
                trading_sessions_to_expiry=_trading_sessions_to_expiry(signal, expiry, trading_dates),
            )
    raise ValueError(f"Unable to resolve option expiry for {signal.isoformat()}")


def annotate_option_expiry(row: dict[str, Any], market_dates: Iterable[date | str] | None = None) -> dict[str, Any]:
    signal_date = _as_date(row.get("signal_timestamp"))
    expiry = nearest_monthly_option_expiry(signal_date, market_dates)
    enriched = dict(row)
    enriched["option_expiry_date"] = expiry.expiry_date.isoformat()
    enriched["is_option_expiry_day"] = expiry.is_expiry_day
    enriched["option_dte_calendar"] = expiry.calendar_days_to_expiry
    enriched["option_dte_trading"] = expiry.trading_sessions_to_expiry
    return enriched


@lru_cache(maxsize=8)
def load_market_dates(db_path: str | Path, *, data_mode: str = "equity_signal_proxy_1m") -> tuple[date, ...]:
    db_path = Path(db_path)
    if not db_path.is_file():
        return ()
    conn = sqlite3.connect(db_path)
    try:
        for symbol in PREFERRED_CALENDAR_SYMBOLS:
            try:
                rows = conn.execute(
                    """
                    SELECT DISTINCT date
                    FROM ohlcv_intraday INDEXED BY idx_ohlcv_intraday_mode_symbol_time
                    WHERE data_mode = ?
                      AND timeframe_sec = 60
                      AND symbol = ?
                    ORDER BY date
                    """,
                    (data_mode, symbol),
                ).fetchall()
            except sqlite3.Error:
                rows = conn.execute(
                    """
                    SELECT DISTINCT date
                    FROM ohlcv_intraday
                    WHERE data_mode = ?
                      AND timeframe_sec = 60
                      AND symbol = ?
                    ORDER BY date
                    """,
                    (data_mode, symbol),
                ).fetchall()
            if rows:
                return tuple(_as_date(row[0]) for row in rows)
        rows = conn.execute(
            """
            SELECT DISTINCT date
            FROM ohlcv_intraday
            WHERE data_mode = ?
            ORDER BY date
            """,
            (data_mode,),
        ).fetchall()
    except sqlite3.Error:
        return ()
    finally:
        conn.close()
    return tuple(_as_date(row[0]) for row in rows)


def build_option_probe_payload(db_path: str | Path, query: dict[str, list[str]]) -> dict[str, Any]:
    symbol = _query_value(query, "symbol", "").upper()
    if not symbol:
        raise ValueError("symbol is required")
    signal_timestamp = _query_value(query, "signal_timestamp")
    entry_timestamp = _query_value(query, "entry_timestamp")
    exit_timestamp = _query_value(query, "exit_timestamp")
    underlying_entry_price = _query_float(query, "underlying_entry_price")
    if not signal_timestamp or not entry_timestamp:
        raise ValueError("signal_timestamp and entry_timestamp are required")
    if underlying_entry_price <= 0:
        raise ValueError("underlying_entry_price must be positive")

    market_dates = load_market_dates(db_path)
    expiry = nearest_monthly_option_expiry(signal_timestamp, market_dates)
    contract_rows = _load_option_contracts(db_path, symbol, expiry.expiry_date)
    if not contract_rows:
        return {
            "symbol": symbol,
            "status": "missing_contracts",
            "option_expiry_date": expiry.expiry_date.isoformat(),
            "is_option_expiry_day": expiry.is_expiry_day,
            "option_dte_trading": expiry.trading_sessions_to_expiry,
            "legs": [],
        }

    legs = []
    for option_type in ("CE", "PE"):
        contract = _nearest_atm_contract(contract_rows, option_type, underlying_entry_price)
        if contract is None:
            legs.append({"option_type": option_type, "status": "missing_contract"})
            continue
        bars = _load_option_bars(db_path, contract["instrument_key"], entry_timestamp, exit_timestamp)
        legs.append(_option_leg_payload(option_type, contract, bars))

    return {
        "symbol": symbol,
        "status": "ok" if any(leg.get("status") == "ok" for leg in legs) else "missing_candles",
        "underlying_entry_price": underlying_entry_price,
        "option_expiry_date": expiry.expiry_date.isoformat(),
        "is_option_expiry_day": expiry.is_expiry_day,
        "option_dte_trading": expiry.trading_sessions_to_expiry,
        "legs": legs,
    }


def _nominal_monthly_expiry(year: int, month: int) -> date:
    expiry_weekday = 1 if date(year, month, 1) >= NSE_TUESDAY_EXPIRY_START else 3
    return _last_weekday(year, month, expiry_weekday)


def _last_weekday(year: int, month: int, weekday: int) -> date:
    if month == 12:
        cursor = date(year, 12, 31)
    else:
        cursor = date(year, month + 1, 1)
        cursor = date.fromordinal(cursor.toordinal() - 1)
    while cursor.weekday() != weekday:
        cursor = date.fromordinal(cursor.toordinal() - 1)
    return cursor


def _adjust_to_previous_market_date(expiry: date, market_dates: set[date] | None) -> date:
    if not market_dates:
        return expiry
    if expiry < min(market_dates) or expiry > max(market_dates):
        return expiry
    cursor = expiry
    month = expiry.month
    while cursor.month == month and cursor not in market_dates:
        cursor = date.fromordinal(cursor.toordinal() - 1)
    return cursor


def _trading_sessions_to_expiry(signal: date, expiry: date, market_dates: set[date] | None) -> int | None:
    if not market_dates:
        return None
    if expiry < signal:
        return None
    if expiry == signal:
        return 0
    if expiry > max(market_dates):
        return None
    return sum(1 for value in market_dates if signal < value <= expiry)


def _add_months(value: date, months: int) -> date:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    day = min(value.day, 28)
    return date(year, month, day)


def _market_date_set(values: Iterable[date | str] | None) -> set[date] | None:
    if values is None:
        return None
    if isinstance(values, set) and all(isinstance(value, date) for value in values):
        return values or None
    parsed = {_as_date(value) for value in values}
    return parsed or None


def _as_date(value: date | datetime | str | Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value)
    if not text:
        raise ValueError("date value is required")
    return date.fromisoformat(text[:10])


def _query_value(query: dict[str, list[str]], key: str, default: str | None = None) -> str:
    values = query.get(key)
    if not values:
        return default or ""
    return values[0] or (default or "")


def _query_float(query: dict[str, list[str]], key: str, default: float = 0.0) -> float:
    try:
        return float(_query_value(query, key, str(default)))
    except ValueError:
        return default


def _load_option_contracts(db_path: str | Path, symbol: str, expiry_date: date) -> list[dict[str, Any]]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT symbol, expiry_date, option_type, strike_price, instrument_key, trading_symbol,
                   lot_size, tick_size, source
            FROM option_contracts
            WHERE symbol = ?
              AND expiry_date = ?
            """,
            (symbol, expiry_date.isoformat()),
        ).fetchall()
    except sqlite3.Error:
        return []
    finally:
        conn.close()
    return [dict(row) for row in rows]


def _nearest_atm_contract(
    contracts: list[dict[str, Any]],
    option_type: str,
    underlying_entry_price: float,
) -> dict[str, Any] | None:
    candidates = [row for row in contracts if row.get("option_type") == option_type]
    if not candidates:
        return None
    return min(
        candidates,
        key=lambda row: (
            abs(float(row.get("strike_price") or 0.0) - underlying_entry_price),
            float(row.get("strike_price") or 0.0),
        ),
    )


def _load_option_bars(
    db_path: str | Path,
    instrument_key: str,
    entry_timestamp: str,
    exit_timestamp: str | None,
) -> list[dict[str, Any]]:
    clauses = ["instrument_key = ?", "data_mode = ?"]
    values: list[Any] = [instrument_key, OPTION_DATA_MODE]
    clauses.append("timestamp >= ?")
    values.append(entry_timestamp)
    if exit_timestamp:
        clauses.append("timestamp <= ?")
        values.append(exit_timestamp)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            f"""
            SELECT timestamp, open, high, low, close, volume, open_interest
            FROM ohlcv_intraday
            WHERE {' AND '.join(clauses)}
            ORDER BY timestamp ASC
            """,
            values,
        ).fetchall()
    except sqlite3.Error:
        return []
    finally:
        conn.close()
    return [dict(row) for row in rows]


def _option_leg_payload(option_type: str, contract: dict[str, Any], bars: list[dict[str, Any]]) -> dict[str, Any]:
    if not bars:
        return {
            "option_type": option_type,
            "status": "missing_candles",
            "contract": contract,
        }
    entry_bar = bars[0]
    exit_bar = bars[-1]
    entry_price = float(entry_bar["open"])
    exit_price = float(exit_bar["close"])
    max_price = max(float(row["high"]) for row in bars)
    min_price = min(float(row["low"]) for row in bars)
    return {
        "option_type": option_type,
        "status": "ok",
        "contract": contract,
        "entry_timestamp": entry_bar["timestamp"],
        "exit_timestamp": exit_bar["timestamp"],
        "entry_price": entry_price,
        "exit_price": exit_price,
        "max_price": max_price,
        "min_price": min_price,
        "exit_return_pct": _pct(exit_price, entry_price),
        "max_return_pct": _pct(max_price, entry_price),
        "max_drawdown_pct": _pct(min_price, entry_price),
        "bars": len(bars),
    }


def _pct(value: float, anchor: float) -> float:
    if anchor <= 0:
        return 0.0
    return (value - anchor) / anchor * 100.0


__all__ = [
    "OPTION_DATA_MODE",
    "OptionExpiryInfo",
    "annotate_option_expiry",
    "build_option_probe_payload",
    "load_market_dates",
    "nearest_monthly_option_expiry",
]
