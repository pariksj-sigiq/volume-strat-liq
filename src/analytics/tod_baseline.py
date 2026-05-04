"""Time-of-day volume baseline calculations."""

from __future__ import annotations

import sqlite3
import statistics
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from collections.abc import Iterator
from zoneinfo import ZoneInfo

from src.analytics.rolling_state import SESSION_MINUTES, minute_of_day_index


IST = ZoneInfo("Asia/Kolkata")


@dataclass(slots=True, frozen=True)
class VolumeObservation:
    """One historical intraday volume observation."""

    symbol: str
    session_date: str
    minute_of_day: int
    volume: float


@dataclass(slots=True, frozen=True)
class TodBaselineRow:
    """One persisted time-of-day baseline row."""

    minute_of_day: int
    baseline_volume: float
    n_sessions: int
    computed_at: datetime


def compute_tod_baseline(
    symbol: str,
    observations: list[VolumeObservation],
    *,
    window_sessions: int,
    min_sessions_required: int,
    smoothing_minutes: int,
    computed_at: datetime,
) -> list[TodBaselineRow]:
    """Compute median TOD baseline rows for one symbol.

    The median is taken over sessions. For each session/minute we first average
    the target minute and its one-sided neighbors. If the target minute has zero
    volume, that session is excluded for that minute.
    """

    if window_sessions <= 0:
        raise ValueError("window_sessions must be positive")
    if min_sessions_required <= 0:
        raise ValueError("min_sessions_required must be positive")
    if smoothing_minutes not in {1, 3}:
        raise ValueError("smoothing_minutes currently supports 1 or 3")

    by_session: dict[str, dict[int, float]] = {}
    for observation in observations:
        if observation.symbol.upper() != symbol.upper():
            continue
        by_session.setdefault(observation.session_date, {})[observation.minute_of_day] = observation.volume
    selected_sessions = sorted(by_session)[-window_sessions:]
    half_window = smoothing_minutes // 2
    rows: list[TodBaselineRow] = []
    for minute in range(SESSION_MINUTES):
        session_values: list[float] = []
        for session in selected_sessions:
            volumes = by_session[session]
            target = volumes.get(minute)
            if target is None or target <= 0:
                continue
            neighbors = [
                volumes.get(candidate)
                for candidate in range(minute - half_window, minute + half_window + 1)
                if 0 <= candidate < SESSION_MINUTES
            ]
            positive_neighbors = [float(value) for value in neighbors if value is not None and value > 0]
            if positive_neighbors:
                session_values.append(sum(positive_neighbors) / len(positive_neighbors))
        if len(session_values) >= min_sessions_required:
            baseline = float(statistics.median(session_values))
            n_sessions = len(session_values)
        else:
            baseline = 0.0
            n_sessions = len(session_values)
        rows.append(
            TodBaselineRow(
                minute_of_day=minute,
                baseline_volume=baseline,
                n_sessions=n_sessions,
                computed_at=computed_at,
            )
        )
    return rows


def load_intraday_volume_observations(
    db_path: Path,
    symbol: str,
    *,
    sessions: int,
    data_mode: str = "equity_signal_proxy_1m",
    timeframe_sec: int = 60,
) -> list[VolumeObservation]:
    """Load the latest historical 1-minute volumes for one symbol from SQLite."""

    if not db_path.is_file():
        raise FileNotFoundError(f"SQLite database not found: {db_path}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        session_rows = conn.execute(
            """
            SELECT DISTINCT date
            FROM ohlcv_intraday
            WHERE symbol = ?
              AND data_mode = ?
              AND timeframe_sec = ?
            ORDER BY date DESC
            LIMIT ?
            """,
            (symbol, data_mode, timeframe_sec, sessions),
        ).fetchall()
        selected_dates = sorted(row["date"] for row in session_rows)
        if not selected_dates:
            return []
        placeholders = ",".join("?" for _ in selected_dates)
        rows = conn.execute(
            f"""
            SELECT symbol, timestamp, date, volume
            FROM ohlcv_intraday
            WHERE symbol = ?
              AND data_mode = ?
              AND timeframe_sec = ?
              AND date IN ({placeholders})
            ORDER BY date, timestamp
            """,
            (symbol, data_mode, timeframe_sec, *selected_dates),
        ).fetchall()
    finally:
        conn.close()

    observations: list[VolumeObservation] = []
    for row in rows:
        timestamp = datetime.fromisoformat(str(row["timestamp"]).replace("Z", "+00:00"))
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=IST)
        local = timestamp.astimezone(IST)
        minute = minute_of_day_index(local.time())
        if 0 <= minute < SESSION_MINUTES:
            observations.append(
                VolumeObservation(
                    symbol=str(row["symbol"]).upper(),
                    session_date=str(row["date"]),
                    minute_of_day=minute,
                    volume=float(row["volume"] or 0.0),
                )
            )
    return observations


def list_symbols_with_intraday_data(
    db_path: Path,
    *,
    data_mode: str = "equity_signal_proxy_1m",
    timeframe_sec: int = 60,
) -> list[str]:
    """Return symbols with cached intraday bars."""

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT symbol
            FROM ohlcv_intraday
            WHERE data_mode = ?
              AND timeframe_sec = ?
            ORDER BY symbol
            """,
            (data_mode, timeframe_sec),
        ).fetchall()
    finally:
        conn.close()
    return [str(row[0]).upper() for row in rows]


def iter_intraday_volume_observations_by_symbol(
    db_path: Path,
    symbols: list[str],
    *,
    sessions: int,
    data_mode: str = "equity_signal_proxy_1m",
    timeframe_sec: int = 60,
) -> Iterator[tuple[str, list[VolumeObservation]]]:
    """Stream recent observations grouped by symbol in one SQLite pass."""

    if not symbols:
        return
    symbol_placeholders = ",".join("?" for _ in symbols)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        date_rows = conn.execute(
            """
            SELECT DISTINCT date
            FROM ohlcv_intraday
            WHERE data_mode = ?
              AND timeframe_sec = ?
            ORDER BY date DESC
            LIMIT ?
            """,
            (data_mode, timeframe_sec, sessions),
        ).fetchall()
        selected_dates = sorted(str(row["date"]) for row in date_rows)
        if not selected_dates:
            return
        date_placeholders = ",".join("?" for _ in selected_dates)
        query = f"""
        SELECT b.symbol, b.timestamp, b.date, b.volume
        FROM ohlcv_intraday b
        WHERE b.data_mode = ?
          AND b.timeframe_sec = ?
          AND b.symbol IN ({symbol_placeholders})
          AND b.date IN ({date_placeholders})
        ORDER BY b.symbol, b.date, b.timestamp
        """
        cursor = conn.execute(
            query,
            (
                data_mode,
                timeframe_sec,
                *symbols,
                *selected_dates,
            ),
        )
        current_symbol: str | None = None
        current: list[VolumeObservation] = []
        for row in cursor:
            symbol = str(row["symbol"]).upper()
            if current_symbol is not None and symbol != current_symbol:
                yield current_symbol, current
                current = []
            current_symbol = symbol
            observation = _observation_from_row(row)
            if observation is not None:
                current.append(observation)
        if current_symbol is not None:
            yield current_symbol, current
    finally:
        conn.close()


def write_tod_baseline_parquet(symbol: str, rows: list[TodBaselineRow], output_dir: Path) -> Path:
    """Write one symbol baseline parquet in the runtime schema."""

    import polars as pl

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{symbol.upper()}.parquet"
    frame = pl.DataFrame(
        {
            "minute_of_day": [row.minute_of_day for row in rows],
            "baseline_volume": [row.baseline_volume for row in rows],
            "n_sessions": [row.n_sessions for row in rows],
            "computed_at": [row.computed_at for row in rows],
        },
        schema={
            "minute_of_day": pl.Int16,
            "baseline_volume": pl.Float64,
            "n_sessions": pl.Int8,
            "computed_at": pl.Datetime(time_zone="Asia/Kolkata"),
        },
    )
    frame.write_parquet(output_path)
    return output_path


def _observation_from_row(row: sqlite3.Row) -> VolumeObservation | None:
    timestamp = datetime.fromisoformat(str(row["timestamp"]).replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=IST)
    local = timestamp.astimezone(IST)
    minute = minute_of_day_index(local.time())
    if not 0 <= minute < SESSION_MINUTES:
        return None
    return VolumeObservation(
        symbol=str(row["symbol"]).upper(),
        session_date=str(row["date"]),
        minute_of_day=minute,
        volume=float(row["volume"] or 0.0),
    )
