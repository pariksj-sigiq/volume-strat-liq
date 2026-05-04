from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from src.analytics.tod_baseline import (
    VolumeObservation,
    compute_tod_baseline,
    iter_intraday_volume_observations_by_symbol,
    load_intraday_volume_observations,
    write_tod_baseline_parquet,
)
from src.terminal import LiveTerminalState
from src.terminal_runtime import evaluate_baseline_status


IST = ZoneInfo("Asia/Kolkata")


def test_compute_tod_baseline_uses_session_median_and_excludes_zero_halts() -> None:
    observations: list[VolumeObservation] = []
    start = datetime(2026, 4, 1, 9, 15, tzinfo=IST)
    for session in range(30):
        day = start.date() + timedelta(days=session)
        volume = 5000.0 if session == 17 else 100.0
        observations.append(VolumeObservation("TEST", day.isoformat(), 0, 100.0))
        observations.append(VolumeObservation("TEST", day.isoformat(), 1, volume))
        observations.append(VolumeObservation("TEST", day.isoformat(), 2, 100.0))
    observations.append(VolumeObservation("TEST", "2026-05-01", 1, 0.0))

    rows = compute_tod_baseline(
        "TEST",
        observations,
        window_sessions=30,
        min_sessions_required=10,
        smoothing_minutes=3,
        computed_at=datetime(2026, 5, 4, 18, 0, tzinfo=IST),
    )

    minute_one = rows[1]
    assert minute_one.minute_of_day == 1
    assert minute_one.n_sessions == 29
    assert minute_one.baseline_volume == 100.0
    assert rows[0].baseline_volume == 100.0


def test_load_observations_from_sqlite_and_write_expected_parquet(tmp_path: Path) -> None:
    db_path = tmp_path / "nse.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE ohlcv_intraday (
                symbol TEXT,
                timestamp TEXT,
                date TEXT,
                timeframe_sec INTEGER,
                volume REAL,
                data_mode TEXT
            )
            """
        )
        for session in range(12):
            day = datetime(2026, 4, 1 + session, 9, 15, tzinfo=IST)
            conn.execute(
                "INSERT INTO ohlcv_intraday VALUES (?, ?, ?, ?, ?, ?)",
                ("TEST", day.isoformat(), day.date().isoformat(), 60, 100 + session, "equity_signal_proxy_1m"),
            )
        conn.commit()
    finally:
        conn.close()

    observations = load_intraday_volume_observations(db_path, "TEST", sessions=12)
    rows = compute_tod_baseline(
        "TEST",
        observations,
        window_sessions=12,
        min_sessions_required=10,
        smoothing_minutes=3,
        computed_at=datetime(2026, 5, 4, 18, 0, tzinfo=IST),
    )
    output = write_tod_baseline_parquet("TEST", rows, tmp_path / "tod")

    assert output.name == "TEST.parquet"
    assert output.is_file()


def test_bulk_observation_iterator_streams_recent_sessions_per_symbol(tmp_path: Path) -> None:
    db_path = tmp_path / "nse.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE ohlcv_intraday (
                symbol TEXT,
                timestamp TEXT,
                date TEXT,
                timeframe_sec INTEGER,
                volume REAL,
                data_mode TEXT
            )
            """
        )
        for symbol in ["AAA", "BBB"]:
            for session in range(4):
                day = datetime(2026, 4, 1 + session, 9, 15, tzinfo=IST)
                conn.execute(
                    "INSERT INTO ohlcv_intraday VALUES (?, ?, ?, ?, ?, ?)",
                    (symbol, day.isoformat(), day.date().isoformat(), 60, 100 + session, "equity_signal_proxy_1m"),
                )
        conn.commit()
    finally:
        conn.close()

    groups = list(iter_intraday_volume_observations_by_symbol(db_path, ["AAA", "BBB"], sessions=2))

    assert [symbol for symbol, _ in groups] == ["AAA", "BBB"]
    assert [len(observations) for _, observations in groups] == [2, 2]
    assert groups[0][1][0].session_date == "2026-04-03"


def test_terminal_snapshot_makes_warmup_only_state_explicit() -> None:
    state = LiveTerminalState()
    state.set_signal_status(
        enabled=False,
        mode="warmup_only",
        baseline_count=0,
        baseline_required=10,
        reason="TOD baseline missing",
    )

    payload = state.snapshot(now=datetime(2026, 5, 4, 9, 30, tzinfo=IST))

    assert payload["signals_enabled"] is False
    assert payload["signal_mode"] == "warmup_only"
    assert payload["baseline_count"] == 0
    assert payload["signal_status_reason"] == "TOD baseline missing"
    assert payload["bar_interval"] == "1min"
    assert payload["min_bars_for_signal"] == 25


def test_evaluate_baseline_status_detects_missing_and_stale_files(tmp_path: Path) -> None:
    missing = evaluate_baseline_status({}, required_count=2, stale_count=0)
    assert missing.enabled is False
    assert missing.mode == "warmup_only"
    assert "missing" in missing.reason

    stale = evaluate_baseline_status({"AAA": [1.0] * 375, "BBB": [1.0] * 375}, required_count=2, stale_count=1)
    assert stale.enabled is False
    assert "stale" in stale.reason

    ready = evaluate_baseline_status({"AAA": [1.0] * 375, "BBB": [1.0] * 375}, required_count=2, stale_count=0)
    assert ready.enabled is True
    assert ready.mode == "alerts_enabled"
