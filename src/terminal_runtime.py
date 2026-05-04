"""Runtime wiring for the live terminal websocket feed."""

from __future__ import annotations

import asyncio
import os
import sqlite3
import threading
import time as time_module
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import yaml

from src.config import load_strategy_config
from src.ingest.bar_aggregator import OneMinuteBarAggregator
from src.ingest.feeds import MarketTick
from src.ingest.upstox_rest import UpstoxRestClient
from src.ingest.upstox_ws import UpstoxMarketDataClient, UpstoxWsConfig
from src.signals.base import Bar
from src.signals.breakout import BreakoutSignal
from src.signals.filters import (
    PreSignalGateChain,
    SessionWindowGate,
    TaintedBarGate,
    TimeWindow,
    TodBaselineGate,
    WarmupGate,
)
from src.terminal import LiveTerminalState, TerminalAlertEngine, TerminalInstrument


ROOT_DIR = Path(__file__).resolve().parents[1]
IST = ZoneInfo("Asia/Kolkata")


@dataclass(slots=True)
class TerminalRuntime:
    """Handle for a background live terminal thread."""

    state: LiveTerminalState
    thread: threading.Thread | None = None
    client: UpstoxMarketDataClient | None = None

    def stop(self) -> None:
        """Stop the websocket client if it has started."""

        if self.client is not None:
            self.client.stop()


@dataclass(slots=True, frozen=True)
class BaselineStatus:
    """Structural signal readiness derived from TOD baseline files."""

    enabled: bool
    mode: str
    reason: str
    baseline_count: int
    baseline_required: int
    stale_count: int = 0


def load_dotenv_file(path: Path = ROOT_DIR / ".env") -> None:
    """Load simple KEY=VALUE env files without printing secret values."""

    if not path.is_file():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def load_terminal_instruments(
    db_path: Path,
    *,
    symbols: Iterable[str] | None = None,
    limit: int | None = None,
) -> list[TerminalInstrument]:
    """Load NSE equity instruments for the realtime terminal."""

    selected = {symbol.strip().upper() for symbol in symbols or [] if symbol.strip()}
    if db_path.is_file():
        from_db = _load_terminal_instruments_from_db(db_path, selected=selected, limit=limit)
        if from_db:
            return from_db
    return _load_terminal_instruments_from_yaml(selected=selected, limit=limit)


def load_tod_baselines(
    baseline_dir: Path = ROOT_DIR / "data" / "tod_baseline",
    *,
    min_sessions_required: int = 10,
    staleness_max_age_hours: int = 30,
) -> tuple[dict[str, list[float]], int]:
    """Load persisted TOD baselines from parquet files when available."""

    if not baseline_dir.is_dir():
        return {}, 0
    try:
        import polars as pl
    except ImportError:
        return {}, 0
    baselines: dict[str, list[float]] = {}
    stale_count = 0
    max_age = timedelta(hours=staleness_max_age_hours)
    now = datetime.now(tz=IST)
    for path in baseline_dir.glob("*.parquet"):
        symbol = path.stem.upper()
        frame = pl.read_parquet(path)
        computed_values = frame.get_column("computed_at").drop_nulls() if "computed_at" in frame.columns else []
        if len(computed_values):
            computed_at = computed_values[0]
            if getattr(computed_at, "tzinfo", None) is None:
                computed_at = computed_at.replace(tzinfo=IST)
            if now - computed_at.astimezone(IST) > max_age:
                stale_count += 1
                continue
        values = [0.0] * 375
        for row in frame.iter_rows(named=True):
            minute = int(row.get("minute_of_day") or -1)
            n_sessions = int(row.get("n_sessions") or 0)
            if 0 <= minute < len(values) and n_sessions >= min_sessions_required:
                values[minute] = float(row.get("baseline_volume") or 0.0)
        if any(value > 0 for value in values):
            baselines[symbol] = values
    return baselines, stale_count


def evaluate_baseline_status(
    baselines: dict[str, list[float]],
    *,
    required_count: int,
    stale_count: int,
) -> BaselineStatus:
    """Classify whether signal alerts can run."""

    baseline_count = len(baselines)
    if stale_count:
        return BaselineStatus(
            enabled=False,
            mode="warmup_only",
            reason=f"{stale_count} TOD baseline files are stale; rebuild before trusting alerts.",
            baseline_count=baseline_count,
            baseline_required=required_count,
            stale_count=stale_count,
        )
    if baseline_count < required_count:
        return BaselineStatus(
            enabled=False,
            mode="warmup_only",
            reason=f"TOD baseline missing for {required_count - baseline_count} symbols.",
            baseline_count=baseline_count,
            baseline_required=required_count,
        )
    return BaselineStatus(
        enabled=True,
        mode="alerts_enabled",
        reason="TOD baselines loaded and fresh.",
        baseline_count=baseline_count,
        baseline_required=required_count,
    )


def load_previous_session_warmup_bars(
    *,
    rest_client: Any,
    instruments: list[TerminalInstrument],
    as_of: date,
    bars_per_symbol: int,
    max_workers: int = 8,
    requests_per_second: float = 15.0,
) -> dict[str, list[Bar]]:
    """Fetch and return prior-session warmup bars keyed by symbol."""

    if bars_per_symbol <= 0:
        raise ValueError("bars_per_symbol must be positive")
    candidate_dates = _previous_trading_day_candidates(as_of, lookback_days=7)
    result: dict[str, list[Bar]] = {}
    request_lock = threading.Lock()
    last_request_at = 0.0
    min_interval = 1.0 / requests_per_second if requests_per_second > 0 else 0.0

    def pace_request() -> None:
        nonlocal last_request_at
        if min_interval <= 0:
            return
        with request_lock:
            now = time_module.monotonic()
            wait_for = min_interval - (now - last_request_at)
            if wait_for > 0:
                time_module.sleep(wait_for)
                now = time_module.monotonic()
            last_request_at = now

    def fetch_one(instrument: TerminalInstrument) -> tuple[str, list[Bar]]:
        for session_date in candidate_dates:
            try:
                pace_request()
                bars = rest_client.fetch_historical_candles(
                    instrument_key=instrument.instrument_key,
                    interval="1minute",
                    from_date=session_date,
                    to_date=session_date,
                    symbol=instrument.symbol,
                )
            except Exception:
                return instrument.symbol, []
            regular_bars = [
                bar for bar in bars
                if bar.ts.astimezone(IST).time() >= datetime.strptime("09:15", "%H:%M").time()
                and bar.ts.astimezone(IST).time() < datetime.strptime("15:30", "%H:%M").time()
            ]
            if regular_bars:
                return instrument.symbol, regular_bars[-bars_per_symbol:]
        return instrument.symbol, []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(fetch_one, instrument) for instrument in instruments]
        for future in as_completed(futures):
            symbol, bars = future.result()
            if bars:
                result[symbol] = bars
    return result


def _previous_trading_day_candidates(as_of: date, *, lookback_days: int) -> list[date]:
    candidates: list[date] = []
    current = as_of - timedelta(days=1)
    while len(candidates) < lookback_days:
        if current.weekday() < 5:
            candidates.append(current)
        current -= timedelta(days=1)
    return candidates


def start_terminal_runtime(
    *,
    db_path: Path,
    symbols: Iterable[str] | None = None,
    mode: str | None = None,
    ws_url: str | None = None,
    state: LiveTerminalState | None = None,
) -> TerminalRuntime:
    """Start the live websocket feed in a background thread."""

    load_dotenv_file()
    terminal_state = state or LiveTerminalState()
    instruments = load_terminal_instruments(db_path, symbols=symbols)
    terminal_state.set_universe(instruments)
    if not instruments:
        terminal_state.set_connection(connected=False, error="No NSE_EQ instruments found for terminal universe")
        return TerminalRuntime(state=terminal_state)

    try:
        config = UpstoxWsConfig.from_env()
    except ValueError as exc:
        terminal_state.set_connection(connected=False, error=str(exc))
        return TerminalRuntime(state=terminal_state)
    if mode:
        config.mode = mode.strip().lower()
    if ws_url:
        config.ws_url = ws_url

    strategy = load_strategy_config(ROOT_DIR / "config" / "strategy.yaml")
    baselines, stale_count = load_tod_baselines(
        min_sessions_required=strategy.tod_baseline.min_sessions_required,
        staleness_max_age_hours=strategy.tod_baseline.staleness_max_age_hours,
    )
    baseline_status = evaluate_baseline_status(
        baselines,
        required_count=len(instruments),
        stale_count=stale_count,
    )
    full_mode_enabled = config.mode == "full"
    signals_enabled = baseline_status.enabled and full_mode_enabled
    signal_mode = baseline_status.mode if signals_enabled else "warmup_only"
    signal_reason = baseline_status.reason
    if not full_mode_enabled:
        signal_reason = "Full mode websocket is required for volume-confirmed alerts."
    terminal_state.set_signal_status(
        enabled=signals_enabled,
        mode=signal_mode,
        baseline_count=baseline_status.baseline_count,
        baseline_required=baseline_status.baseline_required,
        reason=signal_reason,
        bar_interval=strategy.bars.interval,
        min_bars_for_signal=strategy.bars.min_bars_for_signal,
    )
    gate_chain = PreSignalGateChain(
        [
            SessionWindowGate(
                market_open=strategy.session.market_open,
                market_close=strategy.session.market_close,
                skip_windows=tuple(TimeWindow(window.start, window.end) for window in strategy.session.skip_windows),
            ),
            TaintedBarGate(),
            WarmupGate(strategy.bars.min_bars_for_signal),
            TodBaselineGate(),
        ]
    )
    profile_signals = tuple(
        BreakoutSignal(
            profile_name=profile.name,
            profile_label=profile.label,
            min_bars=strategy.bars.min_bars_for_signal,
            lookback_bars=profile.lookback_bars,
            volume_mult=profile.volume_mult,
            min_range_atr=strategy.signal.rules.min_range_atr,
            tp_r_multiple=strategy.risk.tp_r_multiple,
        )
        for profile in strategy.signal.profiles
    )
    alert_engine = TerminalAlertEngine(
        signals=profile_signals,
        baseline_by_symbol=baselines,
        gate_chain=gate_chain,
        on_blocked=terminal_state.record_signal_block,
    )
    if signals_enabled:
        try:
            warmup_bars = load_previous_session_warmup_bars(
                rest_client=UpstoxRestClient.from_env(),
                instruments=instruments,
                as_of=datetime.now(tz=IST).date(),
                bars_per_symbol=20,
            )
            for instrument in instruments:
                alert_engine.prewarm(instrument, warmup_bars.get(instrument.symbol, []))
            terminal_state.set_warmup_status(
                seed_count=len(warmup_bars),
                required_count=len(instruments),
                reason=f"Prewarmed {len(warmup_bars)} / {len(instruments)} symbols from prior-session REST candles.",
            )
        except Exception as exc:
            terminal_state.set_warmup_status(
                seed_count=0,
                required_count=len(instruments),
                reason=f"Prior-session REST warmup failed: {type(exc).__name__}. Scanner will use live 1-minute bars.",
            )
            terminal_state.record_signal_block("SYSTEM", f"warmup_failed:{type(exc).__name__}", datetime.now(tz=IST))
    aggregator = OneMinuteBarAggregator()
    instrument_keys = [instrument.instrument_key for instrument in instruments]
    feed_started = False

    async def on_ticks(ticks: list[MarketTick]) -> None:
        for tick in ticks:
            terminal_state.record_tick(tick)
            for bar in aggregator.update(tick):
                instrument = terminal_state.instrument_for_key(bar.instrument_key or "")
                if instrument is None:
                    continue
                if not signals_enabled:
                    continue
                alerts = alert_engine.on_bar(instrument, bar)
                for alert in alerts:
                    terminal_state.record_engine_alert(alert)

    async def on_status(event: str, payload: dict[str, object]) -> None:
        nonlocal feed_started
        if event in {"subscribed", "tick_rate"}:
            if event == "subscribed":
                feed_started = True
            terminal_state.set_connection(connected=True, mode=config.mode)
        elif event == "reconnect_wait":
            if feed_started:
                aggregator.mark_instruments_tainted(instrument_keys, reason="ws_reconnect")
                aggregator.reset_volume_reference(instrument_keys)
            terminal_state.set_connection(connected=False, mode=config.mode, error=str(payload.get("error") or ""))
        elif event == "connecting":
            terminal_state.set_connection(connected=False, mode=config.mode, error=str(payload.get("error") or ""))

    client = UpstoxMarketDataClient(
        config,
        instrument_keys=[item.instrument_key for item in instruments],
        symbol_by_instrument={item.instrument_key: item.symbol for item in instruments},
        on_ticks=on_ticks,
        on_status=on_status,
    )

    def runner() -> None:
        try:
            asyncio.run(client.run_forever())
        except Exception as exc:
            terminal_state.set_connection(connected=False, error=str(exc))

    thread = threading.Thread(target=runner, name="upstox-terminal-ws", daemon=True)
    thread.start()
    return TerminalRuntime(state=terminal_state, thread=thread, client=client)


def _load_terminal_instruments_from_db(
    db_path: Path,
    *,
    selected: set[str],
    limit: int | None,
) -> list[TerminalInstrument]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        tables = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        if "instruments" not in tables:
            return []
        has_stocks = "stocks" in tables
        query = """
            SELECT i.symbol, i.instrument_key, i.symbol AS trading_symbol,
                   COALESCE(s.theme, s.sub_theme, 'Unknown') AS sector
            FROM instruments i
            LEFT JOIN stocks s ON s.symbol = i.symbol
            WHERE i.segment = 'NSE_EQ'
        """ if has_stocks else """
            SELECT i.symbol, i.instrument_key, i.symbol AS trading_symbol, 'Unknown' AS sector
            FROM instruments i
            WHERE i.segment = 'NSE_EQ'
        """
        rows = conn.execute(query).fetchall()
    finally:
        conn.close()

    instruments: list[TerminalInstrument] = []
    for row in rows:
        symbol = str(row["symbol"]).upper()
        if selected and symbol not in selected:
            continue
        instruments.append(
            TerminalInstrument(
                symbol=symbol,
                instrument_key=str(row["instrument_key"]),
                trading_symbol=str(row["trading_symbol"] or symbol),
                sector=str(row["sector"] or "Unknown"),
            )
        )
    instruments.sort(key=lambda item: item.symbol)
    return instruments[:limit] if limit else instruments


def _load_terminal_instruments_from_yaml(
    *,
    selected: set[str],
    limit: int | None,
) -> list[TerminalInstrument]:
    path = ROOT_DIR / "config" / "universe.yaml"
    if not path.is_file():
        return []
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    rows = raw.get("symbols") or []
    instruments: list[TerminalInstrument] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "").upper()
        instrument_key = str(row.get("instrument_key") or "")
        if not symbol or not instrument_key:
            continue
        if selected and symbol not in selected:
            continue
        instruments.append(
            TerminalInstrument(
                symbol=symbol,
                instrument_key=instrument_key,
                trading_symbol=str(row.get("trading_symbol") or symbol),
                sector=str(row.get("sector") or "Unknown"),
                avg_turnover_cr=float(row.get("avg_turnover_cr") or 0.0),
            )
        )
    instruments.sort(key=lambda item: item.symbol)
    return instruments[:limit] if limit else instruments
