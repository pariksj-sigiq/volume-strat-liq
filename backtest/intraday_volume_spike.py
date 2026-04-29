from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from statistics import median
from typing import Any, Sequence
from urllib.parse import urlencode


@dataclass(frozen=True, slots=True)
class IntradayBar:
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    turnover: float = 0.0
    instrument_key: str | None = None
    trading_symbol: str | None = None
    contract_expiry: date | str | None = None
    lot_size: int = 1
    open_interest: float | None = None
    source: str = "unknown"
    data_mode: str = "unknown"
    timeframe_sec: int = 60
    market_segment: str | None = None
    instrument_type: str | None = None


@dataclass(frozen=True, slots=True)
class IntradayScalpConfig:
    base_lookback: int = 20
    spike_multiple: float = 3.0
    min_turnover: float = 0.0
    close_location_threshold: float = 0.75
    risk_reward: float = 1.5
    max_hold_bars: int = 15
    timeframe_sec: int = 60
    data_mode: str = "intraday_volume_spike"
    max_base_range_pct: float = 0.03
    replay_pre_bars: int = 20
    replay_post_bars: int = 20
    allow_overnight: bool = False


BUCKET_SAME_DAY = "same_day"
BUCKET_NEXT_MORNING_ENTRY = "next_morning_entry"
BUCKET_TWO_DAY_HOLD = "two_day_hold"
DEFAULT_BUCKETS = (
    BUCKET_SAME_DAY,
    BUCKET_NEXT_MORNING_ENTRY,
    BUCKET_TWO_DAY_HOLD,
)


@dataclass(frozen=True, slots=True)
class IntradaySetup:
    symbol: str
    signal_timestamp: datetime
    signal_index: int
    entry_timestamp: datetime
    entry_index: int
    entry_price: float
    stop_loss: float
    target_price: float
    risk_points: float
    risk_reward: float
    risk_reward_label: str
    base_high: float
    base_low: float
    rolling_median_volume: float
    spike_volume: float
    volume_multiple: float
    turnover: float
    close_location: float
    timeframe_sec: int = 60
    instrument_key: str | None = None
    trading_symbol: str | None = None
    contract_expiry: date | str | None = None
    lot_size: int = 1
    open_interest: float | None = None
    source: str = "unknown"


@dataclass(frozen=True, slots=True)
class IntradayTradeResult:
    symbol: str
    signal_timestamp: datetime
    entry_timestamp: datetime
    exit_timestamp: datetime | None
    signal_index: int
    entry_index: int
    exit_index: int | None
    entry_price: float
    stop_loss: float
    target_price: float
    exit_price: float
    exit_reason: str
    bars_held: int
    risk_points: float
    rr: float
    return_pct: float
    pnl_points: float
    pnl_rupees: float
    risk_reward: float
    risk_reward_label: str
    timeframe_sec: int = 60
    instrument_key: str | None = None
    trading_symbol: str | None = None
    contract_expiry: date | str | None = None
    lot_size: int = 1
    open_interest: float | None = None
    source: str = "unknown"


def find_volume_spike_setups(
    bars: Sequence[IntradayBar],
    config: IntradayScalpConfig | None = None,
) -> list[IntradaySetup]:
    config = config or IntradayScalpConfig()
    if config.base_lookback < 1:
        raise ValueError("base_lookback must be at least 1")
    if config.max_hold_bars < 1:
        raise ValueError("max_hold_bars must be at least 1")

    setups: list[IntradaySetup] = []
    ordered_bars = list(bars)
    for signal_index in range(config.base_lookback, len(ordered_bars) - 1):
        signal_bar = ordered_bars[signal_index]
        entry_bar = ordered_bars[signal_index + 1]
        if not config.allow_overnight and entry_bar.timestamp.date() != signal_bar.timestamp.date():
            continue
        base_bars = ordered_bars[signal_index - config.base_lookback : signal_index]
        base_volumes = [max(0.0, bar.volume) for bar in base_bars]
        median_volume = float(median(base_volumes)) if base_volumes else 0.0
        if median_volume <= 0:
            continue

        signal_volume = max(0.0, signal_bar.volume)
        volume_multiple = signal_volume / median_volume
        if volume_multiple < config.spike_multiple:
            continue

        turnover = _bar_turnover(signal_bar)
        if turnover < config.min_turnover:
            continue

        base_high = max(bar.high for bar in base_bars)
        base_low = min(bar.low for bar in base_bars)
        if signal_bar.high <= base_high:
            continue

        if not _is_quiet_base(base_bars, config):
            continue

        close_location = _close_location(signal_bar)
        if close_location < config.close_location_threshold:
            continue

        stop_loss = min(signal_bar.low, base_low)
        entry_price = entry_bar.open
        risk_points = entry_price - stop_loss
        if risk_points <= 0:
            continue

        target_price = entry_price + (risk_points * config.risk_reward)
        setups.append(
            IntradaySetup(
                symbol=signal_bar.symbol,
                signal_timestamp=signal_bar.timestamp,
                signal_index=signal_index,
                entry_timestamp=entry_bar.timestamp,
                entry_index=signal_index + 1,
                entry_price=entry_price,
                stop_loss=stop_loss,
                target_price=target_price,
                risk_points=risk_points,
                risk_reward=config.risk_reward,
                risk_reward_label=_risk_reward_label(config.risk_reward),
                base_high=base_high,
                base_low=base_low,
                rolling_median_volume=median_volume,
                spike_volume=signal_volume,
                volume_multiple=volume_multiple,
                turnover=turnover,
                close_location=close_location,
                timeframe_sec=config.timeframe_sec,
                instrument_key=signal_bar.instrument_key,
                trading_symbol=signal_bar.trading_symbol,
                contract_expiry=signal_bar.contract_expiry,
                lot_size=signal_bar.lot_size,
                open_interest=signal_bar.open_interest,
                source=signal_bar.source,
            )
        )
    return setups


def simulate_intraday_exits(
    setups: Sequence[IntradaySetup],
    bars: Sequence[IntradayBar],
    config: IntradayScalpConfig | None = None,
) -> list[IntradayTradeResult]:
    config = config or IntradayScalpConfig()
    ordered_bars = list(bars)
    results: list[IntradayTradeResult] = []
    for setup in setups:
        first_index = setup.entry_index
        if first_index >= len(ordered_bars):
            continue

        last_index = _last_exit_index(ordered_bars, first_index, config)
        exit_index: int | None = None
        exit_price = setup.entry_price
        exit_reason = "timeout"
        for bar_index in range(first_index, last_index + 1):
            bar = ordered_bars[bar_index]
            hit_target = bar.high >= setup.target_price
            hit_stop = bar.low <= setup.stop_loss
            if hit_target and hit_stop:
                if bar.close >= bar.open:
                    exit_reason = "target_first_ambiguous_green"
                    exit_price = setup.target_price
                else:
                    exit_reason = "stop_first_ambiguous_red"
                    exit_price = setup.stop_loss
                exit_index = bar_index
                break
            if hit_target:
                exit_reason = "target"
                exit_price = setup.target_price
                exit_index = bar_index
                break
            if hit_stop:
                exit_reason = "stop"
                exit_price = setup.stop_loss
                exit_index = bar_index
                break

        if exit_index is None:
            exit_index = last_index
            exit_price = ordered_bars[exit_index].close

        pnl_points = exit_price - setup.entry_price
        rr = pnl_points / setup.risk_points if setup.risk_points else 0.0
        return_pct = (pnl_points / setup.entry_price * 100.0) if setup.entry_price else 0.0
        results.append(
            IntradayTradeResult(
                symbol=setup.symbol,
                signal_timestamp=setup.signal_timestamp,
                entry_timestamp=setup.entry_timestamp,
                exit_timestamp=ordered_bars[exit_index].timestamp if exit_index is not None else None,
                signal_index=setup.signal_index,
                entry_index=setup.entry_index,
                exit_index=exit_index,
                entry_price=setup.entry_price,
                stop_loss=setup.stop_loss,
                target_price=setup.target_price,
                exit_price=exit_price,
                exit_reason=exit_reason,
                bars_held=exit_index - setup.entry_index + 1,
                risk_points=setup.risk_points,
                rr=rr,
                return_pct=return_pct,
                pnl_points=pnl_points,
                pnl_rupees=pnl_points * setup.lot_size,
                risk_reward=setup.risk_reward,
                risk_reward_label=setup.risk_reward_label,
                timeframe_sec=setup.timeframe_sec,
                instrument_key=setup.instrument_key,
                trading_symbol=setup.trading_symbol,
                contract_expiry=setup.contract_expiry,
                lot_size=setup.lot_size,
                open_interest=setup.open_interest,
                source=setup.source,
            )
        )
    return results


def compute_intraday_summary(results: Sequence[IntradayTradeResult]) -> dict[str, Any]:
    total = len(results)
    wins = sum(1 for result in results if result.pnl_points > 0)
    losses = sum(1 for result in results if result.pnl_points < 0)
    flat = total - wins - losses
    timeouts = sum(1 for result in results if result.exit_reason == "timeout")
    rrs = [result.rr for result in results]
    returns = [result.return_pct for result in results]
    pnl_rupees = [result.pnl_rupees for result in results]
    return {
        "total_trades": total,
        "wins": wins,
        "losses": losses,
        "flat": flat,
        "timeouts": timeouts,
        "win_rate_pct": (wins / total * 100.0) if total else 0.0,
        "avg_rr": _average(rrs),
        "avg_return_pct": _average(returns),
        "total_pnl_rupees": sum(pnl_rupees),
        "avg_pnl_rupees": _average(pnl_rupees),
        "best_rr": max(rrs) if rrs else 0.0,
        "worst_rr": min(rrs) if rrs else 0.0,
    }


def build_intraday_analysis_payload(
    bars: Sequence[IntradayBar],
    setups: Sequence[IntradaySetup],
    results: Sequence[IntradayTradeResult],
    config: IntradayScalpConfig | None = None,
) -> dict[str, Any]:
    config = config or IntradayScalpConfig()
    ordered_bars = list(bars)
    setup_lookup = {
        (setup.symbol, setup.signal_index, setup.entry_index): setup
        for setup in setups
    }
    return {
        "data_mode": config.data_mode,
        "timeframe_sec": config.timeframe_sec,
        "bars": [_bar_payload(bar, index) for index, bar in enumerate(ordered_bars)],
        "setups": [_setup_payload(setup) for setup in setups],
        "results": [_result_payload(result) for result in results],
        "summary": compute_intraday_summary(results),
        "replay_windows": [
            _replay_window_payload(result, setup_lookup, ordered_bars, config)
            for result in results
        ],
    }


def load_intraday_bars(
    db_path: str | Path,
    symbol: str,
    *,
    data_mode: str | None = None,
    start_date: date | datetime | str | None = None,
    end_date: date | datetime | str | None = None,
    timeframe_sec: int = 60,
) -> list[IntradayBar]:
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"SQLite database not found: {db_path}")

    clauses = ["symbol = ?", "timeframe_sec = ?"]
    values: list[Any] = [symbol.upper(), int(timeframe_sec)]
    if data_mode:
        clauses.append("data_mode = ?")
        values.append(data_mode)
    if start_date:
        clauses.append("date >= ?")
        values.append(_date_text(start_date))
    if end_date:
        clauses.append("date <= ?")
        values.append(_date_text(end_date))

    query = f"""
        SELECT symbol, timestamp, date, timeframe_sec, open, high, low, close, volume,
               open_interest, instrument_key, trading_symbol, market_segment, instrument_type,
               contract_expiry, lot_size, source, data_mode
        FROM ohlcv_intraday
        WHERE {' AND '.join(clauses)}
        ORDER BY timestamp ASC
    """

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = list(conn.execute(query, values))
    finally:
        conn.close()

    return [_bar_from_row(row) for row in rows]


def build_intraday_symbol_payload(
    db_path: str | Path,
    *,
    symbol: str,
    data_mode: str | None = None,
    start_date: date | datetime | str | None = None,
    end_date: date | datetime | str | None = None,
    config: IntradayScalpConfig | None = None,
) -> dict[str, Any]:
    config = config or IntradayScalpConfig()
    bars = load_intraday_bars(
        db_path,
        symbol,
        data_mode=data_mode,
        start_date=start_date,
        end_date=end_date,
        timeframe_sec=config.timeframe_sec,
    )
    resolved_mode = data_mode or (bars[0].data_mode if bars else config.data_mode)
    config = IntradayScalpConfig(
        base_lookback=config.base_lookback,
        spike_multiple=config.spike_multiple,
        min_turnover=config.min_turnover,
        close_location_threshold=config.close_location_threshold,
        risk_reward=config.risk_reward,
        max_hold_bars=config.max_hold_bars,
        timeframe_sec=config.timeframe_sec,
        data_mode=resolved_mode,
        max_base_range_pct=config.max_base_range_pct,
        replay_pre_bars=config.replay_pre_bars,
        replay_post_bars=config.replay_post_bars,
        allow_overnight=config.allow_overnight,
    )
    setups = find_volume_spike_setups(bars, config)
    results = simulate_intraday_exits(setups, bars, config)
    payload = build_intraday_analysis_payload(bars, setups, results, config)
    payload["symbol"] = symbol.upper()
    payload["date_from"] = _date_text(start_date) if start_date else (bars[0].timestamp.date().isoformat() if bars else None)
    payload["date_to"] = _date_text(end_date) if end_date else (bars[-1].timestamp.date().isoformat() if bars else None)
    return payload


def list_intraday_symbols(
    db_path: str | Path,
    *,
    data_mode: str | None = None,
    timeframe_sec: int = 60,
) -> tuple[str, ...]:
    db_path = Path(db_path)
    clauses = ["timeframe_sec = ?"]
    values: list[Any] = [int(timeframe_sec)]
    if data_mode:
        clauses.append("data_mode = ?")
        values.append(data_mode)
    query = f"""
        SELECT DISTINCT symbol
        FROM ohlcv_intraday
        WHERE {' AND '.join(clauses)}
        ORDER BY symbol
    """
    conn = sqlite3.connect(db_path)
    try:
        rows = list(conn.execute(query, values))
    finally:
        conn.close()
    return tuple(str(row[0]).upper() for row in rows)


def run_intraday_universe_backtest(
    db_path: str | Path,
    *,
    data_mode: str = "equity_signal_proxy_1m",
    symbols: Sequence[str] | None = None,
    start_date: date | datetime | str | None = None,
    end_date: date | datetime | str | None = None,
    config: IntradayScalpConfig | None = None,
    max_instances: int = 250,
    min_instance_rr: float | None = None,
    instance_sort: str = "follow_through",
) -> dict[str, Any]:
    config = config or IntradayScalpConfig(data_mode=data_mode)
    if config.data_mode != data_mode:
        config = _replace_config_data_mode(config, data_mode)
    selected_symbols = tuple(symbol.upper() for symbol in symbols) if symbols else list_intraday_symbols(
        db_path,
        data_mode=data_mode,
        timeframe_sec=config.timeframe_sec,
    )

    all_results: list[IntradayTradeResult] = []
    all_instances: list[dict[str, Any]] = []
    by_symbol: list[dict[str, Any]] = []
    total_bars = 0
    for symbol in selected_symbols:
        bars = load_intraday_bars(
            db_path,
            symbol,
            data_mode=data_mode,
            start_date=start_date,
            end_date=end_date,
            timeframe_sec=config.timeframe_sec,
        )
        total_bars += len(bars)
        setups = find_volume_spike_setups(bars, config)
        results = simulate_intraday_exits(setups, bars, config)
        setup_lookup = {
            (setup.symbol, setup.signal_index, setup.entry_index): setup
            for setup in setups
        }
        all_instances.extend(
            _instance_payload(result, setup_lookup, bars, config, data_mode, bucket=BUCKET_SAME_DAY)
            for result in results
        )
        all_results.extend(results)
        symbol_summary = compute_intraday_summary(results)
        by_symbol.append(
            {
                "symbol": symbol,
                "bars": len(bars),
                "setups": len(setups),
                "trades": symbol_summary["total_trades"],
                "wins": symbol_summary["wins"],
                "losses": symbol_summary["losses"],
                "win_rate_pct": symbol_summary["win_rate_pct"],
                "avg_rr": symbol_summary["avg_rr"],
                "total_pnl_rupees": symbol_summary["total_pnl_rupees"],
                "avg_pnl_rupees": symbol_summary["avg_pnl_rupees"],
            }
        )

    by_symbol.sort(key=lambda row: (row["total_pnl_rupees"], row["trades"]), reverse=True)
    ranked_instances = _rank_instances(
        all_instances,
        min_rr=min_instance_rr,
        sort_by=instance_sort,
    )
    returned_instances = ranked_instances if max_instances <= 0 else ranked_instances[:max_instances]
    return {
        "data_mode": data_mode,
        "timeframe_sec": config.timeframe_sec,
        "symbols_scanned": len(selected_symbols),
        "total_bars": total_bars,
        "summary": compute_intraday_summary(all_results),
        "by_symbol": by_symbol,
        "instances": returned_instances,
        "instances_total": len(ranked_instances),
        "instances_returned": len(returned_instances),
        "instance_sort": instance_sort,
        "min_instance_rr": min_instance_rr,
        "config": _config_payload(config),
        "date_from": _date_text(start_date) if start_date else None,
        "date_to": _date_text(end_date) if end_date else None,
    }


def run_intraday_bucketed_backtest(
    db_path: str | Path,
    *,
    data_mode: str = "equity_signal_proxy_1m",
    symbols: Sequence[str] | None = None,
    start_date: date | datetime | str | None = None,
    end_date: date | datetime | str | None = None,
    config: IntradayScalpConfig | None = None,
    buckets: Sequence[str] = DEFAULT_BUCKETS,
    two_day_hold_bars: int = 750,
    max_instances: int = 500,
    min_instance_rr: float | None = None,
    instance_sort: str = "follow_through",
) -> dict[str, Any]:
    config = config or IntradayScalpConfig(data_mode=data_mode)
    if config.data_mode != data_mode:
        config = _replace_config_data_mode(config, data_mode)

    requested_buckets = tuple(dict.fromkeys(buckets))
    unsupported = [bucket for bucket in requested_buckets if bucket not in DEFAULT_BUCKETS]
    if unsupported:
        allowed = ", ".join(DEFAULT_BUCKETS)
        raise ValueError(f"Unsupported buckets: {', '.join(unsupported)}. Use: {allowed}")

    selected_symbols = tuple(symbol.upper() for symbol in symbols) if symbols else list_intraday_symbols(
        db_path,
        data_mode=data_mode,
        timeframe_sec=config.timeframe_sec,
    )

    same_day_config = _replace_config(config, allow_overnight=False, data_mode=data_mode)
    overnight_config = _replace_config(config, allow_overnight=True, data_mode=data_mode)
    two_day_config = _replace_config(
        config,
        allow_overnight=True,
        max_hold_bars=max(two_day_hold_bars, config.max_hold_bars),
        data_mode=data_mode,
    )

    all_instances: list[dict[str, Any]] = []
    by_symbol: dict[str, dict[str, Any]] = {}
    by_bucket_results: dict[str, list[IntradayTradeResult]] = {bucket: [] for bucket in requested_buckets}
    total_bars = 0

    for symbol in selected_symbols:
        bars = load_intraday_bars(
            db_path,
            symbol,
            data_mode=data_mode,
            start_date=start_date,
            end_date=end_date,
            timeframe_sec=config.timeframe_sec,
        )
        total_bars += len(bars)
        by_symbol[symbol] = _empty_symbol_bucket_row(symbol, len(bars), requested_buckets)
        if not bars:
            continue

        if BUCKET_SAME_DAY in requested_buckets:
            setups = find_volume_spike_setups(bars, same_day_config)
            results = simulate_intraday_exits(setups, bars, same_day_config)
            _append_bucket_instances(
                all_instances,
                by_bucket_results,
                by_symbol[symbol],
                bucket=BUCKET_SAME_DAY,
                results=results,
                setups=setups,
                bars=bars,
                config=same_day_config,
                data_mode=data_mode,
            )

        overnight_setups: list[IntradaySetup] | None = None
        overnight_results: list[IntradayTradeResult] | None = None
        if BUCKET_NEXT_MORNING_ENTRY in requested_buckets:
            overnight_setups = find_volume_spike_setups(bars, overnight_config)
            overnight_results = simulate_intraday_exits(overnight_setups, bars, overnight_config)
            next_morning_results = [
                result
                for result in overnight_results
                if result.signal_timestamp.date() != result.entry_timestamp.date()
            ]
            next_morning_setup_keys = _result_keys(next_morning_results)
            next_morning_setups = [
                setup for setup in overnight_setups if (setup.symbol, setup.signal_index, setup.entry_index) in next_morning_setup_keys
            ]
            _append_bucket_instances(
                all_instances,
                by_bucket_results,
                by_symbol[symbol],
                bucket=BUCKET_NEXT_MORNING_ENTRY,
                results=next_morning_results,
                setups=next_morning_setups,
                bars=bars,
                config=overnight_config,
                data_mode=data_mode,
            )

        if BUCKET_TWO_DAY_HOLD in requested_buckets:
            two_day_setups = find_volume_spike_setups(bars, two_day_config)
            two_day_results = simulate_intraday_exits(two_day_setups, bars, two_day_config)
            two_day_results = [
                result
                for result in two_day_results
                if result.signal_timestamp.date() == result.entry_timestamp.date()
                and result.exit_timestamp is not None
                and result.exit_timestamp.date() != result.entry_timestamp.date()
            ]
            two_day_setup_keys = _result_keys(two_day_results)
            two_day_setups = [
                setup for setup in two_day_setups if (setup.symbol, setup.signal_index, setup.entry_index) in two_day_setup_keys
            ]
            _append_bucket_instances(
                all_instances,
                by_bucket_results,
                by_symbol[symbol],
                bucket=BUCKET_TWO_DAY_HOLD,
                results=two_day_results,
                setups=two_day_setups,
                bars=bars,
                config=two_day_config,
                data_mode=data_mode,
            )

    ranked_instances = _rank_instances(
        all_instances,
        min_rr=min_instance_rr,
        sort_by=instance_sort,
    )
    returned_instances = ranked_instances if max_instances <= 0 else ranked_instances[:max_instances]
    bucket_summaries = {
        bucket: compute_intraday_summary(by_bucket_results[bucket])
        for bucket in requested_buckets
    }
    symbol_rows = sorted(
        by_symbol.values(),
        key=lambda row: (row["total_trades"], row["avg_rr"]),
        reverse=True,
    )
    return {
        "data_mode": data_mode,
        "timeframe_sec": config.timeframe_sec,
        "symbols_scanned": len(selected_symbols),
        "total_bars": total_bars,
        "summary": compute_intraday_summary([result for results in by_bucket_results.values() for result in results]),
        "bucket_summaries": bucket_summaries,
        "by_symbol": symbol_rows,
        "instances": returned_instances,
        "instances_total": len(ranked_instances),
        "instances_returned": len(returned_instances),
        "instance_sort": instance_sort,
        "min_instance_rr": min_instance_rr,
        "buckets": list(requested_buckets),
        "two_day_hold_bars": two_day_hold_bars,
        "config": _config_payload(config),
        "date_from": _date_text(start_date) if start_date else None,
        "date_to": _date_text(end_date) if end_date else None,
    }


def _is_quiet_base(base_bars: Sequence[IntradayBar], config: IntradayScalpConfig) -> bool:
    if config.max_base_range_pct <= 0:
        return True
    base_high = max(bar.high for bar in base_bars)
    base_low = min(bar.low for bar in base_bars)
    closes = [bar.close for bar in base_bars if bar.close > 0]
    anchor = float(median(closes)) if closes else 0.0
    if anchor <= 0:
        return False
    return ((base_high - base_low) / anchor) <= config.max_base_range_pct


def _bar_turnover(bar: IntradayBar) -> float:
    if bar.turnover > 0:
        return bar.turnover
    return bar.volume * bar.close


def _close_location(bar: IntradayBar) -> float:
    candle_range = bar.high - bar.low
    if candle_range <= 0:
        return 1.0 if bar.close >= bar.high else 0.0
    return (bar.close - bar.low) / candle_range


def _risk_reward_label(risk_reward: float) -> str:
    return f"1:{_format_number(risk_reward)}"


def _format_number(value: float) -> str:
    return f"{value:.4f}".rstrip("0").rstrip(".")


def _average(values: Sequence[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _replace_config_data_mode(config: IntradayScalpConfig, data_mode: str) -> IntradayScalpConfig:
    return _replace_config(config, data_mode=data_mode)


def _replace_config(
    config: IntradayScalpConfig,
    *,
    base_lookback: int | None = None,
    spike_multiple: float | None = None,
    min_turnover: float | None = None,
    close_location_threshold: float | None = None,
    risk_reward: float | None = None,
    max_hold_bars: int | None = None,
    timeframe_sec: int | None = None,
    data_mode: str | None = None,
    max_base_range_pct: float | None = None,
    replay_pre_bars: int | None = None,
    replay_post_bars: int | None = None,
    allow_overnight: bool | None = None,
) -> IntradayScalpConfig:
    return IntradayScalpConfig(
        base_lookback=config.base_lookback if base_lookback is None else base_lookback,
        spike_multiple=config.spike_multiple if spike_multiple is None else spike_multiple,
        min_turnover=config.min_turnover if min_turnover is None else min_turnover,
        close_location_threshold=(
            config.close_location_threshold if close_location_threshold is None else close_location_threshold
        ),
        risk_reward=config.risk_reward if risk_reward is None else risk_reward,
        max_hold_bars=config.max_hold_bars if max_hold_bars is None else max_hold_bars,
        timeframe_sec=config.timeframe_sec if timeframe_sec is None else timeframe_sec,
        data_mode=config.data_mode if data_mode is None else data_mode,
        max_base_range_pct=config.max_base_range_pct if max_base_range_pct is None else max_base_range_pct,
        replay_pre_bars=config.replay_pre_bars if replay_pre_bars is None else replay_pre_bars,
        replay_post_bars=config.replay_post_bars if replay_post_bars is None else replay_post_bars,
        allow_overnight=config.allow_overnight if allow_overnight is None else allow_overnight,
    )


def _config_payload(config: IntradayScalpConfig) -> dict[str, Any]:
    return {
        "base_lookback": config.base_lookback,
        "spike_multiple": config.spike_multiple,
        "min_turnover": config.min_turnover,
        "close_location_threshold": config.close_location_threshold,
        "risk_reward": config.risk_reward,
        "max_hold_bars": config.max_hold_bars,
        "timeframe_sec": config.timeframe_sec,
        "data_mode": config.data_mode,
        "max_base_range_pct": config.max_base_range_pct,
        "replay_pre_bars": config.replay_pre_bars,
        "replay_post_bars": config.replay_post_bars,
        "allow_overnight": config.allow_overnight,
    }


def _iso(value: datetime | date | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def _date_text(value: date | datetime | str) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)[:10]


def _parse_timestamp(value: Any) -> datetime:
    text = str(value)
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    return datetime.fromisoformat(text)


def _bar_from_row(row: sqlite3.Row) -> IntradayBar:
    volume = float(row["volume"] if row["volume"] is not None else 0.0)
    close = float(row["close"])
    return IntradayBar(
        symbol=str(row["symbol"]).upper(),
        timestamp=_parse_timestamp(row["timestamp"]),
        open=float(row["open"]),
        high=float(row["high"]),
        low=float(row["low"]),
        close=close,
        volume=volume,
        turnover=volume * close,
        instrument_key=row["instrument_key"],
        trading_symbol=row["trading_symbol"],
        contract_expiry=row["contract_expiry"],
        lot_size=int(row["lot_size"] if row["lot_size"] is not None else 1),
        open_interest=float(row["open_interest"] if row["open_interest"] is not None else 0.0),
        source=str(row["source"] if row["source"] is not None else "unknown"),
        data_mode=str(row["data_mode"] if row["data_mode"] is not None else "unknown"),
        timeframe_sec=int(row["timeframe_sec"] if row["timeframe_sec"] is not None else 60),
        market_segment=row["market_segment"],
        instrument_type=row["instrument_type"],
    )


def _bar_payload(bar: IntradayBar, index: int) -> dict[str, Any]:
    return {
        "index": index,
        "symbol": bar.symbol,
        "timestamp": bar.timestamp.isoformat(),
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
        "turnover": _bar_turnover(bar),
        "instrument_key": bar.instrument_key,
        "trading_symbol": bar.trading_symbol,
        "contract_expiry": _iso(bar.contract_expiry),
        "lot_size": bar.lot_size,
        "open_interest": bar.open_interest,
        "source": bar.source,
        "data_mode": bar.data_mode,
        "timeframe_sec": bar.timeframe_sec,
        "market_segment": bar.market_segment,
        "instrument_type": bar.instrument_type,
    }


def _setup_payload(setup: IntradaySetup) -> dict[str, Any]:
    return {
        "symbol": setup.symbol,
        "signal_timestamp": setup.signal_timestamp.isoformat(),
        "signal_index": setup.signal_index,
        "entry_timestamp": setup.entry_timestamp.isoformat(),
        "entry_index": setup.entry_index,
        "entry_price": setup.entry_price,
        "stop_loss": setup.stop_loss,
        "target_price": setup.target_price,
        "risk_points": setup.risk_points,
        "risk_reward": setup.risk_reward,
        "risk_reward_label": setup.risk_reward_label,
        "base_high": setup.base_high,
        "base_low": setup.base_low,
        "rolling_median_volume": setup.rolling_median_volume,
        "spike_volume": setup.spike_volume,
        "volume_multiple": setup.volume_multiple,
        "turnover": setup.turnover,
        "close_location": setup.close_location,
        "timeframe_sec": setup.timeframe_sec,
        "instrument_key": setup.instrument_key,
        "trading_symbol": setup.trading_symbol,
        "contract_expiry": _iso(setup.contract_expiry),
        "lot_size": setup.lot_size,
        "open_interest": setup.open_interest,
        "source": setup.source,
    }


def _result_payload(result: IntradayTradeResult) -> dict[str, Any]:
    return {
        "symbol": result.symbol,
        "signal_timestamp": result.signal_timestamp.isoformat(),
        "entry_timestamp": result.entry_timestamp.isoformat(),
        "exit_timestamp": _iso(result.exit_timestamp),
        "signal_index": result.signal_index,
        "entry_index": result.entry_index,
        "exit_index": result.exit_index,
        "entry_price": result.entry_price,
        "stop_loss": result.stop_loss,
        "target_price": result.target_price,
        "exit_price": result.exit_price,
        "exit_reason": result.exit_reason,
        "bars_held": result.bars_held,
        "risk_points": result.risk_points,
        "rr": result.rr,
        "return_pct": result.return_pct,
        "pnl_points": result.pnl_points,
        "pnl_rupees": result.pnl_rupees,
        "risk_reward": result.risk_reward,
        "risk_reward_label": result.risk_reward_label,
        "timeframe_sec": result.timeframe_sec,
        "instrument_key": result.instrument_key,
        "trading_symbol": result.trading_symbol,
        "contract_expiry": _iso(result.contract_expiry),
        "lot_size": result.lot_size,
        "open_interest": result.open_interest,
        "source": result.source,
    }


def _replay_window_payload(
    result: IntradayTradeResult,
    setup_lookup: dict[tuple[str, int, int], IntradaySetup],
    bars: Sequence[IntradayBar],
    config: IntradayScalpConfig,
) -> dict[str, Any]:
    setup = setup_lookup.get((result.symbol, result.signal_index, result.entry_index))
    signal_index = setup.signal_index if setup else result.signal_index
    entry_index = setup.entry_index if setup else result.entry_index
    anchor_exit_index = result.exit_index if result.exit_index is not None else entry_index
    start_index = max(0, signal_index - config.replay_pre_bars)
    end_index = min(len(bars) - 1, anchor_exit_index + config.replay_post_bars)
    return {
        "symbol": result.symbol,
        "signal_index": signal_index,
        "entry_index": entry_index,
        "exit_index": result.exit_index,
        "start_index": start_index,
        "end_index": end_index,
        "bars": [
            _bar_payload(bar, index)
            for index, bar in enumerate(bars[start_index : end_index + 1], start=start_index)
        ],
    }


def _empty_symbol_bucket_row(symbol: str, bars: int, buckets: Sequence[str]) -> dict[str, Any]:
    row: dict[str, Any] = {
        "symbol": symbol,
        "bars": bars,
        "total_trades": 0,
        "wins": 0,
        "losses": 0,
        "avg_rr": 0.0,
    }
    for bucket in buckets:
        row[bucket] = {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "avg_rr": 0.0,
        }
    return row


def _result_keys(results: Sequence[IntradayTradeResult]) -> set[tuple[str, int, int]]:
    return {
        (result.symbol, result.signal_index, result.entry_index)
        for result in results
    }


def _append_bucket_instances(
    all_instances: list[dict[str, Any]],
    by_bucket_results: dict[str, list[IntradayTradeResult]],
    symbol_row: dict[str, Any],
    *,
    bucket: str,
    results: Sequence[IntradayTradeResult],
    setups: Sequence[IntradaySetup],
    bars: Sequence[IntradayBar],
    config: IntradayScalpConfig,
    data_mode: str,
) -> None:
    setup_lookup = {
        (setup.symbol, setup.signal_index, setup.entry_index): setup
        for setup in setups
    }
    by_bucket_results[bucket].extend(results)
    all_instances.extend(
        _instance_payload(result, setup_lookup, bars, config, data_mode, bucket=bucket)
        for result in results
    )
    summary = compute_intraday_summary(results)
    symbol_row[bucket] = {
        "trades": summary["total_trades"],
        "wins": summary["wins"],
        "losses": summary["losses"],
        "avg_rr": summary["avg_rr"],
    }
    symbol_row["total_trades"] = int(symbol_row["total_trades"]) + summary["total_trades"]
    symbol_row["wins"] = int(symbol_row["wins"]) + summary["wins"]
    symbol_row["losses"] = int(symbol_row["losses"]) + summary["losses"]
    trade_count = int(symbol_row["total_trades"])
    if trade_count:
        bucket_results = [
            result
            for bucket_value in DEFAULT_BUCKETS
            for result in by_bucket_results.get(bucket_value, [])
            if result.symbol == symbol_row["symbol"]
        ]
        symbol_row["avg_rr"] = _average([result.rr for result in bucket_results])


def _instance_payload(
    result: IntradayTradeResult,
    setup_lookup: dict[tuple[str, int, int], IntradaySetup],
    bars: Sequence[IntradayBar],
    config: IntradayScalpConfig,
    data_mode: str,
    *,
    bucket: str,
) -> dict[str, Any]:
    setup = setup_lookup[(result.symbol, result.signal_index, result.entry_index)]
    excursion = _trade_excursion_payload(result, bars, config)
    return {
        "symbol": result.symbol,
        "bucket": bucket,
        "data_mode": data_mode,
        "signal_timestamp": result.signal_timestamp.isoformat(),
        "entry_timestamp": result.entry_timestamp.isoformat(),
        "exit_timestamp": _iso(result.exit_timestamp),
        "entry_price": result.entry_price,
        "stop_loss": result.stop_loss,
        "target_price": result.target_price,
        "exit_price": result.exit_price,
        "exit_reason": result.exit_reason,
        "bars_held": result.bars_held,
        "risk_points": result.risk_points,
        "rr": result.rr,
        "return_pct": result.return_pct,
        "pnl_points": result.pnl_points,
        "pnl_rupees": result.pnl_rupees,
        "risk_reward": result.risk_reward,
        "risk_reward_label": result.risk_reward_label,
        "base_high": setup.base_high,
        "base_low": setup.base_low,
        "rolling_median_volume": setup.rolling_median_volume,
        "spike_volume": setup.spike_volume,
        "volume_multiple": setup.volume_multiple,
        "turnover": setup.turnover,
        "close_location": setup.close_location,
        "max_favorable_points": excursion["max_favorable_points"],
        "max_favorable_rr": excursion["max_favorable_rr"],
        "max_adverse_points": excursion["max_adverse_points"],
        "max_adverse_rr": excursion["max_adverse_rr"],
        "timeframe_sec": result.timeframe_sec,
        "instrument_key": result.instrument_key,
        "trading_symbol": result.trading_symbol,
        "contract_expiry": _iso(result.contract_expiry),
        "lot_size": result.lot_size,
        "open_interest": result.open_interest,
        "source": result.source,
        "review_url": _review_url(result.symbol, data_mode, config),
    }


def _trade_excursion_payload(
    result: IntradayTradeResult,
    bars: Sequence[IntradayBar],
    config: IntradayScalpConfig,
) -> dict[str, float]:
    last_index = _last_exit_index(bars, result.entry_index, config)
    trade_bars = bars[result.entry_index : last_index + 1]
    if not trade_bars or result.risk_points <= 0:
        return {
            "max_favorable_points": 0.0,
            "max_favorable_rr": 0.0,
            "max_adverse_points": 0.0,
            "max_adverse_rr": 0.0,
        }

    max_favorable_points = max(bar.high - result.entry_price for bar in trade_bars)
    max_adverse_points = min(bar.low - result.entry_price for bar in trade_bars)
    return {
        "max_favorable_points": max_favorable_points,
        "max_favorable_rr": max_favorable_points / result.risk_points,
        "max_adverse_points": max_adverse_points,
        "max_adverse_rr": max_adverse_points / result.risk_points,
    }


def _rank_instances(
    instances: Sequence[dict[str, Any]],
    *,
    min_rr: float | None,
    sort_by: str,
) -> list[dict[str, Any]]:
    filtered = [
        instance
        for instance in instances
        if min_rr is None or float(instance["rr"]) >= min_rr
    ]
    sort_keys = {
        "follow_through": lambda row: (row["rr"], row["max_favorable_rr"], row["volume_multiple"]),
        "best_rr": lambda row: (row["rr"], row["max_favorable_rr"], row["volume_multiple"]),
        "volume_multiple": lambda row: (row["volume_multiple"], row["max_favorable_rr"], row["rr"]),
        "turnover": lambda row: (row["turnover"], row["max_favorable_rr"], row["rr"]),
        "latest": lambda row: (row["signal_timestamp"], row["max_favorable_rr"], row["rr"]),
    }
    if sort_by not in sort_keys:
        allowed = ", ".join(sorted(sort_keys))
        raise ValueError(f"Unsupported instance_sort {sort_by!r}. Use one of: {allowed}")
    return sorted(filtered, key=sort_keys[sort_by], reverse=True)


def _last_exit_index(
    bars: Sequence[IntradayBar],
    first_index: int,
    config: IntradayScalpConfig,
) -> int:
    last_index = min(len(bars) - 1, first_index + config.max_hold_bars - 1)
    if config.allow_overnight or first_index >= len(bars):
        return last_index

    entry_date = bars[first_index].timestamp.date()
    while last_index > first_index and bars[last_index].timestamp.date() != entry_date:
        last_index -= 1
    return last_index


def _review_url(symbol: str, data_mode: str, config: IntradayScalpConfig) -> str:
    query = urlencode(
        {
            "symbol": symbol,
            "data_mode": data_mode,
            "risk_reward": _format_number(config.risk_reward),
            "base_lookback": config.base_lookback,
            "spike_multiple": _format_number(config.spike_multiple),
            "min_turnover": _format_number(config.min_turnover),
            "close_location": _format_number(config.close_location_threshold),
            "max_hold_bars": config.max_hold_bars,
            "max_base_range_pct": _format_number(config.max_base_range_pct),
            "allow_overnight": str(config.allow_overnight).lower(),
        }
    )
    return f"/api/intraday/analyze?{query}"


def write_intraday_universe_report(payload: dict[str, Any], output_path: str | Path) -> None:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.suffix.lower() == ".csv":
        _write_instances_csv(payload.get("instances", []), output_path)
        return
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _write_instances_csv(instances: Sequence[dict[str, Any]], output_path: Path) -> None:
    fieldnames = [
        "bucket",
        "symbol",
        "signal_timestamp",
        "entry_timestamp",
        "exit_timestamp",
        "entry_price",
        "stop_loss",
        "target_price",
        "exit_price",
        "exit_reason",
        "bars_held",
        "rr",
        "max_favorable_rr",
        "max_adverse_rr",
        "return_pct",
        "pnl_points",
        "volume_multiple",
        "spike_volume",
        "rolling_median_volume",
        "turnover",
        "close_location",
        "risk_reward_label",
        "data_mode",
        "review_url",
    ]
    with output_path.open("w", newline="", encoding="utf-8") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=fieldnames)
        writer.writeheader()
        for instance in instances:
            writer.writerow({field: instance.get(field) for field in fieldnames})


def _parse_symbols(values: Sequence[str] | None) -> tuple[str, ...] | None:
    if not values:
        return None
    symbols: list[str] = []
    for value in values:
        symbols.extend(part.strip().upper() for part in str(value).replace(",", " ").split() if part.strip())
    return tuple(dict.fromkeys(symbols)) or None


def _parse_buckets(values: Sequence[str] | None) -> tuple[str, ...]:
    if not values:
        return DEFAULT_BUCKETS
    buckets: list[str] = []
    for value in values:
        buckets.extend(part.strip() for part in str(value).replace(",", " ").split() if part.strip())
    return tuple(dict.fromkeys(buckets)) or DEFAULT_BUCKETS


def _build_config_from_args(args: argparse.Namespace) -> IntradayScalpConfig:
    return IntradayScalpConfig(
        base_lookback=args.base_lookback,
        spike_multiple=args.spike_multiple,
        min_turnover=args.min_turnover,
        close_location_threshold=args.close_location,
        risk_reward=args.risk_reward,
        max_hold_bars=args.max_hold_bars,
        timeframe_sec=args.timeframe_sec,
        data_mode=args.data_mode,
        max_base_range_pct=args.max_base_range_pct,
        replay_pre_bars=args.replay_pre_bars,
        replay_post_bars=args.replay_post_bars,
        allow_overnight=args.allow_overnight,
    )


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backtest the intraday volume-spike scalp over local 1m data.")
    parser.add_argument("--db-path", default="data/nse_data.db", help="SQLite database path.")
    parser.add_argument("--data-mode", default="equity_signal_proxy_1m", help="Intraday data mode to scan.")
    parser.add_argument("--symbols", nargs="*", help="Optional symbols to scan.")
    parser.add_argument("--from", dest="start_date", help="Start date, YYYY-MM-DD.")
    parser.add_argument("--to", dest="end_date", help="End date, YYYY-MM-DD.")
    parser.add_argument("--base-lookback", type=int, default=20)
    parser.add_argument("--spike-multiple", type=float, default=3.0)
    parser.add_argument("--min-turnover", type=float, default=0.0)
    parser.add_argument("--close-location", type=float, default=0.75)
    parser.add_argument("--risk-reward", type=float, default=1.5)
    parser.add_argument("--max-hold-bars", type=int, default=10)
    parser.add_argument("--timeframe-sec", type=int, default=60)
    parser.add_argument("--max-base-range-pct", type=float, default=0.03)
    parser.add_argument("--replay-pre-bars", type=int, default=20)
    parser.add_argument("--replay-post-bars", type=int, default=20)
    parser.add_argument("--allow-overnight", action="store_true", help="Allow late signals to enter or hold past the same trading day.")
    parser.add_argument("--max-instances", type=int, default=250, help="Maximum mined instances to include; <=0 means all.")
    parser.add_argument("--min-instance-rr", type=float, help="Keep instances whose realized R is at least this value.")
    parser.add_argument(
        "--instance-sort",
        default="follow_through",
        choices=("follow_through", "best_rr", "volume_multiple", "turnover", "latest"),
        help="Ranking for the mined instances table.",
    )
    parser.add_argument("--output", help="Optional output path. .csv writes the instances table; other suffixes write JSON.")
    parser.add_argument("--bucketed", action="store_true", help="Run same-day, next-morning-entry, and two-day buckets separately.")
    parser.add_argument("--buckets", nargs="*", help="Optional bucket list: same_day next_morning_entry two_day_hold.")
    parser.add_argument("--two-day-hold-bars", type=int, default=750, help="Maximum bars for the two-day hold bucket.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)
    symbols = _parse_symbols(args.symbols)
    config = _build_config_from_args(args)
    if args.bucketed:
        payload = run_intraday_bucketed_backtest(
            args.db_path,
            data_mode=args.data_mode,
            symbols=symbols,
            start_date=args.start_date,
            end_date=args.end_date,
            config=config,
            buckets=_parse_buckets(args.buckets),
            two_day_hold_bars=args.two_day_hold_bars,
            max_instances=args.max_instances,
            min_instance_rr=args.min_instance_rr,
            instance_sort=args.instance_sort,
        )
    else:
        payload = run_intraday_universe_backtest(
            args.db_path,
            data_mode=args.data_mode,
            symbols=symbols,
            start_date=args.start_date,
            end_date=args.end_date,
            config=config,
            max_instances=args.max_instances,
            min_instance_rr=args.min_instance_rr,
            instance_sort=args.instance_sort,
        )
    if args.output:
        write_intraday_universe_report(payload, args.output)
    else:
        print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0


__all__ = [
    "IntradayBar",
    "IntradayScalpConfig",
    "IntradaySetup",
    "IntradayTradeResult",
    "find_volume_spike_setups",
    "simulate_intraday_exits",
    "compute_intraday_summary",
    "build_intraday_analysis_payload",
    "load_intraday_bars",
    "build_intraday_symbol_payload",
    "list_intraday_symbols",
    "run_intraday_universe_backtest",
    "run_intraday_bucketed_backtest",
    "write_intraday_universe_report",
]


if __name__ == "__main__":
    raise SystemExit(main())
