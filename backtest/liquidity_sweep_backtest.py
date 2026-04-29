from __future__ import annotations

import argparse
import dataclasses
import html
import json
import math
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from statistics import mean, median
from typing import Any, Iterable, Sequence


BENCHMARK_SYMBOL = "NIFTY50"
DEFAULT_DB_PATH = Path("data/nse_data.db")
DEFAULT_OUTPUT_PATH = Path("liquidity_sweep_report.html")


@dataclass(frozen=True, slots=True)
class Bar:
    symbol: str
    date: date
    open: float
    high: float
    low: float
    close: float
    adj_close: float
    volume: float
    instrument_key: str | None = None
    contract_expiry: date | None = None
    lot_size: int = 1
    trading_symbol: str | None = None
    open_interest: float = 0.0
    source: str = "unknown"


@dataclass(frozen=True, slots=True)
class StockMeta:
    symbol: str
    theme: str | None
    sub_theme: str | None
    is_active: bool | None


@dataclass(frozen=True, slots=True)
class Match:
    symbol: str
    signal_date: date
    signal_index: int
    signal_bar: Bar
    prev_bar: Bar
    stock_return_63: float
    benchmark_return_63: float
    d1_red: bool
    d_green: bool
    low_sweep: bool
    prev_red_bar: Bar | None = None
    benchmark_symbol: str = BENCHMARK_SYMBOL


@dataclass(frozen=True, slots=True)
class TradeSetup:
    symbol: str
    signal_date: date
    signal_index: int
    signal_bar: Bar
    prev_bar: Bar
    stock_return_63: float
    benchmark_return_63: float
    d1_red: bool
    d_green: bool
    low_sweep: bool
    entry_price: float
    stop_loss: float
    target_price: float
    target_label: str
    risk_reward: float
    entry_offset_pct: float = 0.0
    prev_red_bar: Bar | None = None
    instrument_key: str | None = None
    contract_expiry: date | None = None
    trading_symbol: str | None = None
    lot_size: int = 1
    benchmark_symbol: str = BENCHMARK_SYMBOL


@dataclass(frozen=True, slots=True)
class TradeResult:
    symbol: str
    signal_date: date
    exit_date: date | None
    entry_price: float
    stop_loss: float
    target_price: float
    exit_price: float
    exit_reason: str
    bars_held: int
    rr: float
    return_pct: float
    gross_return_pct: float
    stock_return_63: float
    benchmark_return_63: float
    d1_red: bool
    d_green: bool
    low_sweep: bool
    target_label: str
    entry_offset_pct: float = 0.0
    stop_reference_date: date | None = None
    instrument_key: str | None = None
    contract_expiry: date | None = None
    trading_symbol: str | None = None
    lot_size: int = 1
    risk_points: float = 0.0
    gross_pnl_points: float = 0.0
    net_pnl_points: float = 0.0
    gross_pnl_rupees: float = 0.0
    net_pnl_rupees: float = 0.0
    benchmark_symbol: str = BENCHMARK_SYMBOL


@dataclass(frozen=True, slots=True)
class BacktestConfig:
    db_path: Path
    symbols: tuple[str, ...] | None = None
    output_path: Path = DEFAULT_OUTPUT_PATH
    d1_red_only: bool = False
    multi_entry: bool = False
    sweep: bool = False
    risk_reward: float = 2.0
    entry_offset_pct: float = 0.5
    walk_forward_bars: int = 60
    fee_bps: float = 0.0
    slippage_bps: float = 0.0
    sweep_target_rrs: tuple[float, ...] = (1.5, 2.0, 2.5, 3.0)
    sweep_entry_offsets: tuple[float, ...] = (0.0, 0.5, 1.0)
    html_title: str = "Liquidity Sweep Backtest"


@dataclass(frozen=True, slots=True)
class BacktestData:
    bars_by_symbol: dict[str, list[Bar]]
    stock_meta: dict[str, StockMeta]
    benchmark_symbol: str
    benchmark_bars: list[Bar]
    benchmark_by_date: dict[date, Bar]
    symbols: tuple[str, ...]
    price_source: str = "cash"


def _coerce_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def _coerce_float(value: Any) -> float:
    if value is None:
        return float("nan")
    return float(value)


def _sanitize_symbols(symbols: Sequence[str] | None) -> tuple[str, ...] | None:
    if symbols is None:
        return None
    cleaned = []
    for symbol in symbols:
        for piece in str(symbol).replace(",", " ").split():
            piece = piece.strip().upper()
            if piece:
                cleaned.append(piece)
    return tuple(dict.fromkeys(cleaned))


def _read_symbols_file(path: str | Path | None) -> tuple[str, ...] | None:
    if not path:
        return None
    symbols: list[str] = []
    for raw_line in Path(path).read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        symbols.extend(part.strip().upper() for part in line.replace(",", " ").split() if part.strip())
    return _sanitize_symbols(symbols)


def _bars_from_rows(symbol: str, rows: Iterable[sqlite3.Row]) -> list[Bar]:
    bars = [
        Bar(
            symbol=symbol,
            date=_coerce_date(row["date"]),
            open=_coerce_float(row["open"]),
            high=_coerce_float(row["high"]),
            low=_coerce_float(row["low"]),
            close=_coerce_float(row["close"]),
            adj_close=_coerce_float(row["adj_close"] if "adj_close" in row.keys() and row["adj_close"] is not None else row["close"]),
            volume=_coerce_float(row["volume"] if "volume" in row.keys() and row["volume"] is not None else 0.0),
            instrument_key=row["instrument_key"] if "instrument_key" in row.keys() else None,
            contract_expiry=_coerce_date(row["expiry_date"]) if "expiry_date" in row.keys() and row["expiry_date"] else None,
            lot_size=int(row["lot_size"]) if "lot_size" in row.keys() and row["lot_size"] is not None else 1,
            trading_symbol=row["trading_symbol"] if "trading_symbol" in row.keys() else None,
            open_interest=_coerce_float(row["open_interest"] if "open_interest" in row.keys() and row["open_interest"] is not None else 0.0),
            source=str(row["source"]) if "source" in row.keys() and row["source"] is not None else "unknown",
        )
        for row in rows
    ]
    bars.sort(key=lambda bar: bar.date)
    return bars


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _has_futures_rows(conn: sqlite3.Connection) -> bool:
    if not _table_exists(conn, "ohlcv_futures_daily"):
        return False
    row = conn.execute("SELECT 1 FROM ohlcv_futures_daily LIMIT 1").fetchone()
    return row is not None


def _load_continuous_futures_bars(conn: sqlite3.Connection, symbol: str) -> list[Bar]:
    rows = list(
        conn.execute(
            """
            SELECT symbol, expiry_date, date, open, high, low, close, adj_close, volume, open_interest,
                   instrument_key, trading_symbol, lot_size, source
            FROM ohlcv_futures_daily
            WHERE symbol = ?
            ORDER BY date ASC, expiry_date ASC
            """,
            (symbol,),
        )
    )
    if not rows:
        return []

    front_month_rows: dict[str, sqlite3.Row] = {}
    for row in rows:
        row_date = str(row["date"])
        chosen = front_month_rows.get(row_date)
        if chosen is None or str(row["expiry_date"]) < str(chosen["expiry_date"]):
            front_month_rows[row_date] = row

    ordered_rows = [front_month_rows[key] for key in sorted(front_month_rows)]
    return _bars_from_rows(symbol, ordered_rows)


def load_data(db_path: str | Path, symbols: Sequence[str] | None = None) -> BacktestData:
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"SQLite database not found: {db_path}")

    requested_symbols = _sanitize_symbols(symbols)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        stock_meta: dict[str, StockMeta] = {}
        for row in conn.execute("SELECT * FROM stocks"):
            symbol = str(row["symbol"]).upper()
            stock_meta[symbol] = StockMeta(
                symbol=symbol,
                theme=row["theme"] if "theme" in row.keys() else None,
                sub_theme=row["sub_theme"] if "sub_theme" in row.keys() else None,
                is_active=bool(row["is_active"]) if "is_active" in row.keys() and row["is_active"] is not None else None,
            )

        if requested_symbols is None:
            fallback = [
                symbol
                for symbol, meta in stock_meta.items()
                if symbol != BENCHMARK_SYMBOL and (meta.is_active is None or meta.is_active)
            ]
            requested_symbols = tuple(sorted(fallback))

        universe = tuple(symbol for symbol in requested_symbols if symbol != BENCHMARK_SYMBOL)

        benchmark_rows = list(
            conn.execute(
                "SELECT symbol, date, open, high, low, close, adj_close, volume "
                "FROM ohlcv_daily WHERE symbol = ? ORDER BY date",
                (BENCHMARK_SYMBOL,),
            )
        )
        benchmark_bars = _bars_from_rows(BENCHMARK_SYMBOL, benchmark_rows)
        benchmark_by_date = {bar.date: bar for bar in benchmark_bars}

        use_futures = _has_futures_rows(conn)
        bars_by_symbol: dict[str, list[Bar]] = {}
        for symbol in universe:
            if use_futures:
                bars = _load_continuous_futures_bars(conn, symbol)
            else:
                rows = list(
                    conn.execute(
                        "SELECT symbol, date, open, high, low, close, adj_close, volume "
                        "FROM ohlcv_daily WHERE symbol = ? ORDER BY date",
                        (symbol,),
                    )
                )
                bars = _bars_from_rows(symbol, rows)
            if bars:
                bars_by_symbol[symbol] = bars

        if not benchmark_bars:
            raise ValueError(f"Benchmark {BENCHMARK_SYMBOL} not found in ohlcv_daily")
    finally:
        conn.close()

    return BacktestData(
        bars_by_symbol=bars_by_symbol,
        stock_meta=stock_meta,
        benchmark_symbol=BENCHMARK_SYMBOL,
        benchmark_bars=benchmark_bars,
        benchmark_by_date=benchmark_by_date,
        symbols=tuple(sorted(bars_by_symbol.keys())),
        price_source="futures" if use_futures else "cash",
    )


def _normalize_bound(value: date | datetime | str | None) -> date | None:
    if value is None or value == "":
        return None
    return _coerce_date(value)


def _date_in_bounds(day: date, start_date: date | None, end_date: date | None) -> bool:
    if start_date is not None and day < start_date:
        return False
    if end_date is not None and day > end_date:
        return False
    return True


def filter_backtest_data(
    data: BacktestData,
    symbols: Sequence[str] | None = None,
    start_date: date | datetime | str | None = None,
    end_date: date | datetime | str | None = None,
) -> BacktestData:
    selected_symbols = _sanitize_symbols(symbols) or data.symbols
    start_bound = _normalize_bound(start_date)
    end_bound = _normalize_bound(end_date)

    filtered_bars_by_symbol: dict[str, list[Bar]] = {}
    for symbol in selected_symbols:
        bars = data.bars_by_symbol.get(symbol, [])
        filtered = [bar for bar in bars if _date_in_bounds(bar.date, start_bound, end_bound)]
        if filtered:
            filtered_bars_by_symbol[symbol] = filtered

    filtered_benchmark_bars = [
        bar for bar in data.benchmark_bars if _date_in_bounds(bar.date, start_bound, end_bound)
    ]

    return BacktestData(
        bars_by_symbol=filtered_bars_by_symbol,
        stock_meta=data.stock_meta,
        benchmark_symbol=data.benchmark_symbol,
        benchmark_bars=filtered_benchmark_bars,
        benchmark_by_date={bar.date: bar for bar in filtered_benchmark_bars},
        symbols=tuple(sorted(filtered_bars_by_symbol.keys())),
        price_source=data.price_source,
    )


def _find_previous_red_bar(bars: Sequence[Bar], start_index: int) -> Bar | None:
    for idx in range(start_index - 1, -1, -1):
        bar = bars[idx]
        if bar.close < bar.open:
            return bar
    return None


def compute_sl(match: Match | TradeSetup, config: BacktestConfig) -> float:
    prev_red_bar = getattr(match, "prev_red_bar", None)
    if prev_red_bar is None:
        return float("nan")
    return max(0.0, prev_red_bar.low)


def compute_entry(stop_loss: float, config: BacktestConfig) -> float:
    return stop_loss * (1.0 + (config.entry_offset_pct / 100.0))


def compute_target(entry_price: float, stop_loss: float, config: BacktestConfig) -> float:
    risk = max(entry_price - stop_loss, 0.0)
    return entry_price + (risk * config.risk_reward)


def target_label(target_rr: float) -> str:
    if float(target_rr).is_integer():
        return f"{int(target_rr)}R"
    return f"{target_rr:.2f}R"


def _return_63(current: Bar, past: Bar) -> float:
    if past.adj_close == 0:
        return float("nan")
    return current.adj_close / past.adj_close - 1.0


def find_pattern_matches(data: BacktestData, config: BacktestConfig) -> list[Match]:
    matches: list[Match] = []
    benchmark_dates = data.benchmark_by_date
    benchmark_bars = data.benchmark_bars
    benchmark_index = {bar.date: idx for idx, bar in enumerate(benchmark_bars)}

    for symbol in data.symbols:
        bars = data.bars_by_symbol.get(symbol, [])
        if len(bars) <= 63:
            continue
        for idx in range(63, len(bars)):
            signal_bar = bars[idx]
            prev_bar = bars[idx - 1]
            if signal_bar.date not in benchmark_dates:
                continue
            benchmark_idx = benchmark_index.get(signal_bar.date)
            if benchmark_idx is None or benchmark_idx < 63:
                continue
            benchmark_past = benchmark_bars[benchmark_idx - 63]
            stock_past = bars[idx - 63]
            stock_return_63 = _return_63(signal_bar, stock_past)
            benchmark_return_63 = _return_63(benchmark_dates[signal_bar.date], benchmark_past)

            d_green = signal_bar.close > signal_bar.open
            low_sweep = signal_bar.low < prev_bar.low
            d1_red = prev_bar.close < prev_bar.open
            prev_red_bar = _find_previous_red_bar(bars, idx)
            benchmark_ok = stock_return_63 > benchmark_return_63
            return_ok = stock_return_63 > 0
            d1_ok = d1_red if config.d1_red_only else True
            if d_green and low_sweep and benchmark_ok and return_ok and d1_ok and prev_red_bar is not None:
                matches.append(
                    Match(
                        symbol=symbol,
                        signal_date=signal_bar.date,
                        signal_index=idx,
                        signal_bar=signal_bar,
                        prev_bar=prev_bar,
                        stock_return_63=stock_return_63,
                        benchmark_return_63=benchmark_return_63,
                        d1_red=d1_red,
                        d_green=d_green,
                        low_sweep=low_sweep,
                        prev_red_bar=prev_red_bar,
                    )
                )
    return matches


def apply_config_to_matches(matches: Sequence[Match], config: BacktestConfig) -> list[TradeSetup]:
    setups: list[TradeSetup] = []
    for match in matches:
        stop_loss = compute_sl(match, config)
        entry_price = compute_entry(stop_loss, config)
        if not (match.signal_bar.low <= entry_price <= match.signal_bar.high):
            continue
        if not math.isfinite(stop_loss) or stop_loss >= entry_price:
            continue
        target_price = compute_target(entry_price, stop_loss, config)
        if target_price <= entry_price:
            continue
        setups.append(
            TradeSetup(
                symbol=match.symbol,
                signal_date=match.signal_date,
                signal_index=match.signal_index,
                signal_bar=match.signal_bar,
                prev_bar=match.prev_bar,
                stock_return_63=match.stock_return_63,
                benchmark_return_63=match.benchmark_return_63,
                d1_red=match.d1_red,
                d_green=match.d_green,
                low_sweep=match.low_sweep,
                entry_price=entry_price,
                stop_loss=stop_loss,
                target_price=target_price,
                target_label=target_label(config.risk_reward),
                risk_reward=config.risk_reward,
                entry_offset_pct=config.entry_offset_pct,
                prev_red_bar=match.prev_red_bar,
                instrument_key=match.signal_bar.instrument_key,
                contract_expiry=match.signal_bar.contract_expiry,
                trading_symbol=match.signal_bar.trading_symbol,
                lot_size=match.signal_bar.lot_size,
            )
        )
    return setups


def _walk_forward_slice(bars: Sequence[Bar], start_index: int, walk_forward_bars: int) -> list[Bar]:
    return list(bars[start_index + 1 : min(len(bars), start_index + walk_forward_bars + 1)])


def _simulate_rr_exit(
    setup: TradeSetup,
    data: BacktestData,
    config: BacktestConfig,
) -> TradeResult | None:
    bars = data.bars_by_symbol.get(setup.symbol, [])
    if setup.signal_index >= len(bars):
        return None
    future_bars = _walk_forward_slice(bars, setup.signal_index, config.walk_forward_bars)
    exit_price = future_bars[-1].close if future_bars else setup.signal_bar.close
    exit_date = future_bars[-1].date if future_bars else setup.signal_date
    exit_reason = "timeout" if future_bars else "no_future_bars"
    bars_held = len(future_bars)

    for offset, bar in enumerate(future_bars, start=1):
        hit_target = bar.high >= setup.target_price
        hit_stop = bar.low <= setup.stop_loss
        if hit_target and hit_stop:
            if bar.close >= bar.open:
                exit_price = setup.target_price
                exit_reason = "target_first_ambiguous_green"
            else:
                exit_price = setup.stop_loss
                exit_reason = "stop_first_ambiguous_red"
            exit_date = bar.date
            bars_held = offset
            break
        if hit_target:
            exit_price = setup.target_price
            exit_reason = "target"
            exit_date = bar.date
            bars_held = offset
            break
        if hit_stop:
            exit_price = setup.stop_loss
            exit_reason = "stop"
            exit_date = bar.date
            bars_held = offset
            break
    gross_return_pct = (exit_price / setup.entry_price) - 1.0
    cost_pct = ((config.fee_bps + config.slippage_bps) * 2.0) / 10_000.0
    return_pct = gross_return_pct - cost_pct
    risk = setup.entry_price - setup.stop_loss
    rr = (exit_price - setup.entry_price) / risk if risk > 0 else float("nan")
    gross_pnl_points = exit_price - setup.entry_price
    net_pnl_points = (return_pct * setup.entry_price) if math.isfinite(return_pct) else float("nan")
    lot_size = max(int(setup.lot_size or 1), 1)
    return TradeResult(
        symbol=setup.symbol,
        signal_date=setup.signal_date,
        exit_date=exit_date,
        entry_price=setup.entry_price,
        stop_loss=setup.stop_loss,
        target_price=setup.target_price,
        exit_price=exit_price,
        exit_reason=exit_reason,
        bars_held=bars_held,
        rr=rr,
        return_pct=return_pct,
        gross_return_pct=gross_return_pct,
        stock_return_63=setup.stock_return_63,
        benchmark_return_63=setup.benchmark_return_63,
        d1_red=setup.d1_red,
        d_green=setup.d_green,
        low_sweep=setup.low_sweep,
        target_label=setup.target_label,
        entry_offset_pct=setup.entry_offset_pct,
        stop_reference_date=setup.prev_red_bar.date if setup.prev_red_bar is not None else None,
        instrument_key=setup.instrument_key,
        contract_expiry=setup.contract_expiry,
        trading_symbol=setup.trading_symbol,
        lot_size=lot_size,
        risk_points=risk,
        gross_pnl_points=gross_pnl_points,
        net_pnl_points=net_pnl_points,
        gross_pnl_rupees=gross_pnl_points * lot_size,
        net_pnl_rupees=net_pnl_points * lot_size,
    )


def simulate_rr_exits(
    setups: Sequence[TradeSetup],
    data: BacktestData,
    config: BacktestConfig,
) -> list[TradeResult]:
    results: list[TradeResult] = []
    for setup in setups:
        result = _simulate_rr_exit(setup, data, config)
        if result is not None:
            results.append(result)
    return results


def compute_stats_rr(results: Sequence[TradeResult]) -> dict[str, Any]:
    rrs = [result.rr for result in results if math.isfinite(result.rr)]
    if not rrs:
        return {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "breakeven": 0,
            "win_rate": 0.0,
            "avg_rr": 0.0,
            "median_rr": 0.0,
            "best_rr": 0.0,
            "worst_rr": 0.0,
            "profit_factor": 0.0,
            "expectancy_rr": 0.0,
        }

    wins = sum(1 for rr in rrs if rr > 0)
    losses = sum(1 for rr in rrs if rr < 0)
    breakeven = len(rrs) - wins - losses
    positive = sum(rr for rr in rrs if rr > 0)
    negative = abs(sum(rr for rr in rrs if rr < 0))
    return {
        "trades": len(rrs),
        "wins": wins,
        "losses": losses,
        "breakeven": breakeven,
        "win_rate": wins / len(rrs),
        "avg_rr": mean(rrs),
        "median_rr": median(rrs),
        "best_rr": max(rrs),
        "worst_rr": min(rrs),
        "profit_factor": positive / negative if negative else float("inf"),
        "expectancy_rr": mean(rrs),
    }


def compute_by_stock(results: Sequence[TradeResult]) -> list[dict[str, Any]]:
    by_symbol: dict[str, list[TradeResult]] = defaultdict(list)
    for result in results:
        by_symbol[result.symbol].append(result)

    summary: list[dict[str, Any]] = []
    for symbol, trades in sorted(by_symbol.items()):
        rrs = [trade.rr for trade in trades if math.isfinite(trade.rr)]
        returns = [trade.return_pct for trade in trades if math.isfinite(trade.return_pct)]
        rupees = [trade.net_pnl_rupees for trade in trades if math.isfinite(trade.net_pnl_rupees)]
        wins = sum(1 for trade in trades if trade.rr > 0)
        summary.append(
            {
                "symbol": symbol,
                "trades": len(trades),
                "win_rate": wins / len(trades) if trades else 0.0,
                "avg_rr": mean(rrs) if rrs else 0.0,
                "avg_return_pct": mean(returns) if returns else 0.0,
                "total_return_pct": sum(returns) if returns else 0.0,
                "best_rr": max(rrs) if rrs else 0.0,
                "worst_rr": min(rrs) if rrs else 0.0,
                "avg_pnl_rupees": mean(rupees) if rupees else 0.0,
                "total_pnl_rupees": sum(rupees) if rupees else 0.0,
            }
        )
    return summary


def simulate_exits(
    setups: Sequence[TradeSetup],
    data: BacktestData,
    config: BacktestConfig,
) -> list[TradeResult]:
    if config.multi_entry:
        return simulate_rr_exits(setups, data, config)

    accepted_results: list[TradeResult] = []
    by_symbol_last_exit_date: dict[str, date] = {}
    for setup in sorted(setups, key=lambda item: (item.symbol, item.signal_date, item.signal_index)):
        last_exit_date = by_symbol_last_exit_date.get(setup.symbol)
        if last_exit_date is not None and setup.signal_date <= last_exit_date:
            continue
        result = _simulate_rr_exit(setup, data, config)
        if result is None:
            continue
        accepted_results.append(result)
        by_symbol_last_exit_date[setup.symbol] = result.exit_date or result.signal_date
    return accepted_results


def compute_stats(results: Sequence[TradeResult]) -> dict[str, Any]:
    returns = [result.return_pct for result in results if math.isfinite(result.return_pct)]
    rrs = [result.rr for result in results if math.isfinite(result.rr)]
    pnl_rupees = [result.net_pnl_rupees for result in results if math.isfinite(result.net_pnl_rupees)]
    if not returns:
        return {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "breakeven": 0,
            "win_rate": 0.0,
            "avg_return_pct": 0.0,
            "median_return_pct": 0.0,
            "best_return_pct": 0.0,
            "worst_return_pct": 0.0,
            "profit_factor": 0.0,
            "expectancy_pct": 0.0,
            "avg_rr": 0.0,
            "max_drawdown_pct": 0.0,
            "equity_end": 1.0,
            "avg_pnl_rupees": 0.0,
            "total_pnl_rupees": 0.0,
        }

    wins = sum(1 for value in returns if value > 0)
    losses = sum(1 for value in returns if value < 0)
    breakeven = len(returns) - wins - losses
    positive = sum(value for value in returns if value > 0)
    negative = abs(sum(value for value in returns if value < 0))
    equity = equity_curve(results)
    drawdown = max((point["drawdown_pct"] for point in equity), default=0.0)
    return {
        "trades": len(returns),
        "wins": wins,
        "losses": losses,
        "breakeven": breakeven,
        "win_rate": wins / len(returns),
        "avg_return_pct": mean(returns),
        "median_return_pct": median(returns),
        "best_return_pct": max(returns),
        "worst_return_pct": min(returns),
        "profit_factor": positive / negative if negative else float("inf"),
        "expectancy_pct": mean(returns),
        "avg_rr": mean(rrs) if rrs else 0.0,
        "max_drawdown_pct": drawdown,
        "equity_end": equity[-1]["equity"] if equity else 1.0,
        "avg_pnl_rupees": mean(pnl_rupees) if pnl_rupees else 0.0,
        "total_pnl_rupees": sum(pnl_rupees) if pnl_rupees else 0.0,
    }


def equity_curve(results: Sequence[TradeResult], start_equity: float = 1.0) -> list[dict[str, Any]]:
    curve: list[dict[str, Any]] = []
    equity = start_equity
    peak = start_equity
    for idx, result in enumerate(sorted(results, key=lambda item: (item.exit_date or item.signal_date, item.signal_date, item.symbol))):
        equity *= 1.0 + result.return_pct
        peak = max(peak, equity)
        drawdown_pct = (peak - equity) / peak if peak > 0 else 0.0
        curve.append(
            {
                "index": idx,
                "date": (result.exit_date or result.signal_date).isoformat(),
                "equity": equity,
                "drawdown_pct": drawdown_pct,
                "return_pct": result.return_pct,
                "symbol": result.symbol,
            }
        )
    return curve


def return_distribution(results: Sequence[TradeResult]) -> dict[str, Any]:
    returns = [result.return_pct for result in results if math.isfinite(result.return_pct)]
    if not returns:
        return {"bins": [], "min": 0.0, "max": 0.0, "mean": 0.0, "median": 0.0}

    lower = min(returns)
    upper = max(returns)
    bucket_count = 12
    if math.isclose(lower, upper):
        bins = [{"label": f"{lower:.2%}", "count": len(returns)}]
    else:
        width = (upper - lower) / bucket_count
        bins = []
        for bucket in range(bucket_count):
            start = lower + bucket * width
            end = start + width
            if bucket == bucket_count - 1:
                count = sum(1 for value in returns if start <= value <= end)
            else:
                count = sum(1 for value in returns if start <= value < end)
            bins.append({"label": f"{start:.2%} to {end:.2%}", "count": count})

    return {
        "bins": bins,
        "min": lower,
        "max": upper,
        "mean": mean(returns),
        "median": median(returns),
    }


def yearly_stats(results: Sequence[TradeResult]) -> list[dict[str, Any]]:
    grouped: dict[int, list[TradeResult]] = defaultdict(list)
    for result in results:
        year = (result.exit_date or result.signal_date).year
        grouped[year].append(result)

    summary: list[dict[str, Any]] = []
    for year, trades in sorted(grouped.items()):
        stats = compute_stats(trades)
        summary.append(
            {
                "year": year,
                "trades": stats["trades"],
                "win_rate": stats["win_rate"],
                "avg_return_pct": stats["avg_return_pct"],
                "avg_rr": stats["avg_rr"],
                "equity_end": stats["equity_end"],
                "max_drawdown_pct": stats["max_drawdown_pct"],
            }
        )
    return summary


def _dedupe_sweep_results(results: Sequence[TradeResult]) -> dict[str, Any]:
    stats = compute_stats(results)
    stats_rr = compute_stats_rr(results)
    return {
        "stats": stats,
        "stats_rr": stats_rr,
        "trades": len(results),
    }


def _sequence_to_jsonable(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if dataclasses.is_dataclass(value):
        return {field.name: _sequence_to_jsonable(getattr(value, field.name)) for field in dataclasses.fields(value)}
    if isinstance(value, dict):
        return {str(key): _sequence_to_jsonable(val) for key, val in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sequence_to_jsonable(item) for item in value]
    return value


def _results_payload(results: Sequence[TradeResult]) -> list[dict[str, Any]]:
    payload = []
    for result in results:
        payload.append(
            {
                "symbol": result.symbol,
                "signal_date": result.signal_date.isoformat(),
                "exit_date": result.exit_date.isoformat() if result.exit_date else None,
                "entry_price": result.entry_price,
                "stop_loss": result.stop_loss,
                "target_price": result.target_price,
                "exit_price": result.exit_price,
                "exit_reason": result.exit_reason,
                "bars_held": result.bars_held,
                "rr": result.rr,
                "return_pct": result.return_pct,
                "gross_return_pct": result.gross_return_pct,
                "stock_return_63": result.stock_return_63,
                "benchmark_return_63": result.benchmark_return_63,
                "d1_red": result.d1_red,
                "d_green": result.d_green,
                "low_sweep": result.low_sweep,
                "target_label": result.target_label,
                "entry_offset_pct": result.entry_offset_pct,
                "stop_reference_date": result.stop_reference_date.isoformat() if result.stop_reference_date else None,
                "instrument_key": result.instrument_key,
                "contract_expiry": result.contract_expiry.isoformat() if result.contract_expiry else None,
                "trading_symbol": result.trading_symbol,
                "lot_size": result.lot_size,
                "risk_points": result.risk_points,
                "gross_pnl_points": result.gross_pnl_points,
                "net_pnl_points": result.net_pnl_points,
                "gross_pnl_rupees": result.gross_pnl_rupees,
                "net_pnl_rupees": result.net_pnl_rupees,
            }
        )
    return payload


def _classify_exit_reason(exit_reason: str) -> str:
    reason = str(exit_reason)
    if "ambiguous" in reason:
        return "ambiguous"
    if reason.startswith("target"):
        return "target"
    if reason.startswith("stop"):
        return "stop"
    if reason in {"timeout", "no_future_bars"}:
        return "timeout"
    return "other"


def _humanize_exit_reason(exit_reason: str) -> str:
    labels = {
        "target": "Target hit",
        "stop": "Stop loss hit",
        "timeout": "Timed out",
        "no_future_bars": "No future bars",
        "target_first_ambiguous_green": "Ambiguous bar resolved as target first",
        "stop_first_ambiguous_red": "Ambiguous bar resolved as stop first",
    }
    return labels.get(exit_reason, exit_reason.replace("_", " ").title())


def _compact_json(value: Any) -> str:
    return json.dumps(_sequence_to_jsonable(value), ensure_ascii=False, separators=(",", ":"))


def _bar_snapshot(bar: Bar | None) -> dict[str, Any] | None:
    if bar is None:
        return None
    return {
        "date": bar.date.isoformat(),
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
    }


def _entry_evidence_detail(signal_bar: Bar, stop_loss: float, entry_price: float, entry_offset_pct: float) -> str:
    within_signal = signal_bar.low <= entry_price <= signal_bar.high
    return (
        f"Stop {stop_loss:.2f} with a {entry_offset_pct:+.2f}% offset projected entry "
        f"to {entry_price:.2f}. Signal range was {signal_bar.low:.2f} to {signal_bar.high:.2f}, "
        f"so the entry {'was' if within_signal else 'was not'} reachable on the signal candle."
    )


def _stop_anchor_detail(stop_bar: Bar | None, stop_loss: float) -> str:
    if stop_bar is None:
        return f"Stop fixed at {stop_loss:.2f} from the most recent prior red candle."
    return (
        f"Prior red candle on {stop_bar.date.isoformat()} printed "
        f"O {stop_bar.open:.2f} / H {stop_bar.high:.2f} / L {stop_bar.low:.2f} / C {stop_bar.close:.2f}. "
        f"Stop stayed fixed to its low at {stop_loss:.2f}."
    )


def _exit_evidence_detail(result: TradeResult, exit_bar: Bar | None, config: BacktestConfig) -> str:
    if exit_bar is None:
        return "Exit bar metadata is unavailable."

    bar_range = (
        f"O {exit_bar.open:.2f} / H {exit_bar.high:.2f} / "
        f"L {exit_bar.low:.2f} / C {exit_bar.close:.2f}"
    )
    exit_date = (result.exit_date or exit_bar.date).isoformat()
    if result.exit_reason == "stop":
        return (
            f"Exit bar on {exit_date} printed {bar_range}. Its low {exit_bar.low:.2f} pierced the stop "
            f"{result.stop_loss:.2f}, so the trade exited intraday from D+1 onward. A close below stop was not required."
        )
    if result.exit_reason == "target":
        return (
            f"Exit bar on {exit_date} printed {bar_range}. Its high {exit_bar.high:.2f} cleared the target "
            f"{result.target_price:.2f}, so the trade exited at the target from D+1 onward."
        )
    if result.exit_reason == "stop_first_ambiguous_red":
        return (
            f"Exit bar on {exit_date} printed {bar_range}. Both stop {result.stop_loss:.2f} and target "
            f"{result.target_price:.2f} sat inside that bar's range, so the backtest resolved the ambiguity as stop first "
            f"because the candle closed red."
        )
    if result.exit_reason == "target_first_ambiguous_green":
        return (
            f"Exit bar on {exit_date} printed {bar_range}. Both stop {result.stop_loss:.2f} and target "
            f"{result.target_price:.2f} sat inside that bar's range, so the backtest resolved the ambiguity as target first "
            f"because the candle closed green."
        )
    if result.exit_reason in {"timeout", "no_future_bars"}:
        return (
            f"No future bar in the next {config.walk_forward_bars} sessions reached stop {result.stop_loss:.2f} "
            f"or target {result.target_price:.2f}. The trade timed out and exited at {result.exit_price:.2f} on {exit_date}."
        )
    return f"Exit bar on {exit_date} printed {bar_range}."


def build_visual_review_payload(
    results: Sequence[TradeResult],
    matches: Sequence[Match],
    data: BacktestData,
    config: BacktestConfig,
    lookback_bars: int = 20,
    lookahead_bars: int = 15,
) -> dict[str, Any]:
    match_map = {(match.symbol, match.signal_date): match for match in matches}
    index_maps = {
        symbol: {bar.date: idx for idx, bar in enumerate(bars)}
        for symbol, bars in data.bars_by_symbol.items()
    }

    reviews: list[dict[str, Any]] = []
    for result in sorted(results, key=lambda item: (item.signal_date, item.symbol), reverse=True):
        bars = data.bars_by_symbol.get(result.symbol, [])
        date_index = index_maps.get(result.symbol, {})
        signal_index = date_index.get(result.signal_date)
        if signal_index is None or not bars:
            continue

        signal_bar = bars[signal_index]
        prev_bar = bars[signal_index - 1] if signal_index > 0 else signal_bar
        inferred_exit_index = min(len(bars) - 1, signal_index + max(result.bars_held, 0))
        if result.exit_date is not None:
            exit_index = date_index.get(result.exit_date, inferred_exit_index)
        else:
            exit_index = inferred_exit_index
        exit_index = max(signal_index, min(len(bars) - 1, exit_index))
        stop_reference_index = date_index.get(result.stop_reference_date) if result.stop_reference_date is not None else None
        stop_reference_bar = bars[stop_reference_index] if stop_reference_index is not None else None
        exit_bar = bars[exit_index] if 0 <= exit_index < len(bars) else None

        window_start = max(0, signal_index - max(1, lookback_bars))
        if stop_reference_index is not None:
            window_start = min(window_start, stop_reference_index)
        window_end = min(len(bars) - 1, exit_index + max(1, lookahead_bars))
        window_bars = bars[window_start : window_end + 1]

        match = match_map.get((result.symbol, result.signal_date))
        stock_meta = data.stock_meta.get(result.symbol)
        excess_return = result.stock_return_63 - result.benchmark_return_63
        risk_points = result.entry_price - result.stop_loss
        sweep_points = prev_bar.low - signal_bar.low
        outcome = _classify_exit_reason(result.exit_reason)

        rationale = [
            {
                "label": "Green signal candle",
                "passed": signal_bar.close > signal_bar.open,
                "detail": f"Close {signal_bar.close:.2f} vs open {signal_bar.open:.2f}",
            },
            {
                "label": "Sweep below D-1 low",
                "passed": signal_bar.low < prev_bar.low,
                "detail": f"Signal low {signal_bar.low:.2f} vs D-1 low {prev_bar.low:.2f} ({sweep_points:.2f} points)",
            },
            {
                "label": "63D relative strength",
                "passed": excess_return > 0 and result.stock_return_63 > 0,
                "detail": (
                    f"{result.symbol} {result.stock_return_63:.2%} vs {data.benchmark_symbol} "
                    f"{result.benchmark_return_63:.2%} (spread {excess_return:.2%})"
                ),
            },
            {
                "label": "Stop anchor",
                "passed": result.stop_reference_date is not None,
                "detail": _stop_anchor_detail(stop_reference_bar, result.stop_loss),
            },
            {
                "label": "Entry from stop anchor",
                "passed": signal_bar.low <= result.entry_price <= signal_bar.high,
                "detail": _entry_evidence_detail(signal_bar, result.stop_loss, result.entry_price, result.entry_offset_pct),
            },
            {
                "label": "D-1 red filter",
                "passed": result.d1_red if config.d1_red_only else True,
                "detail": (
                    f"{'Required' if config.d1_red_only else 'Optional'} | "
                    f"D-1 close {prev_bar.close:.2f} vs open {prev_bar.open:.2f}"
                ),
            },
            {
                "label": "Trade plan",
                "passed": result.entry_price > result.stop_loss and result.target_price > result.entry_price,
                "detail": (
                    f"Entry {result.entry_price:.2f} ({result.entry_offset_pct:+.2f}% from stop), "
                    f"stop {result.stop_loss:.2f}, target {result.target_price:.2f}, "
                    f"risk {risk_points:.2f} points, lot size {result.lot_size}"
                ),
            },
            {
                "label": "Exit bar evidence",
                "passed": outcome == "target",
                "detail": _exit_evidence_detail(result, exit_bar, config),
            },
            {
                "label": "Outcome",
                "passed": outcome == "target",
                "detail": (
                    f"{_humanize_exit_reason(result.exit_reason)} on "
                    f"{(result.exit_date or result.signal_date).isoformat()} after {result.bars_held} bars"
                ),
            },
        ]

        reviews.append(
            {
                "id": f"{result.symbol}|{result.signal_date.isoformat()}",
                "symbol": result.symbol,
                "signal_date": result.signal_date.isoformat(),
                "exit_date": result.exit_date.isoformat() if result.exit_date else None,
                "entry_price": result.entry_price,
                "stop_loss": result.stop_loss,
                "target_price": result.target_price,
                "exit_price": result.exit_price,
                "bars_held": result.bars_held,
                "rr": result.rr,
                "return_pct": result.return_pct,
                "gross_return_pct": result.gross_return_pct,
                "stock_return_63": result.stock_return_63,
                "benchmark_return_63": result.benchmark_return_63,
                "excess_return_63": excess_return,
                "exit_reason": result.exit_reason,
                "exit_label": _humanize_exit_reason(result.exit_reason),
                "outcome": outcome,
                "target_label": result.target_label,
                "entry_offset_pct": result.entry_offset_pct,
                "stop_reference_date": result.stop_reference_date.isoformat() if result.stop_reference_date else None,
                "instrument_key": result.instrument_key,
                "contract_expiry": result.contract_expiry.isoformat() if result.contract_expiry else None,
                "trading_symbol": result.trading_symbol,
                "lot_size": result.lot_size,
                "risk_points": result.risk_points,
                "gross_pnl_points": result.gross_pnl_points,
                "net_pnl_points": result.net_pnl_points,
                "gross_pnl_rupees": result.gross_pnl_rupees,
                "net_pnl_rupees": result.net_pnl_rupees,
                "theme": stock_meta.theme if stock_meta else None,
                "sub_theme": stock_meta.sub_theme if stock_meta else None,
                "reason_summary": (
                    f"Signal day closed green, swept below the prior low, and beat {data.benchmark_symbol} "
                    f"on the 63-day return filter. Stop {result.stop_loss:.2f} and a "
                    f"{result.entry_offset_pct:+.2f}% offset mapped entry from stop to {result.entry_price:.2f} "
                    f"in {result.trading_symbol or result.symbol}, with stop anchored to the prior red candle low "
                    f"and lot size {result.lot_size}."
                ),
                "signal_bar": _bar_snapshot(signal_bar),
                "prev_bar": _bar_snapshot(prev_bar),
                "stop_reference_bar": _bar_snapshot(stop_reference_bar),
                "exit_bar": _bar_snapshot(exit_bar),
                "entry_valid_on_signal": signal_bar.low <= result.entry_price <= signal_bar.high,
                "levels": {
                    "signal_close": signal_bar.close,
                    "entry": result.entry_price,
                    "stop": result.stop_loss,
                    "target": result.target_price,
                    "exit": result.exit_price,
                },
                "markers": {
                    "stop_reference_index": (
                        max(0, stop_reference_index - window_start) if stop_reference_index is not None else None
                    ),
                    "stop_reference_matches_prev": (
                        stop_reference_index is not None and stop_reference_index == max(0, signal_index - 1)
                    ),
                    "prev_index": max(0, signal_index - 1 - window_start),
                    "signal_index": signal_index - window_start,
                    "entry_index": signal_index - window_start,
                    "exit_index": exit_index - window_start,
                    "trade_start_index": signal_index - window_start,
                    "trade_end_index": exit_index - window_start,
                },
                "bars": [
                    [
                        bar.date.isoformat(),
                        bar.open,
                        bar.high,
                        bar.low,
                        bar.close,
                        bar.volume,
                    ]
                    for bar in window_bars
                ],
                "rationale": rationale,
                "match_context": _sequence_to_jsonable(match) if match is not None else None,
            }
        )

    return {
        "count": len(reviews),
        "lookback_bars": int(lookback_bars),
        "lookahead_bars": int(lookahead_bars),
        "trades": reviews,
    }


def _bars_payload(bars: Sequence[Bar]) -> list[dict[str, Any]]:
    return [
        {
            "time": bar.date.isoformat(),
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
        }
        for bar in bars
    ]


def build_analysis_payload(
    data: BacktestData,
    config: BacktestConfig,
    start_date: date | datetime | str | None = None,
    end_date: date | datetime | str | None = None,
) -> dict[str, Any]:
    start_bound = _normalize_bound(start_date)
    end_bound = _normalize_bound(end_date)

    matches = [
        match
        for match in find_pattern_matches(data, config)
        if _date_in_bounds(match.signal_date, start_bound, end_bound)
    ]
    setups = apply_config_to_matches(matches, config)
    results = [
        result
        for result in simulate_exits(setups, data, config)
        if _date_in_bounds(result.signal_date, start_bound, end_bound)
    ]

    summary = compute_stats(results)
    rr_summary = compute_stats_rr(results)
    by_stock = compute_by_stock(results)
    curve = equity_curve(results)
    distribution = return_distribution(results)
    yearly = yearly_stats(results)
    visual_review = build_visual_review_payload(results, matches, data, config)

    visible_symbol_bars = {
        symbol: [
            bar for bar in bars if _date_in_bounds(bar.date, start_bound, end_bound)
        ]
        for symbol, bars in data.bars_by_symbol.items()
    }

    return {
        "summary": summary,
        "rr_summary": rr_summary,
        "by_stock": by_stock,
        "yearly": yearly,
        "curve": curve,
        "distribution": distribution,
        "results": _results_payload(results),
        "match_count": len(matches),
        "match_sample": _sequence_to_jsonable(list(matches[:12])),
        "visual_review": visual_review,
        "symbol_count": len(data.symbols),
        "symbols": list(data.symbols),
        "bars": {
            symbol: _bars_payload(bars)
            for symbol, bars in visible_symbol_bars.items()
            if bars
        },
        "benchmark_bars": _bars_payload(
            [bar for bar in data.benchmark_bars if _date_in_bounds(bar.date, start_bound, end_bound)]
        ),
        "config": _sequence_to_jsonable(config),
        "date_from": start_bound.isoformat() if start_bound else None,
        "date_to": end_bound.isoformat() if end_bound else None,
        "price_source": data.price_source,
    }


def _html_escape_json(value: Any) -> str:
    return html.escape(json.dumps(_sequence_to_jsonable(value), ensure_ascii=False))


def _render_html_report(
    title: str,
    summary: dict[str, Any],
    rr_summary: dict[str, Any],
    by_stock: list[dict[str, Any]],
    yearly: list[dict[str, Any]],
    curve: list[dict[str, Any]],
    distribution: dict[str, Any],
    results: Sequence[TradeResult],
    matches: Sequence[Match],
    config: BacktestConfig,
    visual_review: dict[str, Any],
    extra_sections: dict[str, Any] | None = None,
) -> str:
    tabs = [
        ("overview", "Overview"),
        ("equity", "Equity Curve"),
        ("distribution", "Return Distribution"),
        ("yearly", "Yearly Stats"),
        ("stock", "By Stock"),
        ("review", "Visual Review"),
        ("trades", "Trades"),
        ("config", "Config & Sweep"),
    ]
    results_payload = _results_payload(results)
    match_sample = _sequence_to_jsonable(list(matches[:12]))
    payload = {
        "summary": summary,
        "rr_summary": rr_summary,
        "by_stock": by_stock,
        "yearly": yearly,
        "equity": curve,
        "distribution": distribution,
        "results": results_payload,
        "match_count": len(matches),
        "match_sample": match_sample,
        "config": _sequence_to_jsonable(config),
        "visual_review": visual_review,
        "extra_sections": extra_sections or {},
    }
    chart_equity = {
        "labels": [point["date"] for point in curve],
        "equity": [point["equity"] for point in curve],
        "drawdown": [point["drawdown_pct"] * 100 for point in curve],
    }
    chart_distribution = {
        "labels": [bucket["label"] for bucket in distribution.get("bins", [])],
        "counts": [bucket["count"] for bucket in distribution.get("bins", [])],
    }
    chart_yearly = {
        "labels": [row["year"] for row in yearly],
        "returns": [row["avg_return_pct"] * 100 for row in yearly],
        "win_rate": [row["win_rate"] * 100 for row in yearly],
    }
    chart_stock = {
        "labels": [row["symbol"] for row in by_stock],
        "avg_return": [row["avg_return_pct"] * 100 for row in by_stock],
        "trades": [row["trades"] for row in by_stock],
    }

    tab_buttons = "\n".join(
        f'<button class="tab-button{" active" if idx == 0 else ""}" data-tab="{tab_id}">{label}</button>'
        for idx, (tab_id, label) in enumerate(tabs)
    )

    overview_cards = f"""
      <div class="card-grid">
        <div class="metric-card"><div class="metric-label">Trades</div><div class="metric-value">{summary.get("trades", 0)}</div></div>
        <div class="metric-card"><div class="metric-label">Win Rate</div><div class="metric-value">{summary.get("win_rate", 0.0):.2%}</div></div>
        <div class="metric-card"><div class="metric-label">Avg Return</div><div class="metric-value">{summary.get("avg_return_pct", 0.0):.2%}</div></div>
        <div class="metric-card"><div class="metric-label">Avg RR</div><div class="metric-value">{summary.get("avg_rr", 0.0):.2f}R</div></div>
        <div class="metric-card"><div class="metric-label">Max DD</div><div class="metric-value">{summary.get("max_drawdown_pct", 0.0):.2%}</div></div>
        <div class="metric-card"><div class="metric-label">Profit Factor</div><div class="metric-value">{summary.get("profit_factor", 0.0):.2f}</div></div>
      </div>
    """

    html_results_rows = "\n".join(
        """
        <tr class="trade-row" data-review-id="{symbol}|{signal_date}">
          <td>{symbol}</td>
          <td>{signal_date}</td>
          <td>{exit_date}</td>
          <td>{entry_price:.2f}</td>
          <td>{stop_loss:.2f}</td>
          <td>{target_price:.2f}</td>
          <td>{exit_price:.2f}</td>
          <td>{exit_reason}</td>
          <td>{rr:.2f}</td>
          <td>{return_pct:.2%}</td>
        </tr>
        """.strip().format(**row)
        for row in results_payload
    ) or '<tr><td colspan="10">No trades matched the filters.</td></tr>'

    html_by_stock_rows = "\n".join(
        """
        <tr>
          <td>{symbol}</td>
          <td>{trades}</td>
          <td>{win_rate:.2%}</td>
          <td>{avg_rr:.2f}</td>
          <td>{avg_return_pct:.2%}</td>
          <td>{total_return_pct:.2%}</td>
        </tr>
        """.strip().format(**row)
        for row in by_stock
    ) or '<tr><td colspan="6">No stock breakdown available.</td></tr>'

    html_yearly_rows = "\n".join(
        """
        <tr>
          <td>{year}</td>
          <td>{trades}</td>
          <td>{win_rate:.2%}</td>
          <td>{avg_rr:.2f}</td>
          <td>{avg_return_pct:.2%}</td>
          <td>{max_drawdown_pct:.2%}</td>
        </tr>
        """.strip().format(**row)
        for row in yearly
    ) or '<tr><td colspan="6">No yearly stats available.</td></tr>'

    html_distribution_rows = "\n".join(
        f"<tr><td>{bucket['label']}</td><td>{bucket['count']}</td></tr>" for bucket in distribution.get("bins", [])
    ) or '<tr><td colspan="2">No return distribution available.</td></tr>'

    extra_html = ""
    if extra_sections:
        extra_html = "\n".join(
            f"<section class=\"subpanel\"><h3>{html.escape(str(title))}</h3><pre>{html.escape(json.dumps(value, indent=2, ensure_ascii=False))}</pre></section>"
        for title, value in extra_sections.items()
        )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{html.escape(title)}</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    :root {{
      color-scheme: dark;
      --bg: #0b1020;
      --panel: #121a31;
      --panel-2: #17213d;
      --line: #273454;
      --text: #e7ecff;
      --muted: #96a4c8;
      --accent: #61dafb;
      --accent-2: #85efac;
      --danger: #ff6b8b;
      --warning: #ffbf69;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(97, 218, 251, 0.16), transparent 30%),
        radial-gradient(circle at top right, rgba(133, 239, 172, 0.12), transparent 26%),
        linear-gradient(180deg, #08101f 0%, #0b1020 100%);
      color: var(--text);
      min-height: 100vh;
    }}
    .container {{ max-width: 1560px; margin: 0 auto; padding: 28px; }}
    .hero {{
      display: grid;
      gap: 12px;
      margin-bottom: 18px;
    }}
    .hero h1 {{ margin: 0; font-size: 32px; letter-spacing: -0.02em; }}
    .hero p {{ margin: 0; color: var(--muted); max-width: 1100px; line-height: 1.5; }}
    .tabs {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin: 18px 0;
    }}
    .tab-button {{
      border: 1px solid var(--line);
      background: rgba(18, 26, 49, 0.9);
      color: var(--text);
      border-radius: 999px;
      padding: 10px 14px;
      cursor: pointer;
      transition: transform 120ms ease, border-color 120ms ease, background 120ms ease;
    }}
    .tab-button.active, .tab-button:hover {{
      transform: translateY(-1px);
      border-color: var(--accent);
      background: rgba(25, 37, 68, 0.95);
    }}
    .tab-panel {{
      display: none;
      background: rgba(18, 26, 49, 0.92);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 18px;
      box-shadow: 0 20px 60px rgba(0, 0, 0, 0.2);
    }}
    .tab-panel.active {{ display: block; }}
    .card-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 12px;
      margin-bottom: 18px;
    }}
    .metric-card, .subpanel {{
      background: linear-gradient(180deg, rgba(23, 33, 61, 0.98), rgba(18, 26, 49, 0.98));
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 14px;
    }}
    .metric-label {{ color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em; }}
    .metric-value {{ font-size: 28px; font-weight: 700; margin-top: 6px; }}
    .grid-2 {{ display: grid; grid-template-columns: 2fr 1fr; gap: 16px; }}
    .grid-1 {{ display: grid; grid-template-columns: 1fr; gap: 16px; }}
    .table-wrap {{ overflow: auto; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ padding: 10px 12px; text-align: left; border-bottom: 1px solid var(--line); white-space: nowrap; }}
    th {{ color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em; }}
    .canvas-wrap {{ min-height: 360px; }}
    pre {{
      margin: 0;
      white-space: pre-wrap;
      word-break: break-word;
      color: var(--text);
    }}
    .section-title {{ margin: 0 0 12px 0; font-size: 18px; }}
    .muted {{ color: var(--muted); }}
    .pill {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      border-radius: 999px;
      padding: 6px 10px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.02);
      font-size: 12px;
      line-height: 1;
      color: var(--text);
    }}
    .pill.win {{ border-color: rgba(133, 239, 172, 0.4); color: var(--accent-2); }}
    .pill.loss {{ border-color: rgba(255, 107, 139, 0.45); color: var(--danger); }}
    .pill.timeout {{ border-color: rgba(255, 191, 105, 0.4); color: var(--warning); }}
    .pill.ambiguous {{ border-color: rgba(97, 218, 251, 0.4); color: var(--accent); }}
    .pill.neutral {{ color: var(--muted); }}
    .chip-row {{ display: flex; flex-wrap: wrap; gap: 8px; }}
    .trade-row {{ cursor: pointer; }}
    .trade-row:hover {{ background: rgba(97, 218, 251, 0.05); }}
    .review-layout {{
      display: grid;
      grid-template-columns: 340px 1fr;
      gap: 16px;
      min-height: 760px;
    }}
    .review-sidebar {{
      display: grid;
      grid-template-rows: auto auto 1fr;
      gap: 14px;
      min-height: 760px;
    }}
    .review-controls {{
      display: grid;
      gap: 10px;
    }}
    .review-controls input,
    .review-controls select {{
      width: 100%;
      border-radius: 12px;
      border: 1px solid var(--line);
      background: rgba(8, 16, 31, 0.8);
      color: var(--text);
      padding: 10px 12px;
      outline: none;
    }}
    .trade-list {{
      display: grid;
      gap: 8px;
      overflow: auto;
      align-content: start;
      max-height: 620px;
      padding-right: 4px;
    }}
    .trade-list-item {{
      width: 100%;
      text-align: left;
      border-radius: 14px;
      border: 1px solid var(--line);
      background: rgba(8, 16, 31, 0.55);
      color: var(--text);
      padding: 12px;
      cursor: pointer;
      transition: border-color 120ms ease, transform 120ms ease, background 120ms ease;
    }}
    .trade-list-item:hover,
    .trade-list-item.active {{
      border-color: var(--accent);
      background: rgba(23, 33, 61, 0.95);
      transform: translateY(-1px);
    }}
    .trade-list-top,
    .trade-meta {{
      display: flex;
      justify-content: space-between;
      gap: 10px;
      align-items: center;
    }}
    .trade-list-top {{ margin-bottom: 8px; }}
    .trade-meta {{
      font-size: 12px;
      color: var(--muted);
      margin-top: 4px;
    }}
    .review-main {{
      display: grid;
      gap: 16px;
      align-content: start;
    }}
    .review-header {{
      display: grid;
      gap: 10px;
    }}
    .review-title {{
      margin: 0;
      font-size: 24px;
      letter-spacing: -0.02em;
    }}
    .review-chart-panel {{
      position: relative;
      overflow: hidden;
    }}
    .review-legend {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 10px;
    }}
    .legend-item {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }}
    .legend-swatch {{
      width: 12px;
      height: 12px;
      border-radius: 3px;
      display: inline-block;
    }}
    #reviewChart {{
      width: 100%;
      height: 560px;
      display: block;
      border-radius: 14px;
      background:
        linear-gradient(180deg, rgba(9, 16, 24, 0.96), rgba(10, 16, 28, 0.98)),
        radial-gradient(circle at top right, rgba(97, 218, 251, 0.08), transparent 30%);
      border: 1px solid rgba(39, 52, 84, 0.55);
    }}
    .review-tooltip {{
      position: absolute;
      min-width: 180px;
      pointer-events: none;
      display: none;
      padding: 10px 12px;
      border-radius: 12px;
      border: 1px solid var(--line);
      background: rgba(8, 16, 31, 0.94);
      box-shadow: 0 16px 40px rgba(0, 0, 0, 0.35);
      color: var(--text);
      font-size: 12px;
      z-index: 2;
    }}
    .review-tooltip strong {{
      display: block;
      margin-bottom: 6px;
      font-size: 13px;
    }}
    .rationale-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 12px;
    }}
    .rationale-item {{
      border-radius: 16px;
      border: 1px solid var(--line);
      background: rgba(8, 16, 31, 0.6);
      padding: 14px;
      display: grid;
      gap: 8px;
    }}
    .rationale-item.pass {{ border-color: rgba(133, 239, 172, 0.35); }}
    .rationale-item.fail {{ border-color: rgba(255, 107, 139, 0.4); }}
    .rationale-label {{
      font-size: 13px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
    }}
    .rationale-detail {{
      font-size: 14px;
      line-height: 1.5;
      color: var(--text);
    }}
    .empty-state {{
      border: 1px dashed var(--line);
      border-radius: 16px;
      padding: 28px;
      text-align: center;
      color: var(--muted);
      background: rgba(8, 16, 31, 0.35);
    }}
    @media (max-width: 1024px) {{
      .grid-2 {{ grid-template-columns: 1fr; }}
      .container {{ padding: 16px; }}
      .review-layout {{ grid-template-columns: 1fr; }}
      .review-sidebar {{ min-height: 0; }}
      .trade-list {{ max-height: 280px; }}
    }}
  </style>
</head>
<body>
  <div class="container">
    <header class="hero">
      <h1>{html.escape(title)}</h1>
      <p>Dark, self-contained report for liquidity sweep backtests. The simulator applies the signal filter on D, starts the trade on the signal day, walks forward {config.walk_forward_bars} bars, and resolves same-bar SL/target collisions with candle colour.</p>
    </header>
    <div class="tabs">{tab_buttons}</div>

    <section class="tab-panel active" id="overview">
      {overview_cards}
      <div class="grid-2">
        <div class="subpanel canvas-wrap">
          <h2 class="section-title">Equity Curve</h2>
          <canvas id="equityChart"></canvas>
        </div>
        <div class="subpanel">
          <h2 class="section-title">Summary</h2>
          <pre>{html.escape(json.dumps(payload["summary"], indent=2, ensure_ascii=False))}</pre>
        </div>
      </div>
    </section>

    <section class="tab-panel" id="equity">
      <div class="grid-1">
        <div class="subpanel canvas-wrap"><canvas id="equityChart2"></canvas></div>
        <div class="subpanel"><pre>{html.escape(json.dumps(chart_equity, indent=2, ensure_ascii=False))}</pre></div>
      </div>
    </section>

    <section class="tab-panel" id="distribution">
      <div class="grid-2">
        <div class="subpanel canvas-wrap"><canvas id="distributionChart"></canvas></div>
        <div class="subpanel table-wrap">
          <table>
            <thead><tr><th>Bucket</th><th>Count</th></tr></thead>
            <tbody>{html_distribution_rows}</tbody>
          </table>
        </div>
      </div>
    </section>

    <section class="tab-panel" id="yearly">
      <div class="grid-2">
        <div class="subpanel canvas-wrap"><canvas id="yearlyChart"></canvas></div>
        <div class="subpanel table-wrap">
          <table>
            <thead><tr><th>Year</th><th>Trades</th><th>Win Rate</th><th>Avg RR</th><th>Avg Return</th><th>Max DD</th></tr></thead>
            <tbody>{html_yearly_rows}</tbody>
          </table>
        </div>
      </div>
    </section>

    <section class="tab-panel" id="stock">
      <div class="grid-2">
        <div class="subpanel canvas-wrap"><canvas id="stockChart"></canvas></div>
        <div class="subpanel table-wrap">
          <table>
            <thead><tr><th>Symbol</th><th>Trades</th><th>Win Rate</th><th>Avg RR</th><th>Avg Return</th><th>Total Return</th></tr></thead>
            <tbody>{html_by_stock_rows}</tbody>
          </table>
        </div>
      </div>
    </section>

    <section class="tab-panel" id="review">
      <div class="review-layout">
        <div class="subpanel review-sidebar">
          <div>
            <h2 class="section-title">Visual Trade Review</h2>
            <p class="muted">Inspect one trade at a time with real candlesticks, the exact trigger rationale, and clear entry, stop, target, and exit overlays.</p>
          </div>
          <div class="review-controls">
            <input id="reviewSearch" type="search" placeholder="Search symbol or date" />
            <select id="reviewOutcome">
              <option value="all">All outcomes</option>
              <option value="target">Targets</option>
              <option value="stop">Stops</option>
              <option value="timeout">Timeouts</option>
              <option value="ambiguous">Ambiguous bars</option>
            </select>
            <select id="reviewSort">
              <option value="date_desc">Newest first</option>
              <option value="return_desc">Best return first</option>
              <option value="return_asc">Worst return first</option>
              <option value="rr_desc">Highest RR first</option>
              <option value="rs_desc">Highest RS spread first</option>
            </select>
            <div class="chip-row">
              <span class="pill neutral" id="reviewTradeCount">0 trades</span>
              <span class="pill neutral">Window {visual_review.get("lookback_bars", 0)} bars before / {visual_review.get("lookahead_bars", 0)} bars after</span>
            </div>
          </div>
          <div class="trade-list" id="reviewTradeList"></div>
        </div>
        <div class="review-main">
          <div class="subpanel review-header">
            <div>
              <h2 class="review-title" id="reviewTitle">No trade selected</h2>
              <p class="muted" id="reviewSummary">Choose a trade to inspect the setup visually.</p>
            </div>
            <div class="chip-row" id="reviewChips"></div>
          </div>
          <div class="subpanel review-chart-panel">
            <div class="review-legend">
              <span class="legend-item"><span class="legend-swatch" style="background:#85efac;"></span> Bull candle</span>
              <span class="legend-item"><span class="legend-swatch" style="background:#ff6b8b;"></span> Bear candle</span>
              <span class="legend-item"><span class="legend-swatch" style="background:#61dafb;"></span> Entry and setup levels</span>
              <span class="legend-item"><span class="legend-swatch" style="background:#ff9f68;"></span> Stop-reference candle</span>
              <span class="legend-item"><span class="legend-swatch" style="background:#ffbf69;"></span> Exit marker</span>
            </div>
            <canvas id="reviewChart"></canvas>
            <div class="review-tooltip" id="reviewTooltip"></div>
          </div>
          <div class="rationale-grid" id="reviewRationale">
            <div class="empty-state">Visual trade review will appear here once a trade is selected.</div>
          </div>
        </div>
      </div>
    </section>

    <section class="tab-panel" id="trades">
      <div class="subpanel table-wrap">
        <h2 class="section-title">Trade Log</h2>
        <p class="muted">Click any trade row to jump straight into the visual review tab for that setup.</p>
        <table>
          <thead>
            <tr>
              <th>Symbol</th><th>Signal</th><th>Exit</th><th>Entry</th><th>SL</th><th>Target</th><th>Exit Px</th><th>Reason</th><th>RR</th><th>Return</th>
            </tr>
          </thead>
          <tbody>{html_results_rows}</tbody>
        </table>
      </div>
    </section>

    <section class="tab-panel" id="config">
      <div class="grid-2">
        <div class="subpanel">
          <h2 class="section-title">Configuration</h2>
          <pre>{html.escape(json.dumps(payload["config"], indent=2, ensure_ascii=False))}</pre>
        </div>
        <div class="subpanel">
          <h2 class="section-title">Pattern Match Sample</h2>
          <p class="muted">{len(matches)} raw matches before trade simulation. Showing the first {len(match_sample)} for context.</p>
          <pre>{html.escape(json.dumps(match_sample, indent=2, ensure_ascii=False))}</pre>
        </div>
      </div>
      {extra_html}
    </section>
  </div>

  <script>
    const report = {_compact_json(payload)};
    const chartEquity = {_compact_json(chart_equity)};
    const chartDistribution = {_compact_json(chart_distribution)};
    const chartYearly = {_compact_json(chart_yearly)};
    const chartStock = {_compact_json(chart_stock)};
    const tabButtons = document.querySelectorAll('.tab-button');
    const tabPanels = document.querySelectorAll('.tab-panel');
    tabButtons.forEach((button) => {{
      button.addEventListener('click', () => {{
        tabButtons.forEach((item) => item.classList.remove('active'));
        tabPanels.forEach((panel) => panel.classList.remove('active'));
        button.classList.add('active');
        document.getElementById(button.dataset.tab).classList.add('active');
      }});
    }});
    const palette = {{
      accent: 'rgba(97, 218, 251, 1)',
      accentFill: 'rgba(97, 218, 251, 0.15)',
      green: 'rgba(133, 239, 172, 1)',
      greenFill: 'rgba(133, 239, 172, 0.16)',
      danger: 'rgba(255, 107, 139, 1)',
      dangerFill: 'rgba(255, 107, 139, 0.16)',
      warning: 'rgba(255, 191, 105, 1)',
      warningFill: 'rgba(255, 191, 105, 0.14)',
    }};

    const chartOptions = {{
      responsive: true,
      maintainAspectRatio: false,
      plugins: {{
        legend: {{ labels: {{ color: '#e7ecff' }} }},
      }},
      scales: {{
        x: {{ ticks: {{ color: '#96a4c8' }}, grid: {{ color: 'rgba(39, 52, 84, 0.4)' }} }},
        y: {{ ticks: {{ color: '#96a4c8' }}, grid: {{ color: 'rgba(39, 52, 84, 0.4)' }} }},
      }},
    }};

    if (document.getElementById('equityChart')) {{
      new Chart(document.getElementById('equityChart'), {{
        type: 'line',
        data: {{
          labels: chartEquity.labels,
          datasets: [
            {{
              label: 'Equity',
              data: chartEquity.equity,
              borderColor: palette.accent,
              backgroundColor: palette.accentFill,
              tension: 0.2,
              fill: true,
            }},
            {{
              label: 'Drawdown %',
              data: chartEquity.drawdown,
              borderColor: palette.danger,
              backgroundColor: palette.dangerFill,
              tension: 0.2,
              yAxisID: 'y1',
            }},
          ],
        }},
        options: {{
          ...chartOptions,
          scales: {{
            x: chartOptions.scales.x,
            y: chartOptions.scales.y,
            y1: {{
              position: 'right',
              ticks: {{ color: '#96a4c8' }},
              grid: {{ drawOnChartArea: false }},
            }},
          }},
        }},
      }});
    }}
    if (document.getElementById('equityChart2')) {{
      new Chart(document.getElementById('equityChart2'), {{
        type: 'line',
        data: {{
          labels: chartEquity.labels,
          datasets: [{{
            label: 'Equity',
            data: chartEquity.equity,
            borderColor: palette.accent,
            backgroundColor: palette.accentFill,
            tension: 0.2,
            fill: true,
          }}],
        }},
        options: chartOptions,
      }});
    }}
    if (document.getElementById('distributionChart')) {{
      new Chart(document.getElementById('distributionChart'), {{
        type: 'bar',
        data: {{
          labels: chartDistribution.labels,
          datasets: [{{
            label: 'Trades',
            data: chartDistribution.counts,
            backgroundColor: palette.greenFill,
            borderColor: palette.green,
            borderWidth: 1,
          }}],
        }},
        options: chartOptions,
      }});
    }}
    if (document.getElementById('yearlyChart')) {{
      new Chart(document.getElementById('yearlyChart'), {{
        type: 'bar',
        data: {{
          labels: chartYearly.labels,
          datasets: [
            {{
              label: 'Avg Return %',
              data: chartYearly.returns,
              backgroundColor: palette.accentFill,
              borderColor: palette.accent,
              borderWidth: 1,
            }},
            {{
              label: 'Win Rate %',
              data: chartYearly.win_rate,
              backgroundColor: palette.greenFill,
              borderColor: palette.green,
              borderWidth: 1,
            }},
          ],
        }},
        options: chartOptions,
      }});
    }}
    if (document.getElementById('stockChart')) {{
      new Chart(document.getElementById('stockChart'), {{
        type: 'bar',
        data: {{
          labels: chartStock.labels,
          datasets: [
            {{
              label: 'Avg Return %',
              data: chartStock.avg_return,
              backgroundColor: palette.accentFill,
              borderColor: palette.accent,
              borderWidth: 1,
            }},
            {{
              label: 'Trades',
              data: chartStock.trades,
              backgroundColor: palette.greenFill,
              borderColor: palette.green,
              borderWidth: 1,
            }},
          ],
        }},
        options: chartOptions,
      }});
    }}

    function escapeHtml(value) {{
      return String(value ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
    }}

    function formatPct(value) {{
      const number = Number(value ?? 0);
      return `${{number >= 0 ? '+' : ''}}${{(number * 100).toFixed(2)}}%`;
    }}

    function formatRR(value) {{
      const number = Number(value ?? 0);
      return `${{number >= 0 ? '+' : ''}}${{number.toFixed(2)}}R`;
    }}

    function formatPx(value) {{
      return Number(value ?? 0).toFixed(2);
    }}

    function formatBarOhlc(bar) {{
      if (!bar) {{
        return '—';
      }}
      return `O ${{formatPx(bar.open)}} / H ${{formatPx(bar.high)}} / L ${{formatPx(bar.low)}} / C ${{formatPx(bar.close)}}`;
    }}

    function outcomeClass(outcome) {{
      if (outcome === 'target') return 'win';
      if (outcome === 'stop') return 'loss';
      if (outcome === 'timeout') return 'timeout';
      if (outcome === 'ambiguous') return 'ambiguous';
      return 'neutral';
    }}

    function outcomeLabel(outcome) {{
      if (outcome === 'target') return 'Target';
      if (outcome === 'stop') return 'Stop';
      if (outcome === 'timeout') return 'Timeout';
      if (outcome === 'ambiguous') return 'Ambiguous';
      return 'Other';
    }}

    const visualReview = report.visual_review || {{ trades: [], count: 0 }};
    const reviewState = {{
      query: '',
      outcome: 'all',
      sort: 'date_desc',
      selectedId: visualReview.trades.length ? visualReview.trades[0].id : null,
      hoverIndex: null,
    }};

    const reviewSearch = document.getElementById('reviewSearch');
    const reviewOutcome = document.getElementById('reviewOutcome');
    const reviewSort = document.getElementById('reviewSort');
    const reviewTradeList = document.getElementById('reviewTradeList');
    const reviewTradeCount = document.getElementById('reviewTradeCount');
    const reviewTitle = document.getElementById('reviewTitle');
    const reviewSummary = document.getElementById('reviewSummary');
    const reviewChips = document.getElementById('reviewChips');
    const reviewRationale = document.getElementById('reviewRationale');
    const reviewChart = document.getElementById('reviewChart');
    const reviewTooltip = document.getElementById('reviewTooltip');

    function filteredReviewTrades() {{
      const query = reviewState.query.trim().toLowerCase();
      let trades = visualReview.trades.filter((trade) => {{
        const queryOk = !query || `${{trade.symbol}} ${{trade.signal_date}} ${{trade.exit_label}}`.toLowerCase().includes(query);
        const outcomeOk = reviewState.outcome === 'all' || trade.outcome === reviewState.outcome;
        return queryOk && outcomeOk;
      }});

      const sorters = {{
        date_desc: (left, right) => right.signal_date.localeCompare(left.signal_date),
        return_desc: (left, right) => Number(right.return_pct) - Number(left.return_pct),
        return_asc: (left, right) => Number(left.return_pct) - Number(right.return_pct),
        rr_desc: (left, right) => Number(right.rr) - Number(left.rr),
        rs_desc: (left, right) => Number(right.excess_return_63) - Number(left.excess_return_63),
      }};
      trades = [...trades].sort(sorters[reviewState.sort] || sorters.date_desc);
      return trades;
    }}

    function ensureSelectedTrade(trades) {{
      if (!trades.length) {{
        reviewState.selectedId = null;
        return null;
      }}
      if (!trades.some((trade) => trade.id === reviewState.selectedId)) {{
        reviewState.selectedId = trades[0].id;
      }}
      return trades.find((trade) => trade.id === reviewState.selectedId) || trades[0];
    }}

    function renderReviewTradeList(trades) {{
      reviewTradeCount.textContent = `${{trades.length}} trades`;
      if (!trades.length) {{
        reviewTradeList.innerHTML = '<div class="empty-state">No trades matched the current review filters.</div>';
        return;
      }}
      reviewTradeList.innerHTML = trades.map((trade) => `
        <button class="trade-list-item${{trade.id === reviewState.selectedId ? ' active' : ''}}" data-review-id="${{trade.id}}">
          <div class="trade-list-top">
            <strong>${{escapeHtml(trade.symbol)}}</strong>
            <span class="pill ${{outcomeClass(trade.outcome)}}">${{escapeHtml(outcomeLabel(trade.outcome))}}</span>
          </div>
          <div class="trade-meta"><span>${{escapeHtml(trade.signal_date)}}</span><span>${{formatPct(trade.return_pct)}}</span></div>
          <div class="trade-meta"><span>${{formatRR(trade.rr)}}</span><span>RS ${{formatPct(trade.excess_return_63)}}</span></div>
        </button>
      `).join('');
      reviewTradeList.querySelectorAll('[data-review-id]').forEach((button) => {{
        button.addEventListener('click', () => {{
          reviewState.selectedId = button.dataset.reviewId;
          reviewState.hoverIndex = null;
          renderReviewExperience();
        }});
      }});
    }}

    function renderReviewDetail(trade) {{
      if (!trade) {{
        reviewTitle.textContent = 'No trade selected';
        reviewSummary.textContent = 'Choose a trade to inspect the setup visually.';
        reviewChips.innerHTML = '';
        reviewRationale.innerHTML = '<div class="empty-state">Visual trade review will appear here once a trade is selected.</div>';
        drawTradeReviewChart(null);
        return;
      }}

      reviewTitle.textContent = `${{trade.symbol}} • ${{trade.signal_date}}`;
      reviewSummary.textContent = trade.reason_summary;
      reviewChips.innerHTML = [
        `<span class="pill ${{outcomeClass(trade.outcome)}}">${{escapeHtml(trade.exit_label)}}</span>`,
        `<span class="pill neutral">Return ${{formatPct(trade.return_pct)}}</span>`,
        `<span class="pill neutral">RR ${{formatRR(trade.rr)}}</span>`,
        `<span class="pill neutral">Signal close ${{formatPx(trade.signal_bar?.close)}}</span>`,
        `<span class="pill neutral">Entry ${{formatPx(trade.entry_price)}}</span>`,
        `<span class="pill neutral">Stop ${{formatPx(trade.stop_loss)}}</span>`,
        `<span class="pill neutral">Target ${{formatPx(trade.target_price)}}</span>`,
        `<span class="pill neutral">Bars held ${{trade.bars_held}}</span>`,
        `<span class="pill neutral">63D spread ${{formatPct(trade.excess_return_63)}}</span>`,
      ].concat(trade.theme ? [`<span class="pill neutral">${{escapeHtml(trade.theme)}}${{trade.sub_theme ? ` / ${{escapeHtml(trade.sub_theme)}}` : ''}}</span>`] : []).join('');

      const setupCard = `
          <div class="rationale-item pass">
          <div class="rationale-label">Setup Snapshot</div>
          <div class="rationale-detail">
            Signal date: <strong>${{escapeHtml(trade.signal_date)}}</strong><br />
            Exit date: <strong>${{escapeHtml(trade.exit_date || trade.signal_date)}}</strong><br />
            Stop / Entry: <strong>${{formatPx(trade.stop_loss)}} / ${{formatPx(trade.entry_price)}}</strong><br />
            Entry / Stop / Target: <strong>${{formatPx(trade.entry_price)}} / ${{formatPx(trade.stop_loss)}} / ${{formatPx(trade.target_price)}}</strong><br />
            Stop reference: <strong>${{escapeHtml(trade.stop_reference_date || '—')}}</strong>${{trade.stop_reference_bar ? ` • ${{escapeHtml(formatBarOhlc(trade.stop_reference_bar))}}` : ''}}<br />
            Exit: <strong>${{formatPx(trade.exit_price)}}</strong> via <strong>${{escapeHtml(trade.exit_label)}}</strong>${{trade.exit_bar ? ` • ${{escapeHtml(formatBarOhlc(trade.exit_bar))}}` : ''}}<br />
            Stop rule: <strong>D+1 onward, low ≤ stop; close below stop is not required.</strong>
          </div>
        </div>
      `;

      reviewRationale.innerHTML = [
        setupCard,
        ...trade.rationale.map((item) => `
          <div class="rationale-item${{item.passed ? ' pass' : ' fail'}}">
            <div class="rationale-label">${{escapeHtml(item.label)}}</div>
            <div class="rationale-detail">${{escapeHtml(item.detail)}}</div>
          </div>
        `),
      ].join('');

      drawTradeReviewChart(trade);
    }}

    function drawTradeReviewChart(trade) {{
      if (!reviewChart) {{
        return;
      }}

      const ctx = reviewChart.getContext('2d');
      const width = reviewChart.clientWidth || reviewChart.parentElement.clientWidth || 900;
      const height = reviewChart.clientHeight || 560;
      const dpr = window.devicePixelRatio || 1;
      reviewChart.width = Math.floor(width * dpr);
      reviewChart.height = Math.floor(height * dpr);
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, width, height);

      if (!trade || !trade.bars.length) {{
        ctx.fillStyle = '#96a4c8';
        ctx.font = '14px Inter, sans-serif';
        ctx.fillText('No chart data available for this trade.', 24, 40);
        reviewTooltip.style.display = 'none';
        return;
      }}

      const leftPad = 62;
      const rightPad = 88;
      const topPad = 24;
      const priceBottom = 390;
      const volumeTop = 430;
      const volumeBottom = height - 36;
      const plotWidth = width - leftPad - rightPad;
      const bars = trade.bars;
      const priceValues = bars.flatMap((bar) => [Number(bar[2]), Number(bar[3])]).concat([
        Number(trade.levels.signal_close),
        Number(trade.levels.entry),
        Number(trade.levels.stop),
        Number(trade.levels.target),
        Number(trade.levels.exit),
      ]);
      const rawMin = Math.min(...priceValues);
      const rawMax = Math.max(...priceValues);
      const pricePadding = Math.max((rawMax - rawMin) * 0.08, rawMax * 0.015, 1);
      const priceMin = rawMin - pricePadding;
      const priceMax = rawMax + pricePadding;
      const priceRange = Math.max(priceMax - priceMin, 1);
      const step = plotWidth / Math.max(bars.length, 1);
      const candleWidth = Math.max(4, Math.min(12, step * 0.56));
      const volumeMax = Math.max(...bars.map((bar) => Number(bar[5] || 0)), 1);

      const xForIndex = (index) => leftPad + (index * step) + step / 2;
      const yForPrice = (price) => topPad + ((priceMax - price) / priceRange) * (priceBottom - topPad);
      const yForVolume = (volume) => volumeBottom - ((Number(volume) / volumeMax) * (volumeBottom - volumeTop));

      ctx.fillStyle = 'rgba(8, 16, 31, 0.55)';
      ctx.fillRect(0, 0, width, height);

      const tradeStartX = xForIndex(trade.markers.trade_start_index) - step / 2;
      const tradeEndX = xForIndex(trade.markers.trade_end_index) + step / 2;
      ctx.fillStyle = 'rgba(97, 218, 251, 0.08)';
      ctx.fillRect(tradeStartX, topPad, Math.max(step, tradeEndX - tradeStartX), priceBottom - topPad);

      const gridLines = 5;
      ctx.strokeStyle = 'rgba(39, 52, 84, 0.55)';
      ctx.lineWidth = 1;
      ctx.font = '12px Inter, sans-serif';
      ctx.fillStyle = '#96a4c8';
      for (let line = 0; line <= gridLines; line += 1) {{
        const ratio = line / gridLines;
        const y = topPad + ratio * (priceBottom - topPad);
        const value = priceMax - ratio * priceRange;
        ctx.beginPath();
        ctx.moveTo(leftPad, y);
        ctx.lineTo(width - rightPad, y);
        ctx.stroke();
        ctx.fillText(value.toFixed(2), width - rightPad + 10, y + 4);
      }}

      const dateStep = Math.max(1, Math.floor(bars.length / 6));
      bars.forEach((bar, index) => {{
        if (index % dateStep !== 0 && index !== bars.length - 1) {{
          return;
        }}
        const x = xForIndex(index);
        ctx.fillText(String(bar[0]).slice(5), Math.max(leftPad, x - 16), volumeBottom + 18);
      }});

      function drawLevel(price, color, label) {{
        const y = yForPrice(price);
        ctx.save();
        ctx.strokeStyle = color;
        ctx.setLineDash([6, 6]);
        ctx.beginPath();
        ctx.moveTo(leftPad, y);
        ctx.lineTo(width - rightPad, y);
        ctx.stroke();
        ctx.restore();

        ctx.font = '11px Inter, sans-serif';
        const labelWidth = Math.max(72, Math.ceil(ctx.measureText(label).width) + 18);
        ctx.fillStyle = color;
        ctx.fillRect(width - rightPad + 8, y - 10, labelWidth, 20);
        ctx.fillStyle = '#08101f';
        ctx.fillText(label, width - rightPad + 14, y + 4);
      }}

      const signalCloseMatchesEntry = Math.abs(Number(trade.levels.signal_close) - Number(trade.levels.entry)) < 1e-8;
      drawLevel(trade.levels.entry, '#61dafb', `${{signalCloseMatchesEntry ? 'Entry' : `Entry ${{formatPx(trade.levels.entry)}}`}}`);
      if (!signalCloseMatchesEntry) {{
        drawLevel(trade.levels.signal_close, '#9fe9ff', `Sig close ${{formatPx(trade.levels.signal_close)}}`);
      }}
      drawLevel(trade.levels.stop, '#ff6b8b', `Stop ${{formatPx(trade.levels.stop)}}`);
      drawLevel(trade.levels.target, '#85efac', `Target ${{formatPx(trade.levels.target)}}`);
      drawLevel(trade.levels.exit, '#ffbf69', `Exit ${{formatPx(trade.levels.exit)}}`);

      function drawMarker(index, color, label) {{
        const x = xForIndex(index);
        ctx.save();
        ctx.strokeStyle = color;
        ctx.beginPath();
        ctx.moveTo(x, topPad);
        ctx.lineTo(x, priceBottom);
        ctx.stroke();
        ctx.font = '11px Inter, sans-serif';
        const labelWidth = Math.max(56, Math.ceil(ctx.measureText(label).width) + 16);
        ctx.fillStyle = color;
        ctx.fillRect(x - labelWidth / 2, topPad + 6, labelWidth, 18);
        ctx.fillStyle = '#08101f';
        ctx.fillText(label, x - labelWidth / 2 + 8, topPad + 19);
        ctx.restore();
      }}

      if (trade.markers.stop_reference_matches_prev) {{
        drawMarker(trade.markers.prev_index, '#ff9f68', 'D-1 + SL ref');
      }} else {{
        if (trade.markers.stop_reference_index !== null && trade.markers.stop_reference_index !== undefined) {{
          drawMarker(trade.markers.stop_reference_index, '#ff9f68', 'SL ref');
        }}
        drawMarker(trade.markers.prev_index, 'rgba(150, 164, 200, 0.9)', 'D-1 sweep');
      }}
      drawMarker(trade.markers.signal_index, '#61dafb', 'Signal');
      drawMarker(trade.markers.exit_index, '#ffbf69', 'Exit');

      const hoverIndex = Number.isInteger(reviewState.hoverIndex) ? reviewState.hoverIndex : null;
      bars.forEach((bar, index) => {{
        const [barDate, open, high, low, close, volume] = bar;
        const x = xForIndex(index);
        const openY = yForPrice(Number(open));
        const closeY = yForPrice(Number(close));
        const highY = yForPrice(Number(high));
        const lowY = yForPrice(Number(low));
        const isGreen = Number(close) >= Number(open);
        const color = isGreen ? '#85efac' : '#ff6b8b';
        const fill = isGreen ? 'rgba(133, 239, 172, 0.85)' : 'rgba(255, 107, 139, 0.85)';

        ctx.strokeStyle = color;
        ctx.lineWidth = hoverIndex === index ? 2 : 1.2;
        ctx.beginPath();
        ctx.moveTo(x, highY);
        ctx.lineTo(x, lowY);
        ctx.stroke();

        const bodyTop = Math.min(openY, closeY);
        const bodyHeight = Math.max(2, Math.abs(closeY - openY));
        ctx.fillStyle = fill;
        ctx.fillRect(x - candleWidth / 2, bodyTop, candleWidth, bodyHeight);
        ctx.strokeRect(x - candleWidth / 2, bodyTop, candleWidth, bodyHeight);

        ctx.fillStyle = isGreen ? 'rgba(133, 239, 172, 0.25)' : 'rgba(255, 107, 139, 0.25)';
        const volumeY = yForVolume(volume);
        ctx.fillRect(x - candleWidth / 2, volumeY, candleWidth, volumeBottom - volumeY);
      }});

      if (hoverIndex !== null && hoverIndex >= 0 && hoverIndex < bars.length) {{
        const bar = bars[hoverIndex];
        const x = xForIndex(hoverIndex);
        ctx.save();
        ctx.strokeStyle = 'rgba(255, 255, 255, 0.18)';
        ctx.setLineDash([4, 6]);
        ctx.beginPath();
        ctx.moveTo(x, topPad);
        ctx.lineTo(x, volumeBottom);
        ctx.stroke();
        ctx.restore();

        const rect = reviewChart.getBoundingClientRect();
        reviewTooltip.style.display = 'block';
        reviewTooltip.style.left = `${{Math.min(width - 210, Math.max(12, x + 14))}}px`;
        reviewTooltip.style.top = `${{Math.max(18, yForPrice(Number(bar[2])) - 26)}}px`;
        reviewTooltip.innerHTML = `
          <strong>${{escapeHtml(bar[0])}}</strong>
          O: ${{formatPx(bar[1])}}<br />
          H: ${{formatPx(bar[2])}}<br />
          L: ${{formatPx(bar[3])}}<br />
          C: ${{formatPx(bar[4])}}<br />
          Vol: ${{Number(bar[5] || 0).toLocaleString()}}
        `;
      }} else {{
        reviewTooltip.style.display = 'none';
      }}
    }}

    function renderReviewExperience() {{
      const trades = filteredReviewTrades();
      const selectedTrade = ensureSelectedTrade(trades);
      renderReviewTradeList(trades);
      renderReviewDetail(selectedTrade);
    }}

    if (reviewSearch) {{
      reviewSearch.addEventListener('input', (event) => {{
        reviewState.query = event.target.value || '';
        reviewState.hoverIndex = null;
        renderReviewExperience();
      }});
    }}
    if (reviewOutcome) {{
      reviewOutcome.addEventListener('change', (event) => {{
        reviewState.outcome = event.target.value || 'all';
        reviewState.hoverIndex = null;
        renderReviewExperience();
      }});
    }}
    if (reviewSort) {{
      reviewSort.addEventListener('change', (event) => {{
        reviewState.sort = event.target.value || 'date_desc';
        reviewState.hoverIndex = null;
        renderReviewExperience();
      }});
    }}

    if (reviewChart) {{
      reviewChart.addEventListener('mousemove', (event) => {{
        const trades = filteredReviewTrades();
        const trade = ensureSelectedTrade(trades);
        if (!trade || !trade.bars.length) {{
          return;
        }}
        const rect = reviewChart.getBoundingClientRect();
        const x = event.clientX - rect.left;
        const leftPad = 62;
        const rightPad = 88;
        const plotWidth = rect.width - leftPad - rightPad;
        if (x < leftPad || x > rect.width - rightPad) {{
          reviewState.hoverIndex = null;
          drawTradeReviewChart(trade);
          return;
        }}
        const index = Math.max(0, Math.min(trade.bars.length - 1, Math.floor(((x - leftPad) / plotWidth) * trade.bars.length)));
        reviewState.hoverIndex = index;
        drawTradeReviewChart(trade);
      }});
      reviewChart.addEventListener('mouseleave', () => {{
        reviewState.hoverIndex = null;
        const trades = filteredReviewTrades();
        drawTradeReviewChart(ensureSelectedTrade(trades));
      }});
      window.addEventListener('resize', () => {{
        const trades = filteredReviewTrades();
        drawTradeReviewChart(ensureSelectedTrade(trades));
      }});
    }}

    document.querySelectorAll('.trade-row[data-review-id]').forEach((row) => {{
      row.addEventListener('click', () => {{
        reviewState.selectedId = row.dataset.reviewId;
        reviewState.hoverIndex = null;
        tabButtons.forEach((item) => item.classList.remove('active'));
        tabPanels.forEach((panel) => panel.classList.remove('active'));
        document.querySelector('.tab-button[data-tab="review"]').classList.add('active');
        document.getElementById('review').classList.add('active');
        renderReviewExperience();
      }});
    }});

    renderReviewExperience();
  </script>
</body>
</html>
"""


def _write_report(path: str | Path, html_report: str) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html_report, encoding="utf-8")
    return path


def run_single(data: BacktestData, config: BacktestConfig) -> dict[str, Any]:
    matches = find_pattern_matches(data, config)
    setups = apply_config_to_matches(matches, config)
    results = simulate_exits(setups, data, config)
    summary = compute_stats(results)
    rr_summary = compute_stats_rr(results)
    by_stock = compute_by_stock(results)
    curve = equity_curve(results)
    distribution = return_distribution(results)
    yearly = yearly_stats(results)
    visual_review = build_visual_review_payload(results, matches, data, config)
    report = _render_html_report(
        title=config.html_title,
        summary=summary,
        rr_summary=rr_summary,
        by_stock=by_stock,
        yearly=yearly,
        curve=curve,
        distribution=distribution,
        results=results,
        matches=matches,
        config=config,
        visual_review=visual_review,
    )
    _write_report(config.output_path, report)
    return {
        "mode": "single",
        "summary": summary,
        "rr_summary": rr_summary,
        "by_stock": by_stock,
        "yearly": yearly,
        "curve": curve,
        "distribution": distribution,
        "results": results,
        "matches": matches,
        "visual_review": visual_review,
        "report_path": config.output_path,
    }


def run_multi_entry(data: BacktestData, config: BacktestConfig) -> dict[str, Any]:
    multi_config = dataclasses.replace(config, multi_entry=True)
    matches = find_pattern_matches(data, multi_config)
    setups = apply_config_to_matches(matches, multi_config)
    results = simulate_exits(setups, data, multi_config)
    summary = compute_stats(results)
    rr_summary = compute_stats_rr(results)
    by_stock = compute_by_stock(results)
    curve = equity_curve(results)
    distribution = return_distribution(results)
    yearly = yearly_stats(results)
    visual_review = build_visual_review_payload(results, matches, data, multi_config)
    report = _render_html_report(
        title=f"{config.html_title} - Multi Entry",
        summary=summary,
        rr_summary=rr_summary,
        by_stock=by_stock,
        yearly=yearly,
        curve=curve,
        distribution=distribution,
        results=results,
        matches=matches,
        config=multi_config,
        visual_review=visual_review,
    )
    _write_report(multi_config.output_path, report)
    return {
        "mode": "multi_entry",
        "summary": summary,
        "rr_summary": rr_summary,
        "by_stock": by_stock,
        "yearly": yearly,
        "curve": curve,
        "distribution": distribution,
        "results": results,
        "matches": matches,
        "visual_review": visual_review,
        "report_path": multi_config.output_path,
    }


def run_sweep(data: BacktestData, config: BacktestConfig) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    best: dict[str, Any] | None = None
    for rr in config.sweep_target_rrs:
        for entry_offset in config.sweep_entry_offsets:
            sweep_config = dataclasses.replace(config, risk_reward=float(rr), entry_offset_pct=float(entry_offset))
            matches = find_pattern_matches(data, sweep_config)
            setups = apply_config_to_matches(matches, sweep_config)
            results = simulate_exits(setups, data, sweep_config)
            stats = compute_stats(results)
            stats_rr = compute_stats_rr(results)
            row = {
                "risk_reward": float(rr),
                "entry_offset_pct": float(entry_offset),
                "trades": stats["trades"],
                "win_rate": stats["win_rate"],
                "avg_return_pct": stats["avg_return_pct"],
                "avg_rr": stats_rr["avg_rr"],
                "equity_end": stats["equity_end"],
                "max_drawdown_pct": stats["max_drawdown_pct"],
            }
            rows.append(row)
            if best is None or row["equity_end"] > best["equity_end"]:
                best = row

    best = best or {}
    report = _render_html_report(
        title=f"{config.html_title} - Sweep",
        summary=best,
        rr_summary=best,
        by_stock=[],
        yearly=[],
        curve=[],
        distribution={"bins": []},
        results=[],
        matches=[],
        config=config,
        visual_review={"count": 0, "lookback_bars": 0, "lookahead_bars": 0, "trades": []},
        extra_sections={"sweep_results": rows, "best": best},
    )
    _write_report(config.output_path, report)
    return {
        "mode": "sweep",
        "sweep_results": rows,
        "best": best,
        "report_path": config.output_path,
    }


def _build_config(args: argparse.Namespace) -> BacktestConfig:
    symbols = _read_symbols_file(args.symbols_file)
    if args.symbols:
        cli_symbols = _sanitize_symbols(args.symbols)
        symbols = tuple(dict.fromkeys((symbols or ()) + (cli_symbols or ()))) if symbols or cli_symbols else None
    output_path = Path(args.output)
    return BacktestConfig(
        db_path=Path(args.db_path),
        symbols=symbols,
        output_path=output_path,
        d1_red_only=bool(args.d1_red),
        multi_entry=bool(args.multi_entry),
        sweep=bool(args.sweep),
        risk_reward=float(args.risk_reward),
        entry_offset_pct=float(args.entry_offset_pct),
        walk_forward_bars=int(args.walk_forward_bars),
        fee_bps=float(args.fee_bps),
        slippage_bps=float(args.slippage_bps),
        sweep_target_rrs=tuple(float(value) for value in args.sweep_target_rrs.split(",") if str(value).strip()),
        sweep_entry_offsets=tuple(float(value) for value in args.sweep_entry_offsets.split(",") if str(value).strip()),
        html_title=str(args.title),
    )


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Liquidity sweep backtest over local SQLite OHLCV data."
    )
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="SQLite database path.")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH), help="HTML report path.")
    parser.add_argument("--symbols", nargs="*", help="Universe symbols (bare NSE symbols).")
    parser.add_argument("--symbols-file", help="Optional text file with one symbol per line.")
    parser.add_argument("--title", default="Liquidity Sweep Backtest", help="HTML report title.")
    parser.add_argument("--risk-reward", type=float, default=2.0, help="Target in R multiples.")
    parser.add_argument(
        "--offset",
        "--entry-offset",
        dest="entry_offset_pct",
        type=float,
        default=0.5,
        help="Signed percentage offset from the stop-loss anchor used for entry.",
    )
    parser.add_argument("--walk-forward-bars", type=int, default=60, help="Future bar window for exits.")
    parser.add_argument("--fee-bps", type=float, default=0.0, help="Per-side fee in basis points.")
    parser.add_argument("--slippage-bps", type=float, default=0.0, help="Per-side slippage in basis points.")
    parser.add_argument("--d1-red", action="store_true", help="Require D-1 red candle.")
    parser.add_argument("--multi-entry", action="store_true", help="Allow overlapping trades.")
    parser.add_argument("--sweep", action="store_true", help="Run parameter sweep instead of a single backtest.")
    parser.add_argument(
        "--sweep-target-rrs",
        default="1.5,2.0,2.5,3.0",
        help="Comma-separated RR values for sweep mode.",
    )
    parser.add_argument(
        "--sweep-entry-offsets",
        default="0.0,0.5,1.0",
        help="Comma-separated signed entry offsets in percent for sweep mode.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)
    config = _build_config(args)
    data = load_data(config.db_path, config.symbols)
    if config.sweep:
        report = run_sweep(data, config)
    elif config.multi_entry:
        report = run_multi_entry(data, config)
    else:
        report = run_single(data, config)
    print(json.dumps(_sequence_to_jsonable(report), indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
