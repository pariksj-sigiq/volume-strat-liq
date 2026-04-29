from __future__ import annotations

import argparse
import csv
import json
import mimetypes
import sqlite3
from functools import lru_cache
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from backtest.liquidity_sweep_backtest import (
    DEFAULT_DB_PATH,
    BacktestConfig,
    build_analysis_payload,
    filter_backtest_data,
    load_data,
)
from backtest.intraday_volume_spike import (
    DEFAULT_BUCKETS,
    IntradayScalpConfig,
    build_intraday_symbol_payload,
    load_intraday_bars,
    run_intraday_bucketed_backtest,
)
from backtest.options_overlay import (
    annotate_option_expiry,
    build_option_probe_payload,
    load_market_dates,
)


APP_DIR = Path(__file__).resolve().parent
ROOT_DIR = APP_DIR.parent
STATIC_DIR = APP_DIR / "static"
DEFAULT_INTRADAY_REPORT_PATH = ROOT_DIR / "reports" / "intraday-volume-spike-bucketed-all.csv"
VENDOR_CHART_PATH = ROOT_DIR / "node_modules" / "lightweight-charts" / "dist" / "lightweight-charts.standalone.production.js"

INTRADAY_NUMERIC_FIELDS = {
    "entry_price",
    "stop_loss",
    "target_price",
    "exit_price",
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
}

INTRADAY_INTEGER_FIELDS = {"bars_held", "spike_volume", "rolling_median_volume"}


def _query_value(query: dict[str, list[str]], key: str, default: str | None = None) -> str | None:
    values = query.get(key)
    if not values:
        return default
    for value in values:
        if value != "":
            return value
    return default


def _bool_query(query: dict[str, list[str]], key: str, default: bool = False) -> bool:
    value = (_query_value(query, key) or "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on"}


def _resolve_static_path(pathname: str) -> Path | None:
    if pathname in {"/", "/intraday", "/intraday.html", "/signals", "/signals.html"}:
        return STATIC_DIR / "intraday.html"
    if pathname in {"/daily", "/daily.html", "/index.html"}:
        return STATIC_DIR / "index.html"
    if pathname.startswith("/assets/"):
        candidate = STATIC_DIR / pathname.removeprefix("/assets/")
        if candidate.is_file():
            return candidate
    if pathname == "/vendor/lightweight-charts.js" and VENDOR_CHART_PATH.is_file():
        return VENDOR_CHART_PATH
    return None


@lru_cache(maxsize=2)
def _load_cached_data(db_path: str):
    return load_data(db_path)


def build_meta_payload(db_path: Path) -> dict[str, object]:
    data = _load_cached_data(str(db_path.resolve()))
    all_symbol_bars = [bar for bars in data.bars_by_symbol.values() for bar in bars]
    series_bars = all_symbol_bars or data.benchmark_bars
    date_from = min((bar.date for bar in series_bars), default=None)
    date_to = max((bar.date for bar in series_bars), default=None)
    if len(series_bars) >= 756:
        ordered = sorted(series_bars, key=lambda bar: bar.date)
        default_from = ordered[-756].date.isoformat()
    elif len(series_bars) >= 252:
        ordered = sorted(series_bars, key=lambda bar: bar.date)
        default_from = ordered[-252].date.isoformat()
    else:
        default_from = date_from.isoformat() if date_from else None

    preferred_symbols = ("RELIANCE", "HDFCBANK", "ICICIBANK", "INFOSYS", "SBIN")
    default_symbol = next((symbol for symbol in preferred_symbols if symbol in data.symbols), None)
    if default_symbol is None:
        default_symbol = data.symbols[0] if data.symbols else None
    return {
        "symbols": list(data.symbols),
        "symbol_count": len(data.symbols),
        "date_from": date_from.isoformat() if date_from else None,
        "date_to": date_to.isoformat() if date_to else None,
        "default_symbol": default_symbol,
        "default_from": default_from,
        "default_to": date_to.isoformat() if date_to else None,
        "price_source": data.price_source,
    }


def build_symbol_payload(db_path: Path, query: dict[str, list[str]]) -> tuple[dict[str, object], HTTPStatus]:
    data = _load_cached_data(str(db_path.resolve()))
    symbol = (_query_value(query, "symbol", data.symbols[0] if data.symbols else None) or "").strip().upper()
    if not symbol:
        return {"error": "No symbol available in the database."}, HTTPStatus.BAD_REQUEST
    if symbol not in data.bars_by_symbol:
        return {"error": f"Unknown symbol: {symbol}"}, HTTPStatus.NOT_FOUND

    risk_reward = float(_query_value(query, "risk_reward", "2.0") or 2.0)
    entry_offset_pct = float(
        _query_value(query, "offset", _query_value(query, "entry_offset", "0.5")) or 0.5
    )
    walk_forward_bars = int(_query_value(query, "walk_forward_bars", "60") or 60)
    fee_bps = float(_query_value(query, "fee_bps", "0.0") or 0.0)
    slippage_bps = float(_query_value(query, "slippage_bps", "0.0") or 0.0)
    start_date = _query_value(query, "from")
    end_date = _query_value(query, "to")

    symbol_data = filter_backtest_data(data, symbols=(symbol,))
    config = BacktestConfig(
        db_path=db_path,
        symbols=(symbol,),
        d1_red_only=_bool_query(query, "d1_red", False),
        multi_entry=_bool_query(query, "multi_entry", False),
        risk_reward=risk_reward,
        entry_offset_pct=entry_offset_pct,
        walk_forward_bars=walk_forward_bars,
        fee_bps=fee_bps,
        slippage_bps=slippage_bps,
        html_title=f"Liquidity Sweep Lab • {symbol}",
    )

    payload = build_analysis_payload(symbol_data, config, start_date=start_date, end_date=end_date)
    payload["selected_symbol"] = symbol
    payload["controls"] = {
        "risk_reward": risk_reward,
        "entry_offset_pct": entry_offset_pct,
        "walk_forward_bars": walk_forward_bars,
        "fee_bps": fee_bps,
        "slippage_bps": slippage_bps,
        "d1_red": config.d1_red_only,
        "multi_entry": config.multi_entry,
    }
    return payload, HTTPStatus.OK


def build_intraday_payload_from_query(db_path: Path, query: dict[str, list[str]]) -> tuple[dict[str, object], HTTPStatus]:
    symbol = (_query_value(query, "symbol") or "").strip().upper()
    if not symbol:
        return {"error": "A symbol is required for intraday analysis."}, HTTPStatus.BAD_REQUEST

    data_mode = (_query_value(query, "data_mode", "futures_1m") or "futures_1m").strip()
    start_date = _query_value(query, "from")
    end_date = _query_value(query, "to")
    risk_reward = float(_query_value(query, "risk_reward", "1.5") or 1.5)
    base_lookback = int(_query_value(query, "base_lookback", "20") or 20)
    spike_multiple = float(_query_value(query, "spike_multiple", "3.0") or 3.0)
    min_turnover = float(_query_value(query, "min_turnover", "0.0") or 0.0)
    close_location = float(_query_value(query, "close_location", "0.75") or 0.75)
    max_hold_bars = int(_query_value(query, "max_hold_bars", "10") or 10)
    max_base_range_pct = float(_query_value(query, "max_base_range_pct", "0.03") or 0.03)
    timeframe_sec = int(_query_value(query, "timeframe_sec", "60") or 60)
    allow_overnight = str(_query_value(query, "allow_overnight", "false") or "false").lower() in {
        "1",
        "true",
        "yes",
        "y",
    }

    config = IntradayScalpConfig(
        base_lookback=base_lookback,
        spike_multiple=spike_multiple,
        min_turnover=min_turnover,
        close_location_threshold=close_location,
        risk_reward=risk_reward,
        max_hold_bars=max_hold_bars,
        timeframe_sec=timeframe_sec,
        data_mode=data_mode,
        max_base_range_pct=max_base_range_pct,
        allow_overnight=allow_overnight,
    )
    try:
        payload = build_intraday_symbol_payload(
            db_path,
            symbol=symbol,
            data_mode=data_mode,
            start_date=start_date,
            end_date=end_date,
            config=config,
        )
    except FileNotFoundError as error:
        return {"error": str(error)}, HTTPStatus.NOT_FOUND
    except Exception as error:
        return {"error": str(error)}, HTTPStatus.BAD_REQUEST

    payload["controls"] = {
        "risk_reward": risk_reward,
        "base_lookback": base_lookback,
        "spike_multiple": spike_multiple,
        "min_turnover": min_turnover,
        "close_location": close_location,
        "max_hold_bars": max_hold_bars,
        "max_base_range_pct": max_base_range_pct,
        "timeframe_sec": timeframe_sec,
        "data_mode": data_mode,
        "allow_overnight": allow_overnight,
    }
    return payload, HTTPStatus.OK


def build_intraday_day_payload_from_query(db_path: Path, query: dict[str, list[str]]) -> tuple[dict[str, object], HTTPStatus]:
    symbol = (_query_value(query, "symbol") or "").strip().upper()
    day = (_query_value(query, "date") or "").strip()[:10]
    if not symbol:
        return {"error": "A symbol is required for intraday day candles."}, HTTPStatus.BAD_REQUEST
    if not day:
        return {"error": "A date is required for intraday day candles."}, HTTPStatus.BAD_REQUEST

    data_mode = (_query_value(query, "data_mode", "equity_signal_proxy_1m") or "equity_signal_proxy_1m").strip()
    timeframe_sec = int(_query_value(query, "timeframe_sec", "60") or 60)
    try:
        bars = load_intraday_bars(
            db_path,
            symbol,
            data_mode=data_mode,
            start_date=day,
            end_date=day,
            timeframe_sec=timeframe_sec,
        )
    except FileNotFoundError as error:
        return {"error": str(error)}, HTTPStatus.NOT_FOUND
    except Exception as error:
        return {"error": str(error)}, HTTPStatus.BAD_REQUEST

    return {
        "symbol": symbol,
        "date": day,
        "data_mode": data_mode,
        "timeframe_sec": timeframe_sec,
        "bars_returned": len(bars),
        "bars": [
            {
                "timestamp": bar.timestamp.isoformat(),
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
                "turnover": bar.turnover,
                "open_interest": bar.open_interest,
                "instrument_key": bar.instrument_key,
                "trading_symbol": bar.trading_symbol,
                "contract_expiry": str(bar.contract_expiry) if bar.contract_expiry else None,
                "lot_size": bar.lot_size,
                "source": bar.source,
            }
            for bar in bars
        ],
    }, HTTPStatus.OK


def build_intraday_report_from_query(db_path: Path, query: dict[str, list[str]]) -> tuple[dict[str, object], HTTPStatus]:
    data_mode = (_query_value(query, "data_mode", "equity_signal_proxy_1m") or "equity_signal_proxy_1m").strip()
    start_date = _query_value(query, "from")
    end_date = _query_value(query, "to")
    risk_reward = float(_query_value(query, "risk_reward", "1.5") or 1.5)
    base_lookback = int(_query_value(query, "base_lookback", "20") or 20)
    spike_multiple = float(_query_value(query, "spike_multiple", "4.0") or 4.0)
    min_turnover = float(_query_value(query, "min_turnover", "50000000") or 50_000_000)
    close_location = float(_query_value(query, "close_location", "0.7") or 0.7)
    max_hold_bars = int(_query_value(query, "max_hold_bars", "10") or 10)
    max_base_range_pct = float(_query_value(query, "max_base_range_pct", "0.03") or 0.03)
    timeframe_sec = int(_query_value(query, "timeframe_sec", "60") or 60)
    max_instances = int(_query_value(query, "max_instances", "500") or 500)
    min_instance_rr_raw = _query_value(query, "min_instance_rr")
    min_instance_rr = float(min_instance_rr_raw) if min_instance_rr_raw not in (None, "") else None
    instance_sort = (_query_value(query, "instance_sort", "follow_through") or "follow_through").strip()
    two_day_hold_bars = int(_query_value(query, "two_day_hold_bars", "750") or 750)
    symbols = _parse_query_list(_query_value(query, "symbols"), upper=True)
    buckets = _parse_query_list(_query_value(query, "buckets"), upper=False) or list(DEFAULT_BUCKETS)

    config = IntradayScalpConfig(
        base_lookback=base_lookback,
        spike_multiple=spike_multiple,
        min_turnover=min_turnover,
        close_location_threshold=close_location,
        risk_reward=risk_reward,
        max_hold_bars=max_hold_bars,
        timeframe_sec=timeframe_sec,
        data_mode=data_mode,
        max_base_range_pct=max_base_range_pct,
    )
    try:
        payload = run_intraday_bucketed_backtest(
            db_path,
            data_mode=data_mode,
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            config=config,
            buckets=buckets,
            two_day_hold_bars=two_day_hold_bars,
            max_instances=max_instances,
            min_instance_rr=min_instance_rr,
            instance_sort=instance_sort,
        )
    except FileNotFoundError as error:
        return {"error": str(error)}, HTTPStatus.NOT_FOUND
    except Exception as error:
        return {"error": str(error)}, HTTPStatus.BAD_REQUEST

    payload["controls"] = {
        "risk_reward": risk_reward,
        "base_lookback": base_lookback,
        "spike_multiple": spike_multiple,
        "min_turnover": min_turnover,
        "close_location": close_location,
        "max_hold_bars": max_hold_bars,
        "max_base_range_pct": max_base_range_pct,
        "timeframe_sec": timeframe_sec,
        "data_mode": data_mode,
        "max_instances": max_instances,
        "min_instance_rr": min_instance_rr,
        "instance_sort": instance_sort,
        "two_day_hold_bars": two_day_hold_bars,
        "buckets": buckets,
    }
    return payload, HTTPStatus.OK


def build_precomputed_intraday_report(report_path: Path, db_path: Path | None = None) -> dict[str, object]:
    if not report_path.is_file():
        raise FileNotFoundError(f"Intraday report not found: {report_path}")

    stat = report_path.stat()
    return _build_precomputed_intraday_report_cached(
        str(report_path),
        stat.st_mtime_ns,
        stat.st_size,
        str(db_path) if db_path else "",
    )


@lru_cache(maxsize=4)
def _build_precomputed_intraday_report_cached(
    report_path_value: str,
    report_mtime_ns: int,
    report_size: int,
    db_path_value: str,
) -> dict[str, object]:
    del report_mtime_ns, report_size
    report_path = Path(report_path_value)
    db_path = Path(db_path_value) if db_path_value else None
    market_dates = set(load_market_dates(db_path)) if db_path else set()
    with report_path.open("r", encoding="utf-8", newline="") as handle:
        rows = [
            annotate_option_expiry(_coerce_intraday_csv_row(row), market_dates)
            for row in csv.DictReader(handle)
        ]

    bucket_summaries: dict[str, dict[str, object]] = {}
    for bucket in DEFAULT_BUCKETS:
        bucket_rows = [row for row in rows if row.get("bucket") == bucket]
        bucket_summaries[bucket] = _summarize_intraday_rows(bucket_rows)

    symbols = sorted({str(row.get("symbol", "")).upper() for row in rows if row.get("symbol")})
    data_mode = next((row.get("data_mode") for row in rows if row.get("data_mode")), "equity_signal_proxy_1m")
    latest_signal = max((str(row.get("signal_timestamp") or "") for row in rows), default="")
    earliest_signal = min((str(row.get("signal_timestamp") or "") for row in rows if row.get("signal_timestamp")), default="")

    return {
        "source": "precomputed_csv",
        "report_path": str(report_path),
        "data_mode": data_mode,
        "timeframe_sec": 60,
        "symbols": symbols,
        "symbols_scanned": len(symbols),
        "total_bars": "not_counted_on_page_load",
        "instances": rows,
        "instances_total": len(rows),
        "instances_returned": len(rows),
        "summary": _summarize_intraday_rows(rows),
        "bucket_summaries": bucket_summaries,
        "date_from": earliest_signal[:10] or None,
        "date_to": latest_signal[:10] or None,
    }


def build_precomputed_intraday_report_from_query(
    db_path: Path,
    query: dict[str, list[str]],
) -> tuple[dict[str, object], HTTPStatus]:
    report_path = Path(_query_value(query, "path", str(DEFAULT_INTRADAY_REPORT_PATH)) or DEFAULT_INTRADAY_REPORT_PATH)
    if not report_path.is_absolute():
        report_path = ROOT_DIR / report_path
    report_path = report_path.resolve()
    reports_dir = (ROOT_DIR / "reports").resolve()
    if reports_dir not in report_path.parents and report_path != DEFAULT_INTRADAY_REPORT_PATH.resolve():
        return {"error": "Report path must live under the local reports directory."}, HTTPStatus.BAD_REQUEST
    try:
        return build_precomputed_intraday_report(report_path, db_path), HTTPStatus.OK
    except FileNotFoundError as error:
        return {"error": str(error)}, HTTPStatus.NOT_FOUND
    except Exception as error:
        return {"error": str(error)}, HTTPStatus.BAD_REQUEST


def build_option_probe_from_query(db_path: Path, query: dict[str, list[str]]) -> tuple[dict[str, object], HTTPStatus]:
    try:
        return build_option_probe_payload(db_path, query), HTTPStatus.OK
    except Exception as error:
        return {"error": str(error)}, HTTPStatus.BAD_REQUEST


def _coerce_intraday_csv_row(row: dict[str, str]) -> dict[str, object]:
    coerced: dict[str, object] = {}
    for key, value in row.items():
        if value == "":
            coerced[key] = None
        elif key in INTRADAY_INTEGER_FIELDS:
            coerced[key] = int(float(value))
        elif key in INTRADAY_NUMERIC_FIELDS:
            coerced[key] = float(value)
        else:
            coerced[key] = value
    return coerced


def _summarize_intraday_rows(rows: list[dict[str, object]]) -> dict[str, object]:
    total = len(rows)
    pnl_points = [_as_float(row.get("pnl_points")) for row in rows]
    rrs = [_as_float(row.get("rr")) for row in rows]
    returns = [_as_float(row.get("return_pct")) for row in rows]
    wins = sum(1 for value in pnl_points if value > 0)
    losses = sum(1 for value in pnl_points if value < 0)
    flat = total - wins - losses
    timeouts = sum(1 for row in rows if row.get("exit_reason") == "timeout")
    return {
        "total_trades": total,
        "wins": wins,
        "losses": losses,
        "flat": flat,
        "timeouts": timeouts,
        "win_rate_pct": (wins / total * 100.0) if total else 0.0,
        "avg_rr": _average_numbers(rrs),
        "avg_return_pct": _average_numbers(returns),
        "total_pnl_rupees": 0.0,
        "avg_pnl_rupees": 0.0,
        "best_rr": max(rrs) if rrs else 0.0,
        "worst_rr": min(rrs) if rrs else 0.0,
    }


def _count_intraday_bars(db_path: Path | None) -> int:
    if db_path is None or not db_path.is_file():
        return 0
    try:
        conn = sqlite3.connect(db_path)
        try:
            row = conn.execute(
                """
                SELECT COUNT(*)
                FROM ohlcv_intraday
                WHERE data_mode = 'equity_signal_proxy_1m'
                  AND timeframe_sec = 60
                """
            ).fetchone()
            return int(row[0] or 0) if row else 0
        finally:
            conn.close()
    except sqlite3.Error:
        return 0


def _as_float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _average_numbers(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _parse_query_list(value: str | None, *, upper: bool) -> list[str] | None:
    if not value:
        return None
    items = [part.strip() for part in value.replace(",", " ").split() if part.strip()]
    if upper:
        items = [item.upper() for item in items]
    return items or None


class AppHandler(BaseHTTPRequestHandler):
    server_version = "LiqSweepHTTP/0.1"

    def _send_json(self, payload: dict[str, object], status: HTTPStatus = HTTPStatus.OK) -> None:
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status.value)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _send_file(self, file_path: Path) -> None:
        mime_type, _ = mimetypes.guess_type(file_path.name)
        content_type = mime_type or "application/octet-stream"
        payload = file_path.read_bytes()
        self.send_response(HTTPStatus.OK.value)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        db_path = Path(getattr(self.server, "db_path", DEFAULT_DB_PATH))

        if parsed.path == "/api/health":
            self._send_json({"ok": True})
            return
        if parsed.path == "/api/meta":
            self._send_json(build_meta_payload(db_path))
            return
        if parsed.path == "/api/analyze":
            payload, status = build_symbol_payload(db_path, query)
            self._send_json(payload, status)
            return
        if parsed.path == "/api/intraday/analyze":
            payload, status = build_intraday_payload_from_query(db_path, query)
            self._send_json(payload, status)
            return
        if parsed.path == "/api/intraday/day":
            payload, status = build_intraday_day_payload_from_query(db_path, query)
            self._send_json(payload, status)
            return
        if parsed.path == "/api/intraday/report":
            payload, status = build_intraday_report_from_query(db_path, query)
            self._send_json(payload, status)
            return
        if parsed.path == "/api/intraday/precomputed-report":
            payload, status = build_precomputed_intraday_report_from_query(db_path, query)
            self._send_json(payload, status)
            return
        if parsed.path == "/api/intraday/option-probe":
            payload, status = build_option_probe_from_query(db_path, query)
            self._send_json(payload, status)
            return

        file_path = _resolve_static_path(parsed.path)
        if file_path is not None and file_path.is_file():
            self._send_file(file_path)
            return

        self._send_json({"error": "Not found"}, HTTPStatus.NOT_FOUND)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Serve the Liq Sweep interactive analysis app.")
    parser.add_argument("--host", default="127.0.0.1", help="Host interface to bind.")
    parser.add_argument("--port", type=int, default=8877, help="Port to bind.")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="SQLite database path.")
    args = parser.parse_args(argv)

    server = ThreadingHTTPServer((args.host, args.port), AppHandler)
    server.db_path = str(Path(args.db_path).resolve())  # type: ignore[attr-defined]
    print(f"Liq Sweep app available at http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
