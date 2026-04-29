# Intraday Volume Spike Scalp Design

## Goal

Build a research lane for NSE F&O stocks that finds sudden intraday volume anomalies, tests quick scalp exits on broad 1-minute history, and keeps the signal engine compatible with a future 30-second live websocket adapter.

## Data Reality

Upstox REST historical candle APIs provide minute candles, not 30-second candles. The first research implementation will fetch and backtest 1-minute candles. A later live module can aggregate websocket ticks into 30-second bars and feed the same strategy functions.

Expired futures minute candles may require account access and may fail with a subscription error. The fetcher must report those failures without hiding them. If cash-equity minute data is used as a signal proxy, the payload and summaries must say so explicitly; it must not be described as futures execution data.

## Strategy Shape

The signal is long-only and looks for a quiet base followed by abnormal participation and price acceptance:

- enough prior 1-minute bars exist for a rolling volume baseline
- prior base range is compact
- current volume is at least `spike_multiple` times the rolling median volume
- current traded value clears `min_turnover`
- current high breaks the recent base high
- current close is in the upper part of its candle range
- optional VWAP trend filter can be added later

Entry defaults to the next candle open. Stop defaults to the lower of the spike candle low and recent base low. Target is expressed as human `1:R`. The trade exits at stop, target, or a maximum hold measured in candles.

## Architecture

Add a separate intraday strategy module instead of modifying the daily liquidity-sweep backtest. The daily strategy remains futures-aware and untouched. The intraday module has focused dataclasses, pure signal/exit functions, and payload helpers for API/UI reuse.

Add intraday SQLite storage through the Node Upstox ingestion layer:

- `ohlcv_intraday`
- symbol, timestamp, timeframe seconds, OHLCV, open interest
- instrument metadata, including segment, instrument key, trading symbol, contract expiry, lot size, and source

Add a small script command for 1-minute backfill. The first backfill path should support broad symbol lists and bounded concurrency. Futures data should be preferred where accessible; equity data can be fetched as an explicit signal proxy.

## UI/API

Expose a new `/api/intraday/analyze` endpoint. It should return:

- symbol-level 1-minute bars
- detected setups
- trade results
- summary stats
- selected-trade replay metadata
- clear `data_mode` labels such as `futures_1m` or `equity_signal_proxy_1m`

The existing UI can later add a mode switch. The first implementation can validate through tests and direct API payloads before a larger UI pass.

## Verification

Use test-first implementation. Required checks:

- `npm test`
- `python3 -m unittest discover -s tests -v`
- `node --check src/upstox/backfill.mjs`
- `node --check scripts/upstox_intraday_backfill.mjs`
- `python3 -m py_compile app/server.py backtest/liquidity_sweep_backtest.py backtest/intraday_volume_spike.py`

