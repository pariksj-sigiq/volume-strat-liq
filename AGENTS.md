# AGENTS.md

This file is for future coding agents working in `/Users/pariksj/Desktop/liq-sweep`.

## Mission

Build and maintain a local research workstation for an NSE F&O liquidity-sweep strategy with:

- Node ingestion from Upstox using token-based auth only
- Python backtesting and analysis
- browser-based visual review of individual trades
- futures-aware contract and lot-size handling

## User Intent You Should Preserve

- The user wants this to be execution-oriented, not a toy analytics demo.
- The user prefers reasonable assumptions over repeated questions.
- The user wants symbol-level, visually convincing trade review.
- The user specifically wants stock futures, not cash-equity backtests.
- Lot size matters and should be part of trade economics.
- Risk:Reward must be understandable to humans as `1:R`, not an opaque numeric field.

## Current Architecture

### Node ingestion

Files:

- `scripts/upstox_backfill.mjs`
- `src/upstox/backfill.mjs`

Responsibilities:

- load `.env`
- fetch benchmark `NIFTY50`
- discover active `NSE_FO` futures contracts from the live instrument dump
- discover expired contracts from Upstox expired endpoints
- fetch daily candles for each contract
- write:
  - `futures_contracts`
  - `ohlcv_futures_daily`
  - benchmark `ohlcv_daily`

### Python backtest

File:

- `backtest/liquidity_sweep_backtest.py`

Responsibilities:

- load benchmark and symbol data from SQLite
- prefer `ohlcv_futures_daily` when present
- build a continuous front-month futures series per symbol/date
- detect the liquidity-sweep pattern
- simulate exits
- emit frontend-ready JSON payloads
- include futures identity and lot-size-aware P&L in trade results
- use the prior red candle low as the fixed stop
- use a signed percentage offset from the stop-loss anchor for entry

### Local app

Files:

- `app/server.py`
- `app/static/index.html`
- `app/static/app.js`
- `app/static/styles.css`

Responsibilities:

- serve static assets
- expose:
  - `/api/health`
  - `/api/meta`
  - `/api/analyze`
- render symbol-level interactive analysis
- let the user inspect trades candle by candle

## Data Rules

### Benchmark

- benchmark stays as cash `NIFTY50`
- stored in `ohlcv_daily`

### Strategy instrument

- strategy instrument is continuous front-month stock futures
- loaded from `ohlcv_futures_daily`
- per date, the earliest expiry available for that symbol is selected
- stop is the low of the most recent red candle before the signal
- entry is a signed percentage offset from the stop-loss anchor

### Metadata that must survive end to end

For every analyzed futures bar or trade, preserve whenever available:

- `instrument_key`
- `trading_symbol`
- `contract_expiry`
- `lot_size`
- `open_interest`
- `source`

## Verified Real-World Constraint

Do not assume Upstox gives a full historical futures chain.

As of April 22, 2026:

- active futures contracts are visible in the BOD instrument file
- expired contract APIs work for recent expiries
- older expiries can return empty responses for stock futures

Practical consequence:

- the code targets a 5-year window
- actual local coverage is only “up to 5 years” and depends on Upstox availability by symbol

If the user wants a true 5-to-10-year continuous futures history, you will likely need another source for older contracts.

## Local Runtime Commands

### Install / test

```bash
npm test
python3 -m unittest discover -s tests -v
```

### Static checks

```bash
node --check src/upstox/backfill.mjs
node --check app/static/app.js
python3 -m py_compile app/server.py backtest/liquidity_sweep_backtest.py
```

### Backfill

```bash
npm run fetch:upstox -- --years 5 --concurrency 8
```

### Run app

```bash
python3 -m app.server --host 127.0.0.1 --port 8877
```

## Important Known Context

- This workspace is not a git repository at the root level right now.
- There is an existing `data/nse_data.db` with legacy cash rows plus newer futures tables.
- The frontend already has:
  - typed symbol search
  - ratio-shaped `1:R` input
  - reverse chronological trade sorting
  - detailed candlestick trade review
- The UI was previously confusing around R:R; it has been intentionally reshaped to look like `1 : [reward]`.

## Known Gaps / Open Work

1. Live full backfill completion needs verification after the futures backfill run finishes.
2. The app server should be restarted after DB changes because `load_data` is cached with `lru_cache`.
3. The strategy currently uses the continuous futures series for signal generation.
4. If the user later wants execution realism beyond one lot, position sizing is still not configurable.
5. Roll methodology is currently simple front-month selection by nearest expiry on each date.
6. The UI can be improved further by showing roll boundaries and contract switches explicitly.
7. Later, expose authenticated EC2-side maintenance APIs so local runs can submit backfill payloads/results directly to the production SQLite DB and keep EC2/local parity without ad hoc S3 transfers.

## Future EC2 Maintenance APIs

The user wants routine backfills and quick fixes to become API-driven from the local workstation into EC2.

When implementing this later:

- Add authenticated admin-only endpoints; do not expose unauthenticated DB mutation routes.
- Prefer narrow, idempotent operations such as upserting intraday OHLCV rows, installing generated report CSVs, rebuilding reports, checking coverage, and restarting/reloading app caches.
- Require a dry-run/validate mode for DB-changing payloads.
- Take an EC2 backup before applying mutations to `/opt/liq-sweep/data/nse_data.db`.
- Return coverage deltas and affected row counts so parity can be checked immediately from local.
- Keep secrets out of logs and request/response bodies.
- Preserve the current data contract: local and EC2 should converge on the same SQLite schema and report outputs.

## If You Touch The Strategy

- Do not silently switch back to cash-equity candles.
- Do not strip lot size out of results.
- Do not change ambiguous-bar resolution rules unless the user explicitly asks.
- Keep benchmark logic explicit and separate from strategy instrument logic.

## If You Touch The Fetcher

- Keep token-based auth only.
- Keep bounded concurrency.
- Keep retry logic.
- Keep contract-level storage; do not collapse raw contract history away.
- Avoid printing secrets or tokens.

## If You Need To Debug Coverage

Useful checks:

```sql
SELECT symbol, MIN(date), MAX(date), COUNT(*) FROM ohlcv_futures_daily GROUP BY symbol ORDER BY MIN(date);
SELECT symbol, expiry_date, COUNT(*) FROM ohlcv_futures_daily GROUP BY symbol, expiry_date ORDER BY symbol, expiry_date;
SELECT symbol, lot_size, MIN(date), MAX(date) FROM ohlcv_futures_daily GROUP BY symbol, lot_size ORDER BY symbol, MIN(date);
```
