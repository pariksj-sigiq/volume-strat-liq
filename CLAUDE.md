# CLAUDE.md

This file is a durable context handoff for future agents working on `liq-sweep`.

## Product Intent

The user wants a serious trading-research tool, not a generic dashboard.

The desired workflow is:

1. pull data from Upstox without OAuth redirect flows
2. analyze only the user’s NSE F&O stock universe
3. run daily liquidity-sweep strategy analysis
4. tune risk:reward after the signal is generated
5. inspect trades visually with candlesticks and rationale
6. later run more strategy variants and backtests on top of the same data foundation

## Timeline Of Important Decisions

### Original direction

- clean project
- Node for data fetch
- Python for analysis
- local SQLite storage

### First implementation

- benchmark and stock universe were fetched as cash-equity candles into `ohlcv_daily`
- Python backtest and browser app were built on top of that

### UX expansion

- real chart-based trade review was added
- symbol search was added
- reverse chronological trade sorting was added
- tunable risk:reward was added to the app
- the R:R input was reshaped into a human-readable `1:R` control
- entry was later clarified as a signed `offset %` from the stop-loss anchor
- stop was later fixed to the low of the most recent prior red candle

### Critical correction

The user then clarified:

- the instrument should be stock futures, not cash equities
- lot sizes matter

That forced a model change:

- ingestion must become contract-aware
- analysis must become futures-aware
- trade payloads must include lot size and contract identity

## Current Technical State

### Storage

The project now has these relevant SQLite structures:

#### `stocks`

Universe metadata and active flags.

#### `instruments`

Cash underlyings plus benchmark metadata from the Upstox instrument dump.

#### `ohlcv_daily`

Legacy benchmark / cash table.

Current intended use:

- benchmark `NIFTY50`
- fallback only if futures data is absent

#### `futures_contracts`

Per-contract futures metadata.

#### `ohlcv_futures_daily`

Contract-level daily futures bars with:

- expiry date
- trading symbol
- lot size
- instrument key

### Ingestion assumptions

`src/upstox/backfill.mjs` currently does all of the following:

- targets a 5-year lookback by default
- fetches the live NSE instrument dump
- finds:
  - underlyings
  - benchmark
  - current active futures contracts
- calls expired-instrument APIs to discover recent expired futures contracts
- backfills candles into `ohlcv_futures_daily`

### Analysis assumptions

`backtest/liquidity_sweep_backtest.py` now prefers futures data.

Continuous-series rule:

- for each symbol/date, use the earliest expiry available on that date
- effectively front-month selection

Benchmark rule:

- keep `NIFTY50` from `ohlcv_daily`

Trade result payloads now include:

- `instrument_key`
- `trading_symbol`
- `contract_expiry`
- `lot_size`
- `risk_points`
- `gross_pnl_points`
- `net_pnl_points`
- `gross_pnl_rupees`
- `net_pnl_rupees`

Entry / stop rule now assumed by the code:

- entry = `signal_close * (1 + offset_pct / 100)`
- entry must print inside the signal candle range
- stop = low of the most recent red candle before the signal day

### App assumptions

The app should now be interpreted as a futures-analysis UI.

Important UX points already implemented:

- `1:R` style risk:reward input
- typed stock search
- latest-first trade order
- visual trade review with candlestick chart
- trade-level display of:
  - futures contract
  - expiry
  - lot size
  - per-lot rupee P&L

## Real Data Constraint

This matters a lot.

The current Upstox path does not look like a guaranteed 5-to-10-year stock-futures archive.

Live checks on April 22, 2026 showed:

- current `NSE_FO` contracts are present in the BOD file
- recent expired stock-futures contracts are retrievable
- older stock-futures expiries can return empty arrays

So the implementation should be described honestly as:

- “targeting the last 5 years”
- “bounded by Upstox futures coverage actually returned”

Do not tell the user they have a full 5-year chain unless you verify the final DB contents.

## Practical Caveats

1. The repo root is not currently a git repository.
2. `app/server.py` caches `load_data` with `lru_cache`, so DB changes require a server restart to be visible.
3. The current front-month roll logic is simple and deterministic, not exchange-grade continuous-contract engineering.
4. Strategy signals are presently computed on the futures continuous series itself.

## What Still Needs Attention

### Highest priority

1. Confirm the live futures backfill completed successfully.
2. Audit earliest available futures date per symbol.
3. Restart the local app server after the new backfill.
4. Verify `/api/meta` and `/api/analyze` are returning futures-derived results.

### Next UX improvements

1. Show roll transitions or contract changes in the overview chart.
2. Add an instrument badge in the app header for the current selected symbol.
3. Add explicit explanation of whether a trade came from front-month continuous logic versus a single static contract.
4. Consider showing both percentage return and rupee P&L more prominently.

### Next research improvements

1. Decide whether the signal should be generated on spot or futures.
2. Add per-trade transaction-cost modeling beyond flat bps if needed.
3. Add multi-lot or capital-based sizing later if the user asks.
4. If long-history futures research is essential, integrate a second historical source.

## Commands Future Agents Will Likely Need

### Tests

```bash
npm test
python3 -m unittest discover -s tests -v
```

### Syntax / compile checks

```bash
node --check src/upstox/backfill.mjs
node --check app/static/app.js
python3 -m py_compile app/server.py backtest/liquidity_sweep_backtest.py
```

### Run the futures backfill

```bash
npm run fetch:upstox -- --years 5 --concurrency 8
```

### Run the app

```bash
python3 -m app.server --host 127.0.0.1 --port 8877
```

## Non-Negotiables

- keep token-based Upstox access
- do not expose secrets in logs or docs
- do not silently revert to cash-equity analysis for stock symbols
- keep lot size attached to trade economics
- keep the UI intelligible to a discretionary human reviewer

## Good Mental Model

Think of the project as:

- Node = market data plumbing
- SQLite = raw + normalized local warehouse
- Python = strategy brain
- browser app = human review console

The user’s confidence depends on the last part.

Even if the math is right, if the chart review does not make the trade look sensible, the product is not done.
