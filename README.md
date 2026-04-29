# Liq Sweep

Interactive research workstation for a daily liquidity-sweep strategy on NSE F&O stocks.

The project currently has three major pieces:

1. `src/upstox/backfill.mjs`
   Node-based Upstox ingestion for benchmark data and stock-futures contracts.
2. `backtest/liquidity_sweep_backtest.py`
   Python backtest and payload builder for the strategy.
3. `app/server.py` + `app/static/*`
   Local backend-connected UI for symbol-level analysis and visual trade review.

## Current Product Shape

- Universe: the fixed `FNO_STOCKS` list in `src/upstox/universe.mjs`
- Benchmark: `NIFTY50`
- Strategy engine: long-only daily liquidity-sweep pattern
- Analysis instrument: continuous front-month stock futures series
- Benchmark instrument: cash `NIFTY50`
- Risk model: user-tunable `1:R` target after a valid signal is generated
- Review surface: overview chart, trade atlas, trade ledger, and detailed candlestick replay

## Important Pivot

This project started with cash-equity candles in `ohlcv_daily`.

It has now been pivoted so that:

- stock analysis should come from `ohlcv_futures_daily`
- a continuous front-month futures series is built in Python by choosing the nearest expiry available for each symbol/date
- trade metadata now carries futures identity:
  - `trading_symbol`
  - `contract_expiry`
  - `instrument_key`
  - `lot_size`
  - rupee P&L per lot

The benchmark still comes from `ohlcv_daily` as `NIFTY50`.

## Data Model

### Existing benchmark / legacy cash table

`ohlcv_daily`

- `symbol`
- `date`
- `open`
- `high`
- `low`
- `close`
- `adj_close`
- `volume`
- `instrument_key`
- `open_interest`
- `source`

Used now only for benchmark and as a fallback if futures tables are empty.

### Futures contract metadata

`futures_contracts`

- `symbol`
- `expiry_date`
- `instrument_key`
- `trading_symbol`
- `exchange`
- `segment`
- `instrument_type`
- `lot_size`
- `tick_size`
- `underlying_key`
- `underlying_type`
- `underlying_symbol`
- `source`
- `is_active`
- `updated_at`

### Futures OHLCV

`ohlcv_futures_daily`

- `symbol`
- `expiry_date`
- `date`
- `open`
- `high`
- `low`
- `close`
- `adj_close`
- `volume`
- `open_interest`
- `instrument_key`
- `trading_symbol`
- `lot_size`
- `source`

## Backfill Behavior

`npm run fetch:upstox -- --years 5 --concurrency 8`

What it does:

- loads `.env`
- fetches the Upstox NSE instrument dump
- stores benchmark metadata and benchmark candles
- discovers current `NSE_FO` futures contracts from the BOD instrument file
- discovers expired futures contracts from Upstox expired-instruments APIs
- fetches daily candles contract by contract
- writes futures rows into `ohlcv_futures_daily`

### Upstox API paths in use

- current historical candles: Upstox Historical Candle V3
- expired expiries: `expired-instruments/expiries`
- expired stock-futures contract lookup: `expired-instruments/future/contract`
- expired historical candles: `expired-instruments/historical-candle`

### Verified limitation

Upstox does not appear to expose a full 10-year expired stock-futures chain through the currently accessible expired-contract flow.

On April 22, 2026, live validation showed:

- active futures contracts are available from the current instrument dump
- expired stock-futures contracts are available for recent expiries
- older expiries can return empty lists for some symbols

So the current implementation aims for a 5-year window, but the actual earliest date in the local futures dataset is constrained by what Upstox returns per symbol.

## Strategy Summary

Signal day `D` must satisfy:

- `D.close > D.open`
- `D.low < D-1.low`
- stock/futures 63-day return > `NIFTY50` 63-day return
- stock/futures 63-day return > 0
- optional `D-1` red filter

Trade handling:

- entry is `stop_loss * (1 + offset_pct/100)`
- the entry is only valid if that price printed inside the signal candle range
- stop is fixed at the low of the most recent red candle before the signal day
- target is derived from the user-selected `1:R`
- exits start from `D+1`
- ambiguous future bars resolve:
  - green candle: target first
  - red candle: stop first
- max walk-forward horizon defaults to 60 bars

## App

Start:

```bash
python3 -m app.server --host 127.0.0.1 --port 8877
```

Open:

- [http://127.0.0.1:8877](http://127.0.0.1:8877)

The app currently supports:

- symbol search by typed NSE symbol
- date-window presets
- ratio-shaped `1:R` input
- reverse-chronological trade browsing
- selected-trade candlestick replay
- contract expiry and lot-size display
- per-lot rupee P&L display

## EC2 Deployment

Pushes to `main` deploy code to the ap-south-1 EC2 host through the repo-scoped self-hosted GitHub Actions runner.

The production SQLite data layer is intentionally server-local:

- live DB: `/opt/liq-sweep/data/nse_data.db`
- backups: `/opt/liq-sweep-backups`
- deploy script: `scripts/ec2_deploy.sh`
- manual data backup script: `scripts/ec2_backup_data.sh`
- Nginx proxy config: `infra/nginx/liq-sweep.conf`

Normal deployments do not copy, overwrite, delete, or gzip the database. The deploy script syncs application code only, excludes `data/`, and refuses to deploy if the live DB is missing.
Nginx gzip is enabled for JSON, JS, and CSS so the large intraday report payload is sent compressed.

When the data is intentionally refreshed or mutated, run a manual backup on the EC2 host:

```bash
sudo bash /opt/liq-sweep/scripts/ec2_backup_data.sh
```

## Intraday Volume Spike Scalp

This is a separate research lane for the ETERNAL-style move: quiet intraday tape, sudden abnormal volume, breakout, and quick continuation.

Historical research uses 1-minute candles because Upstox REST historical/intraday candle APIs expose minute intervals, not 30-second candles. The live-market version should aggregate websocket ticks into 30-second bars and feed the same signal engine.

### Intraday data table

`ohlcv_intraday`

- `symbol`
- `timestamp`
- `date`
- `timeframe_sec`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `open_interest`
- `instrument_key`
- `trading_symbol`
- `market_segment`
- `instrument_type`
- `contract_expiry`
- `lot_size`
- `source`
- `data_mode`

`data_mode` is intentionally explicit:

- `futures_1m` means the row came from an NSE F&O futures instrument.
- `equity_signal_proxy_1m` means the row came from NSE cash equity and should be treated as signal research/proxy data, not futures execution data.
- `options_1m` means the row came from an Upstox expired NSE F&O option contract.

### Fetch 1-minute data

For broad signal research over the F&O stock universe:

```bash
npm run fetch:upstox:intraday -- --years 1 --mode equity-signal-proxy --concurrency 8
```

For currently active futures contracts only:

```bash
npm run fetch:upstox:intraday -- --years 1 --mode futures --concurrency 8
```

The futures command depends on what Upstox exposes for active contracts; a complete one-year continuous futures dataset still requires expired-contract minute coverage.

To load ATM option candles for precomputed intraday signals:

```bash
node scripts/upstox_signal_options_backfill.mjs --from-date 2026-03-01 --to-date 2026-03-31 --concurrency 2
```

The option backfill first asks Upstox for the actual expired option expiries for each symbol and stores them in `option_expiries`; this avoids guessing stock-option expiries from calendar rules.

On EC2, run the `Backfill Options` GitHub Actions workflow to fill the preserved production DB and restart the app cache after the job.

### Analyze one symbol from the API

```bash
python3 -m app.server --host 127.0.0.1 --port 8877
```

Then call:

```text
http://127.0.0.1:8877/api/intraday/analyze?symbol=ETERNAL&data_mode=equity_signal_proxy_1m&risk_reward=1.5&base_lookback=20&spike_multiple=3&max_hold_bars=10
```

The intraday signal requires:

- a quiet base over the prior `base_lookback` candles
- current volume at least `spike_multiple` times the rolling median
- traded value above `min_turnover`
- a break above the base high
- close location near the high of the spike candle

Entry is the next 1-minute candle open. Stop is the lower of the spike candle low and base low. Target is expressed as `1:R`, and timeout is measured in candles. By default this is a same-day scalp: the engine skips signals whose next entry candle is on the next trading day and it will not carry a trade overnight.

### Mine prior ETERNAL-style instances

Use the universe backtest CLI to pull out the actual cases across every downloaded symbol. CSV output is the quickest research table; JSON output includes the full summary payload.

```bash
python3 -m backtest.intraday_volume_spike \
  --db-path data/nse_data.db \
  --data-mode equity_signal_proxy_1m \
  --base-lookback 20 \
  --spike-multiple 4 \
  --min-turnover 50000000 \
  --close-location 0.7 \
  --risk-reward 1.5 \
  --max-hold-bars 10 \
  --max-base-range-pct 0.03 \
  --min-instance-rr 1 \
  --instance-sort follow_through \
  --max-instances 500 \
  --output reports/intraday-volume-spike-instances.csv
```

Each mined instance includes signal timestamp, entry/stop/target/exit, realized R, max favorable R, max adverse R, volume multiple, turnover, candle close location, and a `review_url` for symbol-level candle replay.

Use `--allow-overnight` only for a separate research pass where next-day continuation is intentionally allowed.

For bucketed research across same-day, next-morning entry, and two-day follow-through:

```bash
python3 -m backtest.intraday_volume_spike \
  --bucketed \
  --db-path data/nse_data.db \
  --data-mode equity_signal_proxy_1m \
  --base-lookback 20 \
  --spike-multiple 4 \
  --min-turnover 50000000 \
  --close-location 0.7 \
  --risk-reward 1.5 \
  --max-hold-bars 10 \
  --two-day-hold-bars 750 \
  --max-base-range-pct 0.03 \
  --min-instance-rr 1 \
  --instance-sort follow_through \
  --max-instances 500 \
  --output reports/intraday-volume-spike-bucketed.csv
```

The local HTML workbench is available at:

```text
http://127.0.0.1:8877/intraday.html
```

It calls `/api/intraday/report` and renders bucket KPIs, filters, a sortable instance table, and selected-case details.

## Tests

```bash
npm test
python3 -m unittest discover -s tests -v
node --check app/static/app.js
node --check src/upstox/backfill.mjs
node --check scripts/upstox_intraday_backfill.mjs
python3 -m py_compile app/server.py backtest/liquidity_sweep_backtest.py
python3 -m py_compile backtest/intraday_volume_spike.py
```

## High-Priority Next Work

1. Finish a full successful live futures backfill and inspect actual earliest/coverage dates per symbol.
2. Restart the app server after the futures backfill so the cached loader uses the new tables.
3. Add UI surfacing for the exact active contract chain and roll points on overview charts.
4. Decide whether strategy signals should stay on the continuous futures series or be derived from spot while executed on futures.
5. If more than recent futures history is required, add a secondary source for older contract history.
