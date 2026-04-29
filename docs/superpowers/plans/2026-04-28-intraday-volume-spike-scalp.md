# Intraday Volume Spike Scalp Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a 1-minute intraday volume-spike scalp research lane that can later consume 30-second websocket bars.

**Architecture:** Keep the existing daily liquidity-sweep engine untouched. Add a pure Python intraday strategy module, a Node Upstox intraday backfill path, and a narrow API endpoint for JSON analysis. SQLite stores intraday rows with explicit instrument/data-mode metadata.

**Tech Stack:** Python stdlib unittest/sqlite3/dataclasses, Node ESM, better-sqlite3, existing static HTTP app.

---

### Task 1: Intraday Strategy Engine

**Files:**
- Create: `backtest/intraday_volume_spike.py`
- Test: `tests/test_intraday_volume_spike.py`

- [ ] **Step 1: Write failing tests**
  - Test median-volume spike detection after quiet bars.
  - Test breakout/close-location filters reject weak spikes.
  - Test next-candle entry with target/stop/time exits.
  - Test summary payload labels equity proxy data explicitly.

- [ ] **Step 2: Run the tests**
  - Run: `python3 -m unittest tests.test_intraday_volume_spike -v`
  - Expected: import failure because `backtest.intraday_volume_spike` does not exist yet.

- [ ] **Step 3: Implement minimal module**
  - Add `IntradayBar`, `IntradayScalpConfig`, `IntradaySetup`, `IntradayTradeResult`.
  - Add `find_volume_spike_setups`, `simulate_intraday_exits`, `build_intraday_analysis_payload`.
  - Keep functions pure and independent of SQLite.

- [ ] **Step 4: Run the tests**
  - Run: `python3 -m unittest tests.test_intraday_volume_spike -v`
  - Expected: pass.

### Task 2: Intraday SQLite Loader And API

**Files:**
- Modify: `backtest/intraday_volume_spike.py`
- Modify: `app/server.py`
- Test: `tests/test_intraday_volume_spike.py`

- [ ] **Step 1: Write failing loader/API tests**
  - Create a temp DB with `ohlcv_intraday`.
  - Assert loader returns bars with instrument metadata.
  - Assert payload is frontend-ready and includes `data_mode`.

- [ ] **Step 2: Run the tests**
  - Run: `python3 -m unittest tests.test_intraday_volume_spike -v`
  - Expected: fail because loader functions are missing.

- [ ] **Step 3: Implement loader and endpoint**
  - Add `load_intraday_bars`.
  - Add `build_intraday_symbol_payload`.
  - Add route `/api/intraday/analyze`.

- [ ] **Step 4: Run Python tests**
  - Run: `python3 -m unittest tests.test_intraday_volume_spike -v`
  - Expected: pass.

### Task 3: Upstox 1-Minute Intraday Backfill

**Files:**
- Modify: `src/upstox/backfill.mjs`
- Create: `scripts/upstox_intraday_backfill.mjs`
- Test: `tests/node/upstox_backfill.test.mjs`

- [ ] **Step 1: Write failing Node tests**
  - Test intraday candle normalization keeps timestamp, timeframe seconds, source, segment, lot size, and expiry.
  - Test one-month chunking for 1-minute historical requests.
  - Test schema creation includes `ohlcv_intraday`.

- [ ] **Step 2: Run Node tests**
  - Run: `npm test`
  - Expected: fail because intraday helpers are missing.

- [ ] **Step 3: Implement Node helpers and CLI**
  - Add `normalizeIntradayCandle`, `buildIntradayWindows`, `createDb` schema addition, and `writeIntradayCandles`.
  - Add `runIntradayBackfill`.
  - Add script wrapper.

- [ ] **Step 4: Run Node tests**
  - Run: `npm test`
  - Expected: pass.

### Task 4: Documentation And Verification

**Files:**
- Modify: `README.md`
- Modify: `package.json`

- [ ] **Step 1: Add scripts and docs**
  - Add `fetch:upstox:intraday`.
  - Document 1-minute historical research and 30-second live-websocket plan.

- [ ] **Step 2: Run full checks**
  - Run: `npm test`
  - Run: `python3 -m unittest discover -s tests -v`
  - Run: `node --check src/upstox/backfill.mjs`
  - Run: `node --check scripts/upstox_intraday_backfill.mjs`
  - Run: `python3 -m py_compile app/server.py backtest/liquidity_sweep_backtest.py backtest/intraday_volume_spike.py`

