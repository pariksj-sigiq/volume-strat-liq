import fs from "node:fs";
import path from "node:path";
import process from "node:process";

import Database from "better-sqlite3";

const EXPIRED_OPTION_CONTRACT_URL = "https://api.upstox.com/v2/expired-instruments/option/contract";
const EXPIRED_EXPIRIES_URL = "https://api.upstox.com/v2/expired-instruments/expiries";
const EXPIRED_HISTORICAL_CANDLES_BASE_URL = "https://api.upstox.com/v2/expired-instruments/historical-candle";
const OPTION_DATA_MODE = "options_1m";
const PREFERRED_CALENDAR_SYMBOLS = ["RELIANCE", "HDFCBANK", "ICICIBANK", "INFOSYS", "SBIN", "ETERNAL"];
const REQUEST_HEADERS = {
  Accept: "application/json",
  "Content-Type": "application/json",
  "User-Agent": "Mozilla/5.0",
};

function parseCliArgs(argv = process.argv.slice(2)) {
  const config = {};
  for (let index = 0; index < argv.length; index += 1) {
    const part = argv[index];
    if (!part.startsWith("--")) continue;
    const [flag, inlineValue] = part.split("=", 2);
    const key = flag.slice(2);
    const nextValue = inlineValue ?? argv[index + 1];
    if (inlineValue === undefined && nextValue && !nextValue.startsWith("--")) {
      config[key] = nextValue;
      index += 1;
    } else {
      config[key] = inlineValue ?? true;
    }
  }
  return config;
}

function booleanArg(value) {
  return value === true || String(value ?? "").toLowerCase() === "true";
}

function loadEnvFile(cwd = process.cwd()) {
  const envPath = path.join(cwd, ".env");
  if (!fs.existsSync(envPath)) return;
  for (const line of fs.readFileSync(envPath, "utf8").split(/\r?\n/)) {
    if (!line || line.trim().startsWith("#")) continue;
    const separatorIndex = line.indexOf("=");
    if (separatorIndex === -1) continue;
    const key = line.slice(0, separatorIndex).trim();
    const value = line.slice(separatorIndex + 1).trim();
    if (key && process.env[key] === undefined) process.env[key] = value;
  }
}

function readCsv(filePath) {
  const [headerLine, ...lines] = fs.readFileSync(filePath, "utf8").trim().split(/\r?\n/);
  const headers = headerLine.split(",");
  return lines.filter(Boolean).map((line) => {
    const values = line.split(",");
    return Object.fromEntries(headers.map((header, index) => [header, values[index] ?? ""]));
  });
}

function ensureOptionSchema(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS option_contracts (
      symbol TEXT NOT NULL,
      expiry_date TEXT NOT NULL,
      option_type TEXT NOT NULL,
      strike_price REAL NOT NULL,
      instrument_key TEXT NOT NULL UNIQUE,
      trading_symbol TEXT NOT NULL,
      exchange TEXT,
      segment TEXT,
      lot_size INTEGER NOT NULL,
      tick_size REAL,
      underlying_key TEXT,
      underlying_type TEXT,
      underlying_symbol TEXT NOT NULL,
      weekly INTEGER NOT NULL DEFAULT 0,
      source TEXT NOT NULL,
      is_active INTEGER NOT NULL DEFAULT 0,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (symbol, expiry_date, option_type, strike_price)
    );
    CREATE INDEX IF NOT EXISTS idx_option_contracts_symbol_expiry
      ON option_contracts(symbol, expiry_date, option_type, strike_price);

    CREATE TABLE IF NOT EXISTS option_expiries (
      symbol TEXT NOT NULL,
      underlying_key TEXT NOT NULL,
      expiry_date TEXT NOT NULL,
      source TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (symbol, expiry_date)
    );
    CREATE INDEX IF NOT EXISTS idx_option_expiries_symbol_date
      ON option_expiries(symbol, expiry_date);
  `);
}

function loadMarketDates(db) {
  try {
    for (const symbol of PREFERRED_CALENDAR_SYMBOLS) {
      const rows = selectMarketDateRowsForSymbol(db, symbol);
      if (rows.length) return new Set(rows.map((row) => row.date));
    }
    const rows = db
      .prepare(
        `
        SELECT DISTINCT date
        FROM ohlcv_intraday
        WHERE data_mode = 'equity_signal_proxy_1m'
        ORDER BY date
        `,
      )
      .all();
    return new Set(rows.map((row) => row.date));
  } catch {
    return new Set();
  }
}

function selectMarketDateRowsForSymbol(db, symbol) {
  const indexedSql = `
    SELECT DISTINCT date
    FROM ohlcv_intraday INDEXED BY idx_ohlcv_intraday_mode_symbol_time
    WHERE data_mode = 'equity_signal_proxy_1m'
      AND timeframe_sec = 60
      AND symbol = ?
    ORDER BY date
  `;
  const fallbackSql = `
    SELECT DISTINCT date
    FROM ohlcv_intraday
    WHERE data_mode = 'equity_signal_proxy_1m'
      AND timeframe_sec = 60
      AND symbol = ?
    ORDER BY date
  `;
  try {
    return db.prepare(indexedSql).all(symbol);
  } catch {
    return db.prepare(fallbackSql).all(symbol);
  }
}

function optionExpiryForSignal(signalTimestamp, marketDates = new Set()) {
  const signal = new Date(`${String(signalTimestamp).slice(0, 10)}T00:00:00Z`);
  for (let monthOffset = 0; monthOffset < 4; monthOffset += 1) {
    const probe = new Date(Date.UTC(signal.getUTCFullYear(), signal.getUTCMonth() + monthOffset, 1));
    const expiry = adjustToPreviousMarketDate(nominalMonthlyExpiry(probe.getUTCFullYear(), probe.getUTCMonth()), marketDates);
    if (expiry >= signal) return isoDate(expiry);
  }
  throw new Error(`Unable to resolve expiry for ${signalTimestamp}`);
}

function optionExpiryFromUpstoxExpiries(signalTimestamp, expiries = []) {
  const signalDate = String(signalTimestamp).slice(0, 10);
  const sorted = [...new Set(expiries.map(String).filter(Boolean))].sort();
  return sorted.find((expiryDate) => expiryDate >= signalDate) ?? null;
}

function nominalMonthlyExpiry(year, zeroMonth) {
  const start = new Date(Date.UTC(year, zeroMonth, 1));
  const expiryWeekday = start >= new Date("2025-09-01T00:00:00Z") ? 2 : 4;
  const cursor = new Date(Date.UTC(year, zeroMonth + 1, 0));
  while (cursor.getUTCDay() !== expiryWeekday) {
    cursor.setUTCDate(cursor.getUTCDate() - 1);
  }
  return cursor;
}

function adjustToPreviousMarketDate(expiry, marketDates) {
  if (!marketDates.size) return expiry;
  const orderedDates = [...marketDates].sort();
  if (isoDate(expiry) < orderedDates[0] || isoDate(expiry) > orderedDates[orderedDates.length - 1]) {
    return expiry;
  }
  const cursor = new Date(expiry);
  const month = cursor.getUTCMonth();
  while (cursor.getUTCMonth() === month && !marketDates.has(isoDate(cursor))) {
    cursor.setUTCDate(cursor.getUTCDate() - 1);
  }
  return cursor;
}

function isoDate(value) {
  return value.toISOString().slice(0, 10);
}

function getUnderlyingKey(db, symbol) {
  const row = db.prepare("SELECT instrument_key FROM instruments WHERE symbol = ?").get(symbol);
  return row?.instrument_key ?? null;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function errorSummary(error) {
  if (error instanceof Error) return error.message;
  return String(error);
}

async function fetchJson(url, token, options = {}) {
  const maxAttempts = Number(options.maxAttempts ?? 6);
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    let response;
    try {
      response = await fetch(url, {
        headers: { ...REQUEST_HEADERS, Authorization: `Bearer ${token}` },
      });
    } catch (error) {
      if (attempt >= maxAttempts) throw error;
      await sleep(Math.min(30_000, 1_000 * 2 ** (attempt - 1)));
      continue;
    }
    if (response.ok) return response.json();
    if (response.status !== 429 && response.status < 500) {
      const error = new Error(`HTTP ${response.status} for ${url}`);
      error.status = response.status;
      throw error;
    }
    if (attempt >= maxAttempts) {
      const error = new Error(`HTTP ${response.status} for ${url}`);
      error.status = response.status;
      throw error;
    }
    const retryAfter = Number(response.headers.get("retry-after"));
    const delayMs = Number.isFinite(retryAfter) && retryAfter > 0
      ? retryAfter * 1000
      : Math.min(30_000, 1_000 * 2 ** (attempt - 1));
    console.warn(`Retrying HTTP ${response.status} in ${Math.round(delayMs / 1000)}s: ${url}`);
    await sleep(delayMs);
  }
  throw new Error(`Unable to fetch ${url}`);
}

async function fetchExpiredOptionContracts(underlyingKey, expiryDate, token) {
  const url = new URL(EXPIRED_OPTION_CONTRACT_URL);
  url.searchParams.set("instrument_key", underlyingKey);
  url.searchParams.set("expiry_date", expiryDate);
  const payload = await fetchJson(url.toString(), token);
  return payload?.data ?? [];
}

async function fetchExpiredExpiries(underlyingKey, token) {
  const url = new URL(EXPIRED_EXPIRIES_URL);
  url.searchParams.set("instrument_key", underlyingKey);
  const payload = await fetchJson(url.toString(), token);
  return payload?.data ?? [];
}

async function fetchExpiredOptionCandles(instrumentKey, token, fromDate, toDate) {
  const encodedKey = encodeURIComponent(instrumentKey);
  const url = `${EXPIRED_HISTORICAL_CANDLES_BASE_URL}/${encodedKey}/1minute/${toDate}/${fromDate}`;
  const payload = await fetchJson(url, token);
  return payload?.data?.candles ?? [];
}

function normalizeOptionContract(symbol, expiryDate, row) {
  return {
    symbol,
    expiryDate,
    optionType: row.instrument_type,
    strikePrice: Number(row.strike_price),
    instrumentKey: row.instrument_key,
    tradingSymbol: row.trading_symbol,
    exchange: row.exchange ?? "NSE",
    segment: row.segment ?? "NSE_FO",
    lotSize: Number(row.lot_size ?? row.minimum_lot ?? 1),
    tickSize: Number(row.tick_size ?? 0),
    underlyingKey: row.underlying_key ?? null,
    underlyingType: row.underlying_type ?? "EQUITY",
    underlyingSymbol: row.underlying_symbol ?? symbol,
    weekly: row.weekly ? 1 : 0,
    source: "upstox_expired_option_v2",
    isActive: 0,
  };
}

function upsertOptionContracts(db, rows) {
  const insert = db.prepare(`
    INSERT INTO option_contracts (
      symbol, expiry_date, option_type, strike_price, instrument_key, trading_symbol, exchange, segment,
      lot_size, tick_size, underlying_key, underlying_type, underlying_symbol, weekly, source, is_active, updated_at
    ) VALUES (
      @symbol, @expiryDate, @optionType, @strikePrice, @instrumentKey, @tradingSymbol, @exchange, @segment,
      @lotSize, @tickSize, @underlyingKey, @underlyingType, @underlyingSymbol, @weekly, @source, @isActive, @updatedAt
    )
    ON CONFLICT(symbol, expiry_date, option_type, strike_price) DO UPDATE SET
      instrument_key = excluded.instrument_key,
      trading_symbol = excluded.trading_symbol,
      lot_size = excluded.lot_size,
      tick_size = excluded.tick_size,
      source = excluded.source,
      updated_at = excluded.updated_at
  `);
  const now = new Date().toISOString();
  const transaction = db.transaction((items) => {
    for (const item of items) insert.run({ ...item, updatedAt: now });
  });
  transaction(rows);
}

function upsertOptionExpiries(db, symbol, underlyingKey, expiries) {
  const insert = db.prepare(`
    INSERT INTO option_expiries (
      symbol, underlying_key, expiry_date, source, updated_at
    ) VALUES (
      @symbol, @underlyingKey, @expiryDate, @source, @updatedAt
    )
    ON CONFLICT(symbol, expiry_date) DO UPDATE SET
      underlying_key = excluded.underlying_key,
      source = excluded.source,
      updated_at = excluded.updated_at
  `);
  const now = new Date().toISOString();
  const rows = expiries.map((expiryDate) => ({
    symbol,
    underlyingKey,
    expiryDate: String(expiryDate),
    source: "upstox_expired_expiries_v2",
    updatedAt: now,
  }));
  const transaction = db.transaction((items) => {
    for (const item of items) insert.run(item);
  });
  transaction(rows);
}

function nearestAtmContracts(contracts, underlyingPrice) {
  const legs = [];
  for (const optionType of ["CE", "PE"]) {
    const candidates = contracts.filter((contract) => contract.optionType === optionType);
    if (!candidates.length) continue;
    candidates.sort((left, right) =>
      Math.abs(left.strikePrice - underlyingPrice) - Math.abs(right.strikePrice - underlyingPrice) ||
      left.strikePrice - right.strikePrice,
    );
    legs.push(candidates[0]);
  }
  return legs;
}

function writeOptionCandles(db, contract, candles) {
  const insert = db.prepare(`
    INSERT INTO ohlcv_intraday (
      symbol, timestamp, date, timeframe_sec, open, high, low, close, volume, open_interest,
      instrument_key, trading_symbol, market_segment, instrument_type, contract_expiry, lot_size, source, data_mode
    ) VALUES (
      @symbol, @timestamp, @date, 60, @open, @high, @low, @close, @volume, @openInterest,
      @instrumentKey, @tradingSymbol, 'NSE_FO', @optionType, @expiryDate, @lotSize, @source, @dataMode
    )
    ON CONFLICT(instrument_key, timestamp, timeframe_sec, data_mode) DO UPDATE SET
      open = excluded.open,
      high = excluded.high,
      low = excluded.low,
      close = excluded.close,
      volume = excluded.volume,
      open_interest = excluded.open_interest,
      trading_symbol = excluded.trading_symbol,
      source = excluded.source
  `);
  const rows = candles.map(([timestamp, open, high, low, close, volume, openInterest]) => ({
    symbol: contract.symbol,
    timestamp: String(timestamp),
    date: String(timestamp).slice(0, 10),
    open: Number(open),
    high: Number(high),
    low: Number(low),
    close: Number(close),
    volume: Number(volume ?? 0),
    openInterest: Number(openInterest ?? 0),
    instrumentKey: contract.instrumentKey,
    tradingSymbol: contract.tradingSymbol,
    optionType: contract.optionType,
    expiryDate: contract.expiryDate,
    lotSize: contract.lotSize,
    source: "upstox_expired_option_v2",
    dataMode: OPTION_DATA_MODE,
  }));
  const transaction = db.transaction((items) => {
    for (const item of items) insert.run(item);
  });
  transaction(rows);
  return rows.length;
}

function optionWindowAlreadyCached(db, instrumentKey, fromDate, toDate) {
  const row = db
    .prepare(
      `
      SELECT COUNT(*) AS rows
      FROM ohlcv_intraday
      WHERE instrument_key = ?
        AND data_mode = ?
        AND date >= ?
        AND date <= ?
      `,
    )
    .get(instrumentKey, OPTION_DATA_MODE, fromDate, toDate);
  return Number(row?.rows ?? 0) > 0;
}

function pruneOptionDataOutsideRange(db, fromDate, toDate) {
  if (!fromDate || !toDate) return { candles: 0, contracts: 0, expiries: 0 };
  const candles = db
    .prepare(
      `
      DELETE FROM ohlcv_intraday
      WHERE data_mode = ?
        AND (date < ? OR date > ?)
      `,
    )
    .run(OPTION_DATA_MODE, fromDate, toDate).changes;
  const contracts = db
    .prepare(
      `
      DELETE FROM option_contracts
      WHERE expiry_date < ? OR expiry_date > ?
      `,
    )
    .run(fromDate, toDate).changes;
  const expiries = db
    .prepare(
      `
      DELETE FROM option_expiries
      WHERE expiry_date < ? OR expiry_date > ?
      `,
    )
    .run(fromDate, toDate).changes;
  return { candles, contracts, expiries };
}

async function mapWithConcurrency(items, concurrency, worker) {
  let cursor = 0;
  const workers = Array.from({ length: Math.max(1, concurrency) }, async () => {
    while (cursor < items.length) {
      const index = cursor;
      cursor += 1;
      await worker(items[index], index);
    }
  });
  await Promise.all(workers);
}

async function main() {
  loadEnvFile();
  const args = parseCliArgs();
  const token = args.token ?? process.env.UPSTOX_ACCESS_TOKEN;
  if (!token) throw new Error("Missing UPSTOX_ACCESS_TOKEN. Set it in .env or pass --token.");
  const db = new Database(path.resolve(args.db ?? "data/nse_data.db"));
  ensureOptionSchema(db);
  const marketDates = loadMarketDates(db);
  const reportPath = path.resolve(args.report ?? "reports/intraday-volume-spike-bucketed-all.csv");
  const limitSignals = Number(args["limit-signals"] ?? 200);
  const requestDelayMs = Number(args["request-delay-ms"] ?? 150);
  const refresh = booleanArg(args.refresh);
  const failOnWindowError = booleanArg(args["fail-on-window-error"]);
  const retainFromDate = args["retain-from-date"] ? String(args["retain-from-date"]) : "";
  const retainToDate = args["retain-to-date"] ? String(args["retain-to-date"]) : "";
  const symbols = args.symbols
    ? new Set(String(args.symbols).split(/[,\s]+/).map((item) => item.trim().toUpperCase()).filter(Boolean))
    : null;
  let signals = readCsv(reportPath);
  if (symbols) signals = signals.filter((row) => symbols.has(String(row.symbol).toUpperCase()));
  if (args["from-date"]) {
    const fromDate = String(args["from-date"]);
    signals = signals.filter((row) => String(row.signal_timestamp).slice(0, 10) >= fromDate);
  }
  if (args["to-date"]) {
    const toDate = String(args["to-date"]);
    signals = signals.filter((row) => String(row.signal_timestamp).slice(0, 10) <= toDate);
  }
  if (limitSignals > 0) signals = signals.slice(0, limitSignals);

  const chainCache = new Map();
  const expiryCache = new Map();
  const optionWork = new Map();
  let cachedWindows = 0;
  for (const signal of signals) {
    const symbol = String(signal.symbol).toUpperCase();
    const underlyingKey = getUnderlyingKey(db, symbol);
    if (!underlyingKey) {
      console.warn(`Missing underlying instrument key for ${symbol}`);
      continue;
    }
    if (!expiryCache.has(symbol)) {
      const expiries = await fetchExpiredExpiries(underlyingKey, token);
      upsertOptionExpiries(db, symbol, underlyingKey, expiries);
      expiryCache.set(symbol, expiries);
      console.log(`${symbol} -> ${expiries.length} Upstox option expiries`);
    }
    const expiryDate =
      optionExpiryFromUpstoxExpiries(signal.signal_timestamp, expiryCache.get(symbol)) ??
      optionExpiryForSignal(signal.signal_timestamp, marketDates);
    const chainKey = `${symbol}|${expiryDate}`;
    if (!chainCache.has(chainKey)) {
      const rawContracts = await fetchExpiredOptionContracts(underlyingKey, expiryDate, token);
      const contracts = rawContracts
        .map((row) => normalizeOptionContract(symbol, expiryDate, row))
        .filter((row) => ["CE", "PE"].includes(row.optionType));
      upsertOptionContracts(db, contracts);
      chainCache.set(chainKey, contracts);
      console.log(`${chainKey} -> ${contracts.length} contracts`);
    }
    const contracts = chainCache.get(chainKey);
    for (const contract of nearestAtmContracts(contracts, Number(signal.entry_price))) {
      const fromDate = String(signal.entry_timestamp).slice(0, 10);
      const toDate = String(signal.exit_timestamp || signal.entry_timestamp).slice(0, 10);
      if (!refresh && optionWindowAlreadyCached(db, contract.instrumentKey, fromDate, toDate)) {
        cachedWindows += 1;
        continue;
      }
      optionWork.set(`${contract.instrumentKey}|${fromDate}|${toDate}`, { contract, fromDate, toDate });
    }
  }

  const work = [...optionWork.values()];
  console.log(`Fetching ${work.length} unique ATM option candle windows from ${signals.length} signals (${cachedWindows} cached windows skipped).`);
  let fetchedWindows = 0;
  let failedWindows = 0;
  let writtenRows = 0;
  await mapWithConcurrency(work, Number(args.concurrency ?? 2), async ({ contract, fromDate, toDate }, index) => {
    try {
      const candles = await fetchExpiredOptionCandles(contract.instrumentKey, token, fromDate, toDate);
      const rows = writeOptionCandles(db, contract, candles);
      fetchedWindows += 1;
      writtenRows += rows;
      console.log(`[${index + 1}/${work.length}] ${contract.tradingSymbol} ${fromDate}..${toDate} -> ${rows} candles`);
    } catch (error) {
      failedWindows += 1;
      console.warn(
        `[${index + 1}/${work.length}] SKIP ${contract.tradingSymbol} ${fromDate}..${toDate} -> ${errorSummary(error)}`,
      );
      if (failOnWindowError) throw error;
    } finally {
      if (requestDelayMs > 0) await sleep(requestDelayMs);
    }
  });
  console.log(
    `Option candle summary: ${fetchedWindows} fetched, ${cachedWindows} already cached, ` +
      `${failedWindows} failed, ${writtenRows} candles written.`,
  );
  if (retainFromDate && retainToDate) {
    const pruned = pruneOptionDataOutsideRange(db, retainFromDate, retainToDate);
    console.log(
      `Pruned option data outside ${retainFromDate}..${retainToDate}: ` +
        `${pruned.candles} candles, ${pruned.contracts} contracts, ${pruned.expiries} expiries`,
    );
  }
  db.close();
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack : error);
  process.exitCode = 1;
});
