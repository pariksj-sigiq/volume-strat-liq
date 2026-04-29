import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import zlib from "node:zlib";

import Database from "better-sqlite3";

import { BENCHMARK_SYMBOL, FNO_STOCKS } from "./universe.mjs";

const INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz";
const HISTORICAL_CANDLES_BASE_URL = "https://api.upstox.com/v3/historical-candle";
const EXPIRED_EXPIRIES_URL = "https://api.upstox.com/v2/expired-instruments/expiries";
const EXPIRED_FUTURE_CONTRACT_URL = "https://api.upstox.com/v2/expired-instruments/future/contract";
const EXPIRED_HISTORICAL_CANDLES_BASE_URL = "https://api.upstox.com/v2/expired-instruments/historical-candle";
const DEFAULT_LOOKBACK_YEARS = 5;
const DEFAULT_INTRADAY_TIMEFRAME_SEC = 60;
const INTRADAY_DATA_MODES = {
  equitySignalProxy: "equity_signal_proxy_1m",
  futures: "futures_1m",
  options: "options_1m",
};
const REQUEST_HEADERS = {
  Accept: "application/json",
  "Content-Type": "application/json",
  "User-Agent": "Mozilla/5.0",
};
const DEPRECATED_ALIAS_SYMBOLS = new Map([
  ["ADANITOTAL", "ATGL"],
  ["ZOMATO", "ETERNAL"],
]);
const INSTRUMENT_ALIASES = new Map([
  ["INFY", "INFOSYS"],
  ["INDIGO", "INTERGLOBE"],
  ["LTM", "LTIM"],
  ["TMCV", "TATAMOTORS"],
]);

function normalizeSymbol(value) {
  return String(value ?? "")
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");
}

function parseCliArgs(argv = process.argv.slice(2)) {
  const config = {};
  for (let index = 0; index < argv.length; index += 1) {
    const part = argv[index];
    if (!part.startsWith("--")) {
      continue;
    }

    const [flag, inlineValue] = part.split("=", 2);
    const key = flag.slice(2);
    const nextValue = inlineValue ?? argv[index + 1];
    if (inlineValue === undefined && nextValue && !nextValue.startsWith("--")) {
      config[key] = nextValue;
      index += 1;
      continue;
    }
    config[key] = inlineValue ?? true;
  }
  return config;
}

function parseSymbolList(value, fallbackSymbols) {
  if (!value || value === true) {
    return fallbackSymbols;
  }
  const symbols = String(value)
    .split(/[,\s]+/)
    .map((item) => item.trim().toUpperCase())
    .filter(Boolean);
  return symbols.length > 0 ? symbols : fallbackSymbols;
}

function normalizeIntradayMode(value = "futures") {
  const normalized = String(value).toLowerCase().replace(/_/g, "-");
  if (["future", "futures", "futures-1m"].includes(normalized)) {
    return "futures";
  }
  if (["equity", "equity-signal-proxy", "equity-signal-proxy-1m"].includes(normalized)) {
    return "equity_signal_proxy";
  }
  if (["all", "both"].includes(normalized)) {
    return "both";
  }
  throw new Error(`Unsupported intraday mode: ${value}. Use futures, equity-signal-proxy, or both.`);
}

function loadEnvFile(cwd = process.cwd()) {
  const envPath = path.join(cwd, ".env");
  if (!fs.existsSync(envPath)) {
    return;
  }

  const content = fs.readFileSync(envPath, "utf8");
  for (const line of content.split(/\r?\n/)) {
    if (!line || line.trim().startsWith("#")) {
      continue;
    }
    const separatorIndex = line.indexOf("=");
    if (separatorIndex === -1) {
      continue;
    }
    const key = line.slice(0, separatorIndex).trim();
    const value = line.slice(separatorIndex + 1).trim();
    if (key && process.env[key] === undefined) {
      process.env[key] = value;
    }
  }
}

function ensureDirectory(filePath) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
}

function isoDate(value) {
  if (value instanceof Date) {
    return value.toISOString().slice(0, 10);
  }
  return String(value ?? "").slice(0, 10);
}

function parseDateOnly(value, label) {
  const date = new Date(`${value}T00:00:00Z`);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(value)) || Number.isNaN(date.getTime()) || isoDate(date) !== value) {
    throw new Error(`Invalid ${label}: ${value}`);
  }
  return date;
}

function addUtcDays(date, days) {
  const next = new Date(date);
  next.setUTCDate(next.getUTCDate() + days);
  return next;
}

function calendarMonthEnd(date) {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 1, 0));
}

function parseExpiryValue(value) {
  if (typeof value === "number") {
    return new Date(value).toISOString().slice(0, 10);
  }
  if (typeof value === "string" && /^\d{4}-\d{2}-\d{2}/.test(value)) {
    return value.slice(0, 10);
  }
  if (typeof value === "string" && /^\d{2}-\d{2}-\d{4}$/.test(value)) {
    const [day, month, year] = value.split("-");
    return `${year}-${month}-${day}`;
  }
  return isoDate(value);
}

export function buildFetchWindow(toDate = new Date().toISOString().slice(0, 10), years = DEFAULT_LOOKBACK_YEARS) {
  const end = new Date(`${toDate}T00:00:00Z`);
  if (Number.isNaN(end.getTime())) {
    throw new Error(`Invalid toDate: ${toDate}`);
  }
  const start = new Date(end);
  start.setUTCFullYear(start.getUTCFullYear() - Number(years));
  return {
    fromDate: start.toISOString().slice(0, 10),
    toDate,
    years: Number(years),
  };
}

export function buildIntradayWindows(fromDate, toDate, timeframeSec = DEFAULT_INTRADAY_TIMEFRAME_SEC) {
  if (Number(timeframeSec) !== DEFAULT_INTRADAY_TIMEFRAME_SEC) {
    throw new Error("Only 1-minute intraday windows are supported right now.");
  }

  const start = parseDateOnly(fromDate, "fromDate");
  const end = parseDateOnly(toDate, "toDate");
  if (start > end) {
    throw new Error(`fromDate must be before or equal to toDate: ${fromDate} > ${toDate}`);
  }

  const windows = [];
  for (let cursor = start; cursor <= end; cursor = addUtcDays(calendarMonthEnd(cursor), 1)) {
    const chunkEnd = calendarMonthEnd(cursor) < end ? calendarMonthEnd(cursor) : end;
    windows.push({
      fromDate: isoDate(cursor),
      toDate: isoDate(chunkEnd),
      timeframeSec: DEFAULT_INTRADAY_TIMEFRAME_SEC,
    });
  }
  return windows;
}

export function normalizeDailyCandle(symbol, instrumentKey, candle) {
  const [timestamp, open, high, low, close, volume, openInterest] = candle;
  return {
    symbol,
    instrumentKey,
    date: String(timestamp).slice(0, 10),
    open: Number(open),
    high: Number(high),
    low: Number(low),
    close: Number(close),
    adjClose: Number(close),
    volume: Number(volume ?? 0),
    openInterest: Number(openInterest ?? 0),
  };
}

function inferIntradayDataMode(instrument) {
  const segment = instrument.marketSegment ?? instrument.segment;
  const instrumentType = instrument.instrumentType ?? instrument.instrument_type;
  if (segment === "NSE_EQ" || instrumentType === "EQ") {
    return INTRADAY_DATA_MODES.equitySignalProxy;
  }
  if (segment === "NSE_FO" && ["CE", "PE"].includes(instrumentType)) {
    return INTRADAY_DATA_MODES.options;
  }
  if (segment === "NSE_FO" || instrumentType === "FUT") {
    return INTRADAY_DATA_MODES.futures;
  }
  throw new Error(`Unsupported intraday instrument type: ${segment ?? "unknown"} ${instrumentType ?? "unknown"}`);
}

export function normalizeIntradayCandle(instrument, candle, options = {}) {
  const [timestamp, open, high, low, close, volume, openInterest] = candle;
  const timestampText = String(timestamp);
  return {
    symbol: instrument.symbol,
    timestamp: timestampText,
    date: timestampText.slice(0, 10),
    timeframeSec: Number(options.timeframeSec ?? DEFAULT_INTRADAY_TIMEFRAME_SEC),
    open: Number(open),
    high: Number(high),
    low: Number(low),
    close: Number(close),
    volume: Number(volume ?? 0),
    openInterest: Number(openInterest ?? 0),
    instrumentKey: instrument.instrumentKey ?? instrument.instrument_key,
    tradingSymbol:
      instrument.tradingSymbol ??
      instrument.trading_symbol ??
      instrument.shortName ??
      instrument.short_name ??
      instrument.symbol,
    marketSegment: instrument.marketSegment ?? instrument.segment,
    instrumentType: instrument.instrumentType ?? instrument.instrument_type,
    contractExpiry: instrument.contractExpiry ?? instrument.expiryDate ?? null,
    lotSize: Number(instrument.lotSize ?? instrument.lot_size ?? 1),
    source: instrument.source ?? "upstox_v3",
    dataMode: options.dataMode ?? inferIntradayDataMode(instrument),
  };
}

function canonicalSymbolFromInstrument(instrument, wantedSymbols) {
  const candidates = [
    instrument.underlying_symbol,
    instrument.asset_symbol,
    instrument.trading_symbol,
    instrument.name,
  ];
  for (const candidate of candidates) {
    const normalized = normalizeSymbol(candidate);
    const canonical = wantedSymbols.get(normalized) ?? INSTRUMENT_ALIASES.get(normalized);
    if (canonical) {
      return canonical;
    }
  }
  return null;
}

export function selectUniverseInstruments(instruments, wantedSymbols) {
  const exactMatches = new Map(
    [...wantedSymbols].map((symbol) => [normalizeSymbol(symbol), symbol]),
  );
  const symbolMap = new Map();
  for (const instrument of instruments) {
    if (instrument.segment === "NSE_EQ" && instrument.instrument_type === "EQ") {
      const normalized = normalizeSymbol(instrument.trading_symbol);
      const canonicalSymbol = exactMatches.get(normalized) ?? INSTRUMENT_ALIASES.get(normalized);
      if (canonicalSymbol && wantedSymbols.has(canonicalSymbol)) {
        symbolMap.set(canonicalSymbol, {
          symbol: canonicalSymbol,
          instrumentKey: instrument.instrument_key,
          exchange: instrument.exchange,
          segment: instrument.segment,
          instrumentType: instrument.instrument_type,
          isin: instrument.isin ?? null,
          name: instrument.name ?? instrument.short_name ?? normalized,
          shortName: instrument.short_name ?? instrument.name ?? normalized,
          lotSize: Number(instrument.lot_size ?? 1),
          tickSize: Number(instrument.tick_size ?? 0),
        });
      }
      continue;
    }

    if (instrument.segment === "NSE_INDEX" && instrument.instrument_type === "INDEX") {
      const normalizedSymbol = normalizeSymbol(instrument.trading_symbol);
      const normalizedName = normalizeSymbol(instrument.name);
      const normalizedKey = normalizeSymbol(instrument.instrument_key);
      if (
        normalizedSymbol === BENCHMARK_SYMBOL ||
        normalizedName === BENCHMARK_SYMBOL ||
        normalizedKey.endsWith(BENCHMARK_SYMBOL)
      ) {
        symbolMap.set(BENCHMARK_SYMBOL, {
          symbol: BENCHMARK_SYMBOL,
          instrumentKey: instrument.instrument_key,
          exchange: instrument.exchange,
          segment: instrument.segment,
          instrumentType: instrument.instrument_type,
          isin: null,
          name: instrument.name ?? instrument.trading_symbol,
          shortName: instrument.trading_symbol,
          lotSize: 1,
          tickSize: Number(instrument.tick_size ?? 0),
        });
      }
    }
  }

  return [...symbolMap.values()].sort((left, right) => left.symbol.localeCompare(right.symbol));
}

export function selectCurrentFutureContracts(instruments, wantedSymbols, asOfDate = new Date().toISOString().slice(0, 10)) {
  const exactMatches = new Map(
    [...wantedSymbols].map((symbol) => [normalizeSymbol(symbol), symbol]),
  );
  const selected = [];
  for (const instrument of instruments) {
    if (instrument.segment !== "NSE_FO" || instrument.instrument_type !== "FUT") {
      continue;
    }
    const symbol = canonicalSymbolFromInstrument(instrument, exactMatches);
    if (!symbol || !wantedSymbols.has(symbol)) {
      continue;
    }
    const expiryDate = parseExpiryValue(instrument.expiry);
    if (expiryDate < asOfDate) {
      continue;
    }
    selected.push({
      symbol,
      expiryDate,
      instrumentKey: instrument.instrument_key,
      tradingSymbol: instrument.trading_symbol,
      exchange: instrument.exchange,
      segment: instrument.segment,
      instrumentType: instrument.instrument_type,
      lotSize: Number(instrument.lot_size ?? instrument.minimum_lot ?? 1),
      tickSize: Number(instrument.tick_size ?? 0),
      underlyingKey: instrument.underlying_key ?? instrument.asset_key ?? null,
      underlyingType: instrument.underlying_type ?? instrument.asset_type ?? null,
      underlyingSymbol: symbol,
      source: "upstox_active_bod",
      isActive: 1,
    });
  }
  return selected.sort((left, right) =>
    left.symbol.localeCompare(right.symbol) || left.expiryDate.localeCompare(right.expiryDate),
  );
}

function selectIntradayTargets(instruments, selectedEquities, wantedSymbols, mode, asOfDate) {
  const targets = [];
  if (mode === "equity_signal_proxy" || mode === "both") {
    targets.push(
      ...selectedEquities
        .filter((instrument) => instrument.symbol !== BENCHMARK_SYMBOL && instrument.segment === "NSE_EQ")
        .map((instrument) => ({
          ...instrument,
          tradingSymbol: instrument.symbol,
          source: "upstox_v3",
        })),
    );
  }
  if (mode === "futures" || mode === "both") {
    targets.push(...selectCurrentFutureContracts(instruments, wantedSymbols, asOfDate));
  }
  return targets.sort((left, right) =>
    left.symbol.localeCompare(right.symbol) ||
    String(left.contractExpiry ?? left.expiryDate ?? "").localeCompare(String(right.contractExpiry ?? right.expiryDate ?? "")) ||
    String(left.tradingSymbol ?? left.shortName ?? "").localeCompare(String(right.tradingSymbol ?? right.shortName ?? "")),
  );
}

export function createDb(dbPath) {
  ensureDirectory(dbPath);
  const db = new Database(dbPath);
  db.pragma("journal_mode = WAL");
  db.pragma("synchronous = NORMAL");
  db.pragma("foreign_keys = ON");

  db.exec(`
    CREATE TABLE IF NOT EXISTS instruments (
      symbol TEXT PRIMARY KEY,
      instrument_key TEXT NOT NULL UNIQUE,
      exchange TEXT,
      segment TEXT,
      instrument_type TEXT,
      isin TEXT,
      name TEXT,
      short_name TEXT,
      lot_size INTEGER,
      tick_size REAL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS stocks (
      symbol TEXT PRIMARY KEY,
      theme TEXT,
      sub_theme TEXT,
      is_active INTEGER NOT NULL DEFAULT 1
    );

    CREATE TABLE IF NOT EXISTS ohlcv_daily (
      symbol TEXT NOT NULL,
      date TEXT NOT NULL,
      open REAL NOT NULL,
      high REAL NOT NULL,
      low REAL NOT NULL,
      close REAL NOT NULL,
      adj_close REAL NOT NULL,
      volume INTEGER NOT NULL,
      instrument_key TEXT NOT NULL,
      open_interest INTEGER NOT NULL DEFAULT 0,
      source TEXT NOT NULL DEFAULT 'upstox_v3',
      PRIMARY KEY (symbol, date)
    );

    CREATE TABLE IF NOT EXISTS futures_contracts (
      symbol TEXT NOT NULL,
      expiry_date TEXT NOT NULL,
      instrument_key TEXT NOT NULL UNIQUE,
      trading_symbol TEXT NOT NULL,
      exchange TEXT,
      segment TEXT,
      instrument_type TEXT,
      lot_size INTEGER NOT NULL,
      tick_size REAL,
      underlying_key TEXT NOT NULL,
      underlying_type TEXT,
      underlying_symbol TEXT NOT NULL,
      source TEXT NOT NULL,
      is_active INTEGER NOT NULL DEFAULT 0,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (symbol, expiry_date)
    );

    CREATE TABLE IF NOT EXISTS ohlcv_futures_daily (
      symbol TEXT NOT NULL,
      expiry_date TEXT NOT NULL,
      date TEXT NOT NULL,
      open REAL NOT NULL,
      high REAL NOT NULL,
      low REAL NOT NULL,
      close REAL NOT NULL,
      adj_close REAL NOT NULL,
      volume INTEGER NOT NULL,
      open_interest INTEGER NOT NULL DEFAULT 0,
      instrument_key TEXT NOT NULL,
      trading_symbol TEXT NOT NULL,
      lot_size INTEGER NOT NULL,
      source TEXT NOT NULL,
      PRIMARY KEY (symbol, expiry_date, date)
    );

    CREATE TABLE IF NOT EXISTS ohlcv_intraday (
      symbol TEXT NOT NULL,
      timestamp TEXT NOT NULL,
      date TEXT NOT NULL,
      timeframe_sec INTEGER NOT NULL,
      open REAL NOT NULL,
      high REAL NOT NULL,
      low REAL NOT NULL,
      close REAL NOT NULL,
      volume INTEGER NOT NULL,
      open_interest INTEGER NOT NULL DEFAULT 0,
      instrument_key TEXT NOT NULL,
      trading_symbol TEXT NOT NULL,
      market_segment TEXT NOT NULL,
      instrument_type TEXT NOT NULL,
      contract_expiry TEXT,
      lot_size INTEGER NOT NULL,
      source TEXT NOT NULL,
      data_mode TEXT NOT NULL,
      PRIMARY KEY (instrument_key, timestamp, timeframe_sec, data_mode)
    );

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

    CREATE TABLE IF NOT EXISTS option_expiries (
      symbol TEXT NOT NULL,
      underlying_key TEXT NOT NULL,
      expiry_date TEXT NOT NULL,
      source TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (symbol, expiry_date)
    );

    CREATE INDEX IF NOT EXISTS idx_ohlcv_daily_symbol_date ON ohlcv_daily(symbol, date);
    CREATE INDEX IF NOT EXISTS idx_futures_contracts_symbol_expiry ON futures_contracts(symbol, expiry_date);
    CREATE INDEX IF NOT EXISTS idx_ohlcv_futures_symbol_date ON ohlcv_futures_daily(symbol, date);
    CREATE INDEX IF NOT EXISTS idx_ohlcv_intraday_symbol_date ON ohlcv_intraday(symbol, date);
    CREATE INDEX IF NOT EXISTS idx_ohlcv_intraday_mode_date ON ohlcv_intraday(data_mode, date);
    CREATE INDEX IF NOT EXISTS idx_ohlcv_intraday_mode_symbol_time
      ON ohlcv_intraday(data_mode, timeframe_sec, symbol, timestamp);
    CREATE INDEX IF NOT EXISTS idx_option_contracts_symbol_expiry
      ON option_contracts(symbol, expiry_date, option_type, strike_price);
    CREATE INDEX IF NOT EXISTS idx_option_expiries_symbol_date
      ON option_expiries(symbol, expiry_date);
  `);

  return db;
}

async function fetchJson(url, headers = {}) {
  const response = await fetch(url, {
    headers: {
      ...REQUEST_HEADERS,
      ...headers,
    },
  });
  if (!response.ok) {
    const error = new Error(`HTTP ${response.status} for ${url}`);
    error.status = response.status;
    throw error;
  }
  return response.json();
}

async function fetchInstruments() {
  const response = await fetch(INSTRUMENTS_URL, {
    headers: REQUEST_HEADERS,
  });
  if (!response.ok) {
    throw new Error(`Unable to fetch instruments: HTTP ${response.status}`);
  }

  const compressed = Buffer.from(await response.arrayBuffer());
  const json = zlib.gunzipSync(compressed).toString("utf8");
  return JSON.parse(json);
}

function upsertUniverseMetadata(db, selectedInstruments) {
  const upsertInstrument = db.prepare(`
    INSERT INTO instruments (
      symbol, instrument_key, exchange, segment, instrument_type, isin, name, short_name, lot_size, tick_size, updated_at
    ) VALUES (
      @symbol, @instrumentKey, @exchange, @segment, @instrumentType, @isin, @name, @shortName, @lotSize, @tickSize, @updatedAt
    )
    ON CONFLICT(symbol) DO UPDATE SET
      instrument_key = excluded.instrument_key,
      exchange = excluded.exchange,
      segment = excluded.segment,
      instrument_type = excluded.instrument_type,
      isin = excluded.isin,
      name = excluded.name,
      short_name = excluded.short_name,
      lot_size = excluded.lot_size,
      tick_size = excluded.tick_size,
      updated_at = excluded.updated_at
  `);
  const upsertStock = db.prepare(`
    INSERT INTO stocks (symbol, theme, sub_theme, is_active)
    VALUES (@symbol, NULL, NULL, @isActive)
    ON CONFLICT(symbol) DO UPDATE SET is_active = excluded.is_active
  `);

  const now = new Date().toISOString();
  const transaction = db.transaction((rows) => {
    for (const row of rows) {
      upsertInstrument.run({ ...row, updatedAt: now });
      upsertStock.run({ symbol: row.symbol, isActive: 1 });
    }
    for (const symbol of FNO_STOCKS) {
      upsertStock.run({
        symbol,
        isActive: DEPRECATED_ALIAS_SYMBOLS.has(symbol) ? 0 : 1,
      });
    }
    upsertStock.run({ symbol: BENCHMARK_SYMBOL, isActive: 1 });
  });
  transaction(selectedInstruments);
}

function upsertFuturesContracts(db, contracts) {
  const upsert = db.prepare(`
    INSERT INTO futures_contracts (
      symbol, expiry_date, instrument_key, trading_symbol, exchange, segment, instrument_type,
      lot_size, tick_size, underlying_key, underlying_type, underlying_symbol, source, is_active, updated_at
    ) VALUES (
      @symbol, @expiryDate, @instrumentKey, @tradingSymbol, @exchange, @segment, @instrumentType,
      @lotSize, @tickSize, @underlyingKey, @underlyingType, @underlyingSymbol, @source, @isActive, @updatedAt
    )
    ON CONFLICT(symbol, expiry_date) DO UPDATE SET
      instrument_key = excluded.instrument_key,
      trading_symbol = excluded.trading_symbol,
      exchange = excluded.exchange,
      segment = excluded.segment,
      instrument_type = excluded.instrument_type,
      lot_size = excluded.lot_size,
      tick_size = excluded.tick_size,
      underlying_key = excluded.underlying_key,
      underlying_type = excluded.underlying_type,
      underlying_symbol = excluded.underlying_symbol,
      source = excluded.source,
      is_active = excluded.is_active,
      updated_at = excluded.updated_at
  `);
  const transaction = db.transaction((rows) => {
    const now = new Date().toISOString();
    for (const row of rows) {
      upsert.run({ ...row, updatedAt: now });
    }
  });
  transaction(contracts);
}

async function fetchDailyCandles(instrumentKey, token, fromDate, toDate) {
  const encodedKey = encodeURIComponent(instrumentKey);
  const url = `${HISTORICAL_CANDLES_BASE_URL}/${encodedKey}/days/1/${toDate}/${fromDate}`;
  const payload = await fetchJson(url, {
    Authorization: `Bearer ${token}`,
  });

  return payload?.data?.candles ?? [];
}

async function fetchIntradayCandles(instrumentKey, token, fromDate, toDate, timeframeSec = DEFAULT_INTRADAY_TIMEFRAME_SEC) {
  if (Number(timeframeSec) !== DEFAULT_INTRADAY_TIMEFRAME_SEC) {
    throw new Error("Only 1-minute intraday candles are supported right now.");
  }
  const encodedKey = encodeURIComponent(instrumentKey);
  const url = `${HISTORICAL_CANDLES_BASE_URL}/${encodedKey}/minutes/1/${toDate}/${fromDate}`;
  const payload = await fetchJson(url, {
    Authorization: `Bearer ${token}`,
  });

  return payload?.data?.candles ?? [];
}

async function fetchCurrentIntradayCandles(instrumentKey, token, timeframeSec = DEFAULT_INTRADAY_TIMEFRAME_SEC) {
  if (Number(timeframeSec) !== DEFAULT_INTRADAY_TIMEFRAME_SEC) {
    throw new Error("Only 1-minute intraday candles are supported right now.");
  }
  const encodedKey = encodeURIComponent(instrumentKey);
  const url = `${HISTORICAL_CANDLES_BASE_URL}/intraday/${encodedKey}/minutes/1`;
  const payload = await fetchJson(url, {
    Authorization: `Bearer ${token}`,
  });

  return payload?.data?.candles ?? [];
}

async function fetchExpiredExpiries(underlyingKey, token) {
  const url = new URL(EXPIRED_EXPIRIES_URL);
  url.searchParams.set("instrument_key", underlyingKey);
  const payload = await fetchJson(url.toString(), {
    Authorization: `Bearer ${token}`,
  });
  return payload?.data ?? [];
}

async function fetchExpiredFutureContract(underlyingKey, expiryDate, token) {
  const url = new URL(EXPIRED_FUTURE_CONTRACT_URL);
  url.searchParams.set("instrument_key", underlyingKey);
  url.searchParams.set("expiry_date", expiryDate);
  const payload = await fetchJson(url.toString(), {
    Authorization: `Bearer ${token}`,
  });
  return payload?.data ?? [];
}

async function fetchExpiredHistoricalCandles(contractKey, expiryDate, token, fromDate) {
  const encodedKey = encodeURIComponent(contractKey);
  const url = `${EXPIRED_HISTORICAL_CANDLES_BASE_URL}/${encodedKey}/day/${expiryDate}/${fromDate}`;
  const payload = await fetchJson(url, {
    Authorization: `Bearer ${token}`,
  });
  return payload?.data?.candles ?? [];
}

function normalizeFutureCandle(symbol, contract, candle) {
  const base = normalizeDailyCandle(symbol, contract.instrumentKey, candle);
  return {
    symbol,
    expiryDate: contract.expiryDate,
    date: base.date,
    open: base.open,
    high: base.high,
    low: base.low,
    close: base.close,
    adjClose: base.adjClose,
    volume: base.volume,
    openInterest: base.openInterest,
    instrumentKey: contract.instrumentKey,
    tradingSymbol: contract.tradingSymbol,
    lotSize: Number(contract.lotSize ?? 1),
    source: contract.source,
  };
}

function writeBenchmarkCandles(db, rows) {
  const insert = db.prepare(`
    INSERT INTO ohlcv_daily (
      symbol, date, open, high, low, close, adj_close, volume, instrument_key, open_interest, source
    ) VALUES (
      @symbol, @date, @open, @high, @low, @close, @adjClose, @volume, @instrumentKey, @openInterest, 'upstox_v3'
    )
    ON CONFLICT(symbol, date) DO UPDATE SET
      open = excluded.open,
      high = excluded.high,
      low = excluded.low,
      close = excluded.close,
      adj_close = excluded.adj_close,
      volume = excluded.volume,
      instrument_key = excluded.instrument_key,
      open_interest = excluded.open_interest,
      source = excluded.source
  `);
  const transaction = db.transaction((items) => {
    for (const item of items) {
      insert.run(item);
    }
  });
  transaction(rows);
}

function writeFutureCandles(db, rows) {
  const insert = db.prepare(`
    INSERT INTO ohlcv_futures_daily (
      symbol, expiry_date, date, open, high, low, close, adj_close, volume, open_interest,
      instrument_key, trading_symbol, lot_size, source
    ) VALUES (
      @symbol, @expiryDate, @date, @open, @high, @low, @close, @adjClose, @volume, @openInterest,
      @instrumentKey, @tradingSymbol, @lotSize, @source
    )
    ON CONFLICT(symbol, expiry_date, date) DO UPDATE SET
      open = excluded.open,
      high = excluded.high,
      low = excluded.low,
      close = excluded.close,
      adj_close = excluded.adj_close,
      volume = excluded.volume,
      open_interest = excluded.open_interest,
      instrument_key = excluded.instrument_key,
      trading_symbol = excluded.trading_symbol,
      lot_size = excluded.lot_size,
      source = excluded.source
  `);
  const transaction = db.transaction((items) => {
    for (const item of items) {
      insert.run(item);
    }
  });
  transaction(rows);
}

function writeIntradayCandles(db, rows) {
  const insert = db.prepare(`
    INSERT INTO ohlcv_intraday (
      symbol, timestamp, date, timeframe_sec, open, high, low, close, volume, open_interest,
      instrument_key, trading_symbol, market_segment, instrument_type, contract_expiry, lot_size, source, data_mode
    ) VALUES (
      @symbol, @timestamp, @date, @timeframeSec, @open, @high, @low, @close, @volume, @openInterest,
      @instrumentKey, @tradingSymbol, @marketSegment, @instrumentType, @contractExpiry, @lotSize, @source, @dataMode
    )
    ON CONFLICT(instrument_key, timestamp, timeframe_sec, data_mode) DO UPDATE SET
      symbol = excluded.symbol,
      date = excluded.date,
      open = excluded.open,
      high = excluded.high,
      low = excluded.low,
      close = excluded.close,
      volume = excluded.volume,
      open_interest = excluded.open_interest,
      trading_symbol = excluded.trading_symbol,
      market_segment = excluded.market_segment,
      instrument_type = excluded.instrument_type,
      contract_expiry = excluded.contract_expiry,
      lot_size = excluded.lot_size,
      source = excluded.source
  `);
  const transaction = db.transaction((items) => {
    for (const item of items) {
      insert.run(item);
    }
  });
  transaction(rows);
}

function hasIntradayRows(db, target, window, timeframeSec = DEFAULT_INTRADAY_TIMEFRAME_SEC) {
  const row = db
    .prepare(`
      SELECT 1
      FROM ohlcv_intraday
      WHERE instrument_key = ?
        AND data_mode = ?
        AND timeframe_sec = ?
        AND date BETWEEN ? AND ?
      LIMIT 1
    `)
    .get(
      target.instrumentKey,
      inferIntradayDataMode(target),
      Number(timeframeSec),
      window.fromDate,
      window.toDate,
    );
  return row != null;
}

function delay(ms) {
  if (!Number.isFinite(ms) || ms <= 0) {
    return Promise.resolve();
  }
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function mapWithConcurrency(items, concurrency, worker) {
  const results = new Array(items.length);
  let cursor = 0;

  async function runNext() {
    while (true) {
      const current = cursor;
      cursor += 1;
      if (current >= items.length) {
        return;
      }
      results[current] = await worker(items[current], current);
    }
  }

  const workers = Array.from({ length: Math.max(1, concurrency) }, () => runNext());
  await Promise.all(workers);
  return results;
}

async function fetchWithRetry(task, retries = 8) {
  let lastError;
  for (let attempt = 1; attempt <= retries; attempt += 1) {
    try {
      return await task();
    } catch (error) {
      lastError = error;
      if (attempt === retries) {
        break;
      }
      const isRateLimit = Number(error?.status ?? 0) === 429;
      const delayMs = isRateLimit ? 5000 * attempt : 300 * 2 ** (attempt - 1);
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
  throw lastError;
}

function dedupeContracts(contracts) {
  const unique = new Map();
  for (const contract of contracts) {
    unique.set(`${contract.symbol}|${contract.expiryDate}`, contract);
  }
  return [...unique.values()].sort((left, right) =>
    left.symbol.localeCompare(right.symbol) || left.expiryDate.localeCompare(right.expiryDate),
  );
}

async function discoverExpiredContracts(selectedInstruments, token, window, concurrency) {
  const underlyings = selectedInstruments.filter((item) => item.symbol !== BENCHMARK_SYMBOL);
  const discoveryConcurrency = 1;
  const discovered = await mapWithConcurrency(underlyings, discoveryConcurrency, async (instrument) => {
    const expiries = await fetchWithRetry(() => fetchExpiredExpiries(instrument.instrumentKey, token));
    const filteredExpiries = expiries
      .map((value) => parseExpiryValue(value))
      .filter((expiryDate) => expiryDate >= window.fromDate);
    const contracts = await mapWithConcurrency(filteredExpiries, 1, async (expiryDate) => {
      const rows = await fetchWithRetry(() =>
        fetchExpiredFutureContract(instrument.instrumentKey, expiryDate, token),
      );
      return rows.map((row) => ({
        symbol: instrument.symbol,
        expiryDate: parseExpiryValue(row.expiry ?? expiryDate),
        instrumentKey: row.instrument_key,
        tradingSymbol: row.trading_symbol,
        exchange: row.exchange,
        segment: row.segment,
        instrumentType: row.instrument_type,
        lotSize: Number(row.lot_size ?? 1),
        tickSize: Number(row.tick_size ?? 0),
        underlyingKey: row.underlying_key ?? instrument.instrumentKey,
        underlyingType: row.underlying_type ?? "EQUITY",
        underlyingSymbol: instrument.symbol,
        source: "upstox_expired_v2",
        isActive: 0,
      }));
    });
    return contracts.flat();
  });
  return dedupeContracts(discovered.flat());
}

function summarize(db) {
  const futures = db
    .prepare(`
      SELECT
        COUNT(DISTINCT symbol) AS futures_symbols,
        COUNT(DISTINCT symbol || '|' || expiry_date) AS futures_contracts,
        COUNT(*) AS futures_rows,
        MIN(date) AS futures_min_date,
        MAX(date) AS futures_max_date
      FROM ohlcv_futures_daily
    `)
    .get();
  const benchmark = db
    .prepare(`
      SELECT
        COUNT(*) AS benchmark_rows,
        MIN(date) AS benchmark_min_date,
        MAX(date) AS benchmark_max_date
      FROM ohlcv_daily
      WHERE symbol = ?
    `)
    .get(BENCHMARK_SYMBOL);
  return {
    ...futures,
    ...benchmark,
  };
}

function summarizeIntraday(db) {
  return db
    .prepare(`
      SELECT
        COUNT(DISTINCT symbol) AS intraday_symbols,
        COUNT(DISTINCT instrument_key) AS intraday_instruments,
        COUNT(*) AS intraday_rows,
        MIN(timestamp) AS intraday_min_timestamp,
        MAX(timestamp) AS intraday_max_timestamp
      FROM ohlcv_intraday
    `)
    .get();
}

export async function runIntradayBackfill(rawArgs = {}) {
  loadEnvFile();
  const toDate = rawArgs.to ?? new Date().toISOString().slice(0, 10);
  const years = Number(rawArgs.years ?? process.env.UPSTOX_INTRADAY_LOOKBACK_YEARS ?? DEFAULT_LOOKBACK_YEARS);
  const fromDate = rawArgs.from ?? buildFetchWindow(toDate, years).fromDate;
  const activeUniverse = FNO_STOCKS.filter((symbol) => !DEPRECATED_ALIAS_SYMBOLS.has(symbol));
  const wantedSymbols = new Set(parseSymbolList(rawArgs.symbols ?? rawArgs.symbol, activeUniverse));
  const mode = normalizeIntradayMode(rawArgs.mode ?? rawArgs["data-mode"] ?? "futures");
  const args = {
    db: rawArgs.db ?? "data/nse_data.db",
    from: fromDate,
    to: toDate,
    mode,
    concurrency: Number(rawArgs.concurrency ?? process.env.UPSTOX_FETCH_CONCURRENCY ?? 8),
    token: rawArgs.token ?? process.env.UPSTOX_ACCESS_TOKEN,
    failFast: rawArgs["fail-fast"] === true || rawArgs.failFast === true,
    refresh: rawArgs.refresh === true,
    maxChunks: rawArgs["max-chunks"] === undefined ? 0 : Number(rawArgs["max-chunks"]),
    requestDelayMs: rawArgs["request-delay-ms"] === undefined ? 0 : Number(rawArgs["request-delay-ms"]),
    includeToday: rawArgs["include-today"] === true || rawArgs.includeToday === true,
  };

  if (!args.token) {
    throw new Error("Missing UPSTOX_ACCESS_TOKEN. Set it in .env or pass --token.");
  }

  const windows = buildIntradayWindows(args.from, args.to);
  const db = createDb(path.resolve(args.db));
  const instruments = await fetchInstruments();
  const selectedEquities = selectUniverseInstruments(instruments, wantedSymbols);
  upsertUniverseMetadata(db, selectedEquities);

  const selectedSymbols = new Set(selectedEquities.map((item) => item.symbol));
  const missingSymbols = [...wantedSymbols].filter((symbol) => !selectedSymbols.has(symbol));
  if (missingSymbols.length > 0) {
    console.warn(`Missing ${missingSymbols.length} symbols from instrument file: ${missingSymbols.join(", ")}`);
  }

  const targets = selectIntradayTargets(instruments, selectedEquities, wantedSymbols, args.mode, args.to);
  const futuresTargets = targets.filter((target) => target.segment === "NSE_FO" || target.instrumentType === "FUT");
  if (futuresTargets.length > 0) {
    upsertFuturesContracts(db, futuresTargets);
  }
  if (targets.length === 0) {
    throw new Error(`No intraday targets found for mode ${args.mode}.`);
  }

  const allWorkItems = targets.flatMap((target) => windows.map((window) => ({ target, window })));
  const pendingWorkItems = args.refresh
    ? allWorkItems
    : allWorkItems.filter(({ target, window }) => !hasIntradayRows(db, target, window, window.timeframeSec));
  const workItems = args.maxChunks > 0 ? pendingWorkItems.slice(0, args.maxChunks) : pendingWorkItems;
  let totalRows = 0;
  let currentDayRows = 0;
  const skippedChunks = allWorkItems.length - pendingWorkItems.length;
  const failedChunks = [];
  console.log(
    `Intraday backfill ${args.mode}: ${skippedChunks} existing chunks skipped, ${pendingWorkItems.length} pending, ${workItems.length} scheduled.`,
  );
  await mapWithConcurrency(workItems, args.concurrency, async ({ target, window }, index) => {
    const label = `[${index + 1}/${workItems.length}] ${target.symbol} ${target.tradingSymbol ?? target.shortName} ${window.fromDate}..${window.toDate}`;
    try {
      await delay(args.requestDelayMs);
      const candles = await fetchWithRetry(() =>
        fetchIntradayCandles(target.instrumentKey, args.token, window.fromDate, window.toDate, window.timeframeSec),
      );
      const normalized = candles.map((candle) =>
        normalizeIntradayCandle(target, candle, { timeframeSec: window.timeframeSec }),
      );
      writeIntradayCandles(db, normalized);
      totalRows += normalized.length;
      console.log(`${label} -> ${normalized.length} candles`);
    } catch (error) {
      const failed = {
        symbol: target.symbol,
        tradingSymbol: target.tradingSymbol ?? target.shortName ?? target.symbol,
        fromDate: window.fromDate,
        toDate: window.toDate,
        status: Number(error?.status ?? 0) || null,
        message: error instanceof Error ? error.message : String(error),
      };
      failedChunks.push(failed);
      console.warn(`${label} -> failed: ${failed.message}`);
      if (args.failFast) {
        throw error;
      }
    }
  });

  if (args.includeToday) {
    await mapWithConcurrency(targets, args.concurrency, async (target, index) => {
      const label = `[today ${index + 1}/${targets.length}] ${target.symbol} ${target.tradingSymbol ?? target.shortName}`;
      try {
        await delay(args.requestDelayMs);
        const candles = await fetchWithRetry(() =>
          fetchCurrentIntradayCandles(target.instrumentKey, args.token, DEFAULT_INTRADAY_TIMEFRAME_SEC),
        );
        const normalized = candles.map((candle) =>
          normalizeIntradayCandle(target, candle, { timeframeSec: DEFAULT_INTRADAY_TIMEFRAME_SEC }),
        );
        writeIntradayCandles(db, normalized);
        currentDayRows += normalized.length;
        console.log(`${label} -> ${normalized.length} candles`);
      } catch (error) {
        const failed = {
          symbol: target.symbol,
          tradingSymbol: target.tradingSymbol ?? target.shortName ?? target.symbol,
          fromDate: args.to,
          toDate: args.to,
          status: Number(error?.status ?? 0) || null,
          message: error instanceof Error ? error.message : String(error),
        };
        failedChunks.push(failed);
        console.warn(`${label} -> failed: ${failed.message}`);
        if (args.failFast) {
          throw error;
        }
      }
    });
  }

  return {
    window: {
      fromDate: args.from,
      toDate: args.to,
    },
    windows: windows.length,
    mode: args.mode,
    targets: targets.length,
    scheduledChunks: workItems.length,
    pendingChunks: pendingWorkItems.length,
    rowsFetched: totalRows,
    currentDayRows,
    skippedChunks,
    failedChunks: failedChunks.length,
    failedChunkSample: failedChunks.slice(0, 20),
    summary: summarizeIntraday(db),
    dbPath: path.resolve(args.db),
  };
}

export async function runBackfill(rawArgs = {}) {
  loadEnvFile();
  const args = {
    db: rawArgs.db ?? "data/nse_data.db",
    to: rawArgs.to ?? new Date().toISOString().slice(0, 10),
    years: Number(rawArgs.years ?? process.env.UPSTOX_LOOKBACK_YEARS ?? DEFAULT_LOOKBACK_YEARS),
    concurrency: Number(rawArgs.concurrency ?? process.env.UPSTOX_FETCH_CONCURRENCY ?? 8),
    token: rawArgs.token ?? process.env.UPSTOX_ACCESS_TOKEN,
  };

  if (!args.token) {
    throw new Error("Missing UPSTOX_ACCESS_TOKEN. Set it in .env or pass --token.");
  }

  const window = buildFetchWindow(args.to, args.years);
  const db = createDb(path.resolve(args.db));

  const instruments = await fetchInstruments();
  const activeUniverse = FNO_STOCKS.filter((symbol) => !DEPRECATED_ALIAS_SYMBOLS.has(symbol));
  const wantedSymbols = new Set(activeUniverse);
  const selectedInstruments = selectUniverseInstruments(instruments, wantedSymbols);
  upsertUniverseMetadata(db, selectedInstruments);

  const selectedSymbols = new Set(selectedInstruments.map((item) => item.symbol));
  const missingSymbols = activeUniverse.filter((symbol) => !selectedSymbols.has(symbol));
  if (missingSymbols.length > 0) {
    console.warn(`Missing ${missingSymbols.length} symbols from instrument file: ${missingSymbols.join(", ")}`);
  }

  const benchmarkInstrument = selectedInstruments.find((item) => item.symbol === BENCHMARK_SYMBOL);
  if (benchmarkInstrument) {
    const candles = await fetchWithRetry(() =>
      fetchDailyCandles(benchmarkInstrument.instrumentKey, args.token, window.fromDate, window.toDate),
    );
    writeBenchmarkCandles(
      db,
      candles.map((candle) => normalizeDailyCandle(BENCHMARK_SYMBOL, benchmarkInstrument.instrumentKey, candle)),
    );
  }

  const currentFutureContracts = selectCurrentFutureContracts(instruments, wantedSymbols, window.toDate);
  const expiredFutureContracts = await discoverExpiredContracts(
    selectedInstruments,
    args.token,
    window,
    args.concurrency,
  );
  const allContracts = dedupeContracts([...expiredFutureContracts, ...currentFutureContracts]);
  upsertFuturesContracts(db, allContracts);

  await mapWithConcurrency(allContracts, args.concurrency, async (contract, index) => {
    const candles = await fetchWithRetry(() => {
      if (contract.isActive) {
        return fetchDailyCandles(contract.instrumentKey, args.token, window.fromDate, window.toDate);
      }
      return fetchExpiredHistoricalCandles(contract.instrumentKey, contract.expiryDate, args.token, window.fromDate);
    });
    const normalized = candles.map((candle) => normalizeFutureCandle(contract.symbol, contract, candle));
    writeFutureCandles(db, normalized);
    console.log(
      `[${index + 1}/${allContracts.length}] ${contract.symbol} ${contract.expiryDate} -> ${normalized.length} candles`,
    );
  });

  return {
    window,
    benchmarkInstrument,
    futuresContracts: allContracts.length,
    summary: summarize(db),
    dbPath: path.resolve(args.db),
  };
}

export async function runCli(argv = process.argv.slice(2)) {
  const args = parseCliArgs(argv);
  const result = await runBackfill(args);
  console.log(JSON.stringify(result, null, 2));
}

export async function runIntradayCli(argv = process.argv.slice(2)) {
  const args = parseCliArgs(argv);
  const result = await runIntradayBackfill(args);
  console.log(JSON.stringify(result, null, 2));
}
