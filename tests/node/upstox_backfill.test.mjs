import test from "node:test";
import assert from "node:assert/strict";

import {
  buildFetchWindow,
  buildIntradayWindows,
  createDb,
  normalizeDailyCandle,
  normalizeIntradayCandle,
  selectCurrentFutureContracts,
  selectUniverseInstruments,
} from "../../src/upstox/backfill.mjs";

test("buildFetchWindow defaults to an exact five-year inclusive range", () => {
  const window = buildFetchWindow("2026-04-22");
  assert.equal(window.fromDate, "2021-04-22");
  assert.equal(window.toDate, "2026-04-22");
});

test("normalizeDailyCandle maps Upstox candle arrays to sqlite-friendly rows", () => {
  const row = normalizeDailyCandle("RELIANCE", "NSE_EQ|INE002A01018", [
    "2026-04-21T00:00:00+05:30",
    1421.5,
    1460,
    1412.2,
    1458.3,
    1055000,
    0,
  ]);

  assert.deepEqual(row, {
    symbol: "RELIANCE",
    instrumentKey: "NSE_EQ|INE002A01018",
    date: "2026-04-21",
    open: 1421.5,
    high: 1460,
    low: 1412.2,
    close: 1458.3,
    adjClose: 1458.3,
    volume: 1055000,
    openInterest: 0,
  });
});

test("normalizeIntradayCandle preserves intraday futures metadata and explicit data mode", () => {
  const row = normalizeIntradayCandle(
    {
      symbol: "RELIANCE",
      instrumentKey: "NSE_FO|123",
      tradingSymbol: "RELIANCE FUT 28 MAY 26",
      segment: "NSE_FO",
      instrumentType: "FUT",
      expiryDate: "2026-05-28",
      lotSize: 500,
      source: "upstox_active_bod",
    },
    [
      "2026-04-22T09:16:00+05:30",
      1421.5,
      1424.25,
      1420.1,
      1423.75,
      12500,
      987000,
    ],
  );

  assert.deepEqual(row, {
    symbol: "RELIANCE",
    timestamp: "2026-04-22T09:16:00+05:30",
    date: "2026-04-22",
    timeframeSec: 60,
    open: 1421.5,
    high: 1424.25,
    low: 1420.1,
    close: 1423.75,
    volume: 12500,
    openInterest: 987000,
    instrumentKey: "NSE_FO|123",
    tradingSymbol: "RELIANCE FUT 28 MAY 26",
    marketSegment: "NSE_FO",
    instrumentType: "FUT",
    contractExpiry: "2026-05-28",
    lotSize: 500,
    source: "upstox_active_bod",
    dataMode: "futures_1m",
  });
});

test("normalizeIntradayCandle marks equity minute rows as signal proxies", () => {
  const row = normalizeIntradayCandle(
    {
      symbol: "RELIANCE",
      instrumentKey: "NSE_EQ|INE002A01018",
      tradingSymbol: "RELIANCE",
      segment: "NSE_EQ",
      instrumentType: "EQ",
      lotSize: 1,
      source: "upstox_v3",
    },
    ["2026-04-22T09:15:00+05:30", 1420, 1421, 1419.5, 1420.5, 4400],
  );

  assert.equal(row.dataMode, "equity_signal_proxy_1m");
  assert.equal(row.contractExpiry, null);
  assert.equal(row.openInterest, 0);
});

test("normalizeIntradayCandle marks option minute rows separately from futures", () => {
  const row = normalizeIntradayCandle(
    {
      symbol: "RELIANCE",
      instrumentKey: "NSE_FO|987|29-09-2025",
      tradingSymbol: "RELIANCE 1400 CE 29 SEP 25",
      segment: "NSE_FO",
      instrumentType: "CE",
      expiryDate: "2025-09-29",
      lotSize: 500,
      source: "upstox_expired_option_v2",
    },
    ["2025-09-29T10:01:00+05:30", 10, 12, 9, 11, 120000, 450000],
  );

  assert.equal(row.dataMode, "options_1m");
  assert.equal(row.instrumentType, "CE");
  assert.equal(row.contractExpiry, "2025-09-29");
});

test("buildIntradayWindows splits 1m history into calendar-month chunks", () => {
  assert.deepEqual(buildIntradayWindows("2026-01-15", "2026-04-10"), [
    { fromDate: "2026-01-15", toDate: "2026-01-31", timeframeSec: 60 },
    { fromDate: "2026-02-01", toDate: "2026-02-28", timeframeSec: 60 },
    { fromDate: "2026-03-01", toDate: "2026-03-31", timeframeSec: 60 },
    { fromDate: "2026-04-01", toDate: "2026-04-10", timeframeSec: 60 },
  ]);
});

test("createDb creates the ohlcv_intraday schema", () => {
  const db = createDb(":memory:");
  const columns = db.prepare("PRAGMA table_info(ohlcv_intraday)").all();
  const indexes = db.prepare("PRAGMA index_list(ohlcv_intraday)").all();
  const optionColumns = db.prepare("PRAGMA table_info(option_contracts)").all();
  const optionExpiryColumns = db.prepare("PRAGMA table_info(option_expiries)").all();

  assert.deepEqual(
    columns.map((column) => column.name),
    [
      "symbol",
      "timestamp",
      "date",
      "timeframe_sec",
      "open",
      "high",
      "low",
      "close",
      "volume",
      "open_interest",
      "instrument_key",
      "trading_symbol",
      "market_segment",
      "instrument_type",
      "contract_expiry",
      "lot_size",
      "source",
      "data_mode",
    ],
  );
  assert.ok(indexes.some((index) => index.name === "idx_ohlcv_intraday_mode_symbol_time"));
  assert.deepEqual(
    optionColumns.map((column) => column.name),
    [
      "symbol",
      "expiry_date",
      "option_type",
      "strike_price",
      "instrument_key",
      "trading_symbol",
      "exchange",
      "segment",
      "lot_size",
      "tick_size",
      "underlying_key",
      "underlying_type",
      "underlying_symbol",
      "weekly",
      "source",
      "is_active",
      "updated_at",
    ],
  );
  assert.deepEqual(
    optionExpiryColumns.map((column) => column.name),
    [
      "symbol",
      "underlying_key",
      "expiry_date",
      "source",
      "updated_at",
    ],
  );

  db.close();
});

test("selectUniverseInstruments keeps only requested NSE equities and benchmark", () => {
  const instruments = [
    {
      segment: "NSE_EQ",
      instrument_type: "EQ",
      trading_symbol: "BAJAJ-AUTO",
      instrument_key: "NSE_EQ|BAJAJ",
      isin: "INE123",
      short_name: "Bajaj Auto",
    },
    {
      segment: "NSE_EQ",
      instrument_type: "EQ",
      trading_symbol: "RELIANCE",
      instrument_key: "NSE_EQ|INE002A01018",
      isin: "INE002A01018",
      short_name: "Reliance Industries",
    },
    {
      segment: "NSE_EQ",
      instrument_type: "EQ",
      trading_symbol: "INFY",
      instrument_key: "NSE_EQ|INE009A01021",
      isin: "INE009A01021",
      short_name: "Infosys",
      name: "INFOSYS LIMITED",
    },
    {
      segment: "NSE_INDEX",
      instrument_type: "INDEX",
      trading_symbol: "NIFTY",
      name: "Nifty 50",
      instrument_key: "NSE_INDEX|Nifty 50",
    },
    {
      segment: "NSE_EQ",
      instrument_type: "EQ",
      trading_symbol: "NOTME",
      instrument_key: "NSE_EQ|TEST",
    },
  ];

  const selected = selectUniverseInstruments(instruments, new Set(["RELIANCE", "BAJAJ-AUTO", "INFOSYS"]));
  assert.equal(selected.length, 4);
  assert.deepEqual(
    selected.map((item) => item.symbol).sort(),
    ["BAJAJ-AUTO", "INFOSYS", "NIFTY50", "RELIANCE"],
  );
});

test("selectCurrentFutureContracts keeps active NSE stock futures keyed by canonical symbol", () => {
  const instruments = [
    {
      segment: "NSE_FO",
      instrument_type: "FUT",
      underlying_symbol: "INFY",
      asset_symbol: "INFY",
      trading_symbol: "INFY FUT 28 APR 26",
      instrument_key: "NSE_FO|123",
      underlying_key: "NSE_EQ|INE009A01021",
      expiry: Date.parse("2026-04-28T15:30:00Z"),
      lot_size: 300,
      tick_size: 5,
      exchange: "NSE",
    },
    {
      segment: "NSE_FO",
      instrument_type: "FUT",
      underlying_symbol: "RELIANCE",
      asset_symbol: "RELIANCE",
      trading_symbol: "RELIANCE FUT 28 APR 26",
      instrument_key: "NSE_FO|456",
      underlying_key: "NSE_EQ|INE002A01018",
      expiry: Date.parse("2026-04-28T15:30:00Z"),
      lot_size: 500,
      tick_size: 10,
      exchange: "NSE",
    },
    {
      segment: "NSE_FO",
      instrument_type: "FUT",
      underlying_symbol: "RELIANCE",
      asset_symbol: "RELIANCE",
      trading_symbol: "RELIANCE FUT 30 MAR 26",
      instrument_key: "NSE_FO|457",
      underlying_key: "NSE_EQ|INE002A01018",
      expiry: Date.parse("2026-03-30T15:30:00Z"),
      lot_size: 500,
      tick_size: 10,
      exchange: "NSE",
    },
  ];

  const selected = selectCurrentFutureContracts(instruments, new Set(["INFOSYS", "RELIANCE"]), "2026-04-22");
  assert.deepEqual(
    selected.map((item) => ({
      symbol: item.symbol,
      tradingSymbol: item.tradingSymbol,
      expiryDate: item.expiryDate,
      lotSize: item.lotSize,
    })),
    [
      {
        symbol: "INFOSYS",
        tradingSymbol: "INFY FUT 28 APR 26",
        expiryDate: "2026-04-28",
        lotSize: 300,
      },
      {
        symbol: "RELIANCE",
        tradingSymbol: "RELIANCE FUT 28 APR 26",
        expiryDate: "2026-04-28",
        lotSize: 500,
      },
    ],
  );
});
