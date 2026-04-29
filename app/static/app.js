const DATE_PRESETS = [
  { key: '6m', label: '6M', months: 6 },
  { key: '1y', label: '1Y', years: 1 },
  { key: '3y', label: '3Y', years: 3 },
  { key: '5y', label: '5Y', years: 5 },
  { key: '10y', label: '10Y', years: 10 },
  { key: 'max', label: 'Max' },
];

const controls = {
  stockSearch: document.getElementById('stockSearch'),
  symbol: document.getElementById('symbolSelect'),
  from: document.getElementById('fromDate'),
  to: document.getElementById('toDate'),
  riskReward: document.getElementById('riskReward'),
  entryOffset: document.getElementById('entryOffset'),
  walkForward: document.getElementById('walkForward'),
  d1Red: document.getElementById('d1Red'),
  multiEntry: document.getElementById('multiEntry'),
  runButton: document.getElementById('runButton'),
  tradeSearch: document.getElementById('tradeSearch'),
  tradeOutcomeFilter: document.getElementById('tradeOutcomeFilter'),
  tradeSort: document.getElementById('tradeSort'),
  prevTradeButton: document.getElementById('prevTradeButton'),
  nextTradeButton: document.getElementById('nextTradeButton'),
  presetRow: document.getElementById('presetRow'),
  rrPresetRow: document.getElementById('rrPresetRow'),
};

const nodes = {
  healthDot: document.getElementById('healthDot'),
  healthText: document.getElementById('healthText'),
  universeCount: document.getElementById('universeCount'),
  headerWindow: document.getElementById('headerWindow'),
  rrPreview: document.getElementById('rrPreview'),
  stageTitle: document.getElementById('stageTitle'),
  bannerCopy: document.getElementById('bannerCopy'),
  detailTitle: document.getElementById('detailTitle'),
  summaryChips: document.getElementById('summaryChips'),
  runSnapshot: document.getElementById('runSnapshot'),
  summaryStack: document.getElementById('summaryStack'),
  strategyRead: document.getElementById('strategyRead'),
  selectedTradePanel: document.getElementById('selectedTradePanel'),
  selectedTradeLevels: document.getElementById('selectedTradeLevels'),
  outcomeBar: document.getElementById('outcomeBar'),
  outcomeList: document.getElementById('outcomeList'),
  yearGrid: document.getElementById('yearGrid'),
  tradeStrip: document.getElementById('tradeStrip'),
  tradeTableBody: document.getElementById('tradeTableBody'),
  overviewChart: document.getElementById('overviewChart'),
  detailChart: document.getElementById('detailChart'),
  overviewOverlay: document.getElementById('overviewOverlay'),
  detailOverlay: document.getElementById('chartOverlay'),
  stockOptions: document.getElementById('stockOptions'),
};

const state = {
  meta: null,
  payload: null,
  selectedTradeId: null,
  activePreset: null,
  charts: {
    overview: null,
    detail: null,
  },
  series: {
    overviewSymbol: null,
    overviewBenchmark: null,
    detailCandles: null,
    detailVolume: null,
  },
  detailPriceLines: [],
};

function normalizedSymbolInput(value) {
  return String(value || '').trim().toUpperCase();
}

function parseRiskRewardInput(value) {
  const raw = String(value ?? '').trim();
  if (!raw) {
    return null;
  }

  const ratioMatch = raw.match(/^(\d+(?:\.\d+)?)\s*[:/]\s*(\d+(?:\.\d+)?)$/);
  if (ratioMatch) {
    const risk = Number(ratioMatch[1]);
    const reward = Number(ratioMatch[2]);
    if (Number.isFinite(risk) && Number.isFinite(reward) && risk > 0 && reward > 0) {
      return reward / risk;
    }
    return null;
  }

  const numeric = Number(raw);
  if (Number.isFinite(numeric) && numeric > 0) {
    return numeric;
  }

  return null;
}

function currentRiskRewardValue() {
  return parseRiskRewardInput(controls.riskReward.value);
}

function formatRiskRewardLabel(value) {
  const numeric = Number(value ?? 0);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return '1:—';
  }
  return `1:${Number.isInteger(numeric) ? numeric : numeric.toFixed(1)}`;
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function formatPct(value) {
  const number = Number(value ?? 0);
  if (!Number.isFinite(number)) {
    return '—';
  }
  return `${number >= 0 ? '+' : ''}${(number * 100).toFixed(2)}%`;
}

function formatRR(value) {
  const number = Number(value ?? 0);
  if (!Number.isFinite(number)) {
    return '—';
  }
  return `${number >= 0 ? '+' : ''}${number.toFixed(2)}R`;
}

function formatPrice(value) {
  const number = Number(value ?? 0);
  if (!Number.isFinite(number)) {
    return '—';
  }
  return number.toFixed(2);
}

function formatPlain(value, digits = 2) {
  const number = Number(value ?? 0);
  if (!Number.isFinite(number)) {
    return '—';
  }
  return number.toFixed(digits);
}

function formatCurrency(value) {
  const number = Number(value ?? 0);
  if (!Number.isFinite(number)) {
    return '—';
  }
  return new Intl.NumberFormat('en-IN', {
    style: 'currency',
    currency: 'INR',
    maximumFractionDigits: 0,
  }).format(number);
}

function formatDate(isoDate) {
  if (!isoDate) {
    return '—';
  }
  const date = new Date(`${isoDate}T00:00:00`);
  return new Intl.DateTimeFormat('en-IN', { day: '2-digit', month: 'short', year: 'numeric' }).format(date);
}

function formatCompactDate(isoDate) {
  if (!isoDate) {
    return '—';
  }
  const date = new Date(`${isoDate}T00:00:00`);
  return new Intl.DateTimeFormat('en-IN', { day: '2-digit', month: 'short' }).format(date);
}

function formatCount(value) {
  return new Intl.NumberFormat('en-IN').format(Number(value ?? 0));
}

function formatBarOhlc(bar) {
  if (!bar) {
    return '—';
  }
  return `O ${formatPrice(bar.open)} / H ${formatPrice(bar.high)} / L ${formatPrice(bar.low)} / C ${formatPrice(bar.close)}`;
}

function describeWindow(fromValue, toValue) {
  if (!fromValue || !toValue) {
    return '—';
  }
  const fromYear = String(fromValue).slice(0, 4);
  const toYear = String(toValue).slice(0, 4);
  if (fromYear !== toYear) {
    return `${formatDate(fromValue)} → ${formatDate(toValue)}`;
  }
  return `${formatCompactDate(fromValue)} → ${formatCompactDate(toValue)}`;
}

function parseIsoDate(value) {
  return new Date(`${value}T00:00:00`);
}

function toIsoDate(date) {
  return date.toISOString().slice(0, 10);
}

function subtractWindow(date, preset) {
  const next = new Date(date);
  if (preset.years) {
    next.setUTCFullYear(next.getUTCFullYear() - preset.years);
  }
  if (preset.months) {
    next.setUTCMonth(next.getUTCMonth() - preset.months);
  }
  return next;
}

function outcomeClass(outcome) {
  if (outcome === 'target') return 'target';
  if (outcome === 'stop') return 'stop';
  if (outcome === 'timeout') return 'timeout';
  if (outcome === 'ambiguous') return 'ambiguous';
  return 'neutral';
}

function outcomeLabel(outcome) {
  if (outcome === 'target') return 'Target';
  if (outcome === 'stop') return 'Stop';
  if (outcome === 'timeout') return 'Timeout';
  if (outcome === 'ambiguous') return 'Ambiguous';
  return 'Other';
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `HTTP ${response.status}`);
  }
  return response.json();
}

function setLoading(message) {
  nodes.overviewOverlay.textContent = message;
  nodes.detailOverlay.textContent = message;
  nodes.overviewOverlay.classList.add('visible');
  nodes.detailOverlay.classList.add('visible');
}

function clearLoading() {
  nodes.overviewOverlay.classList.remove('visible');
  nodes.detailOverlay.classList.remove('visible');
}

function buildAnalyzeUrl() {
  const params = new URLSearchParams({
    symbol: controls.symbol.value,
    from: controls.from.value,
    to: controls.to.value,
    risk_reward: String(currentRiskRewardValue() ?? 3),
    offset: controls.entryOffset.value || '0.5',
    walk_forward_bars: controls.walkForward.value || '60',
    d1_red: controls.d1Red.checked ? '1' : '0',
    multi_entry: controls.multiEntry.checked ? '1' : '0',
  });
  return `/api/analyze?${params.toString()}`;
}

function syncUrl() {
  const params = new URLSearchParams({
    symbol: normalizedSymbolInput(controls.symbol.value),
    from: controls.from.value,
    to: controls.to.value,
    risk_reward: String(currentRiskRewardValue() ?? 3),
    offset: controls.entryOffset.value || '0.5',
    walk_forward_bars: controls.walkForward.value || '60',
  });
  if (controls.d1Red.checked) {
    params.set('d1_red', '1');
  }
  if (controls.multiEntry.checked) {
    params.set('multi_entry', '1');
  }
  history.replaceState(null, '', `${window.location.pathname}?${params.toString()}`);
}

function hydrateControlsFromUrl(meta) {
  const params = new URLSearchParams(window.location.search);
  const symbol = (params.get('symbol') || '').toUpperCase();
  if (symbol && meta.symbols.includes(symbol)) {
    controls.symbol.value = symbol;
  } else {
    controls.symbol.value = meta.default_symbol || meta.symbols[0] || '';
  }
  controls.from.value = params.get('from') || meta.default_from || meta.date_from || '';
  controls.to.value = params.get('to') || meta.default_to || meta.date_to || '';
  controls.riskReward.value = params.get('risk_reward') || '3';
  controls.entryOffset.value = params.get('offset') || params.get('entry_offset') || '0.5';
  controls.walkForward.value = params.get('walk_forward_bars') || '60';
  controls.d1Red.checked = params.get('d1_red') === '1';
  controls.multiEntry.checked = params.get('multi_entry') === '1';
  controls.stockSearch.value = controls.symbol.value;
}

function setActivePreset(key) {
  state.activePreset = key;
  controls.presetRow.querySelectorAll('[data-preset]').forEach((button) => {
    button.classList.toggle('active', button.dataset.preset === key);
  });
}

function setActiveRiskRewardPreset(value) {
  const normalized = String(parseRiskRewardInput(value) ?? currentRiskRewardValue() ?? 0);
  controls.rrPresetRow.querySelectorAll('[data-rr]').forEach((button) => {
    button.classList.toggle('active', String(Number(button.dataset.rr || 0)) === normalized);
  });
}

function applyPreset(key) {
  if (!state.meta?.date_to || !state.meta?.date_from) {
    return;
  }
  const maxDate = parseIsoDate(state.meta.date_to);
  const minDate = parseIsoDate(state.meta.date_from);
  const preset = DATE_PRESETS.find((item) => item.key === key);
  if (!preset) {
    return;
  }
  let startDate = new Date(minDate);
  if (preset.key !== 'max') {
    startDate = subtractWindow(maxDate, preset);
    if (startDate < minDate) {
      startDate = new Date(minDate);
    }
  }
  controls.from.value = toIsoDate(startDate);
  controls.to.value = toIsoDate(maxDate);
  setActivePreset(key);
  runAnalysis();
}

function ensureCharts() {
  if (!state.charts.overview) {
    state.charts.overview = LightweightCharts.createChart(nodes.overviewChart, {
      autoSize: true,
      layout: {
        background: { type: 'solid', color: '#09121c' },
        textColor: '#8ea2b9',
        fontFamily: 'Avenir Next, SF Pro Display, IBM Plex Sans, sans-serif',
        attributionLogo: false,
      },
      grid: {
        vertLines: { color: 'rgba(142, 162, 185, 0.08)' },
        horzLines: { color: 'rgba(142, 162, 185, 0.08)' },
      },
      rightPriceScale: {
        borderColor: 'rgba(142, 162, 185, 0.18)',
        scaleMargins: { top: 0.14, bottom: 0.12 },
      },
      timeScale: {
        borderColor: 'rgba(142, 162, 185, 0.18)',
        timeVisible: true,
      },
      crosshair: {
        vertLine: { color: 'rgba(99, 223, 235, 0.22)', width: 1 },
        horzLine: { color: 'rgba(99, 223, 235, 0.14)', width: 1 },
      },
    });

    state.series.overviewSymbol = state.charts.overview.addSeries(LightweightCharts.LineSeries, {
      color: '#63dfeb',
      lineWidth: 3,
      priceLineVisible: false,
      lastValueVisible: true,
    });
    state.series.overviewBenchmark = state.charts.overview.addSeries(LightweightCharts.LineSeries, {
      color: '#ffbf74',
      lineWidth: 2,
      lineStyle: 2,
      priceLineVisible: false,
      lastValueVisible: true,
    });
  }

  if (!state.charts.detail) {
    state.charts.detail = LightweightCharts.createChart(nodes.detailChart, {
      autoSize: true,
      layout: {
        background: { type: 'solid', color: '#09121c' },
        textColor: '#8ea2b9',
        fontFamily: 'Avenir Next, SF Pro Display, IBM Plex Sans, sans-serif',
        attributionLogo: false,
      },
      grid: {
        vertLines: { color: 'rgba(142, 162, 185, 0.08)' },
        horzLines: { color: 'rgba(142, 162, 185, 0.08)' },
      },
      rightPriceScale: {
        borderColor: 'rgba(142, 162, 185, 0.18)',
        scaleMargins: { top: 0.08, bottom: 0.26 },
      },
      timeScale: {
        borderColor: 'rgba(142, 162, 185, 0.18)',
        timeVisible: true,
      },
      crosshair: {
        vertLine: { color: 'rgba(99, 223, 235, 0.24)', width: 1 },
        horzLine: { color: 'rgba(99, 223, 235, 0.14)', width: 1 },
      },
    });

    state.series.detailCandles = state.charts.detail.addSeries(LightweightCharts.CandlestickSeries, {
      upColor: '#37d69b',
      downColor: '#ff6e85',
      wickUpColor: '#37d69b',
      wickDownColor: '#ff6e85',
      borderVisible: false,
      priceLineVisible: false,
    });
    state.series.detailVolume = state.charts.detail.addSeries(LightweightCharts.HistogramSeries, {
      priceFormat: { type: 'volume' },
      priceScaleId: '',
      lastValueVisible: false,
    });
    state.charts.detail.priceScale('').applyOptions({
      scaleMargins: { top: 0.8, bottom: 0 },
      borderVisible: false,
    });
  }
}

function clearDetailPriceLines() {
  if (!state.series.detailCandles) {
    return;
  }
  for (const line of state.detailPriceLines) {
    try {
      state.series.detailCandles.removePriceLine(line);
    } catch (error) {
      // Ignore stale lines after rerenders.
    }
  }
  state.detailPriceLines = [];
}

function setOverviewMarkers(trades) {
  if (!state.series.overviewSymbol || !LightweightCharts.createSeriesMarkers) {
    return;
  }
  const markers = [...trades]
    .sort((left, right) => left.signal_date.localeCompare(right.signal_date))
    .map((trade) => ({
      time: trade.signal_date,
      position: 'aboveBar',
      color: trade.outcome === 'target' ? '#37d69b' : trade.outcome === 'stop' ? '#ff6e85' : '#63dfeb',
      shape: 'circle',
      text: trade.outcome === 'target' ? 'T' : trade.outcome === 'stop' ? 'S' : 'LQ',
    }));
  LightweightCharts.createSeriesMarkers(state.series.overviewSymbol, markers);
}

function setDetailMarkers(trade) {
  if (!state.series.detailCandles || !LightweightCharts.createSeriesMarkers) {
    return;
  }
  const bars = trade?.bars || [];
  const markers = [];
  if (trade && bars.length) {
    const stopReferenceBar = trade.markers?.stop_reference_index != null ? bars[trade.markers.stop_reference_index] : null;
    const prevBar = bars[trade.markers?.prev_index];
    const signalBar = bars[trade.markers?.signal_index];
    const exitBar = bars[trade.markers?.exit_index];
    const mergedStopRef = Boolean(trade.markers?.stop_reference_matches_prev && stopReferenceBar && prevBar);
    if (mergedStopRef) {
      markers.push({
        time: stopReferenceBar[0],
        position: 'belowBar',
        color: '#ff8f5a',
        shape: 'circle',
        text: 'D-1 + SL ref',
      });
    } else {
      if (stopReferenceBar) {
        markers.push({
          time: stopReferenceBar[0],
          position: 'belowBar',
          color: '#ff8f5a',
          shape: 'circle',
          text: 'SL ref',
        });
      }
      if (prevBar) {
        markers.push({
          time: prevBar[0],
          position: 'aboveBar',
          color: '#c0cad9',
          shape: 'circle',
          text: 'D-1 sweep',
        });
      }
    }
    if (signalBar) {
      markers.push({
        time: signalBar[0],
        position: 'belowBar',
        color: '#63dfeb',
        shape: 'arrowUp',
        text: 'Signal',
      });
    }
    if (exitBar) {
      markers.push({
        time: exitBar[0],
        position: trade.outcome === 'target' ? 'aboveBar' : 'belowBar',
        color:
          trade.outcome === 'target'
            ? '#37d69b'
            : trade.outcome === 'stop'
              ? '#ff6e85'
              : trade.outcome === 'ambiguous'
                ? '#63dfeb'
                : '#ffbf74',
        shape: trade.outcome === 'target' ? 'circle' : 'square',
        text: trade.exit_label,
      });
    }
  }
  LightweightCharts.createSeriesMarkers(state.series.detailCandles, markers);
}

function buildNormalizedSeries(payload) {
  const symbol = payload.selected_symbol;
  const symbolBars = payload.bars?.[symbol] || [];
  const benchmarkBars = payload.benchmark_bars || [];
  const benchmarkByDate = new Map(benchmarkBars.map((bar) => [bar.time, bar.close]));
  const aligned = [];
  for (const bar of symbolBars) {
    if (benchmarkByDate.has(bar.time)) {
      aligned.push({ time: bar.time, symbolClose: bar.close, benchmarkClose: benchmarkByDate.get(bar.time) });
    }
  }
  if (!aligned.length) {
    return { symbolSeries: [], benchmarkSeries: [] };
  }
  const symbolBase = aligned[0].symbolClose || 1;
  const benchmarkBase = aligned[0].benchmarkClose || 1;
  return {
    symbolSeries: aligned.map((row) => ({ time: row.time, value: (row.symbolClose / symbolBase) * 100 })),
    benchmarkSeries: aligned.map((row) => ({ time: row.time, value: (row.benchmarkClose / benchmarkBase) * 100 })),
  };
}

function currentTrades() {
  const trades = [...(state.payload?.visual_review?.trades || [])];
  const query = (controls.tradeSearch.value || '').trim().toLowerCase();
  const outcome = controls.tradeOutcomeFilter.value || 'all';

  const filtered = trades.filter((trade) => {
    const haystack = [
      trade.signal_date,
      trade.exit_date || '',
      trade.exit_label || '',
      trade.reason_summary || '',
      trade.theme || '',
      trade.sub_theme || '',
    ]
      .join(' ')
      .toLowerCase();
    const queryOk = !query || haystack.includes(query);
    const outcomeOk = outcome === 'all' || trade.outcome === outcome;
    return queryOk && outcomeOk;
  });

  switch (controls.tradeSort.value) {
    case 'earliest':
      filtered.sort((left, right) => left.signal_date.localeCompare(right.signal_date));
      break;
    case 'best':
      filtered.sort((left, right) => Number(right.return_pct) - Number(left.return_pct));
      break;
    case 'worst':
      filtered.sort((left, right) => Number(left.return_pct) - Number(right.return_pct));
      break;
    case 'strongest':
      filtered.sort((left, right) => Number(right.excess_return_63) - Number(left.excess_return_63));
      break;
    case 'longest':
      filtered.sort((left, right) => Number(right.bars_held) - Number(left.bars_held));
      break;
    case 'latest':
    default:
      filtered.sort((left, right) => right.signal_date.localeCompare(left.signal_date));
      break;
  }

  return filtered;
}

function syncStockInputFromSelect() {
  controls.stockSearch.value = controls.symbol.value || '';
}

function resolveTypedSymbol() {
  const symbol = normalizedSymbolInput(controls.stockSearch.value || controls.symbol.value);
  if (!symbol) {
    return null;
  }
  if (!state.meta?.symbols?.includes(symbol)) {
    return null;
  }
  controls.symbol.value = symbol;
  controls.stockSearch.value = symbol;
  return symbol;
}

function selectedTrade(trades) {
  if (!trades.length) {
    state.selectedTradeId = null;
    return null;
  }
  if (!trades.some((trade) => trade.id === state.selectedTradeId)) {
    state.selectedTradeId = trades[0].id;
  }
  return trades.find((trade) => trade.id === state.selectedTradeId) || trades[0];
}

function renderSummary(payload) {
  const summary = payload.summary || {};
  const controlsState = payload.controls || {};
  const rawMatches = Number(payload.match_count ?? 0);
  const trades = Number(summary.trades ?? 0);
  const conversion = rawMatches > 0 ? trades / rawMatches : 0;
  const equityChange = Number(summary.equity_end ?? 1) - 1;
  nodes.universeCount.textContent = formatCount(state.meta?.symbol_count || payload.symbol_count || 0);
  nodes.headerWindow.textContent = describeWindow(payload.date_from || state.meta?.date_from, payload.date_to || state.meta?.date_to);
  nodes.stageTitle.textContent = `${payload.selected_symbol} • ${formatDate(payload.date_from)} to ${formatDate(payload.date_to)}`;
  nodes.bannerCopy.textContent = `${formatCount(trades)} confirmed trades from ${formatCount(rawMatches)} raw matches across the selected window. Read the regime in the overview chart, then inspect the selected setup candle by candle.`;

  nodes.summaryChips.innerHTML = [
    `<span class="chip chip-accent"><strong>${escapeHtml(payload.selected_symbol)}</strong> Symbol</span>`,
    `<span class="chip"><strong>${escapeHtml(String(payload.price_source || state.meta?.price_source || 'cash')).toUpperCase()}</strong> Series</span>`,
    `<span class="chip"><strong>${summary.trades ?? 0}</strong> Triggered trades</span>`,
    `<span class="chip"><strong>${payload.match_count ?? 0}</strong> Raw matches</span>`,
    `<span class="chip"><strong>${formatPlain(controlsState.entry_offset_pct ?? 0, 2)}%</strong> Offset</span>`,
    `<span class="chip"><strong>${formatPct(summary.win_rate ?? 0)}</strong> Win rate</span>`,
    `<span class="chip"><strong>${formatPct(summary.avg_return_pct ?? 0)}</strong> Avg return</span>`,
    `<span class="chip"><strong>${formatRiskRewardLabel(controlsState.risk_reward ?? 0)}</strong> Tuned R:R</span>`,
  ].join('');

  nodes.runSnapshot.innerHTML = [
    ['Signal conversion', formatPct(conversion), `${formatCount(trades)} of ${formatCount(rawMatches)} patterns matured into trades`],
    ['Profit factor', formatPlain(summary.profit_factor ?? 0, 2), `${formatPct(summary.win_rate ?? 0)} win rate with ${formatPct(summary.expectancy_pct ?? 0)} expectancy`],
    ['Drawdown', formatPct(-(summary.max_drawdown_pct ?? 0)), `${formatPct(summary.worst_return_pct ?? 0)} worst realized trade in this run`],
    ['Per-lot P&L', formatCurrency(summary.avg_pnl_rupees ?? 0), `${formatCurrency(summary.total_pnl_rupees ?? 0)} total net P&L across the run`],
    ['Equity path', formatPct(equityChange), `${formatPct(summary.best_return_pct ?? 0)} best trade with ${formatPlain(summary.avg_rr ?? 0, 2)}R average`],
  ]
    .map(
      ([label, value, subvalue]) => `
        <div class="snapshot-card">
          <div class="label">${escapeHtml(label)}</div>
          <div class="value">${escapeHtml(String(value))}</div>
          <div class="subvalue">${escapeHtml(String(subvalue))}</div>
        </div>
      `,
    )
    .join('');

  nodes.summaryStack.innerHTML = [
    ['Trades', formatCount(summary.trades ?? 0)],
    ['Win Rate', formatPct(summary.win_rate ?? 0)],
    ['Avg Return', formatPct(summary.avg_return_pct ?? 0)],
    ['Median Return', formatPct(summary.median_return_pct ?? 0)],
    ['Expectancy', formatPct(summary.expectancy_pct ?? 0)],
    ['Profit Factor', formatPlain(summary.profit_factor ?? 0, 2)],
    ['Avg P&L / Lot', formatCurrency(summary.avg_pnl_rupees ?? 0)],
    ['Max Drawdown', formatPct(-(summary.max_drawdown_pct ?? 0))],
    ['Best Trade', formatPct(summary.best_return_pct ?? 0)],
    ['Equity Change', formatPct(equityChange)],
  ]
    .map(
      ([label, value]) => `
        <div class="summary-card">
          <div class="label">${escapeHtml(label)}</div>
          <div class="value">${escapeHtml(String(value))}</div>
        </div>
      `,
    )
    .join('');

  const lastTrade = (payload.visual_review?.trades || [])[0];
  nodes.strategyRead.innerHTML = `
    <div class="story-block">
      <div class="story-kicker">Signal Logic</div>
      <h4>What qualifies as a setup</h4>
      <p>Signal day must close green, sweep below the previous day's low, and clear the 63-day relative-strength gate versus NIFTY50.</p>
    </div>
      <div class="story-block">
      <div class="story-kicker">Run Configuration</div>
      <ul class="story-list">
        <li>Instrument = front-month continuous single-stock futures, benchmarked against cash NIFTY50</li>
        <li>Entry offset = ${formatPlain(controlsState.entry_offset_pct ?? 0, 2)}% from stop-loss anchor | Risk:Reward = ${formatRiskRewardLabel(controlsState.risk_reward ?? 0)}</li>
        <li>Stop = prior red candle low before the signal day</li>
        <li>Walk-forward horizon = ${formatCount(controlsState.walk_forward_bars ?? 0)} bars</li>
        <li>D-1 red filter = ${controlsState.d1_red ? 'ON' : 'OFF'} | Overlap = ${controlsState.multi_entry ? 'Allowed' : 'Blocked'}</li>
      </ul>
    </div>
    <div class="story-block">
      <div class="story-kicker">Read This Run</div>
      <p>${summary.trades ?? 0} trades survived entry confirmation from ${payload.match_count ?? 0} raw pattern matches in this window.</p>
      <p>${lastTrade ? `Most recent trade: ${formatDate(lastTrade.signal_date)} and ${escapeHtml(lastTrade.exit_label)}.` : 'No qualifying trades were found for the chosen symbol and timeframe.'}</p>
    </div>
  `;
}

function renderOutcome(payload) {
  const trades = payload.visual_review?.trades || [];
  const totals = { target: 0, stop: 0, timeout: 0, ambiguous: 0 };
  for (const trade of trades) {
    if (Object.hasOwn(totals, trade.outcome)) {
      totals[trade.outcome] += 1;
    }
  }
  const total = Math.max(trades.length, 1);
  nodes.outcomeBar.innerHTML = Object.entries(totals)
    .filter(([, count]) => count > 0)
    .map(
      ([key, count]) => `<div class="outcome-segment ${key}" style="width:${(count / total) * 100}%"></div>`,
    )
    .join('');
  nodes.outcomeList.innerHTML = Object.entries(totals)
    .map(
      ([key, count]) => `
        <div class="mini-row">
          <div class="label">${escapeHtml(outcomeLabel(key))}</div>
          <div class="value">${count} • ${formatPct(count / total)}</div>
        </div>
      `,
    )
    .join('');
}

function renderYearGrid(payload) {
  const yearly = payload.yearly || [];
  if (!yearly.length) {
    nodes.yearGrid.innerHTML = '<div class="empty">No yearly summary available for this run.</div>';
    return;
  }
  nodes.yearGrid.innerHTML = yearly
    .map(
      (row) => `
        <div class="year-card">
          <div class="label">${row.year}</div>
          <div class="value">${formatCount(row.trades)} trades</div>
          <div class="value">${formatPct(row.avg_return_pct)}</div>
          <div class="value">${formatPct(row.win_rate)} wins</div>
        </div>
      `,
    )
    .join('');
}

function renderTradeLevels(trade) {
  if (!trade) {
    nodes.selectedTradeLevels.innerHTML = '';
    return;
  }
  const riskPct = trade.entry_price > 0 ? (trade.entry_price - trade.stop_loss) / trade.entry_price : 0;
  const rewardPct = trade.entry_price > 0 ? (trade.target_price - trade.entry_price) / trade.entry_price : 0;
  const cards = [
    ['Signal Close', formatPrice(trade.signal_bar?.close)],
    ['Entry', formatPrice(trade.entry_price)],
    ['Stop Anchor', formatPrice(trade.stop_loss)],
    ['Target', formatPrice(trade.target_price)],
    ['Exit', formatPrice(trade.exit_price)],
    ['Offset', `${formatPlain(trade.entry_offset_pct ?? 0, 2)}%`],
    ['Stop Candle', trade.stop_reference_date || '—'],
    ['Stop Ref Close', formatPrice(trade.stop_reference_bar?.close)],
    ['Contract', trade.trading_symbol || trade.symbol],
    ['Expiry', trade.contract_expiry || '—'],
    ['Lot Size', formatCount(trade.lot_size || 0)],
    ['Risk Below Entry', formatPct(-riskPct)],
    ['Reward Above Entry', formatPct(rewardPct)],
    ['Risk / Lot', formatCurrency(-(trade.risk_points || 0) * (trade.lot_size || 1))],
    ['Net P&L / Lot', formatCurrency(trade.net_pnl_rupees || 0)],
    ['Hold', `${formatCount(trade.bars_held)} bars`],
    ['63D Spread', formatPct(trade.excess_return_63)],
  ];
  if (trade.theme) {
    cards.push(['Theme', trade.theme]);
  }
  nodes.selectedTradeLevels.innerHTML = cards
    .map(
      ([label, value]) => `
        <div class="level-card">
          <div class="label">${escapeHtml(label)}</div>
          <div class="value">${escapeHtml(String(value))}</div>
        </div>
      `,
    )
    .join('');
}

function renderTradeDetails(trade) {
  if (!trade) {
    nodes.detailTitle.textContent = 'No matching trade';
    nodes.selectedTradePanel.innerHTML = '<span class="empty">No trade matches the current filters.</span>';
    renderTradeLevels(null);
    return;
  }

  const bars = trade.bars || [];
  const prevBar = bars[trade.markers?.prev_index] || null;
  const signalBar = bars[trade.markers?.signal_index] || null;
  const stopReferenceBar = trade.stop_reference_bar || (trade.markers?.stop_reference_index != null ? bars[trade.markers.stop_reference_index] : null);
  const exitBar = trade.exit_bar || (trade.markers?.exit_index != null ? bars[trade.markers.exit_index] : null);
  const signalClose = Number(trade.signal_bar?.close ?? signalBar?.[4] ?? trade.entry_price);
  nodes.detailTitle.textContent = `${trade.symbol} • ${formatDate(trade.signal_date)} • ${trade.exit_label}`;

  nodes.selectedTradePanel.innerHTML = `
    <div class="trade-summary">
      <div class="trade-summary-head">
        <div>
          <div class="story-kicker">Trade Thesis</div>
          <h3 class="trade-summary-title">${escapeHtml(trade.symbol)} on ${escapeHtml(formatDate(trade.signal_date))}</h3>
          <p class="trade-subtitle">${escapeHtml(trade.reason_summary)}</p>
        </div>
        <span class="pill ${outcomeClass(trade.outcome)}">${escapeHtml(trade.exit_label)}</span>
      </div>

      <div class="trade-meta-grid">
        <div class="mini-row">
          <div class="label">Signal to Exit</div>
          <div class="value">${escapeHtml(formatDate(trade.signal_date))} → ${escapeHtml(formatDate(trade.exit_date || trade.signal_date))}</div>
        </div>
        <div class="mini-row">
          <div class="label">Return and R</div>
          <div class="value">${formatPct(trade.return_pct)} • ${formatRR(trade.rr)}</div>
        </div>
        <div class="mini-row">
          <div class="label">Futures Contract</div>
          <div class="value">${escapeHtml(trade.trading_symbol || trade.symbol)}${trade.contract_expiry ? ` • ${escapeHtml(trade.contract_expiry)}` : ''}</div>
        </div>
        <div class="mini-row">
          <div class="label">Lot and Net P&amp;L</div>
          <div class="value">${formatCount(trade.lot_size || 0)} qty • ${formatCurrency(trade.net_pnl_rupees || 0)}</div>
        </div>
        <div class="mini-row">
          <div class="label">Stop → Entry</div>
          <div class="value">${formatPrice(trade.stop_loss)} → ${formatPrice(trade.entry_price)}</div>
        </div>
        <div class="mini-row">
          <div class="label">Entry Plan</div>
          <div class="value">${formatPlain(trade.entry_offset_pct ?? 0, 2)}% offset from stop • stop on ${escapeHtml(trade.stop_reference_date || 'prior red candle')}</div>
        </div>
        <div class="mini-row">
          <div class="label">Stop Rule</div>
          <div class="value">D+1 onward, low ≤ stop. Close below stop is not required.</div>
        </div>
        <div class="mini-row">
          <div class="label">63D Relative Strength</div>
          <div class="value">${formatPct(trade.stock_return_63)} vs ${formatPct(trade.benchmark_return_63)}</div>
        </div>
        <div class="mini-row">
          <div class="label">Context</div>
          <div class="value">${escapeHtml(trade.theme || 'Unclassified')}${trade.sub_theme ? ` • ${escapeHtml(trade.sub_theme)}` : ''}</div>
        </div>
      </div>

      <div class="story-block">
        <div class="story-kicker">Visual Checklist</div>
        <ul class="story-list">
          <li>${signalBar ? `Signal candle opened at ${formatPrice(signalBar[1])} and closed at ${formatPrice(signalBar[4])}. Entry was then derived from the stop anchor ${formatPrice(trade.stop_loss)} to ${formatPrice(trade.entry_price)}.` : 'Signal candle metadata unavailable.'}</li>
          <li>${signalBar && prevBar ? `Signal low ${formatPrice(signalBar[3])} swept below D-1 low ${formatPrice(prevBar[3])}.` : 'Could not compute sweep distance from the visible bars.'}</li>
          <li>${signalBar ? `Signal range was ${formatPrice(signalBar[3])} to ${formatPrice(signalBar[2])}, so the planned entry ${formatPrice(trade.entry_price)} ${trade.entry_valid_on_signal ? 'was' : 'was not'} reachable on the signal day.` : 'Could not validate signal-day entry reachability from the visible bars.'}</li>
          <li>${stopReferenceBar ? `Stop reference candle on ${escapeHtml(stopReferenceBar.date || trade.stop_reference_date || '—')} printed ${escapeHtml(formatBarOhlc(stopReferenceBar))}; stop stayed fixed to its low at ${formatPrice(trade.stop_loss)}.` : `Stop stayed fixed at the low of the prior red candle${trade.stop_reference_date ? ` from ${escapeHtml(trade.stop_reference_date)}` : ''}.`}</li>
          <li>${exitBar ? `Exit candle on ${escapeHtml(exitBar.date || trade.exit_date || '—')} printed ${escapeHtml(formatBarOhlc(exitBar))}. ${trade.outcome === 'stop' ? `Its low ${formatPrice(exitBar.low)} pierced the stop ${formatPrice(trade.stop_loss)}, so the stop was hit intraday.` : trade.outcome === 'target' ? `Its high ${formatPrice(exitBar.high)} cleared the target ${formatPrice(trade.target_price)}.` : trade.outcome === 'ambiguous' ? 'Both the stop and target sat inside that bar range, so the tie-breaker was resolved from candle colour.' : `Neither stop nor target was hit before the timed exit at ${formatPrice(trade.exit_price)}.`}` : 'Exit candle metadata unavailable.'}</li>
          <li>${trade.trading_symbol ? `Execution instrument was ${escapeHtml(trade.trading_symbol)} with lot size ${formatCount(trade.lot_size || 0)}.` : 'Execution instrument metadata unavailable.'}</li>
          <li>Entry, signal close, stop, target, and realized exit are pinned as price bands for a fast structural check.</li>
          <li>Markers separate the D-1 sweep candle from the actual stop-reference candle whenever they are different bars.</li>
        </ul>
      </div>

      <div class="rationale-list">
        ${trade.rationale
          .map(
            (item) => `
              <div class="rationale-item ${item.passed ? 'pass' : 'fail'}">
                <div class="title eyebrow">${escapeHtml(item.label)}</div>
                <div>${escapeHtml(item.detail)}</div>
              </div>
            `,
          )
          .join('')}
      </div>
    </div>
  `;

  renderTradeLevels(trade);
}

function updateRiskRewardPreview(trade) {
  if (!nodes.rrPreview) {
    return;
  }
  const ratioValue = currentRiskRewardValue();
  if (!ratioValue) {
    nodes.rrPreview.textContent = 'Use the reward side only, like 2 or 3. The control reads as 1:2 or 1:3.';
    return;
  }
  const ratioText = formatRiskRewardLabel(ratioValue);
  if (!trade) {
    nodes.rrPreview.textContent = `${ratioText} selected. Pick a trade to see the implied move from entry.`;
    return;
  }
  const riskPct = trade.entry_price > 0 ? (trade.entry_price - trade.stop_loss) / trade.entry_price : 0;
  const rewardPct = trade.entry_price > 0 ? (trade.target_price - trade.entry_price) / trade.entry_price : 0;
  nodes.rrPreview.textContent = `${ratioText} on the selected trade means ${formatPct(-riskPct)} to stop and ${formatPct(rewardPct)} to target from entry.`;
}

function renderTradeStrip(trades, selected) {
  if (!trades.length) {
    nodes.tradeStrip.innerHTML = '<div class="empty">No trades matched the current filters.</div>';
    return;
  }

  nodes.tradeStrip.innerHTML = trades
    .map(
      (trade) => `
        <button class="trade-card ${trade.id === selected?.id ? 'active' : ''}" type="button" data-trade-id="${trade.id}">
          <div class="trade-card-head">
            <div>
              <div class="trade-card-title">${escapeHtml(formatDate(trade.signal_date))}</div>
              <div class="meta">${escapeHtml(trade.symbol)}${trade.contract_expiry ? ` • ${escapeHtml(trade.contract_expiry)}` : ''}</div>
            </div>
            <span class="pill ${outcomeClass(trade.outcome)}">${escapeHtml(trade.exit_label)}</span>
          </div>
          <div class="trade-card-values">
            <div class="trade-card-value">
              <span class="meta">Return</span>
              <strong>${formatPct(trade.return_pct)}</strong>
            </div>
            <div class="trade-card-value">
              <span class="meta">R</span>
              <strong>${formatRR(trade.rr)}</strong>
            </div>
            <div class="trade-card-value">
              <span class="meta">63D Spread</span>
              <strong>${formatPct(trade.excess_return_63)}</strong>
            </div>
          </div>
        </button>
      `,
    )
    .join('');

  nodes.tradeStrip.querySelectorAll('[data-trade-id]').forEach((button) => {
    button.addEventListener('click', () => {
      state.selectedTradeId = button.dataset.tradeId;
      refreshTradeViews();
    });
  });

  const active = nodes.tradeStrip.querySelector('.trade-card.active');
  active?.scrollIntoView({ block: 'nearest', inline: 'nearest' });
}

function renderTradeTable(trades, selected) {
  if (!trades.length) {
    nodes.tradeTableBody.innerHTML = '<tr><td colspan="11">No trades matched the current filters.</td></tr>';
    return;
  }

  nodes.tradeTableBody.innerHTML = trades
    .map(
      (trade) => `
        <tr data-trade-id="${trade.id}" class="${trade.id === selected?.id ? 'active' : ''}">
          <td>${escapeHtml(trade.signal_date)}</td>
          <td>${escapeHtml(trade.exit_date || trade.signal_date)}</td>
          <td>${escapeHtml(trade.contract_expiry || '—')}</td>
          <td><span class="pill ${outcomeClass(trade.outcome)}">${escapeHtml(trade.exit_label)}</span></td>
          <td>${formatPrice(trade.entry_price)}</td>
          <td>${formatPrice(trade.stop_loss)}</td>
          <td>${formatPrice(trade.target_price)}</td>
          <td>${formatPct(trade.return_pct)}</td>
          <td>${formatCurrency(trade.net_pnl_rupees || 0)}</td>
          <td>${formatCount(trade.bars_held)}</td>
          <td>${formatPct(trade.excess_return_63)}</td>
        </tr>
      `,
    )
    .join('');

  nodes.tradeTableBody.querySelectorAll('[data-trade-id]').forEach((row) => {
    row.addEventListener('click', () => {
      state.selectedTradeId = row.dataset.tradeId;
      refreshTradeViews();
    });
  });

  const active = nodes.tradeTableBody.querySelector('tr.active');
  active?.scrollIntoView({ block: 'nearest', inline: 'nearest' });
}

function renderOverviewChart(payload) {
  ensureCharts();
  const { symbolSeries, benchmarkSeries } = buildNormalizedSeries(payload);
  state.series.overviewSymbol.setData(symbolSeries);
  state.series.overviewBenchmark.setData(benchmarkSeries);
  setOverviewMarkers(payload.visual_review?.trades || []);
  state.charts.overview.timeScale().fitContent();
}

function renderDetailChart(trade) {
  ensureCharts();
  clearDetailPriceLines();
  if (!trade) {
    state.series.detailCandles.setData([]);
    state.series.detailVolume.setData([]);
    setDetailMarkers(null);
    return;
  }

  const bars = (trade.bars || []).map(([time, open, high, low, close, volume]) => ({
    time,
    open,
    high,
    low,
    close,
    volume,
  }));

  state.series.detailCandles.setData(
    bars.map((bar) => ({
      time: bar.time,
      open: bar.open,
      high: bar.high,
      low: bar.low,
      close: bar.close,
    })),
  );
  state.series.detailVolume.setData(
    bars.map((bar) => ({
      time: bar.time,
      value: Number(bar.volume || 0),
      color: bar.close >= bar.open ? 'rgba(55, 214, 155, 0.38)' : 'rgba(255, 110, 133, 0.38)',
    })),
  );

  setDetailMarkers(trade);

  const levels = [
    ['Entry', trade.entry_price, '#63dfeb'],
    ...(Math.abs(Number(trade.levels?.signal_close ?? trade.entry_price) - trade.entry_price) < 1e-8
      ? []
      : [['Signal Close', trade.levels?.signal_close, '#9fe9ff']]),
    ['Stop', trade.stop_loss, '#ff6e85'],
    ['Target', trade.target_price, '#37d69b'],
    ['Exit', trade.exit_price, '#ffbf74'],
  ];

  for (const [title, price, color] of levels) {
    state.detailPriceLines.push(
      state.series.detailCandles.createPriceLine({
        price,
        color,
        lineWidth: 2,
        axisLabelVisible: true,
        title,
      }),
    );
  }

  if (bars.length) {
    state.charts.detail.timeScale().setVisibleRange({
      from: bars[0].time,
      to: bars[bars.length - 1].time,
    });
  }
}

function refreshTradeViews() {
  const trades = currentTrades();
  const selected = selectedTrade(trades);
  renderTradeStrip(trades, selected);
  renderTradeDetails(selected);
  renderDetailChart(selected);
  renderTradeTable(trades, selected);
  updateRiskRewardPreview(selected);
}

function renderPayload(payload) {
  state.payload = payload;
  renderSummary(payload);
  renderOutcome(payload);
  renderYearGrid(payload);
  renderOverviewChart(payload);
  refreshTradeViews();
}

function moveTradeSelection(direction) {
  const trades = currentTrades();
  if (!trades.length) {
    return;
  }
  const index = trades.findIndex((trade) => trade.id === state.selectedTradeId);
  const currentIndex = index === -1 ? 0 : index;
  const nextIndex = Math.min(Math.max(currentIndex + direction, 0), trades.length - 1);
  state.selectedTradeId = trades[nextIndex].id;
  refreshTradeViews();
}

async function runAnalysis() {
  const symbol = resolveTypedSymbol();
  if (!symbol) {
    nodes.selectedTradePanel.innerHTML = '<span class="empty">Enter a valid F&O symbol from the universe to load its history.</span>';
    return;
  }
  const riskRewardValue = currentRiskRewardValue();
  if (!riskRewardValue) {
    nodes.selectedTradePanel.innerHTML = '<span class="empty">Use a valid risk:reward value like 1.5, 2, 3, or type it as 1:3.</span>';
    updateRiskRewardPreview(null);
    return;
  }
  setLoading('Running analysis');
  controls.runButton.disabled = true;
  controls.runButton.textContent = 'Running Analysis...';
  try {
    syncUrl();
    const payload = await fetchJson(buildAnalyzeUrl());
    renderPayload(payload);
  } catch (error) {
    nodes.selectedTradePanel.innerHTML = `<span class="empty">${escapeHtml(error.message || error)}</span>`;
  } finally {
    controls.runButton.disabled = false;
    controls.runButton.textContent = 'Run Full Analysis';
    clearLoading();
  }
}

async function loadMeta() {
  const [health, meta] = await Promise.all([fetchJson('/api/health'), fetchJson('/api/meta')]);
  nodes.healthDot.classList.remove('dead');
  nodes.healthDot.classList.add(health.ok ? 'live' : 'dead');
  nodes.healthText.textContent = health.ok ? 'Connected' : 'Unavailable';
  state.meta = meta;

  controls.symbol.innerHTML = meta.symbols
    .map((symbol) => `<option value="${escapeHtml(symbol)}">${escapeHtml(symbol)}</option>`)
    .join('');
  nodes.stockOptions.innerHTML = meta.symbols
    .map((symbol) => `<option value="${escapeHtml(symbol)}"></option>`)
    .join('');
  hydrateControlsFromUrl(meta);
  setActiveRiskRewardPreset(controls.riskReward.value);
  nodes.universeCount.textContent = formatCount(meta.symbol_count || meta.symbols.length || 0);
  nodes.headerWindow.textContent = describeWindow(controls.from.value, controls.to.value);
  await runAnalysis();
}

function bindEvents() {
  controls.runButton.addEventListener('click', runAnalysis);
  controls.tradeSearch.addEventListener('input', refreshTradeViews);
  controls.tradeOutcomeFilter.addEventListener('change', refreshTradeViews);
  controls.tradeSort.addEventListener('change', refreshTradeViews);
  controls.prevTradeButton.addEventListener('click', () => moveTradeSelection(-1));
  controls.nextTradeButton.addEventListener('click', () => moveTradeSelection(1));
  controls.stockSearch.addEventListener('change', () => {
    if (resolveTypedSymbol()) {
      runAnalysis();
    }
  });
  controls.stockSearch.addEventListener('keydown', (event) => {
    if (event.key === 'Enter') {
      event.preventDefault();
      if (resolveTypedSymbol()) {
        runAnalysis();
      }
    }
  });
  controls.presetRow.addEventListener('click', (event) => {
    const button = event.target.closest('[data-preset]');
    if (!button) {
      return;
    }
    applyPreset(button.dataset.preset);
  });
  controls.rrPresetRow.addEventListener('click', (event) => {
    const button = event.target.closest('[data-rr]');
    if (!button) {
      return;
    }
    controls.riskReward.value = button.dataset.rr;
    setActiveRiskRewardPreset(button.dataset.rr);
    runAnalysis();
  });
  controls.riskReward.addEventListener('input', () => {
    setActiveRiskRewardPreset(controls.riskReward.value);
    updateRiskRewardPreview(state.payload ? selectedTrade(currentTrades()) : null);
  });
  controls.riskReward.addEventListener('blur', () => {
    const parsed = currentRiskRewardValue();
    if (parsed) {
      controls.riskReward.value = String(parsed);
      setActiveRiskRewardPreset(parsed);
    }
    updateRiskRewardPreview(state.payload ? selectedTrade(currentTrades()) : null);
  });
  controls.from.addEventListener('change', () => setActivePreset(null));
  controls.to.addEventListener('change', () => setActivePreset(null));
  controls.symbol.addEventListener('change', () => {
    syncStockInputFromSelect();
    runAnalysis();
  });
  document.addEventListener('keydown', (event) => {
    const target = event.target;
    const tagName = target?.tagName;
    if (target?.isContentEditable || tagName === 'INPUT' || tagName === 'SELECT' || tagName === 'TEXTAREA') {
      return;
    }
    if (event.key === 'ArrowLeft') {
      moveTradeSelection(-1);
    }
    if (event.key === 'ArrowRight') {
      moveTradeSelection(1);
    }
  });
}

bindEvents();
loadMeta().catch((error) => {
  nodes.healthDot.classList.add('dead');
  nodes.healthText.textContent = 'Failed';
  nodes.selectedTradePanel.innerHTML = `<span class="empty">${escapeHtml(error.message || error)}</span>`;
  clearLoading();
});
