const controls = {
  reportMode: document.getElementById('reportMode'),
  search: document.getElementById('instanceSearch'),
  symbol: document.getElementById('symbolFilter'),
  from: document.getElementById('dateFromFilter'),
  to: document.getElementById('dateToFilter'),
  bucketFilter: document.getElementById('bucketFilter'),
  outcome: document.getElementById('outcomeFilter'),
  minRr: document.getElementById('minRrFilter'),
  minVolume: document.getElementById('minVolumeFilter'),
  minTurnover: document.getElementById('minTurnoverFilter'),
  reset: document.getElementById('resetFilters'),
};

const nodes = {
  status: document.getElementById('intradayStatus'),
  statusDot: document.getElementById('intradayStatusDot'),
  kpis: document.getElementById('intradayKpis'),
  title: document.getElementById('intradayResultTitle'),
  bucketTabs: document.getElementById('bucketTabs'),
  table: document.getElementById('instanceTableBody'),
  detail: document.getElementById('instanceDetail'),
  filteredCount: document.getElementById('filteredCount'),
  reportSource: document.getElementById('reportSourceLabel'),
  insights: document.getElementById('visibleInsights'),
  dateRail: document.getElementById('dateRail'),
  dayReviewTitle: document.getElementById('dayReviewTitle'),
  dayReviewMeta: document.getElementById('dayReviewMeta'),
  dayChart: document.getElementById('dayChart'),
  dayChartTitle: document.getElementById('dayChartTitle'),
  dayChartStatus: document.getElementById('dayChartStatus'),
  daySignals: document.getElementById('daySignals'),
};

const state = {
  payload: null,
  rows: [],
  selectedIndex: 0,
  selectedDateKey: '',
  activeBucket: 'all',
  optionProbeRequestId: 0,
  dayRequestId: 0,
  dayChart: null,
  daySeries: {
    candles: null,
    volume: null,
  },
  dayPriceLines: [],
};

const TABLE_ROW_LIMIT = 800;
const EXCHANGE_DISPLAY_OFFSET_SECONDS = 5.5 * 60 * 60;
const REPORTS = {
  raw: {
    label: 'Raw candidates',
    path: 'reports/intraday-volume-spike-bucketed-all.csv',
  },
  follow: {
    label: '+1R follow-through',
    path: 'reports/intraday-volume-spike-bucketed-full.csv',
  },
};

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function formatNumber(value, digits = 2) {
  const number = Number(value);
  if (!Number.isFinite(number)) return '-';
  return number.toLocaleString('en-IN', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function formatCount(value) {
  return Number(value || 0).toLocaleString('en-IN');
}

function formatR(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return '-';
  return `${number >= 0 ? '+' : ''}${number.toFixed(2)}R`;
}

function formatMoney(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return '-';
  if (Math.abs(number) >= 10000000) return `${(number / 10000000).toFixed(2)}cr`;
  if (Math.abs(number) >= 100000) return `${(number / 100000).toFixed(2)}L`;
  return formatCount(Math.round(number));
}

function formatPct(value, digits = 1) {
  const number = Number(value);
  if (!Number.isFinite(number)) return '-';
  return `${number >= 0 ? '+' : ''}${number.toFixed(digits)}%`;
}

function formatSignedMoney(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return '-';
  const prefix = number >= 0 ? '+' : '-';
  return `${prefix}${formatMoney(Math.abs(number))}`;
}

function formatTimestamp(value) {
  if (!value) return '-';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat('en-IN', {
    day: '2-digit',
    month: 'short',
    year: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).format(date);
}

function chartDisplayTime(value) {
  const seconds = Math.floor(new Date(value).getTime() / 1000);
  return Number.isFinite(seconds) ? seconds + EXCHANGE_DISPLAY_OFFSET_SECONDS : seconds;
}

function formatDateInput(value) {
  return String(value || '').slice(0, 10);
}

function signalDate(row) {
  return formatDateInput(row?.signal_timestamp);
}

function selectedRow() {
  const rows = filteredInstances();
  if (!rows.length) return null;
  return rows[Math.min(state.selectedIndex, rows.length - 1)] || rows[0] || null;
}

function bucketLabel(bucket) {
  if (bucket === 'same_day') return 'Same day';
  if (bucket === 'next_morning_entry') return 'Next morning';
  if (bucket === 'two_day_hold') return 'Two day';
  return bucket || 'Other';
}

function bucketClass(bucket) {
  return `bucket-${String(bucket || 'other').replaceAll('_', '-')}`;
}

function setStatus(text, mode = 'idle') {
  nodes.status.textContent = text;
  nodes.statusDot.classList.toggle('live', mode === 'live');
  nodes.statusDot.classList.toggle('dead', mode === 'error');
}

async function loadReport() {
  const report = REPORTS[controls.reportMode.value] || REPORTS.raw;
  setStatus(`Loading ${report.label}...`, 'live');
  nodes.table.innerHTML = '<tr><td colspan="10">Loading the full mined report...</td></tr>';
  try {
    const params = new URLSearchParams({ path: report.path });
    const response = await fetch(`/api/intraday/precomputed-report?${params.toString()}`);
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
    state.payload = payload;
    state.rows = payload.instances || [];
    state.selectedIndex = 0;
    state.selectedDateKey = '';
    state.activeBucket = 'all';
    hydrateFilters();
    setStatus(`Loaded ${formatCount(payload.instances_returned)} mined instances`, 'live');
    nodes.reportSource.textContent = `${report.label} · ${formatCount(payload.instances_returned)} rows`;
    render();
  } catch (error) {
    setStatus(error.message || String(error), 'error');
    nodes.reportSource.textContent = 'Report unavailable';
    nodes.table.innerHTML = `<tr><td colspan="10">${escapeHtml(error.message || String(error))}</td></tr>`;
  }
}

function hydrateFilters() {
  const symbols = [...new Set(state.rows.map((row) => row.symbol).filter(Boolean))].sort();
  controls.symbol.innerHTML = '<option value="all">All symbols</option>'
    + symbols.map((symbol) => `<option value="${escapeHtml(symbol)}">${escapeHtml(symbol)}</option>`).join('');

  const outcomes = [...new Set(state.rows.map((row) => row.exit_reason).filter(Boolean))].sort();
  controls.outcome.innerHTML = '<option value="all">All outcomes</option>'
    + outcomes.map((outcome) => `<option value="${escapeHtml(outcome)}">${escapeHtml(outcome)}</option>`).join('');

  controls.from.value = state.payload?.date_from || '';
  controls.to.value = state.payload?.date_to || '';
  controls.search.value = '';
  controls.symbol.value = 'all';
  controls.bucketFilter.value = 'all';
  controls.outcome.value = 'all';
  controls.minRr.value = '';
  controls.minVolume.value = '';
  controls.minTurnover.value = '';
  state.activeBucket = 'all';
}

function render() {
  if (!state.payload) return;
  renderKpis();
  renderInsights();
  renderBucketTabs();
  renderDateRail();
  renderTable();
  renderDayReview();
}

function renderKpis() {
  const payload = state.payload;
  const visible = filteredInstances(false);
  const summary = summarizeRows(visible);
  const visibleSymbols = new Set(visible.map((row) => row.symbol).filter(Boolean)).size;
  const cards = [
    ['Visible', visible.length],
    ['Symbols', visibleSymbols || payload.symbols_scanned],
    ['Win rate', `${formatNumber(summary.win_rate_pct || 0, 1)}%`],
    ['Avg R', formatR(summary.avg_rr || 0)],
    ['Best R', formatR(summary.best_rr || 0)],
    ['Worst R', formatR(summary.worst_rr || 0)],
  ];
  nodes.kpis.innerHTML = cards.map(([label, value]) => `
    <div class="intraday-kpi">
      <span>${escapeHtml(label)}</span>
      <strong>${escapeHtml(formatCountIfNumber(value))}</strong>
    </div>
  `).join('');
  nodes.title.textContent = `${formatCount(visible.length)} visible from ${formatCount(payload.instances_total)} mined instances`;
}

function renderInsights() {
  const rows = filteredInstances(false);
  const summary = summarizeRows(rows);
  const cards = [
    ['Loaded rows', state.payload.instances_returned],
    ['Bars scanned', state.payload.total_bars === 'not_counted_on_page_load' ? 'DB preserved' : state.payload.total_bars],
    ['Target exits', `${formatNumber(summary.target_rate_pct, 1)}%`],
    ['Timeout exits', `${formatNumber(summary.timeout_rate_pct, 1)}%`],
    ['Avg volume x', `${formatNumber(summary.avg_volume_multiple, 1)}x`],
    ['Avg turnover', formatMoney(summary.avg_turnover)],
  ];
  nodes.insights.innerHTML = cards.map(([label, value]) => `
    <div class="insight-chip">
      <span>${escapeHtml(label)}</span>
      <strong>${escapeHtml(formatCountIfNumber(value))}</strong>
    </div>
  `).join('');
}

function formatCountIfNumber(value) {
  return typeof value === 'number' ? formatCount(value) : value;
}

function renderBucketTabs() {
  const rows = filteredInstances(false, { ignoreBucket: true });
  const counts = rows.reduce((acc, row) => {
    acc[row.bucket] = (acc[row.bucket] || 0) + 1;
    return acc;
  }, {});
  const tabs = [['all', 'All', rows.length]];
  ['same_day', 'next_morning_entry', 'two_day_hold'].forEach((bucket) => {
    tabs.push([bucket, bucketLabel(bucket), counts[bucket] || 0]);
  });
  nodes.bucketTabs.innerHTML = tabs.map(([bucket, label, count]) => `
    <button class="bucket-tab ${state.activeBucket === bucket ? 'active' : ''}" data-bucket="${escapeHtml(bucket)}" type="button">
      <span>${escapeHtml(label)}</span>
      <strong>${formatCount(count)}</strong>
    </button>
  `).join('');
  nodes.bucketTabs.querySelectorAll('button').forEach((button) => {
    button.addEventListener('click', () => {
      state.activeBucket = button.dataset.bucket || 'all';
      controls.bucketFilter.value = state.activeBucket;
      state.selectedIndex = 0;
      render();
    });
  });
}

function groupRowsByDate(rows) {
  const groups = new Map();
  rows.forEach((row) => {
    const key = signalDate(row);
    if (!key) return;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  });
  return [...groups.entries()]
    .map(([date, groupRows]) => {
      const summary = summarizeRows(groupRows);
      const symbols = new Set(groupRows.map((row) => row.symbol).filter(Boolean));
      const expiryCount = groupRows.filter((row) => row.is_option_expiry_day === true || row.is_option_expiry_day === 'true').length;
      const sorted = [...groupRows].sort((a, b) =>
        Number(b.volume_multiple || 0) - Number(a.volume_multiple || 0) ||
        String(b.signal_timestamp || '').localeCompare(String(a.signal_timestamp || '')),
      );
      return {
        date,
        rows: sorted,
        summary,
        symbols: symbols.size,
        expiryCount,
        top: sorted[0],
      };
    })
    .sort((a, b) => b.date.localeCompare(a.date));
}

function renderDateRail() {
  const groups = groupRowsByDate(filteredInstances(false));
  if (!groups.length) {
    state.selectedDateKey = '';
    nodes.dateRail.innerHTML = '<div class="date-empty">No dates match filters.</div>';
    return;
  }
  if (!state.selectedDateKey || !groups.some((group) => group.date === state.selectedDateKey)) {
    state.selectedDateKey = groups[0].date;
  }
  nodes.dateRail.innerHTML = groups.slice(0, 120).map((group) => {
    const isActive = group.date === state.selectedDateKey;
    const bestR = group.summary.best_rr || 0;
    return `
      <button class="date-cluster ${isActive ? 'active' : ''}" type="button" data-date="${escapeHtml(group.date)}">
        <span class="date-cluster-day">${escapeHtml(formatShortDate(group.date))}</span>
        <span class="date-cluster-meta">${formatCount(group.rows.length)} signals · ${formatCount(group.symbols)} symbols</span>
        <span class="date-cluster-foot">
          <strong class="${bestR >= 0 ? 'positive' : 'negative'}">${escapeHtml(formatR(bestR))}</strong>
          <em>${escapeHtml(formatNumber(group.summary.avg_volume_multiple, 1))}x vol</em>
          ${group.expiryCount ? '<mark>EXP</mark>' : ''}
        </span>
      </button>
    `;
  }).join('');
  nodes.dateRail.querySelectorAll('button[data-date]').forEach((button) => {
    button.addEventListener('click', () => {
      state.selectedDateKey = button.dataset.date || '';
      const rows = filteredInstances();
      const index = rows.findIndex((row) => signalDate(row) === state.selectedDateKey);
      state.selectedIndex = index >= 0 ? index : 0;
      render();
    });
  });
}

function formatShortDate(value) {
  const date = new Date(`${value}T00:00:00+05:30`);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat('en-IN', {
    day: '2-digit',
    month: 'short',
    year: '2-digit',
  }).format(date);
}

function filteredInstances(applySort = true, options = {}) {
  const query = controls.search.value.trim().toLowerCase();
  const bucket = options.ignoreBucket ? 'all' : controls.bucketFilter.value || state.activeBucket || 'all';
  const symbol = controls.symbol.value || 'all';
  const outcome = controls.outcome.value || 'all';
  const from = controls.from.value;
  const to = controls.to.value;
  const minRr = numberOrNull(controls.minRr.value);
  const minVolume = numberOrNull(controls.minVolume.value);
  const minTurnover = numberOrNull(controls.minTurnover.value);

  let rows = [...state.rows];
  if (bucket !== 'all') rows = rows.filter((row) => row.bucket === bucket);
  if (symbol !== 'all') rows = rows.filter((row) => row.symbol === symbol);
  if (outcome !== 'all') rows = rows.filter((row) => row.exit_reason === outcome);
  if (from) rows = rows.filter((row) => formatDateInput(row.signal_timestamp) >= from);
  if (to) rows = rows.filter((row) => formatDateInput(row.signal_timestamp) <= to);
  if (minRr !== null) rows = rows.filter((row) => Number(row.rr || 0) >= minRr);
  if (minVolume !== null) rows = rows.filter((row) => Number(row.volume_multiple || 0) >= minVolume);
  if (minTurnover !== null) rows = rows.filter((row) => Number(row.turnover || 0) >= minTurnover);
  if (query) {
    rows = rows.filter((row) => [
      row.symbol,
      row.bucket,
      row.exit_reason,
      row.signal_timestamp,
      row.option_expiry_date,
      row.is_option_expiry_day ? 'expiry' : '',
      row.entry_timestamp,
      row.exit_timestamp,
    ].some((value) => String(value || '').toLowerCase().includes(query)));
  }

  if (!applySort) return rows;
  rows.sort((a, b) => String(b.signal_timestamp || '').localeCompare(String(a.signal_timestamp || '')));
  return rows;
}

function numberOrNull(value) {
  if (value === '') return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function summarizeRows(rows) {
  const total = rows.length;
  const pnlPoints = rows.map((row) => Number(row.pnl_points || 0));
  const rrs = rows.map((row) => Number(row.rr || 0)).filter(Number.isFinite);
  const mfes = rows.map((row) => Number(row.max_favorable_rr || 0)).filter(Number.isFinite);
  const volumes = rows.map((row) => Number(row.volume_multiple || 0)).filter(Number.isFinite);
  const turnovers = rows.map((row) => Number(row.turnover || 0)).filter(Number.isFinite);
  const wins = pnlPoints.filter((value) => value > 0).length;
  const targets = rows.filter((row) => String(row.exit_reason || '').includes('target')).length;
  const timeouts = rows.filter((row) => row.exit_reason === 'timeout').length;
  return {
    total_trades: total,
    wins,
    win_rate_pct: total ? (wins / total) * 100 : 0,
    avg_rr: average(rrs),
    best_rr: rrs.length ? Math.max(...rrs) : 0,
    worst_rr: rrs.length ? Math.min(...rrs) : 0,
    avg_mfe: average(mfes),
    avg_volume_multiple: average(volumes),
    avg_turnover: average(turnovers),
    target_rate_pct: total ? (targets / total) * 100 : 0,
    timeout_rate_pct: total ? (timeouts / total) * 100 : 0,
  };
}

function average(values) {
  return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;
}

function renderTable() {
  const rows = filteredInstances();
  nodes.filteredCount.textContent = `${formatCount(rows.length)} visible`;
  if (!rows.length) {
    nodes.table.innerHTML = '<tr><td colspan="10">No instances match the current filters.</td></tr>';
    renderDetail(null);
    return;
  }
  state.selectedIndex = Math.min(state.selectedIndex, rows.length - 1);
  const activeRow = rows[state.selectedIndex];
  if (activeRow && signalDate(activeRow) !== state.selectedDateKey) {
    const sameDateIndex = rows.findIndex((row) => signalDate(row) === state.selectedDateKey);
    if (sameDateIndex >= 0) state.selectedIndex = sameDateIndex;
  }
  const visibleRows = rows.slice(0, TABLE_ROW_LIMIT);
  nodes.table.innerHTML = visibleRows.map((row, index) => `
    <tr class="${index === state.selectedIndex ? 'selected' : ''} ${signalDate(row) === state.selectedDateKey ? 'same-date' : ''}" data-index="${index}">
      <td><span class="bucket-pill ${bucketClass(row.bucket)}">${escapeHtml(bucketLabel(row.bucket))}</span></td>
      <td><strong>${escapeHtml(row.symbol)}</strong></td>
      <td>${escapeHtml(formatTimestamp(row.signal_timestamp))}</td>
      <td>${expiryBadge(row)}</td>
      <td>${escapeHtml(formatTimestamp(row.exit_timestamp))}</td>
      <td>${escapeHtml(row.exit_reason)}</td>
      <td class="${Number(row.rr || 0) >= 0 ? 'positive' : 'negative'}">${escapeHtml(formatR(row.rr))}</td>
      <td>${escapeHtml(formatR(row.max_favorable_rr))}</td>
      <td>${escapeHtml(formatNumber(row.volume_multiple, 1))}x</td>
      <td>${escapeHtml(formatMoney(row.turnover))}</td>
    </tr>
  `).join('');
  nodes.table.querySelectorAll('tr[data-index]').forEach((row) => {
    row.addEventListener('click', () => {
      state.selectedIndex = Number(row.dataset.index || 0);
      const current = filteredInstances()[state.selectedIndex];
      state.selectedDateKey = signalDate(current);
      render();
    });
  });
  if (rows.length > visibleRows.length) {
    nodes.table.insertAdjacentHTML(
      'beforeend',
      `<tr class="table-limit-row"><td colspan="10">Showing first ${formatCount(visibleRows.length)} sorted rows. Narrow filters to inspect deeper matches.</td></tr>`,
    );
  }
  renderDetail(rows[state.selectedIndex]);
}

function expiryBadge(row) {
  const expiryDate = row.option_expiry_date;
  if (!expiryDate) return '<span class="option-expiry-badge muted">-</span>';
  const isExpiryDay = row.is_option_expiry_day === true || row.is_option_expiry_day === 'true';
  const tradingDte = Number(row.option_dte_trading);
  const calendarDte = Number(row.option_dte_calendar);
  const label = isExpiryDay
    ? 'EXP'
    : Number.isFinite(tradingDte)
      ? `T-${tradingDte}`
      : Number.isFinite(calendarDte)
        ? `D-${calendarDte}`
        : expiryDate.slice(5);
  const className = isExpiryDay ? 'expiry-day' : 'regular';
  return `<span class="option-expiry-badge ${className}" title="Option expiry ${escapeHtml(expiryDate)}">${escapeHtml(label)}</span>`;
}

function optionDteText(row) {
  const isExpiryDay = row.is_option_expiry_day === true || row.is_option_expiry_day === 'true';
  if (isExpiryDay) return 'Expiry day';
  const tradingDte = Number(row.option_dte_trading);
  if (Number.isFinite(tradingDte)) return `${tradingDte} trading sessions`;
  const calendarDte = Number(row.option_dte_calendar);
  if (Number.isFinite(calendarDte)) return `${calendarDte} calendar days`;
  return '-';
}

function renderDayReview() {
  const rows = filteredInstances();
  const dayRows = rows.filter((row) => signalDate(row) === state.selectedDateKey);
  const current = selectedRow() || dayRows[0] || null;
  if (!current || !dayRows.length) {
    nodes.dayReviewTitle.textContent = 'No signal date selected';
    nodes.dayReviewMeta.textContent = 'Adjust filters to restore the date review.';
    nodes.dayChartTitle.textContent = 'No chart loaded';
    nodes.daySignals.innerHTML = '';
    clearDayChart('No candles to display.');
    return;
  }

  const summary = summarizeRows(dayRows);
  const symbols = new Set(dayRows.map((row) => row.symbol).filter(Boolean));
  nodes.dayReviewTitle.textContent = `${formatShortDate(state.selectedDateKey)} · ${formatCount(dayRows.length)} signals`;
  nodes.dayReviewMeta.textContent = `${formatCount(symbols.size)} symbols · best ${formatR(summary.best_rr)} · avg volume ${formatNumber(summary.avg_volume_multiple, 1)}x`;
  nodes.dayChartTitle.textContent = `${current.symbol} · ${state.selectedDateKey}`;
  renderDaySignalStrip(dayRows, rows);
  loadDayChart(current, dayRows);
}

function renderDaySignalStrip(dayRows, allRows) {
  nodes.daySignals.innerHTML = dayRows.map((row) => {
    const absoluteIndex = allRows.findIndex((candidate) =>
      candidate.symbol === row.symbol &&
      candidate.signal_timestamp === row.signal_timestamp &&
      candidate.bucket === row.bucket
    );
    const selected = absoluteIndex === state.selectedIndex;
    return `
      <button class="day-signal ${selected ? 'active' : ''}" type="button" data-index="${absoluteIndex}">
        <span>${escapeHtml(row.symbol)}</span>
        <strong class="${Number(row.rr || 0) >= 0 ? 'positive' : 'negative'}">${escapeHtml(formatR(row.rr))}</strong>
        <em>${escapeHtml(formatTime(row.signal_timestamp))} · ${escapeHtml(formatNumber(row.volume_multiple, 1))}x</em>
      </button>
    `;
  }).join('');
  nodes.daySignals.querySelectorAll('button[data-index]').forEach((button) => {
    button.addEventListener('click', () => {
      state.selectedIndex = Number(button.dataset.index || 0);
      render();
    });
  });
}

function formatTime(value) {
  if (!value) return '-';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value).slice(11, 16);
  return new Intl.DateTimeFormat('en-IN', {
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).format(date);
}

async function loadDayChart(row, dayRows) {
  const requestId = state.dayRequestId + 1;
  state.dayRequestId = requestId;
  nodes.dayChartStatus.textContent = `Loading ${row.symbol} candles...`;
  nodes.dayChartStatus.classList.remove('hidden');
  const params = new URLSearchParams({
    symbol: row.symbol,
    date: signalDate(row),
    data_mode: row.data_mode || state.payload?.data_mode || 'equity_signal_proxy_1m',
  });
  try {
    const response = await fetch(`/api/intraday/day?${params.toString()}`);
    const payload = await response.json();
    if (requestId !== state.dayRequestId) return;
    if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
    drawDayChart(payload, dayRows, row);
  } catch (error) {
    if (requestId !== state.dayRequestId) return;
    clearDayChart(error.message || String(error));
  }
}

function ensureDayChart() {
  if (state.dayChart || !window.LightweightCharts || !nodes.dayChart) return;
  state.dayChart = LightweightCharts.createChart(nodes.dayChart, {
    autoSize: true,
    layout: {
      background: { type: 'solid', color: '#0b0e11' },
      textColor: '#c0c6d5',
      fontFamily: 'Inter, system-ui, sans-serif',
    },
    grid: {
      vertLines: { color: 'rgba(138, 145, 159, 0.09)' },
      horzLines: { color: 'rgba(138, 145, 159, 0.09)' },
    },
    rightPriceScale: {
      borderColor: '#414753',
      scaleMargins: { top: 0.08, bottom: 0.26 },
    },
    timeScale: {
      borderColor: '#414753',
      timeVisible: true,
      secondsVisible: false,
    },
    crosshair: {
      mode: LightweightCharts.CrosshairMode.Normal,
    },
  });
  state.daySeries.candles = state.dayChart.addSeries(LightweightCharts.CandlestickSeries, {
    upColor: '#00f59b',
    downColor: '#ff5352',
    borderUpColor: '#00f59b',
    borderDownColor: '#ff5352',
    wickUpColor: '#00f59b',
    wickDownColor: '#ff5352',
  });
  state.daySeries.volume = state.dayChart.addSeries(LightweightCharts.HistogramSeries, {
    priceFormat: { type: 'volume' },
    priceScaleId: '',
    color: 'rgba(46, 144, 255, 0.36)',
  });
  state.dayChart.priceScale('').applyOptions({
    scaleMargins: { top: 0.78, bottom: 0 },
  });
}

function drawDayChart(payload, dayRows, selected) {
  ensureDayChart();
  if (!state.dayChart || !state.daySeries.candles || !state.daySeries.volume) {
    nodes.dayChartStatus.textContent = 'Chart library unavailable.';
    return;
  }
  const bars = (payload.bars || []).map((bar) => ({
    time: chartDisplayTime(bar.timestamp),
    open: Number(bar.open),
    high: Number(bar.high),
    low: Number(bar.low),
    close: Number(bar.close),
    volume: Number(bar.volume || 0),
  })).filter((bar) => Number.isFinite(bar.time));
  if (!bars.length) {
    clearDayChart(`No ${payload.symbol} candles cached for ${payload.date}.`);
    return;
  }
  state.daySeries.candles.setData(bars.map(({ time, open, high, low, close }) => ({ time, open, high, low, close })));
  state.daySeries.volume.setData(bars.map((bar) => ({
    time: bar.time,
    value: bar.volume,
    color: bar.close >= bar.open ? 'rgba(0, 245, 155, 0.34)' : 'rgba(255, 83, 82, 0.34)',
  })));
  renderChartMarkers(dayRows, payload.symbol);
  renderTradePriceLines(selected);
  state.dayChart.timeScale().fitContent();
  nodes.dayChartStatus.classList.add('hidden');
}

function renderChartMarkers(dayRows, symbol) {
  if (!state.daySeries.candles || !LightweightCharts.createSeriesMarkers) return;
  const markers = dayRows
    .filter((row) => row.symbol === symbol)
    .map((row) => ({
      time: chartDisplayTime(row.signal_timestamp),
      position: 'aboveBar',
      color: '#2e90ff',
      shape: 'arrowDown',
      text: `${formatR(row.rr)} · ${formatNumber(row.volume_multiple, 1)}x`,
    }));
  LightweightCharts.createSeriesMarkers(state.daySeries.candles, markers);
}

function renderTradePriceLines(row) {
  if (!state.daySeries.candles) return;
  state.dayPriceLines.forEach((line) => state.daySeries.candles.removePriceLine(line));
  state.dayPriceLines = [];
  [
    ['Entry', row.entry_price, '#a7c8ff'],
    ['Stop', row.stop_loss, '#ff5352'],
    ['Target', row.target_price, '#00f59b'],
  ].forEach(([title, price, color]) => {
    const value = Number(price);
    if (!Number.isFinite(value)) return;
    state.dayPriceLines.push(state.daySeries.candles.createPriceLine({
      price: value,
      color,
      lineWidth: 1,
      lineStyle: LightweightCharts.LineStyle.Dashed,
      axisLabelVisible: true,
      title,
    }));
  });
}

function clearDayChart(message) {
  ensureDayChart();
  if (state.daySeries.candles) state.daySeries.candles.setData([]);
  if (state.daySeries.volume) state.daySeries.volume.setData([]);
  nodes.dayChartStatus.textContent = message;
  nodes.dayChartStatus.classList.remove('hidden');
}

function renderDetail(row) {
  state.optionProbeRequestId += 1;
  if (!row) {
    nodes.detail.innerHTML = '<div class="eyebrow">Selected Instance</div><p>No row selected.</p>';
    return;
  }
  nodes.detail.innerHTML = `
    <div class="eyebrow">Selected Instance</div>
    <div class="detail-header">
      <h3>${escapeHtml(row.symbol)}</h3>
      <span class="bucket-pill ${bucketClass(row.bucket)}">${escapeHtml(bucketLabel(row.bucket))}</span>
    </div>
    <div class="rr-bar" style="--mfe:${clampPercent(Number(row.max_favorable_rr || 0) / 5 * 100)}%; --mae:${clampPercent(Math.abs(Number(row.max_adverse_rr || 0)) / 5 * 100)}%;">
      <span></span>
      <strong>${escapeHtml(formatR(row.rr))}</strong>
    </div>
    <div class="detail-metrics">
      <div><span>Entry</span><strong>${escapeHtml(formatNumber(row.entry_price, 2))}</strong></div>
      <div><span>Stop</span><strong>${escapeHtml(formatNumber(row.stop_loss, 2))}</strong></div>
      <div><span>Target</span><strong>${escapeHtml(formatNumber(row.target_price, 2))}</strong></div>
      <div><span>Exit</span><strong>${escapeHtml(formatNumber(row.exit_price, 2))}</strong></div>
      <div><span>Realized</span><strong>${escapeHtml(formatR(row.rr))}</strong></div>
      <div><span>MFE</span><strong>${escapeHtml(formatR(row.max_favorable_rr))}</strong></div>
    </div>
    <dl class="instance-facts">
      <dt>Signal</dt><dd>${escapeHtml(row.signal_timestamp)}</dd>
      <dt>Entry</dt><dd>${escapeHtml(row.entry_timestamp)}</dd>
      <dt>Exit</dt><dd>${escapeHtml(row.exit_timestamp)}</dd>
      <dt>Option expiry</dt><dd>${escapeHtml(row.option_expiry_date || '-')}</dd>
      <dt>Option DTE</dt><dd>${escapeHtml(optionDteText(row))}</dd>
      <dt>Bars held</dt><dd>${escapeHtml(formatCount(row.bars_held))}</dd>
      <dt>Volume multiple</dt><dd>${escapeHtml(formatNumber(row.volume_multiple, 2))}x</dd>
      <dt>Spike volume</dt><dd>${escapeHtml(formatCount(row.spike_volume))}</dd>
      <dt>Median volume</dt><dd>${escapeHtml(formatCount(row.rolling_median_volume))}</dd>
      <dt>Turnover</dt><dd>${escapeHtml(formatMoney(row.turnover))}</dd>
      <dt>Close location</dt><dd>${escapeHtml(formatNumber(row.close_location, 2))}</dd>
      <dt>R target</dt><dd>${escapeHtml(row.risk_reward_label || '-')}</dd>
    </dl>
    <section class="option-probe" id="optionProbe">
      <div class="option-probe-head">
        <div>
          <span class="eyebrow">ATM Option What-if</span>
          <h4>Checking cached CE/PE legs</h4>
        </div>
        ${expiryBadge(row)}
      </div>
      <p class="option-probe-note">Looking for ATM call and put 1-minute candles for this signal window.</p>
    </section>
    <a class="review-link" href="${escapeHtml(row.review_url)}" target="_blank" rel="noreferrer">Open symbol replay JSON</a>
  `;
  loadOptionProbe(row, state.optionProbeRequestId);
}

async function loadOptionProbe(row, requestId) {
  const params = new URLSearchParams({
    symbol: row.symbol,
    signal_timestamp: row.signal_timestamp || '',
    entry_timestamp: row.entry_timestamp || '',
    exit_timestamp: row.exit_timestamp || '',
    underlying_entry_price: String(row.entry_price || ''),
  });
  const target = () => document.getElementById('optionProbe');
  try {
    const response = await fetch(`/api/intraday/option-probe?${params.toString()}`);
    const payload = await response.json();
    if (requestId !== state.optionProbeRequestId) return;
    const node = target();
    if (!node) return;
    if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
    node.innerHTML = renderOptionProbe(payload, row);
  } catch (error) {
    if (requestId !== state.optionProbeRequestId) return;
    const node = target();
    if (!node) return;
    node.innerHTML = `
      <div class="option-probe-head">
        <div>
          <span class="eyebrow">ATM Option What-if</span>
          <h4>Unavailable</h4>
        </div>
        ${expiryBadge(row)}
      </div>
      <p class="option-probe-note warning">${escapeHtml(error.message || String(error))}</p>
    `;
  }
}

function renderOptionProbe(payload, row) {
  const legs = payload.legs || [];
  const hasOkLeg = legs.some((leg) => leg.status === 'ok');
  const statusCopy = optionProbeStatusCopy(payload.status, hasOkLeg);
  return `
    <div class="option-probe-head">
      <div>
        <span class="eyebrow">ATM Option What-if</span>
        <h4>${escapeHtml(statusCopy.title)}</h4>
      </div>
      ${expiryBadge({
        ...row,
        option_expiry_date: payload.option_expiry_date,
        is_option_expiry_day: payload.is_option_expiry_day,
        option_dte_trading: payload.option_dte_trading,
      })}
    </div>
    <p class="option-probe-note ${statusCopy.tone}">${escapeHtml(statusCopy.body)}</p>
    ${legs.length ? `<div class="option-leg-grid">${legs.map(renderOptionLeg).join('')}</div>` : ''}
  `;
}

function optionProbeStatusCopy(status, hasOkLeg) {
  if (hasOkLeg) {
    return {
      title: 'ATM legs found',
      body: 'This is the raw premium path for buying the nearest ATM call and put at trade entry.',
      tone: '',
    };
  }
  if (status === 'missing_contracts') {
    return {
      title: 'Chain not cached',
      body: 'Run the targeted option backfill for this symbol/period to load ATM contracts and 1-minute premiums.',
      tone: 'warning',
    };
  }
  return {
    title: 'Candles not cached',
    body: 'The option contracts are known, but this signal window does not yet have cached option candles.',
    tone: 'warning',
  };
}

function renderOptionLeg(leg) {
  const contract = leg.contract || {};
  const optionType = leg.option_type || contract.option_type || '-';
  if (leg.status !== 'ok') {
    return `
      <article class="option-leg-card muted">
        <div class="option-leg-title">
          <strong>${escapeHtml(optionType)}</strong>
          <span>${escapeHtml(leg.status || 'missing')}</span>
        </div>
        <p>${escapeHtml(contract.trading_symbol || 'No cached candle data for this leg.')}</p>
      </article>
    `;
  }

  const lotSize = Number(contract.lot_size || 1);
  const maxPnl = (Number(leg.max_price) - Number(leg.entry_price)) * lotSize;
  const exitPnl = (Number(leg.exit_price) - Number(leg.entry_price)) * lotSize;
  return `
    <article class="option-leg-card ${optionType === 'CE' ? 'call' : 'put'}">
      <div class="option-leg-title">
        <strong>${escapeHtml(optionType)}</strong>
        <span>${escapeHtml(formatNumber(contract.strike_price, 0))}</span>
      </div>
      <p>${escapeHtml(contract.trading_symbol || '')}</p>
      <div class="option-leg-stats">
        <div><span>Entry</span><strong>${escapeHtml(formatNumber(leg.entry_price, 2))}</strong></div>
        <div><span>Max</span><strong>${escapeHtml(formatNumber(leg.max_price, 2))}</strong></div>
        <div><span>Max %</span><strong class="${Number(leg.max_return_pct || 0) >= 0 ? 'positive' : 'negative'}">${escapeHtml(formatPct(leg.max_return_pct))}</strong></div>
        <div><span>Exit %</span><strong class="${Number(leg.exit_return_pct || 0) >= 0 ? 'positive' : 'negative'}">${escapeHtml(formatPct(leg.exit_return_pct))}</strong></div>
        <div><span>Max/lot</span><strong>${escapeHtml(formatSignedMoney(maxPnl))}</strong></div>
        <div><span>Exit/lot</span><strong>${escapeHtml(formatSignedMoney(exitPnl))}</strong></div>
      </div>
    </article>
  `;
}

function resetFilters() {
  controls.search.value = '';
  controls.symbol.value = 'all';
  controls.bucketFilter.value = 'all';
  controls.outcome.value = 'all';
  controls.minRr.value = '';
  controls.minVolume.value = '';
  controls.minTurnover.value = '';
  controls.from.value = state.payload?.date_from || '';
  controls.to.value = state.payload?.date_to || '';
  state.activeBucket = 'all';
  state.selectedIndex = 0;
  state.selectedDateKey = '';
  render();
}

function applyPreset(preset) {
  resetFilters();
  if (preset === 'winners') {
    controls.minRr.value = '1';
  }
  if (preset === 'losses') {
    controls.outcome.value = [...controls.outcome.options].some((option) => option.value === 'stop') ? 'stop' : 'all';
  }
  if (preset === 'overnight') {
    controls.bucketFilter.value = 'two_day_hold';
    state.activeBucket = 'two_day_hold';
  }
  if (preset === 'eternal') {
    controls.symbol.value = [...controls.symbol.options].some((option) => option.value === 'ETERNAL') ? 'ETERNAL' : 'all';
    controls.search.value = 'ETERNAL';
  }
  state.selectedIndex = 0;
  render();
}

function clampPercent(value) {
  return `${Math.max(0, Math.min(100, value || 0))}%`;
}

[
  controls.search,
  controls.symbol,
  controls.from,
  controls.to,
  controls.bucketFilter,
  controls.outcome,
  controls.minRr,
  controls.minVolume,
  controls.minTurnover,
].forEach((control) => {
  control.addEventListener('input', () => {
    state.activeBucket = controls.bucketFilter.value || 'all';
    state.selectedIndex = 0;
    render();
  });
  control.addEventListener('change', () => {
    state.activeBucket = controls.bucketFilter.value || 'all';
    state.selectedIndex = 0;
    render();
  });
});

controls.reset.addEventListener('click', resetFilters);
controls.reportMode.addEventListener('change', loadReport);
document.querySelectorAll('[data-preset]').forEach((button) => {
  button.addEventListener('click', () => applyPreset(button.dataset.preset));
});

loadReport();
