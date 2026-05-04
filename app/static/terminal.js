const nodes = {
  status: document.getElementById('terminalStatus'),
  updated: document.getElementById('terminalUpdated'),
  kpis: document.getElementById('terminalKpis'),
  alertCount: document.getElementById('alertCount'),
  alerts: document.getElementById('alertList'),
  ticks: document.getElementById('tickTableBody'),
  events: document.getElementById('eventLog'),
  facts: document.getElementById('terminalFacts'),
  search: document.getElementById('terminalSearch'),
  enableNotifications: document.getElementById('enableNotifications'),
  toastRegion: document.getElementById('toastRegion'),
  signalBanner: document.getElementById('signalBanner'),
};

const state = {
  payload: null,
  seenAlerts: new Set(),
  notificationsEnabled: false,
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
  const number = Number(value);
  if (!Number.isFinite(number)) return '-';
  return Math.round(number).toLocaleString('en-IN');
}

function formatPct(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return '-';
  return `${number >= 0 ? '+' : ''}${number.toFixed(2)}%`;
}

function formatTime(value) {
  if (!value) return '-';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '-';
  return new Intl.DateTimeFormat('en-IN', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).format(date);
}

async function loadState() {
  try {
    const response = await fetch('/api/terminal/state', { cache: 'no-store' });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
    handleNewAlerts(payload.alerts || []);
    state.payload = payload;
    render();
  } catch (error) {
    nodes.status.textContent = error.message || String(error);
    nodes.status.className = 'terminal-status error';
  }
}

function render() {
  const payload = state.payload;
  if (!payload) return;
  nodes.status.textContent = payload.connected ? `Live ${payload.feed_mode}` : 'Feed offline';
  nodes.status.className = `terminal-status ${payload.connected ? 'live' : 'offline'}`;
  nodes.updated.textContent = payload.last_tick_at ? `Last tick ${formatTime(payload.last_tick_at)}` : 'No ticks yet';
  renderKpis(payload);
  renderFacts(payload);
  renderSignalBanner(payload);
  renderAlerts(payload.alerts || []);
  renderTicks(payload.ticks || []);
  renderEvents(payload.events || []);
}

function renderSignalBanner(payload) {
  if (payload.signals_enabled) {
    nodes.signalBanner.className = 'terminal-signal-banner enabled';
    nodes.signalBanner.innerHTML = `
      <strong>Alerts enabled</strong>
      <span>${escapeHtml(payload.baseline_count)} TOD baselines loaded. ${escapeHtml(payload.signal_status_reason || '')}</span>
    `;
    return;
  }
  nodes.signalBanner.className = 'terminal-signal-banner warmup';
  nodes.signalBanner.innerHTML = `
    <strong>No-signal warmup</strong>
    <span>${escapeHtml(payload.signal_status_reason || 'TOD baseline is not ready.')} ${escapeHtml(payload.baseline_count || 0)}/${escapeHtml(payload.baseline_required || 0)} baselines loaded.</span>
  `;
}

function renderKpis(payload) {
  const cards = [
    ['Universe', payload.universe_count],
    ['Ticking', payload.tick_count],
    ['Alerts', payload.alert_count],
    ['Mode', payload.feed_mode],
    ['Socket', payload.connected ? 'Connected' : 'Offline'],
  ];
  nodes.kpis.innerHTML = cards.map(([label, value]) => `
    <div class="terminal-kpi">
      <span>${escapeHtml(label)}</span>
      <strong>${escapeHtml(typeof value === 'number' ? formatCount(value) : value)}</strong>
    </div>
  `).join('');
}

function renderFacts(payload) {
  const facts = [
    ['Execution', 'Paper alerts only'],
    ['Socket', payload.connected ? 'Direct token websocket' : (payload.last_error || 'Not started')],
    ['Signal mode', payload.signal_mode || 'warmup_only'],
    ['Baselines', `${payload.baseline_count || 0}/${payload.baseline_required || 0}`],
    ['Feed mode', payload.feed_mode || '-'],
    ['Server time', formatTime(payload.server_time)],
  ];
  nodes.facts.innerHTML = facts.map(([term, value]) => `
    <dt>${escapeHtml(term)}</dt>
    <dd>${escapeHtml(value)}</dd>
  `).join('');
}

function renderAlerts(alerts) {
  nodes.alertCount.textContent = formatCount(alerts.length);
  if (!alerts.length) {
    nodes.alerts.innerHTML = '<p class="terminal-empty">No trade alerts yet. Official alerts stay quiet until baseline and signal state are ready.</p>';
    return;
  }
  nodes.alerts.innerHTML = alerts.slice(0, 12).map((alert) => `
    <article class="alert-item">
      <div>
        <span>${escapeHtml(formatTime(alert.generated_at))}</span>
        <h3>${escapeHtml(alert.symbol)} long setup</h3>
      </div>
      <strong>${escapeHtml(alert.risk_reward)}</strong>
      <dl>
        <dt>Entry</dt><dd>${escapeHtml(formatNumber(alert.entry))}</dd>
        <dt>SL</dt><dd>${escapeHtml(formatNumber(alert.sl))}</dd>
        <dt>TP</dt><dd>${escapeHtml(formatNumber(alert.tp))}</dd>
        <dt>Volume</dt><dd>${escapeHtml(formatNumber(alert.volume_multiple, 1))}x</dd>
      </dl>
    </article>
  `).join('');
}

function renderTicks(ticks) {
  const query = nodes.search.value.trim().toUpperCase();
  const visible = ticks
    .filter((tick) => !query || String(tick.symbol || '').includes(query))
    .slice(0, 80);
  if (!visible.length) {
    nodes.ticks.innerHTML = '<tr><td colspan="7">No ticks match the current filter.</td></tr>';
    return;
  }
  nodes.ticks.innerHTML = visible.map((tick) => `
    <tr>
      <td><strong>${escapeHtml(tick.symbol)}</strong></td>
      <td>${escapeHtml(formatNumber(tick.ltp))}</td>
      <td class="${Number(tick.change_pct || 0) >= 0 ? 'positive' : 'negative'}">${escapeHtml(formatPct(tick.change_pct))}</td>
      <td>${escapeHtml(formatNumber(tick.best_bid))}</td>
      <td>${escapeHtml(formatNumber(tick.best_ask))}</td>
      <td>${escapeHtml(formatCount(tick.volume_traded_today))}</td>
      <td>${escapeHtml(formatTime(tick.ts))}</td>
    </tr>
  `).join('');
}

function renderEvents(events) {
  if (!events.length) {
    nodes.events.innerHTML = '<p class="terminal-empty">No events recorded.</p>';
    return;
  }
  nodes.events.innerHTML = events.slice(0, 18).map((event) => `
    <div class="event-row">
      <span>${escapeHtml(event.type || 'event')}</span>
      <p>${escapeHtml(event.error || event.symbol || event.mode || 'updated')}</p>
    </div>
  `).join('');
}

function handleNewAlerts(alerts) {
  alerts.forEach((alert) => {
    const id = `${alert.symbol}:${alert.generated_at}:${alert.entry}`;
    if (state.seenAlerts.has(id)) return;
    state.seenAlerts.add(id);
    showToast(`${alert.symbol} ${alert.reason}`, `Entry ${formatNumber(alert.entry)} · ${alert.risk_reward}`);
    if (state.notificationsEnabled && window.Notification?.permission === 'granted') {
      new Notification(`${alert.symbol} long setup`, {
        body: `Entry ${formatNumber(alert.entry)} · SL ${formatNumber(alert.sl)} · TP ${formatNumber(alert.tp)}`,
      });
    }
  });
}

function showToast(title, body) {
  const toast = document.createElement('div');
  toast.className = 'terminal-toast';
  toast.innerHTML = `<strong>${escapeHtml(title)}</strong><span>${escapeHtml(body)}</span>`;
  nodes.toastRegion.append(toast);
  window.setTimeout(() => toast.remove(), 5000);
}

nodes.enableNotifications.addEventListener('click', async () => {
  if (!('Notification' in window)) {
    showToast('Browser notifications unavailable', 'In-app alerts will still appear here.');
    return;
  }
  const permission = await Notification.requestPermission();
  state.notificationsEnabled = permission === 'granted';
  nodes.enableNotifications.textContent = state.notificationsEnabled ? 'Alerts enabled' : 'Enable alerts';
});

nodes.search.addEventListener('input', render);

loadState();
window.setInterval(loadState, 1500);
