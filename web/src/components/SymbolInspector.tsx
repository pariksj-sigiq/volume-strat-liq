import { BarChart3, Gauge, MoveUpRight, Radio } from "lucide-react";
import type { ReactNode } from "react";

import type { TerminalAlert, TerminalTick } from "../domain/terminal";
import { formatCompact, formatNumber, formatPct, formatTime } from "../lib/format";
import { Badge } from "./ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "./ui/card";
import { Separator } from "./ui/separator";

type SymbolInspectorProps = {
  selectedSymbol: string | null;
  tick: TerminalTick | null;
  alert: TerminalAlert | null;
  history: TerminalTick[];
};

export function SymbolInspector({ selectedSymbol, tick, alert, history }: SymbolInspectorProps) {
  const symbol = selectedSymbol ?? "Select symbol";
  const directionClass = Number(tick?.change_pct ?? 0) >= 0 ? "positive" : "negative";
  const movementVariant = Number(tick?.change_pct ?? 0) >= 0 ? "success" : "destructive";

  return (
    <Card className="terminal-panel inspector-panel" aria-labelledby="inspectorTitle">
      <CardHeader className="panel-heading">
        <div>
          <p className="eyebrow">Selected instrument</p>
          <CardTitle id="inspectorTitle">{symbol}</CardTitle>
        </div>
        <Badge className={`movement-badge ${directionClass}`} variant={movementVariant}>{formatPct(tick?.change_pct)}</Badge>
      </CardHeader>

      <CardContent className="inspector-content">
        <div className="price-hero">
          <span>Last traded price</span>
          <strong>{formatNumber(tick?.ltp)}</strong>
          <small>Last tick {formatTime(tick?.ts)}</small>
        </div>

        <Sparkline ticks={history} />

        <div className="metric-grid" aria-label="Selected symbol market details">
          <Metric icon={<MoveUpRight size={15} />} label="Best bid" value={formatNumber(tick?.best_bid)} />
          <Metric icon={<MoveUpRight size={15} />} label="Best ask" value={formatNumber(tick?.best_ask)} />
          <Metric icon={<BarChart3 size={15} />} label="Volume" value={formatCompact(tick?.volume_traded_today)} />
          <Metric icon={<Gauge size={15} />} label="Open interest" value={formatCompact(tick?.open_interest)} />
        </div>

        <Separator />

        <div className="signal-rule-panel">
          <div className="mini-heading">
            <Radio size={15} aria-hidden="true" />
            <strong>Official alert gate</strong>
          </div>
          <ul>
            <li>Pre-gates pass: regular session, no skip window, clean websocket bar, 25 closed 1-minute bars, TOD baseline ready.</li>
            <li>Close breaks above the prior 20 closed 1-minute bars.</li>
            <li>Volume is at least 2.5x the time-of-day baseline.</li>
            <li>Close is above same-bar session VWAP.</li>
            <li>Closed 1-minute bar range clears Wilder ATR quality and SL/TP validity checks.</li>
          </ul>
        </div>

        {alert ? (
          <div className="selected-alert">
            <span>Latest alert</span>
            <strong>{alert.profile_label} · {alert.risk_reward} · {formatNumber(alert.volume_multiple, 1)}x volume</strong>
            <dl className="price-ladder">
              <div><dt>Entry</dt><dd>{formatNumber(alert.entry)}</dd></div>
              <div><dt>SL</dt><dd>{formatNumber(alert.sl)}</dd></div>
              <div><dt>TP</dt><dd>{formatNumber(alert.tp)}</dd></div>
            </dl>
          </div>
        ) : (
          <p className="empty-state compact">No alert for this symbol in the retained terminal window.</p>
        )}
      </CardContent>
    </Card>
  );
}

type MetricProps = {
  icon: ReactNode;
  label: string;
  value: string;
};

function Metric({ icon, label, value }: MetricProps) {
  return (
    <div className="metric-tile">
      {icon}
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function Sparkline({ ticks }: { ticks: TerminalTick[] }) {
  const points = ticks
    .map((tick) => Number(tick.ltp))
    .filter((value) => Number.isFinite(value));

  if (points.length < 2) {
    return <div className="sparkline empty">Collecting price path</div>;
  }

  const min = Math.min(...points);
  const max = Math.max(...points);
  const range = max - min || 1;
  const width = 320;
  const height = 96;
  const path = points.map((value, index) => {
    const x = (index / (points.length - 1)) * width;
    const y = height - ((value - min) / range) * (height - 12) - 6;
    return `${index === 0 ? "M" : "L"} ${x.toFixed(2)} ${y.toFixed(2)}`;
  }).join(" ");

  const positive = points[points.length - 1] >= points[0];

  return (
    <svg className="sparkline" viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Recent LTP path">
      <path className="sparkline-grid" d="M 0 24 H 320 M 0 48 H 320 M 0 72 H 320" />
      <path className={positive ? "sparkline-path positive" : "sparkline-path negative"} d={path} />
    </svg>
  );
}
