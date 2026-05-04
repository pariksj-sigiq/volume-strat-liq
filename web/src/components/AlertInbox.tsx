import { Bell, CheckCircle2, TimerReset } from "lucide-react";

import type { TerminalAlert, TerminalState } from "../domain/terminal";
import { formatNumber, formatTime } from "../lib/format";
import { Badge } from "./ui/badge";
import { Button } from "./ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "./ui/card";
import { ScrollArea } from "./ui/scroll-area";

type AlertInboxProps = {
  alerts: TerminalAlert[];
  snapshot: TerminalState | null;
  selectedSymbol: string | null;
  onSelectSymbol: (symbol: string) => void;
};

export function AlertInbox({ alerts, snapshot, selectedSymbol, onSelectSymbol }: AlertInboxProps) {
  const minBars = snapshot?.min_bars_for_signal ?? 25;
  const interval = formatInterval(snapshot?.bar_interval ?? "1min");
  const topBlocks = Object.entries(snapshot?.signal_block_counts ?? {})
    .sort((left, right) => right[1] - left[1])
    .slice(0, 3);

  return (
    <Card className="terminal-panel alert-panel" aria-labelledby="alertsTitle">
      <CardHeader className="panel-heading">
        <div>
          <p className="eyebrow">Notifications</p>
          <CardTitle id="alertsTitle">Alert inbox</CardTitle>
        </div>
        <Badge className="panel-count" variant={alerts.length ? "success" : "secondary"}>{alerts.length}</Badge>
      </CardHeader>

      <CardContent>
        <ScrollArea className="alert-scroll">
          <div className="alert-list">
            {alerts.length === 0 ? (
              <div className="empty-alerts">
                <Bell size={20} aria-hidden="true" />
                <strong>No official alerts yet</strong>
                <span>
                  The scanner stays quiet until gates pass first: regular session, no skip window, clean websocket bar, {minBars} closed {interval} bars, and fresh TOD baseline. Then breakout, volume, VWAP, ATR range, and SL/TP checks must all pass.
                </span>
                {topBlocks.length > 0 ? (
                  <div className="alert-metrics" aria-label="Recent alert gate blockers">
                    {topBlocks.map(([reason, count]) => (
                      <Badge key={reason} variant="outline">{formatReason(reason)} · {count}</Badge>
                    ))}
                  </div>
                ) : null}
              </div>
            ) : alerts.slice(0, 20).map((alert) => (
              <Button
                className={`alert-row ${alert.symbol === selectedSymbol ? "active" : ""}`}
                key={`${alert.symbol}-${alert.generated_at}-${alert.entry}`}
                type="button"
                variant="ghost"
                onClick={() => onSelectSymbol(alert.symbol)}
              >
                <div className="alert-row-main">
                  <span className="alert-time">{formatTime(alert.generated_at)}</span>
                  <strong>{alert.symbol}</strong>
                  <small>{alert.sector || "Unknown sector"}</small>
                </div>
                <div className="alert-metrics">
                  <Badge variant={alert.profile_name === "fast" ? "warning" : "success"}>{alert.profile_label}</Badge>
                  <Badge variant="success"><CheckCircle2 size={13} aria-hidden="true" />{alert.risk_reward}</Badge>
                  <Badge variant="warning"><TimerReset size={13} aria-hidden="true" />{formatNumber(alert.volume_multiple, 1)}x vol</Badge>
                </div>
                <dl className="price-ladder">
                  <div><dt>Entry</dt><dd>{formatNumber(alert.entry)}</dd></div>
                  <div><dt>SL</dt><dd>{formatNumber(alert.sl)}</dd></div>
                  <div><dt>TP</dt><dd>{formatNumber(alert.tp)}</dd></div>
                </dl>
              </Button>
            ))}
          </div>
        </ScrollArea>
      </CardContent>
    </Card>
  );
}

function formatInterval(interval: string) {
  return interval === "1min" ? "1-minute" : interval;
}

function formatReason(reason: string) {
  return reason.replaceAll("_", " ");
}
