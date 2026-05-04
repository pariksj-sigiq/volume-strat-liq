import { ClipboardCheck, Database, History, LockKeyhole, Zap } from "lucide-react";

import type { TerminalEvent, TerminalState } from "../domain/terminal";
import { formatCount, formatTime } from "../lib/format";
import { Badge } from "./ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "./ui/card";
import { ScrollArea } from "./ui/scroll-area";

type TerminalSidebarProps = {
  snapshot: TerminalState | null;
};

export function TerminalSidebar({ snapshot }: TerminalSidebarProps) {
  return (
    <aside className="side-stack" aria-label="Terminal context">
      <Card className="terminal-panel guardrail-panel">
        <CardHeader className="panel-heading">
          <div>
            <p className="eyebrow">Operator mode</p>
            <CardTitle>Paper alerts</CardTitle>
          </div>
          <LockKeyhole size={18} aria-hidden="true" />
        </CardHeader>
        <CardContent>
          <p>
            Live market data is active here. Execution remains paper/alert-only on this surface; no order route is exposed to the browser.
          </p>
          <dl className="fact-list">
            <div><dt>Socket</dt><dd>{snapshot?.connected ? "Direct token websocket" : snapshot?.last_error ?? "Not connected"}</dd></div>
            <div><dt>Signal mode</dt><dd>{snapshot?.signal_mode ?? "warmup_only"}</dd></div>
            <div><dt>Feed mode</dt><dd>{snapshot?.feed_mode ?? "-"}</dd></div>
            <div><dt>Last tick</dt><dd>{formatTime(snapshot?.last_tick_at)}</dd></div>
          </dl>
        </CardContent>
      </Card>

      <Card className={`terminal-panel readiness-panel ${snapshot?.signals_enabled ? "enabled" : "warmup"}`}>
        <CardHeader className="panel-heading">
          <div>
            <p className="eyebrow">Readiness</p>
            <CardTitle>{snapshot?.signals_enabled ? "Alerts enabled" : "No-signal warmup"}</CardTitle>
          </div>
          <Database size={18} aria-hidden="true" />
        </CardHeader>
        <CardContent>
          <p>{snapshot?.signal_status_reason ?? "Waiting for terminal state."}</p>
          <div className="baseline-meter" aria-label="TOD baseline coverage">
            <span style={{ width: baselinePercent(snapshot) }} />
          </div>
          <strong>{formatCount(snapshot?.baseline_count ?? 0)} of {formatCount(snapshot?.baseline_required ?? 0)} TOD baselines</strong>
          <p>{snapshot?.warmup_status_reason ?? "Rolling state warmup pending."}</p>
          <strong>{formatCount(snapshot?.warmup_seed_count ?? 0)} of {formatCount(snapshot?.warmup_required_count ?? 0)} symbols prewarmed</strong>
        </CardContent>
      </Card>

      <Card className="terminal-panel events-panel" aria-labelledby="eventsTitle">
        <CardHeader className="panel-heading">
          <div>
            <p className="eyebrow">Audit trail</p>
            <CardTitle id="eventsTitle">Event log</CardTitle>
          </div>
          <History size={18} aria-hidden="true" />
        </CardHeader>
        <CardContent>
          <ScrollArea className="event-scroll">
            <div className="event-list">
              {(snapshot?.events ?? []).slice(0, 28).length === 0 ? (
                <p className="empty-state compact">No events recorded.</p>
              ) : (snapshot?.events ?? []).slice(0, 28).map((event, index) => (
                <EventRow event={event} key={`${event.type}-${index}`} />
              ))}
            </div>
          </ScrollArea>
        </CardContent>
      </Card>

      <Card className="terminal-panel ios-contract-panel">
        <div className="mini-heading">
          <ClipboardCheck size={15} aria-hidden="true" />
          <strong>Shared client contract</strong>
        </div>
        <p>
          Web and future iOS clients consume the same terminal snapshot fields: feed state, ticks, alerts, readiness, and audit events.
        </p>
      </Card>
    </aside>
  );
}

function baselinePercent(snapshot: TerminalState | null) {
  const count = Number(snapshot?.baseline_count ?? 0);
  const required = Number(snapshot?.baseline_required ?? 0);
  if (!required) return "0%";
  return `${Math.min(100, Math.max(0, (count / required) * 100)).toFixed(1)}%`;
}

function EventRow({ event }: { event: TerminalEvent }) {
  const title = event.type || "event";
  const detail = eventField(event, "reason")
    ?? eventField(event, "error")
    ?? eventField(event, "symbol")
    ?? eventField(event, "mode")
    ?? (typeof eventField(event, "connected") === "boolean"
      ? (eventField(event, "connected") ? "connected" : "disconnected")
      : "updated");

  return (
    <div className="event-item">
      <Zap size={13} aria-hidden="true" />
      <div>
        <Badge variant="outline">{title}</Badge>
        <span>{String(detail)}</span>
      </div>
    </div>
  );
}

function eventField(event: TerminalEvent, key: string): unknown {
  return (event as Record<string, unknown>)[key];
}
