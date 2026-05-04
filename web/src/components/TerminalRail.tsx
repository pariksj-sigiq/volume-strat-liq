import { Activity, BellRing, ChartCandlestick, LineChart, RadioTower, ShieldCheck } from "lucide-react";
import type { ReactNode } from "react";

import type { TerminalState } from "../domain/terminal";
import { formatCount, formatTime } from "../lib/format";
import { Badge } from "./ui/badge";
import { Button } from "./ui/button";
import { Separator } from "./ui/separator";

type TerminalRailProps = {
  snapshot: TerminalState | null;
};

export function TerminalRail({ snapshot }: TerminalRailProps) {
  const connected = Boolean(snapshot?.connected);
  const signalsEnabled = Boolean(snapshot?.signals_enabled);

  return (
    <aside className="terminal-rail" aria-label="Terminal navigation">
      <div className="rail-brand" aria-label="Liq Sweep Terminal">
        <div className="brand-mark">LS</div>
        <div>
          <p className="eyebrow">Liq Sweep</p>
          <strong>Trading Terminal</strong>
        </div>
      </div>

      <nav className="rail-nav" aria-label="Primary">
        <Button asChild variant="secondary">
          <a aria-current="page" href="/terminal">
            <ChartCandlestick size={16} aria-hidden="true" />
            Live terminal
          </a>
        </Button>
        <Button asChild variant="ghost">
          <a href="/">
            <LineChart size={16} aria-hidden="true" />
            Intraday research
          </a>
        </Button>
        <Button asChild variant="ghost">
          <a href="/daily">
            <Activity size={16} aria-hidden="true" />
            Daily futures
          </a>
        </Button>
      </nav>

      <Separator />

      <div className="rail-status">
        <RailStat
          icon={<RadioTower size={15} aria-hidden="true" />}
          label="Feed"
          value={connected ? `Live ${snapshot?.feed_mode ?? "full"}` : "Offline"}
          tone={connected ? "success" : "destructive"}
        />
        <RailStat
          icon={<ShieldCheck size={15} aria-hidden="true" />}
          label="Signals"
          value={signalsEnabled ? "Enabled" : "Warmup"}
          tone={signalsEnabled ? "success" : "warning"}
        />
        <RailStat
          icon={<BellRing size={15} aria-hidden="true" />}
          label="Alerts"
          value={formatCount(snapshot?.alert_count ?? 0)}
          tone={(snapshot?.alert_count ?? 0) > 0 ? "success" : "secondary"}
        />
      </div>

      <div className="rail-footer">
        <span>Server</span>
        <strong>{formatTime(snapshot?.server_time)}</strong>
      </div>
    </aside>
  );
}

type RailStatProps = {
  icon: ReactNode;
  label: string;
  value: string;
  tone: "success" | "warning" | "destructive" | "secondary";
};

function RailStat({ icon, label, value, tone }: RailStatProps) {
  return (
    <div className="rail-stat">
      {icon}
      <span>{label}</span>
      <Badge variant={tone}>{value}</Badge>
    </div>
  );
}
