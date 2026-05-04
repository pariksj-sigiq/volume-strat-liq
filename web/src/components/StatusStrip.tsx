import {
  BellRing,
  CircleAlert,
} from "lucide-react";

import type { TerminalState } from "../domain/terminal";
import { formatTime } from "../lib/format";
import { Badge } from "./ui/badge";
import { Button } from "./ui/button";

type StatusStripProps = {
  snapshot: TerminalState | null;
  loadError: string | null;
  browserAlertsEnabled: boolean;
  onEnableAlerts: () => Promise<boolean>;
};

export function StatusStrip({
  snapshot,
  loadError,
  browserAlertsEnabled,
  onEnableAlerts,
}: StatusStripProps) {
  const signalsEnabled = Boolean(snapshot?.signals_enabled);

  return (
    <header className="site-header">
      <div className="site-header-copy">
        <div className="breadcrumb-line" aria-label="Current workspace">
          <span>Terminal</span>
          <span>/</span>
          <strong>Live NSE scanner</strong>
        </div>
        <h1>Live breakout terminal</h1>
        <p>{snapshot?.signal_status_reason ?? "Waiting for live scanner state."}</p>
      </div>

      <div className="topbar-actions">
        <Badge variant={signalsEnabled ? "success" : "warning"}>
          {signalsEnabled ? "Signals enabled" : "Warmup only"}
        </Badge>
        <div className="clock-block">
          <span>Server</span>
          <strong>{formatTime(snapshot?.server_time)}</strong>
        </div>
        <Button variant="outline" className="icon-action" type="button" onClick={() => void onEnableAlerts()}>
          <BellRing size={17} aria-hidden="true" />
          <span>{browserAlertsEnabled ? "Browser alerts on" : "Enable alerts"}</span>
        </Button>
      </div>

      {(loadError || snapshot?.last_error) && (
        <div className="terminal-error-banner" role="status">
          <CircleAlert size={16} aria-hidden="true" />
          <span>{loadError ?? snapshot?.last_error}</span>
        </div>
      )}
    </header>
  );
}
