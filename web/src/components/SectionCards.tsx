import { Activity, BellRing, DatabaseZap, KeyRound, RadioTower, ShieldCheck } from "lucide-react";
import type { ReactNode } from "react";

import type { TerminalState } from "../domain/terminal";
import { formatCount, formatTime } from "../lib/format";
import { Badge } from "./ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "./ui/card";
import { Progress } from "./ui/progress";

type SectionCardsProps = {
  snapshot: TerminalState | null;
};

export function SectionCards({ snapshot }: SectionCardsProps) {
  const connected = Boolean(snapshot?.connected);
  const signalsEnabled = Boolean(snapshot?.signals_enabled);
  const baselineValue = baselinePercent(snapshot);

  return (
    <section className="section-cards" aria-label="Terminal health summary">
      <FeedCard snapshot={snapshot} connected={connected} />
      <HealthCard
        icon={<ShieldCheck size={17} aria-hidden="true" />}
        label="Signals"
        title={signalsEnabled ? "Enabled" : "Warmup"}
        description={snapshot?.signal_status_reason ?? "Waiting for scanner state"}
        variant={signalsEnabled ? "success" : "warning"}
      />
      <Card className="summary-card">
        <CardHeader>
          <div className="summary-card-heading">
            <DatabaseZap size={17} aria-hidden="true" />
            <Badge variant={signalsEnabled ? "success" : "warning"}>TOD</Badge>
          </div>
          <CardTitle>{formatCount(snapshot?.baseline_count ?? 0)} / {formatCount(snapshot?.baseline_required ?? 0)}</CardTitle>
          <CardDescription>Baseline coverage</CardDescription>
        </CardHeader>
        <CardContent>
          <Progress value={baselineValue} aria-label="TOD baseline coverage" />
          <span className="summary-caption">{baselineValue.toFixed(1)}%</span>
        </CardContent>
      </Card>
      <HealthCard
        icon={<BellRing size={17} aria-hidden="true" />}
        label="Alerts"
        title={formatCount(snapshot?.alert_count ?? 0)}
        description={`Last tick ${formatTime(snapshot?.last_tick_at)}`}
        variant={(snapshot?.alert_count ?? 0) > 0 ? "success" : "secondary"}
        footerIcon={<Activity size={14} aria-hidden="true" />}
      />
    </section>
  );
}

function FeedCard({ snapshot, connected }: { snapshot: TerminalState | null; connected: boolean }) {
  return (
    <Card className={`summary-card feed-summary-card ${connected ? "summary-card-success" : "summary-card-destructive"}`}>
      <CardHeader>
        <div className="summary-card-heading">
          <RadioTower size={17} aria-hidden="true" />
          <Badge variant={connected ? "success" : "destructive"}>Feed</Badge>
        </div>
        <CardTitle>{connected ? "Connected" : "Offline"}</CardTitle>
        <CardDescription>
          {connected ? `${snapshot?.feed_mode ?? "full"} mode websocket` : snapshot?.last_error ?? "No active feed"}
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="feed-detail-grid" aria-label="Websocket feed details">
          <FeedMetric label="Subscribed" value={formatCount(snapshot?.subscribed_instrument_count ?? snapshot?.universe_count ?? 0)} />
          <FeedMetric label="Ticking" value={formatCount(snapshot?.active_instrument_count ?? snapshot?.tick_count ?? 0)} />
          <FeedMetric label="Ticks/min" value={formatCount(snapshot?.tick_rate_per_min ?? 0)} />
          <FeedMetric label="Total ticks" value={formatCount(snapshot?.ticks_total ?? 0)} />
        </div>
        <span className="summary-inline feed-security-note">
          <KeyRound size={14} aria-hidden="true" />
          Token and creds stay server-side; browser only sees this sanitized snapshot.
        </span>
      </CardContent>
    </Card>
  );
}

function FeedMetric({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

type HealthCardProps = {
  icon: ReactNode;
  label: string;
  title: string;
  description: string;
  variant: "success" | "warning" | "destructive" | "secondary";
  footerIcon?: ReactNode;
};

function HealthCard({ icon, label, title, description, variant, footerIcon }: HealthCardProps) {
  return (
    <Card className={`summary-card summary-card-${variant}`}>
      <CardHeader>
        <div className="summary-card-heading">
          {icon}
          <Badge variant={variant}>{label}</Badge>
        </div>
        <CardTitle>{title}</CardTitle>
        <CardDescription>{description}</CardDescription>
      </CardHeader>
      {footerIcon && (
        <CardContent>
          <span className="summary-inline">{footerIcon} Live stream</span>
        </CardContent>
      )}
    </Card>
  );
}

function baselinePercent(snapshot: TerminalState | null) {
  const count = Number(snapshot?.baseline_count ?? 0);
  const required = Number(snapshot?.baseline_required ?? 0);
  if (!required) return 0;
  return Math.min(100, Math.max(0, (count / required) * 100));
}
