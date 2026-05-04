import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import type { TerminalAlert, TerminalState, TerminalTick } from "../domain/terminal";
import { fetchTerminalState } from "../lib/api";
import {
  createAlertId,
  requestAlertNotificationPermission,
  sendBrowserAlertNotification,
  shouldNotifyAlert,
} from "../lib/notifications";

type Toast = {
  id: string;
  title: string;
  body: string;
};

type TickHistory = Record<string, TerminalTick[]>;

const MAX_HISTORY_POINTS = 72;
const POLL_INTERVAL_MS = 1500;

export function useTerminalState() {
  const [snapshot, setSnapshot] = useState<TerminalState | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [toasts, setToasts] = useState<Toast[]>([]);
  const [tickHistory, setTickHistory] = useState<TickHistory>({});
  const [browserAlertsEnabled, setBrowserAlertsEnabled] = useState(false);
  const seenAlerts = useRef<Set<string>>(new Set());
  const browserAlertsEnabledRef = useRef(false);

  const removeToast = useCallback((id: string) => {
    setToasts((current) => current.filter((toast) => toast.id !== id));
  }, []);

  const pushToast = useCallback((alert: TerminalAlert) => {
    const id = createAlertId(alert);
    setToasts((current) => [
      {
        id,
        title: `${alert.symbol} ${alert.profile_label}`,
        body: `Entry ${alert.entry.toFixed(2)} · ${alert.risk_reward}`,
      },
      ...current.filter((toast) => toast.id !== id),
    ].slice(0, 4));
    window.setTimeout(() => removeToast(id), 5000);
  }, [removeToast]);

  const updateTickHistory = useCallback((ticks: TerminalTick[]) => {
    setTickHistory((current) => {
      const next: TickHistory = { ...current };
      for (const tick of ticks) {
        const previous = next[tick.symbol] ?? [];
        const last = previous[previous.length - 1];
        if (last && last.ts === tick.ts && last.ltp === tick.ltp) {
          continue;
        }
        next[tick.symbol] = [...previous, tick].slice(-MAX_HISTORY_POINTS);
      }
      return next;
    });
  }, []);

  const load = useCallback(async () => {
    try {
      const next = await fetchTerminalState();
      setSnapshot(next);
      setLoadError(null);
      updateTickHistory(next.ticks);
      for (const alert of next.alerts) {
        if (!shouldNotifyAlert(seenAlerts.current, alert)) {
          continue;
        }
        pushToast(alert);
        if (browserAlertsEnabledRef.current) {
          sendBrowserAlertNotification(alert);
        }
      }
    } catch (error) {
      setLoadError(error instanceof Error ? error.message : String(error));
    }
  }, [pushToast, updateTickHistory]);

  const enableBrowserAlerts = useCallback(async () => {
    const enabled = await requestAlertNotificationPermission();
    browserAlertsEnabledRef.current = enabled;
    setBrowserAlertsEnabled(enabled);
    return enabled;
  }, []);

  useEffect(() => {
    void load();
    const interval = window.setInterval(() => void load(), POLL_INTERVAL_MS);
    return () => window.clearInterval(interval);
  }, [load]);

  const newestAlertSymbol = snapshot?.alerts[0]?.symbol;
  const defaultSymbol = newestAlertSymbol ?? snapshot?.ticks[0]?.symbol ?? null;

  return useMemo(() => ({
    snapshot,
    loadError,
    toasts,
    tickHistory,
    browserAlertsEnabled,
    defaultSymbol,
    enableBrowserAlerts,
    refresh: load,
    removeToast,
  }), [
    browserAlertsEnabled,
    defaultSymbol,
    enableBrowserAlerts,
    load,
    loadError,
    removeToast,
    snapshot,
    tickHistory,
    toasts,
  ]);
}
